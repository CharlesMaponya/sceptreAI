"""Publish candidate bytes once and recover only the original acknowledgement."""

import hashlib
import os
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import quote, unquote, urlsplit

from pydantic import Field

from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import immutable_object_version
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC
from automl_api.training.object_capabilities import MAX_GRANT_RESPONSE_BYTES, _OutputReceipt

MODEL_OUTPUT_PROTOCOL = "training-model-output-v1"


class ModelOutputReceipt(_OutputReceipt):
    project_id: uuid.UUID
    run_id: uuid.UUID
    attempt_id: uuid.UUID
    candidate_id: uuid.UUID
    candidate_name: str = Field(min_length=1, max_length=160)


def persist_model_output(run, candidate_name, payload):
    try:
        from automl_api.training.worker import _fenced_attempt_context

        context = _fenced_attempt_context()
        if os.environ["AUTOML_MODEL_OUTPUT_PROTOCOL"] != MODEL_OUTPUT_PROTOCOL or context is None:
            raise ValueError("Unsupported model output authority")
        project = uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
        if project != run.project_id or not 0 < len(candidate_name) <= 160:
            raise ValueError("Model output belongs to another project")
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        digest, nonce = hashlib.sha256(payload).hexdigest(), uuid.uuid4()
        path = f"models/{run.id}/{quote(candidate_name, safe='')}"
        try:
            response = control.request(
                "PUT",
                f"{path}/{digest}",
                content=payload,
                request_id=nonce,
                max_bytes=MAX_GRANT_RESPONSE_BYTES,
            )
        except AttemptControlUnavailable:
            response = control.request(
                "GET",
                f"{path}/receipts/{nonce}",
                max_bytes=MAX_GRANT_RESPONSE_BYTES,
            )
        receipt = ModelOutputReceipt.model_validate(response)
        immutable_object_version(receipt.object_version)
        location = urlsplit(receipt.uri)
        suffix = (
            f"/projects/{project}/runs/{run.id}/attempts/{context[0]}/"
            f"models/{receipt.candidate_id}/{digest}.joblib"
        )
        now = datetime.now(UTC)
        if (
            receipt.project_id != project
            or receipt.run_id != run.id
            or receipt.attempt_id != context[0]
            or receipt.candidate_name != candidate_name
            or receipt.sha256 != digest
            or receipt.byte_size != len(payload)
            or not now < receipt.expires_at <= now + timedelta(minutes=15)
            or location.scheme not in {"s3", "s3c", "gs", "az"}
            or not location.netloc
            or location.username is not None
            or location.password is not None
            or location.query
            or location.fragment
            or not unquote(location.path).endswith(suffix)
            or any(part in {".", ".."} for part in unquote(location.path).split("/"))
        ):
            raise ValueError("Model acknowledgement differs from its declaration")
        return receipt.uri, digest
    except Exception:
        raise StaleFence("Training model output could not verify its acknowledgement") from None
