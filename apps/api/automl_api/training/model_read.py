"""Verify the complete exact-version model before worker-only deserialization."""

import hashlib
import hmac
import os
import tempfile
import uuid
from typing import Literal
from urllib.parse import urlsplit

import joblib

from automl_api.schemas.training_model_read import (
    TRAINING_MODEL_READ_PROTOCOL,
    TrainingModelReadRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptRPC
from automl_api.training.object_capabilities import (
    MAX_GRANT_RESPONSE_BYTES,
    _open_granted_stream,
    _ReadGrant,
)


class ModelReadGrant(_ReadGrant):
    protocol: Literal["training-model-read-v1"]
    project_id: uuid.UUID
    attempt_id: uuid.UUID
    run_id: uuid.UUID
    candidate_name: str
    request_id: uuid.UUID
    request_digest: str
    audience: str
    uri: str

    @property
    def byte_size(self):
        return self.byte_limit


def restore_model(run, entry):
    from automl_api.training.worker import _fenced_attempt_context

    try:
        context = _fenced_attempt_context()
        if (
            os.environ["AUTOML_TRAINING_MODEL_READ_PROTOCOL"] != TRAINING_MODEL_READ_PROTOCOL
            or context is None
            or uuid.UUID(os.environ["AUTOML_PROJECT_ID"]) != run.project_id
        ):
            raise ValueError("Unsupported model read context")
        request = TrainingModelReadRequest(
            run_id=run.id,
            candidate_name=entry["model"],
            request_id=uuid.uuid4(),
        )
        payload = request.model_dump(mode="json")
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        # A lost URL is not recoverable: never resend the same read intent.
        grant = ModelReadGrant.model_validate(
            control.request(
                "POST",
                "training/models/read",
                payload=payload,
                max_bytes=MAX_GRANT_RESPONSE_BYTES,
            )
        )
        if (
            grant.project_id != run.project_id
            or grant.run_id != run.id
            or grant.attempt_id != context[0]
            or grant.candidate_name != entry["model"]
            or grant.request_id != request.request_id
            or grant.request_digest != canonical_request_hash(payload)
            or grant.audience != attempt_audience(context[0], context[1])
            or grant.uri != entry["model_artifact_uri"]
            or grant.sha256 != entry["model_artifact_sha256"]
            or not 0 < grant.byte_limit <= 5_000_000_000
        ):
            raise ValueError("Model read acknowledgement differs from its declaration")
        driver = {"s3": "aws_s3", "s3c": "s3_compatible", "gs": "gcs", "az": "azure_blob"}[
            urlsplit(grant.uri).scheme
        ]
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as model:
            digest, size = hashlib.sha256(), 0
            with _open_granted_stream(
                control,
                grant,
                "candidate",
                0,
                driver,
                None,
                grant=grant,
            ) as source:
                while block := source.read(65536):
                    size += len(block)
                    digest.update(block)
                    model.write(block)
            if size != grant.byte_limit or not hmac.compare_digest(
                digest.hexdigest(), grant.sha256
            ):
                raise ValueError("Model stream differs from its verified custody")
            model.seek(0)
            return joblib.load(model)
    except Exception:
        raise StaleFence("Training model restoration could not verify its approved model") from None
