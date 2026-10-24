"""Final publication with fresh Pod identity and receipt-only ACK recovery."""

import os
import uuid

from automl_api.schemas.training_result import (
    RESULT_PARAM_KEYS,
    TRAINING_RESULT_PROTOCOL,
    TrainingResultReply,
    TrainingResultRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC


def publish_training_result(run_id, result, mlflow_run_id):
    protocol = os.getenv("AUTOML_TRAINING_RESULT_PROTOCOL")
    if protocol is None:
        # shortcut: saved workers retain SQL publication until the result protocol drains them.
        return None
    try:
        from automl_api.training.pipeline import _json_safe
        from automl_api.training.worker import _fenced_attempt_context

        context = _fenced_attempt_context()
        if protocol != TRAINING_RESULT_PROTOCOL or context is None:
            raise ValueError("Result publication requires its current declared execution")
        request = TrainingResultRequest(
            run_id=run_id,
            metrics=_json_safe(result.metrics),
            params=_json_safe(
                {key: value for key, value in result.params.items() if key in RESULT_PARAM_KEYS}
            ),
            leaderboard=_json_safe(result.leaderboard),
            primary_metric=result.primary_metric,
            mlflow_run_id=mlflow_run_id,
        )
        payload = request.model_dump(mode="json")
        digest = canonical_request_hash(payload)
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        try:
            response = control.request(
                "POST", "training/result", payload=payload, max_bytes=65536, timeout_seconds=60
            )
        except AttemptControlUnavailable:
            response = control.request(
                "GET",
                f"training/result/{run_id}/receipts/{digest}",
                max_bytes=65536,
                timeout_seconds=60,
            )
        reply = TrainingResultReply.model_validate(response)
        if (
            reply.run_id != run_id
            or reply.attempt_id != context[0]
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
            or reply.request_digest != digest
        ):
            raise ValueError("Result acknowledgement belongs to another execution")
        return True
    except Exception:
        raise StaleFence("Training result could not verify its committed acknowledgement") from None
