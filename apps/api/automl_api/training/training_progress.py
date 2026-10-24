"""Ordered progress control using fresh Pod identity and receipt-only recovery."""

import os
import threading
import uuid

from automl_api.schemas.training_progress import (
    TRAINING_PROGRESS_PROTOCOL,
    TrainingProgressReply,
    TrainingProgressRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC

_streams = {}
_lock = threading.Lock()


def publish_training_progress(run_id, action, **arguments):
    protocol = os.getenv("AUTOML_TRAINING_PROGRESS_PROTOCOL")
    if protocol is None:
        # shortcut: saved workers retain SQL progress until the broker rollout drains them.
        return None
    try:
        from automl_api.training.pipeline import _json_safe
        from automl_api.training.worker import _fenced_attempt_context

        context = _fenced_attempt_context()
        if protocol != TRAINING_PROGRESS_PROTOCOL or context is None:
            raise ValueError("Progress requires its declared current execution")
        key = (
            context[0],
            run_id,
            arguments.get("candidate_name") if action == "phase" else "parent",
        )
        with _lock:
            client, sequence = _streams.get(key, (uuid.uuid4(), 0))
            sequence += 1
            _streams[key] = (client, sequence)
        request = TrainingProgressRequest(
            run_id=run_id,
            request_id=uuid.uuid4(),
            client_id=client,
            sequence=sequence,
            action=action,
            **_json_safe(arguments),
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
                "POST", "training/progress", payload=payload, max_bytes=65536, timeout_seconds=60
            )
        except AttemptControlUnavailable:
            response = control.request(
                "GET",
                f"training/progress/{run_id}/receipts/{request.request_id}",
                max_bytes=65536,
                timeout_seconds=60,
            )
        reply = TrainingProgressReply.model_validate(response)
        if (
            reply.run_id != run_id
            or reply.attempt_id != context[0]
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
            or reply.request_id != request.request_id
            or reply.request_digest != digest
        ):
            raise ValueError("Progress acknowledgement belongs to another request")
        return True
    except Exception:
        raise StaleFence(
            "Training progress could not verify its committed acknowledgement"
        ) from None
