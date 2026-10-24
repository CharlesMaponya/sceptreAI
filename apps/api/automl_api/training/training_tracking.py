"""Declared tracking creation uses control; uncertainty permits receipt lookup only."""

import hashlib
import os
import uuid
from urllib.parse import quote

import mlflow

from automl_api.schemas.training_tracking import (
    TRAINING_TRACKING_PROTOCOL,
    TrainingTrackingReply,
    TrainingTrackingRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC


def ensure_remote_tracking(run_id, role, *, parent_run_id):
    protocol = os.getenv("AUTOML_TRAINING_TRACKING_PROTOCOL")
    if protocol is None:
        # shortcut: saved workers retain SQL tracking creation until their broker rollout drains.
        return None
    try:
        from automl_api.training.worker import _fenced_attempt_context

        context = _fenced_attempt_context()
        if protocol != TRAINING_TRACKING_PROTOCOL or context is None:
            raise ValueError("Tracking requires its declared execution")
        request = TrainingTrackingRequest(run_id=run_id, role=role)
        if (role == "parent") != (parent_run_id is None):
            raise ValueError("Tracking requires its matching local parent context")
        payload = request.model_dump(mode="json")
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        try:
            response = control.request(
                "POST", "training/tracking", payload=payload, max_bytes=8192, timeout_seconds=60
            )
        except AttemptControlUnavailable:
            response = control.request(
                "GET",
                f"training/tracking/{run_id}/receipts/{quote(role, safe='')}",
                max_bytes=8192,
                timeout_seconds=60,
            )
        reply = TrainingTrackingReply.model_validate(response)
        if (
            reply.run_id != run_id
            or reply.role != role
            or reply.attempt_id != context[0]
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
            or reply.request_digest != canonical_request_hash(payload)
            or reply.parent_tracking_run_id != parent_run_id
            or reply.tracking_backend_hash
            != hashlib.sha256(mlflow.get_tracking_uri().encode()).hexdigest()
        ):
            raise ValueError("Tracking acknowledgement belongs to another request")
        return reply.tracking_run_id
    except Exception:
        raise StaleFence(
            "Training tracking could not verify its committed acknowledgement"
        ) from None
