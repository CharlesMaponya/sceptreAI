"""Scoped metadata writes; uncertain POSTs permit one receipt GET, never a resend."""

import os
import uuid
from contextlib import contextmanager
from contextvars import ContextVar

import mlflow

from automl_api.schemas.training_metadata import (
    TRAINING_METADATA_PROTOCOL,
    TrainingMetadataReply,
    TrainingMetadataRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC

_active_metadata = ContextVar("training_metadata", default=None)


def enabled():
    protocol = os.getenv("AUTOML_TRAINING_METADATA_PROTOCOL")
    if protocol is None:
        return False
    from automl_api.schemas.training_tracking import TRAINING_TRACKING_PROTOCOL
    from automl_api.training.worker import _fenced_attempt_context

    if (
        protocol != TRAINING_METADATA_PROTOCOL
        or os.getenv("AUTOML_TRAINING_TRACKING_PROTOCOL") != TRAINING_TRACKING_PROTOCOL
        or _fenced_attempt_context() is None
    ):
        raise StaleFence("Training metadata requires its declared tracking context")
    return True


@contextmanager
def metadata_context(active, run_id, role):
    with active as tracked:
        token = _active_metadata.set((run_id, role, tracked.info.run_id))
        try:
            yield tracked
        finally:
            _active_metadata.reset(token)


def _publish(run_id, role, tracking_id, kind, data):
    from automl_api.training.worker import _fenced_attempt_context

    try:
        context = _fenced_attempt_context()
        request = TrainingMetadataRequest(
            run_id=run_id, role=role, request_id=uuid.uuid4(), kind=kind, data=data
        )
        payload = request.model_dump(mode="json")
        control = AttemptRPC(
            context[0],
            base_url=os.environ["ATTEMPT_CONTROL_BASE_URL"],
            token_file=os.environ.get("AUTOML_IDENTITY_TOKEN_FILE", TOKEN_PATH),
            ca_file=os.environ.get("ATTEMPT_CONTROL_CA_FILE"),
        )
        try:
            response = control.request(
                "POST",
                "training/metadata",
                payload=payload,
                max_bytes=8192,
                timeout_seconds=60,
            )
        except AttemptControlUnavailable:
            from urllib.parse import quote

            response = control.request(
                "GET",
                f"training/metadata/{run_id}/receipts/{quote(role, safe='')}/{request.request_id}",
                max_bytes=8192,
                timeout_seconds=60,
            )
        reply = TrainingMetadataReply.model_validate(response)
        if (
            reply.attempt_id != context[0]
            or reply.run_id != run_id
            or reply.role != role
            or reply.project_id != uuid.UUID(os.environ["AUTOML_PROJECT_ID"])
            or reply.audience != attempt_audience(*context)
            or reply.request_id != request.request_id
            or reply.request_digest != canonical_request_hash(payload)
            or reply.tracking_run_id != tracking_id
        ):
            raise ValueError("Metadata acknowledgement belongs to another operation")
    except Exception:
        raise StaleFence("Training metadata has no verified acknowledgement") from None


def _write(kind, data):
    if not enabled():
        if kind == "tags":
            return mlflow.set_tags(data)
        if kind == "params":
            return mlflow.log_params(data)
        name, value = next(iter(data.items()))
        return mlflow.log_metric(name, value, step=0, synchronous=True)
    current = _active_metadata.get()
    if current is None:
        raise StaleFence("Training metadata has no active owned tracking role")
    if not data:
        return None
    if kind in {"tags", "params"}:
        data = {key: str(value) for key, value in data.items()}
    return _publish(*current, kind, data)


def log_tags(data):
    return _write("tags", data)


def log_params(data):
    return _write("params", data)


def log_metric(name, value):
    return _write("metrics", {name: float(value)})


def mirror(run_id, candidate, tracking_id, metric_count):
    if not enabled():
        return False
    _publish(
        run_id, f"candidate:{candidate}", tracking_id, "mirror", {"metric_count": metric_count}
    )
    return True
