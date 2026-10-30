"""Original native identity seeds for disposable observer fixtures only."""

import uuid

from automl_api.models.enums import OutboxStatus, WorkflowStage
from automl_api.models.workflows import OutboxEntry
from automl_api.services.evaluation_publication import _append
from automl_api.services.workflow_state import canonical_request_hash


def seed_ray_receipt(db, attempt, resource, command, namespace, uid="owned-fixture-ray-uid"):
    source = OutboxEntry(
        project_id=attempt.project_id,
        command_id=command.id,
        event_key=uuid.uuid4().hex,
        topic="ray.training.submit"
        if attempt.stage == WorkflowStage.TRAINING_RUN
        else "ray.dataset.split.submit"
        if attempt.stage == WorkflowStage.SPLITTER
        else "ray.dataset.prepare.submit",
        aggregate_type="model_run"
        if attempt.stage == WorkflowStage.TRAINING_RUN
        else "profiling_job",
        aggregate_id=resource.id,
        payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
        status=OutboxStatus.DELIVERED,
    )
    command.request_hash = canonical_request_hash(command.request_payload or {})
    db.add(source)
    db.flush()
    _append(
        db,
        attempt,
        "ray.submitted",
        dict(
            uid=uid,
            namespace=namespace,
            job_name=attempt.ray_job_name,
            outbox_id=str(source.id),
            request_sha256=canonical_request_hash(source.payload),
            manifest_sha256="a" * 64,
        ),
    )
    return source
