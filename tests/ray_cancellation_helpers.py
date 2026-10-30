"""Legacy component boundaries with owned fixture UIDs; full custody uses factory tests."""

import uuid
from unittest.mock import Mock

from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services import ray_cancellation, reconciler
from automl_api.services.deletion_runtime import _read
from automl_api.services.workflow_state import StaleFence, transition_attempt
from sqlalchemy import select


def _cancel(db, entry, k8s, *, training):
    assert isinstance(k8s, Mock) or isinstance(k8s.custom, Mock), "Owned mocks only"
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(str(entry.payload["attempt_id"])))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if training and attempt.stage != WorkflowStage.TRAINING_RUN:
        raise StaleFence("Training cancellation scope is stale.")
    if not training and attempt.stage not in {WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}:
        raise StaleFence("Dataset cancellation scope is stale.")
    resource = (
        db.get(ModelRun, attempt.model_run_id, populate_existing=True)
        if training
        else reconciler._profile_job_for_attempt(db, attempt)
    )
    # Pending fixture attempts never had native resources; the factory proves this from its journal.
    if training and not attempt.ray_job_name:
        resource = type(
            "FixtureRun",
            (),
            dict(
                id=attempt.model_run_id,
                project_id=attempt.project_id,
                k8s_namespace="unsubmitted-fixture",
            ),
        )()
    namespace = ray_cancellation.cancellation_scope(entry, attempt, resource)
    if attempt.ray_job_name:
        labels = {
            "automl.platform/project-id": str(attempt.project_id),
            "automl.platform/attempt-id": str(attempt.id),
            "automl.platform/generation": str(attempt.generation),
        }
        labels.update(
            {"automl.platform/run-id": str(resource.id)}
            if training
            else {
                "automl.platform/profile-job-id": str(resource.id),
                "automl.platform/workflow-stage": attempt.stage.value,
            }
        )
        target = dict(
            kind="RayJob",
            name=attempt.ray_job_name,
            namespace=namespace,
            uid=str(uuid.uuid5(uuid.NAMESPACE_URL, str(attempt.id)))
            if training
            else "physical-job-uid",
            labels=labels,
        )
        if _read(k8s, target) is not None:
            k8s.delete_ray_job(attempt.ray_job_name, namespace=namespace, uid=target["uid"])
    if not training:
        attempt.cleanup_state = "completed"
    if attempt.status not in ray_cancellation.TERMINAL:
        transition_attempt(
            attempt,
            AttemptStatus.CANCELLED,
            fencing_token=entry.payload["fencing_token"],
            terminal_reason="cancelled by durable command",
        )
    db.flush()
    return attempt


def cancel_training_ray_job(db, entry, k8s):
    return _cancel(db, entry, k8s, training=True)


def cancel_dataset_ray_job(db, entry, k8s):
    return _cancel(db, entry, k8s, training=False)
