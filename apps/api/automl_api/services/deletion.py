"""User-requested, retryable removal of inactive project and run resources."""

from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import Session

from automl_api.db.base import Base
from automl_api.models.datasets import (
    DatasetProviderImport,
    DatasetUploadSession,
    DatasetVersion,
    ProfilingJob,
)
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    ProjectRole,
    RunKind,
    RunStatus,
    ScopeStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    PromotionalScope,
    PromotionalScopeMember,
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowCommand,
)
from automl_api.services.attempt_object_grants import (
    has_unretired_attempt_credentials,
    revoke_attempt_grants,
)
from automl_api.services.projects import user_has_project_role
from automl_api.services.workflow_state import begin_command, enqueue_outbox, replay_dead_outbox
from automl_api.storage.contracts import DeletionPending
from automl_api.storage.object_store import get_object_store as get_object_store

STAGES = {
    "project": ("runtime", "object_store", "prepared_store", "metadata"),
    "run": ("runtime", "object_store", "metadata"),
}


TERMINAL_RUNS = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.PREEMPTED}


def _project_uploads_retained(db: Session, project_id: uuid.UUID) -> bool:
    db.flush()
    for model in (DatasetUploadSession, DatasetVersion):
        if (
            db.scalar(
                select(model.id)
                .where(
                    model.project_id == project_id,
                    model.legal_hold.is_(True) | (model.retention_until > datetime.now(UTC)),
                )
                .limit(1)
            )
            is not None
        ):
            return True
    return False


def _project_uploads_unsettled(db, project_id):
    from automl_api.services.upload_deletion import OPERATIONS, require_retired

    db.flush()
    if (
        db.scalar(
            select(DatasetUploadSession.id)
            .where(
                DatasetUploadSession.project_id == project_id,
                DatasetUploadSession.status.in_(
                    {"initiated", "uploading", "object_completed", "verifying", "retiring"}
                ),
            )
            .limit(1)
        )
        is not None
    ):
        return True
    missing_upload = (
        ~select(DatasetUploadSession.id)
        .where(
            DatasetUploadSession.project_id == project_id,
            DatasetUploadSession.id == WorkflowCommand.resource_id,
        )
        .correlate(WorkflowCommand)
        .exists()
    )
    if (
        db.scalar(
            select(WorkflowCommand.id)
            .where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.operation.in_(OPERATIONS),
                WorkflowCommand.resource_id.is_not(None),
                missing_upload,
            )
            .limit(1)
        )
        is not None
    ):
        return True
    candidates = db.scalars(
        select(DatasetUploadSession)
        .where(DatasetUploadSession.project_id == project_id)
        .execution_options(populate_existing=True)
    )
    for upload in candidates:
        try:
            require_retired(db, upload, now=datetime.now(UTC))
        except (KeyError, TypeError, ValueError, AttributeError):
            return True
    return False


def deployment_retirements(db, project_id, run_ids=None):
    from automl_api.services.deployment_submission import lifecycle_custody
    from automl_api.services.workflow_state import StaleFence

    registry_ids = (
        None
        if run_ids is None
        else set(
            map(
                str,
                db.scalars(
                    select(ModelRegistryEntry.id).where(
                        ModelRegistryEntry.project_id == project_id,
                        ModelRegistryEntry.model_run_id.in_(run_ids),
                    )
                ),
            )
        )
    )
    result = []
    for run in db.scalars(
        select(ModelRun).where(
            ModelRun.project_id == project_id, ModelRun.run_kind == RunKind.DEPLOYMENT
        )
    ):
        if registry_ids is not None and run.tags.get("registry_entry_id") not in registry_ids:
            continue
        try:
            custody = lifecycle_custody(db, run, allow_unstarted=True)
        except StaleFence as exc:
            raise HTTPException(
                409, "Original deployment retirement requires controlled qualification."
            ) from exc
        if custody.get("retired") is not True:
            raise HTTPException(409, "Complete verified deployment retirement before deletion.")
        result.append(
            dict(
                run_id=str(run.id),
                custody={key: value for key, value in custody.items() if key != "manifests"},
            )
        )
    return sorted(result, key=lambda value: value["run_id"])


def _require_stopped_workflows(db, project_id, run_ids=None):
    from automl_api.services.deployment_objects import has_unretired_deployment_writes
    from automl_api.services.legacy_analysis import has_unretired_cleanup

    deployment_retirements(db, project_id, run_ids)

    if has_unretired_deployment_writes(db, project_id, run_ids):
        raise HTTPException(
            409, "Wait for verified deployment object write retirement before deletion."
        )
    if has_unretired_cleanup(db, project_id, run_ids):
        raise HTTPException(409, "Wait for verified legacy Job cleanup before deletion.")
    if run_ids is None:
        from automl_api.models.workflows import WorkflowEvent

        unfinished = db.scalar(
            select(WorkflowAttempt.id)
            .join(
                WorkflowEvent,
                WorkflowEvent.attempt_id == WorkflowAttempt.id,
            )
            .where(
                WorkflowAttempt.project_id == project_id,
                WorkflowEvent.event_key == "provider_import.submission",
                WorkflowAttempt.cleanup_state != "complete",
            )
            .limit(1)
        )
        if unfinished is not None:
            raise HTTPException(
                409, "Wait for provider verification resource cleanup before deletion."
            )
    scopes = select(PromotionalScope.id).where(PromotionalScope.project_id == project_id)
    attempts = select(WorkflowAttempt.id).where(
        WorkflowAttempt.project_id == project_id,
        WorkflowAttempt.status.in_(
            {
                AttemptStatus.PENDING,
                AttemptStatus.CLAIMED,
                AttemptStatus.SUBMITTED,
                AttemptStatus.RUNNING,
            }
        ),
    )
    if run_ids is not None:
        membership = select(PromotionalScopeMember.scope_id).where(
            PromotionalScopeMember.project_id == project_id,
            PromotionalScopeMember.model_run_id.in_(run_ids),
        )
        scopes = scopes.where(PromotionalScope.id.in_(membership))
        attempts = attempts.where(
            or_(WorkflowAttempt.model_run_id.in_(run_ids), WorkflowAttempt.scope_id.in_(membership))
        )
    scopes = scopes.where(
        PromotionalScope.status.not_in(
            {ScopeStatus.SUCCEEDED, ScopeStatus.FAILED, ScopeStatus.CANCELLED}
        )
    )
    if db.scalar(scopes.limit(1)) or db.scalar(attempts.limit(1)):
        raise HTTPException(
            status_code=409,
            detail="Cancel active scopes and wait for workflows to stop before deletion.",
        )


def request_deletion(
    db: Session, user: User, project_id: uuid.UUID, run_id: uuid.UUID | None = None
) -> dict:
    if not user_has_project_role(
        db, user, project_id, ProjectRole.OWNER if run_id is None else ProjectRole.ADMIN
    ):
        raise HTTPException(
            status_code=403, detail="You do not have access to delete this resource."
        )
    project = db.scalar(select(Project).where(Project.id == project_id).with_for_update())
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found.")
    kind = "project" if run_id is None else "run"
    resource_id = run_id or project_id
    existing = db.scalar(
        select(DeletionTombstone).where(
            DeletionTombstone.project_id == project_id,
            DeletionTombstone.resource_type == kind,
            DeletionTombstone.resource_id == resource_id,
        )
    )
    if existing:
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == project_id,
                OutboxEntry.topic == f"{kind}.delete",
                OutboxEntry.aggregate_id == resource_id,
            )
        )
        if entry is not None and entry.status == OutboxStatus.DEAD:
            replay_dead_outbox(db, entry.id, actor_id=user.id)
            project.settings = {
                key: value for key, value in project.settings.items() if key != "deletion_error"
            }
        return {"status": existing.status, "id": str(existing.id)}
    if project.settings.get("deletion_in_progress"):
        raise HTTPException(status_code=409, detail="Another deletion is already in progress.")
    runs = list(
        db.scalars(
            select(ModelRun).where(ModelRun.project_id == project_id).with_for_update()
        ).all()
    )
    selected = runs if run_id is None else [run for run in runs if run.id == run_id]
    if run_id and not selected:
        raise HTTPException(status_code=404, detail="Run not found.")
    if run_id:
        if selected[0].run_kind != RunKind.TRAINING:
            raise HTTPException(status_code=409, detail="Select a training run for deletion.")
        # Analyses and additional candidate runs belong to the training evidence being removed.
        ids = {str(run_id)}
        while True:
            dependents = {
                str(run.id)
                for run in runs
                if any(
                    str(run.tags.get(key, "")) in ids
                    for key in ("source_training_run_id", "leaderboard_parent_run_id")
                )
            }
            if dependents <= ids:
                break
            ids |= dependents
        selected = [run for run in runs if str(run.id) in ids]
        entries = db.scalars(
            select(ModelRegistryEntry.id).where(
                ModelRegistryEntry.project_id == project_id,
                ModelRegistryEntry.model_run_id.in_([run.id for run in selected]),
            )
        ).all()
        entry_ids = {str(value) for value in entries}
        if any(
            run.run_kind == RunKind.DEPLOYMENT
            and str(run.tags.get("registry_entry_id")) in entry_ids
            and not run.tags.get("runtime_cleaned_at")
            for run in runs
        ):
            raise HTTPException(
                status_code=409,
                detail="Shut down and clean up deployments of this model before deleting its run.",
            )
    if any(run.status not in TERMINAL_RUNS for run in selected):
        raise HTTPException(
            status_code=409, detail="Cancel active runs and wait for them to stop before deletion."
        )
    if any(
        run.run_kind == RunKind.DEPLOYMENT and not run.tags.get("runtime_cleaned_at")
        for run in selected
    ):
        raise HTTPException(
            status_code=409,
            detail="Shut down and clean up all deployments before deleting this project.",
        )
    if run_id is None:
        if db.scalar(
            select(DatasetProviderImport.id)
            .join(
                WorkflowCommand,
                (WorkflowCommand.id == DatasetProviderImport.command_id)
                & (WorkflowCommand.project_id == DatasetProviderImport.project_id),
            )
            .where(
                DatasetProviderImport.project_id == project_id,
                WorkflowCommand.status.in_([CommandStatus.PENDING, CommandStatus.RUNNING]),
            )
            .limit(1)
        ):
            raise HTTPException(
                409, detail="Finish or cancel provider import verification before deletion."
            )
        if db.scalar(
            select(ProfilingJob.id)
            .where(
                ProfilingJob.project_id == project_id,
                ProfilingJob.status.not_in(["succeeded", "failed", "cancelled"]),
            )
            .limit(1)
        ):
            raise HTTPException(
                status_code=409, detail="Wait for dataset preparation to finish before deletion."
            )
        if _project_uploads_unsettled(db, project_id):
            raise HTTPException(
                status_code=409,
                detail="Finish active uploads and pending upload cleanup first.",
            )
        if _project_uploads_retained(db, project_id):
            raise HTTPException(
                status_code=409,
                detail="Upload retention or legal hold prevents project deletion.",
            )
    # Never race an already submitted command that can still write to this resource.
    pending = select(OutboxEntry.id).where(
        OutboxEntry.project_id == project_id,
        OutboxEntry.status.in_([OutboxStatus.PENDING, OutboxStatus.CLAIMED]),
    )
    if run_id:
        pending = pending.where(OutboxEntry.aggregate_id.in_([run.id for run in selected]))
    if db.scalar(pending.limit(1)):
        raise HTTPException(
            status_code=409, detail="Wait for pending project operations to settle before deletion."
        )
    _require_stopped_workflows(db, project_id, [run.id for run in selected] if run_id else None)
    tombstone = DeletionTombstone(
        project_id=project_id,
        requested_by_id=user.id,
        resource_type=kind,
        resource_id=resource_id,
        reason="Explicit user deletion",
        status="pending",
    )
    db.add(tombstone)
    db.flush()
    db.add_all(
        [
            DeletionStage(project_id=project_id, tombstone_id=tombstone.id, resource_class=stage)
            for stage in STAGES[kind]
        ]
    )
    if run_id is None:
        project.settings = {**project.settings, "deletion_requested": True}
    project.settings = {**project.settings, "deletion_in_progress": True}
    for run in selected:
        run.tags = {**run.tags, "deletion_requested": True}
    command, _ = begin_command(
        db,
        project_id=project_id,
        actor_id=user.id,
        operation=f"{kind}.delete",
        idempotency_key=str(resource_id),
        payload={"id": str(resource_id)},
    )
    command.status = CommandStatus.RUNNING
    command.max_retries = 30
    enqueue_outbox(
        db,
        command,
        topic=f"{kind}.delete",
        aggregate_type=kind,
        aggregate_id=resource_id,
        payload={"run_ids": [str(run.id) for run in selected], "tombstone_id": str(tombstone.id)},
    )
    db.flush()
    return {"status": "pending", "id": str(tombstone.id)}


def deletion_context(db: Session, entry: OutboxEntry):
    """Validate current deletion policy without constructing a native client."""
    from automl_api.services.deployment_objects import has_unretired_deployment_writes

    project_id = entry.project_id
    project = db.scalar(select(Project).where(Project.id == project_id).with_for_update())
    if project is None:
        return None
    run_ids = [uuid.UUID(value) for value in entry.payload["run_ids"]]
    all_runs = list(
        db.scalars(
            select(ModelRun).where(ModelRun.project_id == project_id).with_for_update()
        ).all()
    )
    runs = (
        all_runs
        if entry.topic == "project.delete"
        else [run for run in all_runs if run.id in run_ids]
    )
    if entry.topic == "project.delete" and any(
        run.run_kind == RunKind.DEPLOYMENT and not run.tags.get("runtime_cleaned_at")
        for run in runs
    ):
        raise RuntimeError("Deployments must be shut down and cleaned up before deletion.")
    if any(run.status not in TERMINAL_RUNS for run in runs):
        raise RuntimeError("An active run still needs to stop before deletion.")
    if entry.topic == "project.delete" and _project_uploads_unsettled(db, project_id):
        raise DeletionPending("Active uploads or pending upload cleanup prevent project deletion.")
    if has_unretired_deployment_writes(
        db, project_id, run_ids if entry.topic == "run.delete" else None
    ):
        raise DeletionPending("Deployment native object writes require verified retirement.")
    _require_stopped_workflows(db, project_id, run_ids if entry.topic == "run.delete" else None)
    tombstone = db.get(DeletionTombstone, uuid.UUID(entry.payload["tombstone_id"]))
    if tombstone is None or tombstone.project_id != project_id:
        raise LookupError("The project-scoped deletion tombstone is missing.")
    if tombstone.legal_hold:
        raise DeletionPending("A legal hold prevents deletion.")
    if entry.topic == "project.delete" and _project_uploads_retained(db, project_id):
        raise DeletionPending("Upload retention or legal hold prevents project deletion.")
    _retire_deletion_grants(db, project_id, run_ids if entry.topic == "run.delete" else None)
    names = STAGES[tombstone.resource_type]
    stages = {
        stage.resource_class: stage
        for stage in db.scalars(
            select(DeletionStage).where(DeletionStage.tombstone_id == tombstone.id)
        )
    }
    missing = [name for name in names if name not in stages]
    if missing:
        # Old queued requests must commit their plan before the first side effect.
        db.add_all(
            [
                DeletionStage(project_id=project_id, tombstone_id=tombstone.id, resource_class=name)
                for name in missing
            ]
        )
        db.flush()
        return project, runs, run_ids, tombstone, stages, None, False
    stage = next((stages[name] for name in names if stages[name].status != "completed"), None)
    return project, runs, run_ids, tombstone, stages, stage, stage is None


def reconcile_deletion(db: Session, entry: OutboxEntry, k8s) -> bool:
    """Legacy callers may validate/backfill plans; native work requires factory ownership."""
    context = deletion_context(db, entry)
    if context is None:
        return True
    if context[-2] is None and not context[-1]:
        return False
    raise RuntimeError("Deletion requires the factory-owned dispatcher.")


def _retire_deletion_grants(db, project_id, run_ids):
    affected = select(WorkflowAttempt.id).where(WorkflowAttempt.project_id == project_id)
    if run_ids is not None:
        scopes = select(PromotionalScopeMember.scope_id).where(
            PromotionalScopeMember.project_id == project_id,
            PromotionalScopeMember.model_run_id.in_(run_ids),
        )
        affected = affected.where(
            or_(WorkflowAttempt.model_run_id.in_(run_ids), WorkflowAttempt.scope_id.in_(scopes))
        )
    attempt_ids = list(db.scalars(affected.order_by(WorkflowAttempt.id).with_for_update()))
    for attempt_id in attempt_ids:
        revoke_attempt_grants(db, attempt_id)
    now = datetime.now(UTC)
    deadline = db.scalar(
        select(func.max(WorkflowAttemptCredentialGrant.expires_at)).where(
            WorkflowAttemptCredentialGrant.attempt_id.in_(attempt_ids),
            WorkflowAttemptCredentialGrant.issuance_started_at.is_not(None),
            WorkflowAttemptCredentialGrant.expires_at > now,
        )
    )
    if deadline is not None:
        pending = DeletionPending("Waiting for attempt object credentials to expire.")
        pending.retry_after_seconds = max(1, math.ceil((deadline - now).total_seconds()))
        raise pending
    if any(has_unretired_attempt_credentials(db, attempt_id) for attempt_id in attempt_ids):
        raise DeletionPending(
            "Legacy champion credentials or unknown native writes require verified retirement."
        )


def _delete_metadata(db, entry, project, run_ids):
    project_id = project.id
    if entry.topic == "project.delete":
        # Explicit reverse FK order also handles restrictive composite project FKs.
        for table in reversed(Base.metadata.sorted_tables):
            if "project_id" in table.c and table not in {
                DeletionTombstone.__table__,
                DeletionStage.__table__,
            }:
                db.execute(delete(table).where(table.c.project_id == project_id))
        db.execute(delete(Project).where(Project.id == project_id))
    else:
        db.execute(
            delete(ModelRegistryEntry).where(
                ModelRegistryEntry.project_id == project_id,
                ModelRegistryEntry.model_run_id.in_(run_ids),
            )
        )
        attempts = select(WorkflowAttempt.id).where(
            WorkflowAttempt.project_id == project_id, WorkflowAttempt.model_run_id.in_(run_ids)
        )
        db.execute(
            delete(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.project_id == project_id,
                WorkflowAttemptCredentialGrant.attempt_id.in_(attempts),
            )
        )
        db.execute(
            delete(ModelRun).where(ModelRun.project_id == project_id, ModelRun.id.in_(run_ids))
        )
        project.settings = {
            key: value for key, value in project.settings.items() if key != "deletion_in_progress"
        }
    db.flush()
