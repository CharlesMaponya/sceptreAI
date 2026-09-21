"""User-requested, retryable removal of inactive project and run resources."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import HTTPException
from kubernetes.client.exceptions import ApiException
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from automl_api.db.base import Base
from automl_api.models.datasets import DatasetUploadSession, ProfilingJob
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowAttempt
from automl_api.services.projects import user_has_project_role
from automl_api.services.workflow_state import begin_command, enqueue_outbox, replay_dead_outbox
from automl_api.storage.object_store import get_object_store

TERMINAL_RUNS = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.PREEMPTED}


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
        if db.scalar(
            select(DatasetUploadSession.id)
            .where(
                DatasetUploadSession.project_id == project_id,
                DatasetUploadSession.status.in_(
                    ["initiated", "uploading", "object_completed", "verifying"]
                ),
            )
            .limit(1)
        ):
            raise HTTPException(
                status_code=409,
                detail="Finish or cancel active uploads before deleting this project.",
            )
        if db.scalar(
            select(DatasetUploadSession.id)
            .where(
                DatasetUploadSession.project_id == project_id,
                DatasetUploadSession.legal_hold.is_(True),
            )
            .limit(1)
        ):
            raise HTTPException(status_code=409, detail="This project contains data on legal hold.")
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
    tombstone = DeletionTombstone(
        project_id=project_id,
        requested_by_id=user.id,
        resource_type=kind,
        resource_id=resource_id,
        reason="Explicit user deletion",
        status="pending",
    )
    db.add(tombstone)
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


def reconcile_deletion(db: Session, entry: OutboxEntry, k8s) -> None:
    project_id = entry.project_id
    project = db.scalar(select(Project).where(Project.id == project_id).with_for_update())
    if project is None:
        return
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
    for run in runs:
        if run.status not in TERMINAL_RUNS:
            raise RuntimeError("An active run still needs to stop before deletion.")
        names = {
            (attempt.ray_job_name, "RayJob")
            for attempt in db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.project_id == project_id, WorkflowAttempt.model_run_id == run.id
                )
            ).all()
            if attempt.ray_job_name
        }
        if run.k8s_job_name and run.run_kind != RunKind.DEPLOYMENT:
            names.add(
                (run.k8s_job_name, "RayJob" if run.tags.get("orchestrator") == "kuberay" else "Job")
            )
        for name, kind in names:
            try:
                if kind == "RayJob":
                    k8s.delete_ray_job(name)
                    k8s.ray_job(name)
                    raise RuntimeError("Waiting for the training cluster to finish shutting down.")
                else:
                    k8s.delete_job(name)
                    if k8s.job_state(name) != "missing":
                        raise RuntimeError("Waiting for the worker to finish shutting down.")
            except ApiException as exc:
                if exc.status != 404:
                    raise
    store = get_object_store()
    if entry.topic == "project.delete":
        store.delete_prefix(f"projects/{project_id}")
        store.delete_prefix(f"automl/projects/{project_id}")
        # Explicit reverse FK order also handles restrictive composite project FKs.
        for table in reversed(Base.metadata.sorted_tables):
            if "project_id" in table.c:
                db.execute(delete(table).where(table.c.project_id == project_id))
        db.execute(delete(Project).where(Project.id == project_id))
    else:
        for run_id in run_ids:
            store.delete_prefix(f"projects/{project_id}/runs/{run_id}")
        db.execute(
            delete(ModelRegistryEntry).where(
                ModelRegistryEntry.project_id == project_id,
                ModelRegistryEntry.model_run_id.in_(run_ids),
            )
        )
        db.execute(
            delete(ModelRun).where(ModelRun.project_id == project_id, ModelRun.id.in_(run_ids))
        )
        tombstone = db.get(DeletionTombstone, uuid.UUID(entry.payload["tombstone_id"]))
        project.settings = {
            key: value for key, value in project.settings.items() if key != "deletion_in_progress"
        }
        tombstone.status = "complete"
        tombstone.completed_at = datetime.now(UTC)
    db.flush()
