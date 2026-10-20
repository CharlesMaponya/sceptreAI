from __future__ import annotations

import json
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import (
    CommandStatus,
    DatasetFormat,
    DatasetStatus,
    ProjectRole,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.schemas.profiling import (
    ColumnProfileRead,
    DatasetProfileRead,
    LeakageAnalysisRead,
    ProfileRequest,
)
from automl_api.schemas.profiling_jobs import ProfilingJobCreate
from automl_api.services.dataset_digests import require_verified_digest
from automl_api.services.leakage import LEAKAGE_SAMPLE_ROWS, detect_target_leakage
from automl_api.services.profiling import (
    _build_preparation_plan,
    _infer_task,
    _load_rows,
    _relationships_against_target,
    profile_dataset_version,
)
from automl_api.services.projects import require_project_role
from automl_api.services.provider_source import require_prepared_source
from automl_api.services.workflow_state import (
    StaleFence,
    begin_command,
    enqueue_outbox,
    transition_command,
)
from automl_api.storage.object_store import get_object_store

FEATURE_BATCH_SIZE = 5
PROFILE_ALGORITHM_VERSION = 4
TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}
MAX_CONTROL_PLANE_PROFILE_BYTES = 100 * 1024 * 1024


def create_profiling_job(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    payload: ProfilingJobCreate,
    *,
    auto_started: bool = False,
) -> tuple[ProfilingJob, bool]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    version = _get_version(db, project_id, dataset_id, dataset_version_id)
    distributed = (
        getattr(version, "source_kind", None) == "provider_distribution"
        or requires_distributed_profile(version)
        or bool(payload.time_column)
    )
    if distributed and version.format == DatasetFormat.EXCEL:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Distributed profiling does not accept Excel workbooks. "
                "Export the workbook as CSV, JSONL/NDJSON, or Parquet and upload it again."
            ),
        )
    target_column = payload.target_column.strip() if payload.target_column else None
    columns = _version_columns(version)
    if payload.time_column and (
        payload.time_column not in columns or payload.time_column == target_column
    ):
        raise HTTPException(
            status_code=422, detail="Choose a time column that exists and differs from the target."
        )
    if target_column and target_column not in columns:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Target column '{target_column}' was not found in the dataset.",
        )

    latest_job = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == project_id,
            ProfilingJob.dataset_version_id == dataset_version_id,
        )
        .order_by(ProfilingJob.created_at.desc())
    )
    if (
        not payload.force
        and latest_job is not None
        and latest_job.target_column == target_column
        and latest_job.overview_json.get("time_column") == payload.time_column
        and latest_job.status in {"queued", "running", "succeeded"}
        and latest_job.overview_json.get("profile_algorithm_version") == PROFILE_ALGORITHM_VERSION
    ):
        return latest_job, False

    active_jobs = list(
        db.scalars(
            select(ProfilingJob).where(
                ProfilingJob.project_id == project_id,
                ProfilingJob.dataset_version_id == dataset_version_id,
                ProfilingJob.status.in_(["queued", "running"]),
            )
        ).all()
    )
    for active_job in active_jobs:
        active_job.status = "cancelled"
        active_job.current_stage = "cancelled"
        active_job.finished_at = datetime.now(UTC)

    reusable_job = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == project_id,
            ProfilingJob.dataset_version_id == dataset_version_id,
            ProfilingJob.completed_columns > 0,
        )
        .order_by(
            ProfilingJob.completed_columns.desc(),
            ProfilingJob.updated_at.desc(),
        )
    )
    reusable_profiles = dict(reusable_job.feature_profiles_json) if reusable_job is not None else {}
    features_fully_reused = bool(
        reusable_job is not None
        and len(reusable_profiles) == len(columns)
        and reusable_job.completed_columns == len(columns)
    )
    task_inference_json = None
    if features_fully_reused:
        reusable_profile_models = {
            name: ColumnProfileRead.model_validate(profile)
            for name, profile in reusable_profiles.items()
        }
        task_inference_json = _infer_task(
            target_column,
            reusable_profile_models,
        ).model_dump(mode="json")

    overview_json: dict[str, Any] = {
        "row_count": reusable_job.row_count if reusable_job else version.row_count,
        "column_count": len(columns),
        "format": version.format.value,
        "filename": version.original_filename,
        "columns": version.schema_json.get("columns", []),
        "stages": {
            "overview": "completed",
            "features": "reused" if features_fully_reused else "queued",
            "relationships": "queued",
            "preparation": "queued",
        },
        "replaces_job_id": str(latest_job.id) if latest_job else None,
        "features_reused_from_job_id": (str(reusable_job.id) if reusable_job else None),
        "profile_algorithm_version": PROFILE_ALGORITHM_VERSION,
        "execution_mode": "kuberay" if distributed else "control_plane",
        "time_column": payload.time_column,
    }
    if task_inference_json is not None:
        overview_json["task_inference"] = task_inference_json

    artifact_uris: dict[str, Any] = {}
    if features_fully_reused and reusable_job is not None:
        features_uri = reusable_job.artifact_uris_json.get("features")
        if features_uri:
            artifact_uris["features"] = features_uri

    completed_columns = len(reusable_profiles)
    initial_progress = (
        0.7 if features_fully_reused else 0.1 + 0.6 * (completed_columns / max(1, len(columns)))
    )
    job = ProfilingJob(
        project_id=project_id,
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
        created_by_id=user.id,
        target_column=target_column,
        status="queued",
        current_stage="overview",
        progress=initial_progress,
        total_columns=len(columns),
        completed_columns=completed_columns,
        row_count=reusable_job.row_count if reusable_job else version.row_count,
        overview_json=overview_json,
        feature_profiles_json=reusable_profiles,
        artifact_uris_json=artifact_uris,
        auto_started=auto_started,
    )
    db.add(job)
    db.flush()
    if distributed:
        _enqueue_distributed_profile(db, user, version, job)
    else:
        _enqueue_local_profile(db, job)
    return job, True


def requires_distributed_profile(version: DatasetVersion) -> bool:
    return bool(
        get_settings().distributed_preparation_enabled
        or (version.byte_size is not None and version.byte_size > MAX_CONTROL_PLANE_PROFILE_BYTES)
    )


def is_distributed_profile(job: ProfilingJob) -> bool:
    return job.overview_json.get("execution_mode") == "kuberay"


def _enqueue_distributed_profile(
    db: Session,
    user: User,
    version: DatasetVersion,
    job: ProfilingJob,
) -> WorkflowAttempt:
    source = require_prepared_source(db, version)
    stage = WorkflowStage.PREPARATION if source is not None else WorkflowStage.SPLITTER
    if source is not None:
        job.overview_json = {
            **job.overview_json,
            "prepared_artifact_id": str(source.prepared_artifact_id),
            "split_revision_id": str(source.split_revision_id),
        }
    request = {
        "profiling_job_id": str(job.id),
        "dataset_version_id": str(version.id),
        "target_column": job.target_column,
        "profile_algorithm_version": PROFILE_ALGORITHM_VERSION,
    }
    command, replayed = begin_command(
        db,
        project_id=job.project_id,
        actor_id=user.id,
        operation="dataset.profile",
        idempotency_key=f"profiling-job:{job.id}",
        payload=request,
    )
    if replayed:
        existing = db.scalar(
            select(WorkflowAttempt).where(
                WorkflowAttempt.project_id == job.project_id,
                WorkflowAttempt.logical_key == f"profile:{job.id}:{stage.value}",
                WorkflowAttempt.generation == 1,
            )
        )
        if existing is None:
            raise RuntimeError("The distributed profiling command has no splitter attempt.")
        return existing

    attempt = WorkflowAttempt(
        project_id=job.project_id,
        stage=stage,
        logical_key=f"profile:{job.id}:{stage.value}",
        dataset_version_id=version.id,
        workload_identity=f"project-{job.project_id}-{stage.value}",
        generation=1,
        fencing_token=uuid.uuid4().hex,
    )
    db.add(attempt)
    db.flush()
    enqueue_outbox(
        db,
        command,
        topic="ray.dataset.prepare.submit" if source is not None else "ray.dataset.split.submit",
        aggregate_type="profiling_job",
        aggregate_id=job.id,
        payload={
            "profiling_job_id": str(job.id),
            "attempt_id": str(attempt.id),
            "fencing_token": attempt.fencing_token,
        },
    )
    command.resource_type = "profiling_job"
    command.resource_id = job.id
    command.response_status = status.HTTP_202_ACCEPTED
    command.response_payload = {
        "profiling_job_id": str(job.id),
        "attempt_id": str(attempt.id),
        "stage": stage.value,
    }
    transition_command(command, CommandStatus.RUNNING)
    job.overview_json = {
        **job.overview_json,
        "workflow_attempt_id": str(attempt.id),
        "workflow_generation": attempt.generation,
    }
    db.flush()
    return attempt


def get_profiling_job(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    job_id: uuid.UUID,
) -> ProfilingJob:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    job = db.scalar(
        select(ProfilingJob).where(
            ProfilingJob.project_id == project_id,
            ProfilingJob.id == job_id,
        )
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Profiling job not found.",
        )
    return job


def latest_profiling_job(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
) -> ProfilingJob | None:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == project_id,
            ProfilingJob.dataset_version_id == dataset_version_id,
        )
        .order_by(ProfilingJob.created_at.desc())
    )


@dataclass(frozen=True)
class ProfileDelivery:
    entry_id: uuid.UUID
    worker_id: str
    generation: int
    available_at: datetime
    artifact_id: uuid.UUID = field(default_factory=uuid.uuid4)


def _enqueue_local_profile(db: Session, job: ProfilingJob) -> bool:
    command, replayed = begin_command(
        db,
        project_id=job.project_id,
        actor_id=job.created_by_id,
        operation="dataset.profile.local",
        idempotency_key=f"profiling-job:{job.id}",
        payload={"profiling_job_id": str(job.id)},
    )
    if replayed:
        return False
    command.resource_type = "profiling_job"
    command.resource_id = job.id
    transition_command(command, CommandStatus.RUNNING)
    enqueue_outbox(
        db,
        command,
        topic="dataset.profile.local",
        aggregate_type="profiling_job",
        aggregate_id=job.id,
        payload={"profiling_job_id": str(job.id)},
    )
    return True


def resume_incomplete_profiling_jobs() -> int:
    """Backfill legacy local jobs without changing live ownership or retry budgets."""
    with get_session_factory()() as db:
        pending = list(
            db.scalars(
                select(ProfilingJob)
                .where(
                    ProfilingJob.status.in_(["queued", "running"]),
                    func.coalesce(ProfilingJob.overview_json["execution_mode"].astext, "")
                    != "kuberay",
                    ~select(WorkflowCommand.id)
                    .where(
                        WorkflowCommand.project_id == ProfilingJob.project_id,
                        WorkflowCommand.operation == "dataset.profile.local",
                        WorkflowCommand.resource_id == ProfilingJob.id,
                    )
                    .exists(),
                )
                .order_by(ProfilingJob.created_at, ProfilingJob.id)
                .limit(100)
                .with_for_update(skip_locked=True)
            )
        )
        count = sum(_enqueue_local_profile(db, job) for job in pending)
        db.commit()
        return count


def _locked_profile_job(db: Session, job_id: uuid.UUID, delivery: ProfileDelivery):
    from automl_api.services.reconciler import _lock_outbox_delivery

    entry = _lock_outbox_delivery(db, delivery.entry_id, delivery.worker_id)
    if (
        entry is None
        or entry.delivery_attempts != delivery.generation
        or entry.available_at != delivery.available_at
        or entry.topic != "dataset.profile.local"
        or entry.payload.get("profiling_job_id") != str(job_id)
    ):
        raise StaleFence("The local profiling delivery is no longer current.")
    job = db.get(ProfilingJob, job_id, with_for_update=True, populate_existing=True)
    if job is None or job.status in {"succeeded", "cancelled"}:
        return None
    if job.project_id != entry.project_id or is_distributed_profile(job):
        raise StaleFence("The profiling job does not belong to this local delivery.")
    now = datetime.now(UTC)
    entry.lease_expires_at = now + timedelta(seconds=90)
    job.heartbeat_at = now
    return job


@contextmanager
def _profile_heartbeat(job_id: uuid.UUID, delivery: ProfileDelivery):
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(15):
            try:
                with get_session_factory()() as db:
                    if _locked_profile_job(db, job_id, delivery) is None:
                        return
                    db.commit()
            except Exception:
                # Checkpoint publication independently verifies the current database lease.
                return

    thread = threading.Thread(target=heartbeat, name="local-profile-heartbeat", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1)


def run_local_profile(db: Session, entry: OutboxEntry, worker_id: str) -> None:
    job_id = uuid.UUID(str(entry.payload["profiling_job_id"]))
    delivery = ProfileDelivery(entry.id, worker_id, entry.delivery_attempts, entry.available_at)
    db.commit()
    try:
        with _profile_heartbeat(job_id, delivery):
            _run_profiling_job(job_id, delivery)
    finally:
        # Keep the exact delivery locked until the dispatcher records its outcome.
        _locked_profile_job(db, job_id, delivery)


def _run_profiling_job(job_id: uuid.UUID, delivery: ProfileDelivery) -> None:
    session_factory = get_session_factory()
    try:
        with session_factory() as db:
            job = _locked_profile_job(db, job_id, delivery)
            if job is None or job.status in {"succeeded", "cancelled"}:
                return
            features_fully_reused = bool(
                job.total_columns
                and job.completed_columns == job.total_columns
                and len(job.feature_profiles_json) == job.total_columns
            )
            job.status = "running"
            job.finished_at = None
            job.failure_message = None
            job.started_at = job.started_at or datetime.now(UTC)
            job.heartbeat_at = datetime.now(UTC)
            if features_fully_reused:
                job.current_stage = "relationships"
                job.progress = 0.75
                _set_stage(job, "features", "reused")
                _set_stage(job, "relationships", "running")
            else:
                job.current_stage = "features"
                _set_stage(job, "features", "running")
            version = _get_version(
                db,
                job.project_id,
                job.dataset_id,
                job.dataset_version_id,
            )
            version.status = DatasetStatus.PROFILING
            partitioned = _supports_partitioned_stages(version)
            db.commit()

        if partitioned:
            _run_partitioned_stages(job_id, delivery)
        else:
            _run_monolithic_fallback(job_id, delivery)
    except StaleFence:
        raise
    except Exception as exc:
        with session_factory() as db:
            job = _locked_profile_job(db, job_id, delivery)
            if job is not None:
                if job.status == "cancelled":
                    return
                entry = db.get(OutboxEntry, delivery.entry_id)
                command = db.get(WorkflowCommand, entry.command_id)
                exhausted = entry.delivery_attempts >= command.max_retries
                job.status = "failed" if exhausted else "queued"
                job.failure_message = str(exc)
                job.finished_at = datetime.now(UTC) if exhausted else None
                job.heartbeat_at = datetime.now(UTC)
                _set_stage(job, job.current_stage, "failed")
                version = db.get(DatasetVersion, job.dataset_version_id)
                if version is not None:
                    # Profiling produces a derived artifact. A failed profile must not
                    # invalidate an upload that was already verified successfully.
                    version.status = DatasetStatus.READY
                db.commit()
        raise


def _run_partitioned_stages(job_id: uuid.UUID, delivery: ProfileDelivery) -> None:
    from automl_api.services.ray_polars_profiling import (
        _load_dataset,
        _profile_column,
        _relationships,
        _sample_rows,
    )

    session_factory = get_session_factory()
    with session_factory() as db:
        job = db.get(ProfilingJob, job_id)
        if job is None:
            return
        version = db.get(DatasetVersion, job.dataset_version_id)
        if version is None:
            raise ValueError("The profiling job's dataset version was not found.")
        require_verified_digest(version)
        previous_count = job.row_count
        existing_profiles = dict(job.feature_profiles_json)
        target_column = job.target_column
    dataset = _load_dataset(version)
    row_count = previous_count or dataset.count()
    columns = [str(column) for column in dataset.schema().names]
    with session_factory() as db:
        job = _locked_profile_job(db, job_id, delivery)
        if job is None:
            return
        job.row_count = row_count
        job.total_columns = len(columns)
        db.commit()

    pending_columns = [column for column in columns if column not in existing_profiles]
    for batch_start in range(0, len(pending_columns), FEATURE_BATCH_SIZE):
        with session_factory() as db:
            current_status = db.scalar(select(ProfilingJob.status).where(ProfilingJob.id == job_id))
            if current_status == "cancelled":
                return
        batch = pending_columns[batch_start : batch_start + FEATURE_BATCH_SIZE]
        batch_profiles = [_profile_column(dataset, column, row_count) for column in batch]
        with session_factory() as db:
            job = _locked_profile_job(db, job_id, delivery)
            if job is None:
                return
            merged_profiles = dict(job.feature_profiles_json)
            merged_profiles.update(
                {profile.name: profile.model_dump(mode="json") for profile in batch_profiles}
            )
            job.feature_profiles_json = merged_profiles
            job.completed_columns = len(merged_profiles)
            job.progress = 0.1 + 0.6 * (job.completed_columns / max(1, job.total_columns))
            job.heartbeat_at = datetime.now(UTC)
            job.artifact_uris_json = _store_stage_artifact(
                job,
                "features",
                merged_profiles,
                delivery,
            )
            db.commit()

    with session_factory() as db:
        job = _locked_profile_job(db, job_id, delivery)
        if job is None or job.status == "cancelled":
            return
        if "features" not in job.artifact_uris_json:
            job.artifact_uris_json = _store_stage_artifact(
                job,
                "features",
                job.feature_profiles_json,
                delivery,
            )
        profiles = {
            name: ColumnProfileRead.model_validate(profile)
            for name, profile in job.feature_profiles_json.items()
        }
        if job.overview_json.get("stages", {}).get("features") != "reused":
            _set_stage(job, "features", "completed")
        _set_stage(job, "relationships", "running")
        job.current_stage = "relationships"
        job.progress = 0.75
        task_inference = _infer_task(job.target_column, profiles)
        job.overview_json = {
            **job.overview_json,
            "task_inference": task_inference.model_dump(mode="json"),
        }
        db.commit()

    relationships, relationship_warnings = _relationships(
        dataset,
        profiles,
        target_column,
    )
    leakage_analysis = detect_target_leakage(
        pd.DataFrame(_sample_rows(dataset, LEAKAGE_SAMPLE_ROWS)),
        target_column,
    )
    with session_factory() as db:
        job = _locked_profile_job(db, job_id, delivery)
        if job is None or job.status == "cancelled":
            return
        relationship_payload = [
            relationship.model_dump(mode="json") for relationship in relationships
        ]
        job.relationships_json = relationship_payload
        job.warnings_json = list(
            dict.fromkeys([*job.warnings_json, *relationship_warnings, *leakage_analysis.warnings])
        )
        job.overview_json = {
            **job.overview_json,
            "leakage_analysis": leakage_analysis.model_dump(mode="json"),
        }
        job.artifact_uris_json = _store_stage_artifact(
            job,
            "relationships",
            relationship_payload,
            delivery,
        )
        _set_stage(job, "relationships", "completed")
        _set_stage(job, "preparation", "running")
        job.current_stage = "preparation"
        job.progress = 0.9
        db.commit()

    _finish_preparation(job_id, delivery)


def _finish_preparation(job_id: uuid.UUID, delivery: ProfileDelivery) -> None:
    session_factory = get_session_factory()
    with session_factory() as db:
        job = _locked_profile_job(db, job_id, delivery)
        if job is None or job.status == "cancelled":
            return
        version = db.get(DatasetVersion, job.dataset_version_id)
        if version is None:
            raise ValueError("The profiling job's dataset version was not found.")
        profiles = [
            ColumnProfileRead.model_validate(profile)
            for profile in job.feature_profiles_json.values()
        ]
        profile_by_name = {profile.name: profile for profile in profiles}
        task_inference = _infer_task(job.target_column, profile_by_name)
        leakage_analysis = LeakageAnalysisRead.model_validate(
            job.overview_json.get("leakage_analysis", {"status": "not_applicable"})
        )
        preparation = _build_preparation_plan(
            profiles,
            job.target_column,
            task_inference.task_type,
            leakage_analysis,
        )
        preparation_payload = [step.model_dump(mode="json") for step in preparation]
        job.preparation_json = preparation_payload
        job.overview_json = {
            **job.overview_json,
            "row_count": job.row_count,
            "column_count": len(profiles),
            "task_inference": task_inference.model_dump(mode="json"),
        }
        job.artifact_uris_json = _store_stage_artifact(
            job,
            "preparation",
            preparation_payload,
            delivery,
        )
        complete_profile = DatasetProfileRead(
            project_id=job.project_id,
            dataset_id=job.dataset_id,
            dataset_version_id=job.dataset_version_id,
            row_count_analyzed=job.row_count or 0,
            column_count=len(profiles),
            target_column=job.target_column,
            task_inference=task_inference,
            columns=profiles,
            relationships=job.relationships_json,
            preparation_plan=preparation,
            leakage_analysis=leakage_analysis,
            warnings=job.warnings_json,
        )
        final_uris = _store_stage_artifact(
            job,
            "complete",
            complete_profile.model_dump(mode="json"),
            delivery,
        )
        job.artifact_uris_json = final_uris
        job.status = "succeeded"
        job.current_stage = "complete"
        job.progress = 1.0
        job.finished_at = datetime.now(UTC)
        job.heartbeat_at = datetime.now(UTC)
        _set_stage(job, "preparation", "completed")
        version.profile_artifact_uri = final_uris["complete"]
        version.status = DatasetStatus.READY
        db.commit()


def _run_monolithic_fallback(job_id: uuid.UUID, delivery: ProfileDelivery) -> None:
    session_factory = get_session_factory()
    with session_factory() as db:
        job = db.get(ProfilingJob, job_id)
        if job is None:
            return
        user = db.get(User, job.created_by_id)
        version = db.get(DatasetVersion, job.dataset_version_id)
        if user is None or version is None:
            raise ValueError("The profiling job's lineage was not found.")
        require_project_role(db, user, job.project_id, ProjectRole.VIEWER)
    features_fully_reused = bool(
        job.total_columns
        and job.completed_columns == job.total_columns
        and len(job.feature_profiles_json) == job.total_columns
    )
    if features_fully_reused:
        profiles = [
            ColumnProfileRead.model_validate(column)
            for column in job.feature_profiles_json.values()
        ]
        profile_by_name = {column.name: column for column in profiles}
        rows, warnings = _load_rows(version)
        task_inference = _infer_task(job.target_column, profile_by_name)
        leakage_analysis = detect_target_leakage(
            pd.DataFrame(rows),
            job.target_column,
        )
        relationships = _relationships_against_target(
            rows,
            profile_by_name,
            job.target_column,
        )
        preparation = _build_preparation_plan(
            profiles,
            job.target_column,
            task_inference.task_type,
            leakage_analysis,
        )
        profile = DatasetProfileRead(
            project_id=job.project_id,
            dataset_id=job.dataset_id,
            dataset_version_id=job.dataset_version_id,
            row_count_analyzed=job.row_count or len(rows),
            column_count=len(profiles),
            target_column=job.target_column,
            task_inference=task_inference,
            columns=profiles,
            relationships=relationships,
            preparation_plan=preparation,
            leakage_analysis=leakage_analysis,
            warnings=list(dict.fromkeys([*warnings, *leakage_analysis.warnings])),
        )
    else:
        profile = profile_dataset_version(version, ProfileRequest(target_column=job.target_column))
    with session_factory() as db:
        job = _locked_profile_job(db, job_id, delivery)
        if job is None:
            return
        version = db.get(DatasetVersion, job.dataset_version_id)
        job.row_count = profile.row_count_analyzed
        job.total_columns = profile.column_count
        job.completed_columns = profile.column_count
        job.feature_profiles_json = {
            column.name: column.model_dump(mode="json") for column in profile.columns
        }
        job.relationships_json = [
            relationship.model_dump(mode="json") for relationship in profile.relationships
        ]
        job.preparation_json = [step.model_dump(mode="json") for step in profile.preparation_plan]
        job.warnings_json = profile.warnings
        job.overview_json = {
            **job.overview_json,
            "row_count": profile.row_count_analyzed,
            "column_count": profile.column_count,
            "task_inference": profile.task_inference.model_dump(mode="json"),
            "leakage_analysis": profile.leakage_analysis.model_dump(mode="json"),
        }
        for stage, payload in (
            ("features", job.feature_profiles_json),
            ("relationships", job.relationships_json),
            ("preparation", job.preparation_json),
            ("complete", profile.model_dump(mode="json")),
        ):
            if stage != "features" or "features" not in job.artifact_uris_json:
                job.artifact_uris_json = _store_stage_artifact(job, stage, payload, delivery)
            if stage != "complete":
                if (
                    stage != "features"
                    or job.overview_json.get("stages", {}).get("features") != "reused"
                ):
                    _set_stage(job, stage, "completed")
        job.status = "succeeded"
        job.current_stage = "complete"
        job.progress = 1.0
        job.finished_at = datetime.now(UTC)
        job.heartbeat_at = datetime.now(UTC)
        version.profile_artifact_uri = job.artifact_uris_json["complete"]
        version.status = DatasetStatus.READY
        db.commit()


def _store_stage_artifact(
    job: ProfilingJob,
    stage: str,
    payload: Any,
    delivery: ProfileDelivery,
) -> dict[str, Any]:
    key = (
        f"projects/{job.project_id}/datasets/{job.dataset_id}/"
        f"versions/{job.dataset_version_id}/profiles/{job.id}/"
        f"deliveries/{delivery.entry_id}/{delivery.artifact_id}/{stage}.json"
    )
    content = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
    stored = get_object_store().put_bytes(key, content)
    return {**job.artifact_uris_json, stage: stored.uri}


def _set_stage(job: ProfilingJob, stage: str, stage_status: str) -> None:
    overview = dict(job.overview_json)
    stages = dict(overview.get("stages", {}))
    stages[stage] = stage_status
    overview["stages"] = stages
    job.overview_json = overview


def _version_columns(version: DatasetVersion) -> list[str]:
    return [
        str(column["name"])
        for column in version.schema_json.get("columns", [])
        if column.get("name")
    ]


def _supports_partitioned_stages(version: DatasetVersion) -> bool:
    if version.format == DatasetFormat.CSV:
        return True
    if version.format == DatasetFormat.JSON:
        filename = (version.original_filename or "").lower()
        return filename.endswith((".jsonl", ".ndjson"))
    return False


def _get_version(
    db: Session,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
) -> DatasetVersion:
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == project_id,
            DatasetVersion.dataset_id == dataset_id,
            DatasetVersion.id == dataset_version_id,
        )
    )
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset version not found.",
        )
    require_prepared_source(db, version)
    return version
