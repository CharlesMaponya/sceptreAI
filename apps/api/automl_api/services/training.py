from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from kubernetes.client import ApiException
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import ProjectRole, RunKind, RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.schemas.training import (
    EstimatorRead,
    ModelRunRead,
    TrainingAddModelsRequest,
    TrainingEstimateRead,
    TrainingEstimateRequest,
    TrainingLaunchRead,
    TrainingLaunchRequest,
    TrainingLeaderboardRead,
    TrainingLogsRead,
    TrainingResourceUsageRead,
)
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.model_evidence import build_model_pipeline
from automl_api.services.projects import require_project_role
from automl_api.storage.object_store import get_object_store
from automl_api.training.evaluation import (
    metric_direction,
    resolve_primary_metric,
)
from automl_api.training.model_catalog import (
    candidate_catalog,
    estimator_catalog_payload,
    select_candidates,
    supported_gpu_vendors,
)

BATCH_RUN_KINDS = (
    RunKind.TRAINING,
    RunKind.VALIDATION,
    RunKind.EXPLAINABILITY,
    RunKind.DRIFT,
)
_IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG = "image_pull_backoff_first_seen_at"
_IMAGE_PULL_BACKOFF_GRACE = timedelta(minutes=2)


def estimate_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: TrainingEstimateRequest,
    client: KubernetesTrainingClient | None = None,
) -> TrainingEstimateRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    version = _get_dataset_version(db, project_id, payload.dataset_version_id)
    _validate_target(version, payload.target_column)
    try:
        resolve_primary_metric(payload.task_type, payload.primary_metric)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    _validate_evaluation_column(version, payload)
    _validate_candidate_models(payload)
    selected_candidate_count = (
        len(payload.candidate_models) if payload.candidate_models else payload.candidate_limit
    )
    selected_names = set(payload.candidate_models)
    selected_specs = [
        candidate
        for candidate in candidate_catalog(payload.task_type)
        if (candidate.name in selected_names if selected_names else candidate.default_selected)
    ][:selected_candidate_count]
    compatible_gpu_vendors = {
        vendor
        for candidate in selected_specs
        for vendor in supported_gpu_vendors(candidate.name)
    }
    cost_weights = {"low": 1.0, "medium": 1.25, "high": 1.75}
    model_cost_factor = (
        sum(cost_weights[candidate.cost_tier] for candidate in selected_specs) / len(selected_specs)
        if selected_specs
        else 1.0
    )
    k8s = client or KubernetesTrainingClient()
    estimate = k8s.estimate(
        dataset_bytes=version.byte_size or 0,
        dataset_rows=version.row_count or 0,
        column_count=version.column_count or 0,
        expected_minutes=payload.expected_minutes,
        prefer_gpu=payload.prefer_gpu,
        task_type=payload.task_type,
        candidate_limit=selected_candidate_count,
        optimization_iterations=payload.optimization_iterations,
        model_cost_factor=model_cost_factor,
        gpu_compatible_vendors=compatible_gpu_vendors,
    )
    try:
        object_exists = get_object_store().exists(version.object_uri)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    if not object_exists:
        estimate.blockers = [
            *estimate.blockers,
            "The dataset object is missing from MinIO. Re-upload or restore this "
            "dataset version before training.",
        ]
        estimate.can_launch = False
    leakage_profile, leakage_analysis = _latest_leakage_analysis(
        db,
        version.id,
        payload.target_column,
    )
    excluded_columns = list(leakage_analysis.get("excluded_columns") or [])
    if excluded_columns:
        estimate.warnings = [
            *estimate.warnings,
            "Profiling will exclude target-leakage features before training: "
            + ", ".join(excluded_columns)
            + ".",
        ]
    elif payload.target_column and leakage_profile is None:
        estimate.warnings = [
            *estimate.warnings,
            "No completed leakage profile matches this target. Training will run the "
            "same high-confidence leakage check before fitting as a safeguard.",
        ]
    active_statuses = [
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
    ]
    _reconcile_active_runs(db, k8s, active_statuses)
    active_db_runs = int(
        db.scalar(
            select(func.count(ModelRun.id)).where(
                ModelRun.status.in_(active_statuses),
                ModelRun.run_kind.in_(BATCH_RUN_KINDS),
            )
        )
        or 0
    )
    if active_db_runs >= estimate.max_concurrent_jobs:
        blocker = (
            f"Database concurrency limit reached ({active_db_runs}/{estimate.max_concurrent_jobs})."
        )
        estimate.blockers = [*estimate.blockers, blocker]
        estimate.can_launch = False
    active_project_runs = int(
        db.scalar(
            select(func.count(ModelRun.id)).where(
                ModelRun.project_id == project_id,
                ModelRun.status.in_(active_statuses),
                ModelRun.run_kind.in_(BATCH_RUN_KINDS),
            )
        )
        or 0
    )
    if active_project_runs >= 1:
        estimate.blockers = [
            *estimate.blockers,
            "This project already has an active training run. "
            "Wait for it to finish so other projects can share the cluster.",
        ]
        estimate.can_launch = False
    return estimate


def launch_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: TrainingLaunchRequest,
    client: KubernetesTrainingClient | None = None,
) -> TrainingLaunchRead:
    _lock_training_admission(db)
    k8s = client or KubernetesTrainingClient()
    estimate = estimate_training_run(db, user, project_id, payload, k8s)
    if not estimate.can_launch:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Training precheck failed.",
                "blockers": estimate.blockers,
                "warnings": estimate.warnings,
            },
        )

    now = datetime.now(UTC)
    selected_candidates = select_candidates(
        payload.task_type,
        payload.candidate_models or None,
        len(payload.candidate_models) if payload.candidate_models else payload.candidate_limit,
    )
    primary_metric = resolve_primary_metric(
        payload.task_type,
        payload.primary_metric,
    )
    leakage_profile, leakage_analysis = _latest_leakage_analysis(
        db,
        payload.dataset_version_id,
        payload.target_column,
    )
    excluded_leakage_columns = list(leakage_analysis.get("excluded_columns") or [])
    pending_leaderboard = [
        {
            "rank": None,
            "model": candidate.name,
            "status": "pending",
            "cost_tier": candidate.cost_tier,
            "primary_score": None,
            "metrics": {},
            "diagnostics": {},
            "best_params": {},
            "duration_seconds": None,
            "error": None,
            "mlflow_run_id": None,
        }
        for candidate in selected_candidates
    ]
    run = ModelRun(
        project_id=project_id,
        dataset_version_id=payload.dataset_version_id,
        created_by_id=user.id,
        run_kind=RunKind.TRAINING,
        status=RunStatus.PRECHECK_RUNNING,
        task_type=payload.task_type,
        target_column=payload.target_column,
        run_name=payload.run_name,
        pipeline_name="tabular_automl_v1",
        k8s_namespace=k8s.settings.training_namespace,
        gpu_requested=estimate.gpu_requested,
        cpu_request_cores=estimate.cpu_request_cores,
        memory_request_mb=estimate.memory_request_mb,
        cpu_limit_cores=estimate.cpu_limit_cores,
        memory_limit_mb=estimate.memory_limit_mb,
        estimated_core_hours=estimate.estimated_core_hours,
        params={
            **payload.params,
            "expected_minutes": payload.expected_minutes,
            "prefer_gpu": payload.prefer_gpu,
            "candidate_limit": (
                len(payload.candidate_models)
                if payload.candidate_models
                else payload.candidate_limit
            ),
            "candidate_models": payload.candidate_models,
            "optimization_iterations": payload.optimization_iterations,
            "cv_folds": payload.cv_folds,
            "evaluation_column": payload.evaluation_column,
            "primary_metric": primary_metric,
            "excluded_leakage_columns": excluded_leakage_columns,
            "leakage_profile_job_id": str(leakage_profile.id) if leakage_profile else None,
            "gpu_vendor": estimate.gpu_vendor,
            "gpu_resource": estimate.gpu_resource,
            "selected_node": estimate.selected_node,
        },
        tags={
            "project_id": str(project_id),
            "orchestrator": "kubernetes",
            "accelerator": estimate.gpu_vendor or "cpu",
            "accelerator_resource": estimate.gpu_resource,
            "selected_node": estimate.selected_node,
            "leaderboard_primary_metric": primary_metric,
            "leaderboard": pending_leaderboard,
            "completed_candidates": 0,
        },
        queued_at=now,
    )
    db.add(run)
    db.flush()
    manifest = k8s.build_job_manifest(
        run_id=run.id,
        project_id=project_id,
        estimate=estimate,
    )
    run.k8s_job_name = manifest["metadata"]["name"]
    try:
        k8s.create_job(manifest)
    except Exception as exc:
        run.status = RunStatus.FAILED
        run.failure_code = "KUBERNETES_JOB_CREATE_FAILED"
        run.failure_message = str(exc)
        run.plain_english_failure = (
            "The cluster rejected the training job before it started. "
            "Check namespace permissions, the training image, and resource availability."
        )
        run.finished_at = datetime.now(UTC)
        db.flush()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=run.plain_english_failure,
        ) from exc
    run.status = RunStatus.QUEUED
    db.flush()
    return TrainingLaunchRead(
        run=ModelRunRead.model_validate(run),
        estimate=estimate,
        manifest=manifest,
    )


def _lock_training_admission(db: Session) -> None:
    if db.bind is not None and db.bind.dialect.name == "postgresql":
        db.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": 7_301_247_011},
        )


def list_training_runs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
) -> list[ModelRun]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind == RunKind.TRAINING,
            )
            .order_by(ModelRun.created_at.desc())
        ).all()
    )


def get_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    *,
    sync: bool = True,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.TRAINING,
        )
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Training run not found.")
    if sync and run.status in {RunStatus.QUEUED, RunStatus.RUNNING} and run.k8s_job_name:
        _sync_run_status(db, run, client or KubernetesTrainingClient())
    return run


def cancel_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    run = get_training_run(db, user, project_id, run_id, sync=False)
    db.refresh(run, with_for_update=True)
    if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.PREEMPTED}:
        return run
    now = datetime.now(UTC)
    if run.status == RunStatus.CANCELLED:
        run.tags = _cancelled_training_tags(run.tags, run.finished_at or now)
        run.finished_at = run.finished_at or now
        db.flush()
        return run
    if run.k8s_job_name:
        try:
            (client or KubernetesTrainingClient()).delete_job(run.k8s_job_name)
        except ApiException as exc:
            if exc.status != 404:
                raise
    run.status = RunStatus.CANCELLED
    run.tags = _cancelled_training_tags(run.tags, now)
    run.finished_at = now
    db.flush()
    return run


def _cancelled_training_tags(
    source: dict | None,
    cancelled_at: datetime,
) -> dict:
    tags = dict(source or {})
    leaderboard: list[dict] = []
    interrupted_candidate = tags.get("cancelled_candidate") or tags.get("current_candidate")
    for source_entry in tags.get("leaderboard", []):
        entry = dict(source_entry)
        if entry.get("status") == "running":
            interrupted_candidate = interrupted_candidate or entry.get("model")
            entry = {
                **entry,
                "status": "cancelled",
                "rank": None,
                "primary_score": None,
                "error": (
                    entry.get("error")
                    or "Training was cancelled before this candidate completed."
                ),
            }
        leaderboard.append(entry)
    timestamp = cancelled_at.isoformat()
    return {
        **tags,
        "leaderboard": leaderboard,
        "cancelled_candidate": interrupted_candidate,
        "current_candidate": None,
        "candidate_phase": "cancelled",
        "candidate_phase_updated_at": timestamp,
        "cancelled_at": timestamp,
    }


def restart_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> TrainingLaunchRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    source = get_training_run(db, user, project_id, run_id, sync=False)
    if source.status not in {
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.PREEMPTED,
    }:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only failed, cancelled, or preempted training runs can be restarted.",
        )

    version = _get_dataset_version(db, project_id, source.dataset_version_id)
    if not get_object_store().exists(version.object_uri):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "The dataset object is missing from MinIO. Re-upload or restore "
                "this dataset version before restarting."
            ),
        )

    source_params = dict(source.params or {})
    payload = TrainingLaunchRequest(
        dataset_version_id=source.dataset_version_id,
        target_column=source.target_column,
        evaluation_column=source_params.get("evaluation_column"),
        task_type=source.task_type,
        primary_metric=source_params.get("primary_metric"),
        prefer_gpu=bool(source_params.get("prefer_gpu", source.gpu_requested)),
        expected_minutes=int(source_params.get("expected_minutes", 10)),
        candidate_limit=int(source_params.get("candidate_limit", 5)),
        candidate_models=list(source_params.get("candidate_models") or []),
        optimization_iterations=int(source_params.get("optimization_iterations", 5)),
        cv_folds=int(source_params.get("cv_folds", 3)),
        run_name=f"{source.run_name or source.id} restart"[:255],
        params=source_params,
    )
    result = launch_training_run(db, user, project_id, payload, client)
    restarted = db.get(ModelRun, result.run.id)
    assert restarted is not None
    restarted.tags = {
        **restarted.tags,
        "restarted_from_run_id": str(source.id),
    }
    source.tags = {
        **source.tags,
        "restarted_by_run_id": str(restarted.id),
    }
    db.flush()
    return result.model_copy(update={"run": ModelRunRead.model_validate(restarted)})


def add_models_to_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    request: TrainingAddModelsRequest,
    client: KubernetesTrainingClient | None = None,
) -> TrainingLaunchRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    selected_run = get_training_run(db, user, project_id, run_id, sync=False)
    if selected_run.status != RunStatus.SUCCEEDED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Models can only be added after the selected training run succeeds.",
        )
    parent = _leaderboard_parent(db, selected_run)
    if parent.status != RunStatus.SUCCEEDED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The original training run must be complete before adding models.",
        )

    requested_models = list(dict.fromkeys(request.candidate_models))
    if len(requested_models) != len(request.candidate_models):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Each added model must be selected only once.",
        )
    available_models = {candidate.name for candidate in candidate_catalog(parent.task_type)}
    unknown_models = [name for name in requested_models if name not in available_models]
    if unknown_models:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported estimators: {', '.join(unknown_models)}.",
        )
    completed_models = {
        entry["model"]
        for entry in parent.tags.get("leaderboard", [])
        if entry.get("status") == "succeeded"
    }
    already_completed = [name for name in requested_models if name in completed_models]
    if already_completed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"These models already completed successfully: {', '.join(already_completed)}."
            ),
        )

    source_params = dict(parent.params or {})
    payload = TrainingLaunchRequest(
        dataset_version_id=parent.dataset_version_id,
        target_column=parent.target_column,
        evaluation_column=source_params.get("evaluation_column"),
        task_type=parent.task_type,
        primary_metric=source_params.get("primary_metric"),
        prefer_gpu=request.prefer_gpu,
        expected_minutes=request.expected_minutes,
        candidate_limit=len(requested_models),
        candidate_models=requested_models,
        optimization_iterations=request.optimization_iterations,
        cv_folds=request.cv_folds,
        run_name=(f"{parent.run_name or parent.id} add {', '.join(requested_models)}")[:255],
        params=source_params,
    )
    result = launch_training_run(db, user, project_id, payload, client)
    extension = db.get(ModelRun, result.run.id)
    assert extension is not None
    extension.tags = {
        **extension.tags,
        "leaderboard_parent_run_id": str(parent.id),
        "incremental_models": requested_models,
    }
    extension_run_ids = list(parent.tags.get("extension_run_ids", []))
    extension_run_ids.append(str(extension.id))
    parent.tags = {
        **parent.tags,
        "extension_run_ids": extension_run_ids,
    }
    db.flush()
    return result.model_copy(update={"run": ModelRunRead.model_validate(extension)})


def training_logs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> TrainingLogsRead:
    run = get_training_run(db, user, project_id, run_id, client=client)
    lines = []
    if run.k8s_job_name:
        try:
            lines = (client or KubernetesTrainingClient()).job_logs(run.id)
        except ApiException as exc:
            if exc.status not in {400, 404}:
                raise
    return TrainingLogsRead(run_id=run.id, status=run.status, lines=lines)


def training_resources(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> TrainingResourceUsageRead:
    run = get_training_run(db, user, project_id, run_id, client=client)
    try:
        snapshot = (client or KubernetesTrainingClient()).training_resource_usage(run.id)
    except ApiException as exc:
        if exc.status not in {400, 404, 503}:
            raise
        snapshot = {"telemetry_available": False, "status_reason": str(exc)}

    # A resource request can outlive a concurrent cancellation while it waits on
    # Kubernetes. Re-lock and refresh before merging telemetry so stale tags can
    # never restore an interrupted candidate or phase.
    db.flush()
    db.refresh(run, with_for_update=True)
    tags = dict(run.tags or {})
    params = dict(run.params or {})
    previous = dict(tags.get("resource_usage") or {})
    cpu_usage = snapshot.get("cpu_usage_cores")
    memory_usage = snapshot.get("memory_usage_mb")
    peak_cpu = max(float(previous.get("peak_cpu_usage_cores") or 0), float(cpu_usage or 0))
    peak_memory = max(int(previous.get("peak_memory_usage_mb") or 0), int(memory_usage or 0))
    stored = {
        **previous,
        **{key: value for key, value in snapshot.items() if value is not None},
        "peak_cpu_usage_cores": peak_cpu or None,
        "peak_memory_usage_mb": peak_memory or None,
        "sampled_at": datetime.now(UTC).isoformat(),
    }
    tags["resource_usage"] = stored
    run.tags = tags
    db.flush()

    total = max(
        1,
        int(params.get("candidate_limit") or len(params.get("candidate_models") or []) or 1),
    )
    completed = min(total, int(tags.get("completed_candidates") or 0))
    terminal = run.status in {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.PREEMPTED,
    }
    progress = 1.0 if run.status == RunStatus.SUCCEEDED else completed / total
    started = run.started_at or run.queued_at or run.created_at
    ended = run.finished_at if terminal and run.finished_at else datetime.now(UTC)
    elapsed = max(0.0, (ended - started).total_seconds())
    remaining = None
    if completed and not terminal:
        remaining = max(0.0, elapsed / completed * (total - completed))
    gpu_vendor = params.get("gpu_vendor") or tags.get("gpu_vendor")
    gpu_resource = params.get("gpu_resource") or tags.get("gpu_resource")
    return TrainingResourceUsageRead(
        run_id=run.id,
        status=run.status,
        pod_name=snapshot.get("pod_name") or stored.get("pod_name"),
        pod_phase=snapshot.get("pod_phase") or stored.get("pod_phase"),
        node_name=(
            snapshot.get("node_name")
            or stored.get("node_name")
            or params.get("selected_node")
        ),
        current_candidate=tags.get("current_candidate"),
        last_candidate=tags.get("cancelled_candidate"),
        current_phase=(
            "complete"
            if run.status == RunStatus.SUCCEEDED
            else run.status.value
            if terminal
            else tags.get("candidate_phase")
        ),
        completed_candidates=completed,
        total_candidates=total,
        progress=progress,
        elapsed_seconds=elapsed,
        estimated_remaining_seconds=remaining,
        cpu_request_cores=run.cpu_request_cores,
        cpu_limit_cores=run.cpu_limit_cores,
        cpu_usage_cores=cpu_usage if cpu_usage is not None else stored.get("cpu_usage_cores"),
        peak_cpu_usage_cores=peak_cpu or None,
        memory_request_mb=run.memory_request_mb,
        memory_limit_mb=run.memory_limit_mb,
        memory_usage_mb=memory_usage if memory_usage is not None else stored.get("memory_usage_mb"),
        peak_memory_usage_mb=peak_memory or None,
        gpu_requested=run.gpu_requested,
        gpu_vendor=str(gpu_vendor) if gpu_vendor else None,
        gpu_resource=str(gpu_resource) if gpu_resource else None,
        gpu_count=1 if run.gpu_requested and gpu_resource else 0,
        telemetry_available=bool(snapshot.get("telemetry_available")),
        restart_count=int(snapshot.get("restart_count") or stored.get("restart_count") or 0),
        status_reason=snapshot.get("status_reason") or stored.get("status_reason"),
        sampled_at=datetime.now(UTC),
    )


def training_leaderboard(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
) -> TrainingLeaderboardRead:
    run = get_training_run(db, user, project_id, run_id)
    leaderboard_run = _leaderboard_parent(db, run)
    by_model = {
        entry["model"]: dict(entry)
        for entry in leaderboard_run.tags.get("leaderboard", [])
    }
    if leaderboard_run.id != run.id:
        by_model.update(
            {
                entry["model"]: dict(entry)
                for entry in run.tags.get("leaderboard", [])
            }
        )
    requested = run.params.get("candidate_models")
    selected = select_candidates(
        run.task_type,
        requested if isinstance(requested, list) and requested else None,
        int(run.params.get("candidate_limit", 5)),
    )
    for candidate in selected:
        by_model.setdefault(
            candidate.name,
            {
                "rank": None,
                "model": candidate.name,
                "status": (
                    "running"
                    if run.tags.get("current_candidate") == candidate.name
                    else "pending"
                ),
                "cost_tier": candidate.cost_tier,
                "primary_score": None,
                "metrics": {},
                "diagnostics": {},
                "best_params": {},
                "duration_seconds": None,
                "error": None,
                "mlflow_run_id": None,
            },
        )
    primary_metric = (
        leaderboard_run.tags.get("leaderboard_primary_metric")
        or run.tags.get("leaderboard_primary_metric")
    )
    entries = list(by_model.values())
    if run.status == RunStatus.CANCELLED:
        entries = _cancelled_training_tags(
            {
                "leaderboard": entries,
                "cancelled_candidate": run.tags.get("cancelled_candidate"),
                "current_candidate": run.tags.get("current_candidate"),
            },
            run.finished_at or datetime.now(UTC),
        )["leaderboard"]
    if primary_metric:
        entries = _rank_combined_leaderboard(entries, primary_metric)
    active_candidate = (
        run.tags.get("current_candidate")
        or leaderboard_run.tags.get("current_candidate")
    )
    active_phase = (
        run.tags.get("candidate_phase")
        or leaderboard_run.tags.get("candidate_phase")
    )
    excluded_columns = list(
        leaderboard_run.params.get("excluded_leakage_columns")
        or run.params.get("excluded_leakage_columns")
        or []
    )
    for entry in entries:
        entry["pipeline"] = build_model_pipeline(
            str(entry["model"]),
            leaderboard_run.task_type,
            str(entry.get("status", "pending")),
            parameters=dict(entry.get("best_params") or {}),
            excluded_columns=excluded_columns,
            current_phase=(
                str(active_phase)
                if entry.get("model") == active_candidate and active_phase
                else None
            ),
        )
    successful = [entry for entry in entries if entry.get("status") == "succeeded"]
    metric_names = {name for entry in entries for name in entry.get("metrics", {})}
    return TrainingLeaderboardRead(
        run_id=run.id,
        status=run.status,
        primary_metric=primary_metric,
        winner=successful[0]["model"] if successful else None,
        metric_directions={name: metric_direction(name) for name in sorted(metric_names)},
        entries=entries,
    )


def _rank_combined_leaderboard(
    entries: list[dict],
    primary_metric: str,
) -> list[dict]:
    successful = [
        entry
        for entry in entries
        if entry.get("status") == "succeeded" and primary_metric in entry.get("metrics", {})
    ]
    unranked = [
        entry
        for entry in entries
        if entry.get("status") == "succeeded" and primary_metric not in entry.get("metrics", {})
    ]
    remaining = [entry for entry in entries if entry.get("status") != "succeeded"]
    successful.sort(
        key=lambda entry: float(entry.get("metrics", {}).get(primary_metric, float("-inf"))),
        reverse=metric_direction(primary_metric) == "maximize",
    )
    for rank, entry in enumerate(successful, start=1):
        entry["rank"] = rank
        entry["primary_score"] = entry.get("metrics", {}).get(primary_metric)
    for entry in remaining:
        entry["rank"] = None
        entry["primary_score"] = None
    for entry in unranked:
        entry["rank"] = None
        entry["primary_score"] = None
    return [*successful, *unranked, *remaining]


def _leaderboard_parent(db: Session, run: ModelRun) -> ModelRun:
    parent_id = run.tags.get("leaderboard_parent_run_id")
    if not parent_id:
        return run
    try:
        parent_uuid = uuid.UUID(str(parent_id))
    except ValueError:
        return run
    parent = db.get(ModelRun, parent_uuid)
    if parent is None or parent.project_id != run.project_id or parent.run_kind != RunKind.TRAINING:
        return run
    return parent


def list_training_estimators(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    task_type: TaskType,
) -> list[EstimatorRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    if task_type == TaskType.UNSPECIFIED:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A concrete task type is required.",
        )
    return [EstimatorRead.model_validate(item) for item in estimator_catalog_payload(task_type)]


def _sync_run_status(
    db: Session,
    run: ModelRun,
    client: KubernetesTrainingClient,
) -> None:
    state = client.job_state(run.k8s_job_name or "")
    db.refresh(run, with_for_update=True)
    if run.status not in {
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
    }:
        return
    now = datetime.now(UTC)
    if state in {"image_pull_backoff", "terminal_waiting_failure"}:
        # Recheck destructive failure decisions after acquiring the run lock. A
        # registry can recover while this request waits behind another update.
        state = client.job_state(run.k8s_job_name or "")
    tags = dict(run.tags or {})
    if state == "image_pull_backoff":
        first_seen = _timestamp_from_tag(tags.get(_IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG))
        if first_seen is None:
            tags[_IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = now.isoformat()
            run.tags = tags
            db.flush()
            return
        if now - first_seen < _IMAGE_PULL_BACKOFF_GRACE:
            return
        state = "terminal_waiting_failure"
    elif _IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG in tags:
        tags.pop(_IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG, None)
        run.tags = tags
    if state == "running":
        run.status = RunStatus.RUNNING
        run.started_at = run.started_at or now
    elif state == "succeeded":
        run.status = RunStatus.SUCCEEDED
        run.started_at = run.started_at or run.queued_at
        run.finished_at = now
    elif state in {"failed", "missing", "terminal_waiting_failure"}:
        if state in {"failed", "terminal_waiting_failure"}:
            failure_code, failure_message = client.job_failure_details(run.k8s_job_name or "")
        else:
            failure_code = "KUBERNETES_JOB_MISSING"
            failure_message = "The Kubernetes Job no longer exists."
        if state == "terminal_waiting_failure":
            try:
                client.delete_job(run.k8s_job_name or "")
            except ApiException as exc:
                if exc.status != 404:
                    raise
        run.status = RunStatus.FAILED
        run.failure_code = failure_code
        run.failure_message = failure_message
        if failure_code == "POD_OOM_KILLED":
            run.plain_english_failure = (
                "Training exceeded its adaptive memory limit. Reduce the model "
                "budget or search iterations, or make more node memory available."
            )
        elif failure_code == "JOB_DEADLINE_EXCEEDED":
            run.plain_english_failure = (
                "Training reached the Kubernetes runtime safety deadline. "
                "Restart it with a longer expected duration or reduce the "
                "candidate and optimization budget."
            )
        elif failure_code == "POD_EVICTED":
            run.plain_english_failure = (
                "Kubernetes evicted this low-priority training pod because the "
                "shared node needed its resources."
            )
        elif failure_code == "TRAINING_IMAGE_NOT_PRESENT":
            run.plain_english_failure = (
                "The training image is not available inside this Kubernetes cluster. "
                "Import it into every local-cluster node, or configure a pullable "
                "registry image, then start a new run."
            )
        elif failure_code == "TRAINING_IMAGE_PULL_FAILED":
            run.plain_english_failure = (
                "Kubernetes could not download the training image. Check the image "
                "name and tag, registry access, and pull credentials; local clusters "
                "can instead import the image into every node."
            )
        elif failure_code == "TRAINING_IMAGE_INVALID":
            run.plain_english_failure = (
                "The configured training image name is invalid. Correct its registry, "
                "repository, and tag before starting a new run."
            )
        elif failure_code == "TRAINING_CONTAINER_CONFIG_INVALID":
            run.plain_english_failure = (
                "Kubernetes could not assemble the training container. Check the "
                "required Secrets, ConfigMaps, environment values, and volume mounts."
            )
        elif failure_code == "TRAINING_CONTAINER_START_FAILED":
            run.plain_english_failure = (
                "Kubernetes could not start the training container. Check its pod "
                "events, image, and runtime configuration before starting a new run."
            )
        else:
            run.plain_english_failure = (
                "The training container failed or disappeared. Review the pod "
                "details and logs shown for this run."
            )
        run.finished_at = now
    db.flush()


def _timestamp_from_tag(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        observed_at = datetime.fromisoformat(value)
    except ValueError:
        return None
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    return observed_at.astimezone(UTC)


def _reconcile_active_runs(
    db: Session,
    client: KubernetesTrainingClient,
    active_statuses: list[RunStatus],
) -> None:
    active_runs = db.scalars(
        select(ModelRun).where(
            ModelRun.status.in_(active_statuses),
            ModelRun.run_kind.in_(BATCH_RUN_KINDS),
        )
    ).all()
    for run in active_runs:
        if not run.k8s_job_name:
            continue
        try:
            _sync_run_status(db, run, client)
        except ApiException:
            # Capacity checks report Kubernetes connectivity separately. Keep
            # the database state unchanged when reconciliation is unavailable.
            return


def _get_dataset_version(
    db: Session,
    project_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
) -> DatasetVersion:
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == project_id,
            DatasetVersion.id == dataset_version_id,
        )
    )
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset version not found.",
        )
    return version


def _latest_leakage_analysis(
    db: Session,
    dataset_version_id: uuid.UUID,
    target_column: str | None,
) -> tuple[ProfilingJob | None, dict]:
    if not target_column:
        return None, {}
    profile = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.dataset_version_id == dataset_version_id,
            ProfilingJob.target_column == target_column,
            ProfilingJob.status == "succeeded",
        )
        .order_by(ProfilingJob.created_at.desc())
    )
    if profile is None:
        return None, {}
    analysis = profile.overview_json.get("leakage_analysis")
    return profile, dict(analysis) if isinstance(analysis, dict) else {}


def _validate_target(version: DatasetVersion, target_column: str | None) -> None:
    if target_column is None:
        return
    columns = {column.get("name") for column in version.schema_json.get("columns", [])}
    if target_column not in columns:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Target column '{target_column}' was not found in the dataset.",
        )


def _validate_evaluation_column(
    version: DatasetVersion,
    payload: TrainingEstimateRequest,
) -> None:
    if payload.evaluation_column is None:
        return
    if payload.task_type != TaskType.CLUSTERING:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="An evaluation column is only supported for clustering.",
        )
    columns = {column.get("name") for column in version.schema_json.get("columns", [])}
    if payload.evaluation_column not in columns:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Evaluation column '{payload.evaluation_column}' was not found in the dataset."
            ),
        )


def _validate_candidate_models(payload: TrainingEstimateRequest) -> None:
    if not payload.candidate_models:
        return
    available = {item["name"] for item in estimator_catalog_payload(payload.task_type)}
    unknown = [name for name in payload.candidate_models if name not in available]
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported estimators: {', '.join(unknown)}.",
        )
