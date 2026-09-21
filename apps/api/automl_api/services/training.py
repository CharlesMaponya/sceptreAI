from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from kubernetes.client import ApiException
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.core.run_names import versioned_run_name
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    ProjectRole,
    RunKind,
    RunStatus,
    TaskType,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    CapacityReservation,
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    FeatureSearchSpaceRevision,
    PromotionalScope,
    PromotionalScopeMember,
    TrainingCandidate,
    WorkflowAttempt,
    WorkflowCommand,
)
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
from automl_api.services.run_admission import (
    AdmissionRequest,
    admission_policy_for,
    evaluate_admission,
)
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    begin_command,
    canonical_request_hash,
    enqueue_outbox,
    seal_promotional_scope,
    transition_attempt,
    transition_command,
)
from automl_api.storage.object_store import get_object_store
from automl_api.training.evaluation import (
    metric_direction,
    resolve_primary_metric,
)
from automl_api.training.feature_selection import FEATURE_STATISTICS_MAX_ROWS
from automl_api.training.model_catalog import (
    PAIRWISE_SAMPLE_MODELS,
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


def _training_runtime_baseline_mb(memory_limit_mb: int) -> float:
    """Reserve Ray/Python overhead without making small development nodes unusable."""
    return max(700.0, min(1_800.0, memory_limit_mb * 0.35))


def _common_sample_tier_rows(
    full_rows: int,
    task_type: TaskType,
    selected_specs: list[Any],
    *,
    memory_limit_mb: int,
    dataset_bytes: int,
    column_count: int,
    optimization_iterations: int,
) -> int:
    """Return the largest row count that fits the observed single-pod envelope."""
    if full_rows <= 0:
        return 0
    task_multiplier = {
        TaskType.REGRESSION: 1.25,
        TaskType.CLASSIFICATION: 1.3,
        TaskType.TIME_SERIES: 1.4,
        TaskType.CLUSTERING: 1.6,
    }.get(task_type, 1.3)
    scalable_specs = [item for item in selected_specs if item.name not in PAIRWISE_SAMPLE_MODELS]
    cost_weights = {"low": 1.0, "medium": 1.25, "high": 1.75}
    model_factor = max(
        (cost_weights[item.cost_tier] for item in scalable_specs),
        default=1.0,
    )
    search_multiplier = 1 + min(max(optimization_iterations, 1), 25) * 0.01
    source_bytes_per_row = dataset_bytes / full_rows if dataset_bytes > 0 else 0
    working_bytes_per_row = (
        max(
            source_bytes_per_row * 2,
            max(column_count, 1) * 8 * 3,
        )
        * task_multiplier
        * model_factor
        * search_multiplier
    )
    usable_bytes = max(
        0,
        (memory_limit_mb * 0.60 - _training_runtime_baseline_mb(memory_limit_mb)) * 1024**2,
    )
    if working_bytes_per_row <= 0:
        return full_rows
    rows = min(full_rows, int(usable_bytes / working_bytes_per_row))
    if rows == full_rows:
        return full_rows
    if rows >= 1_000:
        rows = rows // 1_000 * 1_000
    return max(1, rows)


def _pairwise_sample_rows(full_rows: int, memory_limit_mb: int) -> int:
    usable_bytes = max(
        0,
        (memory_limit_mb * 0.60 - _training_runtime_baseline_mb(memory_limit_mb)) * 1024**2,
    )
    rows = min(full_rows, math.isqrt(max(0, int(usable_bytes / (4 * 8)))))
    if rows == full_rows:
        return full_rows
    if rows >= 100:
        rows = rows // 100 * 100
    return max(1, rows)


def _estimate_split_counts(db, project_id, version, split_revision_id):
    if split_revision_id is None:
        return None
    split = db.scalar(
        select(DatasetSplitRevision).where(
            DatasetSplitRevision.project_id == project_id,
            DatasetSplitRevision.id == split_revision_id,
        )
    )
    if split is None or split.dataset_version_id != version.id or split.sealed_at is None:
        raise HTTPException(
            status_code=409, detail="Select a sealed split for this dataset version."
        )
    counts = split.specification.get("split_counts") or (
        split.specification.get("identity") or {}
    ).get("split_counts", {})
    if any(
        not isinstance(counts.get(role), int) or counts[role] < 0
        for role in ("train", "validation", "final_test")
    ):
        raise HTTPException(
            status_code=409, detail="The split has no valid row counts. Reprofile the dataset."
        )
    return {role: counts[role] for role in ("train", "validation", "final_test")}


def estimate_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: TrainingEstimateRequest,
    client: KubernetesTrainingClient | None = None,
    *,
    idempotency_key: str | None = None,
) -> TrainingEstimateRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    payload = _resolve_estimate_identity(db, project_id, payload)
    _validate_catalog_selection(payload)
    if payload.dataset_version_id is None or payload.task_type is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A resolved dataset version and task type are required.",
        )
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
    if payload.target_column in payload.excluded_columns:
        raise HTTPException(status_code=422, detail="The target cannot be an excluded feature.")
    available_columns = {item["name"] for item in (version.schema_json or {}).get("columns", [])}
    unknown_exclusions = sorted(set(payload.excluded_columns) - available_columns)
    if unknown_exclusions:
        raise HTTPException(
            status_code=422, detail="Unknown excluded columns: " + ", ".join(unknown_exclusions)
        )
    _validate_candidate_models(payload)
    catalog = candidate_catalog(payload.task_type)
    selected_candidate_count = (
        len(catalog)
        if payload.catalog_mode == "all"
        else len(payload.candidate_models) or int(payload.candidate_limit or 5)
    )
    selected_names = set(payload.candidate_models)
    selected_specs = [
        candidate
        for candidate in candidate_catalog(payload.task_type)
        if (
            True
            if payload.catalog_mode == "all"
            else (
                candidate.name in selected_names if selected_names else candidate.default_selected
            )
        )
    ][:selected_candidate_count]
    compatible_gpu_vendors = {
        vendor for candidate in selected_specs for vendor in supported_gpu_vendors(candidate.name)
    }
    cost_weights = {"low": 1.0, "medium": 1.25, "high": 1.75}
    model_cost_factor = (
        max(cost_weights[candidate.cost_tier] for candidate in selected_specs)
        if selected_specs
        else 1.0
    )
    settings = get_settings()
    split_counts = _estimate_split_counts(db, project_id, version, payload.split_revision_id)
    source_rows = int(version.row_count or 0)
    full_rows = split_counts["train"] if split_counts is not None else source_rows
    validation_rows = min(split_counts["validation"] if split_counts else full_rows, 10_000)
    training_bytes = round((version.byte_size or 0) * full_rows / max(1, source_rows))
    k8s = client or KubernetesTrainingClient()
    capacity_probe = k8s.estimate(
        dataset_bytes=1,
        dataset_rows=0,
        column_count=int(version.column_count or 0),
        expected_minutes=payload.expected_minutes,
        prefer_gpu=payload.prefer_gpu,
        task_type=payload.task_type,
        candidate_limit=selected_candidate_count,
        optimization_iterations=payload.optimization_iterations,
        model_cost_factor=model_cost_factor,
        gpu_compatible_vendors=compatible_gpu_vendors,
    )
    sample_rows = _common_sample_tier_rows(
        full_rows,
        payload.task_type,
        selected_specs,
        memory_limit_mb=capacity_probe.memory_limit_mb,
        dataset_bytes=training_bytes,
        column_count=int(version.column_count or 0),
        optimization_iterations=payload.optimization_iterations,
    )
    pairwise_rows = _pairwise_sample_rows(full_rows, capacity_probe.memory_limit_mb)
    sample_ratio = sample_rows / full_rows if full_rows > 0 and sample_rows > 0 else 1.0
    estimate = k8s.estimate(
        dataset_bytes=max(1, round(training_bytes * sample_ratio)),
        dataset_rows=sample_rows,
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
            "The dataset object is missing from object storage. Re-upload or restore this "
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
    if active_project_runs:
        estimate.blockers = [
            *estimate.blockers,
            "This project already has an active training or analysis run. "
            "Wait for it to finish or stop it before launching another run.",
        ]
        estimate.can_launch = False
    active_user_runs = int(
        db.scalar(
            select(func.count(ModelRun.id)).where(
                ModelRun.created_by_id == user.id,
                ModelRun.status.in_(active_statuses),
                ModelRun.run_kind.in_(BATCH_RUN_KINDS),
            )
        )
        or 0
    )
    active_resource_class_runs = active_db_runs
    decision = evaluate_admission(
        admission_policy_for(settings.environment),
        AdmissionRequest(
            project_id=str(project_id),
            user_id=str(user.id),
            resource_class="training",
            evaluation_scope_id=(
                str(payload.evaluation_scope_id) if payload.evaluation_scope_id else None
            ),
        ),
        active_global=active_db_runs,
        active_for_project=active_project_runs,
        active_for_user=active_user_runs,
        active_for_resource_class=active_resource_class_runs,
    )
    if not decision.can_launch:
        estimate.blockers = [*estimate.blockers, *decision.blockers]
        estimate.can_launch = False
    catalog_revision = _resolved_catalog_revision(db, project_id, payload)
    estimate.catalog_revision = catalog_revision.content_digest if catalog_revision else None
    estimate.capacity_profile_revision = (
        f"kubernetes-node-v2:{estimate.selected_node or 'unobserved'}:{estimate.memory_limit_mb}Mi"
    )
    estimate.candidate_count = selected_candidate_count
    estimate.sample_tier_summary = {
        "policy": "capacity_adaptive_random_family_v2",
        "row_count": sample_rows,
        "pairwise_row_count": pairwise_rows,
        "validation_row_count": validation_rows,
        "full_row_count": full_rows,
        "feature_statistics_max_rows": FEATURE_STATISTICS_MAX_ROWS,
        "sampled": bool(sample_rows and full_rows and sample_rows < full_rows),
        "selected_node": estimate.selected_node,
        "pod_memory_limit_mb": estimate.memory_limit_mb,
        "order": (
            "source_ordinal" if payload.task_type == TaskType.TIME_SERIES else "stable_row_id"
        ),
    }
    if split_counts is not None:
        estimate.sample_tier_summary.update(
            {
                "split_counts": split_counts,
                "source_row_count": source_rows,
                "split_revision_id": str(payload.split_revision_id),
            }
        )
    if sample_rows > FEATURE_STATISTICS_MAX_ROWS:
        estimate.warnings = [
            *estimate.warnings,
            f"Feature statistics use at most {FEATURE_STATISTICS_MAX_ROWS:,} "
            "seeded training rows per fold. "
            "The model is fitted on all admitted training rows in that fold.",
        ]
    if estimate.sample_tier_summary["sampled"]:
        estimate.warnings = [
            *estimate.warnings,
            f"Node capacity permits {sample_rows:,} seeded random training rows for "
            f"scalable models and {pairwise_rows:,} target-aware rows for quadratic "
            f"models in one {estimate.memory_limit_mb:,} MiB pod. Every model is evaluated "
            f"on the same seeded sample of up to {validation_rows:,} validation rows.",
        ]
    estimate.resource_class_slot_demand = {
        tier: sum(candidate.cost_tier == tier for candidate in selected_specs)
        for tier in ("low", "medium", "high")
    }
    estimate.required_node_quotas = {
        "cpu_millis": int(estimate.cpu_limit_cores * 1000),
        "memory_bytes": estimate.memory_limit_mb * 1024 * 1024,
        "gpu_count": int(estimate.gpu_requested),
    }
    estimate.expected_object_reads = max(1, selected_candidate_count)
    estimate.projected_cost_range = {
        "minimum": round(estimate.estimated_core_hours * 0.02, 4),
        "maximum": round(estimate.estimated_core_hours * 0.20, 4),
    }
    estimate.cpu_request_cores = estimate.cpu_limit_cores
    estimate.memory_request_mb = estimate.memory_limit_mb
    estimate.deadline_seconds = payload.deadline_seconds
    estimate.active_deadline_seconds = payload.deadline_seconds
    estimate.environment_qualified = not estimate.blockers
    digest_payload = {
        "request": payload.model_dump(mode="json"),
        "catalog_revision": estimate.catalog_revision,
        "capacity_profile_revision": estimate.capacity_profile_revision,
        "candidate_count": selected_candidate_count,
        "required_node_quotas": estimate.required_node_quotas,
    }
    estimate.estimate_digest = canonical_request_hash(digest_payload)
    if payload.reserve_capacity:
        if not idempotency_key:
            raise HTTPException(
                status_code=422,
                detail={"code": "idempotency_key_required"},
            )
        try:
            command, replayed = begin_command(
                db,
                project_id=project_id,
                actor_id=user.id,
                operation="training.estimate.reserve",
                idempotency_key=idempotency_key,
                payload=digest_payload,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail={"code": "idempotency_key_reused"}) from exc
        if replayed and command.response_payload:
            return TrainingEstimateRead.model_validate(command.response_payload)
        reservation = CapacityReservation(
            project_id=project_id,
            command_id=command.id,
            resource_class="training",
            cpu_millis=estimate.required_node_quotas["cpu_millis"],
            memory_bytes=estimate.required_node_quotas["memory_bytes"],
            gpu_count=estimate.required_node_quotas["gpu_count"],
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )
        db.add(reservation)
        db.flush()
        estimate.capacity_reservation = {
            "id": str(reservation.id),
            "expires_at": reservation.expires_at.isoformat(),
        }
        command.resource_type = "capacity_reservation"
        command.resource_id = reservation.id
        command.response_status = 200
        command.response_payload = estimate.model_dump(mode="json")
        transition_command(command, CommandStatus.RUNNING)
        transition_command(command, CommandStatus.SUCCEEDED)
    return estimate


def launch_training_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: TrainingLaunchRequest,
    client: KubernetesTrainingClient | None = None,
    *,
    idempotency_key: str,
) -> TrainingLaunchRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    _lock_training_admission(db)
    try:
        command, replayed = begin_command(
            db,
            project_id=project_id,
            actor_id=user.id,
            operation="training.launch",
            idempotency_key=idempotency_key,
            payload=payload.model_dump(mode="json"),
        )
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if replayed and command.response_payload:
        return TrainingLaunchRead.model_validate(command.response_payload)
    payload = _resolve_estimate_identity(db, project_id, payload)
    k8s = client or KubernetesTrainingClient()
    estimate_payload = TrainingEstimateRequest.model_validate(
        payload.model_dump(
            include=set(TrainingEstimateRequest.model_fields),
            exclude_unset=True,
        )
        | {
            "reserve_capacity": False,
            "evaluation_scope_id": payload.promotional_scope_id or payload.evaluation_scope_id,
            # Launch binds the catalog through the immutable launch revision. Carry that
            # identity into the estimate instead of allowing a newer active catalog to
            # change the launch precheck between estimate and admission.
            "catalog_revision_id": payload.estimator_catalog_revision_id,
        }
    )
    estimate = estimate_training_run(db, user, project_id, estimate_payload, k8s)
    if not estimate.can_launch:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Training precheck failed.",
                "blockers": estimate.blockers,
                "warnings": estimate.warnings,
            },
        )

    revisions = _resolve_launch_revisions(db, project_id, payload)
    supplied_reservation = _validate_launch_reservation(db, project_id, payload, estimate)

    now = datetime.now(UTC)
    if payload.task_type is None or payload.dataset_version_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="A resolved dataset version and task type are required.",
        )
    selected_candidates = (
        candidate_catalog(payload.task_type)
        if payload.catalog_mode == "all"
        else select_candidates(
            payload.task_type,
            payload.candidate_models or None,
            len(payload.candidate_models) if payload.candidate_models else payload.candidate_limit,
        )
    )
    disabled = {item.estimator for item in payload.estimator_overrides if not item.enabled}
    selected_candidates = [item for item in selected_candidates if item.name not in disabled]
    if not selected_candidates:
        raise HTTPException(status_code=422, detail="At least one estimator must be enabled.")
    if any(item.enabled and item.resource_class for item in payload.estimator_overrides):
        raise HTTPException(
            status_code=422,
            detail="Per-estimator resource classes are not supported by this runtime yet.",
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
            "training_rows": (
                estimate.sample_tier_summary.get("pairwise_row_count")
                if candidate.name in PAIRWISE_SAMPLE_MODELS
                else estimate.sample_tier_summary.get("row_count")
            ),
            "validation_rows": estimate.sample_tier_summary.get("validation_row_count"),
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
            "expected_minutes": payload.expected_minutes,
            "deadline_seconds": payload.deadline_seconds,
            "prefer_gpu": payload.prefer_gpu,
            # Persist the resolved catalog, not the request defaults. In
            # catalog_mode=all the request intentionally omits candidate_limit;
            # the worker must still receive every catalog entry.
            "candidate_limit": len(selected_candidates),
            "candidate_models": [candidate.name for candidate in selected_candidates],
            "optimization_iterations": payload.optimization_iterations,
            "cv_folds": payload.cv_folds,
            "evaluation_column": payload.evaluation_column,
            "positive_label": payload.positive_label,
            "primary_metric": primary_metric,
            "excluded_leakage_columns": excluded_leakage_columns,
            "excluded_columns": payload.excluded_columns,
            "leakage_profile_job_id": str(leakage_profile.id) if leakage_profile else None,
            "gpu_vendor": estimate.gpu_vendor,
            "gpu_resource": estimate.gpu_resource,
            "selected_node": estimate.selected_node,
            "sample_tier_rows": estimate.sample_tier_summary.get("row_count"),
            "pairwise_sample_rows": estimate.sample_tier_summary.get("pairwise_row_count"),
            "validation_sample_rows": estimate.sample_tier_summary.get("validation_row_count"),
            "sample_tier_policy": estimate.sample_tier_summary.get("policy"),
            "sample_tier_order": estimate.sample_tier_summary.get("order"),
            "feature_statistics_max_rows": FEATURE_STATISTICS_MAX_ROWS,
            "split_revision_id": str(payload.split_revision_id),
            "feature_contract_revision_id": str(payload.feature_contract_revision_id),
            "feature_registry_revision_id": str(payload.feature_registry_revision_id),
            "feature_recipe_revision_id": str(payload.feature_recipe_revision_id),
            "feature_search_space_revision_id": str(payload.feature_search_space_revision_id),
            "estimator_catalog_revision_id": str(payload.estimator_catalog_revision_id),
            "estimator_overrides": [
                override.model_dump(mode="json") for override in payload.estimator_overrides
            ],
        },
        tags={
            "project_id": str(project_id),
            "orchestrator": "kuberay",
            "accelerator": estimate.gpu_vendor or "cpu",
            "accelerator_resource": estimate.gpu_resource,
            "selected_node": estimate.selected_node,
            "leaderboard_primary_metric": primary_metric,
            "leaderboard": pending_leaderboard,
            "completed_candidates": 0,
            "desired_state": (
                "barrier_pending" if payload.promotional_scope_id else "ray_submission_pending"
            ),
        },
        queued_at=now,
    )
    db.add(run)
    db.flush()

    attempt = WorkflowAttempt(
        project_id=project_id,
        stage=WorkflowStage.TRAINING_RUN,
        logical_key=f"training-run:{run.id}",
        model_run_id=run.id,
        workload_identity=f"project-{project_id}-training",
        generation=1,
        fencing_token=uuid.uuid4().hex,
    )
    db.add(attempt)
    db.flush()
    for ordinal, candidate in enumerate(selected_candidates):
        override = next(
            (item for item in payload.estimator_overrides if item.estimator == candidate.name),
            None,
        )
        if override is not None and not override.enabled:
            continue
        db.add(
            TrainingCandidate(
                project_id=project_id,
                model_run_id=run.id,
                candidate_key=f"{ordinal}:{candidate.name}",
                estimator_key=candidate.name,
                catalog_revision_id=revisions["catalog"].id,
                feature_recipe_revision_id=revisions["recipe"].id,
                params={
                    "resource_class": override.resource_class if override else None,
                    "tunable": candidate.tunable,
                },
            )
        )
    if supplied_reservation is None:
        reservation = CapacityReservation(
            project_id=project_id,
            command_id=command.id,
            resource_class="training",
            cpu_millis=int(estimate.cpu_limit_cores * 1000),
            memory_bytes=estimate.memory_limit_mb * 1024 * 1024,
            gpu_count=1 if estimate.gpu_requested else 0,
            # This is a launch reservation, not an execution deadline. Consumed
            # reservations do not expire running work (including unlimited runs).
            expires_at=now + timedelta(minutes=10),
        )
        db.add(reservation)
    else:
        reservation = supplied_reservation
        reservation.command_id = command.id
    if payload.promotional_scope_id:
        scope = revisions["scope"]
        command.resource_type = "model_run"
        command.resource_id = run.id
        db.flush()
        db.add(
            PromotionalScopeMember(
                project_id=project_id,
                scope_id=scope.id,
                model_run_id=run.id,
                ordinal=_next_scope_ordinal(db, scope.id),
            )
        )
        db.flush()
        if _next_scope_ordinal(db, scope.id) == scope.expected_members:
            seal_promotional_scope(db, scope.id, expected_cas_version=scope.cas_version)
        else:
            run.status = RunStatus.PRECHECK_RUNNING
    else:
        reservation.status = "consumed"
        enqueue_outbox(
            db,
            command,
            topic="ray.training.submit",
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
        )
        run.status = RunStatus.QUEUED
    manifest = {
        "desiredState": run.tags["desired_state"],
        "attemptId": str(attempt.id),
        "fencingToken": attempt.fencing_token,
        "executor": "kuberay",
    }
    result = TrainingLaunchRead(
        run=ModelRunRead.model_validate(run),
        estimate=estimate,
        manifest=manifest,
    )
    command.resource_type = "model_run"
    command.resource_id = run.id
    command.response_status = status.HTTP_202_ACCEPTED
    command.response_payload = result.model_dump(mode="json")
    transition_command(command, CommandStatus.RUNNING)
    transition_command(command, CommandStatus.SUCCEEDED)
    db.flush()
    return result


def _lock_training_admission(db: Session) -> None:
    if db.bind is not None and db.bind.dialect.name == "postgresql":
        db.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": 7_301_247_011},
        )


def get_active_training_run(db: Session, user: User, project_id: uuid.UUID) -> ModelRun | None:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == project_id,
            ModelRun.run_kind == RunKind.TRAINING,
            ModelRun.status.in_([RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING]),
        )
        .order_by(ModelRun.created_at, ModelRun.id)
        .limit(1)
    )


def list_training_runs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    *,
    offset: int = 0,
    limit: int = 100,
) -> list[ModelRun]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind == RunKind.TRAINING,
            )
            .order_by(ModelRun.created_at.desc(), ModelRun.id.desc())
            .offset(offset)
            .limit(limit)
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
    ray_managed = (run.tags or {}).get("orchestrator") == "kuberay"
    attempt = _lock_latest_training_attempt(db, run) if ray_managed else None
    db.refresh(run, with_for_update=True)
    if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.PREEMPTED}:
        return run
    now = datetime.now(UTC)
    if run.status == RunStatus.CANCELLED:
        run.tags = _cancelled_training_tags(run.tags, run.finished_at or now)
        run.finished_at = run.finished_at or now
        db.flush()
        return run
    if ray_managed and attempt is not None:
        command, _ = begin_command(
            db,
            project_id=project_id,
            actor_id=user.id,
            operation="training.cancel",
            idempotency_key=f"training.cancel:{run.id}",
            payload={"run_id": str(run.id)},
        )
        if attempt.status in {
            AttemptStatus.PENDING,
            AttemptStatus.CLAIMED,
            AttemptStatus.SUBMITTED,
            AttemptStatus.RUNNING,
        }:
            transition_attempt(
                attempt,
                AttemptStatus.CANCELLED,
                fencing_token=attempt.fencing_token,
                terminal_reason="cancelled by user",
            )
        enqueue_outbox(
            db,
            command,
            topic="ray.training.cancel",
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
        )
        command.resource_type = "model_run"
        command.resource_id = run.id
        command.response_status = status.HTTP_200_OK
        command.response_payload = {"run_id": str(run.id), "status": "cancelled"}
        transition_command(command, CommandStatus.RUNNING)
        transition_command(command, CommandStatus.SUCCEEDED)
    elif not ray_managed and run.k8s_job_name:
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


def _lock_latest_training_attempt(db: Session, run: ModelRun) -> WorkflowAttempt | None:
    # Workers and recovery lock the attempt before the run. Recheck the latest
    # generation because recovery may have replaced it while this request waited.
    latest = (
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == run.project_id,
            WorkflowAttempt.model_run_id == run.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    while True:
        attempt = db.scalar(latest.with_for_update().execution_options(populate_existing=True))
        current = db.scalar(latest)
        if attempt is None or current.id == attempt.id:
            return attempt


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
                    entry.get("error") or "Training was cancelled before this candidate completed."
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
                "The dataset object is missing from object storage. Re-upload or restore "
                "this dataset version before restarting."
            ),
        )

    source_params = dict(source.params or {})
    payload = TrainingLaunchRequest(
        dataset_version_id=source.dataset_version_id,
        target_column=source.target_column,
        excluded_columns=list(source_params.get("excluded_columns") or []),
        catalog_mode=source_params.get("catalog_mode", "selected"),
        estimator_overrides=list(source_params.get("estimator_overrides") or []),
        evaluation_column=source_params.get("evaluation_column"),
        positive_label=source_params.get("positive_label"),
        task_type=source.task_type,
        primary_metric=source_params.get("primary_metric"),
        prefer_gpu=bool(source_params.get("prefer_gpu", source.gpu_requested)),
        expected_minutes=int(source_params.get("expected_minutes", 10)),
        candidate_limit=int(source_params.get("candidate_limit", 5)),
        candidate_models=list(source_params.get("candidate_models") or []),
        optimization_iterations=int(source_params.get("optimization_iterations", 5)),
        cv_folds=int(source_params.get("cv_folds", 3)),
        run_name=versioned_run_name(
            source.run_name or str(source.id),
            legacy_restart=bool((source.tags or {}).get("restarted_from_run_id")),
            increment=True,
        ),
        split_revision_id=_bound_revision_uuid(source_params, "split_revision_id"),
        feature_contract_revision_id=_bound_revision_uuid(
            source_params, "feature_contract_revision_id"
        ),
        feature_registry_revision_id=_bound_revision_uuid(
            source_params, "feature_registry_revision_id"
        ),
        feature_recipe_revision_id=_bound_revision_uuid(
            source_params, "feature_recipe_revision_id"
        ),
        feature_search_space_revision_id=_bound_revision_uuid(
            source_params, "feature_search_space_revision_id"
        ),
        estimator_catalog_revision_id=_bound_revision_uuid(
            source_params, "estimator_catalog_revision_id"
        ),
    )
    result = launch_training_run(
        db,
        user,
        project_id,
        payload,
        client,
        idempotency_key=f"restart:{source.id}",
    )
    restarted = db.get(ModelRun, result.run.id)
    if restarted is None:
        raise LookupError("The restarted run disappeared before tagging.")
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
        excluded_columns=list(source_params.get("excluded_columns") or []),
        evaluation_column=source_params.get("evaluation_column"),
        positive_label=source_params.get("positive_label"),
        task_type=parent.task_type,
        primary_metric=source_params.get("primary_metric"),
        prefer_gpu=request.prefer_gpu,
        expected_minutes=request.expected_minutes,
        candidate_limit=len(requested_models),
        candidate_models=requested_models,
        optimization_iterations=request.optimization_iterations,
        cv_folds=request.cv_folds,
        run_name=(f"{parent.run_name or parent.id} add {', '.join(requested_models)}")[:255],
        split_revision_id=_bound_revision_uuid(source_params, "split_revision_id"),
        feature_contract_revision_id=_bound_revision_uuid(
            source_params, "feature_contract_revision_id"
        ),
        feature_registry_revision_id=_bound_revision_uuid(
            source_params, "feature_registry_revision_id"
        ),
        feature_recipe_revision_id=_bound_revision_uuid(
            source_params, "feature_recipe_revision_id"
        ),
        feature_search_space_revision_id=_bound_revision_uuid(
            source_params, "feature_search_space_revision_id"
        ),
        estimator_catalog_revision_id=_bound_revision_uuid(
            source_params, "estimator_catalog_revision_id"
        ),
    )
    result = launch_training_run(
        db,
        user,
        project_id,
        payload,
        client,
        idempotency_key=f"add-models:{parent.id}:{','.join(requested_models)}",
    )
    extension = db.get(ModelRun, result.run.id)
    if extension is None:
        raise LookupError("The extended run disappeared before tagging.")
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
    # A healthy current pod has no status reason. Persist that absence so a
    # previous transient lookup failure cannot remain visible in the UI.
    if "status_reason" in snapshot:
        stored["status_reason"] = snapshot["status_reason"]
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
            snapshot.get("node_name") or stored.get("node_name") or params.get("selected_node")
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
        entry["model"]: dict(entry) for entry in leaderboard_run.tags.get("leaderboard", [])
    }
    if leaderboard_run.id != run.id:
        by_model.update({entry["model"]: dict(entry) for entry in run.tags.get("leaderboard", [])})
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
                    "running" if run.tags.get("current_candidate") == candidate.name else "pending"
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
    primary_metric = leaderboard_run.tags.get("leaderboard_primary_metric") or run.tags.get(
        "leaderboard_primary_metric"
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
    elif run.status in {RunStatus.FAILED, RunStatus.PREEMPTED}:
        for entry in entries:
            if entry.get("status") in {"running", "pending", "queued"}:
                started = entry.get("status") == "running"
                entry["status"] = "failed" if started else "skipped"
                entry["error"] = entry.get("error") or (
                    "Training stopped before this candidate completed."
                    if started else "Training stopped before this candidate started."
                )
    if primary_metric:
        entries = _rank_combined_leaderboard(entries, primary_metric)
    active_candidate = run.tags.get("current_candidate") or leaderboard_run.tags.get(
        "current_candidate"
    )
    active_phase = run.tags.get("candidate_phase") or leaderboard_run.tags.get("candidate_phase")
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
            prepared_split=bool(leaderboard_run.params.get("split_revision_id")),
            current_phase=(
                str(entry["phase"])
                if entry.get("status") in {"running", "pending"} and entry.get("phase")
                else str(active_phase)
                if entry.get("status") == "running"
                and entry.get("model") == active_candidate and active_phase
                else None
            ),
        )
    successful = [entry for entry in entries if entry.get("status") == "succeeded"]
    metric_names = {name for entry in entries for name in entry.get("metrics", {})}
    split_counts = None
    split_id = leaderboard_run.params.get("split_revision_id")
    if split_id:
        split = db.scalar(select(DatasetSplitRevision).where(
            DatasetSplitRevision.id == split_id,
            DatasetSplitRevision.project_id == project_id,
            DatasetSplitRevision.dataset_version_id == leaderboard_run.dataset_version_id,
            DatasetSplitRevision.sealed_at.is_not(None),
        ))
        if split is not None:
            counts = split.specification.get("split_counts") or (
                split.specification.get("identity") or {}
            ).get("split_counts", {})
            roles = ("train", "validation", "final_test")
            if all(isinstance(counts.get(role), int) and counts[role] >= 0 for role in roles):
                split_counts = {role: counts[role] for role in roles}
    return TrainingLeaderboardRead(
        run_id=run.id,
        status=run.status,
        primary_metric=primary_metric,
        winner=successful[0]["model"] if successful else None,
        metric_directions={name: metric_direction(name) for name in sorted(metric_names)},
        entries=entries,
        split_counts=split_counts,
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
    *,
    observe_managed: bool = False,
) -> None:
    # Ray attempts are observed and fenced by the workflow reconciler. Looking
    # up their CR name through the legacy batch/v1 Job client incorrectly
    # reports a healthy RayJob as missing.
    desired_state = str((run.tags or {}).get("desired_state") or "")
    if desired_state.startswith("ray_"):
        return
    # An API read can arrive before the outbox creates the Job. Only the
    # reconciler may observe managed batch jobs, after submission is committed.
    if desired_state.startswith("kubernetes_") and (
        not observe_managed or desired_state != "kubernetes_submitted"
    ):
        return
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
        run.failure_code = None
        run.failure_message = None
        run.plain_english_failure = None
        run.finished_at = None
    elif state == "succeeded":
        run.status = RunStatus.SUCCEEDED
        run.started_at = run.started_at or run.queued_at
        run.finished_at = now
        run.failure_code = None
        run.failure_message = None
        run.plain_english_failure = None
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


def _bound_revision_uuid(params: dict, key: str) -> uuid.UUID:
    value = params.get(key)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "This legacy run predates immutable revision bindings and cannot be extended. "
                "Create a new training launch from the current dataset revision."
            ),
        )
    try:
        return uuid.UUID(str(value))
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"The stored {key} is invalid; create a new training launch.",
        ) from exc


def _resolve_launch_revisions(
    db: Session,
    project_id: uuid.UUID,
    payload: TrainingLaunchRequest,
) -> dict[str, object]:
    bindings = {
        "split": (DatasetSplitRevision, payload.split_revision_id),
        "contract": (FeatureContractRevision, payload.feature_contract_revision_id),
        "registry": (FeatureRegistryRevision, payload.feature_registry_revision_id),
        "recipe": (FeatureRecipeRevision, payload.feature_recipe_revision_id),
        "search": (FeatureSearchSpaceRevision, payload.feature_search_space_revision_id),
        "catalog": (EstimatorCatalogRevision, payload.estimator_catalog_revision_id),
    }
    resolved: dict[str, object] = {}
    for key, (model, revision_id) in bindings.items():
        revision = db.scalar(
            select(model).where(model.project_id == project_id, model.id == revision_id)
        )
        if revision is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"The selected {key} revision is missing, stale, or belongs to another project."
                ),
            )
        resolved[key] = revision

    split = resolved["split"]
    contract = resolved["contract"]
    recipe = resolved["recipe"]
    search = resolved["search"]
    if split.dataset_version_id != payload.dataset_version_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The split revision does not belong to the selected dataset version.",
        )
    if (
        contract.task_type != payload.task_type.value
        or contract.target_column != payload.target_column
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The feature contract task or target does not match this launch.",
        )
    if recipe.registry_revision_id != payload.feature_registry_revision_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The feature recipe and registry revisions are not compatible.",
        )
    if search.metric_name != resolve_primary_metric(payload.task_type, payload.primary_metric):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The search-space objective does not match the selected primary metric.",
        )

    if payload.promotional_scope_id is not None:
        scope = db.scalar(
            select(PromotionalScope).where(
                PromotionalScope.project_id == project_id,
                PromotionalScope.id == payload.promotional_scope_id,
            )
        )
        if scope is None or scope.split_revision_id != payload.split_revision_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="The promotional scope is missing or bound to another split revision.",
            )
        if scope.status.value != "open":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="The promotional scope is already sealed or terminal.",
            )
        resolved["scope"] = scope
    return resolved


def _resolve_estimate_identity(
    db: Session,
    project_id: uuid.UUID,
    payload: TrainingEstimateRequest,
) -> TrainingEstimateRequest:
    if payload.experiment_spec_revision_id is None:
        if payload.catalog_mode == "all" and (
            payload.dataset_version_id is None or payload.task_type is None
        ):
            raise HTTPException(
                status_code=422,
                detail={"code": "experiment_spec_required"},
            )
        return payload
    spec = db.scalar(
        select(ExperimentSpecRevision).where(
            ExperimentSpecRevision.project_id == project_id,
            ExperimentSpecRevision.id == payload.experiment_spec_revision_id,
        )
    )
    if spec is None:
        raise HTTPException(status_code=409, detail={"code": "experiment_spec_changed"})
    assertions = {
        "dataset_version_id": spec.dataset_version_id,
        "task_type": TaskType(spec.task_type),
        "target_column": spec.target_column,
        "primary_metric": spec.primary_metric,
        "catalog_revision_id": spec.catalog_revision_id,
    }
    for name, expected in assertions.items():
        supplied = getattr(payload, name)
        if supplied is not None and supplied != expected:
            raise HTTPException(
                status_code=422,
                detail={"code": "experiment_spec_mismatch", "field": name},
            )
    return payload.model_copy(update=assertions)


def _validate_catalog_selection(payload: TrainingEstimateRequest) -> None:
    if payload.catalog_mode == "all":
        if payload.candidate_models or payload.execution_mode_hint != "auto":
            raise HTTPException(status_code=422, detail={"code": "invalid_catalog_selection"})
        fields_set = payload.model_fields_set
        if "candidate_limit" in fields_set and payload.candidate_limit is not None:
            raise HTTPException(status_code=422, detail={"code": "invalid_catalog_selection"})
        if payload.deadline_seconds != 7_200:
            raise HTTPException(
                status_code=422,
                detail={"code": "qualification_deadline_required"},
            )
        if payload.optimization_iterations != 5 or payload.cv_folds != 3:
            raise HTTPException(
                status_code=422,
                detail={"code": "qualification_strength_required"},
            )


def _resolved_catalog_revision(
    db: Session,
    project_id: uuid.UUID,
    payload: TrainingEstimateRequest,
) -> EstimatorCatalogRevision | None:
    revision_id = payload.catalog_revision_id
    if revision_id is None and isinstance(payload, TrainingLaunchRequest):
        revision_id = payload.estimator_catalog_revision_id
    if revision_id:
        revision = db.scalar(
            select(EstimatorCatalogRevision).where(
                EstimatorCatalogRevision.project_id == project_id,
                EstimatorCatalogRevision.id == revision_id,
            )
        )
        if revision is None:
            current = db.scalar(
                select(EstimatorCatalogRevision)
                .where(EstimatorCatalogRevision.project_id == project_id)
                .order_by(EstimatorCatalogRevision.revision.desc())
            )
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "catalog_revision_changed",
                    "requested_revision": str(revision_id),
                    "current_revision": str(current.id) if current else None,
                },
            )
        return revision
    return db.scalar(
        select(EstimatorCatalogRevision)
        .where(EstimatorCatalogRevision.project_id == project_id)
        .order_by(EstimatorCatalogRevision.revision.desc())
    )


def _validate_launch_reservation(
    db: Session,
    project_id: uuid.UUID,
    payload: TrainingLaunchRequest,
    current_estimate: TrainingEstimateRead,
) -> CapacityReservation | None:
    if payload.capacity_reservation_id is None:
        if payload.catalog_mode == "all" and payload.reserve_capacity:
            raise HTTPException(status_code=409, detail={"code": "capacity_reservation_required"})
        return None
    reservation = db.scalar(
        select(CapacityReservation)
        .where(
            CapacityReservation.project_id == project_id,
            CapacityReservation.id == payload.capacity_reservation_id,
        )
        .with_for_update()
    )
    if (
        reservation is None
        or reservation.status != "held"
        or reservation.expires_at <= datetime.now(UTC)
    ):
        raise HTTPException(status_code=409, detail={"code": "capacity_reservation_expired"})
    estimate_command = db.get(WorkflowCommand, reservation.command_id)
    stored = estimate_command.response_payload if estimate_command else {}
    if payload.estimate_digest != stored.get(
        "estimate_digest"
    ) or current_estimate.estimate_digest != stored.get("estimate_digest"):
        raise HTTPException(status_code=409, detail={"code": "capacity_profile_changed"})
    if payload.capacity_profile_revision != stored.get("capacity_profile_revision"):
        raise HTTPException(status_code=409, detail={"code": "capacity_profile_changed"})
    return reservation


def _next_scope_ordinal(db: Session, scope_id: uuid.UUID) -> int:
    return int(
        db.scalar(
            select(func.count())
            .select_from(PromotionalScopeMember)
            .where(PromotionalScopeMember.scope_id == scope_id)
        )
        or 0
    )


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
