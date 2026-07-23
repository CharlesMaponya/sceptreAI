from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.schemas.training import (
    ModelRunRead,
    TrainingEstimateRequest,
)
from automl_api.schemas.validation import (
    AnalysisLaunchRead,
    AnalysisResultRead,
    ExplainabilityLaunchRequest,
    RunArtifactRead,
    ValidationLaunchRequest,
)
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.projects import require_project_role
from automl_api.services.training import (
    _leaderboard_parent,
    _lock_training_admission,
    estimate_training_run,
)
from automl_api.storage.object_store import get_object_store
from automl_api.training.analysis import normalize_feature_importance


def launch_validation_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    request: ValidationLaunchRequest,
    client: KubernetesTrainingClient | None = None,
) -> AnalysisLaunchRead:
    source, model_entry = _source_model(
        db,
        user,
        project_id,
        training_run_id,
        request.model_name,
    )
    version = _dataset_version(db, project_id, request.dataset_version_id)
    _require_matching_validation_columns(source, version)
    if source.target_column and not _has_column(version, source.target_column):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"External dataset does not contain target column '{source.target_column}'."),
        )
    if request.evaluation_column and not _has_column(
        version,
        request.evaluation_column,
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="The clustering evaluation column is missing.",
        )
    return _launch_analysis_run(
        db,
        user,
        source,
        version,
        model_entry,
        RunKind.VALIDATION,
        expected_minutes=request.expected_minutes,
        extra_params={"evaluation_column": request.evaluation_column},
        client=client,
    )


def launch_explainability_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    request: ExplainabilityLaunchRequest,
    client: KubernetesTrainingClient | None = None,
) -> AnalysisLaunchRead:
    source, model_entry = _source_model(
        db,
        user,
        project_id,
        training_run_id,
        request.model_name,
        require_artifact=False,
        require_source_complete=False,
    )
    _lock_training_admission(db)
    existing = _reusable_explainability_run(
        db,
        source,
        str(model_entry["model"]),
    )
    active_statuses = {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING}
    if existing is not None and (not request.force or existing.status in active_statuses):
        return AnalysisLaunchRead(
            run=ModelRunRead.model_validate(existing),
            cached=True,
        )
    version = _dataset_version(db, project_id, source.dataset_version_id)
    return _launch_analysis_run(
        db,
        user,
        source,
        version,
        model_entry,
        RunKind.EXPLAINABILITY,
        expected_minutes=request.expected_minutes,
        extra_params={
            "max_rows": request.max_rows,
            "evaluation_column": source.params.get("evaluation_column"),
        },
        client=client,
    )


def list_analysis_runs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
) -> list[ModelRun]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    source = _training_run(db, project_id, training_run_id)
    source = _leaderboard_parent(db, source)
    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind.in_([RunKind.VALIDATION, RunKind.EXPLAINABILITY]),
            )
            .order_by(ModelRun.created_at.desc())
        ).all()
    )
    return [run for run in runs if run.tags.get("source_training_run_id") == str(source.id)]


def get_analysis_result(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    run_id: uuid.UUID,
) -> AnalysisResultRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    source = _leaderboard_parent(
        db,
        _training_run(db, project_id, training_run_id),
    )
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind.in_([RunKind.VALIDATION, RunKind.EXPLAINABILITY]),
        )
    )
    if run is None or run.tags.get("source_training_run_id") != str(source.id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Validation or explainability run not found.",
        )
    artifacts = list(
        db.scalars(
            select(RunArtifact).where(
                RunArtifact.project_id == project_id,
                RunArtifact.model_run_id == run.id,
            )
        ).all()
    )
    return AnalysisResultRead(
        run_id=run.id,
        status=run.status,
        model_name=str(run.params.get("model_name", "unknown")),
        metrics=run.tags.get("metrics", {}),
        diagnostics=run.tags.get("diagnostics", {}),
        feature_importance=normalize_feature_importance(
            run.tags.get("feature_importance", [])
        ),
        artifacts=[RunArtifactRead.model_validate(artifact) for artifact in artifacts],
    )


def _launch_analysis_run(
    db: Session,
    user: User,
    source: ModelRun,
    version: DatasetVersion,
    model_entry: dict,
    run_kind: RunKind,
    *,
    expected_minutes: int,
    extra_params: dict,
    client: KubernetesTrainingClient | None,
) -> AnalysisLaunchRead:
    _lock_training_admission(db)
    k8s = client or KubernetesTrainingClient()
    estimate = estimate_training_run(
        db,
        user,
        source.project_id,
        TrainingEstimateRequest(
            dataset_version_id=version.id,
            target_column=source.target_column,
            task_type=source.task_type,
            expected_minutes=expected_minutes,
            candidate_limit=1,
            candidate_models=[str(model_entry["model"])],
            optimization_iterations=1,
            cv_folds=2,
        ),
        k8s,
    )
    if not estimate.can_launch:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Analysis precheck failed.",
                "blockers": estimate.blockers,
                "warnings": estimate.warnings,
            },
        )
    run = ModelRun(
        project_id=source.project_id,
        dataset_version_id=version.id,
        created_by_id=user.id,
        run_kind=run_kind,
        status=RunStatus.PRECHECK_RUNNING,
        task_type=source.task_type,
        target_column=source.target_column,
        run_name=f"{model_entry['model']} {run_kind.value}",
        pipeline_name=f"tabular_{run_kind.value}_v1",
        k8s_namespace=k8s.settings.training_namespace,
        gpu_requested=False,
        cpu_request_cores=estimate.cpu_request_cores,
        memory_request_mb=estimate.memory_request_mb,
        cpu_limit_cores=estimate.cpu_limit_cores,
        memory_limit_mb=estimate.memory_limit_mb,
        estimated_core_hours=estimate.estimated_core_hours,
        params={
            "source_training_run_id": str(source.id),
            "model_name": model_entry["model"],
            "model_mlflow_run_id": _model_mlflow_run_id(
                source,
                model_entry,
            ),
            "model_artifact_uri": model_entry.get("model_artifact_uri"),
            "expected_minutes": expected_minutes,
            "positive_label": source.params.get("positive_label"),
            **extra_params,
        },
        tags={
            "project_id": str(source.project_id),
            "orchestrator": "kubernetes",
            "source_training_run_id": str(source.id),
        },
        queued_at=datetime.now(UTC),
    )
    db.add(run)
    db.flush()
    manifest = k8s.build_job_manifest(
        run_id=run.id,
        project_id=run.project_id,
        estimate=estimate,
    )
    run.k8s_job_name = manifest["metadata"]["name"]
    try:
        k8s.create_job(manifest)
    except Exception as exc:
        run.status = RunStatus.FAILED
        run.failure_code = "KUBERNETES_JOB_CREATE_FAILED"
        run.failure_message = str(exc)
        run.plain_english_failure = "Kubernetes could not start the isolated analysis job."
        run.finished_at = datetime.now(UTC)
        db.flush()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=run.plain_english_failure,
        ) from exc
    run.status = RunStatus.QUEUED
    db.flush()
    return AnalysisLaunchRead(
        run=ModelRunRead.model_validate(run),
        estimate=estimate,
        manifest=manifest,
    )


def _source_model(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    model_name: str,
    *,
    require_artifact: bool = True,
    require_source_complete: bool = True,
) -> tuple[ModelRun, dict]:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    source = _leaderboard_parent(
        db,
        _training_run(db, project_id, run_id),
    )
    if require_source_complete and source.status != RunStatus.SUCCEEDED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The source training run must be complete.",
        )
    entry = next(
        (
            item
            for item in source.tags.get("leaderboard", [])
            if item.get("model") == model_name and item.get("status") == "succeeded"
        ),
        None,
    )
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Select a model that completed successfully.",
        )
    if require_artifact and not (
        _model_mlflow_run_id(source, entry) or entry.get("model_artifact_uri")
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The selected model has no persisted artifact.",
        )
    return source, entry


def _training_run(
    db: Session,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
) -> ModelRun:
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.TRAINING,
        )
    )
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Training run not found.",
        )
    return run


def _dataset_version(
    db: Session,
    project_id: uuid.UUID,
    version_id: uuid.UUID,
) -> DatasetVersion:
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == project_id,
            DatasetVersion.id == version_id,
        )
    )
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset version not found.",
        )
    if not get_object_store().exists(version.object_uri):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The selected dataset object is missing from object storage.",
        )
    return version


def _model_mlflow_run_id(source: ModelRun, entry: dict) -> str | None:
    return entry.get("mlflow_run_id") or (
        source.mlflow_run_id if entry.get("model") == source.tags.get("winner") else None
    )


def _reusable_explainability_run(
    db: Session,
    source: ModelRun,
    model_name: str,
) -> ModelRun | None:
    reusable_statuses = {
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
        RunStatus.SUCCEEDED,
    }
    runs = db.scalars(
        select(ModelRun)
        .where(
            ModelRun.project_id == source.project_id,
            ModelRun.run_kind == RunKind.EXPLAINABILITY,
        )
        .order_by(ModelRun.created_at.desc())
    ).all()
    return next(
        (
            run
            for run in runs
            if run.status in reusable_statuses
            and run.tags.get("source_training_run_id") == str(source.id)
            and run.params.get("model_name") == model_name
        ),
        None,
    )


def _has_column(version: DatasetVersion, column: str) -> bool:
    return column in {item.get("name") for item in version.schema_json.get("columns", [])}


def _require_matching_validation_columns(
    source: ModelRun,
    external: DatasetVersion,
) -> None:
    required = {
        str(item.get("name"))
        for item in source.dataset_version.schema_json.get("columns", [])
        if item.get("name")
    }
    available = {
        str(item.get("name"))
        for item in external.schema_json.get("columns", [])
        if item.get("name")
    }
    missing = sorted(required - available)
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "External validation columns do not match the training dataset. "
                f"Missing columns: {', '.join(missing)}."
            ),
        )
