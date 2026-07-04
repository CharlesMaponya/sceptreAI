from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from kubernetes.client import ApiException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    ArtifactKind,
    ModelStage,
    ProjectRole,
    RunKind,
    RunStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.schemas.operations import (
    ArtifactCleanupRead,
    ArtifactCleanupRequest,
    DeploymentStatusRead,
    DriftLaunchRead,
    DriftLaunchRequest,
    ModelDeploymentLaunchRead,
    ModelDeploymentRequest,
    PlatformHealthRead,
    RegistryCreateRequest,
    RegistryEntryRead,
)
from automl_api.schemas.training import ModelRunRead, TrainingEstimateRequest
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.projects import require_project_role
from automl_api.services.training import (
    _leaderboard_parent,
    _lock_training_admission,
    _sync_run_status,
    estimate_training_run,
)
from automl_api.storage.object_store import get_object_store

_STAGE_TRANSITIONS = {
    ModelStage.CANDIDATE: {ModelStage.STAGING, ModelStage.REJECTED, ModelStage.ARCHIVED},
    ModelStage.STAGING: {
        ModelStage.PRODUCTION,
        ModelStage.REJECTED,
        ModelStage.ARCHIVED,
    },
    ModelStage.PRODUCTION: {ModelStage.STAGING, ModelStage.ARCHIVED},
    ModelStage.REJECTED: {ModelStage.CANDIDATE, ModelStage.ARCHIVED},
    ModelStage.ARCHIVED: {ModelStage.CANDIDATE},
}


def register_model(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    request: RegistryCreateRequest,
) -> ModelRegistryEntry:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    _lock_training_admission(db)
    selected_run = _training_run(db, project_id, request.training_run_id)
    parent = _leaderboard_parent(db, selected_run)
    if parent.status != RunStatus.SUCCEEDED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The training run must succeed before a model can be registered.",
        )
    candidate = next(
        (
            entry
            for entry in parent.tags.get("leaderboard", [])
            if entry.get("model") == request.model_name
            and entry.get("status") == "succeeded"
        ),
        None,
    )
    if candidate is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="A successful leaderboard model with that name was not found.",
        )
    artifact_run = _candidate_run(db, parent, candidate)
    existing = db.scalar(
        select(ModelRegistryEntry).where(
            ModelRegistryEntry.project_id == project_id,
            ModelRegistryEntry.model_run_id == artifact_run.id,
            ModelRegistryEntry.model_name == request.model_name,
        )
    )
    if existing is not None:
        return existing

    model_uri = str(candidate.get("model_artifact_uri") or "")
    if not model_uri:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The selected model does not have a durable model artifact.",
        )
    store = get_object_store()
    try:
        artifact_size = store.size(model_uri)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The selected model artifact is unavailable.",
        ) from exc
    artifact = RunArtifact(
        project_id=project_id,
        model_run_id=artifact_run.id,
        kind=ArtifactKind.MODEL_OBJECT,
        name=f"{request.model_name}.joblib",
        object_uri=model_uri,
        content_hash=None,
        byte_size=artifact_size,
        artifact_metadata={
            "model_name": request.model_name,
            "mlflow_run_id": candidate.get("mlflow_run_id"),
        },
    )
    db.add(artifact)
    db.flush()

    version_number = int(
        db.scalar(
            select(func.max(ModelRegistryEntry.version)).where(
                ModelRegistryEntry.project_id == project_id,
                ModelRegistryEntry.model_name == request.model_name,
            )
        )
        or 0
    ) + 1
    primary_metric = parent.tags.get("leaderboard_primary_metric")
    metric_value = (
        candidate.get("metrics", {}).get(primary_metric)
        if primary_metric
        else None
    )
    entry = ModelRegistryEntry(
        project_id=project_id,
        model_run_id=artifact_run.id,
        model_artifact_id=artifact.id,
        stage=ModelStage.CANDIDATE,
        model_name=request.model_name,
        version=version_number,
        feature_space_hash=_feature_space_hash(parent),
        champion_metric_name=primary_metric,
        champion_metric_value=metric_value,
        registry_metadata={
            "registered_by_id": str(user.id),
            "source_training_run_id": str(parent.id),
            "mlflow_run_id": candidate.get("mlflow_run_id"),
            "metrics": candidate.get("metrics", {}),
            "diagnostics": candidate.get("diagnostics", {}),
            "fallback": False,
        },
    )
    db.add(entry)
    db.flush()
    return entry


def list_registry_entries(
    db: Session,
    user: User,
    project_id: uuid.UUID,
) -> list[RegistryEntryRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    entries = db.scalars(
        select(ModelRegistryEntry)
        .where(ModelRegistryEntry.project_id == project_id)
        .order_by(ModelRegistryEntry.created_at.desc())
    ).all()
    return [registry_entry_read(entry) for entry in entries]


def update_registry_stage(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    target_stage: ModelStage,
) -> ModelRegistryEntry:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    _lock_training_admission(db)
    entry = _registry_entry(db, project_id, entry_id)
    if target_stage == entry.stage:
        return entry
    if target_stage not in _STAGE_TRANSITIONS[entry.stage]:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot move a model from {entry.stage.value} to {target_stage.value}.",
        )
    now = datetime.now(UTC)
    if target_stage == ModelStage.PRODUCTION:
        current = db.scalars(
            select(ModelRegistryEntry).where(
                ModelRegistryEntry.project_id == project_id,
                ModelRegistryEntry.feature_space_hash == entry.feature_space_hash,
                ModelRegistryEntry.stage == ModelStage.PRODUCTION,
                ModelRegistryEntry.id != entry.id,
            )
        ).all()
        fallback_ids: set[uuid.UUID] = set()
        for index, previous in enumerate(current):
            is_fallback = index == 0
            previous.stage = ModelStage.STAGING
            previous.registry_metadata = {
                **previous.registry_metadata,
                "fallback": is_fallback,
                "replaced_by_entry_id": str(entry.id),
            }
            if is_fallback:
                fallback_ids.add(previous.id)
        _clear_fallbacks(
            db,
            project_id,
            entry.feature_space_hash,
            exclude_ids=fallback_ids,
        )
        entry.promoted_at = now
    if target_stage == ModelStage.ARCHIVED:
        entry.retired_at = now
    else:
        entry.retired_at = None
    entry.stage = target_stage
    entry.registry_metadata = {
        **entry.registry_metadata,
        "stage_updated_by_id": str(user.id),
        "stage_updated_at": now.isoformat(),
        "fallback": bool(
            entry.registry_metadata.get("fallback")
            and target_stage == ModelStage.STAGING
        ),
    }
    db.flush()
    return entry


def set_registry_fallback(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
) -> ModelRegistryEntry:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    _lock_training_admission(db)
    entry = _registry_entry(db, project_id, entry_id)
    if entry.stage != ModelStage.STAGING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only a staging model can be selected as fallback.",
        )
    _clear_fallbacks(db, project_id, entry.feature_space_hash)
    entry.registry_metadata = {
        **entry.registry_metadata,
        "fallback": True,
        "fallback_selected_by_id": str(user.id),
        "fallback_selected_at": datetime.now(UTC).isoformat(),
    }
    db.flush()
    return entry


def launch_drift_check(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    request: DriftLaunchRequest,
    client: KubernetesTrainingClient | None = None,
) -> DriftLaunchRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    entry = _registry_entry(db, project_id, entry_id)
    source = entry.model_run
    current_version = _dataset_version(db, project_id, request.dataset_version_id)
    k8s = client or KubernetesTrainingClient()
    estimate = estimate_training_run(
        db,
        user,
        project_id,
        TrainingEstimateRequest(
            dataset_version_id=current_version.id,
            target_column=None,
            task_type=source.task_type,
            expected_minutes=request.expected_minutes,
            candidate_limit=1,
            optimization_iterations=1,
            cv_folds=2,
        ),
        k8s,
    )
    if not estimate.can_launch:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Drift precheck failed.",
                "blockers": estimate.blockers,
                "warnings": estimate.warnings,
            },
        )
    run = ModelRun(
        project_id=project_id,
        dataset_version_id=current_version.id,
        created_by_id=user.id,
        run_kind=RunKind.DRIFT,
        status=RunStatus.PRECHECK_RUNNING,
        task_type=source.task_type,
        target_column=source.target_column,
        run_name=f"Drift - {entry.model_name} v{entry.version}",
        pipeline_name="evidently_drift_v1",
        k8s_namespace=k8s.settings.training_namespace,
        cpu_request_cores=estimate.cpu_request_cores,
        memory_request_mb=estimate.memory_request_mb,
        cpu_limit_cores=estimate.cpu_limit_cores,
        memory_limit_mb=estimate.memory_limit_mb,
        params={
            "registry_entry_id": str(entry.id),
            "reference_dataset_version_id": str(source.dataset_version_id),
            "max_rows": request.max_rows,
        },
        tags={"registry_entry_id": str(entry.id)},
        queued_at=datetime.now(UTC),
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
        run.finished_at = datetime.now(UTC)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The cluster rejected the drift job.",
        ) from exc
    run.status = RunStatus.QUEUED
    db.flush()
    return DriftLaunchRead(
        run=ModelRunRead.model_validate(run),
        estimate=estimate,
        manifest=manifest,
    )


def _adaptive_inference_memory(
    model_bytes: int,
    requested_memory: str,
) -> str:
    requested_mib = (
        int(requested_memory[:-2]) * 1024
        if requested_memory.endswith("Gi")
        else int(requested_memory[:-2])
    )
    estimated_mib = 1024 + (max(0, model_bytes) * 12 / 1024 / 1024)
    rounded_estimate_mib = int(math.ceil(estimated_mib / 512.0) * 512)
    selected_mib = max(requested_mib, rounded_estimate_mib)
    if selected_mib % 1024 == 0:
        return f"{selected_mib // 1024}Gi"
    return f"{selected_mib}Mi"


def deploy_registered_model(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    request: ModelDeploymentRequest,
    client: KubernetesTrainingClient | None = None,
) -> ModelDeploymentLaunchRead:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    _lock_training_admission(db)
    entry = _registry_entry(db, project_id, entry_id)
    if entry.stage not in {ModelStage.STAGING, ModelStage.PRODUCTION}:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Only staging or production models can be deployed.",
        )
    active_deployment = next(
        (
            deployment
            for deployment in db.scalars(
                select(ModelRun).where(
                    ModelRun.project_id == project_id,
                    ModelRun.run_kind == RunKind.DEPLOYMENT,
                    ModelRun.status.in_(
                        [
                            RunStatus.PRECHECK_RUNNING,
                            RunStatus.QUEUED,
                            RunStatus.RUNNING,
                            RunStatus.SUCCEEDED,
                        ]
                    ),
                )
            ).all()
            if deployment.tags.get("registry_entry_id") == str(entry.id)
        ),
        None,
    )
    if active_deployment is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="This model version already has an active deployment.",
        )
    k8s = client or KubernetesTrainingClient()
    project = db.get(Project, project_id)
    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Project not found.",
        )
    source = entry.model_run
    memory_request = _adaptive_inference_memory(
        entry.model_artifact.byte_size,
        request.memory_request,
    )
    deployment_run = ModelRun(
        project_id=project_id,
        dataset_version_id=source.dataset_version_id,
        created_by_id=user.id,
        run_kind=RunKind.DEPLOYMENT,
        status=RunStatus.PRECHECK_RUNNING,
        task_type=source.task_type,
        target_column=source.target_column,
        run_name=f"Deploy - {entry.model_name} v{entry.version}",
        pipeline_name="kubernetes_model_deployment_v1",
        k8s_namespace=k8s.settings.training_namespace,
        params={
            "registry_entry_id": str(entry.id),
            "replicas": request.replicas,
            "cpu_request": request.cpu_request,
            "memory_request": memory_request,
        },
        tags={"registry_entry_id": str(entry.id)},
        queued_at=datetime.now(UTC),
    )
    db.add(deployment_run)
    db.flush()
    image = request.image or k8s.settings.inference_image
    manifests = k8s.build_model_deployment_manifest(
        deployment_id=deployment_run.id,
        project_id=project_id,
        project_name=project.name,
        environment=k8s.settings.environment,
        model_name=entry.model_name,
        model_uri=entry.model_artifact.object_uri,
        image=image,
        replicas=request.replicas,
        cpu_request=request.cpu_request,
        memory_request=memory_request,
    )
    deployment_name = manifests["deployment"]["metadata"]["name"]
    service_name = manifests["service"]["metadata"]["name"]
    dockerfile = _generated_model_dockerfile(
        base_image=image,
        model_uri=entry.model_artifact.object_uri,
        model_name=entry.model_name,
        project_name=project.name,
        environment=k8s.settings.environment,
        registry_entry_id=entry.id,
    )
    stored = get_object_store().put_bytes(
        (
            f"projects/{project_id}/deployments/{deployment_run.id}/"
            "Dockerfile"
        ),
        dockerfile.encode("utf-8"),
    )
    db.add(
        RunArtifact(
            project_id=project_id,
            model_run_id=deployment_run.id,
            kind=ArtifactKind.DEPLOYMENT_IMAGE,
            name="Dockerfile",
            object_uri=stored.uri,
            content_hash=hashlib.sha256(dockerfile.encode("utf-8")).hexdigest(),
            byte_size=len(dockerfile.encode("utf-8")),
            artifact_metadata={
                "registry_entry_id": str(entry.id),
                "base_image": image,
                "artifact_type": "dockerfile",
            },
        )
    )
    try:
        k8s.create_model_deployment(manifests)
    except Exception as exc:
        deployment_run.status = RunStatus.FAILED
        deployment_run.failure_code = "KUBERNETES_DEPLOYMENT_CREATE_FAILED"
        deployment_run.failure_message = str(exc)
        deployment_run.finished_at = datetime.now(UTC)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The cluster rejected the model deployment.",
        ) from exc
    urls = k8s.model_deployment_urls(service_name) or {}
    deployment_run.status = RunStatus.RUNNING
    deployment_run.started_at = datetime.now(UTC)
    deployment_run.k8s_job_name = deployment_name
    deployment_run.tags = {
        **deployment_run.tags,
        "service_name": service_name,
        "project_name": project.name,
        "environment": k8s.settings.environment,
        **urls,
        "endpoint": urls.get(
            "endpoint",
            f"http://{service_name}:8080/v1/predict",
        ),
        "image": image,
        "model_artifact_id": str(entry.model_artifact_id),
        "dockerfile_uri": stored.uri,
    }
    db.flush()
    return ModelDeploymentLaunchRead(
        run=ModelRunRead.model_validate(deployment_run),
        manifests=manifests,
        dockerfile_uri=stored.uri,
    )


def list_model_deployments(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> list[DeploymentStatusRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    k8s = client or KubernetesTrainingClient()
    runs = db.scalars(
        select(ModelRun)
        .where(
            ModelRun.project_id == project_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
        )
        .order_by(ModelRun.created_at.desc())
    ).all()
    result = []
    for run in runs:
        runtime_state = "unknown"
        if run.k8s_job_name and run.status != RunStatus.CANCELLED:
            try:
                runtime_state = k8s.model_deployment_state(run.k8s_job_name)
            except Exception:
                runtime_state = "unavailable"
            if runtime_state == "ready":
                run.status = RunStatus.SUCCEEDED
                run.failure_code = None
                run.failure_message = None
                try:
                    urls = k8s.model_deployment_urls(run.k8s_job_name) or {}
                    run.tags = {**run.tags, **urls}
                except Exception:
                    pass
            elif runtime_state == "missing":
                run.status = RunStatus.FAILED
                run.failure_code = "KUBERNETES_DEPLOYMENT_MISSING"
                run.failure_message = "The Kubernetes deployment no longer exists."
                run.finished_at = datetime.now(UTC)
            elif runtime_state in {
                "image_pull_error",
                "crash_loop",
                "configuration_error",
                "out_of_memory",
            }:
                failure_details = {
                    "image_pull_error": (
                        "INFERENCE_IMAGE_PULL_FAILED",
                        "Kubernetes could not pull the inference image. Build it "
                        "inside Minikube or publish it to an accessible registry.",
                    ),
                    "crash_loop": (
                        "INFERENCE_CONTAINER_CRASH_LOOP",
                        "The inference container repeatedly exits. Inspect the "
                        "pod events and previous container logs.",
                    ),
                    "configuration_error": (
                        "INFERENCE_CONTAINER_CONFIGURATION_FAILED",
                        "Kubernetes could not create the inference container. "
                        "Inspect its environment, secrets, and image name.",
                    ),
                    "out_of_memory": (
                        "INFERENCE_CONTAINER_OUT_OF_MEMORY",
                        "The model exceeded its serving memory limit. Redeploy "
                        "it to apply the adaptive model-size memory estimate.",
                    ),
                }
                run.status = RunStatus.FAILED
                run.failure_code, run.failure_message = failure_details[runtime_state]
        result.append(
            DeploymentStatusRead(
                run=ModelRunRead.model_validate(run),
                runtime_state=runtime_state,
                endpoint=run.tags.get("endpoint"),
                base_url=run.tags.get("base_url"),
                docs_url=run.tags.get("docs_url"),
                openapi_url=run.tags.get("openapi_url"),
                status=run.status,
            )
        )
    db.flush()
    return result


def list_drift_runs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> list[ModelRun]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind == RunKind.DRIFT,
            )
            .order_by(ModelRun.created_at.desc())
        ).all()
    )
    k8s = client or KubernetesTrainingClient()
    for run in runs:
        if (
            run.k8s_job_name
            and run.status
            in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING}
        ):
            try:
                _sync_run_status(db, run, k8s)
            except ApiException:
                break
    return runs


def stop_model_deployment(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
        )
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found.")
    if run.k8s_job_name and run.status != RunStatus.CANCELLED:
        try:
            (client or KubernetesTrainingClient()).delete_model_deployment(run.k8s_job_name)
        except ApiException as exc:
            if exc.status != 404:
                raise
    run.status = RunStatus.CANCELLED
    run.finished_at = datetime.now(UTC)
    db.flush()
    return run


def platform_health(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> PlatformHealthRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    snapshot = (client or KubernetesTrainingClient()).capacity_snapshot()
    active_deployments = int(
        db.scalar(
            select(func.count(ModelRun.id)).where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind == RunKind.DEPLOYMENT,
                ModelRun.status.in_([RunStatus.RUNNING, RunStatus.SUCCEEDED]),
            )
        )
        or 0
    )
    object_store_status = "ok"
    try:
        get_object_store().healthcheck()
    except Exception:
        object_store_status = "unavailable"
    return PlatformHealthRead(
        capacity=snapshot.capacity,
        components={
            "database": "ok",
            "kubernetes": "ok" if snapshot.capacity.connected else "unavailable",
            "dataset_cache": "ok" if snapshot.pvc_ready else "unavailable",
            "priority_class": "ok" if snapshot.priority_class_ready else "unavailable",
            "runtime_dependencies": (
                "ok" if snapshot.runtime_dependencies_ready else "unavailable"
            ),
            "object_store": object_store_status,
        },
        active_deployments=active_deployments,
    )


def cleanup_project_resources(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    request: ArtifactCleanupRequest,
    client: KubernetesTrainingClient | None = None,
) -> ArtifactCleanupRead:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    cutoff = datetime.now(UTC) - timedelta(days=request.older_than_days)
    active_artifact_ids: set[uuid.UUID] = set()
    active_deployment_run_ids: set[uuid.UUID] = set()
    active_deployments = db.scalars(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
            ModelRun.status.in_([RunStatus.RUNNING, RunStatus.SUCCEEDED]),
        )
    ).all()
    for deployment in active_deployments:
        active_deployment_run_ids.add(deployment.id)
        artifact_id = deployment.tags.get("model_artifact_id")
        if not artifact_id:
            continue
        try:
            active_artifact_ids.add(uuid.UUID(str(artifact_id)))
        except ValueError:
            continue
    candidates = [
        artifact
        for artifact in db.scalars(
            select(RunArtifact).where(
                RunArtifact.project_id == project_id,
                RunArtifact.created_at < cutoff,
            )
        ).all()
        if (
            not artifact.registry_entries
            and artifact.id not in active_artifact_ids
            and artifact.model_run_id not in active_deployment_run_ids
        )
    ]
    deleted_uris: list[str] = []
    errors: list[str] = []
    if not request.dry_run:
        store = get_object_store()
        for artifact in candidates:
            try:
                store.delete(artifact.object_uri)
                deleted_uris.append(artifact.object_uri)
                db.delete(artifact)
            except Exception as exc:
                errors.append(f"{artifact.object_uri}: {exc}")
    deleted_jobs: list[str] = []
    if not request.dry_run and request.cleanup_finished_jobs:
        try:
            deleted_jobs = (client or KubernetesTrainingClient()).cleanup_finished_jobs(
                project_id
            )
        except Exception as exc:
            errors.append(f"Kubernetes cleanup: {exc}")
    db.flush()
    return ArtifactCleanupRead(
        dry_run=request.dry_run,
        artifact_count=len(candidates),
        artifact_bytes=sum(artifact.byte_size or 0 for artifact in candidates),
        artifact_ids=[artifact.id for artifact in candidates],
        deleted_object_uris=deleted_uris,
        deleted_kubernetes_jobs=deleted_jobs,
        errors=errors,
    )


def registry_entry_read(entry: ModelRegistryEntry) -> RegistryEntryRead:
    return RegistryEntryRead(
        id=entry.id,
        project_id=entry.project_id,
        model_run_id=entry.model_run_id,
        model_artifact_id=entry.model_artifact_id,
        stage=entry.stage,
        model_name=entry.model_name,
        version=entry.version,
        feature_space_hash=entry.feature_space_hash,
        champion_metric_name=entry.champion_metric_name,
        champion_metric_value=entry.champion_metric_value,
        promoted_at=entry.promoted_at,
        retired_at=entry.retired_at,
        registry_metadata=entry.registry_metadata,
        created_at=entry.created_at,
        updated_at=entry.updated_at,
        model_artifact_uri=entry.model_artifact.object_uri,
        is_fallback=bool(entry.registry_metadata.get("fallback")),
    )


def _registry_entry(
    db: Session,
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
) -> ModelRegistryEntry:
    entry = db.scalar(
        select(ModelRegistryEntry).where(
            ModelRegistryEntry.project_id == project_id,
            ModelRegistryEntry.id == entry_id,
        )
    )
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Registry entry not found.",
        )
    return entry


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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Training run not found.")
    return run


def _candidate_run(
    db: Session,
    parent: ModelRun,
    candidate: dict[str, Any],
) -> ModelRun:
    extension_id = candidate.get("extension_run_id")
    if not extension_id:
        return parent
    try:
        extension = db.get(ModelRun, uuid.UUID(str(extension_id)))
    except ValueError:
        extension = None
    if extension is None or extension.project_id != parent.project_id:
        return parent
    return extension


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
    return version


def _feature_space_hash(run: ModelRun) -> str:
    payload = {
        "task_type": run.task_type.value,
        "target_column": run.target_column,
        "schema": run.dataset_version.schema_json,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _clear_fallbacks(
    db: Session,
    project_id: uuid.UUID,
    feature_space_hash: str,
    *,
    exclude_ids: set[uuid.UUID] | None = None,
) -> None:
    exclude_ids = exclude_ids or set()
    entries = db.scalars(
        select(ModelRegistryEntry).where(
            ModelRegistryEntry.project_id == project_id,
            ModelRegistryEntry.feature_space_hash == feature_space_hash,
        )
    ).all()
    for entry in entries:
        if entry.id in exclude_ids:
            continue
        if entry.registry_metadata.get("fallback"):
            entry.registry_metadata = {
                **entry.registry_metadata,
                "fallback": False,
            }


def _generated_model_dockerfile(
    *,
    base_image: str,
    model_uri: str,
    model_name: str,
    project_name: str,
    environment: str,
    registry_entry_id: uuid.UUID,
) -> str:
    for value in (
        base_image,
        model_uri,
        model_name,
        project_name,
        environment,
    ):
        if "\n" in value or "\r" in value:
            raise ValueError("Generated Dockerfile values cannot contain line breaks.")
    labels = {
        "org.opencontainers.image.title": f"{project_name} - {model_name}",
        "ai.sceptre.registry-entry-id": str(registry_entry_id),
    }
    label_text = " \\\n".join(
        f"      {json.dumps(name)}={json.dumps(value)}"
        for name, value in labels.items()
    )
    return (
        f"FROM {base_image}\n"
        f"LABEL {label_text}\n"
        f"ENV MODEL_URI={json.dumps(model_uri)}\n"
        f"ENV MODEL_NAME={json.dumps(model_name)}\n"
        f"ENV PROJECT_NAME={json.dumps(project_name)}\n"
        f"ENV DEPLOYMENT_ENVIRONMENT={json.dumps(environment)}\n"
        "EXPOSE 8080\n"
        'CMD ["uvicorn", "automl_api.inference.app:app", '
        '"--host", "0.0.0.0", "--port", "8080"]\n'
    )
