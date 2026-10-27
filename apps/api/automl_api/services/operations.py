from __future__ import annotations

import hashlib
import json
import logging
import math
import time
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException, status
from kubernetes.client import ApiException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    ArtifactKind,
    CommandStatus,
    ModelStage,
    ProjectRole,
    ProjectStatus,
    RunKind,
    RunStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import DeletionStage, DeletionTombstone, WorkflowCommand
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
from automl_api.services.deployment_controls import CONTROL, queue_control
from automl_api.services.deployment_submission import lifecycle_custody
from automl_api.services.inference_tls import PROTOCOL as INFERENCE_PROTOCOL
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
)
from automl_api.services.model_custody import (
    PROTOCOL as MODEL_CUSTODY_PROTOCOL,
)
from automl_api.services.model_custody import (
    deployment_model_custody,
    registered_model_custody,
    registration_model_custody,
    verify_model_custody,
)
from automl_api.services.projects import require_project_role
from automl_api.services.training import (
    _leaderboard_parent,
    _lock_training_admission,
    _sync_run_status,
    estimate_analysis_resources,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.object_store import get_object_store

logger = logging.getLogger(__name__)

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

_MONITORING_RESOURCE_FLOORS = {
    "small": (0.5, 1024),
    "standard": (1.0, 2048),
    "large": (2.0, 4096),
    "xlarge": (4.0, 8192),
}


@dataclass(frozen=True)
class RegistrationInspection:
    selection_digest: str
    object_uri: str
    token_version: int
    custody: dict
    byte_size: int | None = None
    native_digest: str | None = None


def _lock_registration_source(db, user, project_id, selected, parent, inspection):
    expected_version = user.token_version if inspection is None else inspection.token_version
    project = db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    current = db.get(User, user.id, with_for_update={"read": True}, populate_existing=True)
    if current is None or not current.is_active or current.token_version != expected_version:
        raise HTTPException(401, "Registration authentication changed")
    if project is None or project.status != ProjectStatus.ACTIVE:
        raise HTTPException(409, "Registration project is inactive or missing")
    db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.user_id == current.id,
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    require_project_role(db, current, project_id, ProjectRole.EDITOR)
    db.refresh(parent, with_for_update={"read": True})
    if selected.id != parent.id:
        db.refresh(selected, with_for_update={"read": True})
        if _leaderboard_parent(db, selected).id != parent.id:
            raise HTTPException(409, "Registration source parent changed")
    version = db.get(
        DatasetVersion,
        parent.dataset_version_id,
        with_for_update={"read": True},
        populate_existing=True,
    )
    if version is None or version.project_id != project_id:
        raise HTTPException(409, "Registration source schema changed")
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == project_id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & DeletionTombstone.resource_id.in_([selected.id, parent.id])
            ),
        )
        .limit(1)
    )
    if deleting is not None:
        raise HTTPException(409, "Registration source is being deleted")
    return current


def inspect_model_registration(selection):
    try:
        digest = verify_model_custody(get_object_store(), selection.custody)
        return replace(
            selection, byte_size=selection.custody["completion"]["byte_size"], native_digest=digest
        )
    except Exception:
        raise HTTPException(
            409, "The selected model artifact could not verify its original custody."
        ) from None


def register_model(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    request: RegistryCreateRequest,
    *,
    inspection: RegistrationInspection | None = None,
) -> ModelRegistryEntry | RegistrationInspection:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    _lock_training_admission(db)
    selected_run = _training_run(db, project_id, request.training_run_id)
    parent = _leaderboard_parent(db, selected_run)
    user = _lock_registration_source(db, user, project_id, selected_run, parent, inspection)
    # Candidates publish their durable artifact before reporting success. A sibling
    # still training (or failing) must not block registration of a completed model.
    candidate = next(
        (
            entry
            for entry in parent.tags.get("leaderboard", [])
            if entry.get("model") == request.model_name and entry.get("status") == "succeeded"
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
    model_uri = str(candidate.get("model_artifact_uri") or "")
    if not model_uri:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The selected model does not have a durable model artifact.",
        )
    model_digest = str(candidate.get("model_artifact_sha256") or "")
    if len(model_digest) != 64 or any(char not in "0123456789abcdef" for char in model_digest):
        raise HTTPException(
            status_code=409,
            detail=(
                "This model predates integrity verification. Train a new model before registration."
            ),
        )
    if artifact_run.id != parent.id:
        db.refresh(artifact_run, with_for_update={"read": True})
        if (
            artifact_run.project_id != project_id
            or db.scalar(
                select(DeletionTombstone.id)
                .where(
                    DeletionTombstone.project_id == project_id,
                    DeletionTombstone.resource_type == "run",
                    DeletionTombstone.resource_id == artifact_run.id,
                )
                .limit(1)
            )
            is not None
        ):
            raise HTTPException(409, "Registration candidate source changed or is being deleted")
    try:
        custody = registration_model_custody(db, artifact_run, candidate)
        if existing is not None and inspection is None:
            stored_artifact = db.get(
                RunArtifact, existing.model_artifact_id, populate_existing=True
            )
            if (
                stored_artifact is None
                or registered_model_custody(db, existing, stored_artifact) != custody
                or existing.registry_metadata.get("native_custody_digest")
                != canonical_request_hash(custody)
            ):
                raise StaleFence("Existing registration belongs to another original model")
            return existing
    except (StaleFence, ValueError, KeyError, TypeError, LookupError):
        raise HTTPException(
            409, "Original model custody is missing or changed; controlled qualification required."
        ) from None
    feature_hash = _feature_space_hash(parent)
    selection = RegistrationInspection(
        canonical_request_hash(
            {
                "project_id": project_id,
                "actor_id": user.id,
                "auth_provider": user.auth_provider,
                "request": request.model_dump(mode="json"),
                "selected_run_id": selected_run.id,
                "parent_id": parent.id,
                "artifact_run_id": artifact_run.id,
                "candidate": candidate,
                "feature_space_hash": feature_hash,
                "input": parent.params,
                "primary_metric": parent.tags.get("leaderboard_primary_metric"),
                "dataset_version_id": parent.dataset_version_id,
                "dataset_content_hash": parent.dataset_version.content_hash,
            }
        ),
        model_uri,
        user.token_version,
        custody,
    )
    if inspection is None:
        return selection
    if (
        replace(inspection, byte_size=None, native_digest=None) != selection
        or inspection.native_digest != canonical_request_hash(custody)
        or type(inspection.byte_size) is not int
        or not 0 <= inspection.byte_size <= 2**63 - 1
    ):
        raise HTTPException(409, "The selected model changed during registration. Retry selection.")
    if existing is not None:
        stored_artifact = db.get(RunArtifact, existing.model_artifact_id, populate_existing=True)
        try:
            if (
                stored_artifact is None
                or registered_model_custody(db, existing, stored_artifact) != custody
                or existing.registry_metadata.get("native_custody_digest")
                != inspection.native_digest
            ):
                raise StaleFence("Concurrent registration has no matching verified custody")
        except (StaleFence, ValueError, KeyError, TypeError, LookupError):
            raise HTTPException(
                409, "Existing registration requires controlled qualification."
            ) from None
        return existing
    artifact_size = inspection.byte_size
    artifact = RunArtifact(
        project_id=project_id,
        model_run_id=artifact_run.id,
        kind=ArtifactKind.MODEL_OBJECT,
        name=f"{request.model_name}.joblib",
        object_uri=model_uri,
        content_hash=model_digest,
        byte_size=artifact_size,
        artifact_metadata={
            "model_name": request.model_name,
            "mlflow_run_id": candidate.get("mlflow_run_id"),
            "model_custody": custody,
        },
    )
    db.add(artifact)
    db.flush()

    version_number = (
        int(
            db.scalar(
                select(func.max(ModelRegistryEntry.version)).where(
                    ModelRegistryEntry.project_id == project_id,
                    ModelRegistryEntry.model_name == request.model_name,
                )
            )
            or 0
        )
        + 1
    )
    primary_metric = parent.tags.get("leaderboard_primary_metric")
    metric_value = candidate.get("metrics", {}).get(primary_metric) if primary_metric else None
    entry = ModelRegistryEntry(
        project_id=project_id,
        model_run_id=artifact_run.id,
        model_artifact_id=artifact.id,
        stage=ModelStage.CANDIDATE,
        model_name=request.model_name,
        version=version_number,
        feature_space_hash=feature_hash,
        champion_metric_name=primary_metric,
        champion_metric_value=metric_value,
        registry_metadata={
            "artifact_policy": MODEL_CUSTODY_PROTOCOL,
            "native_custody_digest": inspection.native_digest,
            "desired_state": "artifact_verified",
            "verified_byte_size": artifact_size,
            "verified_at": datetime.now(UTC).isoformat(),
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
    *,
    offset: int = 0,
    limit: int = 100,
) -> list[RegistryEntryRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    entries = db.scalars(
        select(ModelRegistryEntry)
        .where(ModelRegistryEntry.project_id == project_id)
        .order_by(ModelRegistryEntry.created_at.desc(), ModelRegistryEntry.id.desc())
        .offset(offset)
        .limit(limit)
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
            entry.registry_metadata.get("fallback") and target_stage == ModelStage.STAGING
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
    if current_version.id == source.dataset_version_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Drift requires an external dataset version; select a version "
                "different from the model's training dataset."
            ),
        )
    excluded_columns = {
        value for value in (source.target_column, source.params.get("evaluation_column")) if value
    }
    excluded_columns.update(source.params.get("excluded_leakage_columns") or [])
    excluded_columns.update(source.params.get("excluded_columns") or [])
    required_columns = {
        str(item.get("name"))
        for item in source.dataset_version.schema_json.get("columns", [])
        if item.get("name") and item.get("name") not in excluded_columns
    }
    current_columns = {
        str(item.get("name"))
        for item in current_version.schema_json.get("columns", [])
        if item.get("name")
    }
    missing_columns = sorted(required_columns - current_columns)
    if missing_columns:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "External drift columns do not match the training feature space. "
                f"Missing columns: {', '.join(missing_columns)}."
            ),
        )
    linked_deployment = next(
        (
            deployment
            for deployment in db.scalars(
                select(ModelRun)
                .where(
                    ModelRun.project_id == project_id,
                    ModelRun.run_kind == RunKind.DEPLOYMENT,
                )
                .order_by(ModelRun.created_at.desc())
            ).all()
            if deployment.tags.get("registry_entry_id") == str(entry.id)
        ),
        None,
    )
    monitoring_resource_class = str(
        (linked_deployment.params.get("monitoring") or {}).get(
            "resource_class",
            "standard",
        )
        if linked_deployment
        else "standard"
    )
    k8s = client or KubernetesTrainingClient()
    estimate = estimate_analysis_resources(
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
    _apply_monitoring_resource_floor(estimate, monitoring_resource_class)
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
        gpu_requested=False,
        cpu_request_cores=estimate.cpu_request_cores,
        memory_request_mb=estimate.memory_request_mb,
        cpu_limit_cores=estimate.cpu_limit_cores,
        memory_limit_mb=estimate.memory_limit_mb,
        params={
            "registry_entry_id": str(entry.id),
            "deployment_run_id": str(linked_deployment.id) if linked_deployment else None,
            "reference_dataset_version_id": str(source.dataset_version_id),
            "source_training_run_id": str(source.id),
            "excluded_columns": sorted(excluded_columns),
            "evaluation_column": source.params.get("evaluation_column"),
            "max_rows": request.max_rows,
            "monitoring_resource_class": monitoring_resource_class,
        },
        tags={
            "registry_entry_id": str(entry.id),
            **({"deployment_run_id": str(linked_deployment.id)} if linked_deployment else {}),
        },
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
    run.status = RunStatus.QUEUED
    run.tags = {**run.tags, "desired_state": "kubernetes_submission_pending"}
    db.flush()
    return DriftLaunchRead(
        run=ModelRunRead.model_validate(run),
        estimate=estimate,
        manifest=manifest,
    )


def _apply_monitoring_resource_floor(
    estimate: Any,
    resource_class: str,
) -> None:
    cpu_floor, memory_floor = _MONITORING_RESOURCE_FLOORS.get(
        resource_class,
        _MONITORING_RESOURCE_FLOORS["standard"],
    )
    estimate.cpu_request_cores = max(estimate.cpu_request_cores, cpu_floor)
    estimate.cpu_limit_cores = max(estimate.cpu_limit_cores, cpu_floor)
    estimate.memory_request_mb = max(estimate.memory_request_mb, memory_floor)
    estimate.memory_limit_mb = max(estimate.memory_limit_mb, memory_floor)
    blockers = list(estimate.blockers)
    quota_known = estimate.capacity.source == "namespace_resource_quota"
    if quota_known and estimate.cpu_request_cores > estimate.capacity.available_cpu_cores:
        blockers.append(
            f"The {resource_class} monitoring class needs "
            f"{estimate.cpu_request_cores:g} available CPU cores."
        )
    if quota_known and estimate.memory_request_mb > estimate.capacity.available_memory_mb:
        blockers.append(
            f"The {resource_class} monitoring class needs "
            f"{estimate.memory_request_mb} MiB available memory."
        )
    estimate.blockers = list(dict.fromkeys(blockers))
    estimate.can_launch = estimate.can_launch and not estimate.blockers


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
    from automl_api.services import inference_identity
    from automl_api.services.deployment_objects import PROTOCOL as WRITE_PROTOCOL

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
    try:
        custody_digest = canonical_request_hash(
            registered_model_custody(db, entry, entry.model_artifact)
        )
        if entry.registry_metadata.get("native_custody_digest") != custody_digest:
            raise StaleFence("Deployment requires verified original model custody")
    except (StaleFence, ValueError, KeyError, TypeError, LookupError):
        raise HTTPException(
            409, "The registered model requires controlled custody qualification."
        ) from None
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
        gpu_requested=False,
        params={
            "registry_entry_id": str(entry.id),
            "replicas": request.replicas,
            "cpu_request": request.cpu_request,
            "memory_request": memory_request,
        },
        tags={
            "registry_entry_id": str(entry.id),
            "model_custody_digest": custody_digest,
            "deployment_write_protocol": WRITE_PROTOCOL,
            "inference_identity_protocol": INFERENCE_PROTOCOL,
        },
        queued_at=datetime.now(UTC),
    )
    db.add(deployment_run)
    db.flush()
    image = request.image or k8s.settings.inference_image
    if image != k8s.settings.inference_image:
        raise HTTPException(
            status_code=422, detail="Only the operator-approved inference image may be deployed."
        )
    if not entry.model_artifact.content_hash:
        raise HTTPException(status_code=409, detail="The registered model has no integrity digest.")
    try:
        identity_ca = inference_identity.authority()
        identity_control = inference_identity.control_base_url(k8s.settings)
    except (StaleFence, ValueError, OSError):
        raise HTTPException(
            409, "Inference requires its configured TLS authority and HTTPS control endpoint."
        ) from None
    manifests = k8s.build_model_deployment_manifest(
        deployment_id=deployment_run.id,
        project_id=project_id,
        project_name=project.name,
        environment=k8s.settings.environment,
        model_name=entry.model_name,
        model_uri=entry.model_artifact.object_uri,
        model_digest=entry.model_artifact.content_hash,
        identity_ca_digest=identity_ca.digest,
        identity_control_base_url=identity_control,
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
    dockerfile_key = f"projects/{project_id}/deployments/{deployment_run.id}/Dockerfile"
    dockerfile_uri = f"pending://{get_settings().object_store_bucket}/{dockerfile_key}"
    db.add(
        RunArtifact(
            project_id=project_id,
            model_run_id=deployment_run.id,
            kind=ArtifactKind.DEPLOYMENT_IMAGE,
            name="Dockerfile",
            object_uri=dockerfile_uri,
            content_hash=hashlib.sha256(dockerfile.encode("utf-8")).hexdigest(),
            byte_size=len(dockerfile.encode("utf-8")),
            artifact_metadata={
                "registry_entry_id": str(entry.id),
                "base_image": image,
                "artifact_type": "dockerfile",
                "desired_state": "object_write_pending",
                "write_protocol": WRITE_PROTOCOL,
                "content": dockerfile,
            },
        )
    )
    urls = _platform_model_deployment_urls(project_id, deployment_run.id)
    deployment_run.status = RunStatus.QUEUED
    deployment_run.k8s_job_name = deployment_name
    deployment_run.tags = {
        **deployment_run.tags,
        "service_name": service_name,
        "project_name": project.name,
        "environment": k8s.settings.environment,
        **urls,
        "image": image,
        "model_artifact_id": str(entry.model_artifact_id),
        "dockerfile_uri": dockerfile_uri,
        "desired_state": "deployment_pending",
    }
    db.flush()
    return ModelDeploymentLaunchRead(
        run=ModelRunRead.model_validate(deployment_run),
        manifests=manifests,
        dockerfile_uri=dockerfile_uri,
    )


def _internal_model_deployment_urls(
    service_name: str,
    namespace: str,
) -> dict[str, str]:
    base_url = f"http://{service_name}.{namespace}.svc:8080"
    return {
        "internal_endpoint": f"{base_url}/v1/predict",
        "internal_docs_url": f"{base_url}/docs",
        "internal_openapi_url": f"{base_url}/openapi.json",
    }


def _platform_model_deployment_urls(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
) -> dict[str, str]:
    base_url = f"/api/v1/projects/{project_id}/operations/deployments/{run_id}/inference"
    return {
        "platform_endpoint": f"{base_url}/v1/predict",
        "platform_online_endpoint": f"{base_url}/v1/predict/online",
        "platform_offline_endpoint": f"{base_url}/v1/predict/offline",
        "platform_metadata_url": f"{base_url}/v1/metadata",
        "platform_docs_url": f"{base_url}/docs",
        "platform_openapi_url": f"{base_url}/openapi.json",
        "platform_live_url": f"{base_url}/health/live",
        "platform_ready_url": f"{base_url}/health/ready",
    }


@dataclass(frozen=True)
class DeploymentRead:
    request: dict
    request_digest: str
    runtime_state: str | None = None
    urls: dict | None = None


@dataclass(frozen=True)
class DeploymentInspection:
    project_id: uuid.UUID
    actor_id: uuid.UUID
    token_version: int
    reads: tuple[DeploymentRead, ...]


def _deployment_reader(db, user, project_id, inspection):
    expected_version = user.token_version if inspection is None else inspection.token_version
    if inspection is not None and (
        inspection.project_id != project_id or inspection.actor_id != user.id
    ):
        raise HTTPException(409, "Deployment observation belongs to another request")
    project = db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    current = db.get(User, user.id, with_for_update={"read": True}, populate_existing=True)
    if (
        current is None
        or current.is_active is not True
        or current.token_version != expected_version
    ):
        raise HTTPException(401, "Deployment observation authentication changed")
    db.scalar(
        select(ProjectMembership)
        .where(ProjectMembership.project_id == project_id, ProjectMembership.user_id == current.id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    require_project_role(db, current, project_id, ProjectRole.VIEWER)
    if project is None:
        raise HTTPException(404, "Project not found")
    return current


def _deployment_observation_request(db, user, run):
    from automl_api.services.deployment_controls import OPERATION, _digest

    project = db.get(Project, run.project_id, populate_existing=True)
    if (
        project.status != ProjectStatus.ACTIVE
        or project.settings.get("deletion_in_progress")
        or run.tags.get("desired_state") != "deployment_active"
        or run.status not in {RunStatus.RUNNING, RunStatus.SUCCEEDED, RunStatus.FAILED}
    ):
        raise StaleFence("Deployment is not available for active observation")
    custody = lifecycle_custody(db, run)
    if custody.get("retired"):
        raise StaleFence("Deployment was physically retired")
    deployment_model_custody(db, run)
    pointer = run.tags.get(CONTROL) or {}
    control = dict(
        generation=0,
        command_id=None,
        replicas=custody["manifests"]["deployment"]["spec"]["replicas"],
    )
    if pointer:
        command = db.get(
            WorkflowCommand,
            uuid.UUID(pointer["command_id"]),
            with_for_update={"read": True},
            populate_existing=True,
        )
        request = command.request_payload if command is not None else {}
        if (
            pointer.get("state") != "complete"
            or pointer.get("action") != "start"
            or type(pointer.get("generation")) is not int
            or pointer["generation"] < 1
            or command is None
            or command.project_id != run.project_id
            or command.operation != OPERATION
            or command.resource_type != OPERATION
            or command.resource_id != run.id
            or command.status != CommandStatus.SUCCEEDED
            or command.request_hash != canonical_request_hash(request)
            or request.get("protocol") != OPERATION
            or request.get("actor_id") != str(command.actor_id)
            or request.get("run_id") != str(run.id)
            or request.get("generation") != pointer["generation"]
            or request.get("action") != "start"
            or request.get("custody_digest") != _digest(custody)
            or type(request.get("replicas")) is not int
            or request["replicas"] != max(1, int(run.params.get("replicas", 1)))
        ):
            raise StaleFence("Deployment observation lost its original completed control")
        control = dict(
            generation=pointer["generation"],
            command_id=str(command.id),
            replicas=request["replicas"],
        )
    snapshot = {
        key: getattr(run, key)
        for key in (
            "project_id",
            "created_by_id",
            "run_kind",
            "status",
            "params",
            "tags",
            "k8s_job_name",
            "k8s_namespace",
            "failure_code",
            "failure_message",
            "finished_at",
        )
    }
    # Freeze JSON now; SQLAlchemy JSON values can otherwise share mutable references.
    request = dict(
        run_id=str(run.id),
        actor_id=str(user.id),
        token_version=user.token_version,
        snapshot=snapshot,
        custody=custody,
        control=control,
    )
    from copy import deepcopy

    return deepcopy(request)


def inspect_model_deployments(inspection, client=None):
    deadline = time.monotonic() + 30
    reads = []
    try:
        k8s = client or KubernetesTrainingClient()
    except Exception:
        return replace(
            inspection,
            reads=tuple(
                replace(read, runtime_state="unavailable", urls={}) for read in inspection.reads
            ),
        )
    for read in inspection.reads:
        try:
            if canonical_request_hash(read.request) != read.request_digest:
                raise StaleFence("Deployment observation request changed")
            state, urls = k8s.observe_model_deployment(
                read.request["snapshot"]["k8s_job_name"],
                custody=read.request["custody"],
                control=read.request["control"],
                deadline=deadline,
            )
            if state not in {
                "missing",
                "ready",
                "pending",
                "progressing",
                "image_pull_error",
                "crash_loop",
                "configuration_error",
                "out_of_memory",
            }:
                raise ValueError("Unknown deployment runtime state")
            reads.append(replace(read, runtime_state=state, urls=urls))
        except Exception as exc:
            logger.debug("Deployment observation unavailable: %s", type(exc).__name__)
            reads.append(replace(read, runtime_state="unavailable", urls={}))
    return replace(inspection, reads=tuple(reads))


def _publish_deployment_observation(run, state, urls):
    failure_details = {
        "missing": ("KUBERNETES_DEPLOYMENT_MISSING", "The Kubernetes deployment no longer exists."),
        "image_pull_error": (
            "INFERENCE_IMAGE_PULL_FAILED",
            "Kubernetes could not pull the inference image. Publish it to "
            "an accessible registry or import it into the local cluster.",
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
    revision = (run.tags.get("deployment_observation") or {}).get("version", 0)
    if type(revision) is not int or revision < 0:
        raise StaleFence("Deployment observation revision changed")
    tags = {
        key: value
        for key, value in run.tags.items()
        if key not in {"endpoint", "base_url", "docs_url", "openapi_url"}
    }
    tags["deployment_observation"] = dict(
        version=revision + 1, runtime_state=state, observed_at=datetime.now(UTC).isoformat()
    )
    if state == "ready":
        run.status = RunStatus.SUCCEEDED
        run.failure_code = run.failure_message = None
        run.finished_at = None
        tags.update(urls)
    elif state in failure_details:
        run.status = RunStatus.FAILED
        run.failure_code, run.failure_message = failure_details[state]
        run.finished_at = datetime.now(UTC)
    run.tags = tags


def list_model_deployments(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
    *,
    offset: int = 0,
    limit: int = 100,
    inspection: DeploymentInspection | None = None,
) -> list[DeploymentStatusRead] | DeploymentInspection:
    current_user = _deployment_reader(db, user, project_id, inspection)
    runs = db.scalars(
        select(ModelRun)
        .where(ModelRun.project_id == project_id, ModelRun.run_kind == RunKind.DEPLOYMENT)
        .order_by(ModelRun.created_at.desc(), ModelRun.id.desc())
        .offset(offset)
        .limit(limit)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).all()
    captured = {read.request["run_id"]: read for read in inspection.reads} if inspection else {}
    if inspection and len(captured) != len(inspection.reads):
        raise HTTPException(409, "Deployment observation repeats a selected run")
    reads, result = [], []
    for run in runs:
        pending = {
            "deployment_start_pending": "starting",
            "deployment_stop_pending": "stopping",
            "deployment_cleanup_pending": "cleaning",
        }.get(run.tags.get("desired_state"))
        if pending and (run.tags.get(CONTROL) or {}).get("state") == "failed":
            pending = "unavailable"
        submission = (
            run.status in {RunStatus.PRECHECK_RUNNING, RunStatus.QUEUED}
            and run.tags.get("desired_state") == "deployment_pending"
        )
        state = pending or (
            "progressing"
            if submission
            else ("cleaned" if run.tags.get("runtime_cleaned_at") else "stopped")
            if run.status == RunStatus.CANCELLED
            else "unavailable"
        )
        if (
            run.k8s_job_name
            and run.status != RunStatus.CANCELLED
            and not pending
            and not submission
        ):
            try:
                request = _deployment_observation_request(db, current_user, run)
                digest = canonical_request_hash(request)
                if inspection is None:
                    reads.append(DeploymentRead(request=request, request_digest=digest))
                else:
                    observed = captured.get(str(run.id))
                    if (
                        observed is not None
                        and observed.request_digest
                        == digest
                        == canonical_request_hash(observed.request)
                        and observed.runtime_state is not None
                    ):
                        state = observed.runtime_state
                        _publish_deployment_observation(run, state, observed.urls or {})
            except (StaleFence, ValueError, KeyError, TypeError, LookupError):
                state = "unavailable"
        service_name = run.tags.get("service_name") or run.k8s_job_name
        namespace = run.k8s_namespace
        internal_urls = (
            _internal_model_deployment_urls(service_name, namespace)
            if state == "ready" and service_name and namespace
            and run.tags.get("inference_identity_protocol") != INFERENCE_PROTOCOL
            else {}
        )
        platform_urls = (
            _platform_model_deployment_urls(project_id, run.id) if state == "ready" else {}
        )
        result.append(
            DeploymentStatusRead(
                run=ModelRunRead.model_validate(run),
                runtime_state=state,
                service_name=service_name,
                namespace=namespace,
                **{
                    key: run.tags.get(key) if state == "ready" else None
                    for key in ("endpoint", "base_url", "docs_url", "openapi_url")
                },
                **internal_urls,
                **platform_urls,
                status=run.status,
            )
        )
    if reads:
        return DeploymentInspection(
            project_id, current_user.id, current_user.token_version, tuple(reads)
        )
    db.flush()
    return result


def list_drift_runs(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
    *,
    offset: int = 0,
    limit: int = 100,
) -> list[ModelRun]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.run_kind == RunKind.DRIFT,
            )
            .order_by(ModelRun.created_at.desc(), ModelRun.id.desc())
            .offset(offset)
            .limit(limit)
        ).all()
    )
    k8s = client or KubernetesTrainingClient()
    for run in runs:
        if run.k8s_job_name and run.status in {
            RunStatus.QUEUED,
            RunStatus.PRECHECK_RUNNING,
            RunStatus.RUNNING,
        }:
            try:
                _sync_run_status(db, run, k8s)
            except ApiException:
                break
    return runs


def _locked_deployment(
    db: Session, user: User, project_id: uuid.UUID, run_id: uuid.UUID
) -> ModelRun:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    project = db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    if project is None:
        raise HTTPException(404, "Project not found.")
    if isinstance(project.settings, dict) and project.settings.get("deletion_in_progress"):
        raise HTTPException(409, "Deletion is in progress.")
    run = db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found.")
    return run


def stop_model_deployment(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    run = _locked_deployment(db, user, project_id, run_id)
    pointer = run.tags.get(CONTROL) or {}
    retry_failed = pointer.get("action") == "stop" and pointer.get("state") == "failed"
    if run.status == RunStatus.CANCELLED and not retry_failed:
        return run
    if run.k8s_job_name:
        try:
            custody = lifecycle_custody(db, run, allow_unstarted=True)
            if not custody.get("unstarted"):
                queue_control(db, user, run, "stop", custody)
                return run
        except (ValueError, KeyError, TypeError, LookupError):
            raise HTTPException(
                409, "The deployment requires controlled native custody qualification."
            ) from None
    run.status = RunStatus.CANCELLED
    run.finished_at = datetime.now(UTC)
    run.tags = {**run.tags, "desired_state": "deployment_stopped"}
    db.flush()
    return run


def start_model_deployment(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    run = _locked_deployment(db, user, project_id, run_id)
    if (run.tags.get(CONTROL) or {}).get("action") == "cleanup":
        raise HTTPException(409, "Cleanup was admitted; deploy the saved model again.")
    pointer = run.tags.get(CONTROL) or {}
    retry_failed = pointer.get("action") == "start" and pointer.get("state") == "failed"
    if (
        run.status in {RunStatus.RUNNING, RunStatus.SUCCEEDED, RunStatus.QUEUED}
        and not retry_failed
    ):
        return run
    if run.status != RunStatus.CANCELLED and not retry_failed:
        raise HTTPException(
            status_code=409, detail="Shut down this deployment before starting it again."
        )
    if run.tags.get("runtime_cleaned_at") or not run.k8s_job_name:
        raise HTTPException(
            status_code=409,
            detail=(
                "Runtime resources were cleaned up. Deploy the saved model again from the registry."
            ),
        )
    registry_id = run.tags.get("registry_entry_id")
    if registry_id:
        _lock_training_admission(db)
        active = db.scalar(
            select(ModelRun.id)
            .where(
                ModelRun.project_id == project_id,
                ModelRun.id != run.id,
                ModelRun.run_kind == RunKind.DEPLOYMENT,
                ModelRun.tags["registry_entry_id"].astext == str(registry_id),
                ModelRun.status.in_(
                    [
                        RunStatus.QUEUED,
                        RunStatus.PRECHECK_RUNNING,
                        RunStatus.RUNNING,
                        RunStatus.SUCCEEDED,
                    ]
                ),
            )
            .limit(1)
        )
        if active:
            raise HTTPException(
                status_code=409,
                detail=(
                    "This model version already has an active deployment. "
                    "Shut it down before starting this one."
                ),
            )
    try:
        deployment_model_custody(db, run)
    except (StaleFence, ValueError, KeyError, TypeError, LookupError):
        raise HTTPException(
            409, "The deployment requires controlled model custody qualification."
        ) from None
    try:
        queue_control(db, user, run, "start", lifecycle_custody(db, run))
    except (ValueError, KeyError, TypeError, LookupError):
        raise HTTPException(
            409, "The deployment requires controlled native custody qualification."
        ) from None
    return run


def cleanup_model_deployment(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    client: KubernetesTrainingClient | None = None,
) -> ModelRun:
    run = _locked_deployment(db, user, project_id, run_id)
    if run.status != RunStatus.CANCELLED:
        raise HTTPException(status_code=409, detail="Shut down this deployment before cleanup.")
    try:
        custody = lifecycle_custody(db, run, allow_unstarted=True)
        if run.tags.get("runtime_cleaned_at") and custody.get("retired") is True:
            return run
        if not custody.get("unstarted"):
            queue_control(db, user, run, "cleanup", custody)
            return run
    except (ValueError, KeyError, TypeError, LookupError):
        raise HTTPException(
            409, "The deployment requires controlled native custody qualification."
        ) from None
    run.tags = {
        **run.tags,
        "desired_state": "deployment_cleaned",
        "runtime_cleaned_at": datetime.now(UTC).isoformat(),
    }
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
    *,
    execute_side_effects: bool = True,
) -> ArtifactCleanupRead:
    from automl_api.services.deployment_objects import unretired_deployment_write

    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    db.get(Project, project_id, with_for_update=True, populate_existing=True)
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
            and not unretired_deployment_write(artifact)
            and artifact.id not in active_artifact_ids
            and artifact.model_run_id not in active_deployment_run_ids
        )
    ]
    deleted_uris: list[str] = []
    errors: list[str] = []
    if not request.dry_run and not execute_side_effects:
        for artifact in candidates:
            tombstone = DeletionTombstone(
                project_id=project_id,
                requested_by_id=user.id,
                resource_type="run_artifact",
                resource_id=artifact.id,
                reason=f"artifact cleanup older than {request.older_than_days} days",
            )
            db.add(tombstone)
            db.flush()
            db.add(
                DeletionStage(
                    project_id=project_id,
                    tombstone_id=tombstone.id,
                    resource_class="object_store",
                )
            )
    elif not request.dry_run:
        store = get_object_store()
        for artifact in candidates:
            try:
                store.delete(artifact.object_uri)
                deleted_uris.append(artifact.object_uri)
                db.delete(artifact)
            except Exception as exc:
                errors.append(f"{artifact.object_uri}: {exc}")
    deleted_jobs: list[str] = []
    if not request.dry_run and request.cleanup_finished_jobs and execute_side_effects:
        try:
            deleted_jobs = (client or KubernetesTrainingClient()).cleanup_finished_jobs(project_id)
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
        training_dataset_version_id=entry.model_run.dataset_version_id,
        training_feature_columns=[
            str(item.get("name"))
            for item in entry.model_run.dataset_version.schema_json.get("columns", [])
            if item.get("name")
            and item.get("name")
            not in {
                entry.model_run.target_column,
                entry.model_run.params.get("evaluation_column"),
                *list(entry.model_run.params.get("excluded_leakage_columns") or []),
                *list(entry.model_run.params.get("excluded_columns") or []),
            }
        ],
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
        .execution_options(populate_existing=True)
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
        f"      {json.dumps(name)}={json.dumps(value)}" for name, value in labels.items()
    )
    return (
        f"FROM {base_image}\n"
        f"LABEL {label_text}\n"
        f"ENV MODEL_URI={json.dumps(model_uri)}\n"
        f"ENV MODEL_NAME={json.dumps(model_name)}\n"
        f"ENV PROJECT_NAME={json.dumps(project_name)}\n"
        f"ENV DEPLOYMENT_ENVIRONMENT={json.dumps(environment)}\n"
        "EXPOSE 8080 8443\n"
        'CMD ["python", "-m", "automl_api.inference.runtime"]\n'
    )
