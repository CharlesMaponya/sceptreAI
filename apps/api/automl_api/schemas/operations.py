from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from automl_api.models.enums import ModelStage, RunStatus
from automl_api.schemas.training import (
    ClusterCapacityRead,
    ModelRunRead,
    TrainingEstimateRead,
)


class RegistryCreateRequest(BaseModel):
    training_run_id: uuid.UUID
    model_name: str = Field(min_length=1, max_length=220)


class RegistryStageUpdateRequest(BaseModel):
    stage: ModelStage


class RegistryEntryRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    model_run_id: uuid.UUID
    model_artifact_id: uuid.UUID
    stage: ModelStage
    model_name: str
    version: int
    feature_space_hash: str
    champion_metric_name: str | None
    champion_metric_value: float | None
    promoted_at: datetime | None
    retired_at: datetime | None
    registry_metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    model_artifact_uri: str
    is_fallback: bool = False
    training_dataset_version_id: uuid.UUID
    training_feature_columns: list[str] = Field(default_factory=list)


class DriftLaunchRequest(BaseModel):
    dataset_version_id: uuid.UUID
    max_rows: int = Field(default=10_000, ge=100, le=100_000)
    expected_minutes: int = Field(default=10, ge=1, le=120)


class DriftLaunchRead(BaseModel):
    run: ModelRunRead
    estimate: TrainingEstimateRead
    manifest: dict[str, Any]


class ModelDeploymentRequest(BaseModel):
    image: str | None = Field(
        default=None,
        max_length=512,
        pattern=r"^[A-Za-z0-9._:/@-]+$",
    )
    replicas: int = Field(default=1, ge=1, le=10)
    cpu_request: str = Field(default="500m", pattern=r"^[0-9]+m$|^[0-9]+(?:\.[0-9]+)?$")
    memory_request: str = Field(default="1Gi", pattern=r"^[0-9]+(?:Mi|Gi)$")


class ModelDeploymentLaunchRead(BaseModel):
    run: ModelRunRead
    manifests: dict[str, Any]
    dockerfile_uri: str


class PlatformHealthRead(BaseModel):
    capacity: ClusterCapacityRead
    components: dict[str, str]
    active_deployments: int


class ArtifactCleanupRequest(BaseModel):
    older_than_days: int = Field(default=30, ge=1, le=3650)
    dry_run: bool = True
    cleanup_finished_jobs: bool = True


class ArtifactCleanupRead(BaseModel):
    dry_run: bool
    artifact_count: int
    artifact_bytes: int
    artifact_ids: list[uuid.UUID]
    deleted_object_uris: list[str]
    deleted_kubernetes_jobs: list[str]
    errors: list[str]


class DeploymentStatusRead(BaseModel):
    run: ModelRunRead
    runtime_state: str
    service_name: str | None = None
    namespace: str | None = None
    endpoint: str | None = None
    base_url: str | None = None
    docs_url: str | None = None
    openapi_url: str | None = None
    internal_endpoint: str | None = None
    internal_docs_url: str | None = None
    internal_openapi_url: str | None = None
    status: RunStatus
