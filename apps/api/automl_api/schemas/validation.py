from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from automl_api.models.enums import ArtifactKind, RunStatus
from automl_api.schemas.training import ModelRunRead, TrainingEstimateRead


class ValidationLaunchRequest(BaseModel):
    model_name: str = Field(min_length=1, max_length=220)
    dataset_version_id: uuid.UUID
    evaluation_column: str | None = Field(default=None, max_length=255)
    expected_minutes: int = Field(default=5, ge=1, le=30)


class ExplainabilityLaunchRequest(BaseModel):
    model_name: str = Field(min_length=1, max_length=220)
    max_rows: int = Field(default=200, ge=20, le=1000)
    expected_minutes: int = Field(default=10, ge=1, le=30)


class AnalysisLaunchRead(BaseModel):
    run: ModelRunRead
    estimate: TrainingEstimateRead | None = None
    manifest: dict[str, Any] = Field(default_factory=dict)
    cached: bool = False


class RunArtifactRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    model_run_id: uuid.UUID
    kind: ArtifactKind
    name: str
    object_uri: str
    content_hash: str | None
    byte_size: int | None
    artifact_metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class AnalysisResultRead(BaseModel):
    run_id: uuid.UUID
    status: RunStatus
    model_name: str
    metrics: dict[str, float] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    feature_importance: list[dict[str, Any]] = Field(default_factory=list)
    artifacts: list[RunArtifactRead] = Field(default_factory=list)
