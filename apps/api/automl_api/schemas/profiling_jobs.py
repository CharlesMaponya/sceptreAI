from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ProfilingJobCreate(BaseModel):
    target_column: str | None = Field(default=None, max_length=255)
    force: bool = False


class ProfilingJobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    dataset_id: uuid.UUID
    dataset_version_id: uuid.UUID
    target_column: str | None
    status: str
    current_stage: str
    progress: float
    total_columns: int
    completed_columns: int
    row_count: int | None
    overview_json: dict[str, Any]
    feature_profiles_json: dict[str, Any]
    relationships_json: list[dict[str, Any]]
    preparation_json: list[dict[str, Any]]
    warnings_json: list[str]
    artifact_uris_json: dict[str, Any]
    failure_message: str | None
    auto_started: bool
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ProfilingJobStatusRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    dataset_id: uuid.UUID
    dataset_version_id: uuid.UUID
    target_column: str | None
    status: str
    current_stage: str
    progress: float
    total_columns: int
    completed_columns: int
    row_count: int | None
    overview_json: dict[str, Any]
    available_features: list[str]
    warnings_json: list[str]
    artifact_uris_json: dict[str, Any]
    failure_message: str | None
    auto_started: bool
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


class FeatureProfileJobRead(BaseModel):
    job_id: uuid.UUID
    column: str
    status: str
    profile: dict[str, Any] | None = None
