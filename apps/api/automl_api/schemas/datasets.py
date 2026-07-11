from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from automl_api.models.enums import DatasetFormat, DatasetStatus, ObjectStoreType


class DatasetUploadRequest(BaseModel):
    dataset_name: str = Field(min_length=1, max_length=220)
    description: str | None = None
    filename: str = Field(min_length=1, max_length=512)
    tags: dict[str, Any] = Field(default_factory=dict)

    @field_validator("dataset_name", "filename")
    @classmethod
    def strip_text(cls, value: str) -> str:
        return value.strip()


class DatasetRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    created_by_id: uuid.UUID
    name: str
    description: str | None = None
    latest_version_number: int
    tags: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class DatasetVersionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    dataset_id: uuid.UUID
    created_by_id: uuid.UUID
    version_number: int
    status: DatasetStatus
    format: DatasetFormat
    object_store_type: ObjectStoreType
    object_uri: str
    original_filename: str | None = None
    content_hash: str
    byte_size: int | None = None
    row_count: int | None = None
    column_count: int | None = None
    dataset_schema: dict[str, Any] = Field(alias="schema_json")
    inferred_types_json: dict[str, Any]
    quality_report_json: dict[str, Any]
    profile_artifact_uri: str | None = None
    created_at: datetime
    updated_at: datetime


class DatasetUploadResponse(BaseModel):
    dataset: DatasetRead
    version: DatasetVersionRead
    profiling_job_id: uuid.UUID | None = None
    profiling_job_status: str | None = None
