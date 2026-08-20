from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

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
    content_hash_algorithm: str = "sha256"
    content_hash_scope: str = "byte_stream"
    content_hash_verification_status: str = "verified_legacy"
    content_hash_verified_at: datetime | None = None
    data_region: str = "legacy"
    sensitivity: str = "internal"
    retention_until: datetime | None = None
    legal_hold: bool = False
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


class UploadCapabilitiesRead(BaseModel):
    protocol: Literal["multipart", "resumable_offset", "block_list", "single_put"]
    parallel_chunks_per_object: int = Field(ge=1)
    can_list_committed_units: bool
    supports_unit_checksum: bool
    supports_final_checksum: bool
    supports_provider_lifecycle: bool
    supports_abort: bool


class ResumableUploadBeginRequest(BaseModel):
    upload_kind: Literal["dataset", "validation", "drift", "offline_scoring"] = "dataset"
    dataset_name: str = Field(min_length=1, max_length=220)
    description: str | None = None
    filename: str = Field(min_length=1, max_length=512)
    byte_size: int = Field(gt=0)
    content_type: str = Field(min_length=1, max_length=255)
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    sensitivity: Literal["public", "internal", "confidential", "restricted"]
    data_region: str = Field(min_length=2, max_length=64, pattern=r"^[a-zA-Z0-9._-]+$")
    retention_days: int | None = Field(default=None, ge=1, le=3650)
    legal_hold: bool = False
    tags: dict[str, Any] = Field(default_factory=dict)
    target_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("dataset_name", "filename", "content_type", "data_region")
    @classmethod
    def normalize_upload_text(cls, value: str) -> str:
        return value.strip()

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        return value.lower()


class TransferCursorRead(BaseModel):
    unit_number: int = Field(ge=1)
    offset: int = Field(ge=0)
    length: int = Field(gt=0)
    checksum_sha256: str | None = Field(default=None, pattern=r"^[a-fA-F0-9]{64}$")


class TransferReceiptRead(BaseModel):
    unit_number: int = Field(ge=1)
    offset: int = Field(ge=0)
    length: int = Field(gt=0)
    etag: str | None = Field(default=None, max_length=512)
    checksum_sha256: str | None = Field(default=None, max_length=256)
    provider_headers: dict[str, str] = Field(default_factory=dict)


class ResumableUploadRead(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    upload_kind: str
    status: str
    provider_driver: str
    protocol: str
    byte_size: int
    part_size: int
    total_parts: int
    confirmed_bytes: int
    resume_key: str
    expires_at: datetime
    original_filename: str
    expected_object_digest: str | None
    sensitivity: str
    data_region: str
    legal_hold: bool
    capabilities: UploadCapabilitiesRead
    next_cursor: TransferCursorRead | None = None


class TransferInstructionRequest(BaseModel):
    cursor: TransferCursorRead


class TransferInstructionRead(BaseModel):
    instruction_id: str
    method: Literal["PUT", "POST"]
    url: str
    headers: dict[str, str]
    cursor: TransferCursorRead
    expires_at: datetime
    expose_response_headers: list[str]
    enforced_controls: list[str]
    unsupported_controls: list[str]


class UploadProgressRead(BaseModel):
    session_id: uuid.UUID
    status: str
    confirmed_bytes: int
    total_bytes: int
    receipts: list[TransferReceiptRead]
    next_cursor: TransferCursorRead | None
    expires_at: datetime
    complete: bool


class CompleteUploadRequest(BaseModel):
    sha256: str = Field(pattern=r"^[a-fA-F0-9]{64}$")
    receipts: list[TransferReceiptRead] = Field(default_factory=list)

    @field_validator("sha256")
    @classmethod
    def normalize_complete_sha256(cls, value: str) -> str:
        return value.lower()


class UploadCompletionRead(BaseModel):
    session: ResumableUploadRead
    dataset: DatasetRead | None = None
    version: DatasetVersionRead | None = None


class AbortUploadRead(BaseModel):
    id: uuid.UUID
    status: Literal["aborted"]
    aborted_at: datetime
