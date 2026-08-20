from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from automl_api.models.enums import TaskType


class RevisionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=220)
    specification: dict[str, Any]
    expected_revision: int | None = Field(default=None, ge=0)


class FeatureContractCreate(RevisionCreate):
    task_type: TaskType
    target_column: str = Field(min_length=1, max_length=255)


class SearchSpaceCreate(RevisionCreate):
    metric_name: str = Field(min_length=1, max_length=160)
    metric_direction: Literal["maximize", "minimize"]


class ExperimentSpecCreate(RevisionCreate):
    dataset_version_id: uuid.UUID
    split_revision_id: uuid.UUID
    feature_contract_revision_id: uuid.UUID
    feature_search_space_revision_id: uuid.UUID
    search_objective_revision_id: uuid.UUID
    catalog_revision_id: uuid.UUID
    task_type: TaskType
    target_column: str | None = Field(default=None, max_length=255)
    primary_metric: str = Field(min_length=1, max_length=160)


class RevisionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    revision: int
    digest_algorithm: str
    digest_scope: str
    content_digest: str
    specification: dict[str, Any]
    created_at: datetime


class EvaluationScopeCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scope_key: str = Field(min_length=1, max_length=255)
    split_revision_id: uuid.UUID
    experiment_spec_revision_id: uuid.UUID
    canonical_provider: str = Field(min_length=1, max_length=32)
    expected_member_count: int = Field(ge=1, le=100)
    mode: Literal["promotional", "validation_only"]
    membership_deadline_at: datetime
    comparison_policy: dict[str, Any] = Field(default_factory=dict)
    final_threshold_revision: str | None = Field(default=None, max_length=128)


class EvaluationScopeMemberCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: uuid.UUID


class EvaluationScopeRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    scope_key: str
    split_revision_id: uuid.UUID
    experiment_spec_revision_id: uuid.UUID | None
    canonical_provider: str
    mode: str
    expected_members: int
    status: str
    membership_digest: str | None
    membership_deadline_at: datetime | None
    scope_started_at: datetime | None
    scope_deadline_at: datetime | None
    comparison_policy: dict[str, Any]
    final_threshold_revision: str | None
    created_at: datetime


class CatalogEstimatorRead(BaseModel):
    name: str
    source: str
    status: str
    reason: str | None = None
    recipe_id: str
    resource_class: str
    sampling_policy: str
    incremental: bool
    serializable: bool
    inference: bool
    deprecated: bool


class CatalogRead(BaseModel):
    catalog_revision: str
    task_type: TaskType
    generated_at: datetime
    runtime_lock_digest: str
    signature_verified: bool
    estimators: list[CatalogEstimatorRead]


class CapabilitiesRead(BaseModel):
    auth_modes: list[str]
    upload_protocols: list[str]
    task_types: list[TaskType]
    active_catalog_revisions: dict[str, str]
    max_qualified_concurrency: int
    environment_qualified: bool
    deployment_target: str
    upload_data_region: str
    upload_storage_driver: str


class CursorPage(BaseModel):
    items: list[dict[str, Any]]
    next_cursor: str | None = None
