from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field

from automl_api.models.enums import TaskType


class ProfileRequest(BaseModel):
    target_column: str | None = Field(default=None, max_length=255)


class TaskInferenceRead(BaseModel):
    task_type: TaskType
    target_column: str | None = None
    confidence: float
    rationale: str
    requires_confirmation: bool = True


class ColumnProfileRead(BaseModel):
    name: str
    semantic_type: str
    missing_count: int
    missing_ratio: float
    distinct_count: int
    sample_values: list[str]
    statistics: dict[str, Any]
    distribution_type: str
    distribution: list[dict[str, Any]]
    quality_flags: list[str]


class FeatureRelationshipRead(BaseModel):
    source_column: str
    target_column: str
    method: str
    value: float


class PreparationStepRead(BaseModel):
    column: str
    action: str
    strategy: str
    reason: str


class LeakageFindingRead(BaseModel):
    column: str
    kind: str
    severity: str
    confidence: float
    reason: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    auto_excluded: bool = False


class LeakageAnalysisRead(BaseModel):
    status: str
    target_column: str | None = None
    analyzed_rows: int = 0
    duplicate_row_count: int = 0
    duplicate_row_ratio: float = 0.0
    findings: list[LeakageFindingRead] = Field(default_factory=list)
    excluded_columns: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DatasetProfileRead(BaseModel):
    project_id: uuid.UUID
    dataset_id: uuid.UUID
    dataset_version_id: uuid.UUID
    row_count_analyzed: int
    column_count: int
    target_column: str | None
    task_inference: TaskInferenceRead
    columns: list[ColumnProfileRead]
    relationships: list[FeatureRelationshipRead]
    preparation_plan: list[PreparationStepRead]
    leakage_analysis: LeakageAnalysisRead
    warnings: list[str]
