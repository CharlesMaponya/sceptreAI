from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from automl_api.models.enums import RunKind, RunStatus, TaskType


class TrainingEstimateRequest(BaseModel):
    dataset_version_id: uuid.UUID
    target_column: str | None = Field(default=None, max_length=255)
    evaluation_column: str | None = Field(default=None, max_length=255)
    task_type: TaskType
    prefer_gpu: bool = True
    expected_minutes: int = Field(default=10, ge=1, le=120)
    candidate_limit: int = Field(default=5, ge=1, le=20)
    candidate_models: list[str] = Field(default_factory=list, max_length=20)
    optimization_iterations: int = Field(default=5, ge=1, le=25)
    cv_folds: int = Field(default=3, ge=2, le=5)


class TrainingLaunchRequest(TrainingEstimateRequest):
    run_name: str | None = Field(default=None, max_length=255)
    params: dict[str, Any] = Field(default_factory=dict)


class TrainingAddModelsRequest(BaseModel):
    candidate_models: list[str] = Field(min_length=1, max_length=20)
    optimization_iterations: int = Field(default=5, ge=1, le=25)
    cv_folds: int = Field(default=3, ge=2, le=5)
    expected_minutes: int = Field(default=10, ge=1, le=120)
    prefer_gpu: bool = True


class ClusterCapacityRead(BaseModel):
    connected: bool
    source: str
    total_cpu_cores: float
    requested_cpu_cores: float
    used_cpu_cores: float = 0
    available_cpu_cores: float
    total_memory_mb: int
    requested_memory_mb: int
    used_memory_mb: int = 0
    available_memory_mb: int
    ready_nodes: int
    gpu_available: bool
    active_training_jobs: int
    warnings: list[str] = Field(default_factory=list)


class TrainingEstimateRead(BaseModel):
    capacity: ClusterCapacityRead
    estimated_working_set_mb: int
    cpu_request_cores: float
    cpu_limit_cores: float
    memory_request_mb: int
    memory_limit_mb: int
    gpu_requested: bool
    gpu_fallback_reason: str | None = None
    gpu_vendor: str | None = None
    gpu_resource: str | None = None
    selected_node: str | None = None
    expected_minutes: int
    active_deadline_seconds: int
    estimated_core_hours: float
    max_concurrent_jobs: int
    can_launch: bool
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class ModelRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    created_by_id: uuid.UUID
    run_kind: RunKind
    status: RunStatus
    task_type: TaskType
    target_column: str | None
    run_name: str | None
    pipeline_name: str | None
    mlflow_run_id: str | None
    zenml_run_id: str | None
    k8s_namespace: str | None
    k8s_job_name: str | None
    gpu_requested: bool
    cpu_request_cores: float | None
    memory_request_mb: int | None
    cpu_limit_cores: float | None
    memory_limit_mb: int | None
    estimated_core_hours: float | None
    params: dict[str, Any]
    tags: dict[str, Any]
    failure_code: str | None
    failure_message: str | None
    plain_english_failure: str | None
    queued_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


class TrainingLaunchRead(BaseModel):
    run: ModelRunRead
    estimate: TrainingEstimateRead
    manifest: dict[str, Any]


class TrainingLogsRead(BaseModel):
    run_id: uuid.UUID
    status: RunStatus
    lines: list[str]


class TrainingResourceUsageRead(BaseModel):
    run_id: uuid.UUID
    status: RunStatus
    pod_name: str | None = None
    pod_phase: str | None = None
    node_name: str | None = None
    current_candidate: str | None = None
    last_candidate: str | None = None
    current_phase: str | None = None
    completed_candidates: int = 0
    total_candidates: int = 0
    progress: float = 0
    elapsed_seconds: float = 0
    estimated_remaining_seconds: float | None = None
    cpu_request_cores: float | None = None
    cpu_limit_cores: float | None = None
    cpu_usage_cores: float | None = None
    peak_cpu_usage_cores: float | None = None
    memory_request_mb: int | None = None
    memory_limit_mb: int | None = None
    memory_usage_mb: int | None = None
    peak_memory_usage_mb: int | None = None
    gpu_requested: bool = False
    gpu_vendor: str | None = None
    gpu_resource: str | None = None
    gpu_count: int = 0
    gpu_utilization_percent: float | None = None
    gpu_memory_used_mb: int | None = None
    gpu_memory_total_mb: int | None = None
    gpu_telemetry_available: bool = False
    telemetry_available: bool = False
    restart_count: int = 0
    status_reason: str | None = None
    sampled_at: datetime


class LeaderboardEntryRead(BaseModel):
    rank: int | None
    model: str
    status: str
    cost_tier: str = "unknown"
    primary_score: float | None
    metrics: dict[str, float] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    best_params: dict[str, Any] = Field(default_factory=dict)
    duration_seconds: float | None
    error: str | None
    mlflow_run_id: str | None = None
    extension_run_id: uuid.UUID | None = None


class TrainingLeaderboardRead(BaseModel):
    run_id: uuid.UUID
    status: RunStatus
    primary_metric: str | None
    winner: str | None
    metric_directions: dict[str, str] = Field(default_factory=dict)
    entries: list[LeaderboardEntryRead] = Field(default_factory=list)


class EstimatorRead(BaseModel):
    name: str
    task_type: TaskType
    mixin: str
    tunable: bool
    cost_tier: str
    default_selected: bool
