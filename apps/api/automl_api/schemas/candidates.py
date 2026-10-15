from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class CandidateCheckpointRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    sequence: int
    content_digest: str
    event_sequence: int


class CandidateReceiptRead(BaseModel):
    state: Literal["missing", "inconsistent", "verified"]
    result_digest: str | None = None
    result: dict[str, Any] | None = None
    failure: str | None = None


class CandidateAttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    stage: str
    trial_id: uuid.UUID | None
    run_attempt_id: uuid.UUID | None
    predecessor_attempt_id: uuid.UUID | None
    generation: int
    status: str
    ray_job_name: str | None
    ray_cluster_name: str | None
    ray_submission_id: str | None
    heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    retry_count: int
    retry_budget: int
    terminal_reason: str | None
    terminal_cas_version: int
    cleanup_state: str
    checkpoints: list[CandidateCheckpointRead] = Field(default_factory=list)
    ray_trial_ids: list[str] = Field(default_factory=list)
    receipt: CandidateReceiptRead | None = None


class CandidateTrialRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    suggestion_id: str
    status: str
    params_digest: str
    parameters: dict[str, Any]
    result_digest: str | None
    attempts: list[CandidateAttemptRead] = Field(default_factory=list)


class CandidateDetailRead(BaseModel):
    model_config = ConfigDict(from_attributes=True, allow_inf_nan=False)
    id: uuid.UUID
    project_id: uuid.UUID
    model_run_id: uuid.UUID
    candidate_key: str
    estimator_key: str
    catalog_revision_id: uuid.UUID
    feature_recipe_revision_id: uuid.UUID
    status: str
    params: dict[str, Any]
    parameters: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    run_status: str
    lineage: dict[str, uuid.UUID | None]
    lineage_state: Literal["recorded", "incomplete", "inconsistent"]
    resources: dict[str, str | float | int | bool | None]
    sample_tier: dict[str, str | int | None]
    split_digests: dict[str, str]
    summary_state: Literal["recorded", "missing", "ambiguous"]
    recorded_status: str | None = None
    metrics: dict[str, float]
    artifact_digests: dict[str, str]
    terminal_reason: str | None
    run_attempts: list[CandidateAttemptRead]
    trials: list[CandidateTrialRead]
