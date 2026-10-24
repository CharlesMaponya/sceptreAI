"""Closed execution context; final-test locations never cross this boundary."""

import json
import uuid
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from automl_api.models.enums import RunStatus, TaskType
from automl_api.services.dataset_digests import require_distinct_training_roles
from automl_api.services.provider_source import ProviderReadPlan

TRAINING_CONTEXT_PROTOCOL = "training-context-v1"
TRAINING_CONTEXT_MAX_BYTES = 16 * 1024 * 1024
PARAMETER_KEYS = frozenset(
    {
        "launch_command_id",
        "parameter_overrides",
        "parameter_overrides_digest",
        "parameter_override_policy_revision",
        "expected_minutes",
        "deadline_seconds",
        "prefer_gpu",
        "candidate_limit",
        "candidate_models",
        "optimization_iterations",
        "cv_folds",
        "evaluation_column",
        "positive_label",
        "primary_metric",
        "excluded_leakage_columns",
        "excluded_columns",
        "leakage_profile_job_id",
        "gpu_vendor",
        "gpu_resource",
        "selected_node",
        "sample_tier_rows",
        "pairwise_sample_rows",
        "validation_sample_rows",
        "sample_tier_policy",
        "sample_tier_order",
        "feature_statistics_max_rows",
        "experiment_spec_revision_id",
        "search_objective_revision_id",
        "entity_column",
        "event_time_column",
        "prediction_time_column",
        "split_revision_id",
        "feature_contract_revision_id",
        "feature_registry_revision_id",
        "feature_recipe_revision_id",
        "feature_search_space_revision_id",
        "estimator_catalog_revision_id",
        "catalog_runtime_digest",
        "catalog_content_digest",
        "estimator_overrides",
        "deduplicated_rows",
    }
)
TAG_KEYS = frozenset({"candidate_parent_run_id", "leaderboard", "leaderboard_parent_run_id"})
CANDIDATE_KEYS = frozenset(
    {
        "rank",
        "model",
        "status",
        "cost_tier",
        "primary_score",
        "metrics",
        "diagnostics",
        "best_params",
        "duration_seconds",
        "error",
        "mlflow_run_id",
        "model_artifact_uri",
        "model_artifact_sha256",
        "phase",
        "phase_updated_at",
        "extension_run_id",
        "resumed",
        "worker_pod",
        "training_rows",
        "validation_rows",
    }
)


class TrainingContextRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID


class TrainingRunContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: uuid.UUID
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    status: Literal[RunStatus.RUNNING]
    task_type: TaskType
    run_name: str | None
    target_column: str | None
    params: dict[str, JsonValue]
    tags: dict[str, JsonValue]

    @model_validator(mode="after")
    def closed_execution_fields(self):
        if set(self.params) - PARAMETER_KEYS or set(self.tags) - TAG_KEYS:
            raise ValueError("Unapproved training context metadata")
        entries = self.tags.get("leaderboard", [])
        if not isinstance(entries, list) or any(
            not isinstance(entry, dict) or set(entry) - CANDIDATE_KEYS for entry in entries
        ):
            raise ValueError("Unapproved candidate context metadata")
        return self


class TrainingSplitContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: uuid.UUID
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    train_uri: Annotated[str, Field(min_length=1, max_length=2048, strict=True)]
    validation_uri: Annotated[str, Field(min_length=1, max_length=2048, strict=True)]
    split_counts: dict[Literal["train", "validation"], Annotated[int, Field(ge=0, strict=True)]]
    split_strategy: str | None
    time_column: str | None
    row_set_digest: str | None
    provider_import_id: uuid.UUID | None

    @model_validator(mode="after")
    def credential_free_roles(self):
        require_distinct_training_roles(
            {"train": self.train_uri, "validation": self.validation_uri}
        )
        for value in (self.train_uri, self.validation_uri):
            parsed = urlsplit(value)
            if (
                not parsed.scheme
                or parsed.username is not None
                or parsed.query
                or parsed.fragment
                or any(char.isspace() or ord(char) < 32 for char in value)
            ):
                raise ValueError("Training roles require credential-free object locations")
        return self


class TrainingContextReply(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    protocol: Literal["training-context-v1"]
    attempt_id: uuid.UUID
    project_id: uuid.UUID
    audience: str
    run: TrainingRunContext
    split: TrainingSplitContext
    approved_columns: Annotated[list[str], Field(min_length=1)]
    source_plan: ProviderReadPlan | None

    @model_validator(mode="after")
    def safe_lineage(self):
        if (
            self.project_id != self.run.project_id
            or self.project_id != self.split.project_id
            or self.run.dataset_version_id != self.split.dataset_version_id
            or str(self.split.id) != self.run.params.get("split_revision_id")
            or len(set(self.approved_columns)) != len(self.approved_columns)
        ):
            raise ValueError("Training input context lineage changed")
        if self.split.provider_import_id is not None and self.source_plan is None:
            raise ValueError("Provider training context requires its approved safe source plan")
        if self.source_plan is not None and (
            self.split.provider_import_id != self.source_plan.import_id
            or self.source_plan.project_id != self.project_id
            or self.source_plan.dataset_version_id != self.run.dataset_version_id
            or self.source_plan.split_revision_id != self.split.id
            or self.source_plan.manifest.mode != "validation_only"
            or self.source_plan.manifest.final_data is not None
        ):
            raise ValueError("Training context cannot disclose final provider objects")
        json.dumps(self.model_dump(mode="json"), allow_nan=False)
        return self
