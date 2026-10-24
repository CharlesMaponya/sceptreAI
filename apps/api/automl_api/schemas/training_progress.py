"""Finite metadata-only candidate and parent progress commands."""

import json
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from automl_api.schemas.training_result import TrainingResultRequest
from automl_api.services.provider_distribution import Digest

TRAINING_PROGRESS_PROTOCOL = "training-progress-v1"
TRAINING_PROGRESS_MAX_BYTES = 16 * 1024 * 1024
PHASES = frozenset(
    {
        "preparing_data",
        "hyperparameter_search",
        "fitting_final_model",
        "cross_validating",
        "evaluating",
        "learning_curve",
        "logging_to_mlflow",
        "saving_model",
    }
)
TERMINAL = frozenset({"succeeded", "failed", "cancelled", "skipped"})


class TrainingProgressRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    request_id: uuid.UUID
    client_id: uuid.UUID
    sequence: Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]
    action: Literal["phase", "partial"]
    candidate_name: Annotated[str, Field(min_length=1, max_length=160, strict=True)] | None = None
    phase: str | None = None
    entries: Annotated[list[dict[str, JsonValue]], Field(min_length=1, max_length=1000)] | None = (
        None
    )
    primary_metric: Annotated[str, Field(min_length=1, max_length=160, strict=True)] | None = None

    @model_validator(mode="after")
    def closed_progress(self):
        if (
            len(json.dumps(self.model_dump(mode="json"), allow_nan=False).encode())
            > TRAINING_PROGRESS_MAX_BYTES
        ):
            raise ValueError("Progress exceeds its metadata budget")
        if self.action == "phase":
            if (
                not self.candidate_name
                or self.phase not in PHASES
                or self.entries is not None
                or self.primary_metric is not None
            ):
                raise ValueError("Invalid phase arguments")
        else:
            if (
                self.candidate_name is not None
                or self.phase is not None
                or not self.entries
                or not self.primary_metric
            ):
                raise ValueError("Invalid aggregate progress arguments")
            if any(
                entry.get("status") not in TERMINAL | {"pending", "running"}
                for entry in self.entries
            ):
                raise ValueError("Unapproved candidate progress")
            # Apply the same metadata boundary to unfinished and final candidate snapshots.
            TrainingResultRequest(
                run_id=self.run_id,
                metrics={self.primary_metric: 0},
                params={},
                leaderboard=[
                    {
                        **entry,
                        "status": entry["status"] if entry["status"] in TERMINAL else "failed",
                    }
                    for entry in self.entries
                ],
                primary_metric=self.primary_metric,
                mlflow_run_id="progress",
            )
        return self


class TrainingProgressReceiptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    request_id: uuid.UUID


class TrainingProgressReply(TrainingProgressReceiptRequest):
    protocol: Literal["training-progress-v1"]
    attempt_id: uuid.UUID
    project_id: uuid.UUID
    audience: str
    request_digest: Digest
    changed: bool = Field(strict=True)
