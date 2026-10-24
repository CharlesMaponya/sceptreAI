"""Closed, finite metadata for final training publication."""

import json
import math
import re
import uuid
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictFloat,
    StrictInt,
    model_validator,
)

from automl_api.schemas.training import LeaderboardEntryRead
from automl_api.schemas.training_context import CANDIDATE_KEYS
from automl_api.services.provider_distribution import Digest

TRAINING_RESULT_PROTOCOL = "training-result-v1"
TRAINING_RESULT_MAX_BYTES = 16 * 1024 * 1024
RESULT_PARAM_KEYS = frozenset({"excluded_leakage_columns", "deduplicated_rows", "positive_label"})


class TrainingResultRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    metrics: dict[str, StrictInt | StrictFloat]
    params: dict[str, JsonValue]
    leaderboard: Annotated[list[dict[str, JsonValue]], Field(min_length=1, max_length=1000)]
    primary_metric: Annotated[str, Field(min_length=1, max_length=160, strict=True)]
    mlflow_run_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)]

    @model_validator(mode="after")
    def finite_closed_result(self):
        encoded = json.dumps(self.model_dump(mode="json"), allow_nan=False).encode()
        if len(encoded) > TRAINING_RESULT_MAX_BYTES or set(self.params) - RESULT_PARAM_KEYS:
            raise ValueError("Unapproved or oversized result metadata")
        if not self.metrics or len(self.metrics) > 1000:
            raise ValueError("Result requires bounded metrics")
        if any(not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", key) for key in self.metrics):
            raise ValueError("Invalid metric identity")
        excluded = self.params.get("excluded_leakage_columns", [])
        deduplicated = self.params.get("deduplicated_rows", 0)
        if (
            not isinstance(excluded, list)
            or len(excluded) > 10000
            or any(not isinstance(item, str) or len(item) > 255 for item in excluded)
            or type(deduplicated) is not int
            or deduplicated < 0
        ):
            raise ValueError("Invalid derived training parameters")
        label = self.params.get("positive_label")
        if label is not None and type(label) not in {str, int, float, bool}:
            raise ValueError("The positive label must be a scalar")
        numeric = list(self.metrics.values())
        for entry in self.leaderboard:
            if isinstance(entry.get("metrics"), dict):
                numeric.extend(entry["metrics"].values())
            numeric.extend(
                entry[key]
                for key in ("duration_seconds", "primary_score")
                if entry.get(key) is not None
            )
        try:
            finite = all(
                type(value) in {int, float} and math.isfinite(float(value)) for value in numeric
            )
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError("Metrics must be finite representable numbers")
        for entry in self.leaderboard:
            name, status = entry.get("model"), entry.get("status")
            metrics = entry.get("metrics", {})
            if (
                set(entry) - CANDIDATE_KEYS
                or not isinstance(name, str)
                or not 0 < len(name) <= 160
                or status not in {"succeeded", "failed", "cancelled", "skipped"}
                or not isinstance(metrics, dict)
                or len(metrics) > 1000
                or any(type(value) not in {int, float} for value in metrics.values())
                or any(not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", key) for key in metrics)
            ):
                raise ValueError("Invalid terminal candidate metadata")
            LeaderboardEntryRead.model_validate_json(
                json.dumps(
                    {
                        "rank": None,
                        "primary_score": None,
                        "duration_seconds": None,
                        "error": None,
                        **entry,
                    }
                ),
                strict=True,
            )
            for key in ("training_rows", "validation_rows", "rank"):
                value = entry.get(key)
                if value is not None and (
                    type(value) is not int or value < (1 if key == "rank" else 0)
                ):
                    raise ValueError("Invalid candidate count or rank")
            if entry.get("duration_seconds") is not None and entry["duration_seconds"] < 0:
                raise ValueError("Candidate duration cannot be negative")
            if entry.get("resumed") is not None and type(entry["resumed"]) is not bool:
                raise ValueError("Invalid candidate resume marker")
            for key in ("phase", "phase_updated_at", "worker_pod"):
                value = entry.get(key)
                if value is not None and (not isinstance(value, str) or len(value) > 512):
                    raise ValueError("Invalid candidate execution metadata")
            if status != "succeeded" and any(
                entry.get(key) is not None
                for key in ("model_artifact_uri", "model_artifact_sha256")
            ):
                raise ValueError("Only successful candidates may publish model references")
            if status == "succeeded" and (
                not isinstance(entry.get("model_artifact_uri"), str)
                or not 0 < len(entry["model_artifact_uri"]) <= 2048
                or not isinstance(entry.get("model_artifact_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["model_artifact_sha256"])
            ):
                raise ValueError("A successful candidate requires its model receipt identity")
        return self


class TrainingResultReceiptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    request_digest: Digest


class TrainingResultReply(TrainingResultReceiptRequest):
    protocol: Literal["training-result-v1"]
    attempt_id: uuid.UUID
    project_id: uuid.UUID
    audience: str
    committed: bool = Field(strict=True)

    @model_validator(mode="after")
    def require_commit(self):
        if not self.committed:
            raise ValueError("A result acknowledgement requires its committed receipt")
        return self
