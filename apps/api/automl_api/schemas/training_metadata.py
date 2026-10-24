"""Bounded metadata only; callers never choose a native tracking target."""

import json
import sys
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from automl_api.schemas.training_tracking import TrainingTrackingRequest
from automl_api.services.provider_distribution import Digest

TRAINING_METADATA_PROTOCOL = "training-metadata-v1"
TRAINING_METADATA_MAX_BYTES = 256 * 1024


class TrainingMetadataRequest(TrainingTrackingRequest):
    request_id: uuid.UUID
    kind: Literal["tags", "params", "metrics", "mirror"]
    data: dict[str, JsonValue]

    @model_validator(mode="after")
    def bounded_metadata(self):
        import re

        if not 1 <= len(self.data) <= 128:
            raise ValueError("Metadata requires 1 to 128 named values")
        if any(not re.fullmatch(r"[A-Za-z0-9_.:/ -]{1,250}", key) for key in self.data):
            raise ValueError("Invalid metadata name")
        if self.kind == "metrics" and any(
            type(value) not in {int, float}
            or not -sys.float_info.max <= value <= sys.float_info.max
            for value in self.data.values()
        ):
            raise ValueError("Metrics must be finite numbers")
        if self.kind == "mirror" and (
            set(self.data) != {"metric_count"}
            or type(self.data["metric_count"]) is not int
            or not 0 <= self.data["metric_count"] <= 128
            or not self.role.startswith("candidate:")
        ):
            raise ValueError("Invalid candidate mirror")
        if any(len(str(value)) > 5000 for value in self.data.values()):
            raise ValueError("Metadata value exceeds its limit")
        if len(json.dumps(self.data, allow_nan=False).encode()) > 128 * 1024:
            raise ValueError("Metadata exceeds its byte budget")
        return self


class TrainingMetadataReceiptRequest(TrainingTrackingRequest):
    request_id: uuid.UUID


class TrainingMetadataReply(TrainingMetadataReceiptRequest):
    protocol: Literal["training-metadata-v1"]
    project_id: uuid.UUID
    attempt_id: uuid.UUID
    audience: str
    request_digest: Digest
    tracking_run_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)]
    command_id: uuid.UUID
    committed: Literal[True]


class TrainingMetadataReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    attempt_id: uuid.UUID
    command_id: uuid.UUID
    role: Annotated[
        str, Field(pattern=r"^(parent|(?:candidate|failure):[A-Za-z0-9_.-]{1,160})$", strict=True)
    ]
    tracking_run_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)]
    pod_uid: Annotated[str, Field(min_length=1, max_length=255, strict=True)]
    intent_digest: Digest
    receipt_digest: Digest
