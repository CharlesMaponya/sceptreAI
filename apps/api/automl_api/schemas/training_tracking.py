"""Closed metadata for server-owned tracking creation and receipts."""

import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from automl_api.services.provider_distribution import Digest

TRAINING_TRACKING_PROTOCOL = "training-tracking-v1"


class TrainingTrackingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    role: Annotated[
        str, Field(pattern=r"^(parent|(?:candidate|failure):[A-Za-z0-9_.-]{1,160})$", strict=True)
    ]


class TrainingTrackingReply(TrainingTrackingRequest):
    protocol: Literal["training-tracking-v1"]
    attempt_id: uuid.UUID
    project_id: uuid.UUID
    audience: str
    command_id: uuid.UUID
    request_digest: Digest
    tracking_run_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)]
    parent_tracking_run_id: (
        Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)] | None
    )
    tracking_backend_hash: Digest


class TrainingTrackingReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    attempt_id: uuid.UUID
    command_id: uuid.UUID
    role: Annotated[
        str, Field(pattern=r"^(parent|(?:candidate|failure):[A-Za-z0-9_.-]{1,160})$", strict=True)
    ]
    tracking_run_id: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,255}$", strict=True)]
    owner_digest: Digest
    receipt_digest: Digest
