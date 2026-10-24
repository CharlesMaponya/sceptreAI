"""Closed metadata-only contract for one Pod-bound parent execution."""

import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PARENT_CONTROL_PROTOCOL = "parent-control-v1"


class ParentControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    worker_id: uuid.UUID
    run_kind: Literal["training", "validation", "explainability", "drift"] | None = None
    failure: Annotated[str, Field(max_length=2000, strict=True)] | None = None


class ParentControlReply(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    protocol: Literal["parent-control-v1"]
    attempt_id: uuid.UUID
    run_id: uuid.UUID
    project_id: uuid.UUID
    generation: Annotated[int, Field(gt=0, strict=True)]
    run_kind: Literal["training", "validation", "explainability", "drift"]
    audience: str
    has_result: bool = Field(strict=True)
    completed: bool = Field(strict=True)
    changed: bool = Field(strict=True)

    @model_validator(mode="after")
    def committed_completion(self):
        if self.completed and not self.has_result:
            raise ValueError("Parent completion requires its stored result")
        return self
