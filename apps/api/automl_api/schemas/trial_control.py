"""Closed finite metadata for Pod-bound durable trial control."""

import json
import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

TRIAL_CONTROL_PROTOCOL = "trial-control-v1"
TRIAL_CONTROL_MAX_BYTES = 262144


class TrialControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    worker_id: uuid.UUID
    child_attempt_id: uuid.UUID | None = None
    candidate_name: Annotated[str, Field(min_length=1, max_length=160, strict=True)] | None = None
    suggestion_id: Annotated[str, Field(min_length=1, max_length=255, strict=True)] | None = None
    parameters: dict[str, JsonValue] | None = None
    dispatch_id: Annotated[str, Field(min_length=1, max_length=200, strict=True)] | None = None
    result: dict[str, JsonValue] | None = None
    failure: Annotated[str, Field(min_length=1, max_length=2000, strict=True)] | None = None

    @model_validator(mode="after")
    def finite_metadata(self):
        json.dumps(self.model_dump(mode="json"), allow_nan=False)
        return self


class TrialControlReply(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    protocol: Literal["trial-control-v1"]
    parent_attempt_id: uuid.UUID
    run_id: uuid.UUID
    project_id: uuid.UUID
    audience: str
    child_attempt_id: uuid.UUID
    terminal: dict[str, JsonValue] | None

    @model_validator(mode="after")
    def closed_terminal_result(self):
        if self.terminal is not None:
            status = self.terminal.get("status")
            field = "result" if status == "succeeded" else "failure"
            value = self.terminal.get(field)
            if (
                status not in {"succeeded", "failed"}
                or set(self.terminal) != {"status", field}
                or (field == "result" and not isinstance(value, dict))
                or (field == "failure" and (not isinstance(value, str) or not value.strip()))
            ):
                raise ValueError("Invalid terminal trial evidence")
            json.dumps(self.terminal, allow_nan=False)
        return self
