"""The server selects model custody; workers supply only the run and candidate."""

import uuid
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

TRAINING_MODEL_READ_PROTOCOL = "training-model-read-v1"


class TrainingModelReadRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: uuid.UUID
    candidate_name: Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]{1,160}$", strict=True)]
    request_id: uuid.UUID
