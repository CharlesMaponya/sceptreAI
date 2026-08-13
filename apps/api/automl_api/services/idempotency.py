from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from automl_api.models.enums import CommandStatus
from automl_api.models.iam import User
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    begin_command,
    enqueue_outbox,
    transition_command,
)


def durable_mutation[ResponseModel: BaseModel](
    db: Session,
    user: User,
    project_id: uuid.UUID,
    *,
    operation: str,
    idempotency_key: str,
    payload: Mapping[str, Any],
    execute: Callable[[], ResponseModel],
    response_model: type[ResponseModel],
    response_status: int,
    outbox_topic: str | None = None,
    outbox_payload: Callable[[ResponseModel], Mapping[str, Any] | None] | None = None,
    aggregate_type: str | None = None,
    resource_id: uuid.UUID | Callable[[ResponseModel], uuid.UUID] | None = None,
) -> ResponseModel:
    """Execute one mutation and persist a replayable typed response atomically."""
    try:
        command, replayed = begin_command(
            db,
            project_id=project_id,
            actor_id=user.id,
            operation=operation,
            idempotency_key=idempotency_key,
            payload=payload,
        )
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_key_reused", "message": str(exc)},
        ) from exc
    if replayed and command.response_payload:
        return response_model.model_validate(command.response_payload)

    result = execute()
    command.response_status = response_status
    command.response_payload = result.model_dump(mode="json")
    resource = getattr(result, "id", None)
    if resource is None and (nested := getattr(result, "run", None)) is not None:
        resource = getattr(nested, "id", None)
    if resource is None and resource_id is not None:
        resource = resource_id(result) if callable(resource_id) else resource_id
    command.resource_type = operation
    command.resource_id = resource
    transition_command(command, CommandStatus.RUNNING)
    if outbox_topic is None:
        transition_command(command, CommandStatus.SUCCEEDED)
    else:
        if resource is None or outbox_payload is None or aggregate_type is None:
            raise ValueError(
                "Durable side effects require a resource, aggregate type, and payload."
            )
        side_effect_payload = outbox_payload(result)
        if side_effect_payload is None:
            transition_command(command, CommandStatus.SUCCEEDED)
        else:
            enqueue_outbox(
                db,
                command,
                topic=outbox_topic,
                aggregate_type=aggregate_type,
                aggregate_id=resource,
                payload=side_effect_payload,
            )
    db.flush()
    return result
