from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import CommandStatus
from automl_api.services import idempotency
from automl_api.services.workflow_state import IdempotencyConflict
from fastapi import HTTPException
from pydantic import BaseModel


class _Result(BaseModel):
    id: uuid.UUID
    value: str


class _NestedResult(BaseModel):
    run: _Result


def test_durable_mutation_replays_and_persists_resource(monkeypatch) -> None:
    stored = _Result(id=uuid.uuid4(), value="stored")
    replay = SimpleNamespace(response_payload=stored.model_dump(mode="json"))
    monkeypatch.setattr(idempotency, "begin_command", lambda *_args, **_kwargs: (replay, True))
    execute = MagicMock()
    assert (
        idempotency.durable_mutation(
            MagicMock(),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            operation="test",
            idempotency_key="key",
            payload={},
            execute=execute,
            response_model=_Result,
            response_status=201,
        )
        == stored
    )
    execute.assert_not_called()

    command = SimpleNamespace(
        status=CommandStatus.PENDING,
        response_payload={},
        response_status=None,
        resource_type=None,
        resource_id=None,
    )
    monkeypatch.setattr(idempotency, "begin_command", lambda *_args, **_kwargs: (command, False))
    db = MagicMock()
    created = _Result(id=uuid.uuid4(), value="created")
    assert (
        idempotency.durable_mutation(
            db,
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            operation="test.create",
            idempotency_key="key",
            payload={"value": "created"},
            execute=lambda: created,
            response_model=_Result,
            response_status=201,
        )
        == created
    )
    assert command.resource_id == created.id
    assert command.status == CommandStatus.SUCCEEDED


def test_durable_mutation_extracts_nested_run_and_rejects_conflict(monkeypatch) -> None:
    command = SimpleNamespace(
        status=CommandStatus.PENDING,
        response_payload={},
        response_status=None,
        resource_type=None,
        resource_id=None,
    )
    monkeypatch.setattr(idempotency, "begin_command", lambda *_args, **_kwargs: (command, False))
    nested = _NestedResult(run=_Result(id=uuid.uuid4(), value="run"))
    idempotency.durable_mutation(
        MagicMock(),
        SimpleNamespace(id=uuid.uuid4()),
        uuid.uuid4(),
        operation="nested",
        idempotency_key="key",
        payload={},
        execute=lambda: nested,
        response_model=_NestedResult,
        response_status=202,
    )
    assert command.resource_id == nested.run.id

    monkeypatch.setattr(
        idempotency,
        "begin_command",
        MagicMock(side_effect=IdempotencyConflict("changed")),
    )
    with pytest.raises(HTTPException) as error:
        idempotency.durable_mutation(
            MagicMock(),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            operation="nested",
            idempotency_key="key",
            payload={"changed": True},
            execute=lambda: nested,
            response_model=_NestedResult,
            response_status=202,
        )
    assert error.value.detail["code"] == "idempotency_key_reused"


def test_durable_mutation_queues_side_effect_and_completes_noop(monkeypatch) -> None:
    command = SimpleNamespace(
        id=uuid.uuid4(),
        status=CommandStatus.PENDING,
        response_payload={},
        response_status=None,
        resource_type=None,
        resource_id=None,
    )
    monkeypatch.setattr(idempotency, "begin_command", lambda *_args, **_kwargs: (command, False))
    enqueue = MagicMock()
    monkeypatch.setattr(idempotency, "enqueue_outbox", enqueue)
    created = _Result(id=uuid.uuid4(), value="created")

    idempotency.durable_mutation(
        MagicMock(),
        SimpleNamespace(id=uuid.uuid4()),
        uuid.uuid4(),
        operation="side.effect",
        idempotency_key="key",
        payload={},
        execute=lambda: created,
        response_model=_Result,
        response_status=202,
        outbox_topic="external.reconcile",
        aggregate_type="resource",
        outbox_payload=lambda result: {"id": str(result.id)},
    )
    assert command.status == CommandStatus.RUNNING
    enqueue.assert_called_once()

    command.status = CommandStatus.PENDING
    enqueue.reset_mock()
    idempotency.durable_mutation(
        MagicMock(),
        SimpleNamespace(id=uuid.uuid4()),
        uuid.uuid4(),
        operation="side.effect.noop",
        idempotency_key="key-2",
        payload={},
        execute=lambda: created,
        response_model=_Result,
        response_status=202,
        outbox_topic="external.reconcile",
        aggregate_type="resource",
        outbox_payload=lambda _result: None,
    )
    assert command.status == CommandStatus.SUCCEEDED
    enqueue.assert_not_called()

    command.status = CommandStatus.PENDING
    with pytest.raises(ValueError, match="require a resource"):
        idempotency.durable_mutation(
            MagicMock(),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            operation="invalid.side.effect",
            idempotency_key="key-3",
            payload={},
            execute=lambda: created,
            response_model=_Result,
            response_status=202,
            outbox_topic="external.reconcile",
        )
