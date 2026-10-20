"""A durable command key cannot authorize a second blind mutation."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import CommandStatus
from automl_api.models.iam import User
from automl_api.models.workflows import WorkflowCommand
from automl_api.services.idempotency import durable_mutation
from automl_api.services.workflow_state import begin_command
from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

pytest_plugins = ["test_phase1_postgres_contracts"]


class Result(BaseModel):
    id: uuid.UUID


class EmptyResult(BaseModel):
    pass


def invoke(db, user, project_id, execute, *, schema=Result, payload=None):
    return durable_mutation(
        db,
        user,
        project_id,
        operation="test.response.replay",
        idempotency_key="one-request",
        payload=payload or {"v": 1},
        execute=execute,
        response_model=schema,
        response_status=201,
    )


def pending(db, user_id, project_id):
    command, replay = begin_command(
        db,
        project_id=project_id,
        actor_id=user_id,
        operation="test.response.replay",
        idempotency_key="one-request",
        payload={"v": 1},
    )
    assert not replay and command.response_status is None and not command.response_payload
    db.commit()
    return command


def test_cached_command_refreshes_another_connections_committed_response(
    phase1_engine, committed_tenant
):
    user_id, project_id = committed_tenant
    stored = Result(id=uuid.uuid4())
    with Session(phase1_engine, expire_on_commit=False) as db:
        command = pending(db, user_id, project_id)
        with Session(phase1_engine) as other, other.begin():
            current = other.get(WorkflowCommand, command.id)
            current.response_status = 201
            current.response_payload = stored.model_dump(mode="json")
            current.status = CommandStatus.SUCCEEDED
        assert command.response_payload == {} and command.response_status is None
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        assert invoke(db, db.get(User, user_id), project_id, execute) == stored
        execute.assert_not_called()


def test_pending_replay_cannot_execute_without_a_durable_response(phase1_engine, committed_tenant):
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        pending(db, user_id, project_id)
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        with pytest.raises(HTTPException) as error:
            invoke(db, db.get(User, user_id), project_id, execute)
        assert error.value.status_code == 409
        execute.assert_not_called()


def test_confirmed_empty_response_replays_without_executing_again(phase1_engine, committed_tenant):
    user_id, project_id = committed_tenant
    with Session(phase1_engine, expire_on_commit=False) as db:
        execute = MagicMock(return_value=EmptyResult())
        assert (
            invoke(db, db.get(User, user_id), project_id, execute, schema=EmptyResult)
            == EmptyResult()
        )
        db.commit()
        assert (
            invoke(db, db.get(User, user_id), project_id, execute, schema=EmptyResult)
            == EmptyResult()
        )
        assert execute.call_count == 1


@pytest.mark.parametrize("status", list(CommandStatus))
def test_missing_response_never_restarts_a_command_status(phase1_engine, committed_tenant, status):
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        command = pending(db, user_id, project_id)
        command.status = status
        db.commit()
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        with pytest.raises(HTTPException) as error:
            invoke(db, db.get(User, user_id), project_id, execute)
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "idempotency_response_pending"
        execute.assert_not_called()


@pytest.mark.parametrize("status", list(CommandStatus))
def test_legacy_nonempty_response_does_not_require_status_backfill(
    phase1_engine, committed_tenant, status
):
    user_id, project_id = committed_tenant
    stored = Result(id=uuid.uuid4())
    with Session(phase1_engine) as db:
        command = pending(db, user_id, project_id)
        command.status = status
        command.response_payload = stored.model_dump(mode="json")
        db.commit()
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        assert invoke(db, db.get(User, user_id), project_id, execute) == stored
        execute.assert_not_called()
        db.refresh(command)
        assert command.response_status is None and command.status == status


def test_pending_response_can_later_recover_from_another_connection(
    phase1_engine, committed_tenant
):
    user_id, project_id = committed_tenant
    stored = Result(id=uuid.uuid4())
    with Session(phase1_engine, expire_on_commit=False) as db:
        command = pending(db, user_id, project_id)
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        with pytest.raises(HTTPException):
            invoke(db, db.get(User, user_id), project_id, execute)
        with Session(phase1_engine) as other, other.begin():
            current = other.get(WorkflowCommand, command.id)
            current.response_status = 201
            current.response_payload = stored.model_dump(mode="json")
            current.status = CommandStatus.SUCCEEDED
        for _ in range(2):
            assert invoke(db, db.get(User, user_id), project_id, execute) == stored
        execute.assert_not_called()


def test_changed_payload_conflicts_before_missing_response_recovery(
    phase1_engine, committed_tenant
):
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        pending(db, user_id, project_id)
        execute = MagicMock(return_value=Result(id=uuid.uuid4()))
        with pytest.raises(HTTPException) as error:
            invoke(db, db.get(User, user_id), project_id, execute, payload={"v": 2})
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "idempotency_key_reused"
        execute.assert_not_called()


@pytest.mark.parametrize("path", ["reserve", "launch"])
def test_training_missing_response_replay_precedes_mutable_planning(
    phase1_engine, committed_tenant, monkeypatch, path
):
    from automl_api.schemas.training import TrainingEstimateRequest, TrainingLaunchRequest
    from automl_api.services import training

    user_id, project_id = committed_tenant
    request = (
        TrainingEstimateRequest(dataset_version_id=uuid.uuid4(), reserve_capacity=True)
        if path == "reserve"
        else TrainingLaunchRequest(dataset_version_id=uuid.uuid4())
    )
    payload = training._training_request_body(request)
    operation = "training.estimate.reserve" if path == "reserve" else "training.launch"
    if path == "reserve":
        payload = {"request": payload, "protocol": "estimate-identity-v1"}
    resolve = MagicMock(side_effect=AssertionError("Replay must not resolve mutable prerequisites"))
    monkeypatch.setattr(training, "_resolve_estimate_identity", resolve)
    with Session(phase1_engine) as db:
        begin_command(
            db,
            project_id=project_id,
            actor_id=user_id,
            operation=operation,
            idempotency_key="pending-training",
            payload=payload,
        )
        db.commit()
        invoke_training = (
            training.estimate_training_run if path == "reserve" else training.launch_training_run
        )
        with pytest.raises(HTTPException) as error:
            invoke_training(
                db, db.get(User, user_id), project_id, request, idempotency_key="pending-training"
            )
        assert error.value.detail["code"] == "idempotency_response_pending"
        resolve.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
def test_derived_training_replay_does_not_fall_back_to_a_new_launch(
    phase1_engine, committed_tenant, changed
):
    from automl_api.services import training

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        begin_command(
            db,
            project_id=project_id,
            actor_id=user_id,
            operation="training.launch",
            idempotency_key="derived-launch",
            payload={"source": "original"},
        )
        db.commit()
        with pytest.raises(HTTPException) as error:
            training._replay_derived_training_launch(
                db,
                project_id,
                "derived-launch",
                {"source": "changed" if changed else "original"},
                {},
            )
        assert error.value.detail["code"] == (
            "idempotency_key_reused" if changed else "idempotency_response_pending"
        )


def test_contract_mutation_cannot_execute_a_pending_command(phase1_engine, committed_tenant):
    from automl_api.api.routes.contracts import _mutation

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        pending(db, user_id, project_id)
        create = MagicMock(return_value=Result(id=uuid.uuid4()))
        with pytest.raises(HTTPException) as error:
            _mutation(
                db,
                db.get(User, user_id),
                project_id,
                "test.response.replay",
                "one-request",
                {"v": 1},
                create,
                Result,
            )
        assert error.value.detail["code"] == "idempotency_response_pending"
        create.assert_not_called()


def test_scope_membership_cannot_execute_a_pending_command(
    phase1_engine, committed_tenant, monkeypatch
):
    from automl_api.api.routes import contracts
    from automl_api.schemas.contracts import EvaluationScopeMemberCreate

    user_id, project_id = committed_tenant
    scope_id = uuid.uuid4()
    payload = EvaluationScopeMemberCreate(run_id=uuid.uuid4())
    add = MagicMock(side_effect=AssertionError("No duplicate membership"))
    monkeypatch.setattr(contracts, "add_scope_member", add)
    with Session(phase1_engine) as db:
        begin_command(
            db,
            project_id=project_id,
            actor_id=user_id,
            operation="evaluation-scope.member.add",
            idempotency_key="member-request",
            payload={"scope_id": str(scope_id), **payload.model_dump(mode="json")},
        )
        db.commit()
        with pytest.raises(HTTPException) as error:
            contracts.post_scope_member(
                project_id, scope_id, payload, db, db.get(User, user_id), "member-request"
            )
        assert error.value.detail["code"] == "idempotency_response_pending"
        add.assert_not_called()


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_mutation_requests_have_one_callback_and_deterministic_replay(
    phase1_engine, committed_tenant, conflicting
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from sqlalchemy import event, select

    user_id, project_id = committed_tenant
    barrier = Barrier(2)
    calls = []

    def before_flush(db, *_args):
        if any(isinstance(row, WorkflowCommand) and row.project_id == project_id for row in db.new):
            barrier.wait(timeout=15)

    def execute():
        value = Result(id=uuid.uuid4())
        calls.append(value.id)
        return value

    def request(index):
        with Session(phase1_engine) as db:
            try:
                result = invoke(
                    db,
                    db.get(User, user_id),
                    project_id,
                    execute,
                    payload={"v": index if conflicting else 1},
                )
                db.commit()
                return 201, result.model_dump(mode="json")
            except HTTPException as error:
                db.rollback()
                return error.status_code, error.detail

    event.listen(Session, "before_flush", before_flush)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(request, (1, 2)))
    finally:
        event.remove(Session, "before_flush", before_flush)
    assert len(calls) == 1
    if conflicting:
        assert sorted(code for code, _ in results) == [201, 409]
        assert (
            next(body for code, body in results if code == 409)["code"] == "idempotency_key_reused"
        )
    else:
        assert results[0] == results[1] == (201, {"id": str(calls[0])})
    with Session(phase1_engine) as db:
        commands = list(
            db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == project_id))
        )
        assert len(commands) == 1 and commands[0].response_payload == {"id": str(calls[0])}
