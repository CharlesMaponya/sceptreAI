from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    RunKind,
    RunStatus,
    ScopeStatus,
    TaskType,
)
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    CapacityReservation,
    OutboxEntry,
    PromotionalScope,
    PromotionalScopeMember,
    WorkflowAttempt,
    WorkflowCommand,
)
from automl_api.services import final_test_authority as authority
from automl_api.services import workflow_state as state


def _command(**overrides):
    values = {
        "project_id": uuid.uuid4(),
        "actor_id": uuid.uuid4(),
        "operation": "training.launch",
        "idempotency_key": "one",
        "request_hash": state.canonical_request_hash({"a": 1}),
        "request_payload": {"a": 1},
        "status": CommandStatus.PENDING,
    }
    values.update(overrides)
    return WorkflowCommand(**values)


def _attempt(**overrides):
    values = {
        "project_id": uuid.uuid4(),
        "stage": "training_run",
        "logical_key": "run:1",
        "model_run_id": uuid.uuid4(),
        "workload_identity": "project-a-training",
        "generation": 1,
        "fencing_token": "run-1",
        "status": AttemptStatus.PENDING,
        "retry_count": 0,
        "retry_budget": 5,
        "terminal_cas_version": 0,
    }
    values.update(overrides)
    return WorkflowAttempt(**values)


def test_canonical_hash_and_command_transitions_are_deterministic() -> None:
    assert state.canonical_request_hash({"b": 2, "a": 1}) == state.canonical_request_hash(
        {"a": 1, "b": 2}
    )
    command = _command()
    state.transition_command(command, CommandStatus.PENDING)
    state.transition_command(command, CommandStatus.RUNNING)
    state.transition_command(command, CommandStatus.SUCCEEDED)
    with pytest.raises(state.InvalidTransition, match="Cannot transition"):
        state.transition_command(command, CommandStatus.RUNNING)
    with pytest.raises(state.IdempotencyConflict, match="different request"):
        state._validate_replay(command, "not-the-same")
    assert state._validate_replay(command, command.request_hash) is command


def test_attempt_transition_requires_fence_and_terminal_cas() -> None:
    attempt = _attempt()
    with pytest.raises(state.StaleFence, match="fencing token"):
        state.transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token="old")
    state.transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token="run-1")
    state.transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token="run-1")
    state.transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token="run-1")
    state.transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token="run-1")
    with pytest.raises(state.StaleFence, match="CAS"):
        state.transition_attempt(
            attempt,
            AttemptStatus.SUCCEEDED,
            fencing_token="run-1",
            expected_cas_version=2,
        )
    state.transition_attempt(
        attempt,
        AttemptStatus.SUCCEEDED,
        fencing_token="run-1",
        terminal_reason="complete",
        expected_cas_version=0,
    )
    assert attempt.terminal_cas_version == 1
    assert attempt.terminal_reason == "complete"
    with pytest.raises(state.InvalidTransition):
        state.transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token="run-1")


def test_heartbeat_rejects_stale_owner_and_extends_lease() -> None:
    attempt = _attempt(lease_owner="worker-a")
    now = datetime(2026, 8, 13, tzinfo=UTC)
    with pytest.raises(state.StaleFence):
        state.heartbeat_attempt(
            attempt,
            worker_id="worker-b",
            fencing_token="run-1",
            lease_seconds=30,
            now=now,
        )
    state.heartbeat_attempt(
        attempt,
        worker_id="worker-a",
        fencing_token="run-1",
        lease_seconds=30,
        now=now,
    )
    assert attempt.heartbeat_at == now
    assert int((attempt.lease_expires_at - now).total_seconds()) == 30


def test_begin_command_creates_and_replays(monkeypatch) -> None:
    db = MagicMock()
    db.begin_nested.return_value = MagicMock()
    monkeypatch.setattr(state, "_command_by_key", MagicMock(side_effect=[None, _command()]))
    created, replay = state.begin_command(
        db,
        project_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        operation="training.launch",
        idempotency_key="key",
        payload={"a": 1},
    )
    assert replay is False
    assert isinstance(created, WorkflowCommand)
    db.flush.assert_called_once()

    existing = _command(idempotency_key="key")
    monkeypatch.setattr(state, "_command_by_key", lambda *_args: existing)
    replayed, replay = state.begin_command(
        db,
        project_id=existing.project_id,
        actor_id=existing.actor_id,
        operation=existing.operation,
        idempotency_key="key",
        payload={"a": 1},
    )
    assert replay and replayed is existing
    with pytest.raises(ValueError, match="must not be empty"):
        state.begin_command(
            db,
            project_id=existing.project_id,
            actor_id=existing.actor_id,
            operation=existing.operation,
            idempotency_key=" ",
            payload={},
        )


def test_begin_command_recovers_unique_race_and_propagates_unknown_integrity(monkeypatch) -> None:
    from sqlalchemy.exc import IntegrityError

    existing = _command(idempotency_key="race")
    db = MagicMock()
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate"))
    monkeypatch.setattr(state, "_command_by_key", MagicMock(side_effect=[None, existing]))
    replayed, is_replay = state.begin_command(
        db,
        project_id=existing.project_id,
        actor_id=existing.actor_id,
        operation=existing.operation,
        idempotency_key="race",
        payload={"a": 1},
    )
    assert is_replay and replayed is existing
    db.begin_nested.return_value.rollback.assert_called_once()

    monkeypatch.setattr(state, "_command_by_key", MagicMock(side_effect=[None, None]))
    with pytest.raises(IntegrityError):
        state.begin_command(
            db,
            project_id=existing.project_id,
            actor_id=existing.actor_id,
            operation=existing.operation,
            idempotency_key="other-race",
            payload={"a": 1},
        )


def test_enqueue_and_complete_outbox_are_idempotent() -> None:
    command = _command()
    command.id = uuid.uuid4()
    existing = OutboxEntry(
        project_id=command.project_id,
        command_id=command.id,
        event_key="event",
        topic="ray.submit",
        aggregate_type="run",
        aggregate_id=uuid.uuid4(),
    )
    db = MagicMock()
    db.scalar.return_value = existing
    assert (
        state.enqueue_outbox(
            db,
            command,
            topic="ray.submit",
            aggregate_type="run",
            aggregate_id=existing.aggregate_id,
            payload={},
            event_key="event",
        )
        is existing
    )

    db.scalar.return_value = None
    created = state.enqueue_outbox(
        db,
        command,
        topic="ray.submit",
        aggregate_type="run",
        aggregate_id=existing.aggregate_id,
        payload={"run": "one"},
        event_key="new",
    )
    assert created.payload == {"run": "one"}

    missing = MagicMock()
    missing.get.return_value = None
    with pytest.raises(LookupError):
        state.complete_outbox(missing, uuid.uuid4(), worker_id="worker", delivered=True)
    existing.status = OutboxStatus.CLAIMED
    existing.lease_owner = "other"
    db.get.return_value = existing
    with pytest.raises(state.StaleFence):
        state.complete_outbox(db, existing.id, worker_id="worker", delivered=True)
    existing.lease_owner = "worker"
    command.max_retries = 5
    db.get.side_effect = lambda model, _identity: (
        existing if model is OutboxEntry else command
    )
    delivered = state.complete_outbox(db, existing.id, worker_id="worker", delivered=True)
    assert delivered.status == OutboxStatus.DELIVERED
    assert state.complete_outbox(db, existing.id, worker_id="worker", delivered=True) is existing


def test_outbox_retry_budget_dead_letters_and_operator_replay() -> None:
    command = _command(status=CommandStatus.RUNNING, max_retries=2)
    command.id = uuid.uuid4()
    entry = OutboxEntry(
        project_id=command.project_id,
        command_id=command.id,
        event_key="dead-letter",
        topic="ray.training.submit",
        aggregate_type="model_run",
        aggregate_id=uuid.uuid4(),
        status=OutboxStatus.CLAIMED,
        lease_owner="worker",
        delivery_attempts=2,
    )
    entry.id = uuid.uuid4()
    db = MagicMock()
    db.get.side_effect = lambda model, _identity: entry if model is OutboxEntry else command

    result = state.complete_outbox(
        db,
        entry.id,
        worker_id="worker",
        delivered=False,
        error="permanent failure",
    )
    assert result.status == OutboxStatus.DEAD
    assert command.status == CommandStatus.FAILED
    assert command.terminal_reason == "permanent failure"

    actor_id = uuid.uuid4()
    db.scalar.side_effect = [entry, command]
    replayed = state.replay_dead_outbox(db, entry.id, actor_id=actor_id)
    assert replayed.status == OutboxStatus.PENDING
    assert replayed.delivery_attempts == 0
    assert command.status == CommandStatus.RUNNING
    assert command.replayed_by == actor_id


def test_dead_letter_replay_rejects_missing_or_live_rows() -> None:
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="entry"):
        state.replay_dead_outbox(db, uuid.uuid4(), actor_id=uuid.uuid4())

    entry = SimpleNamespace(status=OutboxStatus.PENDING, command_id=uuid.uuid4())
    db.scalar.return_value = entry
    with pytest.raises(state.InvalidTransition, match="dead"):
        state.replay_dead_outbox(db, uuid.uuid4(), actor_id=uuid.uuid4())

    entry.status = OutboxStatus.DEAD
    db.scalar.side_effect = [entry, None]
    with pytest.raises(LookupError, match="command"):
        state.replay_dead_outbox(db, uuid.uuid4(), actor_id=uuid.uuid4())


def test_claim_queues_and_terminal_cas_cover_retry_boundaries() -> None:
    now = datetime.now(UTC)
    pending = OutboxEntry(
        project_id=uuid.uuid4(),
        command_id=uuid.uuid4(),
        event_key="claim",
        topic="ray.submit",
        aggregate_type="run",
        aggregate_id=uuid.uuid4(),
        status=OutboxStatus.PENDING,
        available_at=now,
        delivery_attempts=0,
    )
    db = MagicMock()
    db.scalars.return_value = [pending]
    assert state.claim_outbox(db, worker_id="worker", now=now, lease_seconds=5) == [pending]
    assert pending.delivery_attempts == 1
    assert pending.lease_owner == "worker"

    attempt = _attempt()
    db.scalars.return_value = [attempt]
    assert state.claim_attempts(db, worker_id="attempt-worker", now=now) == [attempt]
    assert attempt.retry_count == 1
    result = SimpleNamespace(rowcount=1)
    db.execute.return_value = result
    assert state.cas_register_terminal_artifact(
        db,
        attempt_id=attempt.id,
        fencing_token=attempt.fencing_token,
        expected_cas_version=0,
        checkpoint_uri="s3://attempt/final",
    )
    result.rowcount = 0
    assert not state.cas_register_terminal_artifact(
        db,
        attempt_id=attempt.id,
        fencing_token="stale",
        expected_cas_version=0,
        checkpoint_uri="s3://attempt/duplicate",
    )


def test_trial_supersession_and_unfinished_count_are_atomic() -> None:
    active = [_attempt(status=AttemptStatus.RUNNING), _attempt(status=AttemptStatus.SUBMITTED)]
    db = MagicMock()
    db.scalars.return_value = active
    assert state.supersede_trial_attempts(
        db,
        project_id=active[0].project_id,
        model_run_id=active[0].model_run_id,
        terminal_reason="run generation advanced",
    ) == 2
    assert all(row.status == AttemptStatus.SUPERSEDED for row in active)
    assert all(row.terminal_cas_version == 1 for row in active)
    db.scalar.return_value = 4
    assert state.unfinished_count(db) == 4
    db.scalar.return_value = None
    assert state.unfinished_count(db) == 0


def test_scope_seal_requires_exact_contiguous_members() -> None:
    scope = PromotionalScope(
        project_id=uuid.uuid4(),
        scope_key="scope",
        split_revision_id=uuid.uuid4(),
        canonical_provider="local",
        expected_members=2,
        status=ScopeStatus.OPEN,
        cas_version=0,
    )
    scope.id = uuid.uuid4()
    members = [
        PromotionalScopeMember(
            project_id=scope.project_id,
            scope_id=scope.id,
            model_run_id=uuid.uuid4(),
            ordinal=index,
        )
        for index in range(2)
    ]
    runs = [
        ModelRun(
            id=member.model_run_id,
            project_id=scope.project_id,
            dataset_version_id=uuid.uuid4(),
            created_by_id=uuid.uuid4(),
            run_kind=RunKind.TRAINING,
            status=RunStatus.PRECHECK_RUNNING,
            task_type=TaskType.REGRESSION,
            params={},
            tags={"desired_state": "barrier_pending"},
        )
        for member in members
    ]
    commands = []
    reservations = []
    attempts = []
    for run in runs:
        command = _command()
        command.id = uuid.uuid4()
        command.resource_type = "model_run"
        command.resource_id = run.id
        commands.append(command)
        reservations.append(
            CapacityReservation(
                project_id=scope.project_id,
                command_id=command.id,
                resource_class="training",
                cpu_millis=1000,
                memory_bytes=1024,
                gpu_count=0,
                status="held",
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
            )
        )
        attempts.append(_attempt(project_id=scope.project_id, model_run_id=run.id))
    db = MagicMock()
    db.scalar.return_value = scope
    db.scalars.side_effect = [members, runs, commands, reservations, attempts]
    sealed = state.seal_promotional_scope(db, scope.id, expected_cas_version=0)
    assert sealed.status == ScopeStatus.SEALED
    assert sealed.membership_digest
    assert all(member.released_at == sealed.sealed_at for member in members)
    assert all(run.status == RunStatus.QUEUED for run in runs)
    assert all(reservation.status == "consumed" for reservation in reservations)
    assert state.seal_promotional_scope(db, scope.id, expected_cas_version=0) is scope

    scope.status = ScopeStatus.OPEN
    scope.cas_version = 1
    with pytest.raises(state.StaleFence):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)
    scope.cas_version = 0
    db.scalars.side_effect = None
    db.scalars.return_value = members[:1]
    with pytest.raises(state.InvalidTransition, match="requires 2"):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)
    db.scalars.return_value = [members[1], members[0]]
    members[1].ordinal = 2
    with pytest.raises(state.InvalidTransition, match="contiguous"):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)


@pytest.mark.parametrize(
    ("lists", "message"),
    [
        ([[], []], "durable launch command"),
        ([[], [], []], "durable launch command"),
    ],
)
def test_scope_seal_rejects_incomplete_durable_graph(lists, message: str) -> None:
    scope = PromotionalScope(
        project_id=uuid.uuid4(),
        scope_key="broken",
        split_revision_id=uuid.uuid4(),
        canonical_provider="local",
        expected_members=1,
        status=ScopeStatus.OPEN,
        cas_version=0,
    )
    scope.id = uuid.uuid4()
    member = PromotionalScopeMember(
        project_id=scope.project_id,
        scope_id=scope.id,
        model_run_id=uuid.uuid4(),
        ordinal=0,
    )
    db = MagicMock()
    db.scalar.return_value = scope
    db.scalars.side_effect = [[member], *lists]
    with pytest.raises(state.InvalidTransition, match=message):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)


def test_scope_seal_rejects_missing_scope() -> None:
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="scope was not found"):
        state.seal_promotional_scope(db, uuid.uuid4(), expected_cas_version=0)


def test_scope_seal_rejects_missing_reservation_and_attempt() -> None:
    scope = PromotionalScope(
        project_id=uuid.uuid4(),
        scope_key="broken",
        split_revision_id=uuid.uuid4(),
        canonical_provider="local",
        expected_members=1,
        status=ScopeStatus.OPEN,
        cas_version=0,
    )
    scope.id = uuid.uuid4()
    member = PromotionalScopeMember(
        project_id=scope.project_id,
        scope_id=scope.id,
        model_run_id=uuid.uuid4(),
        ordinal=0,
    )
    run = SimpleNamespace(id=member.model_run_id)
    command = _command()
    command.id = uuid.uuid4()
    command.resource_id = run.id
    db = MagicMock()
    db.scalar.return_value = scope
    db.scalars.side_effect = [[member], [run], [command], []]
    with pytest.raises(state.InvalidTransition, match="capacity reservation"):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)

    expired = CapacityReservation(
        project_id=scope.project_id,
        command_id=command.id,
        resource_class="training",
        cpu_millis=1,
        memory_bytes=1,
        gpu_count=0,
        status="held",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    scope.status, scope.cas_version = ScopeStatus.OPEN, 0
    db.scalars.side_effect = [[member], [run], [command], [expired]]
    with pytest.raises(state.InvalidTransition, match="unavailable or expired"):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)

    valid = expired
    valid.expires_at = datetime.now(UTC) + timedelta(minutes=1)
    scope.status, scope.cas_version = ScopeStatus.OPEN, 0
    db.scalars.side_effect = [[member], [run], [command], [valid], []]
    with pytest.raises(state.InvalidTransition, match="initial training attempt"):
        state.seal_promotional_scope(db, scope.id, expected_cas_version=0)


def test_scope_cancel_releases_pending_graph_and_rejects_released_scope() -> None:
    scope = SimpleNamespace(id=uuid.uuid4(), status=ScopeStatus.SEALED, cas_version=0)
    with pytest.raises(state.InvalidTransition, match="released scope"):
        state.cancel_promotional_scope(MagicMock(), scope)
    scope.status = ScopeStatus.OPEN
    member = SimpleNamespace(model_run_id=uuid.uuid4())
    run = SimpleNamespace(id=member.model_run_id, status=None, failure_code=None)
    reservation = SimpleNamespace(status="held", released_at=None)
    db = MagicMock()
    db.scalars.side_effect = [[member], [run], [uuid.uuid4()], [reservation]]
    state.cancel_promotional_scope(db, scope)
    assert run.status == RunStatus.CANCELLED
    assert reservation.status == "released"
    assert scope.status == ScopeStatus.CANCELLED
    empty = SimpleNamespace(id=uuid.uuid4(), status=ScopeStatus.OPEN, cas_version=0)
    db.scalars.side_effect = [[]]
    state.cancel_promotional_scope(db, empty)
    assert empty.status == ScopeStatus.CANCELLED


def test_final_authority_allocation_and_signed_lifecycle(monkeypatch) -> None:
    allocation = authority.FinalTestAllocation(
        split_digest="s" * 64,
        project_reference="project-a",
        scope_id=uuid.uuid4(),
        canonical_provider="aws",
        provider_manifest_digest="m" * 64,
        status="allocated",
        cas_version=0,
    )
    allocation.id = uuid.uuid4()
    db = MagicMock()
    monkeypatch.setattr(authority, "_locked_allocation", lambda *_args: allocation)
    monkeypatch.setattr(authority, "_receipt", lambda *_args: None)
    monkeypatch.setattr(
        authority,
        "_signed_receipt",
        lambda _db, **kwargs: SimpleNamespace(**kwargs, signature="signature"),
    )
    with pytest.raises(authority.ProviderRejected, match="canonical"):
        authority.open_allocation(
            db,
            allocation_id=allocation.id,
            provider="gcp",
            provider_manifest_digest=allocation.provider_manifest_digest,
            request_digest="open",
            signing_secret="secret",
        )
    receipt = authority.open_allocation(
        db,
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest=allocation.provider_manifest_digest,
        request_digest="open",
        signing_secret="secret",
    )
    assert receipt.operation == "open"
    assert allocation.status == "opened"
    with pytest.raises(authority.ProviderRejected, match="manifest"):
        authority.commit_result(
            db,
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="wrong",
            request_digest="commit",
            result_digest="result",
            signing_secret="secret",
        )
    receipt = authority.commit_result(
        db,
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest=allocation.provider_manifest_digest,
        request_digest="commit",
        result_digest="result",
        signing_secret="secret",
    )
    assert receipt.operation == "commit"
    assert allocation.result_digest == "result"


def test_final_authority_replays_and_rejects_changed_requests(monkeypatch) -> None:
    allocation = authority.FinalTestAllocation(
        split_digest="s" * 64,
        project_reference="project-a",
        scope_id=uuid.uuid4(),
        canonical_provider="aws",
        provider_manifest_digest="m" * 64,
        status="opened",
        cas_version=1,
        result_digest="result",
    )
    allocation.id = uuid.uuid4()
    receipt = SimpleNamespace(request_digest="request")
    monkeypatch.setattr(authority, "_locked_allocation", lambda *_args: allocation)
    monkeypatch.setattr(authority, "_receipt", lambda *_args: receipt)
    assert authority.commit_result(
        MagicMock(),
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest="m" * 64,
        request_digest="request",
        result_digest="result",
        signing_secret="secret",
    ) is receipt
    with pytest.raises(state.IdempotencyConflict):
        authority.commit_result(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="different",
            result_digest="result",
            signing_secret="secret",
        )
    receipt.request_digest = "request"
    with pytest.raises(state.IdempotencyConflict, match="committed differently"):
        authority.commit_result(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="request",
            result_digest="other-result",
            signing_secret="secret",
        )


def test_final_authority_failure_and_fence_matrix(monkeypatch) -> None:
    locked_allocation = authority._locked_allocation
    allocation = authority.FinalTestAllocation(
        split_digest="s" * 64,
        project_reference="project-a",
        scope_id=uuid.uuid4(),
        canonical_provider="aws",
        provider_manifest_digest="m" * 64,
        status="committed",
        cas_version=2,
    )
    allocation.id = uuid.uuid4()
    monkeypatch.setattr(authority, "_locked_allocation", lambda *_args: allocation)
    monkeypatch.setattr(authority, "_receipt", lambda *_args: None)
    with pytest.raises(state.InvalidTransition):
        authority.open_allocation(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="open",
            signing_secret="secret",
        )
    allocation.status, allocation.cas_version = "allocated", 1
    with pytest.raises(state.StaleFence):
        authority.open_allocation(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="open",
            signing_secret="secret",
        )
    allocation.status, allocation.cas_version = "opened", 2
    with pytest.raises(state.StaleFence):
        authority.commit_result(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="commit",
            result_digest="result",
            signing_secret="secret",
        )
    allocation.status, allocation.cas_version = "allocated", 0
    signed = SimpleNamespace(request_digest="fail")
    monkeypatch.setattr(authority, "_signed_receipt", lambda *_args, **_kwargs: signed)
    assert authority.fail_allocation(
        MagicMock(),
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest="m" * 64,
        request_digest="fail",
        reason="quality gate",
        signing_secret="secret",
        expected_cas_version=0,
    ) is signed
    assert allocation.status == "failed"

    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError):
        locked_allocation(db, uuid.uuid4())


def test_final_authority_fail_replay_stale_and_commit_state(monkeypatch) -> None:
    allocation = authority.FinalTestAllocation(
        split_digest="s" * 64,
        project_reference="project-a",
        scope_id=uuid.uuid4(),
        canonical_provider="aws",
        provider_manifest_digest="m" * 64,
        status="allocated",
        cas_version=0,
    )
    allocation.id = uuid.uuid4()
    receipt = SimpleNamespace(request_digest="fail")
    monkeypatch.setattr(authority, "_locked_allocation", lambda *_args: allocation)
    monkeypatch.setattr(authority, "_receipt", lambda *_args: receipt)
    assert authority.fail_allocation(
        MagicMock(),
        allocation_id=allocation.id,
        provider="aws",
        provider_manifest_digest="m" * 64,
        request_digest="fail",
        reason="reason",
        signing_secret="secret",
        expected_cas_version=0,
    ) is receipt

    monkeypatch.setattr(authority, "_receipt", lambda *_args: None)
    allocation.cas_version = 1
    with pytest.raises(state.StaleFence):
        authority.fail_allocation(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="fail",
            reason="reason",
            signing_secret="secret",
            expected_cas_version=0,
        )
    allocation.status = "allocated"
    with pytest.raises(state.InvalidTransition):
        authority.commit_result(
            MagicMock(),
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="m" * 64,
            request_digest="commit",
            result_digest="result",
            signing_secret="secret",
        )


def test_receipt_signature_detects_tampering() -> None:
    allocation_id = uuid.uuid4()
    payload = {"cas_version": 1}
    body = authority._receipt_bytes(
        allocation_id=allocation_id,
        operation="open",
        provider="aws",
        request_digest="request",
        payload=payload,
    )
    import hashlib
    import hmac

    receipt = SimpleNamespace(
        allocation_id=allocation_id,
        operation="open",
        provider="aws",
        request_digest="request",
        payload=payload,
        signature=hmac.new(b"secret", body, hashlib.sha256).hexdigest(),
    )
    assert authority.verify_receipt(receipt, "secret")
    receipt.signature = "tampered"
    assert not authority.verify_receipt(receipt, "secret")
