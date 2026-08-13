from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Select, and_, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    RunStatus,
    ScopeStatus,
)
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    CapacityReservation,
    OutboxEntry,
    PromotionalScope,
    PromotionalScopeMember,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowCommand,
)


class IdempotencyConflict(ValueError):
    pass


class InvalidTransition(ValueError):
    pass


class StaleFence(ValueError):
    pass


COMMAND_TRANSITIONS = {
    CommandStatus.PENDING: {CommandStatus.RUNNING, CommandStatus.CANCELLED},
    CommandStatus.RUNNING: {CommandStatus.PENDING, CommandStatus.SUCCEEDED, CommandStatus.FAILED},
    CommandStatus.SUCCEEDED: set(),
    CommandStatus.FAILED: {CommandStatus.PENDING},
    CommandStatus.CANCELLED: set(),
}

ATTEMPT_TRANSITIONS = {
    AttemptStatus.PENDING: {
        AttemptStatus.CLAIMED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    },
    AttemptStatus.CLAIMED: {
        AttemptStatus.PENDING,
        AttemptStatus.SUBMITTED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    },
    AttemptStatus.SUBMITTED: {
        AttemptStatus.RUNNING,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    },
    AttemptStatus.RUNNING: {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    },
    AttemptStatus.SUCCEEDED: set(),
    AttemptStatus.FAILED: set(),
    AttemptStatus.CANCELLED: set(),
    AttemptStatus.SUPERSEDED: set(),
}


def canonical_request_hash(payload: Mapping[str, Any] | list[Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def begin_command(
    db: Session,
    *,
    project_id: uuid.UUID,
    actor_id: uuid.UUID,
    operation: str,
    idempotency_key: str,
    payload: Mapping[str, Any],
) -> tuple[WorkflowCommand, bool]:
    if not idempotency_key.strip():
        raise ValueError("Idempotency-Key must not be empty.")

    request_hash = canonical_request_hash(payload)
    existing = _command_by_key(db, project_id, operation, idempotency_key)
    if existing is not None:
        return _validate_replay(existing, request_hash), True

    command = WorkflowCommand(
        project_id=project_id,
        actor_id=actor_id,
        operation=operation,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        request_payload=dict(payload),
    )
    savepoint = db.begin_nested()
    try:
        db.add(command)
        db.flush()
        savepoint.commit()
        return command, False
    except IntegrityError:
        savepoint.rollback()
        existing = _command_by_key(db, project_id, operation, idempotency_key)
        if existing is None:
            raise
        return _validate_replay(existing, request_hash), True


def _command_by_key(
    db: Session, project_id: uuid.UUID, operation: str, idempotency_key: str
) -> WorkflowCommand | None:
    return db.scalar(
        select(WorkflowCommand).where(
            WorkflowCommand.project_id == project_id,
            WorkflowCommand.operation == operation,
            WorkflowCommand.idempotency_key == idempotency_key,
        )
    )


def _validate_replay(command: WorkflowCommand, request_hash: str) -> WorkflowCommand:
    if command.request_hash != request_hash:
        raise IdempotencyConflict(
            "This Idempotency-Key was already used with a different request payload."
        )
    return command


def enqueue_outbox(
    db: Session,
    command: WorkflowCommand,
    *,
    topic: str,
    aggregate_type: str,
    aggregate_id: uuid.UUID,
    payload: Mapping[str, Any],
    event_key: str | None = None,
) -> OutboxEntry:
    key = event_key or f"{command.operation}:{command.id}:{topic}"
    existing = db.scalar(
        select(OutboxEntry).where(
            OutboxEntry.project_id == command.project_id,
            OutboxEntry.event_key == key,
        )
    )
    if existing is not None:
        return existing

    entry = OutboxEntry(
        project_id=command.project_id,
        command_id=command.id,
        event_key=key,
        topic=topic,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload=dict(payload),
    )
    db.add(entry)
    db.flush()
    return entry


def claim_outbox(
    db: Session,
    *,
    worker_id: str,
    limit: int = 25,
    lease_seconds: int = 60,
    now: datetime | None = None,
) -> list[OutboxEntry]:
    claimed_at = now or datetime.now(UTC)
    rows = list(
        db.scalars(
            _claimable_outbox(claimed_at)
            .order_by(OutboxEntry.available_at, OutboxEntry.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    )
    for row in rows:
        row.status = OutboxStatus.CLAIMED
        row.lease_owner = worker_id
        row.lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
        row.delivery_attempts += 1
    db.flush()
    return rows


def _claimable_outbox(now: datetime) -> Select[tuple[OutboxEntry]]:
    return select(OutboxEntry).where(
        OutboxEntry.available_at <= now,
        (
            (OutboxEntry.status == OutboxStatus.PENDING)
            | and_(
                OutboxEntry.status == OutboxStatus.CLAIMED,
                OutboxEntry.lease_expires_at < now,
            )
        ),
    )


def complete_outbox(
    db: Session,
    entry_id: uuid.UUID,
    *,
    worker_id: str,
    delivered: bool,
    error: str | None = None,
    now: datetime | None = None,
) -> OutboxEntry:
    row = db.get(OutboxEntry, entry_id)
    if row is None:
        raise LookupError("Outbox entry was not found.")
    if row.status == OutboxStatus.DELIVERED:
        return row
    if row.status != OutboxStatus.CLAIMED or row.lease_owner != worker_id:
        raise StaleFence("The outbox lease is not owned by this worker.")

    completed_at = now or datetime.now(UTC)
    row.lease_owner = None
    row.lease_expires_at = None
    if delivered:
        row.status = OutboxStatus.DELIVERED
        row.delivered_at = completed_at
        row.last_error = None
    else:
        row.last_error = error or "delivery failed"
        command = db.get(WorkflowCommand, row.command_id)
        retry_budget = command.max_retries if command is not None else 0
        if command is None or row.delivery_attempts >= retry_budget:
            row.status = OutboxStatus.DEAD
            if command is not None:
                command.retry_count = row.delivery_attempts
                command.terminal_reason = row.last_error
                transition_command(command, CommandStatus.FAILED)
        else:
            row.status = OutboxStatus.PENDING
            row.available_at = completed_at
            command.retry_count = row.delivery_attempts
    db.flush()
    return row


def replay_dead_outbox(
    db: Session,
    entry_id: uuid.UUID,
    *,
    actor_id: uuid.UUID,
    now: datetime | None = None,
) -> OutboxEntry:
    row = db.scalar(select(OutboxEntry).where(OutboxEntry.id == entry_id).with_for_update())
    if row is None:
        raise LookupError("Outbox entry was not found.")
    if row.status != OutboxStatus.DEAD:
        raise InvalidTransition("Only a dead outbox entry can be replayed.")
    command = db.scalar(
        select(WorkflowCommand)
        .where(WorkflowCommand.id == row.command_id)
        .with_for_update()
    )
    if command is None:
        raise LookupError("The outbox command was not found.")
    transition_command(command, CommandStatus.PENDING)
    transition_command(command, CommandStatus.RUNNING)
    command.replayed_by = actor_id
    command.retry_count = 0
    command.terminal_reason = None
    row.status = OutboxStatus.PENDING
    row.available_at = now or datetime.now(UTC)
    row.delivery_attempts = 0
    row.delivered_at = None
    row.last_error = None
    db.flush()
    return row


def transition_command(command: WorkflowCommand, target: CommandStatus) -> None:
    if target == command.status:
        return
    if target not in COMMAND_TRANSITIONS[command.status]:
        raise InvalidTransition(f"Cannot transition command from {command.status} to {target}.")
    command.status = target


def transition_attempt(
    attempt: WorkflowAttempt,
    target: AttemptStatus,
    *,
    fencing_token: str,
    terminal_reason: str | None = None,
    expected_cas_version: int | None = None,
) -> None:
    if fencing_token != attempt.fencing_token:
        raise StaleFence("The attempt fencing token is stale.")
    if expected_cas_version is not None and expected_cas_version != attempt.terminal_cas_version:
        raise StaleFence("The attempt terminal CAS version is stale.")
    if target == attempt.status:
        return
    if target not in ATTEMPT_TRANSITIONS[attempt.status]:
        raise InvalidTransition(f"Cannot transition attempt from {attempt.status} to {target}.")
    attempt.status = target
    if target in {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    }:
        attempt.terminal_reason = terminal_reason
        attempt.terminal_cas_version += 1


def heartbeat_attempt(
    attempt: WorkflowAttempt,
    *,
    worker_id: str,
    fencing_token: str,
    lease_seconds: int,
    now: datetime | None = None,
) -> None:
    if attempt.lease_owner != worker_id or attempt.fencing_token != fencing_token:
        raise StaleFence("The attempt lease or fencing token is stale.")
    heartbeat_at = now or datetime.now(UTC)
    attempt.heartbeat_at = heartbeat_at
    attempt.lease_expires_at = heartbeat_at + timedelta(seconds=lease_seconds)


def supersede_trial_attempts(
    db: Session,
    *,
    project_id: uuid.UUID,
    model_run_id: uuid.UUID,
    terminal_reason: str,
) -> int:
    active = {
        AttemptStatus.PENDING,
        AttemptStatus.CLAIMED,
        AttemptStatus.SUBMITTED,
        AttemptStatus.RUNNING,
    }
    rows = list(
        db.scalars(
            select(WorkflowAttempt)
            .join(TrainingTrial, WorkflowAttempt.trial_id == TrainingTrial.id)
            .where(
                WorkflowAttempt.project_id == project_id,
                WorkflowAttempt.model_run_id == model_run_id,
                WorkflowAttempt.status.in_(active),
            )
            .with_for_update()
        )
    )
    for row in rows:
        row.status = AttemptStatus.SUPERSEDED
        row.terminal_reason = terminal_reason
        row.terminal_cas_version += 1
        row.lease_owner = None
        row.lease_expires_at = None
    db.flush()
    return len(rows)


def seal_promotional_scope(
    db: Session,
    scope_id: uuid.UUID,
    *,
    expected_cas_version: int,
) -> PromotionalScope:
    scope = db.scalar(
        select(PromotionalScope).where(PromotionalScope.id == scope_id).with_for_update()
    )
    if scope is None:
        raise LookupError("Promotional scope was not found.")
    if scope.status == ScopeStatus.SEALED:
        return scope
    if scope.status != ScopeStatus.OPEN or scope.cas_version != expected_cas_version:
        raise StaleFence("The promotional scope version is stale.")

    members = list(
        db.scalars(
            select(PromotionalScopeMember)
            .where(PromotionalScopeMember.scope_id == scope.id)
            .order_by(PromotionalScopeMember.ordinal)
            .with_for_update()
        )
    )
    if len(members) != scope.expected_members:
        raise InvalidTransition(
            f"Scope requires {scope.expected_members} members but has {len(members)}."
        )
    ordinals = [member.ordinal for member in members]
    if ordinals != list(range(scope.expected_members)):
        raise InvalidTransition("Scope member ordinals are not exact and contiguous.")

    scope.membership_digest = canonical_request_hash(
        [
            {"ordinal": member.ordinal, "model_run_id": str(member.model_run_id)}
            for member in members
        ]
    )
    started_at = datetime.now(UTC)
    scope.status = ScopeStatus.SEALED
    scope.sealed_at = started_at
    scope.scope_started_at = started_at
    scope.scope_deadline_at = started_at + timedelta(seconds=7_200)
    scope.cas_version = (scope.cas_version or 0) + 1
    for member in members:
        member.released_at = started_at

    run_ids = [member.model_run_id for member in members]
    runs = list(
        db.scalars(
            select(ModelRun).where(ModelRun.id.in_(run_ids)).with_for_update()
        )
    )
    commands = list(
        db.scalars(
            select(WorkflowCommand)
            .where(
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.resource_id.in_(run_ids),
            )
            .with_for_update()
        )
    )
    command_by_run = {command.resource_id: command for command in commands}
    if len(runs) != len(run_ids) or set(command_by_run) != set(run_ids):
        raise InvalidTransition("Every scope member must have one durable launch command.")

    reservations = list(
        db.scalars(
            select(CapacityReservation)
            .where(CapacityReservation.command_id.in_([command.id for command in commands]))
            .with_for_update()
        )
    )
    if len(reservations) != len(run_ids):
        raise InvalidTransition("Every scope member must have one capacity reservation.")
    if any(row.status != "held" or row.expires_at <= started_at for row in reservations):
        raise InvalidTransition("A scope capacity reservation is unavailable or expired.")

    attempts = list(
        db.scalars(
            select(WorkflowAttempt).where(
                WorkflowAttempt.model_run_id.in_(run_ids),
                WorkflowAttempt.stage == "training_run",
                WorkflowAttempt.generation == 1,
            )
        )
    )
    attempt_by_run = {attempt.model_run_id: attempt for attempt in attempts}
    if set(attempt_by_run) != set(run_ids):
        raise InvalidTransition("Every scope member must have one initial training attempt.")

    for reservation in reservations:
        reservation.status = "consumed"
    for run in runs:
        command = command_by_run[run.id]
        attempt = attempt_by_run[run.id]
        run.status = RunStatus.QUEUED
        run.tags = {
            **(run.tags or {}),
            "desired_state": "ray_submission_pending",
            "scope_started_at": started_at.isoformat(),
            "scope_deadline_at": scope.scope_deadline_at.isoformat(),
        }
        enqueue_outbox(
            db,
            command,
            topic="ray.training.submit",
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
            event_key=f"scope:{scope.id}:run:{run.id}:submit",
        )
    db.flush()
    return scope


def cancel_promotional_scope(db: Session, scope: PromotionalScope) -> None:
    """Cancel a pre-test scope and atomically release all held capacity."""
    if scope.status in {ScopeStatus.SUCCEEDED, ScopeStatus.FAILED}:
        raise InvalidTransition("A terminal scope cannot be cancelled.")
    if scope.status in {ScopeStatus.SEALED, ScopeStatus.RUNNING}:
        raise InvalidTransition("A released scope requires workflow cancellation.")
    members = list(
        db.scalars(
            select(PromotionalScopeMember)
            .where(PromotionalScopeMember.scope_id == scope.id)
            .with_for_update()
        )
    )
    run_ids = [member.model_run_id for member in members]
    if run_ids:
        runs = list(db.scalars(select(ModelRun).where(ModelRun.id.in_(run_ids)).with_for_update()))
        for run in runs:
            run.status = RunStatus.CANCELLED
            run.failure_code = "scope_cancelled"
        command_ids = list(
            db.scalars(
                select(WorkflowCommand.id).where(
                    WorkflowCommand.resource_type == "model_run",
                    WorkflowCommand.resource_id.in_(run_ids),
                )
            )
        )
        if command_ids:
            reservations = list(
                db.scalars(
                    select(CapacityReservation)
                    .where(
                        CapacityReservation.command_id.in_(command_ids),
                        CapacityReservation.status == "held",
                    )
                    .with_for_update()
                )
            )
            for reservation in reservations:
                reservation.status = "released"
                reservation.released_at = datetime.now(UTC)
    scope.status = ScopeStatus.CANCELLED
    scope.cas_version = (scope.cas_version or 0) + 1
    db.flush()


def claim_attempts(
    db: Session,
    *,
    worker_id: str,
    limit: int = 10,
    lease_seconds: int = 60,
    now: datetime | None = None,
) -> list[WorkflowAttempt]:
    claimed_at = now or datetime.now(UTC)
    rows = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.available_at <= claimed_at,
                (
                    (WorkflowAttempt.status == AttemptStatus.PENDING)
                    | and_(
                        WorkflowAttempt.status == AttemptStatus.CLAIMED,
                        WorkflowAttempt.lease_expires_at < claimed_at,
                    )
                ),
                WorkflowAttempt.retry_count < WorkflowAttempt.retry_budget,
            )
            .order_by(WorkflowAttempt.available_at, WorkflowAttempt.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    )
    for row in rows:
        row.status = AttemptStatus.CLAIMED
        row.lease_owner = worker_id
        row.lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
        row.heartbeat_at = claimed_at
        row.retry_count += 1
    db.flush()
    return rows


def cas_register_terminal_artifact(
    db: Session,
    *,
    attempt_id: uuid.UUID,
    fencing_token: str,
    expected_cas_version: int,
    checkpoint_uri: str,
) -> bool:
    result = db.execute(
        update(WorkflowAttempt)
        .where(
            WorkflowAttempt.id == attempt_id,
            WorkflowAttempt.fencing_token == fencing_token,
            WorkflowAttempt.terminal_cas_version == expected_cas_version,
            WorkflowAttempt.status.not_in(
                {
                    AttemptStatus.SUCCEEDED,
                    AttemptStatus.CANCELLED,
                    AttemptStatus.SUPERSEDED,
                }
            ),
        )
        .values(
            checkpoint_uri=checkpoint_uri,
            status=AttemptStatus.SUCCEEDED,
            terminal_cas_version=WorkflowAttempt.terminal_cas_version + 1,
        )
    )
    return bool(result.rowcount == 1)


def unfinished_count(db: Session) -> int:
    return int(
        db.scalar(
            select(func.count())
            .select_from(WorkflowAttempt)
            .where(
                WorkflowAttempt.status.in_(
                    {
                        AttemptStatus.PENDING,
                        AttemptStatus.CLAIMED,
                        AttemptStatus.SUBMITTED,
                        AttemptStatus.RUNNING,
                    }
                )
            )
        )
        or 0
    )
