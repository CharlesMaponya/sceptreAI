"""Persist authority allocation before releasing promotional training."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import select

from automl_api.models.enums import CommandStatus, ScopeStatus
from automl_api.models.workflows import PromotionalScope, WorkflowCommand
from automl_api.services.evaluation_planning import validate_evaluation_policy
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.services.provider_distribution import Contract
from automl_api.services.workflow_state import (
    InvalidTransition,
    begin_command,
    canonical_request_hash,
)
from automl_api.training.champion_refit import Digest

OPERATION = "scope.authority.allocate"


class AllocationRequest(Contract):
    split_digest: Digest
    scope_id: uuid.UUID
    canonical_provider: Literal["aws", "gcp", "azure"]
    provider_manifest_digest: Digest


class ScopeAllocationIntent(Contract):
    split_revision_id: uuid.UUID
    experiment_spec_revision_id: uuid.UUID
    project_id: uuid.UUID
    scope_id: uuid.UUID
    project_reference: str
    evaluation_policy_digest: Digest
    allocation: AllocationRequest


class ScopeAllocationResult(Contract):
    allocation_id: uuid.UUID


def _intent(db, scope):
    if scope.mode != "promotional":
        raise InvalidTransition("Authority allocation requires a promotional scope")
    policy, digest = validate_evaluation_policy(db, scope)
    manifest = FinalDataManifest(**policy.final_data.model_dump(), scope_id=scope.id)
    return ScopeAllocationIntent(
        split_revision_id=scope.split_revision_id,
        experiment_spec_revision_id=scope.experiment_spec_revision_id,
        project_id=scope.project_id,
        scope_id=scope.id,
        project_reference=manifest.project_reference,
        evaluation_policy_digest=digest,
        allocation=AllocationRequest(
            split_digest=manifest.split_digest,
            scope_id=scope.id,
            canonical_provider=manifest.provider,
            provider_manifest_digest=manifest.digest,
        ),
    )


def _command(db, scope):
    return db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == scope.project_id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.idempotency_key == str(scope.id),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _validate(db, scope, command):
    expected = _intent(db, scope).model_dump(mode="json")
    if (
        command is None
        or command.resource_type != "promotional_scope"
        or command.resource_id != scope.id
        or command.request_payload != expected
        or command.request_hash != canonical_request_hash(expected)
    ):
        raise InvalidTransition("Scope authority registration is missing or changed")
    return ScopeAllocationIntent.model_validate(command.request_payload)


def register_scope_allocation(db, scope, *, actor_id):
    """Caller commits this command atomically with scope creation; no HTTP here."""
    if not _live(scope, datetime.now(UTC)):
        raise InvalidTransition("Authority registration requires an open scope")
    payload = _intent(db, scope).model_dump(mode="json")
    command, replay = begin_command(
        db,
        project_id=scope.project_id,
        actor_id=actor_id,
        operation=OPERATION,
        idempotency_key=str(scope.id),
        payload=payload,
    )
    if not replay:
        command.resource_type = "promotional_scope"
        command.resource_id = scope.id
    _validate(db, scope, command)
    return command


def require_scope_allocation(db, scope):
    """Accept only the controller's completed command for this exact scope policy."""
    command = _command(db, scope)
    _validate(db, scope, command)
    if (
        command.status != CommandStatus.SUCCEEDED
        or command.response_status != 200
        or command.lease_owner is not None
        or command.lease_expires_at is not None
    ):
        raise InvalidTransition("Scope authority registration is not complete")
    return ScopeAllocationResult.model_validate(command.response_payload).allocation_id


def _scope(db, scope_id):
    return db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.id == scope_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _live(scope, now):
    return (
        scope is not None
        and scope.mode == "promotional"
        and scope.status == ScopeStatus.OPEN
        and scope.membership_deadline_at > now
    )


def reconcile_scope_allocation(factory, scope_id, authority):
    """Bounded, replay-safe HTTP between short claim/publication transactions."""
    if authority.base_url.scheme != "https":
        raise ValueError("Scope allocation requires HTTPS")
    owner = uuid.uuid4().hex
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        now = datetime.now(UTC)
        if not _live(scope, now):
            return False
        command = _command(db, scope)
        if command is None:
            return False
        if command.status == CommandStatus.SUCCEEDED:
            require_scope_allocation(db, scope)
            return False
        if (
            command.status not in (CommandStatus.PENDING, CommandStatus.RUNNING)
            or command.available_at > now
            or (command.lease_expires_at is not None and command.lease_expires_at > now)
        ):
            return False
        if command.retry_count >= command.max_retries:
            command.status = CommandStatus.FAILED
            command.terminal_reason = "Scope allocation retry budget exhausted"
            command.lease_owner = command.lease_expires_at = None
            return False
        command.status = CommandStatus.RUNNING
        command.lease_owner = owner
        command.heartbeat_at = now
        command.lease_expires_at = now + timedelta(seconds=90)
        command.retry_count += 1
        try:
            intent = _validate(db, scope, command)
        except (InvalidTransition, ValueError):
            command.status = CommandStatus.FAILED
            command.lease_owner = command.lease_expires_at = None
            command.terminal_reason = "Scope allocation intent is invalid"
            return False
    try:
        response = authority.post("allocations", json=intent.allocation.model_dump(mode="json"))
        response.raise_for_status()
        result = ScopeAllocationResult(allocation_id=response.json()["allocation_id"])
        response = authority.get(f"allocations/{result.allocation_id}")
        response.raise_for_status()
        state = response.json()
        expected = intent.allocation.model_dump(mode="json") | {
            "allocation_id": str(result.allocation_id),
            "project_reference": intent.project_reference,
            "status": "allocated",
            "cas_version": 0,
        }
        if any(state.get(key) != value for key, value in expected.items()):
            raise InvalidTransition("Authority allocation differs or final data has already opened")
        with factory() as db, db.begin():
            scope = _scope(db, scope_id)
            command = _command(db, scope) if scope is not None else None
            now = datetime.now(UTC)
            if (
                not _live(scope, now)
                or command is None
                or command.status != CommandStatus.RUNNING
                or command.lease_owner != owner
                or command.lease_expires_at is None
                or command.lease_expires_at <= now
            ):
                raise InvalidTransition("Scope allocation ownership or scope expired")
            if _validate(db, scope, command) != intent:
                raise InvalidTransition("Scope allocation intent changed")
            command.response_payload = result.model_dump(mode="json")
            command.response_status = 200
            command.status = CommandStatus.SUCCEEDED
            command.lease_owner = command.lease_expires_at = None
            command.terminal_reason = None
        return True
    except Exception:
        with factory() as db, db.begin():
            scope = _scope(db, scope_id)
            command = _command(db, scope) if scope is not None else None
            if command is not None and command.lease_owner == owner:
                command.status = (
                    CommandStatus.PENDING
                    if _live(scope, datetime.now(UTC)) and command.retry_count < command.max_retries
                    else CommandStatus.FAILED
                )
                command.lease_owner = command.lease_expires_at = None
                command.available_at = datetime.now(UTC) + timedelta(seconds=60)
                command.terminal_reason = "Scope allocation failed; safe replay required"
        raise


def close_scope_allocation(factory, scope_id, authority, *, public_key):
    """Seal a stopped pre-training allocation, including lost creation replies."""
    from automl_api.services.evaluation_reconciler import _abort_registration

    owner = uuid.uuid4().hex
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        now = datetime.now(UTC)
        if scope is None or scope.status not in (
            ScopeStatus.OPEN,
            ScopeStatus.CANCELLED,
            ScopeStatus.FAILED,
        ):
            return False
        if scope.status == ScopeStatus.OPEN:
            if scope.membership_deadline_at > now:
                return False
            scope.status = ScopeStatus.FAILED
            scope.cas_version += 1
        command = _command(db, scope)
        if command is None or command.status == CommandStatus.CANCELLED:
            return False
        if command.available_at > now or (
            command.lease_expires_at is not None and command.lease_expires_at > now
        ):
            return False
        if command.retry_count == 0:
            command.status = CommandStatus.CANCELLED
            return True
        if authority is None or authority.base_url.scheme != "https":
            raise ValueError("Scope allocation requires HTTPS")
        if command.retry_count >= command.max_retries + 5:
            return False
        intent = ScopeAllocationIntent.model_validate(command.request_payload)
        if (
            command.request_hash != canonical_request_hash(intent.model_dump(mode="json"))
            or command.request_payload != intent.model_dump(mode="json")
            or intent.project_id != scope.project_id
            or intent.scope_id != scope.id
            or intent.allocation.scope_id != scope.id
            or command.resource_type != "promotional_scope"
            or command.resource_id != scope.id
        ):
            raise InvalidTransition("Stopped scope allocation intent changed")
        command.status = CommandStatus.RUNNING
        command.lease_owner = owner
        command.lease_expires_at = now + timedelta(seconds=90)
        command.heartbeat_at = now
        command.retry_count += 1
    try:
        response = authority.post("allocations", json=intent.allocation.model_dump(mode="json"))
        response.raise_for_status()
        allocation_id = ScopeAllocationResult(
            allocation_id=response.json()["allocation_id"]
        ).allocation_id
        receipt = _abort_registration(
            authority,
            allocation_id,
            scope_id,
            intent.allocation.provider_manifest_digest,
            public_key,
        )
        with factory() as db, db.begin():
            scope = _scope(db, scope_id)
            command = _command(db, scope) if scope is not None else None
            if (
                scope is None
                or scope.status not in (ScopeStatus.CANCELLED, ScopeStatus.FAILED)
                or command is None
                or command.lease_owner != owner
                or command.lease_expires_at is None
                or command.lease_expires_at <= datetime.now(UTC)
                or command.request_hash != canonical_request_hash(intent.model_dump(mode="json"))
                or command.request_payload != intent.model_dump(mode="json")
            ):
                raise InvalidTransition("Stopped scope allocation ownership changed")
            command.status = CommandStatus.CANCELLED
            command.response_payload = {
                "allocation_id": str(allocation_id),
                "abort_receipt": receipt,
            }
            command.response_status = 200
            command.lease_owner = command.lease_expires_at = None
            command.terminal_reason = "Pre-training scope allocation sealed"
        return True
    except Exception:
        with factory() as db, db.begin():
            scope = _scope(db, scope_id)
            command = _command(db, scope) if scope is not None else None
            if command is not None and command.lease_owner == owner:
                command.status = CommandStatus.FAILED
                command.lease_owner = command.lease_expires_at = None
                command.available_at = datetime.now(UTC) + timedelta(seconds=60)
                command.terminal_reason = "Stopped scope allocation needs safe cleanup replay"
        raise


def defer_scope_allocation(factory, scope_id):
    """Back off invalid controller configuration without starving other scopes."""
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        command = _command(db, scope) if scope is not None else None
        now = datetime.now(UTC)
        if (
            command is not None
            and command.available_at <= now
            and (command.lease_expires_at is None or command.lease_expires_at <= now)
        ):
            command.available_at = now + timedelta(seconds=60)
