"""Preparation attempt ownership for protected provider-copy verification."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import StaleFence, transition_attempt


def verification_key(import_id):
    return f"provider-import:{import_id}:verify"


def verification_identity(attempt_id):
    return f"provider-verify-{attempt_id.hex}"


def verification_attempt(db, row, *, lock=True):
    query = (
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == row.project_id,
            WorkflowAttempt.logical_key == verification_key(row.id),
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    if lock:
        query = query.with_for_update()
    return db.scalar(query.execution_options(populate_existing=True))


def claim_verification_attempt(db, version, row, command, owner):
    previous = verification_attempt(db, row)
    if previous is not None:
        from automl_api.models.workflows import WorkflowEvent

        managed = db.scalar(
            select(WorkflowEvent.id).where(
                WorkflowEvent.attempt_id == previous.id,
                WorkflowEvent.event_key == "provider_import.submission",
            )
        )
        if managed is not None and previous.cleanup_state != "complete":
            raise StaleFence("The previous verification workload has not finished cleanup")
        if (
            previous.stage != WorkflowStage.PREPARATION
            or previous.dataset_version_id != version.id
            or previous.workload_identity != verification_identity(previous.id)
            or previous.generation >= min(previous.retry_budget, command.max_retries)
        ):
            raise StaleFence("Provider verification replacement changed its authority or budget")
        if previous.status == AttemptStatus.RUNNING:
            if previous.lease_expires_at is None or previous.lease_expires_at > datetime.now(UTC):
                raise StaleFence("The provider verification attempt is still owned")
            transition_attempt(
                previous,
                AttemptStatus.SUPERSEDED,
                fencing_token=previous.fencing_token,
                terminal_reason="Verification lease expired",
            )
        if previous.status not in {AttemptStatus.SUPERSEDED, AttemptStatus.FAILED}:
            raise StaleFence("The provider verification generation cannot be replaced")
        db.flush()
    attempt_id = uuid.uuid4()
    attempt = WorkflowAttempt(
        id=attempt_id,
        project_id=row.project_id,
        dataset_version_id=version.id,
        stage=WorkflowStage.PREPARATION,
        logical_key=verification_key(row.id),
        workload_identity=verification_identity(attempt_id),
        generation=1 if previous is None else previous.generation + 1,
        predecessor_attempt_id=previous.id if previous is not None else None,
        retry_budget=command.max_retries if previous is None else previous.retry_budget,
        retry_count=1,
        fencing_token=uuid.uuid4().hex,
    )
    db.add(attempt)
    db.flush()
    for target in (AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING):
        transition_attempt(attempt, target, fencing_token=attempt.fencing_token)
    attempt.lease_owner = owner
    attempt.lease_expires_at = command.lease_expires_at
    attempt.heartbeat_at = command.heartbeat_at
    db.flush()
    return attempt


def owned_verification_attempt(db, row, attempt_id, fence, owner):
    attempt = verification_attempt(db, row)
    if (
        attempt is None
        or attempt.id != attempt_id
        or attempt.fencing_token != fence
        or attempt.dataset_version_id != row.dataset_version_id
        or attempt.stage != WorkflowStage.PREPARATION
        or attempt.status != AttemptStatus.RUNNING
        or attempt.lease_owner != owner
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= datetime.now(UTC)
    ):
        raise StaleFence("Provider verification attempt ownership changed or expired")
    return attempt


def finish_verification_attempt(db, row, attempt_id, fence, owner, target):
    attempt = verification_attempt(db, row)
    if (
        attempt is not None
        and attempt.id == attempt_id
        and attempt.fencing_token == fence
        and attempt.status == AttemptStatus.RUNNING
        and attempt.lease_owner == owner
    ):
        from automl_api.services.attempt_object_grants import revoke_attempt_grants

        revoke_attempt_grants(db, attempt.id)
        transition_attempt(attempt, target, fencing_token=fence)
        attempt.lease_owner = attempt.lease_expires_at = None
        return True
    return False


def stop_verification_attempt(db, row, *, reason):
    attempt = verification_attempt(db, row)
    if attempt is not None and attempt.status in {
        AttemptStatus.PENDING,
        AttemptStatus.CLAIMED,
        AttemptStatus.SUBMITTED,
        AttemptStatus.RUNNING,
    }:
        from automl_api.services.attempt_object_grants import revoke_attempt_grants

        revoke_attempt_grants(db, attempt.id)
        transition_attempt(
            attempt,
            AttemptStatus.CANCELLED,
            fencing_token=attempt.fencing_token,
            terminal_reason=reason,
        )
        attempt.lease_owner = attempt.lease_expires_at = None
