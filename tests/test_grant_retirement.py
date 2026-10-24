"""Native-deadline retirement before destructive project/run cleanup (no provider I/O)."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttemptCredentialGrant, WorkflowEvent
from automl_api.services import attempt_object_grants, deletion
from automl_api.storage.contracts import DeletionPending
from sqlalchemy import select
from test_attempt_object_grant_schema import grant_values, make_attempt

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def clock(monkeypatch):
    state = [datetime.now(UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return state[0]

    monkeypatch.setattr(deletion, "datetime", Clock)
    monkeypatch.setattr(attempt_object_grants, "datetime", Clock)
    return state


def started_grant(attempt, now, **updates):
    return WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            status="issued",
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
            created_at=now,
            expires_at=now + timedelta(seconds=60),
            **updates,
        )
    )


@pytest.mark.parametrize(
    "stage,get_role,put_role",
    [
        (WorkflowStage.SPLITTER, "raw", "manifest"),
        (WorkflowStage.PREPARATION, "train", "prepared"),
        (WorkflowStage.TRAINING_RUN, "train", "model"),
        (WorkflowStage.TRAINING_TRIAL, "train", "model"),
        (WorkflowStage.CHAMPION_REFIT, "candidate", "pipeline"),
        (WorkflowStage.CHAMPION_EVALUATION, "pipeline", "result"),
        (WorkflowStage.PROVIDER_CONFORMANCE, "synthetic", "conformance_evidence"),
    ],
)
@pytest.mark.parametrize("method", ["GET", "PUT"])
def test_retirement_covers_every_stage_and_method(
    phase1_db, tenant, clock, stage, get_role, put_role, method
):
    db = phase1_db
    user, project = tenant
    attempt = make_attempt(db, user, project, stage)
    attempt.status = AttemptStatus.FAILED
    row = started_grant(
        attempt,
        clock[0],
        method=method,
        role=get_role if method == "GET" else put_role,
        object_version="version-1" if method == "GET" else None,
    )
    db.add(row)
    db.flush()
    original_deadline = row.expires_at
    for _ in range(2):
        with pytest.raises(DeletionPending) as waiting:
            deletion._retire_deletion_grants(db, project.id, None)
        assert waiting.value.retry_after_seconds == 60
        db.refresh(row)
        assert row.status == "revoked" and row.revoked_at is not None
        assert row.expires_at == original_deadline
    markers = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_key == "attempt.credentials.revoked",
            )
        )
    )
    assert len(markers) == 1
    clock[0] = original_deadline
    if method == "PUT" or stage in {
        WorkflowStage.CHAMPION_REFIT,
        WorkflowStage.CHAMPION_EVALUATION,
    }:
        # Schema fixtures prove neither native write completion nor legacy retirement.
        with pytest.raises(DeletionPending, match="Legacy champion"):
            deletion._retire_deletion_grants(db, project.id, None)
    else:
        deletion._retire_deletion_grants(db, project.id, None)
    assert row.expires_at == original_deadline


@pytest.mark.parametrize(
    "status", ["pending", "issuing", "issued", "unknown", "failed", "revoked", "expired"]
)
def test_retirement_uses_original_started_deadline_instead_of_logical_status(
    phase1_db, tenant, clock, status
):
    db = phase1_db
    user, project = tenant
    attempt = make_attempt(db, user, project)
    attempt.status = AttemptStatus.FAILED
    values = grant_values(
        attempt, status=status, created_at=clock[0], expires_at=clock[0] + timedelta(seconds=60)
    )
    if status != "pending":
        values.update(issuance_owner=uuid.uuid4(), issuance_started_at=clock[0])
    if status == "issued":
        values["issued_at"] = clock[0]
    if status == "revoked":
        values["revoked_at"] = clock[0]
    row = WorkflowAttemptCredentialGrant(**values)
    db.add(row)
    db.flush()
    if status == "pending":
        deletion._retire_deletion_grants(db, project.id, None)
        assert row.status == "revoked" and row.issuance_started_at is None
    else:
        with pytest.raises(DeletionPending):
            deletion._retire_deletion_grants(db, project.id, None)
        clock[0] = row.expires_at
        deletion._retire_deletion_grants(db, project.id, None)


def test_retirement_waits_for_latest_original_deadline(phase1_db, tenant, clock):
    db = phase1_db
    user, project = tenant
    first = make_attempt(db, user, project)
    first.status = AttemptStatus.SUPERSEDED
    second = make_attempt(db, user, project)
    second.status = AttemptStatus.FAILED
    second.logical_key = first.logical_key
    second.model_run_id = first.model_run_id
    second.generation = first.generation + 1
    second.predecessor_attempt_id = first.id
    rows = [started_grant(a, clock[0]) for a in (first, second)]
    rows[1].expires_at = clock[0] + timedelta(seconds=10)
    db.add_all(rows)
    db.flush()
    with pytest.raises(DeletionPending) as waiting:
        deletion._retire_deletion_grants(db, project.id, [first.model_run_id])
    assert waiting.value.retry_after_seconds == 60
    clock[0] += timedelta(seconds=10)
    with pytest.raises(DeletionPending) as waiting:
        deletion._retire_deletion_grants(db, project.id, [first.model_run_id])
    assert waiting.value.retry_after_seconds == 50
    clock[0] += timedelta(seconds=50)
    deletion._retire_deletion_grants(db, project.id, [first.model_run_id])
