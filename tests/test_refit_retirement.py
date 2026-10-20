"""Refit replacements wait for worker cleanup and recorded native capability expiry."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowAttemptCredentialGrant
from automl_api.services.champion_planning import plan_scope_refit
from sqlalchemy import select
from test_attempt_object_grant_schema import grant_values

pytest_plugins = ["test_champion_refit_jobs"]


def test_refit_replacement_waits_for_confirmed_predecessor_cleanup(planning_case):
    db, store, _, scope, _ = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    db.flush()
    assert first.cleanup_state != "complete"
    assert plan_scope_refit(db, scope.id, store) is None
    assert (
        len(list(db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.scope_id == scope.id))))
        == 1
    )
    first.cleanup_state = "complete"
    db.flush()
    replacement = plan_scope_refit(db, scope.id, store)
    assert replacement.generation == 2 and replacement.predecessor_attempt_id == first.id


@pytest.mark.parametrize("method,role", [("GET", "candidate"), ("PUT", "pipeline")])
def test_refit_replacement_waits_for_started_capability_even_after_cleanup(
    planning_case, method, role
):
    db, store, _, scope, _ = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    first.cleanup_state = "complete"
    now = datetime.now(UTC)
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            first,
            method=method,
            role=role,
            object_version="v1" if method == "GET" else None,
            status="issued",
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    db.add(row)
    db.flush()
    deadline = row.expires_at
    assert plan_scope_refit(db, scope.id, store) is None
    db.refresh(row)
    assert row.status == "revoked" and row.expires_at == deadline


@pytest.fixture
def grant_clock(monkeypatch):
    from automl_api.services import attempt_object_grants

    state = [datetime.now(UTC)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return state[0]

    monkeypatch.setattr(attempt_object_grants, "datetime", Clock)
    return state


def add_grant(db, attempt, now, method, status="issued", *, started=True):
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            method=method,
            role="candidate" if method == "GET" else "pipeline",
            object_version="v1" if method == "GET" else None,
            status=status,
            issuance_owner=uuid.uuid4() if started else None,
            issuance_started_at=now if started else None,
            issued_at=now if status == "issued" else None,
            revoked_at=now if status == "revoked" else None,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    db.add(row)
    db.flush()
    return row


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("status", ["issuing", "issued", "unknown", "failed", "revoked", "expired"])
@pytest.mark.parametrize("sweep", [False, True])
def test_cleanup_and_sweep_preserve_dependencies_until_original_deadline(
    jobs_case, grant_clock, method, status, sweep
):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services import refit_jobs

    db, store, scope, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    row = add_grant(db, attempt, grant_clock[0], method, status)
    deadline = row.expires_at
    name = f"refit-{attempt.id.hex}"
    for _ in range(2):
        if sweep:
            refit_jobs.sweep_refit_resources(factory, k8s)
        else:
            refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    db.refresh(row)
    assert name not in k8s.objects["job"]
    assert name in k8s.objects["secret"] and name in k8s.objects["network_policy"]
    assert row.status == ("expired" if status == "expired" else "revoked")
    assert row.expires_at == deadline
    assert attempt.cleanup_state != "complete"
    assert plan_scope_refit(db, scope.id, store) is None
    markers = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_key == "attempt.credentials.revoked",
            )
        )
    )
    assert len(markers) == 1
    grant_clock[0] = deadline
    if sweep:
        refit_jobs.sweep_refit_resources(factory, k8s)
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    db.refresh(row)
    assert attempt.cleanup_state == "complete" and not any(k8s.objects.values())
    assert row.expires_at == deadline
    replacement = plan_scope_refit(db, scope.id, store)
    assert replacement.generation == 2 and replacement.predecessor_attempt_id == attempt.id
    assert plan_scope_refit(db, scope.id, store).id == replacement.id


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("started", [False, True])
def test_unstarted_or_physically_expired_grants_do_not_block_replacement(
    planning_case, grant_clock, method, started
):
    db, store, _, scope, _ = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    first.cleanup_state = "complete"
    row = add_grant(
        db, first, grant_clock[0], method, "issued" if started else "pending", started=started
    )
    original = row.expires_at
    if started:
        grant_clock[0] = original
    replacement = plan_scope_refit(db, scope.id, store)
    assert replacement.generation == 2
    db.refresh(row)
    assert row.status == "revoked" and row.expires_at == original


def test_retirement_does_not_revoke_another_attempt(planning_case):
    from automl_api.models.enums import WorkflowStage

    db, store, _, scope, _ = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    first.cleanup_state = "complete"
    other = db.scalar(
        select(WorkflowAttempt).where(
            WorkflowAttempt.project_id == scope.project_id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
    )
    now = datetime.now(UTC)
    grant = WorkflowAttemptCredentialGrant(
        **grant_values(
            other,
            status="issued",
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
        )
    )
    db.add(grant)
    db.flush()
    assert plan_scope_refit(db, scope.id, store).generation == 2
    db.refresh(grant)
    assert grant.status == "issued" and grant.revoked_at is None


@pytest.mark.parametrize("method", ["GET", "PUT"])
def test_missing_submission_revokes_recorded_grants_without_guessing_cleanup(
    jobs_case, grant_clock, method
):
    from copy import deepcopy

    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services import refit_jobs

    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs._reconcile(factory, k8s, attempt.id)
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    row = add_grant(db, attempt, grant_clock[0], method, "unknown")
    original = row.expires_at
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "refit_submission"
        )
    )
    db.delete(event)
    db.flush()
    resources = deepcopy(k8s.objects)
    for now in (grant_clock[0], original):
        grant_clock[0] = now
        refit_jobs._reconcile(factory, k8s, attempt.id)
        db.refresh(attempt)
        db.refresh(row)
        assert row.status == "revoked" and row.expires_at == original
        assert attempt.cleanup_state != "complete" and k8s.objects == resources
