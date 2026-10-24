"""Evaluator replacements and cleanup honor recorded predecessor capability expiry."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttemptCredentialGrant
from automl_api.services.evaluation_access import locked_evaluation
from automl_api.services.evaluation_jobs import submit_evaluator
from automl_api.services.evaluation_keys import prepare_evaluator_credentials
from automl_api.services.evaluation_reconciler import _cleanup_attempt
from automl_api.services.workflow_state import InvalidTransition
from test_attempt_object_grant_schema import grant_values
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case
from test_refit_retirement import grant_clock as grant_clock

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


def prepare(c):
    return prepare_evaluator_credentials(
        c.factory, c.scope.id, c.k8s, c.authority, authority_public_key=c.public_key, generation=2
    )


def started_grant(c, method):
    now = datetime.now(UTC)
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            c.attempt,
            method=method,
            role="pipeline" if method == "GET" else "result",
            object_version="v1" if method == "GET" else None,
            status="issued",
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    c.db.add(row)
    c.db.flush()
    return row


@pytest.fixture
def stopped_evaluator(evaluator_job_case, handoff_case):
    c = evaluator_job_case
    c.public_key = handoff_case[4]
    c.attempt.status = AttemptStatus.FAILED
    c.db.flush()
    c.k8s.before_create = lambda: None
    return c


@pytest.mark.parametrize("barrier", ["cleanup", "GET", "PUT"])
def test_direct_key_preparation_requires_retired_predecessor(stopped_evaluator, barrier):
    c = stopped_evaluator
    if barrier != "cleanup":
        assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
        c.db.refresh(c.attempt)
        started_grant(c, barrier)
    c.db.flush()
    keys = set(c.k8s.objects["secret"])
    with pytest.raises(InvalidTransition):
        prepare(c)
    assert set(c.k8s.objects["secret"]) == keys


@pytest.mark.parametrize("path", ["submit", "control"])
def test_registered_replacement_cannot_bypass_predecessor_cleanup(stopped_evaluator, path):
    from automl_api.models.workflows import WorkflowAttempt

    c = stopped_evaluator
    assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    c.db.refresh(c.attempt)
    plan, token, key = prepare(c)
    replacement = c.db.get(WorkflowAttempt, plan.attempt_id)
    claims = {"project_id": plan.project_id, "fence": replacement.fencing_token}
    assert (
        locked_evaluation(c.db, replacement.id, claims, (AttemptStatus.PENDING,))[1].id
        == replacement.id
    )
    c.attempt.cleanup_state = "pending"  # Model a replacement persisted by a predecessor binary.
    c.db.flush()
    with pytest.raises(InvalidTransition):
        if path == "submit":
            submit_evaluator(c.factory, c.k8s, plan, token, key, fence=replacement.fencing_token)
        else:
            locked_evaluation(c.db, replacement.id, claims, (AttemptStatus.PENDING,))
    assert not c.k8s.objects["job"]


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("status", ["issuing", "unknown", "revoked", "expired"])
@pytest.mark.parametrize("mode", ["terminal", "sweep", "key_only"])
def test_cleanup_and_key_sweeps_wait_for_original_recorded_expiry(
    evaluator_job_case, handoff_case, grant_clock, method, status, mode
):
    from automl_api.models.enums import ScopeStatus
    from automl_api.services.evaluation_reconciler import sweep_evaluation_resources
    from test_champion_evaluation_jobs import submit

    c = evaluator_job_case
    c.public_key = handoff_case[4]
    desired = submit(c)
    name = desired["job"]["metadata"]["name"]
    c.db.refresh(c.attempt)
    c.attempt.status = AttemptStatus.FAILED
    now = grant_clock[0]
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            c.attempt,
            method=method,
            role="pipeline" if method == "GET" else "result",
            object_version="v1" if method == "GET" else None,
            status=status,
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            revoked_at=now if status == "revoked" else None,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    c.db.add(row)
    c.db.flush()
    deadline = row.expires_at
    if mode == "key_only":
        c.k8s.objects["job"].clear()
        c.k8s.objects["network_policy"].clear()
        del c.k8s.objects["secret"][name]
        c.scope.status = ScopeStatus.FAILED
        c.db.flush()
    for _ in range(2):
        if mode == "terminal":
            assert not _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
        else:
            sweep_evaluation_resources(c.factory, c.k8s)
    c.db.refresh(row)
    c.db.refresh(c.attempt)
    assert row.expires_at == deadline
    assert row.status == ("expired" if status == "expired" else "revoked")
    assert c.key in c.k8s.objects["secret"] and not c.k8s.objects["job"]
    if mode != "key_only":
        assert name in c.k8s.objects["secret"] and name in c.k8s.objects["network_policy"]
    assert c.attempt.cleanup_state != "complete"
    grant_clock[0] = deadline
    if mode != "terminal":
        sweep_evaluation_resources(c.factory, c.k8s)
    if method == "PUT":
        assert not _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
        c.db.refresh(c.attempt)
        c.db.refresh(row)
        assert c.attempt.cleanup_state != "complete" and row.expires_at == deadline
        assert c.key in c.k8s.objects["secret"]
        with pytest.raises(InvalidTransition):
            prepare(c)
        return
    assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    assert not any(c.k8s.objects.values())
    c.db.refresh(c.attempt)
    c.db.refresh(row)
    assert c.attempt.cleanup_state == "complete" and row.expires_at == deadline


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("started", [False, True])
def test_unstarted_grants_and_expired_reads_allow_replacement_but_unknown_puts_block(
    stopped_evaluator, grant_clock, method, started
):
    c = stopped_evaluator
    assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    c.db.refresh(c.attempt)
    now = grant_clock[0]
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            c.attempt,
            method=method,
            role="pipeline" if method == "GET" else "result",
            object_version="v1" if method == "GET" else None,
            status="issued" if started else "pending",
            issuance_owner=uuid.uuid4() if started else None,
            issuance_started_at=now if started else None,
            issued_at=now if started else None,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    c.db.add(row)
    c.db.flush()
    original = row.expires_at
    if started:
        grant_clock[0] = original
    if started and method == "PUT":
        with pytest.raises(InvalidTransition):
            prepare(c)
        c.db.refresh(row)
        assert row.expires_at == original
        return
    plan, _, key = prepare(c)
    assert plan.attempt_id != c.attempt.id and key != c.key
    assert prepare(c)[0] == plan
    c.db.refresh(row)
    assert row.expires_at == original


def test_completed_publication_replay_keeps_its_existing_terminal_identity(stopped_evaluator):
    from automl_api.models.enums import ScopeStatus
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.evaluation_publication import _locked

    c = stopped_evaluator
    assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    c.db.refresh(c.attempt)
    plan, _, _ = prepare(c)
    replacement = c.db.get(WorkflowAttempt, plan.attempt_id)
    replacement.status = AttemptStatus.SUCCEEDED
    c.scope.status = ScopeStatus.SUCCEEDED
    c.attempt.cleanup_state = "pending"
    c.db.flush()
    assert _locked(c.db, plan, replacement.fencing_token)[1].id == replacement.id


@pytest.mark.parametrize("method", ["GET", "PUT"])
def test_missing_custody_revokes_grants_without_acknowledging_cleanup(
    stopped_evaluator, grant_clock, method
):
    from copy import deepcopy

    from automl_api.models.enums import WorkflowStage
    from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
    from sqlalchemy import select

    c = stopped_evaluator
    row = started_grant(c, method)
    original = row.expires_at
    custody = c.db.scalar(
        select(WorkflowEvent)
        .join(WorkflowAttempt)
        .where(
            WorkflowAttempt.scope_id == c.scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
            WorkflowEvent.event_key == "evaluation_key_ready",
        )
    )
    assert custody is not None
    c.db.delete(custody)
    c.db.flush()
    resources = deepcopy(c.k8s.objects)
    for now in (grant_clock[0], original):
        grant_clock[0] = now
        assert not _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
        c.db.refresh(c.attempt)
        c.db.refresh(row)
        assert c.attempt.cleanup_state != "complete" and c.k8s.objects == resources
        assert row.status == "revoked" and row.expires_at == original


@pytest.mark.parametrize("method", ["GET", "PUT"])
@pytest.mark.parametrize("sweep", [False, True])
def test_other_stage_grants_are_neither_revoked_nor_used_to_block_this_key(
    stopped_evaluator, method, sweep
):
    from automl_api.models.enums import ScopeStatus, WorkflowStage
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.evaluation_reconciler import sweep_evaluation_resources
    from sqlalchemy import select

    c = stopped_evaluator
    refit = c.db.scalar(
        select(WorkflowAttempt).where(
            WorkflowAttempt.scope_id == c.scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
        )
    )
    now = datetime.now(UTC)
    row = WorkflowAttemptCredentialGrant(
        **grant_values(
            refit,
            method=method,
            role="candidate" if method == "GET" else "pipeline",
            object_version="v1" if method == "GET" else None,
            status="issued",
            issuance_owner=uuid.uuid4(),
            issuance_started_at=now,
            issued_at=now,
            created_at=now,
            expires_at=now + timedelta(seconds=45),
        )
    )
    c.db.add(row)
    if sweep:
        c.scope.status = ScopeStatus.FAILED
    c.db.flush()
    original = row.expires_at
    if sweep:
        sweep_evaluation_resources(c.factory, c.k8s)
    else:
        assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    assert c.key not in c.k8s.objects["secret"]
    c.db.refresh(row)
    assert row.status == "issued" and row.revoked_at is None and row.expires_at == original


def test_key_only_sweep_preserves_active_attempt_even_in_a_stopped_scope(evaluator_job_case):
    from automl_api.models.enums import ScopeStatus
    from automl_api.services.evaluation_reconciler import sweep_evaluation_resources

    c = evaluator_job_case
    c.scope.status = ScopeStatus.FAILED
    c.db.flush()
    assert c.attempt.status == AttemptStatus.PENDING
    sweep_evaluation_resources(c.factory, c.k8s)
    assert c.key in c.k8s.objects["secret"]
    c.attempt.status = AttemptStatus.FAILED
    c.db.flush()
    sweep_evaluation_resources(c.factory, c.k8s)
    assert c.key not in c.k8s.objects["secret"]


@pytest.mark.parametrize("capability", [None, "GET", "PUT"])
def test_controller_resumes_registered_legacy_replacement_after_retiring_predecessor(
    stopped_evaluator, grant_clock, capability
):
    from copy import deepcopy

    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.evaluation_reconciler import reconcile_scope
    from sqlalchemy import select

    c = stopped_evaluator
    original_key = deepcopy(c.k8s.objects["secret"][c.key])
    assert _cleanup_attempt(c.factory, c.k8s, c.attempt.id)
    c.db.refresh(c.attempt)
    plan, _, _ = prepare(c)
    # Model a predecessor binary's registered replacement with unfinished old key custody.
    c.k8s.objects["secret"][c.key] = original_key
    c.attempt.cleanup_state = "pending"
    row = started_grant(c, capability) if capability else None
    c.db.flush()

    def step():
        reconcile_scope(
            c.factory, c.k8s, c.scope.id, c.authority, public_key=c.public_key, store=None
        )

    step()
    if row is not None:
        c.db.refresh(row)
        original = row.expires_at
        assert row.status == "revoked" and c.key in c.k8s.objects["secret"]
        assert not c.k8s.objects["job"]
        grant_clock[0] = original
        step()
        c.db.refresh(row)
        assert row.expires_at == original
        if capability == "PUT":
            c.db.refresh(c.attempt)
            assert c.attempt.cleanup_state != "complete"
            assert c.key in c.k8s.objects["secret"]
            assert not c.k8s.objects["job"]
            return
    c.db.refresh(c.attempt)
    assert c.attempt.cleanup_state == "complete" and c.key not in c.k8s.objects["secret"]
    assert f"evaluation-{plan.attempt_id.hex}" in c.k8s.objects["job"]
    step()
    attempts = list(
        c.db.scalars(
            select(WorkflowAttempt).where(
                WorkflowAttempt.scope_id == c.scope.id, WorkflowAttempt.stage == c.attempt.stage
            )
        )
    )
    assert {attempt.id for attempt in attempts} == {c.attempt.id, plan.attempt_id}
