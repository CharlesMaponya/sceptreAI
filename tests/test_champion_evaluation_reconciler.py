from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.evaluation_publication import _event
from automl_api.services.evaluation_reconciler import (
    reconcile_evaluation_jobs_once,
    reconcile_scope,
)
from sqlalchemy import select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import EvaluatorKubernetes
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case
from test_champion_evaluation_worker import run
from test_champion_evaluation_worker import worker_case as worker_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


def step(case, public_key):
    reconcile_scope(
        case.factory, case.k8s, case.scope.id, case.authority, public_key=public_key, store=None
    )


def test_controller_submits_and_replaces_only_after_foreground_cleanup(
    evaluator_job_case, handoff_case
):
    case = evaluator_job_case
    public_key = handoff_case[4]
    step(case, public_key)
    name = f"evaluation-{case.attempt.id.hex}"
    assert name in case.k8s.objects["job"]
    step(case, public_key)
    case.k8s.states[name] = "failed"
    step(case, public_key)
    case.db.refresh(case.attempt)
    assert case.attempt.status == AttemptStatus.FAILED
    assert name not in case.k8s.objects["job"]
    assert name in case.k8s.objects["secret"] and case.key in case.k8s.objects["secret"]
    case.k8s.before_create = lambda: None
    step(case, public_key)
    replacement = case.db.scalar(
        select(WorkflowAttempt).where(
            WorkflowAttempt.scope_id == case.scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
            WorkflowAttempt.generation == 2,
        )
    )
    assert replacement is not None and replacement.status == AttemptStatus.SUBMITTED
    assert case.key not in case.k8s.objects["secret"]
    assert f"evaluation-{replacement.id.hex}" in case.k8s.objects["job"]
    assert (
        case.authority.get(f"allocations/{case.plan.allocation_id}").json()["status"] == "allocated"
    )


def test_controller_seals_post_open_failure_and_cleans_resources(evaluator_job_case, handoff_case):
    case = evaluator_job_case
    public_key = handoff_case[4]
    step(case, public_key)
    case.db.refresh(case.attempt)
    case.attempt.status = AttemptStatus.RUNNING
    case.db.flush()
    path = f"allocations/{case.plan.allocation_id}"
    response = case.authority.post(
        path + "/open",
        headers={"Authorization": f"Bearer {case.token}"},
        json={"provider_manifest_digest": case.plan.final_manifest.digest},
    )
    assert response.status_code == 200
    name = f"evaluation-{case.attempt.id.hex}"
    case.k8s.states[name] = "failed"
    step(case, public_key)
    case.db.refresh(case.scope)
    assert case.scope.status == ScopeStatus.FAILED
    assert case.authority.get(path).json()["status"] == "failed"
    assert name in case.k8s.objects["secret"]  # Foreground deletion not yet confirmed absent.
    step(case, public_key)
    assert not any(case.k8s.objects.values())
    case.db.refresh(case.attempt)
    assert case.attempt.cleanup_state == "complete"


def test_heartbeat_racing_failure_observation_defers_decision(evaluator_job_case, handoff_case):
    case = evaluator_job_case
    step(case, handoff_case[4])
    original = case.k8s.job_state

    def heartbeat(name):
        case.db.refresh(case.attempt)
        case.attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        case.db.flush()
        return "failed"

    case.k8s.job_state = heartbeat
    step(case, handoff_case[4])
    case.db.refresh(case.attempt)
    assert case.attempt.status == AttemptStatus.SUBMITTED
    assert _event(case.db, case.attempt.id, "evaluation_recovery_started") is None
    case.k8s.job_state = original


@pytest.mark.parametrize(
    "committed, outcome",
    [(False, "recover"), (True, "recover"), (False, "corrupt"), (True, "cancel")],
)
def test_controller_recovers_real_worker_output_without_second_grant(
    worker_case, monkeypatch, committed, outcome
):
    case = worker_case
    original = case.authority.post

    def lost(path, **kwargs):
        if path.endswith("/commit"):
            if committed:
                assert original(path, **kwargs).status_code == 200
            raise httpx.ReadError("Lost worker")
        return original(path, **kwargs)

    monkeypatch.setattr(case.authority, "post", lost)
    with pytest.raises(httpx.ReadError):
        run(case)
    if outcome == "corrupt":
        output = _event(case.db, case.plan.attempt_id, "evaluation_output").payload
        case.store._path_from_uri(output["uri"]).write_bytes(b"corrupt result")
    if outcome == "cancel":
        case.db.refresh(case.scope)
        case.scope.status = ScopeStatus.CANCELLED
        case.db.flush()
    k8s = EvaluatorKubernetes()
    reads = list(case.requests)

    @contextmanager
    def authority_factory(reference):
        assert reference == case.plan.final_manifest.project_reference
        yield case.admin

    assert (
        reconcile_evaluation_jobs_once(
            case.factory,
            k8s,
            authority_factory=authority_factory,
            public_key=case.public_key,
            store=case.store,
        )
        == 1
    )
    case.db.refresh(case.scope)
    expected = {
        "recover": ScopeStatus.SUCCEEDED,
        "corrupt": ScopeStatus.FAILED,
        "cancel": ScopeStatus.CANCELLED,
    }[outcome]
    assert case.scope.status == expected
    assert case.requests == reads
    case.mint.assert_called_once()
    assert case.control.post("heartbeat").status_code == 409
    assert reconcile_evaluation_jobs_once(
        case.factory,
        k8s,
        authority_factory=authority_factory,
        public_key=case.public_key,
        store=case.store,
    ) == (1 if outcome == "recover" else 0)
    case.db.refresh(case.attempt)
    assert case.attempt.cleanup_state == "complete"


def test_recovery_fence_rejects_late_start(worker_case):
    case = worker_case
    from automl_api.services.evaluation_authority import _record

    with case.factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, case.plan.attempt_id)
        _record(db, attempt, "evaluation_recovery_started", {"attempt_id": str(attempt.id)})
    assert case.control.post("start").status_code == 409
    case.mint.assert_not_called()


def test_orphan_sweep_retains_keys_until_foreground_job_is_absent(evaluator_job_case, handoff_case):
    from automl_api.models.workflows import PromotionalScope
    from automl_api.services.evaluation_reconciler import sweep_evaluation_resources
    from sqlalchemy import delete

    case = evaluator_job_case
    step(case, handoff_case[4])
    # Simulate the application's project/scope cascade, preserving only external resources.
    case.db.execute(delete(PromotionalScope).where(PromotionalScope.id == case.scope.id))
    case.db.flush()
    sweep_evaluation_resources(case.factory, case.k8s)
    assert not case.k8s.objects["job"]
    assert case.key in case.k8s.objects["secret"]
    sweep_evaluation_resources(case.factory, case.k8s)
    assert not any(case.k8s.objects.values())


@pytest.mark.parametrize("cancelled", [False, True])
def test_missing_policy_closes_only_unstarted_handoff(handoff_case, cancelled):
    db, scope, factory, authority, public_key, _ = handoff_case
    scope.comparison_policy = {
        k: v for k, v in scope.comparison_policy.items() if k != "evaluation_policy"
    }
    if cancelled:
        scope.status = ScopeStatus.CANCELLED
    db.flush()
    version = scope.cas_version
    k8s = EvaluatorKubernetes()

    def no_authority(_):
        pytest.fail("Unstarted missing-policy handoff must not contact authority")

    for count in [1, 0]:
        assert reconcile_evaluation_jobs_once(
            factory, k8s, authority_factory=no_authority, public_key=public_key, store=object()
        ) == count
    db.refresh(scope)
    assert scope.status == (ScopeStatus.CANCELLED if cancelled else ScopeStatus.FAILED)
    assert scope.cas_version == version + (0 if cancelled else 1)
    refit = db.scalar(select(WorkflowAttempt).where(
        WorkflowAttempt.scope_id == scope.id,
        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
    ))
    assert _event(db, refit.id, "evaluation_handoff_closed").payload == {
        "no_authority_intent": True, "reason": "missing_evaluation_policy"
    }
    assert not any(k8s.objects.values())


def test_missing_policy_does_not_hide_registered_handoff(evaluator_job_case, handoff_case):
    case = evaluator_job_case
    case.scope.comparison_policy = {
        k: v for k, v in case.scope.comparison_policy.items() if k != "evaluation_policy"
    }
    case.db.flush()
    status = case.scope.status
    before = case.authority.get(f"allocations/{case.plan.allocation_id}").json()
    assert reconcile_evaluation_jobs_once(
        case.factory, case.k8s, authority_factory=lambda _: pytest.fail("No policy"),
        public_key=handoff_case[4], store=object(),
    ) == 1
    case.db.refresh(case.scope)
    assert case.scope.status == status
    refit = case.db.scalar(select(WorkflowAttempt).where(
        WorkflowAttempt.scope_id == case.scope.id,
        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
    ))
    assert _event(case.db, refit.id, "evaluation_handoff_closed") is None
    assert case.authority.get(f"allocations/{case.plan.allocation_id}").json() == before


def test_missing_policy_preserves_lost_allocation_intent(handoff_case, monkeypatch):
    from automl_api.services.evaluation_authority import prepare_evaluator

    db, scope, factory, authority, public_key, result_key = handoff_case
    original = authority.post

    def lost(path, **kwargs):
        response = original(path, **kwargs)
        if path == "allocations":
            assert response.status_code == 200
            raise httpx.ReadError("Lost allocation reply")
        return response

    monkeypatch.setattr(authority, "post", lost)
    with pytest.raises(httpx.ReadError):
        prepare_evaluator(factory, scope.id, authority,
                          authority_public_key=public_key, result_public_key=result_key)
    scope.comparison_policy = {
        k: v for k, v in scope.comparison_policy.items() if k != "evaluation_policy"
    }
    db.flush()
    assert reconcile_evaluation_jobs_once(
        factory, EvaluatorKubernetes(),
        authority_factory=lambda _: pytest.fail("No policy"), public_key=public_key,
        store=object(),
    ) == 1
    db.refresh(scope)
    assert scope.status == ScopeStatus.RUNNING
    refit = db.scalar(select(WorkflowAttempt).where(
        WorkflowAttempt.scope_id == scope.id,
        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
    ))
    assert _event(db, refit.id, "evaluation_handoff_closed") is None
    assert _event(db, refit.id, "evaluation_authority_intent") is not None
