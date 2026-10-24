"""Stored champion identity agrees with manifests and durable replay delivery."""

import uuid
from copy import deepcopy

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowEvent
from automl_api.services import evaluation_jobs, refit_jobs
from automl_api.training.champion_refit import RefitPlan
from champion_planning_fixture import plan_scope_refit
from sqlalchemy import select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case
from test_champion_refit_jobs import jobs_case as jobs_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


def refit_plan(db, attempt_id):
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == "refit_plan"
        )
    )
    return RefitPlan.model_validate(event.payload)


def account(desired):
    return desired["job"]["spec"]["template"]["spec"]["serviceAccountName"]


def projected_identity(desired, attempt):
    from automl_api.services.attempt_identity import attempt_audience

    pod = desired["job"]["spec"]["template"]["spec"]
    assert desired["object_grant_protocol"] == "pod-bound-object-v2"
    assert pod["automountServiceAccountToken"] is False and pod["enableServiceLinks"] is False
    identity = next(v for v in pod["volumes"] if v["name"] == "identity")
    assert identity["projected"]["sources"] == [
        {
            "serviceAccountToken": {
                "path": "token",
                "expirationSeconds": 600,
                "audience": attempt_audience(attempt.id, attempt.fencing_token),
            }
        }
    ]


def test_new_refit_attempt_and_manifest_share_project_stage_identity(jobs_case):
    db, _, scope, attempt, k8s, _ = jobs_case
    plan = refit_plan(db, attempt.id)
    desired = refit_jobs.submission(plan, scope, k8s.settings, attempt=attempt)
    expected = f"project-{scope.project_id}-refit"
    assert attempt.workload_identity == account(desired) == expected
    projected_identity(desired, attempt)
    other = plan.model_copy(update={"project_id": uuid.uuid4()})
    with pytest.raises(ValueError, match="differs from its attempt"):
        refit_jobs.submission(other, scope, k8s.settings, attempt=attempt)


def test_new_evaluator_attempt_and_manifest_share_project_stage_identity(evaluator_job_case):
    c = evaluator_job_case
    desired = evaluation_jobs.submission(c.plan, c.scope, c.k8s.settings, c.key, attempt=c.attempt)
    expected = f"project-{c.scope.project_id}-evaluation"
    assert c.attempt.workload_identity == account(desired) == expected
    projected_identity(desired, c.attempt)
    assert expected != f"project-{c.scope.project_id}-refit"
    other = c.plan.model_copy(update={"project_id": uuid.uuid4()})
    with pytest.raises(ValueError, match="differs from its attempt"):
        evaluation_jobs.submission(other, c.scope, c.k8s.settings, c.key, attempt=c.attempt)


@pytest.mark.parametrize("legacy", [False, True])
def test_refit_delivery_uses_declared_account_and_replays(jobs_case, legacy):
    db, _, scope, attempt, k8s, _ = jobs_case
    desired = refit_jobs.submission(
        refit_plan(db, attempt.id), scope, k8s.settings, attempt=attempt
    )
    declared = "sceptre-champion-refit" if legacy else f"project-{scope.project_id}-refit"
    desired["job"]["spec"]["template"]["spec"]["serviceAccountName"] = declared
    original = deepcopy(desired)
    ensured = []
    k8s.before_create = lambda: None
    k8s.settings.training_namespace = "new-default-namespace"
    k8s.ensure_service_account = lambda name, namespace=None: ensured.append((namespace, name))
    token = refit_jobs.refit_token(attempt, scope.scope_deadline_at)
    for _ in range(2):
        refit_jobs._ensure_resources(
            k8s, desired, attempt.id, scope.project_id, attempt.fencing_token, token
        )
    assert ensured == [("qa-refit", declared), ("qa-refit", declared)]
    assert desired == original and len(k8s.objects["job"]) == 1


@pytest.mark.parametrize("legacy", [False, True])
def test_evaluator_delivery_uses_declared_account_and_replays(evaluator_job_case, legacy):
    c = evaluator_job_case
    desired = evaluation_jobs.submission(c.plan, c.scope, c.k8s.settings, c.key, attempt=c.attempt)
    declared = (
        "sceptre-champion-evaluation" if legacy else f"project-{c.scope.project_id}-evaluation"
    )
    desired["job"]["spec"]["template"]["spec"]["serviceAccountName"] = declared
    original = deepcopy(desired)
    ensured = []
    c.k8s.before_create = lambda: None
    c.k8s.settings.training_namespace = "new-default-namespace"
    c.k8s.ensure_service_account = lambda name, namespace=None: ensured.append((namespace, name))
    token = evaluation_jobs.evaluation_token(c.attempt, c.scope.scope_deadline_at)
    for _ in range(2):
        evaluation_jobs._ensure_resources(
            c.k8s, desired, c.plan, c.attempt.fencing_token, token, c.token
        )
    assert ensured == [("qa-refit", declared), ("qa-refit", declared)]
    assert desired == original and len(c.k8s.objects["job"]) == 1


def test_refit_replacement_keeps_project_stage_identity(jobs_case):
    db, store, scope, first, _, _ = jobs_case
    first.status = AttemptStatus.FAILED
    first.cleanup_state = "complete"  # No native Job was created by this planning fixture.
    db.flush()
    second = plan_scope_refit(db, scope.id, store)
    assert second.predecessor_attempt_id == first.id and second.generation == 2
    assert (
        first.workload_identity == second.workload_identity == f"project-{scope.project_id}-refit"
    )
    assert first.fencing_token != second.fencing_token


def test_refit_legacy_planning_replay_keeps_original_identity(jobs_case):
    db, store, scope, attempt, _, _ = jobs_case
    original = f"refit:{attempt.id}"
    attempt.workload_identity = original
    db.flush()
    replay = plan_scope_refit(db, scope.id, store)
    assert replay.id == attempt.id and replay.workload_identity == original


def test_evaluator_legacy_planning_replay_keeps_original_identity(evaluator_job_case):
    from automl_api.services.evaluation_planning import register_evaluation_plan

    c = evaluator_job_case
    original = f"evaluation:{c.attempt.id}"
    c.attempt.workload_identity = original
    c.db.flush()
    replay = register_evaluation_plan(
        c.db,
        c.scope.id,
        allocation_id=c.plan.allocation_id,
        result_public_key=c.plan.result_public_key,
    )
    assert replay.id == c.attempt.id and replay.workload_identity == original
