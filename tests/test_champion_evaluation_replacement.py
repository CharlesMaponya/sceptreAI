from datetime import UTC, datetime

import httpx
import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services.evaluation_jobs import submit_evaluator
from automl_api.services.evaluation_keys import prepare_evaluator_credentials
from automl_api.services.workflow_state import InvalidTransition
from sqlalchemy import func, select
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_authority import handoff_case as handoff_case
from test_champion_evaluation_jobs import evaluator_job_case as evaluator_job_case

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


def failed(case):
    case.attempt.status = AttemptStatus.FAILED
    case.attempt.terminal_reason = "Submission failed before final-data grant"
    case.attempt.finished_at = datetime.now(UTC)
    case.db.flush()

    def before_create():
        assert not case.db.get_bind().in_nested_transaction()
        with case.factory() as session:
            assert session.get(WorkflowAttempt, case.attempt.id).status == AttemptStatus.FAILED
            assert (
                session.scalar(
                    select(WorkflowEvent)
                    .join(
                        WorkflowAttempt,
                        WorkflowEvent.attempt_id == WorkflowAttempt.id,
                    )
                    .where(
                        WorkflowAttempt.scope_id == case.scope.id,
                        WorkflowEvent.event_key == "evaluation_key_intent_2",
                    )
                )
                is not None
            )

    case.k8s.before_create = before_create


def prepare(case, generation=2):
    return prepare_evaluator_credentials(
        case.factory,
        case.scope.id,
        case.k8s,
        case.authority,
        authority_public_key=case.public_key,
        generation=generation,
    )


@pytest.mark.parametrize("lost_reply", [False, True])
def test_replacement_has_new_key_and_fence_but_same_data_and_allocation(
    evaluator_job_case, handoff_case, monkeypatch, lost_reply
):
    case = evaluator_job_case
    case.public_key = handoff_case[4]
    failed(case)
    original = case.authority.post
    lost = False

    def post(path, **kwargs):
        nonlocal lost
        response = original(path, **kwargs)
        if lost_reply and path.endswith("/evaluators") and not lost:
            assert response.status_code == 200
            lost = True
            raise httpx.ReadError("Lost replacement identity acknowledgement")
        return response

    monkeypatch.setattr(case.authority, "post", post)
    if lost_reply:
        with pytest.raises(httpx.ReadError):
            prepare(case)
    plan, token, key = prepare(case)
    assert prepare(case) == (plan, token, key)
    assert plan.attempt_id != case.plan.attempt_id and key != case.key
    assert plan.result_public_key != case.plan.result_public_key
    assert plan.model_dump(exclude={"attempt_id", "result_public_key"}) == case.plan.model_dump(
        exclude={"attempt_id", "result_public_key"}
    )
    attempt = case.db.get(WorkflowAttempt, plan.attempt_id)
    assert attempt.generation == 2 and attempt.retry_count == 1
    assert attempt.fencing_token != case.attempt.fencing_token
    with pytest.raises(InvalidTransition):
        prepare(case, generation=1)
    with pytest.raises(InvalidTransition):
        prepare(case, generation=3)
    # Submission's external effects must see the replacement's committed state.
    case.k8s.before_create = lambda: None
    desired = submit_evaluator(
        case.factory, case.k8s, plan, token, key, fence=attempt.fencing_token
    )
    assert desired["job"]["metadata"]["name"] == f"evaluation-{attempt.id.hex}"
    state = case.authority.get(f"allocations/{plan.allocation_id}").json()
    assert state["status"] == "allocated" and state["cas_version"] == 0
    assert (
        case.authority.post(
            f"allocations/{plan.allocation_id}/open",
            headers={"Authorization": f"Bearer {case.token}"},
            json={"provider_manifest_digest": plan.final_manifest.digest},
        ).status_code
        == 401
    )
    assert (
        case.db.scalar(
            select(func.count())
            .select_from(WorkflowAttempt)
            .where(
                WorkflowAttempt.scope_id == case.scope.id,
                WorkflowAttempt.logical_key == f"evaluation:{case.scope.id}",
            )
        )
        == 2
    )


@pytest.mark.parametrize("reason", ["live", "output", "exhausted"])
def test_ineligible_initial_attempt_never_creates_replacement_key(
    evaluator_job_case, handoff_case, reason
):
    case = evaluator_job_case
    case.public_key = handoff_case[4]
    if reason != "live":
        failed(case)
    if reason == "output":
        case.db.add(
            WorkflowEvent(
                project_id=case.scope.project_id,
                attempt_id=case.attempt.id,
                sequence=99,
                event_key="evaluation_output",
                event_type="evaluation_output",
                payload={},
                result_digest="a" * 64,
            )
        )
    if reason == "exhausted":
        case.attempt.retry_budget = 0
    case.db.flush()
    before = set(case.k8s.objects["secret"])
    with pytest.raises(InvalidTransition):
        prepare(case)
    assert set(case.k8s.objects["secret"]) == before


def test_old_open_wins_race_and_replacement_never_gets_authority_identity(
    evaluator_job_case, handoff_case, monkeypatch
):
    case = evaluator_job_case
    case.public_key = handoff_case[4]
    failed(case)
    original = case.authority.post
    path = f"allocations/{case.plan.allocation_id}"

    def post(url, **kwargs):
        if url.endswith("/evaluators"):
            response = original(
                path + "/open",
                headers={"Authorization": f"Bearer {case.token}"},
                json={"provider_manifest_digest": case.plan.final_manifest.digest},
            )
            assert response.status_code == 200, response.text
        return original(url, **kwargs)

    monkeypatch.setattr(case.authority, "post", post)
    with pytest.raises(httpx.HTTPStatusError):
        prepare(case)
    assert case.authority.get(path).json()["status"] == "opened"
    replacement = case.db.scalar(
        select(WorkflowAttempt).where(
            WorkflowAttempt.scope_id == case.scope.id, WorkflowAttempt.generation == 2
        )
    )
    assert replacement is not None
    assert (
        case.db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == replacement.id,
                WorkflowEvent.event_key == "authority_registered",
            )
        )
        is None
    )
    assert not case.k8s.objects["job"]
    with pytest.raises(InvalidTransition):
        prepare(case)
