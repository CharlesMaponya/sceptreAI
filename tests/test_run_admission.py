from __future__ import annotations

import pytest
from automl_api.services.run_admission import (
    QUALIFIED_PRODUCTION_CONCURRENCY,
    AdmissionPolicy,
    AdmissionRequest,
    admission_policy_for,
    evaluate_admission,
)


def _request(**overrides: object) -> AdmissionRequest:
    values: dict[str, object] = {
        "project_id": "project-1",
        "user_id": "user-1",
        "resource_class": "training",
    }
    values.update(overrides)
    return AdmissionRequest(**values)  # type: ignore[arg-type]


def test_production_policy_supports_fifteen_concurrent_runs_per_project() -> None:
    policy = admission_policy_for("production")
    assert policy.global_max_active_runs >= QUALIFIED_PRODUCTION_CONCURRENCY
    assert policy.max_active_runs_per_project >= QUALIFIED_PRODUCTION_CONCURRENCY
    staging = admission_policy_for("staging")
    assert staging.max_active_runs_per_project >= QUALIFIED_PRODUCTION_CONCURRENCY


def test_admission_allows_fifteen_concurrent_runs_in_one_project() -> None:
    """The qualification profile: 15 concurrent runs, one benchmark, one project."""
    policy = admission_policy_for("production")
    decision = evaluate_admission(
        policy,
        _request(),
        active_global=14,
        active_for_project=14,
        active_for_user=4,
        active_for_resource_class=14,
    )
    assert decision.can_launch
    assert decision.blockers == []


def test_admission_blocks_at_each_explicit_limit() -> None:
    policy = AdmissionPolicy(environment="production")
    global_block = evaluate_admission(
        policy, _request(), active_global=15, active_for_project=0,
        active_for_user=0, active_for_resource_class=0,
    )
    assert not global_block.can_launch and "Global concurrency" in global_block.blockers[0]

    project_block = evaluate_admission(
        policy, _request(), active_global=0, active_for_project=15,
        active_for_user=0, active_for_resource_class=0,
    )
    assert not project_block.can_launch and "Project concurrency" in project_block.blockers[0]

    user_block = evaluate_admission(
        policy, _request(), active_global=0, active_for_project=0,
        active_for_user=5, active_for_resource_class=0,
    )
    assert not user_block.can_launch and "User concurrency" in user_block.blockers[0]

    class_block = evaluate_admission(
        policy, _request(), active_global=0, active_for_project=0,
        active_for_user=0, active_for_resource_class=15,
    )
    assert not class_block.can_launch and any("Resource-class" in b for b in class_block.blockers)


def test_scope_bound_runs_bypass_selection_concurrency_limits() -> None:
    policy = admission_policy_for("production")
    request = _request(evaluation_scope_id="scope-1")
    decision = evaluate_admission(
        policy,
        request,
        active_global=policy.global_max_active_runs,
        active_for_project=policy.max_active_runs_per_project,
        active_for_user=policy.max_active_runs_per_user,
        active_for_resource_class=policy.max_active_runs_per_resource_class["low"],
    )
    assert decision.can_launch


def test_unknown_resource_class_is_rejected() -> None:
    policy = admission_policy_for("production")
    decision = evaluate_admission(
        policy,
        _request(resource_class="quantum"),
        active_global=0,
        active_for_project=0,
        active_for_user=0,
        active_for_resource_class=0,
    )
    assert not decision.can_launch
    assert any("Unknown resource class" in blocker for blocker in decision.blockers)


def test_local_policy_is_smaller_but_explicit() -> None:
    policy = admission_policy_for("local")
    assert policy.global_max_active_runs < QUALIFIED_PRODUCTION_CONCURRENCY
    assert policy.max_active_runs_per_resource_class


@pytest.mark.parametrize("environment", ["Production", " STAGING ", "local"])
def test_environment_normalization(environment: str) -> None:
    policy = admission_policy_for(environment)
    assert policy.normalized_environment() == environment.strip().lower()
