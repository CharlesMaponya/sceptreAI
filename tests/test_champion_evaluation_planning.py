from __future__ import annotations

import copy
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services.champion_planning import plan_scope_refit
from automl_api.services.evaluation_planning import (
    register_evaluation_plan,
    validate_evaluation_policy,
)
from automl_api.services.workflow_state import InvalidTransition
from automl_api.training.champion_evaluation import EvaluationPlan
from automl_api.training.champion_refit import (
    FrozenPipeline,
    RefitPlan,
    execute_refit,
    publish_frozen_pipeline,
)
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select
from test_champion_planning import install_evaluation_policy

pytest_plugins = ["test_champion_planning"]


def test_scope_creation_validates_template_without_client_scope_id(policy_case, monkeypatch):
    from automl_api.schemas.contracts import EvaluationScopeCreate
    from automl_api.services import contracts
    from fastapi import HTTPException

    db, _, scope, _ = policy_case
    monkeypatch.setattr(contracts, "require_project_role", lambda *_: None)
    payload = EvaluationScopeCreate(
        scope_key=str(uuid.uuid4()),
        split_revision_id=scope.split_revision_id,
        experiment_spec_revision_id=scope.experiment_spec_revision_id,
        canonical_provider="aws",
        expected_member_count=1,
        mode="promotional",
        membership_deadline_at=datetime.now(UTC) + timedelta(minutes=5),
        comparison_policy=scope.comparison_policy,
        final_threshold_revision="registered-v1",
    )
    created = contracts.create_scope(db, SimpleNamespace(), scope.project_id, payload)
    assert created.id != scope.id
    assert "scope_id" not in created.comparison_policy["evaluation_policy"]["final_data"]
    with pytest.raises(HTTPException) as error:
        contracts.create_scope(
            db,
            SimpleNamespace(),
            scope.project_id,
            payload.model_copy(
                update={
                    "scope_key": str(uuid.uuid4()),
                    "comparison_policy": {"refit_policy": scope.comparison_policy["refit_policy"]},
                }
            ),
        )
    assert error.value.status_code == 422
    assert error.value.detail["code"] == "invalid_evaluation_policy"


@pytest.fixture
def policy_case(planning_case):
    db, store, _, scope, _ = planning_case
    install_evaluation_policy(db, scope)
    policy, digest = validate_evaluation_policy(db, scope)
    scope.comparison_policy = {**scope.comparison_policy, "evaluation_policy_digest": digest}
    db.flush()
    return db, store, scope, policy


@pytest.mark.parametrize(
    "field", ["metric", "row_count", "row_digest", "prefix", "version", "provider", "split"]
)
def test_invalid_policy_cannot_be_registered(policy_case, field):
    db, _, scope, _ = policy_case
    comparison = copy.deepcopy(scope.comparison_policy)
    policy = comparison["evaluation_policy"]
    if field == "metric":
        policy["primary_metric"] = "mae"
    elif field == "row_count":
        policy["final_rows"] = 9
    elif field == "row_digest":
        policy["final_row_digest"] = "f" * 64
    elif field == "prefix":
        policy["final_data"]["objects"][0]["key"] = "raw/source.parquet"
    elif field == "version":
        policy["final_data"]["objects"][0]["version"] = "null"
    elif field == "provider":
        policy["final_data"]["provider"] = "gcp"
    else:
        policy["final_data"]["split_digest"] = "f" * 64
    scope.comparison_policy = comparison
    with pytest.raises(ValueError):
        validate_evaluation_policy(db, scope)


def test_durable_evaluator_registration_waits_for_refit_and_replays_exact_plan(policy_case):
    db, store, scope, policy = policy_case
    allocation = uuid.uuid4()
    public_key = Ed25519PrivateKey.generate().public_key().public_bytes_raw().hex()
    refit = plan_scope_refit(db, scope.id, store)
    with pytest.raises(InvalidTransition, match="frozen pipeline"):
        register_evaluation_plan(
            db, scope.id, allocation_id=allocation, result_public_key=public_key
        )
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == refit.id, WorkflowEvent.event_key == "refit_plan"
        )
    )
    refit_plan = RefitPlan.model_validate(event.payload)
    refit.status = AttemptStatus.RUNNING
    refit.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
    output = FrozenPipeline.model_validate(execute_refit(refit_plan, store))
    publish_frozen_pipeline(db, refit_plan, output, fencing_token=refit.fencing_token)
    attempt = register_evaluation_plan(
        db, scope.id, allocation_id=allocation, result_public_key=public_key
    )
    assert attempt.status == AttemptStatus.PENDING and attempt.generation == 1
    recorded = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "evaluation_plan"
        )
    )
    plan = EvaluationPlan.model_validate(recorded.payload)
    assert plan.final_manifest.scope_id == scope.id
    assert plan.final_manifest.model_dump(exclude={"scope_id"}) == policy.final_data.model_dump()
    assert plan.frozen_pipeline.sha256 == output.frozen_pipeline_digest
    assert recorded.result_digest == plan.digest
    assert (
        register_evaluation_plan(
            db, scope.id, allocation_id=allocation, result_public_key=public_key
        ).id
        == attempt.id
    )
    for values in (
        {"allocation_id": uuid.uuid4(), "result_public_key": public_key},
        {"allocation_id": allocation, "result_public_key": "b" * 64},
    ):
        with pytest.raises(InvalidTransition, match="changed"):
            register_evaluation_plan(db, scope.id, **values)
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowAttempt)
            .where(
                WorkflowAttempt.scope_id == scope.id, WorkflowAttempt.stage == "champion_evaluation"
            )
        )
        == 1
    )
    scope.comparison_policy = {**scope.comparison_policy, "evaluation_policy_digest": "0" * 64}
    with pytest.raises(InvalidTransition, match="before scope release"):
        register_evaluation_plan(
            db, scope.id, allocation_id=allocation, result_public_key=public_key
        )
