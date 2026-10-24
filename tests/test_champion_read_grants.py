"""Provider minting releases SQL locks and cannot disclose stale input access."""

from datetime import UTC, datetime, timedelta

import pytest
from automl_api.api.routes import evaluation_control, refit_control
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt
from automl_api.training.champion_evaluation import EvaluationPlan
from automl_api.training.champion_refit import RefitPlan
from fastapi import HTTPException, Response
from sqlalchemy import delete
from sqlalchemy.orm import Session
from test_champion_evaluation_control import (  # noqa: F401
    compatible_uri_fixture,  # noqa: F401
    handoff_case,
    key_case,
)
from test_champion_evaluation_control import control_case as evaluation_control_case  # noqa: F401
from test_champion_refit_control import control_case as refit_control_case  # noqa: F401

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


@pytest.fixture(params=["refit", "evaluation"])
def grant_case(request, phase1_engine):
    kind = request.param
    case = request.getfixturevalue(kind + "_control_case")
    client, source = case[:2]
    if kind == "refit":
        plan, attempt, scope = case[3:6]
        route, plan_type = refit_control, RefitPlan
        claims = refit_control.refit_claims(case[-1], attempt.id)
    else:
        scope, attempt, plan = case[2:5]
        route, plan_type = evaluation_control, EvaluationPlan
        claims = evaluation_control.evaluation_claims(case[-1], attempt.id)
    assert client.post("start").status_code == 200
    project_id, attempt_id, scope_id = plan.project_id, plan.attempt_id, plan.scope_id
    owner_id = source.get(Project, project_id).owner_id
    # These fresh fixture rows must be visible to genuinely independent connections.
    connection = source.get_bind()
    source.commit()
    connection.commit()
    try:
        yield phase1_engine, route, claims, plan, plan_type
    finally:
        source.close()
        with Session(phase1_engine) as db, db.begin():
            db.execute(delete(Project).where(Project.id == project_id))
            db.execute(delete(User).where(User.id == owner_id))
        with Session(phase1_engine) as db:
            assert db.get(PromotionalScope, scope_id) is None
            assert db.get(WorkflowAttempt, attempt_id) is None


@pytest.mark.parametrize("state", ["live", "revoked", "expired"])
def test_retired_champion_routes_never_mint_or_use_caller_transaction(
    grant_case, monkeypatch, state
):
    from automl_api.services.attempt_object_grants import revoke_attempt_grants

    engine, route, claims, plan, _ = grant_case
    with Session(engine) as db, db.begin():
        if state == "revoked":
            revoke_attempt_grants(db, plan.attempt_id)
        elif state == "expired":
            db.get(WorkflowAttempt, plan.attempt_id).lease_expires_at = datetime.now(
                UTC
            ) - timedelta(seconds=1)

    def forbidden(*args, **kwargs):
        raise AssertionError("Retired routes must never mint")

    from automl_api.services import attempt_object_grants

    monkeypatch.setattr(attempt_object_grants, "mint_refit_read_url", forbidden)
    with Session(engine) as caller:
        assert not caller.in_transaction()
        with pytest.raises(HTTPException) as denied:
            if route is refit_control:
                route.input_grant(plan.attempt_id, 0, claims, caller, Response())
            else:
                route.pipeline(plan.attempt_id, claims, caller, Response())
        assert denied.value.status_code == 410
        assert denied.value.detail["code"] == "pod_bound_grants_required"
        assert denied.value.headers["Cache-Control"] == "no-store"
        assert not caller.in_transaction() and engine.pool.checkedout() == 0
