"""Provider minting releases SQL locks and cannot disclose stale input access."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.api.routes import evaluation_control, refit_control
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.training.champion_evaluation import EvaluationPlan
from automl_api.training.champion_refit import RefitPlan
from fastapi import HTTPException, Response
from sqlalchemy import delete, select
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


@pytest.mark.parametrize(
    "mutation",
    [
        "none",
        "cancel",
        "supersede",
        "fence",
        "expiry",
        "deadline",
        "short_lease",
        "plan",
        "heartbeat",
    ],
)
def test_mint_has_no_caller_connection_and_rechecks_current_authorization(
    grant_case, monkeypatch, mutation
):
    engine, route, claims, plan, plan_type = grant_case
    minted = {"url": "https://objects.test/exact?private-capability", "expires_at": "private"}
    with Session(engine, expire_on_commit=False) as caller:
        # Keep an old ORM event alive: final validation must bypass cached objects too.
        cached = caller.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == plan.attempt_id,
                WorkflowEvent.event_key
                == ("refit_plan" if route is refit_control else "evaluation_plan"),
            )
        )
        caller.commit()
        assert cached is not None
        seen = []

        def mint(_store, _item, deadline):
            assert not caller.in_transaction()
            assert engine.pool.checkedout() == 0
            assert deadline > datetime.now(UTC)
            with Session(engine) as other, other.begin():
                scope = other.scalar(
                    select(PromotionalScope)
                    .where(
                        PromotionalScope.id == plan.scope_id,
                    )
                    .with_for_update(nowait=True)
                )
                attempt = other.scalar(
                    select(WorkflowAttempt)
                    .where(
                        WorkflowAttempt.id == plan.attempt_id,
                    )
                    .with_for_update(nowait=True)
                )
                if mutation == "cancel":
                    scope.status = ScopeStatus.CANCELLED
                elif mutation == "supersede":
                    attempt.status = AttemptStatus.SUPERSEDED
                elif mutation == "fence":
                    attempt.fencing_token = uuid.uuid4().hex
                elif mutation == "expiry":
                    attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
                elif mutation == "deadline":
                    scope.scope_deadline_at = datetime.now(UTC) + timedelta(seconds=5)
                elif mutation == "short_lease":
                    attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=5)
                elif mutation == "heartbeat":
                    attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
                elif mutation == "plan":
                    event = other.scalar(
                        select(WorkflowEvent).where(
                            WorkflowEvent.id == cached.id,
                        )
                    )
                    changed = plan_type.model_validate(
                        {
                            **event.payload,
                            "max_model_bytes": plan.max_model_bytes + 1,
                        }
                    )
                    event.payload = changed.model_dump(mode="json")
                    event.result_digest = changed.digest
            seen.append(True)
            return minted

        monkeypatch.setattr(route, "mint_refit_read_url", mint)

        def invoke(response):
            if route is refit_control:
                return route.input_grant(plan.attempt_id, 0, claims, caller, response)
            return route.pipeline(plan.attempt_id, claims, caller, response)

        response = Response()
        if mutation in {"none", "heartbeat"}:
            assert invoke(response) == minted
            assert response.headers["Cache-Control"] == "no-store"
            if mutation == "none":
                assert invoke(Response()) == minted
                assert len(seen) == 2
        else:
            with pytest.raises(HTTPException) as error:
                invoke(response)
            assert error.value.status_code == 409
            assert minted["url"] not in str(error.value.detail)
        assert seen
        assert not caller.in_transaction() and engine.pool.checkedout() == 0


def test_mint_failure_is_generic_and_releases_connection(grant_case, monkeypatch):
    engine, route, claims, plan, _ = grant_case
    secret = "https://objects.test/private?credential=must-not-disclose"

    def mint(*_args):
        assert engine.pool.checkedout() == 0
        raise ConnectionError(secret)

    monkeypatch.setattr(route, "mint_refit_read_url", mint)
    with Session(engine) as db, pytest.raises(HTTPException) as error:
        if route is refit_control:
            route.input_grant(plan.attempt_id, 0, claims, db, Response())
        else:
            route.pipeline(plan.attempt_id, claims, db, Response())
    assert error.value.status_code == 503
    assert secret not in str(error.value.detail)
    assert engine.pool.checkedout() == 0


def test_renewed_attempt_cannot_disclose_capability_after_its_original_deadline(
    grant_case, monkeypatch
):
    from types import SimpleNamespace

    engine, route, claims, plan, _ = grant_case
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, plan.attempt_id)
        attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=30)

    def mint(_store, _item, deadline):
        assert engine.pool.checkedout() == 0
        with Session(engine) as db, db.begin():
            attempt = db.get(WorkflowAttempt, plan.attempt_id)
            attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
        monkeypatch.setattr(
            route, "datetime", SimpleNamespace(now=lambda tz: deadline + timedelta(seconds=1))
        )
        return {"url": "https://objects.test/expired-capability"}

    monkeypatch.setattr(route, "mint_refit_read_url", mint)
    with Session(engine) as db, pytest.raises(HTTPException) as error:
        if route is refit_control:
            route.input_grant(plan.attempt_id, 0, claims, db, Response())
        else:
            route.pipeline(plan.attempt_id, claims, db, Response())
    assert error.value.status_code == 409
    assert "expired-capability" not in str(error.value.detail)
