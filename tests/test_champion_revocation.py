"""Champion read-capability checks honor logical credential revocation."""

import pytest
from automl_api.api.routes import refit_control
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services.attempt_object_grants import revoke_attempt_grants
from automl_api.services.evaluation_access import locked_evaluation
from automl_api.services.refit_access import locked_refit
from automl_api.services.workflow_state import StaleFence
from fastapi import HTTPException, Response
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_champion_evaluation_control import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_control import control_case as evaluation_control_case  # noqa: F401
from test_champion_evaluation_control import handoff_case as handoff_case
from test_champion_evaluation_control import key_case as key_case
from test_champion_read_grants import grant_case as grant_case
from test_champion_refit_control import control_case as refit_control_case  # noqa: F401

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


def lookup(route, db, plan, claims, statuses=(AttemptStatus.RUNNING,)):
    fn = locked_refit if route is refit_control else locked_evaluation
    return fn(db, plan.attempt_id, claims, statuses)


def invoke(route, db, plan, claims):
    if route is refit_control:
        return route.input_grant(plan.attempt_id, 0, claims, db, Response())
    return route.pipeline(plan.attempt_id, claims, db, Response())


@pytest.mark.parametrize("when", ["committed", "pending"])
def test_champion_revocation_blocks_live_capability(grant_case, monkeypatch, when):
    engine, route, claims, plan, _ = grant_case
    if when == "committed":
        with Session(engine, autoflush=False) as db, db.begin():
            revoke_attempt_grants(db, plan.attempt_id)
    with Session(engine, autoflush=False) as db:
        if when == "pending":
            with db.begin():
                revoke_attempt_grants(db, plan.attempt_id)
                assert any(isinstance(row, WorkflowEvent) for row in db.new)
                with pytest.raises(StaleFence):
                    lookup(route, db, plan, claims)
        else:
            with pytest.raises(StaleFence):
                lookup(route, db, plan, claims)
        with pytest.raises(HTTPException) as denied:
            invoke(route, db, plan, claims)
        assert denied.value.status_code == 410
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, plan.attempt_id).status == AttemptStatus.RUNNING
        events = list(
            db.scalars(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == plan.attempt_id,
                    WorkflowEvent.event_key == "attempt.credentials.revoked",
                )
            )
        )
        assert len(events) == 1


def test_active_champion_without_revocation_keeps_access(grant_case, monkeypatch):
    engine, route, claims, plan, _ = grant_case
    with Session(engine, autoflush=False) as db:
        _, attempt, stored = lookup(route, db, plan, claims)
        assert attempt.status == AttemptStatus.RUNNING and stored.digest == plan.digest


def test_completed_champion_revocation_preserves_acknowledgement_lookup(grant_case):
    engine, route, claims, plan, _ = grant_case
    with Session(engine, autoflush=False) as db, db.begin():
        attempt = db.get(WorkflowAttempt, plan.attempt_id)
        attempt.status = AttemptStatus.SUCCEEDED
        revoke_attempt_grants(db, attempt.id)
    with Session(engine, autoflush=False) as db, db.begin():
        _, attempt, stored = lookup(route, db, plan, claims, (AttemptStatus.SUCCEEDED,))
        assert attempt.status == AttemptStatus.SUCCEEDED and stored.digest == plan.digest
