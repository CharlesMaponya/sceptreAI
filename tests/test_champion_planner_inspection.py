"""Committed PostgreSQL checks for planning across a database-free native read."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.services.champion_planning import validate_refit_policy
from automl_api.training.champion_refit import RefitPlan
from automl_api.workflow_reconciler import plan_champions_once
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session
from test_champion_planning import planning_case as planning_case
from test_champion_refit_execution import refit_case as refit_case
from test_champion_refit_publication import durable_refit as durable_refit
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def phase1_db(phase1_engine, refit_case):
    """Commit exact fixture-owned records so independent sessions see real locks."""
    db = Session(phase1_engine, expire_on_commit=False)
    try:
        yield db
    finally:
        owner = db.info.get("fixture_owner")
        db.rollback()
        db.close()
        with Session(phase1_engine) as cleanup, cleanup.begin():
            cleanup.execute(delete(Project).where(Project.id == refit_case[1].project_id))
            if owner is not None:
                cleanup.execute(delete(User).where(User.id == owner))


@pytest.fixture
def inspected_scope(planning_case, phase1_engine):
    db, store, plan, scope, run = planning_case
    db.info["fixture_owner"] = db.get(Project, scope.project_id).owner_id
    db.commit()
    db.close()

    def factory():
        return Session(phase1_engine)

    return factory, store, plan, scope.id, run.id, phase1_engine


def attempts(factory, scope_id):
    with factory() as db:
        return list(
            db.scalars(
                select(WorkflowAttempt.id).where(
                    WorkflowAttempt.scope_id == scope_id,
                    WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                )
            )
        )


def test_native_inspection_has_no_sql_connection_or_row_locks(inspected_scope, monkeypatch):
    factory, store, plan, scope_id, run_id, engine = inspected_scope
    original = store.stat
    observed = []

    def stat(uri):
        observed.append(engine.pool.checkedout())
        with factory() as db, db.begin():
            db.execute(text("SET LOCAL lock_timeout = '100ms'"))
            db.scalar(
                select(PromotionalScope)
                .where(PromotionalScope.id == scope_id)
                .with_for_update(nowait=True)
            )
            db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))
            list(
                db.scalars(
                    select(WorkflowAttempt)
                    .where(WorkflowAttempt.model_run_id == run_id)
                    .with_for_update(nowait=True)
                )
            )
        return original(uri)

    monkeypatch.setattr(store, "stat", stat)
    assert plan_champions_once(factory, store=store) == 1
    assert observed == [0]
    ids = attempts(factory, scope_id)
    assert len(ids) == 1
    assert plan_champions_once(factory, store=store) == 0
    assert observed == [0] and attempts(factory, scope_id) == ids
    with factory() as db:
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == ids[0], WorkflowEvent.event_key == "refit_plan"
            )
        )
        assert RefitPlan.model_validate(event.payload).candidate == plan.candidate


@pytest.mark.parametrize(
    "change", ["cancel", "deadline", "policy", "membership", "candidate", "training"]
)
def test_current_state_is_revalidated_after_inspection(inspected_scope, monkeypatch, change):
    factory, store, plan, scope_id, run_id, _ = inspected_scope
    original = store.stat
    changed = []

    def stat(uri):
        metadata = original(uri)
        with factory() as db, db.begin():
            db.execute(text("SET LOCAL lock_timeout = '100ms'"))
            scope = db.scalar(
                select(PromotionalScope)
                .where(PromotionalScope.id == scope_id)
                .with_for_update(nowait=True)
            )
            run = db.get(ModelRun, run_id)
            if change == "cancel":
                scope.status = ScopeStatus.CANCELLED
            elif change == "deadline":
                scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "policy":
                policy = {
                    **scope.comparison_policy["refit_policy"],
                    "max_model_bytes": plan.max_model_bytes + 1,
                }
                scope.comparison_policy = {"refit_policy": policy}
                _, digest = validate_refit_policy(db, scope)
                scope.comparison_policy = {**scope.comparison_policy, "refit_policy_digest": digest}
            elif change == "membership":
                scope.membership_digest = "f" * 64
            elif change == "candidate":
                entries = [dict(value) for value in run.tags["leaderboard"]]
                entries[0]["model_artifact_sha256"] = "f" * 64
                run.tags = {**run.tags, "leaderboard": entries}
            else:
                training = db.scalar(
                    select(WorkflowAttempt).where(
                        WorkflowAttempt.model_run_id == run_id,
                        WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
                    )
                )
                training.status = AttemptStatus.RUNNING
        changed.append(True)
        return metadata

    monkeypatch.setattr(store, "stat", stat)
    assert plan_champions_once(factory, store=store) == 0
    assert changed == [True]
    assert attempts(factory, scope_id) == []
    with factory() as db:
        current = db.get(PromotionalScope, scope_id)
        if change == "cancel":
            assert current.status == ScopeStatus.CANCELLED
        elif change in {"policy", "candidate", "training"}:
            assert current.status == ScopeStatus.SEALED


def test_competing_planner_commits_one_plan(inspected_scope, monkeypatch):
    factory, store, _, scope_id, _, _ = inspected_scope
    original = store.stat
    competing = []

    def stat(uri):
        if not competing:
            competing.append(None)
            competing[0] = plan_champions_once(factory, store=store)
        return original(uri)

    monkeypatch.setattr(store, "stat", stat)
    assert plan_champions_once(factory, store=store) == 1
    assert competing == [1]
    assert len(attempts(factory, scope_id)) == 1
    assert plan_champions_once(factory, store=store) == 0


@pytest.mark.parametrize("error", [ConnectionError, FileNotFoundError, ValueError])
def test_inspection_error_is_retryable_without_failing_scope(inspected_scope, monkeypatch, error):
    factory, store, _, scope_id, _, _ = inspected_scope
    original = store.stat
    monkeypatch.setattr(store, "stat", MagicMock(side_effect=error("inspection failed")))
    assert plan_champions_once(factory, store=store) == 0
    assert attempts(factory, scope_id) == []
    with factory() as db:
        assert db.get(PromotionalScope, scope_id).status == ScopeStatus.SEALED
    monkeypatch.setattr(store, "stat", original)
    assert plan_champions_once(factory, store=store) == 1


def test_oversized_current_candidate_is_rejected(inspected_scope, monkeypatch):
    factory, store, plan, scope_id, _, _ = inspected_scope
    metadata = store.stat(plan.candidate.uri)
    monkeypatch.setattr(
        store, "stat", lambda _: replace(metadata, byte_size=plan.max_model_bytes + 1)
    )
    assert plan_champions_once(factory, store=store) == 0
    assert attempts(factory, scope_id) == []
    with factory() as db:
        assert db.get(PromotionalScope, scope_id).status == ScopeStatus.FAILED


def test_metadata_for_another_object_is_not_used(inspected_scope, monkeypatch):
    factory, store, plan, scope_id, _, _ = inspected_scope
    metadata = store.stat(plan.candidate.uri)
    monkeypatch.setattr(store, "stat", lambda _: replace(metadata, uri=metadata.uri + ".other"))
    assert plan_champions_once(factory, store=store) == 0
    assert attempts(factory, scope_id) == []


def test_new_winner_requires_its_own_inspection(inspected_scope, monkeypatch):
    factory, store, plan, scope_id, run_id, _ = inspected_scope
    original = store.stat
    replacement_uri = store.put_bytes(
        f"projects/{plan.project_id}/runs/{run_id}/new-winner.joblib", b"different model"
    ).uri
    seen = []

    def stat(uri):
        seen.append(uri)
        if len(seen) == 1:
            with factory() as db, db.begin():
                run = db.get(ModelRun, run_id)
                run.tags = {
                    **run.tags,
                    "leaderboard": [
                        {
                            "model": "new-winner",
                            "status": "succeeded",
                            "metrics": {"rmse": 1.0},
                            "model_artifact_uri": replacement_uri,
                            "model_artifact_sha256": "e" * 64,
                        }
                    ],
                }
        return original(uri)

    monkeypatch.setattr(store, "stat", stat)
    assert plan_champions_once(factory, store=store) == 0
    assert attempts(factory, scope_id) == []
    assert plan_champions_once(factory, store=store) == 1
    assert seen == [plan.candidate.uri, replacement_uri]
    with factory() as db:
        identity = attempts(factory, scope_id)[0]
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == identity, WorkflowEvent.event_key == "refit_plan"
            )
        )
        selected = RefitPlan.model_validate(event.payload).candidate
        assert selected.uri == replacement_uri and selected.byte_size == len(b"different model")


def test_process_death_during_inspection_releases_work_for_replay(inspected_scope, tmp_path):
    import subprocess
    import sys
    import time

    factory, store, plan, scope_id, run_id, _ = inspected_scope
    ready = tmp_path / "native-read-started"
    script = """
import os, signal, sys, uuid
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0, 'apps/api')
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from automl_api.workflow_reconciler import plan_champions_once
engine = create_engine(os.environ['DATABASE_URL'])
def stat(uri):
    Path(sys.argv[2]).write_text('read started')
    signal.pause()
store = SimpleNamespace(stat=stat, uri_for_key=lambda key: sys.argv[3] + key)
plan_champions_once(lambda: Session(engine), store=store)
"""
    with (tmp_path / "child.log").open("w+") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                str(scope_id),
                str(ready),
                plan.candidate.uri.split("projects/", 1)[0],
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while not ready.exists() and time.monotonic() < deadline:
                assert process.poll() is None, "Owned planning child exited before inspection"
                time.sleep(0.05)
            assert ready.exists(), "Owned planning child did not reach inspection"
            with factory() as db, db.begin():
                assert (
                    db.scalar(
                        select(PromotionalScope.id)
                        .where(PromotionalScope.id == scope_id)
                        .with_for_update(nowait=True)
                    )
                    == scope_id
                )
                assert (
                    db.scalar(
                        select(ModelRun.id)
                        .where(ModelRun.id == run_id)
                        .with_for_update(nowait=True)
                    )
                    == run_id
                )
            process.kill()
            assert process.wait(timeout=10) < 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
    assert attempts(factory, scope_id) == []
    assert plan_champions_once(factory, store=store) == 1
    assert len(attempts(factory, scope_id)) == 1
