"""Cancellation fences every child in the same committed parent transaction."""

import copy
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    OutboxEntry,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowCommand,
    WorkflowEvent,
)
from automl_api.services import training, training_trials
from automl_api.services.workflow_state import StaleFence
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication
from test_training_trial_enrollment import enroll
from test_training_trial_enrollment import trial_scope as trial_scope


@pytest.fixture
def cancellation_case(trial_scope):
    engine, run_id, parent_id, fence, _ = trial_scope
    contexts = []
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.tags = {**run.tags, "orchestrator": "kuberay"}
        actor_id, project_id = run.created_by_id, run.project_id
        for index in range(6):
            row = enroll(
                db, trial_scope, suggestion_id=f"cancel-{index}", execution_id=f"exec-{index}"
            )
            context = dict(
                attempt_id=row.id,
                fencing_token=row.fencing_token,
                run_fencing_token=fence,
                worker_id=f"worker-{index}",
            )
            if index:
                training_trials.start_trial(db, **context)
            if index in {1, 2}:
                row.status = AttemptStatus.CLAIMED if index == 1 else AttemptStatus.SUBMITTED
            if index >= 4:
                training_trials.finish_trial(
                    db,
                    **context,
                    **(
                        {"result": {"cv_score": 0.9}}
                        if index == 4
                        else {"failure": "owned fixture"}
                    ),
                )
            contexts.append(context)
        candidate = db.get(TrainingCandidate, trial_scope[4])
        foreign_run = ModelRun(
            project_id=project_id,
            created_by_id=actor_id,
            dataset_version_id=run.dataset_version_id,
            run_kind=run.run_kind,
            task_type=run.task_type,
            status=RunStatus.RUNNING,
        )
        db.add(foreign_run)
        db.flush()
        foreign_parent = WorkflowAttempt(
            project_id=project_id,
            model_run_id=foreign_run.id,
            stage="training_run",
            logical_key=f"training:{foreign_run.id}",
            workload_identity="foreign-training",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            status=AttemptStatus.RUNNING,
        )
        foreign_candidate = TrainingCandidate(
            project_id=project_id,
            model_run_id=foreign_run.id,
            candidate_key="0:Ridge",
            estimator_key="Ridge",
            catalog_revision_id=candidate.catalog_revision_id,
            feature_recipe_revision_id=candidate.feature_recipe_revision_id,
        )
        db.add_all([foreign_parent, foreign_candidate])
        db.flush()
        foreign_child = enroll(
            db,
            (
                engine,
                foreign_run.id,
                foreign_parent.id,
                foreign_parent.fencing_token,
                foreign_candidate.id,
            ),
        )
        foreign = (foreign_run.id, foreign_parent.id, foreign_child.id)
    return SimpleNamespace(
        engine=engine,
        run_id=run_id,
        parent_id=parent_id,
        actor_id=actor_id,
        project_id=project_id,
        contexts=contexts,
        scope=trial_scope,
        client=MagicMock(),
        foreign=foreign,
    )


def cancel(db, c):
    return training.cancel_training_run(
        db, db.get(User, c.actor_id), c.project_id, c.run_id, c.client
    )


def snapshot(db, c):
    rows = [db.get(ModelRun, c.run_id), db.get(WorkflowAttempt, c.parent_id)]
    rows += [db.get(WorkflowAttempt, context["attempt_id"]) for context in c.contexts]
    rows += [db.get(TrainingTrial, row.trial_id) for row in rows[2:]]
    rows += list(
        db.scalars(
            select(WorkflowEvent)
            .where(WorkflowEvent.attempt_id.in_([context["attempt_id"] for context in c.contexts]))
            .order_by(WorkflowEvent.id)
        )
    )
    rows += [
        db.get(ModelRun, c.foreign[0]),
        db.get(WorkflowAttempt, c.foreign[1]),
        db.get(WorkflowAttempt, c.foreign[2]),
    ]
    return [
        copy.deepcopy({col.key: getattr(row, col.key) for col in row.__table__.columns})
        for row in rows
    ]


def test_cancel_supersedes_all_active_children_and_preserves_completed_results(cancellation_case):
    c = cancellation_case
    with Session(c.engine) as db:
        before = snapshot(db, c)
    for _ in range(3):
        with Session(c.engine) as db, db.begin():
            cancel(db, c)
    with Session(c.engine) as db:
        after = snapshot(db, c)
        assert after[0]["status"] == RunStatus.CANCELLED
        assert after[1]["status"] == AttemptStatus.CANCELLED
        for index, row in enumerate(after[2:6]):
            assert row["status"] == AttemptStatus.SUPERSEDED
            assert row["terminal_cas_version"] == before[index + 2]["terminal_cas_version"] + 1
            assert row["lease_owner"] is None and row["lease_expires_at"] is None
        assert after[6:] == before[6:]
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(
                    OutboxEntry.project_id == c.project_id,
                    OutboxEntry.topic == "ray.training.cancel",
                )
            )
            == 1
        )
    assert not c.client.mock_calls


def test_cancel_rollback_restores_parent_run_children_and_queue(cancellation_case):
    c = cancellation_case
    with Session(c.engine) as db:
        before = snapshot(db, c)
        commands = db.scalar(
            select(func.count())
            .select_from(WorkflowCommand)
            .where(WorkflowCommand.project_id == c.project_id)
        )
    with pytest.raises(RuntimeError, match="owned rollback"), Session(c.engine) as db, db.begin():
        cancel(db, c)
        raise RuntimeError("owned rollback")
    with Session(c.engine) as db:
        assert snapshot(db, c) == before
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == c.project_id)
            )
            == commands
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(OutboxEntry.project_id == c.project_id)
            )
            == 0
        )


@pytest.mark.parametrize("operation", ["start", "heartbeat", "finish"])
def test_cancelled_children_cannot_start_renew_or_publish(cancellation_case, operation):
    c = cancellation_case
    with Session(c.engine) as db, db.begin():
        cancel(db, c)
    for context in c.contexts[:4]:
        with pytest.raises(StaleFence), Session(c.engine) as db, db.begin():
            if operation == "start":
                training_trials.start_trial(db, **context)
            elif operation == "heartbeat":
                training_trials.heartbeat_trial(db, **context)
            else:
                training_trials.finish_trial(db, **context, result={"cv_score": 1.0})


@pytest.mark.parametrize("operation", ["enroll", "finish"])
@pytest.mark.parametrize("first", ["cancel", "worker"])
def test_actual_cancellation_serializes_with_trial_enrollment_and_publication(
    cancellation_case, operation, first
):
    c = cancellation_case
    held, release, waiting = Event(), Event(), Event()
    pids = {}

    def work(db):
        if operation == "enroll":
            return enroll(db, c.scope, suggestion_id="racing", execution_id="racing-execution").id
        return training_trials.finish_trial(db, **c.contexts[3], result={"cv_score": 0.8})

    def owner():
        with Session(c.engine) as db, db.begin():
            pids["owner"] = db.scalar(text("SELECT pg_backend_pid()"))
            result = cancel(db, c) if first == "cancel" else work(db)
            held.set()
            assert release.wait(10)
            return result

    def waiter():
        with Session(c.engine) as db, db.begin():
            pids["waiter"] = db.scalar(text("SELECT pg_backend_pid()"))
            waiting.set()
            try:
                if first == "cancel":
                    work(db)
                else:
                    cancel(db, c)
            except StaleFence:
                assert first == "cancel"
                return "fenced"
            return "committed"

    with ThreadPoolExecutor(max_workers=2) as pool:
        owning = pool.submit(owner)
        try:
            assert held.wait(10)
            blocked = pool.submit(waiter)
            assert waiting.wait(10)
            deadline = time.monotonic() + 5
            observed = False
            while time.monotonic() < deadline:
                with c.engine.connect() as db:
                    observed = db.scalar(
                        text("SELECT :owner = ANY(pg_blocking_pids(:waiter))"), pids
                    )
                if observed:
                    break
                time.sleep(0.01)
            assert observed, (
                "The real waiter must block on the owned cancellation/trial transaction"
            )
        finally:
            release.set()
        result = owning.result(timeout=10)
        assert blocked.result(timeout=10) == ("fenced" if first == "cancel" else "committed")
    with Session(c.engine) as db:
        assert db.get(ModelRun, c.run_id).status == RunStatus.CANCELLED
        if first == "worker" and operation == "enroll":
            assert db.get(WorkflowAttempt, result).status == AttemptStatus.SUPERSEDED
        expected = (
            AttemptStatus.SUCCEEDED
            if first == "worker" and operation == "finish"
            else (AttemptStatus.SUPERSEDED)
        )
        assert db.get(WorkflowAttempt, c.contexts[3]["attempt_id"]).status == expected
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.model_run_id == c.run_id,
                    WorkflowAttempt.trial_id.is_not(None),
                    WorkflowAttempt.status.in_(
                        [
                            AttemptStatus.PENDING,
                            AttemptStatus.CLAIMED,
                            AttemptStatus.SUBMITTED,
                            AttemptStatus.RUNNING,
                        ]
                    ),
                )
            )
        )
