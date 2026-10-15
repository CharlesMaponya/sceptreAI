from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingTrial, WorkflowAttempt, WorkflowEvent
from automl_api.services import training_trials as trials
from automl_api.services.workflow_state import StaleFence
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import enroll, trial_scope  # noqa: F401


@pytest.fixture
def execution(trial_scope):  # noqa: F811
    engine = trial_scope[0]
    with Session(engine) as db, db.begin():
        attempt = enroll(db, trial_scope)
        context = dict(
            attempt_id=attempt.id, fencing_token=attempt.fencing_token,
            run_fencing_token=trial_scope[3], worker_id="worker-a",
        )
    return engine, context, trial_scope


def start(execution):
    engine, context, _ = execution
    with Session(engine) as db, db.begin():
        assert trials.start_trial(db, **context) is None


@pytest.mark.parametrize("failed", [False, True])
def test_trial_terminal_replay_is_immutable_and_returns_evidence(execution, failed):
    engine, context, _ = execution
    start(execution)
    values = (
        {"failure": "estimator rejected parameters"} if failed else {"result": {"cv_score": 0.9}}
    )
    for _ in range(3):
        with Session(engine) as db, db.begin():
            payload = trials.finish_trial(db, **context, **values)
    with Session(engine) as db, db.begin():
        assert trials.start_trial(db, **{**context, "worker_id": "reader"}) == payload
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        assert attempt.terminal_cas_version == 1
        assert attempt.lease_expires_at is None
        assert db.scalar(select(func.count()).select_from(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id
        )) == 1
    with pytest.raises(StaleFence, match="different terminal"), Session(engine) as db, db.begin():
        trials.finish_trial(db, **context, result={"cv_score": 0.8})


@pytest.mark.parametrize("operation", ["start", "heartbeat", "finish"])
@pytest.mark.parametrize(
    "invalid", ["owner", "lease", "parent", "parent_fence", "child_fence", "run"],
)
def test_trial_operations_reject_invalid_ownership(execution, operation, invalid):
    engine, context, scope = execution
    start(execution)
    with Session(engine) as db, db.begin():
        if invalid == "lease":
            db.get(WorkflowAttempt, context["attempt_id"]).lease_expires_at = (
                datetime.now(UTC) - timedelta(seconds=1)
            )
        elif invalid == "parent":
            db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
        elif invalid == "run":
            db.get(ModelRun, scope[1]).status = RunStatus.CANCELLED
    changes = dict(context)
    if invalid == "owner":
        changes["worker_id"] = "different-worker"
    elif invalid == "parent_fence":
        changes["run_fencing_token"] = "wrong"
    elif invalid == "child_fence":
        changes["fencing_token"] = "wrong"
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        if operation == "start":
            trials.start_trial(db, **changes)
        elif operation == "heartbeat":
            trials.heartbeat_trial(db, **changes)
        else:
            trials.finish_trial(db, **changes, result={"cv_score": 0.9})
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, context["attempt_id"]).terminal_cas_version == 0
        assert db.scalar(select(func.count()).select_from(WorkflowEvent).where(
            WorkflowEvent.attempt_id == context["attempt_id"]
        )) == 0


def test_heartbeat_renews_only_live_lease(execution):
    engine, context, _ = execution
    start(execution)
    with Session(engine) as db, db.begin():
        soon = datetime.now(UTC) + timedelta(seconds=2)
        db.get(WorkflowAttempt, context["attempt_id"]).lease_expires_at = soon
    with Session(engine) as db, db.begin():
        trials.heartbeat_trial(db, **context)
        assert db.get(WorkflowAttempt, context["attempt_id"]).lease_expires_at > soon


@pytest.mark.parametrize("changed", [False, True])
def test_concurrent_terminal_publication_commits_once(execution, changed):
    engine, context, _ = execution
    start(execution)
    barrier = Barrier(2)

    def publish(index):
        barrier.wait(timeout=10)
        try:
            with Session(engine) as db, db.begin():
                trials.finish_trial(db, **context, result={"score": index if changed else 1})
            return "committed"
        except StaleFence:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(publish, range(2)))
    assert outcomes.count("conflict") == (1 if changed else 0)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, context["attempt_id"]).terminal_cas_version == 1
        assert db.scalar(select(func.count()).select_from(WorkflowEvent).where(
            WorkflowEvent.attempt_id == context["attempt_id"]
        )) == 1


def test_concurrent_workers_cannot_both_start(execution):
    engine, context, _ = execution
    barrier = Barrier(2)

    def claim(index):
        barrier.wait(timeout=10)
        try:
            with Session(engine) as db, db.begin():
                trials.start_trial(db, **{**context, "worker_id": f"worker-{index}"})
            return "owner"
        except StaleFence:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(claim, range(2))) == ["owner", "rejected"]


def test_lost_terminal_commit_ack_is_replayed_without_second_event(execution):
    engine, context, _ = execution
    start(execution)

    class LostCommit(Session):
        def commit(self):
            super().commit()
            raise ConnectionError("lost committed response")

    with LostCommit(engine) as db:
        expected = trials.finish_trial(db, **context, result={"score": 1})
        with pytest.raises(ConnectionError):
            db.commit()
    for _ in range(2):
        with Session(engine) as db, db.begin():
            assert trials.start_trial(db, **context) == expected
            assert trials.finish_trial(db, **context, result={"score": 1}) == expected
    with Session(engine) as db:
        assert db.scalar(select(func.count()).select_from(WorkflowEvent).where(
            WorkflowEvent.attempt_id == context["attempt_id"]
        )) == 1


@pytest.mark.parametrize("corruption", ["event", "digest", "active_result"])
def test_trial_evidence_corruption_fails_closed(execution, corruption):
    engine, context, _ = execution
    start(execution)
    with Session(engine) as db, db.begin():
        trials.finish_trial(db, **context, result={"score": 1})
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        if corruption == "event":
            event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
            event.payload = {"status": "succeeded", "result": {"score": 99}}
        elif corruption == "active_result":
            attempt.status = AttemptStatus.RUNNING
        else:
            db.get(TrainingTrial, attempt.trial_id).result_digest = "wrong"
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        trials.start_trial(db, **context)


def test_terminal_publication_rolls_back_as_one_transaction(execution):
    engine, context, _ = execution
    start(execution)
    with pytest.raises(ConnectionError), Session(engine) as db, db.begin():
        trials.finish_trial(db, **context, result={"score": 1})
        raise ConnectionError("transaction lost before commit")
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        assert attempt.status == AttemptStatus.RUNNING and attempt.terminal_cas_version == 0
        assert db.get(TrainingTrial, attempt.trial_id).result_digest is None
        assert db.scalar(select(func.count()).select_from(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id
        )) == 0


@pytest.mark.parametrize("values", [
    {"result": {"score": float("nan")}}, {"result": []}, {"failure": " "},
    {"result": {}, "failure": "ambiguous"}, {},
])
def test_invalid_terminal_payload_does_not_publish(execution, values):
    engine, context, _ = execution
    start(execution)
    with pytest.raises(ValueError), Session(engine) as db, db.begin():
        trials.finish_trial(db, **context, **values)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, context["attempt_id"]).status == AttemptStatus.RUNNING


def test_failure_evidence_redacts_credentials(execution):
    engine, context, _ = execution
    start(execution)
    with Session(engine) as db, db.begin():
        payload = trials.finish_trial(
            db, **context, failure="failed postgresql://user:trial-secret@db/test",
        )
        assert "trial-secret" not in payload["failure"]
        assert "trial-secret" not in db.get(WorkflowAttempt, context["attempt_id"]).terminal_reason


def test_lost_start_ack_retains_exclusive_owner(execution):
    engine, context, _ = execution

    class LostCommit(Session):
        def commit(self):
            super().commit()
            raise ConnectionError("lost start response")

    with LostCommit(engine) as db:
        assert trials.start_trial(db, **context) is None
        with pytest.raises(ConnectionError):
            db.commit()
    for _ in range(2):
        with Session(engine) as db, db.begin():
            assert trials.start_trial(db, **context) is None
            assert db.get(WorkflowAttempt, context["attempt_id"]).retry_count == 1
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        trials.start_trial(db, **{**context, "worker_id": "duplicate-owner"})


def test_exhausted_trial_cannot_acquire_a_worker_lease(execution):
    engine, context, _ = execution
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        attempt.retry_count = attempt.retry_budget
    with pytest.raises(StaleFence, match="retry budget"), Session(engine) as db, db.begin():
        trials.start_trial(db, **context)
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        assert attempt.status == AttemptStatus.PENDING and attempt.lease_owner is None


def test_cached_terminal_event_is_refreshed_before_replay(execution):
    engine, context, _ = execution
    start(execution)
    with Session(engine) as db, db.begin():
        trials.finish_trial(db, **context, result={"score": 1})
    with Session(engine, expire_on_commit=False) as cached:
        event = cached.scalar(select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == context["attempt_id"]
        ))
        cached.commit()
        with Session(engine) as db, db.begin():
            db.get(WorkflowEvent, event.id).payload = {
                "status": "succeeded", "result": {"score": 99}
            }
        assert event.payload["result"] == {"score": 1}
        with pytest.raises(StaleFence, match="evidence"):
            trials.start_trial(cached, **context)
        cached.rollback()
