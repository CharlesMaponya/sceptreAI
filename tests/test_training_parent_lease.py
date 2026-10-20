import sys
from datetime import UTC, datetime, timedelta
from threading import Event
from threading import enumerate as threads

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline, worker
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401


def test_parent_start_commits_owner_and_refuses_duplicate_worker(publication):  # noqa: F811
    engine, run_id, attempt_id, fence, _ = publication
    assert worker._begin_fenced_attempt(run_id, worker_id="worker-a") == (attempt_id, fence)
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.lease_owner == "worker-a" and attempt.lease_expires_at > datetime.now(UTC)
    assert worker._begin_fenced_attempt(run_id, worker_id="worker-a") == (attempt_id, fence)
    with pytest.raises(StaleFence):
        worker._begin_fenced_attempt(run_id, worker_id="worker-b")


@pytest.mark.parametrize("worker_id", ["", " ", "x" * 256])
def test_invalid_worker_identity_is_rejected(publication, worker_id):  # noqa: F811
    with pytest.raises(ValueError):
        worker._begin_fenced_attempt(publication[1], worker_id=worker_id)


def test_heartbeat_renews_only_current_live_parent_owner(publication):  # noqa: F811
    engine, run_id, attempt_id, fence, _ = publication
    worker._begin_fenced_attempt(run_id, worker_id="worker")
    soon = datetime.now(UTC) + timedelta(seconds=5)
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = soon
    worker._heartbeat_fenced_attempt(run_id, (attempt_id, fence), "worker")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).lease_expires_at > soon
    with pytest.raises(StaleFence):
        worker._heartbeat_fenced_attempt(run_id, (attempt_id, fence), "wrong-worker")


@pytest.mark.parametrize(
    "operation", ["start", "heartbeat", "progress", "success", "failure", "complete"]
)
def test_expired_parent_cannot_renew_or_publish(publication, operation):  # noqa: F811
    engine, run_id, attempt_id, fence, result = publication
    worker._begin_fenced_attempt(run_id, worker_id="worker")
    expired = datetime.now(UTC) - timedelta(seconds=1)
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = expired
    with pytest.raises(StaleFence):
        if operation == "start":
            worker._begin_fenced_attempt(run_id, worker_id="worker")
        elif operation == "heartbeat":
            worker._heartbeat_fenced_attempt(run_id, (attempt_id, fence), "worker")
        elif operation == "progress":
            pipeline._persist_candidate_phase(run_id, "Ridge", "fitting")
        elif operation == "success":
            pipeline._persist_training_success(run_id, result, "mlflow")
        elif operation == "failure":
            worker._fail_fenced_attempt(
                run_id, (attempt_id, fence), ValueError("failed"), worker_id="worker"
            )
        else:
            worker._complete_fenced_attempt(run_id, (attempt_id, fence), worker_id="worker")
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.lease_expires_at == expired and attempt.terminal_cas_version == 0
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING


def test_committed_result_remains_recoverable_after_parent_lease_expiry(publication):  # noqa: F811
    engine, run_id, attempt_id, fence, result = publication
    worker._begin_fenced_attempt(run_id, worker_id="worker")
    assert pipeline._persist_training_success(run_id, result, "mlflow")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    assert worker._begin_fenced_attempt(run_id, worker_id="recoverer") == (attempt_id, fence)
    assert worker._resume_committed_result(run_id, (attempt_id, fence))
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1


def configure_main(publication_scope, monkeypatch):
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "worker",
            "--run-id",
            str(publication_scope[1]),
            "--attempt-protocol",
            "training-parameters-v1",
        ],
    )
    monkeypatch.setattr(worker, "_PARENT_HEARTBEAT_INTERVAL", 0.01)
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", "direct")


def test_actual_entrypoint_heartbeats_and_stops_after_publication(publication, monkeypatch):  # noqa: F811
    engine, run_id, attempt_id, _, result = publication
    configure_main(publication, monkeypatch)
    renewed = Event()
    heartbeat = worker._heartbeat_fenced_attempt

    def observe(*args):
        heartbeat(*args)
        renewed.set()

    monkeypatch.setattr(worker, "_heartbeat_fenced_attempt", observe)

    def execute(_):
        assert renewed.wait(3), "parent heartbeat did not run"
        assert pipeline._persist_training_success(run_id, result, "mlflow")

    monkeypatch.setattr(pipeline, "execute_training_run", execute)
    worker.main()
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.status == AttemptStatus.SUCCEEDED and attempt.lease_owner
    assert not any(t.name == "training-parent-heartbeat" for t in threads())


@pytest.mark.parametrize("error", [ValueError("model failed"), StaleFence("ownership unresolved")])
def test_entrypoint_stops_heartbeat_and_preserves_uncertain_ownership(
    publication,  # noqa: F811
    monkeypatch,
    error,
):
    engine, _, attempt_id, _, _ = publication
    configure_main(publication, monkeypatch)

    def execute(_):
        raise error

    monkeypatch.setattr(pipeline, "execute_training_run", execute)
    with pytest.raises(type(error)):
        worker.main()
    with Session(engine) as db:
        expected = AttemptStatus.RUNNING if isinstance(error, StaleFence) else AttemptStatus.FAILED
        assert db.get(WorkflowAttempt, attempt_id).status == expected
    assert not any(t.name == "training-parent-heartbeat" for t in threads())


def test_parent_start_race_commits_one_owner(publication):  # noqa: F811
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    engine, run_id, attempt_id, _, _ = publication
    barrier = Barrier(2)

    def claim(owner):
        barrier.wait(timeout=3)
        try:
            worker._begin_fenced_attempt(run_id, worker_id=owner)
            return owner
        except StaleFence:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ["a", "b"]))
    assert results.count(None) == 1
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).lease_owner in results


def test_heartbeat_failure_aborts_entrypoint_without_marking_model_failure(
    publication,  # noqa: F811
    monkeypatch,
):
    from unittest.mock import MagicMock

    engine, _, attempt_id, _, _ = publication
    configure_main(publication, monkeypatch)

    def broken(*_):
        raise ConnectionError("heartbeat database acknowledgement unavailable")

    monkeypatch.setattr(worker, "_heartbeat_fenced_attempt", broken)

    def execute(_):
        thread = next(t for t in threads() if t.name == "training-parent-heartbeat")
        thread.join(timeout=3)
        assert not thread.is_alive()

    monkeypatch.setattr(pipeline, "execute_training_run", execute)
    failed = MagicMock(side_effect=AssertionError("uncertain ownership marked as model failure"))
    monkeypatch.setattr(worker, "_fail_fenced_attempt", failed)
    with pytest.raises(StaleFence, match="heartbeat"):
        worker.main()
    failed.assert_not_called()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.RUNNING
    assert not any(t.name == "training-parent-heartbeat" for t in threads())


@pytest.mark.parametrize("operation", ["failure", "complete"])
def test_another_worker_cannot_finish_owned_parent(publication, operation):  # noqa: F811
    _, run_id, attempt_id, fence, _ = publication
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    with pytest.raises(StaleFence, match="Another worker"):
        if operation == "complete":
            worker._complete_fenced_attempt(run_id, (attempt_id, fence), worker_id="other")
        else:
            worker._fail_fenced_attempt(
                run_id, (attempt_id, fence), ValueError("failed"), worker_id="other"
            )


def test_cancelled_run_cannot_be_resurrected_by_parent_start(publication):  # noqa: F811
    engine, run_id, attempt_id, _, _ = publication
    with Session(engine) as db, db.begin():
        db.get(ModelRun, run_id).status = RunStatus.CANCELLED
    with pytest.raises(StaleFence):
        worker._begin_fenced_attempt(run_id, worker_id="late-worker")
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.CANCELLED
        assert db.get(WorkflowAttempt, attempt_id).lease_owner is None
