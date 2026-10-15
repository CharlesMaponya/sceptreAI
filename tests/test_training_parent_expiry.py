import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.services import reconciler, training_trials
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import enroll, trial_scope  # noqa: F401


@pytest.fixture
def expired(trial_scope):  # noqa: F811
    engine, run_id, parent_id, _, _ = trial_scope
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.k8s_namespace = "isolated-expiry-test"
        db.add(
            WorkflowCommand(
                project_id=run.project_id,
                actor_id=run.created_by_id,
                operation="training.launch",
                idempotency_key=uuid.uuid4().hex,
                request_hash="a" * 64,
                resource_type="model_run",
                resource_id=run.id,
            )
        )
        parent = db.get(WorkflowAttempt, parent_id)
        parent.ray_job_name = f"old-{uuid.uuid4().hex}"
        parent.lease_owner = "dead-worker"
        parent.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        for index in range(2):
            child = enroll(
                db, trial_scope, suggestion_id=f"trial-{index}", execution_id=f"old-{index}"
            )
            training_trials.start_trial(
                db,
                attempt_id=child.id,
                fencing_token=child.fencing_token,
                run_fencing_token=parent.fencing_token,
                worker_id=f"worker-{index}",
            )
        parent.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    return trial_scope


def test_expired_parent_queues_cleanup_and_replacement_without_native_side_effects(expired):
    engine, run_id, parent_id, _, _ = expired
    k8s = MagicMock()
    with Session(engine) as db, db.begin():
        assert reconciler.observe_training_ray_jobs(db, k8s) == 1
    k8s.ray_job.assert_not_called()
    k8s.delete_ray_job.assert_not_called()
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, parent_id)
        assert old.status == AttemptStatus.SUPERSEDED
        old_children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(old_children) == 2 and all(
            c.status == AttemptStatus.SUPERSEDED for c in old_children
        )
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))
        assert {e.topic for e in entries} == {"ray.training.cancel", "ray.training.submit"}
        assert len(entries) == 2
        cancel_id = next(e.id for e in entries if e.topic == "ray.training.cancel")
    for _ in range(2):
        with Session(engine) as db, db.begin():
            assert reconciler.observe_training_ray_jobs(db, k8s) == 0
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, cancel_id)
        old = db.get(WorkflowAttempt, parent_id)
        reconciler.cancel_training_ray_job(db, entry, k8s)
        k8s.delete_ray_job.assert_called_once_with(
            old.ray_job_name, namespace="isolated-expiry-test"
        )
        assert old.status == AttemptStatus.SUPERSEDED


@pytest.mark.parametrize("terminal", [False, True])
def test_wrong_fence_never_deletes_even_a_terminal_attempt(expired, terminal):
    from types import SimpleNamespace

    engine, run_id, parent_id, _, _ = expired
    with Session(engine) as db, db.begin():
        parent = db.get(WorkflowAttempt, parent_id)
        if terminal:
            parent.status = AttemptStatus.SUPERSEDED
        entry = SimpleNamespace(
            project_id=parent.project_id,
            aggregate_type="model_run",
            aggregate_id=run_id,
            payload={"attempt_id": str(parent.id), "fencing_token": "wrong"},
        )
        k8s = MagicMock()
        with pytest.raises(StaleFence):
            reconciler.cancel_training_ray_job(db, entry, k8s)
        k8s.delete_ray_job.assert_not_called()


@pytest.mark.parametrize("invalid", ["project", "aggregate_id", "aggregate_type", "trial_stage"])
def test_cancel_scope_is_checked_before_external_delete(expired, invalid):
    from types import SimpleNamespace

    engine, run_id, parent_id, fence, _ = expired
    with Session(engine) as db:
        parent = db.get(WorkflowAttempt, parent_id)
        entry = SimpleNamespace(
            project_id=parent.project_id,
            aggregate_type="model_run",
            aggregate_id=run_id,
            payload={"attempt_id": str(parent_id), "fencing_token": fence},
        )
        if invalid == "project":
            entry.project_id = uuid.uuid4()
        elif invalid == "aggregate_id":
            entry.aggregate_id = uuid.uuid4()
        elif invalid == "aggregate_type":
            entry.aggregate_type = "another-scope"
        else:
            child = db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id)
            )
            entry.payload = {"attempt_id": str(child.id), "fencing_token": child.fencing_token}
        k8s = MagicMock()
        with pytest.raises(StaleFence):
            reconciler.cancel_training_ray_job(db, entry, k8s)
        k8s.delete_ray_job.assert_not_called()


def test_expiry_at_retry_budget_still_queues_cleanup(expired):
    engine, run_id, parent_id, _, _ = expired
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, parent_id).retry_budget = 1
    k8s = MagicMock()
    with Session(engine) as db, db.begin():
        reconciler.observe_training_ray_jobs(db, k8s)
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.FAILED
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.FAILED
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))
        assert len(entries) == 1 and entries[0].topic == "ray.training.cancel"
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == parent_id)
            )
        )
    k8s.delete_ray_job.assert_not_called()


def test_expiry_replacement_and_cleanup_intent_roll_back_together(expired):
    engine, run_id, parent_id, _, _ = expired
    with pytest.raises(RuntimeError), Session(engine) as db, db.begin():
        reconciler.observe_training_ray_jobs(db, MagicMock())
        raise RuntimeError("before reconciliation commit")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.RUNNING
        assert not list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == parent_id)
            )
        )


@pytest.mark.parametrize("lease", ["live", "legacy"])
def test_live_and_legacy_parent_do_not_expire(expired, lease):
    engine, run_id, parent_id, _, _ = expired
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, parent_id).lease_expires_at = (
            datetime.now(UTC) + timedelta(seconds=90) if lease == "live" else None
        )
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "RUNNING"}}
    with Session(engine) as db, db.begin():
        reconciler.observe_training_ray_jobs(db, k8s)
    k8s.ray_job.assert_called_once()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.RUNNING
        assert not list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))


def test_committed_result_wins_over_expired_lease(publication):  # noqa: F811
    engine, run_id, parent_id, _, result = publication
    assert pipeline._persist_training_success(run_id, result, "mlflow")
    with Session(engine) as db, db.begin():
        parent = db.get(WorkflowAttempt, parent_id)
        parent.ray_job_name = "committed-job"
        parent.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    k8s = MagicMock()
    with Session(engine) as db, db.begin():
        reconciler.observe_training_ray_jobs(db, k8s)
    k8s.ray_job.assert_not_called()
    k8s.delete_ray_job.assert_not_called()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.SUCCEEDED
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert not list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))


def test_concurrent_expiry_observers_queue_one_cleanup_and_replacement(expired):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    engine, run_id, _, _, _ = expired
    barrier = Barrier(2)

    def observe(_):
        barrier.wait(timeout=3)
        with Session(engine) as db, db.begin():
            return reconciler.observe_training_ray_jobs(db, MagicMock())

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(observe, range(2))) == [0, 1]
    with Session(engine) as db:
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))
        assert len(entries) == 2


def test_lost_delete_ack_replays_without_changing_old_terminal_fence(expired):
    engine, run_id, parent_id, _, _ = expired
    with Session(engine) as db, db.begin():
        reconciler.observe_training_ray_jobs(db, MagicMock())
    k8s = MagicMock()
    k8s.delete_ray_job.side_effect = [ConnectionError("lost delete ack"), None]
    with pytest.raises(ConnectionError), Session(engine) as db, db.begin():
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run_id, OutboxEntry.topic == "ray.training.cancel"
            )
        )
        reconciler.cancel_training_ray_job(db, entry, k8s)
    with Session(engine) as db, db.begin():
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run_id, OutboxEntry.topic == "ray.training.cancel"
            )
        )
        reconciler.cancel_training_ray_job(db, entry, k8s)
        parent = db.get(WorkflowAttempt, parent_id)
        assert parent.status == AttemptStatus.SUPERSEDED and parent.terminal_cas_version == 1
    assert k8s.delete_ray_job.call_count == 2
