import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import ray_observer_fixture
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, OutboxStatus, WorkflowStage
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.services import reconciler
from automl_api.services.workflow_state import StaleFence
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_local_profiling_delivery import profile_source  # noqa: F401
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_preparation_admission import dataset_scope  # noqa: F401


@pytest.fixture
def expired_dataset(dataset_scope):  # noqa: F811
    engine, profile_id, attempt_id, fence, stage, _, _, _ = dataset_scope
    with Session(engine) as db, db.begin():
        profile = db.get(ProfilingJob, profile_id)
        profile.overview_json = {
            **profile.overview_json,
            "ray_job_namespace": "dataset-expiry-test",
        }
        db.add(
            WorkflowCommand(
                project_id=profile.project_id,
                actor_id=profile.created_by_id,
                operation="dataset.profile",
                idempotency_key=uuid.uuid4().hex,
                request_hash="a" * 64,
                resource_type="profiling_job",
                resource_id=profile_id,
            )
        )
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.status = AttemptStatus.RUNNING
        attempt.ray_job_name = f"old-{uuid.uuid4().hex}"
        attempt.lease_owner = "dead-worker"
        attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    return engine, profile_id, attempt_id, fence, stage


def cancellation(db, profile_id):
    return db.scalar(
        select(OutboxEntry).where(
            OutboxEntry.aggregate_id == profile_id, OutboxEntry.topic == "ray.dataset.cancel"
        )
    )


def labeled_job(attempt, profile_id):
    return {
        "metadata": {
            "uid": "physical-job-uid",
            "namespace": "dataset-expiry-test",
            "labels": {
                "automl.platform/project-id": str(attempt.project_id),
                "automl.platform/profile-job-id": str(profile_id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/workflow-stage": attempt.stage.value,
                "automl.platform/generation": str(attempt.generation),
            },
        }
    }


def test_expiry_atomically_queues_cleanup_and_one_replacement(expired_dataset):
    engine, profile_id, old_id, fence, stage = expired_dataset
    k8s = MagicMock()
    with Session(engine) as db, db.begin():
        assert ray_observer_fixture.observe_dataset_ray_jobs(db, k8s) == 1
    k8s.ray_job.assert_not_called()
    k8s.delete_ray_job.assert_not_called()
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        successor = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
        )
        assert old.status == AttemptStatus.SUPERSEDED and old.terminal_cas_version == 1
        assert successor.generation == 2 and successor.fencing_token != fence
        assert successor.status == AttemptStatus.PENDING and successor.lease_expires_at is None
        assert db.get(ProfilingJob, profile_id).overview_json["workflow_attempt_id"] == str(
            successor.id
        )
        entries = list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == profile_id))
        )
        submit = (
            "ray.dataset.split.submit"
            if stage == WorkflowStage.SPLITTER
            else "ray.dataset.prepare.submit"
        )
        assert len(entries) == 2 and {e.topic for e in entries} == {submit, "ray.dataset.cancel"}
        assert cancellation(db, profile_id).payload == {
            "attempt_id": str(old_id),
            "fencing_token": fence,
            "namespace": "dataset-expiry-test",
        }
    with Session(engine) as db, db.begin():
        assert ray_observer_fixture.observe_dataset_ray_jobs(db, k8s) == 0


def test_expiry_at_budget_still_cleans_up(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, old_id).retry_budget = 1
    with Session(engine) as db, db.begin():
        ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.FAILED
        assert db.get(ProfilingJob, profile_id).status == "failed"
        assert cancellation(db, profile_id) is not None
        assert (
            db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            is None
        )


def test_replacement_and_cancel_intent_roll_back_together(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db:
        ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())
        db.rollback()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.RUNNING
        assert cancellation(db, profile_id) is None
        assert (
            db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            is None
        )


@pytest.mark.parametrize("state", ["live", "legacy", "committed", "moved", "inactive"])
def test_observer_preserves_live_legacy_and_inactive_scope(expired_dataset, state):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, old_id)
        profile = db.get(ProfilingJob, profile_id)
        if state == "live":
            attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        elif state == "legacy":
            attempt.lease_expires_at = None
        elif state == "committed":
            attempt.status, attempt.terminal_cas_version = AttemptStatus.SUCCEEDED, 1
            attempt.checkpoint_uri = "embedded://test/committed.json"
        elif state == "moved":
            profile.overview_json = {
                **profile.overview_json,
                "workflow_attempt_id": str(uuid.uuid4()),
            }
        else:
            profile.status = "failed"
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "RUNNING"}}
    with Session(engine) as db, db.begin():
        ray_observer_fixture.observe_dataset_ray_jobs(db, k8s)
    with Session(engine) as db:
        assert cancellation(db, profile_id) is None
        assert db.get(WorkflowAttempt, old_id).status == (
            AttemptStatus.SUCCEEDED if state == "committed" else AttemptStatus.RUNNING
        )
    if state in {"committed", "moved", "inactive"}:
        k8s.ray_job.assert_not_called()
    else:
        k8s.ray_job.assert_called_once_with(
            k8s.ray_job.call_args.args[0], namespace="dataset-expiry-test"
        )


def test_observer_refreshes_cached_attempt_and_profile(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as cached:
        cached.get(WorkflowAttempt, old_id)
        profile = cached.get(ProfilingJob, profile_id)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, old_id).status = AttemptStatus.SUPERSEDED
            db.get(ProfilingJob, profile_id).status = "failed"
        # SQL selection excludes a committed terminal attempt despite an old identity map.
        assert ray_observer_fixture.observe_dataset_ray_jobs(cached, MagicMock()) == 0
        cached.rollback()
    with Session(engine) as cached:
        profile = cached.get(ProfilingJob, profile_id)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, old_id).status = AttemptStatus.RUNNING
        k8s = MagicMock()
        assert ray_observer_fixture.observe_dataset_ray_jobs(cached, k8s) == 1
        assert profile.status == "failed"
        assert cancellation(cached, profile_id) is None
        k8s.ray_job.assert_not_called()


@pytest.mark.parametrize(
    "invalid",
    ["fence", "project", "profile", "aggregate_type", "live", "namespace", "labels"],
)
def test_cancel_rejects_wrong_scope_before_deletion(expired_dataset, invalid):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())
    with Session(engine) as db:
        saved = cancellation(db, profile_id)
        entry = SimpleNamespace(
            project_id=saved.project_id,
            aggregate_id=saved.aggregate_id,
            aggregate_type=saved.aggregate_type,
            payload=dict(saved.payload),
        )
        old = db.get(WorkflowAttempt, old_id)
        k8s = MagicMock()
        k8s.ray_job.return_value = labeled_job(old, profile_id)
        if invalid == "fence":
            entry.payload["fencing_token"] = "stale"
        elif invalid == "project":
            entry.project_id = uuid.uuid4()
        elif invalid == "profile":
            entry.aggregate_id = uuid.uuid4()
        elif invalid == "aggregate_type":
            entry.aggregate_type = "model_run"
        elif invalid == "live":
            successor = db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            successor.status = AttemptStatus.SUPERSEDED
            db.flush()
            old.status = AttemptStatus.RUNNING
            db.flush()
        elif invalid == "namespace":
            entry.payload.pop("namespace")
        else:
            k8s.ray_job.return_value["metadata"]["labels"]["automl.platform/attempt-id"] = str(
                uuid.uuid4()
            )
        with db.no_autoflush, pytest.raises(StaleFence):
            reconciler.cancel_dataset_ray_job(db, entry, k8s)
        k8s.delete_ray_job.assert_not_called()
        if invalid != "labels":
            k8s.ray_job.assert_not_called()


def test_cancellation_delivery_replays_after_lost_delete_ack(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())
    k8s = MagicMock()
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        k8s.ray_job.return_value = labeled_job(old, profile_id)
        name = old.ray_job_name
    k8s.delete_ray_job.side_effect = RuntimeError("delete committed; response lost")
    with Session(engine) as db, pytest.raises(RuntimeError):
        reconciler.cancel_dataset_ray_job(db, cancellation(db, profile_id), k8s)
    k8s.delete_ray_job.assert_called_once_with(
        name, namespace="dataset-expiry-test", uid="physical-job-uid"
    )
    k8s.ray_job.side_effect = ApiException(status=404)
    with Session(engine) as db, db.begin():
        entry = cancellation(db, profile_id)
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "cleanup-worker"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        entry_id = entry.id
        db.flush()
        reconciler.reconcile_entry(db, entry, worker_id="cleanup-worker", k8s=k8s)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        old = db.get(WorkflowAttempt, old_id)
        assert old.status == AttemptStatus.SUPERSEDED and old.terminal_cas_version == 1
        assert old.cleanup_state == "completed"


def test_dataset_cancel_rejects_training_attempt_before_external_effect():
    db, k8s = MagicMock(), MagicMock()
    db.scalar.return_value = SimpleNamespace(stage=WorkflowStage.TRAINING_RUN)
    entry = SimpleNamespace(payload={"attempt_id": str(uuid.uuid4()), "fencing_token": "f"})
    with pytest.raises(StaleFence):
        reconciler.cancel_dataset_ray_job(db, entry, k8s)
    k8s.ray_job.assert_not_called()
    k8s.delete_ray_job.assert_not_called()


def test_expiry_refreshes_cached_live_lease_before_decision(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, old_id).lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
    with Session(engine) as cached:
        old = cached.get(WorkflowAttempt, old_id)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, old_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        k8s = MagicMock()
        assert ray_observer_fixture.observe_dataset_ray_jobs(cached, k8s) == 1
        assert old.status == AttemptStatus.SUPERSEDED
        assert cancellation(cached, profile_id) is not None
        k8s.ray_job.assert_not_called()
        cached.commit()


def test_concurrent_expiry_observers_create_one_generation(expired_dataset):
    from concurrent.futures import ThreadPoolExecutor

    engine, profile_id, old_id, _, _ = expired_dataset

    def observe():
        with Session(engine) as db, db.begin():
            return ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())

    with ThreadPoolExecutor(max_workers=2) as workers:
        assert sum(workers.map(lambda _: observe(), range(2))) == 1
    with Session(engine) as db:
        successors = list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
        )
        assert len(successors) == 1
        entries = list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == profile_id))
        )
        assert len(entries) == 2


def test_delete_uid_conflict_keeps_cleanup_pending(expired_dataset):
    engine, profile_id, old_id, _, _ = expired_dataset
    with Session(engine) as db, db.begin():
        ray_observer_fixture.observe_dataset_ray_jobs(db, MagicMock())
    k8s = MagicMock()
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        k8s.ray_job.return_value = labeled_job(old, profile_id)
        k8s.delete_ray_job.side_effect = ApiException(status=409)
        with pytest.raises(ApiException):
            reconciler.cancel_dataset_ray_job(db, cancellation(db, profile_id), k8s)
        assert old.cleanup_state == "pending"


@pytest.mark.parametrize(
    "status", [AttemptStatus.SUPERSEDED, AttemptStatus.FAILED, AttemptStatus.CANCELLED]
)
def test_retired_submission_never_recreates_old_ray_job(expired_dataset, status):
    engine, profile_id, old_id, fence, stage = expired_dataset
    with Session(engine) as cached:
        old = cached.get(WorkflowAttempt, old_id)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, old_id).status = status
        k8s = MagicMock()
        entry = SimpleNamespace(
            project_id=old.project_id,
            aggregate_type="profiling_job",
            aggregate_id=profile_id,
            topic="ray.dataset.split.submit"
            if stage == WorkflowStage.SPLITTER
            else "ray.dataset.prepare.submit",
            payload={
                "attempt_id": str(old_id),
                "fencing_token": fence,
                "profiling_job_id": str(profile_id),
            },
        )
        assert reconciler.submit_dataset_ray_job(cached, entry, k8s) is old
        assert old.status == status
        k8s.create_ray_job.assert_not_called()
        k8s.ensure_service_account.assert_not_called()


@pytest.mark.parametrize("invalid", ["project", "profile", "aggregate", "pointer"])
def test_dataset_submit_scope_fails_before_native_creation(expired_dataset, invalid):
    engine, profile_id, old_id, fence, stage = expired_dataset
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        old.status = AttemptStatus.PENDING
        db.flush()
        entry = SimpleNamespace(
            project_id=old.project_id,
            aggregate_type="profiling_job",
            aggregate_id=profile_id,
            topic="ray.dataset.split.submit"
            if stage == WorkflowStage.SPLITTER
            else "ray.dataset.prepare.submit",
            payload={
                "attempt_id": str(old_id),
                "fencing_token": fence,
                "profiling_job_id": str(profile_id),
            },
        )
        if invalid == "project":
            entry.project_id = uuid.uuid4()
        elif invalid == "profile":
            entry.payload["profiling_job_id"] = str(uuid.uuid4())
        elif invalid == "aggregate":
            entry.aggregate_type = "model_run"
        else:
            profile = db.get(ProfilingJob, profile_id)
            profile.overview_json = {
                **profile.overview_json,
                "workflow_attempt_id": str(uuid.uuid4()),
            }
            db.flush()
        k8s = MagicMock()
        with pytest.raises(StaleFence):
            reconciler.submit_dataset_ray_job(db, entry, k8s)
        k8s.create_ray_job.assert_not_called()
        k8s.ensure_service_account.assert_not_called()
