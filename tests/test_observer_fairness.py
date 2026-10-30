"""An invalid receipt must not abort another valid row in the same observation batch."""

import uuid
from datetime import UTC, datetime, timedelta

from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import reconciler
from ray_receipt_fixtures import seed_ray_receipt
from sqlalchemy import delete
from sqlalchemy.orm import Session
from test_ray_observer_identity import labeled_observation
from test_ray_observer_inspection import (
    committed_tenant as committed_tenant,
)
from test_ray_observer_inspection import (
    phase1_engine as phase1_engine,
)
from test_ray_observer_inspection import (
    profile_source as profile_source,
)
from test_ray_observer_inspection import (
    ray_scope as ray_scope,
)


def test_held_row_rotates_to_valid_work_after_a_full_scan_batch(ray_scope):
    engine, bad_id, resource_id, kind, k8s = ray_scope
    with Session(engine) as db, db.begin():
        bad = db.get(WorkflowAttempt, bad_id)
        original = db.get(ModelRun if kind == "training" else ProfilingJob, resource_id)
        if kind == "training":
            resource = ModelRun(
                project_id=bad.project_id,
                created_by_id=original.created_by_id,
                dataset_version_id=original.dataset_version_id,
                run_kind=RunKind.TRAINING,
                task_type="regression",
                status=RunStatus.RUNNING,
                k8s_namespace="observer-test",
            )
        else:
            resource = ProfilingJob(
                project_id=bad.project_id,
                created_by_id=original.created_by_id,
                dataset_id=original.dataset_id,
                dataset_version_id=original.dataset_version_id,
                status="running",
            )
        db.add(resource)
        db.flush()
        good = WorkflowAttempt(
            project_id=bad.project_id,
            stage=bad.stage,
            model_run_id=resource.id if kind == "training" else None,
            dataset_version_id=bad.dataset_version_id,
            logical_key=f"training-run:{resource.id}"
            if kind == "training"
            else f"profile:{resource.id}:{kind}",
            workload_identity="owned-good-observer",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            status=AttemptStatus.SUBMITTED,
            ray_job_name="owned-good-" + uuid.uuid4().hex,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        db.add(good)
        db.flush()
        good_id = good.id
        if kind != "training":
            resource.overview_json = {
                "workflow_attempt_id": str(good.id),
                "ray_job_namespace": "observer-test",
            }
        command = WorkflowCommand(
            project_id=bad.project_id,
            actor_id=original.created_by_id,
            operation="training.launch" if kind == "training" else "dataset.profile",
            idempotency_key=uuid.uuid4().hex,
            request_hash="a" * 64,
            resource_type="model_run" if kind == "training" else "profiling_job",
            resource_id=resource.id,
        )
        db.add(command)
        db.flush()
        seed_ray_receipt(db, good, resource, command, "observer-test")
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == bad.id, WorkflowEvent.event_key == "ray.submitted"
            )
        )
        body = labeled_observation(good, "observer-test", {"jobStatus": "RUNNING"})
        old_status = bad.status
    calls = []

    def read(name, *, namespace):
        calls.append(engine.pool.checkedout())
        assert name == body["metadata"]["name"] and namespace == "observer-test"
        return body

    k8s.ray_job.side_effect = read
    observer = (
        reconciler.observe_training_ray_jobs
        if kind == "training"
        else reconciler.observe_dataset_ray_jobs
    )
    clocks = (
        "updated_at",
        "heartbeat_at",
        "lease_expires_at",
        "lease_owner",
        "terminal_cas_version",
    )
    with Session(engine) as db:
        before = {
            key: tuple(getattr(db.get(WorkflowAttempt, key), field) for field in clocks)
            for key in (bad_id, good_id)
        }
    requests = []
    for _ in range(2):
        # Fresh sessions model a restarted observer; no in-memory cursor is retained.
        with Session(engine) as db, db.begin():
            observer(db, read=requests.append, limit=1)
    assert len(requests) == 1 and requests[0].attempt_id == good_id
    with Session(engine) as db:
        for key in (bad_id, good_id):
            row = db.get(WorkflowAttempt, key)
            assert tuple(getattr(row, field) for field in clocks) == before[key]
            assert row.last_observed_at is not None
    captured = reconciler.inspect_ray_observation(k8s, requests[0])
    with Session(engine) as db, db.begin():
        observer(db, attempt_id=good_id, read=lambda _request: captured)
    assert calls == [0]
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, bad_id).status == old_status
        assert db.get(WorkflowAttempt, good_id).status == AttemptStatus.RUNNING


def test_scan_clock_rolls_back_and_does_not_touch_worker_clocks(ray_scope):
    engine, identity, _, kind, _ = ray_scope
    observer = (
        reconciler.observe_training_ray_jobs
        if kind == "training"
        else reconciler.observe_dataset_ray_jobs
    )
    with Session(engine) as db:
        row = db.get(WorkflowAttempt, identity)
        before = (row.updated_at, row.heartbeat_at, row.lease_expires_at, row.terminal_cas_version)
    try:
        with Session(engine) as db, db.begin():
            observer(db, read=lambda _request: None)
            assert db.get(WorkflowAttempt, identity).last_observed_at is not None
            raise RuntimeError("owned rollback control")
    except RuntimeError as exc:
        assert str(exc) == "owned rollback control"
    with Session(engine) as db:
        row = db.get(WorkflowAttempt, identity)
        assert row.last_observed_at is None
        assert (
            row.updated_at,
            row.heartbeat_at,
            row.lease_expires_at,
            row.terminal_cas_version,
        ) == before


def test_two_scanners_skip_locked_rows_without_renewing_worker_leases(ray_scope):
    from concurrent.futures import ThreadPoolExecutor

    from sqlalchemy import select

    engine, identity, _, kind, _ = ray_scope
    observer = (
        reconciler.observe_training_ray_jobs
        if kind == "training"
        else reconciler.observe_dataset_ray_jobs
    )
    with Session(engine) as locked, locked.begin():
        locked.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == identity).with_for_update()
        )

        def scan():
            requests = []
            with Session(engine) as db, db.begin():
                assert observer(db, read=requests.append, limit=1) == 0
            return requests

        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(scan).result(timeout=5) == []
    with Session(engine) as db, db.begin():
        requests = []
        assert observer(db, read=requests.append, limit=1) == 1 and len(requests) == 1
