import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion, ProfilingJob
from automl_api.models.enums import DatasetFormat
from automl_api.models.iam import User
from automl_api.models.workflows import OutboxEntry
from automl_api.schemas.profiling_jobs import ProfilingJobCreate
from automl_api.services import profiling_jobs as jobs
from automl_api.services import workflow_state as state
from digest_fixtures import verified_digest_metadata
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


@pytest.fixture
def profile_source(phase1_engine, committed_tenant, monkeypatch, tmp_path):  # noqa: F811
    from automl_api.services import profiling, ray_polars_profiling
    from automl_api.storage.embedded import EmbeddedObjectStoreDriver

    user_id, project_id = committed_tenant
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="test")
    stored = store.put_bytes("profile.csv", b"x,y\n1,2\n3,4\n")
    monkeypatch.setattr(jobs, "get_object_store", lambda: store)
    monkeypatch.setattr(profiling, "get_object_store", lambda: store)
    monkeypatch.setattr(ray_polars_profiling, "get_object_store", lambda: store)
    with Session(phase1_engine) as db, db.begin():
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="Profile")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            **verified_digest_metadata(),
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            format=DatasetFormat.CSV,
            object_uri=stored.uri,
            original_filename="profile.csv",
            content_hash=hashlib.sha256(b"x,y\n1,2\n3,4\n").hexdigest(),
            byte_size=len(b"x,y\n1,2\n3,4\n"),
            row_count=2,
            column_count=2,
            schema_json={"columns": [{"name": "x"}, {"name": "y"}]},
        )
        db.add(version)
        db.flush()
        ids = (dataset.id, version.id)
    monkeypatch.setattr(jobs, "get_session_factory", lambda: lambda: Session(phase1_engine))
    return phase1_engine, user_id, project_id, *ids, store


def test_local_profile_creation_enqueues_one_durable_delivery(profile_source):
    engine, user_id, project_id, dataset_id, version_id, _ = profile_source
    with Session(engine) as db:
        user = db.get(User, user_id)
        job, created = jobs.create_profiling_job(
            db, user, project_id, dataset_id, version_id, ProfilingJobCreate()
        )
        assert created
        same, created = jobs.create_profiling_job(
            db, user, project_id, dataset_id, version_id, ProfilingJobCreate()
        )
        assert same.id == job.id and not created
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == project_id)))
        assert len(entries) == 1
        assert entries[0].topic == "dataset.profile.local"
        assert entries[0].payload == {"profiling_job_id": str(job.id)}
        db.commit()


def test_legacy_recovery_does_not_reset_a_live_delivery(profile_source):
    engine, user_id, project_id, dataset_id, version_id, _ = profile_source
    with Session(engine) as db, db.begin():
        job = ProfilingJob(
            project_id=project_id,
            dataset_id=dataset_id,
            dataset_version_id=version_id,
            created_by_id=user_id,
            status="running",
            overview_json={"execution_mode": "control_plane"},
        )
        db.add(job)
        db.flush()
        job_id = job.id
    assert jobs.resume_incomplete_profiling_jobs() == 1
    with Session(engine) as db:
        entry = state.claim_outbox(db, worker_id="live")[0]
        db.commit()
        lease = entry.lease_expires_at
        entry_id = entry.id
    assert jobs.resume_incomplete_profiling_jobs() == 0
    with Session(engine) as db:
        assert db.get(ProfilingJob, job_id).status == "running"
        assert db.get(OutboxEntry, entry_id).lease_expires_at == lease
        assert db.get(OutboxEntry, entry_id).lease_owner == "live"


def queued_profile(source, *, format=DatasetFormat.JSON):
    import io

    import pandas as pd

    engine, user_id, project_id, dataset_id, version_id, store = source
    content = b'[{"x":1,"y":2},{"x":3,"y":4}]'
    filename = "profile.json"
    if format == DatasetFormat.EXCEL:
        stream = io.BytesIO()
        pd.DataFrame({"x": [1, 3], "y": [2, 4]}).to_excel(stream, index=False)
        content, filename = stream.getvalue(), "profile.xlsx"
    elif format == DatasetFormat.CSV:
        content, filename = b"x,y\n1,2\n3,4\n", "profile.csv"
    stored = store.put_bytes(filename, content)
    with Session(engine) as db:
        version = db.get(DatasetVersion, version_id)
        version.format, version.original_filename = format, filename
        version.object_uri, version.byte_size = stored.uri, len(content)
        version.content_hash = hashlib.sha256(content).hexdigest()
        job, _ = jobs.create_profiling_job(
            db, db.get(User, user_id), project_id, dataset_id, version_id, ProfilingJobCreate()
        )
        job_id = job.id
        db.commit()
        entry = state.claim_outbox(db, worker_id="one", limit=1)[0]
        delivery = jobs.ProfileDelivery(
            entry.id, "one", entry.delivery_attempts, entry.available_at
        )
        db.commit()
    return job_id, delivery


@pytest.mark.parametrize("format", [DatasetFormat.CSV, DatasetFormat.EXCEL])
def test_monolithic_reader_preserves_csv_and_excel_with_fenced_publication(profile_source, format):
    engine, _, _, _, version_id, store = profile_source
    job_id, delivery = queued_profile(profile_source, format=format)
    jobs._run_monolithic_fallback(job_id, delivery)
    with Session(engine) as db:
        job = db.get(ProfilingJob, job_id)
        assert job.status == "succeeded"
        assert job.row_count == (0 if format == DatasetFormat.EXCEL else 2)
        if format == DatasetFormat.EXCEL:
            assert any("stored metadata only" in warning for warning in job.warnings_json)
        assert job.completed_columns == 2
        uri = db.get(DatasetVersion, version_id).profile_artifact_uri
        assert f"/deliveries/{delivery.entry_id}/{delivery.artifact_id}/complete.json" in uri
        assert store.exists(uri)


def test_local_delivery_replays_without_rewriting_artifacts(profile_source, monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.services.reconciler import reconcile_entry

    engine, _, _, _, _, store = profile_source
    job_id, delivery = queued_profile(profile_source)
    writes = MagicMock(wraps=store.put_bytes)
    monkeypatch.setattr(store, "put_bytes", writes)
    with Session(engine) as db:
        entry = db.get(OutboxEntry, delivery.entry_id)
        reconcile_entry(db, entry, worker_id="one")
        db.commit()
        written = writes.call_count
        assert written == 4
        for _ in range(2):
            reconcile_entry(db, entry, worker_id="one")
            db.commit()
        assert writes.call_count == written
        assert db.get(ProfilingJob, job_id).status == "succeeded"


@pytest.mark.parametrize("replacement_worker", ["two", "one"])
def test_stale_delivery_cannot_publish_after_compute(
    profile_source, monkeypatch, replacement_worker
):
    from unittest.mock import MagicMock

    from sqlalchemy import update

    engine, _, _, _, version_id, store = profile_source
    job_id, delivery = queued_profile(profile_source)
    original = jobs.profile_dataset_version
    writes = MagicMock(wraps=store.put_bytes)
    monkeypatch.setattr(store, "put_bytes", writes)

    def compute(*args):
        result = original(*args)
        with Session(engine) as db, db.begin():
            db.execute(
                update(OutboxEntry)
                .where(OutboxEntry.id == delivery.entry_id)
                .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            replacement = state.claim_outbox(db, worker_id=replacement_worker, limit=1)[0]
            assert replacement.delivery_attempts == 2
        return result

    monkeypatch.setattr(jobs, "profile_dataset_version", compute)
    with pytest.raises(state.StaleFence):
        jobs._run_monolithic_fallback(job_id, delivery)
    writes.assert_not_called()
    with Session(engine) as db:
        assert db.get(DatasetVersion, version_id).profile_artifact_uri is None
        assert db.get(ProfilingJob, job_id).status == "queued"


def test_failure_budget_and_operator_replay(profile_source, monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.reconciler import reconcile_entry

    engine, user_id, _, _, _, _ = profile_source
    job_id, delivery = queued_profile(profile_source)
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, delivery.entry_id)
        db.get(WorkflowCommand, entry.command_id).max_retries = 2
    original = jobs.profile_dataset_version
    monkeypatch.setattr(
        jobs, "profile_dataset_version", MagicMock(side_effect=RuntimeError("reader failed"))
    )
    for attempt in (1, 2):
        with Session(engine) as db:
            entry = db.get(OutboxEntry, delivery.entry_id)
            if attempt == 2:
                entry.available_at = datetime.now(UTC)
                db.commit()
                entry = state.claim_outbox(db, worker_id="one", limit=1)[0]
                db.commit()
            with pytest.raises(RuntimeError, match="reader failed"):
                reconcile_entry(db, entry, worker_id="one")
            db.commit()
            assert db.get(ProfilingJob, job_id).status == ("failed" if attempt == 2 else "queued")
            assert entry.status == (OutboxStatus.DEAD if attempt == 2 else OutboxStatus.PENDING)
    monkeypatch.setattr(jobs, "profile_dataset_version", original)
    with Session(engine) as db:
        state.replay_dead_outbox(db, delivery.entry_id, actor_id=user_id)
        db.commit()
        entry = state.claim_outbox(db, worker_id="one", limit=1)[0]
        db.commit()
        reconcile_entry(db, entry, worker_id="one")
        db.commit()
        assert db.get(ProfilingJob, job_id).status == "succeeded"
        assert entry.status == OutboxStatus.DELIVERED


def test_partitioned_csv_publishes_all_checkpoints(profile_source):
    from automl_api.services.reconciler import reconcile_entry

    engine, _, _, _, version_id, store = profile_source
    job_id, delivery = queued_profile(profile_source, format=DatasetFormat.CSV)
    with Session(engine) as db:
        reconcile_entry(db, db.get(OutboxEntry, delivery.entry_id), worker_id="one")
        db.commit()
        job = db.get(ProfilingJob, job_id)
        assert job.status == "succeeded"
        assert job.row_count == 2 and job.completed_columns == 2
        assert set(job.artifact_uris_json) == {
            "features",
            "relationships",
            "preparation",
            "complete",
        }
        assert all(store.exists(uri) for uri in job.artifact_uris_json.values())
        assert (
            db.get(DatasetVersion, version_id).profile_artifact_uri
            == job.artifact_uris_json["complete"]
        )


def test_heartbeat_renews_delivery_during_computation(profile_source):
    import time

    engine, *_ = profile_source
    job_id, delivery = queued_profile(profile_source)
    with Session(engine) as db:
        original = db.get(OutboxEntry, delivery.entry_id).lease_expires_at
    with jobs._profile_heartbeat(job_id, delivery):
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            with Session(engine) as db:
                entry = db.get(OutboxEntry, delivery.entry_id)
                if entry.lease_expires_at > original:
                    assert entry.lease_owner == delivery.worker_id
                    assert db.get(ProfilingJob, job_id).heartbeat_at is not None
                    break
            time.sleep(0.2)
        else:
            pytest.fail("The profile heartbeat did not renew its outbox lease")


def test_operator_replay_cannot_revive_an_old_delivery_fence(profile_source):
    engine, user_id, *_ = profile_source
    job_id, delivery = queued_profile(profile_source)
    from automl_api.models.workflows import WorkflowCommand

    with Session(engine) as db:
        entry = db.get(OutboxEntry, delivery.entry_id)
        db.get(WorkflowCommand, entry.command_id).max_retries = 1
        state.complete_outbox(db, entry.id, worker_id="one", delivered=False)
        db.commit()
        state.replay_dead_outbox(db, entry.id, actor_id=user_id)
        db.commit()
        replacement = state.claim_outbox(db, worker_id="one", limit=1)[0]
        assert replacement.delivery_attempts == delivery.generation
        db.commit()
    with Session(engine) as db, pytest.raises(state.StaleFence):
        jobs._locked_profile_job(db, job_id, delivery)


def test_cancelled_profile_cannot_publish_computed_artifacts(profile_source, monkeypatch):
    from unittest.mock import MagicMock

    engine, _, _, _, version_id, store = profile_source
    job_id, delivery = queued_profile(profile_source)
    original = jobs.profile_dataset_version
    writes = MagicMock(wraps=store.put_bytes)
    monkeypatch.setattr(store, "put_bytes", writes)

    def compute(*args):
        result = original(*args)
        with Session(engine) as db, db.begin():
            db.get(ProfilingJob, job_id).status = "cancelled"
        return result

    monkeypatch.setattr(jobs, "profile_dataset_version", compute)
    jobs._run_monolithic_fallback(job_id, delivery)
    writes.assert_not_called()
    with Session(engine) as db:
        assert db.get(ProfilingJob, job_id).status == "cancelled"
        assert db.get(DatasetVersion, version_id).profile_artifact_uri is None


def test_retry_reuses_committed_feature_checkpoint(profile_source, monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.services import ray_polars_profiling
    from automl_api.services.reconciler import reconcile_entry

    engine, *_ = profile_source
    job_id, delivery = queued_profile(profile_source, format=DatasetFormat.CSV)
    original = ray_polars_profiling._relationships
    monkeypatch.setattr(
        ray_polars_profiling, "_relationships", MagicMock(side_effect=RuntimeError("interrupted"))
    )
    with Session(engine) as db:
        with pytest.raises(RuntimeError, match="interrupted"):
            reconcile_entry(db, db.get(OutboxEntry, delivery.entry_id), worker_id="one")
        db.commit()
        job = db.get(ProfilingJob, job_id)
        assert job.status == "queued" and job.completed_columns == 2
        feature_uri = job.artifact_uris_json["features"]
    monkeypatch.setattr(ray_polars_profiling, "_relationships", original)
    columns = MagicMock(side_effect=AssertionError("Committed features must be reused"))
    monkeypatch.setattr(ray_polars_profiling, "_profile_column", columns)
    with Session(engine) as db:
        entry = state.claim_outbox(db, worker_id="two", limit=1)[0]
        db.commit()
        reconcile_entry(db, entry, worker_id="two")
        db.commit()
        job = db.get(ProfilingJob, job_id)
        assert job.status == "succeeded"
        assert job.artifact_uris_json["features"] == feature_uri
    columns.assert_not_called()
