"""Actual worker deletion must release SQL and retain qualified native history."""

import traceback
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api import workflow_reconciler
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionStage, DeletionTombstone, OutboxEntry
from automl_api.services import deletion, reconciler
from automl_api.services.deletion_workflow import reconcile_deletion as reconcile_factory
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.workflow_reconciler import run_once
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def factory_case(phase1_engine, committed_tenant, tmp_path, monkeypatch):
    actor_id, project_id = committed_tenant
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    c = SimpleNamespace(
        engine=phase1_engine,
        factory=factory,
        actor_id=actor_id,
        project_id=project_id,
        checkedout=[],
        deleted=[],
        callback=None,
        errors=[],
        inspected=[],
    )

    class Native(EmbeddedObjectStoreDriver):
        def prefix_absent(self, prefix):
            assert phase1_engine.pool.checkedout() == 0
            c.inspected.append(prefix)
            return super().prefix_absent(prefix)

        def delete_prefix(self, prefix):
            c.checkedout.append(phase1_engine.pool.checkedout())
            assert c.checkedout[-1] == 0
            if c.callback:
                c.callback()
            super().delete_prefix(prefix)
            c.deleted.append(prefix)

    c.store = Native(root=tmp_path, bucket="test")
    c.uri = c.store.put_bytes(f"projects/{project_id}/owned.txt", b"owned").uri
    c.foreign_uri = c.store.put_bytes(f"projects/{uuid.uuid4()}/preserve.txt", b"preserve").uri

    def store(*_):
        c.checkedout.append(phase1_engine.pool.checkedout())
        assert c.checkedout[-1] == 0
        return c.store

    def dispatch(*args, **kwargs):
        try:
            return reconcile_factory(*args, **kwargs)
        except Exception:
            c.errors.append(traceback.format_exc())
            raise

    monkeypatch.setattr(workflow_reconciler, "reconcile_deletion", dispatch)
    monkeypatch.setattr(deletion, "get_object_store", store)
    monkeypatch.setattr(reconciler, "KubernetesTrainingClient", MagicMock)
    with factory() as db, db.begin():
        response = deletion.request_deletion(db, db.get(User, actor_id), project_id)
        c.tombstone_id = uuid.UUID(response["id"])
        c.entry_id = db.scalar(select(OutboxEntry.id).where(OutboxEntry.project_id == project_id))
    try:
        yield c
    finally:
        with factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == project_id))
            db.execute(delete(DeletionTombstone).where(DeletionTombstone.project_id == project_id))


def _dispatch(c, count=1):
    for _ in range(count):
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            if entry is None:
                return
            entry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        run_once(c.factory, worker_id="owned-deletion-worker")


def test_actual_dispatcher_releases_sql_before_native_cleanup(factory_case):
    c = factory_case
    _dispatch(c, 8)
    assert not c.errors, "\n".join(c.errors)
    assert c.checkedout and set(c.checkedout) == {0}
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None
        assert db.get(DeletionTombstone, c.tombstone_id).status == "complete"
    assert not c.store.exists(c.uri) and c.store.exists(c.foreign_uri)


def test_historical_stage_proof_survives_actual_project_command_and_outbox_erasure(factory_case):
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.deletion_workflow import _proof

    c = factory_case
    with c.factory() as db:
        command_id = db.get(OutboxEntry, c.entry_id).command_id
    _dispatch(c, 8)
    assert not c.errors
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None and db.get(OutboxEntry, c.entry_id) is None
        assert db.get(WorkflowCommand, command_id) is None
        stages = list(
            db.scalars(select(DeletionStage).where(DeletionStage.project_id == c.project_id))
        )
        assert {stage.resource_class for stage in stages} == set(deletion.STAGES["project"])
        for stage in stages:
            assert stage.status == "completed"
            assert stage.custody["intent"]["project_id"] == str(c.project_id)
            assert stage.custody["intent"]["command_id"] == str(command_id)
            _proof(stage.custody)


def test_completed_legacy_flags_cannot_skip_native_qualification(factory_case):
    c = factory_case
    with c.factory() as db, db.begin():
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id, DeletionStage.resource_class == "runtime"
            )
        )
        stage.status = "completed"
    _dispatch(c)
    assert c.errors and "Legacy deletion completion" in c.errors[-1]
    assert not c.checkedout and not c.deleted
    with c.factory() as db:
        assert db.get(Project, c.project_id) is not None
        assert db.get(DeletionTombstone, c.tombstone_id).status == "pending"


def test_policy_change_keeps_known_native_fact_without_publishing_or_redeleting(factory_case):
    c = factory_case
    _dispatch(c)

    def hold():
        c.callback = None
        with c.factory() as db, db.begin():
            db.get(DeletionTombstone, c.tombstone_id).legal_hold = True

    c.callback = hold
    _dispatch(c)
    assert c.deleted == [f"projects/{c.project_id}"]
    with c.factory() as db:
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.status == "pending" and "published" not in stage.custody
        assert stage.custody["attempts"][0]["outcome"]["absent"] is True
        assert db.get(Project, c.project_id) is not None
    with c.factory() as db, db.begin():
        db.get(DeletionTombstone, c.tombstone_id).legal_hold = False
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors
    assert c.deleted.count(f"projects/{c.project_id}") == 1
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


def test_unknown_cleanup_is_only_inspected_after_lease_expiry(factory_case, monkeypatch):
    c = factory_case
    _dispatch(c)
    original = c.store.delete_prefix

    def lose_ack(prefix):
        original(prefix)
        raise ConnectionError("owned test lost native deletion acknowledgement")

    monkeypatch.setattr(c.store, "delete_prefix", lose_ack)
    _dispatch(c)
    assert c.errors and len(c.deleted) == 1
    _dispatch(c)
    assert len(c.deleted) == 1
    with c.factory() as db, db.begin():
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.custody["attempts"][0].get("outcome") is None
        stage.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    monkeypatch.setattr(c.store, "delete_prefix", original)
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors and c.deleted.count(f"projects/{c.project_id}") == 1
    with c.factory() as db:
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.status == "completed"
        assert stage.custody["attempts"][0].get("outcome") is None
        assert stage.custody["attempts"][1]["recovery"] is True
        assert stage.custody["attempts"][1]["reconciliation"]["absent"] is True
        assert db.get(Project, c.project_id) is None


def test_unknown_delete_before_effect_repairs_only_original_prefix_after_expiry(
    factory_case, monkeypatch
):
    c = factory_case
    _dispatch(c)
    original = c.store.delete_prefix
    monkeypatch.setattr(
        c.store, "delete_prefix", MagicMock(side_effect=ConnectionError("before effect"))
    )
    _dispatch(c)
    assert c.store.exists(c.uri) and not c.deleted
    _dispatch(c)
    assert not c.deleted
    with c.factory() as db, db.begin():
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        stage.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    monkeypatch.setattr(c.store, "delete_prefix", original)
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors and c.deleted.count(f"projects/{c.project_id}") == 1
    assert c.store.exists(c.foreign_uri) and not c.store.exists(c.uri)


@pytest.mark.parametrize("mode", ["before", "after", "database_after", "database_fact"])
def test_actual_process_and_database_loss_recover_original_deletion(factory_case, mode):
    import json
    import signal
    import subprocess
    import sys
    import time
    from pathlib import Path

    from automl_api.models.enums import OutboxStatus
    from sqlalchemy import text

    c = factory_case
    _dispatch(c)  # Commit qualified empty runtime closure first.
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = "owned-deletion-process"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts += 1
    root = c.store.root.resolve()
    marker, log_path = root / f"fault-{mode}.json", root / f"child-{mode}.log"
    child_script = Path(".unlazy/phase1-deletion-factory/process_child.py").resolve()
    with log_path.open("w") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                str(child_script),
                str(c.entry_id),
                str(c.project_id),
                str(root),
                mode,
                str(marker),
            ],
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 30
            while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            assert marker.exists(), log_path.read_text()[-2000:]
            witness = json.loads(marker.read_text())
            assert witness["pid"] == child.pid and witness["project_id"] == str(c.project_id)
            assert child.poll() is None
            assert witness["sql_checked_out"] == int(mode == "database_fact")
            if mode.startswith("database"):
                with c.engine.connect() as admin, admin.begin():
                    terminated = admin.scalar(
                        text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE pid=:pid AND datname=:database AND application_name=:application"
                        ),
                        dict(
                            pid=witness["backend_pid"],
                            database="automl_branch_0e6a232bc7_tests",
                            application=f"phase1_deletion_fault_{child.pid}",
                        ),
                    )
                    assert terminated is True
                child.stdin.write(b"c")
                child.stdin.flush()
                assert child.wait(timeout=20) == 1, log_path.read_text()[-2000:]
                assert (
                    "OperationalError" in log_path.read_text()
                    or "DBAPIError" in log_path.read_text()
                )
            else:
                child.kill()
                assert child.wait(timeout=10) == -signal.SIGKILL
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            child.stdin.close()
    assert c.store.exists(c.uri) is (mode == "before")
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.custody["attempts"][0].get("outcome") is None
        stage.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        if entry.status == OutboxStatus.CLAIMED:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    _dispatch(c, 8)
    assert not c.errors and c.store.exists(c.foreign_uri)
    assert c.deleted.count(f"projects/{c.project_id}") == int(mode == "before")
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.status == "completed"
        assert stage.custody["attempts"][0].get("outcome") is None
        assert stage.custody["attempts"][1]["recovery"] is True
        assert stage.custody["attempts"][1]["reconciliation"]["absent"] is True


@pytest.mark.parametrize("field", ["project-id", "run-id", "attempt-id", "generation"])
def test_original_uid_does_not_override_changed_runtime_ownership(field):
    from automl_api.services.deletion_runtime import cleanup_runtime
    from automl_api.services.workflow_state import StaleFence

    labels = {
        "automl.platform/project-id": "project",
        "automl.platform/run-id": "run",
        "automl.platform/attempt-id": "attempt",
        "automl.platform/generation": "1",
    }
    target = dict(
        kind="RayJob",
        uid="original",
        namespace="original-ns",
        name="original-name",
        project_id="project",
        run_id="run",
        attempt_id="attempt",
        labels=labels,
    )
    native = MagicMock()
    native.ray_job.return_value = {
        "metadata": {
            "uid": target["uid"],
            "namespace": target["namespace"],
            "name": target["name"],
            "labels": {**labels, f"automl.platform/{field}": "other"},
        }
    }
    with pytest.raises(StaleFence, match="ownership"):
        cleanup_runtime(native, [target])
    native.delete_ray_job.assert_not_called()


@pytest.mark.parametrize(
    "kind", ["uploads", "versions", "markers", "empty", "truncated", "missing_status", "outside"]
)
def test_s3_prefix_absence_is_read_only_and_requires_qualified_complete_inventory(kind):
    from automl_api.storage.contracts import DeletionPending, UploadContractError
    from automl_api.storage.s3 import S3ObjectStoreDriver

    native = MagicMock()
    uploads = {"ResponseMetadata": {"HTTPStatusCode": 200}, "IsTruncated": False}
    versions = dict(uploads)
    if kind == "uploads":
        uploads["Uploads"] = [{"Key": "projects/own/blob", "UploadId": "original"}]
    elif kind == "versions":
        versions["Versions"] = [{"Key": "projects/own/blob", "VersionId": "v1"}]
    elif kind == "markers":
        versions["DeleteMarkers"] = [{"Key": "projects/own/blob", "VersionId": "v1"}]
    elif kind == "truncated":
        versions["IsTruncated"] = True
    elif kind == "missing_status":
        versions.pop("ResponseMetadata")
    elif kind == "outside":
        versions["Versions"] = [{"Key": "projects/peer/blob", "VersionId": "v1"}]
    native.list_multipart_uploads.return_value = uploads
    native.list_object_versions.return_value = versions
    store = S3ObjectStoreDriver(bucket="owned", client=native)
    if kind in {"truncated", "missing_status", "outside"}:
        with pytest.raises((DeletionPending, UploadContractError)):
            store.prefix_absent("projects/own")
    else:
        assert store.prefix_absent("projects/own") is (kind == "empty")
    native.delete_objects.assert_not_called()
    native.abort_multipart_upload.assert_not_called()


@pytest.mark.parametrize("provider", ["gcs", "azure_blob"])
@pytest.mark.parametrize("state", ["empty", "current", "retained", "outside"])
def test_cloud_prefix_inspection_never_mutates_or_mistakes_retained_data_for_absence(
    provider, state
):
    from automl_api.storage.azure import AzureBlobObjectStoreDriver
    from automl_api.storage.contracts import UploadContractError
    from automl_api.storage.gcs import GCSObjectStoreDriver

    native = MagicMock()
    blob = SimpleNamespace(name="projects/peer/blob" if state == "outside" else "projects/own/blob")
    if provider == "gcs":
        store = GCSObjectStoreDriver(bucket="owned", client=native)
        native.list_blobs.side_effect = lambda *_, **kwargs: (
            [blob]
            if state in {"current", "outside"} or state == "retained" and kwargs.get("soft_deleted")
            else []
        )
    else:
        native.get_container_client.return_value = native
        store = AzureBlobObjectStoreDriver(
            account_url="https://owned.blob.core.windows.net",
            account_name="owned",
            container="owned",
            credential="owned-test",
            service_client=native,
        )
        native.list_blobs.return_value = [] if state == "empty" else [blob]
    if state == "outside":
        with pytest.raises(UploadContractError):
            store.prefix_absent("projects/own")
    else:
        assert store.prefix_absent("projects/own") is (state == "empty")
    native.delete_blob.assert_not_called()
    native.delete_blobs.assert_not_called()


def test_live_native_lease_and_late_fact_cannot_acknowledge_a_successor(factory_case):
    import threading

    from automl_api.services.deletion_workflow import reconcile_deletion

    c = factory_case
    _dispatch(c)
    entered, release = threading.Event(), threading.Event()

    def pause():
        entered.set()
        assert release.wait(15)

    c.callback = pause
    worker = threading.Thread(target=_dispatch, args=(c,))
    worker.start()
    try:
        assert entered.wait(15)
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            entry.lease_owner = "successor"
            entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        constructors = len(c.checkedout)
        reconcile_deletion(c.factory, c.entry_id, "successor")
        assert not c.deleted and len(c.checkedout) == constructors
    finally:
        c.callback = None
        release.set()
        worker.join(15)
    assert not worker.is_alive() and c.errors and "lease" in c.errors[-1]
    with c.factory() as db:
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == c.project_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.status == "pending" and "published" not in stage.custody
        assert stage.custody["attempts"][0]["outcome"]["absent"] is True
        assert db.get(Project, c.project_id) is not None
    c.errors.clear()
    _dispatch(c, 8)
    assert not c.errors and c.deleted.count(f"projects/{c.project_id}") == 1
    assert c.store.exists(c.foreign_uri)
