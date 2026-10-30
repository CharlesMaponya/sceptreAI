"""Real SQL and a versioned SDK double exercise buffered native custody."""

import json
import signal
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectRole, ProjectStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services import buffered_chunks, buffered_uploads, deletion
from automl_api.services import buffered_execution as execution
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import ObjectMetadata, UploadContractError
from fastapi import HTTPException
from fault_worker_environment import environment
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker
from test_buffered_input_admission import admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


class Native:
    driver_name = "s3_compatible"

    def __init__(self, c, root):
        assert c.engine.pool.checkedout() == 0
        self.c, self.root = c, root
        self.create_hook = self.read_hook = None
        self.reply_patch = {}
        self.create_error = None
        self.read_error = None
        self.reads, self.creates, self.checkouts = [], [], []
        self.root.mkdir(parents=True, exist_ok=True)

    def _free(self):
        self.checkouts.append(self.c.engine.pool.checkedout())
        assert self.checkouts[-1] == 0

    def uri_for_key(self, key):
        self._free()
        return f"s3c://{self.c.settings.object_store_bucket}/{key}"

    def create_bytes(self, key, body):
        self._free()
        with self.c.factory() as db:
            row = db.get(BufferedUploadInput, self.c.identifier)
            assert row.phase == "issued" and b"".join(buffered_chunks.load_parts(db, row)) == body
            assert db.get(WorkflowCommand, self.c.identifier).status == CommandStatus.RUNNING
        if self.create_hook:
            self.create_hook()
        version = str(uuid.uuid4())
        (self.root / version).write_bytes(body)
        self.creates.append((key, version, body))
        (self.root / "accepted.json").write_text(json.dumps(dict(key=key, version=version)))
        if self.create_error:
            raise self.create_error
        values = dict(
            uri=self.uri_for_key(key), byte_size=len(body), provider_headers={"version_id": version}
        )
        values.update(self.reply_patch)
        return ObjectMetadata(**values)

    def stat(self, uri, *, version=None):
        self._free()
        assert version is not None, "Never read latest metadata"
        self.reads.append(("stat", uri, version))
        return ObjectMetadata(
            uri=uri,
            byte_size=(self.root / version).stat().st_size,
            provider_headers={"version_id": version},
        )

    def open_stream(self, uri, *, version=None):
        self._free()
        assert version is not None, "Never read the latest byte stream"
        self.reads.append(("stream", uri, version))
        if self.read_hook:
            self.read_hook()
        if self.read_error:
            raise self.read_error
        return (self.root / version).open("rb")


@pytest.fixture
def pending(admission, tmp_path):
    c = admission
    c.settings = replace(
        buffered_uploads.get_settings(),
        object_store_type="s3_compatible",
        object_store_endpoint="https://objects.test",
        object_store_access_key="test",
        object_store_secret_key="test",
    )
    c.identifier = c.admit(settings=c.settings)
    c.worker = "owned-buffered-execution"
    c.native = Native(c, tmp_path / "native")

    def claim():
        with c.factory() as db, db.begin():
            row = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == c.identifier))
            row.status, row.lease_owner = OutboxStatus.CLAIMED, c.worker
            row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            row.delivery_attempts += 1
            c.entry_id = row.id

    c.claim = claim
    claim()
    c.run = lambda: execution.reconcile_buffered_upload(
        c.factory, c.entry_id, c.worker, store_factory=lambda _: c.native, settings=c.settings
    )
    return c


def test_original_reply_is_verified_then_published_atomically(pending):
    c = pending
    c.run()
    with c.factory() as db:
        row, command = (
            db.get(BufferedUploadInput, c.identifier),
            db.get(WorkflowCommand, c.identifier),
        )
        assert row.phase == "published" and row.content is None
        assert command.status == CommandStatus.SUCCEEDED and command.response_status == 201
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        version = db.scalar(select(DatasetVersion).where(DatasetVersion.project_id == c.project_id))
        assert version.content_hash == row.content_sha256 and version.byte_size == len(c.body)
        assert version.content_hash_verification_status == "verified"
        assert version.original_filename == c.payload.filename
        cached = command.response_payload
    c.run()
    assert len(c.native.creates) == 1
    assert {v for _, _, v in c.native.reads} == {c.native.creates[0][1]}
    assert set(c.native.checkouts) == {0}
    assert c.admit(settings=c.settings) == c.identifier
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.identifier).response_payload == cached


@pytest.mark.parametrize("phase", ["admitted", "issued", "recorded", "published"])
def test_project_deletion_holds_before_any_prefix_erase(pending, phase):
    c = pending
    with c.factory() as db, db.begin():
        row = db.get(BufferedUploadInput, c.identifier)
        if phase != "admitted":
            row.phase = "issued"
            db.flush()
        if phase in {"recorded", "published"}:
            row.phase, row.native_receipt = "recorded", {"synthetic_test": "no native mutation"}
            db.flush()
        if phase == "published":
            row.phase, row.content = "published", None
        db.flush()
        assert deletion._project_uploads_unsettled(db, c.project_id)


@pytest.mark.parametrize("field", ["uri", "byte_size", "missing-version"])
def test_unqualified_write_reply_remains_unknown_without_latest_adoption(pending, field):
    c = pending
    c.native.reply_patch = (
        {"uri": "s3c://foreign/key"}
        if field == "uri"
        else ({"byte_size": 1} if field == "byte_size" else {"provider_headers": {}})
    )
    with pytest.raises(UploadContractError):
        c.run()
    c.claim()
    with pytest.raises(StaleFence, match="unknown"):
        c.run()
    assert len(c.native.creates) == 1 and c.native.reads == []
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "issued" and row.native_receipt == {} and row.content == c.body


def test_lost_write_reply_is_not_reissued_after_redelivery(pending):
    c = pending
    c.native.create_error = RuntimeError("Accepted original response lost")
    with pytest.raises(RuntimeError):
        c.run()
    c.claim()
    with pytest.raises(StaleFence, match="unknown"):
        c.run()
    assert len(c.native.creates) == 1


@pytest.mark.parametrize(
    "mutation",
    [
        "inactive",
        "token",
        "archived",
        "deleting",
        "request",
        "key",
        "type",
        "resource",
        "lease",
        "viewer",
        "expired",
    ],
)
def test_known_original_reply_survives_changed_authority_without_publication(pending, mutation):
    c = pending

    def change():
        with c.factory() as db, db.begin():
            actor = db.get(User, c.actor_id)
            project = db.get(Project, c.project_id)
            command = db.get(WorkflowCommand, c.identifier)
            if mutation == "inactive":
                actor.is_active = False
            elif mutation == "token":
                actor.token_version += 1
            elif mutation == "archived":
                project.status = ProjectStatus.ARCHIVED
            elif mutation == "deleting":
                project.settings = {"deletion_in_progress": True}
            elif mutation == "request":
                command.request_payload = {**command.request_payload, "filename": "changed.csv"}
            elif mutation == "key":
                command.idempotency_key += "-changed"
            elif mutation == "type":
                command.resource_type = "foreign"
            elif mutation == "resource":
                command.resource_id = uuid.uuid4()
            elif mutation in {"viewer", "expired"}:
                member = db.scalar(
                    select(ProjectMembership).where(
                        ProjectMembership.project_id == c.project_id,
                        ProjectMembership.user_id == c.actor_id,
                    )
                )
                if mutation == "viewer":
                    member.role = ProjectRole.VIEWER
                else:
                    member.expires_at = datetime(2000, 1, 1, tzinfo=UTC)
            else:
                db.get(OutboxEntry, c.entry_id).lease_owner = "another-worker"

    c.native.create_hook = change
    with pytest.raises((HTTPException, StaleFence, UploadContractError)):
        c.run()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert (
            row.phase == "recorded"
            and row.native_receipt["object_version"] == c.native.creates[0][1]
        )
        assert row.content == c.body
        assert (
            db.scalar(
                select(func.count())
                .select_from(DatasetVersion)
                .where(DatasetVersion.project_id == c.project_id)
            )
            == 0
        )
    assert c.native.reads == []


def test_read_failure_resumes_original_fact_without_second_create(pending):
    c = pending
    c.native.read_error = OSError("Original read unavailable")
    with pytest.raises(OSError):
        c.run()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "recorded"
    c.native.read_error = None
    c.claim()
    c.run()
    assert len(c.native.creates) == 1


def test_verification_checks_original_bytes_even_when_latest_has_expected_data(pending):
    c = pending
    c.native.read_error = OSError("Pause after fact")
    with pytest.raises(OSError):
        c.run()
    original = c.native.creates[0][1]
    (c.native.root / original).write_bytes(b"x" * len(c.body))
    (c.native.root / "latest").write_bytes(c.body)
    c.native.read_error = None
    c.claim()
    with pytest.raises(UploadContractError):
        c.run()
    assert len(c.native.creates) == 1
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "recorded"


def test_publication_rollback_keeps_native_fact_and_private_bytes(pending, monkeypatch):
    c = pending
    publish = execution._publish

    def fail(*args):
        publish(*args)
        raise RuntimeError("Injected publication transaction failure")

    monkeypatch.setattr(execution, "_publish", fail)
    with pytest.raises(RuntimeError):
        c.run()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "recorded"
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert (
            db.scalar(
                select(func.count()).select_from(Dataset).where(Dataset.project_id == c.project_id)
            )
            == 0
        )
    monkeypatch.setattr(execution, "_publish", publish)
    c.claim()
    c.run()
    assert len(c.native.creates) == 1


def test_revocation_after_verification_prevents_publication(pending):
    c = pending

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).is_active = False

    c.native.read_hook = revoke
    with pytest.raises(HTTPException):
        c.run()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "recorded"
        assert db.get(WorkflowCommand, c.identifier).response_status is None


def test_constructor_failure_can_resume_before_any_write_is_issued(pending):
    c = pending

    def unavailable(_settings):
        assert c.engine.pool.checkedout() == 0
        raise RuntimeError("Credential refresh unavailable")

    with pytest.raises(RuntimeError):
        execution.reconcile_buffered_upload(
            c.factory, c.entry_id, c.worker, store_factory=unavailable, settings=c.settings
        )
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "admitted"
    c.claim()
    c.run()
    assert len(c.native.creates) == 1


def test_backend_change_prevents_sdk_creation(pending):
    c = pending
    changed = replace(c.settings, object_store_bucket="another-bucket")

    def forbidden(_settings):
        pytest.fail("Changed backend must be rejected before SDK construction")

    with pytest.raises(StaleFence, match="backend"):
        execution.reconcile_buffered_upload(
            c.factory, c.entry_id, c.worker, store_factory=forbidden, settings=changed
        )
    assert c.native.creates == []


def test_missing_private_history_blocks_deletion_even_after_row_removal(pending):
    c = pending
    with c.factory() as db, db.begin():
        row = db.get(BufferedUploadInput, c.identifier)
        row.phase, row.content = "retired", None
        db.flush()
        db.delete(row)
        db.flush()
        assert deletion._project_uploads_unsettled(db, c.project_id)


def test_expired_lease_rolls_back_prepared_publication(pending, monkeypatch):
    c = pending
    publish = execution._publish

    def expire(db, *args):
        publish(db, *args)
        db.get(OutboxEntry, c.entry_id).lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

    monkeypatch.setattr(execution, "_publish", expire)
    with pytest.raises(StaleFence, match="lease"):
        c.run()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "recorded"
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert db.get(WorkflowCommand, c.identifier).response_status is None


@pytest.mark.parametrize("existing", [False, True])
def test_independent_native_writes_publish_consecutive_versions(pending, tmp_path, existing):
    c = pending
    if existing:
        with c.factory() as db, db.begin():
            db.add(
                Dataset(
                    project_id=c.project_id,
                    created_by_id=c.actor_id,
                    name=c.payload.dataset_name,
                    description="Keep original description",
                )
            )
    identifier = c.admit(key=c.key + "-second", settings=c.settings)
    second_engine = create_engine(c.engine.url, pool_size=4, max_overflow=0)
    other = SimpleNamespace(
        **{
            **vars(c),
            "engine": second_engine,
            "factory": sessionmaker(second_engine, expire_on_commit=False),
            "identifier": identifier,
            "worker": c.worker + "-second",
        }
    )
    native = Native(other, tmp_path / "other-native")
    barrier = threading.Barrier(2)
    c.native.read_hook = lambda: barrier.wait(timeout=10)
    native.read_hook = lambda: barrier.wait(timeout=10)
    with other.factory() as db, db.begin():
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == identifier))
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, other.worker
        entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        entry.delivery_attempts += 1
        other.entry_id = entry.id
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(c.run)
            second = pool.submit(
                execution.reconcile_buffered_upload,
                other.factory,
                other.entry_id,
                other.worker,
                store_factory=lambda _: native,
                settings=other.settings,
            )
            first.result(timeout=20)
            second.result(timeout=20)
        with c.factory() as db:
            datasets = list(db.scalars(select(Dataset).where(Dataset.project_id == c.project_id)))
            assert len(datasets) == 1 and datasets[0].latest_version_number == 2
            if existing:
                assert datasets[0].description == "Keep original description"
            versions = list(
                db.scalars(
                    select(DatasetVersion.version_number)
                    .where(DatasetVersion.project_id == c.project_id)
                    .order_by(DatasetVersion.version_number)
                )
            )
            assert versions == [1, 2]
        assert len(c.native.creates) == len(native.creates) == 1
    finally:
        second_engine.dispose()


@pytest.mark.parametrize(
    "mode", ["before-write", "after-write", "after-fact", "backend-fact", "backend-publication"]
)
def test_process_and_backend_loss_preserve_custody_without_blind_reissue(pending, tmp_path, mode):
    c = pending
    with (tmp_path / "worker.log").open("w") as output:
        process = subprocess.Popen(
            [
                sys.executable,
                "tests/support/buffered_execution_fault_worker.py",
                str(c.entry_id),
                str(c.identifier),
                str(tmp_path),
                mode,
            ],
            env=environment(c.engine),
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
    try:
        marker = tmp_path / "execution-checkpoint.json"
        deadline = time.monotonic() + 20
        while not marker.exists() and time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / "worker.log").read_text()
            time.sleep(0.02)
        assert marker.exists(), "Continue observing the same worker rather than restarting it"
        witness = json.loads(marker.read_text())
        assert witness["phase"] == mode and witness["pid"] == process.pid
        backend_loss = mode.startswith("backend-")
        assert witness["sql_checkouts"] == int(backend_loss)
        if backend_loss:
            with c.engine.connect() as db, db.begin():
                terminated = db.scalar(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE pid=:pid AND datname=:database AND application_name=:application"
                    ),
                    dict(
                        pid=witness["backend_pid"],
                        database=c.engine.url.database,
                        application=f"phase1_buffered_execution_fault_{process.pid}",
                    ),
                )
                assert terminated is True
            process.stdin.write(b"c")
            process.stdin.flush()
            assert process.wait(timeout=15) == 1, (tmp_path / "worker.log").read_text()
            assert "OperationalError" in (tmp_path / "worker.log").read_text()
        else:
            process.kill()
            assert process.wait(timeout=5) == -signal.SIGKILL
        known = mode in {"after-fact", "backend-publication"}
        with c.factory() as db:
            row = db.get(BufferedUploadInput, c.identifier)
            assert row.phase == ("recorded" if known else "issued")
            assert b"".join(buffered_chunks.load_parts(db, row)) == c.body
            assert deletion._project_uploads_unsettled(db, c.project_id)
        if known:
            c.claim()
            c.run()
            with c.factory() as db:
                assert db.get(BufferedUploadInput, c.identifier).phase == "published"
        else:
            c.claim()
            with pytest.raises(StaleFence, match="unknown"):
                c.run()
        assert c.native.creates == []
        assert (c.native.root / "accepted.json").exists() == (mode != "before-write")
        deadline = time.monotonic() + 5
        with c.factory() as db:
            while time.monotonic() < deadline:
                db.execute(text("SELECT pg_stat_clear_snapshot()"))
                remaining = db.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE :prefix"
                    ),
                    {"prefix": f"phase1_buffered_execution_fault_{process.pid}"},
                )
                if not remaining:
                    break
                time.sleep(0.02)
        assert not remaining
        witness.update(
            child_returncode=process.returncode,
            backend_terminated=backend_loss,
            known_outcome_recovered=known,
            recovery_creates=len(c.native.creates),
            backend_quiescent=True,
        )
        marker.write_text(json.dumps(witness, indent=2) + "\n")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdin.close()
