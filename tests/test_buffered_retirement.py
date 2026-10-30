"""Original private-input retirement uses committed SQL and version-pinned native work."""

import copy
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import CommandStatus, OutboxStatus
from automl_api.models.iam import User
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowCommand,
)
from automl_api.services import buffered_chunks, buffered_deletion, buffered_uploads, deletion
from automl_api.services import buffered_retirement as retirement
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import UploadContractError
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_buffered_execution import pending as pending
from test_buffered_input_admission import admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def case(pending, monkeypatch):
    c = pending
    c.erases, c.callback = [], None
    c.cancel_key = "cancel-" + uuid.uuid4().hex
    monkeypatch.setattr(retirement, "get_settings", lambda: c.settings)
    monkeypatch.setattr(buffered_deletion, "get_settings", lambda: c.settings)

    def erase(uri, *, version=None):
        c.native._free()
        assert version is not None
        path = c.native.root / version
        if not path.exists():
            return
        c.erases.append((uri, version))
        path.unlink()
        if c.callback:
            c.callback()

    c.native.delete = erase

    def store(*_):
        c.native._free()
        return c.native

    monkeypatch.setattr(retirement, "get_object_store", store)

    def prepare(phase):
        if phase == "recorded":
            c.native.read_error = OSError("Keep original known creation unpublished")
        elif phase == "issued":
            c.native.create_error = OSError("Original response lost")
        if phase in {"recorded", "issued"}:
            with pytest.raises(OSError):
                c.run()
        elif phase == "published":
            c.run()

    c.prepare = prepare
    c.cancel = lambda **kw: retirement.retire_input(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.identifier,
        kw.pop("key", c.cancel_key),
        settings=c.settings,
        **kw,
    )
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_original_unpublished_input_retires_and_replays_without_another_delete(case, phase):
    c = case
    c.prepare(phase)
    original_version = c.native.creates[0][1] if c.native.creates else None
    neighbor = c.native.root / "replacement-version"
    neighbor.write_bytes(b"Preserve another version")
    result = c.cancel()
    assert result["status"] == "aborted" and result["id"] == str(c.identifier)
    assert c.cancel() == result
    assert len(c.erases) == int(phase == "recorded")
    if original_version:
        assert (
            c.erases[0][1] == original_version and not (c.native.root / original_version).exists()
        )
    assert neighbor.read_bytes() == b"Preserve another version"
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "retired" and row.content is None
        assert db.get(WorkflowCommand, c.identifier).status == CommandStatus.CANCELLED
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DEAD
        proof = retirement.require_retired(db, row)
        assert proof["result"] == result
        assert retirement.released_reservations(db, c.project_id) == len(c.body)
    c.settings = replace(c.settings, project_storage_quota_bytes=len(c.body))
    c.admit(key=c.key + "-replacement", settings=c.settings)


@pytest.mark.parametrize("phase", ["issued", "published"])
def test_unknown_or_published_input_cannot_be_cancelled(case, phase):
    c = case
    c.prepare(phase)
    with pytest.raises(UploadContractError):
        c.cancel()
    assert c.erases == []
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == phase


@pytest.mark.parametrize("change", ["inactive", "token", "source", "lease", "backend"])
def test_known_native_absence_survives_lost_publication_authority(case, change):
    c = case
    c.prepare("recorded")
    with c.factory() as db:
        old_key = db.get(WorkflowCommand, c.identifier).idempotency_key

    def mutate():
        with c.factory() as db, db.begin():
            owner = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == retirement.PROTOCOL,
                )
            )
            if change == "inactive":
                db.get(User, c.actor_id).is_active = False
            elif change == "token":
                db.get(User, c.actor_id).token_version += 1
            elif change == "source":
                db.get(WorkflowCommand, c.identifier).idempotency_key += "-changed"
            elif change == "lease":
                owner.lease_owner = "another-original-attempt"
            else:
                c.settings = replace(c.settings, object_store_bucket="another-bucket")

    c.callback = mutate
    with pytest.raises((HTTPException, StaleFence, UploadContractError)):
        c.cancel()
    with c.factory() as db:
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.response_payload["attempts"][0]["outcome"]["absent"] is True
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert retirement.released_reservations(db, c.project_id) == 0
    c.callback = None
    with c.factory() as db, db.begin():
        actor = db.get(User, c.actor_id)
        actor.is_active = True
        if change == "token":
            c.token_version = actor.token_version
        else:
            actor.token_version = c.token_version
        db.get(WorkflowCommand, c.identifier).idempotency_key = old_key
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        owner.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    c.settings = replace(
        c.settings, object_store_bucket=buffered_uploads.get_settings().object_store_bucket
    )
    assert c.cancel()["status"] == "aborted"
    assert len(c.erases) == 1
    if change == "token":
        with c.factory() as db:
            proof = retirement.require_retired(db, db.get(BufferedUploadInput, c.identifier))
            assert proof["attempts"][0]["authorization"]["token_version"] == c.token_version - 1
            assert proof["authorization"]["token_version"] == c.token_version


def test_terminal_replay_uses_current_credentials_without_repeating_native_work(case):
    c = case
    c.prepare("recorded")
    result = c.cancel()
    with c.factory() as db, db.begin():
        actor = db.get(User, c.actor_id)
        actor.token_version += 1
        current_token = actor.token_version
    with pytest.raises(HTTPException) as rejected:
        c.cancel()
    assert rejected.value.status_code == 401
    c.token_version = current_token
    assert c.cancel() == result
    assert len(c.erases) == 1


@pytest.mark.parametrize("field", ["owner", "marker", "outcome", "result", "delivery"])
def test_corrupt_retirement_proof_cannot_release_quota_or_project_custody(case, field):
    c = case
    c.prepare("recorded")
    c.cancel()
    with c.factory() as db, db.begin():
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        if field == "owner":
            db.delete(owner)
        elif field == "marker":
            db.get(WorkflowCommand, c.identifier).terminal_reason = "missing original owner"
        elif field in {"outcome", "result"}:
            value = copy.deepcopy(owner.response_payload)
            if field == "outcome":
                value["attempts"][0]["outcome"]["absent"] = False
            else:
                value["published"]["result"]["id"] = str(uuid.uuid4())
            owner.response_payload = value
        else:
            db.delete(db.get(OutboxEntry, c.entry_id))
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        with pytest.raises((UploadContractError, StaleFence)):
            retirement.require_retired(db, row)
        assert retirement.released_reservations(db, c.project_id) == 0
    c.settings = replace(c.settings, project_storage_quota_bytes=len(c.body))
    with pytest.raises(ValueError, match="quota"):
        c.admit(key=c.key + "-blocked", settings=c.settings)
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409


def test_project_deletion_keeps_authenticated_standalone_retirement(case):
    c = case
    c.prepare("recorded")
    c.cancel()
    with c.factory() as db, db.begin():
        result = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        assert result["status"] == "pending"
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
            )
        )
        snapshot = entry.payload["buffered_inputs"][0]
        assert snapshot["phase"] == "retired" and snapshot["retirement"]["native_outcome"]["absent"]


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_public_abort_route_releases_auth_sql_before_native_work(case, phase):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    c = case
    c.prepare(phase)
    app = FastAPI()
    app.include_router(router)

    def database():
        with c.factory() as db:
            yield db

    def actor():
        with c.factory() as db:
            return db.get(User, c.actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = actor
    app.dependency_overrides[get_session_factory] = lambda: c.factory

    def post(key):
        return TestClient(app).post(
            f"/projects/{c.project_id}/datasets/buffered/{c.identifier}/abort",
            headers={"Idempotency-Key": key},
        )

    first = post(c.cancel_key)
    assert first.status_code == 200, first.text
    assert post(c.cancel_key).json() == first.json()
    assert post(c.cancel_key + "-other").status_code == 409
    assert len(c.erases) == int(phase == "recorded")
    assert set(c.native.checkouts) == ({0} if phase == "recorded" else set())


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_publication_rollback_retains_native_absence_and_original_bytes(case, phase, monkeypatch):
    c = case
    c.prepare(phase)
    actual = retirement._publish

    def fail(*args):
        actual(*args)
        raise RuntimeError("Fail after logical retirement before SQL commit")

    monkeypatch.setattr(retirement, "_publish", fail)
    with pytest.raises(RuntimeError):
        c.cancel()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == phase
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.response_payload["attempts"][0]["outcome"]["absent"]
        assert retirement.released_reservations(db, c.project_id) == 0
    monkeypatch.setattr(retirement, "_publish", actual)
    assert c.cancel()["status"] == "aborted"
    assert len(c.erases) == int(phase == "recorded")


def test_changed_retirement_request_retains_original_native_fact(case):
    c = case
    c.prepare("recorded")
    captured = {}

    def mutate():
        with c.factory() as db, db.begin():
            owner = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == retirement.PROTOCOL,
                )
            )
            captured.update(
                payload=copy.deepcopy(owner.request_payload), request_hash=owner.request_hash
            )
            owner.request_payload = {**owner.request_payload, "purpose": "changed-purpose"}

    c.callback = mutate
    with pytest.raises(StaleFence):
        c.cancel()
    with c.factory() as db, db.begin():
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.response_payload["attempts"][0]["outcome"]["absent"]
        owner.request_payload, owner.request_hash = captured["payload"], captured["request_hash"]
    c.callback = None
    assert c.cancel()["status"] == "aborted" and len(c.erases) == 1


def test_native_lease_blocks_competing_delete_then_original_version_recovers(case, monkeypatch):
    c = case
    c.prepare("recorded")
    actual = retirement._fact
    monkeypatch.setattr(
        retirement, "_fact", lambda *_args: (_ for _ in ()).throw(RuntimeError("Crash before fact"))
    )
    with pytest.raises(RuntimeError):
        c.cancel()
    with pytest.raises(HTTPException) as held:
        c.cancel()
    assert held.value.status_code == 409 and len(c.erases) == 1
    with c.factory() as db, db.begin():
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        owner.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    monkeypatch.setattr(retirement, "_fact", actual)
    assert c.cancel()["status"] == "aborted"
    assert {version for _, version in c.erases} == {c.native.creates[0][1]}


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_expiry_uses_original_immutable_admission_time_and_current_policy(
    admission, monkeypatch, phase, tmp_path
):
    from automl_api.services import buffered_execution
    from test_buffered_execution import Native

    c = admission

    class PastAdmission(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(days=2)

    monkeypatch.setattr(buffered_uploads, "datetime", PastAdmission)
    c.settings = replace(
        buffered_uploads.get_settings(),
        object_store_type="s3_compatible",
        object_store_endpoint="https://objects.test",
        object_store_access_key="test",
        object_store_secret_key="test",
    )
    monkeypatch.setattr(retirement, "get_settings", lambda: c.settings)
    c.identifier = c.admit(settings=c.settings)
    native = Native(c, tmp_path / "expiry-native")
    if phase == "recorded":
        with c.factory() as db, db.begin():
            entry = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == c.identifier))
            entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "expiry-test"
            entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            entry.delivery_attempts += 1
            entry_id = entry.id
        native.read_error = OSError("Leave original known fact")
        with pytest.raises(OSError):
            buffered_execution.reconcile_buffered_upload(
                c.factory,
                entry_id,
                "expiry-test",
                store_factory=lambda _: native,
                settings=c.settings,
            )
    erases = []

    def erase(uri, *, version=None):
        native._free()
        erases.append(version)
        (native.root / version).unlink(missing_ok=True)

    native.delete = erase
    monkeypatch.setattr(retirement, "get_object_store", lambda *_: native)
    assert retirement.cleanup_inputs(c.factory, settings=c.settings, limit=1) == 1
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "retired" and row.content is None
        assert retirement.require_retired(db, row)["result"]["status"] == "expired"
    assert len(erases) == int(phase == "recorded")
    assert retirement.cleanup_inputs(c.factory, settings=c.settings, limit=1) == 0


def test_cleanup_does_not_steal_abort_owner_and_rotates_to_next_input(admission, monkeypatch):
    c = admission

    class PastAdmission(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(days=2)

    monkeypatch.setattr(buffered_uploads, "datetime", PastAdmission)
    settings = buffered_uploads.get_settings()
    monkeypatch.setattr(retirement, "get_settings", lambda: settings)
    first = c.admit(settings=settings)
    actual = retirement._fact
    monkeypatch.setattr(
        retirement,
        "_fact",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("Pause original abort")),
    )
    with pytest.raises(RuntimeError):
        retirement.retire_input(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            first,
            "original-abort",
            settings=settings,
        )
    monkeypatch.setattr(retirement, "_fact", actual)
    second = c.admit(key=c.key + "-second", settings=settings)
    assert retirement.cleanup_inputs(c.factory, settings=settings, limit=1) == 0
    assert retirement.cleanup_inputs(c.factory, settings=settings, limit=1) == 1
    with c.factory() as db:
        assert db.get(BufferedUploadInput, first).phase == "admitted"
        assert db.get(BufferedUploadInput, second).phase == "retired"


def test_fresh_input_does_not_expire_and_foreign_scope_cannot_abort(case):
    c = case
    assert retirement.cleanup_inputs(c.factory, settings=c.settings) == 0
    with pytest.raises(HTTPException):
        retirement.retire_input(
            c.factory,
            c.actor_id,
            c.token_version,
            uuid.uuid4(),
            c.identifier,
            c.cancel_key,
            settings=c.settings,
        )
    assert c.erases == []


@pytest.mark.parametrize(
    "mode", ["before-delete", "after-delete", "after-fact", "backend-fact", "backend-publication"]
)
def test_process_and_backend_loss_keep_original_retirement_custody(case, tmp_path, mode):
    import json
    import signal
    import subprocess
    import sys
    import time

    from fault_worker_environment import environment
    from sqlalchemy import text

    c = case
    c.prepare("recorded")
    with (tmp_path / "retirement-worker.log").open("w") as output:
        process = subprocess.Popen(
            [
                sys.executable,
                "tests/support/buffered_retirement_fault_worker.py",
                str(c.identifier),
                c.cancel_key,
                str(tmp_path),
                mode,
            ],
            env=environment(c.engine),
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=output,
        )
    try:
        marker = tmp_path / "retirement-checkpoint.json"
        deadline = time.monotonic() + 30
        while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists(), (tmp_path / "retirement-worker.log").read_text()
        witness = json.loads(marker.read_text())
        assert witness["pid"] == process.pid and witness["input_id"] == str(c.identifier)
        database = mode.startswith("backend-")
        assert witness["sql_checkouts"] == int(database)
        if database:
            with c.engine.connect() as db, db.begin():
                stopped = db.scalar(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE pid=:pid AND datname=:database AND application_name=:application"
                    ),
                    dict(
                        pid=witness["backend_pid"],
                        database=c.engine.url.database,
                        application=f"phase1_buffered_retire_fault_{process.pid}",
                    ),
                )
                assert stopped is True
            process.stdin.write(b"c")
            process.stdin.flush()
            assert process.wait(timeout=15) == 1
            assert "OperationalError" in (tmp_path / "retirement-worker.log").read_text()
        else:
            process.kill()
            assert process.wait(timeout=5) == -signal.SIGKILL
        known = mode in {"after-fact", "backend-publication"}
        with c.factory() as db, db.begin():
            row = db.get(BufferedUploadInput, c.identifier)
            assert (
                row.phase == "recorded" and b"".join(buffered_chunks.load_parts(db, row)) == c.body
            )
            owner = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == retirement.PROTOCOL,
                )
            )
            assert (
                any(u.get("outcome", {}).get("absent") for u in owner.response_payload["attempts"])
                == known
            )
            if not known:
                owner.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        assert c.cancel()["status"] == "aborted"
        assert len(c.native.creates) == 1
        child_marker = tmp_path / "retirement-accepted.json"
        child_deletes = (
            json.loads(child_marker.read_text())["deletes"] if child_marker.exists() else 0
        )
        assert child_deletes + len(c.erases) == 1
        with c.factory() as db:
            assert retirement.require_retired(db, db.get(BufferedUploadInput, c.identifier))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                db.execute(text("SELECT pg_stat_clear_snapshot()"))
                remains = db.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE application_name=:application"
                    ),
                    dict(application=f"phase1_buffered_retire_fault_{process.pid}"),
                )
                if not remains:
                    break
                time.sleep(0.02)
            assert not remains
        witness.update(
            child_returncode=process.returncode,
            backend_terminated=database,
            known_outcome=known,
            child_deletes=child_deletes,
            recovery_deletes=len(c.erases),
            backend_quiescent=True,
        )
        marker.write_text(json.dumps(witness, indent=2) + "\n")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdin.close()


def test_current_provider_policy_change_keeps_native_fact_until_restored(case, monkeypatch):
    from automl_api.services import provider_import

    c = case
    c.prepare("recorded")
    protected = []

    def policy(_db, _uri):
        if protected:
            raise ValueError("Current provider protection prevents cleanup")

    monkeypatch.setattr(provider_import, "assert_provider_uri_cleanup_safe", policy)
    c.callback = lambda: protected.append(True)
    with pytest.raises(ValueError, match="provider protection"):
        c.cancel()
    with c.factory() as db:
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.response_payload["attempts"][0]["outcome"]["absent"]
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
    protected.clear()
    c.callback = None
    assert c.cancel()["status"] == "aborted" and len(c.erases) == 1


def test_any_dataset_reference_blocks_standalone_native_retirement(case):
    from automl_api.models.datasets import Dataset, DatasetVersion
    from automl_api.models.enums import DatasetFormat, ObjectStoreType

    c = case
    c.prepare("recorded")
    with c.factory() as db, db.begin():
        dataset = Dataset(project_id=c.project_id, created_by_id=c.actor_id, name="Reference")
        db.add(dataset)
        db.flush()
        receipt = db.get(BufferedUploadInput, c.identifier).native_receipt
        db.add(
            DatasetVersion(
                project_id=c.project_id,
                created_by_id=c.actor_id,
                dataset_id=dataset.id,
                version_number=1,
                format=DatasetFormat.CSV,
                object_store_type=ObjectStoreType.S3_COMPATIBLE,
                object_uri=receipt["uri"],
                content_hash="d" * 64,
                byte_size=len(c.body),
            )
        )
    with pytest.raises(UploadContractError, match="Referenced"):
        c.cancel()
    assert c.erases == []
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.identifier).status == CommandStatus.RUNNING


def test_retirement_key_reuse_cannot_cancel_another_original_input(case):
    c = case
    c.cancel()
    other = c.admit(key=c.key + "-other", settings=c.settings)
    with pytest.raises(ValueError):
        retirement.retire_input(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            other,
            c.cancel_key,
            settings=c.settings,
        )
    with c.factory() as db:
        assert db.get(BufferedUploadInput, other).phase == "admitted"
        assert db.get(WorkflowCommand, other).status == CommandStatus.PENDING


def test_constructor_failure_retains_durable_cancellation_without_issued_delete(case, monkeypatch):
    c = case
    c.prepare("recorded")
    actual = retirement.get_object_store

    def fail(*_):
        assert c.engine.pool.checkedout() == 0
        raise OSError("Provider credentials unavailable")

    monkeypatch.setattr(retirement, "get_object_store", fail)
    with pytest.raises(OSError):
        c.cancel()
    with c.factory() as db:
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.response_payload["attempts"] == []
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
    monkeypatch.setattr(retirement, "get_object_store", actual)
    assert c.cancel()["status"] == "aborted" and len(c.erases) == 1


def test_shorter_current_ttl_does_not_shorten_original_admission_promise(admission, monkeypatch):
    c = admission

    class PastAdmission(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(hours=18)

    monkeypatch.setattr(buffered_uploads, "datetime", PastAdmission)
    settings = buffered_uploads.get_settings()
    identifier = c.admit(settings=settings)
    shorter = replace(settings, upload_session_ttl_seconds=60)
    monkeypatch.setattr(retirement, "get_settings", lambda: shorter)
    assert retirement.cleanup_inputs(c.factory, settings=shorter) == 0
    with pytest.raises(UploadContractError, match="not expired"):
        retirement.retire_input(
            c.factory,
            c.actor_id,
            None,
            c.project_id,
            identifier,
            "buffered-expire:" + str(identifier),
            purpose="expire",
            settings=shorter,
        )
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).content == c.body


def test_actual_cleanup_worker_uses_buffered_policy_service(admission, monkeypatch):
    from automl_api import workflow_reconciler

    c = admission

    class PastAdmission(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(days=2)

    monkeypatch.setattr(buffered_uploads, "datetime", PastAdmission)
    settings = buffered_uploads.get_settings()
    monkeypatch.setattr(retirement, "get_settings", lambda: settings)
    identifier = c.admit(settings=settings)
    monkeypatch.setattr(workflow_reconciler, "recover_pending_admissions", lambda *_args, **_kw: 0)
    monkeypatch.setattr(workflow_reconciler, "expire_pending_admissions", lambda *_args, **_kw: 0)
    monkeypatch.setattr(workflow_reconciler, "cleanup_pending_admissions", lambda *_args, **_kw: 0)
    monkeypatch.setattr(workflow_reconciler, "cleanup_abandoned_uploads", lambda *_args, **_kw: {})
    assert workflow_reconciler.cleanup_uploads_once(c.factory) == 1
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).phase == "retired"


def test_unversioned_embedded_known_write_holds_without_deleting_current_file(
    admission, monkeypatch, tmp_path
):
    from automl_api.services import buffered_execution
    from automl_api.storage.embedded import EmbeddedObjectStoreDriver

    c = admission
    settings = replace(
        buffered_uploads.get_settings(),
        object_store_type="embedded",
        local_object_store_path=tmp_path,
    )
    identifier = c.admit(settings=settings)
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket=settings.object_store_bucket)
    receipt = buffered_execution._receipt

    def legacy_receipt(*args):
        return {**receipt(*args), "object_version": None}

    monkeypatch.setattr(buffered_execution, "_receipt", legacy_receipt)
    with c.factory() as db, db.begin():
        delivery = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == identifier))
        delivery.status, delivery.lease_owner = OutboxStatus.CLAIMED, "local-original"
        delivery.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        delivery.delivery_attempts += 1
        entry_id = delivery.id

    def unavailable(*_):
        raise OSError("Pause original local verification")

    monkeypatch.setattr(buffered_execution, "_verify", unavailable)
    with pytest.raises(OSError):
        buffered_execution.reconcile_buffered_upload(
            c.factory, entry_id, "local-original", settings=settings, store_factory=lambda _: store
        )
    monkeypatch.setattr(retirement, "get_settings", lambda: settings)
    monkeypatch.setattr(
        retirement,
        "get_object_store",
        lambda *_: pytest.fail("Do not construct an unqualified deletion client"),
    )
    with pytest.raises(UploadContractError, match="immutable"):
        retirement.retire_input(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            identifier,
            "local-cancel",
            settings=settings,
        )
    with c.factory() as db:
        row = db.get(BufferedUploadInput, identifier)
        assert row.phase == "recorded" and row.content == c.body
        assert store.read_bytes(row.native_receipt["uri"]) == c.body
        assert db.get(WorkflowCommand, identifier).status == CommandStatus.RUNNING


def test_exhausted_native_retry_budget_holds_before_sdk_construction(case, monkeypatch):
    c = case
    c.prepare("recorded")

    def failed_delete(*_args, **_kwargs):
        c.native._free()
        raise OSError("Original conditional delete unavailable")

    c.native.delete = failed_delete
    with pytest.raises(OSError):
        c.cancel()
    with c.factory() as db, db.begin():
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == retirement.PROTOCOL,
            )
        )
        assert owner.retry_count == len(owner.response_payload["attempts"]) == 1
        owner.max_retries = 1
        owner.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    monkeypatch.setattr(
        retirement,
        "get_object_store",
        lambda *_: pytest.fail("No client after exhausted native budget"),
    )
    with pytest.raises(UploadContractError, match="retry budget"):
        c.cancel()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "recorded" and row.content == c.body
        assert retirement.released_reservations(db, c.project_id) == 0


def test_recorded_absence_can_finish_after_native_budget_is_lowered(case):
    c = case
    c.prepare("recorded")

    def lower_budget():
        with c.factory() as db, db.begin():
            owner = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == retirement.PROTOCOL,
                )
            )
            owner.max_retries = 0

    c.callback = lower_budget
    assert c.cancel()["status"] == "aborted" and len(c.erases) == 1
