"""Actual local files participate in the same durable buffered ownership protocol."""

import os
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
from automl_api.services import (
    buffered_deletion,
    buffered_execution,
    buffered_retirement,
    buffered_uploads,
    deletion,
)
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from test_buffered_input_admission import admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def local(admission, tmp_path, monkeypatch):
    c = admission
    c.settings = replace(
        buffered_uploads.get_settings(),
        object_store_type="embedded",
        local_object_store_path=tmp_path,
    )
    c.calls = []
    c.store = EmbeddedObjectStoreDriver(root=tmp_path, bucket=c.settings.object_store_bucket)

    def guard(name, function):
        def call(*args, **kwargs):
            assert c.engine.pool.checkedout() == 0
            c.calls.append(name)
            return function(*args, **kwargs)

        return call

    for name in (
        "uri_for_key",
        "create_bytes",
        "stat",
        "open_stream",
        "delete",
        "delete_prefix",
        "prefix_absent",
    ):
        monkeypatch.setattr(c.store, name, guard(name, getattr(c.store, name)))
    c.storage = guard("constructor", lambda *_: c.store)
    for module in (buffered_execution, buffered_retirement, buffered_deletion):
        monkeypatch.setattr(module, "get_settings", lambda: c.settings)
    monkeypatch.setattr(buffered_retirement, "get_object_store", c.storage)

    def prepare(*, published=False, expired=False):
        if expired:

            class Past(datetime):
                @classmethod
                def now(cls, tz=None):
                    return datetime.now(tz) - timedelta(days=2)

            monkeypatch.setattr(buffered_uploads, "datetime", Past)
        c.identifier = c.admit(settings=c.settings)
        with c.factory() as db, db.begin():
            entry = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == c.identifier))
            entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "local-original"
            entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            entry.delivery_attempts += 1
            c.entry_id = entry.id
        if published:
            c.run()
        else:
            verify = buffered_execution._verify

            def pause(*_):
                raise OSError("Pause before original local verification")

            monkeypatch.setattr(buffered_execution, "_verify", pause)
            try:
                with pytest.raises(OSError, match="Pause"):
                    c.run()
            finally:
                monkeypatch.setattr(buffered_execution, "_verify", verify)
        with c.factory() as db:
            row = db.get(BufferedUploadInput, c.identifier)
            c.receipt = dict(row.native_receipt)
            c.key_path = c.store._path(row.manifest["object_key"])
            c.object_key = row.manifest["object_key"]

    c.run = lambda: buffered_execution.reconcile_buffered_upload(
        c.factory, c.entry_id, "local-original", settings=c.settings, store_factory=c.storage
    )
    c.prepare = prepare
    c.cancel = lambda: buffered_retirement.retire_input(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.identifier,
        "original-local-cancel",
        settings=c.settings,
    )
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


def test_actual_local_factory_publishes_original_incarnation_and_clears_input(local):
    c = local
    c.prepare(published=True)
    assert c.receipt["object_version"].startswith("embedded-v1:")
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "published" and row.content is None
        command = db.get(WorkflowCommand, c.identifier)
        assert command.status == CommandStatus.SUCCEEDED and command.response_status == 201
    before = list(c.calls)
    c.run()
    assert c.calls == before
    assert c.key_path.read_bytes() == c.body
    assert c.calls.count("create_bytes") == 1


@pytest.mark.parametrize("replacement", [False, True])
def test_actual_local_abort_route_preserves_replacement_and_replays(local, replacement):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory

    c = local
    c.prepare()
    if replacement:
        c.store.put_bytes(c.object_key, b"preserve replacement")
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

    def abort():
        return TestClient(app).post(
            f"/projects/{c.project_id}/datasets/buffered/{c.identifier}/abort",
            headers={"Idempotency-Key": "original-local-cancel"},
        )

    response = abort()
    assert response.status_code == 200, response.text
    before = list(c.calls)
    assert abort().json() == response.json() and c.calls == before
    assert c.calls.count("delete") == 1
    assert c.key_path.exists() == replacement
    if replacement:
        assert c.key_path.read_bytes() == b"preserve replacement"
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        proof = buffered_retirement.require_retired(db, row)
        assert proof["target"]["version"] == c.receipt["object_version"]
        assert buffered_retirement.released_reservations(db, c.project_id) == len(c.body)
    with c.factory() as db, db.begin():
        result = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        assert result["status"] == "pending"
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id,
                OutboxEntry.topic == "project.delete",
            )
        )
        assert entry.payload["buffered_inputs"][0]["retirement"] == proof


def test_local_native_absence_survives_publication_rollback_without_second_delete(
    local, monkeypatch
):
    c = local
    c.prepare()
    publish = buffered_retirement._publish

    def rollback(*args):
        publish(*args)
        raise RuntimeError("Owned publication rollback")

    monkeypatch.setattr(buffered_retirement, "_publish", rollback)
    with pytest.raises(RuntimeError, match="rollback"):
        c.cancel()
    assert not c.key_path.exists()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert buffered_retirement.released_reservations(db, c.project_id) == 0
    monkeypatch.setattr(buffered_retirement, "_publish", publish)
    assert c.cancel()["status"] == "aborted"
    assert c.calls.count("delete") == 1


def test_local_known_input_expires_using_original_incarnation(local):
    c = local
    c.prepare(expired=True)
    assert buffered_retirement.cleanup_inputs(c.factory, settings=c.settings) == 1
    assert not c.key_path.exists() and c.calls.count("delete") == 1
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert buffered_retirement.require_retired(db, row)["result"]["status"] == "expired"


def test_missing_local_identity_retains_bytes_and_quota_until_original_evidence_restored(local):
    c = local
    c.prepare()
    os.removexattr(c.key_path, c.store._identity_attribute)
    with pytest.raises(UploadContractError):
        c.cancel()
    assert c.key_path.read_bytes() == c.body
    with c.factory() as db, db.begin():
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert buffered_retirement.released_reservations(db, c.project_id) == 0
        owner = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == buffered_retirement.PROTOCOL,
            )
        )
        assert not any("outcome" in unit for unit in owner.response_payload["attempts"])
        owner.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    # Test restores the exact captured xattr; production never synthesizes a lost identity.
    os.setxattr(
        c.key_path, c.store._identity_attribute, c.receipt["object_version"].encode("ascii")
    )
    with c.key_path.open("rb") as source:
        os.fsync(source.fileno())
    assert c.cancel()["status"] == "aborted" and not c.key_path.exists()


@pytest.mark.parametrize("version", [None, "not-an-original-local-incarnation"])
def test_unqualified_original_local_reply_cannot_publish_or_drop_private_input(
    local, monkeypatch, version
):
    c = local
    create = c.store.create_bytes

    def unqualified(*args):
        metadata = create(*args)
        return replace(
            metadata, provider_headers={} if version is None else {"version_id": version}
        )

    monkeypatch.setattr(c.store, "create_bytes", unqualified)
    with pytest.raises(UploadContractError):
        c.prepare()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "issued" and row.content == c.body and row.native_receipt == {}
        assert c.store._path(row.manifest["object_key"]).read_bytes() == c.body
        assert db.get(WorkflowCommand, c.identifier).response_status is None
    assert c.calls.count("create_bytes") == 1


def test_legacy_unversioned_local_verification_cannot_adopt_latest(local, monkeypatch):
    c = local
    receipt = buffered_execution._receipt

    def legacy(*args):
        return {**receipt(*args), "object_version": None}

    monkeypatch.setattr(buffered_execution, "_receipt", legacy)
    c.prepare()
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "local-original"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        entry.delivery_attempts += 1
    before = list(c.calls)
    with pytest.raises(UploadContractError, match="controlled recovery"):
        c.run()
    assert c.calls[len(before) :] == ["constructor", "uri_for_key"]
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "recorded" and row.content == c.body
