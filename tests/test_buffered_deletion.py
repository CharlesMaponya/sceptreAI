"""Project deletion owns exact buffered custody until physical cleanup qualifies."""

import copy
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowCommand,
)
from automl_api.services import (
    buffered_chunks,
    buffered_deletion,
    buffered_execution,
    deletion,
    deletion_workflow,
)
from automl_api.services.workflow_state import StaleFence
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_buffered_execution import pending as pending
from test_buffered_input_admission import admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def case(pending, monkeypatch):
    c = pending
    c.deleted, c.callback = [], None
    monkeypatch.setattr(buffered_deletion, "get_settings", lambda: c.settings)
    monkeypatch.setattr(deletion_workflow, "get_settings", lambda: c.settings)

    def absent(prefix):
        c.native._free()
        return not any(
            (c.native.root / version).exists()
            for key, version, _ in c.native.creates
            if key.startswith(prefix + "/")
        )

    def erase(prefix):
        c.native._free()
        c.deleted.append(prefix)
        for key, version, _ in c.native.creates:
            if key.startswith(prefix + "/"):
                (c.native.root / version).unlink(missing_ok=True)
        if c.callback:
            c.callback()

    c.native.prefix_absent, c.native.delete_prefix = absent, erase
    monkeypatch.setattr(deletion, "get_object_store", lambda *_: c.native)

    def prepare(phase):
        if phase == "recorded":
            c.native.read_error = OSError("Retain the known original write")
            with pytest.raises(OSError):
                c.run()
        elif phase == "published":
            c.run()
        elif phase == "issued":
            c.native.create_error = OSError("Original native reply lost")
            with pytest.raises(OSError):
                c.run()

    def request():
        with c.factory() as db, db.begin():
            response = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
            c.tombstone_id = uuid.UUID(response["id"])
            c.deletion_id = db.scalar(
                select(OutboxEntry.id).where(
                    OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
                )
            )
        return response

    def step():
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.deletion_id)
            entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "owned-buffered-deletion"
            entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            entry.delivery_attempts += 1
        deletion_workflow.reconcile_deletion(c.factory, c.deletion_id, "owned-buffered-deletion")

    c.prepare, c.request, c.step = prepare, request, step
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


@pytest.mark.parametrize("phase", ["admitted", "recorded", "published"])
def test_original_known_custody_closes_through_all_deletion_stages(case, phase):
    c = case
    c.prepare(phase)
    creates = len(c.native.creates)
    foreign = c.native.root / "foreign-neighbor"
    foreign.write_bytes(b"Preserve another key")
    c.request()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == phase
        assert (
            row.content is None
            if phase == "published"
            else b"".join(buffered_chunks.load_parts(db, row)) == c.body
        )
        command, entry = db.get(WorkflowCommand, c.identifier), db.get(OutboxEntry, c.entry_id)
        assert command.status == (
            CommandStatus.SUCCEEDED if phase == "published" else CommandStatus.CANCELLED
        )
        assert entry.status == (
            OutboxStatus.DELIVERED if phase == "published" else OutboxStatus.DEAD
        )
        assert entry.lease_owner is entry.lease_expires_at is None
        manifest = db.get(OutboxEntry, c.deletion_id).payload["buffered_inputs"]
        assert len(manifest) == 1 and manifest[0]["input_id"] == str(c.identifier)
        assert "PRIVATE_INPUT_CANARY" not in json.dumps(manifest)
        original = copy.deepcopy(row.native_receipt)
    if phase == "published":
        c.run()
    else:
        with pytest.raises(StaleFence):
            c.run()
    assert len(c.native.creates) == creates
    for index in range(4):
        c.step()
        with c.factory() as db:
            if index < 3:
                row = db.get(BufferedUploadInput, c.identifier)
                assert row.phase == phase and row.native_receipt == original
                assert (
                    row.content is None
                    if phase == "published"
                    else b"".join(buffered_chunks.load_parts(db, row)) == c.body
                )
            else:
                assert db.get(Project, c.project_id) is None
                assert db.get(BufferedUploadInput, c.identifier) is None
                assert db.get(DeletionTombstone, c.tombstone_id).status == "complete"
                stages = list(
                    db.scalars(
                        select(DeletionStage).where(DeletionStage.tombstone_id == c.tombstone_id)
                    )
                )
                assert len(stages) == 4 and all(stage.status == "completed" for stage in stages)
                for stage in stages:
                    assert (
                        stage.custody["intent"]["source"]["payload"]["buffered_inputs"] == manifest
                    )
    assert len(c.native.creates) == creates and foreign.read_bytes() == b"Preserve another key"
    assert set(c.native.checkouts) == {0}


def test_unknown_write_blocks_owner_deletion_without_cancelling_original_delivery(case):
    c = case
    c.prepare("issued")
    with c.factory() as db:
        before = db.get(OutboxEntry, c.entry_id).status
    with pytest.raises(HTTPException) as held:
        c.request()
    assert held.value.status_code == 409
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "issued"
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert db.get(WorkflowCommand, c.identifier).status == CommandStatus.RUNNING
        assert db.get(OutboxEntry, c.entry_id).status == before
        assert not db.get(Project, c.project_id).settings.get("deletion_in_progress")
    assert c.deleted == [] and len(c.native.creates) == 1


@pytest.mark.parametrize("phase", ["admitted", "recorded", "published"])
def test_request_rollback_preserves_input_and_original_delivery(case, phase, monkeypatch):
    c = case
    c.prepare(phase)
    with c.factory() as db:
        before = db.get(OutboxEntry, c.entry_id).status

    def fail(*args, **kwargs):
        raise RuntimeError("Fail after transfer before deletion delivery commit")

    monkeypatch.setattr(deletion, "enqueue_outbox", fail)
    with pytest.raises(RuntimeError):
        c.request()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == phase and row.content == (None if phase == "published" else c.body)
        assert db.get(OutboxEntry, c.entry_id).status == before
        assert db.get(WorkflowCommand, c.identifier).status != CommandStatus.CANCELLED
        assert not db.get(Project, c.project_id).settings.get("deletion_in_progress")
    assert c.deleted == []


@pytest.mark.parametrize(
    "change",
    [
        "viewer",
        "hold",
        "retention",
        "backend",
        "missing-input",
        "missing-both",
        "missing-delivery",
        "command",
    ],
)
def test_unqualified_project_request_has_no_transfer_or_native_effect(case, change):
    c = case
    c.prepare("published")
    with c.factory() as db, db.begin():
        if change == "viewer":
            member = db.scalar(
                select(ProjectMembership).where(
                    ProjectMembership.project_id == c.project_id,
                    ProjectMembership.user_id == c.actor_id,
                )
            )
            member.role = ProjectRole.VIEWER
        elif change in {"hold", "retention"}:
            version = db.scalar(
                select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
            )
            if change == "hold":
                version.legal_hold = True
            else:
                version.retention_until = datetime.now(UTC) + timedelta(days=1)
        elif change == "backend":
            from dataclasses import replace

            c.settings = replace(c.settings, object_store_bucket="another-bucket")
        elif change in {"missing-input", "missing-both"}:
            row = db.get(BufferedUploadInput, c.identifier)
            row.phase = "retired"
            db.flush()
            db.delete(row)
            if change == "missing-both":
                db.delete(db.get(OutboxEntry, c.entry_id))
        elif change == "missing-delivery":
            db.delete(db.get(OutboxEntry, c.entry_id))
        else:
            db.get(WorkflowCommand, c.identifier).idempotency_key += "changed"
    with pytest.raises(HTTPException) as held:
        c.request()
    assert held.value.status_code in {403, 409}
    assert c.deleted == []


@pytest.mark.parametrize(
    "change",
    ["inventory", "receipt", "command", "delivery", "version", "hold", "tombstone", "backend"],
)
def test_changed_handoff_stops_before_native_prefix_erasure(case, change):
    c = case
    c.prepare("published")
    c.request()
    c.step()
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.deletion_id)
        if change in {"inventory", "receipt"}:
            payload = copy.deepcopy(entry.payload)
            if change == "inventory":
                payload["buffered_inputs"] = []
            else:
                payload["buffered_inputs"][0]["native_receipt"]["object_version"] = "replacement"
            entry.payload = payload
        elif change == "command":
            db.get(WorkflowCommand, c.identifier).idempotency_key += "changed"
        elif change == "delivery":
            db.get(OutboxEntry, c.entry_id).status = OutboxStatus.DEAD
        elif change in {"version", "hold"}:
            version = db.scalar(
                select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
            )
            if change == "version":
                version.object_uri += "changed"
            else:
                version.legal_hold = True
        elif change == "tombstone":
            db.get(DeletionTombstone, c.tombstone_id).legal_hold = True
        else:
            from dataclasses import replace

            c.settings = replace(c.settings, object_store_bucket="another-bucket")
    c.step()
    assert c.deleted == []
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "published"
        assert db.get(Project, c.project_id) is not None


def test_policy_change_after_native_absence_retains_fact_then_replays_without_erase(case):
    c = case
    c.prepare("published")
    c.request()
    c.step()

    def protect():
        with c.factory() as db, db.begin():
            version = db.scalar(
                select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
            )
            version.legal_hold = True

    c.callback = protect
    c.step()
    assert c.deleted == [f"projects/{c.project_id}"]
    with c.factory() as db:
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.tombstone_id == c.tombstone_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        assert stage.status != "completed" and stage.custody["attempts"][0]["outcome"]["absent"]
        assert db.get(BufferedUploadInput, c.identifier).phase == "published"
    c.callback = None
    with c.factory() as db, db.begin():
        db.scalar(
            select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
        ).legal_hold = False
    c.step()
    assert c.deleted == [f"projects/{c.project_id}"]
    c.step()
    c.step()
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


@pytest.mark.parametrize("phase", ["admitted", "recorded", "published"])
def test_metadata_rollback_preserves_transferred_custody_and_retires_without_native_retry(
    case, phase, monkeypatch
):
    c = case
    c.prepare(phase)
    c.request()
    for _ in range(3):
        c.step()
    erased = list(c.deleted)
    actual = deletion._delete_metadata

    def fail(*args):
        actual(*args)
        raise RuntimeError("Fail after private retirement and metadata delete")

    monkeypatch.setattr(deletion, "_delete_metadata", fail)
    with pytest.raises(RuntimeError):
        c.step()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == phase and row.content == (None if phase == "published" else c.body)
        assert db.get(Project, c.project_id) is not None
    monkeypatch.setattr(deletion, "_delete_metadata", actual)
    c.step()
    assert c.deleted == erased
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_legal_hold_preserves_private_bytes_after_physical_stages(case, phase):
    c = case
    c.prepare(phase)
    c.request()
    for _ in range(3):
        c.step()
    with c.factory() as db, db.begin():
        db.get(DeletionTombstone, c.tombstone_id).legal_hold = True
    c.step()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == phase and row.content == c.body
        assert db.get(Project, c.project_id) is not None
    with c.factory() as db, db.begin():
        db.get(DeletionTombstone, c.tombstone_id).legal_hold = False
    c.step()
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


@pytest.mark.parametrize("issued", [False, True])
def test_project_delete_and_native_issue_share_original_project_fence(case, issued):
    c = case
    ready, resume = threading.Event(), threading.Event()

    def pause():
        ready.set()
        assert resume.wait(timeout=15)

    def store(_settings):
        if not issued:
            pause()
        return c.native

    if issued:
        c.native.create_hook = pause
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            buffered_execution.reconcile_buffered_upload,
            c.factory,
            c.entry_id,
            c.worker,
            store_factory=store,
            settings=c.settings,
        )
        try:
            assert ready.wait(timeout=15)
            if issued:
                with pytest.raises(HTTPException) as held:
                    c.request()
                assert held.value.status_code == 409
            else:
                c.request()
        finally:
            resume.set()
        if issued:
            future.result(timeout=15)
            c.request()
            assert len(c.native.creates) == 1
        else:
            with pytest.raises(StaleFence):
                future.result(timeout=15)
            assert c.native.creates == []
    for _ in range(4):
        c.step()
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


def test_deletion_owns_complete_mixed_buffered_inventory(case):
    c = case
    c.prepare("published")
    other = c.admit(key=c.key + "-other", settings=c.settings)
    c.request()
    with c.factory() as db:
        manifest = db.get(OutboxEntry, c.deletion_id).payload["buffered_inputs"]
        assert {item["input_id"] for item in manifest} == {str(c.identifier), str(other)}
        assert {item["phase"] for item in manifest} == {"admitted", "published"}
        assert db.get(WorkflowCommand, other).status == CommandStatus.CANCELLED
    for _ in range(4):
        c.step()
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None


def test_other_pending_operation_refuses_without_cancelling_buffered_input(case):
    from automl_api.services.workflow_state import begin_command, enqueue_outbox

    c = case
    with c.factory() as db, db.begin():
        command, _ = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.actor_id,
            operation="other.operation",
            idempotency_key="other",
            payload={},
        )
        enqueue_outbox(
            db,
            command,
            topic="other.operation",
            aggregate_type="project",
            aggregate_id=c.project_id,
            payload={},
        )
    with c.factory() as db, db.begin():
        with pytest.raises(HTTPException) as held:
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        assert held.value.status_code == 409
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.identifier).status == CommandStatus.PENDING
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.CLAIMED
        assert db.get(BufferedUploadInput, c.identifier).content == c.body
        assert not db.get(Project, c.project_id).settings.get("deletion_in_progress")


@pytest.mark.parametrize("driver", ["embedded", "aws_s3", "s3_compatible", "gcs", "azure_blob"])
@pytest.mark.parametrize("foreign", [False, True])
def test_known_receipt_transfer_requires_original_configured_uri_without_sdk(
    admission, monkeypatch, driver, foreign
):
    from dataclasses import replace

    from automl_api.core.config import get_settings
    from automl_api.storage.contracts import ObjectMetadata, UploadContractError

    c = admission
    settings = replace(
        get_settings(),
        object_store_type=driver,
        object_store_endpoint="https://objects.test",
        object_store_access_key="test",
        object_store_secret_key="test",
        azure_storage_account="ownedaccount",
    )
    monkeypatch.setattr(buffered_deletion, "get_settings", lambda: settings)
    identifier = c.admit(settings=settings)
    with c.factory() as db, db.begin():
        row = db.get(BufferedUploadInput, identifier)
        source = row.manifest
        scheme = {
            "embedded": "embedded",
            "aws_s3": "s3",
            "s3_compatible": "s3c",
            "gcs": "gs",
            "azure_blob": "az",
        }[driver]
        authority = settings.object_store_bucket
        if driver == "azure_blob":
            authority = settings.azure_storage_account + "/" + authority
        uri = f"{scheme}://{authority}/{source['object_key']}"
        if foreign:
            uri += "/foreign"
        row.phase = "issued"
        db.get(WorkflowCommand, identifier).status = CommandStatus.RUNNING
        db.flush()
        row.phase = "recorded"
        row.native_receipt = buffered_execution._receipt(
            ObjectMetadata(
                uri=uri,
                byte_size=len(c.body),
                provider_headers={
                    "generation" if driver == "gcs" else "version_id": "embedded-v1:" + "a" * 32
                    if driver == "embedded"
                    else "original-version"
                },
            ),
            source,
            uri,
        )
    with c.factory() as db, db.begin():
        if foreign:
            with pytest.raises((UploadContractError, StaleFence)):
                buffered_deletion.plan(db, c.project_id)
        else:
            plan = buffered_deletion.plan(db, c.project_id)
            assert len(plan) == 1 and plan[0]["native_receipt"]["uri"] == uri
