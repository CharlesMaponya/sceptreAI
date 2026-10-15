import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import OutboxStatus
from automl_api.models.workflows import OutboxEntry
from automl_api.services import reconciler
from automl_api.services import workflow_state as state
from sqlalchemy import delete, update
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


@pytest.fixture
def delivery(phase1_engine, committed_tenant):  # noqa: F811
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        command, _ = state.begin_command(
            db,
            project_id=project_id,
            actor_id=user_id,
            operation="test.delivery",
            idempotency_key=str(uuid.uuid4()),
            payload={},
        )
        entry = state.enqueue_outbox(
            db,
            command,
            topic="test.external",
            aggregate_type="project",
            aggregate_id=project_id,
            payload={},
        )
        entry_id = entry.id
    return phase1_engine, user_id, project_id, entry_id


@pytest.mark.parametrize(
    "ownership", ["stolen", "expired", "pending", "unleased", "delivered", "deleted", "current"]
)
def test_dispatch_checks_durable_lease_before_side_effect(delivery, ownership):
    engine, _, _, entry_id = delivery
    effect = MagicMock()
    with Session(engine, expire_on_commit=False) as stale:
        entry = state.claim_outbox(stale, worker_id="old", limit=1)[0]
        assert entry.id == entry_id
        stale.commit()
        with Session(engine) as other, other.begin():
            if ownership == "deleted":
                other.execute(delete(OutboxEntry).where(OutboxEntry.id == entry_id))
            elif ownership != "current":
                values = {"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
                if ownership == "pending":
                    values.update(status=OutboxStatus.PENDING)
                if ownership == "unleased":
                    values.update(lease_expires_at=None)
                if ownership == "delivered":
                    values.update(status=OutboxStatus.DELIVERED, lease_owner=None)
                other.execute(
                    update(OutboxEntry).where(OutboxEntry.id == entry_id).values(**values)
                )
                if ownership == "stolen":
                    assert state.claim_outbox(other, worker_id="new", limit=1)[0].id == entry_id
        if ownership in {"stolen", "expired", "pending", "unleased"}:
            with pytest.raises(state.StaleFence):
                reconciler.reconcile_entry(
                    stale, entry, worker_id="old", handlers={"test.external": effect}
                )
            stale.rollback()
        else:
            reconciler.reconcile_entry(
                stale, entry, worker_id="old", handlers={"test.external": effect}
            )
            stale.commit()
        assert effect.call_count == (1 if ownership == "current" else 0)
        if ownership == "current":
            for _ in range(2):
                reconciler.reconcile_entry(
                    stale, entry, worker_id="old", handlers={"test.external": effect}
                )
            assert effect.call_count == 1


def test_handler_lock_prevents_reclaim_during_external_effect(delivery):
    engine, _, _, entry_id = delivery
    with Session(engine) as db:
        entry = state.claim_outbox(db, worker_id="current", limit=1)[0]
        db.commit()

        def effect(_entry):
            with Session(engine) as competitor, competitor.begin():
                assert (
                    state.claim_outbox(
                        competitor, worker_id="other", now=datetime.now(UTC) + timedelta(minutes=2)
                    )
                    == []
                )

        reconciler.reconcile_entry(
            db, entry, worker_id="current", handlers={"test.external": effect}
        )
        db.commit()
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("boundary", ["heartbeat", "publication"])
@pytest.mark.parametrize("takeover", [False, True, "upload"])
def test_upload_cannot_renew_or_publish_after_lease_takeover(
    delivery, monkeypatch, boundary, takeover
):
    engine, user_id, project_id, entry_id = delivery
    with Session(engine) as db, db.begin():
        upload = DatasetUploadSession(
            project_id=project_id,
            created_by_id=user_id,
            dataset_name="test",
            original_filename="test.csv",
            upload_kind="prediction",
            byte_size=4,
            part_size=4,
            total_parts=1,
            object_key="test",
            multipart_upload_id="test",
            resume_key=str(uuid.uuid4()),
            status="object_completed",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            completed_object_uri="embedded://test/object",
            expected_object_digest="a" * 64,
        )
        db.add(upload)
        db.flush()
        upload_id = upload.id
        entry = db.get(OutboxEntry, entry_id)
        entry.topic = "upload.reconcile"
        entry.payload = {"session_id": str(upload_id)}
    store = MagicMock()
    store.exists.return_value = True
    store.size.return_value = 4
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)

    replacement_lease = []

    def inspect(*_args, **kwargs):
        if takeover == "upload":
            with Session(engine) as other, other.begin():
                other.execute(
                    update(DatasetUploadSession)
                    .where(DatasetUploadSession.id == upload_id)
                    .values(lease_owner="new")
                )
                replacement_lease.append(other.get(OutboxEntry, entry_id).lease_expires_at)
        elif takeover:
            with Session(engine) as other, other.begin():
                other.execute(
                    update(OutboxEntry)
                    .where(OutboxEntry.id == entry_id)
                    .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
                )
                replacement = state.claim_outbox(other, worker_id="new", limit=1)[0]
                assert replacement.id == entry_id
                replacement_lease.append(replacement.lease_expires_at)
        if boundary == "heartbeat":
            kwargs["on_progress"](4)
        return SimpleNamespace(
            sha256="a" * 64,
            scanner_status="passed",
            scanner_name="test",
            scanner_version="1",
            signature_version="1",
            evidence={},
        )

    monkeypatch.setattr(reconciler, "inspect_and_scan_stream", inspect)
    with Session(engine, expire_on_commit=False) as db:
        entry = state.claim_outbox(db, worker_id="old", limit=1)[0]
        db.commit()
        if takeover:
            with pytest.raises(state.StaleFence):
                reconciler.reconcile_entry(db, entry, worker_id="old")
            db.rollback()
        else:
            reconciler.reconcile_entry(db, entry, worker_id="old")
            db.commit()
    with Session(engine) as db:
        current = db.get(OutboxEntry, entry_id)
        upload = db.get(DatasetUploadSession, upload_id)
        if takeover:
            assert current.lease_owner == ("old" if takeover == "upload" else "new")
            if takeover == "upload":
                assert upload.lease_owner == "new"
            assert current.lease_expires_at == replacement_lease[0]
            assert upload.status == "verifying"
            assert upload.observed_object_digest is None
        else:
            assert current.status == OutboxStatus.DELIVERED
            assert upload.status == "ready"
            assert upload.observed_object_digest == "a" * 64
