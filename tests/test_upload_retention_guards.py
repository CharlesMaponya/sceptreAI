"""Stored upload retention must fence every destructive cleanup action."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.services import uploads
from automl_api.storage.contracts import UploadContractError
from sqlalchemy import update
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine

STATUSES = ["initiated", "uploading", "object_completed", "verifying", "quarantined"]


@pytest.fixture
def retention_scope(phase1_engine, committed_tenant, monkeypatch):
    user_id, project_id = committed_tenant
    now = datetime.now(UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(uploads, "datetime", Clock)
    driver = SimpleNamespace(
        driver_name="s3_compatible", abort_upload=MagicMock(), delete=MagicMock()
    )
    monkeypatch.setattr(uploads, "get_object_store", lambda *_: driver)

    def create(status, policy="none", age=1):
        key = f"projects/{project_id}/{uuid.uuid4()}"
        with Session(phase1_engine) as db, db.begin():
            row = DatasetUploadSession(
                project_id=project_id,
                created_by_id=user_id,
                dataset_name="retention",
                original_filename="input.csv",
                byte_size=4,
                part_size=4,
                total_parts=1,
                object_key=key,
                multipart_upload_id=uuid.uuid4().hex,
                resume_key=uuid.uuid4().hex,
                provider_driver=driver.driver_name,
                status=status,
                aborted_at=now if status == "aborted" else None,
                expires_at=now - timedelta(days=age),
                quarantine_delete_after=now - timedelta(days=age),
                completed_object_uri=f"s3c://bucket/{key}",
                legal_hold=policy == "hold",
                retention_until={
                    "future": now + timedelta(days=1),
                    "past": now - timedelta(seconds=1),
                    "equal": now,
                }.get(policy),
            )
            db.add(row)
            db.flush()
            return row.id

    return phase1_engine, now, driver, create


def snapshot(row):
    return {
        column.name: getattr(row, column.name) for column in DatasetUploadSession.__table__.columns
    }


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["hold", "future"])
@pytest.mark.parametrize("operation", ["abort", "cleanup"])
def test_retained_upload_cannot_be_destroyed(retention_scope, status, policy, operation):
    engine, now, driver, create = retention_scope
    identity = create(status, policy)
    with Session(engine) as db, db.begin():
        row = db.get(DatasetUploadSession, identity)
        before = snapshot(row)
        if operation == "abort":
            with pytest.raises(UploadContractError, match="retention|hold"):
                uploads.abort_upload_session(row)
        else:
            assert sum(uploads.cleanup_abandoned_uploads(db, now=now).values()) == 0
        assert snapshot(row) == before
    driver.abort_upload.assert_not_called()
    driver.delete.assert_not_called()
    with Session(engine) as db:
        assert snapshot(db.get(DatasetUploadSession, identity)) == before


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["none", "past", "equal"])
@pytest.mark.parametrize("operation", ["abort", "cleanup"])
def test_eligible_upload_can_be_retired(retention_scope, status, policy, operation):
    engine, now, driver, create = retention_scope
    identity = create(status, policy)
    with Session(engine) as db, db.begin():
        row = db.get(DatasetUploadSession, identity)
        if operation == "abort":
            assert uploads.abort_upload_session(row).status == "aborted"
        else:
            assert sum(uploads.cleanup_abandoned_uploads(db, now=now).values()) == 1
        if operation == "abort" or status in {"initiated", "uploading"}:
            driver.abort_upload.assert_called_once()
            driver.delete.assert_not_called()
            assert row.status == ("aborted" if operation == "abort" else "expired")
        else:
            driver.delete.assert_called_once_with(row.completed_object_uri)
            driver.abort_upload.assert_not_called()
            assert row.status == "failed"


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_retained_row_does_not_consume_cleanup_limit(retention_scope, policy):
    engine, now, driver, create = retention_scope
    retained = create("uploading", policy, age=2)
    eligible = create("object_completed")
    with Session(engine) as db, db.begin():
        assert uploads.cleanup_abandoned_uploads(db, now=now, limit=1) == {
            "expired": 0,
            "completed_deleted": 1,
            "quarantine_deleted": 0,
        }
        assert db.get(DatasetUploadSession, retained).status == "uploading"
        assert db.get(DatasetUploadSession, eligible).status == "failed"
    driver.abort_upload.assert_not_called()
    driver.delete.assert_called_once()


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_already_aborted_retained_upload_is_an_idempotent_noop(retention_scope, policy):
    engine, _, driver, create = retention_scope
    identity = create("aborted", policy)
    with Session(engine) as db, db.begin():
        row = db.get(DatasetUploadSession, identity)
        before = snapshot(row)
        assert uploads.abort_upload_session(row).status == "aborted"
        assert snapshot(row) == before
    driver.abort_upload.assert_not_called()
    driver.delete.assert_not_called()


def test_cleanup_uses_current_locked_row_instead_of_cached_status(retention_scope):
    engine, now, driver, create = retention_scope
    identity = create("uploading")
    with Session(engine, expire_on_commit=False) as db:
        cached = db.get(DatasetUploadSession, identity)
        db.commit()
        with Session(engine) as other, other.begin():
            other.execute(
                update(DatasetUploadSession)
                .where(DatasetUploadSession.id == identity)
                .values(status="object_completed")
            )
        assert cached.status == "uploading"
        assert uploads.cleanup_abandoned_uploads(db, now=now)["completed_deleted"] == 1
        db.commit()
        assert cached.status == "failed"
    driver.abort_upload.assert_not_called()
    driver.delete.assert_called_once()
