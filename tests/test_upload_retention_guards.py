"""Real SQL retention fences for factory-owned upload retirement."""

import copy
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.iam import User
from automl_api.services import upload_retirement as retirement
from automl_api.services import uploads
from automl_api.storage.contracts import UploadContractError
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase2_upload_services import _settings

STATUSES = ["initiated", "uploading", "object_completed", "verifying", "quarantined"]


@pytest.fixture
def retention_scope(phase1_engine, committed_tenant, monkeypatch):
    user_id, project_id = committed_tenant
    now = datetime.now(UTC)
    factory = sessionmaker(bind=phase1_engine, expire_on_commit=False, autoflush=False)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(retirement, "datetime", Clock)
    driver = SimpleNamespace(
        driver_name="s3_compatible",
        abort_upload=MagicMock(),
        delete=MagicMock(),
        uri_for_key=lambda key: f"s3c://bucket/{key}",
    )

    def store(*_):
        assert phase1_engine.pool.checkedout() == 0
        return driver

    monkeypatch.setattr(retirement, "get_object_store", store)

    def create(status, policy="none", age=1):
        identity = uuid.uuid4()
        key = f"projects/{project_id}/raw/{identity}/input.csv"
        with factory() as db, db.begin():
            db.add(
                DatasetUploadSession(
                    id=identity,
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
                    protocol="multipart",
                    provider_state={
                        "completion_protocol": uploads.UPLOAD_COMPLETION_PROTOCOL,
                        "upload_object_uri": driver.uri_for_key(key),
                    },
                    expected_object_digest=hashlib.sha256(b"data").hexdigest(),
                    digest_algorithm="sha256",
                    digest_scope="byte_stream",
                    status=status,
                    aborted_at=now if status == "aborted" else None,
                    expires_at=now - timedelta(days=age),
                    quarantine_delete_after=now - timedelta(days=age),
                    completed_object_uri=driver.uri_for_key(key)
                    if status not in {"initiated", "uploading", "aborted"}
                    else None,
                    legal_hold=policy == "hold",
                    retention_until={
                        "future": now + timedelta(days=1),
                        "past": now - timedelta(seconds=1),
                        "equal": now,
                    }.get(policy),
                )
            )
        return identity

    with factory() as db:
        token_version = db.get(User, user_id).token_version

    def abort(identity):
        return retirement.abort_upload_session(
            factory,
            user_id,
            token_version,
            project_id,
            identity,
            str(identity),
            settings=_settings(),
        )

    def cleanup(**kwargs):
        return retirement.cleanup_abandoned_uploads(
            factory, settings=_settings(), now=now, **kwargs
        )

    def read(identity):
        with factory() as db:
            return copy.deepcopy(
                {
                    c.name: getattr(db.get(DatasetUploadSession, identity), c.name)
                    for c in DatasetUploadSession.__table__.columns
                }
            )

    return SimpleNamespace(
        engine=phase1_engine,
        now=now,
        driver=driver,
        create=create,
        abort=abort,
        cleanup=cleanup,
        read=read,
        factory=factory,
    )


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["hold", "future"])
@pytest.mark.parametrize("operation", ["abort", "cleanup"])
def test_retained_upload_cannot_be_destroyed(retention_scope, status, policy, operation):
    c = retention_scope
    identity = c.create(status, policy)
    before = c.read(identity)
    if operation == "abort":
        with pytest.raises(UploadContractError, match="retention|hold"):
            c.abort(identity)
    else:
        assert not any(c.cleanup().values())
    assert c.read(identity) == before
    c.driver.abort_upload.assert_not_called()
    c.driver.delete.assert_not_called()


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["none", "past", "equal"])
@pytest.mark.parametrize("operation", ["abort", "cleanup"])
def test_eligible_upload_can_be_retired(retention_scope, status, policy, operation):
    c = retention_scope
    identity = c.create(status, policy)
    before = c.read(identity)
    if status not in {"initiated", "uploading"}:
        if operation == "abort":
            with pytest.raises(UploadContractError, match="history is missing"):
                c.abort(identity)
        else:
            assert not any(c.cleanup().values())
        assert c.read(identity) == before
        c.driver.abort_upload.assert_not_called()
        c.driver.delete.assert_not_called()
        return
    if operation == "abort":
        assert c.abort(identity).status == "aborted"
    else:
        assert c.cleanup() == dict(expired=1, completed_deleted=0, quarantine_deleted=0)
    c.driver.abort_upload.assert_called_once()
    c.driver.delete.assert_not_called()
    assert c.read(identity)["status"] == ("aborted" if operation == "abort" else "expired")


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_retained_row_does_not_consume_cleanup_limit(retention_scope, policy):
    c = retention_scope
    retained, eligible = c.create("uploading", policy, age=2), c.create("uploading")
    assert c.cleanup(limit=1) == dict(expired=1, completed_deleted=0, quarantine_deleted=0)
    assert c.read(retained)["status"] == "uploading"
    assert c.read(eligible)["status"] == "expired"
    c.driver.abort_upload.assert_called_once()


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_already_aborted_retained_upload_is_an_idempotent_noop(retention_scope, policy):
    c = retention_scope
    identity = c.create("aborted", policy)
    before = c.read(identity)
    assert c.abort(identity).status == "aborted"
    assert c.read(identity) == before
    c.driver.abort_upload.assert_not_called()
    c.driver.delete.assert_not_called()


def test_cleanup_uses_current_locked_row_instead_of_cached_status(retention_scope):
    c = retention_scope
    identity = c.create("uploading")
    with c.factory() as db:
        cached = db.get(DatasetUploadSession, identity)
        db.commit()
        with c.factory() as other, other.begin():
            other.execute(
                update(DatasetUploadSession)
                .where(DatasetUploadSession.id == identity)
                .values(status="object_completed")
            )
        assert cached.status == "uploading"
        assert not any(c.cleanup().values())
        db.refresh(cached)
        assert cached.status == "object_completed"
    c.driver.abort_upload.assert_not_called()
