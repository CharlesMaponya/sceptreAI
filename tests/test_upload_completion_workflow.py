"""Real SQL completion custody, separate from controlled provider responses."""

import copy
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import CompleteUploadRequest
from automl_api.services import upload_admission as admission
from automl_api.services import upload_completion as completion
from automl_api.services import upload_retirement as retirement
from automl_api.services import uploads
from automl_api.storage.contracts import (
    CompletedObject,
    TransferReceipt,
    UploadCapabilities,
    UploadContractError,
    UploadProgress,
)
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase2_upload_services import Driver, _payload, _settings


@pytest.fixture
def completion_case(phase1_engine, committed_tenant, monkeypatch, request):
    actor_id, project_id = committed_tenant
    factory = sessionmaker(bind=phase1_engine, expire_on_commit=False, autoflush=False)
    c = SimpleNamespace(
        engine=phase1_engine,
        factory=factory,
        actor_id=actor_id,
        project_id=project_id,
        calls=[],
        fault=None,
        callback=None,
        constructions=0,
    )
    provider = getattr(request, "param", "s3_compatible")
    quota = 45_000_000_000 if provider == "offline" else 100
    params = getattr(getattr(request.node, "callspec", None), "params", {})
    verification_mode = params.get("verification_case")
    is_verification = "verification_case" in request.fixturenames
    # Admission binds these fields; dependent fixtures select them before native creation.
    upload_kind = (
        "dataset"
        if provider in {"dataset", "held-dataset"}
        or is_verification
        and verification_mode != "offline"
        else "offline_scoring"
    )
    retention_days = 30 if is_verification and verification_mode == "retained" else None
    legal_hold = provider == "held-dataset"
    if provider in {"offline", "dataset", "held-dataset"}:
        provider = "s3_compatible"

    class Native(Driver):
        def capabilities(self):
            if self.driver_name == "gcs":
                return UploadCapabilities("resumable_offset", 1, False, False, True, True)
            return super().capabilities()

        def uri_for_key(self, key):
            return f"gs://bucket/{key}" if self.driver_name == "gcs" else super().uri_for_key(key)

        def query_progress(self, upload):
            c.calls.append(("progress", c.engine.pool.checkedout()))
            assert c.calls[-1][1] == 0
            if c.callback:
                c.callback("progress")
            if self.driver_name == "gcs":
                return UploadProgress(10, 10, (), True)
            return UploadProgress(
                10, 10, (TransferReceipt(1, 0, 6, "part1"), TransferReceipt(2, 6, 4, "part2")), True
            )

        def complete_upload(self, upload, receipts):
            c.calls.append(("complete", c.engine.pool.checkedout()))
            assert c.calls[-1][1] == 0
            if c.callback:
                c.callback("complete")
            if c.fault == "lost":
                raise OSError("Accepted native completion response lost")
            return CompletedObject(
                self.uri_for_key(upload.object_key),
                upload.byte_size,
                "provider-checksum",
                {"generation": "7"} if self.driver_name == "gcs" else {"version_id": "original-v1"},
            )

        def exists(self, uri):
            raise AssertionError("Latest-object existence is not original completion custody")

        def stat(self, uri):
            raise AssertionError("Latest-object metadata must never supply the receipt")

    c.store = Native()
    c.store.driver_name = provider
    monkeypatch.setattr(uploads, "get_object_store", lambda *_: c.store)
    settings = _settings(project_storage_quota_bytes=quota)
    monkeypatch.setattr(completion, "get_settings", lambda: settings)
    with factory() as db:
        c.token_version = db.get(User, actor_id).token_version

    def admission_store(*_):
        assert c.engine.pool.checkedout() == 0
        return c.store

    monkeypatch.setattr(admission, "get_object_store", admission_store)
    c.sha256 = hashlib.sha256(b"a,b\n1,2\n\n\n").hexdigest()
    c.upload_id = admission.begin_upload_session(
        factory,
        actor_id,
        c.token_version,
        project_id,
        _payload(
            sha256=c.sha256,
            retention_days=retention_days,
            upload_kind=upload_kind,
            legal_hold=legal_hold,
        ),
        uuid.uuid4().hex,
        origin="https://app.test",
        settings=settings,
    ).id

    def store(*_):
        assert c.engine.pool.checkedout() == 0
        c.constructions += 1
        return c.store

    monkeypatch.setattr(completion, "get_object_store", store)
    c.payload = CompleteUploadRequest(sha256=c.sha256, receipts=[])
    c.key = uuid.uuid4().hex
    c.send = lambda: completion.complete_upload_session(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        c.payload,
        c.key,
        settings=settings,
    )
    return c


def test_original_completion_commits_before_native_io_and_replays_without_provider(completion_case):
    c = completion_case
    result = c.send()
    assert result.session.status == "object_completed"
    assert c.send() == result
    assert c.calls == [("progress", 0), ("complete", 0)]
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        journal, command = completion.journal(db, upload)
        assert (
            journal["phase"] == "published"
            and journal["receipt"]["object_version"] == "original-v1"
        )
        assert command.response_status == 200
        rows = list(db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id)))
        assert len(rows) == 1 and rows[0].topic == completion.TOPIC
        assert rows[0].payload["intent_id"] == journal["intent"]["id"]
    assert "native_completion_receipt_digest" not in result.model_dump()


def test_lost_completion_ack_never_reissues_or_adopts_latest(completion_case):
    c = completion_case
    c.fault = "lost"
    with pytest.raises(OSError):
        c.send()
    c.fault = None
    with pytest.raises(HTTPException) as failure:
        c.send()
    assert failure.value.status_code == 409
    assert c.calls == [("progress", 0), ("complete", 0)]
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "uploading"
        assert upload.provider_state[completion.RECORD]["phase"] == "issued"


@pytest.mark.parametrize(
    "fault,code",
    [("user", 401), ("token", 401), ("membership", 403), ("project", 409), ("source", 409)],
)
def test_revocation_during_native_completion_keeps_receipt_without_publication(
    completion_case, fault, code
):
    c = completion_case

    def revoke(stage):
        if stage != "complete":
            return
        with c.factory() as db, db.begin():
            if fault == "user":
                db.get(User, c.actor_id).is_active = False
            elif fault == "token":
                db.get(User, c.actor_id).token_version += 1
            elif fault == "membership":
                db.scalar(
                    select(ProjectMembership).where(
                        ProjectMembership.project_id == c.project_id,
                        ProjectMembership.user_id == c.actor_id,
                    )
                ).expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif fault == "project":
                project = db.get(Project, c.project_id)
                project.settings = {**project.settings, "deletion_in_progress": True}
            else:
                db.get(DatasetUploadSession, c.upload_id).expected_object_digest = "b" * 64

    c.callback = revoke
    with pytest.raises((HTTPException, UploadContractError)) as failure:
        c.send()
    if isinstance(failure.value, HTTPException):
        assert failure.value.status_code == code
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record = upload.provider_state[completion.RECORD]
        assert (
            record["phase"] == "recorded" and record["receipt"]["object_version"] == "original-v1"
        )
        assert upload.status == "uploading" and upload.completed_object_uri is None
        assert not list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))
        )


@pytest.mark.parametrize("part", ["version", "source", "command", "missing-receipt", "phase"])
def test_changed_original_custody_cannot_replay_completion(completion_case, part):
    c = completion_case
    c.send()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        record = copy.deepcopy(upload.provider_state[completion.RECORD])
        if part == "version":
            record["receipt"]["object_version"] = "replacement"
        elif part == "source":
            upload.expected_object_digest = "b" * 64
        elif part == "command":
            db.get(WorkflowCommand, uuid.UUID(record["intent"]["command_id"])).request_hash = "bad"
        elif part == "missing-receipt":
            record.pop("receipt")
        else:
            record["phase"] = "admitted"
        upload.provider_state = {**upload.provider_state, completion.RECORD: record}
    with pytest.raises(UploadContractError):
        c.send()
    assert c.calls == [("progress", 0), ("complete", 0)]


def test_unresolved_completion_blocks_abort_and_expiry_cleanup(completion_case):
    c = completion_case
    c.fault = "lost"
    with pytest.raises(OSError):
        c.send()
    with pytest.raises(UploadContractError, match="outcome"):
        retirement.abort_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            "cancel",
            settings=_settings(),
        )
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    assert not any(retirement.cleanup_abandoned_uploads(c.factory, settings=_settings()).values())
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "uploading"


def test_missing_journal_cannot_restart_an_original_committed_native_attempt(
    completion_case, monkeypatch
):
    c = completion_case
    c.fault = "lost"
    with pytest.raises(OSError):
        c.send()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        upload.provider_state = {
            k: v for k, v in upload.provider_state.items() if k != completion.RECORD
        }
        expiry = upload.expires_at

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            moment = expiry + timedelta(seconds=1)
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(retirement, "datetime", Clock)
    with pytest.raises(UploadContractError, match="history is missing"):
        retirement.abort_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            "cancel",
            settings=_settings(),
        )
    assert not any(retirement.cleanup_abandoned_uploads(c.factory, settings=_settings()).values())
    c.fault = None
    c.key = uuid.uuid4().hex
    with pytest.raises(UploadContractError, match="history is missing"):
        c.send()
    assert c.calls == [("progress", 0), ("complete", 0)]


@pytest.mark.parametrize("completion_case", ["dataset"], indirect=True)
def test_receipt_survives_publication_rollback_without_another_provider_commit(
    completion_case, monkeypatch
):
    from automl_api.models.datasets import DatasetVersion

    c = completion_case
    original = uploads.record_completed_upload

    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError("Publication transaction interrupted after materialization")

    monkeypatch.setattr(uploads, "record_completed_upload", interrupted)
    with pytest.raises(OSError):
        c.send()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, _ = completion.journal(db, upload)
        assert record["phase"] == "recorded" and upload.dataset_version_id is None
    monkeypatch.setattr(uploads, "record_completed_upload", original)
    result = c.send()
    assert result.version.version_number == 1
    assert c.calls == [("progress", 0), ("complete", 0)]
    c.key = uuid.uuid4().hex
    assert c.send() == result
    with c.factory() as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
                    )
                )
            )
            == 1
        )
        assert (
            len(list(db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))))
            == 1
        )


def test_unqualified_old_admission_is_refused_without_provider_io(completion_case):
    c = completion_case
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).provider_state = {"transfer_unit_size": 6}
    with pytest.raises(UploadContractError, match="versioned completion"):
        c.send()
    assert c.calls == [] and c.constructions == 0


def test_wrong_client_digest_is_refused_without_provider_io(completion_case):
    c = completion_case
    c.payload.sha256 = "b" * 64
    with pytest.raises(UploadContractError, match="Final client"):
        c.send()
    assert c.calls == [] and c.constructions == 0


def test_incomplete_native_progress_can_resume_without_reissuing_a_write(completion_case):
    c = completion_case
    original = c.store.query_progress

    def partial(upload):
        original(upload)
        return UploadProgress(6, 10, (TransferReceipt(1, 0, 6, "part1"),), False)

    c.store.query_progress = partial
    with pytest.raises(UploadContractError, match="not confirmed"):
        c.send()
    c.store.query_progress = original
    assert c.send().session.status == "object_completed"
    assert c.calls == [("progress", 0), ("progress", 0), ("complete", 0)]


def test_concurrent_same_and_different_key_cannot_repeat_original_commit(completion_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = completion_case
    entered, release = Event(), Event()

    def pause(stage):
        if stage == "complete":
            entered.set()
            assert release.wait(timeout=10)

    c.callback = pause
    original_key = c.key
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(c.send)
        try:
            assert entered.wait(timeout=10)
            for key in [original_key, uuid.uuid4().hex]:
                c.key = key
                with pytest.raises(HTTPException) as failure:
                    c.send()
                assert failure.value.status_code == 409
        finally:
            c.key = original_key
            release.set()
        result = future.result(timeout=10)
    assert c.send() == result
    assert c.calls == [("progress", 0), ("complete", 0)]


@pytest.mark.parametrize("completion_case", ["gcs"], indirect=True)
def test_gcs_unknown_read_can_resume_only_through_original_session_receipt(completion_case):
    c = completion_case
    c.fault = "lost"
    with pytest.raises(OSError):
        c.send()
    c.fault = None
    assert c.send().session.status == "object_completed"
    assert c.calls == [("progress", 0), ("complete", 0), ("complete", 0)]
    with c.factory() as db:
        record, _ = completion.journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert record["receipt"]["object_version"] == "7"


def test_actual_complete_route_releases_authentication_sql_before_factory_and_native_io(
    completion_case,
):
    from typing import Annotated

    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    c = completion_case
    app = FastAPI()
    app.include_router(router)

    def database():
        with c.factory() as db:
            yield db

    def principal(db: Annotated[object, Depends(get_db)]):
        return db.get(User, c.actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    app.dependency_overrides[get_current_user] = principal
    with TestClient(app) as client:
        response = client.post(
            f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/complete",
            headers={"Idempotency-Key": c.key},
            json=c.payload.model_dump(mode="json"),
        )
    assert response.status_code == 200, response.text
    assert "native_completion_receipt_digest" not in response.json()
    assert c.calls == [("progress", 0), ("complete", 0)] and c.engine.pool.checkedout() == 0


@pytest.mark.parametrize("upload_status", ["uploading", "object_completed", "verifying", "ready"])
@pytest.mark.parametrize(
    "metadata",
    [
        {"digest_scope": "multipart_manifest"},
        {"digest_scope": "future-v2"},
        {"digest_scope": None},
        {"digest_algorithm": "md5"},
        {"digest_algorithm": None},
        {"expected_object_digest": None},
        {"expected_object_digest": "a" * 63},
        {"expected_object_digest": "z" * 64},
        {"expected_object_digest": "A" * 64},
        {"expected_object_digest": 1},
    ],
)
def test_completion_rejects_unqualified_digest_before_provider_or_replay(
    completion_case, upload_status, metadata
):
    c = completion_case
    if any(
        value is None and not DatasetUploadSession.__table__.c[name].nullable
        for name, value in metadata.items()
    ):
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            with c.factory() as db, db.begin():
                upload = db.get(DatasetUploadSession, c.upload_id)
                for name, value in metadata.items():
                    setattr(upload, name, value)
        assert c.calls == [] and c.constructions == 0
        return
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        upload.status = upload_status
        for name, value in metadata.items():
            setattr(upload, name, value)
    with pytest.raises(UploadContractError, match="byte-stream SHA-256"):
        c.send()
    assert c.calls == [] and c.constructions == 0
