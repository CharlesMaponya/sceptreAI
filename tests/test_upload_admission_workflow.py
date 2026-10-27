"""Real SQL upload admission; native creation observes only committed state."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import ResumableUploadRead
from automl_api.services import upload_admission as admission
from automl_api.services import uploads
from automl_api.storage.contracts import UploadCapabilities, UploadContractError
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase2_upload_services import Driver, _payload, _settings


@pytest.fixture
def admission_case(phase1_engine, committed_tenant, monkeypatch):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory

    actor_id, project_id = committed_tenant
    c = SimpleNamespace(
        engine=phase1_engine,
        actor_id=actor_id,
        project_id=project_id,
        factory=sessionmaker(bind=phase1_engine, expire_on_commit=False, autoflush=False),
        key=uuid.uuid4().hex,
        calls=[],
        callback=None,
        receipt_patch={},
        native_error=None,
        constructions=0,
        native_replies=[],
        on_construction=None,
    )
    c.upload_id = uuid.uuid5(project_id, f"dataset.upload.begin:{actor_id}:{c.key}")

    class Native(Driver):
        def begin_upload(self, request):
            assert c.engine.pool.checkedout() == 0
            with c.factory() as db:
                upload = db.get(DatasetUploadSession, c.upload_id)
                command = db.scalar(
                    select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
                )
                assert upload is not None and upload.status == "initiated"
                assert command is not None and command.status.value == "running"
                intent_id = upload.provider_state[admission.RECORD]["intent"]["id"]
                assert request.object_key.startswith(
                    f"projects/{c.project_id}/raw/{c.upload_id}/{intent_id}/"
                )
            c.calls.append(request)
            if c.callback:
                c.callback()
            reply = replace(super().begin_upload(request), **c.receipt_patch)
            c.native_replies.append(reply)
            if c.native_error:
                raise c.native_error
            return reply

    c.store = Native()

    def store(*_):
        assert c.engine.pool.checkedout() == 0
        c.constructions += 1
        if c.on_construction:
            c.on_construction()
        return c.store

    monkeypatch.setattr(uploads, "get_object_store", store)
    monkeypatch.setattr(admission, "get_object_store", store)
    monkeypatch.setattr(admission, "get_settings", _settings)
    app = FastAPI()
    app.include_router(router)

    def db():
        with c.factory() as session:
            yield session

    def user():
        with c.factory() as session:
            return session.get(User, actor_id)

    app.dependency_overrides[get_db] = db
    app.dependency_overrides[get_current_user] = user
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    from automl_api.api.routes import datasets

    monkeypatch.setattr(uploads, "get_settings", _settings)
    monkeypatch.setattr(datasets, "get_settings", _settings)
    c.app = app
    c.payload = _payload(retention_days=None)
    c.settings = _settings()
    with c.factory() as session:
        c.token_version = session.get(User, actor_id).token_version
    c.begin = lambda **kwargs: admission.begin_upload_session(
        c.factory,
        actor_id,
        c.token_version,
        project_id,
        kwargs.get("payload", c.payload),
        kwargs.get("key", c.key),
        origin=kwargs.get("origin", "https://app.test"),
        settings=c.settings,
    )
    c.post = lambda: TestClient(app).post(
        f"/projects/{project_id}/datasets/uploads",
        headers={"Origin": "https://app.test", "Idempotency-Key": c.key},
        json=c.payload.model_dump(mode="json"),
    )
    return c


def test_actual_begin_route_commits_quota_before_sql_free_native_creation(admission_case):
    c = admission_case
    response = c.post()
    assert response.status_code == 201, response.text
    result = ResumableUploadRead.model_validate(response.json())
    assert result.id == c.upload_id and len(c.calls) == 1
    assert c.post().json() == response.json() and len(c.calls) == 1
    assert c.constructions == 1


@pytest.mark.parametrize("fence", ["actor", "membership", "project", "source"])
def test_known_reply_survives_changed_authority_without_publication(admission_case, fence):
    c = admission_case

    def change():
        with c.factory() as db, db.begin():
            if fence == "actor":
                db.get(User, c.actor_id).token_version += 1
            elif fence == "membership":
                row = db.scalar(
                    select(ProjectMembership).where(
                        ProjectMembership.project_id == c.project_id,
                        ProjectMembership.user_id == c.actor_id,
                    )
                )
                row.role = ProjectRole.VIEWER
            elif fence == "project":
                db.get(Project, c.project_id).settings = {"deletion_in_progress": True}
            else:
                db.get(DatasetUploadSession, c.upload_id).original_filename = "changed.csv"

    c.callback = change
    with pytest.raises((HTTPException, UploadContractError)):
        c.begin()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        _, record, result = admission._journal(db, command)
        assert record["phase"] == "recorded" and record["receipt"]["provider_id"] == "provider-1"
        assert (
            result is None
            and upload.status == "initiated"
            and command.status == CommandStatus.RUNNING
        )
    assert len(c.calls) == 1


def test_fresh_token_retry_publishes_known_receipt_without_native_replay(admission_case):
    c = admission_case

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).token_version += 1

    c.callback = revoke
    with pytest.raises(HTTPException) as denied:
        c.begin()
    assert denied.value.status_code == 401
    c.token_version += 1
    c.callback = None
    assert c.begin().status == "uploading" and len(c.calls) == 1


def test_concurrent_same_key_cannot_issue_second_native_creation(admission_case):
    c = admission_case

    def compete():
        with pytest.raises(HTTPException) as pending:
            c.begin()
        assert pending.value.status_code == 409

    c.callback = compete
    assert c.begin().id == c.upload_id and len(c.calls) == 1


def test_committed_admission_reservation_prevents_competing_key_exceeding_quota(admission_case):
    c = admission_case
    c.settings = _settings(project_storage_quota_bytes=10)

    def compete():
        with pytest.raises(ValueError, match="quota"):
            c.begin(key=uuid.uuid4().hex)

    c.callback = compete
    assert c.begin().id == c.upload_id
    with c.factory() as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(DatasetUploadSession).where(
                            DatasetUploadSession.project_id == c.project_id
                        )
                    )
                )
            )
            == 1
        )
    assert len(c.calls) == 1


def test_conflicting_payload_cannot_reuse_original_native_session(admission_case):
    c = admission_case
    c.begin()
    with pytest.raises(HTTPException) as conflict:
        c.begin(payload=c.payload.model_copy(update={"filename": "another.csv"}))
    assert conflict.value.status_code == 409 and len(c.calls) == 1


@pytest.mark.parametrize(
    "patch",
    [
        dict(provider_id=""),
        dict(provider_id=False),
        dict(provider_id="x" * 1025),
        dict(object_key="foreign"),
        dict(protocol="block_list"),
        dict(byte_size=True),
        dict(byte_size=11),
        dict(expires_at=datetime.now(UTC) - timedelta(days=1)),
        dict(state={"completion_protocol": uploads.UPLOAD_COMPLETION_PROTOCOL}),
    ],
)
def test_unqualified_native_reply_stays_unpublished_and_reserved(admission_case, patch):
    c = admission_case
    c.receipt_patch = patch
    with pytest.raises(UploadContractError):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "initiated" and row.provider_state[admission.RECORD]["phase"] == "unknown"
        )
        assert row.multipart_upload_id == "" and "completion_protocol" not in row.provider_state
    assert len(c.calls) == 1


def test_pending_native_admission_cannot_issue_transfer_progress_or_completion(admission_case):
    from automl_api.schemas.datasets import CompleteUploadRequest, TransferCursorRead
    from automl_api.services.upload_completion import complete_upload_session
    from automl_api.services.upload_retirement import abort_upload_session

    c = admission_case
    c.native_error = OSError("response lost after acceptance")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        for action in [
            lambda: uploads.query_upload_progress(row),
            lambda: uploads.issue_transfer_instruction(
                row,
                TransferCursorRead(unit_number=1, offset=0, length=6),
                origin="https://app.test",
                settings=c.settings,
            ),
        ]:
            with pytest.raises(UploadContractError, match="admission"):
                action()
    with pytest.raises(UploadContractError):
        complete_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            CompleteUploadRequest(sha256=c.payload.sha256),
            uuid.uuid4().hex,
            settings=c.settings,
        )
    with pytest.raises(UploadContractError):
        abort_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            uuid.uuid4().hex,
            settings=c.settings,
        )
    assert len(c.calls) == 1


@pytest.mark.parametrize("failure", ["quota", "region", "embedded", "missing-project"])
def test_admission_policy_refuses_before_any_native_creation(admission_case, failure):
    c = admission_case
    if failure == "quota":
        c.settings = _settings(project_storage_quota_bytes=9)
    elif failure == "region":
        c.settings = _settings(object_store_region="af-south-1")
    elif failure == "embedded":
        c.store.capabilities = lambda: UploadCapabilities(
            "single_put", 1, False, False, False, False
        )
    else:
        with pytest.raises(HTTPException) as missing:
            admission.begin_upload_session(
                c.factory,
                c.actor_id,
                c.token_version,
                uuid.uuid4(),
                c.payload,
                c.key,
                origin="https://app.test",
                settings=c.settings,
            )
        assert missing.value.status_code == 404 and not c.calls
        return
    with pytest.raises(ValueError):
        c.begin()
    assert not c.calls


def test_publication_rollback_retains_reply_without_repeating_creation(admission_case, monkeypatch):
    c = admission_case
    save = admission._save

    def fail_after_publish(upload, record):
        save(upload, record)
        if record["phase"] == "published":
            raise RuntimeError("publication rolled back")

    monkeypatch.setattr(admission, "_save", fail_after_publish)
    with pytest.raises(RuntimeError, match="publication rolled back"):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "initiated" and row.multipart_upload_id == ""
        assert row.provider_state[admission.RECORD]["phase"] == "recorded"
    monkeypatch.setattr(admission, "_save", save)
    assert c.begin().status == "uploading" and len(c.calls) == c.constructions == 1


@pytest.mark.parametrize(
    "fault", ["intent", "source", "attempt", "receipt", "result", "marker", "record", "command"]
)
def test_corrupted_admission_cannot_construct_or_reissue_provider(admission_case, fault):
    import copy

    c = admission_case
    c.begin()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        state = copy.deepcopy(upload.provider_state)
        record = state[admission.RECORD]
        if fault == "intent":
            record["intent"]["source"]["byte_size"] += 1
        elif fault == "source":
            upload.original_filename = "changed.csv"
        elif fault == "attempt":
            record["attempt"]["id"] = str(uuid.uuid4())
        elif fault == "receipt":
            record["receipt"]["provider_id"] = "foreign"
        elif fault == "result":
            record["result_digest"] = "foreign"
        elif fault == "marker":
            state.pop(admission.STARTED)
        elif fault == "record":
            state.pop(admission.RECORD)
            state.pop(admission.STARTED)
        else:
            command.response_payload = {
                **command.response_payload,
                "admission_intent_digest": "foreign",
            }
        upload.provider_state = state
    with pytest.raises(UploadContractError):
        c.begin()
    assert len(c.calls) == c.constructions == 1


def test_stale_creation_reply_is_retained_without_replacing_winner(admission_case, monkeypatch):
    c = admission_case

    def overlap():
        c.callback = None
        later = datetime.now(UTC) + timedelta(minutes=6)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return later if tz else later.replace(tzinfo=None)

        monkeypatch.setattr(admission, "datetime", Clock)
        c.winner = c.begin()

    c.callback = overlap
    with pytest.raises(UploadContractError, match="lost its native attempt"):
        c.begin()
    assert c.begin() == c.winner and len(c.calls) == 2
    with c.factory() as db:
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        _, record, result = admission._journal(db, command)
        assert record["phase"] == "published" and record["attempt"]["number"] == 2
        assert (
            result == c.winner
            and len(command.response_payload["superseded_admission_receipts"]) == 1
        )


def test_unpublished_gcs_creation_loss_can_renew_without_delivering_old_capability(
    admission_case, monkeypatch
):
    c = admission_case
    c.store.driver_name = "gcs"
    c.store.capabilities = lambda: UploadCapabilities(
        "resumable_offset", 1, False, False, True, True
    )
    c.store.uri_for_key = lambda key: f"gs://bucket/{key}"
    c.receipt_patch = dict(provider_id="https://storage.test/session-one")
    c.native_error = OSError("accepted session URI response lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.multipart_upload_id == "" and upload.status == "initiated"
    later = datetime.now(UTC) + timedelta(minutes=6)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return later if tz else later.replace(tzinfo=None)

    monkeypatch.setattr(admission, "datetime", Clock)
    c.native_error = None
    c.receipt_patch = dict(provider_id="https://storage.test/session-two")
    result = c.begin()
    assert result.status == "uploading" and len(c.calls) == 2
    assert [r.provider_id for r in c.native_replies] == [
        "https://storage.test/session-one",
        "https://storage.test/session-two",
    ]
    assert "session-one" not in result.model_dump_json()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.multipart_upload_id == "https://storage.test/session-two"


def test_known_legacy_begin_response_replays_without_native_construction(admission_case):
    c = admission_case
    result = c.begin()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        upload.provider_state = {
            k: v
            for k, v in upload.provider_state.items()
            if k not in {admission.RECORD, admission.STARTED}
        }
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        command.response_payload = result.model_dump(mode="json")
    assert c.begin() == result and len(c.calls) == c.constructions == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("object_store_endpoint", "https://other-store.test"),
        ("object_store_type", "gcs"),
        ("object_store_public_endpoint", "https://other-browser.test"),
        ("object_store_bucket", "another-bucket"),
        ("object_store_region", "af-south-1"),
        ("gcs_project", "another-project"),
        ("azure_storage_account", "another-account"),
    ],
)
def test_configured_store_identity_cannot_rebind_original_admission(admission_case, field, value):
    c = admission_case
    c.begin()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        with pytest.raises(UploadContractError, match="Configured store"):
            admission.require_store(
                _settings(**{field: value}), upload.provider_state, upload.provider_driver
            )


@pytest.mark.parametrize(
    "changed", [dict(object_store_endpoint="https://changed.test"), dict(object_store_type="gcs")]
)
def test_changed_store_cannot_publish_a_retained_reply(admission_case, changed):
    c = admission_case
    original = c.settings

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).token_version += 1

    c.callback = revoke
    with pytest.raises(HTTPException):
        c.begin()
    c.token_version += 1
    c.settings = _settings(**changed)
    with pytest.raises(UploadContractError, match="Configured store"):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "initiated"
            and row.provider_state[admission.RECORD]["phase"] == "recorded"
        )
    c.settings = original
    c.callback = None
    assert c.begin().status == "uploading" and len(c.calls) == c.constructions == 1


def test_changed_store_blocks_connected_mutations_before_native_object_work(
    admission_case, monkeypatch
):
    from unittest.mock import MagicMock

    from automl_api.schemas.datasets import CompleteUploadRequest, TransferCursorRead
    from automl_api.services import upload_completion, upload_retirement

    c = admission_case
    c.begin()
    changed = _settings(object_store_endpoint="https://another-store.test")
    complete_store, retire_store = MagicMock(), MagicMock()
    monkeypatch.setattr(upload_completion, "get_object_store", complete_store)
    monkeypatch.setattr(upload_retirement, "get_object_store", retire_store)
    c.store.query_progress = MagicMock()
    c.store.create_transfer_instruction = MagicMock()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
    actions = [
        lambda: uploads.query_upload_progress(upload, settings=changed),
        lambda: uploads.issue_transfer_instruction(
            upload,
            TransferCursorRead(unit_number=1, offset=0, length=6),
            origin="https://app.test",
            settings=changed,
        ),
        lambda: upload_completion.complete_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            CompleteUploadRequest(sha256=c.payload.sha256),
            "complete",
            settings=changed,
        ),
        lambda: upload_retirement.abort_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            "abort",
            settings=changed,
        ),
    ]
    for action in actions:
        with pytest.raises(UploadContractError, match="Configured store"):
            action()
    complete_store.assert_not_called()
    retire_store.assert_not_called()
    c.store.query_progress.assert_not_called()
    c.store.create_transfer_instruction.assert_not_called()
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "uploading"


def test_subprocess_loss_after_creation_recovers_original_reserved_session(
    admission_case, tmp_path, monkeypatch
):
    import json
    import os
    import signal
    import subprocess
    import sys

    from automl_api.storage.contracts import ProviderUpload

    c = admission_case
    witness = tmp_path / "accepted-native-session.json"
    script = r"""
import json,os,signal,sys,uuid
from pathlib import Path
from sqlalchemy import create_engine,select
from sqlalchemy.orm import sessionmaker
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import ResumableUploadBeginRequest
from automl_api.services import upload_admission as a
from test_phase2_upload_services import Driver,_settings
engine=create_engine(os.environ['DATABASE_URL'])
factory=sessionmaker(bind=engine,expire_on_commit=False,autoflush=False)
actor,project=map(uuid.UUID,sys.argv[1:3])
class Native(Driver):
 def begin_upload(self,request):
  assert engine.pool.checkedout()==0
  with factory() as db:
   row=db.scalar(select(DatasetUploadSession).where(DatasetUploadSession.object_key==request.object_key))
   command=db.scalar(select(WorkflowCommand).where(WorkflowCommand.resource_id==row.id))
   _,record,_=a._journal(db,command)
   assert row.status=='initiated' and record['phase']=='issued'
  Path(sys.argv[5]).write_text(json.dumps(dict(key=request.object_key,provider_id='original-session',writes=1)))
  os.kill(os.getpid(),signal.SIGKILL)
a.get_object_store=lambda *_:Native()
a.begin_upload_session(factory,actor,int(sys.argv[4]),project,ResumableUploadBeginRequest.model_validate_json(sys.argv[6]),sys.argv[3],origin='https://app.test',settings=_settings())
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(c.actor_id),
            str(c.project_id),
            c.key,
            str(c.token_version),
            str(witness),
            c.payload.model_dump_json(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env=dict(os.environ),
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    accepted = json.loads(witness.read_text())
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "initiated" and row.multipart_upload_id == ""
        record = row.provider_state[admission.RECORD]
        assert record["phase"] == "issued"
        later = datetime.fromisoformat(record["attempt"]["expires_at"]) + timedelta(seconds=1)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return later if tz else later.replace(tzinfo=None)

    monkeypatch.setattr(admission, "datetime", Clock)
    observations = []

    def recover_original(request):
        assert c.engine.pool.checkedout() == 0
        assert request.object_key == accepted["key"]
        observations.append(request.object_key)
        assert json.loads(witness.read_text())["writes"] == 1
        return ProviderUpload(
            accepted["provider_id"],
            request.object_key,
            "multipart",
            request.byte_size,
            request.expires_at,
            {"transfer_unit_size": request.transfer_unit_size},
        )

    c.store.begin_upload = recover_original
    assert c.begin().id == c.upload_id
    assert c.begin().id == c.upload_id and observations == [accepted["key"]]
    assert json.loads(witness.read_text())["writes"] == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.multipart_upload_id == "original-session" and row.status == "uploading"


def test_admission_snapshots_shared_payload_before_constructor_callbacks(admission_case):
    c = admission_case

    def mutate_shared_payload():
        c.payload.byte_size = 11
        c.payload.tags["changed"] = True

    c.on_construction = mutate_shared_payload
    result = c.begin()
    assert result.byte_size == 10 and c.payload.byte_size == 11
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        assert upload.byte_size == command.request_payload["byte_size"] == 10
        assert upload.tags == command.request_payload["tags"] == {}
    assert c.calls[0].byte_size == 10


def test_recorded_native_reply_cannot_be_downgraded_into_a_new_issue(admission_case, monkeypatch):
    import copy

    c = admission_case

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).token_version += 1

    c.callback = revoke
    with pytest.raises(HTTPException):
        c.begin()
    c.token_version += 1
    c.callback = None
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(upload.provider_state)
        record = state[admission.RECORD]
        assert record["phase"] == "recorded"
        record["phase"] = "unknown"
        record.pop("receipt")
        later = datetime.fromisoformat(record["attempt"]["expires_at"]) + timedelta(seconds=1)
        upload.provider_state = state

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return later if tz else later.replace(tzinfo=None)

    monkeypatch.setattr(admission, "datetime", Clock)
    with pytest.raises(UploadContractError):
        c.begin()
    assert len(c.calls) == c.constructions == 1


def test_released_hold_allows_original_published_staged_admission_to_retire(
    admission_case, monkeypatch
):
    from automl_api.services import upload_retirement

    c = admission_case
    c.payload.legal_hold = True
    c.begin()
    monkeypatch.setattr(upload_retirement, "get_object_store", lambda *_: c.store)
    with pytest.raises(UploadContractError, match="hold"):
        upload_retirement.abort_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            "retire",
            settings=c.settings,
        )
    assert c.store.aborted == 0
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        original_intent = row.provider_state[admission.RECORD]["intent"]
        row.legal_hold = False
    result = upload_retirement.abort_upload_session(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        "retire",
        settings=c.settings,
    )
    assert result.status == "aborted" and c.store.aborted == 1
    with c.factory() as db:
        assert (
            db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD]["intent"]
            == original_intent
        )


def test_minio_alias_and_rotated_credentials_preserve_store_identity(admission_case):
    c = admission_case
    c.begin()
    changed = _settings(
        object_store_type="minio",
        object_store_access_key="rotated-key",
        object_store_secret_key="rotated-secret",
    )
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        admission.require_store(changed, upload.provider_state, upload.provider_driver)
    assert len(c.calls) == 1
