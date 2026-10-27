"""Factory-owned cancellation and retirement against real owned PostgreSQL."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_retirement as retirement
from automl_api.storage.contracts import DeletionPending, UploadContractError
from fastapi import HTTPException
from test_phase2_upload_services import _settings
from test_upload_completion_workflow import committed_tenant as committed_tenant
from test_upload_completion_workflow import completion_case as completion_case
from test_upload_completion_workflow import phase1_engine as phase1_engine


@pytest.fixture
def retirement_case(completion_case, monkeypatch):
    c = completion_case
    c.events, c.effects = [], []
    c.on_native = None
    c.retirement_error = None
    c.error_kind = None
    c.gcs_completed = False
    c.cancel_key = uuid.uuid4().hex

    def event(kind):
        assert c.engine.pool.checkedout() == 0
        c.events.append(kind)
        if c.on_native:
            c.on_native(kind)

    def store(*_):
        event("store")
        return c.store

    def native(kind, identity):
        event(kind)
        with c.factory() as db:
            upload = db.get(DatasetUploadSession, identity)
            record, command = retirement._journal(db, upload)
            assert upload.status == "retiring" and record["phase"] == "issued"
            assert command.status == CommandStatus.RUNNING
            assert upload.provider_state[retirement.STARTED] is True
        c.effects.append(kind)
        if c.retirement_error and (c.error_kind is None or kind == c.error_kind):
            raise c.retirement_error

    def abort(upload):
        assert upload.provider_id and upload.object_key
        native("abort", uuid.UUID(upload.object_key.split("/raw/", 1)[1].split("/", 1)[0]))

    def delete(uri, *, version=None):
        identity = uuid.UUID(uri.split("/raw/", 1)[1].split("/", 1)[0])
        with c.factory() as db:
            row = db.get(DatasetUploadSession, identity)
            assert uri == c.store.uri_for_key(row.object_key)
        assert version == ("7" if c.store.driver_name == "gcs" else "original-v1")
        native("delete", identity)

    with c.factory() as db:
        c.object_key = db.get(DatasetUploadSession, c.upload_id).object_key
    monkeypatch.setattr(c.store, "abort_upload", abort)
    monkeypatch.setattr(c.store, "delete", delete)
    monkeypatch.setattr(retirement, "get_object_store", store)
    monkeypatch.setattr(retirement, "get_settings", _settings)

    def original_session(upload, *, allow_expired=False):
        from automl_api.storage.contracts import CompletedObject, UploadProgress

        assert allow_expired
        event("session_status")
        completed = (
            CompletedObject(
                c.store.uri_for_key(upload.object_key), upload.byte_size, None, {"generation": "7"}
            )
            if c.gcs_completed
            else None
        )
        return UploadProgress(
            upload.byte_size if completed else 0, upload.byte_size, (), bool(completed)
        ), completed

    monkeypatch.setattr(c.store, "_query_progress", original_session, raising=False)

    def at(moment):
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return moment if tz else moment.replace(tzinfo=None)

        monkeypatch.setattr(retirement, "datetime", Clock)
        c.now = moment

    c.at = at
    c.cancel = lambda: retirement.abort_upload_session(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        c.cancel_key,
        settings=_settings(),
    )
    return c


def test_actual_abort_route_releases_sql_before_native_cancellation(retirement_case):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    c = retirement_case
    app = FastAPI()
    app.include_router(router)

    def db():
        with c.factory() as session:
            yield session

    def user():
        with c.factory() as session:
            return session.get(User, c.actor_id)

    app.dependency_overrides[get_db] = db
    app.dependency_overrides[get_current_user] = user
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    with TestClient(app) as client:
        result = client.post(
            f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/abort",
            headers={"Idempotency-Key": c.cancel_key},
        )
    assert result.status_code == 200, result.text
    assert result.json()["status"] == "aborted" and c.effects == ["abort"]


def test_original_staged_cancel_commits_intent_and_replays_without_provider(retirement_case):
    c = retirement_case
    result = c.cancel()
    assert result.status == "aborted"
    c.events.clear()
    assert c.cancel() == result and not c.events
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, owner = retirement._journal(db, upload)
        assert record["phase"] == "published" and owner.status == CommandStatus.SUCCEEDED
        assert record["target"] == {"kind": "staged"}


def test_known_completed_input_retires_only_original_version(retirement_case):
    c = retirement_case
    c.send()
    result = c.cancel()
    assert result.status == "aborted" and c.effects == ["delete"]


@pytest.mark.parametrize("fault", ["actor", "source", "hold", "project"])
def test_known_native_outcome_survives_revocation_without_publishing(retirement_case, fault):
    c = retirement_case

    def revoke(kind):
        if kind != "abort":
            return
        with c.factory() as db, db.begin():
            upload = db.get(DatasetUploadSession, c.upload_id)
            if fault == "actor":
                db.get(User, c.actor_id).token_version += 1
            elif fault == "source":
                upload.original_filename = "changed.csv"
            elif fault == "hold":
                upload.legal_hold = True
            else:
                db.get(Project, c.project_id).settings = {"deletion_in_progress": True}

    c.on_native = revoke
    with pytest.raises((HTTPException, UploadContractError, ValueError)):
        c.cancel()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, _ = retirement._journal(db, upload)
        assert record["phase"] == "observed" and upload.status == "retiring"
    assert c.effects == ["abort"]


def test_publication_rollback_preserves_known_outcome_and_does_not_repeat_abort(
    retirement_case, monkeypatch
):
    c = retirement_case
    original = retirement.transition_command

    def fail(command, target):
        if target == CommandStatus.SUCCEEDED:
            raise RuntimeError("Publication rolled back")
        return original(command, target)

    monkeypatch.setattr(retirement, "transition_command", fail)
    with pytest.raises(RuntimeError, match="Publication rolled back"):
        c.cancel()
    monkeypatch.setattr(retirement, "transition_command", original)
    c.events.clear()
    assert c.cancel().status == "aborted" and not c.events and c.effects == ["abort"]


def test_missing_retirement_journal_cannot_restart_native_work(retirement_case):
    c = retirement_case
    c.retirement_error = OSError("Accepted response lost")
    with pytest.raises(OSError):
        c.cancel()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        upload.provider_state = {
            k: v for k, v in upload.provider_state.items() if k != retirement.RECORD
        }
    c.events.clear()
    with pytest.raises(UploadContractError, match="history is missing"):
        c.cancel()
    assert not c.events and c.effects == ["abort"]


def test_in_progress_attempt_refuses_same_or_different_key(retirement_case):
    c = retirement_case

    def concurrent(kind):
        if kind != "abort":
            return
        with pytest.raises(HTTPException):
            c.cancel()
        with pytest.raises(HTTPException):
            retirement.abort_upload_session(
                c.factory,
                c.actor_id,
                c.token_version,
                c.project_id,
                c.upload_id,
                "another-key",
                settings=_settings(),
            )

    c.on_native = concurrent
    assert c.cancel().status == "aborted" and c.effects == ["abort"]


def test_native_retention_is_pending_until_independent_original_absence(retirement_case):
    c = retirement_case
    c.send()
    c.retirement_error = DeletionPending("Original native version retained")
    with pytest.raises(DeletionPending):
        c.cancel()
    with pytest.raises(HTTPException, match="retention"):
        c.cancel()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, _ = retirement._journal(db, upload)
        assert record["phase"] == "waiting" and upload.status == "retiring"


@pytest.mark.parametrize("purpose", ["expire", "quarantine"])
def test_cleanup_classes_use_original_version_without_sql(retirement_case, purpose):
    c = retirement_case
    if purpose == "quarantine":
        c.send()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        expiry = upload.expires_at
        if purpose == "quarantine":
            upload.status = "quarantined"
            upload.quarantine_delete_after = datetime.now(UTC) - timedelta(hours=1)
    if purpose == "expire":
        c.at(expiry + timedelta(seconds=1))
    result = retirement.cleanup_abandoned_uploads(c.factory, settings=_settings())
    assert result == {
        "expired": int(purpose == "expire"),
        "completed_deleted": 0,
        "quarantine_deleted": int(purpose == "quarantine"),
    }
    assert c.effects == (["abort"] if purpose == "expire" else ["delete"])


@pytest.mark.parametrize("completion_case", ["gcs"], indirect=True)
@pytest.mark.parametrize("completed", [False, True])
def test_gcs_cancel_uses_original_session_receipt_before_version_retirement(
    retirement_case, completed
):
    c = retirement_case
    c.gcs_completed = completed
    assert c.cancel().status == "aborted"
    assert "session_status" in c.events
    assert c.effects == (["delete"] if completed else ["abort"])
    with c.factory() as db:
        record, _ = retirement._journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert record["target"]["kind"] == ("version" if completed else "staged")
        if completed:
            assert record["target"]["version"] == "7"
            assert record["target"]["original_session_receipt"]["object_version"] == "7"


def test_fresh_authenticated_retry_can_publish_retained_known_outcome(retirement_case):
    c = retirement_case

    def revoke(kind):
        if kind == "abort":
            with c.factory() as db, db.begin():
                db.get(User, c.actor_id).token_version += 1

    c.on_native = revoke
    with pytest.raises(HTTPException):
        c.cancel()
    c.token_version += 1
    c.events.clear()
    assert c.cancel().status == "aborted" and not c.events and c.effects == ["abort"]


def test_actual_cleanup_worker_calls_factory_without_holding_sql(retirement_case):
    from automl_api.workflow_reconciler import cleanup_uploads_once

    c = retirement_case
    with c.factory() as db:
        c.at(db.get(DatasetUploadSession, c.upload_id).expires_at + timedelta(seconds=1))
    assert cleanup_uploads_once(c.factory) == 1
    assert c.effects == ["abort"]
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "expired"


def test_expired_completed_cleanup_and_original_verification_delivery_are_terminal(retirement_case):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import OutboxEntry
    from sqlalchemy import select

    c = retirement_case
    c.send()
    with c.factory() as db:
        c.at(db.get(DatasetUploadSession, c.upload_id).expires_at + timedelta(seconds=1))
    assert retirement.cleanup_abandoned_uploads(c.factory, settings=_settings()) == dict(
        expired=0, completed_deleted=1, quarantine_deleted=0
    )
    assert c.effects == ["delete"]
    with c.factory() as db:
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.aggregate_id == c.upload_id))
        assert entry.status == OutboxStatus.DEAD and entry.lease_owner is None
        assert db.get(WorkflowCommand, entry.command_id).status == CommandStatus.FAILED


@pytest.mark.parametrize("status", ["quarantined", "object_completed", "verifying"])
def test_upload_retention_wait_releases_cleanup_batch_slot(retirement_case, monkeypatch, status):
    from test_phase2_upload_services import _payload

    c = retirement_case
    c.send()
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        row.status = status
        row.quarantine_delete_after = datetime.now(UTC) - timedelta(seconds=1)
        expiry = row.expires_at
    from automl_api.services.upload_admission import begin_upload_session

    other = begin_upload_session(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        _payload(sha256=c.sha256, retention_days=None),
        uuid.uuid4().hex,
        origin="https://app.test",
        settings=_settings(),
    ).id
    c.at(expiry + timedelta(minutes=1))
    c.retirement_error = DeletionPending("Original version still retained")
    c.error_kind = "delete"
    assert not any(
        retirement.cleanup_abandoned_uploads(c.factory, settings=_settings(), limit=1).values()
    )
    assert retirement.cleanup_abandoned_uploads(c.factory, settings=_settings(), limit=1) == dict(
        expired=1, completed_deleted=0, quarantine_deleted=0
    )
    with c.factory() as db:
        assert db.get(DatasetUploadSession, other).status == "expired"
        first = db.get(DatasetUploadSession, c.upload_id)
        assert first.status == "retiring"
        c.at(first.lease_expires_at + timedelta(seconds=1))
    c.retirement_error = None
    outcome = retirement.cleanup_abandoned_uploads(c.factory, settings=_settings(), limit=1)
    assert outcome["quarantine_deleted" if status == "quarantined" else "completed_deleted"] == 1
    assert c.effects == ["delete", "abort", "delete"]


def test_cancellation_during_completion_progress_blocks_original_native_commit(retirement_case):
    c = retirement_case

    def during_progress(kind):
        if kind == "progress":
            assert c.cancel().status == "aborted"

    c.callback = during_progress
    with pytest.raises(UploadContractError, match="retirement prevents"):
        c.send()
    assert c.calls == [("progress", 0)] and c.effects == ["abort"]
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "aborted"
        record, _ = retirement._journal(db, upload)
        assert record["phase"] == "published"


@pytest.mark.parametrize("completion_case", ["gcs"], indirect=True)
def test_gcs_unknown_cancel_can_recover_only_original_session_completion(retirement_case):
    c = retirement_case
    c.retirement_error = UploadContractError("GCS cancellation returned404")
    with pytest.raises(UploadContractError):
        c.cancel()
    c.retirement_error = None
    c.gcs_completed = True
    assert c.cancel().status == "aborted"
    assert c.effects == ["abort", "delete"]
    with c.factory() as db:
        record, _ = retirement._journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert record["target"]["kind"] == "version" and record["target"]["version"] == "7"


def test_old_attempt_failure_cannot_overwrite_newer_success(retirement_case):
    c = retirement_case

    def newer(kind):
        if kind != "abort":
            return
        c.on_native = None
        with c.factory() as db:
            record, _ = retirement._journal(db, db.get(DatasetUploadSession, c.upload_id))
            c.at(datetime.fromisoformat(record["attempt"]["expires_at"]) + timedelta(seconds=1))
        assert c.cancel().status == "aborted"
        raise OSError("Older native attempt failed after replacement")

    c.on_native = newer
    with pytest.raises(OSError, match="Older native attempt"):
        c.cancel()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, owner = retirement._journal(db, upload)
        assert upload.status == "aborted" and record["phase"] == "published"
        assert owner.status == CommandStatus.SUCCEEDED
    assert c.effects == ["abort"]


@pytest.mark.parametrize(
    "field", ["intent", "source", "target", "attempt", "receipt", "result", "marker", "command"]
)
def test_corrupt_retirement_custody_cannot_construct_provider(retirement_case, field):
    import copy

    c = retirement_case
    c.cancel()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(upload.provider_state)
        record = state[retirement.RECORD]
        if field == "marker":
            state.pop(retirement.STARTED)
        elif field == "command":
            command = db.get(WorkflowCommand, uuid.UUID(record["intent"]["command_id"]))
            command.resource_id = uuid.uuid4()
        elif field == "intent":
            record["intent"]["id"] = str(uuid.uuid4())
        elif field == "source":
            record["intent"]["source"]["uri"] = "s3c://unowned/foreign"
        elif field == "target":
            record["target"] = {
                "kind": "version",
                "uri": "s3c://unowned/foreign",
                "version": "replacement",
            }
        elif field == "attempt":
            record["attempt"]["id"] = str(uuid.uuid4())
        elif field == "receipt":
            record["receipt"]["observed_at"] = "changed"
        else:
            record["result"]["status"] = "ready"
        upload.provider_state = state
    c.events.clear()
    with pytest.raises(UploadContractError):
        c.cancel()
    assert not c.events and c.effects == ["abort"]


def test_subprocess_loss_after_native_acceptance_recovers_original_intent(
    retirement_case, tmp_path
):
    import json
    import os
    import signal
    import subprocess
    import sys

    c = retirement_case
    witness = tmp_path / "original-native-acceptance.json"
    script = r"""
import json,os,signal,sys,uuid
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from test_phase2_upload_services import _settings
from automl_api.models.datasets import DatasetUploadSession
from automl_api.services import upload_retirement as r
engine=create_engine(os.environ['DATABASE_URL'])
factory=sessionmaker(bind=engine,expire_on_commit=False,autoflush=False)
actor,project,upload=map(uuid.UUID,sys.argv[1:4])
with factory() as db:
    row=db.get(DatasetUploadSession,upload)
    original_uri=row.provider_state['upload_object_uri']
    original_key=row.object_key
class Native:
    driver_name='s3_compatible'
    def uri_for_key(self,key):
        assert key==original_key
        return original_uri
    def abort_upload(self,native):
        assert engine.pool.checkedout()==0
        with factory() as db:
            row=db.get(DatasetUploadSession,upload)
            record,owner=r._journal(db,row)
            assert row.status=='retiring' and record['phase']=='issued'
            assert row.provider_state[r.STARTED] is True
        assert engine.pool.checkedout()==0
        Path(sys.argv[6]).write_text(json.dumps(dict(key=native.object_key,provider_id=native.provider_id,writes=1)))
        os.kill(os.getpid(),signal.SIGKILL)
r.get_object_store=lambda *_: Native()
r.abort_upload_session(factory,actor,int(sys.argv[4]),project,upload,sys.argv[5],settings=_settings())
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(c.actor_id),
            str(c.project_id),
            str(c.upload_id),
            str(c.token_version),
            c.cancel_key,
            str(witness),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env=dict(os.environ),
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    accepted = json.loads(witness.read_text())
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, _ = retirement._journal(db, upload)
        assert record["phase"] == "issued" and upload.status == "retiring"
        assert accepted == dict(
            key=upload.object_key, provider_id=upload.multipart_upload_id, writes=1
        )
        c.at(datetime.fromisoformat(record["attempt"]["expires_at"]) + timedelta(seconds=1))

    def independently_absent(upload):
        assert c.engine.pool.checkedout() == 0
        assert (upload.object_key, upload.provider_id) == (accepted["key"], accepted["provider_id"])
        assert json.loads(witness.read_text())["writes"] == 1

    c.store.abort_upload = independently_absent
    assert c.cancel().status == "aborted"
    assert json.loads(witness.read_text())["writes"] == 1
    with c.factory() as db:
        record, owner = retirement._journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert record["phase"] == "published" and owner.status == CommandStatus.SUCCEEDED


@pytest.fixture
def dataset_retirement_case(retirement_case):
    c = retirement_case
    c.send()
    with c.factory() as db:
        c.version_id = db.get(DatasetUploadSession, c.upload_id).dataset_version_id
        assert c.version_id is not None
    return c


@pytest.mark.parametrize("retention", ["none", "past", "equal"])
@pytest.mark.parametrize("completion_case", ["dataset"], indirect=True)
def test_unready_unreferenced_dataset_version_retires_after_its_retention(
    dataset_retirement_case, retention
):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.enums import DatasetStatus

    c = dataset_retirement_case
    moment = datetime.now(UTC)
    c.at(moment)
    with c.factory() as db, db.begin():
        db.get(DatasetVersion, c.version_id).retention_until = {
            "none": None,
            "past": moment - timedelta(seconds=1),
            "equal": moment,
        }[retention]
    assert c.cancel().status == "aborted" and c.effects == ["delete"]
    with c.factory() as db:
        assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.FAILED


@pytest.mark.parametrize("fence", ["ready", "hold", "retention", "reference"])
@pytest.mark.parametrize("completion_case", ["dataset"], indirect=True)
def test_dataset_version_fences_refuse_before_provider_construction(dataset_retirement_case, fence):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.enums import DatasetStatus, RunKind, RunStatus, TaskType
    from automl_api.models.runs import ModelRun

    c = dataset_retirement_case
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, c.version_id)
        if fence == "ready":
            version.status = DatasetStatus.READY
        elif fence == "hold":
            version.legal_hold = True
        elif fence == "retention":
            version.retention_until = datetime.now(UTC) + timedelta(days=1)
        else:
            db.add(
                ModelRun(
                    project_id=c.project_id,
                    dataset_version_id=c.version_id,
                    created_by_id=c.actor_id,
                    run_kind=RunKind.TRAINING,
                    task_type=TaskType.REGRESSION,
                    status=RunStatus.FAILED,
                )
            )
    with pytest.raises(UploadContractError, match="readiness, references or retention"):
        c.cancel()
    assert not c.events and not c.effects
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "object_completed"


@pytest.mark.parametrize("fence", ["ready", "hold", "retention", "reference", "uri"])
@pytest.mark.parametrize("completion_case", ["dataset"], indirect=True)
def test_dataset_version_changes_during_native_work_hold_publication(
    dataset_retirement_case, fence
):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.enums import DatasetStatus, RunKind, RunStatus, TaskType
    from automl_api.models.runs import ModelRun

    c = dataset_retirement_case

    def change(kind):
        if kind != "delete":
            return
        with c.factory() as db, db.begin():
            version = db.get(DatasetVersion, c.version_id)
            if fence == "ready":
                version.status = DatasetStatus.READY
            elif fence == "hold":
                version.legal_hold = True
            elif fence == "retention":
                version.retention_until = datetime.now(UTC) + timedelta(days=1)
            elif fence == "uri":
                version.object_uri += ".foreign"
            else:
                db.add(
                    ModelRun(
                        project_id=c.project_id,
                        dataset_version_id=c.version_id,
                        created_by_id=c.actor_id,
                        run_kind=RunKind.TRAINING,
                        task_type=TaskType.REGRESSION,
                        status=RunStatus.FAILED,
                    )
                )

    c.on_native = change
    with pytest.raises(ValueError):
        c.cancel()
    assert c.effects == ["delete"]
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record, command = retirement._journal(db, upload)
        assert record["phase"] == "observed" and upload.status == "retiring"
        assert command.status == CommandStatus.RUNNING


@pytest.mark.parametrize("completion_case", ["azure_blob"], indirect=True)
def test_unsupported_azure_staged_abort_does_not_admit_irreversible_retirement(retirement_case):
    c = retirement_case
    with c.factory() as db:
        c.at(db.get(DatasetUploadSession, c.upload_id).expires_at + timedelta(seconds=1))
    with pytest.raises(UploadContractError, match="Azure staged"):
        c.cancel()
    assert not c.events and not c.effects
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "uploading" and retirement.RECORD not in upload.provider_state
        assert retirement.STARTED not in upload.provider_state


@pytest.mark.parametrize("completion_case", ["held-dataset"], indirect=True)
def test_released_dataset_hold_preserves_original_receipts_and_allows_retirement(retirement_case):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.services import upload_completion

    c = retirement_case
    c.send()
    with pytest.raises(UploadContractError, match="hold"):
        c.cancel()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        before, command = upload_completion.journal(db, upload)
        response = command.response_payload
        version = db.get(DatasetVersion, upload.dataset_version_id)
        assert upload.legal_hold and version.legal_hold
        upload.legal_hold = version.legal_hold = False
    assert c.cancel().status == "aborted" and c.effects == ["delete"]
    with c.factory() as db:
        after, command = upload_completion.journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert after == before and command.response_payload == response


@pytest.mark.parametrize("completion_case", ["dataset"], indirect=True)
@pytest.mark.parametrize("scope", ["upload", "version"])
def test_extended_retention_can_expire_without_rewriting_original_source(retirement_case, scope):
    from automl_api.models.datasets import DatasetVersion

    c = retirement_case
    c.send()
    deadline = datetime.now(UTC) + timedelta(hours=1)

    def retain(kind):
        if kind != "delete":
            return
        with c.factory() as db, db.begin():
            upload = db.get(DatasetUploadSession, c.upload_id)
            row = upload if scope == "upload" else db.get(DatasetVersion, upload.dataset_version_id)
            row.retention_until = deadline

    c.on_native = retain
    with pytest.raises(UploadContractError):
        c.cancel()
    with c.factory() as db:
        record, _ = retirement._journal(db, db.get(DatasetUploadSession, c.upload_id))
        assert record["phase"] == "observed"
    c.on_native = None
    c.at(deadline + timedelta(seconds=1))
    assert c.cancel().status == "aborted" and c.effects == ["delete"]
