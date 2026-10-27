"""Native cancellation is a separate durable effect after logical expiry."""

from datetime import datetime, timedelta

import pytest
from automl_api import workflow_reconciler as worker
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.iam import User
from automl_api.services import upload_admission as admission
from automl_api.services.upload_admission_expiry import expire_pending_admissions
from fastapi import HTTPException
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_workflow import admission_case as admission_case


def _retired_known(c):
    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).is_active = False

    c.callback = revoke
    with pytest.raises(HTTPException):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[admission.RECORD]["phase"] == "recorded"
        now = row.expires_at + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now) == 1
    return now


def test_actual_worker_cancels_retired_known_original_session(admission_case, monkeypatch):
    c = admission_case
    now = _retired_known(c)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(worker, "datetime", Clock)
    original = c.store.abort_upload
    calls = []

    def abort(native):
        assert c.engine.pool.checkedout() == 0
        calls.append(native)
        original(native)

    monkeypatch.setattr(c.store, "abort_upload", abort)
    assert worker.cleanup_uploads_once(c.factory) == 1
    assert c.store.aborted == len(calls) == 1
    assert calls[0] == c.native_replies[0]
    assert worker.cleanup_uploads_once(c.factory) == 0
    assert c.store.aborted == 1


def _command(db, c):
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.upload_admission_cleanup import OPERATION
    from sqlalchemy import select

    return db.scalar(
        select(WorkflowCommand).where(
            WorkflowCommand.resource_id == c.upload_id, WorkflowCommand.operation == OPERATION
        )
    )


def test_cleanup_commits_intent_before_constructor_and_abort_and_preserves_retirement(
    admission_case, monkeypatch
):
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.services.workflow_state import canonical_request_hash as digest

    c = admission_case
    now = _retired_known(c)
    with c.factory() as db:
        retirement = db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD][
            "retirement"
        ]
    calls = []

    def inspect():
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db:
            row = db.get(DatasetUploadSession, c.upload_id)
            command = _command(db, c)
            state = row.provider_state[cleanup.RECORD]
            assert command.status.value == "running" and state["phase"] == "issued"
            assert command.response_payload["cleanup_intent_digest"] == digest(state["intent"])
            assert command.response_payload["cleanup_attempt_digest"] == digest(state["attempt"])
            assert row.lease_expires_at > now
        calls.append(True)

    c.on_construction = inspect
    original = c.store.abort_upload
    monkeypatch.setattr(c.store, "abort_upload", lambda native: inspect() or original(native))
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert len(calls) == 2 and c.store.aborted == 1
    assert not cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.store.aborted == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "expired" and row.lease_expires_at is None
        assert row.provider_state[admission.RECORD]["retirement"] == retirement
        assert retirement["native_absence_confirmed"] is False
        assert _command(db, c).status.value == "succeeded"


@pytest.mark.parametrize("change", ["source", "hold", "retention", "project"])
def test_known_abort_reply_survives_changed_source_or_policy(admission_case, monkeypatch, change):
    from automl_api.models.projects import Project
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    now = _retired_known(c)
    with c.factory() as db:
        filename = db.get(DatasetUploadSession, c.upload_id).original_filename
    original = c.store.abort_upload

    def mutate(native):
        assert c.engine.pool.checkedout() == 0
        original(native)
        with c.factory() as db, db.begin():
            row = db.get(DatasetUploadSession, c.upload_id)
            if change == "source":
                row.original_filename = "changed.csv"
            elif change == "hold":
                row.legal_hold = True
            elif change == "retention":
                row.retention_until = now + timedelta(days=1)
            else:
                db.get(Project, c.project_id).settings = {"deletion_in_progress": True}

    monkeypatch.setattr(c.store, "abort_upload", mutate)
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db:
        state = db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
        assert state["phase"] == "recorded" and state["receipt"]["cancelled"] is True
        assert _command(db, c).status.value == "running"
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        row.original_filename, row.legal_hold, row.retention_until = filename, False, None
        db.get(Project, c.project_id).settings = {}
    constructions = c.constructions
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.store.aborted == 1 and c.constructions == constructions


def test_publication_failure_replays_known_abort_without_native_work(admission_case, monkeypatch):
    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    now = _retired_known(c)
    publish = cleanup._publish

    def fail(*_):
        raise OSError("publication connection lost")

    monkeypatch.setattr(cleanup, "_publish", fail)
    with pytest.raises(OSError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db:
        assert (
            db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]["phase"]
            == "recorded"
        )
    monkeypatch.setattr(cleanup, "_publish", publish)
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.store.aborted == 1


@pytest.mark.parametrize("field", ["hold", "retention", "source", "receipt", "superseded", "store"])
def test_cleanup_fences_uncertain_history_before_constructor(admission_case, field):
    import copy
    from dataclasses import replace

    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    now = _retired_known(c)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(row.provider_state)
        if field == "hold":
            row.legal_hold = True
        elif field == "retention":
            row.retention_until = now + timedelta(days=1)
        elif field == "source":
            row.original_filename = "foreign.csv"
        elif field == "receipt":
            state[admission.RECORD]["receipt"]["provider_id"] = "foreign"
            row.provider_state = state
        elif field == "superseded":
            import uuid

            from automl_api.models.workflows import WorkflowCommand

            command = db.get(
                WorkflowCommand, uuid.UUID(state[admission.RECORD]["intent"]["command_id"])
            )
            command.response_payload = {
                **command.response_payload,
                "superseded_admission_receipts": {"other": state[admission.RECORD]["receipt"]},
            }
    if field == "store":
        c.settings = replace(c.settings, object_store_bucket="foreign")
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(
            c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
        )
    assert c.constructions == 1 and c.store.aborted == 0


@pytest.mark.parametrize(
    "field", ["missing", "target", "attempt", "request", "anchor", "receipt", "response"]
)
def test_corrupt_cleanup_custody_cannot_restart_native_work(admission_case, monkeypatch, field):
    import copy

    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    now = _retired_known(c)
    publish = cleanup._publish
    monkeypatch.setattr(
        cleanup, "_publish", lambda *_: (_ for _ in ()).throw(OSError("commit failed"))
    )
    with pytest.raises(OSError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    monkeypatch.setattr(cleanup, "_publish", publish)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        value = copy.deepcopy(row.provider_state)
        command = _command(db, c)
        if field == "missing":
            value.pop(cleanup.RECORD)
        elif field == "target":
            value[cleanup.RECORD]["intent"]["target"]["provider_id"] = "foreign"
        elif field == "attempt":
            value[cleanup.RECORD]["attempt"]["id"] = "foreign"
        elif field == "request":
            command.request_payload = {**command.request_payload, "session_id": "foreign"}
        elif field == "anchor":
            command.response_payload = {
                **command.response_payload,
                "cleanup_receipt_digest": "foreign",
            }
        elif field == "receipt":
            value[cleanup.RECORD]["receipt"]["cancelled"] = False
        else:
            command.response_status = 200
        row.provider_state = value
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(
            c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=6)
        )
    assert c.store.aborted == 1


def test_native_errors_have_one_live_owner_and_bounded_retry_budget(admission_case, monkeypatch):
    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    now = _retired_known(c)
    calls = []

    def fail(_native):
        assert c.engine.pool.checkedout() == 0
        calls.append(True)
        raise OSError("provider outcome unknown")

    monkeypatch.setattr(c.store, "abort_upload", fail)
    for index in range(5):
        moment = now + timedelta(minutes=index * 6)
        with pytest.raises(OSError):
            cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=moment)
        assert not cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=moment)
    assert not cleanup.cleanup_admission(
        c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=30)
    )
    assert len(calls) == 5
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[cleanup.RECORD]["phase"] == "exhausted"
        assert _command(db, c).status.value == "failed"
        assert "controlled recovery" in row.terminal_reason


def test_competing_cleanup_owners_issue_one_native_abort(admission_case, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    now = _retired_known(c)
    original = c.store.abort_upload

    def compete(native):
        assert c.engine.pool.checkedout() == 0
        with ThreadPoolExecutor(max_workers=1) as executor:
            second = executor.submit(
                cleanup.cleanup_admission, c.factory, c.project_id, c.upload_id, now=now
            )
            assert second.result(timeout=10) is False
        original(native)

    monkeypatch.setattr(c.store, "abort_upload", compete)
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.store.aborted == 1


def test_constructor_policy_change_is_rechecked_before_abort(admission_case):
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    now = _retired_known(c)

    def hold():
        with c.factory() as db, db.begin():
            db.get(DatasetUploadSession, c.upload_id).legal_hold = True

    c.on_construction = hold
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.store.aborted == 0
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[cleanup.RECORD]["phase"] == "unknown"


def test_corrupt_first_cleanup_does_not_starve_next_known_session(admission_case):
    import uuid

    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    first_now = _retired_known(c)
    first = c.upload_id
    with c.factory() as db, db.begin():
        db.get(User, c.actor_id).is_active = True
        db.get(DatasetUploadSession, first).original_filename = "changed.csv"
    c.key = uuid.uuid4().hex
    c.upload_id = uuid.uuid5(c.project_id, f"dataset.upload.begin:{c.actor_id}:{c.key}")
    second_now = _retired_known(c)
    now = max(first_now, second_now)
    assert cleanup.cleanup_pending_admissions(c.factory, now=now, limit=1) == 0
    assert cleanup.cleanup_pending_admissions(c.factory, now=now, limit=1) == 1
    assert c.store.aborted == 1
    with c.factory() as db:
        assert db.get(DatasetUploadSession, first).lease_expires_at > now
        assert cleanup.RECORD not in db.get(DatasetUploadSession, first).provider_state
        assert (
            db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]["phase"]
            == "published"
        )


def test_unknown_creation_remains_fenced_without_a_cancellation_identity(admission_case):
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    c.native_error = OSError("opaque creation response lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        now = db.get(DatasetUploadSession, c.upload_id).expires_at + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now) == 1
    assert cleanup.cleanup_pending_admissions(c.factory, now=now) == 0
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.constructions == 1 and c.store.aborted == 0


@pytest.mark.parametrize("fault", [None, "disconnect", "surviving-empty"])
def test_real_sdk_creation_and_durable_cleanup_recover_original_session(
    admission_case, monkeypatch, fault
):
    import socket
    from dataclasses import replace
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit

    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import DeletionPending
    from automl_api.storage.s3 import S3ObjectStoreDriver
    from botocore.exceptions import BotoCoreError

    c = admission_case
    for name in list(__import__("os").environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    events = []
    state = dict(active=False, fault=fault)

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, body=b""):
            assert c.engine.pool.checkedout() == 0
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            if "uploadId" not in query:
                self.respond(
                    200,
                    b"<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>",
                )
                return
            events.append(("read", urlsplit(self.path).path, query["uploadId"]))
            if state["active"]:
                self.respond(
                    200, b"<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"
                )
            else:
                self.respond(404, b"<Error><Code>NoSuchUpload</Code></Error>")

        def do_POST(self):
            events.append(("create", urlsplit(self.path).path, ["original"]))
            state["active"] = True
            self.respond(
                200,
                b"<InitiateMultipartUploadResult><UploadId>original</UploadId></InitiateMultipartUploadResult>",
            )

        def do_DELETE(self):
            assert c.engine.pool.checkedout() == 0
            events.append(
                ("abort", urlsplit(self.path).path, parse_qs(urlsplit(self.path).query)["uploadId"])
            )
            state["active"] = state["fault"] == "surviving-empty"
            if state["fault"] == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            else:
                self.respond(204)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    native = S3ObjectStoreDriver(
        bucket="bucket",
        region="us-east-1",
        endpoint_url=endpoint,
        access_key="unit",
        secret_key="unit",
    )
    c.settings = replace(
        c.settings,
        object_store_type="aws_s3",
        object_store_endpoint=endpoint,
        object_store_bucket="bucket",
        object_store_region="us-east-1",
    )
    c.payload = c.payload.model_copy(update={"data_region": "us-east-1"})
    c.store = native
    begin = native.begin_upload

    def create(request):
        value = begin(request)
        c.callback()
        return value

    monkeypatch.setattr(native, "begin_upload", create)
    try:
        now = _retired_known(c)
        if fault:
            with pytest.raises(BotoCoreError if fault == "disconnect" else DeletionPending):
                cleanup.cleanup_admission(
                    c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
                )
            with c.factory() as db:
                value = db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
                assert value["phase"] == "unknown" and "receipt" not in value
            assert not cleanup.cleanup_admission(
                c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
            )
            state["fault"] = None
            now += timedelta(minutes=6)
        assert cleanup.cleanup_admission(
            c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
        )
        with c.factory() as db:
            row = db.get(DatasetUploadSession, c.upload_id)
            key = row.object_key
            assert (
                row.provider_state[admission.RECORD]["retirement"]["native_absence_confirmed"]
                is False
            )
            assert row.provider_state[cleanup.RECORD]["phase"] == "published"
        assert not state["active"]
        assert sum(kind == "create" for kind, _, _ in events) == 1
        assert sum(kind == "abort" for kind, _, _ in events) == (
            2 if fault == "surviving-empty" else 1
        )
        assert all(
            path == f"/bucket/{key}" and identity == ["original"] for _, path, identity in events
        )
    finally:
        native.client.close()
        native.presign_client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("status", [499, 404])
def test_gcs_original_uri_cancellation_requires_actual_http_acknowledgement(
    admission_case, monkeypatch, status
):
    from dataclasses import replace
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    import httpx
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadCapabilities, UploadContractError
    from automl_api.storage.gcs import GCSObjectStoreDriver

    c = admission_case
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_DELETE(self):
            assert c.engine.pool.checkedout() == 0
            paths.append(self.path)
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    uri = f"http://127.0.0.1:{server.server_port}/opaque/original?session=original"
    c.settings = replace(c.settings, object_store_type="gcs", object_store_bucket="bucket")
    c.store.driver_name = "gcs"
    c.store.capabilities = lambda: UploadCapabilities(
        "resumable_offset", 1, False, False, True, True
    )
    c.store.uri_for_key = lambda key: f"gs://bucket/{key}"
    c.receipt_patch = {"provider_id": uri}
    original = c.store.abort_upload
    native = object.__new__(GCSObjectStoreDriver)
    try:
        with httpx.Client(trust_env=False) as native.http:

            def abort(target):
                assert target.provider_id == uri
                native.abort_upload(target)
                original(target)

            monkeypatch.setattr(c.store, "abort_upload", abort)
            now = _retired_known(c)
            if status == 499:
                assert cleanup.cleanup_admission(
                    c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
                )
            else:
                with pytest.raises(UploadContractError):
                    cleanup.cleanup_admission(
                        c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
                    )
                assert not cleanup.cleanup_admission(
                    c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
                )
            assert paths == ["/opaque/original?session=original"]
            assert c.store.aborted == int(status == 499)
            with c.factory() as db:
                value = db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
                assert value["phase"] == ("published" if status == 499 else "unknown")
                assert ("receipt" in value) == (status == 499)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_two_background_workers_count_one_original_cleanup(admission_case, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, local

    from automl_api.services import upload_admission_cleanup as cleanup
    from sqlalchemy import event

    c = admission_case
    now = _retired_known(c)
    barrier = Barrier(2)
    original = cleanup.cleanup_admission
    owner = local()

    def checkout(*_):
        owner.connections = getattr(owner, "connections", 0) + 1

    def checkin(*_):
        owner.connections -= 1

    def native_store(_settings):
        assert getattr(owner, "connections", 0) == 0
        c.constructions += 1
        return c.store

    def claimed(*args, **kwargs):
        barrier.wait(timeout=10)
        return original(*args, **kwargs)

    monkeypatch.setattr(cleanup, "cleanup_admission", claimed)
    monkeypatch.setattr(admission, "get_object_store", native_store)
    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            tasks = [
                executor.submit(cleanup.cleanup_pending_admissions, c.factory, now=now)
                for _ in range(2)
            ]
            assert sorted(task.result(timeout=20) for task in tasks) == [0, 1]
        assert c.store.aborted == 1 and c.constructions == 2
    finally:
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)


def test_provider_cannot_inject_cleanup_state_into_admission_receipt(admission_case):
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    c.receipt_patch = {"state": {cleanup.RECORD: {"phase": "published"}}}
    with pytest.raises(UploadContractError):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert cleanup.RECORD not in row.provider_state
        assert row.status == "initiated"


def test_unexpected_cleanup_history_prevents_logical_expiry(admission_case):
    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    c.native_error = OSError("creation reply lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        now = row.expires_at + timedelta(seconds=1)
        row.provider_state = {**row.provider_state, cleanup.RECORD: {"phase": "published"}}
    assert expire_pending_admissions(c.factory, now=now) == 0
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "initiated"


def test_late_cleanup_reply_is_retained_without_replacing_new_owner(admission_case, monkeypatch):
    from automl_api.services import upload_admission_cleanup as cleanup

    c = admission_case
    now = _retired_known(c)
    original = c.store.abort_upload
    calls = []

    def late(target):
        calls.append(target)
        if len(calls) == 1:
            assert cleanup.cleanup_admission(
                c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=6)
            )
        original(target)

    monkeypatch.setattr(c.store, "abort_upload", late)
    assert not cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert len(calls) == 2 and calls[0] == calls[1]
    with c.factory() as db:
        value = db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
        command = _command(db, c)
        assert value["phase"] == "published" and value["attempt"]["number"] == 2
        superseded = command.response_payload["superseded_cleanup_receipts"]
        assert len(superseded) == 1 and value["attempt"]["id"] not in superseded
        assert command.status.value == "succeeded"


def test_prior_unknown_creation_attempt_blocks_known_reply_cancellation(
    admission_case, monkeypatch
):
    from automl_api.services import upload_admission_cleanup as cleanup
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    c.native_error = OSError("first native creation reply lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        moment = datetime.fromisoformat(
            row.provider_state[admission.RECORD]["attempt"]["expires_at"]
        ) + timedelta(seconds=1)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(admission, "datetime", Clock)

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).is_active = False

    c.native_error, c.callback = None, revoke
    with pytest.raises(HTTPException):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[admission.RECORD]["attempt"]["number"] == 2
        now = row.expires_at + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now) == 1
    with pytest.raises(UploadContractError, match="controlled recovery"):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.constructions == len(c.calls) == 2 and c.store.aborted == 0
