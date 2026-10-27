"""Actual process and SQL failure boundaries around original native cancellation."""

import copy
import json
import os
import signal
import subprocess
import sys
import uuid
from dataclasses import replace
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_admission as admission
from automl_api.services import upload_admission_cleanup as cleanup
from automl_api.storage.s3 import S3ObjectStoreDriver
from sqlalchemy import select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_cleanup import _command, _retired_known
from test_upload_admission_workflow import admission_case as admission_case


def test_admission_reports_actual_attempt_budget_before_native_creation(admission_case):
    c = admission_case

    def inspect():
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db:
            command = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.resource_id == c.upload_id,
                    WorkflowCommand.operation == admission.OPERATION,
                )
            )
            assert command.retry_count == 1 and command.max_retries == 5

    c.callback = inspect
    assert c.begin().status == "uploading"


def test_cleanup_reports_actual_attempt_budget_before_native_cancellation(admission_case):
    c = admission_case
    now = _retired_known(c)

    def inspect():
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db:
            command = _command(db, c)
            assert command.retry_count == 1 and command.max_retries == 5

    c.on_construction = inspect
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)


CHILD = r"""
import json,os,signal,sys,uuid
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine,event,text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session,sessionmaker
from automl_api.core.config import Settings
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_admission as admission
from automl_api.services import upload_admission_cleanup as cleanup

mode,project,upload,moment,config,witness=sys.argv[1:]
project,upload=uuid.UUID(project),uuid.UUID(upload)
moment=datetime.fromisoformat(moment)
settings=Settings(**json.loads(config))
url=os.environ['DATABASE_URL']
assert '@127.0.0.1:55439/automl_branch_0e6a232bc7_tests' in url
engine=create_engine(url,pool_size=4,max_overflow=0,pool_pre_ping=True)
factory=sessionmaker(bind=engine,expire_on_commit=False)
details=dict(mode=mode,pid=os.getpid(),native_done=False,backend_killed=False)

def record(phase):
 details['phase']=phase
 Path(witness).write_text(json.dumps(details))

def die(phase):
 record(phase)
 os.kill(os.getpid(),signal.SIGKILL)

original_store=admission.get_object_store
def store(settings):
 assert engine.pool.checkedout()==0
 with factory() as db:
  row=db.get(DatasetUploadSession,upload)
  state=row.provider_state[cleanup.RECORD]
  command=db.get(WorkflowCommand,uuid.UUID(state['intent']['command_id']))
  assert state['phase']=='issued' and command.retry_count==1 and command.max_retries==5
 if mode=='before_native': die('issued')
 native=original_store(settings)
 abort=native.abort_upload
 def cancel(target):
  assert engine.pool.checkedout()==0
  abort(target)
  details['native_done']=True
  if mode=='after_native': die('native_accepted')
 native.abort_upload=cancel
 return native
admission.get_object_store=store

original_publish=cleanup._publish
def publish(*args):
 if mode=='after_record': die('recorded')
 value=original_publish(*args)
 if mode=='after_publish': die('published')
 return value
cleanup._publish=publish

@event.listens_for(Session,'before_commit')
def lose_connection(db):
 wanted='issued' if mode=='intent_commit_loss' else 'recorded'
 if mode not in {'intent_commit_loss','receipt_commit_loss'} or details['backend_killed']:
  return
 for row in db.dirty:
  state=row.provider_state.get(cleanup.RECORD,{}) if isinstance(row,DatasetUploadSession) else {}
  if state.get('phase')==wanted:
   details['backend_killed']=True
   db.flush()
   pid=db.scalar(text('SELECT pg_backend_pid()'))
   details['backend_pid']=pid
   with engine.begin() as control:
    assert control.scalar(text('SELECT pg_backend_pid()'))!=pid
    assert control.scalar(text('SELECT pg_terminate_backend(:pid)'),{'pid':pid})
   record(wanted+'_before_commit')
   return

try:
 cleanup.cleanup_admission(factory,project,upload,settings=settings,now=moment)
except DBAPIError as exc:
 assert mode in {'intent_commit_loss','receipt_commit_loss'} and details['backend_killed']
 details['error_type']=type(exc).__name__
 record(details['phase'])
 sys.exit(42)
finally:
 engine.dispose()
raise AssertionError('The requested real failure window did not interrupt work')
"""


@pytest.fixture
def native_case(admission_case, monkeypatch):
    c = admission_case
    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    state = SimpleNamespace(active=False, events=[])

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
            state.events.append(("read", urlsplit(self.path).path, query["uploadId"]))
            if state.active:
                self.respond(
                    200, b"<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>"
                )
            else:
                self.respond(404, b"<Error><Code>NoSuchUpload</Code></Error>")

        def do_POST(self):
            state.events.append(("create", urlsplit(self.path).path, ["original"]))
            state.active = True
            self.respond(
                200,
                b"<InitiateMultipartUploadResult><UploadId>original</UploadId></InitiateMultipartUploadResult>",
            )

        def do_DELETE(self):
            state.events.append(
                ("abort", urlsplit(self.path).path, parse_qs(urlsplit(self.path).query)["uploadId"])
            )
            state.active = False
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
        compatible=True,
    )
    c.settings = replace(
        c.settings,
        object_store_type="s3_compatible",
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
        yield c, state, _retired_known(c)
    finally:
        native.client.close()
        native.presign_client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    "window",
    [
        "before_native",
        "after_native",
        "after_record",
        "after_publish",
        "intent_commit_loss",
        "receipt_commit_loss",
    ],
)
def test_real_process_and_database_failure_recover_original_native_cleanup(
    native_case, tmp_path, monkeypatch, window
):
    c, native, now = native_case
    witness = tmp_path / "failure.json"
    with c.factory() as db:
        retirement = copy.deepcopy(
            db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD]["retirement"]
        )
    names = (
        "object_store_type",
        "object_store_endpoint",
        "object_store_public_endpoint",
        "object_store_bucket",
        "object_store_region",
        "gcs_project",
        "azure_storage_account",
    )
    config = {name: getattr(c.settings, name) for name in names}
    config.update(object_store_access_key="unit", object_store_secret_key="unit")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD,
            window,
            str(c.project_id),
            str(c.upload_id),
            now.isoformat(),
            json.dumps(config),
            str(witness),
        ],
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == (42 if window.endswith("commit_loss") else -signal.SIGKILL), (
        result.stderr
    )
    observed = json.loads(witness.read_text())
    assert observed["pid"] > 0 and observed["mode"] == window
    assert observed["backend_killed"] == window.endswith("commit_loss")
    if window.endswith("commit_loss"):
        assert observed["backend_pid"] > 0 and observed["error_type"] == "OperationalError"
    assert observed["native_done"] == (window not in {"before_native", "intent_commit_loss"})
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        phase = row.provider_state.get(cleanup.RECORD, {}).get("phase")
        assert phase == (
            {
                "after_record": "recorded",
                "after_publish": "published",
                "intent_commit_loss": None,
            }.get(window, "issued")
        )
        if phase is not None:
            command_id = command.id
            assert command.retry_count == 1 and command.max_retries == 5
        else:
            command_id = None
            assert command is None
        assert row.provider_state[admission.RECORD]["retirement"] == retirement
    calls = len(native.events)
    if window in {"before_native", "after_native", "receipt_commit_loss"}:
        assert not cleanup.cleanup_admission(
            c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
        )
        assert len(native.events) == calls
        now += timedelta(minutes=6)
    if window in {"after_record", "after_publish"}:

        def no_native(_settings):
            raise AssertionError("Known cleanup must not construct another native client")

        monkeypatch.setattr(admission, "get_object_store", no_native)
    assert cleanup.cleanup_admission(
        c.factory, c.project_id, c.upload_id, settings=c.settings, now=now
    ) is (window != "after_publish")
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        assert command_id is None or command.id == command_id
        assert command.status.value == "succeeded" and command.response_status == 200
        assert command.retry_count == (
            2 if window in {"before_native", "after_native", "receipt_commit_loss"} else 1
        )
        assert command.max_retries == 5
        assert row.provider_state[cleanup.RECORD]["phase"] == "published"
        assert row.provider_state[admission.RECORD]["retirement"] == retirement
        key = row.object_key
        original = db.get(
            WorkflowCommand, uuid.UUID(row.provider_state[admission.RECORD]["intent"]["command_id"])
        )
        assert original.status.value == "failed" and original.response_status is None
        assert "result" not in original.response_payload
    assert retirement["native_absence_confirmed"] is False
    assert not native.active
    assert sum(kind == "create" for kind, _, _ in native.events) == 1
    assert sum(kind == "abort" for kind, _, _ in native.events) == 1
    assert all(
        path == f"/bucket/{key}" and identity == ["original"] for _, path, identity in native.events
    )


def test_legacy_known_cleanup_reporting_remains_read_only(admission_case):
    c = admission_case
    now = _retired_known(c)
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db, db.begin():
        command = _command(db, c)
        command.retry_count, command.max_retries = 0, 3
    calls = c.constructions
    assert not cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.constructions == calls and c.store.aborted == 1
    with c.factory() as db:
        command = _command(db, c)
        assert command.retry_count == 0 and command.max_retries == 3


def test_reporting_fields_cannot_expand_original_cleanup_attempt_budget(
    admission_case, monkeypatch
):
    c = admission_case
    now = _retired_known(c)
    calls = []

    def fail(_target):
        calls.append(True)
        raise OSError("Original native cancellation is uncertain")

    monkeypatch.setattr(c.store, "abort_upload", fail)
    for number in range(5):
        if number == 1:
            with c.factory() as db, db.begin():
                command = _command(db, c)
                command.retry_count, command.max_retries = 0, 100
        with pytest.raises(OSError):
            cleanup.cleanup_admission(
                c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=number * 6)
            )
    assert not cleanup.cleanup_admission(
        c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=30)
    )
    assert len(calls) == 5
    with c.factory() as db:
        command = _command(db, c)
        assert command.status.value == "failed"
        assert command.retry_count == command.max_retries == 5
