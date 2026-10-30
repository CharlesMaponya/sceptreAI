"""Pause only the owned retirement process and its named test backend."""

import json
import os
import sys
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import buffered_retirement as service
from automl_api.services import buffered_uploads
from fault_worker_environment import database_url
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker
from test_buffered_execution import Native

input_id, key, directory, mode = sys.argv[1:]
input_id = uuid.UUID(input_id)
root = Path(directory).resolve()
assert root.is_dir() and mode in {
    "before-delete",
    "after-delete",
    "after-fact",
    "backend-fact",
    "backend-publication",
}
engine = create_engine(
    database_url(),
    pool_size=4,
    max_overflow=0,
    connect_args={"application_name": f"phase1_buffered_retire_fault_{os.getpid()}"},
)
factory = sessionmaker(engine, expire_on_commit=False)
settings = replace(
    buffered_uploads.get_settings(),
    object_store_type="s3_compatible",
    object_store_endpoint="https://objects.test",
    object_store_access_key="test",
    object_store_secret_key="test",
)
with factory() as db:
    original = db.get(WorkflowCommand, input_id)
    row = db.get(BufferedUploadInput, input_id)
    project_id, actor_id, token_version = (
        original.project_id,
        original.actor_id,
        row.manifest["token_version"],
    )
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
context = SimpleNamespace(engine=engine, factory=factory, settings=settings, identifier=input_id)
native = Native(context, root / "native")


def pause():
    pending = root / "retirement-checkpoint.tmp"
    pending.write_text(
        json.dumps(
            dict(
                phase=mode,
                pid=os.getpid(),
                backend_pid=backend_pid,
                input_id=str(input_id),
                project_id=str(project_id),
                sql_checkouts=engine.pool.checkedout(),
            )
        )
        + "\n"
    )
    pending.replace(root / "retirement-checkpoint.json")
    assert os.read(sys.stdin.fileno(), 1) == b"c"


def erase(uri, *, version=None):
    native._free()
    assert version is not None
    if mode == "before-delete":
        pause()
    path = native.root / version
    if path.exists():
        path.unlink()
        marker = root / "retirement-accepted.json"
        assert not marker.exists()
        marker.write_text(json.dumps(dict(uri=uri, version=version, deletes=1)) + "\n")
    if mode == "after-delete":
        pause()


native.delete = erase
service.get_settings = lambda: settings
service.get_object_store = lambda *_: native
if mode == "after-fact":
    actual = service._fact

    def fact(*args):
        actual(*args)
        pause()

    service._fact = fact
elif mode.startswith("backend-"):

    @event.listens_for(Session, "before_commit")
    def interrupt_commit(db):
        global backend_pid
        owners = [
            value
            for value in db.identity_map.values()
            if isinstance(value, WorkflowCommand) and value.operation == service.PROTOCOL
        ]
        for owner in owners:
            journal = owner.response_payload
            matched = (
                bool(journal.get("published"))
                if mode == "backend-publication"
                else not journal.get("published")
                and any("outcome" in u for u in journal.get("attempts", []))
            )
            if matched:
                db.flush()
                backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
                pause()
                break


service.retire_input(factory, actor_id, token_version, project_id, input_id, key, settings=settings)
