"""Pause the owned execution process around durable native outcome boundaries."""

import json
import os
import sys
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.services import buffered_execution as service
from automl_api.services import buffered_uploads
from fault_worker_environment import database_url
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker
from test_buffered_execution import Native

entry_id, command_id, directory, mode = sys.argv[1:]
root = Path(directory).resolve()
assert root.is_dir() and mode in {
    "before-write",
    "after-write",
    "after-fact",
    "backend-fact",
    "backend-publication",
}
engine = create_engine(
    database_url(),
    pool_size=4,
    max_overflow=0,
    connect_args={"application_name": f"phase1_buffered_execution_fault_{os.getpid()}"},
)
factory = sessionmaker(engine, expire_on_commit=False)
settings = replace(
    buffered_uploads.get_settings(),
    object_store_type="s3_compatible",
    object_store_endpoint="https://objects.test",
    object_store_access_key="test",
    object_store_secret_key="test",
)
context = SimpleNamespace(
    engine=engine, factory=factory, settings=settings, identifier=uuid.UUID(command_id)
)
native = Native(context, root / "native")
with factory() as db:
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))


def pause(phase):
    temporary = root / "execution-checkpoint.tmp"
    temporary.write_text(
        json.dumps(
            {
                "phase": phase,
                "pid": os.getpid(),
                "backend_pid": backend_pid,
                "command_id": command_id,
                "sql_checkouts": engine.pool.checkedout(),
            }
        )
        + "\n"
    )
    temporary.replace(root / "execution-checkpoint.json")
    assert os.read(sys.stdin.fileno(), 1) == b"c"


if mode == "before-write":
    native.create_hook = lambda: pause(mode)
elif mode == "after-write":
    actual = service._record

    def record(*args):
        pause(mode)
        return actual(*args)

    service._record = record
elif mode == "after-fact":
    actual_verify = service._verify

    def verify(*args):
        pause(mode)
        return actual_verify(*args)

    service._verify = verify
if mode.startswith("backend-"):

    @event.listens_for(Session, "before_commit")
    def interrupt_commit(db):
        global backend_pid
        target_phase = "recorded" if mode == "backend-fact" else "published"
        if any(
            isinstance(row, BufferedUploadInput) and row.phase == target_phase
            for row in db.identity_map.values()
        ):
            db.flush()
            backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
            pause(mode)


service.reconcile_buffered_upload(
    factory,
    uuid.UUID(entry_id),
    "owned-buffered-execution",
    store_factory=lambda _: native,
    settings=settings,
)
