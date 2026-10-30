"""Owned fault control using the parent dedicated test database."""

import json
import os
import sys
from pathlib import Path

from automl_api.models.workflows import DeletionStage, OutboxEntry
from automl_api.services import deletion
from automl_api.services.deletion_workflow import reconcile_deletion
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from fault_worker_environment import database_url
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

entry_id, project_id, directory, mode, checkpoint = sys.argv[1:]
root = Path(directory).resolve()
marker = Path(checkpoint).resolve()
assert root.is_dir() and marker.parent == root
assert (root / "test" / "projects" / project_id / "owned.txt").is_file()
engine = create_engine(
    database_url(),
    pool_size=1,
    max_overflow=0,
    connect_args={"application_name": f"phase1_deletion_fault_{os.getpid()}"},
)
factory = sessionmaker(engine, expire_on_commit=False)
with engine.connect() as db:
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
with factory() as db:
    entry = db.get(OutboxEntry, entry_id)
    assert str(entry.project_id) == project_id and entry.topic == "project.delete"
    worker = entry.lease_owner
    assert worker == "owned-deletion-process"


def pause(phase):
    marker.write_text(
        json.dumps(
            dict(
                pid=os.getpid(),
                backend_pid=backend_pid,
                project_id=project_id,
                entry_id=entry_id,
                mode=mode,
                phase=phase,
                sql_checked_out=engine.pool.checkedout(),
            ),
            indent=2,
        )
        + "\n"
    )
    assert os.read(sys.stdin.fileno(), 1) == b"c"


class Native(EmbeddedObjectStoreDriver):
    def delete_prefix(self, prefix):
        assert engine.pool.checkedout() == 0
        assert prefix == f"projects/{project_id}"
        if mode == "before":
            pause("before_native")
        super().delete_prefix(prefix)
        if mode in {"after", "database_after"}:
            pause("after_native")


if mode == "database_fact":

    @event.listens_for(Session, "after_flush")
    def fact_flush(db, _):
        for row in db.dirty:
            if isinstance(row, DeletionStage) and row.resource_class == "object_store":
                units = row.custody.get("attempts", [])
                if units and (units[-1].get("outcome") or {}).get("absent") is True:
                    pause("uncommitted_native_fact")


store = Native(root=root, bucket="test")
deletion.get_object_store = lambda *_: store
reconcile_deletion(factory, entry_id, worker)
engine.dispose()
