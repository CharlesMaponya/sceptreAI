"""Owned fault control using the parent dedicated test database."""

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from automl_api.models.enums import OutboxStatus
from automl_api.models.workflows import OutboxEntry, WorkflowEvent
from automl_api.services import ray_cancellation as service
from fault_worker_environment import database_url
from kubernetes.client import ApiException
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

entry_id, project_id, directory, mode = sys.argv[1:]
root = Path(directory).resolve()
assert root.is_dir()
marker = root / "checkpoint.json"
engine = create_engine(
    database_url(),
    pool_size=1,
    max_overflow=0,
    connect_args={"application_name": f"phase1_ray_cancel_fault_{os.getpid()}"},
)
sessions = sessionmaker(engine, expire_on_commit=False)
with engine.connect() as db:
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
with sessions() as db:
    entry = db.get(OutboxEntry, entry_id)
    assert str(entry.project_id) == project_id and entry.topic in service.TOPICS
    assert entry.lease_owner == "owned-ray-cancel-process"


def pause(phase):
    pending = marker.with_suffix(".tmp")
    pending.write_text(
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
    pending.replace(marker)
    assert os.read(sys.stdin.fileno(), 1) == b"c"


fact_flushed = False


@contextmanager
def factory():
    global fact_flushed
    with sessions() as db:
        yield db
    if mode == "after_fact" and fact_flushed:
        fact_flushed = False
        pause("committed_retirement_fact")


@event.listens_for(Session, "after_flush")
def flushed(db, _):
    global fact_flushed
    if any(
        isinstance(row, WorkflowEvent) and row.event_key.startswith(service.RESULT)
        for row in db.new
    ):
        fact_flushed = True
        if mode == "database_fact":
            pause("uncommitted_retirement_fact")
    if mode == "database_publication" and any(
        isinstance(row, OutboxEntry) and row.id == entry.id and row.status == OutboxStatus.DELIVERED
        for row in db.dirty
    ):
        pause("uncommitted_retirement_publication")


class Native:
    custom = SimpleNamespace(
        api_client=SimpleNamespace(
            configuration=SimpleNamespace(
                host="https://owned-factory.invalid", verify_ssl=True, retries=0, ssl_ca_cert=None
            )
        )
    )

    def __init__(self):
        assert engine.pool.checkedout() == 0
        self.core = SimpleNamespace(list_namespaced_pod=self.empty)
        self.batch = SimpleNamespace(list_namespaced_job=self.empty)
        self.custom = SimpleNamespace(
            api_client=self.custom.api_client, list_namespaced_custom_object=self.empty
        )

    def empty(self, **_kwargs):
        assert engine.pool.checkedout() == 0
        return {"items": [], "metadata": {}}

    def ray_job(self, name, *, namespace):
        assert engine.pool.checkedout() == 0
        state = json.loads((root / "native.json").read_text())
        if state["body"] is None:
            raise ApiException(status=404)
        assert state["body"]["metadata"]["name"] == name
        assert state["body"]["metadata"]["namespace"] == namespace
        return state["body"]

    def delete_ray_job(self, name, *, namespace, uid):
        assert engine.pool.checkedout() == 0
        state = json.loads((root / "native.json").read_text())
        assert state["body"]["metadata"]["uid"] == uid
        assert state["body"]["metadata"]["name"] == name
        assert state["body"]["metadata"]["namespace"] == namespace
        if mode == "before_delete":
            pause("before_native_delete")
        state["body"] = None
        state["deletes"] += 1
        (root / "native.json").write_text(json.dumps(state))
        if mode == "after_delete":
            pause("after_native_delete")


service.reconcile_ray_cancellation(
    factory, entry_id, "owned-ray-cancel-process", k8s_factory=Native
)
engine.dispose()
