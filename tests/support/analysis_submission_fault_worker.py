"""Owned fault control using the parent dedicated test database."""

import copy
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from automl_api.models.enums import OutboxStatus
from automl_api.models.workflows import OutboxEntry, WorkflowEvent
from automl_api.services import analysis_submission as service
from fault_worker_environment import database_url
from kubernetes import client
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

entry_id, project_id, directory, mode = sys.argv[1:]
root = Path(directory).resolve()
assert root.is_dir()
marker = root / "checkpoint.json"
url = database_url()
engine = create_engine(
    url,
    pool_size=1,
    max_overflow=0,
    connect_args={"application_name": f"phase1_analysis_submit_fault_{os.getpid()}"},
)
factory = sessionmaker(engine, expire_on_commit=False)
with engine.connect() as db:
    backend_pid = db.scalar(text("SELECT pg_backend_pid()"))
with factory() as db:
    entry = db.get(OutboxEntry, entry_id)
    assert str(entry.project_id) == project_id and entry.topic == service.TOPIC
    assert entry.lease_owner == "owned-analysis-process"


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


class Native:
    settings = SimpleNamespace(
        training_namespace=entry.payload["manifest"]["metadata"]["namespace"],
        max_concurrent_jobs=1000,
    )

    def __init__(self):
        assert engine.pool.checkedout() == 0

    def capacity_snapshot(self):
        assert engine.pool.checkedout() == 0
        return SimpleNamespace(capacity=SimpleNamespace(connected=True, active_training_jobs=0))

    custom = SimpleNamespace(
        api_client=SimpleNamespace(
            configuration=SimpleNamespace(
                host="https://owned-analysis-submission.invalid",
                verify_ssl=True,
                retries=0,
                ssl_ca_cert=None,
            )
        )
    )

    def ensure_service_account(self, name, namespace=None):
        assert engine.pool.checkedout() == 0
        if mode == "before_sa":
            pause("before_service_account")
        (root / "service-account.json").write_text(json.dumps(dict(name=name, namespace=namespace)))
        if mode == "after_sa":
            pause("after_service_account")

    def create_job(self, manifest):
        assert engine.pool.checkedout() == 0
        assert not (root / "native.json").exists()
        if mode == "before_create":
            pause("before_native_create")
        created = copy.deepcopy(manifest)
        created["metadata"].update(uid="owned-original-uid", resourceVersion="1")
        (root / "native.json").write_text(json.dumps(dict(body=created, creates=1, activations=0)))
        if mode == "after_create":
            pause("after_native_create")
        return client.ApiClient()._ApiClient__deserialize(created, "V1Job")

    def activate_job(self, original):
        assert engine.pool.checkedout() == 0
        state = json.loads((root / "native.json").read_text())
        assert state["body"]["metadata"]["uid"] == original["metadata"]["uid"]
        assert state["body"]["spec"] == original["spec"]
        if mode == "before_activate":
            pause("before_native_activation")
        state["body"]["spec"]["suspend"] = False
        state["activations"] += 1
        state["body"]["metadata"]["resourceVersion"] = "2"
        (root / "native.json").write_text(json.dumps(state))
        if mode == "after_activate":
            pause("after_native_activation")
        return state["body"]


original_fact = service._fact


def fact(*args, **kwargs):
    original_fact(*args, **kwargs)
    key = args[3]
    if mode == "after_create_fact" and key == service.CREATE_RESULT:
        pause("committed_create_fact")
    if mode == "after_activation_fact" and key.startswith(service.ACT_RESULT):
        pause("committed_activation_fact")


service._fact = fact

if mode.startswith("database_"):

    @event.listens_for(Session, "after_flush")
    def flushed(db, _):
        for row in db.new:
            if isinstance(row, WorkflowEvent) and (
                (mode == "database_fact" and row.event_key == service.CREATE_RESULT)
                or (mode == "database_publication" and row.event_key == "analysis.submitted")
                or (
                    mode == "database_activation_fact"
                    and row.event_key.startswith(service.ACT_RESULT)
                )
            ):
                pause("uncommitted_" + row.event_key)
        if mode == "database_ack" and any(
            isinstance(row, OutboxEntry) and row.status == OutboxStatus.DELIVERED
            for row in db.dirty
        ):
            pause("uncommitted_delivery_ack")


service.reconcile_analysis_submission(
    factory, entry_id, "owned-analysis-process", k8s_factory=Native
)
engine.dispose()
