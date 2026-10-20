"""Inline MLflow writer commits its ownership before any external operation."""

import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import mlflow
import pytest
from automl_api.models.enums import AttemptStatus, CommandStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand
from automl_api.services import tracking_reconciliation as recovery
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_tracking import _leave_uncertain_tracking, tracking  # noqa: F401


def command_for(engine, run):
    with Session(engine) as db:
        return db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )


def test_all_inline_remote_operations_release_caller_connections_and_locks(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    seen = []
    for name in (
        "get_experiment_by_name",
        "create_experiment",
        "search_runs",
        "create_run",
        "get_run",
    ):
        original = getattr(client, name)

        def call(*args, _name=name, _original=original, **kwargs):
            assert engine.pool.checkedout() == 0, _name
            command = command_for(engine, run)
            assert command is not None
            with Session(engine) as db, db.begin():
                db.get(ModelRun, run.id, with_for_update=True)
                db.get(WorkflowCommand, command.id, with_for_update=True)
            if _name == "create_run":
                assert command.response_payload == {"creation_started": True}
                assert command.lease_owner.startswith("tracking-writer-")
            seen.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(client, name, call)
    start = mlflow.start_run

    def resume(*args, **kwargs):
        assert engine.pool.checkedout() == 0
        assert command_for(engine, run).status == CommandStatus.SUCCEEDED
        seen.append("resume")
        return start(*args, **kwargs)

    monkeypatch.setattr(mlflow, "start_run", resume)
    for _ in range(2):
        with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
            pass
    assert set(seen) == {
        "get_experiment_by_name",
        "create_experiment",
        "search_runs",
        "create_run",
        "get_run",
        "resume",
    }
    assert seen.count("create_run") == 1 and seen.count("create_experiment") == 1


@pytest.mark.parametrize(
    "mutation", ["cancel", "attempt", "owner", "expiry", "payload", "resource"]
)
def test_writer_fences_new_create_after_read_lookup(tracking, monkeypatch, mutation):  # noqa: F811
    engine, run, attempt_id, client = tracking
    original = client.search_runs

    def lookup(*args, **kwargs):
        matches = original(*args, **kwargs)
        with Session(engine) as db, db.begin():
            command = db.get(WorkflowCommand, command_for(engine, run).id)
            if mutation == "cancel":
                db.get(ModelRun, run.id).status = RunStatus.CANCELLED
            elif mutation == "attempt":
                db.get(WorkflowAttempt, attempt_id).status = AttemptStatus.SUPERSEDED
            elif mutation == "owner":
                command.lease_owner = "different-writer"
            elif mutation == "expiry":
                command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif mutation == "payload":
                command.request_payload = {**command.request_payload, "role": "changed"}
            else:
                command.resource_type = "project"
        return matches

    monkeypatch.setattr(client, "search_runs", lookup)
    create = MagicMock(side_effect=AssertionError("stale owner must not create"))
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()


def test_cancel_after_remote_creation_cannot_publish_or_resume(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    create = client.create_run

    def created(*args, **kwargs):
        remote = create(*args, **kwargs)
        with Session(engine) as db, db.begin():
            db.get(ModelRun, run.id).status = RunStatus.CANCELLED
        return remote

    monkeypatch.setattr(client, "create_run", created)
    resume = MagicMock(side_effect=AssertionError("cancelled work cannot resume"))
    monkeypatch.setattr(mlflow, "start_run", resume)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    command = command_for(engine, run)
    assert command.response_payload == {"creation_started": True} and command.lease_owner is None
    resume.assert_not_called()


@pytest.mark.parametrize(
    "stored", [{"creation_started": False}, {"creation_started": 1}, {"run_id": ""}, [], None]
)
def test_ambiguous_stored_creation_state_never_reissues_post(tracking, monkeypatch, stored):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch, created=False)
    with Session(engine) as db, db.begin():
        command = command_for(engine, run)
        db.execute(
            update(WorkflowCommand)
            .where(WorkflowCommand.id == command.id)
            .values(response_payload=stored)
        )
    read = MagicMock(side_effect=AssertionError("invalid state must fail before remote I/O"))
    monkeypatch.setattr(client, "get_experiment_by_name", read)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    client.create_run.assert_not_called()
    read.assert_not_called()


def test_unknown_run_does_not_create_missing_experiment(tracking, monkeypatch):  # noqa: F811
    _, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch, created=False)
    monkeypatch.setattr(client, "get_experiment_by_name", lambda *_: None)
    create = MagicMock(side_effect=AssertionError("uncertain work must not recreate experiment"))
    monkeypatch.setattr(client, "create_experiment", create)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()
    client.create_run.assert_not_called()


def test_writer_heartbeat_renews_while_remote_lookup_waits(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    monkeypatch.setattr(recovery, "HEARTBEAT_SECONDS", 0.01)
    original = client.search_runs

    def lookup(*args, **kwargs):
        first = command_for(engine, run).heartbeat_at
        deadline = time.monotonic() + 5
        while command_for(engine, run).heartbeat_at <= first:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
        pass
    assert command_for(engine, run).lease_owner is None


def test_live_writer_cannot_be_preempted_by_another_writer(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    search = client.search_runs

    def lookup(*args, **kwargs):
        owner = command_for(engine, run).lease_owner
        with pytest.raises(pipeline.TrackingCreationUnresolved, match="another writer"):
            pipeline._tracked_mlflow_run(run, role="parent", run_name="competing")
        assert command_for(engine, run).lease_owner == owner
        return search(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    create = MagicMock(wraps=client.create_run)
    monkeypatch.setattr(client, "create_run", create)
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
        pass
    assert create.call_count == 1


def test_lost_initial_lease_commit_ack_releases_only_own_claim(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    lost = []

    class LostCommit(Session):
        def commit(self):
            initial = any(
                isinstance(row, WorkflowCommand)
                and row.lease_owner
                and row.lease_owner.startswith("tracking-writer-")
                and row.response_payload == {}
                for row in self.identity_map.values()
            )
            super().commit()
            if initial and not lost:
                lost.append(True)
                raise ConnectionError("initial tracking acknowledgement lost")

    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: LostCommit(engine))
    create = MagicMock(wraps=client.create_run)
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(ConnectionError, match="initial tracking"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    assert command_for(engine, run).lease_owner is None
    create.assert_not_called()
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
        pass
    assert create.call_count == 1


def test_lost_creation_marker_ack_never_sends_or_retries_a_post(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    lost = []

    class LostCommit(Session):
        def commit(self):
            marker = any(
                isinstance(row, WorkflowCommand)
                and row.response_payload == {"creation_started": True}
                for row in self.identity_map.values()
            )
            super().commit()
            if marker and not lost:
                lost.append(True)
                raise ConnectionError("creation marker acknowledgement lost")

    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: LostCommit(engine))
    create = MagicMock(wraps=client.create_run)
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(ConnectionError, match="creation marker"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    assert command_for(engine, run).response_payload == {"creation_started": True}
    for _ in range(2):
        with pytest.raises(pipeline.TrackingCreationUnresolved):
            pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()


def test_cached_remote_read_cannot_resume_after_cancellation(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
        pass
    get_run = client.get_run

    def read(*args, **kwargs):
        result = get_run(*args, **kwargs)
        with Session(engine) as db, db.begin():
            db.get(ModelRun, run.id).status = RunStatus.CANCELLED
        return result

    monkeypatch.setattr(client, "get_run", read)
    resume = MagicMock(side_effect=AssertionError("cancelled cached replay cannot resume"))
    monkeypatch.setattr(mlflow, "start_run", resume)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    resume.assert_not_called()


def test_cancelled_lookup_cannot_create_an_experiment(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking

    def read(_name):
        with Session(engine) as db, db.begin():
            db.get(ModelRun, run.id).status = RunStatus.CANCELLED
        return None

    monkeypatch.setattr(client, "get_experiment_by_name", read)
    create = MagicMock(side_effect=AssertionError("cancelled work cannot create experiment"))
    monkeypatch.setattr(client, "create_experiment", create)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()


def test_writer_rejects_backend_different_from_committed_uri(tracking, monkeypatch):  # noqa: F811
    _, run, _, _ = tracking
    client = MagicMock(tracking_uri="different-backend")
    monkeypatch.setattr(pipeline, "MlflowClient", lambda: client)
    with pytest.raises(ValueError, match="backend differs"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    client.get_experiment_by_name.assert_not_called()
    client.create_run.assert_not_called()


@pytest.mark.parametrize("stage", ["lookup", "before_post", "after_post"])
def test_writer_process_death_preserves_creation_knowledge(tracking, monkeypatch, stage):  # noqa: F811
    import json
    import signal
    import subprocess
    import sys
    from pathlib import Path

    engine, run, _, client = tracking
    script = """
import json,os,signal,uuid
from datetime import UTC,datetime
from pathlib import Path
from automl_api.db.base import Base
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.training import pipeline
from sqlalchemy import create_engine,select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
import mlflow
from mlflow import MlflowClient
assert make_url(os.environ['DATABASE_URL']).database.endswith('_tests')
engine=create_engine(os.environ['DATABASE_URL'])
pipeline.get_session_factory=lambda:lambda:Session(engine)
mlflow.set_tracking_uri(TRACKING_URI)
client=MlflowClient(tracking_uri=TRACKING_URI)
create_experiment=client.create_experiment
client.create_experiment=lambda name:create_experiment(name,artifact_location=ARTIFACT_URI)
def die():
 with Session(engine) as db:
  command=db.scalar(select(WorkflowCommand).where(WorkflowCommand.resource_id==uuid.UUID(RUN_ID)))
  print('WRITER_DEATH='+json.dumps({'owner':command.lease_owner,'response':command.response_payload}),flush=True)
 os.kill(os.getpid(),signal.SIGKILL)
if STAGE=='lookup':client.get_experiment_by_name=lambda name:die()
else:
 create_run=client.create_run
 def request(*args,**kwargs):
  if STAGE=='after_post':create_run(*args,**kwargs)
  die()
 client.create_run=request
pipeline.MlflowClient=lambda:client
with Session(engine) as db:run=db.get(ModelRun,uuid.UUID(RUN_ID))
pipeline._tracked_mlflow_run(run,role='parent',run_name='parent')
"""
    artifact_uri = (
        Path(client.tracking_uri.removeprefix("sqlite:///")).parent / "child-artifacts"
    ).as_uri()
    header = (
        "RUN_ID="
        + repr(str(run.id))
        + "\nTRACKING_URI="
        + repr(client.tracking_uri)
        + "\nARTIFACT_URI="
        + repr(artifact_uri)
        + "\nSTAGE="
        + repr(stage)
        + "\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", header + script], capture_output=True, text=True, timeout=45
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    reports = [
        json.loads(line.split("=", 1)[1])
        for line in result.stdout.splitlines()
        if line.startswith("WRITER_DEATH=")
    ]
    assert len(reports) == 1
    command = command_for(engine, run)
    assert command.lease_owner == reports[0]["owner"]
    assert command.response_payload == ({} if stage == "lookup" else {"creation_started": True})
    with pytest.raises(pipeline.TrackingCreationUnresolved, match="another writer"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    with Session(engine) as db, db.begin():
        db.get(WorkflowCommand, command.id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    create = MagicMock(wraps=client.create_run)
    monkeypatch.setattr(client, "create_run", create)
    if stage == "before_post":
        with pytest.raises(pipeline.TrackingCreationUnresolved):
            pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
        create.assert_not_called()
    else:
        with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
            pass
        assert create.call_count == (1 if stage == "lookup" else 0)
        assert command_for(engine, run).status == CommandStatus.SUCCEEDED
