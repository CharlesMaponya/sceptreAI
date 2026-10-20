"""Recovery owns a committed lease while performing read-only MLflow requests."""

import time
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import CommandStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import tracking_reconciliation as recovery
from automl_api.training import pipeline
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_tracking import _leave_uncertain_tracking, tracking  # noqa: F401


def command_for(engine, run):
    with Session(engine) as db:
        return db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )


def test_remote_lookup_releases_connections_and_commits_exclusive_lease(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    remote_id = _leave_uncertain_tracking(tracking, monkeypatch)
    original = client.get_experiment_by_name

    def lookup(name):
        assert engine.pool.checkedout() == 0
        with Session(engine) as db, db.begin():
            command = db.get(WorkflowCommand, command_for(engine, run).id, with_for_update=True)
            db.get(ModelRun, run.id, with_for_update=True)
            assert command.lease_owner.startswith("tracking-")
            assert command.lease_expires_at > datetime.now(UTC)
            assert command.heartbeat_at and command.retry_count == 1
        assert (
            recovery.reconcile_tracking_once(lambda: Session(engine), client=client, limit=1) == 0
        )
        return original(name)

    monkeypatch.setattr(client, "get_experiment_by_name", lookup)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client) == 1
    command = command_for(engine, run)
    assert command.response_payload == {"run_id": remote_id}
    assert command.lease_owner is None and command.lease_expires_at is None
    client.create_run.assert_not_called()


def test_expired_committed_claim_recovers_after_worker_death(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    remote_id = _leave_uncertain_tracking(tracking, monkeypatch)
    now = datetime.now(UTC)
    abandoned = recovery._claim_tracking(lambda: Session(engine), now)
    assert abandoned and command_for(engine, run).lease_owner == abandoned.owner
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client, now=now) == 0
    assert (
        recovery.reconcile_tracking_once(
            lambda: Session(engine),
            client=client,
            now=now + timedelta(seconds=recovery.LEASE_SECONDS + 1),
        )
        == 1
    )
    command = command_for(engine, run)
    assert command.retry_count == 2 and command.response_payload == {"run_id": remote_id}
    assert command.lease_owner is None
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    client.create_run.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        "owner",
        "expiry",
        "payload",
        "hash",
        "scope",
        "resource_type",
        "response",
        "dataset",
        "existing_scope",
    ],
)
def test_late_lookup_cannot_publish_after_ownership_or_scope_change(tracking, monkeypatch, change):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    command_id = command_for(engine, run).id
    original = client.search_runs

    def lookup(*args, **kwargs):
        with Session(engine) as db, db.begin():
            command = db.get(WorkflowCommand, command_id)
            if change == "owner":
                command.lease_owner = "replacement-owner"
            elif change == "expiry":
                command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "payload":
                command.request_payload = {**command.request_payload, "role": "changed-role"}
            elif change == "hash":
                command.request_hash = "0" * 64
            elif change == "scope":
                command.resource_id = uuid.uuid4()
            elif change == "existing_scope":
                other_run = ModelRun(
                    project_id=run.project_id,
                    dataset_version_id=run.dataset_version_id,
                    created_by_id=run.created_by_id,
                )
                db.add(other_run)
                db.flush()
                command.resource_id = other_run.id
            elif change == "resource_type":
                command.resource_type = "project"
            elif change == "response":
                command.response_payload = {"changed": True}
            elif change == "dataset":
                current = db.get(DatasetVersion, run.dataset_version_id)
                other = DatasetVersion(
                    project_id=run.project_id,
                    dataset_id=current.dataset_id,
                    created_by_id=current.created_by_id,
                    version_number=current.version_number + 1,
                    format=current.format,
                    object_uri=current.object_uri,
                    content_hash="b" * 64,
                )
                db.add(other)
                db.flush()
                db.get(ModelRun, run.id).dataset_version_id = other.id
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client, limit=1) == 0
    command = command_for(engine, run)
    assert command.status != CommandStatus.SUCCEEDED
    assert "run_id" not in command.response_payload
    assert command.retry_count == 1
    client.create_run.assert_not_called()


def test_heartbeat_renews_only_the_live_unchanged_claim(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    now = datetime.now(UTC)
    claim = recovery._claim_tracking(lambda: Session(engine), now)
    first = command_for(engine, run)
    assert recovery._renew_tracking(lambda: Session(engine), claim, now + timedelta(seconds=1))
    second = command_for(engine, run)
    assert (
        second.heartbeat_at > first.heartbeat_at
        and second.lease_expires_at > first.lease_expires_at
    )
    with Session(engine) as db, db.begin():
        db.get(WorkflowCommand, claim.command_id).lease_owner = "replacement"
    assert not recovery._renew_tracking(lambda: Session(engine), claim, now + timedelta(seconds=2))
    with Session(engine) as db, db.begin():
        command = db.get(WorkflowCommand, claim.command_id)
        command.lease_owner = claim.owner
        command.lease_expires_at = now - timedelta(seconds=1)
    assert not recovery._renew_tracking(lambda: Session(engine), claim, now)


def test_heartbeat_runs_during_slow_lookup(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    monkeypatch.setattr(recovery, "HEARTBEAT_SECONDS", 0.01)
    original = client.search_runs

    def lookup(*args, **kwargs):
        first = command_for(engine, run).heartbeat_at
        deadline = time.monotonic() + 5
        while command_for(engine, run).heartbeat_at <= first:
            assert time.monotonic() < deadline, "Heartbeat did not advance while lookup was waiting"
            time.sleep(0.01)
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client) == 1
    assert command_for(engine, run).lease_owner is None


def test_parent_publication_wins_over_inflight_recovery(tracking, monkeypatch):  # noqa: F811
    engine, run, _, client = tracking
    remote_id = _leave_uncertain_tracking(tracking, monkeypatch)
    original = client.search_runs
    adopted = []

    def lookup(*args, **kwargs):
        if not adopted:
            adopted.append(True)
            with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as parent:
                assert parent.info.run_id == remote_id
            command = command_for(engine, run)
            assert command.lease_owner is None and command.status == CommandStatus.SUCCEEDED
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client, limit=1) == 0
    command = command_for(engine, run)
    assert command.response_payload == {"run_id": remote_id}
    with Session(engine) as db:
        assert db.get(ModelRun, run.id).mlflow_run_id == remote_id
    client.create_run.assert_not_called()


def add_healthy_recovery(fixture, now):
    import hashlib

    from automl_api.services.workflow_state import begin_command

    engine, run, _, client = fixture
    original_create = type(client).create_run.__get__(client, type(client))
    with Session(engine) as db, db.begin():
        good, _ = begin_command(
            db,
            project_id=run.project_id,
            actor_id=run.created_by_id,
            operation="mlflow.run.ensure",
            idempotency_key="healthy-" + uuid.uuid4().hex,
            payload={
                "model_run_id": str(run.id),
                "attempt_id": "legacy",
                "role": "parent",
                "tracking_backend_hash": hashlib.sha256(client.tracking_uri.encode()).hexdigest(),
            },
        )
        good.resource_type = "model_run"
        good.resource_id = run.id
        good.response_payload = {"creation_started": True}
        good.available_at = now - timedelta(seconds=1)
        experiment = client.get_experiment_by_name(f"automl-project-{run.project_id}")
        remote = original_create(
            experiment.experiment_id, tags=recovery.tracking_identity(good, run)
        )
        good_id = good.id
    return good_id, remote.info.run_id


@pytest.mark.parametrize("malformed", ["model_run", "role", "attempt_id", "non_object"])
def test_malformed_claim_consumes_budget_without_starving_healthy_work(
    tracking,  # noqa: F811
    monkeypatch,
    malformed,
):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    now = datetime.now(UTC)
    command_id = command_for(engine, run).id
    with Session(engine) as db, db.begin():
        command = db.get(WorkflowCommand, command_id)
        command.max_retries = 2
        command.available_at = now - timedelta(seconds=2)
        payload = dict(command.request_payload)
        if malformed == "non_object":
            command.request_payload = ["legacy-invalid"]
        elif malformed == "model_run":
            command.request_payload = {**payload, "model_run_id": str(uuid.uuid4())}
        else:
            command.request_payload = {
                key: value for key, value in payload.items() if key != malformed
            }
    good_id, remote_id = add_healthy_recovery(tracking, now)
    lookup = MagicMock(wraps=client.search_runs)
    monkeypatch.setattr(client, "search_runs", lookup)
    assert (
        recovery.reconcile_tracking_once(lambda: Session(engine), client=client, now=now, limit=2)
        == 1
    )
    with Session(engine) as db:
        bad = db.get(WorkflowCommand, command_id)
        assert bad.retry_count == 1 and bad.terminal_reason and bad.lease_owner is None
        assert db.get(WorkflowCommand, good_id).response_payload == {"run_id": remote_id}
    assert (
        recovery.reconcile_tracking_once(
            lambda: Session(engine), client=client, now=now + timedelta(seconds=61), limit=2
        )
        == 0
    )
    with Session(engine) as db:
        bad = db.get(WorkflowCommand, command_id)
        assert bad.status == CommandStatus.FAILED and bad.retry_count == 2
    assert lookup.call_count == 1
    client.create_run.assert_not_called()


@pytest.mark.parametrize("marker", ["not-a-boolean", 1, {}, [], None])
def test_malformed_creation_marker_cannot_break_queue_query(tracking, monkeypatch, marker):  # noqa: F811
    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    now = datetime.now(UTC)
    bad_id = command_for(engine, run).id
    with Session(engine) as db, db.begin():
        bad = db.get(WorkflowCommand, bad_id)
        from sqlalchemy import update

        db.execute(
            update(WorkflowCommand)
            .where(WorkflowCommand.id == bad_id)
            .values(response_payload={"creation_started": marker})
        )
        bad.available_at = now - timedelta(seconds=2)
    good_id, remote_id = add_healthy_recovery(tracking, now)
    assert (
        recovery.reconcile_tracking_once(lambda: Session(engine), client=client, now=now, limit=1)
        == 1
    )
    with Session(engine) as db:
        assert db.get(WorkflowCommand, bad_id).retry_count == 0
        assert db.get(WorkflowCommand, good_id).response_payload == {"run_id": remote_id}
    client.create_run.assert_not_called()


@pytest.mark.parametrize("budget", [0, 1, 2])
def test_actual_lookup_process_death_leaves_recoverable_committed_lease(
    tracking,  # noqa: F811
    monkeypatch,
    budget,
):
    import json
    import signal
    import subprocess
    import sys

    engine, run, _, client = tracking
    remote_id = _leave_uncertain_tracking(tracking, monkeypatch)
    command_id = command_for(engine, run).id
    with Session(engine) as db, db.begin():
        db.get(WorkflowCommand, command_id).max_retries = budget
    lookup = MagicMock(wraps=client.search_runs)
    monkeypatch.setattr(client, "search_runs", lookup)
    script = """
import json,os,signal
from datetime import UTC,datetime
from automl_api.db.base import Base
from automl_api.services.tracking_reconciliation import _claim_tracking
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
url=os.environ['DATABASE_URL']
assert make_url(url).database.endswith('_tests')
engine=create_engine(url)
claim=_claim_tracking(lambda:Session(engine),datetime.now(UTC))
assert str(claim.command_id)==EXPECTED_ID
print('CHILD_CLAIM='+json.dumps({'id':str(claim.command_id),'owner':claim.owner}),flush=True)
os.kill(os.getpid(),signal.SIGKILL)
"""
    result = subprocess.run(
        [sys.executable, "-c", "EXPECTED_ID=" + repr(str(command_id)) + "\n" + script],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    records = [
        json.loads(line.split("=", 1)[1])
        for line in result.stdout.splitlines()
        if line.startswith("CHILD_CLAIM=")
    ]
    assert len(records) == 1
    abandoned = command_for(engine, run)
    assert abandoned.lease_owner == records[0]["owner"] and abandoned.retry_count == min(1, budget)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    assert recovery.reconcile_tracking_once(
        lambda: Session(engine),
        client=client,
        now=abandoned.lease_expires_at + timedelta(seconds=1),
    ) == (1 if budget == 2 else 0)
    current = command_for(engine, run)
    assert current.retry_count == budget
    if budget == 2:
        assert current.response_payload == {"run_id": remote_id}
        assert lookup.call_count == 1
    else:
        assert current.status == CommandStatus.FAILED
        assert current.response_payload == {"creation_started": True}
        assert lookup.call_count == 0
    assert current.lease_owner is None
    client.create_run.assert_not_called()


def test_payload_boolean_integer_mutation_cannot_publish(tracking, monkeypatch):  # noqa: F811
    from automl_api.services.workflow_state import canonical_request_hash
    from sqlalchemy import update

    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    command_id = command_for(engine, run).id
    with Session(engine) as db, db.begin():
        command = db.get(WorkflowCommand, command_id)
        command.request_payload = {**command.request_payload, "opaque_flag": True}
        command.request_hash = canonical_request_hash(command.request_payload)
    original = client.search_runs

    def lookup(*args, **kwargs):
        with Session(engine) as db, db.begin():
            command = db.get(WorkflowCommand, command_id)
            changed = {**command.request_payload, "opaque_flag": 1}
            db.execute(
                update(WorkflowCommand)
                .where(WorkflowCommand.id == command_id)
                .values(request_payload=changed)
            )
        return original(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client, limit=1) == 0
    command = command_for(engine, run)
    assert command.response_payload == {"creation_started": True}
    assert command.status != CommandStatus.SUCCEEDED


def test_publication_waits_for_own_heartbeat_transaction_to_close(tracking, monkeypatch):  # noqa: F811
    import threading

    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    monkeypatch.setattr(recovery, "HEARTBEAT_SECONDS", 0.01)
    holding = threading.Event()
    release = threading.Event()
    state = {"released": False}

    def slow_renew(_factory, claim, _now):
        with Session(engine) as db, db.begin():
            command = db.get(WorkflowCommand, claim.command_id, with_for_update=True)
            assert command.lease_owner == claim.owner
            holding.set()
            assert release.wait(5), "Heartbeat transaction was not released"
        state["released"] = True
        return True

    monkeypatch.setattr(recovery, "_renew_tracking", slow_renew)
    original_lookup = client.search_runs
    timers = []

    def lookup(*args, **kwargs):
        assert holding.wait(5), "Heartbeat did not acquire its row lock"
        timer = threading.Timer(0.3, release.set)
        timers.append(timer)
        timer.start()
        return original_lookup(*args, **kwargs)

    monkeypatch.setattr(client, "search_runs", lookup)
    original_publish = recovery._publish_tracking

    def publish(*args, **kwargs):
        assert state["released"], "Publication raced its own heartbeat lock"
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(recovery, "_publish_tracking", publish)
    try:
        assert recovery.reconcile_tracking_once(lambda: Session(engine), client=client) == 1
    finally:
        release.set()
        for timer in timers:
            timer.join(timeout=2)
    assert command_for(engine, run).lease_owner is None
