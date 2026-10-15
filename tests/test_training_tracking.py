from unittest.mock import MagicMock

import mlflow
import pytest
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.training import pipeline
from mlflow import MlflowClient
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401


@pytest.fixture
def tracking(publication, monkeypatch, tmp_path):  # noqa: F811
    engine, run_id, attempt_id, _, _ = publication
    previous = mlflow.get_tracking_uri()
    uri = f"sqlite:///{tmp_path / 'tracking.db'}"
    mlflow.set_tracking_uri(uri)
    monkeypatch.setattr(mlflow.tracking.fluent, "_active_experiment_id", None)
    monkeypatch.delenv("MLFLOW_RUN_ID", raising=False)
    client = MlflowClient(tracking_uri=uri)
    create_experiment = client.create_experiment
    monkeypatch.setattr(
        client,
        "create_experiment",
        lambda name: create_experiment(name, artifact_location=(tmp_path / "artifacts").as_uri()),
    )
    monkeypatch.setattr(pipeline, "MlflowClient", lambda: client)
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
    try:
        yield engine, run, attempt_id, client
    finally:
        mlflow.end_run()
        mlflow.set_tracking_uri(previous)


@pytest.mark.parametrize("role", ["parent", "candidate:Ridge", "failure:Ridge"])
def test_lost_create_ack_recovers_one_tagged_tracking_run(tracking, monkeypatch, role):
    engine, run, attempt_id, client = tracking
    original = client.create_run
    created_ids = []

    def lost_ack(*args, **kwargs):
        with Session(engine) as db:
            commands = list(
                db.scalars(
                    select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
                )
            )
            assert len(commands) == 1 and commands[0].operation == "mlflow.run.ensure"
            assert commands[0].response_payload == {"creation_started": True}
        created = original(*args, **kwargs)
        created_ids.append(created.info.run_id)
        raise ConnectionError("creation acknowledgement lost")

    monkeypatch.setattr(client, "create_run", lost_ack)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role=role, run_name="fixture")
    monkeypatch.setattr(client, "create_run", MagicMock(side_effect=AssertionError("duplicate")))
    for _ in range(2):
        with pipeline._tracked_mlflow_run(run, role=role, run_name="fixture") as active:
            assert active.info.run_id == created_ids[0]
    tracked = client.get_run(created_ids[0])
    assert tracked.data.tags["automl_attempt_id"] == str(attempt_id)
    assert tracked.data.tags["automl_run_id"] == str(run.id)
    assert tracked.data.tags["project_id"] == str(run.project_id)
    with Session(engine) as db:
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        assert command.response_payload == {"run_id": created_ids[0]}
        if role == "parent":
            assert db.get(ModelRun, run.id).mlflow_run_id == created_ids[0]


@pytest.mark.parametrize("corruption", ["deleted", "lineage"])
def test_tracking_replay_rejects_deleted_or_changed_remote_record(tracking, corruption):
    _, run, _, client = tracking
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="fixture") as active:
        tracking_id = active.info.run_id
    if corruption == "deleted":
        client.delete_run(tracking_id)
    else:
        client.set_tag(tracking_id, "automl_run_id", "another-run")
    with pytest.raises(ValueError, match="lineage"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="fixture")


def test_nested_candidate_is_linked_during_remote_creation(tracking):
    _, run, _, client = tracking
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as parent:
        with pipeline._tracked_mlflow_run(
            run,
            role="candidate:Ridge",
            run_name="Ridge",
            parent_run_id=parent.info.run_id,
        ) as candidate:
            assert (
                client.get_run(candidate.info.run_id).data.tags["mlflow.parentRunId"]
                == parent.info.run_id
            )
        assert mlflow.active_run().info.run_id == parent.info.run_id


def test_lost_database_commit_ack_replays_recorded_tracking_id(tracking, monkeypatch):
    engine, run, _, client = tracking
    commits = []

    class LostAcknowledgement(Session):
        def commit(self):
            super().commit()
            commits.append(True)
            if len(commits) == 3:
                raise ConnectionError("database commit acknowledgement lost")

    monkeypatch.setattr(
        pipeline, "get_session_factory", lambda: lambda: LostAcknowledgement(engine)
    )
    with pytest.raises(ConnectionError, match="database commit"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(engine))
    monkeypatch.setattr(client, "create_run", MagicMock(side_effect=AssertionError("duplicate")))
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as active:
        with Session(engine) as db:
            assert db.get(ModelRun, run.id).mlflow_run_id == active.info.run_id


def test_concurrent_tracking_creation_reuses_one_remote_run(tracking, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    _, run, _, client = tracking
    create = MagicMock(wraps=client.create_run)
    monkeypatch.setattr(client, "create_run", create)
    barrier = Barrier(2)

    def start(_index):
        barrier.wait(timeout=10)
        try:
            with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as active:
                return active.info.run_id
        except RuntimeError as exc:
            assert "creation is unresolved" in str(exc)
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(start, range(2)))
    assert any(ids)
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as replay:
        assert all(value is None or value == replay.info.run_id for value in ids)
    assert create.call_count == 1


def test_tracking_key_rejects_changed_parent_before_remote_effect(tracking, monkeypatch):
    from automl_api.services.workflow_state import IdempotencyConflict

    _, run, _, client = tracking
    with pipeline._tracked_mlflow_run(run, role="candidate:Ridge", run_name="Ridge"):
        pass
    create = MagicMock(side_effect=AssertionError("unexpected remote create"))
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(IdempotencyConflict):
        pipeline._tracked_mlflow_run(
            run,
            role="candidate:Ridge",
            run_name="Ridge",
            parent_run_id="changed-parent",
        )
    create.assert_not_called()


def test_uncertain_remote_creation_is_never_blindly_retried(tracking, monkeypatch):
    _, run, _, client = tracking
    create = MagicMock(side_effect=ConnectionError("request outcome unknown"))
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    for _ in range(2):
        with pytest.raises(pipeline.TrackingCreationUnresolved, match="unresolved"):
            pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    assert create.call_count == 1


def test_multiple_remote_matches_fail_closed(tracking, monkeypatch):
    _, run, _, client = tracking
    original = client.create_run

    def duplicated(*args, **kwargs):
        original(*args, **kwargs)
        original(*args, **kwargs)
        raise ConnectionError("remote duplicate")

    monkeypatch.setattr(client, "create_run", duplicated)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    with pytest.raises(ValueError, match="Multiple MLflow"):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")


def test_missing_cached_remote_run_is_not_recreated(tracking, monkeypatch):
    engine, run, _, client = tracking
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent"):
        pass
    with Session(engine) as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        command.response_payload = {"run_id": "0" * 32}
    create = MagicMock(side_effect=AssertionError("must not replace missing tracking history"))
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(mlflow.exceptions.MlflowException):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()


def test_committed_result_cannot_resume_mutating_mlflow(tracking, monkeypatch):
    from automl_api.services.workflow_state import StaleFence

    engine, run, attempt_id, client = tracking
    with Session(engine) as db, db.begin():
        current = db.get(ModelRun, run.id)
        current.tags = {
            "training_result_receipt": {"attempt_id": str(attempt_id), "digest": "sealed"}
        }
    create = MagicMock(side_effect=AssertionError("committed result cannot create tracking"))
    monkeypatch.setattr(client, "create_run", create)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    create.assert_not_called()


def _leave_uncertain_tracking(tracking, monkeypatch, *, created=True):
    _, run, _, client = tracking
    original = client.create_run
    ids = []

    def lose_response(*args, **kwargs):
        if created:
            ids.append(original(*args, **kwargs).info.run_id)
        raise ConnectionError("unknown outcome")

    monkeypatch.setattr(client, "create_run", lose_response)
    with pytest.raises(pipeline.TrackingCreationUnresolved):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="parent")
    monkeypatch.setattr(
        client, "create_run", MagicMock(side_effect=AssertionError("read-only recovery"))
    )
    return ids[0] if ids else None


def test_reconciler_recovers_creation_without_resuming_remote_run(tracking, monkeypatch):
    from automl_api.models.enums import CommandStatus
    from automl_api.services.tracking_reconciliation import reconcile_tracking_once

    engine, run, _, client = tracking
    tracking_id = _leave_uncertain_tracking(tracking, monkeypatch)
    monkeypatch.setattr(mlflow, "start_run", MagicMock(side_effect=AssertionError("no resume")))
    assert reconcile_tracking_once(lambda: Session(engine), client=client) == 1
    for _ in range(2):
        assert reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    with Session(engine) as db:
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        assert command.status == CommandStatus.SUCCEEDED
        assert command.response_payload == {"run_id": tracking_id}
        assert db.get(ModelRun, run.id).mlflow_run_id is None
    client.create_run.assert_not_called()


def test_reconciler_bounds_unknown_outcome_lookups_without_recreating(tracking, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import CommandStatus
    from automl_api.services.tracking_reconciliation import reconcile_tracking_once

    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch, created=False)
    with Session(engine) as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        command.max_retries = 2
        command_id = command.id
    now = datetime.now(UTC)
    for seconds in (0, 61, 122):
        assert (
            reconcile_tracking_once(
                lambda: Session(engine), client=client, now=now + timedelta(seconds=seconds)
            )
            == 0
        )
    with Session(engine) as db:
        command = db.get(WorkflowCommand, command_id)
        assert command.status == CommandStatus.FAILED and command.retry_count == 2
        assert command.response_payload == {"creation_started": True}
        assert command.terminal_reason
    client.create_run.assert_not_called()


def test_reconciler_skips_live_worker_lock(tracking, monkeypatch):
    from automl_api.services.tracking_reconciliation import reconcile_tracking_once

    engine, run, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    with Session(engine) as owner, owner.begin():
        owner.get(ModelRun, run.id, with_for_update=True)
        assert reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    assert reconcile_tracking_once(lambda: Session(engine), client=client) == 1


@pytest.mark.parametrize("invalid", ["backend", "lineage"])
def test_reconciler_rejects_wrong_tracking_identity(tracking, monkeypatch, invalid):
    from automl_api.models.enums import CommandStatus
    from automl_api.services.tracking_reconciliation import reconcile_tracking_once

    engine, run, _, client = tracking
    tracking_id = _leave_uncertain_tracking(tracking, monkeypatch)
    if invalid == "backend":
        client = MagicMock(tracking_uri="different-backend")
    else:
        client.set_tag(tracking_id, "automl_run_id", "wrong-run")
    assert reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    with Session(engine) as db:
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        assert command.status != CommandStatus.SUCCEEDED
        assert command.response_payload == {"creation_started": True}
    client.create_run.assert_not_called()


def test_concurrent_reconcilers_bind_tracking_once(tracking, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from automl_api.services.tracking_reconciliation import reconcile_tracking_once

    engine, _, _, client = tracking
    _leave_uncertain_tracking(tracking, monkeypatch)
    barrier = Barrier(2)

    def recover(_index):
        barrier.wait(timeout=10)
        return reconcile_tracking_once(lambda: Session(engine), client=client)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(recover, range(2))) == 1
    client.create_run.assert_not_called()


def test_late_visible_outcome_can_be_adopted_after_lookup_budget(tracking, monkeypatch):
    from automl_api.models.enums import CommandStatus
    from automl_api.services.tracking_reconciliation import (
        reconcile_tracking_once,
        tracking_identity,
    )

    engine, run, _, client = tracking
    original = client.create_run
    _leave_uncertain_tracking(tracking, monkeypatch, created=False)
    with Session(engine) as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.project_id == run.project_id)
        )
        command.max_retries = 1
        command_id = command.id
        identity = tracking_identity(command, db.get(ModelRun, run.id))
    assert reconcile_tracking_once(lambda: Session(engine), client=client) == 0
    with Session(engine) as db:
        assert db.get(WorkflowCommand, command_id).status == CommandStatus.FAILED
    experiment = client.get_experiment_by_name(f"automl-project-{run.project_id}")
    late = original(experiment.experiment_id, tags=identity, run_name="late original response")
    with pipeline._tracked_mlflow_run(run, role="parent", run_name="parent") as recovered:
        assert recovered.info.run_id == late.info.run_id
    with Session(engine) as db:
        assert db.get(WorkflowCommand, command_id).status == CommandStatus.SUCCEEDED
        assert db.get(WorkflowCommand, command_id).terminal_reason is None
    client.create_run.assert_not_called()


@pytest.mark.parametrize("blocked", ["run", "command", "missing_run"])
def test_tracking_batch_reaches_work_behind_busy_or_orphaned_rows(tracking, monkeypatch, blocked):
    import hashlib
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import CommandStatus
    from automl_api.services.tracking_reconciliation import (
        reconcile_tracking_once,
        tracking_identity,
    )
    from automl_api.services.workflow_state import begin_command
    from sqlalchemy import text

    engine, template, _, client = tracking
    now = datetime.now(UTC)
    experiment_id = client.create_experiment(f"automl-project-{template.project_id}")
    commands = []
    runs = []
    remote_ids = []
    with Session(engine) as db, db.begin():
        for index in range(2):
            run = ModelRun(
                project_id=template.project_id,
                dataset_version_id=template.dataset_version_id,
                created_by_id=template.created_by_id,
            )
            db.add(run)
            db.flush()
            command, _ = begin_command(
                db, project_id=run.project_id, actor_id=run.created_by_id,
                operation="mlflow.run.ensure", idempotency_key=f"queue-{index}",
                payload={
                    "model_run_id": str(run.id), "attempt_id": "legacy", "role": "parent",
                    "tracking_backend_hash": hashlib.sha256(
                        client.tracking_uri.encode()
                    ).hexdigest(),
                },
            )
            command.resource_type = "model_run"
            command.resource_id = run.id
            command.response_payload = {"creation_started": True}
            command.available_at = now - timedelta(seconds=2 - index)
            remote_ids.append(client.create_run(
                experiment_id, tags=tracking_identity(command, run)
            ).info.run_id)
            commands.append(command.id)
            runs.append(run.id)
        if blocked == "missing_run":
            db.delete(db.get(ModelRun, runs[0]))
    monkeypatch.setattr(client, "create_run", MagicMock(side_effect=AssertionError("no creation")))

    def session_factory():
        db = Session(engine)
        db.execute(text("SET LOCAL lock_timeout = '200ms'"))
        return db

    with Session(engine) as owner, owner.begin():
        if blocked != "missing_run":
            owner.get(
                ModelRun if blocked == "run" else WorkflowCommand,
                runs[0] if blocked == "run" else commands[0], with_for_update=True,
            )
        assert reconcile_tracking_once(session_factory, client=client, limit=1, now=now) == 1
        with Session(engine) as db:
            first, second = (db.get(WorkflowCommand, key) for key in commands)
            assert first.status == CommandStatus.PENDING and first.retry_count == 0
            assert second.status == CommandStatus.SUCCEEDED
            assert second.response_payload == {"run_id": remote_ids[1]}
    if blocked != "missing_run":
        assert reconcile_tracking_once(session_factory, client=client, limit=1, now=now) == 1
    assert reconcile_tracking_once(session_factory, client=client, limit=1, now=now) == 0
    client.create_run.assert_not_called()
