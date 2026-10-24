"""Registry storage reads release SQL and cannot publish stale observed metadata."""

import uuid
from types import SimpleNamespace

import pytest
from automl_api.models.enums import (
    CommandStatus,
    OutboxStatus,
)
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import OutboxEntry
from automl_api.services.workflow_state import begin_command, enqueue_outbox
from automl_api.workflow_reconciler import run_once
from registry_custody_fixture import qualified_model as qualified_model
from sqlalchemy import delete, update
from sqlalchemy.orm import Session, sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import model_api as model_api
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def registry_case(qualified_model, monkeypatch):
    from automl_api.models.iam import User
    from automl_api.schemas.operations import RegistryCreateRequest
    from automl_api.services import operations, registry_inspection

    c = qualified_model
    payload = RegistryCreateRequest(training_run_id=c.run_id, model_name=c.model_name)
    with c.factory() as db:
        selection = operations.register_model(db, db.get(User, c.user_id), c.project_id, payload)
        assert isinstance(selection, operations.RegistrationInspection)
        db.commit()
    inspected = operations.inspect_model_registration(selection)
    with c.factory() as db, db.begin():
        registry = operations.register_model(
            db,
            db.get(User, c.user_id),
            c.project_id,
            payload,
            inspection=inspected,
        )
        c.registry_id, c.artifact_id = registry.id, registry.model_artifact_id
        metadata = dict(registry.registry_metadata)
        metadata.pop("native_custody_digest")
        metadata["desired_state"] = "artifact_verification_pending"
        registry.registry_metadata = metadata
        db.execute(
            update(OutboxEntry)
            .where(
                OutboxEntry.project_id == c.project_id,
                OutboxEntry.aggregate_id == c.run_id,
                OutboxEntry.topic == "ray.training.submit",
            )
            .values(status=OutboxStatus.DELIVERED, lease_owner=None, lease_expires_at=None)
        )
        command, _ = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.user_id,
            operation="test.registry",
            idempotency_key=uuid.uuid4().hex,
            payload={"registry_entry_id": str(registry.id)},
        )
        command.status = CommandStatus.RUNNING
        entry = enqueue_outbox(
            db,
            command,
            topic="registry.reconcile",
            aggregate_type="registry_entry",
            aggregate_id=registry.id,
            payload={"registry_entry_id": str(registry.id)},
        )
        db.flush()
        c.command_id, c.entry_id = command.id, entry.id
    c.native_counts.clear()
    monkeypatch.setattr(registry_inspection, "get_object_store", lambda: c.store)
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(
                delete(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            )


def test_registry_delivery_releases_sql_during_native_inspection(registry_case):
    c = registry_case
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == [0, 0]
    with c.factory() as db:
        row = db.get(ModelRegistryEntry, c.registry_id)
        assert row.registry_metadata["desired_state"] == "artifact_verified"
        assert row.registry_metadata["verified_byte_size"] == 10
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"


def test_verified_registry_replay_never_repeats_native_reads(registry_case):
    from automl_api.models.enums import OutboxStatus

    c = registry_case
    assert run_once(c.factory, worker_id="registry-owner") == 1
    with c.factory() as db, db.begin():
        original = dict(db.get(ModelRegistryEntry, c.registry_id).registry_metadata)
        db.get(OutboxEntry, c.entry_id).status = OutboxStatus.PENDING
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == [0, 0]
    with c.factory() as db:
        assert db.get(ModelRegistryEntry, c.registry_id).registry_metadata == original


@pytest.mark.parametrize("hook", ["during_exists", "during_size"])
@pytest.mark.parametrize(
    "change", ["artifact", "registry", "run", "command", "project", "lease", "delivery"]
)
def test_registry_rechecks_source_and_delivery_after_native_io(registry_case, hook, change):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import ProjectStatus
    from automl_api.models.projects import Project
    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import text

    c = registry_case

    def mutate():
        with c.factory() as db, db.begin():
            db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            if change == "artifact":
                db.get(RunArtifact, c.artifact_id).content_hash = "c" * 64
            elif change == "registry":
                db.get(ModelRegistryEntry, c.registry_id).registry_metadata = {
                    "desired_state": "retired"
                }
            elif change == "run":
                db.get(ModelRun, c.run_id).tags = {"changed": True}
            elif change == "command":
                db.get(WorkflowCommand, c.command_id).request_hash = "c" * 64
            elif change == "project":
                db.get(Project, c.project_id).status = ProjectStatus.ARCHIVED
            elif change == "lease":
                db.get(OutboxEntry, c.entry_id).lease_expires_at = datetime.now(UTC) - timedelta(
                    seconds=1
                )
            else:
                db.get(OutboxEntry, c.entry_id).delivery_attempts += 1

    c.state[hook] = mutate
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == [0, 0]
    with c.factory() as db:
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata.get("desired_state")
            != "artifact_verified"
        )
        assert db.get(OutboxEntry, c.entry_id).status.value == "claimed"


@pytest.mark.parametrize("fault", ["missing", "size", "negative", "bool", "unknown_size"])
def test_registry_native_read_failures_preserve_retry_semantics(registry_case, fault):
    from automl_api.models.workflows import WorkflowCommand

    c = registry_case
    if fault == "missing":
        c.state["exists"] = False
    elif fault == "unknown_size":
        with c.factory() as db, db.begin():
            db.get(RunArtifact, c.artifact_id).byte_size = None
        c.state["size"] = 9
    else:
        c.state["size"] = {"size": 9, "negative": -1, "bool": True}[fault]
    assert run_once(c.factory, worker_id="registry-owner") == 1
    with c.factory() as db:
        row = db.get(OutboxEntry, c.entry_id)
        registry = db.get(ModelRegistryEntry, c.registry_id)
        if fault == "unknown_size":
            assert (
                row.status.value == "delivered"
                and registry.registry_metadata["verified_byte_size"] == 9
            )
        else:
            assert row.status.value == "pending" and row.last_error
            assert db.get(WorkflowCommand, c.command_id).retry_count == 1
            assert registry.registry_metadata["desired_state"] != "artifact_verified"


@pytest.mark.parametrize("same_owner", [False, True])
def test_late_registry_failure_cannot_change_a_new_delivery(registry_case, same_owner):
    from datetime import UTC, datetime, timedelta

    from automl_api.services.workflow_state import claim_outbox

    c = registry_case

    def takeover():
        with c.factory() as db, db.begin():
            db.get(OutboxEntry, c.entry_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        with c.factory() as db, db.begin():
            assert (
                claim_outbox(
                    db, worker_id="registry-owner" if same_owner else "successor", limit=1
                )[0].id
                == c.entry_id
            )
        raise OSError("owned failed read after takeover")

    c.state["during_exists"] = takeover
    assert run_once(c.factory, worker_id="registry-owner") == 1
    with c.factory() as db:
        row = db.get(OutboxEntry, c.entry_id)
        assert (
            row.status.value == "claimed" and row.delivery_attempts == 2 and row.last_error is None
        )
        assert row.lease_owner == ("registry-owner" if same_owner else "successor")
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
            != "artifact_verified"
        )


def test_two_registry_readers_cannot_publish_the_old_delivery(registry_case):
    from datetime import UTC, datetime, timedelta

    from automl_api.services.registry_inspection import reconcile_registry_artifact
    from automl_api.services.workflow_state import claim_outbox

    c = registry_case

    def successor():
        c.state.pop("during_exists")
        with c.factory() as db, db.begin():
            db.get(OutboxEntry, c.entry_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        with c.factory() as db, db.begin():
            claim_outbox(db, worker_id="successor", limit=1)
        reconcile_registry_artifact(c.factory, c.entry_id, "successor")

    c.state["during_exists"] = successor
    assert run_once(c.factory, worker_id="registry-owner") == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
            == "artifact_verified"
        )
    assert c.native_counts == [0, 0, 0, 0]


def test_registry_sql_connection_loss_rolls_back_publication_then_retries_reads(registry_case):
    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import event

    c = registry_case
    killed = []

    def before_flush(db, *_args):
        if not killed and any(
            isinstance(row, ModelRegistryEntry)
            and row.id == c.registry_id
            and row.registry_metadata.get("desired_state") == "artifact_verified"
            for row in db.dirty
        ):
            killed.append(True)
            db.connection().exec_driver_sql("SELECT pg_terminate_backend(pg_backend_pid())")

    event.listen(Session, "before_flush", before_flush)
    try:
        assert run_once(c.factory, worker_id="registry-owner") == 1
    finally:
        event.remove(Session, "before_flush", before_flush)
    assert killed == [True]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "pending"
        assert db.get(WorkflowCommand, c.command_id).retry_count == 1
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
            != "artifact_verified"
        )
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == [0, 0, 0, 0]


def test_registry_lost_commit_ack_never_repeats_native_reads(registry_case):
    from sqlalchemy import event

    c = registry_case
    lost = []

    def before_flush(db, *_args):
        if any(isinstance(row, ModelRegistryEntry) and row.id == c.registry_id for row in db.dirty):
            db.info["owned_registry_commit"] = True

    def after_commit(db):
        if db.info.get("owned_registry_commit") and not lost:
            lost.append(True)
            raise OSError("owned registry committed reply loss")

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    try:
        assert run_once(c.factory, worker_id="registry-owner") == 1
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
    assert lost == [True] and c.native_counts == [0, 0]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
            == "artifact_verified"
        )
    assert run_once(c.factory, worker_id="registry-owner") == 0
    assert c.native_counts == [0, 0]


def _registry_reader_process(database_url, entry_id, tracking_uri, ready):
    import time

    from automl_api.services import registry_inspection
    from sqlalchemy import create_engine

    engine = create_engine(database_url)
    from automl_api.services import training_tracking

    training_tracking.get_settings = lambda: SimpleNamespace(mlflow_tracking_uri=tracking_uri)

    def exists(_uri):
        assert engine.pool.checkedout() == 0
        ready.set()
        time.sleep(30)
        return True

    def open_stream(uri, *, version):
        exists(uri)
        raise AssertionError("The controlled reader should have been killed")

    registry_inspection.get_object_store = lambda: SimpleNamespace(open_stream=open_stream)
    registry_inspection.reconcile_registry_artifact(
        sessionmaker(bind=engine), entry_id, "registry-owner"
    )


def test_registry_process_death_before_inspection_cannot_publish(registry_case):
    import multiprocessing
    from datetime import UTC, datetime, timedelta

    from automl_api.services.workflow_state import claim_outbox

    c = registry_case
    with c.factory() as db, db.begin():
        assert claim_outbox(db, worker_id="registry-owner", limit=1)[0].id == c.entry_id
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    child = context.Process(
        target=_registry_reader_process,
        args=(
            c.engine.url.render_as_string(hide_password=False),
            c.entry_id,
            c.producer.tracking.uri,
            ready,
        ),
    )
    child.start()
    try:
        assert ready.wait(30) and child.is_alive()
        child.kill()
        child.join(10)
        assert child.exitcode == -9
        with c.factory() as db, db.begin():
            assert db.get(OutboxEntry, c.entry_id).status.value == "claimed"
            assert (
                db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
                != "artifact_verified"
            )
            db.get(OutboxEntry, c.entry_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        assert run_once(c.factory, worker_id="successor") == 1
        assert c.native_counts == [0, 0]
    finally:
        if child.is_alive():
            child.kill()
            child.join(10)
        child.close()


@pytest.mark.parametrize("part", ["artifact", "ack", "policy", "producer"])
def test_background_refuses_unqualified_verified_history(registry_case, part):
    from automl_api.models.workflows import WorkflowEvent

    c = registry_case
    with c.factory() as db, db.begin():
        registry = db.get(ModelRegistryEntry, c.registry_id)
        artifact = db.get(RunArtifact, c.artifact_id)
        if part == "artifact":
            artifact.artifact_metadata = {
                k: v for k, v in artifact.artifact_metadata.items() if k != "model_custody"
            }
        elif part == "ack":
            registry.registry_metadata = {
                **registry.registry_metadata,
                "desired_state": "artifact_verified",
            }
        elif part == "policy":
            registry.registry_metadata = {
                **registry.registry_metadata,
                "artifact_policy": "unknown",
            }
        else:
            db.execute(
                delete(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.producer.scope[2],
                    WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
                )
            )
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == []
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "claimed"
        assert (
            "native_custody_digest"
            not in db.get(ModelRegistryEntry, c.registry_id).registry_metadata
        )


def test_background_detects_custody_change_during_stream(registry_case):
    from automl_api.models.workflows import WorkflowEvent

    c = registry_case

    def remove_receipt():
        with c.factory() as db, db.begin():
            db.execute(
                delete(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.producer.scope[2],
                    WorkflowEvent.event_key == f"training.model.grant.{c.producer.candidate_id}",
                )
            )

    c.state["during_size"] = remove_receipt
    assert run_once(c.factory, worker_id="registry-owner") == 1
    assert c.native_counts == [0, 0]
    with c.factory() as db:
        assert (
            db.get(ModelRegistryEntry, c.registry_id).registry_metadata["desired_state"]
            != "artifact_verified"
        )
