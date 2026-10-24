"""Native completion markers distinguish settlement from a still-live writer."""

import copy
import signal
import uuid
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus, CommandStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import training_metadata as control
from automl_api.services.attempt_object_grants import has_unretired_attempt_credentials
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from test_training_tracking_metadata_api import bound_run as bound_run
from test_training_tracking_metadata_api import capacity_client as capacity_client
from test_training_tracking_metadata_api import committed_tenant as committed_tenant
from test_training_tracking_metadata_api import context_api as context_api
from test_training_tracking_metadata_api import metadata_api as metadata_api
from test_training_tracking_metadata_api import phase1_engine as phase1_engine
from test_training_tracking_metadata_api import selected_scope as selected_scope
from test_training_tracking_metadata_api import tracking_api as tracking_api


def command_for(db, m):
    return db.scalars(
        select(WorkflowCommand).where(
            WorkflowCommand.project_id == m.c.scope[4],
            WorkflowCommand.operation == control.OPERATION,
        )
    ).one()


def marker_ack_loss(m, monkeypatch):
    original = m.c.native.set_tag
    markers = []

    def lost(*args, **kwargs):
        assert kwargs["synchronous"] is True
        assert m.c.scope[0].pool.checkedout() == 0
        original(*args, **kwargs)
        markers.append((args, kwargs))
        raise RuntimeError("owned completion marker response loss")

    monkeypatch.setattr(m.c.native, "set_tag", lost)
    assert m.publish().status_code == 503
    assert len(m.writes) == len(markers) == 1
    return SimpleNamespace(original=original, markers=markers)


def test_unknown_marker_ack_recovers_by_get_without_resending_either_write(
    metadata_api, monkeypatch
):
    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    assert m.publish().status_code == 503
    assert m.receipt().status_code == m.publish().status_code == 200
    assert len(m.writes) == len(lost.markers) == 1
    assert len(m.c.native.get_metric_history(m.parent_id, "rmse")) == 1
    with Session(m.c.scope[0]) as db:
        assert not has_unretired_attempt_credentials(db, m.c.scope[2])


def test_read_only_settlement_after_cancellation_does_not_publish_an_ack(metadata_api, monkeypatch):
    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    with Session(m.c.scope[0]) as db, db.begin():
        db.get(WorkflowAttempt, m.c.scope[2]).status = AttemptStatus.CANCELLED
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 1
    with Session(m.c.scope[0]) as db:
        command = command_for(db, m)
        assert command.status == CommandStatus.RUNNING and command.response_payload == {}
        assert not has_unretired_attempt_credentials(db, m.c.scope[2])
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 0
    assert len(m.writes) == len(lost.markers) == 1
    assert m.receipt().status_code == 401


def test_batch_values_without_completion_marker_cannot_retire(metadata_api, monkeypatch):
    m = metadata_api
    original = m.c.native.log_batch

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("owned unknown batch outcome before completion marker")

    monkeypatch.setattr(m.c.native, "log_batch", lost)
    assert m.publish().status_code == 503
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 0
    assert m.receipt().status_code == 503
    with Session(m.c.scope[0]) as db:
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert len(m.writes) == 1


@pytest.mark.parametrize("field", ["binding_digest", "pod_uid", "actor_id"])
def test_reconciliation_rejects_a_rehashed_changed_intent(metadata_api, monkeypatch, field):
    from automl_api.services.workflow_state import canonical_request_hash

    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    with Session(m.c.scope[0]) as db, db.begin():
        command = command_for(db, m)
        payload = copy.deepcopy(command.request_payload)
        payload[field] = "f" * 64
        command.request_payload = payload
        command.request_hash = canonical_request_hash(payload)
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 0
    assert m.receipt().status_code == m.publish().status_code == 401
    with Session(m.c.scope[0]) as db:
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert len(m.writes) == len(lost.markers) == 1


@pytest.mark.parametrize("field", ["operation", "idempotency_key", "resource_id", "deleted"])
def test_original_command_binding_prevents_another_writer_after_history_changes(
    metadata_api, monkeypatch, field
):
    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    with Session(m.c.scope[0]) as db, db.begin():
        command = command_for(db, m)
        if field == "deleted":
            db.delete(command)
        else:
            setattr(command, field, uuid.uuid4() if field == "resource_id" else "changed-history")
    assert m.receipt().status_code == m.publish().status_code == 401
    with Session(m.c.scope[0]) as db:
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert len(m.writes) == len(lost.markers) == 1


@pytest.mark.parametrize("change", ["missing", "hash", "native_marker"])
def test_unqualified_command_or_native_marker_retains_the_barrier(
    metadata_api, monkeypatch, change
):
    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    with Session(m.c.scope[0]) as db, db.begin():
        command = command_for(db, m)
        if change == "native_marker":
            key, _ = control._marker(
                command.id, command.request_payload, int(command.created_at.timestamp() * 1000)
            )
            lost.original(m.parent_id, key, "foreign-marker", synchronous=True)
        else:
            event = db.scalar(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == m.c.scope[2],
                    WorkflowEvent.event_key == control.INTENT + m.body["request_id"],
                )
            )
            if change == "missing":
                db.delete(event)
            else:
                event.result_digest = "f" * 64
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 0
    with Session(m.c.scope[0]) as db:
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert len(m.writes) == len(lost.markers) == 1


def test_actual_api_process_death_after_marker_is_settled_without_a_second_write(metadata_api):
    import multiprocessing
    import os

    from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore

    m = metadata_api
    context = multiprocessing.get_context("fork")
    reached = context.Event()

    def child():
        m.c.scope[0].dispose(close=False)
        SqlAlchemyStore._engine_map[m.c.uri].dispose(close=False)
        original = m.c.native.set_tag

        def crash(*args, **kwargs):
            original(*args, **kwargs)
            reached.set()
            os.kill(os.getpid(), signal.SIGKILL)

        m.c.native.set_tag = crash
        m.publish()

    process = context.Process(target=child)
    process.start()
    try:
        assert reached.wait(timeout=25)
        process.join(timeout=10)
        assert not process.is_alive() and process.exitcode == -signal.SIGKILL
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=10)
        process.close()
    with Session(m.c.scope[0]) as db:
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
        assert command_for(db, m).response_payload == {}
    assert control.reconcile_tracking_metadata_once(sessionmaker(bind=m.c.scope[0])) == 1
    assert m.receipt().status_code == 200
    assert len(m.c.native.get_metric_history(m.parent_id, "rmse")) == 1
    with Session(m.c.scope[0]) as db:
        assert not has_unretired_attempt_credentials(db, m.c.scope[2])


def test_malformed_inspection_does_not_starve_a_later_owned_marker(metadata_api, monkeypatch):
    from datetime import UTC, datetime, timedelta

    m = metadata_api
    lost = marker_ack_loss(m, monkeypatch)
    with Session(m.c.scope[0]) as db, db.begin():
        original = command_for(db, m)
        original_id = original.id
        db.add(
            WorkflowCommand(
                project_id=original.project_id,
                actor_id=original.actor_id,
                operation=control.OPERATION,
                resource_type="model_run",
                resource_id=m.c.scope[1],
                idempotency_key="malformed-inspection-" + uuid.uuid4().hex,
                request_payload={"request_json": "broken"},
                request_hash="f" * 64,
                status=CommandStatus.RUNNING,
                available_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
    factory = sessionmaker(bind=m.c.scope[0])
    assert control.reconcile_tracking_metadata_once(factory, limit=1) == 0
    assert control.reconcile_tracking_metadata_once(factory, limit=1) == 1
    with Session(m.c.scope[0]) as db:
        assert control._verified(db.get(WorkflowCommand, original_id))
        assert has_unretired_attempt_credentials(db, m.c.scope[2])
    assert len(m.writes) == len(lost.markers) == 1
