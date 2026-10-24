"""Owned native metadata, original tracking custody and immutable command receipts."""

import copy
import uuid
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus, CommandStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import training_metadata as control
from automl_api.services import training_tracking as tracking
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_training_model_output_api import model_api as model_api
from test_training_result_api import result_api as result_api
from test_training_tracking_api import bound_run as bound_run
from test_training_tracking_api import capacity_client as capacity_client
from test_training_tracking_api import committed_tenant as committed_tenant
from test_training_tracking_api import context_api as context_api
from test_training_tracking_api import phase1_engine as phase1_engine
from test_training_tracking_api import selected_scope as selected_scope
from test_training_tracking_api import tracking_api as tracking_api


@pytest.fixture
def metadata_api(tracking_api, monkeypatch):
    c = tracking_api
    response = c.publish()
    assert response.status_code == 200, response.text
    parent_id = response.json()["tracking_run_id"]
    body = dict(
        run_id=str(c.scope[1]),
        role="parent",
        request_id=str(uuid.uuid4()),
        kind="metrics",
        data={"rmse": 0.2},
    )
    base = f"/internal/attempts/{c.scope[2]}/training/metadata"
    writes = []
    native_batch = c.native.log_batch

    def write(*args, **kwargs):
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db:
            command = db.scalars(
                select(WorkflowCommand).where(
                    WorkflowCommand.operation == control.OPERATION,
                    WorkflowCommand.resource_id == c.scope[1],
                    WorkflowCommand.status == CommandStatus.RUNNING,
                )
            ).one()
            assert command.status == CommandStatus.RUNNING
            assert command.request_payload["target"] == args[0]
        writes.append((args, kwargs))
        return native_batch(*args, **kwargs)

    monkeypatch.setattr(c.native, "log_batch", write)

    def publish(payload=None):
        return c.client.post(
            base, json=body if payload is None else payload, headers=c.fresh_headers()
        )

    def receipt(payload=None):
        item = body if payload is None else payload
        return c.client.get(
            f"{base}/{item['run_id']}/receipts/{item['role']}/{item['request_id']}",
            headers=c.fresh_headers(),
        )

    return SimpleNamespace(
        c=c, body=body, publish=publish, receipt=receipt, writes=writes, parent_id=parent_id
    )


@pytest.mark.parametrize(
    "kind,data",
    [
        ("tags", {"task_type": "regression"}),
        ("params", {"model__alpha": 1, "columns": ["a", "b"]}),
        ("metrics", {"rmse": 0.2, "duration_seconds": 1.25}),
    ],
)
def test_native_metadata_is_owned_committed_once_and_receipt_replayable(metadata_api, kind, data):
    m = metadata_api
    m.body.update(kind=kind, data=data)
    response = m.publish()
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert m.publish().json() == m.receipt().json() == response.json()
    assert len(m.writes) == 1
    native = m.c.native.get_run(m.parent_id)
    if kind == "tags":
        assert native.data.tags["task_type"] == "regression"
    elif kind == "params":
        assert native.data.params == {key: str(value) for key, value in data.items()}
    else:
        assert native.data.metrics == data
        assert len(m.c.native.get_metric_history(m.parent_id, "rmse")) == 1


def test_unknown_batch_ack_remains_unresolved_without_rewriting(metadata_api, monkeypatch):
    m = metadata_api
    original = m.c.native.log_batch

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("owned native response loss")

    monkeypatch.setattr(m.c.native, "log_batch", lost)
    assert m.publish().status_code == 503
    assert m.publish().status_code == 503
    assert len(m.writes) == 1
    assert m.receipt().status_code == 503
    assert m.publish().status_code == 503
    assert len(m.writes) == 1
    assert len(m.c.native.get_metric_history(m.parent_id, "rmse")) == 1


def test_uncertain_absent_effect_is_not_reissued(metadata_api, monkeypatch):
    m = metadata_api
    calls = []

    def absent(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("owned failure before native effects")

    monkeypatch.setattr(m.c.native, "log_batch", absent)
    assert m.publish().status_code == 503
    assert m.receipt().status_code == 503
    assert m.publish().status_code == 503
    assert calls == [1]


def test_metadata_nonce_cannot_change_its_payload(metadata_api):
    m = metadata_api
    assert m.publish().status_code == 200
    changed = copy.deepcopy(m.body)
    changed["data"]["rmse"] = 999.0
    assert m.publish(changed).status_code == 401
    assert len(m.writes) == 1


@pytest.mark.parametrize("fault", ["missing", "changed"])
def test_metadata_needs_original_tracking_publication(metadata_api, fault):
    m = metadata_api
    with Session(m.c.scope[0]) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == m.c.scope[2],
                WorkflowEvent.event_key == tracking.PUBLISHED + "parent",
            )
        )
        if fault == "missing":
            db.delete(event)
        else:
            event.result_digest = "f" * 64
    assert m.publish().status_code == 401
    assert m.writes == []


def test_tracking_metadata_rejects_another_valid_producer_pod(metadata_api):
    m = metadata_api
    review, pod, _ = m.c.identity
    pod.metadata.uid = "another-current-producer"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert m.publish().status_code == 401
    assert m.writes == []


@pytest.mark.parametrize(
    "kind,data",
    [
        ("tags", {"mlflow.parentRunId": "foreign"}),
        ("tags", {"project_id": str(uuid.uuid4())}),
        ("tags", {"task_type": "classification"}),
        ("tags", {"candidate_model": "Ridge"}),
    ],
)
def test_worker_metadata_cannot_change_reserved_native_scope(metadata_api, kind, data):
    m = metadata_api
    m.body.update(kind=kind, data=data)
    assert m.publish().status_code == 401
    assert m.writes == []


def test_cancellation_during_native_write_prevents_publication(metadata_api, monkeypatch):
    m = metadata_api
    original = m.c.native.log_batch

    def cancel(*args, **kwargs):
        result = original(*args, **kwargs)
        with Session(m.c.scope[0]) as db, db.begin():
            db.get(WorkflowAttempt, m.c.scope[2]).status = AttemptStatus.CANCELLED
        return result

    monkeypatch.setattr(m.c.native, "log_batch", cancel)
    assert m.publish().status_code == 401
    with Session(m.c.scope[0]) as db:
        command = db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.operation == control.OPERATION,
                WorkflowCommand.resource_id == m.c.scope[1],
            )
        ).one()
        assert command.status == CommandStatus.RUNNING
        assert command.response_payload == {}
    assert len(m.writes) == 1


@pytest.mark.parametrize("field", ["request_hash", "resource_id", "response_payload"])
def test_metadata_receipt_rejects_changed_command_history(metadata_api, field):
    m = metadata_api
    assert m.publish().status_code == 200
    with Session(m.c.scope[0]) as db, db.begin():
        command = db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.operation == control.OPERATION,
                WorkflowCommand.resource_id == m.c.scope[1],
            )
        ).one()
        setattr(
            command,
            field,
            {"committed": True}
            if field == "response_payload"
            else uuid.uuid4()
            if field == "resource_id"
            else "f" * 64,
        )
    assert m.receipt().status_code == 401
    assert len(m.writes) == 1


def test_competing_metadata_requests_have_one_native_writer(metadata_api, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    m = metadata_api
    original = m.c.native.log_batch
    entered, release = Event(), Event()

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=15)
        return original(*args, **kwargs)

    monkeypatch.setattr(m.c.native, "log_batch", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(m.publish)
        try:
            assert entered.wait(timeout=10)
            assert m.publish().status_code == 503
        finally:
            release.set()
        assert first.result(timeout=15).status_code == 200
    assert m.receipt().status_code == 200
    assert len(m.writes) == 1


@pytest.mark.parametrize("change", ["cancel", "lease", "source", "policy"])
def test_authority_change_during_identity_prevents_metadata_effects(metadata_api, change):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    m = metadata_api
    review, _, auth = m.c.identity

    def verify(*args, **kwargs):
        assert m.c.scope[0].pool.checkedout() == 0
        with Session(m.c.scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, m.c.scope[2])
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "source":
                run = db.get(ModelRun, m.c.scope[1])
                run.params = {**run.params, "sample_tier_rows": 123}
            else:
                entry = db.scalars(
                    select(OutboxEntry).where(
                        OutboxEntry.aggregate_id == m.c.scope[1],
                        OutboxEntry.topic == "ray.training.submit",
                    )
                ).one()
                entry.payload = {**entry.payload, "credential_protocol": "ray-training-tracking-v8"}
        return review

    auth.create_token_review.side_effect = verify
    assert m.publish().status_code == 401
    assert m.writes == []


@pytest.mark.parametrize(
    "kind,data",
    [
        ("metrics", {"rmse": True}),
        ("metrics", {}),
        ("metrics", {"rmse": "0.2"}),
        ("params", {"x": "v" * 5001}),
        ("params", {"x" * 251: 1}),
        ("mirror", {"metric_count": -1}),
    ],
)
def test_invalid_metadata_is_rejected_before_identity_or_native_writes(metadata_api, kind, data):
    m = metadata_api
    calls = m.c.identity[2].create_token_review.call_count
    m.body.update(kind=kind, data=data)
    assert m.publish().status_code == 422
    assert m.c.identity[2].create_token_review.call_count == calls
    assert m.writes == []


def test_existing_tag_values_cannot_acknowledge_a_still_running_native_writer(
    metadata_api, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    m = metadata_api
    m.body.update(kind="tags", data={"task_type": "regression"})
    original = m.c.native.log_batch
    entered, release = Event(), Event()

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=15)
        return original(*args, **kwargs)

    monkeypatch.setattr(m.c.native, "log_batch", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(m.publish)
        try:
            assert entered.wait(timeout=10)
            assert m.receipt().status_code == 503
        finally:
            release.set()
        assert first.result(timeout=15).status_code == 200
    assert m.receipt().status_code == 200
    assert len(m.writes) == 1


def test_native_metadata_intent_blocks_retirement_until_writer_is_verified(
    metadata_api, monkeypatch
):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from automl_api.services.attempt_object_grants import has_unretired_attempt_credentials

    m = metadata_api
    original = m.c.native.log_batch
    entered, release = Event(), Event()

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=15)
        return original(*args, **kwargs)

    monkeypatch.setattr(m.c.native, "log_batch", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(m.publish)
        try:
            assert entered.wait(timeout=10)
            with Session(m.c.scope[0]) as db:
                assert has_unretired_attempt_credentials(db, m.c.scope[2])
        finally:
            release.set()
        assert first.result(timeout=15).status_code == 200
    with Session(m.c.scope[0]) as db:
        assert not has_unretired_attempt_credentials(db, m.c.scope[2])


@pytest.mark.parametrize("role", ["candidate:Ridge", "failure:Ridge"])
def test_selected_child_metadata_keeps_its_exact_role_and_native_target(metadata_api, role):
    m = metadata_api
    review, pod, _ = m.c.identity
    pod.metadata.uid = "owned-child-metadata-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    response = m.c.publish({**m.c.body, "role": role})
    assert response.status_code == 200, response.text
    child_id = response.json()["tracking_run_id"]
    m.body.update(role=role)
    response = m.publish()
    assert response.status_code == 200, response.text
    assert response.json()["tracking_run_id"] == child_id
    assert m.c.native.get_run(child_id).data.metrics["rmse"] == 0.2
    assert "rmse" not in m.c.native.get_run(m.parent_id).data.metrics


def test_candidate_mirror_uses_server_derived_parent_and_registry(metadata_api):
    from automl_api.models.runs import ModelRun
    from automl_api.training.pipeline import _registered_model_name

    m = metadata_api
    role = "candidate:Ridge"
    response = m.c.publish({**m.c.body, "role": role})
    assert response.status_code == 200, response.text
    child_id = response.json()["tracking_run_id"]
    m.body.update(role=role, kind="mirror", data={"metric_count": 2})
    response = m.publish()
    assert response.status_code == 200, response.text
    assert response.json()["tracking_run_id"] == child_id
    native = m.c.native.get_run(m.parent_id)
    assert native.data.tags["candidate.Ridge.run_id"] == child_id
    assert native.data.tags["candidate.Ridge.metric_count"] == "2"
    with Session(m.c.scope[0]) as db:
        assert native.data.tags["candidate.Ridge.registered_model"] == _registered_model_name(
            db.get(ModelRun, m.c.scope[1]), "Ridge"
        )
    assert len(m.writes) == 1


@pytest.mark.parametrize(
    "kind,data",
    [
        ("params", {"weights": {"z": 2, "a": 1e20}, "zero": -0.0, "small": 1e-20}),
        ("metrics", {"large": 1e20, "zero": -0.0, "small": 1e-20}),
    ],
)
def test_marker_recovery_preserves_jsonb_numeric_and_mapping_representations(
    metadata_api, monkeypatch, kind, data
):
    m = metadata_api
    m.body.update(kind=kind, data=data)
    original = m.c.native.set_tag

    def lost(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("owned marker response loss after numeric metadata")

    monkeypatch.setattr(m.c.native, "set_tag", lost)
    assert m.publish().status_code == 503
    assert m.receipt().status_code == m.publish().status_code == 200
    assert len(m.writes) == 1


def test_sql_connection_loss_after_native_settlement_recovers_without_another_write(metadata_api):
    from sqlalchemy import event, text

    m = metadata_api
    killed = []

    def before_flush(db, *_args):
        if any(
            isinstance(row, WorkflowCommand)
            and row.operation == control.OPERATION
            and row.status == CommandStatus.SUCCEEDED
            for row in db.dirty
        ):
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with m.c.scope[0].connect().execution_options(isolation_level="AUTOCOMMIT") as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(Session, "before_flush", before_flush)
    try:
        assert m.publish().status_code == 503
    finally:
        event.remove(Session, "before_flush", before_flush)
    assert len(killed) == 1
    assert m.receipt().status_code == 200
    assert len(m.writes) == 1
    assert len(m.c.native.get_metric_history(m.parent_id, "rmse")) == 1


def test_integer_metric_outside_native_range_is_rejected_before_identity(metadata_api):
    m = metadata_api
    before = m.c.identity[2].create_token_review.call_count
    m.body["data"] = {"rmse": 10**400}
    assert m.publish().status_code == 422
    assert m.c.identity[2].create_token_review.call_count == before
    assert m.writes == []


def test_v9_model_write_requires_original_metadata_readiness(model_api):
    from sqlalchemy import delete

    c = model_api
    c.ensure_tracking()
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == "training.metadata.ready.candidate:" + c.name,
            )
        )
    assert c.upload().status_code == 401
    assert c.writes == []


@pytest.mark.parametrize("role", ["parent", "candidate"])
def test_v9_final_publication_requires_original_metadata_readiness(result_api, role):
    from sqlalchemy import delete

    c, _, publish, _ = result_api
    key = "parent" if role == "parent" else "candidate:" + c.name
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == "training.metadata.ready." + key,
            )
        )
    assert publish().status_code == 401


@pytest.mark.parametrize(
    "field",
    [
        "attempt_id",
        "command_id",
        "role",
        "tracking_run_id",
        "pod_uid",
        "intent_digest",
        "receipt_digest",
        "missing",
    ],
)
def test_model_slot_metadata_snapshot_cannot_be_rehashed_to_another_source(result_api, field):
    from automl_api.services.workflow_state import canonical_request_hash

    c, _, publish, _ = result_api
    with Session(c.scope[0]) as db, db.begin():
        slot = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.candidate_id}",
            )
        )
        payload = copy.deepcopy(slot.payload)
        if field == "missing":
            payload.pop("metadata_receipt")
        else:
            payload["metadata_receipt"][field] = (
                str(uuid.uuid4())
                if field in {"attempt_id", "command_id"}
                else "failure:Ridge"
                if field == "role"
                else "f" * 64
            )
        slot.payload = payload
        slot.result_digest = canonical_request_hash(payload)
    assert c.receipt().status_code == publish().status_code == 401
    assert len(c.writes) == 1


def test_stored_result_cannot_rebind_to_another_valid_parent_metadata_command(result_api):
    from automl_api.models.runs import ModelRun
    from automl_api.services.workflow_state import canonical_request_hash

    c, _, publish, receipt = result_api
    with Session(c.scope[0]) as db:
        task_type = db.get(ModelRun, c.scope[1]).task_type.value
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/metadata",
        json={
            "run_id": str(c.scope[1]),
            "role": "parent",
            "request_id": str(uuid.uuid4()),
            "kind": "tags",
            "data": {"task_type": task_type},
        },
        headers=c.headers,
    )
    assert response.status_code == 200, response.text
    other_id = uuid.UUID(response.json()["command_id"])
    assert publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == control.READY + "parent",
            )
        )
        other = db.get(WorkflowCommand, other_id)
        assert event.payload["command_id"] != str(other_id)
        event.payload = control._ready_payload(other, other.request_payload)
        event.result_digest = canonical_request_hash(event.payload)
    assert publish().status_code == receipt().status_code == 401


@pytest.mark.parametrize("model_api", ["saved-v8"], indirect=True)
def test_saved_v8_model_custody_keeps_its_original_policy_without_metadata_ready(model_api):
    c = model_api
    assert c.upload().status_code == 200
    assert c.receipt().status_code == 200
    with Session(c.scope[0]) as db:
        assert (
            list(
                db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.scope[2],
                        WorkflowEvent.event_key.startswith(control.READY),
                    )
                )
            )
            == []
        )
        slot = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == f"training.model.grant.{c.candidate_id}",
            )
        )
        assert "metadata_receipt" not in slot.payload


def test_token_expiry_after_native_io_prevents_metadata_acknowledgement(metadata_api, monkeypatch):
    from datetime import UTC, datetime, timedelta

    m = metadata_api
    original = m.c.native.log_batch
    future = datetime.now(UTC) + timedelta(minutes=20)

    class ExpiredClock:
        @staticmethod
        def now(tz=None):
            return future

    def expire(*args, **kwargs):
        result = original(*args, **kwargs)
        monkeypatch.setattr(tracking, "datetime", ExpiredClock)
        return result

    monkeypatch.setattr(m.c.native, "log_batch", expire)
    assert m.publish().status_code == 401
    with Session(m.c.scope[0]) as db:
        command = db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.operation == control.OPERATION,
                WorkflowCommand.resource_id == m.c.scope[1],
            )
        ).one()
        assert command.status == CommandStatus.RUNNING and command.response_payload == {}
        assert control._verified(command)
    assert len(m.writes) == 1
