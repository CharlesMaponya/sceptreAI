"""Owned SQL and real MLflow storage qualify tracking creation control authority."""

import copy
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import mlflow
import pytest
from automl_api.models.enums import AttemptStatus, CommandStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    TrainingCandidate,
    WorkflowAttempt,
    WorkflowCommand,
    WorkflowEvent,
)
from automl_api.services import training_tracking as control
from automl_api.services.workflow_state import canonical_request_hash
from mlflow import MlflowClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from test_training_model_output_api import bound_run as bound_run
from test_training_model_output_api import capacity_client as capacity_client
from test_training_model_output_api import committed_tenant as committed_tenant
from test_training_model_output_api import context_api as context_api
from test_training_model_output_api import phase1_engine as phase1_engine
from test_training_model_output_api import selected_scope as selected_scope


@pytest.fixture
def tracking_api(context_api, monkeypatch, tmp_path):
    scope, _, client, identity = context_api
    engine, run_id, attempt_id, fence, project_id = scope
    uri = f"sqlite:///{tmp_path / 'tracking.db'}"
    native = MlflowClient(tracking_uri=uri)
    create_experiment = native.create_experiment
    monkeypatch.setattr(
        native,
        "create_experiment",
        lambda name: create_experiment(name, artifact_location=(tmp_path / "artifacts").as_uri()),
    )
    monkeypatch.setattr(control, "get_settings", lambda: SimpleNamespace(mlflow_tracking_uri=uri))
    monkeypatch.setattr(
        control,
        "MlflowClient",
        lambda *, tracking_uri: (
            native if tracking_uri == uri else pytest.fail("Tracking backend changed")
        ),
    )
    token = identity[2].create_token_review.call_args.args[0].spec.token
    headers = {"Authorization": "Bearer " + token}
    base = f"/internal/attempts/{attempt_id}/training/tracking"
    body = {"run_id": str(run_id), "role": "parent"}
    from threading import local

    from sqlalchemy import event

    activity = local()

    def checkout(*args):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*args):
        activity.connections -= 1

    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)
    calls = []
    created = []
    for name in [
        "get_experiment_by_name",
        "create_experiment",
        "search_runs",
        "create_run",
        "get_run",
    ]:
        original = getattr(native, name)

        def operation(*args, _name=name, _original=original, **kwargs):
            assert getattr(activity, "connections", 0) == 0, _name
            with Session(engine) as db:
                owner = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt_id,
                        WorkflowEvent.event_key.startswith(control.OWNER),
                    )
                )
                assert owner is not None
            calls.append(_name)
            result = _original(*args, **kwargs)
            if _name == "create_run":
                created.append(result.info.run_id)
            return result

        monkeypatch.setattr(native, name, operation)

    def fresh_headers():
        import time

        import jwt

        token = jwt.encode(
            {"exp": int(time.time()) + 600, "pod_uid": identity[1].metadata.uid},
            "tracking-fixture-key-at-least-32-bytes",
            algorithm="HS256",
        )
        return {"Authorization": "Bearer " + token}

    def publish(payload=None):
        return client.post(base, json=body if payload is None else payload, headers=fresh_headers())

    def receipt(payload=None):
        item = body if payload is None else payload
        return client.get(
            f"{base}/{item['run_id']}/receipts/{item['role']}", headers=fresh_headers()
        )

    yield SimpleNamespace(
        scope=scope,
        client=client,
        identity=identity,
        native=native,
        uri=uri,
        body=body,
        publish=publish,
        receipt=receipt,
        calls=calls,
        created=created,
        headers=headers,
        fresh_headers=fresh_headers,
    )

    event.remove(engine, "checkout", checkout)
    event.remove(engine, "checkin", checkin)
    from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore

    owned_engine = SqlAlchemyStore._engine_map.pop(uri, None)
    if owned_engine is not None:
        owned_engine.dispose()
    for suffix in ("", "-journal", "-wal", "-shm"):
        (tmp_path / ("tracking.db" + suffix)).unlink(missing_ok=True)


def test_parent_tracking_is_server_named_and_replays_one_native_run(tracking_api):
    c = tracking_api
    assert mlflow.active_run() is None
    response = c.publish()
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    result = response.json()
    assert result["request_digest"] == canonical_request_hash(c.body)
    assert result["tracking_backend_hash"] == hashlib.sha256(c.uri.encode()).hexdigest()
    assert c.publish().json() == c.receipt().json() == result
    assert len(c.created) == 1
    native = c.native.get_run(result["tracking_run_id"])
    with Session(c.scope[0]) as db:
        run = db.get(ModelRun, c.scope[1])
        assert run.mlflow_run_id == result["tracking_run_id"]
        assert native.info.run_name == (run.run_name or str(run.id))
        assert native.data.tags["task_type"] == run.task_type.value
    assert native.data.tags["project_id"] == str(c.scope[4])
    assert native.data.tags["automl_tracking_role"] == "parent"
    assert mlflow.active_run() is None


@pytest.mark.parametrize("kind", ["candidate", "failure"])
def test_selected_candidate_roles_have_the_verified_parent_and_server_tags(tracking_api, kind):
    c = tracking_api
    parent = c.publish().json()["tracking_run_id"]
    review, pod, _ = c.identity
    pod.metadata.uid = "separate-valid-candidate-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    body = {**c.body, "role": f"{kind}:Ridge"}
    response = c.publish(body)
    assert response.status_code == 200, response.text
    assert c.receipt(body).json() == response.json()
    native = c.native.get_run(response.json()["tracking_run_id"])
    assert native.info.run_name == "Ridge"
    assert native.data.tags["mlflow.parentRunId"] == parent
    assert native.data.tags["candidate_model"] == "Ridge"
    assert (native.data.tags.get("candidate_status") == "failed") == (kind == "failure")
    assert len(c.created) == 2


def test_unfinished_parent_is_required_before_candidate_creation(tracking_api):
    c = tracking_api
    assert c.publish({**c.body, "role": "candidate:Ridge"}).status_code == 503
    assert not c.created


def test_separate_candidate_pod_cannot_create_or_read_parent_tracking(tracking_api):
    c = tracking_api
    assert c.publish().status_code == 200
    review, pod, _ = c.identity
    pod.metadata.uid = "another-valid-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert c.publish().status_code == c.receipt().status_code == 401
    assert len(c.created) == 1


@pytest.mark.parametrize("action", ["post", "receipt"])
def test_tracking_role_is_bound_to_the_original_candidate_pod(tracking_api, action):
    c = tracking_api
    assert c.publish().status_code == 200
    body = {**c.body, "role": "candidate:Ridge"}
    assert c.publish(body).status_code == 200
    review, pod, _ = c.identity
    pod.metadata.uid = "replacement-candidate-pod"
    review.status.user.extra["authentication.kubernetes.io/pod-uid"] = [pod.metadata.uid]
    assert (c.publish(body) if action == "post" else c.receipt(body)).status_code == 401
    assert len(c.created) == 2


@pytest.mark.parametrize(
    "fault",
    [
        "unknown_role",
        "unknown_candidate",
        "extra",
        "backend",
        "parent",
        "name",
        "tags",
        "foreign_run",
        "too_long",
        "null_role",
    ],
)
def test_invalid_tracking_requests_never_create_native_resources(tracking_api, fault):
    c = tracking_api
    body = copy.deepcopy(c.body)
    if fault == "unknown_role":
        body["role"] = "delete:parent"
    elif fault == "unknown_candidate":
        assert c.publish().status_code == 200
        body["role"] = "candidate:NeverSelected"
    elif fault == "foreign_run":
        body["run_id"] = str(uuid.uuid4())
    elif fault == "too_long":
        body["role"] = "candidate:" + "R" * 161
    elif fault == "null_role":
        body["role"] = None
    else:
        body[
            {
                "extra": "DATABASE_URL",
                "backend": "tracking_uri",
                "parent": "parent_run_id",
                "name": "run_name",
                "tags": "tags",
            }[fault]
        ] = "never-accepted"
    count = len(c.created)
    assert c.publish(body).status_code in {401, 422}
    assert len(c.created) == count


@pytest.mark.parametrize(
    "change", ["cancel", "revoke", "lease", "source", "candidate", "policy", "project", "attempt"]
)
def test_changed_authority_during_token_review_cannot_create_tracking(tracking_api, change):
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.evaluation_publication import _append

    c = tracking_api
    review, pod, auth = c.identity

    def verify(*args, **kwargs):
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.scope[2], with_for_update=True)
            run = db.get(ModelRun, c.scope[1], with_for_update=True)
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "revoke":
                _append(db, attempt, "attempt.credentials.revoked", {})
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "source":
                run.params = {**run.params, "sample_tier_rows": 123}
            elif change == "candidate":
                row = db.scalar(
                    select(TrainingCandidate).where(TrainingCandidate.model_run_id == run.id)
                )
                row.params = {"alpha": 123}
            elif change == "policy":
                row = db.scalar(
                    select(OutboxEntry).where(
                        OutboxEntry.aggregate_id == run.id,
                        OutboxEntry.topic == "ray.training.submit",
                    )
                )
                row.payload = {**row.payload, "credential_protocol": "ray-training-progress-v7"}
            else:
                pod.metadata.labels[f"automl.platform/{change}-id"] = str(uuid.uuid4())
        return review

    auth.create_token_review.side_effect = verify
    assert c.publish().status_code == 401
    assert not c.created


@pytest.mark.parametrize("change", ["cancel", "lease", "candidate", "source", "owner"])
def test_authority_changed_during_native_lookup_prevents_the_next_write(
    tracking_api, monkeypatch, change
):
    c = tracking_api
    original = c.native.search_runs

    def search(*args, **kwargs):
        result = original(*args, **kwargs)
        with Session(c.scope[0]) as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.scope[2])
            run = db.get(ModelRun, c.scope[1])
            if change == "cancel":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "candidate":
                db.scalar(
                    select(TrainingCandidate).where(TrainingCandidate.model_run_id == run.id)
                ).params = {"alpha": 123}
            elif change == "source":
                run.params = {**run.params, "sample_tier_rows": 123}
            else:
                owner = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == control.OWNER + "parent",
                    )
                )
                owner.payload = {**owner.payload, "pod_uid": "changed-producer"}
                owner.result_digest = canonical_request_hash(owner.payload)
        return result

    monkeypatch.setattr(c.native, "search_runs", search)
    assert c.publish().status_code == 401
    assert not c.created


def test_lost_http_acknowledgement_recovers_by_receipt_only(tracking_api, monkeypatch):
    c = tracking_api
    original = control.control_training_tracking

    def lost(*args, **kwargs):
        result = original(*args, **kwargs)
        if not kwargs.get("receipt_only"):
            raise OSError("owned committed HTTP acknowledgement loss")
        return result

    monkeypatch.setattr(control, "control_training_tracking", lost)
    assert c.publish().status_code == 503
    assert c.receipt().status_code == 200
    assert len(c.created) == 1


@pytest.mark.parametrize("created", [True, False])
def test_unknown_native_creation_is_never_reissued(tracking_api, monkeypatch, created):
    c = tracking_api
    original = c.native.create_run
    calls = []

    def unknown(*args, **kwargs):
        calls.append(1)
        if created:
            original(*args, **kwargs)
        raise ConnectionError("owned uncertain native creation")

    monkeypatch.setattr(c.native, "create_run", unknown)
    assert c.publish().status_code == 503
    assert c.receipt().status_code == 503
    response = c.publish()
    assert response.status_code == (200 if created else 503), response.text
    assert len(calls) == 1
    assert len(c.created) == int(created)


def test_replay_refuses_changed_native_tracking_lineage(tracking_api):
    c = tracking_api
    tracking_id = c.publish().json()["tracking_run_id"]
    c.native.set_tag(tracking_id, "automl_run_id", str(uuid.uuid4()))
    assert c.publish().status_code == 401
    assert len(c.created) == 1


def test_lost_database_commit_acknowledgement_recovers_the_known_id(tracking_api):
    from automl_api.db.session import get_session_factory

    c = tracking_api
    lost = []

    class LostAcknowledgement(Session):
        def commit(self):
            published = any(
                isinstance(row, WorkflowCommand)
                and row.operation == "mlflow.run.ensure"
                and row.status == CommandStatus.SUCCEEDED
                for row in self.identity_map.values()
            )
            super().commit()
            if published and not lost:
                lost.append(True)
                raise ConnectionError("owned committed SQL acknowledgement loss")

    c.client.app.dependency_overrides[get_session_factory] = lambda: sessionmaker(
        c.scope[0], class_=LostAcknowledgement, expire_on_commit=False
    )
    assert c.publish().status_code == 503
    assert lost == [True]
    assert c.receipt().status_code == 200
    assert len(c.created) == 1


def test_real_sql_connection_loss_recovers_from_known_native_outcome_without_recreate(tracking_api):
    from automl_api.services.tracking_reconciliation import reconcile_tracking_once
    from sqlalchemy import event, text

    c = tracking_api
    killed = []

    def before_commit(db):
        db.flush()
        if any(
            isinstance(row, WorkflowCommand)
            and row.operation == "mlflow.run.ensure"
            and row.status == CommandStatus.SUCCEEDED
            for row in db.identity_map.values()
        ):
            pid = db.scalar(text("SELECT pg_backend_pid()"))
            with c.scope[0].connect().execution_options(isolation_level="AUTOCOMMIT") as other:
                assert other.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(Session, "before_commit", before_commit)
    try:
        assert c.publish().status_code == 503
    finally:
        event.remove(Session, "before_commit", before_commit)
    assert len(killed) == 1 and len(c.created) == 1
    assert c.receipt().status_code == 503
    factory = sessionmaker(c.scope[0], expire_on_commit=False)
    assert reconcile_tracking_once(factory, client=c.native, limit=1) == 1
    response = c.receipt()
    assert response.status_code == 200, response.text
    assert response.json()["tracking_run_id"] == c.created[0]
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).mlflow_run_id == c.created[0]


def test_concurrent_parent_creation_keeps_one_native_run(tracking_api):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    c = tracking_api
    barrier = Barrier(2)

    def create(_):
        barrier.wait(timeout=10)
        return c.publish()

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(create, range(2)))
    assert any(response.status_code == 200 for response in responses)
    assert all(response.status_code in {200, 503} for response in responses)
    assert len(c.created) == 1
    assert c.receipt().status_code == 200


def test_native_identity_and_tracking_io_hold_no_caller_sql_locks(tracking_api, monkeypatch):
    c = tracking_api
    review, _, auth = c.identity

    def unlocked():
        assert c.scope[0].pool.checkedout() == 0
        with Session(c.scope[0]) as db, db.begin():
            db.execute(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == c.scope[2])
                .with_for_update(nowait=True)
            )
            db.execute(
                select(ModelRun).where(ModelRun.id == c.scope[1]).with_for_update(nowait=True)
            )

    def verify(*args, **kwargs):
        unlocked()
        return review

    auth.create_token_review.side_effect = verify
    for name in [
        "get_experiment_by_name",
        "create_experiment",
        "search_runs",
        "create_run",
        "get_run",
    ]:
        original = getattr(c.native, name)

        def operation(*args, _original=original, **kwargs):
            unlocked()
            return _original(*args, **kwargs)

        monkeypatch.setattr(c.native, name, operation)
    assert c.publish().status_code == 200
    assert c.publish().status_code == 200
    assert len(c.created) == 1


@pytest.mark.parametrize(
    "change",
    ["command_id", "request_hash", "tracking_run_id", "parent_tracking_run_id", "owner_digest"],
)
def test_changed_published_tracking_receipt_is_never_disclosed(tracking_api, change):
    c = tracking_api
    assert c.publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == control.PUBLISHED + "parent",
            )
        )
        event.payload = {**event.payload, change: str(uuid.uuid4())}
        event.result_digest = canonical_request_hash(event.payload)
    count = len(c.created)
    assert c.receipt().status_code == 401
    assert len(c.created) == count


def test_changed_command_native_id_conflicts_with_its_publication_receipt(tracking_api):
    c = tracking_api
    assert c.publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.resource_id == c.scope[1],
                WorkflowCommand.operation == "mlflow.run.ensure",
            )
        )
        command.response_payload = {"run_id": "f" * 32}
    assert c.receipt().status_code == 401
    assert len(c.created) == 1


def test_missing_ack_recovers_only_a_verified_existing_native_id(tracking_api):
    from sqlalchemy import delete

    c = tracking_api
    original = c.publish().json()
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == control.PUBLISHED + "parent",
            )
        )
    count = c.calls.count("get_run")
    assert c.receipt().json() == original
    assert c.calls.count("get_run") == count + 1
    assert len(c.created) == 1


def test_native_get_tamper_cannot_repair_a_missing_ack(tracking_api):
    from sqlalchemy import delete

    c = tracking_api
    tracking_id = c.publish().json()["tracking_run_id"]
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key == control.PUBLISHED + "parent",
            )
        )
    c.native.set_tag(tracking_id, "automl_run_id", str(uuid.uuid4()))
    assert c.receipt().status_code == 401
    assert len(c.created) == 1


def test_tracking_creation_receipt_works_with_production_autoflush_disabled(tracking_api):
    from automl_api.db.session import get_session_factory

    c = tracking_api
    c.client.app.dependency_overrides[get_session_factory] = lambda: sessionmaker(
        c.scope[0], autoflush=False, expire_on_commit=False
    )
    response = c.publish()
    assert response.status_code == 200, response.text
    assert c.receipt().json() == response.json()
    with Session(c.scope[0]) as db:
        assert db.get(ModelRun, c.scope[1]).mlflow_run_id == response.json()["tracking_run_id"]


def test_tracking_metadata_transport_is_bounded_before_native_work(tracking_api):
    c = tracking_api
    response = c.client.post(
        f"/internal/attempts/{c.scope[2]}/training/tracking", content=b" " * 4097, headers=c.headers
    )
    assert response.status_code == 413 and response.headers["cache-control"] == "no-store"
    assert not c.created


@pytest.mark.parametrize("change", ["deleted", "lineage", "nested_parent"])
def test_candidate_creation_requires_current_native_parent_lineage(tracking_api, change):
    c = tracking_api
    parent_id = c.publish().json()["tracking_run_id"]
    if change == "deleted":
        c.native.delete_run(parent_id)
    elif change == "lineage":
        c.native.set_tag(parent_id, "automl_run_id", str(uuid.uuid4()))
    else:
        c.native.set_tag(parent_id, "mlflow.parentRunId", "foreign-parent")
    response = c.publish({**c.body, "role": "candidate:Ridge"})
    assert response.status_code == 401, response.text
    assert len(c.created) == 1


def test_existing_tracking_without_original_pod_owner_requires_controlled_recovery(tracking_api):
    from sqlalchemy import delete

    c = tracking_api
    assert c.publish().status_code == 200
    with Session(c.scope[0]) as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.scope[2],
                WorkflowEvent.event_key.in_(
                    [control.OWNER + "parent", control.PUBLISHED + "parent"]
                ),
            )
        )
    assert c.publish().status_code == 401
    assert len(c.created) == 1
    with Session(c.scope[0]) as db:
        assert (
            db.scalar(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.scope[2],
                    WorkflowEvent.event_key == control.OWNER + "parent",
                )
            )
            is None
        )
