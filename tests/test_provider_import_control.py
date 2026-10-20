"""Real HTTP control binds pending verification to one authenticated publisher pod."""

import uuid
from datetime import UTC, datetime
from unittest.mock import Mock

import jwt
import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetProviderImport, DatasetVersion
from automl_api.models.enums import AttemptStatus, CommandStatus, DatasetStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services.attempt_object_grants import revoke_attempt_grants
from automl_api.services.provider_copy_verification import verify_provider_objects
from automl_api.services.provider_import import claim_provider_import
from automl_api.services.workflow_state import canonical_request_hash
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from test_provider_import_attempts import bind_identity
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def control_case(pending_case, monkeypatch):
    c = pending_case
    c.execution = claim_provider_import(
        c.factory, c.version_id, policy=c.policy, public_key=c.key.public_key()
    )
    c.proof = verify_provider_objects(c.value, c.store, c.limits)
    bind_identity(c, monkeypatch)
    c.k8s._configured = True
    app = FastAPI()
    app.include_router(attempt_control.router)
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    c.store_factory = Mock(return_value=c.store)
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.k8s)
    monkeypatch.setattr(attempt_control, "get_object_store", c.store_factory)
    c.base = f"/internal/attempts/{c.attempt_id}/provider-import/"
    with TestClient(app) as api:
        c.api = api
        yield c


def send(c, action, **kwargs):
    return c.api.post(c.base + action, headers={"Authorization": f"Bearer {c.token}"}, **kwargs)


def ready(c):
    reply = send(c, "result", json=c.proof.model_dump(mode="json"))
    assert reply.status_code == 200, reply.text
    with c.factory() as db:
        assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.READY
        assert db.get(WorkflowAttempt, c.attempt_id).status == AttemptStatus.SUCCEEDED
        assert db.get(WorkflowCommand, c.command_id).status == CommandStatus.SUCCEEDED
    return reply


def test_start_replay_heartbeat_and_publication_use_one_durable_publisher(control_case):
    c = control_case
    first, replay = send(c, "start"), send(c, "start")
    assert first.status_code == replay.status_code == 200
    assert first.json() == replay.json()
    assert first.headers["cache-control"] == "no-store"
    assert first.json()["manifest"] == c.value.model_dump(mode="json")
    assert first.json()["budget"] == c.limits.model_dump(mode="json")
    assert send(c, "heartbeat").status_code == 200
    c.store_factory.assert_not_called()
    with c.factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowEvent)
                .where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.event_key == "provider_import.publisher",
                )
            )
            == 1
        )
    ready(c)
    c.store_factory.assert_not_called()
    assert c.store.writes == []


@pytest.mark.parametrize("action", ["heartbeat", "result", "failure"])
def test_control_requires_a_started_publisher(control_case, action):
    c = control_case
    payload = (
        c.proof.model_dump(mode="json")
        if action == "result"
        else ({"code": "invalid_copy"} if action == "failure" else None)
    )
    assert send(c, action, json=payload).status_code == 401
    c.store_factory.assert_not_called()
    assert send(c, "start").status_code == 200
    ready(c)


def test_another_valid_pod_cannot_adopt_the_started_execution(control_case, monkeypatch):
    c = control_case
    assert send(c, "start").status_code == 200
    bind_identity(c, monkeypatch)
    c.k8s._configured = True
    c.token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 601, "jti": str(uuid.uuid4())},
        "unit-import-token-at-least-32-bytes",
        algorithm="HS256",
    )
    for action in ("start", "heartbeat", "result"):
        kwargs = {"json": c.proof.model_dump(mode="json")} if action == "result" else {}
        assert send(c, action, **kwargs).status_code == 401
    c.store_factory.assert_not_called()


def test_changed_request_budget_cannot_rebind_the_started_execution(control_case):
    c = control_case
    assert send(c, "start").status_code == 200
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        original, digest = command.request_payload, command.request_hash
        command.request_payload = {
            **original,
            "budget": {
                **original["budget"],
                "timeout_seconds": original["budget"]["timeout_seconds"] + 1,
            },
        }
        command.request_hash = canonical_request_hash(command.request_payload)
    assert send(c, "heartbeat").status_code == 401
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    c.store_factory.assert_not_called()
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        command.request_payload, command.request_hash = original, digest
    ready(c)


def test_revoked_publisher_cannot_renew_or_publish(control_case):
    c = control_case
    assert send(c, "start").status_code == 200
    with c.factory() as db, db.begin():
        revoke_attempt_grants(db, c.attempt_id)
    assert send(c, "heartbeat").status_code == 401
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    c.store_factory.assert_not_called()


def test_pod_loss_during_publication_authorization_cannot_mark_ready(control_case):
    c = control_case
    assert send(c, "start").status_code == 200
    pod = c.k8s.core.read_namespaced_pod("verifier", "unused")
    review = c.auth.create_token_review.return_value

    def lose_pod(*_args, **_kwargs):
        assert c.engine.pool.checkedout() == 0
        pod.metadata.deletion_timestamp = datetime.now(UTC)
        return review

    c.auth.create_token_review.side_effect = lose_pod
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    with c.factory() as db:
        assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.UPLOADED
        assert db.get(DatasetProviderImport, c.import_id).status == "pending"
    c.store_factory.assert_not_called()
    assert c.store.writes == []


@pytest.mark.parametrize("code,terminal", [("invalid_copy", True), ("verifier_unavailable", False)])
def test_failure_reports_use_the_existing_single_retry_owner(control_case, code, terminal):
    c = control_case
    assert send(c, "start").status_code == 200
    assert send(c, "failure", json={"code": code}).status_code == 200
    with c.factory() as db:
        command = db.get(WorkflowCommand, c.command_id)
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert command.retry_count == 1
        assert command.status == (CommandStatus.FAILED if terminal else CommandStatus.PENDING)
        assert attempt.status == (AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED)
        assert command.lease_owner is None and attempt.lease_owner is None
    c.store_factory.assert_not_called()


@pytest.mark.parametrize(
    "action,body",
    [
        ("start", {"mode": "raw"}),
        ("heartbeat", {"owner": "other"}),
        ("failure", {"code": "invalid_copy", "secret": "never-log"}),
        ("failure", {"code": "arbitrary"}),
        ("result", {"uri": "s3://raw"}),
    ],
)
def test_execution_arguments_and_arbitrary_metadata_are_rejected(control_case, action, body):
    c = control_case
    assert send(c, action, json=body).status_code == 422
    c.store_factory.assert_not_called()


def test_oversized_metadata_is_rejected_before_authentication(control_case):
    c = control_case
    c.auth.reset_mock()
    assert send(c, "result", content=b"x" * (1024**2 + 1)).status_code == 413
    c.auth.create_token_review.assert_not_called()
    c.store_factory.assert_not_called()


def test_publication_with_production_autoflush_disabled(control_case):
    c = control_case
    c.factory.configure(autoflush=False)
    assert send(c, "start").status_code == 200
    response = ready(c)
    assert send(c, "result", json=c.proof.model_dump(mode="json")).json() == response.json()
    with c.factory() as db:
        events = db.scalars(
            select(WorkflowEvent)
            .where(WorkflowEvent.attempt_id == c.attempt_id)
            .order_by(WorkflowEvent.sequence)
        ).all()
        sequences = [e.sequence for e in events]
        assert sequences == list(range(1, len(events) + 1))
        keys = [e.event_key for e in events]
        assert keys.count("attempt.credentials.revoked") == 1
        assert keys.count("provider_import.receipt") == 1
        assert keys.index("attempt.credentials.revoked") < keys.index("provider_import.receipt")
    c.store_factory.assert_not_called()
