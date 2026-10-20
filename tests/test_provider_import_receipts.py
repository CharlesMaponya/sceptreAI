"""Committed verification receipts never restore a terminal attempt's data authority."""

import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import provider_import
from automl_api.services.workflow_state import canonical_request_hash
from kubernetes.client import ApiException
from sqlalchemy import select
from test_provider_import_attempts import bind_identity
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_control import control_case as control_case  # noqa: F401
from test_provider_import_control import ready, send
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def receipt_case(control_case):
    c = control_case
    assert send(c, "start").status_code == 200
    c.response = ready(c).json()
    c.remote_before = (list(c.store.calls), list(c.store.writes))
    c.store_factory.reset_mock()
    return c


def replay(c):
    for action in ("result", "receipt"):
        response = send(c, action, json=c.proof.model_dump(mode="json"))
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        assert response.json() == c.response
    c.store_factory.assert_not_called()
    assert (c.store.calls, c.store.writes) == c.remote_before
    return response


def test_uncommitted_receipt_lookup_never_repeats_publication(control_case):
    c = control_case
    for started in (False, True):
        if started:
            assert send(c, "start").status_code == 200
        assert send(c, "receipt", json=c.proof.model_dump(mode="json")).status_code == 401
        c.store_factory.assert_not_called()
        assert not c.store.writes
    ready(c)


def test_predecessor_receipt_cannot_be_adopted_by_a_later_generation(receipt_case):
    c = receipt_case
    with c.factory() as db, db.begin():
        previous = db.get(WorkflowAttempt, c.attempt_id)
        replacement = WorkflowAttempt(
            project_id=c.project_id,
            dataset_version_id=c.version_id,
            stage=previous.stage,
            logical_key=previous.logical_key,
            workload_identity=previous.workload_identity,
            predecessor_attempt_id=previous.id,
            generation=previous.generation + 1,
            fencing_token=uuid.uuid4().hex,
        )
        db.add(replacement)
        db.flush()
        replacement_id = replacement.id
    assert send(c, "receipt", json=c.proof.model_dump(mode="json")).status_code == 401
    with c.factory() as db, db.begin():
        db.delete(db.get(WorkflowAttempt, replacement_id))
    replay(c)


def test_exact_terminal_receipt_replays_without_restoring_data_authority(receipt_case):
    c = receipt_case
    replay(c)
    replay(c)
    for action in ("start", "heartbeat", "failure"):
        data = {"code": "verifier_unavailable"} if action == "failure" else None
        assert send(c, action, json=data).status_code == 401
    base = f"/internal/attempts/{c.attempt_id}/"
    headers = {"Authorization": f"Bearer {c.token}"}
    assert c.api.post(base + "admission", headers=headers).status_code == 401
    assert (
        c.api.post(
            base + "objects/read",
            headers=headers,
            json={
                "request_id": str(uuid.uuid4()),
                "role": "train",
                "object_index": 0,
            },
        ).status_code
        == 401
    )
    replay(c)


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest_digest", "0" * 64),
        ("rows", {"train": 2, "validation": 999}),
        ("verified_at", datetime.now(UTC) + timedelta(days=1)),
    ],
)
def test_receipt_requires_the_exact_committed_proof(receipt_case, field, value):
    c = receipt_case
    data = c.proof.model_copy(update={field: value}).model_dump(mode="json")
    assert send(c, "result", json=data).status_code == 401
    replay(c)


def test_another_authenticated_pod_cannot_recover_the_receipt(receipt_case, monkeypatch):
    c = receipt_case
    previous = c.k8s, c.auth, c.token
    bind_identity(c, monkeypatch)
    c.k8s._configured = True
    c.token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 601, "jti": str(uuid.uuid4())},
        "unit-import-token-at-least-32-bytes",
        algorithm="HS256",
    )
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    c.k8s, c.auth, c.token = previous
    replay(c)


@pytest.mark.parametrize(
    "event_key,fault",
    [
        ("provider_import.receipt", "request"),
        ("provider_import.receipt", "response"),
        ("provider_import.receipt", "digest"),
        ("provider_import.publisher", "pod"),
        ("provider_import.receipt", "missing"),
    ],
)
def test_missing_or_self_rehashed_changed_evidence_cannot_disclose_receipt(
    receipt_case, event_key, fault
):
    c = receipt_case
    with c.factory() as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key == event_key,
            )
        )
        event_id = event.id
        original = event.payload, event.result_digest, event.event_key
        if fault == "missing":
            event.event_key = "unavailable.receipt"
        elif fault == "digest":
            event.result_digest = "0" * 64
        else:
            changes = (
                {"request_digest": "0" * 64}
                if fault == "request"
                else (
                    {"pod_uid": str(uuid.uuid4())}
                    if fault == "pod"
                    else {"response": {"dataset_version_id": str(uuid.uuid4())}}
                )
            )
            event.payload = {**event.payload, **changes}
            event.result_digest = canonical_request_hash(event.payload)
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    with c.factory() as db, db.begin():
        event = db.get(WorkflowEvent, event_id)
        event.payload, event.result_digest, event.event_key = original
    replay(c)


def test_self_rehashed_command_rewrite_cannot_rebind_a_receipt(receipt_case):
    c = receipt_case
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        original = command.request_payload, command.request_hash
        command.request_payload = {
            **command.request_payload,
            "budget": {
                **command.request_payload["budget"],
                "timeout_seconds": command.request_payload["budget"]["timeout_seconds"] + 1,
            },
        }
        command.request_hash = canonical_request_hash(command.request_payload)
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        command.request_payload, command.request_hash = original
    replay(c)


@pytest.mark.parametrize(
    "fault", ["deleting", "pod_deleted", "expired", "identity_denied", "identity_down"]
)
def test_receipt_rechecks_live_project_and_pod_authority(receipt_case, fault, monkeypatch):
    c = receipt_case
    with monkeypatch.context() as patch:
        if fault == "deleting":
            with c.factory() as db, db.begin():
                project = db.get(Project, c.project_id)
                original = project.settings
                project.settings = {**(original or {}), "deletion_in_progress": True}
        elif fault == "pod_deleted":
            pod = c.k8s.core.read_namespaced_pod("verifier", "unused")
            patch.setattr(pod.metadata, "deletion_timestamp", datetime.now(UTC))
        elif fault == "expired":
            patch.setattr(
                c,
                "token",
                jwt.encode(
                    {"exp": int(datetime.now(UTC).timestamp()) - 1},
                    "unit-import-token-at-least-32-bytes",
                    algorithm="HS256",
                ),
            )
        else:
            patch.setattr(
                c.auth.create_token_review,
                "side_effect",
                ApiException(status=401)
                if fault == "identity_denied"
                else OSError("Identity unavailable"),
            )
        expected = 503 if fault == "identity_down" else 401
        assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == expected
    if fault == "deleting":
        with c.factory() as db, db.begin():
            db.get(Project, c.project_id).settings = original
    replay(c)


def test_source_is_rechecked_after_tokenreview_without_a_sql_connection(receipt_case):
    c = receipt_case
    review = c.auth.create_token_review.return_value

    def remove_authority(*_args, **_kwargs):
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            project = db.get(Project, c.project_id)
            project.settings = {**(project.settings or {}), "deletion_in_progress": True}
        return review

    c.auth.create_token_review.side_effect = remove_authority
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 401
    with c.factory() as db, db.begin():
        db.get(Project, c.project_id).settings = {}
    c.auth.create_token_review.side_effect = None
    replay(c)


def test_receipt_and_ready_roll_back_together_if_receipt_recording_fails(control_case, monkeypatch):
    c = control_case
    assert send(c, "start").status_code == 200
    original = provider_import.verification_receipt_payload

    def unavailable(*_args):
        raise RuntimeError("Receipt serialization unavailable")

    monkeypatch.setattr(provider_import, "verification_receipt_payload", unavailable)
    assert send(c, "result", json=c.proof.model_dump(mode="json")).status_code == 503
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.command_id).status.value == "running"
        assert (
            db.scalar(
                select(WorkflowEvent.id).where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.event_key == "provider_import.receipt",
                )
            )
            is None
        )
    monkeypatch.setattr(provider_import, "verification_receipt_payload", original)
    ready(c)
    assert c.store.writes == []
