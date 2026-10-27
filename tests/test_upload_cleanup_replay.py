"""Operator approval extends bounded cleanup without changing its native target."""

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_admission as admission
from automl_api.services import upload_admission_cleanup as cleanup
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.services.workflow_state import transition_command
from automl_api.storage.contracts import UploadContractError
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_cleanup import _command, _retired_known
from test_upload_admission_deletion import deletion_case as deletion_case
from test_upload_admission_workflow import admission_case as admission_case


def _exhausted(c, monkeypatch):
    now = _retired_known(c)
    abort = c.store.abort_upload

    def fail(_target):
        raise OSError("Native cancellation response unavailable")

    monkeypatch.setattr(c.store, "abort_upload", fail)
    for number in range(5):
        with pytest.raises(OSError):
            cleanup.cleanup_admission(
                c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=number * 6)
            )
    moment = now + timedelta(minutes=31)
    assert not cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=moment)
    monkeypatch.setattr(c.store, "abort_upload", abort)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(cleanup, "datetime", Clock)
    with c.factory() as db, db.begin():
        db.get(User, c.actor_id).is_active = True
    return moment


def _post(c, key="operator-replay", reason="Provider access restored"):
    return TestClient(c.app).post(
        f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/cleanup/replay",
        headers={"Idempotency-Key": key},
        json={"reason": reason},
    )


def test_actual_owner_replay_authorizes_one_bounded_cleanup_epoch(admission_case, monkeypatch):
    c = admission_case
    now = _exhausted(c, monkeypatch)
    constructors = c.constructions
    with c.factory() as db:
        original = _command(db, c).id
    result = _post(c)
    assert result.status_code == 202, result.text
    assert result.json()["attempt_budget"] == 10
    assert _post(c).json() == result.json()
    assert c.constructions == constructors and c.store.aborted == 0
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db:
        command = _command(db, c)
        assert command.id == original and command.retry_count == 6 and command.max_retries == 10
        assert command.replayed_by == c.actor_id
    assert c.store.aborted == 1


def _approvals(db, c):
    return list(
        db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == cleanup.REPLAY,
            )
        )
    )


def test_same_key_conflict_cannot_authorize_another_epoch(admission_case, monkeypatch):
    c = admission_case
    _exhausted(c, monkeypatch)
    assert _post(c).status_code == 202
    result = _post(c, reason="A different instruction")
    assert result.status_code == 409
    with c.factory() as db:
        assert len(_approvals(db, c)) == 1 and _command(db, c).max_retries == 10
    assert _post(c, key="another-key").status_code == 409


@pytest.mark.parametrize(
    "gate", ["viewer", "editor", "membership", "inactive", "token", "deleting"]
)
def test_replay_requires_fresh_owner_authorization(admission_case, monkeypatch, gate):
    c = admission_case
    _exhausted(c, monkeypatch)
    with c.factory() as db, db.begin():
        user = db.get(User, c.actor_id)
        membership = db.scalar(
            select(ProjectMembership).where(
                ProjectMembership.project_id == c.project_id,
                ProjectMembership.user_id == c.actor_id,
            )
        )
        if gate in {"viewer", "editor"}:
            membership.role = ProjectRole.VIEWER if gate == "viewer" else ProjectRole.EDITOR
        elif gate == "membership":
            db.delete(membership)
        elif gate == "inactive":
            user.is_active = False
        elif gate == "token":
            user.token_version += 1
        else:
            db.get(Project, c.project_id).settings = {"deletion_in_progress": True}
    constructors = c.constructions
    if gate == "token":
        with pytest.raises(HTTPException) as denied:
            cleanup.replay_cleanup(
                c.factory,
                c.actor_id,
                c.token_version,
                c.project_id,
                c.upload_id,
                "stale-token",
                "Access restored",
            )
        assert denied.value.status_code == 401
    else:
        result = _post(c)
        assert result.status_code == (
            401 if gate == "inactive" else 409 if gate == "deleting" else 403
        )
    with c.factory() as db:
        assert _approvals(db, c) == []
        assert _command(db, c).status == CommandStatus.FAILED
    assert c.constructions == constructors


@pytest.mark.parametrize(
    "gate", ["hold", "retention", "source", "store", "receipt", "not_exhausted"]
)
def test_replay_cannot_override_original_target_policy_or_state(admission_case, monkeypatch, gate):
    c = admission_case
    now = _exhausted(c, monkeypatch)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(row.provider_state)
        if gate == "hold":
            row.legal_hold = True
        elif gate == "retention":
            row.retention_until = now + timedelta(days=1)
        elif gate == "source":
            row.original_filename = "foreign.csv"
        elif gate == "receipt":
            state[admission.RECORD]["receipt"]["provider_id"] = "foreign"
        elif gate == "not_exhausted":
            state[cleanup.RECORD]["phase"] = "unknown"
            command = _command(db, c)
            transition_command(command, CommandStatus.PENDING)
            transition_command(command, CommandStatus.RUNNING)
        row.provider_state = state
    if gate == "store":
        from dataclasses import replace

        monkeypatch.setattr(
            admission, "get_settings", lambda: replace(c.settings, object_store_bucket="foreign")
        )
    constructors = c.constructions
    assert _post(c).status_code == 409
    with c.factory() as db:
        assert _approvals(db, c) == []
    assert c.constructions == constructors


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"reason": " "},
        {"reason": "ok", "provider_id": "foreign"},
        {"reason": "ok", "max_retries": 100},
    ],
)
def test_http_replay_rejects_missing_reason_and_target_or_budget_overrides(
    admission_case, monkeypatch, body
):
    c = admission_case
    _exhausted(c, monkeypatch)
    response = TestClient(c.app).post(
        f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/cleanup/replay",
        headers={"Idempotency-Key": "bad-body"},
        json=body,
    )
    assert response.status_code == 422
    with c.factory() as db:
        assert _approvals(db, c) == []


@pytest.mark.parametrize(
    "field",
    [
        "missing_list",
        "erased_root",
        "empty_list",
        "missing_approval",
        "request",
        "response",
        "foreign_root",
        "duplicate",
    ],
)
def test_broken_replay_authority_cannot_issue_native_work(admission_case, monkeypatch, field):
    c = admission_case
    now = _exhausted(c, monkeypatch)
    assert _post(c).status_code == 202
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(row.provider_state)
        command = _command(db, c)
        approval = _approvals(db, c)[0]
        if field in {"missing_list", "erased_root"}:
            state[cleanup.RECORD].pop("replays")
            if field == "erased_root":
                response = dict(command.response_payload)
                response.pop("cleanup_replays_digest")
                command.response_payload = response
        elif field == "empty_list":
            state[cleanup.RECORD]["replays"] = []
            command.response_payload = {
                **command.response_payload,
                "cleanup_replays_digest": digest([]),
            }
        elif field == "missing_approval":
            db.delete(approval)
        elif field in {"request", "foreign_root"}:
            request = dict(approval.request_payload)
            request["reason" if field == "request" else "cleanup_command_id"] = "foreign"
            approval.request_payload = request
        elif field == "response":
            approval.response_payload = {
                **approval.response_payload,
                "result": {"attempt_budget": 100},
            }
        else:
            values = state[cleanup.RECORD]["replays"]
            values.append(copy.deepcopy(values[0]))
            command.response_payload = {
                **command.response_payload,
                "cleanup_replays_digest": digest(values),
            }
        row.provider_state = state
    constructors = c.constructions
    with pytest.raises(UploadContractError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert c.constructions == constructors and c.store.aborted == 0


@pytest.mark.parametrize("same_key", [False, True])
def test_concurrent_replay_requests_grant_one_epoch(admission_case, monkeypatch, same_key):
    c = admission_case
    _exhausted(c, monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as executor:
        tasks = [
            executor.submit(_post, c, "same" if same_key else f"key-{number}")
            for number in range(2)
        ]
        responses = [task.result(timeout=20) for task in tasks]
    assert sorted(response.status_code for response in responses) == (
        [202, 202] if same_key else [202, 409]
    )
    if same_key:
        assert responses[0].json() == responses[1].json()
    with c.factory() as db:
        assert len(_approvals(db, c)) == 1 and _command(db, c).max_retries == 10


def test_approval_rollback_preserves_exhausted_history_and_same_key_retry(
    admission_case, monkeypatch
):
    c = admission_case
    _exhausted(c, monkeypatch)
    save = cleanup._save

    def fail(*_):
        raise OSError("Approval transaction connection lost")

    monkeypatch.setattr(cleanup, "_save", fail)
    with pytest.raises(OSError):
        _post(c)
    with c.factory() as db:
        assert _approvals(db, c) == []
        assert _command(db, c).status == CommandStatus.FAILED
        assert (
            "replays"
            not in db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
        )
    monkeypatch.setattr(cleanup, "_save", save)
    assert _post(c).status_code == 202


def test_each_audited_epoch_has_five_attempts_and_keeps_prior_history(admission_case, monkeypatch):
    c = admission_case
    now = _exhausted(c, monkeypatch)
    first = _post(c)
    assert first.status_code == 202
    abort = c.store.abort_upload
    calls = []

    def fail(target):
        calls.append(target)
        raise OSError("Native provider still unavailable")

    monkeypatch.setattr(c.store, "abort_upload", fail)
    for number in range(5):
        with pytest.raises(OSError):
            cleanup.cleanup_admission(
                c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=number * 6)
            )
    assert not cleanup.cleanup_admission(
        c.factory, c.project_id, c.upload_id, now=now + timedelta(minutes=30)
    )
    assert len(calls) == 5 and all(target == calls[0] for target in calls)
    next_time = now + timedelta(minutes=31)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next_time if tz else next_time.replace(tzinfo=None)

    monkeypatch.setattr(cleanup, "datetime", Clock)
    second = _post(c, key="second-epoch")
    assert second.status_code == 202 and second.json()["attempt_budget"] == 15
    monkeypatch.setattr(c.store, "abort_upload", abort)
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=next_time)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        approvals = row.provider_state[cleanup.RECORD]["replays"]
        assert len(approvals) == 2 and approvals[0]["command_id"] == first.json()["id"]
        assert _command(db, c).retry_count == 11 and _command(db, c).max_retries == 15
        nodes = sorted(_approvals(db, c), key=lambda node: node.request_payload["epoch"])
        assert [node.request_payload["prior_attempt"]["number"] for node in nodes] == [5, 10]
        assert all(
            node.response_status == 202 and node.status == CommandStatus.SUCCEEDED for node in nodes
        )


def test_actual_approval_backend_loss_rolls_back_the_entire_epoch(admission_case, monkeypatch):
    from sqlalchemy import event, text
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.orm import Session

    c = admission_case
    _exhausted(c, monkeypatch)
    killed = []

    def terminate(db):
        if killed:
            return
        for row in db.dirty:
            if (
                isinstance(row, DatasetUploadSession)
                and row.id == c.upload_id
                and row.provider_state[cleanup.RECORD].get("replays")
            ):
                db.flush()
                pid = db.scalar(text("SELECT pg_backend_pid()"))
                with c.engine.begin() as control:
                    assert control.scalar(text("SELECT pg_backend_pid()")) != pid
                    assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
                killed.append(pid)
                return

    event.listen(Session, "before_commit", terminate)
    try:
        with pytest.raises(DBAPIError):
            _post(c)
    finally:
        event.remove(Session, "before_commit", terminate)
    assert len(killed) == 1
    with c.factory() as db:
        assert _approvals(db, c) == [] and _command(db, c).status == CommandStatus.FAILED
        assert (
            "replays"
            not in db.get(DatasetUploadSession, c.upload_id).provider_state[cleanup.RECORD]
        )
    assert _post(c).status_code == 202


def test_lost_http_approval_reply_replays_without_another_grant(admission_case, monkeypatch):
    from automl_api.api.routes import datasets

    c = admission_case
    _exhausted(c, monkeypatch)
    replay = datasets.replay_cleanup
    committed = []

    def disconnect(*args):
        committed.append(replay(*args))
        raise OSError("Approval committed but HTTP reply was lost")

    monkeypatch.setattr(datasets, "replay_cleanup", disconnect)
    with pytest.raises(OSError):
        _post(c)
    monkeypatch.setattr(datasets, "replay_cleanup", replay)
    response = _post(c)
    assert response.status_code == 202 and response.json() == committed[0].model_dump(mode="json")
    with c.factory() as db:
        assert len(_approvals(db, c)) == 1 and _command(db, c).max_retries == 10


def test_replay_chain_authenticates_deletion_and_corruption_stops_queued_stage(
    deletion_case, monkeypatch
):
    from unittest.mock import MagicMock

    from automl_api.models.workflows import OutboxEntry
    from automl_api.services import deletion
    from automl_api.storage.contracts import DeletionPending

    c = deletion_case
    now = _exhausted(c, monkeypatch)
    response = _post(c)
    assert response.status_code == 202
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    assert _post(c).json() == response.json()
    with c.factory() as db, db.begin():
        assert (
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)["status"]
            == "pending"
        )
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))
        entry_id = entry.id
        approval = _approvals(db, c)[0]
        approval.request_hash = "foreign"
    store = MagicMock()
    monkeypatch.setattr(deletion, "get_object_store", store)
    with pytest.raises(DeletionPending):
        with c.factory() as db, db.begin():
            deletion.reconcile_deletion(db, db.get(OutboxEntry, entry_id), MagicMock())
    store.assert_not_called()


def test_orphan_operator_approval_keeps_project_deletion_held(admission_case, monkeypatch):
    import uuid

    from automl_api.services import deletion

    c = admission_case
    now = _exhausted(c, monkeypatch)
    assert _post(c).status_code == 202
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        original = db.get(
            WorkflowCommand, uuid.UUID(row.provider_state[admission.RECORD]["intent"]["command_id"])
        )
        db.delete(row)
        db.delete(_command(db, c))
        db.delete(original)
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409
    with c.factory() as db:
        assert len(_approvals(db, c)) == 1
