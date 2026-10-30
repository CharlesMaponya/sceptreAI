"""Committed queue ownership, bounded recovery grants and original private custody."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from automl_api import workflow_reconciler
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import AuthProvider, OutboxStatus, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import ProjectMembership
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services import (
    buffered_deletion,
    buffered_execution,
    buffered_uploads,
    deletion,
    workflow_state,
)
from automl_api.services import buffered_quarantine as quarantine
from automl_api.services.workflow_state import StaleFence
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_buffered_execution import pending as pending
from test_buffered_input_admission import admission as admission
from test_buffered_retirement import case as case
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def qcase(case, monkeypatch):
    c = case
    monkeypatch.setattr(quarantine, "get_settings", lambda: c.settings)
    monkeypatch.setattr(buffered_execution, "get_settings", lambda: c.settings)
    monkeypatch.setattr(workflow_reconciler, "get_object_store", lambda *_: c.native)
    c.replay_key = "replay-" + uuid.uuid4().hex
    c.replay = lambda **kw: quarantine.replay(
        c.factory,
        kw.pop("actor_id", c.actor_id),
        kw.pop("token_version", c.token_version),
        c.project_id,
        c.identifier,
        kw.pop("key", c.replay_key),
        kw.pop("request", {"reason": "Owned qualification recovery"}),
    )

    def hold(phase="recorded", budget=1):
        with c.factory() as db, db.begin():
            db.get(WorkflowCommand, c.identifier).max_retries = budget
        if phase == "recorded":
            c.native.read_error = OSError("Original verification unavailable")
            with pytest.raises(OSError):
                c.run()
        elif phase == "issued":
            c.native.create_error = OSError("Original reply lost")
            with pytest.raises(OSError):
                c.run()
        else:

            def fail(*_):
                raise OSError("Credential construction unavailable")

            with pytest.raises(OSError):
                buffered_execution.reconcile_buffered_upload(
                    c.factory, c.entry_id, c.worker, store_factory=fail, settings=c.settings
                )
        with c.factory() as db:
            row = db.get(BufferedUploadInput, c.identifier)
            original = db.get(WorkflowCommand, c.identifier)
            entry = db.get(OutboxEntry, c.entry_id)
            return quarantine.held(row, original, entry)

    c.hold = hold

    def ready():
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            if entry.status == OutboxStatus.PENDING:
                entry.available_at = datetime.now(UTC) - timedelta(seconds=1)

    c.ready = ready
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            original = db.get(WorkflowCommand, c.identifier)
            if original is not None:
                original.replayed_by = None
            db.execute(
                delete(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == quarantine.REPLAY,
                )
            )


def test_actual_worker_dispatches_original_buffered_factory_outside_sql(qcase):
    c = qcase
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.status, entry.lease_owner, entry.lease_expires_at = OutboxStatus.PENDING, None, None
        entry.delivery_attempts = 0
    assert workflow_reconciler.run_once(c.factory, worker_id=c.worker) == 1
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.identifier).response_status == 201
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    assert len(c.native.creates) == 1 and set(c.native.checkouts) == {0}
    assert workflow_reconciler.run_once(c.factory, worker_id=c.worker) == 0


def test_direct_session_dispatch_cannot_execute_buffered_native_work(qcase):
    from automl_api.services.reconciler import reconcile_entry

    c = qcase
    with pytest.raises(RuntimeError, match="factory-owned"):
        with c.factory() as db, db.begin():
            reconcile_entry(db, db.get(OutboxEntry, c.entry_id), worker_id=c.worker)
    assert c.native.creates == []


@pytest.mark.parametrize("budget", [0, 1])
def test_exhausted_budget_never_constructs_sdk(qcase, budget):
    c = qcase
    with c.factory() as db, db.begin():
        db.get(WorkflowCommand, c.identifier).max_retries = budget
        db.get(OutboxEntry, c.entry_id).delivery_attempts = budget + 1
    with pytest.raises(StaleFence, match="budget"):
        buffered_execution.reconcile_buffered_upload(
            c.factory,
            c.entry_id,
            c.worker,
            settings=c.settings,
            store_factory=lambda *_: pytest.fail("No native client after budget exhaustion"),
        )
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DEAD
        assert db.get(BufferedUploadInput, c.identifier).content == c.body


def test_failed_deliveries_consume_budget_without_refund(qcase):
    c = qcase
    with c.factory() as db, db.begin():
        db.get(WorkflowCommand, c.identifier).max_retries = 2

    def fail(*_):
        raise OSError("Do not leak credentials in persisted errors")

    for attempt in (1, 2):
        with pytest.raises(OSError):
            buffered_execution.reconcile_buffered_upload(
                c.factory, c.entry_id, c.worker, settings=c.settings, store_factory=fail
            )
        with c.factory() as db:
            entry = db.get(OutboxEntry, c.entry_id)
            assert entry.delivery_attempts == attempt
            assert db.get(WorkflowCommand, c.identifier).retry_count == attempt
            assert entry.status == (OutboxStatus.PENDING if attempt == 1 else OutboxStatus.DEAD)
            assert "credentials" not in entry.last_error
        if attempt == 1:
            c.claim()
    assert c.native.creates == []


def test_unknown_native_write_is_not_recreated_or_operator_adopted(qcase):
    c = qcase
    held = c.hold("issued", 5)
    assert held["reason"] == "unknown_outcome"
    assert workflow_reconciler.run_once(c.factory, worker_id=c.worker) == 0
    with pytest.raises(HTTPException, match="controlled"):
        c.replay()
    assert len(c.native.creates) == 1
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "issued" and row.content == c.body


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_administrator_replay_restores_only_bounded_original_work_and_caches(qcase, phase):
    c = qcase
    c.hold(phase)
    result = c.replay()
    assert result.attempt_budget == 6 and c.replay() == result
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).delivery_attempts == 1
        assert db.get(WorkflowCommand, c.identifier).retry_count == 1
    c.native.read_error = None
    c.claim()
    c.run()
    assert len(c.native.creates) == 1
    assert c.replay() == result


@pytest.mark.parametrize("change", ["role", "inactive", "token"])
def test_revoked_grant_blocks_native_work_with_original_custody(qcase, change):
    c = qcase
    c.hold("admitted")
    c.replay()
    with c.factory() as db, db.begin():
        if change == "role":
            db.scalar(
                select(ProjectMembership).where(
                    ProjectMembership.project_id == c.project_id,
                    ProjectMembership.user_id == c.actor_id,
                )
            ).role = ProjectRole.EDITOR
        elif change == "inactive":
            db.get(User, c.actor_id).is_active = False
        else:
            db.get(User, c.actor_id).token_version += 1
    c.claim()
    with pytest.raises(HTTPException):
        c.run()
    assert c.native.creates == []
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).content == c.body


def test_new_credential_grant_does_not_rebind_original_manifest(qcase):
    c = qcase
    c.hold("recorded")
    with c.factory() as db, db.begin():
        actor = db.get(User, c.actor_id)
        actor.token_version += 1
        c.token_version = actor.token_version
        original_manifest = dict(db.get(BufferedUploadInput, c.identifier).manifest)
    c.replay()
    c.native.read_error = None
    c.claim()
    c.run()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).manifest == original_manifest
    assert len(c.native.creates) == 1


def test_another_project_administrator_can_grant_original_known_recovery(qcase):
    c = qcase
    c.hold("recorded")
    with c.factory() as db, db.begin():
        actor = User(
            email=f"buffered-admin-{uuid.uuid4().hex}@example.test",
            full_name="Recovery administrator",
            auth_provider=AuthProvider.SIMPLE,
        )
        db.add(actor)
        db.flush()
        actor_id, token = actor.id, actor.token_version
        c.extra_users.append(actor.id)
        db.add(
            ProjectMembership(
                project_id=c.project_id,
                user_id=actor.id,
                role=ProjectRole.ADMIN,
                accepted_at=datetime.now(UTC),
            )
        )
        db.get(User, c.actor_id).is_active = False
    c.replay(actor_id=actor_id, token_version=token)
    c.native.read_error = None
    c.claim()
    c.run()
    assert len(c.native.creates) == 1


@pytest.mark.parametrize(
    "field", ["actor", "hash", "result", "budget", "parent", "receipt", "missing"]
)
def test_changed_grant_history_holds_before_another_sdk(qcase, field):
    c = qcase
    c.hold("recorded")
    result = c.replay()
    with c.factory() as db, db.begin():
        node = db.get(WorkflowCommand, result.id)
        if field == "missing":
            db.delete(node)
        elif field == "hash":
            node.request_hash = "changed"
        else:
            import copy

            payload = copy.deepcopy(node.response_payload)
            if field == "result":
                payload["result"]["attempt_budget"] += 1
            elif field == "budget":
                payload["authorization"]["budget"] += 1
            elif field == "actor":
                payload["authorization"]["actor_id"] = str(uuid.uuid4())
            elif field == "parent":
                payload["authorization"]["previous"] = str(uuid.uuid4())
            else:
                payload["authorization"]["known_receipt"] = {}
            node.response_payload = payload
    c.claim()
    with pytest.raises(StaleFence):
        buffered_execution.reconcile_buffered_upload(
            c.factory,
            c.entry_id,
            c.worker,
            settings=c.settings,
            store_factory=lambda *_: pytest.fail("Changed grant cannot reach storage"),
        )
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).content == c.body


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_known_quarantined_input_can_abort_and_transfer_complete_deletion_proof(qcase, phase):
    c = qcase
    c.hold(phase)
    result = c.cancel()
    assert result["status"] == "aborted" and len(c.erases) == int(phase == "recorded")
    with c.factory() as db, db.begin():
        assert (
            buffered_uploads.require_storage_quota(db, c.project_id, len(c.body), c.settings)
            is None
        )
        deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
            )
        )
        assert entry.payload["buffered_inputs"][0]["retirement"]["intent"][
            "producer_quarantine"
        ].startswith(quarantine.PREFIX)


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_project_delete_directly_transfers_known_quarantine(qcase, phase):
    c = qcase
    c.hold(phase)
    with c.factory() as db, db.begin():
        deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
            )
        )
        assert entry.payload["buffered_inputs"][0]["producer_quarantine"].startswith(
            quarantine.PREFIX
        )
        buffered_deletion.require_transfer(db, c.project_id, entry.payload["buffered_inputs"])


@pytest.mark.parametrize("same_key", [True, False])
def test_concurrent_replay_approvals_advance_budget_once(qcase, same_key):
    c = qcase
    c.hold("recorded")

    def approve(index):
        try:
            return c.replay(key=c.replay_key if same_key else c.replay_key + str(index))
        except StaleFence:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(approve, (0, 1)))
    if same_key:
        assert results[0] == results[1]
    else:
        assert sum(value is not None for value in results) == 1
    with c.factory() as db:
        assert db.get(WorkflowCommand, c.identifier).max_retries == 6


def test_replay_publication_rollback_preserves_original_quarantine(qcase, monkeypatch):
    c = qcase
    c.hold("recorded")
    history = quarantine.history

    def rollback(db, row, command, entry):
        value = history(db, row, command, entry)
        if value:
            raise RuntimeError("Owned approval commit rollback")
        return value

    monkeypatch.setattr(quarantine, "history", rollback)
    with pytest.raises(RuntimeError):
        c.replay()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert quarantine.held(
            row, db.get(WorkflowCommand, c.identifier), db.get(OutboxEntry, c.entry_id)
        )
        assert db.get(WorkflowCommand, c.identifier).max_retries == 1
    monkeypatch.setattr(quarantine, "history", history)
    assert c.replay().attempt_budget == 6


def test_generic_dead_delivery_replay_cannot_bypass_authenticated_grant(qcase):
    c = qcase
    c.hold("recorded")
    with pytest.raises(workflow_state.InvalidTransition, match="authenticated"):
        with c.factory() as db, db.begin():
            workflow_state.replay_dead_outbox(db, c.entry_id, actor_id=c.actor_id)


@pytest.mark.parametrize("budget", [2, 5])
def test_late_original_fact_resumes_only_its_known_delivery_within_existing_budget(qcase, budget):
    c = qcase
    with c.factory() as db, db.begin():
        db.get(WorkflowCommand, c.identifier).max_retries = budget

    def supersede():
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            entry.lease_owner = "second-buffered-owner"
            entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
            entry.delivery_attempts += 1
        with pytest.raises(StaleFence):
            buffered_execution.reconcile_buffered_upload(
                c.factory,
                c.entry_id,
                "second-buffered-owner",
                settings=c.settings,
                store_factory=lambda *_: pytest.fail(
                    "No second create while original reply is outstanding"
                ),
            )

    c.native.create_hook = supersede
    with pytest.raises(StaleFence):
        c.run()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        entry = db.get(OutboxEntry, c.entry_id)
        assert row.phase == "recorded" and row.native_receipt and row.content == c.body
        assert entry.status == (OutboxStatus.DEAD if budget == 2 else OutboxStatus.PENDING)
    if budget == 2:
        assert c.replay().attempt_budget == 7
    c.native.create_hook = None
    c.claim()
    c.run()
    assert len(c.native.creates) == 1


def test_actual_http_replay_enforces_fresh_auth_and_returns_cached_bounded_grant(qcase):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes.datasets import router
    from automl_api.db.session import get_db, get_session_factory
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    c = qcase
    c.hold("recorded")
    app = FastAPI()
    app.include_router(router)

    def database():
        with c.factory() as db:
            yield db

    def actor():
        with c.factory() as db:
            return db.get(User, c.actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = actor
    app.dependency_overrides[get_session_factory] = lambda: c.factory

    def post(reason="Owned HTTP recovery"):
        return TestClient(app).post(
            f"/projects/{c.project_id}/datasets/buffered/{c.identifier}/replay",
            headers={"Idempotency-Key": c.replay_key},
            json={"reason": reason},
        )

    result = post()
    assert result.status_code == 202, result.text
    assert result.json()["attempt_budget"] == 6 and post().json() == result.json()
    assert post("Changed original reason").status_code == 409
    with c.factory() as db, db.begin():
        db.scalar(
            select(ProjectMembership).where(
                ProjectMembership.project_id == c.project_id,
                ProjectMembership.user_id == c.actor_id,
            )
        ).role = ProjectRole.EDITOR
    assert post().status_code == 403
    assert len(c.native.creates) == 1


def test_multiple_replay_epochs_preserve_budget_chain_and_archive_in_deletion(qcase):
    c = qcase
    c.hold("recorded")
    first = c.replay()
    # Revoke the first grant, consuming only its existing remaining delivery budget.
    with c.factory() as db, db.begin():
        db.get(User, c.actor_id).token_version += 1
    for _ in range(5):
        c.claim()
        with pytest.raises(HTTPException):
            c.run()
    with c.factory() as db, db.begin():
        c.token_version = db.get(User, c.actor_id).token_version
    second = c.replay(key=c.replay_key + "-second")
    assert (first.attempt_budget, second.attempt_budget) == (6, 11)
    c.native.read_error = None
    c.claim()
    c.run()
    with c.factory() as db, db.begin():
        deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
            )
        )
        approvals = entry.payload["buffered_inputs"][0]["producer_replays"]
        assert [item["id"] for item in approvals] == [str(first.id), str(second.id)]
        buffered_deletion.require_transfer(db, c.project_id, entry.payload["buffered_inputs"])


@pytest.mark.parametrize("field", ["source", "phase", "attempt", "budget", "reason"])
def test_corrupt_quarantine_cannot_authorize_replay_or_retirement(qcase, field):
    import json

    c = qcase
    c.hold("recorded")
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.identifier)
        value = json.loads(command.terminal_reason[len(quarantine.PREFIX) :])
        value[field] = "changed"
        command.terminal_reason = quarantine.PREFIX + json.dumps(value)
        db.get(OutboxEntry, c.entry_id).last_error = command.terminal_reason
    for action in (c.replay, c.cancel):
        with pytest.raises(StaleFence):
            action()
    assert c.erases == []
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).content == c.body


def test_known_quarantine_expires_without_recreating_or_discarding_original_proof(
    qcase, monkeypatch
):
    from automl_api.services import buffered_retirement

    c = qcase

    class Past(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) - timedelta(days=2)

    monkeypatch.setattr(buffered_uploads, "datetime", Past)
    c.identifier = c.admit(settings=c.settings, key=c.key + "-expired")
    c.claim()
    c.hold("recorded")
    assert buffered_retirement.cleanup_inputs(c.factory, settings=c.settings) == 1
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        proof = buffered_retirement.require_retired(db, row)
        assert proof["result"]["status"] == "expired"
        assert proof["intent"]["producer_quarantine"].startswith(quarantine.PREFIX)
    assert len(c.native.creates) == 1 and len(c.erases) == 1
