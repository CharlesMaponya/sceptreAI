"""Committed outbox cleanup preserves data until credential retirement."""

import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    RunStatus,
    ScopeStatus,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    PromotionalScope,
    PromotionalScopeMember,
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowCommand,
)
from automl_api.services import deletion, reconciler
from automl_api.services import workflow_state as state
from automl_api.storage.contracts import DeletionPending
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker
from test_attempt_object_grant_schema import grant_values, make_attempt
from test_attempt_object_grants import grant_case as grant_case  # noqa: F401
from test_attempt_object_grants import issue
from test_grant_retirement import clock as clock  # noqa: F401
from test_grant_retirement import started_grant
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def cleanup_case(phase1_engine, committed_tenant, tmp_path, monkeypatch, clock):
    actor_id, project_id = committed_tenant
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    monkeypatch.setattr(state, "datetime", deletion.datetime)
    monkeypatch.setattr(reconciler, "datetime", deletion.datetime)
    with factory() as db, db.begin():
        user, project = db.get(User, actor_id), db.get(Project, project_id)
        other = Project(owner_id=actor_id, created_by_id=actor_id, name="unrelated grant project")
        db.add(other)
        db.flush()
        other_id = other.id
        attempts = [make_attempt(db, user, project) for _ in range(2)]
        foreign = make_attempt(db, user, other)
        for attempt in [*attempts, foreign]:
            attempt.status = AttemptStatus.FAILED
            run = db.get(ModelRun, attempt.model_run_id)
            run.status = RunStatus.FAILED
            run.k8s_job_name = f"retirement-{run.id.hex}"
        rows = [started_grant(a, clock[0]) for a in [*attempts, foreign]]
        db.add_all(rows)
        db.flush()
        ids = [a.id for a in attempts]
        run_ids = [a.model_run_id for a in attempts]
        grant_ids = [g.id for g in rows]
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="retirement-test")
    owned = store.put_bytes(f"projects/{project_id}/runs/{run_ids[0]}/artifact", b"owned")
    sibling = store.put_bytes(f"projects/{project_id}/runs/{run_ids[1]}/artifact", b"sibling")
    foreign_object = store.put_bytes(
        f"projects/{other_id}/runs/{foreign.model_run_id}/artifact", b"foreign"
    )
    original = store.delete_prefix
    store.delete_prefix = MagicMock(side_effect=original)
    get_store = MagicMock(return_value=store)
    monkeypatch.setattr(deletion, "get_object_store", get_store)
    c = SimpleNamespace(
        factory=factory,
        project_id=project_id,
        actor_id=actor_id,
        other_id=other_id,
        attempt_ids=ids,
        run_ids=run_ids,
        grant_ids=grant_ids,
        foreign_attempt_id=foreign.id,
        store=store,
        get_store=get_store,
        owned=owned.uri,
        sibling=sibling.uri,
        foreign_object=foreign_object.uri,
        k8s=SimpleNamespace(
            delete_job=MagicMock(),
            job_state=MagicMock(return_value="missing"),
            delete_ray_job=MagicMock(),
            ray_job=MagicMock(),
        ),
        clock=clock,
    )
    try:
        yield c
    finally:
        with factory() as db, db.begin():
            for pid in (project_id, other_id):
                db.execute(
                    delete(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.project_id == pid
                    )
                )
                db.execute(delete(DeletionStage).where(DeletionStage.project_id == pid))
                db.execute(delete(DeletionTombstone).where(DeletionTombstone.project_id == pid))
            db.execute(delete(Project).where(Project.id == other_id))


def request(c, kind):
    with c.factory() as db, db.begin():
        response = deletion.request_deletion(
            db, db.get(User, c.actor_id), c.project_id, c.run_ids[0] if kind == "run" else None
        )
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == kind + ".delete"
            )
        )
        entry.available_at = c.clock[0]
        c.entry_id, c.tombstone_id, c.command_id = (
            entry.id,
            uuid.UUID(response["id"]),
            entry.command_id,
        )


def dispatch(c):
    with c.factory() as db:
        claimed = state.claim_outbox(db, worker_id="cleanup", now=c.clock[0])
        assert [e.id for e in claimed] == [c.entry_id]
        db.commit()
        try:
            reconciler.reconcile_entry(db, claimed[0], worker_id="cleanup", k8s=c.k8s)
        finally:
            db.commit()


@pytest.mark.parametrize("kind", ["run", "project"])
def test_deletion_defers_durably_then_finishes_without_touching_unrelated_data(cleanup_case, kind):
    c = cleanup_case
    request(c, kind)
    for _ in range(2):
        dispatch(c)
        with c.factory() as db:
            entry = db.get(OutboxEntry, c.entry_id)
            assert entry.status == OutboxStatus.PENDING
            assert entry.delivery_attempts == 0
            assert db.get(WorkflowCommand, c.command_id).retry_count == 0
            assert 0 < (entry.available_at - c.clock[0]).total_seconds() <= 60
            c.clock[0] += timedelta(seconds=1)
            deadline = db.get(WorkflowAttemptCredentialGrant, c.grant_ids[0]).expires_at
            assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[0]).status == "revoked"
            sibling = db.get(WorkflowAttemptCredentialGrant, c.grant_ids[1])
            assert sibling.status == ("revoked" if kind == "project" else "issued")
            assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[2]).status == "issued"
            assert all(
                s.status == "pending"
                for s in db.scalars(
                    select(DeletionStage).where(DeletionStage.tombstone_id == c.tombstone_id)
                )
            )
        # Retry before the provider deadline is not claimable.
        with c.factory() as db, db.begin():
            assert state.claim_outbox(db, worker_id="too-early", now=c.clock[0]) == []
        assert all(not method.mock_calls for method in vars(c.k8s).values())
        c.get_store.assert_not_called()
        assert (
            c.store.exists(c.owned)
            and c.store.exists(c.sibling)
            and c.store.exists(c.foreign_object)
        )
        # Bring the second attempted delivery due without changing credential expiry.
        with c.factory() as db, db.begin():
            db.get(OutboxEntry, c.entry_id).available_at = c.clock[0]
    c.clock[0] = deadline
    for _ in range(4 if kind == "project" else 3):
        dispatch(c)
        c.clock[0] += timedelta(seconds=6)
    with c.factory() as db:
        tombstone = db.get(DeletionTombstone, c.tombstone_id)
        assert tombstone.status == "complete" and tombstone.completed_at is not None
        assert db.get(ModelRun, c.run_ids[0]) is None
        assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[0]) is None
        if kind == "project":
            assert db.get(Project, c.project_id) is None
            assert db.get(OutboxEntry, c.entry_id) is None
            assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[1]) is None
        else:
            assert db.get(Project, c.project_id) is not None
            assert db.get(ModelRun, c.run_ids[1]) is not None
            assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[1]).status == "issued"
            entry = db.get(OutboxEntry, c.entry_id)
            assert entry.status == OutboxStatus.DELIVERED and entry.delivery_attempts == 1
            assert db.get(WorkflowCommand, c.command_id).status == CommandStatus.SUCCEEDED
        assert db.get(Project, c.other_id) is not None
        assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[2]).status == "issued"
    assert not c.store.exists(c.owned)
    assert c.store.exists(c.sibling) == (kind == "run")
    assert c.store.exists(c.foreign_object)
    with c.store.open_stream(c.foreign_object) as reader:
        assert reader.read() == b"foreign"
    assert c.store.delete_prefix.call_count == (2 if kind == "project" else 1)
    assert c.k8s.delete_job.call_count == (2 if kind == "project" else 1)
    c.k8s.delete_ray_job.assert_not_called()


def test_associated_scope_grants_retire_even_when_champion_belongs_to_another_member(cleanup_case):
    c = cleanup_case
    with c.factory() as db, db.begin():
        user, project = db.get(User, c.actor_id), db.get(Project, c.project_id)
        champion = make_attempt(db, user, project, stage=WorkflowStage.CHAMPION_REFIT)
        champion.status = AttemptStatus.FAILED
        champion.model_run_id = c.run_ids[1]
        scope = db.get(PromotionalScope, champion.scope_id)
        scope.status = ScopeStatus.FAILED
        db.add(
            PromotionalScopeMember(
                project_id=c.project_id, scope_id=scope.id, model_run_id=c.run_ids[0], ordinal=0
            )
        )
        grant = started_grant(champion, c.clock[0], role="candidate")
        grant.expires_at = c.clock[0] + timedelta(seconds=120)
        db.add(grant)
        db.flush()
        grant_id = grant.id
        # Primary selected run has no still-live capability at the first deadline.
        db.get(WorkflowAttemptCredentialGrant, c.grant_ids[0]).expires_at = c.clock[0] + timedelta(
            seconds=1
        )
    request(c, "run")
    dispatch(c)
    with c.factory() as db:
        assert db.get(WorkflowAttemptCredentialGrant, grant_id).status == "revoked"
        assert db.get(OutboxEntry, c.entry_id).available_at == c.clock[0] + timedelta(seconds=120)
        assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[1]).status == "issued"
    c.get_store.assert_not_called()
    c.clock[0] += timedelta(seconds=120)
    for _ in range(3):
        dispatch(c)
        c.clock[0] += timedelta(seconds=6)
    with c.factory() as db:
        assert db.get(ModelRun, c.run_ids[0]) is None
        assert db.get(ModelRun, c.run_ids[1]) is not None
        assert db.get(WorkflowAttempt, champion.id) is not None
        assert db.get(WorkflowAttemptCredentialGrant, grant_id).status == "revoked"
        assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[1]).status == "issued"


def test_legal_hold_prevents_cleanup_and_grant_mutation(cleanup_case):
    c = cleanup_case
    request(c, "run")
    with c.factory() as db, db.begin():
        db.get(DeletionTombstone, c.tombstone_id).legal_hold = True
    with pytest.raises(ValueError, match="legal hold"):
        dispatch(c)
    with c.factory() as db:
        assert db.get(WorkflowAttemptCredentialGrant, c.grant_ids[0]).status == "issued"
        assert db.get(ModelRun, c.run_ids[0]) is not None
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 1
        assert "legal hold" in entry.last_error
    c.get_store.assert_not_called()
    assert all(not method.mock_calls for method in vars(c.k8s).values())


def test_started_mint_cannot_disclose_or_escape_retirement_when_deletion_commits(
    grant_case, monkeypatch
):
    from automl_api.services.attempt_object_grants import ObjectGrantUnavailable

    c = grant_case
    get_store = MagicMock(side_effect=AssertionError("No deletion may reach storage yet"))
    monkeypatch.setattr(deletion, "get_object_store", get_store)

    def stop_and_delete(*_args, **_kwargs):
        with c.factory() as db, db.begin():
            db.get(WorkflowAttempt, c.attempt_id).status = AttemptStatus.FAILED
            db.get(ModelRun, c.run_id).status = RunStatus.FAILED
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id, c.run_id)
        with c.factory() as db, db.begin():
            entry = db.scalar(
                select(OutboxEntry).where(
                    OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "run.delete"
                )
            )
            with pytest.raises(DeletionPending):
                deletion.reconcile_deletion(db, entry, MagicMock())
        return "https://objects.invalid/never-disclose?private-signature"

    c.signer.side_effect = stop_and_delete
    try:
        with pytest.raises(ObjectGrantUnavailable) as error:
            issue(c)
        assert "private-signature" not in str(error.value)
        with c.factory() as db:
            row = db.scalar(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id
                )
            )
            assert row.status == "revoked"
            assert row.issuance_started_at is not None and row.issued_at is None
            assert row.expires_at > deletion.datetime.now(deletion.UTC)
            assert db.get(ModelRun, c.run_id) is not None
            assert "private-signature" not in str(vars(row))
        c.signer.assert_called_once()
        get_store.assert_not_called()
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


def test_pending_intent_is_retired_without_wait_and_cannot_mint_after_tombstone(grant_case):
    from automl_api.services.workflow_state import StaleFence

    c = grant_case
    try:
        with c.factory() as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.attempt_id)
            attempt.status = AttemptStatus.FAILED
            db.get(ModelRun, c.run_id).status = RunStatus.FAILED
            row = WorkflowAttemptCredentialGrant(**grant_values(attempt))
            db.add(row)
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id, c.run_id)
        with c.factory() as db, db.begin():
            entry = db.scalar(
                select(OutboxEntry).where(
                    OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "run.delete"
                )
            )
            assert deletion.reconcile_deletion(db, entry, MagicMock()) is False
            row = db.get(WorkflowAttemptCredentialGrant, row.id)
            assert row.status == "revoked" and row.issuance_started_at is None
            assert row.expires_at > deletion.datetime.now(deletion.UTC)
            assert (
                db.scalar(
                    select(DeletionStage.status).where(
                        DeletionStage.project_id == c.project_id,
                        DeletionStage.resource_class == "runtime",
                    )
                )
                == "completed"
            )
        with pytest.raises(StaleFence):
            issue(c)
        c.signer.assert_not_called()
        c.auth.create_token_review.assert_not_called()
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )
