"""Real PostgreSQL and authority registration before any refit or release."""

import copy
import uuid
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.enums import CommandStatus, ScopeStatus
from automl_api.models.qualification import FinalTestAllocation
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.qualification_control import create_app
from automl_api.services.scope_allocation import (
    reconcile_scope_allocation,
    register_scope_allocation,
    require_scope_allocation,
)
from automl_api.services.workflow_state import InvalidTransition, seal_promotional_scope
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


@pytest.fixture
def early_case(policy_case, authority_api):
    db, _, scope, _ = policy_case
    scope.status = ScopeStatus.OPEN
    scope.membership_deadline_at = datetime.now(UTC) + timedelta(minutes=5)
    actor_id = db.scalar(
        select(ModelRun.created_by_id).where(ModelRun.project_id == scope.project_id)
    )
    command = register_scope_allocation(db, scope, actor_id=actor_id)
    db.flush()

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    _, _, kwargs = authority_api
    reference = str(scope.project_id)
    allocator = kwargs["principals"][0].model_copy(update={"project_reference": reference})
    try:
        with TestClient(
            create_app(**{**kwargs, "principals": [allocator]}),
            base_url="https://authority.test/",
            headers={"Authorization": "Bearer allocator"},
        ) as client:
            yield db, scope, command, factory, client
    finally:
        with kwargs["session_factory"]() as session, session.begin():
            session.execute(
                delete(FinalTestAllocation).where(
                    FinalTestAllocation.project_reference == reference
                )
            )


def test_early_allocation_has_durable_intent_before_http_and_replays(early_case):
    db, scope, command, factory, client = early_case
    calls = []

    def post(path, **kwargs):
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            stored = session.get(WorkflowCommand, command.id)
            assert stored.status == CommandStatus.RUNNING and stored.retry_count == 1
            assert stored.lease_owner and stored.lease_expires_at > datetime.now(UTC)
            assert stored.request_payload["scope_id"] == str(scope.id)
        calls.append(path)
        return client.post(path, **kwargs)

    authority = SimpleNamespace(base_url=client.base_url, post=post, get=client.get)
    assert reconcile_scope_allocation(factory, scope.id, authority)
    db.refresh(command)
    allocation_id = require_scope_allocation(db, scope)
    assert allocation_id == uuid.UUID(command.response_payload["allocation_id"])
    assert not reconcile_scope_allocation(factory, scope.id, authority)
    assert calls == ["allocations"]
    state = client.get(f"allocations/{allocation_id}").json()
    assert state["status"] == "allocated" and state["receipts"] == []
    assert command.lease_owner is command.lease_expires_at is None
    assert scope.status == ScopeStatus.OPEN


@pytest.mark.parametrize("lost", ["allocations", "get"])
def test_lost_response_reuses_one_central_allocation(early_case, lost):
    db, scope, command, factory, client = early_case

    def post(path, **kwargs):
        response = client.post(path, **kwargs)
        response.raise_for_status()
        if lost == "allocations":
            raise ConnectionError("private credentials must not persist")
        return response

    def get(path):
        client.get(path).raise_for_status()
        raise ConnectionError("private endpoint must not persist")

    with pytest.raises(ConnectionError):
        reconcile_scope_allocation(
            factory, scope.id, SimpleNamespace(base_url=client.base_url, post=post, get=get)
        )
    db.refresh(command)
    assert command.status == CommandStatus.PENDING and command.retry_count == 1
    assert "private" not in command.terminal_reason
    assert not reconcile_scope_allocation(factory, scope.id, client)  # Retry backoff.
    command.available_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    assert reconcile_scope_allocation(factory, scope.id, client)
    db.refresh(command)
    assert command.retry_count == 2
    allocation_id = require_scope_allocation(db, scope)
    replay = client.post("allocations", json=command.request_payload["allocation"]).json()
    assert replay["allocation_id"] == str(allocation_id)


@pytest.mark.parametrize("fault", ["cancel", "policy", "lease", "expiry", "deadline", "delete"])
def test_lost_scope_or_ownership_cannot_publish_after_http(early_case, fault):
    db, scope, command, factory, client = early_case

    def post(path, **kwargs):
        response = client.post(path, **kwargs)
        with factory() as session, session.begin():
            current = session.get(type(scope), scope.id)
            row = session.get(WorkflowCommand, command.id)
            if fault == "cancel":
                current.status = ScopeStatus.CANCELLED
            elif fault == "policy":
                policy = copy.deepcopy(current.comparison_policy)
                policy["evaluation_policy"]["max_decoded_bytes"] += 1
                current.comparison_policy = policy
            elif fault == "lease":
                row.lease_owner = "new-owner"
            elif fault == "expiry":
                row.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif fault == "deadline":
                current.membership_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
            else:
                session.delete(row)
        return response

    command_id = command.id
    with pytest.raises(InvalidTransition):
        reconcile_scope_allocation(
            factory, scope.id, SimpleNamespace(base_url=client.base_url, post=post, get=client.get)
        )
    db.expire_all()
    current = db.get(WorkflowCommand, command_id)
    assert current is None or current.status != CommandStatus.SUCCEEDED
    if fault == "lease":
        assert current.lease_owner == "new-owner"


@pytest.mark.parametrize(
    "fault", ["missing", "pending", "failed", "hash", "response", "policy", "resource"]
)
def test_barrier_rejects_untrusted_registration(early_case, fault):
    db, scope, command, factory, client = early_case
    assert reconcile_scope_allocation(factory, scope.id, client)
    db.refresh(command)
    if fault == "missing":
        db.delete(command)
    elif fault in ("pending", "failed"):
        command.status = CommandStatus(fault)
    elif fault == "hash":
        command.request_hash = "0" * 64
    elif fault == "response":
        command.response_payload = {"allocation_id": str(uuid.uuid4()), "untrusted": True}
    elif fault == "resource":
        command.resource_id = uuid.uuid4()
    else:
        policy = copy.deepcopy(scope.comparison_policy)
        policy["evaluation_policy"]["max_decoded_bytes"] += 1
        scope.comparison_policy = policy
    db.flush()
    with pytest.raises((InvalidTransition, ValueError)):
        with db.begin_nested():
            seal_promotional_scope(db, scope.id, expected_cas_version=scope.cas_version)
    assert scope.status == ScopeStatus.OPEN


@pytest.mark.parametrize("fault", ["live_owner", "budget", "cancelled", "expired_scope", "http"])
def test_unavailable_claims_do_not_send_allocation(early_case, fault):
    db, scope, command, factory, client = early_case
    calls = []
    if fault == "live_owner":
        command.status = CommandStatus.RUNNING
        command.lease_owner = "predecessor"
        command.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
    elif fault == "budget":
        command.retry_count = command.max_retries
    elif fault == "cancelled":
        scope.status = ScopeStatus.CANCELLED
    elif fault == "expired_scope":
        scope.membership_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    authority = SimpleNamespace(
        base_url=SimpleNamespace(scheme="http" if fault == "http" else "https"),
        post=lambda *a, **k: calls.append(a),
        get=client.get,
    )
    if fault == "http":
        with pytest.raises(ValueError, match="HTTPS"):
            reconcile_scope_allocation(factory, scope.id, authority)
    else:
        assert not reconcile_scope_allocation(factory, scope.id, authority)
    assert calls == []


@pytest.fixture
def committed_early(early_case, phase1_engine):
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy.orm import sessionmaker

    source, scope, command, _, client = early_case
    project_id, scope_id, command_id = scope.project_id, scope.id, command.id
    owner_id = source.get(Project, project_id).owner_id
    connection = source.get_bind()
    source.commit()
    connection.commit()
    source.close()
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    try:
        yield phase1_engine, scope_id, command_id, factory, client
    finally:
        with factory() as db, db.begin():
            db.execute(delete(Project).where(Project.id == project_id))
            db.execute(delete(User).where(User.id == owner_id))
        with factory() as db:
            assert db.get(WorkflowCommand, command_id) is None


@pytest.mark.parametrize("fault", ["normal", "cancel", "new_owner", "expired_predecessor"])
def test_committed_claim_releases_connections_and_locks_during_http(committed_early, fault):
    from automl_api.models.workflows import PromotionalScope

    engine, scope_id, command_id, factory, client = committed_early
    if fault == "expired_predecessor":
        with factory() as db, db.begin():
            command = db.get(WorkflowCommand, command_id)
            command.status = CommandStatus.RUNNING
            command.lease_owner = "dead-predecessor"
            command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            command.retry_count = 1

    def post(path, **kwargs):
        assert engine.pool.checkedout() == 0
        # Independent connections can lock both rows while the provider request runs.
        with factory() as db, db.begin():
            scope = db.scalar(
                select(PromotionalScope)
                .where(PromotionalScope.id == scope_id)
                .with_for_update(nowait=True)
            )
            command = db.scalar(
                select(WorkflowCommand)
                .where(WorkflowCommand.id == command_id)
                .with_for_update(nowait=True)
            )
            assert command.status == CommandStatus.RUNNING
            assert command.retry_count == (2 if fault == "expired_predecessor" else 1)
            if fault == "cancel":
                scope.status = ScopeStatus.CANCELLED
            elif fault == "new_owner":
                command.lease_owner = "replacement"
        return client.post(path, **kwargs)

    def get(path):
        assert engine.pool.checkedout() == 0
        return client.get(path)

    authority = SimpleNamespace(base_url=client.base_url, post=post, get=get)
    if fault in ("cancel", "new_owner"):
        with pytest.raises(InvalidTransition):
            reconcile_scope_allocation(factory, scope_id, authority)
    else:
        assert reconcile_scope_allocation(factory, scope_id, authority)
    with factory() as db:
        command = db.get(WorkflowCommand, command_id)
        if fault == "new_owner":
            assert command.lease_owner == "replacement"
        elif fault == "cancel":
            assert command.status == CommandStatus.FAILED
        else:
            assert require_scope_allocation(db, db.get(PromotionalScope, scope_id))


def test_controller_registers_open_scope_with_approved_project_identity(early_case, monkeypatch):
    from automl_api.services import evaluation_reconciler

    db, scope, command, factory, client = early_case
    seen = []

    @contextmanager
    def authority_factory(project_id, reference):
        assert project_id == scope.project_id and reference == str(scope.project_id)
        seen.append(project_id)
        yield client

    monkeypatch.setattr(evaluation_reconciler, "_next_sweep", float("inf"))
    assert (
        evaluation_reconciler.reconcile_evaluation_jobs_once(
            factory, object(), authority_factory=authority_factory, public_key=object()
        )
        == 1
    )
    db.refresh(command)
    assert command.status == CommandStatus.SUCCEEDED and seen == [scope.project_id]
    assert (
        evaluation_reconciler.reconcile_evaluation_jobs_once(
            factory, object(), authority_factory=authority_factory, public_key=object()
        )
        == 0
    )


def test_malformed_intent_fails_durably_without_provider_io(early_case):
    db, scope, command, factory, client = early_case
    command.request_hash = "f" * 64
    db.flush()
    authority = SimpleNamespace(
        base_url=client.base_url,
        post=lambda *a, **k: pytest.fail("Invalid intent sent"),
        get=client.get,
    )
    assert not reconcile_scope_allocation(factory, scope.id, authority)
    db.refresh(command)
    assert command.status == CommandStatus.FAILED and command.retry_count == 1
    assert not reconcile_scope_allocation(factory, scope.id, authority)


@pytest.mark.parametrize(
    "phase", ["never_sent", "allocated", "lost_reply", "lost_abort_reply", "expired"]
)
def test_stopped_scope_seals_without_final_grant(early_case, authority_api, phase):
    from automl_api.services.scope_allocation import close_scope_allocation

    db, scope, command, factory, client = early_case
    public_key = authority_api[2]["signing_secret"].public_key()
    if phase != "never_sent":
        if phase == "lost_reply":

            def post(path, **kwargs):
                client.post(path, **kwargs).raise_for_status()
                raise ConnectionError("Lost creation response")

            with pytest.raises(ConnectionError):
                reconcile_scope_allocation(
                    factory,
                    scope.id,
                    SimpleNamespace(base_url=client.base_url, post=post, get=client.get),
                )
        else:
            assert reconcile_scope_allocation(factory, scope.id, client)
        db.refresh(command)
        command.available_at = datetime.now(UTC) - timedelta(seconds=1)
    if phase == "expired":
        scope.membership_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        scope.status = ScopeStatus.CANCELLED
    db.flush()
    if phase == "lost_abort_reply":

        def post(path, **kwargs):
            response = client.post(path, **kwargs)
            if path.endswith("/abort"):
                response.raise_for_status()
                raise ConnectionError("Lost abort response")
            return response

        with pytest.raises(ConnectionError):
            close_scope_allocation(
                factory,
                scope.id,
                SimpleNamespace(base_url=client.base_url, post=post, get=client.get),
                public_key=public_key,
            )
        db.refresh(command)
        command.available_at = datetime.now(UTC) - timedelta(seconds=1)
        db.flush()
    target = None if phase == "never_sent" else client
    assert close_scope_allocation(factory, scope.id, target, public_key=public_key)
    db.refresh(command)
    assert command.status == CommandStatus.CANCELLED
    if phase == "never_sent":
        assert command.retry_count == 0
    else:
        state = client.get(f"allocations/{command.response_payload['allocation_id']}").json()
        assert state["status"] == "failed"
        assert [r["operation"] for r in state["receipts"]] == ["abort"]
        assert command.response_payload["abort_receipt"] == state["receipts"][0]
    assert not close_scope_allocation(factory, scope.id, client, public_key=public_key)


def test_controller_cleans_stopped_pretraining_scope(early_case, authority_api, monkeypatch):
    from automl_api.services import evaluation_reconciler

    db, scope, command, factory, client = early_case
    assert reconcile_scope_allocation(factory, scope.id, client)
    scope.status = ScopeStatus.CANCELLED
    db.flush()
    monkeypatch.setattr(evaluation_reconciler, "_next_sweep", float("inf"))
    assert (
        evaluation_reconciler.reconcile_evaluation_jobs_once(
            factory,
            object(),
            authority_factory=lambda *_: nullcontext(client),
            public_key=authority_api[2]["signing_secret"].public_key(),
        )
        == 1
    )
    db.refresh(command)
    assert command.status == CommandStatus.CANCELLED


@pytest.mark.parametrize("entry", ["training", "membership"])
def test_api_services_return_conflict_while_registration_pending(early_case, monkeypatch, entry):
    from automl_api.models.workflows import OutboxEntry, PromotionalScopeMember
    from automl_api.services import contracts, training
    from fastapi import HTTPException
    from test_training_service_runtime import _launch_request

    db, scope, command, _, _ = early_case
    user = SimpleNamespace(id=command.actor_id)
    run = db.scalar(select(ModelRun).where(ModelRun.project_id == scope.project_id))
    if entry == "membership":
        db.execute(
            delete(PromotionalScopeMember).where(PromotionalScopeMember.scope_id == scope.id)
        )
        monkeypatch.setattr(contracts, "require_project_role", lambda *_: None)

        def invoke():
            return contracts.add_scope_member(db, user, scope.project_id, scope.id, run.id)
    else:
        monkeypatch.setattr(training, "require_project_role", lambda *_: None)
        monkeypatch.setattr(training, "_lock_training_admission", lambda *_: None)
        monkeypatch.setattr(
            training, "_resolve_estimate_identity", lambda _db, _p, request: request
        )
        monkeypatch.setattr(training, "_resolve_launch_revisions", lambda *_: {"scope": scope})
        payload = _launch_request(run.dataset_version_id).model_copy(
            update={"promotional_scope_id": scope.id}
        )

        def invoke():
            return training.launch_training_run(
                db, user, scope.project_id, payload, idempotency_key=uuid.uuid4().hex
            )

    with pytest.raises(HTTPException) as error:
        with db.begin_nested():
            invoke()
    assert error.value.status_code == 409
    assert error.value.detail == {"code": "scope_authority_not_ready"}
    assert scope.status == ScopeStatus.OPEN
    assert not list(
        db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == scope.project_id))
    )


def test_bad_controller_configuration_defers_without_starving_queue(early_case, monkeypatch):
    from automl_api.services import evaluation_reconciler

    db, _, command, factory, _ = early_case

    @contextmanager
    def unavailable(*_):
        raise ValueError("Private configuration must not be logged")
        yield

    monkeypatch.setattr(evaluation_reconciler, "_next_sweep", float("inf"))
    assert (
        evaluation_reconciler.reconcile_evaluation_jobs_once(
            factory, object(), authority_factory=unavailable, public_key=object()
        )
        == 1
    )
    db.refresh(command)
    assert command.retry_count == 0
    assert command.available_at > datetime.now(UTC)
    assert (
        evaluation_reconciler.reconcile_evaluation_jobs_once(
            factory, object(), authority_factory=unavailable, public_key=object()
        )
        == 0
    )


def test_native_evaluator_seed_waits_for_real_registration(policy_case, phase1_engine):
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from automl_api.models.workflows import PromotionalScope, WorkflowAttempt
    from sqlalchemy.orm import sessionmaker

    from scripts.validate_refit_workload import seed

    _, store, source_scope, _ = policy_case
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    scope_id, attempt_id, rows = seed(
        factory,
        store,
        slow=False,
        evaluation_policy=source_scope.comparison_policy["evaluation_policy"],
    )
    try:
        assert attempt_id is None and rows == 10
        with factory() as db:
            scope = db.get(PromotionalScope, scope_id)
            project = db.get(Project, scope.project_id)
            project_id, owner_id = project.id, project.owner_id
            assert scope.status == ScopeStatus.OPEN
            assert scope.sealed_at is scope.scope_started_at is scope.scope_deadline_at is None
            assert not list(
                db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.scope_id == scope_id))
            )
            command = db.scalar(
                select(WorkflowCommand).where(WorkflowCommand.resource_id == scope_id)
            )
            assert command.status == CommandStatus.PENDING and command.retry_count == 0
            with pytest.raises(InvalidTransition, match="not complete"):
                require_scope_allocation(db, scope)
    finally:
        with factory() as db, db.begin():
            scope = db.get(PromotionalScope, scope_id)
            project = db.get(Project, scope.project_id)
            owner_id, project_id = project.owner_id, project.id
            db.execute(delete(Project).where(Project.id == project_id))
            db.execute(delete(User).where(User.id == owner_id))


@pytest.mark.parametrize("entry", ["seal", "membership"])
def test_cached_scope_cannot_admit_after_committed_cancellation(
    committed_early, monkeypatch, entry
):
    from automl_api.models.workflows import PromotionalScope, PromotionalScopeMember
    from automl_api.services import contracts
    from automl_api.services.workflow_state import StaleFence
    from fastapi import HTTPException

    _, scope_id, _, factory, client = committed_early
    assert reconcile_scope_allocation(factory, scope_id, client)
    with factory() as caller:
        cached = caller.get(PromotionalScope, scope_id)
        project_id = cached.project_id
        version = cached.cas_version
        run_id = caller.scalar(select(ModelRun.id).where(ModelRun.project_id == project_id))
        caller.commit()
        with factory() as other, other.begin():
            current = other.get(PromotionalScope, scope_id)
            current.status = ScopeStatus.CANCELLED
            current.cas_version += 1
            other.execute(
                delete(PromotionalScopeMember).where(PromotionalScopeMember.scope_id == scope_id)
            )
        assert cached.status == ScopeStatus.OPEN
        if entry == "seal":
            with pytest.raises(StaleFence):
                seal_promotional_scope(caller, scope_id, expected_cas_version=version)
        else:
            monkeypatch.setattr(contracts, "require_project_role", lambda *_: None)
            with pytest.raises(HTTPException) as error:
                contracts.add_scope_member(caller, SimpleNamespace(), project_id, scope_id, run_id)
            assert error.value.status_code == 409
            assert error.value.detail == {"code": "scope_membership_closed"}
        assert cached.status == ScopeStatus.CANCELLED
        assert not list(
            caller.scalars(
                select(PromotionalScopeMember).where(PromotionalScopeMember.scope_id == scope_id)
            )
        )
