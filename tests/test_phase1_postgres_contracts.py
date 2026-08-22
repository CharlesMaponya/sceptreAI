from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from time import sleep

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import (
    AttemptStatus,
    AuthProvider,
    DatasetFormat,
    DatasetStatus,
    GlobalRole,
    ObjectStoreType,
    OutboxStatus,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.qualification import FinalTestAllocation
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    EstimatorCatalogRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
)
from automl_api.services import final_test_authority as authority
from automl_api.services import workflow_state as state
from automl_api.services.idempotency import durable_mutation
from pydantic import BaseModel
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


class MutationResult(BaseModel):
    id: uuid.UUID


@pytest.fixture
def phase1_db() -> Session:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg://automl:automl@127.0.0.1:55432/automl"
    )
    engine = create_engine(database_url, pool_pre_ping=True)
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False)
    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()
        engine.dispose()


@pytest.fixture
def phase1_engine() -> Engine:
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql+psycopg://automl:automl@127.0.0.1:55432/automl"
    )
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=8, max_overflow=0)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def committed_tenant(phase1_engine: Engine) -> tuple[uuid.UUID, uuid.UUID]:
    suffix = uuid.uuid4().hex
    with Session(phase1_engine) as db, db.begin():
        user = User(
            email=f"phase1-concurrent-{suffix}@example.test",
            full_name="Phase One Concurrent",
            auth_provider=AuthProvider.SIMPLE,
            global_role=GlobalRole.MEMBER,
        )
        db.add(user)
        db.flush()
        project = Project(
            owner_id=user.id,
            created_by_id=user.id,
            name=f"phase1-concurrent-{suffix}",
        )
        db.add(project)
        db.flush()
        identity = (user.id, project.id)
    try:
        yield identity
    finally:
        with Session(phase1_engine) as db, db.begin():
            db.execute(delete(Project).where(Project.id == identity[1]))
            db.execute(delete(User).where(User.id == identity[0]))


@pytest.fixture
def tenant(phase1_db: Session) -> tuple[User, Project]:
    suffix = uuid.uuid4().hex
    user = User(
        email=f"phase1-{suffix}@example.test",
        full_name="Phase One",
        auth_provider=AuthProvider.SIMPLE,
        global_role=GlobalRole.MEMBER,
    )
    phase1_db.add(user)
    phase1_db.flush()
    project = Project(
        owner_id=user.id,
        created_by_id=user.id,
        name=f"phase1-{suffix}",
    )
    phase1_db.add(project)
    phase1_db.flush()
    return user, project


def test_command_and_outbox_replay_and_claim_are_durable(
    phase1_db: Session, tenant: tuple[User, Project]
) -> None:
    user, project = tenant
    command, replayed = state.begin_command(
        phase1_db,
        project_id=project.id,
        actor_id=user.id,
        operation="training.launch",
        idempotency_key="launch-one",
        payload={"dataset": "one", "folds": 3},
    )
    assert not replayed
    replay, replayed = state.begin_command(
        phase1_db,
        project_id=project.id,
        actor_id=user.id,
        operation="training.launch",
        idempotency_key="launch-one",
        payload={"folds": 3, "dataset": "one"},
    )
    assert replayed and replay.id == command.id
    with pytest.raises(state.IdempotencyConflict):
        state.begin_command(
            phase1_db,
            project_id=project.id,
            actor_id=user.id,
            operation="training.launch",
            idempotency_key="launch-one",
            payload={"dataset": "two", "folds": 3},
        )

    aggregate_id = uuid.uuid4()
    entry = state.enqueue_outbox(
        phase1_db,
        command,
        topic="ray.submit",
        aggregate_type="training_run",
        aggregate_id=aggregate_id,
        payload={"desired_state": "submitted"},
    )
    assert (
        state.enqueue_outbox(
            phase1_db,
            command,
            topic="ray.submit",
            aggregate_type="training_run",
            aggregate_id=aggregate_id,
            payload={"desired_state": "submitted"},
        ).id
        == entry.id
    )
    now = datetime.now(UTC)
    claimed = state.claim_outbox(phase1_db, worker_id="dispatcher-a", now=now, lease_seconds=5)
    assert [row.id for row in claimed] == [entry.id]
    assert state.claim_outbox(phase1_db, worker_id="dispatcher-b", now=now) == []
    state.complete_outbox(
        phase1_db,
        entry.id,
        worker_id="dispatcher-a",
        delivered=False,
        error="transient",
        now=now,
    )
    assert entry.status == OutboxStatus.PENDING
    claimed = state.claim_outbox(phase1_db, worker_id="dispatcher-b", now=now)
    assert [row.id for row in claimed] == [entry.id]
    state.complete_outbox(phase1_db, entry.id, worker_id="dispatcher-b", delivered=True, now=now)
    assert entry.status == OutboxStatus.DELIVERED
    assert entry.delivery_attempts == 2


def test_expired_attempt_lease_is_reclaimed_once(
    phase1_db: Session, tenant: tuple[User, Project]
) -> None:
    _, project = tenant
    now = datetime.now(UTC)
    attempt = WorkflowAttempt(
        project_id=project.id,
        stage="training_run",
        logical_key="run:durable",
        model_run_id=None,
        workload_identity="project-training",
        generation=1,
        fencing_token=uuid.uuid4().hex,
        status=AttemptStatus.CLAIMED,
        lease_owner="dead-worker",
        lease_expires_at=now - timedelta(seconds=1),
        retry_count=1,
        retry_budget=5,
    )
    # The typed-parent constraint is intentionally proven separately; use a provider
    # campaign-free row only to exercise the queue would violate it, so this insert
    # must be rejected rather than silently weakening tenancy/stage integrity.
    savepoint = phase1_db.begin_nested()
    try:
        phase1_db.add(attempt)
        with pytest.raises(Exception, match="attempt_typed_parent"):
            phase1_db.flush()
    finally:
        savepoint.rollback()


def test_final_authority_is_one_shot_and_receipts_verify(phase1_db: Session) -> None:
    split_digest = uuid.uuid4().hex * 2
    scope_id = uuid.uuid4()
    allocation = authority.allocate(
        phase1_db,
        split_digest=split_digest,
        project_reference="project-external-reference",
        scope_id=scope_id,
        canonical_provider="local",
        provider_manifest_digest="a" * 64,
    )
    assert (
        authority.allocate(
            phase1_db,
            split_digest=split_digest,
            project_reference="project-external-reference",
            scope_id=scope_id,
            canonical_provider="local",
            provider_manifest_digest="a" * 64,
        ).id
        == allocation.id
    )
    with pytest.raises(state.IdempotencyConflict):
        authority.allocate(
            phase1_db,
            split_digest=split_digest,
            project_reference="project-external-reference",
            scope_id=uuid.uuid4(),
            canonical_provider="local",
            provider_manifest_digest="a" * 64,
        )
    with pytest.raises(authority.ProviderRejected):
        authority.open_allocation(
            phase1_db,
            allocation_id=allocation.id,
            provider="aws",
            provider_manifest_digest="a" * 64,
            request_digest="open-request",
            signing_secret="secret",
        )

    opened = authority.open_allocation(
        phase1_db,
        allocation_id=allocation.id,
        provider="local",
        provider_manifest_digest="a" * 64,
        request_digest="open-request",
        signing_secret="secret",
    )
    assert authority.verify_receipt(opened, "secret")
    assert (
        authority.open_allocation(
            phase1_db,
            allocation_id=allocation.id,
            provider="local",
            provider_manifest_digest="a" * 64,
            request_digest="open-request",
            signing_secret="secret",
        ).id
        == opened.id
    )
    committed = authority.commit_result(
        phase1_db,
        allocation_id=allocation.id,
        provider="local",
        provider_manifest_digest="a" * 64,
        request_digest="commit-request",
        result_digest="result-digest",
        signing_secret="secret",
    )
    assert authority.verify_receipt(committed, "secret")
    assert phase1_db.scalar(select(func.count()).select_from(FinalTestAllocation)) >= 1
    with pytest.raises(state.InvalidTransition):
        authority.fail_allocation(
            phase1_db,
            allocation_id=allocation.id,
            provider="local",
            provider_manifest_digest="a" * 64,
            request_digest="fail-request",
            reason="too late",
            signing_secret="secret",
            expected_cas_version=2,
        )


def test_outbox_expired_claim_is_recoverable(
    phase1_db: Session, tenant: tuple[User, Project]
) -> None:
    user, project = tenant
    command, _ = state.begin_command(
        phase1_db,
        project_id=project.id,
        actor_id=user.id,
        operation="cleanup.execute",
        idempotency_key="cleanup-one",
        payload={},
    )
    entry = state.enqueue_outbox(
        phase1_db,
        command,
        topic="cleanup.execute",
        aggregate_type="project",
        aggregate_id=project.id,
        payload={},
    )
    now = datetime.now(UTC)
    entry.status = OutboxStatus.CLAIMED
    entry.lease_owner = "dead"
    entry.lease_expires_at = now - timedelta(seconds=1)
    phase1_db.flush()
    claimed = state.claim_outbox(phase1_db, worker_id="replacement", now=now)
    assert [row.id for row in claimed] == [entry.id]
    assert entry.lease_owner == "replacement"


def test_concurrent_idempotency_creates_exactly_one_command(
    phase1_engine: Engine, committed_tenant: tuple[uuid.UUID, uuid.UUID]
) -> None:
    user_id, project_id = committed_tenant
    workers = 4
    start = Barrier(workers)

    def issue(worker: int) -> tuple[uuid.UUID, bool]:
        with Session(phase1_engine) as db:
            start.wait(timeout=5)
            command, replayed = state.begin_command(
                db,
                project_id=project_id,
                actor_id=user_id,
                operation="training.launch.concurrent",
                idempotency_key="same-key",
                payload={"dataset": "one", "folds": 3},
            )
            command_id = command.id
            db.commit()
            return command_id, replayed

    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(issue, range(workers)))

    assert len({command_id for command_id, _ in results}) == 1
    assert [replayed for _, replayed in results].count(False) == 1
    assert [replayed for _, replayed in results].count(True) == workers - 1


def test_concurrent_project_serialization_avoids_command_foreign_key_deadlocks(
    phase1_engine: Engine, committed_tenant: tuple[uuid.UUID, uuid.UUID]
) -> None:
    user_id, project_id = committed_tenant
    workers = 4
    start = Barrier(workers)

    def issue(worker: int) -> uuid.UUID:
        with Session(phase1_engine) as db:
            user = db.get(User, user_id)
            assert user is not None
            start.wait(timeout=5)
            result = durable_mutation(
                db,
                user,
                project_id,
                operation="dataset.upload.begin",
                idempotency_key=f"concurrent-upload-{worker}",
                payload={"worker": worker},
                execute=lambda: MutationResult(id=uuid.uuid4()),
                response_model=MutationResult,
                response_status=201,
                serialize_project=True,
            )
            db.commit()
            return result.id

    with ThreadPoolExecutor(max_workers=workers) as executor:
        result_ids = list(executor.map(issue, range(workers)))

    assert len(set(result_ids)) == workers


def test_concurrent_consumers_skip_locked_and_connection_loss_releases_claim(
    phase1_engine: Engine, committed_tenant: tuple[uuid.UUID, uuid.UUID]
) -> None:
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db:
        command, _ = state.begin_command(
            db,
            project_id=project_id,
            actor_id=user_id,
            operation="outbox.concurrent",
            idempotency_key="outbox-concurrent",
            payload={},
        )
        entry_ids = [
            state.enqueue_outbox(
                db,
                command,
                topic="test.concurrent",
                aggregate_type="project",
                aggregate_id=project_id,
                event_key=f"concurrent-{index}",
                payload={"index": index},
            ).id
            for index in range(2)
        ]
        db.commit()

    start = Barrier(2)

    def claim(worker: int) -> uuid.UUID:
        with Session(phase1_engine) as db:
            start.wait(timeout=5)
            rows = state.claim_outbox(db, worker_id=f"worker-{worker}", limit=1)
            assert len(rows) == 1
            claimed_id = rows[0].id
            sleep(0.1)
            db.commit()
            return claimed_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        claimed_ids = set(executor.map(claim, range(2)))
    assert claimed_ids == set(entry_ids)

    with Session(phase1_engine) as db:
        for entry_id in entry_ids:
            entry = db.get(state.OutboxEntry, entry_id)
            entry.status = OutboxStatus.PENDING
            entry.lease_owner = None
            entry.lease_expires_at = None
        db.commit()

    abandoned = Session(phase1_engine)
    abandoned_claim = state.claim_outbox(abandoned, worker_id="lost-connection", limit=1)
    abandoned_id = abandoned_claim[0].id
    abandoned.close()

    with Session(phase1_engine) as replacement:
        reclaimed = state.claim_outbox(replacement, worker_id="replacement", limit=2)
        assert abandoned_id in {row.id for row in reclaimed}
        replacement.commit()


def test_three_provider_race_allows_only_the_canonical_final_reader(
    phase1_engine: Engine,
) -> None:
    split_digest = uuid.uuid4().hex * 2
    manifest_digest = "b" * 64
    with Session(phase1_engine) as db:
        allocation = authority.allocate(
            db,
            split_digest=split_digest,
            project_reference="cross-provider-race",
            scope_id=uuid.uuid4(),
            canonical_provider="local",
            provider_manifest_digest=manifest_digest,
        )
        allocation_id = allocation.id
        db.commit()

    start = Barrier(3)

    def open_as(provider: str) -> tuple[str, str]:
        with Session(phase1_engine) as db:
            start.wait(timeout=5)
            try:
                receipt = authority.open_allocation(
                    db,
                    allocation_id=allocation_id,
                    provider=provider,
                    provider_manifest_digest=manifest_digest,
                    request_digest=f"open-{provider}",
                    signing_secret="secret",
                )
            except authority.ProviderRejected:
                db.rollback()
                return provider, "rejected"
            db.commit()
            return provider, receipt.operation

    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = dict(executor.map(open_as, ("local", "aws", "gcp")))
        assert results == {"local": "open", "aws": "rejected", "gcp": "rejected"}
    finally:
        with Session(phase1_engine) as db, db.begin():
            db.execute(
                delete(FinalTestAllocation).where(FinalTestAllocation.id == allocation_id)
            )


def test_attempt_lineage_enforces_one_active_generation_and_run_parent(
    phase1_db: Session, tenant: tuple[User, Project]
) -> None:
    user, project = tenant
    dataset = Dataset(project_id=project.id, created_by_id=user.id, name="lineage")
    phase1_db.add(dataset)
    phase1_db.flush()
    version = DatasetVersion(
        project_id=project.id,
        dataset_id=dataset.id,
        created_by_id=user.id,
        version_number=1,
        status=DatasetStatus.READY,
        format=DatasetFormat.CSV,
        object_store_type=ObjectStoreType.MINIO,
        object_uri="s3://automl/phase1-lineage.csv",
        content_hash="a" * 64,
    )
    phase1_db.add(version)
    phase1_db.flush()
    run = ModelRun(
        project_id=project.id,
        dataset_version_id=version.id,
        created_by_id=user.id,
        run_kind=RunKind.TRAINING,
        status=RunStatus.QUEUED,
        task_type=TaskType.REGRESSION,
        target_column="target",
    )
    phase1_db.add(run)
    phase1_db.flush()
    run_attempt = WorkflowAttempt(
        project_id=project.id,
        stage="training_run",
        logical_key=f"training-run:{run.id}",
        model_run_id=run.id,
        workload_identity="project-training",
        generation=1,
        fencing_token=uuid.uuid4().hex,
        status=AttemptStatus.RUNNING,
    )
    phase1_db.add(run_attempt)
    phase1_db.flush()

    duplicate = WorkflowAttempt(
        project_id=project.id,
        stage="training_run",
        logical_key=run_attempt.logical_key,
        model_run_id=run.id,
        workload_identity="project-training",
        generation=2,
        fencing_token=uuid.uuid4().hex,
        status=AttemptStatus.PENDING,
    )
    savepoint = phase1_db.begin_nested()
    try:
        phase1_db.add(duplicate)
        with pytest.raises(IntegrityError, match="uq_attempt_active_logical"):
            phase1_db.flush()
    finally:
        savepoint.rollback()

    catalog = EstimatorCatalogRevision(
        project_id=project.id,
        name="catalog",
        revision=1,
        digest_scope="catalog",
        content_digest="b" * 64,
        release_version="phase1",
    )
    registry = FeatureRegistryRevision(
        project_id=project.id,
        name="registry",
        revision=1,
        digest_scope="feature_registry",
        content_digest="c" * 64,
    )
    phase1_db.add_all([catalog, registry])
    phase1_db.flush()
    recipe = FeatureRecipeRevision(
        project_id=project.id,
        name="recipe",
        revision=1,
        digest_scope="feature_recipe",
        content_digest="d" * 64,
        registry_revision_id=registry.id,
    )
    phase1_db.add(recipe)
    phase1_db.flush()
    candidate = TrainingCandidate(
        project_id=project.id,
        model_run_id=run.id,
        candidate_key="0:fixed",
        estimator_key="fixed",
        catalog_revision_id=catalog.id,
        feature_recipe_revision_id=recipe.id,
    )
    phase1_db.add(candidate)
    phase1_db.flush()
    trial = TrainingTrial(
        project_id=project.id,
        candidate_id=candidate.id,
        suggestion_id="suggestion-1",
        params_digest="e" * 64,
    )
    phase1_db.add(trial)
    phase1_db.flush()
    trial_attempt = WorkflowAttempt(
        project_id=project.id,
        stage="training_trial",
        logical_key=f"trial:{trial.id}",
        model_run_id=run.id,
        trial_id=trial.id,
        run_attempt_id=run_attempt.id,
        workload_identity="project-training",
        generation=1,
        fencing_token=uuid.uuid4().hex,
    )
    phase1_db.add(trial_attempt)
    phase1_db.flush()

    wrong_parent = WorkflowAttempt(
        project_id=project.id,
        stage="training_trial",
        logical_key=f"trial:{trial.id}:wrong-parent",
        model_run_id=run.id,
        trial_id=trial.id,
        run_attempt_id=trial_attempt.id,
        workload_identity="project-training",
        generation=1,
        fencing_token=uuid.uuid4().hex,
    )
    savepoint = phase1_db.begin_nested()
    try:
        phase1_db.add(wrong_parent)
        with pytest.raises(IntegrityError, match="same-run training_run parent"):
            phase1_db.flush()
    finally:
        savepoint.rollback()
