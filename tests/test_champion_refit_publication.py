from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import AttemptStatus, AuthProvider, RunStatus, ScopeStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    PromotionalScope,
    WorkflowAttempt,
    WorkflowCheckpoint,
    WorkflowEvent,
)
from automl_api.services.workflow_state import IdempotencyConflict, StaleFence
from automl_api.training.champion_refit import (
    FrozenPipeline,
    execute_refit,
    publish_frozen_pipeline,
)
from sqlalchemy import func, select

pytest_plugins = ["test_phase1_postgres_contracts", "test_champion_refit_execution"]


@pytest.fixture
def durable_refit(phase1_db, refit_case):
    db = phase1_db
    store, plan = refit_case
    user = User(
        email=f"refit-{uuid.uuid4()}@example.test",
        full_name="Refit test",
        auth_provider=AuthProvider.SIMPLE,
    )
    db.add(user)
    db.flush()
    db.add(Project(id=plan.project_id, owner_id=user.id, created_by_id=user.id, name="refit"))
    db.flush()
    dataset = Dataset(project_id=plan.project_id, created_by_id=user.id, name="refit")
    db.add(dataset)
    db.flush()
    db.add(
        DatasetVersion(
            id=plan.dataset_version_id,
            project_id=plan.project_id,
            dataset_id=dataset.id,
            created_by_id=user.id,
            version_number=1,
            object_uri="s3://test/raw.csv",
            content_hash="a" * 64,
            format="CSV",
            object_store_type="EMBEDDED",
        )
    )
    db.flush()
    split = DatasetSplitRevision(
        project_id=plan.project_id,
        dataset_version_id=plan.dataset_version_id,
        revision=1,
        content_digest="b" * 64,
        digest_scope="split",
        train_digest=plan.partitions[0].row_digest,
        validation_digest=plan.partitions[1].row_digest,
        final_test_digest="c" * 64,
    )
    db.add(split)
    db.flush()
    scope = PromotionalScope(
        id=plan.scope_id,
        project_id=plan.project_id,
        scope_key=str(plan.scope_id),
        split_revision_id=split.id,
        canonical_provider="aws",
        expected_members=1,
        status=ScopeStatus.RUNNING,
        scope_deadline_at=datetime.now(UTC) + timedelta(hours=1),
        comparison_policy={"refit_policy_digest": plan.policy_digest},
    )
    db.add(scope)
    db.add(
        ModelRun(
            id=plan.run_id,
            project_id=plan.project_id,
            dataset_version_id=plan.dataset_version_id,
            created_by_id=user.id,
            status=RunStatus.SUCCEEDED,
            task_type=TaskType.REGRESSION,
        )
    )
    db.flush()
    attempt = WorkflowAttempt(
        id=plan.attempt_id,
        project_id=plan.project_id,
        scope_id=plan.scope_id,
        model_run_id=plan.run_id,
        dataset_version_id=plan.dataset_version_id,
        stage="champion_refit",
        logical_key=f"refit:{plan.scope_id}",
        generation=1,
        fencing_token=uuid.uuid4().hex,
        workload_identity="refit",
        status=AttemptStatus.RUNNING,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        retry_budget=1,
    )
    db.add(attempt)
    db.flush()
    db.add(
        WorkflowEvent(
            project_id=plan.project_id,
            attempt_id=attempt.id,
            sequence=1,
            event_key="refit_plan",
            event_type="refit_plan",
            payload=plan.model_dump(mode="json"),
            result_digest=plan.digest,
        )
    )
    db.flush()
    result = FrozenPipeline.model_validate(execute_refit(plan, store))
    return db, plan, result, attempt, scope


def test_refit_publishes_one_pipeline_with_idempotent_replay(durable_refit):
    db, plan, result, attempt, _ = durable_refit
    first = publish_frozen_pipeline(db, plan, result, fencing_token=attempt.fencing_token)
    db.flush()
    db.expire_all()
    replay = publish_frozen_pipeline(db, plan, result, fencing_token=attempt.fencing_token)
    assert replay.id == first.id
    assert db.get(WorkflowAttempt, attempt.id).status == AttemptStatus.SUCCEEDED
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowCheckpoint)
            .where(WorkflowCheckpoint.attempt_id == attempt.id)
        )
        == 1
    )
    altered = result.model_copy(
        update={
            "frozen_pipeline_digest": "d" * 64,
            "frozen_pipeline_uri": result.frozen_pipeline_uri.replace(
                result.frozen_pipeline_digest, "d" * 64
            ),
        }
    )
    with pytest.raises(IdempotencyConflict):
        publish_frozen_pipeline(db, plan, altered, fencing_token=attempt.fencing_token)


@pytest.mark.parametrize(
    "invalid", ["fence", "cancelled", "policy", "plan", "lease_expired", "deadline_expired"]
)
def test_refit_publication_rejects_stale_or_changed_execution(durable_refit, invalid):
    db, plan, result, attempt, scope = durable_refit
    fence = attempt.fencing_token
    if invalid == "fence":
        fence = "stale"
    elif invalid == "cancelled":
        scope.status = ScopeStatus.CANCELLED
    elif invalid == "policy":
        scope.comparison_policy = {"refit_policy_digest": "d" * 64}
    elif invalid == "lease_expired":
        attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    elif invalid == "deadline_expired":
        scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        plan = plan.model_copy(update={"max_rows": plan.max_rows + 1})
    db.flush()
    with pytest.raises(StaleFence):
        publish_frozen_pipeline(db, plan, result, fencing_token=fence)
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowCheckpoint)
            .where(WorkflowCheckpoint.attempt_id == attempt.id)
        )
        == 0
    )


def test_independent_refit_retry_preserves_choice_and_rejects_old_publisher(
    durable_refit, refit_case
):
    from automl_api.services.workflow_state import transition_attempt

    db, old_plan, old_result, old_attempt, _ = durable_refit
    store, _ = refit_case
    transition_attempt(
        old_attempt,
        AttemptStatus.FAILED,
        fencing_token=old_attempt.fencing_token,
        terminal_reason="worker lost after artifact upload",
    )
    db.flush()
    plan = old_plan.model_copy(update={"attempt_id": uuid.uuid4()})
    assert plan.policy_digest == old_plan.policy_digest
    assert plan.digest != old_plan.digest
    attempt = WorkflowAttempt(
        id=plan.attempt_id,
        project_id=plan.project_id,
        scope_id=plan.scope_id,
        dataset_version_id=plan.dataset_version_id,
        model_run_id=plan.run_id,
        stage="champion_refit",
        logical_key=old_attempt.logical_key,
        generation=2,
        fencing_token=uuid.uuid4().hex,
        workload_identity="refit",
        status=AttemptStatus.RUNNING,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        retry_budget=1,
        retry_count=1,
        predecessor_attempt_id=old_attempt.id,
    )
    db.add(attempt)
    db.flush()
    db.add(
        WorkflowEvent(
            project_id=plan.project_id,
            attempt_id=attempt.id,
            sequence=1,
            event_key="refit_plan",
            event_type="refit_plan",
            payload=plan.model_dump(mode="json"),
            result_digest=plan.digest,
        )
    )
    db.flush()
    result = FrozenPipeline.model_validate(execute_refit(plan, store))
    checkpoint = publish_frozen_pipeline(db, plan, result, fencing_token=attempt.fencing_token)
    assert checkpoint.attempt_id == attempt.id
    assert db.get(WorkflowAttempt, old_attempt.id).status == AttemptStatus.FAILED
    with pytest.raises((StaleFence, IdempotencyConflict)):
        publish_frozen_pipeline(db, old_plan, old_result, fencing_token=old_attempt.fencing_token)
    assert (
        db.scalar(
            select(func.count())
            .select_from(WorkflowCheckpoint)
            .join(
                WorkflowAttempt,
                WorkflowCheckpoint.attempt_id == WorkflowAttempt.id,
            )
            .where(WorkflowAttempt.scope_id == plan.scope_id)
        )
        == 1
    )


def test_worker_executes_a_submitted_attempt_and_rejects_duplicate_start(durable_refit, refit_case):
    from automl_api.training.champion_refit import run_refit_attempt
    from sqlalchemy.orm import Session

    db, plan, _, attempt, _ = durable_refit
    store, _ = refit_case
    attempt.status = AttemptStatus.SUBMITTED
    db.flush()
    factory = lambda: Session(bind=db.get_bind(), expire_on_commit=False)  # noqa: E731
    result = run_refit_attempt(
        factory,
        store,
        project_id=plan.project_id,
        attempt_id=attempt.id,
        fencing_token=attempt.fencing_token,
    )
    db.expire_all()
    assert attempt.status == AttemptStatus.SUCCEEDED
    assert result.row_count == 10
    assert attempt.checkpoint_uri == result.frozen_pipeline_uri
    with pytest.raises(StaleFence):
        run_refit_attempt(
            factory,
            store,
            project_id=plan.project_id,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
        )


def test_worker_records_failure_without_publishing(durable_refit, refit_case, monkeypatch):
    from automl_api.training.champion_refit import run_refit_attempt
    from sqlalchemy.orm import Session

    db, plan, _, attempt, _ = durable_refit
    store, _ = refit_case
    attempt.status = AttemptStatus.SUBMITTED
    db.flush()
    factory = lambda: Session(bind=db.get_bind(), expire_on_commit=False)  # noqa: E731

    def broken(*args):
        raise OSError("store unavailable, do-not-store-provider-secrets")

    monkeypatch.setattr(store, "stat", broken)
    with pytest.raises(OSError):
        run_refit_attempt(
            factory,
            store,
            project_id=plan.project_id,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
        )
    db.expire_all()
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.checkpoint_uri is None
    assert "do-not-store-provider-secrets" not in attempt.terminal_reason


@pytest.mark.parametrize("cancel_scope", [False, True])
def test_worker_heartbeat_checks_current_scope_state(
    durable_refit, refit_case, monkeypatch, cancel_scope
):
    import threading

    from automl_api.training.champion_refit import run_refit_attempt
    from sqlalchemy.orm import Session

    db, plan, _, attempt, scope = durable_refit
    store, _ = refit_case
    attempt.status = AttemptStatus.SUBMITTED
    db.flush()
    factory = lambda: Session(bind=db.get_bind(), expire_on_commit=False)  # noqa: E731
    original_event = threading.Event

    class OneTick(original_event):
        calls = 0

        def wait(self, timeout=None):
            self.calls += 1
            return self.calls > 1

    class InlineHeartbeat:
        def __init__(self, *, target, **kwargs):
            self.target = target

        def start(self):
            if cancel_scope:
                scope.status = ScopeStatus.CANCELLED
                db.flush()
            self.target()

        def join(self, timeout=None):
            pass

    # Exercise the actual PostgreSQL heartbeat statement once without a timer;
    # cancellation happens after the worker's committed start transaction.
    monkeypatch.setattr(threading, "Event", OneTick)
    monkeypatch.setattr(threading, "Thread", InlineHeartbeat)
    if cancel_scope:
        with pytest.raises(StaleFence):
            run_refit_attempt(
                factory,
                store,
                project_id=plan.project_id,
                attempt_id=attempt.id,
                fencing_token=attempt.fencing_token,
            )
    else:
        result = run_refit_attempt(
            factory,
            store,
            project_id=plan.project_id,
            attempt_id=attempt.id,
            fencing_token=attempt.fencing_token,
        )
        assert result.row_count == 10
    db.expire_all()
    assert attempt.status == (AttemptStatus.FAILED if cancel_scope else AttemptStatus.SUCCEEDED)


@pytest.mark.parametrize("root", ["s3c://test", "az://account/container", "gs://test"])
def test_publication_preserves_provider_bucket_and_container(durable_refit, root):
    from automl_api.qualification_control import RefitPublication

    db, plan, result, attempt, _ = durable_refit
    candidate = plan.candidate.model_copy(
        update={
            "uri": plan.candidate.uri.replace("embedded://test", root),
        }
    )
    plan = plan.model_copy(update={"candidate": candidate})
    event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == "refit_plan",
        )
    )
    event.payload = plan.model_dump(mode="json")
    event.result_digest = plan.digest
    result = result.model_copy(
        update={
            "frozen_pipeline_uri": result.frozen_pipeline_uri.replace("embedded://test", root),
        }
    )
    db.flush()
    checkpoint = publish_frozen_pipeline(db, plan, result, fencing_token=attempt.fencing_token)
    assert checkpoint.object_uri.startswith(root + "/projects/")
    publication = RefitPublication(
        refit_attempt_id=attempt.id,
        frozen_pipeline_digest=result.frozen_pipeline_digest,
        frozen_pipeline_uri=result.frozen_pipeline_uri,
        refit_policy_digest=result.refit_policy_digest,
    )
    assert publication.frozen_pipeline_uri == checkpoint.object_uri
