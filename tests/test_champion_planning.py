from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus, ScopeStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    CapacityReservation,
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureSearchSpaceRevision,
    OutboxEntry,
    PromotionalScopeMember,
    SearchObjectiveRevision,
    WorkflowAttempt,
    WorkflowCommand,
    WorkflowEvent,
)
from automl_api.services.champion_planning import plan_scope_refit, validate_refit_policy
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash
from automl_api.training.champion_refit import RefitPlan
from sqlalchemy import delete, select

pytest_plugins = ["test_champion_refit_publication"]


@pytest.fixture
def planning_case(durable_refit, refit_case):
    db, plan, _, attempt, scope = durable_refit
    store, _ = refit_case
    db.execute(delete(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
    db.delete(attempt)
    db.flush()
    common = dict(project_id=scope.project_id, revision=1, digest_scope="fixture",
                  content_digest="a" * 64, name="fixture")
    contract = FeatureContractRevision(**common, task_type="regression", target_column="target")
    space = FeatureSearchSpaceRevision(**common, metric_name="rmse", metric_direction="minimize")
    objective = SearchObjectiveRevision(**common, metric_name="rmse", metric_direction="minimize")
    catalog = EstimatorCatalogRevision(**common, release_version="fixture")
    db.add_all([contract, space, objective, catalog])
    db.flush()
    experiment = ExperimentSpecRevision(
        **common, dataset_version_id=plan.dataset_version_id,
        split_revision_id=scope.split_revision_id, feature_contract_revision_id=contract.id,
        feature_search_space_revision_id=space.id, search_objective_revision_id=objective.id,
        catalog_revision_id=catalog.id, task_type="regression",
        target_column="target", primary_metric="rmse",
    )
    db.add(experiment)
    db.flush()
    split = db.get(DatasetSplitRevision, scope.split_revision_id)
    split.sealed_at = datetime.now(UTC)
    split.digest_scope = "xor-sha256-row-id-v1"
    split.specification = {
        "split_counts": {part.role: part.rows for part in plan.partitions},
        "uris": {part.role: part.uri.rsplit("/", 1)[0] for part in plan.partitions},
    }
    scope.experiment_spec_revision_id = experiment.id
    scope.mode = "promotional"
    scope.status = ScopeStatus.SEALED
    scope.sealed_at = datetime.now(UTC)
    scope.comparison_policy = {
        "refit_policy": plan.model_dump(mode="json", exclude={
            "project_id", "scope_id", "attempt_id", "run_id", "candidate",
        }),
        "refit_policy_digest": plan.policy_digest,
    }
    scope.membership_digest = canonical_request_hash([
        {"ordinal": 0, "model_run_id": str(plan.run_id)},
    ])
    db.add(PromotionalScopeMember(
        project_id=plan.project_id, scope_id=scope.id, model_run_id=plan.run_id,
        ordinal=0, released_at=datetime.now(UTC),
    ))
    db.add(WorkflowAttempt(
        project_id=plan.project_id, model_run_id=plan.run_id, stage="training_run",
        logical_key=f"training:{plan.run_id}", workload_identity="fixture",
        generation=1, fencing_token=uuid.uuid4().hex, status=AttemptStatus.SUCCEEDED,
    ))
    run = db.get(ModelRun, plan.run_id)
    run.target_column = plan.target_column
    run.params = {"split_revision_id": str(split.id)}
    run.tags = {"leaderboard_primary_metric": "rmse", "winner": "wrong-cached-winner",
                "leaderboard": [
                    {"model": "candidate", "status": "succeeded", "metrics": {"rmse": 2.0},
                     "model_artifact_uri": plan.candidate.uri,
                     "model_artifact_sha256": plan.candidate.sha256},
                    {"model": "wrong-cached-winner", "status": "succeeded",
                     "metrics": {"rmse": 8.0}, "primary_score": -999,
                     "model_artifact_uri": "s3://wrong/unused",
                     "model_artifact_sha256": "f" * 64},
                ]}
    db.flush()
    return db, store, plan, scope, run


def test_planner_selects_validation_score_and_replays_one_durable_plan(planning_case):
    db, store, expected, scope, _ = planning_case
    attempt = plan_scope_refit(db, scope.id, store)
    assert attempt.status == AttemptStatus.PENDING
    assert scope.status == ScopeStatus.RUNNING
    event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
    actual = RefitPlan.model_validate(event.payload)
    assert actual.candidate == expected.candidate
    assert actual.policy_digest == expected.policy_digest
    assert event.result_digest == actual.digest
    assert plan_scope_refit(db, scope.id, store).id == attempt.id


def test_refit_replacement_keeps_original_candidate_without_reranking(planning_case):
    db, store, expected, scope, run = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    run.tags = {"leaderboard": []}  # A retry uses durable selection, not mutable leaderboards.
    db.flush()
    replacement = plan_scope_refit(db, scope.id, store)
    assert replacement.generation == 2
    assert replacement.predecessor_attempt_id == first.id
    assert replacement.fencing_token != first.fencing_token
    event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == replacement.id))
    assert RefitPlan.model_validate(event.payload).candidate == expected.candidate
    replacement.status = AttemptStatus.FAILED
    db.flush()
    assert plan_scope_refit(db, scope.id, store) is None
    assert scope.status == ScopeStatus.FAILED


def test_active_training_attempt_blocks_selection_even_with_terminal_run(planning_case):
    db, store, _, scope, run = planning_case
    training = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run.id))
    training.status = AttemptStatus.RUNNING
    db.flush()
    assert plan_scope_refit(db, scope.id, store) is None
    assert scope.status == ScopeStatus.SEALED


@pytest.mark.parametrize("fault", ["membership", "digest", "row_count", "tier", "split"])
def test_planner_rejects_changed_barrier_policy_or_search_tier(planning_case, fault):
    db, store, _, scope, run = planning_case
    if fault == "membership":
        scope.membership_digest = "e" * 64
    elif fault == "digest":
        scope.comparison_policy = {**scope.comparison_policy, "refit_policy_digest": "e" * 64}
    elif fault == "row_count":
        split = db.get(DatasetSplitRevision, scope.split_revision_id)
        split.specification = {**split.specification, "split_counts": {"train": 6, "validation": 5}}
    elif fault == "tier":
        run.params = {**run.params, "sample_tier_rows": 2}
    else:
        run.params = {"split_revision_id": str(uuid.uuid4())}
    db.flush()
    with pytest.raises(InvalidTransition):
        plan_scope_refit(db, scope.id, store)
    assert not list(db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.scope_id == scope.id)))


def test_expired_scope_cannot_plan_a_refit(planning_case):
    db, store, _, scope, _ = planning_case
    scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    assert plan_scope_refit(db, scope.id, store) is None
    assert scope.status == ScopeStatus.FAILED


def test_policy_digest_matches_worker_and_rejects_another_role_prefix(planning_case):
    db, _, expected, scope, _ = planning_case
    _, digest = validate_refit_policy(db, scope)
    assert digest == expected.policy_digest
    policy = scope.comparison_policy["refit_policy"]
    changed = [{**part, "uri": part["uri"].replace("split_role=train", "split_role=final_test")}
               for part in policy["partitions"]]
    scope.comparison_policy = {"refit_policy": {**policy, "partitions": changed}}
    with pytest.raises(InvalidTransition):
        validate_refit_policy(db, scope)


def test_log_loss_selection_uses_lower_validation_loss(planning_case):
    from automl_api.models.enums import TaskType
    from automl_api.training.evaluation import metric_direction

    db, store, expected, scope, run = planning_case
    experiment = db.get(ExperimentSpecRevision, scope.experiment_spec_revision_id)
    experiment.task_type = "classification"
    experiment.primary_metric = "log_loss"
    run.task_type = TaskType.CLASSIFICATION
    entries = run.tags["leaderboard"]
    for entry in entries:
        entry["metrics"] = {"log_loss": entry["metrics"]["rmse"]}
    run.tags = {"leaderboard": entries, "leaderboard_primary_metric": "log_loss"}
    policy = {**scope.comparison_policy["refit_policy"], "task_type": "classification"}
    scope.comparison_policy = {"refit_policy": policy}
    _, digest = validate_refit_policy(db, scope)
    scope.comparison_policy = {**scope.comparison_policy, "refit_policy_digest": digest}
    db.flush()
    assert metric_direction("log_loss") == "minimize"
    attempt = plan_scope_refit(db, scope.id, store)
    event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
    assert RefitPlan.model_validate(event.payload).candidate == expected.candidate


def test_global_selection_includes_every_member_and_waits_for_last_run(planning_case):
    db, store, expected, scope, first_run = planning_case
    second_run = ModelRun(
        project_id=first_run.project_id, dataset_version_id=first_run.dataset_version_id,
        created_by_id=first_run.created_by_id, task_type=first_run.task_type,
        target_column=first_run.target_column, params=first_run.params,
        status=RunStatus.RUNNING,
    )
    db.add(second_run)
    db.flush()
    with store.open_stream(expected.candidate.uri) as source:
        obj = store.put_bytes(
            f"projects/{scope.project_id}/runs/{second_run.id}/better.joblib", source.read(),
        )
    second_run.tags = {"leaderboard_primary_metric": "rmse", "leaderboard": [{
        "model": "better", "status": "succeeded", "metrics": {"rmse": 1.0},
        "model_artifact_uri": obj.uri, "model_artifact_sha256": expected.candidate.sha256,
    }]}
    db.add(PromotionalScopeMember(
        project_id=scope.project_id, scope_id=scope.id, model_run_id=second_run.id,
        ordinal=1, released_at=datetime.now(UTC),
    ))
    training = WorkflowAttempt(
        project_id=scope.project_id, model_run_id=second_run.id, stage="training_run",
        logical_key=f"training:{second_run.id}", workload_identity="fixture",
        generation=1, fencing_token=uuid.uuid4().hex, status=AttemptStatus.RUNNING,
    )
    db.add(training)
    scope.expected_members = 2
    scope.membership_digest = canonical_request_hash([
        {"ordinal": i, "model_run_id": str(run.id)}
        for i, run in enumerate((first_run, second_run))
    ])
    db.flush()
    assert plan_scope_refit(db, scope.id, store) is None
    training.status = AttemptStatus.SUCCEEDED
    second_run.status = RunStatus.SUCCEEDED
    db.flush()
    attempt = plan_scope_refit(db, scope.id, store)
    assert attempt.model_run_id == second_run.id
    event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
    assert RefitPlan.model_validate(event.payload).candidate.uri == obj.uri


def test_planned_retry_executes_and_publishes_original_selection(planning_case):
    from automl_api.training.champion_refit import (
        FrozenPipeline,
        execute_refit,
        publish_frozen_pipeline,
    )

    db, store, expected, scope, _ = planning_case
    first = plan_scope_refit(db, scope.id, store)
    first.status = AttemptStatus.FAILED
    db.flush()
    second = plan_scope_refit(db, scope.id, store)
    event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == second.id))
    plan = RefitPlan.model_validate(event.payload)
    assert plan.candidate == expected.candidate
    second.status = AttemptStatus.RUNNING
    second.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
    db.flush()
    result = FrozenPipeline.model_validate(execute_refit(plan, store))
    checkpoint = publish_frozen_pipeline(db, plan, result, fencing_token=second.fencing_token)
    assert checkpoint.content_digest == result.frozen_pipeline_digest
    assert plan_scope_refit(db, scope.id, store).id == second.id


@pytest.mark.parametrize("invalid_policy", [False, True])
def test_reconciliation_loop_persists_plan_and_does_not_duplicate(planning_case, invalid_policy):
    from contextlib import contextmanager

    from automl_api.workflow_reconciler import plan_champions_once
    from sqlalchemy.orm import Session

    db, store, _, scope, _ = planning_case
    if invalid_policy:
        scope.comparison_policy = {}
        db.flush()

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    assert plan_champions_once(factory, store=store) == int(not invalid_policy)
    assert plan_champions_once(factory, store=store) == 0
    attempts = list(db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.scope_id == scope.id)))
    assert len(attempts) == int(not invalid_policy)
    if invalid_policy:
        db.refresh(scope)
        assert scope.status == ScopeStatus.FAILED
        assert scope.comparison_policy["planning_error"] == "ValidationError"
    else:
        assert attempts[0].status == AttemptStatus.PENDING


@pytest.mark.parametrize("already_executed", [False, True])
def test_seal_registers_policy_before_releasing_new_training(planning_case, already_executed):
    from automl_api.services.workflow_state import seal_promotional_scope

    db, _, expected, scope, run = planning_case
    scope.status = ScopeStatus.OPEN
    scope.cas_version = 0
    scope.scope_started_at = None
    scope.scope_deadline_at = None
    scope.sealed_at = None
    scope.comparison_policy = {"refit_policy": scope.comparison_policy["refit_policy"]}
    install_evaluation_policy(db, scope)
    member = db.scalar(select(PromotionalScopeMember).where(
        PromotionalScopeMember.scope_id == scope.id,
    ))
    member.released_at = None
    training = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run.id))
    if not already_executed:
        training.status = AttemptStatus.PENDING
        run.status = RunStatus.PRECHECK_RUNNING
        run.tags = {**run.tags, "leaderboard": [], "desired_state": "barrier_pending"}
    command = WorkflowCommand(
        project_id=scope.project_id, actor_id=run.created_by_id, operation="training.launch",
        idempotency_key=str(uuid.uuid4()), request_hash="a" * 64,
        resource_type="model_run", resource_id=run.id,
    )
    db.add(command)
    db.flush()
    db.add(CapacityReservation(
        project_id=scope.project_id, command_id=command.id, resource_class="training",
        cpu_millis=1000, memory_bytes=1024, gpu_count=0, status="held",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    ))
    db.flush()
    if already_executed:
        with pytest.raises(InvalidTransition, match="executed before sealing"):
            with db.begin_nested():
                seal_promotional_scope(db, scope.id, expected_cas_version=0)
        assert scope.status == ScopeStatus.OPEN
        assert member.released_at is None
        assert not list(db.scalars(select(OutboxEntry).where(OutboxEntry.command_id == command.id)))
    else:
        seal_promotional_scope(db, scope.id, expected_cas_version=0)
        assert scope.comparison_policy["refit_policy_digest"] == expected.policy_digest
        assert len(scope.comparison_policy["evaluation_policy_digest"]) == 64
        assert scope.status == ScopeStatus.SEALED and run.status == RunStatus.QUEUED
        assert member.released_at is not None
        assert len(list(db.scalars(select(OutboxEntry).where(
            OutboxEntry.command_id == command.id,
        )))) == 1


def install_evaluation_policy(db, scope):
    """Synthetic final metadata; these planning fixtures never read final objects."""
    split = db.get(DatasetSplitRevision, scope.split_revision_id)
    split.specification = {**split.specification,
        "split_counts": {**split.specification["split_counts"], "final_test": 10},
        "uris": {**split.specification["uris"],
                 "final_input": "s3://locked-final/inputs", "final_label": "s3://locked-final/labels"}}
    policy = dict(target_column="target", task_type="regression", primary_metric="rmse",
        positive_label=None, final_rows=10, final_row_digest=split.final_test_digest,
        max_decoded_bytes=1000000, max_input_bytes=1000000, max_model_bytes=1000000,
        final_data=dict(project_reference=str(scope.project_id), split_digest=split.content_digest,
            provider="aws", bucket="locked-final", azure_account=None,
            objects=[dict(role=role, key=f"{role}/part.parquet", version="1", sha256="a"*64,
                          byte_size=100) for role in ("inputs", "labels")]))
    scope.comparison_policy = {**scope.comparison_policy, "evaluation_policy": policy}
    db.flush()
    return policy
