"""Persist champion selection and bounded refit retries before job submission."""

from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.models.enums import AttemptStatus, RunStatus, ScopeStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    ExperimentSpecRevision,
    PromotionalScope,
    PromotionalScopeMember,
    WorkflowAttempt,
    WorkflowEvent,
)
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash
from automl_api.training.champion_refit import RefitObject, RefitPlan, RefitPolicy, _within
from automl_api.training.evaluation import metric_direction, resolve_primary_metric

ACTIVE_ATTEMPTS = {
    AttemptStatus.PENDING, AttemptStatus.CLAIMED,
    AttemptStatus.SUBMITTED, AttemptStatus.RUNNING,
}
TERMINAL_RUNS = {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}


def validate_refit_member(run, scope, policy, metric):
    if (run.dataset_version_id != policy.dataset_version_id
        or run.task_type != policy.task_type or run.target_column != policy.target_column
        or (run.params or {}).get("split_revision_id") != str(scope.split_revision_id)
        or (run.tags or {}).get("leaderboard_primary_metric") != metric):
        raise InvalidTransition("Scope member does not match the comparison experiment")
    for role, key in (("train", "sample_tier_rows"), ("validation", "validation_sample_rows")):
        sampled_rows = (run.params or {}).get(key)
        if sampled_rows is not None and sampled_rows < sum(
            part.rows for part in policy.partitions if part.role == role
        ):
            raise InvalidTransition("Refit cannot expand a sampled search tier")


def validate_refit_policy(db: Session, scope: PromotionalScope) -> tuple[RefitPolicy, str]:
    """Validate the full train/validation tier before releasing scope members.

    Sampled tier transitions need their own immutable split revision; this path
    must never infer a larger tier after seeing validation results.
    """
    policy = RefitPolicy.model_validate((scope.comparison_policy or {}).get("refit_policy"))
    split = db.get(DatasetSplitRevision, scope.split_revision_id)
    experiment = db.get(ExperimentSpecRevision, scope.experiment_spec_revision_id)
    if (
        split is None or experiment is None or split.sealed_at is None
        or split.project_id != scope.project_id or experiment.project_id != scope.project_id
        or split.dataset_version_id != policy.dataset_version_id
        or experiment.dataset_version_id != policy.dataset_version_id
        or experiment.split_revision_id != split.id
        or experiment.target_column != policy.target_column
        or experiment.task_type != policy.task_type
        or split.digest_scope != "xor-sha256-row-id-v1"
    ):
        raise InvalidTransition("Refit policy must match the sealed split and experiment")
    resolve_primary_metric(experiment.task_type, experiment.primary_metric)
    specification = split.specification or {}
    for role in ("train", "validation"):
        parts = [part for part in policy.partitions if part.role == role]
        digest = 0
        for part in parts:
            digest ^= int(part.row_digest, 16)
        prefix = (specification.get("uris") or {}).get(role)
        if (
            not parts or not prefix
            or any(not _within(part.uri, prefix.rstrip("/") + "/") for part in parts)
            or sum(part.rows for part in parts) != (
                specification.get("split_counts") or {}
            ).get(role)
            or f"{digest:064x}" != getattr(split, f"{role}_digest")
        ):
            raise InvalidTransition("Refit partitions differ from the sealed train/validation tier")
    digest = canonical_request_hash({
        **policy.model_dump(mode="json"),
        "project_id": str(scope.project_id), "scope_id": str(scope.id),
    })
    return policy, digest


def plan_scope_refit(db: Session, scope_id: uuid.UUID, store) -> WorkflowAttempt | None:
    """Caller commits the pending attempt and plan; this never submits a workload.

    Scope-before-refit-attempt locking is shared with publication. Training locks
    use SKIP LOCKED so an observer finishing a member cannot deadlock the planner.
    """
    scope = db.scalar(
        select(PromotionalScope).where(PromotionalScope.id == scope_id)
        .with_for_update(skip_locked=True).execution_options(populate_existing=True)
    )
    if scope is None or scope.mode != "promotional" or scope.status not in {
        ScopeStatus.SEALED, ScopeStatus.RUNNING,
    }:
        return None
    # Rotate waiting barriers without writing a scope locked by another planner.
    scope.updated_at = datetime.now(UTC)
    if scope.scope_deadline_at is None or scope.scope_deadline_at <= datetime.now(UTC):
        scope.status = ScopeStatus.FAILED
        return None
    policy, policy_digest = validate_refit_policy(db, scope)
    if (scope.comparison_policy or {}).get("refit_policy_digest") != policy_digest:
        raise InvalidTransition("Refit policy was not registered before scope release")

    attempts = list(db.scalars(
        select(WorkflowAttempt).where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
        ).order_by(WorkflowAttempt.generation).with_for_update()
        .execution_options(populate_existing=True)
    ))
    if attempts:
        latest = attempts[-1]
        if latest.status in ACTIVE_ATTEMPTS or latest.status == AttemptStatus.SUCCEEDED:
            return latest
        if latest.status != AttemptStatus.FAILED or latest.generation >= 2:
            scope.status = ScopeStatus.FAILED
            return None
        original = db.scalar(select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempts[0].id,
            WorkflowEvent.event_key == "refit_plan",
        ))
        if original is None:
            raise InvalidTransition("Original champion selection is missing")
        plan = RefitPlan.model_validate(original.payload)
        if original.result_digest != plan.digest or plan.policy_digest != policy_digest:
            raise InvalidTransition("Original champion selection changed")
        plan = plan.model_copy(update={"attempt_id": uuid.uuid4()})
        return _record_plan(db, scope, plan, predecessor=latest)

    members = list(db.scalars(
        select(PromotionalScopeMember).where(PromotionalScopeMember.scope_id == scope.id)
        .order_by(PromotionalScopeMember.ordinal)
    ))
    if (
        len(members) != scope.expected_members
        or [member.ordinal for member in members] != list(range(scope.expected_members))
        or any(member.released_at is None for member in members)
        or scope.membership_digest != canonical_request_hash([
            {"ordinal": member.ordinal, "model_run_id": str(member.model_run_id)}
            for member in members
        ])
    ):
        raise InvalidTransition("Champion selection requires the exact sealed membership")
    run_ids = [member.model_run_id for member in members]
    training_query = select(WorkflowAttempt).where(
        WorkflowAttempt.project_id == scope.project_id,
        WorkflowAttempt.model_run_id.in_(run_ids),
        WorkflowAttempt.stage.in_({WorkflowStage.TRAINING_RUN, WorkflowStage.TRAINING_TRIAL}),
    )
    expected_ids = set(db.scalars(training_query.with_only_columns(WorkflowAttempt.id)))
    training = list(db.scalars(training_query.with_for_update(skip_locked=True)
                               .execution_options(populate_existing=True)))
    if {attempt.id for attempt in training} != expected_ids or any(
        attempt.status in ACTIVE_ATTEMPTS for attempt in training
    ):
        return None
    if {attempt.model_run_id for attempt in training
        if attempt.stage == WorkflowStage.TRAINING_RUN} != set(run_ids):
        raise InvalidTransition("Every scope member requires durable terminal training")
    runs = list(db.scalars(select(ModelRun).where(
        ModelRun.project_id == scope.project_id, ModelRun.id.in_(run_ids),
    ).with_for_update(skip_locked=True).execution_options(populate_existing=True)))
    if len(runs) != len(run_ids) or any(run.status not in TERMINAL_RUNS for run in runs):
        return None
    experiment = db.get(ExperimentSpecRevision, scope.experiment_spec_revision_id)
    metric = experiment.primary_metric
    ranked = []
    for run in runs:
        validate_refit_member(run, scope, policy, metric)
        for entry in (run.tags or {}).get("leaderboard", []):
            if entry.get("status") != "succeeded":
                continue
            score = (entry.get("metrics") or {}).get(metric)
            if (isinstance(score, bool) or not isinstance(score, (int, float))
                or not math.isfinite(score)):
                continue
            # Never use primary_score, rank, test metrics or the run's cached winner.
            rank = -score if metric_direction(metric) == "maximize" else score
            ranked.append((rank, str(run.id), str(entry["model"]), run, entry))
    if not ranked:
        scope.status = ScopeStatus.FAILED
        return None
    _, _, _, run, entry = min(ranked, key=lambda item: item[:3])
    uri = entry["model_artifact_uri"]
    if not _within(uri, store.uri_for_key(f"projects/{scope.project_id}/runs/{run.id}/")):
        raise InvalidTransition("Selected candidate artifact belongs to another run")
    candidate = RefitObject(
        uri=uri, sha256=entry["model_artifact_sha256"], byte_size=store.stat(uri).byte_size,
    )
    if candidate.byte_size > policy.max_model_bytes:
        raise InvalidTransition("Selected pipeline exceeds the preregistered model budget")
    plan = RefitPlan(
        **policy.model_dump(), project_id=scope.project_id, scope_id=scope.id,
        attempt_id=uuid.uuid4(), run_id=run.id, candidate=candidate,
    )
    return _record_plan(db, scope, plan)


def _record_plan(db, scope, plan, predecessor=None):
    attempt = WorkflowAttempt(
        id=plan.attempt_id, project_id=scope.project_id, scope_id=scope.id,
        model_run_id=plan.run_id, dataset_version_id=plan.dataset_version_id,
        stage=WorkflowStage.CHAMPION_REFIT, logical_key=f"refit:{scope.id}",
        workload_identity=f"refit:{plan.attempt_id}", fencing_token=uuid.uuid4().hex,
        generation=predecessor.generation + 1 if predecessor else 1,
        predecessor_attempt_id=predecessor.id if predecessor else None,
        retry_budget=1, status=AttemptStatus.PENDING,
    )
    db.add(attempt)
    db.flush()
    db.add(WorkflowEvent(
        project_id=scope.project_id, attempt_id=attempt.id,
        sequence=1, event_key="refit_plan", event_type="refit_plan",
        payload=plan.model_dump(mode="json"), result_digest=plan.digest,
    ))
    scope.status = ScopeStatus.RUNNING
    db.flush()
    return attempt
