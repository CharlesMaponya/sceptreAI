"""Durable trial identity shared by local candidate and distributed trial executors."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from automl_api.core.redaction import redact_text
from automl_api.models.enums import AttemptStatus, RunStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowEvent,
)
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    transition_attempt,
)


def enroll_trial(
    db: Session,
    *,
    run_attempt_id: uuid.UUID,
    run_fencing_token: str,
    candidate_id: uuid.UUID,
    suggestion_id: str,
    parameters: dict,
    execution_id: str,
) -> WorkflowAttempt:
    """Enroll before dispatch; the caller must commit the transaction before executing."""
    if any(not value.strip() or len(value) > 255 for value in (suggestion_id, execution_id)):
        raise ValueError("Trial suggestion and execution IDs must contain 1 to 255 characters.")
    json.dumps(parameters, allow_nan=False)
    digest = canonical_request_hash(parameters)
    parent, run = _locked_parent_run(db, run_attempt_id, run_fencing_token)
    candidate = db.scalar(
        select(TrainingCandidate).where(
            TrainingCandidate.id == candidate_id,
            TrainingCandidate.project_id == parent.project_id,
            TrainingCandidate.model_run_id == run.id,
        )
    )
    if candidate is None:
        raise StaleFence("The trial candidate does not belong to this training run.")
    from automl_api.training.parameter_overrides import validate_trial_overrides

    validate_trial_overrides(run, candidate, parameters)

    trial = db.scalar(
        select(TrainingTrial)
        .where(
            TrainingTrial.candidate_id == candidate.id,
            TrainingTrial.suggestion_id == suggestion_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if trial is None:
        trial = TrainingTrial(
            project_id=parent.project_id,
            candidate_id=candidate.id,
            suggestion_id=suggestion_id,
            parameters=parameters,
            params_digest=digest,
        )
        db.add(trial)
        db.flush()
    elif trial.params_digest != digest or canonical_request_hash(trial.parameters) != digest:
        raise ValueError("Trial suggestion parameters conflict with the committed identity.")
    previous = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == parent.project_id,
            WorkflowAttempt.trial_id == trial.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_TRIAL,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if previous is not None and previous.run_attempt_id == parent.id:
        if previous.ray_submission_id is None:
            predecessor = (
                db.get(
                    WorkflowAttempt,
                    previous.predecessor_attempt_id,
                    populate_existing=True,
                )
                if previous.predecessor_attempt_id is not None
                else None
            )
            if (
                previous.status != AttemptStatus.PENDING
                or previous.retry_count != 0
                or previous.project_id != parent.project_id
                or previous.model_run_id != run.id
                or previous.logical_key != f"trial:{trial.id}"
                or previous.workload_identity != parent.workload_identity
                or previous.lease_owner is not None
                or previous.lease_expires_at is not None
                or previous.terminal_cas_version != 0
                or trial.result_digest is not None
                or previous.generation > min(parent.retry_budget, previous.retry_budget)
                or predecessor is None
                or predecessor.status != AttemptStatus.SUPERSEDED
                or predecessor.stage != WorkflowStage.TRAINING_TRIAL
                or predecessor.project_id != parent.project_id
                or predecessor.model_run_id != run.id
                or predecessor.trial_id != trial.id
                or predecessor.run_attempt_id != parent.predecessor_attempt_id
                or previous.generation != predecessor.generation + 1
            ):
                raise StaleFence("The pending trial replacement has invalid execution lineage.")
            previous.ray_submission_id = execution_id
        if previous.ray_submission_id != execution_id:
            raise StaleFence("This parent already enrolled a different trial execution.")
        return previous
    adopted_payload = None
    reference = None
    if trial.result_digest is not None:
        if previous is None:
            raise StaleFence("Completed trial evidence requires verified predecessor adoption.")
        reference = authorized_trial_result(db, parent, previous)
        adopted_payload = _terminal_evidence(db, trial, previous)
    elif previous is not None and previous.status != AttemptStatus.SUPERSEDED:
        raise StaleFence("The prior trial attempt must be superseded before replacement.")
    generation = 1 if previous is None else previous.generation + 1
    retry_budget = parent.retry_budget if previous is None else previous.retry_budget
    if generation > min(parent.retry_budget, retry_budget):
        raise StaleFence("The durable trial retry budget is exhausted.")
    attempt = WorkflowAttempt(
        project_id=parent.project_id,
        model_run_id=run.id,
        trial_id=trial.id,
        run_attempt_id=parent.id,
        stage=WorkflowStage.TRAINING_TRIAL,
        logical_key=f"trial:{trial.id}",
        workload_identity=parent.workload_identity,
        generation=generation,
        retry_budget=retry_budget,
        fencing_token=uuid.uuid4().hex,
        predecessor_attempt_id=previous.id if previous else None,
        ray_job_name=parent.ray_job_name,
        ray_cluster_name=parent.ray_cluster_name,
        ray_submission_id=execution_id,
        predecessor_checkpoint_allowlist=[reference] if reference is not None else [],
    )
    db.add(attempt)
    db.flush()
    if adopted_payload is not None:
        for status in (
            AttemptStatus.CLAIMED,
            AttemptStatus.SUBMITTED,
            AttemptStatus.RUNNING,
            previous.status,
        ):
            transition_attempt(
                attempt,
                status,
                fencing_token=attempt.fencing_token,
                expected_cas_version=attempt.terminal_cas_version,
                terminal_reason=adopted_payload.get("failure"),
            )
        for sequence, (key, payload) in enumerate(
            (
                ("trial.adopted", reference),
                ("trial.terminal", adopted_payload),
            ),
            start=1,
        ):
            db.add(
                WorkflowEvent(
                    project_id=attempt.project_id,
                    attempt_id=attempt.id,
                    sequence=sequence,
                    event_key=key,
                    event_type=key,
                    payload=payload,
                    result_digest=canonical_request_hash(payload),
                )
            )
        db.flush()
    return attempt


def _locked_parent_run(db, parent_id, parent_token):
    # Match replacement: parent attempt before run, then child records.
    parent = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == parent_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        parent is None
        or parent.stage != WorkflowStage.TRAINING_RUN
        or parent.fencing_token != parent_token
        or parent.status != AttemptStatus.RUNNING
        or (parent.lease_expires_at is not None and parent.lease_expires_at <= datetime.now(UTC))
    ):
        raise StaleFence("Trial execution requires the current running training attempt.")
    run = db.scalar(
        select(ModelRun)
        .where(ModelRun.id == parent.model_run_id, ModelRun.project_id == parent.project_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        run is None
        or run.status != RunStatus.RUNNING
        or (run.tags or {}).get("training_result_receipt")
    ):
        raise StaleFence("This run no longer accepts trial execution.")
    return parent, run


def _locked_execution(db, attempt_id, fencing_token, run_fencing_token):
    identity = db.execute(
        select(WorkflowAttempt.run_attempt_id, WorkflowAttempt.trial_id).where(
            WorkflowAttempt.id == attempt_id
        )
    ).one_or_none()
    if identity is None or identity.run_attempt_id is None or identity.trial_id is None:
        raise StaleFence("The trial execution identity is missing.")
    parent, run = _locked_parent_run(db, identity.run_attempt_id, run_fencing_token)
    trial = db.scalar(
        select(TrainingTrial)
        .join(TrainingCandidate, TrainingCandidate.id == TrainingTrial.candidate_id)
        .where(
            TrainingTrial.id == identity.trial_id,
            TrainingTrial.project_id == parent.project_id,
            TrainingCandidate.project_id == parent.project_id,
            TrainingCandidate.model_run_id == run.id,
        )
        .with_for_update(of=TrainingTrial)
        .execution_options(populate_existing=True)
    )
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        trial is None
        or attempt is None
        or attempt.stage != WorkflowStage.TRAINING_TRIAL
        or attempt.project_id != parent.project_id
        or attempt.model_run_id != run.id
        or attempt.run_attempt_id != parent.id
        or attempt.trial_id != trial.id
        or attempt.fencing_token != fencing_token
        or trial.params_digest != canonical_request_hash(trial.parameters)
        or (
            trial.result_digest is not None
            and attempt.status not in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}
        )
    ):
        raise StaleFence("The trial execution lineage or fence is stale.")
    return trial, attempt


def _terminal_evidence(db, trial, attempt):
    event = db.scalar(
        select(WorkflowEvent)
        .where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == "trial.terminal",
        )
        .execution_options(populate_existing=True)
    )
    if (
        event is None
        or event.event_type != "trial.terminal"
        or event.payload.get("status") != attempt.status.value
        or trial.status != attempt.status.value
        or event.result_digest != canonical_request_hash(event.payload)
        or trial.result_digest != event.result_digest
    ):
        raise StaleFence("The committed trial result evidence is missing or inconsistent.")
    return event.payload


def trial_result_reference(db, parent, attempt):
    """Verify an immutable database CV receipt before explicitly allowing its adoption."""
    trial = db.scalar(
        select(TrainingTrial)
        .join(
            TrainingCandidate,
            TrainingCandidate.id == TrainingTrial.candidate_id,
        )
        .where(
            TrainingTrial.id == attempt.trial_id,
            TrainingTrial.project_id == parent.project_id,
            TrainingCandidate.project_id == parent.project_id,
            TrainingCandidate.model_run_id == parent.model_run_id,
        )
        .execution_options(populate_existing=True)
    )
    if (
        trial is None
        or attempt.stage != WorkflowStage.TRAINING_TRIAL
        or attempt.status not in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}
        or attempt.project_id != parent.project_id
        or attempt.model_run_id != parent.model_run_id
        or attempt.run_attempt_id != parent.id
        or attempt.terminal_cas_version != 1
        or attempt.logical_key != f"trial:{trial.id}"
        or attempt.workload_identity != parent.workload_identity
        or trial.params_digest != canonical_request_hash(trial.parameters)
    ):
        raise StaleFence("The predecessor trial result has invalid lineage.")
    payload = _terminal_evidence(db, trial, attempt)
    json.dumps(payload, allow_nan=False)
    return {
        "kind": "trial_result",
        "attempt_id": str(attempt.id),
        "trial_id": str(trial.id),
        "result_digest": trial.result_digest,
    }


def authorized_trial_result(db, parent, attempt):
    """Require explicit receipt authorization at every skipped parent generation."""
    source_parent = db.get(WorkflowAttempt, attempt.run_attempt_id, populate_existing=True)
    if source_parent is None:
        raise StaleFence("The predecessor trial parent is missing.")
    reference = trial_result_reference(db, source_parent, attempt)
    current = parent
    for _ in range(parent.retry_budget):
        if reference not in current.predecessor_checkpoint_allowlist:
            raise StaleFence("The predecessor trial result is not explicitly authorized.")
        predecessor = (
            db.get(
                WorkflowAttempt,
                current.predecessor_attempt_id,
                populate_existing=True,
            )
            if current.predecessor_attempt_id is not None
            else None
        )
        if (
            predecessor is None
            or predecessor.status != AttemptStatus.SUPERSEDED
            or predecessor.stage != WorkflowStage.TRAINING_RUN
            or predecessor.project_id != parent.project_id
            or predecessor.model_run_id != parent.model_run_id
            or predecessor.workload_identity != parent.workload_identity
            or current.generation != predecessor.generation + 1
        ):
            raise StaleFence("The predecessor trial result has invalid parent lineage.")
        if predecessor.id == source_parent.id:
            return reference
        current = predecessor
    raise StaleFence("The predecessor trial result exceeds the parent recovery budget.")


def _live_owner(attempt, worker_id, now):
    if (
        attempt.status != AttemptStatus.RUNNING
        or attempt.lease_owner != worker_id
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= now
    ):
        raise StaleFence("The trial worker no longer owns a live lease.")


def start_trial(db, *, attempt_id, fencing_token, run_fencing_token, worker_id, lease_limit=None):
    """Return verified terminal evidence on replay, or None after acquiring execution."""
    if not worker_id.strip() or len(worker_id) > 255:
        raise ValueError("A bounded, non-empty trial worker identity is required.")
    trial, attempt = _locked_execution(db, attempt_id, fencing_token, run_fencing_token)
    if lease_limit is not None and lease_limit <= datetime.now(UTC):
        raise StaleFence("The trial admission proof has expired.")
    if attempt.status in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}:
        return _terminal_evidence(db, trial, attempt)
    now = datetime.now(UTC)
    if attempt.status == AttemptStatus.PENDING:
        if attempt.retry_count >= attempt.retry_budget:
            raise StaleFence("The trial claim retry budget is exhausted.")
        attempt.retry_count += 1
        for status in (AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING):
            transition_attempt(attempt, status, fencing_token=fencing_token)
        attempt.lease_owner = worker_id
        trial.status = "running"
    else:
        _live_owner(attempt, worker_id, now)
    attempt.heartbeat_at = now
    attempt.lease_expires_at = _trial_deadline(db, attempt, now, lease_limit)
    return None


def _trial_deadline(db, attempt, now, lease_limit):
    limits = [now + timedelta(seconds=90)]
    parent = db.get(WorkflowAttempt, attempt.run_attempt_id)
    if parent.lease_expires_at is not None:
        limits.append(parent.lease_expires_at)
    if lease_limit is not None:
        limits.append(lease_limit)
    deadline = min(limits)
    if deadline <= now:
        raise StaleFence("The trial admission or parent lease has expired.")
    return deadline


def heartbeat_trial(
    db,
    *,
    attempt_id,
    fencing_token,
    run_fencing_token,
    worker_id,
    lease_limit=None,
):
    _, attempt = _locked_execution(db, attempt_id, fencing_token, run_fencing_token)
    now = datetime.now(UTC)
    _live_owner(attempt, worker_id, now)
    attempt.heartbeat_at = now
    attempt.lease_expires_at = _trial_deadline(db, attempt, now, lease_limit)


def finish_trial(
    db,
    *,
    attempt_id,
    fencing_token,
    run_fencing_token,
    worker_id,
    result=None,
    failure=None,
):
    """Persist immutable result/event/CAS together; the caller owns commit and retries."""
    if (result is None) == (failure is None):
        raise ValueError("Provide exactly one trial result or failure.")
    status = AttemptStatus.SUCCEEDED if failure is None else AttemptStatus.FAILED
    payload = {"status": status.value}
    if failure is None:
        if not isinstance(result, dict):
            raise ValueError("Trial results must be a JSON object.")
        json.dumps(result, allow_nan=False)
        payload["result"] = result
    else:
        payload["failure"] = redact_text(str(failure))[:2000]
        if not payload["failure"].strip():
            raise ValueError("A trial failure requires a terminal reason.")
    digest = canonical_request_hash(payload)
    trial, attempt = _locked_execution(db, attempt_id, fencing_token, run_fencing_token)
    if attempt.status in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}:
        committed = _terminal_evidence(db, trial, attempt)
        if canonical_request_hash(committed) != digest:
            raise StaleFence("A different terminal trial result is already committed.")
        return committed
    _live_owner(attempt, worker_id, datetime.now(UTC))
    transition_attempt(
        attempt,
        status,
        fencing_token=fencing_token,
        expected_cas_version=attempt.terminal_cas_version,
        terminal_reason=payload.get("failure"),
    )
    trial.status = status.value
    trial.result_digest = digest
    attempt.lease_expires_at = None
    sequence = (
        db.scalar(
            select(func.max(WorkflowEvent.sequence)).where(
                WorkflowEvent.attempt_id == attempt.id,
            )
        )
        or 0
    ) + 1
    db.add(
        WorkflowEvent(
            project_id=attempt.project_id,
            attempt_id=attempt.id,
            sequence=sequence,
            event_key="trial.terminal",
            event_type="trial.terminal",
            payload=payload,
            result_digest=digest,
        )
    )
    db.flush()
    return payload
