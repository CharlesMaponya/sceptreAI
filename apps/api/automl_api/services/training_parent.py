"""Shared SQL parent lifecycle mutations for legacy workers and authenticated control."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import (
    StaleFence,
    attempt_credentials_revoked,
    cas_register_terminal_artifact,
    transition_attempt,
)
from automl_api.training.attempt_admission import proof_deadline

_PARENT_LEASE_SECONDS = 90


def _parent_deadline(attempt, proof):
    deadline = attempt.heartbeat_at + timedelta(seconds=_PARENT_LEASE_SECONDS)
    if proof is None:
        return deadline
    if (
        proof["attempt_id"] != str(attempt.id)
        or proof.get("project_id") != str(attempt.project_id)
        or proof.get("generation") != attempt.generation
    ):
        raise StaleFence("Admission proof differs from the current parent lineage.")
    return min(deadline, proof_deadline(proof))


def _require_parent_lease(db, attempt, worker_id=None):
    expires = getattr(attempt, "lease_expires_at", None)
    if expires is not None and expires <= datetime.now(UTC):
        raise StaleFence("The training parent lease has expired.")
    owner = getattr(attempt, "lease_owner", None)
    if worker_id is not None and owner is not None and owner != worker_id:
        raise StaleFence("Another worker owns the training parent lease.")
    if attempt_credentials_revoked(db, attempt.id):
        raise StaleFence("The training parent execution credentials have been revoked.")


def start_parent(db, run_id, context, worker_id, proof):
    if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 255:
        raise ValueError("A bounded, non-empty training worker identity is required.")
    attempt_id, fencing_token = context
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None or attempt.model_run_id != run_id:
        raise ValueError("The fenced training attempt does not belong to this run.")
    if attempt.fencing_token != fencing_token:
        raise StaleFence("The Ray worker received a stale run fence.")
    if attempt.status == AttemptStatus.SUBMITTED:
        transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token=fencing_token)
    elif attempt.status != AttemptStatus.RUNNING:
        raise StaleFence(f"The Ray worker cannot start an attempt in {attempt.status} state.")
    run = db.get(ModelRun, run_id, with_for_update=True, populate_existing=True)
    if run is None:
        raise ValueError(f"Run {run_id} was not found.")
    receipt = (getattr(run, "tags", None) or {}).get("training_result_receipt") or (
        getattr(run, "tags", None) or {}
    ).get("analysis_result_receipt")
    if receipt and receipt.get("attempt_id") == str(attempt_id):
        return context
    if run.status not in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING}:
        raise StaleFence("The training run no longer admits this worker.")
    _require_parent_lease(db, attempt, worker_id)
    attempt.lease_owner = worker_id
    attempt.heartbeat_at = datetime.now(UTC)
    attempt.lease_expires_at = _parent_deadline(attempt, proof)
    run.status = RunStatus.RUNNING
    run.started_at = run.started_at or datetime.now(UTC)
    run.failure_code = None
    run.failure_message = None
    run.plain_english_failure = None
    run.finished_at = None
    return context


def heartbeat_parent(db, run_id, context, worker_id, proof):
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == context[0])
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt is None
        or attempt.model_run_id != run_id
        or attempt.fencing_token != context[1]
        or (attempt.status != AttemptStatus.RUNNING)
        or (attempt.lease_owner != worker_id)
        or (attempt.lease_expires_at is None)
    ):
        raise StaleFence("The training parent heartbeat no longer owns this attempt.")
    _require_parent_lease(db, attempt, worker_id)
    run = db.scalar(
        select(ModelRun)
        .where(ModelRun.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None or run.status != RunStatus.RUNNING:
        raise StaleFence("The training parent run no longer accepts heartbeats.")
    attempt.heartbeat_at = datetime.now(UTC)
    attempt.lease_expires_at = _parent_deadline(attempt, proof)


def complete_parent(db, run_id, context, worker_id=None):
    attempt_id, fencing_token = context
    attempt = db.scalar(
        select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
    )
    run = db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update())
    if attempt is None or run is None or attempt.model_run_id != run.id:
        raise ValueError("The fenced training completion lineage is missing.")
    receipt = (run.tags or {}).get("training_result_receipt") or (
        getattr(run, "tags", None) or {}
    ).get("analysis_result_receipt")
    if not receipt or receipt.get("attempt_id") != str(attempt_id):
        _require_parent_lease(db, attempt, worker_id)
    from automl_api.services.training_result import require_training_result_receipt

    require_training_result_receipt(db, attempt, run)
    analysis_receipt = (run.tags or {}).get("analysis_result_receipt")
    checkpoint_uri = str(
        run.tags.get("artifact_uri")
        if analysis_receipt
        else run.tags.get("winner_model_artifact_uri") or ""
    )
    if analysis_receipt and attempt.status == AttemptStatus.SUCCEEDED:
        if (
            attempt.fencing_token != fencing_token
            or analysis_receipt.get("attempt_id") != str(attempt.id)
            or analysis_receipt.get("artifact_uri") != checkpoint_uri
            or (attempt.checkpoint_uri != checkpoint_uri)
            or (attempt.terminal_cas_version != 1)
            or (run.status != RunStatus.SUCCEEDED)
        ):
            raise StaleFence("Committed analysis result differs from its terminal attempt.")
        return
    if not checkpoint_uri:
        raise ValueError("The winning model artifact was not published before terminal CAS.")
    if not cas_register_terminal_artifact(
        db,
        attempt_id=attempt.id,
        fencing_token=fencing_token,
        expected_cas_version=attempt.terminal_cas_version,
        checkpoint_uri=checkpoint_uri,
    ):
        raise StaleFence("The fenced terminal artifact CAS was rejected.")
    run.status = RunStatus.SUCCEEDED
    run.finished_at = datetime.now(UTC)


def fail_parent(db, run_id, context, exc, worker_id=None):
    attempt_id, fencing_token = context
    attempt = db.scalar(
        select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
    )
    if attempt is None or attempt.model_run_id != run_id:
        raise ValueError("The fenced failed attempt lineage is missing.")
    if attempt.fencing_token != fencing_token:
        raise StaleFence("The failed Ray worker received a stale run fence.")
    if attempt.status not in {
        AttemptStatus.CLAIMED,
        AttemptStatus.SUBMITTED,
        AttemptStatus.RUNNING,
    }:
        return False
    run = db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update())
    if run is None:
        raise ValueError(f"Run {run_id} was not found.")
    if run.status in {RunStatus.CANCELLED, RunStatus.SUCCEEDED}:
        return False
    receipt = (getattr(run, "tags", None) or {}).get("training_result_receipt") or (
        getattr(run, "tags", None) or {}
    ).get("analysis_result_receipt")
    if receipt and receipt.get("attempt_id") == str(attempt_id):
        return False
    _require_parent_lease(db, attempt, worker_id)
    transition_attempt(
        attempt,
        AttemptStatus.FAILED,
        fencing_token=fencing_token,
        terminal_reason=redact_text(str(exc))[:2000],
        expected_cas_version=attempt.terminal_cas_version,
    )
    run.status = RunStatus.FAILED
    run.failure_code = "ray_attempt_failed"
    run.failure_message = redact_text(str(exc))[:2000]
    run.finished_at = datetime.now(UTC)
    return True


def has_parent_result(db, run_id, context):
    run = db.get(ModelRun, run_id)
    receipt = (getattr(run, "tags", None) or {}).get("training_result_receipt") or (
        getattr(run, "tags", None) or {}
    ).get("analysis_result_receipt")
    return bool(receipt and receipt.get("attempt_id") == str(context[0]))
