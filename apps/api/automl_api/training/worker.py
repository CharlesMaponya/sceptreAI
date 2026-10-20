from __future__ import annotations

import argparse
import os
import threading
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import (
    StaleFence,
    cas_register_terminal_artifact,
    transition_attempt,
)
from automl_api.training.attempt_admission import (
    ANALYSIS_PROTOCOL,
    TRAINING_PROTOCOL,
    admit_parent,
    proof_deadline,
)

_PARENT_HEARTBEAT_INTERVAL = 15
_PARENT_LEASE_SECONDS = 90


def _fenced_attempt_context() -> tuple[uuid.UUID, str] | None:
    raw_attempt_id = os.getenv("AUTOML_ATTEMPT_ID", "").strip()
    fencing_token = os.getenv("AUTOML_FENCING_TOKEN", "").strip()
    if not raw_attempt_id and not fencing_token:
        return None
    if not raw_attempt_id or not fencing_token:
        raise ValueError("AUTOML_ATTEMPT_ID and AUTOML_FENCING_TOKEN must be set together.")
    return uuid.UUID(raw_attempt_id), fencing_token


def _begin_fenced_attempt(
    run_id: uuid.UUID,
    *,
    worker_id: str | None = None,
) -> tuple[uuid.UUID, str] | None:
    context = _fenced_attempt_context()
    proof = admit_parent(context)
    if context is None:
        return None
    worker_id = uuid.uuid4().hex if worker_id is None else worker_id
    if not isinstance(worker_id, str) or not worker_id.strip() or len(worker_id) > 255:
        raise ValueError("A bounded, non-empty training worker identity is required.")
    attempt_id, fencing_token = context
    with get_session_factory()() as db:
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
        _require_parent_lease(attempt, worker_id)
        attempt.lease_owner = worker_id
        attempt.heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = _parent_deadline(attempt, proof)
        run.status = RunStatus.RUNNING
        run.started_at = run.started_at or datetime.now(UTC)
        run.failure_code = None
        run.failure_message = None
        run.plain_english_failure = None
        run.finished_at = None
        db.commit()
    return context


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


def _require_parent_lease(attempt, worker_id=None):
    expires = getattr(attempt, "lease_expires_at", None)
    if expires is not None and expires <= datetime.now(UTC):
        raise StaleFence("The training parent lease has expired.")
    owner = getattr(attempt, "lease_owner", None)
    if worker_id is not None and owner is not None and owner != worker_id:
        raise StaleFence("Another worker owns the training parent lease.")


def _heartbeat_fenced_attempt(run_id, context, worker_id):
    proof = admit_parent(context)
    with get_session_factory()() as db:
        attempt = db.scalar(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.id == context[0],
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            attempt is None
            or attempt.model_run_id != run_id
            or attempt.fencing_token != context[1]
            or attempt.status != AttemptStatus.RUNNING
            or attempt.lease_owner != worker_id
            or attempt.lease_expires_at is None
        ):
            raise StaleFence("The training parent heartbeat no longer owns this attempt.")
        _require_parent_lease(attempt, worker_id)
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
        db.commit()


def _complete_fenced_attempt(
    run_id: uuid.UUID,
    context: tuple[uuid.UUID, str],
    *,
    worker_id: str | None = None,
) -> None:
    attempt_id, fencing_token = context
    with get_session_factory()() as db:
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
            _require_parent_lease(attempt, worker_id)
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
                or attempt.checkpoint_uri != checkpoint_uri
                or attempt.terminal_cas_version != 1
                or run.status != RunStatus.SUCCEEDED
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
        db.commit()


def _resume_committed_result(run_id: uuid.UUID, context: tuple[uuid.UUID, str]) -> bool:
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        receipt = (getattr(run, "tags", None) or {}).get("training_result_receipt") or (
            getattr(run, "tags", None) or {}
        ).get("analysis_result_receipt")
        if not receipt or receipt.get("attempt_id") != str(context[0]):
            return False
    _complete_fenced_attempt(run_id, context)
    return True


def _fail_fenced_attempt(
    run_id: uuid.UUID,
    context: tuple[uuid.UUID, str],
    exc: Exception,
    *,
    worker_id: str | None = None,
) -> bool:
    attempt_id, fencing_token = context
    with get_session_factory()() as db:
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
        _require_parent_lease(attempt, worker_id)
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
        db.commit()
        return True


def _enable_rapids_accelerator() -> bool:
    if os.getenv("AUTOML_GPU_VENDOR", "").strip().lower() != "nvidia":
        os.environ["AUTOML_RAPIDS_ACTIVE"] = "0"
        return False
    try:
        from cuml import accel

        accel.install()
    except Exception as exc:
        os.environ["AUTOML_RAPIDS_ACTIVE"] = "0"
        print(
            f"RAPIDS cuML accelerator unavailable; continuing with CPU fallback: {exc}",
            flush=True,
        )
        return False
    os.environ["AUTOML_RAPIDS_ACTIVE"] = "1"
    print("RAPIDS cuML accelerator enabled for supported sklearn estimators", flush=True)
    return True


def main() -> None:
    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"
    # MLflow's async queue can retry a partially committed metric batch and
    # violate the SQL metric primary key. Training favors durable synchronous writes.
    os.environ.setdefault("MLFLOW_ENABLE_ASYNC_LOGGING", "false")
    _enable_rapids_accelerator()
    from automl_api.training.analysis import execute_analysis_run
    from automl_api.training.pipeline import execute_training_run, tabular_automl_pipeline

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt-protocol", choices=[ANALYSIS_PROTOCOL, TRAINING_PROTOCOL])
    args = parser.parse_args()
    run_id = uuid.UUID(args.run_id)
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Run {run_id} was not found.")
        run_kind = run.run_kind
    context = _fenced_attempt_context()
    if args.attempt_protocol and context is None:
        raise StaleFence("The declared worker protocol requires a durable attempt context.")
    if context is not None:
        expected_protocol = {
            RunKind.TRAINING: TRAINING_PROTOCOL,
            RunKind.VALIDATION: ANALYSIS_PROTOCOL,
            RunKind.EXPLAINABILITY: ANALYSIS_PROTOCOL,
            RunKind.DRIFT: ANALYSIS_PROTOCOL,
        }.get(run_kind)
        if expected_protocol is None or args.attempt_protocol != expected_protocol:
            raise StaleFence("Fenced execution requires the supported worker protocol.")
    worker_id = uuid.uuid4().hex
    attempt_context = _begin_fenced_attempt(run_id, worker_id=worker_id)
    if attempt_context is not None and _resume_committed_result(run_id, attempt_context):
        print(f"Recovered committed result for run {args.run_id}", flush=True)
        return
    execution_mode = os.getenv("TRAINING_EXECUTION_MODE", "direct").lower()
    print(
        f"Starting {run_kind.value} run {args.run_id} in {execution_mode} mode",
        flush=True,
    )
    stop = threading.Event()
    heartbeat_errors = []

    def heartbeat():
        while not stop.wait(_PARENT_HEARTBEAT_INTERVAL):
            try:
                _heartbeat_fenced_attempt(run_id, attempt_context, worker_id)
            except Exception as exc:
                heartbeat_errors.append(exc)
                return

    thread = None
    if attempt_context is not None:
        thread = threading.Thread(target=heartbeat, name="training-parent-heartbeat", daemon=True)
        thread.start()
    try:
        if run_kind in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}:
            execute_analysis_run(run_id)
        elif execution_mode == "zenml":
            tabular_automl_pipeline(run_id=args.run_id)
        else:
            execute_training_run(run_id)
        if heartbeat_errors and not (
            attempt_context is not None and _resume_committed_result(run_id, attempt_context)
        ):
            raise StaleFence(
                "Training parent heartbeat could not verify ownership."
            ) from heartbeat_errors[0]
        if attempt_context is not None:
            _complete_fenced_attempt(run_id, attempt_context, worker_id=worker_id)
    except StaleFence:
        raise
    except Exception as exc:
        if attempt_context is not None:
            _fail_fenced_attempt(run_id, attempt_context, exc, worker_id=worker_id)
        raise
    finally:
        stop.set()
        if thread is not None:
            thread.join(timeout=1)
    print(f"Completed {run_kind.value} run {args.run_id}", flush=True)


if __name__ == "__main__":
    main()
