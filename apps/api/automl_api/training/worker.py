from __future__ import annotations

import argparse
import os
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import (
    StaleFence,
    cas_register_terminal_artifact,
    transition_attempt,
)


def _fenced_attempt_context() -> tuple[uuid.UUID, str] | None:
    raw_attempt_id = os.getenv("AUTOML_ATTEMPT_ID", "").strip()
    fencing_token = os.getenv("AUTOML_FENCING_TOKEN", "").strip()
    if not raw_attempt_id and not fencing_token:
        return None
    if not raw_attempt_id or not fencing_token:
        raise ValueError("AUTOML_ATTEMPT_ID and AUTOML_FENCING_TOKEN must be set together.")
    return uuid.UUID(raw_attempt_id), fencing_token


def _begin_fenced_attempt(run_id: uuid.UUID) -> tuple[uuid.UUID, str] | None:
    context = _fenced_attempt_context()
    if context is None:
        return None
    attempt_id, fencing_token = context
    with get_session_factory()() as db:
        attempt = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
        )
        if attempt is None or attempt.model_run_id != run_id:
            raise ValueError("The fenced training attempt does not belong to this run.")
        if attempt.fencing_token != fencing_token:
            raise StaleFence("The Ray worker received a stale run fence.")
        if attempt.status == AttemptStatus.SUBMITTED:
            transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token=fencing_token)
        elif attempt.status != AttemptStatus.RUNNING:
            raise StaleFence(f"The Ray worker cannot start an attempt in {attempt.status} state.")
        attempt.heartbeat_at = datetime.now(UTC)
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Run {run_id} was not found.")
        run.status = RunStatus.RUNNING
        run.started_at = run.started_at or datetime.now(UTC)
        db.commit()
    return context


def _complete_fenced_attempt(run_id: uuid.UUID, context: tuple[uuid.UUID, str]) -> None:
    attempt_id, fencing_token = context
    with get_session_factory()() as db:
        attempt = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
        )
        run = db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update())
        if attempt is None or run is None or attempt.model_run_id != run.id:
            raise ValueError("The fenced training completion lineage is missing.")
        checkpoint_uri = str(run.tags.get("winner_model_artifact_uri") or "")
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


def _fail_fenced_attempt(
    run_id: uuid.UUID,
    context: tuple[uuid.UUID, str],
    exc: Exception,
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
        transition_attempt(
            attempt,
            AttemptStatus.FAILED,
            fencing_token=fencing_token,
            terminal_reason=str(exc)[:2000],
            expected_cas_version=attempt.terminal_cas_version,
        )
        run = db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update())
        if run is None:
            raise ValueError(f"Run {run_id} was not found.")
        run.status = RunStatus.FAILED
        run.failure_code = "ray_attempt_failed"
        run.failure_message = str(exc)[:2000]
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
    # MLflow's async queue can retry a partially committed metric batch and
    # violate the SQL metric primary key. Training favors durable synchronous writes.
    os.environ.setdefault("MLFLOW_ENABLE_ASYNC_LOGGING", "false")
    _enable_rapids_accelerator()
    from automl_api.training.analysis import execute_analysis_run
    from automl_api.training.pipeline import execute_training_run, tabular_automl_pipeline

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_id = uuid.UUID(args.run_id)
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Run {run_id} was not found.")
        run_kind = run.run_kind
    attempt_context = _begin_fenced_attempt(run_id)
    execution_mode = os.getenv("TRAINING_EXECUTION_MODE", "direct").lower()
    print(
        f"Starting {run_kind.value} run {args.run_id} in {execution_mode} mode",
        flush=True,
    )
    try:
        if run_kind in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}:
            execute_analysis_run(run_id)
        elif execution_mode == "zenml":
            tabular_automl_pipeline(run_id=args.run_id)
        else:
            execute_training_run(run_id)
        if attempt_context is not None:
            _complete_fenced_attempt(run_id, attempt_context)
    except Exception as exc:
        if attempt_context is not None:
            _fail_fenced_attempt(run_id, attempt_context, exc)
        raise
    print(f"Completed {run_kind.value} run {args.run_id}", flush=True)


if __name__ == "__main__":
    main()
