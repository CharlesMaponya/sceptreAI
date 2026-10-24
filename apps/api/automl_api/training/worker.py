from __future__ import annotations

import argparse
import os
import threading
import uuid

from automl_api.db.session import get_session_factory
from automl_api.models.enums import RunKind
from automl_api.models.runs import ModelRun
from automl_api.services.training_parent import (
    _parent_deadline as _parent_deadline,
)
from automl_api.services.training_parent import (
    _require_parent_lease as _require_parent_lease,
)
from automl_api.services.training_parent import (
    complete_parent,
    fail_parent,
    has_parent_result,
    heartbeat_parent,
    start_parent,
)
from automl_api.services.workflow_state import StaleFence
from automl_api.training.attempt_admission import (
    ANALYSIS_PROTOCOL,
    TRAINING_PROTOCOL,
    admit_parent,
)
from automl_api.training.parent_control import parent_control

_PARENT_HEARTBEAT_INTERVAL = 15


def _fenced_attempt_context() -> tuple[uuid.UUID, str] | None:
    raw_attempt_id = os.getenv("AUTOML_ATTEMPT_ID", "").strip()
    fencing_token = os.getenv("AUTOML_FENCING_TOKEN", "").strip()
    if not raw_attempt_id and not fencing_token:
        return None
    if not raw_attempt_id or not fencing_token:
        raise ValueError("AUTOML_ATTEMPT_ID and AUTOML_FENCING_TOKEN must be set together.")
    return uuid.UUID(raw_attempt_id), fencing_token


def _begin_fenced_attempt(run_id: uuid.UUID, *, worker_id: str | None = None, expected_kind=None):
    context = _fenced_attempt_context()
    worker_id = uuid.uuid4().hex if worker_id is None else worker_id
    if parent_control("start", run_id, context, worker_id, expected_kind=expected_kind) is not None:
        return context
    proof = admit_parent(context)
    if context is None:
        return None
    with get_session_factory()() as db:
        result = start_parent(db, run_id, context, worker_id, proof)
        db.commit()
        return result


def _heartbeat_fenced_attempt(run_id, context, worker_id):
    if parent_control("heartbeat", run_id, context, worker_id) is not None:
        return
    proof = admit_parent(context)
    with get_session_factory()() as db:
        heartbeat_parent(db, run_id, context, worker_id, proof)
        db.commit()


def _complete_fenced_attempt(run_id, context, *, worker_id=None):
    reply = parent_control("complete", run_id, context, worker_id)
    if reply is not None:
        if not reply.has_result or not reply.completed:
            raise StaleFence("Parent control did not confirm a committed result.")
        return
    with get_session_factory()() as db:
        complete_parent(db, run_id, context, worker_id)
        db.commit()


def _resume_committed_result(run_id, context):
    reply = parent_control("receipt", run_id, context)
    if reply is not None:
        if reply.has_result and not reply.completed:
            raise StaleFence("Parent control did not finish the stored result.")
        return reply.completed
    with get_session_factory()() as db:
        if not has_parent_result(db, run_id, context):
            return False
    _complete_fenced_attempt(run_id, context)
    return True


def _fail_fenced_attempt(run_id, context, exc, *, worker_id=None):
    reply = parent_control("failure", run_id, context, worker_id, failure=exc)
    if reply is not None:
        return reply.changed
    with get_session_factory()() as db:
        changed = fail_parent(db, run_id, context, exc, worker_id)
        if changed:
            db.commit()
        return changed


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
    metadata = parent_control("context", run_id, _fenced_attempt_context())
    if metadata is None:
        with get_session_factory()() as db:
            run = db.get(ModelRun, run_id)
            if run is None:
                raise ValueError(f"Run {run_id} was not found.")
            run_kind = run.run_kind
    else:
        run_kind = RunKind(metadata.run_kind)
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
    attempt_context = _begin_fenced_attempt(
        run_id,
        worker_id=worker_id,
        **({"expected_kind": run_kind.value} if metadata is not None else {}),
    )
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
