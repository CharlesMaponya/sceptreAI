"""Execute trial computation outside transactions, with durable identity and result replay."""

from __future__ import annotations

import json
import os
import threading
import uuid

from automl_api.core.config import get_settings
from automl_api.db.session import get_session_factory
from automl_api.services.training_trials import (
    enroll_candidate_evaluation,
    finish_trial,
    heartbeat_trial,
    start_trial,
)
from automl_api.services.workflow_state import StaleFence
from automl_api.training.attempt_admission import admit_parent, proof_deadline
from automl_api.training.trial_control import trial_control
from automl_api.training.worker import _fenced_attempt_context

_HEARTBEAT_INTERVAL = 15


class TrialExecutionUnresolved(StaleFence):
    """An uncertain database outcome must stop model fallback and replacement computation."""


class TrialEvaluationFailed(RuntimeError):
    """A replayed trial has already recorded an ordinary evaluation failure."""


def requires_durable_trials():
    return bool(
        os.getenv("AUTOML_TRIAL_CONTROL_PROTOCOL") is not None
        or os.getenv("AUTOML_ATTEMPT_ID")
        or os.getenv("AUTOML_FENCING_TOKEN")
        or get_settings().environment == "production"
        or os.getenv("AUTOML_MODEL_PODS") == "1"
        or os.getenv("TRAINING_EXECUTION_MODE") in {"ray", "candidate"}
    )


def trial_parent_context():
    try:
        parent = _fenced_attempt_context()
    except (ValueError, TypeError) as exc:
        raise StaleFence("Trial execution requires a complete parent fence.") from exc
    if parent is None and requires_durable_trials():
        raise StaleFence("Production trial execution requires a durable parent attempt.")
    return parent


def enroll_evaluation(
    run,
    candidate_name,
    suggestion_id,
    parameters,
    parent,
    *,
    dispatch_id=None,
    proof=None,
):
    reply = trial_control(
        "enroll",
        run,
        parent,
        candidate_name=candidate_name,
        suggestion_id=suggestion_id,
        parameters=parameters,
        dispatch_id=dispatch_id,
    )
    if reply is not None:
        return {"attempt_id": reply.child_attempt_id, "remote": True}
    proof = admit_parent(parent, project_id=run.project_id) if proof is None else proof
    if proof is not None and proof.get("project_id") != str(run.project_id):
        raise StaleFence("Trial admission belongs to another project.")
    factory = get_session_factory()
    try:
        with factory() as db:
            context = enroll_candidate_evaluation(
                db,
                run_id=run.id,
                project_id=run.project_id,
                candidate_name=candidate_name,
                suggestion_id=suggestion_id,
                parameters=parameters,
                parent=parent,
                dispatch_id=dispatch_id,
            )
            proof_deadline(proof)
            db.commit()
            return context
    except StaleFence:
        raise
    except Exception as exc:
        raise TrialExecutionUnresolved("Durable trial enrollment is unresolved.") from exc


def evaluate_trial(
    run,
    candidate_name,
    suggestion_id,
    parameters,
    evaluate,
    *,
    parent_context=None,
):
    parent = parent_context if parent_context is not None else trial_parent_context()
    if parent is None:
        return evaluate()
    proof = (
        None
        if os.getenv("AUTOML_TRIAL_CONTROL_PROTOCOL") is not None
        else admit_parent(parent, project_id=run.project_id)
    )
    if proof is not None and proof.get("project_id") != str(run.project_id):
        raise StaleFence("Trial admission belongs to another project.")
    context = enroll_evaluation(run, candidate_name, suggestion_id, parameters, parent, proof=proof)
    context["worker_id"] = uuid.uuid4().hex
    remote = context.pop("remote", False)
    factory = None if remote else get_session_factory()

    def remote_operation(action, **payload):
        reply = trial_control(
            action,
            run,
            parent,
            child_attempt_id=context["attempt_id"],
            worker_id=context["worker_id"],
            **payload,
        )
        if reply is None:
            raise StaleFence("Trial control declaration was withdrawn during execution")
        return reply.terminal

    try:
        if remote:
            cached = remote_operation("start")
        else:
            with factory() as db:
                cached = start_trial(db, **context, lease_limit=proof_deadline(proof))
                db.commit()
    except StaleFence:
        raise
    except Exception as exc:
        raise TrialExecutionUnresolved("Durable trial start is unresolved.") from exc
    if cached is not None:
        if cached["status"] == "failed":
            raise TrialEvaluationFailed(cached["failure"])
        return cached["result"]

    def publish(**payload):
        try:
            if remote:
                remote_operation("finish", **payload)
            else:
                with factory() as db:
                    finish_trial(db, **context, **payload)
                    db.commit()
        except StaleFence:
            raise
        except Exception as exc:
            raise TrialExecutionUnresolved("Durable trial publication is unresolved.") from exc

    stop = threading.Event()

    def heartbeat():
        while not stop.wait(_HEARTBEAT_INTERVAL):
            try:
                if remote:
                    remote_operation("heartbeat")
                else:
                    renewed = admit_parent(parent, project_id=run.project_id)
                    with factory() as db:
                        heartbeat_trial(db, **context, lease_limit=proof_deadline(renewed))
                        db.commit()
            except Exception:
                # Publication independently checks the current parent and live child lease.
                return

    thread = threading.Thread(target=heartbeat, name="trial-heartbeat", daemon=True)
    thread.start()
    try:
        try:
            result = evaluate()
            if not isinstance(result, dict):
                raise ValueError("Trial results must be a JSON object.")
            json.dumps(result, allow_nan=False)
        except StaleFence:
            raise
        except Exception as exc:
            publish(failure=f"{type(exc).__name__}: {exc}")
            raise
        publish(result=result)
        return result
    finally:
        stop.set()
        thread.join(timeout=1)
