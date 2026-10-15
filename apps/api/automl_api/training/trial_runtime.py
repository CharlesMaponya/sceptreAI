"""Execute trial computation outside transactions, with durable identity and result replay."""

from __future__ import annotations

import json
import os
import threading
import uuid

from sqlalchemy import func, select

from automl_api.core.config import get_settings
from automl_api.db.session import get_session_factory
from automl_api.models.workflows import TrainingCandidate, WorkflowEvent
from automl_api.services.training_trials import (
    enroll_trial,
    finish_trial,
    heartbeat_trial,
    start_trial,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import admit_parent, proof_deadline
from automl_api.training.worker import _fenced_attempt_context

_HEARTBEAT_INTERVAL = 15


class TrialExecutionUnresolved(StaleFence):
    """An uncertain database outcome must stop model fallback and replacement computation."""


class TrialEvaluationFailed(RuntimeError):
    """A replayed trial has already recorded an ordinary evaluation failure."""


def requires_durable_trials():
    return bool(
        os.getenv("AUTOML_ATTEMPT_ID")
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
    run, candidate_name, suggestion_id, parameters, parent, *, dispatch_id=None, proof=None,
):
    proof = admit_parent(parent, project_id=run.project_id) if proof is None else proof
    if proof is not None and proof.get("project_id") != str(run.project_id):
        raise StaleFence("Trial admission belongs to another project.")
    factory = get_session_factory()
    try:
        with factory() as db:
            candidates = list(
                db.scalars(
                    select(TrainingCandidate.id)
                    .where(
                        TrainingCandidate.project_id == run.project_id,
                        TrainingCandidate.model_run_id == run.id,
                        TrainingCandidate.estimator_key == candidate_name,
                    )
                    .limit(2)
                )
            )
            if len(candidates) != 1:
                raise StaleFence("A trial requires exactly one durable candidate identity.")
            attempt = enroll_trial(
                db,
                run_attempt_id=parent[0],
                run_fencing_token=parent[1],
                candidate_id=candidates[0],
                suggestion_id=suggestion_id,
                parameters=parameters,
                execution_id=canonical_request_hash(
                    {
                        "parent": str(parent[0]),
                        "candidate": str(candidates[0]),
                        "suggestion": suggestion_id,
                    }
                ),
            )
            context = dict(
                attempt_id=attempt.id,
                fencing_token=attempt.fencing_token,
                run_fencing_token=parent[1],
            )
            if dispatch_id is not None:
                if not dispatch_id.strip() or len(dispatch_id) > 200:
                    raise ValueError("Tune dispatch identity must contain 1 to 200 characters.")
                payload = {"ray_trial_id": dispatch_id, "suggestion_id": suggestion_id}
                digest = canonical_request_hash(payload)
                key = f"trial.dispatch:{dispatch_id}"
                event = db.scalar(
                    select(WorkflowEvent)
                    .where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == key,
                    )
                    .execution_options(populate_existing=True)
                )
                if event is not None:
                    if (
                        event.result_digest != digest
                        or canonical_request_hash(event.payload) != digest
                    ):
                        raise StaleFence("Tune dispatch evidence conflicts with its identity.")
                else:
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
                            event_key=key,
                            event_type="trial.dispatch",
                            payload=payload,
                            result_digest=digest,
                        )
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
    proof = admit_parent(parent, project_id=run.project_id)
    if proof is not None and proof.get("project_id") != str(run.project_id):
        raise StaleFence("Trial admission belongs to another project.")
    context = enroll_evaluation(run, candidate_name, suggestion_id, parameters, parent, proof=proof)
    context["worker_id"] = uuid.uuid4().hex
    factory = get_session_factory()
    try:
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
