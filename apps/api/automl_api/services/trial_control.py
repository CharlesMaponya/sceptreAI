"""Pod-bound trial authority; native identity verification runs outside SQL."""

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial

from sqlalchemy import select

from automl_api.models.enums import RunKind, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingCandidate, TrainingTrial, WorkflowAttempt
from automl_api.schemas.trial_control import (
    TRIAL_CONTROL_MAX_BYTES,
    TrialControlReply,
    TrialControlRequest,
)
from automl_api.services.attempt_identity import _admit, _scope, attempt_audience
from automl_api.services.training_trials import (
    enroll_candidate_evaluation,
    finish_trial,
    heartbeat_trial,
    start_trial,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash


def _request_arguments(action, request):
    present = {
        name
        for name, value in request.model_dump().items()
        if value is not None and name not in {"run_id", "worker_id"}
    }
    if action == "enroll":
        required = {"candidate_name", "suggestion_id", "parameters"}
        if present - {"dispatch_id"} != required:
            raise StaleFence("Trial enrollment has invalid arguments")
    elif action in {"start", "heartbeat"}:
        if present != {"child_attempt_id"} or request.worker_id.int == 0:
            raise StaleFence("Trial execution has invalid arguments")
    elif action == "finish":
        if (
            present not in ({"child_attempt_id", "result"}, {"child_attempt_id", "failure"})
            or request.worker_id.int == 0
        ):
            raise StaleFence("Trial publication has invalid arguments")
    else:
        raise StaleFence("Unknown trial action")


def _trial_scope(db, parent_id, *, request, lock=False, expected_project=None):
    scope = _scope(db, parent_id, lock=lock, expected_project=expected_project)
    parent = db.get(WorkflowAttempt, parent_id, populate_existing=True)
    run = db.get(ModelRun, request.run_id, populate_existing=True)
    if (
        parent.stage != WorkflowStage.TRAINING_RUN
        or parent.model_run_id != request.run_id
        or run is None
        or run.project_id != parent.project_id
        or run.run_kind != RunKind.TRAINING
    ):
        raise StaleFence("Trials require their current training parent")
    candidate_query = select(TrainingCandidate).where(
        TrainingCandidate.project_id == scope.project_id,
        TrainingCandidate.model_run_id == run.id,
    )
    child_binding = None
    if request.child_attempt_id is not None:
        child = db.get(WorkflowAttempt, request.child_attempt_id, populate_existing=True)
        if (
            child is None
            or child.stage != WorkflowStage.TRAINING_TRIAL
            or child.run_attempt_id != parent.id
            or child.model_run_id != run.id
            or child.project_id != parent.project_id
        ):
            raise StaleFence("Trial child belongs to another execution")
        trial = db.get(TrainingTrial, child.trial_id, populate_existing=True)
        if trial is None or trial.project_id != parent.project_id:
            raise StaleFence("Trial identity is missing")
        candidate_query = candidate_query.where(TrainingCandidate.id == trial.candidate_id)
        child_binding = {
            "id": child.id,
            "generation": child.generation,
            "fence": child.fencing_token,
            "trial_id": trial.id,
            "parameters": trial.parameters,
            "digest": trial.params_digest,
        }
    else:
        candidate_query = candidate_query.where(
            TrainingCandidate.estimator_key == request.candidate_name
        )
    if lock:
        candidate_query = candidate_query.with_for_update(read=True)
    candidates = list(db.scalars(candidate_query.execution_options(populate_existing=True)))
    if len(candidates) != 1:
        raise StaleFence("Trial requires exactly one current candidate")
    candidate = candidates[0]
    return replace(
        scope,
        binding_digest=canonical_request_hash(
            {
                "parent": scope.binding_digest,
                "run_kind": run.run_kind,
                "params": run.params,
                "candidate": {
                    key: getattr(candidate, key)
                    for key in (
                        "id",
                        "candidate_key",
                        "estimator_key",
                        "catalog_revision_id",
                        "feature_recipe_revision_id",
                        "params",
                    )
                },
                "child": child_binding,
            }
        ),
    )


def control_trial(factory, parent_id, token, k8s, action, request):
    request = TrialControlRequest.model_validate(request)
    _request_arguments(action, request)
    resolver = partial(_trial_scope, request=request)
    with factory() as db, db.begin():
        captured = resolver(db, parent_id)
    proof = _admit(factory, parent_id, token, k8s, resolver, event_type="trial.control.identity")
    with factory() as db, db.begin():
        current = resolver(db, parent_id, lock=True, expected_project=captured.project_id)
        if (
            current != captured
            or proof["generation"] != current.generation
            or proof["audience"] != attempt_audience(current.id, current.fencing_token)
            or proof["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Trial authority changed during native identity verification")
        terminal = None
        if action == "enroll":
            context = enroll_candidate_evaluation(
                db,
                run_id=request.run_id,
                project_id=current.project_id,
                candidate_name=request.candidate_name,
                suggestion_id=request.suggestion_id,
                parameters=request.parameters,
                parent=(parent_id, current.fencing_token),
                dispatch_id=request.dispatch_id,
            )
            child_id = context["attempt_id"]
        else:
            child_id = request.child_attempt_id
            child = db.get(WorkflowAttempt, child_id, populate_existing=True)
            owner = (
                "trial-"
                + hashlib.sha256(f"{proof['pod_uid']}:{request.worker_id}".encode()).hexdigest()
            )
            context = dict(
                attempt_id=child_id,
                fencing_token=child.fencing_token,
                run_fencing_token=current.fencing_token,
                worker_id=owner,
            )
            if action == "start":
                terminal = start_trial(
                    db, **context, lease_limit=datetime.fromtimestamp(proof["expires_at"], UTC)
                )
            elif action == "heartbeat":
                heartbeat_trial(
                    db, **context, lease_limit=datetime.fromtimestamp(proof["expires_at"], UTC)
                )
            else:
                terminal = finish_trial(
                    db, **context, result=request.result, failure=request.failure
                )
        if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
            raise StaleFence("Trial identity expired before commit")
        reply = TrialControlReply(
            protocol="trial-control-v1",
            parent_attempt_id=parent_id,
            run_id=request.run_id,
            project_id=current.project_id,
            audience=proof["audience"],
            child_attempt_id=child_id,
            terminal=terminal,
        )
        if len(reply.model_dump_json().encode()) > TRIAL_CONTROL_MAX_BYTES:
            raise StaleFence("Trial reply exceeds its byte budget")
        return reply.model_dump(mode="json")
