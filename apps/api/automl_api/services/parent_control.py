"""Pod-bound parent lifecycle control; no native I/O runs in a SQL transaction."""

import hashlib
import re
import uuid
from datetime import UTC, datetime
from functools import partial

from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, ProjectStatus, RunKind, RunStatus, WorkflowStage
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import DeletionTombstone, WorkflowAttempt
from automl_api.schemas.parent_control import ParentControlReply, ParentControlRequest
from automl_api.services.attempt_identity import AdmissionScope, _admit, _scope, attempt_audience
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.training_parent import (
    complete_parent,
    fail_parent,
    heartbeat_parent,
    start_parent,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash

PUBLISHER = "parent.control.publisher"
FAILURE = "parent.control.failed"
KINDS = {RunKind.TRAINING, RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}


def _rows(db, attempt_id, run_id, *, lock=False, expected_project=None):
    attempt = db.get(WorkflowAttempt, attempt_id, populate_existing=True)
    if attempt is None:
        raise StaleFence("Parent execution is missing")
    project = db.get(
        Project,
        attempt.project_id,
        with_for_update={"read": True} if lock else None,
        populate_existing=True,
    )
    attempt = db.get(
        WorkflowAttempt, attempt_id, with_for_update=True if lock else None, populate_existing=True
    )
    run = db.get(ModelRun, run_id, with_for_update=True if lock else None, populate_existing=True)
    if (
        attempt is None
        or run is None
        or project is None
        or project.status != ProjectStatus.ACTIVE
        or attempt.project_id != project.id
        or run.project_id != project.id
        or (expected_project is not None and project.id != expected_project)
        or attempt.model_run_id != run.id
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or run.run_kind not in KINDS
        or not run.k8s_namespace
        or not isinstance(run.tags, dict)
    ):
        raise StaleFence("Parent execution scope changed")
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == project.id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & (DeletionTombstone.resource_id == run.id)
            ),
        )
        .limit(1)
    )
    if deleting is not None:
        raise StaleFence("Parent execution is deleting")
    return attempt, run


def _location(attempt, run):
    return canonical_request_hash(
        {
            "project_id": run.project_id,
            "run_id": run.id,
            "dataset_version_id": run.dataset_version_id,
            "run_kind": run.run_kind,
            "target": run.target_column,
            "task_type": run.task_type,
            "source_ids": {
                key: (run.params or {}).get(key)
                for key in ("split_revision_id", "prepared_artifact_id", "source_training_run_id")
            },
            "namespace": run.k8s_namespace,
            "ray_job_name": attempt.ray_job_name,
            "generation": attempt.generation,
            "workload_identity": attempt.workload_identity,
            "fence_sha256": hashlib.sha256(attempt.fencing_token.encode()).hexdigest(),
        }
    )


def _publisher(db, attempt, run):
    event = _event(db, attempt.id, PUBLISHER)
    if event is None:
        return None
    if (
        event.event_type != PUBLISHER
        or not isinstance(event.payload, dict)
        or set(event.payload) != {"owner", "pod_uid", "location_digest", "source_binding"}
        or event.result_digest != canonical_request_hash(event.payload)
        or event.payload["location_digest"] != _location(attempt, run)
        or not all(isinstance(value, str) and value for value in event.payload.values())
    ):
        raise StaleFence("Parent publisher identity changed")
    return event


def _result(attempt, run):
    training = run.run_kind == RunKind.TRAINING
    key = "training_result_receipt" if training else "analysis_result_receipt"
    other = "analysis_result_receipt" if training else "training_result_receipt"
    if other in run.tags:
        raise StaleFence("Parent has an ambiguous result receipt")
    if key not in run.tags:
        return None
    receipt = run.tags[key]
    uri = run.tags.get("winner_model_artifact_uri" if training else "artifact_uri")
    fields = {"attempt_id", "digest"} if training else {"attempt_id", "digest", "artifact_uri"}
    leaderboard = run.tags.get("leaderboard")
    if (
        not isinstance(receipt, dict)
        or set(receipt) != fields
        or receipt.get("attempt_id") != str(attempt.id)
        or not isinstance(receipt.get("digest"), str)
        or not re.fullmatch("[0-9a-f]{64}", receipt["digest"])
        or not isinstance(uri, str)
        or not uri
        or (not training and receipt.get("artifact_uri") != uri)
        or (
            training
            and (
                not isinstance(leaderboard, list)
                or not leaderboard
                or not isinstance(leaderboard[0], dict)
                or leaderboard[0].get("model_artifact_uri") != uri
                or leaderboard[0].get("model") != run.tags.get("winner")
            )
        )
    ):
        raise StaleFence("Parent result receipt is incomplete or belongs to another attempt")
    terminal = attempt.status == AttemptStatus.SUCCEEDED and run.status == RunStatus.SUCCEEDED
    pending = (
        training and attempt.status == AttemptStatus.RUNNING and run.status == RunStatus.RUNNING
    )
    if not (
        (terminal and attempt.terminal_cas_version == 1 and attempt.checkpoint_uri == uri)
        or (pending and attempt.terminal_cas_version == 0)
    ):
        raise StaleFence("Parent result and terminal state disagree")
    return receipt


def _failed(db, attempt, run):
    event = _event(db, attempt.id, FAILURE)
    if event is None:
        return (
            run.run_kind != RunKind.TRAINING
            and attempt.status == AttemptStatus.FAILED
            and run.status == RunStatus.FAILED
            and run.failure_code == "ANALYSIS_FAILED"
            and attempt.terminal_cas_version == 1
            and isinstance(run.failure_message, str)
            and attempt.terminal_reason == run.failure_message[:2000]
        )
    if (
        attempt.status != AttemptStatus.FAILED
        or run.status != RunStatus.FAILED
        or run.failure_code != "ray_attempt_failed"
        or attempt.terminal_cas_version != 1
        or event.event_type != FAILURE
        or event.result_digest != canonical_request_hash(event.payload)
        or event.payload != {"reason_digest": canonical_request_hash(run.failure_message), "cas": 1}
        or attempt.terminal_reason != run.failure_message
    ):
        raise StaleFence("Parent failure receipt changed")
    return True


def _control_scope(db, attempt_id, *, request, action, lock=False, expected_project=None):
    attempt, run = _rows(
        db, attempt_id, request.run_id, lock=lock, expected_project=expected_project
    )
    if action == "start" and request.run_kind != run.run_kind.value:
        raise StaleFence("Parent execution kind changed before start")
    result = _result(attempt, run)
    if result is not None and run.run_kind == RunKind.TRAINING:
        from automl_api.services.training_result import require_training_result_receipt

        require_training_result_receipt(db, attempt, run)
    failure = action == "failure" and _failed(db, attempt, run)
    if result is None and not failure:
        return _scope(db, attempt_id, lock=lock, expected_project=expected_project)
    publisher = _publisher(db, attempt, run)
    if publisher is None or (result is not None and action == "heartbeat"):
        raise StaleFence("Stored-result recovery has no publisher or cannot renew execution")
    return AdmissionScope(
        attempt.id,
        attempt.project_id,
        attempt.generation,
        attempt.fencing_token,
        attempt.workload_identity,
        attempt.ray_job_name,
        run.k8s_namespace,
        canonical_request_hash(
            {
                "publisher": publisher.result_digest,
                "result": result,
                "artifact_uri": run.tags.get(
                    "winner_model_artifact_uri"
                    if run.run_kind == RunKind.TRAINING
                    else "artifact_uri"
                ),
                "failure": failure,
                "failure_digest": canonical_request_hash(run.failure_message) if failure else None,
                "attempt_status": attempt.status,
                "run_status": run.status,
                "cas": attempt.terminal_cas_version,
                "checkpoint": attempt.checkpoint_uri,
            }
        ),
    )


def _reply(attempt, run, *, changed=False):
    return ParentControlReply(
        protocol="parent-control-v1",
        attempt_id=attempt.id,
        run_id=run.id,
        project_id=attempt.project_id,
        generation=attempt.generation,
        run_kind=run.run_kind.value,
        audience=attempt_audience(attempt.id, attempt.fencing_token),
        has_result=_result(attempt, run) is not None,
        completed=attempt.status == AttemptStatus.SUCCEEDED and run.status == RunStatus.SUCCEEDED,
        changed=changed,
    ).model_dump(mode="json")


def control_parent(factory, attempt_id, token, k8s, action, request):
    request = ParentControlRequest.model_validate(request)
    if action not in {"context", "start", "heartbeat", "receipt", "complete", "failure"}:
        raise StaleFence("Unknown parent control action")
    if (action == "failure") != (request.failure is not None):
        raise StaleFence("Parent control has invalid failure arguments")
    resolver = partial(_control_scope, request=request, action=action)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(factory, attempt_id, token, k8s, resolver, event_type="parent.control.identity")
    with factory() as db, db.begin():
        current = resolver(
            db, attempt_id, lock=True, expected_project=uuid.UUID(proof["project_id"])
        )
        if (
            current != captured
            or proof["generation"] != current.generation
            or proof["audience"] != attempt_audience(current.id, current.fencing_token)
            or proof["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Parent authority changed during identity verification")
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        publisher = _publisher(db, attempt, run)
        result = _result(attempt, run)
        owner = (
            "parent-"
            + hashlib.sha256(f"{proof['pod_uid']}:{request.worker_id}".encode()).hexdigest()
        )
        if publisher is not None and publisher.payload["pod_uid"] != proof["pod_uid"]:
            raise StaleFence("Another Pod owns this parent publisher")
        if action == "context":
            return _reply(attempt, run)
        if result is not None:
            changed = False
            if action in {"receipt", "complete"} and attempt.status != AttemptStatus.SUCCEEDED:
                complete_parent(db, run.id, (attempt.id, attempt.fencing_token))
                changed = True
            if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
                raise StaleFence("Parent identity expired before result acknowledgement")
            return _reply(attempt, run, changed=changed)
        if action == "receipt":
            return _reply(attempt, run)
        if action == "complete":
            raise StaleFence("Parent completion requires a committed result receipt")
        if action == "start":
            if publisher is None:
                publisher = _append(
                    db,
                    attempt,
                    PUBLISHER,
                    {
                        "owner": owner,
                        "pod_uid": proof["pod_uid"],
                        "location_digest": _location(attempt, run),
                        "source_binding": current.binding_digest,
                    },
                )
            if (
                publisher.payload["owner"] != owner
                or publisher.payload["source_binding"] != current.binding_digest
            ):
                raise StaleFence("Another worker or source owns this parent")
            start_parent(db, run.id, (attempt.id, attempt.fencing_token), owner, proof)
            changed = True
        else:
            if publisher is None or publisher.payload["owner"] != owner:
                raise StaleFence("Parent control has no matching publisher")
            if action == "failure" and _failed(db, attempt, run):
                return _reply(attempt, run)
            if publisher.payload["source_binding"] != current.binding_digest:
                raise StaleFence("Parent source changed after ownership was acquired")
            if action == "heartbeat":
                heartbeat_parent(db, run.id, (attempt.id, attempt.fencing_token), owner, proof)
                changed = True
            else:
                changed = fail_parent(
                    db,
                    run.id,
                    (attempt.id, attempt.fencing_token),
                    RuntimeError(request.failure),
                    owner,
                )
                if changed:
                    _append(
                        db,
                        attempt,
                        FAILURE,
                        {
                            "reason_digest": canonical_request_hash(run.failure_message),
                            "cas": attempt.terminal_cas_version,
                        },
                    )
        if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
            raise StaleFence("Parent identity expired before publication")
        return _reply(attempt, run, changed=changed)
