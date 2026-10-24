"""Ordered, Pod-owned progress receipts around short fenced SQL mutations."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial

from sqlalchemy import BigInteger, cast, select

from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, TrainingCandidate, WorkflowEvent
from automl_api.schemas.training_progress import (
    TERMINAL,
    TrainingProgressReceiptRequest,
    TrainingProgressReply,
    TrainingProgressRequest,
)
from automl_api.services.attempt_identity import _admit, attempt_audience
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.parent_control import _publisher, _rows
from automl_api.services.training_parent import _require_parent_lease
from automl_api.services.training_result import _result_scope, _stored_result
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    RAY_TRAINING_PROGRESS_PROTOCOL,
    RAY_TRAINING_TRACKING_PROTOCOL,
    StaleFence,
    canonical_request_hash,
)

PROGRESS_EVENT = "training.progress.committed"
PROGRESS_KEY = "training.progress."


def requires_progress_rpc(db, attempt, run):
    from automl_api.services.reconciler import _ray_credential_protocol

    rows = [
        row
        for row in db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == run.project_id,
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
            )
        )
        if row.payload.get("attempt_id") == str(attempt.id)
    ]
    if len(rows) != 1:
        raise StaleFence("Progress requires one saved training declaration")
    if rows[0].payload.get("fencing_token") != attempt.fencing_token:
        raise StaleFence("Progress declaration fence changed")
    return _ray_credential_protocol(rows[0]) in {
        RAY_TRAINING_PROGRESS_PROTOCOL,
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
    }


def require_terminal_metadata(run, entries):
    previous = {
        entry["model"]: entry
        for entry in (run.tags or {}).get("leaderboard", [])
        if entry.get("extension_run_id") in {None, str(run.id)}
    }
    defaults = dict(
        cost_tier="unknown",
        metrics={},
        diagnostics={},
        best_params={},
        duration_seconds=None,
        error=None,
        mlflow_run_id=None,
    )
    mutable = {"rank", "primary_score", "phase", "phase_updated_at", "resumed"}

    def core(entry):
        return {key: value for key, value in {**defaults, **entry}.items() if key not in mutable}

    for entry in entries:
        old = previous.get(entry["model"])
        if old and old.get("status") in TERMINAL and entry.get("status") in TERMINAL:
            if canonical_request_hash(core(old)) != canonical_request_hash(core(entry)):
                raise StaleFence("Terminal candidate metadata cannot change")


def _progress_scope(db, attempt_id, *, run_id, lock=False, expected_project=None):
    scope = _result_scope(
        db, attempt_id, run_id=run_id, lock=lock, expected_project=expected_project
    )
    attempt, run = _rows(db, attempt_id, run_id, lock=True, expected_project=scope.project_id)
    if not requires_progress_rpc(db, attempt, run):
        raise StaleFence("Progress RPC requires its saved protocol")
    candidates = list(
        db.scalars(
            select(TrainingCandidate)
            .where(
                TrainingCandidate.project_id == scope.project_id,
                TrainingCandidate.model_run_id == run_id,
            )
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
    )
    names = [row.estimator_key for row in candidates]
    if len(names) != len(set(names)) or set(names) != set(run.params["candidate_models"]):
        raise StaleFence("Progress requires the unique selected candidate set")
    routing = {
        key: run.tags.get(key) for key in ("leaderboard_parent_run_id", "candidate_parent_run_id")
    }
    target = routing["leaderboard_parent_run_id"]
    if target:
        parent = db.get(ModelRun, uuid.UUID(str(target)))
        if parent is None or parent.project_id != scope.project_id or parent.id == run.id:
            raise StaleFence("Progress parent routing belongs to another scope")
    return replace(
        scope,
        binding_digest=canonical_request_hash(
            {
                "scope": scope.binding_digest,
                "routing": routing,
                "candidates": sorted(
                    [
                        {
                            key: getattr(row, key)
                            for key in (
                                "id",
                                "candidate_key",
                                "estimator_key",
                                "catalog_revision_id",
                                "feature_recipe_revision_id",
                                "params",
                            )
                        }
                        for row in candidates
                    ],
                    key=lambda row: str(row["id"]),
                ),
            }
        ),
    )


def _stored(event, run):
    payload = event.payload
    if (
        event.event_type != PROGRESS_EVENT
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "run_id",
            "request_id",
            "request_digest",
            "pod_uid",
            "client_id",
            "sequence",
            "stream",
            "changed",
        }
        or payload["run_id"] != str(run.id)
        or event.event_key != PROGRESS_KEY + payload["request_id"]
        or event.result_digest != canonical_request_hash(payload)
        or type(payload["sequence"]) is not int
        or payload["sequence"] < 1
        or type(payload["changed"]) is not bool
    ):
        raise StaleFence("Stored progress acknowledgement changed")
    uuid.UUID(payload["request_id"])
    uuid.UUID(payload["client_id"])
    return payload


def _reply(attempt, run, stored):
    return TrainingProgressReply(
        protocol="training-progress-v1",
        attempt_id=attempt.id,
        project_id=run.project_id,
        run_id=run.id,
        audience=attempt_audience(attempt.id, attempt.fencing_token),
        request_id=stored["request_id"],
        request_digest=stored["request_digest"],
        changed=stored["changed"],
    ).model_dump(mode="json")


def control_training_progress(factory, attempt_id, token, k8s, request, *, receipt_only=False):
    from automl_api.training import pipeline

    request = (
        TrainingProgressReceiptRequest if receipt_only else TrainingProgressRequest
    ).model_validate(request)
    resolver = partial(_progress_scope, run_id=request.run_id)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(
        factory, attempt_id, token, k8s, resolver, event_type="training.progress.identity"
    )
    with factory() as db, db.begin():
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        if (
            current != captured
            or proof["generation"] != current.generation
            or proof["audience"] != attempt_audience(current.id, current.fencing_token)
            or proof["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Progress authority changed during identity verification")
        publisher = _publisher(db, attempt, run)
        prior = _event(db, attempt.id, PROGRESS_KEY + str(request.request_id))
        if receipt_only:
            if prior is None:
                raise StaleFence("No committed progress acknowledgement exists")
            stored = _stored(prior, run)
            if stored["pod_uid"] != proof["pod_uid"]:
                raise StaleFence("Progress receipt belongs to another Pod")
            if stored["stream"] == "parent" and publisher.payload["pod_uid"] != proof["pod_uid"]:
                raise StaleFence("Aggregate progress requires its parent publisher")
            return _reply(attempt, run, stored)
        if request.action == "partial":
            stream = "parent"
            if publisher.payload["pod_uid"] != proof["pod_uid"]:
                raise StaleFence("Aggregate progress requires its parent publisher")
            names = [entry["model"] for entry in request.entries]
            if (
                len(names) != len(set(names))
                or set(names) != set(run.params["candidate_models"])
                or request.primary_metric != run.params["primary_metric"]
            ):
                raise StaleFence("Aggregate progress requires the complete selected tournament")
            candidate = None
        else:
            candidate = db.scalar(
                select(TrainingCandidate).where(
                    TrainingCandidate.project_id == run.project_id,
                    TrainingCandidate.model_run_id == run.id,
                    TrainingCandidate.estimator_key == request.candidate_name,
                )
            )
            if candidate is None:
                raise StaleFence("Phase belongs to an unselected candidate")
            stream = str(candidate.id)
        digest = canonical_request_hash(request.model_dump(mode="json"))
        if prior is not None:
            stored = _stored(prior, run)
            if stored["pod_uid"] != proof["pod_uid"] or stored["request_digest"] != digest:
                raise StaleFence("Progress request identity was reused")
            return _reply(attempt, run, stored)
        latest_event = db.scalar(
            select(WorkflowEvent)
            .where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_type == PROGRESS_EVENT,
                WorkflowEvent.payload["stream"].astext == stream,
            )
            .order_by(cast(WorkflowEvent.payload["sequence"].astext, BigInteger).desc())
            .limit(1)
        )
        latest = _stored(latest_event, run) if latest_event is not None else None
        if latest and (
            latest["pod_uid"] != proof["pod_uid"] or latest["client_id"] != str(request.client_id)
        ):
            raise StaleFence("Progress stream belongs to another producer")
        reused = db.scalar(
            select(WorkflowEvent.id)
            .where(
                WorkflowEvent.attempt_id == attempt.id,
                WorkflowEvent.event_type == PROGRESS_EVENT,
                WorkflowEvent.payload["stream"].astext == stream,
                WorkflowEvent.payload["sequence"].astext == str(request.sequence),
            )
            .limit(1)
        )
        if reused is not None:
            raise StaleFence("Progress sequence was reused for another request")
        changed = False
        complete = _stored_result(db, attempt, run) is not None
        if not complete and (latest is None or request.sequence > latest["sequence"]):
            context = (attempt.id, attempt.fencing_token)
            if request.action == "partial":
                entries = request.model_dump(mode="json")["entries"]
                pipeline._store_partial_leaderboard(
                    db, run, entries, request.primary_metric, context=context, model_pods=True
                )
                changed = True
            elif candidate.status not in TERMINAL:
                changed = pipeline._store_candidate_phase(
                    db, run, request.candidate_name, request.phase, context=context
                )
        stored = dict(
            run_id=str(run.id),
            request_id=str(request.request_id),
            request_digest=digest,
            pod_uid=proof["pod_uid"],
            client_id=str(request.client_id),
            sequence=request.sequence,
            stream=stream,
            changed=changed,
        )
        event = _append(db, attempt, PROGRESS_KEY + str(request.request_id), stored)
        event.event_type = PROGRESS_EVENT
        if not complete:
            _require_parent_lease(db, attempt, publisher.payload["owner"])
        if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
            raise StaleFence("Progress identity expired before commit")
        return _reply(attempt, run, stored)
