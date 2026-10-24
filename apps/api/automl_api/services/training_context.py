"""Validated training input metadata with fresh authority around native identity I/O."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial

from automl_api.models.enums import AttemptStatus, RunKind, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    ExperimentSpecRevision,
    SearchObjectiveRevision,
    WorkflowAttempt,
)
from automl_api.schemas.training_context import (
    CANDIDATE_KEYS,
    PARAMETER_KEYS,
    TAG_KEYS,
    TRAINING_CONTEXT_MAX_BYTES,
    TrainingContextReply,
    TrainingContextRequest,
)
from automl_api.services.attempt_identity import _admit, _scope, attempt_audience
from automl_api.services.experiment_identity import BINDINGS
from automl_api.services.parent_control import _publisher
from automl_api.services.training_parent import _require_parent_lease
from automl_api.services.workflow_state import StaleFence, canonical_request_hash


def _input_graph(db, run, *, lock):
    graph = {}
    for field, model in {
        **BINDINGS,
        "experiment_spec_revision_id": ExperimentSpecRevision,
        "search_objective_revision_id": SearchObjectiveRevision,
    }.items():
        key = "estimator_catalog_revision_id" if field == "catalog_revision_id" else field
        row = db.get(
            model,
            uuid.UUID(run.params[key]),
            populate_existing=True,
            with_for_update={"read": True} if lock else None,
        )
        if row is None or row.project_id != run.project_id:
            raise StaleFence("Training input graph belongs to another project or is missing")
        graph[field] = {
            "id": str(row.id),
            "digest": row.content_digest,
            "specification": row.specification,
        }
    return graph


def _reply(scope, prepared):
    run, split, columns, source = prepared
    tags = {key: value for key, value in (run.tags or {}).items() if key in TAG_KEYS}
    if "leaderboard" in tags:
        tags["leaderboard"] = [
            {key: value for key, value in entry.items() if key in CANDIDATE_KEYS}
            for entry in tags["leaderboard"]
        ]
    spec = split.specification
    counts = spec.get("split_counts") or (spec.get("identity") or {}).get("split_counts") or {}
    reply = TrainingContextReply(
        protocol="training-context-v1",
        attempt_id=scope.id,
        project_id=scope.project_id,
        audience=attempt_audience(scope.id, scope.fencing_token),
        run={
            **{
                key: getattr(run, key)
                for key in (
                    "id",
                    "project_id",
                    "dataset_version_id",
                    "status",
                    "task_type",
                    "run_name",
                    "target_column",
                )
            },
            "params": {key: value for key, value in run.params.items() if key in PARAMETER_KEYS},
            "tags": tags,
        },
        split={
            **{key: getattr(split, key) for key in ("id", "project_id", "dataset_version_id")},
            "train_uri": spec["uris"]["train"],
            "validation_uri": spec["uris"]["validation"],
            "split_counts": {role: int(counts.get(role) or 0) for role in ("train", "validation")},
            **{
                key: spec.get(key)
                for key in ("split_strategy", "time_column", "row_set_digest", "provider_import_id")
            },
        },
        approved_columns=columns,
        source_plan=source,
    )
    if len(reply.model_dump_json().encode()) > TRAINING_CONTEXT_MAX_BYTES:
        raise StaleFence("Training context exceeds its metadata budget")
    return reply.model_dump(mode="json")


def _context_scope(
    db,
    attempt_id,
    *,
    request,
    lock=False,
    expected_project=None,
    include_progress=True,
    input_binding=None,
):
    from automl_api.training import pipeline

    scope = _scope(db, attempt_id, lock=lock, expected_project=expected_project)
    attempt = db.get(WorkflowAttempt, attempt_id, populate_existing=True)
    run = db.get(ModelRun, request.run_id, populate_existing=True)
    if (
        attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.status != AttemptStatus.RUNNING
        or attempt.model_run_id != request.run_id
        or run is None
        or run.project_id != scope.project_id
        or run.run_kind != RunKind.TRAINING
        or (run.tags or {}).get("training_result_receipt") is not None
    ):
        raise StaleFence("Training context requires its current unfinished training parent")
    publisher = _publisher(db, attempt, run)
    if publisher is None or attempt.lease_owner != publisher.payload["owner"]:
        raise StaleFence("Training context requires its admitted parent publisher")
    graph = _input_graph(db, run, lock=lock)
    prepared = pipeline._prepare_training_context(
        db, run.id, context=(scope.id, scope.fencing_token), require_bound=True, mark_running=False
    )
    if prepared is None:
        raise StaleFence("A completed run cannot authorize new training input")
    reply = _reply(scope, prepared)
    if not include_progress:
        reply["run"]["tags"] = {}
    if input_binding is not None:
        input_binding.update(
            {
                "graph": graph,
                "context": {
                    key: value
                    for key, value in reply.items()
                    if key not in {"attempt_id", "audience"}
                },
            }
        )
    return replace(
        scope,
        binding_digest=canonical_request_hash(
            {
                "source": scope.binding_digest,
                "publisher": publisher.result_digest,
                "graph": graph,
                "context": reply,
            }
        ),
    )


def load_training_context(factory, attempt_id, token, k8s, request):
    from automl_api.training import pipeline

    request = TrainingContextRequest.model_validate(request)
    resolver = partial(_context_scope, request=request)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(
        factory, attempt_id, token, k8s, resolver, event_type="training.context.identity"
    )
    with factory() as db, db.begin():
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt = db.get(WorkflowAttempt, attempt_id, populate_existing=True)
        run = db.get(ModelRun, request.run_id, populate_existing=True)
        publisher = _publisher(db, attempt, run)
        if (
            current != captured
            or proof["generation"] != current.generation
            or proof["audience"] != attempt_audience(current.id, current.fencing_token)
            or publisher.payload["pod_uid"] != proof["pod_uid"]
        ):
            raise StaleFence("Training input authority changed during identity verification")
        prepared = pipeline._prepare_training_context(
            db,
            request.run_id,
            context=(current.id, current.fencing_token),
            require_bound=True,
            mark_running=True,
        )
        if prepared is None:
            raise StaleFence("Training context cannot restart a completed result")
        reply = _reply(current, prepared)
        _require_parent_lease(db, attempt, publisher.payload["owner"])
        if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
            raise StaleFence("Training identity expired before initialization")
        return reply
