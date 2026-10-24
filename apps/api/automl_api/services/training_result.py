"""Short SQL publication phases around fresh native parent identity."""

import uuid
from datetime import UTC, datetime
from functools import partial

from sqlalchemy import select

from automl_api.models.enums import MetricKind, MetricSplit, RunKind
from automl_api.models.runs import Metric
from automl_api.schemas.parent_control import ParentControlRequest
from automl_api.schemas.training_context import TrainingContextRequest
from automl_api.schemas.training_result import (
    RESULT_PARAM_KEYS,
    TrainingResultReceiptRequest,
    TrainingResultReply,
    TrainingResultRequest,
)
from automl_api.services.attempt_identity import _admit, attempt_audience
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.parent_control import _control_scope, _publisher, _result, _rows
from automl_api.services.training_context import _context_scope
from automl_api.services.training_model_output import _declared_model_attempt
from automl_api.services.training_parent import _require_parent_lease
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    RAY_TRAINING_PROGRESS_PROTOCOL,
    RAY_TRAINING_RESULT_PROTOCOL,
    RAY_TRAINING_TRACKING_PROTOCOL,
    StaleFence,
    canonical_request_hash,
)

RESULT_EVENT = "training.result.published"


def _stored_result(db, attempt, run):
    event = _event(db, attempt.id, RESULT_EVENT)
    if event is None:
        return None
    payload = event.payload
    stored_receipt = _result(attempt, run)
    if stored_receipt is None:
        raise StaleFence("Stored training publication has no matching result")
    if run.tags.get("winner_mlflow_run_id") != (run.tags.get("leaderboard") or [{}])[0].get(
        "mlflow_run_id"
    ):
        raise StaleFence("Stored winner tracking identity changed")
    publisher = _publisher(db, attempt, run)
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {"request_digest", "result_receipt", "params", "derived_params", "pod_uid"}
        or event.event_type != RESULT_EVENT
        or event.result_digest != canonical_request_hash(payload)
        or stored_receipt != payload["result_receipt"]
        or publisher is None
        or payload["pod_uid"] != publisher.payload["pod_uid"]
    ):
        raise StaleFence("Stored training publication acknowledgement changed")
    rows = list(
        db.scalars(
            select(Metric).where(
                Metric.project_id == run.project_id,
                Metric.model_run_id == run.id,
                Metric.kind == MetricKind.PERFORMANCE,
                Metric.split == MetricSplit.VALIDATION,
            )
        )
    )
    from automl_api.training.evaluation import metric_direction

    if any(
        row.step != 0
        or row.higher_is_better != (metric_direction(row.name) == "maximize")
        or row.value_json != {}
        for row in rows
    ):
        raise StaleFence("Stored training metric metadata changed")
    if canonical_request_hash(payload["derived_params"]) != canonical_request_hash(
        {key: run.params.get(key) for key in RESULT_PARAM_KEYS}
    ):
        raise StaleFence("Stored derived training parameters changed")
    metrics = {row.name: row.value for row in rows}
    if len(metrics) != len(rows):
        raise StaleFence("Stored training metrics are ambiguous")
    expected = canonical_request_hash(
        {
            "metrics": metrics,
            "params": payload["params"],
            "leaderboard": run.tags["leaderboard"],
            "primary_metric": run.tags["leaderboard_primary_metric"],
            "mlflow_run_id": run.mlflow_run_id,
        }
    )
    if expected != payload["result_receipt"]["digest"]:
        raise StaleFence("Stored training result differs from its publication receipt")
    from automl_api.services.training_tracking import require_parent_tracking

    require_parent_tracking(db, attempt, run, run.mlflow_run_id)
    from automl_api.services.training_model_output import require_stored_tracking

    require_stored_tracking(db, attempt, run)
    from automl_api.services.training_metadata import result_metadata

    result_metadata(db, attempt, run, payload["result_receipt"])
    return payload


def _result_scope(db, attempt_id, *, run_id, lock=False, expected_project=None):
    # Serialise receipt lookup with an in-flight commit before deciding it is absent.
    attempt, run = _rows(db, attempt_id, run_id, lock=True, expected_project=expected_project)
    if run.run_kind != RunKind.TRAINING or not _declared_model_attempt(db, run, attempt):
        raise StaleFence("Training publication requires its declared training parent")

    if not requires_training_result_rpc(db, attempt, run):
        raise StaleFence("Training result RPC requires its saved publication protocol")
    if _stored_result(db, attempt, run) is not None:
        return _control_scope(
            db,
            attempt_id,
            request=ParentControlRequest(run_id=run_id, worker_id=uuid.UUID(int=0)),
            action="receipt",
            lock=True,
            expected_project=run.project_id,
        )
    if _result(attempt, run) is not None:
        raise StaleFence("A legacy result cannot be relabelled as an RPC publication")
    return _context_scope(
        db,
        attempt_id,
        request=TrainingContextRequest(run_id=run_id),
        lock=True,
        expected_project=run.project_id,
        include_progress=False,
    )


def _reply(attempt, run, stored):
    return TrainingResultReply(
        protocol="training-result-v1",
        attempt_id=attempt.id,
        project_id=run.project_id,
        run_id=run.id,
        audience=attempt_audience(attempt.id, attempt.fencing_token),
        request_digest=stored["request_digest"],
        committed=True,
    ).model_dump(mode="json")


def control_training_result(factory, attempt_id, token, k8s, request, *, receipt_only=False):
    from automl_api.training import pipeline

    request = (
        TrainingResultReceiptRequest if receipt_only else TrainingResultRequest
    ).model_validate(request)
    digest = (
        request.request_digest
        if receipt_only
        else canonical_request_hash(request.model_dump(mode="json"))
    )
    resolver = partial(_result_scope, run_id=request.run_id)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(factory, attempt_id, token, k8s, resolver, event_type="training.result.identity")
    with factory() as db, db.begin():
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        publisher = _publisher(db, attempt, run)
        if (
            current != captured
            or proof["generation"] != current.generation
            or proof["audience"] != attempt_audience(current.id, current.fencing_token)
            or publisher is None
            or publisher.payload["pod_uid"] != proof["pod_uid"]
            or proof["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise StaleFence("Training publication authority changed during identity verification")
        stored = _stored_result(db, attempt, run)
        if stored is not None:
            if stored["request_digest"] != digest:
                raise StaleFence("This parent already published another training result")
            return _reply(attempt, run, stored)
        if receipt_only:
            raise StaleFence("No committed training result acknowledgement exists")
        selected = run.params["candidate_models"]
        entries = request.model_dump(mode="json")["leaderboard"]
        names = [entry["model"] for entry in entries]
        if (
            len(names) != len(set(names))
            or set(names) != set(selected)
            or request.primary_metric != run.params["primary_metric"]
        ):
            raise StaleFence("Final publication requires the complete declared tournament")
        fixed_label = run.params.get("positive_label")
        if (
            fixed_label is not None
            and "positive_label" in request.params
            and canonical_request_hash(fixed_label)
            != canonical_request_hash(request.params["positive_label"])
        ):
            raise StaleFence("Training result cannot change the declared positive label")
        duplicate_rows = request.params.get("deduplicated_rows", 0)
        if duplicate_rows:
            from automl_api.models.workflows import DatasetSplitRevision

            split = db.get(DatasetSplitRevision, uuid.UUID(run.params["split_revision_id"]))
            counts = (
                split.specification.get("split_counts")
                or (split.specification.get("identity") or {}).get("split_counts")
                or {}
            )
            if duplicate_rows > int(counts.get("train") or 0):
                raise StaleFence("Derived duplicate rows exceed the sealed training partition")
        ranked = [
            {"duration_seconds": None, "error": None, "cost_tier": "unknown", **entry}
            for entry in pipeline.rank_leaderboard(entries, request.primary_metric)
        ]
        winner = ranked[0]
        if (
            winner["status"] != "succeeded"
            or request.primary_metric not in winner.get("metrics", {})
            or request.metrics != winner["metrics"]
            or names != [entry["model"] for entry in ranked]
        ):
            raise StaleFence("Final metrics and winner differ from the ranked tournament")
        result = pipeline.TournamentResult(
            metrics={key: float(value) for key, value in request.metrics.items()},
            model=None,
            params=request.model_dump(mode="json")["params"],
            leaderboard=ranked,
            primary_metric=request.primary_metric,
        )
        from automl_api.services.training_tracking import require_parent_tracking

        require_parent_tracking(db, attempt, run, request.mlflow_run_id, live=True)
        pipeline._store_training_success(
            db, run, result, request.mlflow_run_id, context=(attempt.id, attempt.fencing_token)
        )
        _append(
            db,
            attempt,
            RESULT_EVENT,
            {
                "request_digest": digest,
                "result_receipt": run.tags["training_result_receipt"],
                "params": result.params,
                "derived_params": {key: run.params.get(key) for key in RESULT_PARAM_KEYS},
                "pod_uid": proof["pod_uid"],
            },
        )
        db.flush()
        _require_parent_lease(db, attempt, publisher.payload["owner"])
        if proof["expires_at"] <= int(datetime.now(UTC).timestamp()):
            raise StaleFence("Training identity expired before result commit")
        stored = _stored_result(db, attempt, run)
        return _reply(attempt, run, stored)


def requires_training_result_rpc(db, attempt, run):
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services.reconciler import _ray_credential_protocol

    entries = [
        entry
        for entry in db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == run.project_id,
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
            )
        )
        if entry.payload.get("attempt_id") == str(attempt.id)
    ]
    if len(entries) > 1:
        raise StaleFence("Training result has ambiguous saved publication declarations")
    if not entries:
        return False
    required = _ray_credential_protocol(entries[0]) in {
        RAY_TRAINING_RESULT_PROTOCOL,
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
        RAY_TRAINING_PROGRESS_PROTOCOL,
    }
    if required and entries[0].payload.get("fencing_token") != attempt.fencing_token:
        raise StaleFence("Training result publication declaration fence changed")
    return required


def require_training_result_receipt(db, attempt, run):
    """All training completion callers enforce the saved result protocol."""
    if run.run_kind != RunKind.TRAINING or not requires_training_result_rpc(db, attempt, run):
        return None
    stored = _stored_result(db, attempt, run)
    if stored is None:
        raise StaleFence("Training completion requires its declared RPC publication receipt")
    return stored
