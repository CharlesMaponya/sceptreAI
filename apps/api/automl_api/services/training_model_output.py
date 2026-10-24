"""Sealed training-candidate authority for the shared native output broker."""

import re
import uuid
from dataclasses import dataclass, replace
from functools import partial

from sqlalchemy import select

from automl_api.models.workflows import OutboxEntry, TrainingCandidate, WorkflowEvent
from automl_api.schemas.training_context import TrainingContextRequest
from automl_api.services.attempt_identity import _admit
from automl_api.services.evaluation_publication import _event
from automl_api.services.training_context import _context_scope
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    RAY_TRAINING_PROGRESS_PROTOCOL,
    RAY_TRAINING_RESULT_PROTOCOL,
    RAY_TRAINING_TRACKING_PROTOCOL,
    StaleFence,
    canonical_request_hash,
)

MODEL_CREDENTIAL_PROTOCOL = "ray-model-output-v5"


@dataclass(frozen=True)
class TrainingModelPlan:
    project_id: uuid.UUID
    run_id: uuid.UUID
    attempt_id: uuid.UUID
    candidate_id: uuid.UUID
    candidate_name: str
    input_digest: str
    tracking_receipt: dict | None = None
    metadata_receipt: dict | None = None


def model_scope(
    db, attempt_id, *, request, lock=False, expected_project=None, tracking_required=True
):
    input_binding = {}
    scope = _context_scope(
        db,
        attempt_id,
        request=TrainingContextRequest(run_id=request.run_id),
        lock=lock,
        expected_project=expected_project,
        include_progress=False,
        input_binding=input_binding,
    )
    candidates = (
        select(TrainingCandidate)
        .where(
            TrainingCandidate.project_id == scope.project_id,
            TrainingCandidate.model_run_id == request.run_id,
            TrainingCandidate.estimator_key == request.candidate_name,
        )
        .execution_options(populate_existing=True)
    )
    entries = (
        select(OutboxEntry)
        .where(
            OutboxEntry.project_id == scope.project_id,
            OutboxEntry.aggregate_id == request.run_id,
            OutboxEntry.topic == "ray.training.submit",
        )
        .execution_options(populate_existing=True)
    )
    if lock:
        candidates, entries = (
            candidates.with_for_update(read=True),
            entries.with_for_update(read=True),
        )
    candidates = list(db.scalars(candidates))
    entries = [
        row
        for row in db.scalars(entries)
        if (
            row.payload.get("attempt_id") == str(attempt_id)
            and row.payload.get("fencing_token") == scope.fencing_token
            and row.payload.get("credential_protocol")
            in {
                MODEL_CREDENTIAL_PROTOCOL,
                RAY_TRAINING_RESULT_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
            }
        )
    ]
    if len(candidates) != 1 or len(entries) != 1:
        raise StaleFence("Model output requires its declared current candidate and writer")
    candidate, entry = candidates[0], entries[0]
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.training_tracking import requires_tracking_receipts, tracking_reference

    run, attempt = db.get(ModelRun, request.run_id), db.get(WorkflowAttempt, attempt_id)
    tracking = None
    if tracking_required and requires_tracking_receipts(db, attempt, run):
        tracking = tracking_reference(
            db, attempt, run, f"candidate:{request.candidate_name}", live=True
        )
    metadata = None
    if tracking_required and tracking is not None:
        from automl_api.services.training_metadata import metadata_reference

        metadata = metadata_reference(
            db, attempt, run, f"candidate:{request.candidate_name}", tracking
        )
    binding = {
        "context": scope.binding_digest,
        "outbox": {"id": entry.id, "payload": entry.payload},
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
    }
    if tracking is not None:
        binding["tracking_receipt"] = tracking
    if metadata is not None:
        binding["metadata_receipt"] = metadata
    return replace(
        scope,
        binding_digest=canonical_request_hash(binding),
        source_plan=TrainingModelPlan(
            scope.project_id,
            request.run_id,
            attempt_id,
            candidate.id,
            candidate.estimator_key,
            canonical_request_hash({"input": input_binding, "candidate": binding["candidate"]}),
            tracking,
            metadata,
        ),
    )


def admit_model(factory, attempt_id, token, k8s, request):
    resolver = partial(model_scope, request=request)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(factory, attempt_id, token, k8s, resolver, event_type="training.model.identity")
    if captured.source_plan.tracking_receipt is not None:
        from automl_api.models.runs import ModelRun
        from automl_api.models.workflows import WorkflowAttempt
        from automl_api.schemas.training_tracking import TrainingTrackingRequest
        from automl_api.services.training_tracking import _owner

        with factory() as db, db.begin():
            owner = _owner(
                db,
                db.get(WorkflowAttempt, attempt_id),
                db.get(ModelRun, request.run_id),
                TrainingTrackingRequest(
                    run_id=request.run_id, role=f"candidate:{request.candidate_name}"
                ),
            )
            if owner is None or owner.payload["pod_uid"] != proof["pod_uid"]:
                raise StaleFence("Model writer is not its original candidate tracking producer")
    proof["model_scope"] = captured
    return proof


def model_slot(db, row):
    """The reserved candidate slot is immutable even after expiry or an unknown write."""
    events = list(
        db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == row.attempt_id,
                WorkflowEvent.event_key.startswith("training.model.grant."),
                WorkflowEvent.payload["grant_id"].astext == str(row.id),
            )
        )
    )
    if len(events) != 1:
        raise StaleFence("Model output has no unique candidate reservation")
    event = events[0]
    candidate = db.get(TrainingCandidate, uuid.UUID(event.payload["candidate_id"]))
    if candidate is None or candidate.project_id != row.project_id:
        raise StaleFence("Model output candidate is missing")
    expected = {
        "grant_id": str(row.id),
        "candidate_id": str(candidate.id),
        "candidate_name": candidate.estimator_key,
        "run_id": str(candidate.model_run_id),
    }
    if "input_digest" in event.payload:
        digest = event.payload["input_digest"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise StaleFence("Model custody input digest changed")
        expected["input_digest"] = digest
    if "tracking_receipt" in event.payload:
        from automl_api.models.runs import ModelRun
        from automl_api.models.workflows import WorkflowAttempt
        from automl_api.schemas.training_tracking import (
            TrainingTrackingReference,
            TrainingTrackingRequest,
        )
        from automl_api.services.training_tracking import _owner, validate_tracking_reference

        expected["tracking_receipt"] = TrainingTrackingReference.model_validate(
            event.payload["tracking_receipt"]
        ).model_dump(mode="json")
        run = db.get(ModelRun, candidate.model_run_id)
        reference = validate_tracking_reference(
            db, run, expected["tracking_receipt"], f"candidate:{candidate.estimator_key}"
        )
        owner = _owner(
            db,
            db.get(WorkflowAttempt, row.attempt_id),
            run,
            TrainingTrackingRequest(run_id=run.id, role=f"candidate:{candidate.estimator_key}"),
        )
        if (
            reference["attempt_id"] != str(row.attempt_id)
            or owner.payload["pod_uid"] != row.pod_uid
        ):
            raise StaleFence("Model slot belongs to another tracking producer")
    else:
        from automl_api.models.runs import ModelRun
        from automl_api.models.workflows import WorkflowAttempt
        from automl_api.services.training_tracking import requires_tracking_receipts

        source = db.get(WorkflowAttempt, row.attempt_id)
        run = db.get(ModelRun, candidate.model_run_id)
        if source is not None and requires_tracking_receipts(db, source, run):
            raise StaleFence(
                "Model slot has no original tracking custody; controlled qualification required"
            )
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.training_metadata import (
        requires_metadata_receipts,
        validate_metadata_reference,
    )

    source = db.get(WorkflowAttempt, row.attempt_id)
    run = db.get(ModelRun, candidate.model_run_id)
    if "metadata_receipt" in event.payload:
        metadata = validate_metadata_reference(
            db,
            run,
            event.payload["metadata_receipt"],
            f"candidate:{candidate.estimator_key}",
            expected.get("tracking_receipt"),
        )
        if metadata["attempt_id"] != str(row.attempt_id) or metadata["pod_uid"] != row.pod_uid:
            raise StaleFence("Model metadata belongs to another original producer")
        expected["metadata_receipt"] = metadata
    elif source is not None and requires_metadata_receipts(db, source, run):
        raise StaleFence(
            "Model slot has no original metadata readiness; controlled recovery required"
        )
    if (
        event.payload != expected
        or event.result_digest != canonical_request_hash(expected)
        or event.event_key != f"training.model.grant.{candidate.id}"
    ):
        raise StaleFence("Model slot changed")
    return expected


def require_training_model_completion(db, attempt_id, fence, candidate_id, expected):
    """Only an original verified native completion may authorize model publication."""
    from automl_api.models.workflows import WorkflowAttemptCredentialGrant
    from automl_api.services.attempt_object_grants import output_completion
    from automl_api.services.attempt_output_grants import _receipt

    slot = _event(db, attempt_id, f"training.model.grant.{candidate_id}")
    if slot is not None:
        row = db.get(WorkflowAttemptCredentialGrant, uuid.UUID(slot.payload["grant_id"]))
        if (
            row is not None
            and row.attempt_id == attempt_id
            and row.fencing_token == fence
            and row.role == "model"
            and row.method == "PUT"
            and model_slot(db, row)["candidate_id"] == str(candidate_id)
        ):
            completion = output_completion(db, row)
            if (
                completion is not None
                and {key: completion[key] for key in ("uri", "sha256", "byte_size")} == expected
            ):
                _receipt(db, row)
                return
    raise StaleFence("Model has no matching verified native completion")


def _declared_model_attempt(db, run, attempt):
    rows = list(
        db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == run.project_id,
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
            )
        )
    )
    declared = [row for row in rows if row.payload.get("attempt_id") == str(attempt.id)]
    if len(declared) > 1:
        raise StaleFence("Model publication has ambiguous custody declarations")
    if not declared:
        return False
    from automl_api.services.reconciler import _ray_credential_protocol

    selected = _ray_credential_protocol(declared[0]) in {
        MODEL_CREDENTIAL_PROTOCOL,
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
        RAY_TRAINING_PROGRESS_PROTOCOL,
        RAY_TRAINING_RESULT_PROTOCOL,
    }
    if selected and declared[0].payload.get("fencing_token") != attempt.fencing_token:
        raise StaleFence("Model publication custody declaration fence changed")
    return selected


def _model_reference(db, row, candidate_id):
    from automl_api.services.attempt_object_grants import output_completion
    from automl_api.services.attempt_output_grants import _receipt

    slot = model_slot(db, row)
    # shortcut: old slots need controlled input qualification before predecessor adoption.
    if (
        slot["candidate_id"] != str(candidate_id)
        or "input_digest" not in slot
        or output_completion(db, row) is None
    ):
        raise StaleFence("Model has no verified input-bound custody completion")
    _receipt(db, row)
    reference = {
        "kind": "training_model",
        "attempt_id": str(row.attempt_id),
        "grant_id": str(row.id),
        "grant_digest": row.grant_digest,
        "candidate_id": str(candidate_id),
        "input_digest": slot["input_digest"],
    }

    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.training_tracking import requires_tracking_receipts

    source = db.get(WorkflowAttempt, row.attempt_id)
    run = db.get(ModelRun, uuid.UUID(slot["run_id"]))
    if (
        source is not None
        and requires_tracking_receipts(db, source, run)
        and "tracking_receipt" not in slot
    ):
        raise StaleFence(
            "Model has no original tracking custody; controlled qualification required"
        )
    if "tracking_receipt" in slot:
        from automl_api.services.training_tracking import validate_tracking_reference

        reference["tracking_receipt"] = validate_tracking_reference(
            db, run, slot["tracking_receipt"], f"candidate:{slot['candidate_name']}"
        )
    if "metadata_receipt" in slot:
        reference["metadata_receipt"] = slot["metadata_receipt"]
    return reference


def _authorized_model_reference(db, parent, row, candidate_id):
    from automl_api.models.enums import AttemptStatus, WorkflowStage
    from automl_api.models.workflows import WorkflowAttempt

    reference = _model_reference(db, row, candidate_id)
    current = parent
    for _ in range(parent.retry_budget):
        if reference not in current.predecessor_checkpoint_allowlist:
            raise StaleFence("Predecessor model custody is not explicitly authorized")
        predecessor = (
            db.get(WorkflowAttempt, current.predecessor_attempt_id)
            if current.predecessor_attempt_id
            else None
        )
        if (
            predecessor is None
            or predecessor.status != AttemptStatus.SUPERSEDED
            or predecessor.stage != WorkflowStage.TRAINING_RUN
            or predecessor.project_id != parent.project_id
            or predecessor.model_run_id != parent.model_run_id
            or predecessor.workload_identity != parent.workload_identity
            or current.generation != predecessor.generation + 1
        ):
            raise StaleFence("Predecessor model custody has invalid lineage")
        if predecessor.id == row.attempt_id:
            if row.fencing_token != predecessor.fencing_token:
                raise StaleFence("Predecessor model custody fence changed")
            return reference
        current = predecessor
    raise StaleFence("Predecessor model custody exceeds the recovery budget")


def _candidate_model_row(db, run, entry):
    from automl_api.models.workflows import WorkflowAttemptCredentialGrant

    candidate = list(
        db.scalars(
            select(TrainingCandidate).where(
                TrainingCandidate.project_id == run.project_id,
                TrainingCandidate.model_run_id == run.id,
                TrainingCandidate.estimator_key == entry["model"],
            )
        )
    )
    if len(candidate) != 1:
        raise StaleFence("Model publication requires one durable candidate")
    rows = list(
        db.scalars(
            select(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.project_id == run.project_id,
                WorkflowAttemptCredentialGrant.role == "model",
                WorkflowAttemptCredentialGrant.method == "PUT",
                WorkflowAttemptCredentialGrant.object_uri == entry.get("model_artifact_uri"),
                WorkflowAttemptCredentialGrant.content_sha256 == entry.get("model_artifact_sha256"),
            )
        )
    )
    if len(rows) != 1 or model_slot(db, rows[0])["candidate_id"] != str(candidate[0].id):
        raise StaleFence("Model has no unique matching verified custody completion")
    return candidate[0], rows[0]


def require_tracking_entry_shapes(db, run, entries):
    if not any(
        entry.get("status") != "succeeded" and entry.get("mlflow_run_id") is not None
        for entry in entries
    ):
        return
    from automl_api.models.enums import WorkflowStage
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.training_tracking import requires_tracking_receipts

    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == run.project_id,
            WorkflowAttempt.model_run_id == run.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    if attempt is not None and requires_tracking_receipts(db, attempt, run):
        raise StaleFence("Only successful candidate results may publish tracking references")


def require_published_models(db, run, entries, *, context=None):
    """Both partial and final publication require the original native model receipt."""
    import os
    from types import SimpleNamespace

    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.attempt_object_grants import _authority

    attempt_id = str(context[0]) if context is not None else os.getenv("AUTOML_ATTEMPT_ID")
    from automl_api.models.enums import WorkflowStage

    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.model_run_id == run.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    if attempt is None or not _declared_model_attempt(db, run, attempt):
        return
    if (
        attempt_id != str(attempt.id)
        or (context[1] if context is not None else os.getenv("AUTOML_FENCING_TOKEN"))
        != attempt.fencing_token
        or attempt.project_id != run.project_id
        or attempt.model_run_id != run.id
    ):
        raise StaleFence("Model publication has no current declared custody owner")
    from automl_api.services.training_progress import (
        require_terminal_metadata,
        requires_progress_rpc,
    )

    if requires_progress_rpc(db, attempt, run):
        require_terminal_metadata(run, entries)
    require_tracking_entry_shapes(db, run, entries)
    names = [entry["model"] for entry in entries]
    if len(names) != len(set(names)):
        raise StaleFence("Model publication contains duplicate candidates")
    for entry in entries:
        if entry.get("status") != "succeeded":
            continue
        candidate, row = _candidate_model_row(db, run, entry)
        scope = model_scope(
            db,
            attempt.id,
            request=SimpleNamespace(
                run_id=run.id,
                candidate_name=candidate.estimator_key,
            ),
            lock=True,
            expected_project=run.project_id,
            tracking_required=row.attempt_id == attempt.id,
        )
        if row.attempt_id == attempt.id:
            recorded_input = model_slot(db, row).get("input_digest")
            if recorded_input is not None and recorded_input != scope.source_plan.input_digest:
                raise StaleFence("Model custody input binding changed")
            if row.authority_digest != _authority(scope):
                raise StaleFence("Model custody source authority changed")
        else:
            reference = _authorized_model_reference(db, attempt, row, candidate.id)
            if reference["input_digest"] != scope.source_plan.input_digest:
                raise StaleFence("Predecessor model custody input binding changed")
        require_model_tracking(db, attempt, run, candidate, row, entry)
        require_training_model_completion(
            db,
            row.attempt_id,
            row.fencing_token,
            candidate.id,
            {
                "uri": entry.get("model_artifact_uri"),
                "sha256": entry.get("model_artifact_sha256"),
                "byte_size": row.byte_limit,
            },
        )


def training_model_allowlist(db, parent, run):
    """Carry only verified published model references into a replacement parent."""
    if not _declared_model_attempt(db, run, parent):
        return []
    result = []
    for entry in (run.tags or {}).get("leaderboard", []):
        if entry.get("status") != "succeeded":
            continue
        candidate, row = _candidate_model_row(db, run, entry)
        reference = (
            _model_reference(db, row, candidate.id)
            if row.attempt_id == parent.id
            else _authorized_model_reference(db, parent, row, candidate.id)
        )
        result.append(reference)
    return result


def require_model_tracking(db, attempt, run, candidate, row, entry):
    from automl_api.services.training_tracking import (
        requires_tracking_receipts,
        validate_tracking_reference,
    )

    if not requires_tracking_receipts(db, attempt, run):
        return
    slot = model_slot(db, row)
    if "tracking_receipt" not in slot or slot["tracking_receipt"]["attempt_id"] != str(
        row.attempt_id
    ):
        raise StaleFence(
            "Model has no original tracking custody; controlled qualification required"
        )
    recorded = validate_tracking_reference(
        db, run, slot["tracking_receipt"], f"candidate:{candidate.estimator_key}"
    )
    if entry.get("mlflow_run_id") != recorded["tracking_run_id"]:
        raise StaleFence("Candidate tracking ID differs from its model reservation")


def require_stored_tracking(db, attempt, run):
    from automl_api.services.training_tracking import requires_tracking_receipts

    if not requires_tracking_receipts(db, attempt, run):
        return
    for entry in (run.tags or {}).get("leaderboard", []):
        if entry.get("status") != "succeeded":
            continue
        candidate, row = _candidate_model_row(db, run, entry)
        if row.attempt_id != attempt.id:
            _authorized_model_reference(db, attempt, row, candidate.id)
        require_model_tracking(db, attempt, run, candidate, row, entry)
