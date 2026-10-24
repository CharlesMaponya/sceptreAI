"""Parent-only, exact-version reads of published input-bound model custody."""

import uuid
from dataclasses import dataclass, replace
from functools import partial

from sqlalchemy import select

from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowAttemptCredentialGrant
from automl_api.schemas.training_model_read import (
    TRAINING_MODEL_READ_PROTOCOL,
    TrainingModelReadRequest,
)
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.attempt_object_grants import (
    InputGrantRequest,
    _digest,
    attempt_credentials_revoked,
    issue_input_grant,
    output_completion,
)
from automl_api.services.parent_control import _publisher
from automl_api.services.training_model_output import (
    _authorized_model_reference,
    _candidate_model_row,
    _model_reference,
    model_scope,
    require_model_tracking,
)
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    StaleFence,
    canonical_request_hash,
)
from automl_api.storage.contracts import immutable_object_version
from automl_api.training.champion_refit import RefitObject, _within


class PublishedModel(RefitObject):
    version: str


@dataclass(frozen=True)
class ModelReadPlan:
    project_id: uuid.UUID
    run_id: uuid.UUID
    candidate: PublishedModel


def _authorization(db, attempt_id, proof, request, *, declaration):
    scope = model_scope(
        db,
        attempt_id,
        request=declaration,
        lock=True,
        expected_project=uuid.UUID(proof["project_id"]),
        tracking_required=False,
    )
    attempt, run = db.get(WorkflowAttempt, attempt_id), db.get(ModelRun, declaration.run_id)
    publisher = _publisher(db, attempt, run)
    entries = list(
        db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
                OutboxEntry.payload["attempt_id"].astext == str(attempt.id),
            )
        )
    )
    if (
        attempt_credentials_revoked(db, attempt_id)
        or proof["generation"] != scope.generation
        or proof["audience"] != attempt_audience(scope.id, scope.fencing_token)
        or publisher is None
        or publisher.payload["pod_uid"] != proof["pod_uid"]
        or len(entries) != 1
        or entries[0].payload.get("credential_protocol") != RAY_CREDENTIAL_PROTOCOL
    ):
        raise StaleFence("Model reads require the current declared parent publisher")
    published = [
        entry
        for entry in (run.tags or {}).get("leaderboard", [])
        if entry.get("model") == declaration.candidate_name
    ]
    if len(published) != 1 or published[0].get("status") != "succeeded":
        raise StaleFence("Model reads require a published successful candidate")
    entry = published[0]
    candidate, row = _candidate_model_row(db, run, entry)
    reference = (
        _model_reference(db, row, candidate.id)
        if row.attempt_id == attempt.id
        else _authorized_model_reference(db, attempt, row, candidate.id)
    )
    if reference["input_digest"] != scope.source_plan.input_digest or (
        row.attempt_id == attempt.id and row.fencing_token != attempt.fencing_token
    ):
        raise StaleFence("Published model input authority changed")
    require_model_tracking(db, attempt, run, candidate, row, entry)
    completion = output_completion(db, row)
    item = PublishedModel(
        uri=completion["uri"],
        sha256=completion["sha256"],
        byte_size=completion["byte_size"],
        version=immutable_object_version(completion["object_version"]),
    )
    return (
        replace(
            scope,
            binding_digest=canonical_request_hash(
                {
                    "source": scope.binding_digest,
                    "custody": reference,
                    "completion": completion,
                }
            ),
            source_plan=ModelReadPlan(scope.project_id, run.id, item),
        ),
        attempt,
        item,
    )


def _validate_paths(scope, store):
    plan = scope.source_plan
    if not _within(
        plan.candidate.uri,
        store.uri_for_key(f"projects/{plan.project_id}/runs/{plan.run_id}/"),
    ):
        raise StaleFence("Published model is outside its approved training run")


def issue_model_read(factory, attempt_id, token, k8s, request, *, store_factory):
    request = TrainingModelReadRequest.model_validate(request)
    reply = issue_input_grant(
        factory,
        attempt_id,
        token,
        k8s,
        InputGrantRequest(request_id=request.request_id, role="checkpoint", object_index=0),
        store_factory=store_factory,
        authorize=partial(_authorization, declaration=request),
        validate_paths=_validate_paths,
    )
    with factory() as db:
        row = db.get(WorkflowAttemptCredentialGrant, uuid.UUID(reply["grant_id"]))
        if (
            row is None
            or row.grant_digest != reply["grant_digest"]
            or row.grant_digest != _digest(row)
        ):
            raise StaleFence("Model read acknowledgement changed")
        return {
            **reply,
            "protocol": TRAINING_MODEL_READ_PROTOCOL,
            "project_id": str(row.project_id),
            "attempt_id": str(attempt_id),
            "run_id": str(request.run_id),
            "candidate_name": request.candidate_name,
            "request_id": str(request.request_id),
            "request_digest": canonical_request_hash(request.model_dump(mode="json")),
            "audience": attempt_audience(attempt_id, row.fencing_token),
            "uri": row.object_uri,
        }
