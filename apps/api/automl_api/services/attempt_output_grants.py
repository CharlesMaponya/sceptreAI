"""One native output operation per durable, pod-bound champion grant."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.projects import Project
from automl_api.models.workflows import (
    PromotionalScope,
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
)
from automl_api.services.attempt_identity import _scope, admit_attempt, attempt_audience
from automl_api.services.attempt_object_grants import (
    ObjectGrantUnavailable,
    _authority,
    _digest,
    _live,
    _record_unknown,
    _validate_champion_paths,
    output_completion,
)
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.training_model_output import admit_model, model_scope, model_slot
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.attempt_outputs import (
    S3_SINGLE_PUT_BYTES,
    _verify_bytes,
    write_attempt_output,
)
from automl_api.storage.contracts import UploadCapabilityUnavailable, immutable_object_version
from automl_api.training.champion_evaluation import verify_result


class OutputGrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: uuid.UUID
    role: Literal["pipeline", "result"]
    byte_size: Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ModelGrantRequest(OutputGrantRequest):
    role: Literal["model"] = "model"
    run_id: uuid.UUID
    candidate_name: Annotated[
        str, Field(min_length=1, max_length=160, pattern=r"^[A-Za-z0-9_.-]+$")
    ]


def _request(value):
    model = isinstance(value, ModelGrantRequest) or (
        isinstance(value, dict) and value.get("role") == "model"
    )
    return (ModelGrantRequest if model else OutputGrantRequest).model_validate(value)


def _admission(factory, attempt_id, token, k8s, request):
    return (
        admit_model(factory, attempt_id, token, k8s, request)
        if request.role == "model"
        else admit_attempt(factory, attempt_id, token, k8s)
    )


def _intent_key(db, row):
    if row.role == "model":
        return f"training.model.output.{model_slot(db, row)['candidate_id']}"
    return "refit_output" if row.role == "pipeline" else "evaluation_output"


def _authorization(db, attempt_id, proof, request, store):
    if request.role == "model":
        scope = model_scope(
            db,
            attempt_id,
            request=request,
            lock=True,
            expected_project=uuid.UUID(proof["project_id"]),
        )
        if scope != proof["model_scope"] or request.byte_size > S3_SINGLE_PUT_BYTES:
            raise StaleFence("Training model authority changed or exceeds the output budget")
        plan = scope.source_plan
        uri = store.uri_for_key(
            f"projects/{plan.project_id}/runs/{plan.run_id}/attempts/{attempt_id}/"
            f"models/{plan.candidate_id}/{request.sha256}.joblib"
        )
        return scope, db.get(WorkflowAttempt, attempt_id), uri
    scope = _scope(db, attempt_id, lock=True, expected_project=uuid.UUID(proof["project_id"]))
    attempt = db.get(WorkflowAttempt, attempt_id)
    plan = scope.source_plan
    refit = attempt.stage == WorkflowStage.CHAMPION_REFIT
    if (
        plan is None
        or attempt.stage not in {WorkflowStage.CHAMPION_REFIT, WorkflowStage.CHAMPION_EVALUATION}
        or plan.credential_protocol != "pod-bound-object-v2"
        or scope.generation != proof["generation"]
        or attempt_audience(scope.id, scope.fencing_token) != proof["audience"]
        or attempt.status != AttemptStatus.RUNNING
        or not attempt.lease_owner
        or attempt.lease_expires_at is None
        or request.role != ("pipeline" if refit else "result")
        or request.byte_size > (plan.max_model_bytes if refit else 65536)
    ):
        raise StaleFence("Output requires the active pod-bound champion declaration")
    _validate_champion_paths(scope, store)
    stage, filename = ("refit", "pipeline.joblib") if refit else ("evaluation", "result.json")
    uri = store.uri_for_key(
        f"projects/{plan.project_id}/scopes/{plan.scope_id}/"
        f"{stage}/{plan.attempt_id}/{request.sha256}/{filename}"
    )
    return scope, attempt, uri


def _same(row, scope, attempt, uri, proof, request):
    if (
        row is None
        or row.grant_digest != _digest(row)
        or row.project_id != scope.project_id
        or row.attempt_id != attempt.id
        or row.stage != attempt.stage.value
        or row.fencing_token != scope.fencing_token
        or row.authority_digest != _authority(scope)
        or row.pod_uid != proof["pod_uid"]
        or row.request_id != request.request_id
        or row.method != "PUT"
        or row.role != request.role
        or row.object_uri != uri
        or row.object_version is not None
        or row.byte_limit != request.byte_size
        or row.content_sha256 != request.sha256
        or row.status == "revoked"
        or (scope.grant_deadline is not None and row.expires_at > scope.grant_deadline)
    ):
        raise StaleFence("Output grant or authority changed")
    _live(row, attempt, proof)


def _row(db, attempt_id, request_id):
    return db.scalar(
        select(WorkflowAttemptCredentialGrant)
        .where(
            WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
            WorkflowAttemptCredentialGrant.request_id == request_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _receipt(db, row):
    completed = output_completion(db, row)
    if completed is not None:
        intent = _event(db, row.attempt_id, _intent_key(db, row))
        expected = {
            "uri": row.object_uri,
            "sha256": row.content_sha256,
            "byte_size": row.byte_limit,
        }
        digest = (
            row.content_sha256
            if row.role in {"pipeline", "model"}
            else canonical_request_hash(expected)
        )
        if intent is None or intent.payload != expected or intent.result_digest != digest:
            raise StaleFence("Output intent changed before acknowledgement")
    model = model_slot(db, row) if row.role == "model" else None
    return {
        **(
            {
                key: value
                for key, value in model.items()
                if key in {"candidate_id", "candidate_name", "run_id"}
            }
            if model
            else {}
        ),
        **({"project_id": str(row.project_id), "attempt_id": str(row.attempt_id)} if model else {}),
        "grant_id": str(row.id),
        "grant_digest": row.grant_digest,
        "expires_at": row.expires_at.isoformat(),
        "uri": row.object_uri,
        "sha256": row.content_sha256,
        "byte_size": row.byte_limit,
        "object_version": completed["object_version"] if completed is not None else None,
    }


def require_output_completion(db, plan, fence, expected):
    """Publication cannot substitute a readable object for verified native custody."""
    if plan.credential_protocol != "pod-bound-object-v2":
        raise StaleFence("Worker publication requires the pod-bound output protocol")
    refit = hasattr(plan, "candidate")
    rows = db.scalars(
        select(WorkflowAttemptCredentialGrant).where(
            WorkflowAttemptCredentialGrant.project_id == plan.project_id,
            WorkflowAttemptCredentialGrant.attempt_id == plan.attempt_id,
            WorkflowAttemptCredentialGrant.fencing_token == fence,
            WorkflowAttemptCredentialGrant.stage
            == (
                WorkflowStage.CHAMPION_REFIT.value
                if refit
                else WorkflowStage.CHAMPION_EVALUATION.value
            ),
            WorkflowAttemptCredentialGrant.method == "PUT",
            WorkflowAttemptCredentialGrant.role == ("pipeline" if refit else "result"),
        )
    )
    for row in rows:
        completion = output_completion(db, row)
        if (
            completion is not None
            and {key: completion[key] for key in ("uri", "sha256", "byte_size")} == expected
        ):
            _receipt(db, row)
            return
    raise StaleFence("Output has no matching verified native completion")


def prepare_output_grant(factory, attempt_id, token, k8s, request, *, store_factory):
    """Reserve the exact output before streaming; another nonce cannot claim its role."""
    request = _request(request)
    proof = _admission(factory, attempt_id, token, k8s, request)
    store = store_factory()
    if store.driver_name not in {"aws_s3", "s3_compatible", "gcs", "azure_blob"}:
        raise UploadCapabilityUnavailable("Attempt output requires versioned native storage")
    if store.driver_name in {"aws_s3", "s3_compatible"} and request.byte_size > S3_SINGLE_PUT_BYTES:
        raise UploadCapabilityUnavailable("Larger S3 output requires tracked multipart custody")
    with factory() as db, db.begin():
        scope, attempt, uri = _authorization(db, attempt_id, proof, request, store)
        row = _row(db, attempt_id, request.request_id)
        if row is None:
            existing = db.scalar(
                select(WorkflowAttemptCredentialGrant.id).where(
                    WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
                    WorkflowAttemptCredentialGrant.method == "PUT",
                    WorkflowAttemptCredentialGrant.role == request.role,
                )
            )
            event_key = "refit_output" if request.role == "pipeline" else "evaluation_output"
            if request.role == "model":
                event_key = f"training.model.grant.{scope.source_plan.candidate_id}"
                existing = None
            if existing is not None or _event(db, attempt_id, event_key) is not None:
                raise StaleFence("This output already has a declaration or uncertain writer")
            now = datetime.now(UTC)
            row = WorkflowAttemptCredentialGrant(
                project_id=scope.project_id,
                attempt_id=attempt.id,
                stage=attempt.stage.value,
                fencing_token=scope.fencing_token,
                request_id=request.request_id,
                pod_uid=proof["pod_uid"],
                token_sha256=proof["token_sha256"],
                authority_digest=_authority(scope),
                method="PUT",
                role=request.role,
                object_uri=uri,
                object_version=None,
                byte_limit=request.byte_size,
                content_sha256=request.sha256,
                created_at=now,
                expires_at=min(
                    now + timedelta(seconds=60),
                    attempt.lease_expires_at,
                    datetime.fromtimestamp(proof["expires_at"], UTC),
                    scope.grant_deadline or now + timedelta(seconds=60),
                ),
            )
            if row.expires_at <= now:
                raise StaleFence("No output grant lifetime remains")
            row.grant_digest = _digest(row)
            db.add(row)
            db.flush()
            if request.role == "model":
                plan = scope.source_plan
                _append(
                    db,
                    attempt,
                    event_key,
                    {
                        "grant_id": str(row.id),
                        "candidate_id": str(plan.candidate_id),
                        "candidate_name": plan.candidate_name,
                        "run_id": str(plan.run_id),
                        "input_digest": plan.input_digest,
                        **(
                            {"tracking_receipt": plan.tracking_receipt}
                            if plan.tracking_receipt is not None
                            else {}
                        ),
                        **(
                            {"metadata_receipt": plan.metadata_receipt}
                            if plan.metadata_receipt is not None
                            else {}
                        ),
                    },
                )
        _same(row, scope, attempt, uri, proof, request)
        if row.status != "pending" and output_completion(db, row) is None:
            raise ObjectGrantUnavailable("The original output operation is unresolved")
        return _receipt(db, row)


def output_receipt(factory, attempt_id, token, k8s, request_id, *, store_factory):
    """Read-only acknowledgement recovery; never starts another native operation."""
    with factory() as db, db.begin():
        row = _row(db, attempt_id, request_id)
        if row is None:
            raise StaleFence("The output grant does not exist")
        data = dict(
            request_id=row.request_id,
            role=row.role,
            byte_size=row.byte_limit,
            sha256=row.content_sha256,
        )
        if row.role == "model":
            slot = model_slot(db, row)
            data.update(run_id=slot["run_id"], candidate_name=slot["candidate_name"])
        request = _request(data)
    proof = _admission(factory, attempt_id, token, k8s, request)
    store = store_factory()
    with factory() as db, db.begin():
        scope, attempt, uri = _authorization(db, attempt_id, proof, request, store)
        row = _row(db, attempt_id, request_id)
        _same(row, scope, attempt, uri, proof, request)
        if output_completion(db, row) is None:
            raise ObjectGrantUnavailable("The original output operation is unresolved")
        return _receipt(db, row)


def complete_output_grant(factory, attempt_id, token, k8s, request, source, *, store_factory):
    request = _request(request)
    proof = _admission(factory, attempt_id, token, k8s, request)
    store = store_factory()
    source.seek(0)
    _verify_bytes(source, request.byte_size, request.sha256)
    source.seek(0)
    with factory() as db, db.begin():
        scope, attempt, uri = _authorization(db, attempt_id, proof, request, store)
        row = _row(db, attempt_id, request.request_id)
        _same(row, scope, attempt, uri, proof, request)
        if output_completion(db, row) is not None:
            return _receipt(db, row)
        if row.status != "pending":
            raise ObjectGrantUnavailable("The original output operation is unresolved")
        if request.role == "result":
            verify_result(source.read(65537), scope.source_plan, expected_digest=request.sha256)
            source.seek(0)
        desired = {"uri": uri, "sha256": request.sha256, "byte_size": request.byte_size}
        key = _intent_key(db, row)
        if _event(db, attempt_id, key) is not None:
            raise StaleFence("Another output intent already exists")
        intent = _append(db, attempt, key, desired)
        if request.role in {"pipeline", "model"}:
            intent.result_digest = request.sha256
        row.status, row.issuance_owner = "issuing", uuid.uuid4()
        row.issuance_started_at = datetime.now(UTC)
        grant_id, owner, grant_digest = row.id, row.issuance_owner, row.grant_digest
        project_id, scope_id = scope.project_id, getattr(scope.source_plan, "scope_id", None)
    try:
        stored = write_attempt_output(
            store,
            uri,
            source,
            byte_size=request.byte_size,
            sha256=request.sha256,
            grant_digest=grant_digest,
        )
        version = immutable_object_version(
            stored.provider_headers.get(
                "generation" if store.driver_name == "gcs" else "version_id"
            )
        )
        if stored.uri != uri or stored.byte_size != request.byte_size or len(version) > 256:
            raise ValueError("Native output acknowledgement changed")
        # Native completion survives revocation; worker disclosure still requires fresh authority.
        with factory() as db, db.begin():
            db.get(Project, project_id, with_for_update={"read": True})
            if scope_id is not None:
                db.get(PromotionalScope, scope_id, with_for_update=True)
            attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True)
            row = db.get(WorkflowAttemptCredentialGrant, grant_id, with_for_update=True)
            if (
                row is None
                or row.issuance_owner != owner
                or row.grant_digest != grant_digest
                or row.grant_digest != _digest(row)
            ):
                raise StaleFence("Native completion lost its original issuance owner")
            completion = {
                "grant_id": str(row.id),
                "grant_digest": grant_digest,
                "issuance_owner": str(owner),
                "uri": uri,
                "sha256": request.sha256,
                "byte_size": request.byte_size,
                "object_version": version,
            }
            event = _append(db, attempt, f"attempt.output.completed.{grant_id}", completion)
            event.event_type = "attempt.output.completed"
            if row.status == "issuing":
                row.status, row.issued_at = "issued", datetime.now(UTC)
        return output_receipt(
            factory, attempt_id, token, k8s, request.request_id, store_factory=store_factory
        )
    except Exception:
        try:
            _record_unknown(factory, grant_id, owner)
        finally:
            raise ObjectGrantUnavailable(
                "Output acknowledgement unavailable; do not repeat the write"
            ) from None
