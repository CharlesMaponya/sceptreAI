"""Durable, principal-bound object capability issuance; never persist signed URLs."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services.attempt_identity import _scope, admit_attempt, attempt_audience
from automl_api.services.refit_access import mint_refit_read_url
from automl_api.services.workflow_state import StaleFence, canonical_request_hash


class InputGrantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: uuid.UUID
    role: Literal["train", "validation"]
    object_index: Annotated[int, Field(ge=0, lt=10000, strict=True)]


class ObjectGrantUnavailable(RuntimeError):
    pass


INTENT_FIELDS = (
    "project_id",
    "attempt_id",
    "stage",
    "fencing_token",
    "request_id",
    "pod_uid",
    "token_sha256",
    "authority_digest",
    "method",
    "role",
    "object_uri",
    "object_version",
    "byte_limit",
    "content_sha256",
    "expires_at",
)


def _digest(row):
    return canonical_request_hash({key: getattr(row, key) for key in INTENT_FIELDS})


def _authority(scope):
    return canonical_request_hash(
        {
            "project_id": scope.project_id,
            "attempt_id": scope.id,
            "generation": scope.generation,
            "fence": scope.fencing_token,
            "workload_identity": scope.workload_identity,
            "namespace": scope.namespace,
            "ray_job_name": scope.ray_job_name,
            "source": scope.binding_digest,
        }
    )


def _authorization(db, attempt_id, proof, request):
    scope = _scope(db, attempt_id, lock=True, expected_project=uuid.UUID(proof["project_id"]))
    attempt = db.get(WorkflowAttempt, attempt_id)
    revoked = db.scalar(
        select(WorkflowEvent.id).where(
            WorkflowEvent.attempt_id == attempt_id,
            WorkflowEvent.event_key == "attempt.credentials.revoked",
        )
    )
    if (
        revoked is not None
        or scope.project_id != uuid.UUID(proof["project_id"])
        or scope.generation != proof["generation"]
        or attempt_audience(scope.id, scope.fencing_token) != proof["audience"]
        or attempt.status != AttemptStatus.RUNNING
        or not attempt.lease_owner
        or attempt.lease_expires_at is None
        or scope.source_plan is None
        or attempt.stage not in {WorkflowStage.PREPARATION, WorkflowStage.TRAINING_RUN}
    ):
        raise StaleFence("The attempt has no live approved safe-object declaration")
    objects = [o for o in scope.source_plan.manifest.safe_data.objects if o.role == request.role]
    if request.object_index >= len(objects):
        raise StaleFence("The approved object does not exist")
    return scope, attempt, objects[request.object_index]


def _same_intent(row, scope, attempt, item, proof, request):
    if row is None:
        raise StaleFence("Object grant intent is missing")
    if (
        row.grant_digest != _digest(row)
        or row.project_id != scope.project_id
        or row.attempt_id != scope.id
        or row.stage != attempt.stage.value
        or row.fencing_token != scope.fencing_token
        or row.authority_digest != _authority(scope)
        or row.pod_uid != proof["pod_uid"]
        or row.request_id != request.request_id
        or row.method != "GET"
        or row.role != request.role
        or row.object_uri != item.uri
        or row.object_version != item.version
        or row.content_sha256 != item.sha256
        or row.byte_limit != item.byte_size
    ):
        raise StaleFence("Object grant request or authority changed")


def _live(row, attempt, proof):
    now = datetime.now(UTC)
    if (
        row.expires_at <= now
        or attempt.lease_expires_at < row.expires_at
        or datetime.fromtimestamp(proof["expires_at"], UTC) < row.expires_at
    ):
        raise StaleFence("The original object grant deadline is no longer authorized")


def _receipt(row, url=None):
    return dict(
        grant_id=str(row.id),
        grant_digest=row.grant_digest,
        expires_at=row.expires_at.isoformat(),
        byte_limit=row.byte_limit,
        sha256=row.content_sha256,
        url=url,
    )


def _record_unknown(factory, grant_id, owner):
    with factory() as db, db.begin():
        row = db.scalar(
            select(WorkflowAttemptCredentialGrant)
            .where(WorkflowAttemptCredentialGrant.id == grant_id)
            .with_for_update()
        )
        if row is not None and row.status == "issuing" and row.issuance_owner == owner:
            row.status = "expired" if row.expires_at <= datetime.now(UTC) else "unknown"
            row.failure_reason = "Capability issuance or acknowledgement is unknown"


def issue_input_grant(factory, attempt_id, token, k8s, request, *, store_factory):
    """One provider mint per nonce; replay returns a receipt, never a stored URL.

    A caller needing another delivery requests a fresh read intent under current
    authority. Existing native URLs retain their original deadline. Revocation
    alone is not physical retirement; terminal/replacement barriers must await it.
    """
    request = InputGrantRequest.model_validate(request)
    proof = admit_attempt(factory, attempt_id, token, k8s)
    owner = uuid.uuid4()
    with factory() as db, db.begin():
        scope, attempt, item = _authorization(db, attempt_id, proof, request)
        existing = db.scalar(
            select(WorkflowAttemptCredentialGrant)
            .where(
                WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
                WorkflowAttemptCredentialGrant.request_id == request.request_id,
            )
            .with_for_update()
        )
        if existing is not None:
            _same_intent(existing, scope, attempt, item, proof, request)
            _live(existing, attempt, proof)
            if existing.status == "issued":
                return _receipt(existing)
            if existing.status != "pending":
                raise StaleFence("The original object grant cannot be issued again")
            grant_id = existing.id
        else:
            now = datetime.now(UTC)
            row = WorkflowAttemptCredentialGrant(
                project_id=scope.project_id,
                attempt_id=scope.id,
                stage=attempt.stage.value,
                fencing_token=scope.fencing_token,
                request_id=request.request_id,
                pod_uid=proof["pod_uid"],
                token_sha256=proof["token_sha256"],
                authority_digest=_authority(scope),
                method="GET",
                role=request.role,
                object_uri=item.uri,
                object_version=item.version,
                byte_limit=item.byte_size,
                content_sha256=item.sha256,
                expires_at=min(
                    now + timedelta(seconds=60),
                    attempt.lease_expires_at,
                    datetime.fromtimestamp(proof["expires_at"], UTC),
                ),
                created_at=now,
            )
            if row.expires_at <= now:
                raise StaleFence("No object grant lifetime remains")
            row.grant_digest = _digest(row)
            db.add(row)
            db.flush()
            grant_id = row.id
    with factory() as db, db.begin():
        scope, attempt, current_item = _authorization(db, attempt_id, proof, request)
        row = db.get(WorkflowAttemptCredentialGrant, grant_id, with_for_update=True)
        _same_intent(row, scope, attempt, current_item, proof, request)
        _live(row, attempt, proof)
        if row.status != "pending":
            raise StaleFence("The object grant has already claimed issuance")
        row.status, row.issuance_owner = "issuing", owner
        row.issuance_started_at = datetime.now(UTC)
        deadline = row.expires_at
    try:
        from automl_api.services.provider_import import reject_owned_source_namespace

        store = store_factory()
        reject_owned_source_namespace(store, scope.source_plan.manifest)
        grant = mint_refit_read_url(store, item, deadline, object_version=item.version)
        if datetime.fromisoformat(grant["expires_at"]) != deadline:
            raise ValueError("Provider grant deadline changed")
        fresh = admit_attempt(factory, attempt_id, token, k8s)
        with factory() as db, db.begin():
            scope, attempt, item = _authorization(db, attempt_id, fresh, request)
            row = db.get(WorkflowAttemptCredentialGrant, grant_id, with_for_update=True)
            _same_intent(row, scope, attempt, item, fresh, request)
            _live(row, attempt, fresh)
            if row.status != "issuing" or row.issuance_owner != owner:
                raise StaleFence("Object capability publication lost ownership")
            row.status, row.issued_at = "issued", datetime.now(UTC)
            reply = _receipt(row, grant["url"])
        return reply
    except Exception:
        try:
            _record_unknown(factory, grant_id, owner)
        finally:
            raise ObjectGrantUnavailable("Object capability unavailable") from None


def revoke_attempt_grants(db, attempt_id):
    """Stop future disclosure; native capabilities still require deadline retirement."""
    attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True)
    if attempt is None:
        raise StaleFence("The revoked attempt does not exist")
    db.flush()
    existing = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt_id,
            WorkflowEvent.event_key == "attempt.credentials.revoked",
        )
    )
    if existing is None:
        payload = {
            "attempt_id": str(attempt.id),
            "project_id": str(attempt.project_id),
            "fencing_token_sha256": canonical_request_hash({"fence": attempt.fencing_token}),
        }
        sequence = 1 + (
            db.scalar(
                select(func.max(WorkflowEvent.sequence)).where(
                    WorkflowEvent.attempt_id == attempt_id,
                )
            )
            or 0
        )
        db.add(
            WorkflowEvent(
                project_id=attempt.project_id,
                attempt_id=attempt_id,
                sequence=sequence,
                event_key="attempt.credentials.revoked",
                event_type="attempt.credentials.revoked",
                payload=payload,
                result_digest=canonical_request_hash(payload),
            )
        )
    rows = db.scalars(
        select(WorkflowAttemptCredentialGrant)
        .where(
            WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
            WorkflowAttemptCredentialGrant.status.not_in(("revoked", "expired")),
        )
        .with_for_update()
    ).all()
    now = datetime.now(UTC)
    for row in rows:
        row.status, row.revoked_at = "revoked", now
    return len(rows)


def unretired_grant_deadline(db, attempt_id):
    """Logical grant status cannot shorten a capability's original native deadline."""
    return db.scalar(
        select(func.max(WorkflowAttemptCredentialGrant.expires_at)).where(
            WorkflowAttemptCredentialGrant.attempt_id == attempt_id,
            WorkflowAttemptCredentialGrant.issuance_started_at.is_not(None),
            WorkflowAttemptCredentialGrant.expires_at > datetime.now(UTC),
        )
    )
