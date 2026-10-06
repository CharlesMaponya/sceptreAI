"""Durable refit publication and scoped evaluator identities for the authority."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import jwt

from automl_api.models.enums import FinalTestStatus
from automl_api.services import final_test_authority as authority
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    InvalidTransition,
    canonical_request_hash,
)

ISSUER = "sceptre-qualification-control"
AUDIENCE = "sceptre-final-evaluator"


def publish_refit(db, *, allocation_id, project_reference, payload, signing_key):
    """The allocator attests a provider-side frozen-pipeline CAS, never an evaluator."""
    allocation = authority._locked_allocation(db, allocation_id)
    if allocation.project_reference != project_reference:
        raise authority.ProviderRejected("Allocation belongs to another project")
    request_digest = canonical_request_hash(payload)
    existing = authority._receipt(db, allocation_id, "refit_published")
    if existing is not None:
        authority._same_request(existing, request_digest)
        return existing
    if allocation.status != FinalTestStatus.ALLOCATED:
        raise InvalidTransition("Refit must be published before final data opens")
    return authority._signed_receipt(
        db,
        allocation=allocation,
        operation="refit_published",
        provider=allocation.canonical_provider,
        request_digest=request_digest,
        signing_secret=signing_key,
        payload=payload,
    )


def register_evaluator(
    db,
    *,
    allocation_id,
    project_reference,
    evaluator_attempt_id,
    expected_generation,
    identity_expires_at,
    signing_key,
    scope_deadline_at=None,
):
    if scope_deadline_at is not None and scope_deadline_at.tzinfo is None:
        raise authority.ProviderRejected("Evaluator scope deadline must be timezone aware")
    allocation = authority._locked_allocation(db, allocation_id)
    if allocation.project_reference != project_reference:
        raise authority.ProviderRejected("Allocation belongs to another project")
    refit = authority._receipt(db, allocation_id, "refit_published")
    if refit is None:
        raise InvalidTransition("A frozen pipeline must be published before evaluator registration")
    existing = authority._latest_evaluator_registration(db, allocation_id)
    attempt_id = str(evaluator_attempt_id)
    current_generation = int(existing.payload["generation"]) if existing else 0
    if existing and existing.payload["evaluator_attempt_id"] == attempt_id:
        if expected_generation != current_generation - 1:
            raise IdempotencyConflict("Evaluator replay changed its generation")
        if scope_deadline_at is not None and existing.payload["exp"] > int(
            scope_deadline_at.timestamp()
        ):
            raise IdempotencyConflict("Evaluator replay shortened its registered deadline")
        return jwt.encode(dict(sorted(existing.payload.items())), signing_key, algorithm="EdDSA")
    if allocation.status != FinalTestStatus.ALLOCATED:
        raise InvalidTransition("An opened final split cannot receive a replacement evaluator")
    if current_generation != expected_generation or current_generation >= 2:
        raise InvalidTransition("Evaluator generation is stale or its retry budget is exhausted")
    # Never recycle an old attempt ID after a pre-open replacement.
    first = authority._receipt(db, allocation_id, "evaluator_1")
    if first and first.payload["evaluator_attempt_id"] == attempt_id:
        raise InvalidTransition("A superseded evaluator cannot be registered again")
    now = datetime.now(UTC)
    expires_at = min(identity_expires_at, now + timedelta(hours=2))
    if scope_deadline_at is not None:
        expires_at = min(expires_at, scope_deadline_at)
    if int(expires_at.timestamp()) <= int(now.timestamp()):
        raise authority.ProviderRejected("Allocator identity expired before evaluator registration")
    claims = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": attempt_id,
        "iat": int(now.timestamp()),
        "exp": int(expires_at.timestamp()),
        "allocation_id": str(allocation.id),
        "scope_id": str(allocation.scope_id),
        "project_reference": allocation.project_reference,
        "provider": allocation.canonical_provider,
        "evaluator_attempt_id": attempt_id,
        "frozen_pipeline_digest": refit.payload["frozen_pipeline_digest"],
        "generation": current_generation + 1,
    }
    receipt = authority._signed_receipt(
        db,
        allocation=allocation,
        operation=f"evaluator_{current_generation + 1}",
        provider=allocation.canonical_provider,
        request_digest=canonical_request_hash(
            {
                "evaluator_attempt_id": attempt_id,
                "expected_generation": expected_generation,
            }
        ),
        signing_secret=signing_key,
        payload=claims,
    )
    return jwt.encode(dict(sorted(receipt.payload.items())), signing_key, algorithm="EdDSA")


def authenticate_evaluator(db, token, public_key):
    claims = jwt.decode(
        token,
        public_key,
        algorithms=["EdDSA"],
        audience=AUDIENCE,
        issuer=ISSUER,
        options={"require": ["exp", "iat", "sub", "allocation_id", "generation"]},
    )
    allocation_id = uuid.UUID(claims["allocation_id"])
    registered = authority._latest_evaluator_registration(db, allocation_id)
    if registered is None or registered.payload != claims:
        raise jwt.InvalidTokenError("Evaluator identity is missing or superseded")
    return claims
