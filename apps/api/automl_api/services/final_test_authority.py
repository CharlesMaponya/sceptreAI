from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from automl_api.models.enums import FinalTestStatus
from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt
from automl_api.services.workflow_state import IdempotencyConflict, InvalidTransition, StaleFence


class ProviderRejected(PermissionError):
    pass


def allocate(
    db: Session,
    *,
    split_digest: str,
    project_reference: str,
    scope_id: uuid.UUID,
    canonical_provider: str,
    provider_manifest_digest: str,
) -> FinalTestAllocation:
    lookup = select(FinalTestAllocation).where(
        or_(
            FinalTestAllocation.split_digest == split_digest,
            FinalTestAllocation.scope_id == scope_id,
        )
    )
    existing = db.scalar(lookup)
    if existing is not None:
        _same_allocation(
            existing,
            split_digest=split_digest,
            project_reference=project_reference,
            scope_id=scope_id,
            canonical_provider=canonical_provider,
            provider_manifest_digest=provider_manifest_digest,
        )
        return existing

    allocation = FinalTestAllocation(
        split_digest=split_digest,
        project_reference=project_reference,
        scope_id=scope_id,
        canonical_provider=canonical_provider,
        provider_manifest_digest=provider_manifest_digest,
    )
    savepoint = db.begin_nested()
    try:
        db.add(allocation)
        db.flush()
        savepoint.commit()
        return allocation
    except IntegrityError:
        savepoint.rollback()
        existing = db.scalar(lookup)
        if existing is None:
            raise
        _same_allocation(
            existing,
            split_digest=split_digest,
            project_reference=project_reference,
            scope_id=scope_id,
            canonical_provider=canonical_provider,
            provider_manifest_digest=provider_manifest_digest,
        )
        return existing


def _same_allocation(
    allocation: FinalTestAllocation,
    *,
    split_digest: str,
    project_reference: str,
    scope_id: uuid.UUID,
    canonical_provider: str,
    provider_manifest_digest: str,
) -> None:
    if (
        allocation.split_digest != split_digest
        or allocation.project_reference != project_reference
        or allocation.scope_id != scope_id
        or allocation.canonical_provider != canonical_provider
        or allocation.provider_manifest_digest != provider_manifest_digest
    ):
        raise IdempotencyConflict("The locked split already belongs to another allocation.")


def open_allocation(
    db: Session,
    *,
    allocation_id: uuid.UUID,
    provider: str,
    provider_manifest_digest: str,
    request_digest: str,
    signing_secret: str | Ed25519PrivateKey,
    expected_cas_version: int = 0,
    evaluator_attempt_id: uuid.UUID | None = None,
    frozen_pipeline_digest: str | None = None,
) -> FinalTestAuthorityReceipt:
    binding = _evaluation_binding(evaluator_attempt_id, frozen_pipeline_digest)
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    _require_registered_evaluator(db, allocation.id, binding)
    existing = _receipt(db, allocation.id, "open")
    if existing is not None:
        _same_evaluator(existing, binding)
        _same_request(existing, request_digest)
        return existing
    if allocation.status != FinalTestStatus.ALLOCATED:
        raise InvalidTransition(f"Cannot open an allocation in state {allocation.status}.")
    if allocation.cas_version != expected_cas_version:
        raise StaleFence("The final-test allocation version is stale.")

    allocation.status = FinalTestStatus.OPENED
    allocation.opened_at = datetime.now(UTC)
    allocation.cas_version += 1
    return _signed_receipt(
        db,
        allocation=allocation,
        operation="open",
        provider=provider,
        request_digest=request_digest,
        signing_secret=signing_secret,
        payload={
            "cas_version": allocation.cas_version,
            "opened_at": allocation.opened_at,
            **binding,
        },
    )


def commit_result(
    db: Session,
    *,
    allocation_id: uuid.UUID,
    provider: str,
    provider_manifest_digest: str,
    request_digest: str,
    result_digest: str,
    signing_secret: str | Ed25519PrivateKey,
    expected_cas_version: int = 1,
    evaluator_attempt_id: uuid.UUID | None = None,
    frozen_pipeline_digest: str | None = None,
) -> FinalTestAuthorityReceipt:
    binding = _evaluation_binding(evaluator_attempt_id, frozen_pipeline_digest)
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    _require_registered_evaluator(db, allocation.id, binding)
    opened = _receipt(db, allocation.id, "open")
    if opened is not None:
        _same_evaluator(opened, binding)
    elif binding:
        raise InvalidTransition("A bound evaluator must open before committing.")
    existing = _receipt(db, allocation.id, "commit")
    if existing is not None:
        _same_evaluator(existing, binding)
        _same_request(existing, request_digest)
        if allocation.result_digest != result_digest:
            raise IdempotencyConflict("The final-test result was already committed differently.")
        return existing
    if allocation.status != FinalTestStatus.OPENED:
        raise InvalidTransition(f"Cannot commit an allocation in state {allocation.status}.")
    if allocation.cas_version != expected_cas_version:
        raise StaleFence("The final-test allocation version is stale.")

    allocation.status = FinalTestStatus.COMMITTED
    allocation.result_digest = result_digest
    allocation.committed_at = datetime.now(UTC)
    allocation.cas_version += 1
    return _signed_receipt(
        db,
        allocation=allocation,
        operation="commit",
        provider=provider,
        request_digest=request_digest,
        signing_secret=signing_secret,
        payload={
            "cas_version": allocation.cas_version,
            "committed_at": allocation.committed_at,
            "result_digest": result_digest,
            **binding,
        },
    )


def fail_allocation(
    db: Session,
    *,
    allocation_id: uuid.UUID,
    provider: str,
    provider_manifest_digest: str,
    request_digest: str,
    reason: str,
    signing_secret: str | Ed25519PrivateKey,
    expected_cas_version: int,
    evaluator_attempt_id: uuid.UUID | None = None,
    frozen_pipeline_digest: str | None = None,
) -> FinalTestAuthorityReceipt:
    binding = _evaluation_binding(evaluator_attempt_id, frozen_pipeline_digest)
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    _require_registered_evaluator(db, allocation.id, binding)
    opened = _receipt(db, allocation.id, "open")
    if opened is not None:
        _same_evaluator(opened, binding)
    existing = _receipt(db, allocation.id, "fail")
    if existing is not None:
        _same_evaluator(existing, binding)
        _same_request(existing, request_digest)
        if allocation.terminal_reason != reason:
            raise IdempotencyConflict("The final-test failure was already recorded differently.")
        return existing
    if allocation.status not in {FinalTestStatus.ALLOCATED, FinalTestStatus.OPENED}:
        raise InvalidTransition(f"Cannot fail an allocation in state {allocation.status}.")
    if allocation.cas_version != expected_cas_version:
        raise StaleFence("The final-test allocation version is stale.")
    allocation.status = FinalTestStatus.FAILED
    allocation.terminal_reason = reason
    allocation.cas_version += 1
    return _signed_receipt(
        db,
        allocation=allocation,
        operation="fail",
        provider=provider,
        request_digest=request_digest,
        signing_secret=signing_secret,
        payload={"cas_version": allocation.cas_version, "reason": reason, **binding},
    )


def abort_unregistered(
    db,
    *,
    allocation_id,
    project_reference,
    scope_id,
    provider_manifest_digest,
    request_digest,
    reason,
    signing_secret,
):
    """Seal an abandoned handoff under the same lock used by registration/open."""
    allocation = _locked_allocation(db, allocation_id)
    if allocation.project_reference != project_reference or allocation.scope_id != scope_id:
        raise ProviderRejected("Allocation belongs to another project or scope")
    _authorize(allocation, allocation.canonical_provider, provider_manifest_digest)
    if (
        _latest_evaluator_registration(db, allocation_id) is not None
        or _receipt(db, allocation_id, "open") is not None
    ):
        raise InvalidTransition("Registered or opened allocations require evaluator recovery")
    existing = _receipt(db, allocation_id, "abort")
    if existing is not None:
        _same_request(existing, request_digest)
        return existing
    if allocation.status != FinalTestStatus.ALLOCATED or allocation.cas_version != 0:
        raise InvalidTransition("Only an unregistered allocation may be aborted")
    allocation.status = FinalTestStatus.FAILED
    allocation.terminal_reason = reason
    allocation.cas_version += 1
    return _signed_receipt(
        db,
        allocation=allocation,
        operation="abort",
        provider=allocation.canonical_provider,
        request_digest=request_digest,
        signing_secret=signing_secret,
        payload={
            "scope_id": str(scope_id),
            "provider_manifest_digest": provider_manifest_digest,
            "reason": reason,
            "cas_version": allocation.cas_version,
        },
    )


def _latest_evaluator_registration(db, allocation_id):
    # There are at most two pre-open evaluator generations, both append-only.
    return _receipt(db, allocation_id, "evaluator_2") or _receipt(db, allocation_id, "evaluator_1")


def _require_registered_evaluator(db, allocation_id, binding):
    registered = _latest_evaluator_registration(db, allocation_id)
    if registered is not None:
        _same_evaluator(registered, binding)
    elif _receipt(db, allocation_id, "refit_published") is not None:
        raise ProviderRejected("The frozen pipeline has no registered evaluator")


def _evaluation_binding(
    evaluator_attempt_id: uuid.UUID | None, frozen_pipeline_digest: str | None
) -> dict[str, str]:
    # Old internal/HMAC callers may replay unbound historical records. The HTTP
    # service always supplies both fields from its trusted identity configuration.
    if evaluator_attempt_id is None and frozen_pipeline_digest is None:
        return {}
    if (
        evaluator_attempt_id is None
        or frozen_pipeline_digest is None
        or re.fullmatch(r"[0-9a-f]{64}", frozen_pipeline_digest) is None
    ):
        raise ValueError("Evaluator binding requires an attempt UUID and SHA-256 pipeline digest.")
    return {
        "evaluator_attempt_id": str(uuid.UUID(str(evaluator_attempt_id))),
        "frozen_pipeline_digest": frozen_pipeline_digest,
    }


def _same_evaluator(receipt: FinalTestAuthorityReceipt, binding: dict[str, str]) -> None:
    if any(
        receipt.payload.get(key) != binding.get(key)
        for key in ("evaluator_attempt_id", "frozen_pipeline_digest")
    ):
        raise ProviderRejected("The allocation is bound to another evaluator or frozen pipeline.")


def verify_receipt(
    receipt: FinalTestAuthorityReceipt, verification_key: str | Ed25519PublicKey
) -> bool:
    payload = _receipt_bytes(
        allocation_id=receipt.allocation_id,
        operation=receipt.operation,
        provider=receipt.provider,
        request_digest=receipt.request_digest,
        payload=receipt.payload,
    )
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), receipt.receipt_digest):
        return False
    if receipt.signature_algorithm == "ed25519":
        try:
            key = (
                serialization.load_pem_public_key(verification_key.encode())
                if isinstance(verification_key, str)
                else verification_key
            )
            if not isinstance(key, Ed25519PublicKey):
                return False
            if (
                receipt.payload.get("signing_key_id")
                != hashlib.sha256(key.public_bytes_raw()).hexdigest()
            ):
                return False
            key.verify(bytes.fromhex(receipt.signature), payload)
            return True
        except (ValueError, InvalidSignature):
            return False
    # Historical HMAC receipts remain verifiable only inside their old trust domain.
    # Never reinterpret a public PEM key as an HMAC secret (algorithm confusion).
    if (
        receipt.signature_algorithm == "hmac-sha256"
        and isinstance(verification_key, str)
        and not verification_key.lstrip().startswith("-----BEGIN")
    ):
        expected = hmac.new(verification_key.encode(), payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, receipt.signature)
    return False


def _locked_allocation(db: Session, allocation_id: uuid.UUID) -> FinalTestAllocation:
    allocation = db.scalar(
        select(FinalTestAllocation)
        .where(FinalTestAllocation.id == allocation_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if allocation is None:
        raise LookupError("Final-test allocation was not found.")
    return allocation


def _authorize(
    allocation: FinalTestAllocation, provider: str, provider_manifest_digest: str
) -> None:
    if provider != allocation.canonical_provider:
        raise ProviderRejected("Only the canonical provider may access this locked split.")
    if provider_manifest_digest != allocation.provider_manifest_digest:
        raise ProviderRejected("The provider-distribution manifest does not match.")


def _receipt(
    db: Session, allocation_id: uuid.UUID, operation: str
) -> FinalTestAuthorityReceipt | None:
    return db.scalar(
        select(FinalTestAuthorityReceipt).where(
            FinalTestAuthorityReceipt.allocation_id == allocation_id,
            FinalTestAuthorityReceipt.operation == operation,
        )
    )


def _same_request(receipt: FinalTestAuthorityReceipt, request_digest: str) -> None:
    if receipt.request_digest != request_digest:
        raise IdempotencyConflict("This final-test operation was already requested differently.")


def _signed_receipt(
    db: Session,
    *,
    allocation: FinalTestAllocation,
    operation: str,
    provider: str,
    request_digest: str,
    signing_secret: str | Ed25519PrivateKey,
    payload: dict[str, Any],
) -> FinalTestAuthorityReceipt:
    stored_payload = json.loads(json.dumps(payload, default=str))
    if isinstance(signing_secret, Ed25519PrivateKey):
        stored_payload["signing_key_id"] = hashlib.sha256(
            signing_secret.public_key().public_bytes_raw()
        ).hexdigest()
    body = _receipt_bytes(
        allocation_id=allocation.id,
        operation=operation,
        provider=provider,
        request_digest=request_digest,
        payload=stored_payload,
    )
    if isinstance(signing_secret, Ed25519PrivateKey):
        signature = signing_secret.sign(body).hex()
        algorithm = "ed25519"
    else:
        signature = hmac.new(signing_secret.encode(), body, hashlib.sha256).hexdigest()
        algorithm = "hmac-sha256"
    receipt = FinalTestAuthorityReceipt(
        allocation_id=allocation.id,
        operation=operation,
        provider=provider,
        request_digest=request_digest,
        receipt_digest=hashlib.sha256(body).hexdigest(),
        signature_algorithm=algorithm,
        signature=signature,
        payload=stored_payload,
    )
    db.add(receipt)
    db.flush()
    return receipt


def _receipt_bytes(
    *,
    allocation_id: uuid.UUID,
    operation: str,
    provider: str,
    request_digest: str,
    payload: dict[str, Any],
) -> bytes:
    return json.dumps(
        {
            "allocation_id": str(allocation_id),
            "operation": operation,
            "provider": provider,
            "request_digest": request_digest,
            "payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
