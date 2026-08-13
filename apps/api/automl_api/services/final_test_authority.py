from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
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
    existing = db.scalar(
        select(FinalTestAllocation).where(FinalTestAllocation.split_digest == split_digest)
    )
    if existing is not None:
        _same_allocation(
            existing,
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
        existing = db.scalar(
            select(FinalTestAllocation).where(FinalTestAllocation.split_digest == split_digest)
        )
        if existing is None:
            raise
        _same_allocation(
            existing,
            scope_id=scope_id,
            canonical_provider=canonical_provider,
            provider_manifest_digest=provider_manifest_digest,
        )
        return existing


def _same_allocation(
    allocation: FinalTestAllocation,
    *,
    scope_id: uuid.UUID,
    canonical_provider: str,
    provider_manifest_digest: str,
) -> None:
    if (
        allocation.scope_id != scope_id
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
    signing_secret: str,
    expected_cas_version: int = 0,
) -> FinalTestAuthorityReceipt:
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    existing = _receipt(db, allocation.id, "open")
    if existing is not None:
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
        payload={"cas_version": allocation.cas_version, "opened_at": allocation.opened_at},
    )


def commit_result(
    db: Session,
    *,
    allocation_id: uuid.UUID,
    provider: str,
    provider_manifest_digest: str,
    request_digest: str,
    result_digest: str,
    signing_secret: str,
    expected_cas_version: int = 1,
) -> FinalTestAuthorityReceipt:
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    existing = _receipt(db, allocation.id, "commit")
    if existing is not None:
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
    signing_secret: str,
    expected_cas_version: int,
) -> FinalTestAuthorityReceipt:
    allocation = _locked_allocation(db, allocation_id)
    _authorize(allocation, provider, provider_manifest_digest)
    existing = _receipt(db, allocation.id, "fail")
    if existing is not None:
        _same_request(existing, request_digest)
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
        payload={"cas_version": allocation.cas_version, "reason": reason},
    )


def verify_receipt(receipt: FinalTestAuthorityReceipt, signing_secret: str) -> bool:
    payload = _receipt_bytes(
        allocation_id=receipt.allocation_id,
        operation=receipt.operation,
        provider=receipt.provider,
        request_digest=receipt.request_digest,
        payload=receipt.payload,
    )
    expected = hmac.new(signing_secret.encode(), payload, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, receipt.signature)


def _locked_allocation(db: Session, allocation_id: uuid.UUID) -> FinalTestAllocation:
    allocation = db.scalar(
        select(FinalTestAllocation).where(FinalTestAllocation.id == allocation_id).with_for_update()
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
    signing_secret: str,
    payload: dict[str, Any],
) -> FinalTestAuthorityReceipt:
    stored_payload = json.loads(json.dumps(payload, default=str))
    body = _receipt_bytes(
        allocation_id=allocation.id,
        operation=operation,
        provider=provider,
        request_digest=request_digest,
        payload=stored_payload,
    )
    signature = hmac.new(signing_secret.encode(), body, hashlib.sha256).hexdigest()
    receipt = FinalTestAuthorityReceipt(
        allocation_id=allocation.id,
        operation=operation,
        provider=provider,
        request_digest=request_digest,
        receipt_digest=hashlib.sha256(body).hexdigest(),
        signature_algorithm="hmac-sha256",
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
