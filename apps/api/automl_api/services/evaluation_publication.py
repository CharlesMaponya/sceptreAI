"""Durable signed evaluation output and authority-backed scope publication."""

from __future__ import annotations

import hashlib
import io
from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy import func, select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import (
    PromotionalScope,
    WorkflowAttempt,
    WorkflowCheckpoint,
    WorkflowEvent,
)
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    StaleFence,
    canonical_request_hash,
    cas_register_terminal_artifact,
)
from automl_api.training.champion_evaluation import EvaluationPlan, verify_result
from automl_api.training.champion_refit import RefitObject, _copy_verified


def _event(db, attempt_id, key):
    return db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == key
        )
    )


def _append(db, attempt, key, payload):
    sequence = 1 + (
        db.scalar(
            select(func.max(WorkflowEvent.sequence)).where(WorkflowEvent.attempt_id == attempt.id)
        )
        or 0
    )
    event = WorkflowEvent(
        project_id=attempt.project_id,
        attempt_id=attempt.id,
        sequence=sequence,
        event_key=key,
        event_type=key,
        payload=payload,
        result_digest=canonical_request_hash(payload),
    )
    db.add(event)
    db.flush()
    return event


def _locked(
    db, plan, fence, *, writing=False, statuses=(AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED)
):
    scope = db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.id == plan.scope_id, PromotionalScope.project_id == plan.project_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == plan.attempt_id, WorkflowAttempt.project_id == plan.project_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        scope is None
        or attempt is None
        or attempt.scope_id != scope.id
        or attempt.stage != WorkflowStage.CHAMPION_EVALUATION
        or attempt.fencing_token != fence
        or scope.status not in {ScopeStatus.RUNNING, ScopeStatus.SUCCEEDED}
        or attempt.status not in statuses
    ):
        raise StaleFence("Evaluation publication no longer owns its scope and attempt")
    event = _event(db, attempt.id, "evaluation_plan")
    if (
        event is None
        or event.payload != plan.model_dump(mode="json")
        or event.result_digest != plan.digest
    ):
        raise StaleFence("Evaluation publication differs from its durable execution plan")
    frozen = db.scalar(
        select(WorkflowCheckpoint)
        .join(WorkflowAttempt, WorkflowCheckpoint.attempt_id == WorkflowAttempt.id)
        .where(
            WorkflowAttempt.scope_id == scope.id,
            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
            WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
        )
    )
    if frozen is None or (frozen.object_uri, frozen.content_digest) != (
        plan.frozen_pipeline.uri,
        plan.frozen_pipeline.sha256,
    ):
        raise StaleFence("Evaluation does not match the published frozen pipeline")
    if scope.canonical_provider != plan.final_manifest.provider:
        raise StaleFence("Evaluation provider differs from the sealed scope")
    if writing:
        now = datetime.now(UTC)
        if (
            scope.status != ScopeStatus.RUNNING
            or attempt.status != AttemptStatus.RUNNING
            or scope.scope_deadline_at is None
            or scope.scope_deadline_at <= now
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            raise StaleFence("Evaluation output requires an active lease and scope")
    return scope, attempt


def persist_evaluation_result(session_factory, plan: EvaluationPlan, fence, payload, store):
    """Commit immutable output intent before storage; permit only identical write replay."""
    digest = hashlib.sha256(payload).hexdigest()
    verify_result(payload, plan, expected_digest=digest)
    key = (
        f"projects/{plan.project_id}/scopes/{plan.scope_id}/evaluation/"
        f"{plan.attempt_id}/{digest}/result.json"
    )
    intent = {"uri": store.uri_for_key(key), "sha256": digest, "byte_size": len(payload)}
    with session_factory() as db, db.begin():
        _, attempt = _locked(db, plan, fence, writing=True)
        existing = _event(db, attempt.id, "evaluation_output")
        if existing is None:
            _append(db, attempt, "evaluation_output", intent)
        elif existing.payload != intent or existing.result_digest != canonical_request_hash(intent):
            raise IdempotencyConflict("Evaluation output was already declared differently")
    stored = store.put_bytes(key, payload)
    if stored.uri != intent["uri"]:
        raise ValueError("Evaluation output store returned a different URI")
    return intent


def _read_result(store, intent, plan):
    size = intent["byte_size"]
    if not isinstance(size, int) or not 0 < size <= 65536:
        raise ValueError("Evaluation output size is invalid")
    with io.BytesIO() as stream:
        _copy_verified(
            store,
            RefitObject(uri=intent["uri"], sha256=intent["sha256"], byte_size=size),
            stream,
            lambda: None,
        )
        verify_result(stream.getvalue(), plan, expected_digest=intent["sha256"])


def _verify_commit(receipt, plan, digest, public_key):
    try:
        record = SimpleNamespace(**receipt)
        valid = (
            record.signature_algorithm == "ed25519"
            and verify_receipt(record, public_key)
            and str(record.allocation_id) == str(plan.allocation_id)
            and record.operation == "commit"
            and record.provider == plan.final_manifest.provider
            and record.payload["result_digest"] == digest
            and record.payload["evaluator_attempt_id"] == str(plan.attempt_id)
            and record.payload["frozen_pipeline_digest"] == plan.frozen_pipeline.sha256
            and record.payload["cas_version"] == 2
        )
    except (KeyError, AttributeError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("Authority result receipt signature or lineage is invalid")


def recover_evaluation_result(session_factory, plan, fence, store, *, commit, authority_public_key):
    """Verify stored bytes, replay one authority commit, then CAS-complete the scope.

    `commit` submits only the declared digest to the allocation in this plan and
    returns its signed receipt. It must never acquire or renew final-data access.
    Expired worker leases do not prevent recovery of an already stored result;
    cancellation, supersession and failed scopes still reject publication.
    """
    with session_factory() as db, db.begin():
        _, attempt = _locked(db, plan, fence)
        event = _event(db, attempt.id, "evaluation_output")
        if event is None or event.result_digest != canonical_request_hash(event.payload):
            raise StaleFence("Evaluation has no valid durable output intent")
        intent = dict(event.payload)
    # No authority operation occurs until the result object passes independent verification.
    _read_result(store, intent, plan)
    receipt = commit(intent["sha256"])
    _verify_commit(receipt, plan, intent["sha256"], authority_public_key)
    with session_factory() as db, db.begin():
        scope, attempt = _locked(db, plan, fence)
        output = _event(db, attempt.id, "evaluation_output")
        if output is None or output.payload != intent:
            raise StaleFence("Evaluation output intent changed during commit")
        existing = _event(db, attempt.id, "evaluation_committed")
        publication = {"output": intent, "authority_receipt": receipt}
        if existing is not None:
            if existing.payload != publication or existing.result_digest != canonical_request_hash(
                publication
            ):
                raise IdempotencyConflict("Scope already committed a different evaluation")
            return intent
        if scope.status != ScopeStatus.RUNNING or attempt.status != AttemptStatus.RUNNING:
            raise StaleFence("Scope or evaluator is already terminal")
        if not cas_register_terminal_artifact(
            db,
            attempt_id=attempt.id,
            fencing_token=fence,
            expected_cas_version=attempt.terminal_cas_version,
            checkpoint_uri=intent["uri"],
        ):
            raise StaleFence("Evaluation terminal publication lost its fence")
        event = _append(db, attempt, "evaluation_committed", publication)
        db.add(
            WorkflowCheckpoint(
                project_id=plan.project_id,
                attempt_id=attempt.id,
                sequence=1,
                object_uri=intent["uri"],
                content_digest=intent["sha256"],
                event_sequence=event.sequence,
            )
        )
        scope.status = ScopeStatus.SUCCEEDED
        scope.cas_version += 1
    return intent
