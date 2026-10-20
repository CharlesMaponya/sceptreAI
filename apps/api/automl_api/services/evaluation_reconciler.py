"""Reconcile separate evaluator workloads against durable plans and authority state."""

import json
import logging
import os
import ssl
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
from sqlalchemy import or_, select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.services.attempt_object_grants import (
    revoke_attempt_grants,
    unretired_grant_deadline,
)
from automl_api.services.champion_planning import ACTIVE_ATTEMPTS
from automl_api.services.champion_workloads import delete_owned_resource
from automl_api.services.evaluation_authority import _record
from automl_api.services.evaluation_jobs import LABEL, _url, submit_evaluator
from automl_api.services.evaluation_keys import prepare_evaluator_credentials
from automl_api.services.evaluation_publication import (
    _event,
    _read_result,
    recover_evaluation_result,
)
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.refit_jobs import _cleanup
from automl_api.services.workflow_state import (
    InvalidTransition,
    canonical_request_hash,
    transition_attempt,
)
from automl_api.storage.object_store import get_object_store
from automl_api.training.champion_evaluation import EvaluationPlan

LOGGER = logging.getLogger(__name__)
_next_sweep = 0.0


@contextmanager
def configured_authority(project_id, project_reference):
    """Controller-only project token map; never copied to an evaluator Pod."""
    try:
        bindings = json.loads(Path(os.environ["EVALUATION_ALLOCATOR_TOKENS_FILE"]).read_text())
        binding = bindings[str(uuid.UUID(str(project_id)))]
        if (
            not isinstance(binding, dict)
            or set(binding) != {"project_reference", "token"}
            or not isinstance(project_reference, str)
            or not project_reference.strip()
            or binding["project_reference"] != project_reference
        ):
            raise ValueError
        token = binding["token"]
        if (
            not isinstance(token, str)
            or not token
            or any(ord(c) <= 32 or ord(c) >= 127 for c in token)
        ):
            raise ValueError
    except (KeyError, ValueError, TypeError, OSError):
        raise ValueError("Missing or invalid project-bound allocator identity") from None
    with httpx.Client(
        base_url=_url(os.environ["EVALUATION_AUTHORITY_URL"]) + "/",
        headers={"Authorization": f"Bearer {token}"},
        verify=ssl.create_default_context(cafile=os.getenv("EVALUATION_CA_FILE")),
        follow_redirects=False,
        timeout=httpx.Timeout(30, connect=10),
    ) as client:
        yield client


def _scope(db, scope_id):
    return db.scalar(
        select(PromotionalScope)
        .where(
            PromotionalScope.id == scope_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _attempts(db, scope_id):
    return list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.scope_id == scope_id,
                WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
            )
            .order_by(WorkflowAttempt.generation)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )


def _stop(attempt, reason):
    if attempt.status in ACTIVE_ATTEMPTS:
        transition_attempt(
            attempt,
            AttemptStatus.CANCELLED
            if attempt.status == AttemptStatus.PENDING
            else AttemptStatus.FAILED,
            fencing_token=attempt.fencing_token,
            terminal_reason=reason,
        )


def _cleanup_attempt(factory, k8s, attempt_id):
    with factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        if attempt is None or attempt.status in ACTIVE_ATTEMPTS:
            return False
        if attempt.stage != WorkflowStage.CHAMPION_EVALUATION or attempt.scope_id is None:
            return False
        revoke_attempt_grants(db, attempt_id)
        retiring = unretired_grant_deadline(db, attempt_id) is not None
        event = _event(db, attempt_id, "evaluation_submission")
        if event is not None and event.result_digest != canonical_request_hash(event.payload):
            raise InvalidTransition("Evaluator cleanup submission changed")
        scope_id, generation = attempt.scope_id, attempt.generation
        labels = {
            LABEL: str(attempt_id),
            "automl.platform/evaluation-scope": str(scope_id),
            "automl.platform/project-id": str(attempt.project_id),
        }
        key_name = f"evaluation-key-{scope_id.hex}-{generation}"
        key_label = "evaluation_key_ready" + ("" if generation == 1 else f"_{generation}")
        key_events = list(
            db.scalars(
                select(WorkflowEvent)
                .join(WorkflowAttempt)
                .where(
                    WorkflowAttempt.scope_id == scope_id,
                    WorkflowAttempt.project_id == attempt.project_id,
                    WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                    WorkflowEvent.event_key == key_label,
                )
            )
        )
        key_event = key_events[0] if len(key_events) == 1 else None
        key = key_event.payload if key_event is not None else None
        key_digest = key_event.result_digest if key_event is not None else None
        namespace = k8s.settings.training_namespace
        if event is None:
            if (
                key_event is None
                or not isinstance(key_event.payload, dict)
                or key_event.result_digest != canonical_request_hash(key_event.payload)
                or not isinstance(key_event.payload.get("namespace"), str)
                or not key_event.payload["namespace"]
            ):
                return False
            namespace = key_event.payload["namespace"]
        desired = (
            event.payload
            if event
            else {
                "job": {
                    "metadata": {
                        "name": f"evaluation-{attempt_id.hex}",
                        "namespace": namespace,
                        "labels": labels,
                    }
                }
            }
        )
    if not _cleanup(k8s, desired, labels=labels, retiring=retiring):
        return False
    if key_event is None:
        return False
    key_labels = {
        "automl.platform/evaluation-scope": str(scope_id),
        "automl.platform/project-id": labels["automl.platform/project-id"],
        "automl.platform/evaluation-generation": str(generation),
    }
    if (
        not isinstance(key, dict)
        or key_digest != canonical_request_hash(key)
        or key.get("name") != key_name
        or key.get("algorithm") != "Ed25519"
        or not isinstance(key.get("namespace"), str)
        or not key["namespace"]
        or key.get("labels") != key_labels
    ):
        raise InvalidTransition("Evaluator cleanup key custody changed")
    if not delete_owned_resource(
        k8s.core.read_namespaced_secret,
        k8s.core.delete_namespaced_secret,
        {"name": key_name, "namespace": key["namespace"], "labels": key_labels},
    ):
        return False
    with factory() as db, db.begin():
        _scope(db, scope_id)
        attempt = db.get(WorkflowAttempt, attempt_id)
        if attempt is not None and attempt.status not in ACTIVE_ATTEMPTS:
            attempt.cleanup_state = "complete"
    return True


def _authority_state(authority, plan, public_key):
    response = authority.get(f"allocations/{plan.allocation_id}")
    response.raise_for_status()
    state = response.json()
    expected = dict(
        allocation_id=str(plan.allocation_id),
        scope_id=str(plan.scope_id),
        project_reference=plan.final_manifest.project_reference,
        split_digest=plan.final_manifest.split_digest,
        provider_manifest_digest=plan.final_manifest.digest,
        canonical_provider=plan.final_manifest.provider,
    )
    if any(state.get(key) != value for key, value in expected.items()):
        raise InvalidTransition("Recovery authority allocation differs from the registered plan")
    for receipt in state["receipts"]:
        if (
            receipt.get("signature_algorithm") != "ed25519"
            or str(receipt.get("allocation_id")) != str(plan.allocation_id)
            or not verify_receipt(SimpleNamespace(**receipt), public_key)
        ):
            raise InvalidTransition("Recovery authority receipt is invalid")
    return state


def _binding(plan, generation):
    return dict(
        provider_manifest_digest=plan.final_manifest.digest,
        evaluator_attempt_id=str(plan.attempt_id),
        frozen_pipeline_digest=plan.frozen_pipeline.sha256,
        generation=generation,
    )


def _seal_failed(factory, authority, scope_id, state, plans, public_key):
    registered = next(
        (
            r
            for r in reversed(state["receipts"])
            if r["operation"] in {"evaluator_1", "evaluator_2"}
        ),
        None,
    )
    if registered is None:
        receipt = _abort_registration(
            authority,
            state["allocation_id"],
            scope_id,
            state["provider_manifest_digest"],
            public_key,
        )
        _finish_failure(factory, scope_id, receipt)
        return
    claims = registered["payload"]
    plan = next((p for p in plans if str(p.attempt_id) == claims["evaluator_attempt_id"]), None)
    if plan is None or plan.frozen_pipeline.sha256 != claims["frozen_pipeline_digest"]:
        raise InvalidTransition("Authority evaluator has no matching local plan")
    if state["status"] == "failed":
        receipt = next(r for r in state["receipts"] if r["operation"] == "fail")
    else:
        response = authority.post(
            f"allocations/{plan.allocation_id}/recovery/fail",
            json={
                **_binding(plan, claims["generation"]),
                "expected_cas_version": state["cas_version"],
                "reason": "Evaluator terminated without a recoverable result",
            },
        )
        response.raise_for_status()
        receipt = response.json()
    if (
        receipt.get("operation") != "fail"
        or receipt.get("signature_algorithm") != "ed25519"
        or not verify_receipt(SimpleNamespace(**receipt), public_key)
        or receipt.get("allocation_id") != str(plan.allocation_id)
        or receipt["payload"].get("evaluator_attempt_id") != str(plan.attempt_id)
        or receipt["payload"].get("frozen_pipeline_digest") != plan.frozen_pipeline.sha256
    ):
        raise InvalidTransition("Authority failure receipt is invalid")
    _finish_failure(factory, scope_id, receipt)


def _finish_failure(factory, scope_id, receipt):
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        if scope is None or scope.status == ScopeStatus.SUCCEEDED:
            raise InvalidTransition("Cannot fail a completed or deleted scope")
        attempts = _attempts(db, scope_id)
        for attempt in attempts:
            _stop(attempt, "Authority sealed evaluator failure")
            _record(db, attempt, "evaluation_failed", receipt)
        if scope.status == ScopeStatus.RUNNING:
            scope.status = ScopeStatus.FAILED
            scope.cas_version += 1


def _abort_registration(authority, allocation_id, scope_id, manifest_digest, public_key):
    body = dict(
        scope_id=str(scope_id),
        provider_manifest_digest=manifest_digest,
        reason="Evaluator registration did not finish before scope stopped",
    )
    response = authority.post(f"allocations/{allocation_id}/recovery/abort", json=body)
    if response.status_code == 409:
        # An operator may already have sealed this allocation with a different
        # reason. Recover that immutable receipt instead of requesting a rewrite.
        snapshot = authority.get(f"allocations/{allocation_id}")
        snapshot.raise_for_status()
        state = snapshot.json()
        if any(
            state.get(k) != v
            for k, v in {
                "allocation_id": str(allocation_id),
                "scope_id": str(scope_id),
                "provider_manifest_digest": manifest_digest,
                "status": "failed",
                "cas_version": 1,
            }.items()
        ):
            raise InvalidTransition("Authority abort state is invalid")
        receipts = [r for r in state.get("receipts", []) if r.get("operation") == "abort"]
        if len(receipts) != 1:
            raise InvalidTransition("Authority abort receipt is missing or ambiguous")
        receipt = receipts[0]
        reason = receipt.get("payload", {}).get("reason")
        if not isinstance(reason, str) or not 1 <= len(reason) <= 2000:
            raise InvalidTransition("Authority abort reason is invalid")
        body = {**body, "reason": reason}
    else:
        response.raise_for_status()
        receipt = response.json()
    if (
        receipt.get("operation") != "abort"
        or receipt.get("signature_algorithm") != "ed25519"
        or receipt.get("allocation_id") != str(allocation_id)
        or receipt.get("request_digest") != canonical_request_hash(body)
        or any(
            receipt.get("payload", {}).get(k) != v for k, v in {**body, "cas_version": 1}.items()
        )
        or not verify_receipt(SimpleNamespace(**receipt), public_key)
    ):
        raise InvalidTransition("Authority abort receipt is invalid")
    return receipt


def _close_handoff(factory, scope_id, authority, public_key):
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        refit = db.scalar(
            select(WorkflowAttempt).where(
                WorkflowAttempt.scope_id == scope_id,
                WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
            )
        )
        refit_id = refit.id
        intent = _event(db, refit_id, "evaluation_authority_intent")
        request = None
        if intent is not None:
            if intent.result_digest != canonical_request_hash(intent.payload):
                raise InvalidTransition("Authority handoff intent changed")
            request = intent.payload["allocation"]
    receipt = {"no_authority_intent": True}
    if request is not None:
        # Replay allocation even if its original reply was lost. This also seals
        # against an earlier delayed allocator request arriving after cleanup.
        response = authority.post("allocations", json=request)
        response.raise_for_status()
        receipt = _abort_registration(
            authority,
            response.json()["allocation_id"],
            scope_id,
            request["provider_manifest_digest"],
            public_key,
        )
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        if scope.status == ScopeStatus.SUCCEEDED:
            raise InvalidTransition("Cannot close a succeeded handoff")
        refit = db.get(WorkflowAttempt, refit_id)
        _record(db, refit, "evaluation_handoff_closed", receipt)
        for attempt in _attempts(db, scope_id):
            _stop(attempt, "Evaluator handoff aborted")
        if scope.status == ScopeStatus.RUNNING:
            scope.status = ScopeStatus.FAILED
            scope.cas_version += 1


def reconcile_scope(factory, k8s, scope_id, authority, *, public_key, store):
    """One idempotent lifecycle step; authority failure never starts another evaluation."""
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        if scope is None:
            return
        attempts = _attempts(db, scope_id)
        scope.updated_at = datetime.now(UTC)
        latest = attempts[-1] if attempts else None
        terminal_scope = scope.status != ScopeStatus.RUNNING
        deadline = scope.scope_deadline_at
        snapshots = [(a.id, a.status, a.cleanup_state) for a in attempts]
        plans = []
        for attempt in attempts:
            event = _event(db, attempt.id, "evaluation_plan")
            if event is None:
                raise InvalidTransition("Evaluator plan is missing")
            plan = EvaluationPlan.model_validate(event.payload)
            if event.result_digest != plan.digest or plan.attempt_id != attempt.id:
                raise InvalidTransition("Evaluator plan changed")
            plans.append(plan)
        if latest is not None:
            attempt_id, fence, generation = latest.id, latest.fencing_token, latest.generation
            observed_status, lease = latest.status, latest.lease_expires_at
            output = _event(db, latest.id, "evaluation_output") is not None
        succeeded = scope.status == ScopeStatus.SUCCEEDED
    if succeeded:
        for attempt_id, _, _ in snapshots:
            _cleanup_attempt(factory, k8s, attempt_id)
        return
    if latest is None:
        if not terminal_scope and deadline is not None and deadline > datetime.now(UTC):
            plan, token, key = prepare_evaluator_credentials(
                factory, scope_id, k8s, authority, authority_public_key=public_key
            )
            with factory() as db:
                fence = db.get(WorkflowAttempt, plan.attempt_id).fencing_token
            submit_evaluator(factory, k8s, plan, token, key, fence=fence)
        else:
            _close_handoff(factory, scope_id, authority, public_key)
        return
    if generation == 2 and not _cleanup_attempt(factory, k8s, snapshots[0][0]):
        return
    plan = plans[-1]
    name = f"evaluation-{attempt_id.hex}"
    job_state = k8s.job_state(name)
    now = datetime.now(UTC)
    live = not terminal_scope and deadline is not None and deadline > now
    healthy = live and lease is not None and lease > now
    if (
        observed_status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
        and healthy
        and job_state in {"queued", "running"}
    ):
        return
    state = _authority_state(authority, plan, public_key)
    if terminal_scope and state["status"] == "committed":
        receipt = next(r for r in state["receipts"] if r["operation"] == "commit")
        with factory() as db, db.begin():
            scope = _scope(db, scope_id)
            if scope.status == ScopeStatus.RUNNING:
                return
            for attempt in _attempts(db, scope_id):
                _stop(attempt, "Scope stopped after authority commit")
                _record(db, attempt, "evaluation_stopped_after_commit", receipt)
        for old_id, _, _ in snapshots:
            _cleanup_attempt(factory, k8s, old_id)
        return
    if (
        live
        and observed_status in {AttemptStatus.PENDING, AttemptStatus.SUBMITTED}
        and (observed_status == AttemptStatus.PENDING or healthy and job_state == "missing")
        and state["status"] == "allocated"
    ):
        plan, token, key = prepare_evaluator_credentials(
            factory,
            scope_id,
            k8s,
            authority,
            authority_public_key=public_key,
            generation=generation,
        )
        submit_evaluator(factory, k8s, plan, token, key, fence=fence)
        return
    # Fence uploads/start/heartbeats before inspecting result intent or changing authority.
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        current = _attempts(db, scope_id)
        if not current or current[-1].id != attempt_id or current[-1].status != observed_status:
            return
        latest = current[-1]
        if latest.lease_expires_at != lease:
            return  # A heartbeat raced observation; inspect again next pass.
        latest.lease_expires_at = now
        _record(db, latest, "evaluation_recovery_started", {"attempt_id": str(attempt_id)})
        output_event = _event(db, latest.id, "evaluation_output")
        output = output_event is not None
        output_intent = output_event.payload if output else None
    if output and not terminal_scope and state["status"] in {"opened", "committed"}:
        try:
            _read_result(store, output_intent, plan)
        except (ValueError, FileNotFoundError):
            output = False
    if output and not terminal_scope and state["status"] in {"opened", "committed"}:

        def commit(digest):
            response = authority.post(
                f"allocations/{plan.allocation_id}/recovery/commit",
                json={
                    **_binding(plan, generation),
                    "expected_cas_version": 1,
                    "result_digest": digest,
                },
            )
            response.raise_for_status()
            return response.json()

        recover_evaluation_result(
            factory, plan, fence, store, commit=commit, authority_public_key=public_key
        )
        return
    if state["status"] == "committed":
        raise InvalidTransition("Committed authority result has no publishable local output")
    if state["status"] == "allocated" and live and generation == 1 and output_intent is None:
        with factory() as db, db.begin():
            _scope(db, scope_id)
            first = _attempts(db, scope_id)[-1]
            if first.id != attempt_id:
                return
            _stop(first, "Evaluator submission failed before final-data open")
            retry = first.status == AttemptStatus.FAILED and first.retry_budget >= 1
        if retry:
            if not _cleanup_attempt(factory, k8s, attempt_id):
                return
            new_plan, token, key = prepare_evaluator_credentials(
                factory, scope_id, k8s, authority, authority_public_key=public_key, generation=2
            )
            with factory() as db:
                new_fence = db.get(WorkflowAttempt, new_plan.attempt_id).fencing_token
            submit_evaluator(factory, k8s, new_plan, token, key, fence=new_fence)
            return
    _seal_failed(factory, authority, scope_id, state, plans, public_key)
    for old_id, _, _ in snapshots:
        _cleanup_attempt(factory, k8s, old_id)


def _close_unconfigured_handoff(factory, scope_id):
    """Fail an unstarted legacy handoff without hiding any external recovery intent."""
    with factory() as db, db.begin():
        scope = _scope(db, scope_id)
        if "evaluation_policy" in scope.comparison_policy:
            raise InvalidTransition("Evaluator policy changed during reconciliation")
        refit = db.scalar(
            select(WorkflowAttempt).where(
                WorkflowAttempt.scope_id == scope_id,
                WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
            )
        )
        if (
            refit is None
            or _attempts(db, scope_id)
            or _event(db, refit.id, "evaluation_authority_intent") is not None
            or scope.status == ScopeStatus.SUCCEEDED
        ):
            raise InvalidTransition("Missing evaluator policy requires authority recovery")
        _record(
            db,
            refit,
            "evaluation_handoff_closed",
            {"no_authority_intent": True, "reason": "missing_evaluation_policy"},
        )
        if scope.status == ScopeStatus.RUNNING:
            scope.status = ScopeStatus.FAILED
            scope.cas_version += 1


def reconcile_evaluation_jobs_once(
    factory, k8s, *, authority_factory=None, public_key=None, store=None, limit=25
):
    if authority_factory is None:
        if not os.getenv("EVALUATION_ALLOCATOR_TOKENS_FILE"):
            return 0
        authority_factory = configured_authority
        public_key = Path(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]).read_text()
    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.scope_allocation import (
        OPERATION,
        ScopeAllocationIntent,
        close_scope_allocation,
        defer_scope_allocation,
        reconcile_scope_allocation,
    )

    with factory() as db:
        registrations = list(
            db.execute(
                select(
                    PromotionalScope.id,
                    PromotionalScope.project_id,
                    PromotionalScope.comparison_policy,
                )
                .join(
                    WorkflowCommand,
                    (WorkflowCommand.resource_id == PromotionalScope.id)
                    & (WorkflowCommand.project_id == PromotionalScope.project_id),
                )
                .where(
                    PromotionalScope.mode == "promotional",
                    PromotionalScope.status == ScopeStatus.OPEN,
                    PromotionalScope.membership_deadline_at > datetime.now(UTC),
                    WorkflowCommand.operation == OPERATION,
                    WorkflowCommand.status.in_([CommandStatus.PENDING, CommandStatus.RUNNING]),
                    WorkflowCommand.available_at <= datetime.now(UTC),
                    or_(
                        WorkflowCommand.lease_expires_at.is_(None),
                        WorkflowCommand.lease_expires_at <= datetime.now(UTC),
                    ),
                )
                .order_by(WorkflowCommand.available_at, WorkflowCommand.id)
                .limit(limit)
            )
        )
    for scope_id, project_id, policy in registrations:
        try:
            reference = policy["evaluation_policy"]["final_data"]["project_reference"]
            with authority_factory(project_id, reference) as authority:
                reconcile_scope_allocation(factory, scope_id, authority)
        except Exception as error:
            defer_scope_allocation(factory, scope_id)
            LOGGER.error(
                "Early scope allocation failed",
                extra={
                    "scope_id": str(scope_id),
                    "error_type": type(error).__name__,
                },
            )
    with factory() as db:
        stopped = list(
            db.execute(
                select(
                    PromotionalScope.id,
                    PromotionalScope.project_id,
                    WorkflowCommand.request_payload,
                    WorkflowCommand.retry_count,
                )
                .join(
                    WorkflowCommand,
                    (WorkflowCommand.resource_id == PromotionalScope.id)
                    & (WorkflowCommand.project_id == PromotionalScope.project_id),
                )
                .where(
                    PromotionalScope.mode == "promotional",
                    or_(
                        PromotionalScope.status.in_([ScopeStatus.CANCELLED, ScopeStatus.FAILED]),
                        (PromotionalScope.status == ScopeStatus.OPEN)
                        & (PromotionalScope.membership_deadline_at <= datetime.now(UTC)),
                    ),
                    ~select(WorkflowAttempt.id)
                    .where(
                        WorkflowAttempt.scope_id == PromotionalScope.id,
                        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                    )
                    .exists(),
                    WorkflowCommand.operation == OPERATION,
                    WorkflowCommand.status != CommandStatus.CANCELLED,
                    WorkflowCommand.retry_count < WorkflowCommand.max_retries + 5,
                    WorkflowCommand.available_at <= datetime.now(UTC),
                    or_(
                        WorkflowCommand.lease_expires_at.is_(None),
                        WorkflowCommand.lease_expires_at <= datetime.now(UTC),
                    ),
                )
                .order_by(WorkflowCommand.available_at, WorkflowCommand.id)
                .limit(limit)
            )
        )
    for scope_id, project_id, payload, retries in stopped:
        try:
            if retries == 0:
                close_scope_allocation(factory, scope_id, None, public_key=public_key)
                continue
            intent = ScopeAllocationIntent.model_validate(payload)
            with authority_factory(project_id, intent.project_reference) as authority:
                close_scope_allocation(factory, scope_id, authority, public_key=public_key)
        except Exception as error:
            defer_scope_allocation(factory, scope_id)
            LOGGER.error(
                "Stopped scope allocation cleanup failed",
                extra={
                    "scope_id": str(scope_id),
                    "error_type": type(error).__name__,
                },
            )
    with factory() as db:
        scopes = list(
            db.execute(
                select(
                    PromotionalScope.id,
                    PromotionalScope.project_id,
                    PromotionalScope.comparison_policy,
                )
                .where(
                    PromotionalScope.mode == "promotional",
                    select(WorkflowAttempt.id)
                    .where(
                        WorkflowAttempt.scope_id == PromotionalScope.id,
                        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                        WorkflowAttempt.status == AttemptStatus.SUCCEEDED,
                    )
                    .exists(),
                    or_(
                        PromotionalScope.status == ScopeStatus.RUNNING,
                        (PromotionalScope.status != ScopeStatus.SUCCEEDED)
                        & ~select(WorkflowAttempt.id)
                        .where(
                            WorkflowAttempt.scope_id == PromotionalScope.id,
                            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                        )
                        .exists()
                        & ~select(WorkflowEvent.id)
                        .join(
                            WorkflowAttempt,
                            WorkflowEvent.attempt_id == WorkflowAttempt.id,
                        )
                        .where(
                            WorkflowAttempt.scope_id == PromotionalScope.id,
                            WorkflowEvent.event_key == "evaluation_handoff_closed",
                        )
                        .exists(),
                        select(WorkflowAttempt.id)
                        .where(
                            WorkflowAttempt.scope_id == PromotionalScope.id,
                            WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                            WorkflowAttempt.cleanup_state != "complete",
                        )
                        .exists(),
                    ),
                )
                .order_by(PromotionalScope.updated_at, PromotionalScope.id)
                .limit(limit)
            )
        )
    store = (store or get_object_store()) if scopes else None
    for scope_id, project_id, policy in scopes:
        try:
            if "evaluation_policy" not in policy:
                _close_unconfigured_handoff(factory, scope_id)
                continue
            reference = policy["evaluation_policy"]["final_data"]["project_reference"]
            with authority_factory(project_id, reference) as authority:
                reconcile_scope(
                    factory, k8s, scope_id, authority, public_key=public_key, store=store
                )
        except Exception as error:
            LOGGER.error(
                "Evaluator reconciliation failed",
                extra={
                    "scope_id": str(scope_id),
                    "error_type": type(error).__name__,
                },
            )
            with factory() as db, db.begin():
                scope = _scope(db, scope_id)
                if scope is not None:
                    scope.updated_at = datetime.now(UTC)
    global _next_sweep
    if time.monotonic() >= _next_sweep:
        _next_sweep = time.monotonic() + 30
        sweep_evaluation_resources(factory, k8s)
    return len(scopes) + len(registrations) + len(stopped)


def sweep_evaluation_resources(factory, k8s):
    """Remove late creates and project-cascade orphans, retaining keys while Jobs exist."""
    namespace = k8s.settings.training_namespace
    names, keys = {}, {}
    scope_label = "automl.platform/evaluation-scope"
    project_label = "automl.platform/project-id"
    for kind, listing, selector in (
        ("secret", k8s.core.list_namespaced_secret, scope_label),
        ("network", k8s.networking.list_namespaced_network_policy, LABEL),
        ("job", k8s.batch.list_namespaced_job, LABEL),
    ):
        page = None
        while True:
            result = listing(
                namespace=namespace, label_selector=selector, limit=100, _continue=page
            )
            for resource in result.items:
                labels = resource.metadata.labels or {}
                try:
                    scope_id = uuid.UUID(labels[scope_label])
                    project_id = uuid.UUID(labels[project_label])
                    if LABEL in labels:
                        attempt_id = uuid.UUID(labels[LABEL])
                        if resource.metadata.name == f"evaluation-{attempt_id.hex}":
                            owned_labels = {
                                LABEL: str(attempt_id),
                                scope_label: str(scope_id),
                                project_label: str(project_id),
                            }
                            names.setdefault(attempt_id, {})[kind] = {
                                "metadata": {
                                    "name": resource.metadata.name,
                                    "namespace": namespace,
                                    "labels": owned_labels,
                                    "uid": getattr(resource.metadata, "uid", None),
                                }
                            }
                    elif kind == "secret":
                        generation = labels["automl.platform/evaluation-generation"]
                        if generation in ("1", "2") and resource.metadata.name == (
                            f"evaluation-key-{scope_id.hex}-{generation}"
                        ):
                            keys[resource.metadata.name] = {
                                "name": resource.metadata.name,
                                "namespace": namespace,
                                "uid": getattr(resource.metadata, "uid", None),
                                "labels": {
                                    scope_label: str(scope_id),
                                    project_label: str(project_id),
                                    "automl.platform/evaluation-generation": generation,
                                },
                            }
                except (KeyError, TypeError, ValueError, AttributeError):
                    continue
            page = result.metadata._continue
            if not page:
                break
    waiting = set()
    for attempt_id, observed in names.items():
        observed_scopes = {
            uuid.UUID(resource["metadata"]["labels"][scope_label]) for resource in observed.values()
        }
        metadata = next(iter(observed.values()))["metadata"]
        labels = metadata["labels"]
        scope_id = uuid.UUID(labels[scope_label])
        with factory() as db:
            attempt = db.get(WorkflowAttempt, attempt_id)
            if attempt is not None and (
                attempt.stage != WorkflowStage.CHAMPION_EVALUATION
                or attempt.scope_id != scope_id
                or str(attempt.project_id) != labels[project_label]
            ):
                waiting.update(observed_scopes)
                if attempt.scope_id is not None:
                    waiting.add(attempt.scope_id)
                continue
            active = attempt is not None and attempt.status in ACTIVE_ATTEMPTS
        if active:
            waiting.update(observed_scopes)
        elif attempt is not None:
            if not _cleanup_attempt(factory, k8s, attempt_id):
                waiting.update(observed_scopes)
        elif not _cleanup(
            k8s,
            {
                **observed,
                "job": {
                    "metadata": {
                        "name": f"evaluation-{attempt_id.hex}",
                        "namespace": namespace,
                        "labels": labels,
                        **observed.get("job", {}).get("metadata", {}),
                    }
                },
            },
            labels=labels,
        ):
            waiting.update(observed_scopes)
    for metadata in keys.values():
        scope_id = uuid.UUID(metadata["labels"][scope_label])
        if scope_id in waiting:
            continue
        with factory() as db, db.begin():
            scope = db.get(PromotionalScope, scope_id)
            if scope is not None and (
                scope.status == ScopeStatus.RUNNING
                or str(scope.project_id) != metadata["labels"][project_label]
            ):
                continue
            attempts = list(
                db.scalars(
                    select(WorkflowAttempt)
                    .where(
                        WorkflowAttempt.scope_id == scope_id,
                        WorkflowAttempt.project_id == uuid.UUID(metadata["labels"][project_label]),
                        WorkflowAttempt.stage == WorkflowStage.CHAMPION_EVALUATION,
                        WorkflowAttempt.generation
                        == int(metadata["labels"]["automl.platform/evaluation-generation"]),
                    )
                    .with_for_update()
                )
            )
            if any(attempt.status in ACTIVE_ATTEMPTS for attempt in attempts):
                continue
            retiring = False
            for attempt in attempts:
                revoke_attempt_grants(db, attempt.id)
                retiring = unretired_grant_deadline(db, attempt.id) is not None or retiring
            if retiring:
                continue
        delete_owned_resource(
            k8s.core.read_namespaced_secret, k8s.core.delete_namespaced_secret, metadata
        )
