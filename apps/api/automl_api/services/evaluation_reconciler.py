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
from kubernetes.client import ApiException
from sqlalchemy import or_, select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.services.champion_planning import ACTIVE_ATTEMPTS
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
def configured_authority(project_reference):
    """Controller-only project token map; never copied to an evaluator Pod."""
    tokens = json.loads(Path(os.environ["EVALUATION_ALLOCATOR_TOKENS_FILE"]).read_text())
    token = tokens[project_reference]
    if not isinstance(token, str) or not token:
        raise ValueError("Missing project allocator identity")
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
    with factory() as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        if attempt is None or attempt.status in ACTIVE_ATTEMPTS:
            return False
        event = _event(db, attempt_id, "evaluation_submission")
        scope_id, generation = attempt.scope_id, attempt.generation
        desired = (
            event.payload
            if event
            else {
                "job": {
                    "metadata": {
                        "name": f"evaluation-{attempt_id.hex}",
                        "namespace": k8s.settings.training_namespace,
                    }
                }
            }
        )
    if not _cleanup(k8s, desired):
        return False
    try:
        k8s.core.delete_namespaced_secret(
            name=f"evaluation-key-{scope_id.hex}-{generation}",
            namespace=k8s.settings.training_namespace,
        )
    except ApiException as error:
        if error.status != 404:
            raise
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
    with factory() as db:
        scopes = list(
            db.execute(
                select(PromotionalScope.id, PromotionalScope.comparison_policy)
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
    for scope_id, policy in scopes:
        try:
            if "evaluation_policy" not in policy:
                _close_unconfigured_handoff(factory, scope_id)
                continue
            reference = policy["evaluation_policy"]["final_data"]["project_reference"]
            with authority_factory(reference) as authority:
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
    return len(scopes)


def sweep_evaluation_resources(factory, k8s):
    """Remove late creates and project-cascade orphans, retaining keys while Jobs exist."""
    namespace = k8s.settings.training_namespace
    names, keys = {}, {}
    scope_label = "automl.platform/evaluation-scope"
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
                    if LABEL in labels:
                        attempt_id = uuid.UUID(labels[LABEL])
                        if resource.metadata.name == f"evaluation-{attempt_id.hex}":
                            names[attempt_id] = scope_id
                    elif kind == "secret":
                        generation = int(labels["automl.platform/evaluation-generation"])
                        if generation in (1, 2) and resource.metadata.name == (
                            f"evaluation-key-{scope_id.hex}-{generation}"
                        ):
                            keys[resource.metadata.name] = scope_id
                except (KeyError, TypeError, ValueError):
                    continue
            page = result.metadata._continue
            if not page:
                break
    waiting = set()
    for attempt_id, scope_id in names.items():
        with factory() as db:
            attempt = db.get(WorkflowAttempt, attempt_id)
            active = attempt is not None and attempt.status in ACTIVE_ATTEMPTS
        if active:
            waiting.add(scope_id)
        elif attempt is not None:
            if not _cleanup_attempt(factory, k8s, attempt_id):
                waiting.add(scope_id)
        elif not _cleanup(
            k8s,
            {
                "job": {
                    "metadata": {
                        "name": f"evaluation-{attempt_id.hex}",
                        "namespace": namespace,
                    }
                }
            },
        ):
            waiting.add(scope_id)
    for name, scope_id in keys.items():
        if scope_id in waiting:
            continue
        with factory() as db:
            scope = db.get(PromotionalScope, scope_id)
            if scope is not None and scope.status == ScopeStatus.RUNNING:
                continue
        try:
            k8s.core.delete_namespaced_secret(name=name, namespace=namespace)
        except ApiException as error:
            if error.status != 404:
                raise
