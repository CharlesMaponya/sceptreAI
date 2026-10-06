"""Durable, idempotent submission and observation of isolated refit Jobs."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from kubernetes.client import ApiException
from sqlalchemy import func, or_, select

from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.services.champion_planning import ACTIVE_ATTEMPTS
from automl_api.services.champion_workloads import DIGEST, isolated_workload
from automl_api.services.refit_access import locked_refit, refit_claims, refit_token
from automl_api.services.workflow_state import canonical_request_hash, transition_attempt

LOGGER = logging.getLogger(__name__)
LABEL = "automl.platform/refit-attempt"
_next_sweep = 0.0


def submission(plan, scope, settings):
    base = os.environ.get("REFIT_CONTROL_BASE_URL", "").rstrip("/")
    parts = urlsplit(base)
    image = os.environ.get("REFIT_IMAGE", settings.training_image)
    if (
        parts.scheme != "https"
        or not parts.netloc
        or parts.username
        or parts.query
        or parts.fragment
        or not re.fullmatch(r".+@sha256:[0-9a-f]{64}", image)
    ):
        raise ValueError("Refit submission requires an HTTPS control URL and digest-pinned image")
    egress = json.loads(os.environ.get("REFIT_EGRESS_RULES", "[]"))
    if not isinstance(egress, list) or not egress:
        raise ValueError("Refit submission requires explicit TLS endpoint egress rules")
    for rule in egress:
        if (
            not rule.get("to")
            or not rule.get("ports")
            or any(
                port.get("protocol", "TCP") != "TCP" or port.get("port") not in {443, 8443, 8334}
                for port in rule["ports"]
            )
        ):
            raise ValueError("Refit endpoint rules require explicit peers and TLS ports")
    memory_mib = int(os.environ.get("REFIT_MEMORY_MIB", "2048"))
    cpu = int(os.environ.get("REFIT_CPU_CORES", "1"))
    if memory_mib <= 0 or cpu <= 0 or plan.max_decoded_bytes > memory_mib * 1024 * 1024:
        raise ValueError("Refit inputs exceed the configured workload memory limit")
    seconds = int((scope.scope_deadline_at - datetime.now(UTC)).total_seconds())
    if seconds <= 0:
        raise ValueError("Refit scope deadline expired")
    name = f"refit-{plan.attempt_id.hex}"
    labels = {LABEL: str(plan.attempt_id), "automl.platform/workflow-stage": "champion-refit"}
    metadata = {"name": name, "namespace": settings.training_namespace, "labels": labels}
    disk = max(
        256 * 1024 * 1024,
        2 * max(item.byte_size for item in (plan.candidate, *plan.partitions))
        + plan.max_model_bytes,
    )
    env = [
        {"name": "REFIT_CONTROL_URL", "value": f"{base}/{plan.attempt_id}/"},
        {
            "name": "REFIT_CONTROL_TOKEN",
            "valueFrom": {
                "secretKeyRef": {"name": name, "key": "token"},
            },
        },
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
        {"name": "OMP_NUM_THREADS", "value": str(cpu)},
    ]
    volumes = [{"name": "tmp", "emptyDir": {"sizeLimit": str(disk)}}]
    mounts = [{"name": "tmp", "mountPath": "/tmp"}]
    ca = os.environ.get("REFIT_CA_SECRET")
    if ca:
        volumes.append(
            {
                "name": "ca",
                "secret": {
                    "secretName": ca,
                    "items": [{"key": "ca.crt", "path": "ca.crt"}],
                },
            }
        )
        mounts.append({"name": "ca", "mountPath": "/refit-ca", "readOnly": True})
        env.append({"name": "REFIT_CA_FILE", "value": "/refit-ca/ca.crt"})
    return isolated_workload(
        metadata=metadata,
        image=image,
        seconds=seconds,
        env=env,
        volumes=volumes,
        mounts=mounts,
        disk=disk,
        memory_mib=memory_mib,
        cpu=cpu,
        settings=settings,
        egress=egress,
        worker="refit",
        service_account="sceptre-champion-refit",
    )


def _locked(db, attempt_id):
    scope_id = db.scalar(select(WorkflowAttempt.scope_id).where(WorkflowAttempt.id == attempt_id))
    scope = db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.id == scope_id)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    if scope is None:
        return None, None
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == attempt_id)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    return scope, attempt


def _terminal(attempt, reason):
    target = (
        AttemptStatus.CANCELLED if attempt.status == AttemptStatus.PENDING else AttemptStatus.FAILED
    )
    transition_attempt(attempt, target, fencing_token=attempt.fencing_token, terminal_reason=reason)


def _ensure_resources(k8s, desired, attempt_id, project_id, fence, token):
    namespace = desired["job"]["metadata"]["namespace"]
    name = desired["job"]["metadata"]["name"]
    k8s.ensure_service_account("sceptre-champion-refit")
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "immutable": True,
        "metadata": {"name": name, "labels": {LABEL: str(attempt_id)}},
        "stringData": {"token": token},
    }
    try:
        k8s.core.create_namespaced_secret(namespace, secret)
    except ApiException as exc:
        if exc.status != 409:
            raise
        existing = k8s.core.read_namespaced_secret(name, namespace)
        claims = refit_claims(base64.b64decode(existing.data["token"]).decode(), attempt_id)
        if (existing.metadata.labels or {}).get(LABEL) != str(attempt_id) or (
            claims["project_id"] != project_id or claims["fence"] != fence
        ):
            raise ValueError("Existing refit Secret has different ownership") from None
    for resource, create, read in (
        (
            desired["network"],
            k8s.networking.create_namespaced_network_policy,
            k8s.networking.read_namespaced_network_policy,
        ),
        (desired["job"], k8s.batch.create_namespaced_job, k8s.batch.read_namespaced_job),
    ):
        try:
            create(namespace=namespace, body=resource)
        except ApiException as exc:
            if exc.status != 409:
                raise
            existing = read(name=name, namespace=namespace)
            if (existing.metadata.annotations or {}).get(DIGEST) != resource["metadata"][
                "annotations"
            ][DIGEST] or (existing.metadata.labels or {}).get(LABEL) != str(attempt_id):
                raise ValueError(
                    "Existing refit workload differs from its durable manifest"
                ) from None


def _cleanup(k8s, desired):
    name, namespace = (desired["job"]["metadata"][key] for key in ("name", "namespace"))
    # Keep the deny policy until foreground Job deletion has removed every worker Pod.
    try:
        k8s.batch.delete_namespaced_job(name, namespace, propagation_policy="Foreground")
        return False
    except ApiException as exc:
        if exc.status != 404:
            raise
    for delete in (
        k8s.core.delete_namespaced_secret,
        k8s.networking.delete_namespaced_network_policy,
    ):
        try:
            delete(name=name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
    return True


def reconcile_refit_jobs_once(session_factory, k8s, *, limit=25):
    with session_factory() as db:
        ids = list(
            db.scalars(
                select(WorkflowAttempt.id)
                .where(
                    WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                    or_(
                        WorkflowAttempt.status.in_(ACTIVE_ATTEMPTS),
                        WorkflowAttempt.cleanup_state != "complete",
                    ),
                )
                .order_by(WorkflowAttempt.updated_at, WorkflowAttempt.id)
                .limit(limit)
            )
        )
    for attempt_id in ids:
        try:
            _reconcile(session_factory, k8s, attempt_id)
        except Exception as exc:
            LOGGER.error(
                "refit workload reconciliation failed",
                extra={
                    "attempt_id": str(attempt_id),
                    "error_type": type(exc).__name__,
                },
            )
            try:
                with session_factory() as db, db.begin():
                    scope, attempt = _locked(db, attempt_id)
                    if attempt is not None:
                        attempt.updated_at = datetime.now(UTC)
                        if isinstance(exc, ValueError) and attempt.status in ACTIVE_ATTEMPTS:
                            pending = attempt.status == AttemptStatus.PENDING
                            _terminal(
                                attempt, "Refit submission configuration or ownership rejected"
                            )
                            if pending:
                                scope.status = ScopeStatus.FAILED
            except Exception as recovery:
                LOGGER.error(
                    "refit recovery unavailable", extra={"error_type": type(recovery).__name__}
                )
    global _next_sweep
    if time.monotonic() >= _next_sweep:
        _next_sweep = time.monotonic() + 30
        sweep_refit_resources(session_factory, k8s)
    return len(ids)


def sweep_refit_resources(session_factory, k8s):
    """Recover a late external create after terminal cleanup or a project cascade."""
    namespace = k8s.settings.training_namespace
    names = set()
    for listing in (
        k8s.core.list_namespaced_secret,
        k8s.networking.list_namespaced_network_policy,
        k8s.batch.list_namespaced_job,
    ):
        page = None
        while True:
            result = listing(namespace=namespace, label_selector=LABEL, limit=100, _continue=page)
            for resource in result.items:
                value = (resource.metadata.labels or {}).get(LABEL, "")
                try:
                    attempt_id = uuid.UUID(value)
                except ValueError:
                    continue
                if resource.metadata.name == f"refit-{attempt_id.hex}":
                    names.add(attempt_id)
            page = result.metadata._continue
            if not page:
                break
    for attempt_id in names:
        with session_factory() as db:
            attempt = db.get(WorkflowAttempt, attempt_id)
            if attempt is not None and attempt.status in ACTIVE_ATTEMPTS:
                continue
        _cleanup(
            k8s,
            {
                "job": {
                    "metadata": {
                        "name": f"refit-{attempt_id.hex}",
                        "namespace": namespace,
                    }
                }
            },
        )


def _reconcile(session_factory, k8s, attempt_id):
    with session_factory() as db, db.begin():
        scope, attempt = _locked(db, attempt_id)
        if attempt is None:
            return
        now = datetime.now(UTC)
        attempt.updated_at = now
        if attempt.status in ACTIVE_ATTEMPTS and (
            scope.status != ScopeStatus.RUNNING
            or scope.scope_deadline_at is None
            or scope.scope_deadline_at <= now
        ):
            _terminal(attempt, "Refit scope stopped or expired")
            if scope.status == ScopeStatus.RUNNING:
                scope.status = ScopeStatus.FAILED
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == attempt_id,
                WorkflowEvent.event_key == "refit_submission",
            )
        )
        if attempt.status == AttemptStatus.PENDING:
            _, _, plan = locked_refit(
                db,
                attempt_id,
                {
                    "project_id": attempt.project_id,
                    "fence": attempt.fencing_token,
                },
                (AttemptStatus.PENDING,),
            )
            desired = submission(plan, scope, k8s.settings)
            sequence = 1 + (
                db.scalar(
                    select(func.max(WorkflowEvent.sequence)).where(
                        WorkflowEvent.attempt_id == attempt_id,
                    )
                )
                or 0
            )
            event = WorkflowEvent(
                project_id=attempt.project_id,
                attempt_id=attempt_id,
                sequence=sequence,
                event_key="refit_submission",
                event_type="refit_submission",
                payload=desired,
                result_digest=canonical_request_hash(desired),
            )
            db.add(event)
            transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=attempt.fencing_token)
            transition_attempt(
                attempt, AttemptStatus.SUBMITTED, fencing_token=attempt.fencing_token
            )
            attempt.lease_expires_at = min(scope.scope_deadline_at, now + timedelta(minutes=5))
        if event is None:
            if attempt.status not in ACTIVE_ATTEMPTS:
                attempt.cleanup_state = "complete"
                return
            raise ValueError("Refit attempt has no durable submission")
        desired = event.payload
        if event.result_digest != canonical_request_hash(desired):
            raise ValueError("Refit submission manifest changed")
        if attempt.status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING} and (
            attempt.lease_expires_at is None or attempt.lease_expires_at <= now
        ):
            _terminal(attempt, "Refit startup or heartbeat lease expired")
        terminal = attempt.status not in ACTIVE_ATTEMPTS
        observed_status = attempt.status
        project_id, fence = attempt.project_id, attempt.fencing_token
        token = None if terminal else refit_token(attempt, scope.scope_deadline_at)
    # Durable SUBMITTED state, manifest and startup lease are committed before all side effects.
    if terminal:
        if _cleanup(k8s, desired):
            with session_factory() as db, db.begin():
                _, attempt = _locked(db, attempt_id)
                if attempt is not None and attempt.status not in ACTIVE_ATTEMPTS:
                    attempt.cleanup_state = "complete"
        return
    name = desired["job"]["metadata"]["name"]
    state = k8s.job_state(name)
    with session_factory() as db, db.begin():
        scope, attempt = _locked(db, attempt_id)
        if attempt is None or attempt.status != observed_status:
            return
        now = datetime.now(UTC)
        if (
            scope.status != ScopeStatus.RUNNING
            or scope.scope_deadline_at <= now
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            _terminal(attempt, "Refit scope or lease expired during observation")
            return
        if state in {"failed", "terminal_waiting_failure", "succeeded", "terminating"} or (
            state == "missing" and attempt.status == AttemptStatus.RUNNING
        ):
            _terminal(attempt, f"Refit Job {state} without terminal publication")
            return
        resubmit = state == "missing" and attempt.status == AttemptStatus.SUBMITTED
    if resubmit:
        _ensure_resources(k8s, desired, attempt_id, project_id, fence, token)
