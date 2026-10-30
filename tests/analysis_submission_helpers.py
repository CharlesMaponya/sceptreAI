"""Mock-only component boundaries; real native custody is checked through the factory."""

import uuid
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

from automl_api.core.redaction import redact_text
from automl_api.models.enums import AttemptStatus, CommandStatus, RunKind, RunStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import reconciler
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    complete_outbox,
    defer_outbox,
    transition_attempt,
    transition_command,
)
from kubernetes.client import ApiException
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from urllib3.exceptions import HTTPError


def submit_analysis_job(db, entry, k8s):
    assert isinstance(k8s, Mock), "Component adapter must never call real native clients"
    identity = (
        entry.payload.get("manifest", {})
        .get("metadata", {})
        .get("labels", {})
        .get("automl.platform/attempt-id")
    )
    if not identity and reconciler.get_settings().environment in {"production", "staging"}:
        run = reconciler.prepare_legacy_analysis_delivery(db, entry)
        if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED}:
            return run
        try:
            k8s.batch.read_namespaced_job(
                name=run.k8s_job_name, namespace=run.k8s_namespace, _request_timeout=5
            )
        except (ApiException, HTTPError) as exc:
            if not isinstance(exc, ApiException) or exc.status != 404:
                entry.last_error = redact_text(f"Legacy analysis preflight unavailable: {exc}")[
                    :2000
                ]
                db.flush()
                return None
        else:
            raise StaleFence("Legacy Job remains")
        try:
            pods = k8s.core.list_namespaced_pod(
                namespace=run.k8s_namespace,
                label_selector=f"automl.platform/run-id={run.id}",
                _request_timeout=5,
            ).items
        except (ApiException, HTTPError) as exc:
            entry.last_error = redact_text(f"Legacy analysis preflight unavailable: {exc}")[:2000]
            db.flush()
            return None
        if pods:
            raise StaleFence("Legacy Pods remain")
        return reconciler.publish_legacy_analysis_delivery(db, entry, run, k8s)
    plan = prepare_analysis_submission(db, entry)
    attempt, manifest, run = plan.attempt, plan.manifest, plan.resource
    if manifest is None:
        return run
    capacity = k8s.capacity_snapshot().capacity
    # Include submitted rows whose pod has not appeared yet. The admission lock
    # prevents two reconcilers from both taking the last available slot.
    submitted = int(
        db.scalar(
            select(func.count(ModelRun.id)).where(
                ModelRun.id != run.id,
                ModelRun.k8s_namespace == k8s.settings.training_namespace,
                ModelRun.run_kind.in_({RunKind.EXPLAINABILITY, RunKind.VALIDATION, RunKind.DRIFT}),
                ModelRun.status.in_({RunStatus.QUEUED, RunStatus.RUNNING}),
                ModelRun.tags["desired_state"].astext == "kubernetes_submitted",
            )
        )
        or 0
    )
    if (
        not capacity.connected
        or max(capacity.active_training_jobs, submitted) >= k8s.settings.max_concurrent_jobs
    ):
        run.tags = {**run.tags, "waiting_for_capacity": True}
        db.flush()
        return None
    if attempt is not None:
        if attempt.status == AttemptStatus.PENDING:
            transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=attempt.fencing_token)
        k8s.ensure_service_account(attempt.workload_identity, namespace=run.k8s_namespace)
    created = k8s.create_job(manifest)
    return publish_analysis_submission(db, entry, plan, created)


def reconcile_entry(db, entry, *, worker_id, k8s):
    assert isinstance(k8s, Mock), "Component adapter must never call real native clients"
    delivery = reconciler._lock_outbox_delivery(db, entry.id, worker_id)
    if delivery is None:
        return
    if submit_analysis_job(db, entry, k8s) is None:
        defer_outbox(db, entry, worker_id=worker_id)
        return
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    command = db.get(WorkflowCommand, entry.command_id)
    if command is not None and command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)
    db.flush()


@dataclass(frozen=True)
class AnalysisSubmissionPlan:
    attempt: WorkflowAttempt | None
    manifest: dict[str, Any] | None
    resource: ModelRun


def prepare_analysis_submission(db: Session, entry: OutboxEntry):
    from automl_api.services.training import _lock_training_admission

    _lock_training_admission(db)
    manifest = dict(entry.payload["manifest"])
    identity = (manifest.get("metadata") or {}).get("labels", {}).get("automl.platform/attempt-id")
    attempt = None
    if identity:
        attempt = db.scalar(
            select(WorkflowAttempt)
            .where(WorkflowAttempt.id == uuid.UUID(identity))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        env = {
            item["name"]: item.get("value")
            for item in manifest["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        if (
            attempt is None
            or attempt.project_id != entry.project_id
            or attempt.stage != WorkflowStage.TRAINING_RUN
            or str(attempt.model_run_id) != str(entry.payload["run_id"])
            or attempt.logical_key != f"analysis-run:{attempt.model_run_id}"
            or env.get("AUTOML_FENCING_TOKEN") != attempt.fencing_token
        ):
            raise StaleFence("The analysis submission scope or fence is stale.")
        if attempt.status not in {AttemptStatus.PENDING, AttemptStatus.CLAIMED}:
            return AnalysisSubmissionPlan(attempt, None, db.get(ModelRun, attempt.model_run_id))
    elif reconciler.get_settings().environment in {"production", "staging"}:
        raise RuntimeError("Legacy analysis requires the factory-owned dispatcher.")
    run = db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == entry.project_id,
            ModelRun.id == uuid.UUID(str(entry.payload["run_id"])),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None:
        raise LookupError("The tracked analysis run no longer exists.")
    if run.tags.get("desired_state") == "kubernetes_submitted":
        return AnalysisSubmissionPlan(attempt, None, run)
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED}:
        return AnalysisSubmissionPlan(attempt, None, run)
    if attempt is not None and (
        run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or str(run.tags.get("workflow_attempt_id", "")) != str(attempt.id)
        or manifest["metadata"].get("namespace") != run.k8s_namespace
    ):
        raise StaleFence("The analysis submission no longer owns this run.")
    return AnalysisSubmissionPlan(attempt, manifest, run)


def publish_analysis_submission(db, entry, plan, created):
    attempt, manifest, run = plan.attempt, plan.manifest, plan.resource
    if attempt is not None:
        metadata = created.metadata
        if (
            not isinstance(metadata.uid, str)
            or not metadata.uid
            or metadata.namespace != run.k8s_namespace
            or metadata.name != manifest["metadata"]["name"]
        ):
            raise StaleFence("The submitted analysis Job identity is incomplete.")
        payload = {
            "namespace": metadata.namespace,
            "job_name": metadata.name,
            "uid": metadata.uid,
            "outbox_id": str(entry.id),
            "manifest_sha256": canonical_request_hash(manifest),
        }
        sequence = (
            db.scalar(
                select(func.max(WorkflowEvent.sequence)).where(
                    WorkflowEvent.attempt_id == attempt.id
                )
            )
            or 0
        )
        db.add(
            WorkflowEvent(
                project_id=attempt.project_id,
                attempt_id=attempt.id,
                sequence=sequence + 1,
                event_key="analysis.submitted",
                event_type="analysis.submitted",
                payload=payload,
                result_digest=canonical_request_hash(payload),
            )
        )
        transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token=attempt.fencing_token)
    run.status = RunStatus.QUEUED
    run.tags = {**run.tags, "desired_state": "kubernetes_submitted", "waiting_for_capacity": False}
    db.flush()
    return run
