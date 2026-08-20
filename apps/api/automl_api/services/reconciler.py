from __future__ import annotations

import uuid
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from automl_api.models.enums import AttemptStatus, CommandStatus, DatasetStatus, RunStatus
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowAttempt,
    WorkflowCommand,
)
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
    object_store_workload_environment,
)
from automl_api.services.upload_policy import inspect_and_scan_stream
from automl_api.services.workflow_state import (
    complete_outbox,
    enqueue_outbox,
    transition_attempt,
    transition_command,
)
from automl_api.storage.object_store import get_object_store


class UnknownOutboxTopic(ValueError):
    pass


RETRY_OWNER = {
    "ray.training.submit": "workflow-reconciler",
    "ray.training.cancel": "workflow-reconciler",
    "kubernetes.analysis.submit": "workflow-reconciler",
    "upload.reconcile": "workflow-reconciler",
    "registry.reconcile": "workflow-reconciler",
    "deployment.reconcile": "workflow-reconciler",
    "governance.reconcile": "workflow-reconciler",
    "cleanup.execute": "workflow-reconciler",
}


def reconcile_entry(
    db: Session,
    entry: OutboxEntry,
    *,
    worker_id: str,
    k8s: KubernetesTrainingClient | None = None,
    handlers: dict[str, Callable[[OutboxEntry], None]] | None = None,
) -> None:
    try:
        if entry.topic == "ray.training.submit":
            submit_training_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "ray.training.cancel":
            cancel_training_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "kubernetes.analysis.submit":
            submit_analysis_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "upload.reconcile":
            reconcile_upload_session(db, entry)
        elif entry.topic == "registry.reconcile":
            reconcile_registry_artifact(db, entry)
        elif entry.topic == "deployment.reconcile":
            reconcile_deployment(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "governance.reconcile":
            reconcile_governance_object(db, entry)
        elif entry.topic == "cleanup.execute":
            reconcile_cleanup(db, entry, k8s or KubernetesTrainingClient())
        elif handlers and entry.topic in handlers:
            handlers[entry.topic](entry)
        elif entry.topic in RETRY_OWNER:
            raise RuntimeError(f"No reconciler handler is configured for {entry.topic}.")
        else:
            raise UnknownOutboxTopic(entry.topic)
    except Exception as exc:
        entry_id = entry.id
        db.rollback()
        complete_outbox(
            db,
            entry_id,
            worker_id=worker_id,
            delivered=False,
            error=redact_text(str(exc)),
        )
        raise
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    command = db.get(WorkflowCommand, entry.command_id)
    if command is not None and command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)
        db.flush()


def submit_analysis_job(db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient) -> ModelRun:
    run = db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == entry.project_id,
            ModelRun.id == uuid.UUID(str(entry.payload["run_id"])),
        )
        .with_for_update()
    )
    if run is None:
        raise LookupError("The tracked analysis run no longer exists.")
    if run.tags.get("desired_state") == "kubernetes_submitted":
        return run
    k8s.create_job(dict(entry.payload["manifest"]))
    run.status = RunStatus.QUEUED
    run.tags = {**run.tags, "desired_state": "kubernetes_submitted"}
    db.flush()
    return run


def reconcile_upload_session(db: Session, entry: OutboxEntry) -> DatasetUploadSession:
    session = db.scalar(
        select(DatasetUploadSession)
        .where(
            DatasetUploadSession.project_id == entry.project_id,
            DatasetUploadSession.id == uuid.UUID(str(entry.payload["session_id"])),
        )
        .with_for_update()
    )
    if session is None:
        raise LookupError("The tracked upload session no longer exists.")
    if session.status in {"expired", "aborted", "quarantined", "failed", "ready"}:
        return session
    if session.status not in {"completed", "object_completed", "verifying"}:
        if session.expires_at <= datetime.now(UTC):
            session.status = "expired"
            session.terminal_reason = "upload session expired before durable completion"
            session.lease_owner = None
            session.lease_expires_at = None
            db.flush()
            return session
        raise RuntimeError("The upload session is not yet eligible for terminal reconciliation.")
    upload_kind = getattr(session, "upload_kind", "dataset")
    version = None
    if session.dataset_version_id is not None:
        version = db.scalar(
            select(DatasetVersion).where(
                DatasetVersion.project_id == session.project_id,
                DatasetVersion.id == session.dataset_version_id,
            )
        )
    if upload_kind == "dataset" and session.dataset_version_id is None:
        _quarantine_upload(session, "completed upload has no dataset version")
        db.flush()
        return session
    if session.dataset_version_id is not None and version is None:
        _quarantine_upload(session, "completed upload references a missing dataset version")
        db.flush()
        return session
    store = get_object_store()
    object_uri = getattr(session, "completed_object_uri", None) or (
        version.object_uri if version is not None else None
    )
    if not object_uri:
        _quarantine_upload(session, "completed upload has no durable object URI")
        db.flush()
        return session
    session_id = session.id
    project_id = session.project_id
    expected_size = session.byte_size
    expected_digest = getattr(session, "expected_object_digest", None)
    filename = getattr(session, "original_filename", "legacy.csv")
    version_id = session.dataset_version_id
    if expected_digest:
        session.status = "verifying"
    lease_seconds = max(60, get_settings().upload_scanner_timeout_seconds + 120)
    lease_started_at = datetime.now(UTC)
    entry.lease_expires_at = lease_started_at + timedelta(seconds=lease_seconds)
    session.lease_owner = entry.lease_owner or "workflow-reconciler"
    session.lease_expires_at = entry.lease_expires_at
    session.heartbeat_at = lease_started_at
    db.flush()
    # Storage reads and scanners can take minutes for multi-GiB objects. Commit the
    # durable verifying state and extended outbox lease before any external I/O so
    # PostgreSQL never sits idle in a transaction while bytes are streamed.
    db.commit()

    if not store.exists(object_uri):
        raise OSError("The completed upload object is not visible in durable storage.")
    observed_size = store.size(object_uri)
    inspection = None
    inspection_error: TimeoutError | ValueError | None = None
    if observed_size == expected_size and expected_digest:
        last_heartbeat_at = lease_started_at

        def renew_verification_lease(processed_bytes: int) -> None:
            nonlocal last_heartbeat_at
            observed_at = datetime.now(UTC)
            if (
                processed_bytes < expected_size
                and (observed_at - last_heartbeat_at).total_seconds() < 30
            ):
                return
            session.heartbeat_at = observed_at
            session.lease_expires_at = observed_at + timedelta(seconds=lease_seconds)
            entry.lease_expires_at = session.lease_expires_at
            db.flush()
            db.commit()
            last_heartbeat_at = observed_at

        source = store.open_stream(object_uri)
        try:
            inspection = inspect_and_scan_stream(
                source,
                filename=filename,
                expected_size=expected_size,
                settings=get_settings(),
                on_progress=renew_verification_lease,
            )
        except (TimeoutError, ValueError) as exc:
            inspection_error = exc
        finally:
            source.close()

    session = db.scalar(
        select(DatasetUploadSession)
        .where(
            DatasetUploadSession.project_id == project_id,
            DatasetUploadSession.id == session_id,
        )
        .with_for_update()
    )
    if session is None:
        raise LookupError("The tracked upload session no longer exists.")
    if session.status in {"expired", "aborted", "quarantined", "failed", "ready"}:
        return session
    if observed_size != expected_size:
        _quarantine_upload(
            session,
            f"object byte size mismatch: expected {expected_size}, observed {observed_size}",
            version,
        )
        db.flush()
        return session
    if not expected_digest:
        # Legacy multipart-manifest hashes remain accurately labelled and size-verified;
        # Phase 2 sessions always carry a client byte-stream SHA-256.
        session.terminal_reason = None
        session.lease_owner = None
        session.lease_expires_at = None
        session.heartbeat_at = datetime.now(UTC)
        db.flush()
        return session
    if inspection_error is not None:
        session.scanner_status = "denied" if isinstance(inspection_error, ValueError) else "failed"
        _quarantine_upload(session, str(inspection_error), version)
        db.flush()
        return session
    if inspection is None:
        raise RuntimeError("Upload inspection did not produce a terminal result.")
    session.observed_object_digest = inspection.sha256
    if inspection.sha256 != expected_digest:
        _quarantine_upload(
            session, "object SHA-256 differs from the client manifest", version
        )
        db.flush()
        return session
    verified_at = datetime.now(UTC)
    session.status = "ready"
    session.checksum_verified_at = verified_at
    session.scanner_status = inspection.scanner_status
    session.scanner_name = inspection.scanner_name
    session.scanner_version = inspection.scanner_version
    session.scanner_signature_version = inspection.signature_version
    session.scanner_evidence = inspection.evidence
    session.terminal_reason = None
    session.lease_owner = None
    session.lease_expires_at = None
    session.heartbeat_at = verified_at
    if version_id is not None:
        version = db.scalar(
            select(DatasetVersion)
            .where(
                DatasetVersion.project_id == project_id,
                DatasetVersion.id == version_id,
            )
            .with_for_update()
        )
    else:
        version = None
    if version is not None:
        version.content_hash = inspection.sha256
        version.content_hash_algorithm = "sha256"
        version.content_hash_scope = "byte_stream"
        version.content_hash_verification_status = "verified"
        version.content_hash_verified_at = verified_at
        version.status = DatasetStatus.READY
        if inspection.schema_columns and not (getattr(version, "schema_json", {}) or {}).get(
            "columns"
        ):
            version.column_count = len(inspection.schema_columns)
            version.schema_json = {
                "columns": [{"name": name} for name in inspection.schema_columns],
                "source": "verified_upload_stream",
            }
            version.inferred_types_json = {
                name: {"semantic_type": "pending", "nullable": None}
                for name in inspection.schema_columns
            }
            version.quality_report_json = {
                "completeness_score": None,
                "warnings": ["Rich column statistics are pending dataset preparation."],
            }
    db.flush()
    return session


def _quarantine_upload(
    session: DatasetUploadSession,
    reason: str,
    version: DatasetVersion | None = None,
) -> None:
    settings = get_settings()
    session.status = "quarantined"
    session.quarantine_reason = reason
    session.terminal_reason = reason
    session.quarantine_delete_after = datetime.now(UTC) + timedelta(
        seconds=settings.upload_quarantine_retention_seconds
    )
    session.lease_owner = None
    session.lease_expires_at = None
    if version is not None:
        version.status = DatasetStatus.FAILED


def reconcile_registry_artifact(db: Session, entry: OutboxEntry) -> ModelRegistryEntry:
    registry_entry = db.scalar(
        select(ModelRegistryEntry)
        .where(
            ModelRegistryEntry.project_id == entry.project_id,
            ModelRegistryEntry.id == uuid.UUID(str(entry.payload["registry_entry_id"])),
        )
        .with_for_update()
    )
    if registry_entry is None:
        raise LookupError("The tracked registry entry no longer exists.")
    if registry_entry.registry_metadata.get("desired_state") == "artifact_verified":
        return registry_entry
    artifact = db.scalar(
        select(RunArtifact).where(
            RunArtifact.project_id == registry_entry.project_id,
            RunArtifact.id == registry_entry.model_artifact_id,
        )
    )
    if artifact is None:
        raise LookupError("The registry model artifact no longer exists.")
    store = get_object_store()
    if not store.exists(artifact.object_uri):
        raise OSError("The registry model artifact is not visible in durable storage.")
    observed_size = store.size(artifact.object_uri)
    if artifact.byte_size is not None and observed_size != artifact.byte_size:
        raise ValueError("The registry model artifact byte size does not match its manifest.")
    registry_entry.registry_metadata = {
        **registry_entry.registry_metadata,
        "desired_state": "artifact_verified",
        "verified_byte_size": observed_size,
        "verified_at": datetime.now(UTC).isoformat(),
    }
    db.flush()
    return registry_entry


def reconcile_deployment(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> ModelRun:
    run = db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == entry.project_id,
            ModelRun.id == uuid.UUID(str(entry.payload["run_id"])),
        )
        .with_for_update()
    )
    if run is None:
        raise LookupError("The tracked deployment run no longer exists.")
    if run.tags.get("desired_state") == "deployment_active":
        return run
    artifact = db.scalar(
        select(RunArtifact).where(
            RunArtifact.project_id == run.project_id,
            RunArtifact.model_run_id == run.id,
            RunArtifact.object_uri == str(entry.payload["dockerfile_uri"]),
        )
    )
    if artifact is None:
        raise LookupError("The tracked deployment Dockerfile no longer exists.")
    content = str(artifact.artifact_metadata.get("content") or "")
    if not content:
        raise ValueError("The tracked deployment Dockerfile content is empty.")
    key = artifact.object_uri.split("/", 3)[-1]
    stored = get_object_store().put_bytes(key, content.encode("utf-8"))
    artifact.object_uri = stored.uri
    artifact.artifact_metadata = {
        key: value
        for key, value in artifact.artifact_metadata.items()
        if key not in {"content", "desired_state"}
    }
    k8s.create_model_deployment(dict(entry.payload["manifests"]))
    run.status = RunStatus.RUNNING
    run.started_at = run.started_at or datetime.now(UTC)
    run.tags = {**run.tags, "desired_state": "deployment_active"}
    db.flush()
    return run


def reconcile_governance_object(db: Session, entry: OutboxEntry) -> RunArtifact:
    artifact = db.scalar(
        select(RunArtifact)
        .where(
            RunArtifact.project_id == entry.project_id,
            RunArtifact.id == uuid.UUID(str(entry.payload["report_id"])),
        )
        .with_for_update()
    )
    if artifact is None:
        raise LookupError("The tracked governance report no longer exists.")
    if artifact.artifact_metadata.get("desired_state") == "object_written":
        return artifact
    json_content = artifact.artifact_metadata.get("pending_json")
    html_content = artifact.artifact_metadata.get("pending_html")
    html_uri = str(artifact.artifact_metadata.get("html_uri") or "")
    if not isinstance(json_content, str) or not isinstance(html_content, str) or not html_uri:
        raise ValueError("The tracked governance report payload is incomplete.")
    store = get_object_store()
    json_key = artifact.object_uri.split("/", 3)[-1]
    html_key = html_uri.split("/", 3)[-1]
    artifact.object_uri = store.put_bytes(json_key, json_content.encode("utf-8")).uri
    stored_html = store.put_bytes(html_key, html_content.encode("utf-8"))
    artifact.artifact_metadata = {
        **{
            key: value
            for key, value in artifact.artifact_metadata.items()
            if key not in {"pending_json", "pending_html"}
        },
        "html_uri": stored_html.uri,
        "desired_state": "object_written",
    }
    db.flush()
    return artifact


def reconcile_cleanup(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> list[uuid.UUID]:
    artifact_ids = [uuid.UUID(str(value)) for value in entry.payload.get("artifact_ids", [])]
    completed: list[uuid.UUID] = []
    store = get_object_store()
    for artifact_id in artifact_ids:
        artifact = db.scalar(
            select(RunArtifact)
            .where(
                RunArtifact.project_id == entry.project_id,
                RunArtifact.id == artifact_id,
            )
            .with_for_update()
        )
        tombstone = db.scalar(
            select(DeletionTombstone).where(
                DeletionTombstone.project_id == entry.project_id,
                DeletionTombstone.resource_type == "run_artifact",
                DeletionTombstone.resource_id == artifact_id,
            )
        )
        if tombstone is None:
            raise LookupError("The durable deletion tombstone no longer exists.")
        if tombstone.status == "completed":
            completed.append(artifact_id)
            continue
        if tombstone.legal_hold:
            raise ValueError("A legal hold prevents artifact deletion.")
        stage = db.scalar(
            select(DeletionStage).where(
                DeletionStage.project_id == entry.project_id,
                DeletionStage.tombstone_id == tombstone.id,
                DeletionStage.resource_class == "object_store",
            )
        )
        if artifact is not None:
            store.delete(artifact.object_uri)
            db.delete(artifact)
        if stage is not None:
            stage.status = "completed"
        tombstone.status = "completed"
        tombstone.completed_at = datetime.now(UTC)
        completed.append(artifact_id)
    if entry.payload.get("cleanup_finished_jobs"):
        k8s.cleanup_finished_jobs(entry.project_id)
    db.flush()
    return completed


def submit_training_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt_id = uuid.UUID(str(entry.payload["attempt_id"]))
    fencing_token = str(entry.payload["fencing_token"])
    attempt = db.scalar(
        select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
    )
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if attempt.status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED}:
        if attempt.fencing_token != fencing_token:
            raise ValueError("The submitted Ray attempt fence does not match the outbox command.")
        return attempt

    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == attempt.project_id,
            ModelRun.id == attempt.model_run_id,
        )
    )
    if run is None:
        raise LookupError("The tracked training run no longer exists.")
    if attempt.status == AttemptStatus.PENDING:
        transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=fencing_token)
    name = f"sceptre-run-{str(run.id)[:8]}-g{attempt.generation}"
    k8s.ensure_service_account(attempt.workload_identity)
    manifest = build_ray_job_manifest(run, attempt, name=name)
    created = k8s.create_ray_job(manifest)
    metadata = created.get("metadata", {})
    status = created.get("status", {})
    transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token=fencing_token)
    attempt.ray_job_name = str(metadata.get("name") or name)
    attempt.ray_cluster_name = status.get("rayClusterName") or f"{name}-raycluster"
    attempt.ray_submission_id = status.get("jobId")
    run.k8s_job_name = attempt.ray_job_name
    run.tags = {**run.tags, "desired_state": "ray_submitted"}
    db.flush()
    return attempt


def cancel_training_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(str(entry.payload["attempt_id"])))
        .with_for_update()
    )
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if attempt.ray_job_name:
        k8s.delete_ray_job(attempt.ray_job_name)
    if attempt.status not in {
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    }:
        transition_attempt(
            attempt,
            AttemptStatus.CANCELLED,
            fencing_token=str(entry.payload["fencing_token"]),
            terminal_reason="cancelled by durable command",
        )
    db.flush()
    return attempt


def observe_training_ray_jobs(
    db: Session,
    k8s: KubernetesTrainingClient,
    *,
    limit: int = 25,
) -> int:
    """Observe active RayJobs under a database row lock and replace lost generations."""
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.stage == "training_run",
                WorkflowAttempt.status.in_({AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}),
                WorkflowAttempt.ray_job_name.is_not(None),
            )
            .order_by(WorkflowAttempt.updated_at, WorkflowAttempt.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    )
    for attempt in attempts:
        try:
            ray_job = k8s.ray_job(str(attempt.ray_job_name))
        except ApiException as exc:
            if exc.status != 404:
                raise
            _replace_lost_ray_attempt(db, attempt, reason="RayJob disappeared before terminal CAS")
            continue

        status = ray_job.get("status") or {}
        cluster_name = status.get("rayClusterName")
        submission_id = status.get("jobId")
        if cluster_name:
            attempt.ray_cluster_name = str(cluster_name)
        if submission_id:
            attempt.ray_submission_id = str(submission_id)
        job_status = str(status.get("jobStatus") or "").upper()
        deployment_status = str(status.get("jobDeploymentStatus") or "").upper()
        if job_status == "RUNNING" or deployment_status == "RUNNING":
            if attempt.status == AttemptStatus.SUBMITTED:
                transition_attempt(
                    attempt,
                    AttemptStatus.RUNNING,
                    fencing_token=attempt.fencing_token,
                )
            run = db.get(ModelRun, attempt.model_run_id)
            if run is not None and run.status in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING}:
                run.status = RunStatus.RUNNING
                run.started_at = run.started_at or datetime.now(UTC)
        elif job_status in {"FAILED", "STOPPED"} or deployment_status == "FAILED":
            _replace_lost_ray_attempt(
                db,
                attempt,
                reason=f"RayJob terminal status {job_status or deployment_status}",
            )
        elif job_status == "SUCCEEDED" or deployment_status == "COMPLETE":
            _replace_lost_ray_attempt(
                db,
                attempt,
                reason="RayJob completed without a fenced terminal artifact CAS",
            )
    db.flush()
    return len(attempts)


def _replace_lost_ray_attempt(
    db: Session,
    attempt: WorkflowAttempt,
    *,
    reason: str,
) -> WorkflowAttempt | None:
    run = db.scalar(
        select(ModelRun)
        .where(
            ModelRun.project_id == attempt.project_id,
            ModelRun.id == attempt.model_run_id,
        )
        .with_for_update()
    )
    if run is None:
        raise LookupError("The lost Ray attempt has no tracked training run.")
    active_trial_attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.project_id == attempt.project_id,
                WorkflowAttempt.run_attempt_id == attempt.id,
                WorkflowAttempt.stage == "training_trial",
                WorkflowAttempt.status.in_(
                    {
                        AttemptStatus.PENDING,
                        AttemptStatus.CLAIMED,
                        AttemptStatus.SUBMITTED,
                        AttemptStatus.RUNNING,
                    }
                ),
            )
            .with_for_update()
        )
    )
    terminal = attempt.generation >= attempt.retry_budget
    transition_attempt(
        attempt,
        AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED,
        fencing_token=attempt.fencing_token,
        terminal_reason=reason,
        expected_cas_version=attempt.terminal_cas_version,
    )
    for trial_attempt in active_trial_attempts:
        transition_attempt(
            trial_attempt,
            AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED,
            fencing_token=trial_attempt.fencing_token,
            terminal_reason=reason,
            expected_cas_version=trial_attempt.terminal_cas_version,
        )
    db.flush()
    if terminal:
        run.status = RunStatus.FAILED
        run.failure_code = "ray_retry_budget_exhausted"
        run.failure_message = reason
        run.finished_at = datetime.now(UTC)
        return None

    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == attempt.project_id,
            WorkflowCommand.resource_type == "model_run",
            WorkflowCommand.resource_id == run.id,
            WorkflowCommand.operation == "training.launch",
        )
        .order_by(WorkflowCommand.created_at.desc())
        .limit(1)
        .with_for_update()
    )
    if command is None:
        raise LookupError("The lost Ray attempt has no durable launch command.")
    replacement = WorkflowAttempt(
        project_id=attempt.project_id,
        stage=attempt.stage,
        logical_key=attempt.logical_key,
        model_run_id=attempt.model_run_id,
        workload_identity=attempt.workload_identity,
        generation=attempt.generation + 1,
        fencing_token=uuid.uuid4().hex,
        predecessor_attempt_id=attempt.id,
        predecessor_checkpoint_allowlist=(
            [attempt.checkpoint_uri] if attempt.checkpoint_uri else []
        ),
        retry_budget=attempt.retry_budget,
    )
    db.add(replacement)
    db.flush()
    for trial_attempt in active_trial_attempts:
        db.add(
            WorkflowAttempt(
                project_id=trial_attempt.project_id,
                stage=trial_attempt.stage,
                logical_key=trial_attempt.logical_key,
                model_run_id=trial_attempt.model_run_id,
                trial_id=trial_attempt.trial_id,
                run_attempt_id=replacement.id,
                workload_identity=trial_attempt.workload_identity,
                generation=trial_attempt.generation + 1,
                fencing_token=uuid.uuid4().hex,
                predecessor_attempt_id=trial_attempt.id,
                predecessor_checkpoint_allowlist=(
                    [trial_attempt.checkpoint_uri] if trial_attempt.checkpoint_uri else []
                ),
                retry_budget=trial_attempt.retry_budget,
            )
        )
    run.status = RunStatus.QUEUED
    run.tags = {**run.tags, "desired_state": "ray_submission_pending"}
    enqueue_outbox(
        db,
        command,
        topic="ray.training.submit",
        aggregate_type="model_run",
        aggregate_id=run.id,
        payload={
            "attempt_id": str(replacement.id),
            "fencing_token": replacement.fencing_token,
        },
        event_key=f"ray:{run.id}:generation:{replacement.generation}:submit",
    )
    return replacement


def build_ray_job_manifest(run: ModelRun, attempt: WorkflowAttempt, *, name: str) -> dict[str, Any]:
    settings = get_settings()
    cpu_request = run.cpu_request_cores or settings.training_cpu_request_cores
    cpu_limit = run.cpu_limit_cores or settings.training_cpu_limit_cores
    memory_request = run.memory_request_mb or settings.training_memory_request_mb
    memory_limit = run.memory_limit_mb or settings.training_memory_limit_mb
    environment = [
        {"name": "AUTOML_RUN_ID", "value": str(run.id)},
        {"name": "AUTOML_PROJECT_ID", "value": str(run.project_id)},
        {"name": "AUTOML_ATTEMPT_ID", "value": str(attempt.id)},
        {"name": "AUTOML_FENCING_TOKEN", "value": attempt.fencing_token},
        {"name": "TRAINING_EXECUTION_MODE", "value": "ray"},
        {
            "name": "DATABASE_URL",
            "valueFrom": {
                "secretKeyRef": {
                    "name": settings.database_secret_name,
                    "key": settings.database_secret_key,
                }
            },
        },
        *object_store_workload_environment(settings),
        {"name": "MLFLOW_TRACKING_URI", "value": settings.mlflow_tracking_uri},
        {"name": "MLFLOW_ENABLE_ASYNC_LOGGING", "value": "false"},
    ]
    volumes = [
        {
            "name": "ray-runtime",
            "emptyDir": {"sizeLimit": f"{settings.dataset_cache_size_gb}Gi"},
        },
        {
            "name": "shared-memory",
            "emptyDir": {"medium": "Memory", "sizeLimit": "512Mi"},
        },
    ]

    def pod_spec(container_name: str, resources: dict[str, dict[str, str]]) -> dict[str, Any]:
        spec = {
            "serviceAccountName": attempt.workload_identity,
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": container_name,
                    "image": settings.training_image,
                    "imagePullPolicy": settings.training_image_pull_policy,
                    "env": deepcopy(environment),
                    "resources": resources,
                    "volumeMounts": [
                        {"name": "ray-runtime", "mountPath": "/tmp/ray"},
                        {"name": "shared-memory", "mountPath": "/dev/shm"},
                    ],
                }
            ],
            "volumes": deepcopy(volumes),
        }
        if settings.workload_image_pull_secrets:
            spec["imagePullSecrets"] = [
                {"name": secret_name} for secret_name in settings.workload_image_pull_secrets
            ]
        return spec

    submitter_spec = pod_spec(
        "ray-job-submitter",
        {
            "requests": {"cpu": "100m", "memory": "256Mi"},
            "limits": {"cpu": "500m", "memory": "1Gi"},
        },
    )
    submitter_spec["restartPolicy"] = "Never"

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {
            "name": name,
            "namespace": run.k8s_namespace,
            "labels": {
                "app.kubernetes.io/name": "sceptre",
                "automl.platform/project-id": str(run.project_id),
                "automl.platform/run-id": str(run.id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/generation": str(attempt.generation),
            },
        },
        "spec": {
            "submissionMode": "K8sJobMode",
            "jobId": f"sceptre-{attempt.fencing_token[:16]}",
            "backoffLimit": 0,
            "submitterConfig": {"backoffLimit": 0},
            "submitterPodTemplate": {"spec": submitter_spec},
            "shutdownAfterJobFinishes": False,
            "deletionStrategy": {
                "onSuccess": {"policy": "DeleteNone"},
                "onFailure": {"policy": "DeleteNone"},
            },
            "entrypoint": f"python -m automl_api.training.worker --run-id {run.id}",
            "rayClusterSpec": {
                "rayVersion": "2.56.1",
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "rayStartParams": {"dashboard-host": "0.0.0.0"},
                    "template": {
                        "spec": pod_spec(
                            "ray-head",
                            {
                                "requests": {"cpu": "250m", "memory": "512Mi"},
                                "limits": {"cpu": "1", "memory": "2Gi"},
                            },
                        )
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "workers",
                        "replicas": 1,
                        "minReplicas": 0,
                        "maxReplicas": 10,
                        "rayStartParams": {},
                        "template": {
                            "spec": pod_spec(
                                "ray-worker",
                                {
                                    "requests": {
                                        "cpu": str(cpu_request),
                                        "memory": f"{memory_request}Mi",
                                    },
                                    "limits": {
                                        "cpu": str(cpu_limit),
                                        "memory": f"{memory_limit}Mi",
                                    },
                                },
                            )
                        },
                    }
                ],
            },
        },
    }
