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
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion, ProfilingJob
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
    RunKind,
    RunStatus,
    WorkflowStage,
)
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
from automl_api.storage.contracts import ByteRange, ObjectStoreDriver
from automl_api.storage.object_store import get_object_store


class UnknownOutboxTopic(ValueError):
    pass


UPLOAD_VERIFICATION_RANGE_BYTES = 64 * 1024 * 1024
RAYJOB_STARTUP_FAILURE_GRACE = timedelta(minutes=5)
_FATAL_RAY_STARTUP_REASONS = frozenset(
    {
        "CreateContainerConfigError",
        "CreateContainerError",
        "ErrImageNeverPull",
        "ImagePullBackOff",
        "InvalidImageName",
    }
)


class _BoundedObjectReader:
    """Read a large object through bounded provider range requests.

    Some S3-compatible and cloud SDK streams buffer a long-lived full-object GET
    aggressively. Integrity inspection only needs a sequential stream, so rotating
    fixed-size range requests places a hard upper bound on storage-side and SDK
    buffering without materializing the object in the control plane.
    """

    def __init__(
        self,
        store: ObjectStoreDriver,
        uri: str,
        byte_size: int,
        *,
        range_size: int = UPLOAD_VERIFICATION_RANGE_BYTES,
    ) -> None:
        if byte_size < 0 or range_size <= 0:
            raise ValueError("Object and verification range sizes must be valid.")
        self._store = store
        self._uri = uri
        self._byte_size = byte_size
        self._range_size = range_size
        self._offset = 0
        self._range_end = -1
        self._source = None
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed verification stream.")
        remaining_object = self._byte_size - self._offset
        if remaining_object <= 0 or size == 0:
            return b""
        requested = remaining_object if size is None or size < 0 else min(size, remaining_object)
        result = bytearray()
        while len(result) < requested:
            if self._source is None:
                self._open_next_range()
            range_remaining = self._range_end - self._offset + 1
            chunk = self._source.read(min(requested - len(result), range_remaining))
            if not chunk:
                raise OSError("Object storage ended a verification range before its declared size.")
            result.extend(chunk)
            self._offset += len(chunk)
            if self._offset > self._range_end:
                self._close_range()
        return bytes(result)

    def close(self) -> None:
        if self._closed:
            return
        self._close_range()
        self._closed = True

    def _open_next_range(self) -> None:
        self._range_end = min(
            self._byte_size - 1,
            self._offset + self._range_size - 1,
        )
        self._source = self._store.open_stream(
            self._uri,
            ByteRange(self._offset, self._range_end),
        )

    def _close_range(self) -> None:
        if self._source is not None:
            self._source.close()
            self._source = None


RETRY_OWNER = {
    "ray.dataset.split.submit": "workflow-reconciler",
    "ray.dataset.prepare.submit": "workflow-reconciler",
    "ray.training.submit": "workflow-reconciler",
    "ray.training.cancel": "workflow-reconciler",
    "kubernetes.analysis.submit": "workflow-reconciler",
    "upload.reconcile": "workflow-reconciler",
    "registry.reconcile": "workflow-reconciler",
    "deployment.reconcile": "workflow-reconciler",
    "governance.reconcile": "workflow-reconciler",
    "cleanup.execute": "workflow-reconciler",
    "project.delete": "workflow-reconciler",
    "run.delete": "workflow-reconciler",
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
        if entry.topic in {"ray.dataset.split.submit", "ray.dataset.prepare.submit"}:
            submit_dataset_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "ray.training.submit":
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
        elif entry.topic in {"project.delete", "run.delete"}:
            from automl_api.services.deletion import reconcile_deletion

            project_deletion = entry.topic == "project.delete"
            reconcile_deletion(db, entry, k8s or KubernetesTrainingClient())
            if project_deletion:
                return  # The project cascade removed this outbox entry and its command.
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
        deleting_project_id = (
            entry.project_id if entry.topic in {"project.delete", "run.delete"} else None
        )
        db.rollback()
        if deleting_project_id:
            from automl_api.models.projects import Project

            project = db.get(Project, deleting_project_id)
            if project is not None:
                project.settings = {**project.settings, "deletion_error": redact_text(str(exc))}
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

        source = _BoundedObjectReader(
            store,
            object_uri,
            expected_size,
            range_size=UPLOAD_VERIFICATION_RANGE_BYTES,
        )
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
        _quarantine_upload(session, "object SHA-256 differs from the client manifest", version)
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
    if run.status == RunStatus.CANCELLED or run.tags.get("desired_state") in {
        "deployment_active",
        "deployment_stopped",
        "deployment_cleaned",
    }:
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
    if attempt.fencing_token != fencing_token:
        raise ValueError("The submitted Ray attempt fence does not match the outbox command.")
    if attempt.status in {
        AttemptStatus.SUBMITTED,
        AttemptStatus.RUNNING,
        AttemptStatus.SUCCEEDED,
        AttemptStatus.FAILED,
        AttemptStatus.CANCELLED,
        AttemptStatus.SUPERSEDED,
    }:
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
    run.failure_code = None
    run.failure_message = None
    run.plain_english_failure = None
    run.finished_at = None
    run.tags = {**run.tags, "desired_state": "ray_submitted"}
    db.flush()
    return attempt


def submit_dataset_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt_id = uuid.UUID(str(entry.payload["attempt_id"]))
    fencing_token = str(entry.payload["fencing_token"])
    profile_job_id = uuid.UUID(str(entry.payload["profiling_job_id"]))
    attempt = db.scalar(
        select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
    )
    if attempt is None:
        raise LookupError("The tracked dataset Ray attempt no longer exists.")
    if attempt.fencing_token != fencing_token:
        raise ValueError("The submitted dataset Ray attempt fence does not match the command.")
    if attempt.status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED}:
        return attempt

    expected_stage = (
        WorkflowStage.SPLITTER
        if entry.topic == "ray.dataset.split.submit"
        else WorkflowStage.PREPARATION
    )
    if attempt.stage != expected_stage:
        raise ValueError(f"Dataset Ray topic does not match attempt stage {attempt.stage.value}.")
    job = db.scalar(
        select(ProfilingJob).where(
            ProfilingJob.project_id == attempt.project_id,
            ProfilingJob.id == profile_job_id,
        )
    )
    if job is None or job.dataset_version_id != attempt.dataset_version_id:
        raise LookupError("The dataset Ray attempt has no scoped profiling job.")
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == attempt.project_id,
            DatasetVersion.id == attempt.dataset_version_id,
        )
    )
    if version is None:
        raise LookupError("The dataset Ray attempt has no scoped dataset version.")

    if attempt.status == AttemptStatus.PENDING:
        transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=fencing_token)
    stage_name = expected_stage.value
    name = f"sceptre-{stage_name}-{str(job.id)[:8]}-g{attempt.generation}"
    k8s.ensure_service_account(attempt.workload_identity)
    manifest = build_dataset_ray_job_manifest(job, version, attempt, name=name)
    created = k8s.create_ray_job(manifest)
    metadata = created.get("metadata", {})
    ray_status = created.get("status", {})
    transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token=fencing_token)
    attempt.ray_job_name = str(metadata.get("name") or name)
    attempt.ray_cluster_name = ray_status.get("rayClusterName") or f"{name}-raycluster"
    attempt.ray_submission_id = ray_status.get("jobId")
    job.status = "running"
    job.current_stage = stage_name
    job.started_at = job.started_at or datetime.now(UTC)
    job.heartbeat_at = datetime.now(UTC)
    job.overview_json = {
        **job.overview_json,
        "workflow_attempt_id": str(attempt.id),
        "workflow_generation": attempt.generation,
        "ray_job_name": attempt.ray_job_name,
    }
    version.status = DatasetStatus.PROFILING
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
        if job_status in {"FAILED", "STOPPED"} or deployment_status == "FAILED":
            terminal_status = "FAILED" if deployment_status == "FAILED" else job_status
            deadline_exceeded = status.get("reason") == "DeadlineExceeded"
            message = str(status.get("message") or "")
            out_of_memory = any(
                marker in f"{status.get('reason', '')} {message}".lower()
                for marker in (
                    "outofmemory", "out of memory", "oomkill", "oom kill", "running low on memory",
                )
            )
            _replace_lost_ray_attempt(
                db,
                attempt,
                reason=(
                    (message or "RayJob exceeded its runtime limit")[:4000]
                    if deadline_exceeded or out_of_memory
                    else (message or f"RayJob terminal status {terminal_status}")[:4000]
                ),
                terminal_failure_code=("JOB_DEADLINE_EXCEEDED" if deadline_exceeded
                    else "TRAINING_OUT_OF_MEMORY" if out_of_memory else None),
            )
        elif job_status == "SUCCEEDED" or deployment_status == "COMPLETE":
            _replace_lost_ray_attempt(
                db,
                attempt,
                reason="RayJob completed without a fenced terminal artifact CAS",
            )
        elif job_status == "RUNNING" or deployment_status == "RUNNING":
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
                run.failure_code = None
                run.failure_message = None
                run.plain_english_failure = None
                run.finished_at = None
    db.flush()
    return len(attempts)


def observe_analysis_jobs(db: Session, k8s: KubernetesTrainingClient, *, limit: int = 25) -> int:
    from automl_api.services.training import _sync_run_status

    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.run_kind.in_({RunKind.EXPLAINABILITY, RunKind.VALIDATION, RunKind.DRIFT}),
                ModelRun.status.in_({RunStatus.QUEUED, RunStatus.RUNNING}),
                ModelRun.tags["desired_state"].astext == "kubernetes_submitted",
            )
            .order_by(ModelRun.updated_at, ModelRun.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    )
    for run in runs:
        _sync_run_status(db, run, k8s, observe_managed=True)
    return len(runs)


def observe_dataset_ray_jobs(
    db: Session,
    k8s: KubernetesTrainingClient,
    *,
    limit: int = 25,
) -> int:
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.stage.in_({WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}),
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
            _replace_lost_dataset_attempt(
                db, attempt, reason="Dataset RayJob disappeared before terminal CAS"
            )
            continue
        ray_status = ray_job.get("status") or {}
        if ray_status.get("rayClusterName"):
            attempt.ray_cluster_name = str(ray_status["rayClusterName"])
        if ray_status.get("jobId"):
            attempt.ray_submission_id = str(ray_status["jobId"])
        job_status = str(ray_status.get("jobStatus") or "").upper()
        deployment_status = str(ray_status.get("jobDeploymentStatus") or "").upper()
        startup_failure = _ray_startup_failure(ray_status, attempt.updated_at)
        if job_status in {"FAILED", "STOPPED"} or deployment_status == "FAILED":
            terminal_status = "FAILED" if deployment_status == "FAILED" else job_status
            _replace_lost_dataset_attempt(
                db,
                attempt,
                reason=f"Dataset RayJob terminal status {terminal_status}",
            )
        elif job_status == "SUCCEEDED" or deployment_status == "COMPLETE":
            _replace_lost_dataset_attempt(
                db,
                attempt,
                reason="Dataset RayJob completed without a fenced terminal artifact CAS",
            )
        elif job_status == "RUNNING" or deployment_status == "RUNNING":
            if attempt.status == AttemptStatus.SUBMITTED:
                transition_attempt(
                    attempt,
                    AttemptStatus.RUNNING,
                    fencing_token=attempt.fencing_token,
                )
            profile = _profile_job_for_attempt(db, attempt)
            profile.heartbeat_at = datetime.now(UTC)
        elif startup_failure is not None:
            _replace_lost_dataset_attempt(db, attempt, reason=startup_failure)
    db.flush()
    return len(attempts)


def _ray_startup_failure(ray_status: dict[str, Any], updated_at: datetime | None) -> str | None:
    """Return a durable KubeRay startup failure after a bounded recovery grace."""
    if updated_at is None:
        return None
    observed_at = updated_at
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    if observed_at > datetime.now(UTC) - RAYJOB_STARTUP_FAILURE_GRACE:
        return None

    reasons: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            reason = value.get("reason")
            if reason:
                reasons.add(str(reason))
            for nested in value.values():
                collect(nested)
        elif isinstance(value, list):
            for nested in value:
                collect(nested)

    collect(ray_status.get("rayClusterStatus") or {})
    fatal = sorted(reasons & _FATAL_RAY_STARTUP_REASONS)
    if not fatal:
        return None
    return f"Dataset RayJob could not start: {', '.join(fatal)}"


def _profile_job_for_attempt(db: Session, attempt: WorkflowAttempt) -> ProfilingJob:
    marker = "profile:"
    if not attempt.logical_key.startswith(marker):
        raise LookupError("The dataset attempt logical key is not profile-scoped.")
    profile_id = uuid.UUID(attempt.logical_key.split(":", 2)[1])
    profile = db.scalar(
        select(ProfilingJob).where(
            ProfilingJob.project_id == attempt.project_id,
            ProfilingJob.id == profile_id,
            ProfilingJob.dataset_version_id == attempt.dataset_version_id,
        )
    )
    if profile is None:
        raise LookupError("The dataset attempt has no tracked profiling job.")
    return profile


def _replace_lost_dataset_attempt(
    db: Session,
    attempt: WorkflowAttempt,
    *,
    reason: str,
) -> WorkflowAttempt | None:
    profile = _profile_job_for_attempt(db, attempt)
    # The worker records the actual exception before Ray reports a terminal status.
    # Retain it instead of replacing the useful message with a generic FAILED label.
    worker_reason = getattr(attempt, "terminal_reason", None)
    if isinstance(worker_reason, str) and worker_reason:
        reason = worker_reason
    terminal = attempt.generation >= attempt.retry_budget
    transition_attempt(
        attempt,
        AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED,
        fencing_token=attempt.fencing_token,
        terminal_reason=reason,
        expected_cas_version=attempt.terminal_cas_version,
    )
    if terminal:
        profile.status = "failed"
        profile.current_stage = "failed"
        profile.failure_message = reason
        profile.finished_at = datetime.now(UTC)
        version = db.get(DatasetVersion, attempt.dataset_version_id)
        if version is not None:
            version.status = DatasetStatus.READY
        return None

    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == attempt.project_id,
            WorkflowCommand.resource_type == "profiling_job",
            WorkflowCommand.resource_id == profile.id,
            WorkflowCommand.operation == "dataset.profile",
        )
        .order_by(WorkflowCommand.created_at.desc())
        .limit(1)
        .with_for_update()
    )
    if command is None:
        raise LookupError("The lost dataset Ray attempt has no durable profile command.")
    replacement = WorkflowAttempt(
        project_id=attempt.project_id,
        stage=attempt.stage,
        logical_key=attempt.logical_key,
        dataset_version_id=attempt.dataset_version_id,
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
    topic = (
        "ray.dataset.split.submit"
        if attempt.stage == WorkflowStage.SPLITTER
        else "ray.dataset.prepare.submit"
    )
    enqueue_outbox(
        db,
        command,
        topic=topic,
        aggregate_type="profiling_job",
        aggregate_id=profile.id,
        payload={
            "profiling_job_id": str(profile.id),
            "attempt_id": str(replacement.id),
            "fencing_token": replacement.fencing_token,
        },
        event_key=(
            f"profile:{profile.id}:{attempt.stage.value}:generation:{replacement.generation}:submit"
        ),
    )
    profile.status = "queued"
    profile.current_stage = attempt.stage.value
    profile.failure_message = None
    profile.overview_json = {
        **profile.overview_json,
        "workflow_attempt_id": str(replacement.id),
        "workflow_generation": replacement.generation,
    }
    return replacement


def _replace_lost_ray_attempt(
    db: Session,
    attempt: WorkflowAttempt,
    *,
    reason: str,
    terminal_failure_code: str | None = None,
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
    terminal = terminal_failure_code is not None or attempt.generation >= attempt.retry_budget
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
        run.failure_code = terminal_failure_code or "ray_retry_budget_exhausted"
        run.failure_message = reason
        run.plain_english_failure = (
            "Training reached its runtime limit. Reduce the model search or increase "
            "available compute before restarting."
            if terminal_failure_code == "JOB_DEADLINE_EXCEEDED"
            else "Training exceeded the worker's memory capacity. Automatic retries stopped "
            "to avoid repeating the same model search. Completed candidate evidence is retained. "
            "Reduce model complexity or increase worker memory before restarting."
            if terminal_failure_code == "TRAINING_OUT_OF_MEMORY"
            else "Automatic recovery exhausted its retry budget. "
            "Review the run logs before restarting."
        )
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
    run.failure_code = None
    run.failure_message = None
    run.plain_english_failure = None
    run.finished_at = None
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
    environment = [
        {"name": "AUTOML_RUN_ID", "value": str(run.id)},
        {"name": "TRAINING_EXECUTION_MODE", "value": "ray"},
        {"name": "AUTOML_MODEL_PODS", "value": "1"},
        {"name": "AUTOML_TUNE_CONCURRENCY", "value": str(settings.training_max_concurrent_trials)},
        {"name": "AUTOML_CPU_THREADS", "value": str(run.cpu_limit_cores or settings.training_cpu_limit_cores)},
        {"name": "AUTOML_GPU_VENDOR", "value": str((run.params or {}).get("gpu_vendor") or "")},
        {"name": "MLFLOW_TRACKING_URI", "value": settings.mlflow_tracking_uri},
        {"name": "MLFLOW_ENABLE_ASYNC_LOGGING", "value": "false"},
    ]
    labels = {
        "automl.platform/run-id": str(run.id),
    }
    worker_resources = {
        "requests": {
            "cpu": str(run.cpu_request_cores or settings.training_cpu_request_cores),
            "memory": f"{run.memory_request_mb or settings.training_memory_request_mb}Mi",
        },
        "limits": {
            "cpu": str(run.cpu_limit_cores or settings.training_cpu_limit_cores),
            "memory": f"{run.memory_limit_mb or settings.training_memory_limit_mb}Mi",
        },
    }
    worker_resources["requests"] = dict(worker_resources["limits"])
    gpu_resource = (run.params or {}).get("gpu_resource")
    if run.gpu_requested and gpu_resource:
        worker_resources["requests"][gpu_resource] = "1"
        worker_resources["limits"][gpu_resource] = "1"
    manifest = _build_ephemeral_ray_job_manifest(
        namespace=run.k8s_namespace,
        project_id=run.project_id,
        attempt=attempt,
        name=name,
        entrypoint=f"python -m automl_api.training.worker --run-id {run.id}",
        environment=environment,
        labels=labels,
        worker_resources=worker_resources,
    )
    cluster = manifest["spec"]["rayClusterSpec"]
    cluster["headGroupSpec"]["rayStartParams"]["num-cpus"] = "0"
    workers = cluster["workerGroupSpecs"][0]
    workers["rayStartParams"]["num-cpus"] = worker_resources["limits"]["cpu"]
    workers["minReplicas"] = 0
    # A candidate reserves every logical worker CPU. The head coordinates and
    # publishes results; it never competes with a worker's estimator fit.
    deadline = (run.params or {}).get("deadline_seconds", settings.training_active_deadline_seconds)
    if deadline is None:
        manifest["spec"].pop("activeDeadlineSeconds", None)
    else:
        manifest["spec"]["activeDeadlineSeconds"] = int(deadline)
    if run.gpu_requested:
        vendor = (run.params or {}).get("gpu_vendor")
        gpu_image = (
            settings.training_image_nvidia if vendor == "nvidia" else settings.training_image_intel
        )
        cluster = manifest["spec"]["rayClusterSpec"]
        for template in (
            cluster["headGroupSpec"]["template"],
            cluster["workerGroupSpecs"][0]["template"],
            manifest["spec"]["submitterPodTemplate"],
        ):
            template["spec"]["containers"][0]["image"] = gpu_image
    selected_node = str((run.params or {}).get("selected_node") or "").strip()
    if selected_node:
        # The sample size and head limit were qualified against this exact
        # node. Bind only the model-bearing head; data workers remain free for
        # Kubernetes/Karpenter to place or scale independently.
        manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["nodeSelector"] = {
            "kubernetes.io/hostname": selected_node
        }
    return manifest


def build_dataset_ray_job_manifest(
    job: ProfilingJob,
    version: DatasetVersion,
    attempt: WorkflowAttempt,
    *,
    name: str,
) -> dict[str, Any]:
    stage = attempt.stage.value
    environment = [
        {"name": "AUTOML_PROFILE_JOB_ID", "value": str(job.id)},
        {"name": "AUTOML_DATASET_VERSION_ID", "value": str(version.id)},
        {"name": "DATASET_EXECUTION_MODE", "value": stage},
    ]
    labels = {
        "automl.platform/profile-job-id": str(job.id),
        "automl.platform/dataset-version-id": str(version.id),
        "automl.platform/workflow-stage": stage,
    }
    return _build_ephemeral_ray_job_manifest(
        namespace=get_settings().training_namespace,
        project_id=job.project_id,
        attempt=attempt,
        name=name,
        entrypoint=(
            f"python -m automl_api.preparation_worker --profile-job-id {job.id} --stage {stage}"
        ),
        environment=environment,
        labels=labels,
        worker_resources={
            "requests": {"cpu": "1", "memory": "4Gi"},
            "limits": {"cpu": "2", "memory": "5Gi"},
        },
    )


def _build_ephemeral_ray_job_manifest(
    *,
    namespace: str,
    project_id: uuid.UUID,
    attempt: WorkflowAttempt,
    name: str,
    entrypoint: str,
    environment: list[dict[str, Any]],
    labels: dict[str, str],
    worker_resources: dict[str, dict[str, str]],
) -> dict[str, Any]:
    settings = get_settings()
    workload_labels = {
        "app.kubernetes.io/name": "sceptre",
        "automl.platform/project-id": str(project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
        **labels,
    }
    is_dataset_workload = attempt.stage in {
        WorkflowStage.SPLITTER,
        WorkflowStage.PREPARATION,
    }
    common_environment = [
        # Each Ray Data task reserves one CPU; avoid nested Polars thread pools.
        {"name": "POLARS_MAX_THREADS", "value": "1"},
        {"name": "AUTOML_PROJECT_ID", "value": str(project_id)},
        {"name": "AUTOML_ATTEMPT_ID", "value": str(attempt.id)},
        {"name": "AUTOML_FENCING_TOKEN", "value": attempt.fencing_token},
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
        *environment,
    ]
    # Pod-scoped ephemeral volumes: emptyDir is created per-pod by the kubelet
    # (never a shared host path), so these mount points cannot collide across
    # workloads or outlive the attempt.
    RAY_RUNTIME_MOUNT = "/tmp/ray"  # nosec B108 - emptyDir volume, pod-scoped
    SHARED_MEMORY_MOUNT = "/dev/shm"  # nosec B108 - Memory-backed emptyDir
    volumes = [
        {
            "name": "ray-runtime",
            "emptyDir": {"sizeLimit": f"{settings.dataset_cache_size_gb}Gi"},
        },
        {
            "name": "shared-memory",
            "emptyDir": {"medium": "Memory", "sizeLimit": "2Gi"},
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
                    "env": deepcopy(common_environment),
                    "resources": resources,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "volumeMounts": [
                        {"name": "ray-runtime", "mountPath": RAY_RUNTIME_MOUNT},
                        {"name": "shared-memory", "mountPath": SHARED_MEMORY_MOUNT},
                    ],
                }
            ],
            "volumes": deepcopy(volumes),
        }
        if container_name == "ray-head":
            # KubeRay creates a cluster-specific autoscaler identity and RBAC.
            # Workers and submitters keep their unprivileged stage identity.
            spec.pop("serviceAccountName")
            spec["automountServiceAccountToken"] = True
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
            "namespace": namespace,
            "labels": workload_labels,
        },
        "spec": {
            "submissionMode": "K8sJobMode",
            "jobId": f"sceptre-{attempt.fencing_token[:16]}",
            "backoffLimit": 0,
            "submitterConfig": {"backoffLimit": 0},
            "submitterPodTemplate": {
                "metadata": {"labels": deepcopy(workload_labels)},
                "spec": submitter_spec,
            },
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": settings.training_job_ttl_seconds,
            "activeDeadlineSeconds": settings.training_active_deadline_seconds,
            "entrypoint": entrypoint,
            "rayClusterSpec": {
                "rayVersion": "2.58.0",
                "enableInTreeAutoscaling": True,
                "autoscalerOptions": {
                    "idleTimeoutSeconds": 60,
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "256Mi"},
                    },
                },
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "rayStartParams": {
                        # The Ray dashboard must bind on all pod interfaces for
                        # its health probes; the head is ClusterIP-only inside
                        # the namespace, never exposed externally.
                        "dashboard-host": "0.0.0.0",  # nosec B104 - ClusterIP only
                        # Dataset drivers perform bounded local aggregation on
                        # the head. Keep Ray Data tasks on workers so that work
                        # cannot amplify the driver's memory footprint.
                        "num-cpus": "0" if is_dataset_workload else "1",
                        "object-store-memory": "268435456",
                    },
                    "template": {
                        "metadata": {"labels": deepcopy(workload_labels)},
                        "spec": pod_spec(
                            "ray-head",
                            (
                                {
                                    "requests": {"cpu": "500m", "memory": "4Gi"},
                                    "limits": {"cpu": "2", "memory": "6Gi"},
                                }
                                if is_dataset_workload
                                # The training model is materialized in the Ray
                                # driver, so the head must use the run's observed
                                # node-capacity plan rather than a fixed 4 GiB cap.
                                else worker_resources
                            ),
                        ),
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "workers",
                        "replicas": 1,
                        "minReplicas": 1,
                        "maxReplicas": max(1, settings.ray_max_workers),
                        # Dataset workers have a two-core limit; advertise both
                        # after bounding summary payloads so two batches can be
                        # processed concurrently without growing driver memory.
                        "rayStartParams": {"num-cpus": "2"} if is_dataset_workload else {},
                        "template": {
                            "metadata": {"labels": deepcopy(workload_labels)},
                            "spec": pod_spec(
                                "ray-worker",
                                worker_resources,
                            ),
                        },
                    }
                ],
            },
        },
    }
