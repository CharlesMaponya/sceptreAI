from __future__ import annotations

import math
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from kubernetes.client import ApiException
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from urllib3.exceptions import HTTPError

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion, ProfilingJob
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
    OutboxStatus,
    RunKind,
    RunStatus,
    WorkflowStage,
)
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowCommand,
    WorkflowEvent,
)
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
    mount_provider_source_configuration,
    object_store_workload_environment,
)
from automl_api.services.upload_policy import inspect_and_scan_stream
from automl_api.services.uploads import require_upload_byte_digest
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_MINIMAL_CREDENTIAL_PROTOCOL,
    RAY_MODEL_OUTPUT_PROTOCOL,
    RAY_PARENT_CONTROL_PROTOCOL,
    RAY_TRAINING_CONTEXT_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    RAY_TRAINING_PROGRESS_PROTOCOL,
    RAY_TRAINING_RESULT_PROTOCOL,
    RAY_TRAINING_TRACKING_PROTOCOL,
    RAY_TRIAL_CONTROL_PROTOCOL,
    StaleFence,
    canonical_request_hash,
    cas_register_terminal_artifact,
    complete_outbox,
    defer_outbox,
    enqueue_outbox,
    transition_attempt,
    transition_command,
)
from automl_api.storage.contracts import ByteRange, DeletionPending, ObjectStoreDriver
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
    "dataset.profile.local": "workflow-reconciler",
    "ray.dataset.split.submit": "workflow-reconciler",
    "ray.dataset.prepare.submit": "workflow-reconciler",
    "ray.dataset.cancel": "workflow-reconciler",
    "ray.training.submit": "workflow-reconciler",
    "ray.training.cancel": "workflow-reconciler",
    "kubernetes.analysis.submit": "workflow-reconciler",
    "kubernetes.analysis.cancel": "workflow-reconciler",
    "kubernetes.analysis.legacy.cancel": "workflow-reconciler",
    "upload.reconcile": "workflow-reconciler",
    "registry.reconcile": "workflow-reconciler",
    "deployment.reconcile": "workflow-reconciler",
    "governance.reconcile": "workflow-reconciler",
    "cleanup.execute": "workflow-reconciler",
    "project.delete": "workflow-reconciler",
    "run.delete": "workflow-reconciler",
}


def _lock_outbox_delivery(db: Session, entry_id: uuid.UUID, worker_id: str) -> OutboxEntry | None:
    with db.no_autoflush:
        entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
    if entry is None or entry.status == OutboxStatus.DELIVERED:
        return None
    if (
        entry.status != OutboxStatus.CLAIMED
        or entry.lease_owner != worker_id
        or entry.lease_expires_at is None
        or entry.lease_expires_at <= datetime.now(UTC)
    ):
        raise StaleFence("The outbox delivery lease is no longer owned by this worker.")
    return entry


def reconcile_entry(
    db: Session,
    entry: OutboxEntry,
    *,
    worker_id: str,
    k8s: KubernetesTrainingClient | None = None,
    handlers: dict[str, Callable[[OutboxEntry], None]] | None = None,
) -> None:
    entry = _lock_outbox_delivery(db, entry.id, worker_id)
    if entry is None:
        return
    if entry.topic in {"kubernetes.analysis.legacy.cancel", "registry.reconcile"}:
        raise RuntimeError("This topic requires the factory-owned dispatcher.")
    try:
        if entry.topic == "dataset.profile.local":
            from automl_api.services.profiling_jobs import run_local_profile

            run_local_profile(db, entry, worker_id)
        elif entry.topic in {"ray.dataset.split.submit", "ray.dataset.prepare.submit"}:
            submit_dataset_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "ray.dataset.cancel":
            cancel_dataset_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "ray.training.submit":
            submit_training_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "ray.training.cancel":
            cancel_training_ray_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "kubernetes.analysis.submit":
            if submit_analysis_job(db, entry, k8s or KubernetesTrainingClient()) is None:
                defer_outbox(db, entry, worker_id=worker_id)
                return
        elif entry.topic == "kubernetes.analysis.cancel":
            cancel_analysis_job(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "upload.reconcile":
            reconcile_upload_session(db, entry)
        elif entry.topic == "deployment.reconcile":
            reconcile_deployment(db, entry, k8s or KubernetesTrainingClient())
        elif entry.topic == "governance.reconcile":
            reconcile_governance_object(db, entry)
        elif entry.topic in {"project.delete", "run.delete"}:
            from automl_api.services.deletion import reconcile_deletion

            project_deletion = entry.topic == "project.delete"
            if not reconcile_deletion(db, entry, k8s or KubernetesTrainingClient()):
                defer_outbox(db, entry, worker_id=worker_id)
                return
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
    except StaleFence:
        db.rollback()
        raise
    except DeletionPending as exc:
        defer_outbox(db, entry, worker_id=worker_id, delay_seconds=exc.retry_after_seconds)
        entry.last_error = redact_text(str(exc))
        db.flush()
        return
    except Exception as exc:
        entry_id = entry.id
        deleting_project_id = (
            entry.project_id if entry.topic in {"project.delete", "run.delete"} else None
        )
        db.rollback()
        complete_outbox(
            db,
            entry_id,
            worker_id=worker_id,
            delivered=False,
            error=redact_text(str(exc)),
        )
        if deleting_project_id:
            from automl_api.models.projects import Project

            project = db.get(Project, deleting_project_id)
            if project is not None:
                project.settings = {**project.settings, "deletion_error": redact_text(str(exc))}
        raise
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    command = db.get(WorkflowCommand, entry.command_id)
    if command is not None and command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)
        db.flush()


def submit_analysis_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> ModelRun | None:
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
            return db.get(ModelRun, attempt.model_run_id)
    elif get_settings().environment in {"production", "staging"}:
        return _migrate_first_analysis_delivery(db, entry, k8s)
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
        return run
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED}:
        return run
    if attempt is not None and (
        run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or str(run.tags.get("workflow_attempt_id", "")) != str(attempt.id)
        or manifest["metadata"].get("namespace") != run.k8s_namespace
    ):
        raise StaleFence("The analysis submission no longer owns this run.")
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


def _migrate_first_analysis_delivery(db, entry, k8s):
    now = datetime.now(UTC)
    command = db.get(WorkflowCommand, entry.command_id, populate_existing=True)
    if (
        entry.aggregate_type != "model_run"
        or entry.delivery_attempts != 1
        or entry.status != OutboxStatus.CLAIMED
        or not entry.lease_owner
        or entry.lease_expires_at is None
        or entry.lease_expires_at <= now
        or command is None
        or command.project_id != entry.project_id
        or command.replayed_by is not None
    ):
        raise StaleFence("Ambiguous legacy analysis delivery requires controlled replay.")
    run = db.get(
        ModelRun,
        uuid.UUID(str(entry.payload["run_id"])),
        with_for_update=True,
        populate_existing=True,
    )
    if run is None or run.project_id != entry.project_id or entry.aggregate_id != run.id:
        raise StaleFence("Legacy analysis delivery has invalid run scope.")
    if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED}:
        return run
    if (
        run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or run.status not in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING}
        or run.gpu_requested
        or run.tags.get("desired_state") != "kubernetes_submission_pending"
        or run.tags.get("workflow_attempt_id")
        or run.tags.get("analysis_result_receipt")
        or run.tags.get("artifact_uri")
        or not run.k8s_namespace
        or not run.k8s_job_name
    ):
        raise StaleFence("Legacy analysis is not an unsubmitted, unmoved pending scope.")
    legacy = entry.payload["manifest"]
    meta = legacy.get("metadata") or {}
    labels = meta.get("labels") or {}
    if (
        meta.get("name") != run.k8s_job_name
        or meta.get("namespace") != run.k8s_namespace
        or labels.get("automl.platform/run-id") != str(run.id)
        or labels.get("automl.platform/project-id") != str(run.project_id)
        or legacy.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("labels", {})
        .get("automl.platform/run-id")
        != str(run.id)
    ):
        raise StaleFence("Legacy analysis manifest identity is ambiguous.")
    try:
        k8s.batch.read_namespaced_job(
            name=run.k8s_job_name, namespace=run.k8s_namespace, _request_timeout=5
        )
    except (ApiException, HTTPError) as exc:
        if not isinstance(exc, ApiException) or exc.status != 404:
            entry.last_error = redact_text(f"Legacy analysis preflight unavailable: {exc}")[:2000]
            db.flush()
            return None
    else:
        raise StaleFence("A legacy analysis Job still exists; drain it before controlled replay.")
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
        raise StaleFence("Legacy analysis Pods remain; drain them before controlled replay.")
    attempt = WorkflowAttempt(
        id=uuid.uuid4(),
        project_id=run.project_id,
        model_run_id=run.id,
        stage=WorkflowStage.TRAINING_RUN,
        logical_key=f"analysis-run:{run.id}",
        workload_identity=f"project-{run.project_id}-analysis",
        generation=1,
        fencing_token=uuid.uuid4().hex,
    )
    manifest = k8s.bind_analysis_attempt_manifest(legacy, attempt)
    pod = manifest["spec"]["template"]["spec"]
    pod["containers"][0]["image"] = k8s.settings.training_image
    pod["containers"][0]["imagePullPolicy"] = k8s.settings.training_image_pull_policy
    if k8s.settings.workload_image_pull_secrets:
        pod["imagePullSecrets"] = [
            {"name": name} for name in k8s.settings.workload_image_pull_secrets
        ]
    else:
        pod.pop("imagePullSecrets", None)
    db.add(attempt)
    db.flush()
    evidence = {
        "outbox_id": str(entry.id),
        "legacy_manifest_sha256": canonical_request_hash(legacy),
        "manifest_sha256": canonical_request_hash(manifest),
        "legacy_job_name": run.k8s_job_name,
        "namespace": run.k8s_namespace,
    }
    db.add(
        WorkflowEvent(
            project_id=run.project_id,
            attempt_id=attempt.id,
            sequence=1,
            event_key="analysis.legacy.upgraded",
            event_type="analysis.legacy.upgraded",
            payload=evidence,
            result_digest=canonical_request_hash(evidence),
        )
    )
    entry.payload = {**entry.payload, "manifest": manifest}
    run.k8s_job_name = manifest["metadata"]["name"]
    run.tags = {**run.tags, "workflow_attempt_id": str(attempt.id)}
    # Defer the delivery so tracking and the new manifest commit before any Job creation.
    db.flush()
    return None


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
    expected_digest = require_upload_byte_digest(session)
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
    entry_id, worker_id = entry.id, entry.lease_owner
    session_id = session.id
    project_id = session.project_id
    expected_size = session.byte_size
    filename = getattr(session, "original_filename", "legacy.csv")
    version_id = session.dataset_version_id
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
    if observed_size == expected_size:
        last_heartbeat_at = lease_started_at

        def renew_verification_lease(processed_bytes: int) -> None:
            nonlocal last_heartbeat_at
            observed_at = datetime.now(UTC)
            if (
                processed_bytes < expected_size
                and (observed_at - last_heartbeat_at).total_seconds() < 30
            ):
                return
            if _lock_outbox_delivery(db, entry_id, worker_id) is None:
                raise StaleFence("Upload verification no longer owns an active delivery.")
            with db.no_autoflush:
                db.refresh(session, with_for_update=True)
            if session.lease_owner != worker_id or session.status != "verifying":
                raise StaleFence("Upload verification ownership changed.")
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
        except StaleFence:
            raise
        except (TimeoutError, ValueError) as exc:
            inspection_error = exc
        finally:
            source.close()

    if _lock_outbox_delivery(db, entry_id, worker_id) is None:
        raise StaleFence("Upload verification no longer owns an active delivery.")
    session = db.scalar(
        select(DatasetUploadSession)
        .where(
            DatasetUploadSession.project_id == project_id,
            DatasetUploadSession.id == session_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if session is None:
        raise LookupError("The tracked upload session no longer exists.")
    if session.status in {"expired", "aborted", "quarantined", "failed", "ready"}:
        return session
    if session.lease_owner != worker_id:
        raise StaleFence("Upload verification ownership changed.")
    if observed_size != expected_size:
        _quarantine_upload(
            session,
            f"object byte size mismatch: expected {expected_size}, observed {observed_size}",
            version,
        )
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


def _ray_credential_protocol(entry):
    if "credential_protocol" not in entry.payload:
        return None
    if entry.payload["credential_protocol"] not in (
        RAY_MINIMAL_CREDENTIAL_PROTOCOL,
        RAY_PARENT_CONTROL_PROTOCOL,
        RAY_TRIAL_CONTROL_PROTOCOL,
        RAY_TRAINING_CONTEXT_PROTOCOL,
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
        RAY_TRAINING_PROGRESS_PROTOCOL,
        RAY_TRAINING_RESULT_PROTOCOL,
        RAY_MODEL_OUTPUT_PROTOCOL,
    ):
        raise StaleFence("Ray submission has an unknown credential protocol.")
    return entry.payload["credential_protocol"]


def submit_training_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt_id = uuid.UUID(str(entry.payload["attempt_id"]))
    fencing_token = str(entry.payload["fencing_token"])
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if attempt.fencing_token != fencing_token:
        raise ValueError("The submitted Ray attempt fence does not match the outbox command.")
    if (
        entry.topic != "ray.training.submit"
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.project_id != entry.project_id
        or entry.aggregate_type != "model_run"
        or attempt.model_run_id != entry.aggregate_id
        or attempt.logical_key != f"training-run:{entry.aggregate_id}"
    ):
        raise StaleFence("Training submission scope differs from its attempt.")
    credential_protocol = _ray_credential_protocol(entry)
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
    from automl_api.services.provider_source import validate_attempt_source

    validate_attempt_source(db, attempt, run)
    if attempt.status == AttemptStatus.PENDING:
        transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=fencing_token)
    name = f"sceptre-run-{str(run.id)[:8]}-g{attempt.generation}"
    manifest = build_ray_job_manifest(
        run, attempt, name=name, credential_protocol=credential_protocol
    )
    k8s.ensure_service_account(attempt.workload_identity)
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
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == attempt_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise LookupError("The tracked dataset Ray attempt no longer exists.")
    if attempt.fencing_token != fencing_token:
        raise ValueError("The submitted dataset Ray attempt fence does not match the command.")
    expected_stage = (
        WorkflowStage.SPLITTER
        if entry.topic == "ray.dataset.split.submit"
        else WorkflowStage.PREPARATION
    )
    if (
        entry.topic not in {"ray.dataset.split.submit", "ray.dataset.prepare.submit"}
        or attempt.stage != expected_stage
    ):
        raise ValueError(f"Dataset Ray topic does not match attempt stage {attempt.stage.value}.")
    if (
        attempt.project_id != entry.project_id
        or entry.aggregate_type != "profiling_job"
        or entry.aggregate_id != profile_job_id
        or attempt.logical_key != f"profile:{profile_job_id}:{expected_stage.value}"
    ):
        raise StaleFence("Dataset submission scope differs from its attempt.")
    credential_protocol = _ray_credential_protocol(entry)
    if attempt.status not in {AttemptStatus.PENDING, AttemptStatus.CLAIMED}:
        return attempt
    job = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == attempt.project_id,
            ProfilingJob.id == profile_job_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if job is None or job.dataset_version_id != attempt.dataset_version_id:
        raise LookupError("The dataset Ray attempt has no scoped profiling job.")
    if job.status not in {"queued", "running"} or str(
        job.overview_json.get("workflow_attempt_id", "")
    ) != str(attempt.id):
        raise StaleFence("Dataset submission no longer owns the active profile.")
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == attempt.project_id,
            DatasetVersion.id == attempt.dataset_version_id,
        )
    )
    if version is None:
        raise LookupError("The dataset Ray attempt has no scoped dataset version.")
    from automl_api.services.provider_source import validate_attempt_source

    validate_attempt_source(db, attempt)

    if attempt.status == AttemptStatus.PENDING:
        transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=fencing_token)
    stage_name = expected_stage.value
    name = f"sceptre-{stage_name}-{str(job.id)[:8]}-g{attempt.generation}"
    k8s.ensure_service_account(attempt.workload_identity)
    manifest = build_dataset_ray_job_manifest(
        job, version, attempt, name=name, credential_protocol=credential_protocol
    )
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
        "ray_job_namespace": manifest["metadata"]["namespace"],
    }
    version.status = DatasetStatus.PROFILING
    db.flush()
    return attempt


def cancel_dataset_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(str(entry.payload["attempt_id"])))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt is None
        or attempt.stage not in {WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}
        or attempt.project_id != entry.project_id
        or attempt.fencing_token != str(entry.payload["fencing_token"])
        or entry.aggregate_type != "profiling_job"
        or attempt.status
        not in {
            AttemptStatus.SUPERSEDED,
            AttemptStatus.FAILED,
            AttemptStatus.CANCELLED,
        }
    ):
        raise StaleFence("Dataset cancellation scope, status or fence is stale.")
    profile = _profile_job_for_attempt(db, attempt)
    namespace = entry.payload.get("namespace")
    if profile.id != entry.aggregate_id or not isinstance(namespace, str) or not namespace:
        raise StaleFence("Dataset cancellation profile or namespace is missing.")
    if attempt.ray_job_name:
        try:
            job = k8s.ray_job(attempt.ray_job_name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            metadata = job.get("metadata") or {}
            expected = {
                "automl.platform/project-id": str(attempt.project_id),
                "automl.platform/profile-job-id": str(profile.id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/workflow-stage": attempt.stage.value,
                "automl.platform/generation": str(attempt.generation),
            }
            if (
                not metadata.get("uid")
                or metadata.get("namespace") != namespace
                or any((metadata.get("labels") or {}).get(k) != v for k, v in expected.items())
            ):
                raise StaleFence("Dataset cancellation RayJob identity differs from its attempt.")
            k8s.delete_ray_job(attempt.ray_job_name, namespace=namespace, uid=metadata["uid"])
    attempt.cleanup_state = "completed"
    db.flush()
    return attempt


def cancel_training_ray_job(
    db: Session, entry: OutboxEntry, k8s: KubernetesTrainingClient
) -> WorkflowAttempt:
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(str(entry.payload["attempt_id"])))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if (
        attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.fencing_token != str(entry.payload["fencing_token"])
        or attempt.project_id != entry.project_id
        or entry.aggregate_type != "model_run"
        or attempt.model_run_id != entry.aggregate_id
    ):
        raise StaleFence("The Ray cancellation scope or fence is stale.")
    if attempt.ray_job_name:
        run = db.get(ModelRun, attempt.model_run_id, populate_existing=True)
        if run is None or run.project_id != attempt.project_id or not run.k8s_namespace:
            raise StaleFence("The Ray cancellation run namespace is missing or inconsistent.")
        try:
            job = k8s.ray_job(attempt.ray_job_name, namespace=run.k8s_namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
        else:
            metadata = job.get("metadata") if isinstance(job, dict) else None
            metadata = metadata if isinstance(metadata, dict) else {}
            labels = metadata.get("labels")
            uid = metadata.get("uid")
            expected = {
                "automl.platform/project-id": str(attempt.project_id),
                "automl.platform/run-id": str(run.id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/generation": str(attempt.generation),
            }
            if (
                not isinstance(uid, str)
                or not uid.strip()
                or metadata.get("name") != attempt.ray_job_name
                or metadata.get("namespace") != run.k8s_namespace
                or not isinstance(labels, dict)
                or any(labels.get(key) != value for key, value in expected.items())
            ):
                raise StaleFence("Training cancellation RayJob identity differs from its attempt.")
            k8s.delete_ray_job(attempt.ray_job_name, namespace=run.k8s_namespace, uid=uid)
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


@dataclass(frozen=True)
class JobObservation:
    attempt_id: uuid.UUID
    snapshot_digest: str
    job_name: str
    namespace: str | None
    result: Any | None = None


def _job_observation(attempt, resource, model, job_name, namespace, *, evidence=None):
    return JobObservation(
        attempt_id=attempt.id,
        snapshot_digest=canonical_request_hash(
            {
                "rows": [
                    {column.key: getattr(row, column.key, None) for column in table.columns}
                    for row, table in (
                        (attempt, WorkflowAttempt.__table__),
                        (resource, model.__table__),
                    )
                ],
                "evidence": evidence,
            }
        ),
        job_name=job_name,
        namespace=namespace,
    )


def inspect_ray_observation(k8s, request: JobObservation) -> JobObservation:
    try:
        result = k8s.ray_job(request.job_name, namespace=request.namespace)
    except ApiException as exc:
        if exc.status != 404:
            raise
        result = None
    return replace(request, result=deepcopy(result))


def _verify_ray_observation(job, attempt, resource_id, namespace):
    metadata = job.get("metadata") if isinstance(job, dict) else None
    metadata = metadata if isinstance(metadata, dict) else {}
    labels, uid = metadata.get("labels"), metadata.get("uid")
    spec = job.get("spec") if isinstance(job, dict) else None
    expected = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
    }
    if attempt.stage == WorkflowStage.TRAINING_RUN:
        expected["automl.platform/run-id"] = str(resource_id)
    else:
        expected.update(
            {
                "automl.platform/profile-job-id": str(resource_id),
                "automl.platform/workflow-stage": attempt.stage.value,
                "automl.platform/dataset-version-id": str(attempt.dataset_version_id),
            }
        )
    if (
        not isinstance(namespace, str)
        or not namespace
        or not isinstance(attempt.ray_job_name, str)
        or not attempt.ray_job_name.strip()
        or metadata.get("namespace") != namespace
        or metadata.get("name") != attempt.ray_job_name
        or not isinstance(uid, str)
        or not uid.strip()
        or not isinstance(labels, dict)
        or any(labels.get(key) != value for key, value in expected.items())
        or not isinstance(spec, dict)
        or spec.get("jobId") != f"sceptre-{attempt.fencing_token[:16]}"
    ):
        raise StaleFence("Ray observation identity differs from its current attempt.")
    try:
        pod = spec["submitterPodTemplate"]["spec"]
        env = pod["containers"][0]["env"]
        expected_env = {
            "AUTOML_FENCING_TOKEN": attempt.fencing_token,
            "AUTOML_PROJECT_ID": str(attempt.project_id),
            "AUTOML_ATTEMPT_ID": str(attempt.id),
        }
        if (
            pod["serviceAccountName"] != attempt.workload_identity
            or len(pod["containers"]) != 1
            or not isinstance(env, list)
            or any(
                [item.get("value") for item in env if item.get("name") == name] != [value]
                for name, value in expected_env.items()
            )
        ):
            raise StaleFence("Ray submitter fence or stage identity differs from its attempt.")
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        raise StaleFence("Ray submitter identity is missing or malformed.") from exc


def observe_training_ray_jobs(
    db: Session,
    *,
    read: Callable[[JobObservation], JobObservation | None],
    limit: int = 25,
    attempt_id: uuid.UUID | None = None,
) -> int:
    """Collect native read requests or publish an unchanged, freshly locked observation."""
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.stage == "training_run",
                WorkflowAttempt.status.in_({AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}),
                WorkflowAttempt.ray_job_name.is_not(None),
            )
            .where(WorkflowAttempt.id == attempt_id if attempt_id is not None else True)
            .order_by(WorkflowAttempt.updated_at, WorkflowAttempt.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    )
    for attempt in attempts:
        run = db.scalar(
            select(ModelRun)
            .where(ModelRun.id == attempt.model_run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if run is None:
            raise LookupError("The lost Ray attempt has no tracked training run.")
        if run.project_id != attempt.project_id:
            raise StaleFence("Ray observation run scope differs from its attempt.")
        if run.run_kind != RunKind.TRAINING:
            continue
        receipt = (run.tags or {}).get("training_result_receipt") if run is not None else None
        if (
            run is not None
            and run.status == RunStatus.RUNNING
            and receipt
            and receipt.get("attempt_id") == str(attempt.id)
            and run.tags.get("winner_model_artifact_uri")
        ):
            from automl_api.services.training_result import require_training_result_receipt

            require_training_result_receipt(db, attempt, run)
            # The result transaction is authoritative even if its worker lost the commit response.
            if not cas_register_terminal_artifact(
                db,
                attempt_id=attempt.id,
                fencing_token=attempt.fencing_token,
                expected_cas_version=attempt.terminal_cas_version,
                checkpoint_uri=str(run.tags["winner_model_artifact_uri"]),
            ):
                raise StaleFence("The recovered training result no longer owns this attempt.")
            run.status = RunStatus.SUCCEEDED
            run.finished_at = datetime.now(UTC)
            continue
        if run is not None and run.status in {
            RunStatus.CANCELLED,
            RunStatus.FAILED,
            RunStatus.SUCCEEDED,
        }:
            continue
        expires = getattr(attempt, "lease_expires_at", None)
        if expires is not None and expires <= datetime.now(UTC):
            command = _training_launch_command(db, attempt)
            enqueue_outbox(
                db,
                command,
                topic="ray.training.cancel",
                aggregate_type="model_run",
                aggregate_id=attempt.model_run_id,
                payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
                event_key=f"ray:{attempt.model_run_id}:generation:{attempt.generation}:expired:cancel",
            )
            _replace_lost_ray_attempt(db, attempt, reason="Training parent lease expired")
            continue
        request = _job_observation(
            attempt,
            run,
            ModelRun,
            str(attempt.ray_job_name),
            getattr(run, "k8s_namespace", None),
        )
        observed = read(request)
        if observed is None or replace(observed, result=None) != request:
            continue
        ray_job = observed.result
        if ray_job is None:
            _replace_lost_ray_attempt(db, attempt, reason="RayJob disappeared before terminal CAS")
            continue

        _verify_ray_observation(ray_job, attempt, run.id, request.namespace)
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
                    "outofmemory",
                    "out of memory",
                    "oomkill",
                    "oom kill",
                    "running low on memory",
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
                terminal_failure_code=(
                    "JOB_DEADLINE_EXCEEDED"
                    if deadline_exceeded
                    else "TRAINING_OUT_OF_MEMORY"
                    if out_of_memory
                    else None
                ),
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


def _analysis_submission(db, attempt):
    event = db.scalar(
        select(WorkflowEvent)
        .where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == "analysis.submitted",
        )
        .execution_options(populate_existing=True)
    )
    if (
        event is None
        or event.project_id != attempt.project_id
        or event.event_type != "analysis.submitted"
        or event.result_digest != canonical_request_hash(event.payload)
        or any(
            not isinstance(event.payload.get(k), str) or not event.payload[k]
            for k in ("namespace", "job_name", "uid", "outbox_id", "manifest_sha256")
        )
    ):
        raise StaleFence("Analysis submission evidence is missing or changed.")
    source = db.get(OutboxEntry, uuid.UUID(event.payload["outbox_id"]), populate_existing=True)
    if (
        source is None
        or source.project_id != attempt.project_id
        or source.topic != "kubernetes.analysis.submit"
        or source.aggregate_type != "model_run"
        or source.aggregate_id != attempt.model_run_id
        or canonical_request_hash(source.payload.get("manifest", {}))
        != event.payload["manifest_sha256"]
    ):
        raise StaleFence("Analysis source manifest no longer matches its submission evidence.")
    return event.payload, source


def _queue_analysis_cleanup(db, attempt, identity, source):
    command = db.get(WorkflowCommand, source.command_id)
    if command is None or command.project_id != attempt.project_id:
        raise StaleFence("Analysis cleanup has no scoped durable command.")
    enqueue_outbox(
        db,
        command,
        topic="kubernetes.analysis.cancel",
        aggregate_type="model_run",
        aggregate_id=attempt.model_run_id,
        payload={
            "attempt_id": str(attempt.id),
            "fencing_token": attempt.fencing_token,
            **{k: identity[k] for k in ("namespace", "job_name", "uid")},
        },
        event_key=f"analysis:{attempt.id}:cleanup",
    )
    attempt.cleanup_state = "queued"


def cancel_analysis_job(db, entry, k8s):
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(entry.payload["attempt_id"]))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt is None
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.logical_key != f"analysis-run:{attempt.model_run_id}"
        or attempt.project_id != entry.project_id
        or entry.aggregate_type != "model_run"
        or attempt.model_run_id != entry.aggregate_id
        or attempt.fencing_token != entry.payload["fencing_token"]
        or attempt.status
        not in {
            AttemptStatus.SUPERSEDED,
            AttemptStatus.FAILED,
            AttemptStatus.CANCELLED,
            AttemptStatus.SUCCEEDED,
        }
    ):
        raise StaleFence("Analysis cleanup scope, status or fence is stale.")
    identity, _ = _analysis_submission(db, attempt)
    if any(entry.payload.get(k) != identity[k] for k in ("namespace", "job_name", "uid")):
        raise StaleFence("Analysis cleanup identity differs from its recorded Job.")
    try:
        job = k8s.batch.read_namespaced_job(
            name=identity["job_name"], namespace=identity["namespace"], _request_timeout=5
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
    else:
        _verify_analysis_job(job, attempt, identity)
        try:
            k8s.delete_job(
                identity["job_name"], namespace=identity["namespace"], uid=identity["uid"]
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
    attempt.cleanup_state = "completed"
    db.flush()
    return attempt


def _verify_analysis_job(job, attempt, identity):
    expected = {
        "project-id": str(attempt.project_id),
        "run-id": str(attempt.model_run_id),
        "attempt-id": str(attempt.id),
        "generation": str(attempt.generation),
        "workflow-stage": attempt.stage.value,
    }
    if (
        job.metadata.uid != identity["uid"]
        or job.metadata.namespace != identity["namespace"]
        or job.metadata.name != identity["job_name"]
        or any(
            (job.metadata.labels or {}).get(f"automl.platform/{k}") != v
            for k, v in expected.items()
        )
    ):
        raise StaleFence("The physical analysis Job no longer matches this attempt.")


def _replace_analysis_attempt(db, attempt, run, source, k8s, reason):
    reason = redact_text(reason)[:2000]
    exhausted = attempt.generation >= attempt.retry_budget
    transition_attempt(
        attempt,
        AttemptStatus.FAILED if exhausted else AttemptStatus.SUPERSEDED,
        fencing_token=attempt.fencing_token,
        expected_cas_version=attempt.terminal_cas_version,
        terminal_reason=reason,
    )
    if exhausted:
        run.status = RunStatus.FAILED
        run.failure_code, run.failure_message = "ANALYSIS_RETRY_EXHAUSTED", reason
        run.finished_at = datetime.now(UTC)
        return
    replacement = WorkflowAttempt(
        project_id=attempt.project_id,
        **_retry_lineage(attempt),
        stage=attempt.stage,
        logical_key=attempt.logical_key,
        workload_identity=attempt.workload_identity,
        generation=attempt.generation + 1,
        fencing_token=uuid.uuid4().hex,
        predecessor_attempt_id=attempt.id,
        retry_budget=attempt.retry_budget,
    )
    db.add(replacement)
    db.flush()
    manifest = k8s.bind_analysis_attempt_manifest(source.payload["manifest"], replacement)
    command = db.get(WorkflowCommand, source.command_id)
    enqueue_outbox(
        db,
        command,
        topic="kubernetes.analysis.submit",
        aggregate_type="model_run",
        aggregate_id=run.id,
        payload={"run_id": str(run.id), "manifest": manifest},
        event_key=f"analysis:{replacement.id}:submit",
    )
    run.k8s_job_name = manifest["metadata"]["name"]
    run.status, run.finished_at = RunStatus.QUEUED, None
    run.failure_code = run.failure_message = run.plain_english_failure = None
    run.tags = {
        **run.tags,
        "workflow_attempt_id": str(replacement.id),
        "desired_state": "kubernetes_submission_pending",
        "waiting_for_capacity": False,
    }


def inspect_analysis_observation(k8s, request: JobObservation) -> JobObservation:
    try:
        result = k8s.batch.read_namespaced_job(
            name=request.job_name,
            namespace=request.namespace,
            _request_timeout=5,
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
        result = None
    return replace(request, result=deepcopy(result))


def observe_analysis_attempts(
    db,
    k8s,
    *,
    read: Callable[[JobObservation], JobObservation | None],
    limit=25,
    attempt_id: uuid.UUID | None = None,
):
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .join(
                WorkflowEvent,
                (WorkflowEvent.attempt_id == WorkflowAttempt.id)
                & (WorkflowEvent.event_key == "analysis.submitted"),
            )
            .where(
                WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
                WorkflowAttempt.logical_key.like("analysis-run:%"),
                (WorkflowAttempt.status.in_({AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}))
                | (
                    (
                        WorkflowAttempt.status.in_(
                            {
                                AttemptStatus.SUCCEEDED,
                                AttemptStatus.FAILED,
                                AttemptStatus.CANCELLED,
                                AttemptStatus.SUPERSEDED,
                            }
                        )
                    )
                    & (WorkflowAttempt.cleanup_state == "pending")
                ),
            )
            .where(WorkflowAttempt.id == attempt_id if attempt_id is not None else True)
            .order_by(WorkflowAttempt.updated_at, WorkflowAttempt.id)
            .limit(limit)
            .with_for_update(of=WorkflowAttempt, skip_locked=True)
            .execution_options(populate_existing=True)
        )
    )
    for attempt in attempts:
        run = db.get(ModelRun, attempt.model_run_id, with_for_update=True, populate_existing=True)
        if (
            run is None
            or run.project_id != attempt.project_id
            or run.run_kind
            not in {
                RunKind.VALIDATION,
                RunKind.EXPLAINABILITY,
                RunKind.DRIFT,
            }
        ):
            raise StaleFence("Analysis observer run scope changed.")
        identity, source = _analysis_submission(db, attempt)
        if attempt.status in {
            AttemptStatus.SUCCEEDED,
            AttemptStatus.FAILED,
            AttemptStatus.CANCELLED,
            AttemptStatus.SUPERSEDED,
        }:
            _queue_analysis_cleanup(db, attempt, identity, source)
            continue
        if str(run.tags.get("workflow_attempt_id", "")) != str(attempt.id) or run.status in {
            RunStatus.CANCELLED,
            RunStatus.FAILED,
        }:
            transition_attempt(
                attempt,
                AttemptStatus.CANCELLED,
                fencing_token=attempt.fencing_token,
                expected_cas_version=attempt.terminal_cas_version,
                terminal_reason="Analysis run no longer admits this attempt",
            )
            _queue_analysis_cleanup(db, attempt, identity, source)
            continue
        receipt = (run.tags or {}).get("analysis_result_receipt")
        if receipt is not None or run.status == RunStatus.SUCCEEDED:
            # Reports and CAS commit together. Never rerun an inconsistent committed scope.
            raise StaleFence("Analysis receipt and terminal attempt state are inconsistent.")
        now = datetime.now(UTC)
        reason = None
        if attempt.lease_expires_at is not None and attempt.lease_expires_at <= now:
            reason = "Analysis worker lease expired"
        elif (
            attempt.lease_expires_at is None
            and attempt.updated_at < now - RAYJOB_STARTUP_FAILURE_GRACE
        ):
            reason = "Analysis worker did not claim its startup lease"
        if reason is None:
            request = _job_observation(
                attempt,
                run,
                ModelRun,
                identity["job_name"],
                identity["namespace"],
                evidence={
                    "submission": identity,
                    "source": {
                        column.key: getattr(source, column.key)
                        for column in OutboxEntry.__table__.columns
                    },
                },
            )
            observed = read(request)
            if observed is None or replace(observed, result=None) != request:
                continue
            job = observed.result
            if job is None:
                reason = "Analysis Job disappeared before report CAS"
            else:
                _verify_analysis_job(job, attempt, identity)
                conditions = job.status.conditions or [] if job.status else []
                if any(
                    c.type in {"Failed", "Complete"} and c.status == "True" for c in conditions
                ) or (job.status and (job.status.failed or job.status.succeeded)):
                    reason = "Analysis Job terminated before report CAS"
        if reason is not None:
            _queue_analysis_cleanup(db, attempt, identity, source)
            _replace_analysis_attempt(db, attempt, run, source, k8s, reason)
    db.flush()
    return len(attempts)


def observe_analysis_jobs(db: Session, *, read, limit: int = 25, run_id=None) -> int:
    from automl_api.services.legacy_analysis import observe_jobs

    return observe_jobs(db, read=read, limit=limit, run_id=run_id)


def observe_dataset_ray_jobs(
    db: Session,
    *,
    read: Callable[[JobObservation], JobObservation | None],
    limit: int = 25,
    attempt_id: uuid.UUID | None = None,
) -> int:
    attempts = list(
        db.scalars(
            select(WorkflowAttempt)
            .where(
                WorkflowAttempt.stage.in_({WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}),
                WorkflowAttempt.status.in_({AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}),
                WorkflowAttempt.ray_job_name.is_not(None),
            )
            .where(WorkflowAttempt.id == attempt_id if attempt_id is not None else True)
            .order_by(WorkflowAttempt.updated_at, WorkflowAttempt.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    )
    for attempt in attempts:
        profile = _profile_job_for_attempt(db, attempt)
        if profile.status not in {"queued", "running"} or str(
            profile.overview_json.get("workflow_attempt_id", "")
        ) != str(attempt.id):
            continue
        # shortcut: legacy submissions use the configured namespace; retire after their jobs drain.
        namespace = (
            profile.overview_json.get("ray_job_namespace") or get_settings().training_namespace
        )
        expires = attempt.lease_expires_at
        if expires is not None and expires <= datetime.now(UTC):
            command = _dataset_profile_command(db, attempt, profile)
            enqueue_outbox(
                db,
                command,
                topic="ray.dataset.cancel",
                aggregate_type="profiling_job",
                aggregate_id=profile.id,
                payload={
                    "attempt_id": str(attempt.id),
                    "fencing_token": attempt.fencing_token,
                    "namespace": namespace,
                },
                event_key=f"profile:{profile.id}:{attempt.stage.value}:g{attempt.generation}:expired:cancel",
            )
            _replace_lost_dataset_attempt(db, attempt, reason="Dataset worker lease expired")
            continue
        request = _job_observation(
            attempt, profile, ProfilingJob, str(attempt.ray_job_name), namespace
        )
        observed = read(request)
        if observed is None or replace(observed, result=None) != request:
            continue
        ray_job = observed.result
        if ray_job is None:
            _replace_lost_dataset_attempt(
                db, attempt, reason="Dataset RayJob disappeared before terminal CAS"
            )
            continue
        _verify_ray_observation(ray_job, attempt, profile.id, namespace)
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
    parts = attempt.logical_key.split(":")
    if len(parts) != 3 or parts[0] != "profile" or parts[2] != attempt.stage.value:
        raise LookupError("The dataset attempt logical key is not profile-scoped.")
    profile_id = uuid.UUID(parts[1])
    profile = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == attempt.project_id,
            ProfilingJob.id == profile_id,
            ProfilingJob.dataset_version_id == attempt.dataset_version_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
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

    from automl_api.services.provider_source import validate_attempt_source

    validate_attempt_source(db, attempt)
    command = _dataset_profile_command(db, attempt, profile)
    replacement = WorkflowAttempt(
        project_id=attempt.project_id,
        stage=attempt.stage,
        logical_key=attempt.logical_key,
        **_retry_lineage(attempt),
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


def _dataset_profile_command(db, attempt, profile):
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
    return command


def _training_launch_command(db, attempt):
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == attempt.project_id,
            WorkflowCommand.resource_type == "model_run",
            WorkflowCommand.resource_id == attempt.model_run_id,
            WorkflowCommand.operation == "training.launch",
        )
        .order_by(WorkflowCommand.created_at.desc())
        .limit(1)
        .with_for_update()
    )
    if command is None:
        raise LookupError("The lost Ray attempt has no durable launch command.")
    return command


def _retry_lineage(attempt):
    return {
        name: getattr(attempt, name, None)
        for name in (
            "dataset_version_id",
            "model_run_id",
            "trial_id",
            "scope_id",
            "conformance_campaign_id",
        )
    }


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
    trial_attempts = list(
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
                        AttemptStatus.SUCCEEDED,
                        AttemptStatus.FAILED,
                    }
                ),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    active_trial_attempts = [
        child
        for child in trial_attempts
        if child.status
        in {
            AttemptStatus.PENDING,
            AttemptStatus.CLAIMED,
            AttemptStatus.SUBMITTED,
            AttemptStatus.RUNNING,
        }
    ]
    terminal = (
        terminal_failure_code is not None
        or attempt.generation >= attempt.retry_budget
        or any(child.generation >= child.retry_budget for child in active_trial_attempts)
    )
    transition_attempt(
        attempt,
        AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED,
        fencing_token=attempt.fencing_token,
        terminal_reason=reason,
        expected_cas_version=attempt.terminal_cas_version,
    )
    for trial_attempt in active_trial_attempts:
        target = AttemptStatus.SUPERSEDED
        if terminal:
            target = (
                AttemptStatus.CANCELLED
                if trial_attempt.status == AttemptStatus.PENDING
                else AttemptStatus.FAILED
            )
        transition_attempt(
            trial_attempt,
            target,
            fencing_token=trial_attempt.fencing_token,
            terminal_reason=reason,
            expected_cas_version=trial_attempt.terminal_cas_version,
        )
        if terminal:
            trial = db.get(TrainingTrial, trial_attempt.trial_id)
            if trial is not None:
                trial.status = trial_attempt.status.value
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

    command = _training_launch_command(db, attempt)
    from automl_api.services.training_trials import authorized_trial_result, trial_result_reference

    result_allowlist = [
        trial_result_reference(db, attempt, child)
        for child in trial_attempts
        if child.status in {AttemptStatus.SUCCEEDED, AttemptStatus.FAILED}
    ]
    from automl_api.services.training_model_output import training_model_allowlist

    model_allowlist = training_model_allowlist(db, attempt, run)
    authorized_trials = {reference["trial_id"] for reference in result_allowlist}
    for reference in attempt.predecessor_checkpoint_allowlist or []:
        if not isinstance(reference, dict) or reference.get("kind") != "trial_result":
            continue
        if reference.get("trial_id") in authorized_trials:
            continue
        try:
            source_id = uuid.UUID(reference["attempt_id"])
        except (KeyError, ValueError, TypeError) as exc:
            raise StaleFence("The inherited trial result identity is invalid.") from exc
        source = db.get(WorkflowAttempt, source_id, populate_existing=True)
        if source is None or authorized_trial_result(db, attempt, source) != reference:
            raise StaleFence("The inherited trial result authorization is invalid.")
        result_allowlist.append(reference)
        authorized_trials.add(reference["trial_id"])
    replacement = WorkflowAttempt(
        project_id=attempt.project_id,
        stage=attempt.stage,
        logical_key=attempt.logical_key,
        **_retry_lineage(attempt),
        workload_identity=attempt.workload_identity,
        generation=attempt.generation + 1,
        fencing_token=uuid.uuid4().hex,
        predecessor_attempt_id=attempt.id,
        predecessor_checkpoint_allowlist=(
            ([attempt.checkpoint_uri] if attempt.checkpoint_uri else [])
            + result_allowlist
            + model_allowlist
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
                **_retry_lineage(trial_attempt),
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


def build_ray_job_manifest(
    run: ModelRun, attempt: WorkflowAttempt, *, name: str, credential_protocol: str | None = None
) -> dict[str, Any]:
    from automl_api.training.attempt_admission import TRAINING_PROTOCOL

    settings = get_settings()
    environment = [
        {"name": "AUTOML_RUN_ID", "value": str(run.id)},
        {"name": "TRAINING_EXECUTION_MODE", "value": "ray"},
        {"name": "AUTOML_MODEL_PODS", "value": "1"},
        {"name": "AUTOML_TUNE_CONCURRENCY", "value": str(settings.training_max_concurrent_trials)},
        {
            "name": "AUTOML_CPU_THREADS",
            "value": str(run.cpu_limit_cores or settings.training_cpu_limit_cores),
        },
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
        entrypoint=(
            f"python -m automl_api.training.worker --run-id {run.id} "
            f"--attempt-protocol {TRAINING_PROTOCOL}"
        ),
        environment=environment,
        labels=labels,
        worker_resources=worker_resources,
        credential_protocol=credential_protocol,
    )
    cluster = manifest["spec"]["rayClusterSpec"]
    cluster["headGroupSpec"]["rayStartParams"]["num-cpus"] = "0"
    workers = cluster["workerGroupSpecs"][0]
    # Ray's start CLI accepts integer CPUs, even when Kubernetes uses a
    # fractional quota. The task still reserves the full pod quota.
    workers["rayStartParams"]["num-cpus"] = str(math.ceil(float(worker_resources["limits"]["cpu"])))
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
    credential_protocol: str | None = None,
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
        credential_protocol=credential_protocol,
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
    credential_protocol: str | None = None,
) -> dict[str, Any]:
    if credential_protocol not in (
        None,
        RAY_MINIMAL_CREDENTIAL_PROTOCOL,
        RAY_PARENT_CONTROL_PROTOCOL,
        RAY_TRIAL_CONTROL_PROTOCOL,
        RAY_TRAINING_CONTEXT_PROTOCOL,
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
        RAY_TRAINING_PROGRESS_PROTOCOL,
        RAY_TRAINING_RESULT_PROTOCOL,
        RAY_MODEL_OUTPUT_PROTOCOL,
    ):
        raise ValueError("Ray submission has an unknown credential protocol.")
    settings = get_settings()
    from automl_api.services.attempt_identity import attempt_audience
    from automl_api.training.attempt_admission import TOKEN_PATH, control_base_url

    admission_url = settings.attempt_control_base_url
    if admission_url:
        admission_url = control_base_url(admission_url)
    elif settings.environment in {"production", "staging"}:
        raise ValueError("Production Ray workloads require an HTTPS attempt admission URL.")
    if (
        credential_protocol
        in (
            RAY_TRAINING_RESULT_PROTOCOL,
            RAY_TRAINING_PROGRESS_PROTOCOL,
            RAY_TRAINING_TRACKING_PROTOCOL,
            RAY_TRAINING_METADATA_PROTOCOL,
            RAY_CREDENTIAL_PROTOCOL,
        )
        and attempt.stage == WorkflowStage.TRAINING_RUN
        and not admission_url
    ):
        raise ValueError("Declared training result publication requires an HTTPS control URL")
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
    storage_environment = object_store_workload_environment(settings)
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
        *storage_environment,
        *environment,
    ]
    common_environment.extend(
        [
            {"name": "ENVIRONMENT", "value": settings.environment},
            {"name": "ATTEMPT_CONTROL_BASE_URL", "value": admission_url},
            {"name": "AUTOML_IDENTITY_TOKEN_FILE", "value": TOKEN_PATH},
        ]
    )
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

    volumes.append(
        {
            "name": "attempt-identity",
            "projected": {
                "defaultMode": 0o444,
                "sources": [
                    {
                        "serviceAccountToken": {
                            "path": "token",
                            "expirationSeconds": 600,
                            "audience": attempt_audience(attempt.id, attempt.fencing_token),
                        }
                    }
                ],
            },
        }
    )

    def pod_spec(container_name: str, resources: dict[str, dict[str, str]]) -> dict[str, Any]:
        excluded = set()
        if credential_protocol in (
            RAY_MINIMAL_CREDENTIAL_PROTOCOL,
            RAY_PARENT_CONTROL_PROTOCOL,
            RAY_TRIAL_CONTROL_PROTOCOL,
            RAY_TRAINING_CONTEXT_PROTOCOL,
            RAY_TRAINING_TRACKING_PROTOCOL,
            RAY_TRAINING_METADATA_PROTOCOL,
            RAY_CREDENTIAL_PROTOCOL,
            RAY_TRAINING_PROGRESS_PROTOCOL,
            RAY_TRAINING_RESULT_PROTOCOL,
            RAY_MODEL_OUTPUT_PROTOCOL,
        ):
            if container_name == "ray-job-submitter":
                excluded = {"DATABASE_URL", "MLFLOW_TRACKING_URI"} | {
                    item["name"] for item in storage_environment
                }
            elif is_dataset_workload and container_name == "ray-worker":
                excluded = {"DATABASE_URL"}
        spec = {
            "serviceAccountName": attempt.workload_identity,
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": container_name,
                    "image": settings.training_image,
                    "imagePullPolicy": settings.training_image_pull_policy,
                    "env": [
                        deepcopy(item)
                        for item in common_environment
                        if item["name"] not in excluded
                    ],
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
                        {
                            "name": "attempt-identity",
                            "mountPath": "/var/run/sceptre/identity",
                            "readOnly": True,
                        },
                    ],
                }
            ],
            "volumes": deepcopy(volumes),
        }
        if (
            credential_protocol
            in (
                RAY_PARENT_CONTROL_PROTOCOL,
                RAY_TRIAL_CONTROL_PROTOCOL,
                RAY_TRAINING_CONTEXT_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_RESULT_PROTOCOL,
                RAY_MODEL_OUTPUT_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name == "ray-head"
            and admission_url
        ):
            from automl_api.schemas.parent_control import PARENT_CONTROL_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_PARENT_CONTROL_PROTOCOL", "value": PARENT_CONTROL_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_TRIAL_CONTROL_PROTOCOL,
                RAY_TRAINING_CONTEXT_PROTOCOL,
                RAY_MODEL_OUTPUT_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_RESULT_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name in {"ray-head", "ray-worker"}
            and admission_url
        ):
            from automl_api.schemas.trial_control import TRIAL_CONTROL_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRIAL_CONTROL_PROTOCOL", "value": TRIAL_CONTROL_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_TRAINING_CONTEXT_PROTOCOL,
                RAY_MODEL_OUTPUT_PROTOCOL,
                RAY_TRAINING_RESULT_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name == "ray-head"
            and admission_url
        ):
            from automl_api.schemas.training_context import TRAINING_CONTEXT_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRAINING_CONTEXT_PROTOCOL", "value": TRAINING_CONTEXT_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_MODEL_OUTPUT_PROTOCOL,
                RAY_TRAINING_RESULT_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name in {"ray-head", "ray-worker"}
            and admission_url
        ):
            from automl_api.training.model_output import MODEL_OUTPUT_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_MODEL_OUTPUT_PROTOCOL", "value": MODEL_OUTPUT_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_TRAINING_RESULT_PROTOCOL,
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name == "ray-head"
            and admission_url
        ):
            from automl_api.schemas.training_result import TRAINING_RESULT_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRAINING_RESULT_PROTOCOL", "value": TRAINING_RESULT_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_TRAINING_PROGRESS_PROTOCOL,
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name in {"ray-head", "ray-worker"}
            and admission_url
        ):
            from automl_api.schemas.training_progress import TRAINING_PROGRESS_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRAINING_PROGRESS_PROTOCOL", "value": TRAINING_PROGRESS_PROTOCOL}
            )
        if (
            credential_protocol
            in (
                RAY_TRAINING_TRACKING_PROTOCOL,
                RAY_TRAINING_METADATA_PROTOCOL,
                RAY_CREDENTIAL_PROTOCOL,
            )
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name in {"ray-head", "ray-worker"}
            and admission_url
        ):
            from automl_api.schemas.training_tracking import TRAINING_TRACKING_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRAINING_TRACKING_PROTOCOL", "value": TRAINING_TRACKING_PROTOCOL}
            )
        if (
            credential_protocol in (RAY_TRAINING_METADATA_PROTOCOL, RAY_CREDENTIAL_PROTOCOL)
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name in {"ray-head", "ray-worker"}
            and admission_url
        ):
            from automl_api.schemas.training_metadata import TRAINING_METADATA_PROTOCOL

            spec["containers"][0]["env"].append(
                {"name": "AUTOML_TRAINING_METADATA_PROTOCOL", "value": TRAINING_METADATA_PROTOCOL}
            )
        if (
            credential_protocol == RAY_CREDENTIAL_PROTOCOL
            and attempt.stage == WorkflowStage.TRAINING_RUN
            and container_name == "ray-head"
            and admission_url
        ):
            from automl_api.schemas.training_model_read import TRAINING_MODEL_READ_PROTOCOL

            spec["containers"][0]["env"].append(
                {
                    "name": "AUTOML_TRAINING_MODEL_READ_PROTOCOL",
                    "value": TRAINING_MODEL_READ_PROTOCOL,
                }
            )
        mount_provider_source_configuration(spec)
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
                        "numOfHosts": 1,
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
