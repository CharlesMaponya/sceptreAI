from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.models.enums import AttemptStatus, CommandStatus, RunStatus
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowAttempt,
    WorkflowCommand,
)
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import (
    complete_outbox,
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
        complete_outbox(
            db,
            entry.id,
            worker_id=worker_id,
            delivered=False,
            error=str(exc),
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


def build_ray_job_manifest(run: ModelRun, attempt: WorkflowAttempt, *, name: str) -> dict[str, Any]:
    settings = get_settings()
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
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": 300,
            "entrypoint": f"python -m automl_api.training.worker --run-id {run.id}",
            "rayClusterSpec": {
                "rayVersion": "2.56.1",
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "rayStartParams": {"dashboard-host": "0.0.0.0"},
                    "template": {
                        "spec": {
                            "serviceAccountName": attempt.workload_identity,
                            "containers": [{"name": "ray-head", "image": settings.training_image}],
                        }
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
                            "spec": {
                                "serviceAccountName": attempt.workload_identity,
                                "containers": [
                                    {"name": "ray-worker", "image": settings.training_image}
                                ],
                            }
                        },
                    }
                ],
            },
        },
    }
