from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowAttempt,
)
from automl_api.services import reconciler
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from kubernetes.client import ApiException


def _run() -> ModelRun:
    run = ModelRun(
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        run_kind=RunKind.TRAINING,
        status=RunStatus.QUEUED,
        task_type=TaskType.REGRESSION,
        target_column="target",
        k8s_namespace="sceptre",
        tags={"desired_state": "ray_submission_pending"},
    )
    run.id = uuid.uuid4()
    return run


def _attempt(run: ModelRun, status: AttemptStatus = AttemptStatus.CLAIMED) -> WorkflowAttempt:
    attempt = WorkflowAttempt(
        project_id=run.project_id,
        stage="training_run",
        logical_key=f"training-run:{run.id}",
        model_run_id=run.id,
        workload_identity="project-training",
        generation=1,
        fencing_token="fence-one",
        status=status,
        terminal_cas_version=0,
    )
    attempt.id = uuid.uuid4()
    return attempt


def _entry(attempt: WorkflowAttempt, topic: str = "ray.training.submit") -> OutboxEntry:
    entry = OutboxEntry(
        project_id=attempt.project_id,
        command_id=uuid.uuid4(),
        event_key="event-one",
        topic=topic,
        aggregate_type="model_run",
        aggregate_id=attempt.model_run_id,
        payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
        status=OutboxStatus.CLAIMED,
        lease_owner="worker-a",
    )
    entry.id = uuid.uuid4()
    return entry


def test_ray_manifest_is_ephemeral_project_bound_and_fenced() -> None:
    run = _run()
    attempt = _attempt(run)
    manifest = reconciler.build_ray_job_manifest(run, attempt, name="ray-run")
    assert manifest["kind"] == "RayJob"
    assert manifest["spec"]["shutdownAfterJobFinishes"] is True
    assert "rayClusterSpec" in manifest["spec"]
    assert manifest["metadata"]["labels"]["automl.platform/attempt-id"] == str(attempt.id)
    assert (
        manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"][
            "serviceAccountName"
        ]
        == "project-training"
    )


def test_submit_reconciler_persists_ray_identity_and_is_idempotent() -> None:
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt)
    db = MagicMock()
    db.scalar.side_effect = [attempt, run]
    k8s = MagicMock()
    k8s.create_ray_job.return_value = {
        "metadata": {"name": "created-job"},
        "status": {"rayClusterName": "created-cluster", "jobId": "ray-id"},
    }
    result = reconciler.submit_training_ray_job(db, entry, k8s)
    assert result.status == AttemptStatus.SUBMITTED
    assert result.ray_job_name == "created-job"
    assert result.ray_cluster_name == "created-cluster"
    assert result.ray_submission_id == "ray-id"
    assert run.tags["desired_state"] == "ray_submitted"
    k8s.create_ray_job.assert_called_once()
    k8s.ensure_service_account.assert_called_once_with("project-training")

    db.scalar.side_effect = [attempt]
    assert reconciler.submit_training_ray_job(db, entry, k8s) is attempt
    k8s.create_ray_job.assert_called_once()


def test_submit_reconciler_rejects_missing_and_stale_attempt() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    entry = _entry(attempt)
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="attempt"):
        reconciler.submit_training_ray_job(db, entry, MagicMock())
    db.scalar.return_value = attempt
    entry.payload["fencing_token"] = "old"
    with pytest.raises(ValueError, match="fence"):
        reconciler.submit_training_ray_job(db, entry, MagicMock())

    pending = _attempt(run)
    pending_entry = _entry(pending)
    db.scalar.side_effect = [pending, None]
    with pytest.raises(LookupError, match="training run"):
        reconciler.submit_training_ray_job(db, pending_entry, MagicMock())


def test_cancel_reconciler_deletes_once_and_terminal_state_is_stable() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.ray_job_name = "ray-job"
    entry = _entry(attempt, "ray.training.cancel")
    db = MagicMock()
    db.scalar.return_value = attempt
    k8s = MagicMock()
    result = reconciler.cancel_training_ray_job(db, entry, k8s)
    assert result.status == AttemptStatus.CANCELLED
    k8s.delete_ray_job.assert_called_once_with("ray-job")

    db.scalar.return_value = attempt
    reconciler.cancel_training_ray_job(db, entry, k8s)
    assert attempt.terminal_cas_version == 1
    db.scalar.return_value = None
    with pytest.raises(LookupError):
        reconciler.cancel_training_ray_job(db, entry, k8s)


def test_cancel_reconciler_handles_an_unsubmitted_attempt_without_external_delete() -> None:
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt, "ray.training.cancel")
    db = MagicMock()
    db.scalar.return_value = attempt
    k8s = MagicMock()

    reconciler.cancel_training_ray_job(db, entry, k8s)

    k8s.delete_ray_job.assert_not_called()
    assert attempt.status == AttemptStatus.CANCELLED


def test_entry_dispatch_requeues_failure_and_delivers_success(monkeypatch) -> None:
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt)
    complete = MagicMock()
    monkeypatch.setattr(reconciler, "complete_outbox", complete)
    monkeypatch.setattr(reconciler, "submit_training_ray_job", MagicMock())
    reconciler.reconcile_entry(MagicMock(), entry, worker_id="worker-a", k8s=MagicMock())
    assert complete.call_args.kwargs["delivered"] is True

    handler = MagicMock(side_effect=RuntimeError("external down"))
    entry.topic = "registry.reconcile"
    with pytest.raises(RuntimeError, match="external down"):
        reconciler.reconcile_entry(
            MagicMock(),
            entry,
            worker_id="worker-a",
            handlers={"registry.reconcile": handler},
        )
    assert complete.call_args.kwargs["delivered"] is False
    entry.topic = "unknown"
    with pytest.raises(reconciler.UnknownOutboxTopic):
        reconciler.reconcile_entry(MagicMock(), entry, worker_id="worker-a")


@pytest.mark.parametrize(
    ("topic", "handler_name"),
    [
        ("ray.training.cancel", "cancel_training_ray_job"),
        ("kubernetes.analysis.submit", "submit_analysis_job"),
        ("deployment.reconcile", "reconcile_deployment"),
        ("governance.reconcile", "reconcile_governance_object"),
        ("cleanup.execute", "reconcile_cleanup"),
    ],
)
def test_entry_dispatches_every_builtin_topic(monkeypatch, topic: str, handler_name: str) -> None:
    entry = _entry(_attempt(_run()), topic)
    handler = MagicMock()
    monkeypatch.setattr(reconciler, handler_name, handler)
    complete = MagicMock()
    monkeypatch.setattr(reconciler, "complete_outbox", complete)
    command = SimpleNamespace(status=CommandStatus.RUNNING)
    db = MagicMock()
    db.get.return_value = command

    reconciler.reconcile_entry(db, entry, worker_id="worker-a", k8s=MagicMock())

    handler.assert_called_once()
    assert command.status == CommandStatus.SUCCEEDED
    db.flush.assert_called_once()


def test_entry_rejects_known_topic_without_a_handler(monkeypatch) -> None:
    entry = _entry(_attempt(_run()), "upload.reconcile")
    complete = MagicMock()
    monkeypatch.setattr(reconciler, "complete_outbox", complete)
    with pytest.raises(RuntimeError, match="No reconciler handler"):
        reconciler.reconcile_entry(MagicMock(), entry, worker_id="worker-a")
    assert complete.call_args.kwargs["delivered"] is False


def test_analysis_reconciler_submits_once_and_records_desired_state() -> None:
    run = _run()
    run.tags = {"desired_state": "kubernetes_submission_pending"}
    entry = _entry(_attempt(run), "kubernetes.analysis.submit")
    entry.payload = {"run_id": str(run.id), "manifest": {"metadata": {"name": "analysis"}}}
    db = MagicMock()
    db.scalar.return_value = run
    k8s = MagicMock()

    assert reconciler.submit_analysis_job(db, entry, k8s) is run
    k8s.create_job.assert_called_once_with(entry.payload["manifest"])
    assert run.tags["desired_state"] == "kubernetes_submitted"
    assert reconciler.submit_analysis_job(db, entry, k8s) is run
    k8s.create_job.assert_called_once()

    db.scalar.return_value = None
    with pytest.raises(LookupError, match="analysis run"):
        reconciler.submit_analysis_job(db, entry, k8s)


def test_deployment_reconciler_writes_object_and_submits_once(monkeypatch) -> None:
    run = _run()
    run.status = RunStatus.QUEUED
    run.tags = {"desired_state": "deployment_pending"}
    artifact = SimpleNamespace(
        object_uri="minio://automl/projects/p/deployments/d/Dockerfile",
        artifact_metadata={"content": "FROM scratch", "desired_state": "object_write_pending"},
    )
    entry = _entry(_attempt(run), "deployment.reconcile")
    entry.payload = {
        "run_id": str(run.id),
        "dockerfile_uri": artifact.object_uri,
        "manifests": {"deployment": {}, "service": {}},
    }
    db = MagicMock()
    db.scalar.side_effect = [run, artifact]
    store = MagicMock()
    store.put_bytes.return_value = SimpleNamespace(uri=artifact.object_uri)
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    k8s = MagicMock()

    assert reconciler.reconcile_deployment(db, entry, k8s) is run
    store.put_bytes.assert_called_once_with("projects/p/deployments/d/Dockerfile", b"FROM scratch")
    k8s.create_model_deployment.assert_called_once_with(entry.payload["manifests"])
    assert run.status == RunStatus.RUNNING
    assert run.tags["desired_state"] == "deployment_active"
    db.scalar.side_effect = [run]
    assert reconciler.reconcile_deployment(db, entry, k8s) is run
    k8s.create_model_deployment.assert_called_once()


def test_deployment_reconciler_rejects_missing_run_artifact_and_content() -> None:
    run = _run()
    run.tags = {"desired_state": "deployment_pending"}
    entry = _entry(_attempt(run), "deployment.reconcile")
    entry.payload = {
        "run_id": str(run.id),
        "dockerfile_uri": "minio://automl/deployment/Dockerfile",
        "manifests": {},
    }
    db = MagicMock()
    db.scalar.side_effect = [None]
    with pytest.raises(LookupError, match="deployment run"):
        reconciler.reconcile_deployment(db, entry, MagicMock())
    db.scalar.side_effect = [run, None]
    with pytest.raises(LookupError, match="Dockerfile"):
        reconciler.reconcile_deployment(db, entry, MagicMock())
    empty = SimpleNamespace(object_uri=entry.payload["dockerfile_uri"], artifact_metadata={})
    db.scalar.side_effect = [run, empty]
    with pytest.raises(ValueError, match="content is empty"):
        reconciler.reconcile_deployment(db, entry, MagicMock())


def test_governance_reconciler_writes_both_objects_once(monkeypatch) -> None:
    report_id = uuid.uuid4()
    entry = _entry(_attempt(_run()), "governance.reconcile")
    entry.payload = {"report_id": str(report_id)}
    artifact = SimpleNamespace(
        object_uri="minio://automl/projects/p/report.json",
        artifact_metadata={
            "pending_json": "{}",
            "pending_html": "<html></html>",
            "html_uri": "minio://automl/projects/p/report.html",
            "desired_state": "object_write_pending",
        },
    )
    db = MagicMock()
    db.scalar.return_value = artifact
    store = MagicMock()
    store.put_bytes.side_effect = [
        SimpleNamespace(uri=artifact.object_uri),
        SimpleNamespace(uri=artifact.artifact_metadata["html_uri"]),
    ]
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)

    assert reconciler.reconcile_governance_object(db, entry) is artifact
    assert store.put_bytes.call_count == 2
    assert artifact.artifact_metadata["desired_state"] == "object_written"
    assert "pending_json" not in artifact.artifact_metadata
    assert reconciler.reconcile_governance_object(db, entry) is artifact
    assert store.put_bytes.call_count == 2


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"pending_json": {}, "pending_html": "html", "html_uri": "uri"},
        {"pending_json": "json", "pending_html": {}, "html_uri": "uri"},
        {"pending_json": "json", "pending_html": "html", "html_uri": ""},
    ],
)
def test_governance_reconciler_rejects_missing_or_incomplete_payload(metadata) -> None:
    entry = _entry(_attempt(_run()), "governance.reconcile")
    entry.payload = {"report_id": str(uuid.uuid4())}
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="governance report"):
        reconciler.reconcile_governance_object(db, entry)
    db.scalar.return_value = SimpleNamespace(artifact_metadata=metadata)
    with pytest.raises(ValueError, match="incomplete"):
        reconciler.reconcile_governance_object(db, entry)


def test_cleanup_reconciler_completes_and_replays_tombstones(monkeypatch) -> None:
    artifact_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
    entry = _entry(_attempt(_run()), "cleanup.execute")
    entry.payload = {
        "artifact_ids": [str(value) for value in artifact_ids],
        "cleanup_finished_jobs": True,
    }
    artifact = SimpleNamespace(object_uri="minio://automl/old")
    tombstone = DeletionTombstone(
        project_id=entry.project_id,
        requested_by_id=uuid.uuid4(),
        resource_type="run_artifact",
        resource_id=artifact_ids[0],
        reason="cleanup",
        legal_hold=False,
        status="pending",
    )
    tombstone.id = uuid.uuid4()
    stage = DeletionStage(
        project_id=entry.project_id,
        tombstone_id=tombstone.id,
        resource_class="object_store",
        status="pending",
    )
    completed = SimpleNamespace(status="completed", legal_hold=False)
    missing_artifact_tombstone = SimpleNamespace(
        id=uuid.uuid4(), status="pending", legal_hold=False, completed_at=None
    )
    db = MagicMock()
    db.scalar.side_effect = [
        artifact,
        tombstone,
        stage,
        None,
        completed,
        None,
        missing_artifact_tombstone,
        None,
    ]
    store = MagicMock()
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    k8s = MagicMock()

    assert reconciler.reconcile_cleanup(db, entry, k8s) == artifact_ids
    store.delete.assert_called_once_with(artifact.object_uri)
    db.delete.assert_called_once_with(artifact)
    assert stage.status == "completed"
    assert tombstone.status == "completed"
    assert missing_artifact_tombstone.status == "completed"
    k8s.cleanup_finished_jobs.assert_called_once_with(entry.project_id)


def test_cleanup_reconciler_rejects_missing_tombstone_and_legal_hold(monkeypatch) -> None:
    artifact_id = uuid.uuid4()
    entry = _entry(_attempt(_run()), "cleanup.execute")
    entry.payload = {"artifact_ids": [str(artifact_id)]}
    monkeypatch.setattr(reconciler, "get_object_store", MagicMock())
    db = MagicMock()
    db.scalar.side_effect = [None, None]
    with pytest.raises(LookupError, match="tombstone"):
        reconciler.reconcile_cleanup(db, entry, MagicMock())
    db.scalar.side_effect = [None, SimpleNamespace(status="pending", legal_hold=True)]
    with pytest.raises(ValueError, match="legal hold"):
        reconciler.reconcile_cleanup(db, entry, MagicMock())


def test_cleanup_reconciler_can_skip_cluster_cleanup(monkeypatch) -> None:
    artifact_id = uuid.uuid4()
    entry = _entry(_attempt(_run()), "cleanup.execute")
    entry.payload = {"artifact_ids": [str(artifact_id)], "cleanup_finished_jobs": False}
    tombstone = SimpleNamespace(
        id=uuid.uuid4(), status="pending", legal_hold=False, completed_at=None
    )
    db = MagicMock()
    db.scalar.side_effect = [None, tombstone, None]
    monkeypatch.setattr(reconciler, "get_object_store", MagicMock())
    k8s = MagicMock()

    assert reconciler.reconcile_cleanup(db, entry, k8s) == [artifact_id]
    k8s.cleanup_finished_jobs.assert_not_called()


def test_kubernetes_rayjob_methods_handle_conflicts_and_not_found() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client._configured = True
    client._configuration_error = None
    client.settings = SimpleNamespace(training_namespace="sceptre")
    client.custom = MagicMock()
    manifest = {"metadata": {"name": "ray-job"}}
    client.custom.create_namespaced_custom_object.side_effect = ApiException(status=409)
    client.custom.get_namespaced_custom_object.return_value = {"metadata": {"name": "ray-job"}}
    assert client.create_ray_job(manifest)["metadata"]["name"] == "ray-job"
    assert client.ray_job("ray-job")["metadata"]["name"] == "ray-job"
    client.custom.delete_namespaced_custom_object.side_effect = ApiException(status=404)
    client.delete_ray_job("ray-job")

    client._configured = False
    client._configuration_error = "no kube config"
    with pytest.raises(RuntimeError, match="no kube"):
        client.create_ray_job(manifest)
    with pytest.raises(RuntimeError, match="no kube"):
        client.ray_job("ray-job")
    with pytest.raises(RuntimeError, match="no kube"):
        client.delete_ray_job("ray-job")


def test_kubernetes_service_account_creation_is_idempotent() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = SimpleNamespace(training_namespace="sceptre")
    client.core = MagicMock()
    client.ensure_service_account("project-training")
    body = client.core.create_namespaced_service_account.call_args.kwargs["body"]
    assert body["metadata"]["name"] == "project-training"
    assert body["automountServiceAccountToken"] is False
    client.core.create_namespaced_service_account.side_effect = ApiException(status=409)
    client.ensure_service_account("project-training")
    client.core.create_namespaced_service_account.side_effect = ApiException(status=403)
    with pytest.raises(ApiException):
        client.ensure_service_account("project-training")
