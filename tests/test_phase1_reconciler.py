from __future__ import annotations

import hashlib
import io
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
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
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
    object_store_workload_environment,
)
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
        retry_count=0,
        retry_budget=5,
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


def test_ray_manifest_is_ephemeral_project_bound_and_fenced(monkeypatch) -> None:
    monkeypatch.setattr(
        reconciler,
        "get_settings",
        lambda: Settings(
            object_store_type="s3_compatible",
            object_store_endpoint="http://objects:8333",
            object_store_access_key="access",
            object_store_secret_key="secret",
        ),
    )
    run = _run()
    attempt = _attempt(run)
    manifest = reconciler.build_ray_job_manifest(run, attempt, name="ray-run")
    assert manifest["kind"] == "RayJob"
    assert manifest["spec"]["submissionMode"] == "K8sJobMode"
    assert manifest["spec"]["jobId"] == "sceptre-fence-one"
    assert manifest["spec"]["backoffLimit"] == 0
    assert manifest["spec"]["submitterConfig"] == {"backoffLimit": 0}
    assert manifest["spec"]["shutdownAfterJobFinishes"] is False
    assert "ttlSecondsAfterFinished" not in manifest["spec"]
    assert manifest["spec"]["deletionStrategy"] == {
        "onSuccess": {"policy": "DeleteNone"},
        "onFailure": {"policy": "DeleteNone"},
    }
    assert "clusterSelector" not in manifest["spec"]
    assert "rayClusterSpec" in manifest["spec"]
    assert manifest["metadata"]["labels"]["automl.platform/attempt-id"] == str(attempt.id)
    assert (
        manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"][
            "serviceAccountName"
        ]
        == "project-training"
    )
    cluster = manifest["spec"]["rayClusterSpec"]
    submitter_spec = manifest["spec"]["submitterPodTemplate"]["spec"]
    head_spec = cluster["headGroupSpec"]["template"]["spec"]
    worker_spec = cluster["workerGroupSpecs"][0]["template"]["spec"]
    assert submitter_spec["serviceAccountName"] == "project-training"
    assert submitter_spec["restartPolicy"] == "Never"
    assert submitter_spec["automountServiceAccountToken"] is False
    assert head_spec["automountServiceAccountToken"] is False
    assert worker_spec["automountServiceAccountToken"] is False
    head_env = {item["name"]: item for item in head_spec["containers"][0]["env"]}
    worker_env = {item["name"]: item for item in worker_spec["containers"][0]["env"]}
    submitter_env = {item["name"]: item for item in submitter_spec["containers"][0]["env"]}
    for key in (
        "DATABASE_URL",
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_ACCESS_KEY",
        "OBJECT_STORE_SECRET_KEY",
        "MLFLOW_TRACKING_URI",
        "AUTOML_FENCING_TOKEN",
    ):
        assert key in head_env
        assert key in worker_env
        assert key in submitter_env
    assert head_env["OBJECT_STORE_TYPE"]["value"] == "s3_compatible"
    assert head_env["DATABASE_URL"]["valueFrom"]["secretKeyRef"]["key"] == "DATABASE_URL"
    assert worker_env["TRAINING_EXECUTION_MODE"]["value"] == "ray"
    assert submitter_spec["containers"][0]["image"] == head_spec["containers"][0]["image"]
    assert submitter_spec["containers"][0]["resources"] == {
        "requests": {"cpu": "100m", "memory": "256Mi"},
        "limits": {"cpu": "500m", "memory": "1Gi"},
    }
    assert worker_spec["containers"][0]["resources"]["requests"] == {
        "cpu": "1.0",
        "memory": "1024Mi",
    }
    assert {volume["name"] for volume in worker_spec["volumes"]} == {
        "ray-runtime",
        "shared-memory",
    }


def test_native_object_store_workloads_use_identity_without_static_s3_secrets() -> None:
    environment = object_store_workload_environment(
        Settings(
            object_store_type="gcs",
            object_store_region="africa-south1",
            gcs_project="sceptre-dev",
        )
    )
    values = {item["name"]: item for item in environment}
    assert values["OBJECT_STORE_TYPE"]["value"] == "gcs"
    assert values["OBJECT_STORE_REGION"]["value"] == "africa-south1"
    assert values["GCS_PROJECT"]["value"] == "sceptre-dev"
    assert "OBJECT_STORE_ACCESS_KEY" not in values
    assert "OBJECT_STORE_SECRET_KEY" not in values


def test_ray_manifest_honors_run_resources_and_pull_secrets(monkeypatch) -> None:
    run = _run()
    run.cpu_request_cores = 2.5
    run.cpu_limit_cores = 3.0
    run.memory_request_mb = 2048
    run.memory_limit_mb = 3072
    attempt = _attempt(run)
    settings = reconciler.get_settings()
    monkeypatch.setattr(
        reconciler,
        "get_settings",
        lambda: SimpleNamespace(**{**settings.__dict__, "workload_image_pull_secrets": ("pull",)}),
    )

    manifest = reconciler.build_ray_job_manifest(run, attempt, name="ray-run")
    worker_spec = manifest["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"]["spec"]
    assert worker_spec["imagePullSecrets"] == [{"name": "pull"}]
    assert manifest["spec"]["submitterPodTemplate"]["spec"]["imagePullSecrets"] == [
        {"name": "pull"}
    ]
    assert worker_spec["containers"][0]["resources"] == {
        "requests": {"cpu": "2.5", "memory": "2048Mi"},
        "limits": {"cpu": "3.0", "memory": "3072Mi"},
    }


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


def test_submit_reconciler_claims_a_new_pending_attempt_before_submission() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.PENDING)
    entry = _entry(attempt)
    db = MagicMock()
    db.scalar.side_effect = [attempt, run]
    k8s = MagicMock()
    k8s.create_ray_job.return_value = {"metadata": {"name": "created-job"}}

    reconciler.submit_training_ray_job(db, entry, k8s)

    assert attempt.status == AttemptStatus.SUBMITTED
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


def test_ray_observer_tracks_running_identity_and_run_state() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.get.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "status": {
            "jobStatus": "RUNNING",
            "rayClusterName": "observed-cluster",
            "jobId": "observed-job-id",
        }
    }

    assert reconciler.observe_training_ray_jobs(db, k8s) == 1

    assert attempt.status == AttemptStatus.RUNNING
    assert attempt.ray_cluster_name == "observed-cluster"
    assert attempt.ray_submission_id == "observed-job-id"
    assert run.status == RunStatus.RUNNING
    assert run.started_at is not None


def test_ray_observer_ignores_nonterminal_waiting_status() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobDeploymentStatus": "Initializing"}}

    assert reconciler.observe_training_ray_jobs(db, k8s) == 1
    assert attempt.status == AttemptStatus.SUBMITTED
    db.get.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        {"jobStatus": "FAILED"},
        {"jobStatus": "STOPPED"},
        {"jobDeploymentStatus": "Failed"},
        {"jobStatus": "SUCCEEDED"},
        {"jobDeploymentStatus": "Complete"},
    ],
)
def test_ray_observer_replaces_terminal_jobs_without_a_terminal_cas(monkeypatch, status) -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    attempt.retry_budget = 5
    command = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalar.side_effect = [run, command]
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": status}

    assert reconciler.observe_training_ray_jobs(db, k8s) == 1

    assert attempt.status == AttemptStatus.SUPERSEDED
    replacement = next(
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], WorkflowAttempt)
    )
    assert replacement.generation == 2
    assert replacement.predecessor_attempt_id == attempt.id
    assert run.tags["desired_state"] == "ray_submission_pending"
    enqueue.assert_called_once()


def test_ray_observer_reclaims_a_missing_job_and_replaces_active_trials(monkeypatch) -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.checkpoint_uri = "s3://checkpoints/run"
    trial_attempt = WorkflowAttempt(
        project_id=run.project_id,
        stage="training_trial",
        logical_key="trial:one",
        model_run_id=run.id,
        trial_id=uuid.uuid4(),
        run_attempt_id=attempt.id,
        workload_identity=attempt.workload_identity,
        generation=1,
        fencing_token="trial-fence",
        status=AttemptStatus.RUNNING,
        terminal_cas_version=0,
        retry_budget=5,
        checkpoint_uri="s3://checkpoints/trial",
    )
    trial_attempt.id = uuid.uuid4()
    command = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalars.side_effect = [[attempt], [trial_attempt]]
    db.scalar.side_effect = [run, command]
    monkeypatch.setattr(reconciler, "enqueue_outbox", MagicMock())
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=404)

    reconciler.observe_training_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.SUPERSEDED
    assert trial_attempt.status == AttemptStatus.SUPERSEDED
    replacements = [
        call.args[0] for call in db.add.call_args_list if isinstance(call.args[0], WorkflowAttempt)
    ]
    assert len(replacements) == 2
    run_replacement, trial_replacement = replacements
    assert run_replacement.predecessor_checkpoint_allowlist == [attempt.checkpoint_uri]
    assert trial_replacement.predecessor_checkpoint_allowlist == [trial_attempt.checkpoint_uri]
    assert trial_replacement.run_attempt_id == run_replacement.id


def test_ray_observer_fails_closed_at_retry_budget_and_on_invalid_lineage() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    attempt.retry_budget = 1
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalar.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "FAILED"}}

    reconciler.observe_training_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.FAILED
    assert run.status == RunStatus.FAILED
    assert run.failure_code == "ray_retry_budget_exhausted"
    db.add.assert_not_called()

    missing = _attempt(_run(), AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.side_effect = [[missing]]
    db.scalar.return_value = None
    k8s.ray_job.side_effect = ApiException(status=404)
    with pytest.raises(LookupError, match="no tracked training run"):
        reconciler.observe_training_ray_jobs(db, k8s)


def test_ray_observer_propagates_api_errors_and_missing_commands() -> None:
    attempt = _attempt(_run(), AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=503)
    with pytest.raises(ApiException):
        reconciler.observe_training_ray_jobs(db, k8s)

    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalar.side_effect = [run, None]
    k8s.ray_job.side_effect = ApiException(status=404)
    with pytest.raises(LookupError, match="no durable launch command"):
        reconciler.observe_training_ray_jobs(db, k8s)


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
    entry.topic = "custom.reconcile"
    failed_db = MagicMock()
    with pytest.raises(RuntimeError, match="external down"):
        reconciler.reconcile_entry(
            failed_db,
            entry,
            worker_id="worker-a",
            handlers={"custom.reconcile": handler},
        )
    failed_db.rollback.assert_called_once()
    assert complete.call_args.kwargs["delivered"] is False
    entry.topic = "unknown"
    with pytest.raises(reconciler.UnknownOutboxTopic):
        reconciler.reconcile_entry(MagicMock(), entry, worker_id="worker-a")


@pytest.mark.parametrize(
    ("topic", "handler_name"),
    [
        ("ray.training.cancel", "cancel_training_ray_job"),
        ("kubernetes.analysis.submit", "submit_analysis_job"),
        ("upload.reconcile", "reconcile_upload_session"),
        ("registry.reconcile", "reconcile_registry_artifact"),
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
    entry = _entry(_attempt(_run()), "reserved.reconcile")
    monkeypatch.setitem(reconciler.RETRY_OWNER, "reserved.reconcile", "workflow-reconciler")
    complete = MagicMock()
    monkeypatch.setattr(reconciler, "complete_outbox", complete)
    with pytest.raises(RuntimeError, match="No reconciler handler"):
        reconciler.reconcile_entry(MagicMock(), entry, worker_id="worker-a")
    assert complete.call_args.kwargs["delivered"] is False


def test_upload_reconciler_handles_terminal_expired_and_quarantined_sessions(
    monkeypatch,
) -> None:
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="upload session"):
        reconciler.reconcile_upload_session(db, entry)

    session = SimpleNamespace(status="aborted")
    db.scalar.return_value = session
    assert reconciler.reconcile_upload_session(db, entry) is session

    session = SimpleNamespace(
        id=uuid.UUID(str(entry.payload["session_id"])),
        status="pending",
        expires_at=reconciler.datetime.now(reconciler.UTC),
        terminal_reason=None,
        lease_owner="worker",
        lease_expires_at=reconciler.datetime.now(reconciler.UTC),
    )
    db.scalar.return_value = session
    reconciler.reconcile_upload_session(db, entry)
    assert session.status == "expired"
    assert session.lease_owner is None

    session = SimpleNamespace(
        status="pending",
        expires_at=reconciler.datetime.now(reconciler.UTC).replace(year=2099),
    )
    db.scalar.return_value = session
    with pytest.raises(RuntimeError, match="not yet eligible"):
        reconciler.reconcile_upload_session(db, entry)

    session = SimpleNamespace(
        id=uuid.UUID(str(entry.payload["session_id"])),
        status="completed",
        dataset_version_id=None,
        quarantine_reason=None,
        terminal_reason=None,
    )
    db.scalar.return_value = session
    reconciler.reconcile_upload_session(db, entry)
    assert session.status == "quarantined"


def test_upload_reconciler_verifies_object_size_and_visibility(monkeypatch) -> None:
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    session = SimpleNamespace(
        id=uuid.UUID(str(entry.payload["session_id"])),
        status="completed",
        project_id=entry.project_id,
        dataset_version_id=uuid.uuid4(),
        byte_size=10,
        quarantine_reason=None,
        terminal_reason=None,
        lease_owner="worker",
        lease_expires_at=reconciler.datetime.now(reconciler.UTC),
        heartbeat_at=None,
    )
    db = MagicMock()
    db.scalar.side_effect = [session, None]
    reconciler.reconcile_upload_session(db, entry)
    assert session.status == "quarantined"
    assert "missing dataset version" in session.quarantine_reason

    version = SimpleNamespace(object_uri="minio://automl/object")
    store = MagicMock()
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    session.status = "completed"
    db.scalar.side_effect = [session, version, session]
    store.exists.return_value = False
    with pytest.raises(OSError, match="not visible"):
        reconciler.reconcile_upload_session(db, entry)

    db.scalar.side_effect = [session, version, session]
    store.exists.return_value = True
    store.size.return_value = 9
    reconciler.reconcile_upload_session(db, entry)
    assert session.status == "quarantined"
    assert "expected 10, observed 9" in session.quarantine_reason

    session.status = "completed"
    db.scalar.side_effect = [session, version, session]
    store.size.return_value = 10
    reconciler.reconcile_upload_session(db, entry)
    assert session.status == "completed"
    assert session.terminal_reason is None
    assert session.lease_owner is None
    assert session.heartbeat_at is not None


def test_phase2_upload_reconciler_streams_scans_and_verifies_sha256(monkeypatch) -> None:
    content = b"a,b\n1,2\n"
    digest = hashlib.sha256(content).hexdigest()
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    session = SimpleNamespace(
        id=uuid.UUID(str(entry.payload["session_id"])),
        status="object_completed",
        project_id=entry.project_id,
        upload_kind="dataset",
        dataset_version_id=uuid.uuid4(),
        completed_object_uri="s3c://automl/object.csv",
        original_filename="object.csv",
        byte_size=len(content),
        expected_object_digest=digest,
        scanner_status="pending",
        quarantine_reason=None,
        terminal_reason=None,
        lease_owner="worker",
        lease_expires_at=reconciler.datetime.now(reconciler.UTC),
        heartbeat_at=None,
    )
    version = SimpleNamespace(
        object_uri=session.completed_object_uri,
        content_hash="pending",
        content_hash_algorithm="sha256",
        content_hash_scope="byte_stream",
        content_hash_verification_status="pending",
        content_hash_verified_at=None,
        status=DatasetStatus.UPLOADED,
        schema_json={},
        inferred_types_json={},
        quality_report_json={},
        column_count=None,
    )
    db = MagicMock()
    db.scalar.side_effect = [session, version, session, version]
    store = MagicMock()
    store.exists.return_value = True
    store.size.return_value = len(content)
    store.open_stream.return_value = io.BytesIO(content)
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    monkeypatch.setattr(
        reconciler,
        "get_settings",
        lambda: Settings(upload_scanner_signature_version="builtin-v1"),
    )

    assert reconciler.reconcile_upload_session(db, entry) is session
    assert session.status == "ready"
    assert session.observed_object_digest == digest
    assert session.scanner_status == "allowed"
    assert session.lease_owner is None
    assert session.lease_expires_at is None
    assert version.status == DatasetStatus.READY
    assert version.content_hash_verification_status == "verified"
    assert version.column_count == 2
    assert version.schema_json == {
        "columns": [{"name": "a"}, {"name": "b"}],
        "source": "verified_upload_stream",
    }
    assert version.inferred_types_json["a"]["semantic_type"] == "pending"
    assert store.open_stream.return_value.closed


def test_phase2_upload_reconciler_quarantines_digest_and_scanner_failures(
    monkeypatch,
) -> None:
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    content = b"a,b\n1,2\n"
    version = SimpleNamespace(object_uri="s3c://automl/object.csv")
    store = MagicMock()
    store.exists.return_value = True
    store.size.return_value = len(content)
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    monkeypatch.setattr(reconciler, "get_settings", lambda: Settings())

    def session(filename: str = "object.csv") -> SimpleNamespace:
        return SimpleNamespace(
            id=uuid.UUID(str(entry.payload["session_id"])),
            status="object_completed",
            project_id=entry.project_id,
            upload_kind="dataset",
            dataset_version_id=uuid.uuid4(),
            completed_object_uri=version.object_uri,
            original_filename=filename,
            byte_size=len(content),
            expected_object_digest="0" * 64,
            scanner_status="pending",
            quarantine_reason=None,
            terminal_reason=None,
            lease_owner="worker",
            lease_expires_at=reconciler.datetime.now(reconciler.UTC),
        )

    mismatch = session()
    store.open_stream.return_value = io.BytesIO(content)
    db = MagicMock()
    db.scalar.side_effect = [mismatch, version, mismatch]
    reconciler.reconcile_upload_session(db, entry)
    assert mismatch.status == "quarantined"
    assert version.status == DatasetStatus.FAILED
    assert "SHA-256" in mismatch.quarantine_reason

    denied = session()
    store.open_stream.return_value = io.BytesIO(b"MZ bad!!")
    denied.byte_size = 8
    db.scalar.side_effect = [denied, version, denied]
    reconciler.reconcile_upload_session(db, entry)
    assert denied.status == "quarantined"
    assert denied.scanner_status == "denied"


def test_registry_reconciler_verifies_manifest_and_is_idempotent(monkeypatch) -> None:
    entry = _entry(_attempt(_run()), "registry.reconcile")
    entry.payload = {"registry_entry_id": str(uuid.uuid4())}
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="registry entry"):
        reconciler.reconcile_registry_artifact(db, entry)

    registry_entry = SimpleNamespace(registry_metadata={"desired_state": "artifact_verified"})
    db.scalar.return_value = registry_entry
    assert reconciler.reconcile_registry_artifact(db, entry) is registry_entry

    registry_entry = SimpleNamespace(
        project_id=entry.project_id,
        model_artifact_id=uuid.uuid4(),
        registry_metadata={},
    )
    db.scalar.side_effect = [registry_entry, None]
    with pytest.raises(LookupError, match="model artifact"):
        reconciler.reconcile_registry_artifact(db, entry)

    artifact = SimpleNamespace(object_uri="minio://automl/model", byte_size=10)
    store = MagicMock()
    monkeypatch.setattr(reconciler, "get_object_store", lambda: store)
    db.scalar.side_effect = [registry_entry, artifact]
    store.exists.return_value = False
    with pytest.raises(OSError, match="not visible"):
        reconciler.reconcile_registry_artifact(db, entry)

    db.scalar.side_effect = [registry_entry, artifact]
    store.exists.return_value = True
    store.size.return_value = 9
    with pytest.raises(ValueError, match="byte size"):
        reconciler.reconcile_registry_artifact(db, entry)

    artifact.byte_size = None
    db.scalar.side_effect = [registry_entry, artifact]
    store.size.return_value = 9
    reconciler.reconcile_registry_artifact(db, entry)
    assert registry_entry.registry_metadata["desired_state"] == "artifact_verified"
    assert registry_entry.registry_metadata["verified_byte_size"] == 9


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
