from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import ray_observer_fixture
from analysis_submission_helpers import reconcile_entry as reconcile_analysis_entry
from analysis_submission_helpers import submit_analysis_job
from automl_api.core.config import Settings
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
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
    object_store_workload_environment,
)
from digest_fixtures import verified_raw_version
from kubernetes.client import ApiException
from ray_cancellation_helpers import cancel_training_ray_job
from ray_submission_helpers import submit_training_ray_job


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
    if status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}:
        attempt.ray_job_name = f"ray-{attempt.id.hex}"
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
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
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
    assert manifest["spec"]["shutdownAfterJobFinishes"] is True
    assert manifest["spec"]["ttlSecondsAfterFinished"] == 300
    assert manifest["spec"]["activeDeadlineSeconds"] > 0
    assert "deletionStrategy" not in manifest["spec"]
    assert "clusterSelector" not in manifest["spec"]
    assert "rayClusterSpec" in manifest["spec"]
    assert manifest["metadata"]["labels"]["automl.platform/attempt-id"] == str(attempt.id)
    for template in (
        manifest["spec"]["submitterPodTemplate"],
        manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"],
        manifest["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"],
    ):
        assert template["metadata"]["labels"]["automl.platform/run-id"] == str(run.id)
        assert template["metadata"]["labels"]["automl.platform/attempt-id"] == str(attempt.id)
    cluster = manifest["spec"]["rayClusterSpec"]
    assert cluster["enableInTreeAutoscaling"] is True
    assert "serviceAccountName" not in cluster["headGroupSpec"]["template"]["spec"]
    head_start = cluster["headGroupSpec"]["rayStartParams"]
    assert head_start["num-cpus"] == "0"
    workers = cluster["workerGroupSpecs"][0]
    assert workers["rayStartParams"]["num-cpus"] == "2"
    assert workers["minReplicas"] == 0
    assert head_start["object-store-memory"] == "268435456"
    submitter_spec = manifest["spec"]["submitterPodTemplate"]["spec"]
    head_spec = cluster["headGroupSpec"]["template"]["spec"]
    worker_spec = cluster["workerGroupSpecs"][0]["template"]["spec"]
    assert submitter_spec["serviceAccountName"] == "project-training"
    assert submitter_spec["restartPolicy"] == "Never"
    assert submitter_spec["automountServiceAccountToken"] is False
    assert head_spec["automountServiceAccountToken"] is True
    assert worker_spec["automountServiceAccountToken"] is False
    assert head_spec["containers"][0]["resources"]["limits"]["memory"] == "4096Mi"
    assert head_spec["volumes"][1]["emptyDir"]["sizeLimit"] == "2Gi"
    head_env = {item["name"]: item for item in head_spec["containers"][0]["env"]}
    worker_env = {item["name"]: item for item in worker_spec["containers"][0]["env"]}
    submitter_env = {item["name"]: item for item in submitter_spec["containers"][0]["env"]}
    for pod in (submitter_spec, head_spec, worker_spec):
        names = [item["name"] for item in pod["containers"][0]["env"]]
        assert len(names) == len(set(names))
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
        "cpu": "2.0",
        "memory": "4096Mi",
    }
    assert worker_env["AUTOML_MODEL_PODS"]["value"] == "1"
    assert {volume["name"] for volume in worker_spec["volumes"]} == {
        "ray-runtime",
        "shared-memory",
        "attempt-identity",
    }


def test_training_ray_head_is_bound_to_capacity_qualified_node_only() -> None:
    run = _run()
    run.params = {"selected_node": "memory-worker-a"}

    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="ray-run")
    cluster = manifest["spec"]["rayClusterSpec"]

    assert cluster["headGroupSpec"]["template"]["spec"]["nodeSelector"] == {
        "kubernetes.io/hostname": "memory-worker-a"
    }
    assert "nodeSelector" not in cluster["workerGroupSpecs"][0]["template"]["spec"]
    assert "nodeSelector" not in manifest["spec"]["submitterPodTemplate"]["spec"]


def test_unlimited_training_omits_ray_deadline_without_removing_resource_limits() -> None:
    run = _run()
    run.params = {"deadline_seconds": None}
    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="unlimited")
    assert "activeDeadlineSeconds" not in manifest["spec"]
    head = manifest["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
    assert head["containers"][0]["resources"]["limits"]["memory"]
    run.params = {"deadline_seconds": 3600}
    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="limited")
    assert manifest["spec"]["activeDeadlineSeconds"] == 3600


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
        "requests": {"cpu": "3.0", "memory": "3072Mi"},
        "limits": {"cpu": "3.0", "memory": "3072Mi"},
    }


def test_submit_reconciler_persists_ray_identity_and_is_idempotent() -> None:
    run = _run()
    run.failure_code = "KUBERNETES_JOB_MISSING"
    run.failure_message = "stale"
    run.plain_english_failure = "stale"
    run.finished_at = datetime.now(UTC)
    attempt = _attempt(run)
    entry = _entry(attempt)
    db = MagicMock()
    values = iter([attempt, run])
    db.scalar.side_effect = lambda statement: (
        None
        if statement.column_descriptions[0]["entity"].__name__
        in {"DatasetProviderImport", "PromotionalScopeMember", "WorkflowEvent"}
        else next(values)
    )
    db.get.return_value = verified_raw_version(id=run.dataset_version_id, project_id=run.project_id)
    k8s = MagicMock()
    k8s.create_ray_job.side_effect = lambda body: {
        **body,
        "metadata": {**body["metadata"], "uid": "original-ray-uid"},
        "status": {"rayClusterName": "created-cluster", "jobId": "ray-id"},
    }
    result = submit_training_ray_job(db, entry, k8s)
    assert result.status == AttemptStatus.SUBMITTED
    assert result.ray_job_name == k8s.create_ray_job.call_args.args[0]["metadata"]["name"]
    assert result.ray_cluster_name == "created-cluster"
    assert result.ray_submission_id == "ray-id"
    assert run.tags["desired_state"] == "ray_submitted"
    assert run.failure_code is None
    assert run.failure_message is None
    assert run.plain_english_failure is None
    assert run.finished_at is None
    k8s.create_ray_job.assert_called_once()
    k8s.ensure_service_account.assert_called_once_with("project-training")

    db.scalar.side_effect = [attempt]
    assert submit_training_ray_job(db, entry, k8s) is attempt
    k8s.create_ray_job.assert_called_once()


def test_submit_reconciler_claims_a_new_pending_attempt_before_submission() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.PENDING)
    entry = _entry(attempt)
    db = MagicMock()
    values = iter([attempt, run])
    db.scalar.side_effect = lambda statement: (
        None
        if statement.column_descriptions[0]["entity"].__name__
        in {"DatasetProviderImport", "PromotionalScopeMember", "WorkflowEvent"}
        else next(values)
    )
    db.get.return_value = verified_raw_version(id=run.dataset_version_id, project_id=run.project_id)
    k8s = MagicMock()
    k8s.create_ray_job.side_effect = lambda body: {
        **body,
        "metadata": {**body["metadata"], "uid": "original-ray-uid"},
    }

    submit_training_ray_job(db, entry, k8s)

    assert attempt.status == AttemptStatus.SUBMITTED
    k8s.create_ray_job.assert_called_once()


def test_submit_reconciler_rejects_missing_and_stale_attempt() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    entry = _entry(attempt)
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="attempt"):
        submit_training_ray_job(db, entry, MagicMock())
    db.scalar.return_value = attempt
    entry.payload["fencing_token"] = "old"
    with pytest.raises(ValueError, match="fence"):
        submit_training_ray_job(db, entry, MagicMock())

    pending = _attempt(run)
    pending_entry = _entry(pending)
    db.scalar.side_effect = [pending, None]
    with pytest.raises(LookupError, match="training run"):
        submit_training_ray_job(db, pending_entry, MagicMock())


def test_cancel_reconciler_deletes_once_and_terminal_state_is_stable() -> None:
    from test_training_ray_cancel_identity import ray_job_identity

    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.updated_at = datetime.now(UTC) - timedelta(minutes=31)
    attempt.ray_job_name = "ray-job"
    entry = _entry(attempt, "ray.training.cancel")
    db = MagicMock()
    db.scalar.return_value = attempt
    db.get.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = ray_job_identity(run, attempt)
    result = cancel_training_ray_job(db, entry, k8s)
    assert result.status == AttemptStatus.CANCELLED
    k8s.delete_ray_job.assert_called_once_with(
        "ray-job", namespace=run.k8s_namespace, uid=k8s.ray_job.return_value["metadata"]["uid"]
    )

    db.scalar.return_value = attempt
    cancel_training_ray_job(db, entry, k8s)
    assert attempt.terminal_cas_version == 1
    db.scalar.return_value = None
    with pytest.raises(LookupError):
        cancel_training_ray_job(db, entry, k8s)


@pytest.mark.parametrize(
    "status", [AttemptStatus.CANCELLED, AttemptStatus.SUPERSEDED, AttemptStatus.FAILED]
)
def test_pending_submission_cannot_create_work_after_terminal_fence(status) -> None:
    attempt = _attempt(_run(), status)
    db, k8s = MagicMock(), MagicMock()
    db.scalar.return_value = attempt
    assert submit_training_ray_job(db, _entry(attempt), k8s) is attempt
    k8s.create_ray_job.assert_not_called()
    k8s.ensure_service_account.assert_not_called()


def test_cancel_reconciler_handles_an_unsubmitted_attempt_without_external_delete() -> None:
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt, "ray.training.cancel")
    db = MagicMock()
    db.scalar.return_value = attempt
    k8s = MagicMock()

    cancel_training_ray_job(db, entry, k8s)

    k8s.delete_ray_job.assert_not_called()
    assert attempt.status == AttemptStatus.CANCELLED


def test_ray_observer_tracks_running_identity_and_run_state() -> None:
    run = _run()
    run.failure_code = "KUBERNETES_JOB_MISSING"
    run.failure_message = "stale"
    run.plain_english_failure = "stale"
    run.finished_at = datetime.now(UTC)
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    db.get.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "status": {
            "jobStatus": "RUNNING",
            "rayClusterName": "observed-cluster",
            "jobId": "observed-job-id",
        }
    }

    assert ray_observer_fixture.observe_training_ray_jobs(db, k8s) == 1

    assert attempt.status == AttemptStatus.RUNNING
    assert attempt.ray_cluster_name == "observed-cluster"
    assert attempt.ray_submission_id == "observed-job-id"
    assert run.status == RunStatus.RUNNING
    assert run.started_at is not None
    assert run.failure_code is None
    assert run.failure_message is None
    assert run.plain_english_failure is None
    assert run.finished_at is None


def test_ray_observer_ignores_nonterminal_waiting_status() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobDeploymentStatus": "Initializing"}}

    assert ray_observer_fixture.observe_training_ray_jobs(db, k8s) == 1
    assert attempt.status == AttemptStatus.SUBMITTED
    db.get.assert_not_called()


def test_ray_observer_prioritizes_failed_deployment_over_stale_running_job(
    monkeypatch,
) -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.retry_budget = 5
    command = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalars.side_effect = [[attempt], [], []]
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [run, run, command]
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "RUNNING", "jobDeploymentStatus": "Failed"}}

    assert ray_observer_fixture.observe_training_ray_jobs(db, k8s) == 1

    assert attempt.status == AttemptStatus.SUPERSEDED
    assert attempt.terminal_reason == "RayJob terminal status FAILED"
    enqueue.assert_called_once()


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
    db.scalars.side_effect = [[attempt], [], []]
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [run, run, command]
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": status}

    assert ray_observer_fixture.observe_training_ray_jobs(db, k8s) == 1

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
    db.scalars.side_effect = [[attempt], [trial_attempt], []]
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [run, run, command]
    monkeypatch.setattr(reconciler, "enqueue_outbox", MagicMock())
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=404)

    ray_observer_fixture.observe_training_ray_jobs(db, k8s)

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
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "FAILED"}}

    ray_observer_fixture.observe_training_ray_jobs(db, k8s)

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
        ray_observer_fixture.observe_training_ray_jobs(db, k8s)


def test_ray_deadline_stops_without_repeating_the_same_model_search(monkeypatch) -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.retry_budget = 5
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "status": {
            "jobStatus": "RUNNING",
            "jobDeploymentStatus": "Failed",
            "reason": "DeadlineExceeded",
            "message": "RayJob exceeded 7200 seconds",
        }
    }

    ray_observer_fixture.observe_training_ray_jobs(db, k8s)

    assert attempt.status == AttemptStatus.FAILED
    assert run.status == RunStatus.FAILED
    assert run.failure_code == "JOB_DEADLINE_EXCEEDED"
    assert run.failure_message == "RayJob exceeded 7200 seconds"
    assert "runtime limit" in run.plain_english_failure
    assert run.finished_at is not None
    enqueue.assert_not_called()
    db.add.assert_not_called()


def test_ray_observer_propagates_api_errors_and_missing_commands() -> None:
    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=503)
    with pytest.raises(ApiException):
        ray_observer_fixture.observe_training_ray_jobs(db, k8s)

    run = _run()
    attempt = _attempt(run, AttemptStatus.SUBMITTED)
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalars.return_value = [attempt]
    db.scalar.side_effect = [run, run, None]
    k8s.ray_job.side_effect = ApiException(status=404)
    with pytest.raises(LookupError, match="no durable launch command"):
        ray_observer_fixture.observe_training_ray_jobs(db, k8s)


def _delivery_db(entry):
    db = MagicMock()
    db.get.side_effect = lambda model, *_args, **_kwargs: entry if model is OutboxEntry else None
    return db


def test_entry_dispatch_requeues_failure_and_delivers_success(monkeypatch) -> None:
    run = _run()
    attempt = _attempt(run)
    entry = _entry(attempt)
    complete = MagicMock()
    monkeypatch.setattr(reconciler, "complete_outbox", complete)
    entry.topic = "custom.success"
    reconciler.reconcile_entry(
        _delivery_db(entry),
        entry,
        worker_id="worker-a",
        handlers={"custom.success": MagicMock()},
    )
    assert complete.call_args.kwargs["delivered"] is True

    handler = MagicMock(side_effect=RuntimeError("external down"))
    entry.topic = "custom.reconcile"
    failed_db = _delivery_db(entry)
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
        reconciler.reconcile_entry(_delivery_db(entry), entry, worker_id="worker-a")


@pytest.mark.parametrize(
    ("topic", "handler_name"),
    [
        ("kubernetes.analysis.submit", "submit_analysis_job"),
        ("upload.reconcile", "reconcile_upload_session"),
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
    db.get.side_effect = lambda model, *_args, **_kwargs: entry if model is OutboxEntry else command

    if topic == "kubernetes.analysis.submit":
        with pytest.raises(RuntimeError, match="factory-owned dispatcher"):
            reconciler.reconcile_entry(db, entry, worker_id="worker-a", k8s=MagicMock())
        handler.assert_not_called()
        complete.assert_not_called()
        return
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
        reconciler.reconcile_entry(_delivery_db(entry), entry, worker_id="worker-a")
    assert complete.call_args.kwargs["delivered"] is False


def test_phase2_upload_verification_uses_bounded_provider_ranges() -> None:
    content = b"0123456789"
    opened: list[io.BytesIO] = []
    store = MagicMock()

    def open_range(_uri, byte_range, *, version):
        assert version == "original-v1"
        source = io.BytesIO(content[byte_range.start : byte_range.end_inclusive + 1])
        opened.append(source)
        return source

    store.open_stream.side_effect = open_range
    reader = reconciler._BoundedObjectReader(
        store,
        "s3c://automl/large.csv",
        len(content),
        range_size=4,
        version="original-v1",
    )

    assert reader.read(3) == b"012"
    assert reader.read(5) == b"34567"
    assert reader.read() == b"89"
    assert reader.read(1) == b""
    reader.close()
    reader.close()

    ranges = [call.args[1] for call in store.open_stream.call_args_list]
    assert [(value.start, value.end_inclusive) for value in ranges] == [
        (0, 3),
        (4, 7),
        (8, 9),
    ]
    assert all(source.closed for source in opened)


def test_phase2_upload_verification_rejects_short_provider_range() -> None:
    store = MagicMock()
    store.open_stream.return_value = io.BytesIO(b"x")
    reader = reconciler._BoundedObjectReader(
        store, "s3c://automl/object.csv", 2, version="original-v1"
    )
    with pytest.raises(OSError, match="before its declared size"):
        reader.read(2)
    reader.close()


def test_registry_requires_factory_owned_dispatch():
    entry = _entry(_attempt(_run()), "registry.reconcile")
    with pytest.raises(RuntimeError, match="factory-owned dispatcher"):
        reconciler.reconcile_entry(_delivery_db(entry), entry, worker_id="worker-a")


def test_analysis_reconciler_submits_once_and_records_desired_state() -> None:
    run = _run()
    run.tags = {"desired_state": "kubernetes_submission_pending"}
    entry = _entry(_attempt(run), "kubernetes.analysis.submit")
    entry.payload = {"run_id": str(run.id), "manifest": {"metadata": {"name": "analysis"}}}
    db = MagicMock()
    db.scalar.side_effect = [run, 0, run, None]
    k8s = MagicMock()
    k8s.capacity_snapshot.return_value.capacity = SimpleNamespace(
        connected=True, active_training_jobs=0
    )
    k8s.settings.max_concurrent_jobs = 1

    assert submit_analysis_job(db, entry, k8s) is run
    k8s.create_job.assert_called_once_with(entry.payload["manifest"])
    assert run.tags["desired_state"] == "kubernetes_submitted"
    assert submit_analysis_job(db, entry, k8s) is run
    k8s.create_job.assert_called_once()

    with pytest.raises(LookupError, match="analysis run"):
        submit_analysis_job(db, entry, k8s)


@pytest.mark.parametrize(
    ("connected", "pods", "submitted"), [(True, 1, 0), (True, 0, 1), (False, 0, 0)]
)
def test_analysis_waits_for_capacity_without_spending_retry_budget(connected, pods, submitted):
    run = _run()
    run.status = RunStatus.QUEUED
    run.tags = {"desired_state": "kubernetes_submission_pending"}
    entry = _entry(_attempt(run), "kubernetes.analysis.submit")
    entry.payload = {"run_id": str(run.id), "manifest": {"metadata": {"name": "analysis"}}}
    entry.delivery_attempts = 3
    db = MagicMock()
    db.scalar.side_effect = [run, submitted]
    db.get.return_value = entry
    k8s = MagicMock()
    k8s.capacity_snapshot.return_value.capacity = SimpleNamespace(
        connected=connected, active_training_jobs=pods
    )
    k8s.settings.max_concurrent_jobs = 1
    before = datetime.now(UTC)
    reconcile_analysis_entry(db, entry, worker_id="worker-a", k8s=k8s)
    k8s.create_job.assert_not_called()
    assert run.status == RunStatus.QUEUED
    assert run.tags["waiting_for_capacity"] is True
    assert entry.status == OutboxStatus.PENDING
    assert entry.delivery_attempts == 2
    assert entry.available_at > before
    assert entry.lease_owner is None


@pytest.mark.parametrize("terminal", [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED])
def test_analysis_queued_command_cannot_resurrect_terminal_run(terminal):
    run = _run()
    run.status = terminal
    run.tags = {"desired_state": "kubernetes_submission_pending"}
    entry = _entry(_attempt(run), "kubernetes.analysis.submit")
    entry.payload = {"run_id": str(run.id), "manifest": {}}
    db, k8s = MagicMock(), MagicMock()
    db.scalar.return_value = run
    assert submit_analysis_job(db, entry, k8s) is run
    k8s.create_job.assert_not_called()
    k8s.capacity_snapshot.assert_not_called()


def test_deployment_topic_requires_factory_dispatcher():
    entry = _entry(_attempt(_run()), "deployment.reconcile")
    with pytest.raises(RuntimeError, match="factory-owned dispatcher"):
        reconciler.reconcile_entry(_delivery_db(entry), entry, worker_id="worker-a")


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
    run = _run()
    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="ray-job")
    client.custom.create_namespaced_custom_object.side_effect = ApiException(status=409)
    client.custom.get_namespaced_custom_object.return_value = {
        **manifest,
        "metadata": {**manifest["metadata"], "uid": "native-ray-uid"},
    }
    with pytest.raises(ApiException) as held:
        client.create_ray_job(manifest)
    assert held.value.status == 409
    client.custom.get_namespaced_custom_object.assert_not_called()
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


@pytest.mark.parametrize(
    "message",
    [
        "3 worker(s) were killed due to the node running low on memory. "
        "OOM kill reason: threshold exceeded",
        "ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory",
    ],
)
def test_ray_memory_failure_stops_instead_of_restarting_candidate_search(monkeypatch, message):
    run = _run()
    run.tags = {"leaderboard": [{"model": "DecisionTreeClassifier", "status": "succeeded"}]}
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.retry_budget = 5
    db = MagicMock()
    db.scalars.side_effect = [[attempt], []]
    db.scalars.return_value = [attempt]
    db.scalar.return_value = run
    enqueue = MagicMock()
    monkeypatch.setattr(reconciler, "enqueue_outbox", enqueue)
    k8s = MagicMock()
    k8s.ray_job.return_value = {"status": {"jobStatus": "FAILED", "message": message}}
    ray_observer_fixture.observe_training_ray_jobs(db, k8s)
    assert attempt.status == AttemptStatus.FAILED
    assert run.failure_code == "TRAINING_OUT_OF_MEMORY"
    assert run.failure_message == message
    assert "memory capacity" in run.plain_english_failure
    assert run.tags["leaderboard"][0]["status"] == "succeeded"
    enqueue.assert_not_called()


def test_fractional_kubernetes_cpu_uses_valid_integer_ray_start_argument(monkeypatch):
    from ray.scripts.scripts import start

    monkeypatch.setattr(reconciler, "get_settings", lambda: Settings())
    run = _run()
    run.cpu_limit_cores = 2.5
    manifest = reconciler.build_ray_job_manifest(run, _attempt(run), name="fractional-cpu")
    worker = manifest["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]
    argument = worker["rayStartParams"]["num-cpus"]
    option = next(parameter for parameter in start.params if parameter.name == "num_cpus")
    assert option.type.convert(argument, option, None) == 3
    resources = worker["template"]["spec"]["containers"][0]["resources"]
    assert resources["limits"]["cpu"] == resources["requests"]["cpu"] == "2.5"


@pytest.mark.parametrize("age", [None, timedelta(minutes=1), timedelta(minutes=6)])
@pytest.mark.parametrize(
    "reason", ["ImagePullBackOff", "CreateContainerConfigError", "Unschedulable", None]
)
def test_dataset_startup_recovery_observes_grace_and_does_not_retry_capacity_waits(age, reason):
    observed = None if age is None else datetime.now(UTC) - age
    status = {
        "rayClusterStatus": {
            "conditions": [{"reason": reason}, {"ready": False}],
            "nested": {"conditions": []},
        }
    }
    failure = reconciler._ray_startup_failure(status, observed)
    fatal = age == timedelta(minutes=6) and reason in {
        "ImagePullBackOff",
        "CreateContainerConfigError",
    }
    assert bool(failure) is fatal
    if fatal:
        assert reason in failure
        # Old database timestamps without timezone information represent UTC.
        assert reconciler._ray_startup_failure(status, observed.replace(tzinfo=None)) == failure


@pytest.mark.parametrize("upload_status", ["completed", "object_completed", "verifying"])
@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {
            "digest_algorithm": "sha256",
            "digest_scope": "multipart_manifest",
            "expected_object_digest": "a" * 64,
        },
        {
            "digest_algorithm": "md5",
            "digest_scope": "byte_stream",
            "expected_object_digest": "a" * 64,
        },
        {
            "digest_algorithm": "sha256",
            "digest_scope": "byte_stream",
            "expected_object_digest": None,
        },
        {
            "digest_algorithm": "sha256",
            "digest_scope": "byte_stream",
            "expected_object_digest": "z" * 64,
        },
    ],
)
def test_upload_reconciler_blocks_unqualified_digest_without_mutating_history(
    monkeypatch, upload_status, metadata
) -> None:
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    session = SimpleNamespace(
        id=uuid.UUID(entry.payload["session_id"]),
        project_id=entry.project_id,
        status=upload_status,
        dataset_version_id=uuid.uuid4(),
        completed_object_uri="s3c://bucket/original",
        **metadata,
    )
    original = vars(session).copy()
    db = MagicMock()
    db.scalar.return_value = session
    store = MagicMock()
    monkeypatch.setattr(reconciler, "get_object_store", store)
    with pytest.raises(ValueError, match="byte-stream SHA-256"):
        reconciler.reconcile_upload_session(db, entry)
    store.assert_not_called()
    db.flush.assert_not_called()
    db.commit.assert_not_called()
    assert vars(session) == original


@pytest.mark.parametrize("status", ["completed", "object_completed", "verifying"])
def test_legacy_upload_custody_cannot_infer_readiness_or_mutate_history(monkeypatch, status):
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    session = SimpleNamespace(
        status=status,
        digest_algorithm="sha256",
        digest_scope="byte_stream",
        expected_object_digest="a" * 64,
    )
    original = vars(session).copy()
    db = MagicMock()
    db.scalar.return_value = session
    store = MagicMock()
    monkeypatch.setattr(reconciler, "get_object_store", store)
    with pytest.raises(ValueError, match="original immutable completion custody"):
        reconciler.reconcile_upload_session(db, entry)
    assert vars(session) == original
    store.assert_not_called()
    db.flush.assert_not_called()
    db.commit.assert_not_called()


def test_legacy_upload_terminal_replay_is_read_only():
    entry = _entry(_attempt(_run()), "upload.reconcile")
    entry.payload = {"session_id": str(uuid.uuid4())}
    db = MagicMock()
    db.scalar.return_value = None
    with pytest.raises(LookupError, match="upload session"):
        reconciler.reconcile_upload_session(db, entry)
    session = SimpleNamespace(status="aborted")
    db.scalar.return_value = session
    assert reconciler.reconcile_upload_session(db, entry) is session
    db.flush.assert_not_called()
    db.commit.assert_not_called()
