import sys
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.models.runs import Metric, ModelRun, RunArtifact
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services import reconciler
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.training import analysis, attempt_admission, worker
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401


@pytest.fixture(params=[RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT])
def analysis_scope(publication, monkeypatch, tmp_path, request):  # noqa: F811
    engine, run_id, attempt_id, fence, _ = publication
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.run_kind = request.param
        run.status = RunStatus.QUEUED
        run.k8s_namespace = "analysis-test"
        run.tags = {"workflow_attempt_id": str(attempt_id)}
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.logical_key = f"analysis-run:{run_id}"
        attempt.workload_identity = f"project-{run.project_id}-analysis"
        attempt.status = AttemptStatus.SUBMITTED
        project_id = run.project_id
    store = EmbeddedObjectStoreDriver(root=tmp_path / "store", bucket="analysis-test")
    token_file = tmp_path / "identity-token"
    token_file.write_text("test-token")
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "https://control.invalid/internal/attempts")
    monkeypatch.setattr(
        analysis, "get_session_factory", lambda: lambda: Session(engine, expire_on_commit=False)
    )
    monkeypatch.setattr(analysis, "get_object_store", lambda: store)
    monkeypatch.setattr(
        attempt_admission,
        "get_settings",
        lambda: Settings(environment="local", attempt_control_base_url=""),
    )
    proof = {
        "attempt_id": str(attempt_id),
        "project_id": str(project_id),
        "generation": 1,
        "audience": attempt_audience(attempt_id, fence),
        "expires_at": int(time.time()) + 600,
    }
    post = MagicMock(
        side_effect=lambda *_a, **_k: httpx.Response(
            200, json=proof, request=httpx.Request("POST", "https://control.invalid")
        )
    )
    monkeypatch.setattr(attempt_admission.httpx, "post", post)
    result = {"metrics": {"rmse": 2.0}, "diagnostics": {"rows": 2}, "feature_importance": []}
    return engine, run_id, attempt_id, fence, store, result, proof, post


def test_analysis_success_commits_one_report_and_terminal_cas(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, fence, _, result, _, _ = analysis_scope
    monkeypatch.setattr(
        sys, "argv", ["worker", "--run-id", str(run_id), "--attempt-protocol", "analysis-v1"]
    )
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    for name in ("_execute_validation", "_execute_explainability", "_execute_drift"):
        monkeypatch.setattr(analysis, name, lambda *_: result)
    worker.main()
    assert worker._resume_committed_result(run_id, (attempt_id, fence))
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        run = db.get(ModelRun, run_id)
        assert attempt.status == AttemptStatus.SUCCEEDED and attempt.terminal_cas_version == 1
        assert attempt.checkpoint_uri == run.tags["artifact_uri"]
        assert run.status == RunStatus.SUCCEEDED
        assert run.tags["analysis_result_receipt"]["attempt_id"] == str(attempt_id)
        assert (
            db.scalar(
                select(func.count())
                .select_from(RunArtifact)
                .where(RunArtifact.model_run_id == run_id)
            )
            == 1
        )
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 1
        )
    with pytest.raises(StaleFence):
        analysis._persist_analysis_result(run_id, result)


@pytest.mark.parametrize("state", ["expired", "superseded", "cancelled"])
def test_stale_analysis_cannot_start_publish_or_overwrite_failure(
    analysis_scope, monkeypatch, state
):
    engine, run_id, attempt_id, _, store, result, _, _ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.failure_message = "control-plane decision"
        if state == "expired":
            db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        elif state == "superseded":
            db.get(WorkflowAttempt, attempt_id).status = AttemptStatus.SUPERSEDED
        else:
            run.status = RunStatus.CANCELLED
    wrapped = MagicMock(wraps=store)
    monkeypatch.setattr(analysis, "get_object_store", lambda: wrapped)
    with pytest.raises(StaleFence):
        analysis.execute_analysis_run(run_id)
    with pytest.raises(StaleFence):
        analysis._persist_analysis_result(run_id, result)
    try:
        analysis._mark_analysis_failed(run_id, ValueError("late worker error"))
    except StaleFence:
        pass
    wrapped.put_bytes.assert_not_called()
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).failure_message == "control-plane decision"
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0


def test_analysis_manifest_projects_bound_identity(analysis_scope):
    engine, run_id, attempt_id, fence, _, _, _, _ = analysis_scope
    k8s = KubernetesTrainingClient(
        Settings(
            environment="production",
            training_namespace="analysis-test",
            attempt_control_base_url="https://control.invalid/internal/attempts",
        )
    )
    estimate = SimpleNamespace(
        cpu_request_cores=1,
        memory_request_mb=512,
        cpu_limit_cores=1,
        memory_limit_mb=1024,
        gpu_vendor=None,
        gpu_requested=False,
        active_deadline_seconds=600,
    )
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        manifest = k8s.build_job_manifest(
            run_id=run_id, project_id=attempt.project_id, estimate=estimate, attempt=attempt
        )
        assert f"-g{attempt.generation}" in manifest["metadata"]["name"]
        pod = manifest["spec"]["template"]["spec"]
        env = {e["name"]: e.get("value") for e in pod["containers"][0]["env"]}
        assert env["ENVIRONMENT"] == "production" and env["AUTOML_FENCING_TOKEN"] == fence
        assert env["AUTOML_ATTEMPT_ID"] == str(attempt_id)
        assert (
            pod["serviceAccountName"] == attempt.workload_identity
            and pod["automountServiceAccountToken"] is False
        )
        token = next(v for v in pod["volumes"] if v["name"] == "attempt-identity")["projected"][
            "sources"
        ][0]["serviceAccountToken"]
        assert token == {
            "path": "token",
            "audience": attempt_audience(attempt_id, fence),
            "expirationSeconds": 600,
        }
        assert manifest["spec"]["template"]["metadata"]["labels"][
            "automl.platform/project-id"
        ] == str(attempt.project_id)
        mount = next(
            v for v in pod["containers"][0]["volumeMounts"] if v["name"] == "attempt-identity"
        )
        assert mount["readOnly"] and "subPath" not in mount


def test_analysis_submission_transitions_exact_attempt_once(analysis_scope):
    engine, run_id, attempt_id, fence, _, _, _, _ = analysis_scope

    k8s = MagicMock()
    k8s.settings.max_concurrent_jobs = 1000
    k8s.settings.training_namespace = "analysis-test"
    k8s.capacity_snapshot.return_value.capacity = SimpleNamespace(
        connected=True, active_training_jobs=0
    )
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.status = AttemptStatus.PENDING
        manifest = {
            "metadata": {
                "name": "analysis-test-job",
                "namespace": "analysis-test",
                "labels": {"automl.platform/attempt-id": str(attempt_id)},
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{"env": [{"name": "AUTOML_FENCING_TOKEN", "value": fence}]}]
                    }
                }
            },
        }
        entry = SimpleNamespace(
            id=attempt_id,
            project_id=attempt.project_id,
            payload={"run_id": str(run_id), "manifest": manifest},
        )
        k8s.create_job.return_value = SimpleNamespace(
            metadata=SimpleNamespace(
                name="analysis-test-job", namespace="analysis-test", uid="test-job-uid"
            )
        )
        assert reconciler.submit_analysis_job(db, entry, k8s).id == run_id
        assert attempt.status == AttemptStatus.SUBMITTED
    with Session(engine) as db, db.begin():
        reconciler.submit_analysis_job(db, entry, k8s)
    k8s.create_job.assert_called_once_with(manifest)


def test_analysis_launch_creates_bound_attempt_with_manifest(analysis_scope, monkeypatch):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.iam import User
    from automl_api.services import validation
    from test_validation import _estimate

    engine, run_id, _, _, _, _, _, _ = analysis_scope
    monkeypatch.setattr(validation, "estimate_training_run", lambda *_: _estimate())
    k8s = KubernetesTrainingClient(Settings(training_namespace="analysis-test"))
    with Session(engine) as db, db.begin():
        source = db.get(ModelRun, run_id)
        launched = validation._launch_analysis_run(
            db,
            db.get(User, source.created_by_id),
            source,
            db.get(DatasetVersion, source.dataset_version_id),
            {"model": "Ridge", "model_mlflow_run_id": "source"},
            source.run_kind,
            expected_minutes=5,
            extra_params={},
            client=k8s,
        )
        new_id = launched.run.id
    with Session(engine) as db:
        run = db.get(ModelRun, new_id)
        attempt = db.get(WorkflowAttempt, run.tags["workflow_attempt_id"])
        assert attempt.model_run_id == new_id and attempt.logical_key == f"analysis-run:{new_id}"
        assert attempt.project_id == run.project_id and attempt.status == AttemptStatus.PENDING
        assert launched.manifest["metadata"]["labels"]["automl.platform/attempt-id"] == str(
            attempt.id
        )
        assert run.k8s_job_name == launched.manifest["metadata"]["name"]


def test_analysis_cas_failure_rolls_back_result_metadata(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, _, _, result, _, _ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    monkeypatch.setattr(analysis, "cas_register_terminal_artifact", lambda *_a, **_k: False)
    with pytest.raises(StaleFence):
        analysis._persist_analysis_result(run_id, result)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING
        assert (
            db.scalar(
                select(func.count())
                .select_from(RunArtifact)
                .where(RunArtifact.model_run_id == run_id)
            )
            == 0
        )


def test_analysis_expiry_during_upload_cannot_publish(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, _, store, result, _, _ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    future = datetime.now(UTC) + timedelta(seconds=100)
    wrapped = MagicMock(wraps=store)

    def put(*args):
        value = store.put_bytes(*args)
        monkeypatch.setattr(worker, "datetime", SimpleNamespace(now=lambda *_: future))
        return value

    wrapped.put_bytes.side_effect = put
    monkeypatch.setattr(analysis, "get_object_store", lambda: wrapped)
    with pytest.raises(StaleFence):
        analysis._persist_analysis_result(run_id, result)
    wrapped.put_bytes.assert_called_once()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0
        assert "analysis_result_receipt" not in db.get(ModelRun, run_id).tags


def test_analysis_lost_commit_response_preserves_receipt(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, fence, _, result, _, _ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")

    class LostAck(Session):
        def commit(self):
            super().commit()
            raise RuntimeError("commit response lost")

    monkeypatch.setattr(
        analysis, "get_session_factory", lambda: lambda: LostAck(engine, expire_on_commit=False)
    )
    with pytest.raises(RuntimeError, match="response lost"):
        analysis._persist_analysis_result(run_id, result)
    assert worker._resume_committed_result(run_id, (attempt_id, fence))
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(RunArtifact)
                .where(RunArtifact.model_run_id == run_id)
            )
            == 1
        )


def test_analysis_worker_error_is_fenced_and_redacted(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, _, _, _, _, _ = analysis_scope
    monkeypatch.setattr(
        sys, "argv", ["worker", "--run-id", str(run_id), "--attempt-protocol", "analysis-v1"]
    )
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)

    def broken(*_):
        raise ValueError("cannot read s3://account:secret@bucket/model")

    for name in ("_execute_validation", "_execute_explainability", "_execute_drift"):
        monkeypatch.setattr(analysis, name, broken)
    with pytest.raises(ValueError):
        worker.main()
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        run = db.get(ModelRun, run_id)
        assert attempt.status == AttemptStatus.FAILED and run.status == RunStatus.FAILED
        assert "secret" not in attempt.terminal_reason and "secret" not in run.failure_message


def test_worker_failure_after_cancellation_preserves_control_plane_decision(analysis_scope):
    engine, run_id, attempt_id, fence, _, _, _, _ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.status = RunStatus.CANCELLED
        run.failure_message = "cancelled by user"
    assert not worker._fail_fenced_attempt(
        run_id, (attempt_id, fence), ValueError("late failure"), worker_id="owner"
    )
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.CANCELLED
        assert db.get(ModelRun, run_id).failure_message == "cancelled by user"
