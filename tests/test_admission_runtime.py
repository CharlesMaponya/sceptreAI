import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import httpx
import pytest
from automl_api.core.config import Settings
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services import reconciler
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence
from automl_api.training import attempt_admission, trial_runtime, worker
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401


@pytest.fixture
def admission(runtime, monkeypatch, tmp_path):  # noqa: F811
    engine, run, scope = runtime
    path = tmp_path / "token"
    path.write_text("projected-token-a")
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(path))
    monkeypatch.setenv(
        "ATTEMPT_CONTROL_BASE_URL", "https://control.invalid/api/v1/internal/attempts"
    )
    proof = {
        "attempt_id": str(scope[2]),
        "project_id": str(run.project_id),
        "generation": 1,
        "audience": attempt_audience(scope[2], scope[3]),
        "expires_at": int(time.time()) + 30,
    }

    def response(*_a, **_k):
        return httpx.Response(
            200, json=proof, request=httpx.Request("POST", "https://control.invalid")
        )

    post = MagicMock(side_effect=response)
    monkeypatch.setattr(attempt_admission.httpx, "post", post)
    return runtime, proof, path, post


def test_worker_admission_reloads_rotated_token_and_caps_parent_lease(admission):
    runtime_scope, proof, path, post = admission
    engine, run, scope = runtime_scope
    context = worker._begin_fenced_attempt(run.id, worker_id="owned")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, scope[2]).lease_expires_at == datetime.fromtimestamp(
            proof["expires_at"], UTC
        )
    path.write_text("projected-token-b")
    proof["expires_at"] = int(time.time()) + 50
    worker._heartbeat_fenced_attempt(run.id, context, "owned")
    assert post.call_args.kwargs["headers"]["Authorization"] == "Bearer projected-token-b"
    assert post.call_args.kwargs["follow_redirects"] is False
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, scope[2]).lease_expires_at == datetime.fromtimestamp(
            proof["expires_at"], UTC
        )


@pytest.mark.parametrize(
    "invalid",
    [
        "http",
        "userinfo",
        "query",
        "missing_token",
        "denied",
        "expired",
        "long_expiry",
        "attempt",
        "audience",
        "project",
        "generation",
    ],
)
def test_worker_never_claims_with_invalid_admission(admission, monkeypatch, invalid):
    runtime_scope, proof, path, post = admission
    engine, run, scope = runtime_scope
    if invalid in {"http", "userinfo", "query"}:
        monkeypatch.setenv(
            "ATTEMPT_CONTROL_BASE_URL",
            {
                "http": "http://control.invalid",
                "userinfo": "https://secret@control.invalid",
                "query": "https://control.invalid?token=secret",
            }[invalid],
        )
    elif invalid == "missing_token":
        path.unlink()
    elif invalid == "denied":
        post.side_effect = httpx.ConnectError("unavailable")
    elif invalid == "expired":
        proof["expires_at"] = int(time.time()) - 1
    elif invalid == "long_expiry":
        proof["expires_at"] = int(time.time()) + 3600
    elif invalid == "attempt":
        proof["attempt_id"] = "another"
    elif invalid == "audience":
        proof["audience"] = "another"
    elif invalid == "project":
        proof["project_id"] = "another"
    else:
        proof["generation"] = 99
    with pytest.raises(StaleFence):
        worker._begin_fenced_attempt(run.id, worker_id="untrusted")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, scope[2]).lease_owner is None


def test_trial_lease_is_bounded_by_own_verified_admission(admission):
    runtime_scope, proof, _, _ = admission
    engine, run, scope = runtime_scope

    def compute():
        with Session(engine) as db:
            child = db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == scope[2])
            )
            assert child.lease_expires_at == datetime.fromtimestamp(proof["expires_at"], UTC)
        return {"score": 0.5}

    assert trial_runtime.evaluate_trial(run, "Ridge", "admitted", {"alpha": 1}, compute) == {
        "score": 0.5
    }


def test_production_admission_cannot_be_disabled(monkeypatch):
    monkeypatch.delenv("ATTEMPT_CONTROL_BASE_URL", raising=False)
    monkeypatch.setattr(
        attempt_admission, "get_settings", lambda: Settings(environment="production")
    )
    with pytest.raises(StaleFence, match="configuration"):
        attempt_admission.admit_parent(None)


def test_projected_manifest_keeps_token_out_of_env_and_scopes_all_pods(monkeypatch):
    from test_phase1_reconciler import _attempt, _run

    settings = Settings(
        environment="production",
        attempt_control_base_url="https://control.invalid/api/v1/internal/attempts",
    )
    monkeypatch.setattr(reconciler, "get_settings", lambda: settings)
    run = _run()
    attempt = _attempt(run)
    manifest = reconciler.build_ray_job_manifest(run, attempt, name="scoped-ray")
    cluster = manifest["spec"]["rayClusterSpec"]
    pods = [
        manifest["spec"]["submitterPodTemplate"]["spec"],
        cluster["headGroupSpec"]["template"]["spec"],
        cluster["workerGroupSpecs"][0]["template"]["spec"],
    ]
    for pod in pods:
        volume = next(v for v in pod["volumes"] if v["name"] == "attempt-identity")
        source = volume["projected"]["sources"][0]["serviceAccountToken"]
        assert source["expirationSeconds"] == 600
        assert source["audience"] == attempt_audience(attempt.id, attempt.fencing_token)
        mount = next(
            m for m in pod["containers"][0]["volumeMounts"] if m["name"] == "attempt-identity"
        )
        assert mount["readOnly"] and "subPath" not in mount
        env = {e["name"]: e for e in pod["containers"][0]["env"]}
        assert env["ENVIRONMENT"]["value"] == "production"
        assert env["ATTEMPT_CONTROL_BASE_URL"]["value"].startswith("https://")
        assert "token" not in {e.get("value") for e in env.values()}
    assert "serviceAccountName" not in pods[1]
    assert (
        not pods[0]["automountServiceAccountToken"] and not pods[2]["automountServiceAccountToken"]
    )


def test_production_manifest_requires_control_url(monkeypatch):
    from test_phase1_reconciler import _attempt, _run

    monkeypatch.setattr(reconciler, "get_settings", lambda: Settings(environment="production"))
    run = _run()
    with pytest.raises(ValueError, match="admission URL"):
        reconciler.build_ray_job_manifest(run, _attempt(run), name="missing-admission")


def test_dispatch_admission_failure_creates_no_trial(admission):
    from automl_api.models.workflows import TrainingTrial

    runtime_scope, _, _, post = admission
    engine, run, scope = runtime_scope
    post.side_effect = httpx.ConnectError("control unavailable")
    with pytest.raises(StaleFence):
        trial_runtime.enroll_evaluation(
            run, "Ridge", "pre-dispatch", {"alpha": 1}, (scope[2], scope[3]), dispatch_id="physical"
        )
    with Session(engine) as db:
        assert not list(
            db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        )


def test_cached_trial_still_requires_current_admission(admission):
    runtime_scope, proof, _, _ = admission
    _, run, _ = runtime_scope
    compute = MagicMock(return_value={"score": 0.5})
    assert trial_runtime.evaluate_trial(run, "Ridge", "cached", {"alpha": 1}, compute) == {
        "score": 0.5
    }
    proof["expires_at"] = int(time.time()) - 1
    with pytest.raises(StaleFence):
        trial_runtime.evaluate_trial(run, "Ridge", "cached", {"alpha": 1}, compute)
    compute.assert_called_once()


def test_child_cannot_publish_after_its_proof_expires(admission, monkeypatch):
    from automl_api.services import training_trials

    runtime_scope, proof, _, _ = admission
    engine, run, scope = runtime_scope
    actual_datetime = training_trials.datetime

    class AfterExpiry(actual_datetime):
        @classmethod
        def now(cls, tz=None):
            return actual_datetime.fromtimestamp(proof["expires_at"] + 1, UTC)

    def compute():
        monkeypatch.setattr(training_trials, "datetime", AfterExpiry)
        return {"score": 0.5}

    with pytest.raises(StaleFence):
        trial_runtime.evaluate_trial(run, "Ridge", "expires-during-cv", {"alpha": 1}, compute)
    with Session(engine) as db:
        child = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == scope[2]))
        assert child.terminal_cas_version == 0


def test_actual_entrypoint_uses_admission_for_start_and_renewal(admission, monkeypatch):
    import sys
    from threading import Event

    from automl_api.models.enums import RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.training import pipeline

    runtime_scope, _, _, post = admission
    engine, run, _ = runtime_scope
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["worker", "--run-id", str(run.id), "--attempt-protocol", "training-catalog-v1"],
    )
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", "direct")
    monkeypatch.setattr(worker, "_PARENT_HEARTBEAT_INTERVAL", 0.01)
    renewed = Event()
    original = worker._heartbeat_fenced_attempt

    def heartbeat(*args):
        original(*args)
        renewed.set()

    monkeypatch.setattr(worker, "_heartbeat_fenced_attempt", heartbeat)
    result = pipeline.TournamentResult(
        metrics={"r2": 0.9},
        model=None,
        params={},
        primary_metric="r2",
        leaderboard=[
            {"model": "Ridge", "status": "succeeded", "model_artifact_uri": "s3://test/model"}
        ],
    )

    def execute(_):
        assert renewed.wait(3)
        assert pipeline._persist_training_success(run.id, result, "mlflow")

    monkeypatch.setattr(pipeline, "execute_training_run", execute)
    worker.main()
    assert post.call_count >= 2
    with Session(engine) as db:
        assert db.get(ModelRun, run.id).status == RunStatus.SUCCEEDED
