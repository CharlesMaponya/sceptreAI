"""Declared tracking creation uses control RPC and never opens worker SQL."""

import hashlib
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import TaskType
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC


@pytest.fixture
def remote_tracking(monkeypatch, tmp_path):
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        run_name="owned launch",
        tags={},
    )
    attempt = uuid.uuid4()
    token = tmp_path / "identity"
    token.write_text("fixture-token")
    for key, value in {
        "AUTOML_TRAINING_TRACKING_PROTOCOL": "training-tracking-v1",
        "AUTOML_ATTEMPT_ID": str(attempt),
        "AUTOML_FENCING_TOKEN": "f" * 32,
        "AUTOML_PROJECT_ID": str(run.project_id),
        "AUTOML_IDENTITY_TOKEN_FILE": str(token),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.example.test/internal/attempts",
    }.items():
        monkeypatch.setenv(key, value)
    sql = MagicMock(side_effect=AssertionError("declared tracking opened worker SQL"))
    native = MagicMock(side_effect=AssertionError("declared worker opened tracking creation SDK"))
    monkeypatch.setattr(pipeline, "get_session_factory", sql)
    monkeypatch.setattr(pipeline, "MlflowClient", native)
    monkeypatch.setattr(
        pipeline.mlflow, "get_tracking_uri", lambda: "https://tracking.example.test"
    )
    active = object()
    start = MagicMock(return_value=active)
    monkeypatch.setattr(pipeline.mlflow, "start_run", start)
    state = {"calls": [], "lose_ack": False, "reply": None}

    def request(control, method, path, **kwargs):
        assert control.attempt_id == attempt
        state["calls"].append((method, path, kwargs))
        if method == "POST":
            body = kwargs["payload"]
            state["reply"] = dict(
                protocol="training-tracking-v1",
                attempt_id=str(attempt),
                project_id=str(run.project_id),
                run_id=str(run.id),
                role=body["role"],
                audience=attempt_audience(attempt, "f" * 32),
                request_digest=canonical_request_hash(body),
                command_id=str(uuid.uuid4()),
                tracking_run_id="a" * 32,
                parent_tracking_run_id=None if body["role"] == "parent" else "owned-parent",
                tracking_backend_hash=hashlib.sha256(b"https://tracking.example.test").hexdigest(),
            )
            if state["lose_ack"]:
                raise AttemptControlUnavailable("owned committed response loss")
        return state["reply"]

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run, sql, native, active, start, state


@pytest.mark.parametrize("role", ["parent", "candidate:Ridge", "failure:Ridge"])
def test_declared_tracking_creation_never_opens_worker_sql(remote_tracking, role):
    run, sql, native, active, start, state = remote_tracking
    parent = None if role == "parent" else "owned-parent"
    assert (
        pipeline._tracked_mlflow_run(run, role=role, run_name="owned", parent_run_id=parent)
        is active
    )
    assert [method for method, _, _ in state["calls"]] == ["POST"]
    start.assert_called_once_with(run_id="a" * 32, nested=parent is not None)
    sql.assert_not_called()
    native.assert_not_called()


def test_unknown_tracking_protocol_never_falls_back_to_worker_sql(remote_tracking, monkeypatch):
    monkeypatch.setenv("AUTOML_TRAINING_TRACKING_PROTOCOL", "unknown")
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(remote_tracking[0], role="parent", run_name="owned")
    remote_tracking[1].assert_not_called()


def test_uncertain_tracking_post_uses_receipt_lookup_without_recreation(remote_tracking):
    run, sql, native, active, start, state = remote_tracking
    state["lose_ack"] = True
    assert pipeline._tracked_mlflow_run(run, role="parent", run_name="owned") is active
    assert [method for method, _, _ in state["calls"]] == ["POST", "GET"]
    assert all(kwargs["timeout_seconds"] == 60 for _, _, kwargs in state["calls"])
    start.assert_called_once_with(run_id="a" * 32, nested=False)
    sql.assert_not_called()
    native.assert_not_called()


@pytest.mark.parametrize(
    "fault",
    [
        "run_id",
        "attempt_id",
        "project_id",
        "role",
        "audience",
        "request_digest",
        "tracking_backend_hash",
        "parent_tracking_run_id",
        "tracking_run_id",
        "command_id",
        "extra",
    ],
)
def test_wrong_tracking_ack_never_resumes_native_run_or_falls_back(
    remote_tracking, monkeypatch, fault
):
    run, sql, native, _, start, state = remote_tracking
    original = AttemptRPC.request

    def corrupted(control, method, path, **kwargs):
        reply = dict(original(control, method, path, **kwargs))
        reply[fault] = (
            str(uuid.uuid4()) if fault in {"run_id", "attempt_id", "project_id"} else "never-valid"
        )
        if fault == "tracking_run_id":
            reply[fault] = "https://foreign.invalid/run"
        return reply

    monkeypatch.setattr(AttemptRPC, "request", corrupted)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="owned")
    assert [method for method, _, _ in state["calls"]] == ["POST"]
    start.assert_not_called()
    sql.assert_not_called()
    native.assert_not_called()


def test_tracking_receipt_loss_never_reposts_or_resumes_native_run(remote_tracking, monkeypatch):
    run, sql, native, _, start, state = remote_tracking
    state["lose_ack"] = True
    original = AttemptRPC.request

    def unavailable(control, method, path, **kwargs):
        original(control, method, path, **kwargs)
        raise AttemptControlUnavailable("owned receipt loss")

    monkeypatch.setattr(AttemptRPC, "request", unavailable)
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role="parent", run_name="owned")
    assert [method for method, _, _ in state["calls"]] == ["POST", "GET"]
    start.assert_not_called()
    sql.assert_not_called()
    native.assert_not_called()


@pytest.mark.parametrize(
    "role,parent_id",
    [("parent", "unexpected-parent"), ("candidate:Ridge", None), ("failure:Ridge", None)],
)
def test_invalid_local_parent_context_is_rejected_before_creation(remote_tracking, role, parent_id):
    run, sql, native, _, start, state = remote_tracking
    with pytest.raises(StaleFence):
        pipeline._tracked_mlflow_run(run, role=role, run_name="owned", parent_run_id=parent_id)
    assert state["calls"] == []
    start.assert_not_called()
    sql.assert_not_called()
    native.assert_not_called()


def test_v8_tracking_flags_only_training_compute_and_preserves_v7():
    from unittest.mock import patch

    from automl_api.core.config import Settings
    from automl_api.services import reconciler
    from test_phase1_reconciler import _attempt, _run
    from test_ray_pod_credentials import env, pods

    run = _run()
    with patch.object(
        reconciler,
        "get_settings",
        return_value=Settings(
            attempt_control_base_url="https://control.example.test/internal/attempts"
        ),
    ):
        saved = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="saved-v7", credential_protocol="ray-training-progress-v7"
        )
        current = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="current-v8", credential_protocol="ray-training-tracking-v8"
        )
    assert all("AUTOML_TRAINING_TRACKING_PROTOCOL" not in env(pod) for pod in pods(saved))
    submitter, head, worker = pods(current)
    assert "AUTOML_TRAINING_TRACKING_PROTOCOL" not in env(submitter)
    for pod in (head, worker):
        assert env(pod)["AUTOML_TRAINING_TRACKING_PROTOCOL"]["value"] == "training-tracking-v1"
        assert env(pod)["AUTOML_TRAINING_PROGRESS_PROTOCOL"]["value"] == "training-progress-v1"
    with patch.object(
        reconciler, "get_settings", return_value=Settings(attempt_control_base_url="")
    ):
        with pytest.raises(ValueError, match="control URL"):
            reconciler.build_ray_job_manifest(
                run,
                _attempt(run),
                name="missing-v8",
                credential_protocol="ray-training-tracking-v8",
            )
