"""Declared trial workers use control RPC without local SQL or legacy admission."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.models.runs import ModelRun
from automl_api.services.attempt_identity import attempt_audience
from automl_api.training import trial_runtime
from automl_api.training.attempt_rpc import AttemptRPC
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_pod_credentials import env, pods
from test_ray_pod_credentials import manifests as manifests
from test_ray_pod_credentials import stage as stage
from test_training_publication_postgres import publication as publication
from test_training_trial_enrollment import trial_scope as trial_scope


@pytest.fixture
def remote_trial(trial_scope, monkeypatch, tmp_path):
    engine, run_id, parent_id, fence, _ = trial_scope
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
    child = uuid.uuid4()
    token = tmp_path / "token"
    token.write_text("fixture-token")
    monkeypatch.setenv("AUTOML_TRIAL_CONTROL_PROTOCOL", "trial-control-v1")
    monkeypatch.setenv("AUTOML_PROJECT_ID", str(run.project_id))
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "https://control.example.test/internal/attempts")
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token))
    sql = MagicMock(side_effect=AssertionError("declared trial opened worker SQL"))
    admission = MagicMock(side_effect=AssertionError("declared trial used legacy admission"))
    monkeypatch.setattr(trial_runtime, "get_session_factory", sql)
    monkeypatch.setattr(trial_runtime, "admit_parent", admission)
    state = {"terminal": None, "calls": []}

    def request(control, method, path, *, payload, max_bytes):
        state["calls"].append(path)
        assert control.attempt_id == parent_id and method == "POST"
        if path == "trials/finish":
            state["terminal"] = (
                {"status": "failed", "failure": payload["failure"]}
                if payload.get("failure") is not None
                else {"status": "succeeded", "result": payload["result"]}
            )
        return {
            "protocol": "trial-control-v1",
            "parent_attempt_id": str(parent_id),
            "run_id": str(run_id),
            "project_id": str(run.project_id),
            "audience": attempt_audience(parent_id, fence),
            "child_attempt_id": str(child),
            "terminal": state["terminal"] if path != "trials/enroll" else None,
        }

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run, state, sql, admission


def execute(remote_trial, callback):
    return trial_runtime.evaluate_trial(remote_trial[0], "Ridge", "cv", {"alpha": 1}, callback)


def test_declared_trial_enrolls_starts_finishes_without_worker_sql(remote_trial):
    compute = MagicMock(return_value={"scores": [0.9, 0.8]})
    assert execute(remote_trial, compute) == {"scores": [0.9, 0.8]}
    assert remote_trial[1]["calls"] == ["trials/enroll", "trials/start", "trials/finish"]
    compute.assert_called_once()
    remote_trial[2].assert_not_called()
    remote_trial[3].assert_not_called()


def test_declared_trial_two_terminal_replays_do_not_compute(remote_trial):
    remote_trial[1]["terminal"] = {"status": "succeeded", "result": {"scores": [1.0]}}
    compute = MagicMock(side_effect=AssertionError("terminal trial recomputed"))
    for _ in range(2):
        assert execute(remote_trial, compute) == {"scores": [1.0]}
    compute.assert_not_called()
    remote_trial[2].assert_not_called()
    remote_trial[3].assert_not_called()


def test_declared_trial_records_failure_and_replays_without_compute(remote_trial):
    compute = MagicMock(side_effect=ValueError("invalid fold"))
    with pytest.raises(ValueError, match="invalid fold"):
        execute(remote_trial, compute)
    with pytest.raises(trial_runtime.TrialEvaluationFailed, match="invalid fold"):
        execute(remote_trial, compute)
    compute.assert_called_once()
    remote_trial[2].assert_not_called()
    remote_trial[3].assert_not_called()


def test_unmarked_local_evaluation_still_runs_without_control(monkeypatch):
    for name in (
        "AUTOML_TRIAL_CONTROL_PROTOCOL",
        "AUTOML_ATTEMPT_ID",
        "AUTOML_FENCING_TOKEN",
        "AUTOML_MODEL_PODS",
        "TRAINING_EXECUTION_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
    assert trial_runtime.evaluate_trial(None, "Ridge", "local", {}, lambda: {"scores": [1]}) == {
        "scores": [1]
    }


@pytest.mark.parametrize("protocol", ["", "unknown", "parent-control-v1"])
def test_bad_trial_protocol_cannot_fall_back_or_compute(remote_trial, monkeypatch, protocol):
    monkeypatch.setenv("AUTOML_TRIAL_CONTROL_PROTOCOL", protocol)
    compute = MagicMock()
    with pytest.raises(trial_runtime.StaleFence):
        execute(remote_trial, compute)
    compute.assert_not_called()
    remote_trial[2].assert_not_called()
    remote_trial[3].assert_not_called()


def test_declared_trial_without_parent_cannot_compute(remote_trial, monkeypatch):
    monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    compute = MagicMock()
    with pytest.raises(trial_runtime.StaleFence):
        execute(remote_trial, compute)
    compute.assert_not_called()


def test_remote_heartbeat_does_not_open_sql_or_legacy_admission(remote_trial, monkeypatch):
    import time

    monkeypatch.setattr(trial_runtime, "_HEARTBEAT_INTERVAL", 0.01)

    def compute():
        time.sleep(0.1)
        assert "trials/heartbeat" in remote_trial[1]["calls"]
        return {"scores": [1]}

    assert execute(remote_trial, compute) == {"scores": [1]}
    remote_trial[2].assert_not_called()
    remote_trial[3].assert_not_called()


def test_control_unavailable_stops_compute_without_sql(remote_trial, monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("https://user:password@unavailable.invalid")

    monkeypatch.setattr(AttemptRPC, "request", unavailable)
    compute = MagicMock()
    with pytest.raises(trial_runtime.StaleFence) as error:
        execute(remote_trial, compute)
    assert "password" not in str(error.value)
    compute.assert_not_called()
    remote_trial[2].assert_not_called()


def test_oversized_trial_metadata_fails_before_rpc(remote_trial):
    with pytest.raises(trial_runtime.StaleFence):
        trial_runtime.enroll_evaluation(
            remote_trial[0],
            "Ridge",
            "cv",
            {"large": "x" * 262144},
            trial_runtime.trial_parent_context(),
        )
    assert remote_trial[1]["calls"] == []
    remote_trial[2].assert_not_called()


@pytest.mark.parametrize("configured", [False, True])
def test_v3_training_roles_select_trial_control_and_older_policies_do_not(
    manifests, configured, monkeypatch
):
    from dataclasses import replace

    from automl_api.services import reconciler

    build, _, stage, settings = manifests
    settings = replace(
        settings,
        attempt_control_base_url="https://control.example.test/internal/attempts"
        if configured
        else "",
    )
    monkeypatch.setattr(reconciler, "get_settings", lambda: settings)
    submitter, head, compute = pods(build(credential_protocol="ray-trial-control-v3"))
    assert "AUTOML_TRIAL_CONTROL_PROTOCOL" not in env(submitter)
    for pod in (head, compute):
        assert env(pod).get("AUTOML_TRIAL_CONTROL_PROTOCOL") == (
            {"name": "AUTOML_TRIAL_CONTROL_PROTOCOL", "value": "trial-control-v1"}
            if configured and stage == "training_run"
            else None
        )
    assert ("AUTOML_PARENT_CONTROL_PROTOCOL" in env(head)) == (
        configured and stage == "training_run"
    )
    assert "AUTOML_PARENT_CONTROL_PROTOCOL" not in env(compute)
    for protocol in (None, "ray-pod-env-v1", "ray-parent-control-v2"):
        assert all(
            "AUTOML_TRIAL_CONTROL_PROTOCOL" not in env(pod)
            for pod in pods(build(credential_protocol=protocol))
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_attempt_id", str(uuid.UUID(int=1))),
        ("run_id", str(uuid.UUID(int=2))),
        ("project_id", str(uuid.UUID(int=3))),
        ("audience", "wrong-audience"),
        ("protocol", "unknown"),
        ("fencing_token", "forbidden-extra-field"),
    ],
)
def test_wrong_or_open_control_reply_cannot_start_compute(remote_trial, monkeypatch, field, value):
    request = AttemptRPC.request

    def tampered(*args, **kwargs):
        reply = request(*args, **kwargs)
        reply[field] = value
        return reply

    monkeypatch.setattr(AttemptRPC, "request", tampered)
    compute = MagicMock()
    with pytest.raises(trial_runtime.StaleFence):
        execute(remote_trial, compute)
    compute.assert_not_called()
    remote_trial[2].assert_not_called()


def test_protocol_withdrawal_during_compute_cannot_acknowledge_unstored_result(
    remote_trial, monkeypatch
):
    def compute():
        monkeypatch.delenv("AUTOML_TRIAL_CONTROL_PROTOCOL")
        return {"scores": [1]}

    with pytest.raises(trial_runtime.StaleFence, match="withdrawn"):
        execute(remote_trial, compute)
    assert remote_trial[1]["terminal"] is None
    remote_trial[2].assert_not_called()


def test_lost_finish_response_recovers_without_recomputing(remote_trial, monkeypatch):
    request = AttemptRPC.request
    lost = []

    def lose_ack(*args, **kwargs):
        reply = request(*args, **kwargs)
        if args[2] == "trials/finish" and not lost:
            lost.append(True)
            raise ConnectionError("lost finish response")
        return reply

    monkeypatch.setattr(AttemptRPC, "request", lose_ack)
    compute = MagicMock(return_value={"scores": [1]})
    with pytest.raises(trial_runtime.StaleFence):
        execute(remote_trial, compute)
    assert execute(remote_trial, compute) == {"scores": [1]}
    assert lost == [True]
    compute.assert_called_once()
    remote_trial[2].assert_not_called()


def test_finish_acknowledgement_for_other_result_is_rejected(remote_trial, monkeypatch):
    request = AttemptRPC.request

    def wrong_result(*args, **kwargs):
        reply = request(*args, **kwargs)
        if args[2] == "trials/finish":
            reply["terminal"] = {"status": "succeeded", "result": {"scores": [0]}}
        return reply

    monkeypatch.setattr(AttemptRPC, "request", wrong_result)
    with pytest.raises(trial_runtime.StaleFence):
        execute(remote_trial, lambda: {"scores": [1]})
    remote_trial[2].assert_not_called()
