"""Declared final result publication uses control RPC without worker SQL."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC
from test_attempt_rpc import control as control


@pytest.fixture
def remote_result(monkeypatch, tmp_path):
    run_id, attempt, project = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    token = tmp_path / "identity"
    token.write_text("fixture-token")
    for key, value in {
        "AUTOML_TRAINING_RESULT_PROTOCOL": "training-result-v1",
        "AUTOML_ATTEMPT_ID": str(attempt),
        "AUTOML_FENCING_TOKEN": "f" * 32,
        "AUTOML_PROJECT_ID": str(project),
        "AUTOML_IDENTITY_TOKEN_FILE": str(token),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.example.test/internal/attempts",
    }.items():
        monkeypatch.setenv(key, value)
    sql = MagicMock(side_effect=AssertionError("declared result opened worker SQL"))
    monkeypatch.setattr(pipeline, "get_session_factory", sql)
    state = {"calls": [], "lose_ack": False, "reply": None}

    def request(control, method, path, **kwargs):
        assert control.attempt_id == attempt
        state["calls"].append((method, path))
        if method == "POST":
            payload = kwargs["payload"]
            state["reply"] = {
                "protocol": "training-result-v1",
                "run_id": str(run_id),
                "attempt_id": str(attempt),
                "project_id": str(project),
                "audience": attempt_audience(attempt, "f" * 32),
                "request_digest": canonical_request_hash(payload),
                "committed": True,
            }
            if state["lose_ack"]:
                raise AttemptControlUnavailable("owned acknowledgement loss")
        return state["reply"]

    monkeypatch.setattr(AttemptRPC, "request", request)
    result = pipeline.TournamentResult(
        metrics={"rmse": 0.2},
        model=object(),
        params={"deduplicated_rows": 0},
        leaderboard=[
            {
                "model": "Ridge",
                "status": "succeeded",
                "metrics": {"rmse": 0.2},
                "model_artifact_uri": "s3://owned/model",
                "model_artifact_sha256": "a" * 64,
            }
        ],
        primary_metric="rmse",
    )
    return run_id, result, sql, state


def test_declared_result_uses_rpc_without_sql(remote_result):
    run_id, result, sql, state = remote_result
    assert pipeline._persist_training_success(run_id, result, "owned-tracking")
    assert [method for method, _ in state["calls"]] == ["POST"]
    sql.assert_not_called()


def test_lost_result_ack_reads_receipt_without_reposting(remote_result):
    run_id, result, sql, state = remote_result
    state["lose_ack"] = True
    assert pipeline._persist_training_success(run_id, result, "owned-tracking")
    assert [method for method, _ in state["calls"]] == ["POST", "GET"]
    sql.assert_not_called()


def test_unknown_result_protocol_never_falls_back_to_sql(remote_result, monkeypatch):
    monkeypatch.setenv("AUTOML_TRAINING_RESULT_PROTOCOL", "unknown")
    with pytest.raises(StaleFence):
        pipeline._persist_training_success(remote_result[0], remote_result[1], "owned-tracking")
    remote_result[2].assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_digest", "f" * 64),
        ("run_id", str(uuid.UUID(int=1))),
        ("attempt_id", str(uuid.UUID(int=2))),
        ("project_id", str(uuid.UUID(int=3))),
        ("committed", False),
        ("committed", 1),
        ("unexpected", "field"),
    ],
)
def test_result_acknowledgement_must_match_exact_closed_identity(
    remote_result, monkeypatch, field, value
):
    original = AttemptRPC.request

    def request(*args, **kwargs):
        return {**original(*args, **kwargs), field: value}

    monkeypatch.setattr(AttemptRPC, "request", request)
    with pytest.raises(StaleFence):
        pipeline._persist_training_success(remote_result[0], remote_result[1], "owned-tracking")
    remote_result[2].assert_not_called()


def test_v6_requires_control_url_and_preserves_v5_without_new_result_flag():
    from unittest.mock import patch

    from automl_api.core.config import Settings
    from automl_api.services import reconciler
    from test_phase1_reconciler import _attempt, _run
    from test_ray_pod_credentials import env, pods

    run = _run()
    with patch.object(
        reconciler, "get_settings", return_value=Settings(attempt_control_base_url="")
    ):
        with pytest.raises(ValueError, match="control URL"):
            reconciler.build_ray_job_manifest(
                run,
                _attempt(run),
                name="missing-control",
                credential_protocol="ray-training-result-v6",
            )
        old = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="saved-v5", credential_protocol="ray-model-output-v5"
        )
        assert all("AUTOML_TRAINING_RESULT_PROTOCOL" not in env(pod) for pod in pods(old))
    with patch.object(
        reconciler,
        "get_settings",
        return_value=Settings(
            attempt_control_base_url="https://control.example.test/internal/attempts"
        ),
    ):
        current = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="saved-v6", credential_protocol="ray-training-result-v6"
        )
    submitter, head, worker = pods(current)
    assert env(head)["AUTOML_TRAINING_RESULT_PROTOCOL"]["value"] == "training-result-v1"
    assert all("AUTOML_TRAINING_RESULT_PROTOCOL" not in env(pod) for pod in (submitter, worker))


def test_unrepresentable_metrics_fail_before_any_rpc_or_sql(remote_result):
    remote_result[1].metrics["rmse"] = 10**500
    with pytest.raises(StaleFence):
        pipeline._persist_training_success(remote_result[0], remote_result[1], "owned-tracking")
    assert not remote_result[3]["calls"]
    remote_result[2].assert_not_called()


def test_result_projection_preserves_row_counts_and_excludes_unused_params(
    remote_result, monkeypatch
):
    run_id, result, sql, state = remote_result
    result.leaderboard[0].update(training_rows=8, validation_rows=2)
    result.params["DATABASE_URL"] = "never-export"
    original = AttemptRPC.request

    def request(control, method, path, **kwargs):
        assert "DATABASE_URL" not in kwargs["payload"]["params"]
        assert kwargs["payload"]["leaderboard"][0]["training_rows"] == 8
        return original(control, method, path, **kwargs)

    monkeypatch.setattr(AttemptRPC, "request", request)
    assert pipeline._persist_training_success(run_id, result, "owned-tracking")
    sql.assert_not_called()


@pytest.mark.parametrize("timeout", [None, 1, 60])
def test_result_deadline_is_bounded_without_changing_default_rpc_timeout(control, timeout):
    rpc, _, requests, reply = control
    reply["timeout"] = 5 if timeout is None else timeout
    assert rpc.request(
        "POST", "training/result", payload={}, max_bytes=64, timeout_seconds=timeout
    ) == {"ok": True}
    assert len(requests) == 1


@pytest.mark.parametrize("timeout", [0, -1, 61, True, "60"])
def test_invalid_control_deadline_never_sends_identity(control, timeout):
    rpc, _, requests, _ = control
    with pytest.raises(AttemptControlUnavailable):
        rpc.request("POST", "training/result", payload={}, max_bytes=64, timeout_seconds=timeout)
    assert not requests
