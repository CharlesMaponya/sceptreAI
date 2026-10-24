"""Declared progress uses authenticated control, never local SQL."""

import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import pipeline
from automl_api.training.attempt_rpc import AttemptControlUnavailable, AttemptRPC


@pytest.fixture
def remote_progress(monkeypatch, tmp_path):
    run, attempt, project = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    token = tmp_path / "identity"
    token.write_text("fixture-token")
    for key, value in {
        "AUTOML_TRAINING_PROGRESS_PROTOCOL": "training-progress-v1",
        "AUTOML_ATTEMPT_ID": str(attempt),
        "AUTOML_FENCING_TOKEN": "f" * 32,
        "AUTOML_PROJECT_ID": str(project),
        "AUTOML_IDENTITY_TOKEN_FILE": str(token),
        "ATTEMPT_CONTROL_BASE_URL": "https://control.example.test/internal/attempts",
    }.items():
        monkeypatch.setenv(key, value)
    sql = MagicMock(side_effect=AssertionError("declared progress opened worker SQL"))
    monkeypatch.setattr(pipeline, "get_session_factory", sql)
    state = {"calls": [], "lose_ack": False, "reply": None}

    def request(control, method, path, **kwargs):
        assert control.attempt_id == attempt
        state["calls"].append((method, path, kwargs))
        if method == "POST":
            body = kwargs["payload"]
            state["reply"] = {
                "protocol": "training-progress-v1",
                "attempt_id": str(attempt),
                "project_id": str(project),
                "run_id": str(run),
                "audience": attempt_audience(attempt, "f" * 32),
                "request_id": body["request_id"],
                "request_digest": canonical_request_hash(body),
                "changed": True,
            }
            if state["lose_ack"]:
                raise AttemptControlUnavailable("owned response loss")
        return state["reply"]

    monkeypatch.setattr(AttemptRPC, "request", request)
    return run, sql, state


@pytest.mark.parametrize("kind", ["phase", "partial"])
def test_declared_progress_does_not_open_worker_sql(remote_progress, kind):
    run, sql, state = remote_progress
    if kind == "phase":
        pipeline._persist_candidate_phase(run, "Ridge", "fitting_final_model")
    else:
        pipeline._persist_partial_leaderboard(
            run, [{"model": "Ridge", "status": "pending", "metrics": {}}], "rmse"
        )
    assert [method for method, _, _ in state["calls"]] == ["POST"]
    sql.assert_not_called()


def test_uncertain_progress_uses_receipt_lookup_only(remote_progress):
    run, sql, state = remote_progress
    state["lose_ack"] = True
    pipeline._persist_candidate_phase(run, "Ridge", "cross_validating")
    assert [method for method, _, _ in state["calls"]] == ["POST", "GET"]
    sql.assert_not_called()


def test_unknown_progress_protocol_never_falls_back(remote_progress, monkeypatch):
    monkeypatch.setenv("AUTOML_TRAINING_PROGRESS_PROTOCOL", "unknown")
    with pytest.raises(StaleFence):
        pipeline._persist_candidate_phase(remote_progress[0], "Ridge", "evaluating")
    remote_progress[1].assert_not_called()


@pytest.mark.parametrize(
    "fault", ["attempt", "run", "project", "audience", "digest", "request", "unknown"]
)
def test_wrong_progress_ack_never_falls_back_to_sql(remote_progress, monkeypatch, fault):
    run, sql, _ = remote_progress
    original = AttemptRPC.request

    def corrupted(control, method, path, **kwargs):
        reply = dict(original(control, method, path, **kwargs))
        key = {
            "attempt": "attempt_id",
            "run": "run_id",
            "project": "project_id",
            "request": "request_id",
            "digest": "request_digest",
            "audience": "audience",
            "unknown": "extra",
        }[fault]
        reply[key] = str(uuid.uuid4()) if key.endswith("_id") else "f" * 64
        return reply

    monkeypatch.setattr(AttemptRPC, "request", corrupted)
    with pytest.raises(StaleFence):
        pipeline._persist_candidate_phase(run, "Ridge", "evaluating")
    sql.assert_not_called()


def test_progress_receipt_loss_never_reposts_or_opens_sql(remote_progress, monkeypatch):
    run, sql, state = remote_progress
    state["lose_ack"] = True
    original = AttemptRPC.request

    def lost(control, method, path, **kwargs):
        original(control, method, path, **kwargs)
        raise AttemptControlUnavailable("owned receipt loss")

    monkeypatch.setattr(AttemptRPC, "request", lost)
    with pytest.raises(StaleFence):
        pipeline._persist_candidate_phase(run, "Ridge", "evaluating")
    assert [method for method, _, _ in state["calls"]] == ["POST", "GET"]
    sql.assert_not_called()


def test_progress_sequence_is_monotonic_per_candidate_and_uses_bounded_timeout(remote_progress):
    run, _, state = remote_progress
    for name in ("Ridge", "Ridge", "RandomForestRegressor"):
        pipeline._persist_candidate_phase(run, name, "evaluating")
    payloads = [kwargs["payload"] for _, _, kwargs in state["calls"]]
    assert [body["sequence"] for body in payloads] == [1, 2, 1]
    assert payloads[0]["client_id"] == payloads[1]["client_id"] != payloads[2]["client_id"]
    assert len({body["request_id"] for body in payloads}) == 3
    assert all(kwargs["timeout_seconds"] == 60 for _, _, kwargs in state["calls"])


def test_v7_progress_flags_only_training_compute_and_preserves_v6():
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
            run, _attempt(run), name="saved-v6", credential_protocol="ray-training-result-v6"
        )
        current = reconciler.build_ray_job_manifest(
            run, _attempt(run), name="current-v7", credential_protocol="ray-training-progress-v7"
        )
    assert all("AUTOML_TRAINING_PROGRESS_PROTOCOL" not in env(pod) for pod in pods(saved))
    submitter, head, worker = pods(current)
    assert "AUTOML_TRAINING_PROGRESS_PROTOCOL" not in env(submitter)
    for pod in (head, worker):
        assert env(pod)["AUTOML_TRAINING_PROGRESS_PROTOCOL"]["value"] == "training-progress-v1"
    assert env(head)["AUTOML_TRAINING_RESULT_PROTOCOL"]["value"] == "training-result-v1"
    with patch.object(
        reconciler, "get_settings", return_value=Settings(attempt_control_base_url="")
    ):
        with pytest.raises(ValueError, match="control URL"):
            reconciler.build_ray_job_manifest(
                run,
                _attempt(run),
                name="missing-v7",
                credential_protocol="ray-training-progress-v7",
            )


def test_resumed_candidate_keeps_original_row_counts_in_initial_progress_and_result(monkeypatch):
    import copy
    from types import SimpleNamespace

    import pandas as pd
    from automl_api.models.enums import TaskType
    from automl_api.training.model_catalog import CandidateSpec
    from test_training_pipeline_runtime import _run

    run = _run(task_type=TaskType.REGRESSION)
    run.params.update(primary_metric="rmse", cv_folds=3)
    candidate = CandidateSpec(
        name="Ridge", estimator=MagicMock(), search_space={}, cost_tier="low", default_selected=True
    )
    entry = dict(
        model="Ridge",
        status="succeeded",
        metrics={"rmse": 0.2},
        best_params={},
        training_rows=7,
        validation_rows=2,
    )
    snapshots = []
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_: [candidate])
    monkeypatch.setattr(
        pipeline, "detect_target_leakage", lambda *_: SimpleNamespace(excluded_columns=[])
    )
    monkeypatch.setattr(
        pipeline, "_completed_candidates", lambda _: {"Ridge": copy.deepcopy(entry)}
    )
    model = object()
    monkeypatch.setattr(
        pipeline, "_candidate_entries", lambda *_: iter([(0, {**entry, "_model": model})])
    )
    monkeypatch.setattr(
        pipeline,
        "_persist_partial_leaderboard",
        lambda _, entries, metric: snapshots.append(copy.deepcopy(entries)),
    )
    data = pd.DataFrame({"feature": range(30), "target": range(30)})
    result = pipeline._fit_model(data, run)
    assert result.model is model
    assert len(snapshots) == 2
    for entries in [*snapshots, result.leaderboard]:
        assert entries[0]["training_rows"] == 7 and entries[0]["validation_rows"] == 2
