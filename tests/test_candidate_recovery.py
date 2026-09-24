from __future__ import annotations

import hashlib
import io
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import joblib
import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training import pipeline
from automl_api.training.model_catalog import CandidateSpec


def checkpoint(monkeypatch):
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        params={"cv_folds": 2},
        tags={},
        task_type=TaskType.REGRESSION,
        target_column="target",
    )
    store = MagicMock()
    store.uri_for_key.side_effect = lambda key: f"s3c://bucket/{key}"
    payload = io.BytesIO()
    joblib.dump({"saved": True}, payload)
    store.read_bytes.return_value = payload.getvalue()
    entry = {
        "model": "Ridge",
        "status": "succeeded",
        "best_params": {},
        "metrics": {"rmse": 1.0},
        "mlflow_run_id": "original-model-run",
        "model_artifact_uri": store.uri_for_key(
            f"projects/{run.project_id}/runs/{run.id}/models/Ridge.joblib"
        ),
        "model_artifact_sha256": hashlib.sha256(payload.getvalue()).hexdigest(),
    }
    run.tags = {"leaderboard": [entry]}
    monkeypatch.setattr(pipeline, "get_object_store", lambda: store)
    return run, store, entry


def test_completed_candidate_is_reused_and_other_candidates_continue(monkeypatch):
    run, _, entry = checkpoint(monkeypatch)
    specs = [
        CandidateSpec(name, MagicMock(), {}, "low", True) for name in ("Ridge", "DummyRegressor")
    ]
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_: specs)
    monkeypatch.setattr(
        pipeline, "detect_target_leakage", lambda *_: SimpleNamespace(excluded_columns=[])
    )
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    persisted = MagicMock()
    monkeypatch.setattr(pipeline, "_persist_partial_leaderboard", persisted)
    fit = MagicMock(
        return_value={
            "model": "DummyRegressor",
            "status": "succeeded",
            "metrics": {"rmse": 2.0},
            "best_params": {},
            "_model": "second",
        }
    )
    monkeypatch.setattr(pipeline, "_fit_candidate", fit)
    frame = pd.DataFrame({"x": range(40), "target": [n % 7 for n in range(40)]})
    result = pipeline._fit_model(frame, run)
    assert fit.call_count == 1
    assert fit.call_args.args[0].name == "DummyRegressor"
    assert result.model == {"saved": True}
    assert result.leaderboard[0]["mlflow_run_id"] == entry["mlflow_run_id"]
    assert result.leaderboard[0]["resumed"] is True
    assert persisted.call_count == 3


@pytest.mark.parametrize("mutation", ["digest", "project", "traversal"])
def test_candidate_recovery_rejects_tampering_before_deserialization(monkeypatch, mutation):
    run, store, entry = checkpoint(monkeypatch)
    if mutation == "digest":
        entry["model_artifact_sha256"] = "0" * 64
    elif mutation == "project":
        entry["model_artifact_uri"] = "s3c://bucket/projects/another/model.joblib"
    else:
        entry["model_artifact_uri"] = store.uri_for_key(
            f"projects/{run.project_id}/runs/{run.id}/%2e%2e/model.joblib"
        )
    load = MagicMock()
    monkeypatch.setattr(pipeline.joblib, "load", load)
    with pytest.raises(ValueError):
        pipeline._restore_candidate(run, entry)
    load.assert_not_called()


def test_incomplete_models_are_not_mistaken_for_durable_checkpoints(monkeypatch):
    run, _, entry = checkpoint(monkeypatch)
    run.tags["leaderboard"] += [
        {**entry, "model": "failed", "status": "failed"},
        {**entry, "model": "incomplete", "model_artifact_sha256": None},
    ]
    assert set(pipeline._completed_candidates(run)) == {"Ridge"}


def test_pending_phase_is_initialized_without_overwriting_another_workers_progress(monkeypatch):
    from automl_api.models.enums import RunStatus

    monkeypatch.setenv("AUTOML_MODEL_PODS", "1")
    candidate = CandidateSpec("Ridge", MagicMock(), {}, "low", True)
    pending = pipeline._pending_candidate(candidate)
    initial = {**pending}
    initial.pop("phase")
    other = {**initial, "model": "Forest", "status": "running", "phase": "learning_curve"}
    run = SimpleNamespace(
        id=uuid.uuid4(), status=RunStatus.RUNNING, tags={"leaderboard": [initial, other]}
    )
    db = MagicMock()
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(pipeline, "_locked_run", lambda *a, **k: run)
    pipeline._persist_partial_leaderboard(run.id, [pending, {**pending, "model": "Forest"}], "rmse")
    by_model = {entry["model"]: entry for entry in run.tags["leaderboard"]}
    assert by_model["Ridge"]["phase"] == "waiting_for_worker"
    assert by_model["Forest"]["phase"] == "learning_curve"
    assert by_model["Forest"]["status"] == "running"
    assert run.tags["current_candidate"] == "Forest"
    assert run.tags["candidate_phase"] == "learning_curve"
    db.__enter__.return_value.commit.assert_called_once()
