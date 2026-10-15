from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import AttemptStatus, TaskType
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingTrial, WorkflowAttempt, WorkflowEvent
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline, trial_runtime
from automl_api.training.model_catalog import CandidateSpec
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_pipeline_runtime import _mock_candidate_side_effects
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401


@pytest.fixture
def runtime(trial_scope, monkeypatch):  # noqa: F811
    engine, run_id, _, _, _ = trial_scope
    monkeypatch.setattr(trial_runtime, "get_session_factory", lambda: lambda: Session(engine))
    monkeypatch.setenv("AUTOML_MODEL_PODS", "1")
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", "candidate")
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
    return engine, run, trial_scope


def evaluate(runtime, callback, parameters=None):
    return trial_runtime.evaluate_trial(
        runtime[1],
        "Ridge",
        "fixture-cv",
        parameters or {"alpha": 1.0},
        callback,
    )


def test_actual_cv_runs_after_committed_lease_and_replays_without_refitting(runtime):
    engine, _, scope = runtime
    x = pd.DataFrame({"x": np.arange(24, dtype=float)})
    y = x["x"] * 2 + 1
    calls = []

    def compute():
        with Session(engine) as db:
            child = db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == scope[2])
            )
            assert child.status == AttemptStatus.RUNNING and child.lease_owner
            assert child.lease_expires_at and child.retry_count == 1
        calls.append(True)
        return {"scores": cross_val_score(Ridge(), x, y, cv=3).tolist()}

    expected = evaluate(runtime, compute)
    assert len(expected["scores"]) == 3
    for _ in range(2):
        assert evaluate(runtime, compute) == expected
    assert calls == [True]
    with Session(engine) as db:
        trial = db.scalar(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        assert trial.status == "succeeded" and trial.result_digest


def test_fixed_cpu_candidate_pipeline_uses_durable_cv(runtime, monkeypatch):
    engine, run, scope = runtime
    model_pipeline = pipeline._supervised_model_pipeline
    _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(
        pipeline, "configure_estimator_for_training", lambda *_a, **_k: (Ridge(), "cpu")
    )
    monkeypatch.setattr(pipeline, "_supervised_model_pipeline", model_pipeline)
    cv = MagicMock(wraps=cross_val_score)
    monkeypatch.setattr(pipeline, "cross_val_score", cv)
    frame = pd.DataFrame({"x": np.arange(30, dtype=float)})
    for _ in range(2):
        entry = pipeline._fit_candidate(
            CandidateSpec("Ridge", Ridge(), {}, "low", False),
            frame.iloc[:24],
            frame["x"].iloc[:24] * 2 + 1,
            frame.iloc[24:],
            frame["x"].iloc[24:] * 2 + 1,
            TaskType.REGRESSION,
            1,
            3,
            "neg_root_mean_squared_error",
            run,
        )
        assert entry["status"] == "succeeded", entry.get("error")
    assert cv.call_count == 1
    with Session(engine) as db:
        trial = db.scalar(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        assert trial.suggestion_id == "fixed-cv" and trial.status == "succeeded"


def test_model_failure_is_recorded_and_replay_does_not_recompute(runtime):
    compute = MagicMock(side_effect=ValueError("invalid model data"))
    with pytest.raises(ValueError, match="invalid model data"):
        evaluate(runtime, compute)
    for _ in range(2):
        with pytest.raises(trial_runtime.TrialEvaluationFailed, match="invalid model data"):
            evaluate(runtime, compute)
    assert compute.call_count == 1


@pytest.mark.parametrize("stage", [1, 2, 3])
def test_commit_ack_loss_never_blindly_recomputes(runtime, monkeypatch, stage):
    engine, _, _ = runtime
    commits = []

    class LostCommit(Session):
        def commit(self):
            super().commit()
            commits.append(True)
            if len(commits) == stage:
                raise ConnectionError("lost committed response")

    monkeypatch.setattr(trial_runtime, "get_session_factory", lambda: lambda: LostCommit(engine))
    compute = MagicMock(return_value={"score": 1.0})
    with pytest.raises(trial_runtime.TrialExecutionUnresolved):
        evaluate(runtime, compute)
    assert compute.call_count == (1 if stage == 3 else 0)
    if stage == 2:
        with pytest.raises(StaleFence, match="live lease"):
            evaluate(runtime, compute)
        compute.assert_not_called()
    else:
        assert evaluate(runtime, compute) == {"score": 1.0}
        assert compute.call_count == 1


def test_replaced_parent_blocks_publication(runtime):
    engine, _, scope = runtime

    def compute():
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
        return {"score": 1.0}

    with pytest.raises(StaleFence):
        evaluate(runtime, compute)
    with Session(engine) as db:
        child = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == scope[2]))
        assert child.terminal_cas_version == 0
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowEvent)
                .where(WorkflowEvent.attempt_id == child.id)
            )
            == 0
        )


def test_changed_trial_parameters_abort_without_model_fallback(runtime):
    compute = MagicMock(return_value={"score": 1.0})
    evaluate(runtime, compute)
    with pytest.raises(trial_runtime.TrialExecutionUnresolved):
        evaluate(runtime, compute, {"alpha": 2.0})
    assert compute.call_count == 1


def test_heartbeat_uses_committed_trial_identity(runtime, monkeypatch):
    heartbeat_seen = Event()
    original = trial_runtime.heartbeat_trial

    def heartbeat(*args, **kwargs):
        original(*args, **kwargs)
        heartbeat_seen.set()

    monkeypatch.setattr(trial_runtime, "heartbeat_trial", heartbeat)
    monkeypatch.setattr(trial_runtime, "_HEARTBEAT_INTERVAL", 0.02)

    def compute():
        assert heartbeat_seen.wait(timeout=5)
        return {"score": 1.0}

    assert evaluate(runtime, compute) == {"score": 1.0}


@pytest.mark.parametrize("missing", ["both", "token", "attempt"])
def test_production_context_never_bypasses_durable_trial(runtime, monkeypatch, missing):
    if missing in {"both", "attempt"}:
        monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    if missing in {"both", "token"}:
        monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    compute = MagicMock()
    with pytest.raises(StaleFence):
        evaluate(runtime, compute)
    compute.assert_not_called()


def test_explicit_local_legacy_execution_remains_available(monkeypatch):
    for name in (
        "AUTOML_ATTEMPT_ID",
        "AUTOML_FENCING_TOKEN",
        "AUTOML_MODEL_PODS",
        "TRAINING_EXECUTION_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(trial_runtime, "get_settings", lambda: SimpleNamespace(environment="local"))
    compute = MagicMock(return_value={"scores": [1.0]})
    assert trial_runtime.evaluate_trial(None, "Ridge", "legacy", {}, compute) == {"scores": [1.0]}
    compute.assert_called_once()
    monkeypatch.setattr(
        trial_runtime, "get_settings", lambda: SimpleNamespace(environment="production")
    )
    with pytest.raises(StaleFence):
        trial_runtime.evaluate_trial(None, "Ridge", "legacy", {}, compute)
    assert compute.call_count == 1


@pytest.mark.parametrize("result", [{"score": float("nan")}, []])
def test_invalid_numerical_result_is_a_durable_model_failure(runtime, result):
    compute = MagicMock(return_value=result)
    with pytest.raises(ValueError):
        evaluate(runtime, compute)
    with pytest.raises(trial_runtime.TrialEvaluationFailed):
        evaluate(runtime, compute)
    assert compute.call_count == 1
