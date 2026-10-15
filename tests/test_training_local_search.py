from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import AttemptStatus, TaskType
from automl_api.models.workflows import TrainingTrial, WorkflowAttempt
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline, trial_runtime, tune_runtime
from automl_api.training.model_catalog import CandidateSpec
from sklearn.linear_model import Ridge
from skopt.space import Real
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_pipeline_runtime import _mock_candidate_side_effects
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401


def search(runtime_scope, *, iterations=3, high=10):
    frame = pd.DataFrame({"x": np.arange(30, dtype=float)})
    return tune_runtime.search_candidate_local(
        Ridge(),
        {"alpha": Real(0.01, high, prior="log-uniform")},
        frame,
        frame["x"] * 2 + 1,
        3,
        "neg_root_mean_squared_error",
        run=runtime_scope[1],
        candidate_name="Ridge",
        iterations=iterations,
        base_parameters=Ridge().get_params(deep=False),
    )


def test_real_search_enrolls_every_suggestion_and_replays_without_cv(runtime, monkeypatch):  # noqa: F811
    engine, _, scope = runtime
    original = tune_runtime._cv_result
    seen = []

    def compute(config, **kwargs):
        with Session(engine) as db:
            trial = db.scalar(
                select(TrainingTrial).where(
                    TrainingTrial.candidate_id == scope[4],
                    TrainingTrial.suggestion_id == f"search:{config['sceptre_suggestion_id']}",
                )
            )
            attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.trial_id == trial.id))
            assert attempt.status == AttemptStatus.RUNNING and attempt.lease_owner
            assert trial.parameters["suggestion"] == config
        seen.append(config)
        return original(config, **kwargs)

    monkeypatch.setattr(tune_runtime, "_cv_result", compute)
    expected = search(runtime)
    assert expected["trials"] == 3 and expected["failed_trials"] == 0
    assert len(expected["suggestion_log"]["suggestions"]) == 3
    for _ in range(2):
        assert search(runtime) == expected
    assert len(seen) == 3
    with Session(engine) as db:
        trials = list(
            db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        )
        assert len(trials) == 3 and all(trial.status == "succeeded" for trial in trials)


def test_production_candidate_search_uses_durable_local_engine(runtime, monkeypatch):  # noqa: F811
    engine, run, scope = runtime
    model_pipeline = pipeline._supervised_model_pipeline
    _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(pipeline, "_supervised_model_pipeline", model_pipeline)
    monkeypatch.setattr(
        pipeline, "configure_estimator_for_training", lambda *_a, **_k: (Ridge(), "cpu")
    )
    monkeypatch.setattr(
        pipeline, "BayesSearchCV", MagicMock(side_effect=AssertionError("untracked"))
    )
    monkeypatch.setattr(
        tune_runtime.tune, "Tuner", MagicMock(side_effect=AssertionError("nested Ray scheduling"))
    )
    cv = MagicMock(wraps=tune_runtime._cv_result)
    monkeypatch.setattr(tune_runtime, "_cv_result", cv)
    frame = pd.DataFrame({"x": np.arange(30, dtype=float)})
    candidate = CandidateSpec(
        "Ridge", Ridge(), {"model__alpha": Real(0.01, 10, prior="log-uniform")}, "low", True
    )
    results = []
    for _ in range(2):
        entry = pipeline._fit_candidate(
            candidate,
            frame.iloc[:24],
            frame["x"].iloc[:24] * 2 + 1,
            frame.iloc[24:],
            frame["x"].iloc[24:] * 2 + 1,
            TaskType.REGRESSION,
            2,
            3,
            "neg_root_mean_squared_error",
            run,
        )
        assert entry["status"] == "succeeded", entry.get("error")
        assert entry["diagnostics"]["runtime"]["search_engine"] == "local_durable"
        results.append(entry["best_params"])
    assert results[0] == results[1] and cv.call_count == 2
    with Session(engine) as db:
        assert (
            len(
                list(
                    db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
                )
            )
            == 2
        )


@pytest.mark.parametrize("changed", ["iterations", "dimensions"])
def test_changed_search_contract_cannot_reuse_old_suggestions(runtime, monkeypatch, changed):  # noqa: F811
    original = search(runtime, iterations=2)
    assert original["trials"] == 2
    compute = MagicMock(side_effect=AssertionError("conflicting search must not compute"))
    monkeypatch.setattr(tune_runtime, "_cv_result", compute)
    with pytest.raises(trial_runtime.TrialExecutionUnresolved):
        search(
            runtime,
            iterations=3 if changed == "iterations" else 2,
            high=20 if changed == "dimensions" else 10,
        )
    compute.assert_not_called()


def test_failed_search_replay_does_not_repeat_estimator_work(runtime, monkeypatch):  # noqa: F811
    compute = MagicMock(side_effect=ValueError("bad estimator configuration"))
    monkeypatch.setattr(tune_runtime, "_cv_result", compute)
    with pytest.raises(ValueError, match="bad estimator"):
        search(runtime)
    for _ in range(2):
        with pytest.raises(trial_runtime.TrialEvaluationFailed):
            search(runtime)
    assert compute.call_count == 1


def test_parent_loss_between_suggestions_blocks_further_cv(runtime, monkeypatch):  # noqa: F811
    engine, _, scope = runtime
    original = trial_runtime.evaluate_trial
    calls = []

    def evaluate(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(True)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
        return result

    monkeypatch.setattr(trial_runtime, "evaluate_trial", evaluate)
    with pytest.raises(StaleFence):
        search(runtime)
    assert calls == [True]


def test_numpy_categories_have_canonical_json_identity():
    import json

    from skopt.space import Categorical

    dimensions = tune_runtime._dimensions({"value": Categorical([np.int64(1), np.int64(2)])})
    assert json.loads(json.dumps(dimensions["value"].to_dict()))["categories"] == [1, 2]
