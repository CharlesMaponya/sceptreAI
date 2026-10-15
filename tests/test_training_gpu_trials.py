from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import AttemptStatus, TaskType
from automl_api.models.workflows import TrainingTrial, WorkflowAttempt
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline, tune_runtime
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


@pytest.mark.parametrize("mode", ["candidate", "ray"])
@pytest.mark.parametrize("search", [False, True])
@pytest.mark.parametrize("failure", [None, "model", "stale"])
def test_accelerated_cv_and_fallback_have_committed_owners(
    runtime, monkeypatch, search, failure, mode  # noqa: F811
):
    engine, run, scope = runtime
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", mode)
    real_pipeline = pipeline._supervised_model_pipeline
    _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(pipeline, "_supervised_model_pipeline", real_pipeline)
    monkeypatch.setattr(pipeline, "_runtime_training_resources", lambda: (1, "nvidia", False))
    active = []

    def configure(*_a, gpu_vendor, **_k):
        accelerator = "nvidia" if gpu_vendor else "cpu"
        active.append(accelerator)
        return Ridge(), accelerator

    monkeypatch.setattr(pipeline, "configure_estimator_for_training", configure)
    monkeypatch.setattr(
        pipeline, "BayesSearchCV", MagicMock(side_effect=AssertionError("untracked"))
    )
    target = tune_runtime if search else pipeline
    original = target.cross_val_score
    seen = []

    def compute(*args, **kwargs):
        with Session(engine) as db:
            owned = db.scalar(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.run_attempt_id == scope[2],
                    WorkflowAttempt.status == AttemptStatus.RUNNING,
                )
            )
            assert owned is not None and owned.lease_owner
            trial = db.get(TrainingTrial, owned.trial_id)
            expected = "nvidia:" if active[-1] == "nvidia" else "cpu-fallback:"
            assert trial.suggestion_id.startswith(expected)
            if active[-1] == "cpu":
                contract = trial.parameters["search"] if search else trial.parameters
                assert contract["fallback_from"] == "nvidia"
        seen.append(active[-1])
        if active[-1] == "nvidia" and failure:
            if failure == "stale":
                raise StaleFence("lost GPU ownership")
            raise ValueError("GPU kernel fixture failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(target, "cross_val_score", compute)
    frame = pd.DataFrame({"x": np.arange(30, dtype=float)})
    candidate = CandidateSpec(
        "Ridge", Ridge(), {"model__alpha": Real(0.01, 10)} if search else {}, "low", True
    )

    def fit():
        return pipeline._fit_candidate(
            candidate,
            frame.iloc[:24],
            frame.x.iloc[:24] * 2 + 1,
            frame.iloc[24:],
            frame.x.iloc[24:] * 2 + 1,
            TaskType.REGRESSION,
            2,
            3,
            "neg_root_mean_squared_error",
            run,
        )

    if failure == "stale":
        with pytest.raises(StaleFence, match="lost GPU ownership"):
            fit()
        assert active == seen == ["nvidia"]
        return
    first = fit()
    assert first["status"] == "succeeded", first.get("error")
    expected_calls = (
        ["nvidia"] + ["cpu"] * (2 if search else 1)
        if failure
        else ["nvidia"] * (2 if search else 1)
    )
    assert seen == expected_calls
    second = fit()
    assert second["status"] == "succeeded", second.get("error")
    assert first["best_params"] == second["best_params"]
    assert seen == expected_calls
    with Session(engine) as db:
        trials = list(
            db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        )
        assert len(trials) == len(expected_calls)
        assert all(t.result_digest for t in trials)
        assert sum(t.status == "failed" for t in trials) == bool(failure)
