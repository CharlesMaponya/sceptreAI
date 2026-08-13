from __future__ import annotations

import uuid
from contextlib import AbstractContextManager
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training import pipeline
from automl_api.training.model_catalog import CandidateSpec
from sklearn.base import BaseEstimator
from sklearn.model_selection import KFold, TimeSeriesSplit


class _MlflowRun(AbstractContextManager):
    def __init__(self, run_id: str = "candidate") -> None:
        self.info = SimpleNamespace(run_id=run_id)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Clusterer(BaseEstimator):
    def __init__(self, n_clusters: int = 2) -> None:
        self.n_clusters = n_clusters

    def fit(self, features):
        return self

    def predict(self, features):
        return np.arange(len(features)) % self.n_clusters

    def fit_predict(self, features):
        return self.predict(features)


class _Filter:
    evidence_ = {"removed": []}

    def __init__(self, **_kwargs):
        pass

    def fit_transform(self, values):
        return values

    def transform(self, values):
        return values


class _Prepare:
    def fit_transform(self, values):
        return np.asarray(values)

    def transform(self, values):
        return np.asarray(values)


def _run(**overrides):
    values = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "task_type": TaskType.CLUSTERING,
        "params": {},
        "tags": {},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _mock_cluster_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pipeline, "_runtime_training_resources", lambda: (2, None, False))
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda candidate, **_kwargs: (candidate.estimator, "cpu"),
    )
    monkeypatch.setattr(pipeline, "CorrelatedFeatureFilter", _Filter)
    monkeypatch.setattr(pipeline, "_preprocessor", lambda: _Prepare())
    monkeypatch.setattr(
        pipeline,
        "clustering_evaluation",
        lambda features, labels, reference: {
            "silhouette": float(len(np.unique(labels))) / 10,
            "external": float(reference is not None),
        },
    )
    monkeypatch.setattr(
        pipeline,
        "aggregate_fold_metrics",
        lambda folds: (
            {key: float(np.mean([item[key] for item in folds])) for key in folds[0]},
            {key: 0.0 for key in folds[0]},
        ),
    )
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", lambda *_args: None)
    monkeypatch.setattr(pipeline.mlflow, "active_run", lambda: _MlflowRun("parent"))
    monkeypatch.setattr(pipeline.mlflow, "start_run", lambda **_kwargs: _MlflowRun())
    for name in ("set_tags", "log_params", "log_dict"):
        monkeypatch.setattr(pipeline.mlflow, name, lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_log_metrics_synchronously", lambda *_args: None)
    monkeypatch.setattr(pipeline, "_log_metric_synchronously", lambda *_args: None)
    monkeypatch.setattr(pipeline, "_log_sklearn_model", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "_mirror_candidate_evidence_to_parent", lambda *_args: None)
    monkeypatch.setattr(pipeline, "_persist_candidate_model", lambda *_args: "s3://model")


def test_clustering_candidate_searches_cluster_count_and_persists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_cluster_runtime(monkeypatch)
    candidate = CandidateSpec("Cluster", _Clusterer(), {}, "low", True)
    features = pd.DataFrame({"x": [0, 1, 2, 3, 4, 5], "y": [0, 0, 1, 1, 2, 2]})
    entry = pipeline._fit_clustering_candidate(
        candidate,
        features,
        np.asarray(["a", "a", "b", "b", "c", "c"]),
        KFold(n_splits=2),
        _run(),
    )
    assert entry["status"] == "succeeded"
    assert entry["best_params"]["model__n_clusters"] >= 2
    assert entry["diagnostics"]["external_evaluation"] is True
    assert entry["model_artifact_uri"] == "s3://model"


def test_clustering_candidate_failure_is_a_rankable_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_cluster_runtime(monkeypatch)
    monkeypatch.setattr(pipeline, "clustering_evaluation", lambda *_args: {})
    candidate = CandidateSpec("Broken", _Clusterer(), {}, "low", True)
    entry = pipeline._fit_clustering_candidate(
        candidate,
        pd.DataFrame({"x": range(6)}),
        None,
        KFold(n_splits=2),
        _run(),
    )
    assert entry["status"] == "failed"
    assert "valid clusters" in entry["error"]


def test_clustering_tournament_validation_success_and_total_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = CandidateSpec("Cluster", _Clusterer(), {}, "low", True)
    run = _run(params={"candidate_limit": 30, "cv_folds": 10, "evaluation_column": "truth"})
    data = pd.DataFrame({"x": range(20), "truth": ["a", "b"] * 10})
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [candidate])
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", lambda *_args: None)
    monkeypatch.setattr(pipeline, "_persist_partial_leaderboard", lambda *_args: None)
    monkeypatch.setattr(
        pipeline,
        "_fit_clustering_candidate",
        lambda *_args: {
            **pipeline._pending_candidate(candidate),
            "status": "succeeded",
            "metrics": {"silhouette": 0.5},
            "best_params": {"model__n_clusters": 2},
            "_model": "fitted",
        },
    )
    result = pipeline._fit_clustering(data, run)
    assert result.model == "fitted" and result.params["evaluation_column"] == "truth"

    with pytest.raises(ValueError, match="evaluation column"):
        pipeline._fit_clustering(pd.DataFrame({"x": range(20)}), run)
    run.params = {}
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [])
    with pytest.raises(ValueError, match="No supported clustering"):
        pipeline._fit_clustering(pd.DataFrame({"x": range(20)}), run)
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [candidate])
    monkeypatch.setattr(
        pipeline,
        "_fit_clustering_candidate",
        lambda *_args: {
            **pipeline._pending_candidate(candidate),
            "status": "failed",
            "error": "boom",
        },
    )
    with pytest.raises(RuntimeError, match="Every clustering candidate failed"):
        pipeline._fit_clustering(pd.DataFrame({"x": range(20)}), run)


@pytest.mark.parametrize(
    ("task", "rows", "folds", "expected"),
    [
        (TaskType.CLASSIFICATION, [0, 0, 1, 1, 1], 5, 2),
        (TaskType.REGRESSION, list(range(100)), 5, 2),
    ],
)
def test_cross_validation_strategy_bounds(task, rows, folds, expected) -> None:
    assert pipeline._cross_validation_strategy(pd.Series(rows), task, folds) == expected


def test_cross_validation_strategy_rejects_too_little_data() -> None:
    with pytest.raises(ValueError, match="target class"):
        pipeline._cross_validation_strategy(pd.Series([0, 1, 1]), TaskType.CLASSIFICATION, 3)
    with pytest.raises(ValueError, match="six training"):
        pipeline._cross_validation_strategy(pd.Series(range(5)), TaskType.TIME_SERIES, 3)
    with pytest.raises(ValueError, match="four training"):
        pipeline._cross_validation_strategy(pd.Series(range(3)), TaskType.REGRESSION, 3)
    strategy = pipeline._cross_validation_strategy(pd.Series(range(40)), TaskType.TIME_SERIES, 5)
    assert isinstance(strategy, TimeSeriesSplit) and strategy.n_splits == 2


def test_time_series_split_and_order_column_edges() -> None:
    features = pd.DataFrame({"value": [3, 1, 2], "event_time": [3, 1, 2]})
    target = pd.Series([30, 10, 20])
    train_x, test_x, train_y, test_y = pipeline._supervised_split(
        features, target, TaskType.TIME_SERIES
    )
    assert train_x["event_time"].tolist() == [1, 2]
    assert test_x["event_time"].tolist() == [3]
    assert train_y.tolist() == [10, 20] and test_y.tolist() == [30]
    assert pipeline._time_order_column(pd.DataFrame({"plain": [1, 2]})) is None
    assert (
        pipeline._time_order_column(pd.DataFrame({"when": pd.to_datetime(["2026-01-01"])}))
        == "when"
    )


def test_runtime_resources_parse_invalid_vendor_and_truthy_rapids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AUTOML_CPU_THREADS", "invalid")
    monkeypatch.setenv("AUTOML_GPU_VENDOR", "amd")
    monkeypatch.setenv("AUTOML_RAPIDS_ACTIVE", "YES")
    assert pipeline._runtime_training_resources() == (1, None, True)
    monkeypatch.setenv("AUTOML_CPU_THREADS", "0")
    monkeypatch.setenv("AUTOML_GPU_VENDOR", " NVIDIA ")
    monkeypatch.setenv("AUTOML_RAPIDS_ACTIVE", "no")
    assert pipeline._runtime_training_resources() == (1, "nvidia", False)


def test_learning_curve_handles_errors_sign_and_empty_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        pipeline,
        "learning_curve",
        lambda *_args, **_kwargs: (
            np.asarray([2, 4]),
            np.asarray([[-1.0, -2.0], [np.nan, np.nan]]),
            np.asarray([[-3.0, -4.0], [1.0, 2.0]]),
        ),
    )
    result = pipeline._learning_curve_diagnostics(
        object(), pd.DataFrame({"x": range(4)}), pd.Series(range(4)), cv=2, scoring="neg_rmse"
    )
    assert result["scoring"] == "rmse" and len(result["points"]) == 1
    monkeypatch.setattr(
        pipeline,
        "learning_curve",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad")),
    )
    assert (
        pipeline._learning_curve_diagnostics(
            object(), pd.DataFrame(), pd.Series(dtype=float), cv=2, scoring="r2"
        )
        is None
    )


def test_shift_and_json_helpers_preserve_numpy_scalars() -> None:
    assert pipeline._shift_nonnegative([[-1, 2]]).tolist() == [[0, 3]]
    assert pipeline._json_safe({"number": np.int64(4), "object": uuid.UUID(int=0)}) == {
        "number": 4,
        "object": str(uuid.UUID(int=0)),
    }
