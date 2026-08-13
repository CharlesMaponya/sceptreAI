from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training import evaluation
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC


@pytest.mark.parametrize(
    ("task", "requested", "expected"),
    [
        (TaskType.CLASSIFICATION, None, "balanced_accuracy"),
        (TaskType.REGRESSION, "r2", "r2"),
        (TaskType.TIME_SERIES, "mae", "mae"),
        (TaskType.CLUSTERING, "davies_bouldin", "davies_bouldin"),
    ],
)
def test_primary_metric_resolution(task: TaskType, requested: str | None, expected: str) -> None:
    assert evaluation.resolve_primary_metric(task, requested) == expected


def test_primary_metric_resolution_rejects_cross_task_metric() -> None:
    with pytest.raises(ValueError, match="not supported.*regression"):
        evaluation.resolve_primary_metric(TaskType.REGRESSION, "accuracy")


@pytest.mark.parametrize(
    ("task", "metric", "classes", "expected"),
    [
        (TaskType.CLASSIFICATION, "roc_auc", 2, "roc_auc"),
        (TaskType.CLASSIFICATION, "roc_auc", 3, "roc_auc_ovr_weighted"),
        (TaskType.CLASSIFICATION, "log_loss", 2, "neg_log_loss"),
        (TaskType.CLASSIFICATION, "f1_macro", 2, "f1_macro"),
        (TaskType.REGRESSION, "rmse", None, "neg_root_mean_squared_error"),
        (TaskType.REGRESSION, "mae", None, "neg_mean_absolute_error"),
        (TaskType.REGRESSION, "mse", None, "neg_mean_squared_error"),
        (TaskType.REGRESSION, "r2", None, "r2"),
    ],
)
def test_cross_validation_scoring_contract(task, metric, classes, expected) -> None:
    assert evaluation.cross_validation_scoring(task, metric, target_classes=classes) == expected


def test_multiclass_evaluation_emits_one_curve_per_class() -> None:
    features, target = load_iris(return_X_y=True, as_frame=True)
    model = LogisticRegression(max_iter=500).fit(features, target)
    metrics, diagnostics = evaluation.classification_evaluation(
        model, features, target, model.predict(features)
    )
    assert metrics["roc_auc"] == metrics["roc_auc_ovr_weighted"]
    assert "average_precision_weighted" in metrics
    assert len(diagnostics["roc_curves"]) == 3
    assert "positive_label" not in diagnostics


def test_binary_decision_function_supports_first_class_as_positive() -> None:
    features = pd.DataFrame({"x": [-3, -2, -1, 1, 2, 3]})
    target = pd.Series(["rare", "common", "common", "common", "common", "common"])
    model = LinearSVC().fit(features, target)
    metrics, diagnostics = evaluation.classification_evaluation(
        model, features, target, model.predict(features), positive_label="rare"
    )
    assert "roc_auc" in metrics
    assert diagnostics["positive_label"] == "rare"
    assert "log_loss" not in metrics


def test_classification_probability_metric_error_does_not_discard_core_metrics() -> None:
    class BrokenProbabilityModel:
        classes_ = np.asarray([0, 1])

        def predict_proba(self, _features: pd.DataFrame) -> np.ndarray:
            return np.asarray([[1.0, 0.0]])

    target = pd.Series([0, 1])
    metrics, diagnostics = evaluation.classification_evaluation(
        BrokenProbabilityModel(),
        pd.DataFrame({"x": [0, 1]}),
        target,
        np.asarray([0, 1]),
    )
    assert metrics["accuracy"] == 1
    assert "log_loss" not in metrics
    assert diagnostics["confusion_matrix"] == [[1, 0], [0, 1]]


def test_positive_label_and_inference_failures_are_explicit() -> None:
    with pytest.raises(ValueError, match="only be inferred for a binary"):
        evaluation.default_binary_positive_label(pd.Series([1, 2, 3]))
    with pytest.raises(ValueError, match="not present"):
        evaluation._binary_positive_index(["no", "yes"], pd.Series(["no", "yes"]), "maybe")
    assert evaluation._binary_positive_index(["a", "b", "c"], pd.Series(["a"]), None) is None


def test_regression_boundaries_omit_undefined_metrics() -> None:
    metrics, diagnostics = evaluation.regression_evaluation(
        pd.Series([1.0]),
        pd.Series([0.0, 0.0]),
        np.asarray([-1.0, -2.0]),
        TaskType.REGRESSION,
    )
    assert "mape" not in metrics
    assert "rmsle" not in metrics
    assert "mase" not in metrics
    assert "directional_accuracy" not in metrics
    assert diagnostics["holdout_rows"] == 2


def test_clustering_without_reference_and_invalid_partition() -> None:
    features = np.asarray([[0, 0], [0, 1], [8, 8], [8, 9]], dtype=float)
    metrics = evaluation.clustering_evaluation(features, np.asarray([0, 0, 1, 1]), None)
    assert set(metrics) == {"silhouette", "davies_bouldin", "calinski_harabasz"}
    with pytest.raises(ValueError, match="between 2 and n-1"):
        evaluation.clustering_evaluation(features, np.asarray([0, 0, 0, 0]), None)
    with pytest.raises(ValueError, match="between 2 and n-1"):
        evaluation.clustering_evaluation(features, np.asarray([0, 1, 2, 3]), None)


def test_fold_aggregation_skips_all_nan_and_filters_nonfinite() -> None:
    means, stddev = evaluation.aggregate_fold_metrics(
        [{"score": 1, "empty": np.nan}, {"score": 3, "empty": np.nan}]
    )
    assert means == {"score": 2.0}
    assert stddev == {"score": 1.0}
    assert evaluation.finite_metrics({"yes": 1, "nan": np.nan, "inf": np.inf}) == {"yes": 1.0}
    assert evaluation.metric_direction("rmse") == "minimize"
    assert evaluation.metric_direction("accuracy") == "maximize"


def test_curve_sampling_probability_and_summary_helpers() -> None:
    short = evaluation._sample_curve_indices(3)
    long = evaluation._sample_curve_indices(1_000, maximum=20)
    assert short.tolist() == [0, 1, 2]
    assert len(long) == 20 and long[0] == 0 and long[-1] == 999
    assert evaluation._probabilities(object(), pd.DataFrame()) is None
    assert evaluation._decision_scores(object(), pd.DataFrame()) is None
    model = SimpleNamespace(
        predict_proba=lambda _features: [[0.2, 0.8]],
        decision_function=lambda _features: [0.7],
    )
    assert evaluation._probabilities(model, pd.DataFrame()).shape == (1, 2)
    assert evaluation._decision_scores(model, pd.DataFrame()).tolist() == [0.7]
    summary = evaluation._summary(np.asarray([1.0, 2.0, 3.0]))
    assert summary["minimum"] == 1 and summary["maximum"] == 3 and summary["mean"] == 2


def test_curve_payloads_bound_and_sample_points() -> None:
    target = np.asarray([0, 0, 1, 1])
    scores = np.asarray([0.1, 0.2, 0.8, 0.9])
    roc = evaluation._roc_curve_payload(target, scores, "yes")
    precision = evaluation._precision_recall_payload(target, scores, "yes")
    assert roc["label"] == "yes" and roc["points"][0]["threshold"] is None
    assert precision["label"] == "yes" and precision["points"][-1]["threshold"] is None
