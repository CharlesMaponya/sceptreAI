from __future__ import annotations

import numpy as np
import pandas as pd
from automl_api.models.enums import TaskType
from automl_api.training.evaluation import (
    classification_evaluation,
    clustering_evaluation,
    regression_evaluation,
)
from sklearn.linear_model import LogisticRegression


def test_classification_evaluation_contains_review_metrics_and_diagnostics() -> None:
    features = pd.DataFrame({"x": [-2, -1, 1, 2, 3, -3]})
    target = pd.Series([0, 0, 1, 1, 1, 0])
    model = LogisticRegression().fit(features, target)
    predictions = model.predict(features)

    metrics, diagnostics = classification_evaluation(
        model,
        features,
        target,
        predictions,
    )

    assert {
        "accuracy",
        "balanced_accuracy",
        "precision_weighted",
        "recall_weighted",
        "f1_weighted",
        "mcc",
        "cohen_kappa",
        "roc_auc",
        "log_loss",
        "brier_score",
    }.issubset(metrics)
    assert len(diagnostics["confusion_matrix"]) == 2
    assert "classification_report" in diagnostics


def test_regression_and_time_series_evaluation_contains_error_diagnostics() -> None:
    train_y = pd.Series([1.0, 2.0, 4.0, 7.0])
    test_y = pd.Series([8.0, 10.0, 13.0])
    predictions = np.asarray([7.5, 10.5, 12.0])

    metrics, diagnostics = regression_evaluation(
        train_y,
        test_y,
        predictions,
        TaskType.TIME_SERIES,
    )

    assert {
        "rmse",
        "mse",
        "mae",
        "r2",
        "smape",
        "mape",
        "mase",
        "directional_accuracy",
    }.issubset(metrics)
    assert diagnostics["chronological_holdout"]
    assert "residual_summary" in diagnostics


def test_clustering_evaluation_supports_internal_and_external_metrics() -> None:
    features = np.asarray([[0.0, 0.0], [0.1, 0.2], [5.0, 5.0], [5.2, 5.1]])
    labels = np.asarray([0, 0, 1, 1])
    reference = np.asarray(["a", "a", "b", "b"])

    metrics = clustering_evaluation(features, labels, reference)

    assert {
        "silhouette",
        "davies_bouldin",
        "calinski_harabasz",
        "adjusted_rand",
        "normalized_mutual_info",
        "adjusted_mutual_info",
        "fowlkes_mallows",
        "homogeneity",
    } == set(metrics)
