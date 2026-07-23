from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training.evaluation import (
    classification_evaluation,
    clustering_evaluation,
    default_binary_positive_label,
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
        positive_label="1",
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
    assert diagnostics["roc_curves"][0]["points"]
    assert diagnostics["precision_recall_curves"][0]["points"]


def test_binary_evaluation_defaults_to_the_minority_event_class() -> None:
    features = pd.DataFrame({"x": np.arange(20)})
    target = pd.Series(["Attrited Customer"] * 4 + ["Existing Customer"] * 16)
    model = LogisticRegression().fit(features, target)

    _, diagnostics = classification_evaluation(
        model,
        features,
        target,
        model.predict(features),
    )

    assert diagnostics["positive_label"] == "Attrited Customer"
    assert diagnostics["positive_label_source"] == "minority_class"
    assert diagnostics["roc_curves"][0]["label"] == "Attrited Customer"


def test_binary_evaluation_honors_an_explicit_positive_class() -> None:
    features = pd.DataFrame({"x": np.arange(20)})
    target = pd.Series(["Attrited Customer"] * 4 + ["Existing Customer"] * 16)
    model = LogisticRegression().fit(features, target)

    _, diagnostics = classification_evaluation(
        model,
        features,
        target,
        model.predict(features),
        positive_label="Existing Customer",
    )

    assert diagnostics["positive_label"] == "Existing Customer"
    assert diagnostics["positive_label_source"] == "configured"
    assert diagnostics["roc_curves"][0]["label"] == "Existing Customer"


def test_balanced_legacy_evaluation_marks_the_class_order_fallback() -> None:
    features = pd.DataFrame({"x": np.arange(20)})
    target = pd.Series(["Attrited Customer"] * 10 + ["Existing Customer"] * 10)
    model = LogisticRegression().fit(features, target)

    _, diagnostics = classification_evaluation(
        model,
        features,
        target,
        model.predict(features),
    )

    assert diagnostics["positive_label"] == "Existing Customer"
    assert diagnostics["positive_label_source"] == "legacy_class_order"


def test_balanced_new_training_requires_an_explicit_positive_class() -> None:
    target = pd.Series(["Attrited Customer"] * 10 + ["Existing Customer"] * 10)

    with pytest.raises(ValueError, match="balanced.*positive class"):
        default_binary_positive_label(target)


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
