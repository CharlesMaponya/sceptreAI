from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    adjusted_mutual_info_score,
    adjusted_rand_score,
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    calinski_harabasz_score,
    classification_report,
    cohen_kappa_score,
    confusion_matrix,
    davies_bouldin_score,
    explained_variance_score,
    f1_score,
    fowlkes_mallows_score,
    homogeneity_score,
    log_loss,
    matthews_corrcoef,
    max_error,
    mean_absolute_error,
    mean_squared_error,
    median_absolute_error,
    normalized_mutual_info_score,
    precision_recall_curve,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
    roc_curve,
    silhouette_score,
)
from sklearn.preprocessing import label_binarize

from automl_api.models.enums import TaskType

LOWER_IS_BETTER_METRICS = {
    "davies_bouldin",
    "mae",
    "mape",
    "max_error",
    "median_absolute_error",
    "mse",
    "rmse",
    "rmsle",
    "smape",
}


def classification_evaluation(
    fitted: Any,
    test_x: pd.DataFrame,
    test_y: pd.Series,
    predictions: np.ndarray,
) -> tuple[dict[str, float], dict[str, Any]]:
    labels = list(getattr(fitted, "classes_", np.unique(test_y)))
    metrics = {
        "accuracy": float(accuracy_score(test_y, predictions)),
        "balanced_accuracy": float(balanced_accuracy_score(test_y, predictions)),
        "precision_macro": float(
            precision_score(test_y, predictions, average="macro", zero_division=0)
        ),
        "precision_weighted": float(
            precision_score(test_y, predictions, average="weighted", zero_division=0)
        ),
        "recall_macro": float(recall_score(test_y, predictions, average="macro", zero_division=0)),
        "recall_weighted": float(
            recall_score(test_y, predictions, average="weighted", zero_division=0)
        ),
        "f1_macro": float(f1_score(test_y, predictions, average="macro", zero_division=0)),
        "f1_weighted": float(f1_score(test_y, predictions, average="weighted", zero_division=0)),
        "mcc": float(matthews_corrcoef(test_y, predictions)),
        "cohen_kappa": float(cohen_kappa_score(test_y, predictions)),
    }
    matrix = confusion_matrix(test_y, predictions, labels=labels)
    diagnostics: dict[str, Any] = {
        "labels": [str(label) for label in labels],
        "confusion_matrix": matrix.tolist(),
        "classification_report": classification_report(
            test_y,
            predictions,
            labels=labels,
            output_dict=True,
            zero_division=0,
        ),
        "prediction_distribution": {
            str(label): int(np.sum(predictions == label)) for label in labels
        },
    }
    probabilities = _probabilities(fitted, test_x)
    scores = _decision_scores(fitted, test_x)
    try:
        if probabilities is not None:
            metrics["log_loss"] = float(log_loss(test_y, probabilities, labels=labels))
            if len(labels) == 2:
                positive = probabilities[:, 1]
                metrics["roc_auc"] = float(roc_auc_score(test_y, positive))
                binary_y = (np.asarray(test_y) == labels[1]).astype(int)
                metrics["average_precision"] = float(average_precision_score(binary_y, positive))
                metrics["brier_score"] = float(brier_score_loss(binary_y, positive))
                metrics["gini"] = float(2 * metrics["roc_auc"] - 1)
                diagnostics["roc_curves"] = [
                    _roc_curve_payload(binary_y, positive, str(labels[1]))
                ]
                diagnostics["precision_recall_curves"] = [
                    _precision_recall_payload(binary_y, positive, str(labels[1]))
                ]
                if matrix.shape == (2, 2):
                    true_negative, false_positive, _, _ = matrix.ravel()
                    denominator = true_negative + false_positive
                    if denominator:
                        metrics["specificity"] = float(true_negative / denominator)
            else:
                metrics["roc_auc_ovr_weighted"] = float(
                    roc_auc_score(
                        test_y,
                        probabilities,
                        labels=labels,
                        multi_class="ovr",
                        average="weighted",
                    )
                )
                encoded = label_binarize(test_y, classes=labels)
                metrics["average_precision_weighted"] = float(
                    average_precision_score(
                        encoded,
                        probabilities,
                        average="weighted",
                    )
                )
                diagnostics["roc_curves"] = [
                    _roc_curve_payload(encoded[:, index], probabilities[:, index], str(label))
                    for index, label in enumerate(labels)
                ]
                diagnostics["precision_recall_curves"] = [
                    _precision_recall_payload(
                        encoded[:, index],
                        probabilities[:, index],
                        str(label),
                    )
                    for index, label in enumerate(labels)
                ]
        elif scores is not None:
            metrics["roc_auc"] = float(roc_auc_score(test_y, scores))
            if len(labels) == 2:
                binary_y = (np.asarray(test_y) == labels[1]).astype(int)
                metrics["gini"] = float(2 * metrics["roc_auc"] - 1)
                diagnostics["roc_curves"] = [
                    _roc_curve_payload(binary_y, np.asarray(scores), str(labels[1]))
                ]
                diagnostics["precision_recall_curves"] = [
                    _precision_recall_payload(binary_y, np.asarray(scores), str(labels[1]))
                ]
    except ValueError:
        pass
    return finite_metrics(metrics), diagnostics


def regression_evaluation(
    train_y: pd.Series,
    test_y: pd.Series,
    predictions: np.ndarray,
    task_type: TaskType,
) -> tuple[dict[str, float], dict[str, Any]]:
    actual = np.asarray(test_y, dtype=float)
    predicted = np.asarray(predictions, dtype=float)
    residuals = actual - predicted
    mse = mean_squared_error(actual, predicted)
    metrics = {
        "rmse": float(math.sqrt(mse)),
        "mse": float(mse),
        "mae": float(mean_absolute_error(actual, predicted)),
        "median_absolute_error": float(median_absolute_error(actual, predicted)),
        "max_error": float(max_error(actual, predicted)),
        "r2": float(r2_score(actual, predicted)),
        "explained_variance": float(explained_variance_score(actual, predicted)),
        "smape": float(
            np.mean(
                2
                * np.abs(actual - predicted)
                / np.maximum(np.abs(actual) + np.abs(predicted), np.finfo(float).eps)
            )
        ),
    }
    nonzero = np.abs(actual) > np.finfo(float).eps
    if np.any(nonzero):
        metrics["mape"] = float(
            np.mean(np.abs((actual[nonzero] - predicted[nonzero]) / actual[nonzero]))
        )
    if np.all(actual >= 0) and np.all(predicted >= 0):
        metrics["rmsle"] = float(
            math.sqrt(mean_squared_error(np.log1p(actual), np.log1p(predicted)))
        )
    if task_type == TaskType.TIME_SERIES:
        training_values = np.asarray(train_y, dtype=float)
        naive_error = (
            float(np.mean(np.abs(np.diff(training_values)))) if len(training_values) > 1 else 0.0
        )
        if naive_error > np.finfo(float).eps:
            metrics["mase"] = float(metrics["mae"] / naive_error)
        if len(actual) > 1:
            actual_direction = np.sign(np.diff(actual))
            predicted_direction = np.sign(np.diff(predicted))
            metrics["directional_accuracy"] = float(
                np.mean(actual_direction == predicted_direction)
            )
    sample_size = min(1000, len(actual))
    sample_indices = np.unique(np.linspace(0, len(actual) - 1, sample_size, dtype=int))
    diagnostics = {
        "residual_summary": _summary(residuals),
        "actual_summary": _summary(actual),
        "prediction_summary": _summary(predicted),
        "prediction_samples": [
            {
                "order": int(index),
                "actual": float(actual[index]),
                "predicted": float(predicted[index]),
                "residual": float(residuals[index]),
            }
            for index in sample_indices
        ],
        "holdout_rows": int(len(actual)),
        "chronological_holdout": task_type == TaskType.TIME_SERIES,
    }
    return finite_metrics(metrics), diagnostics


def clustering_evaluation(
    features: np.ndarray,
    cluster_labels: np.ndarray,
    reference_labels: np.ndarray | None,
) -> dict[str, float]:
    unique_labels = np.unique(cluster_labels)
    if len(unique_labels) < 2 or len(unique_labels) >= len(cluster_labels):
        raise ValueError("Clustering metrics require between 2 and n-1 clusters.")
    metrics = {
        "silhouette": float(
            silhouette_score(
                features,
                cluster_labels,
                sample_size=min(2000, len(features)),
                random_state=42,
            )
        ),
        "davies_bouldin": float(davies_bouldin_score(features, cluster_labels)),
        "calinski_harabasz": float(calinski_harabasz_score(features, cluster_labels)),
    }
    if reference_labels is not None:
        metrics.update(
            {
                "adjusted_rand": float(adjusted_rand_score(reference_labels, cluster_labels)),
                "normalized_mutual_info": float(
                    normalized_mutual_info_score(reference_labels, cluster_labels)
                ),
                "adjusted_mutual_info": float(
                    adjusted_mutual_info_score(reference_labels, cluster_labels)
                ),
                "fowlkes_mallows": float(fowlkes_mallows_score(reference_labels, cluster_labels)),
                "homogeneity": float(homogeneity_score(reference_labels, cluster_labels)),
            }
        )
    return finite_metrics(metrics)


def aggregate_fold_metrics(
    fold_metrics: list[dict[str, float]],
) -> tuple[dict[str, float], dict[str, float]]:
    metric_names = sorted({name for fold in fold_metrics for name in fold})
    means = {}
    standard_deviations = {}
    for name in metric_names:
        values = np.asarray(
            [fold.get(name, np.nan) for fold in fold_metrics],
            dtype=float,
        )
        if np.all(np.isnan(values)):
            continue
        means[name] = float(np.nanmean(values))
        standard_deviations[name] = float(np.nanstd(values))
    return finite_metrics(means), finite_metrics(standard_deviations)


def finite_metrics(metrics: dict[str, float]) -> dict[str, float]:
    return {name: float(value) for name, value in metrics.items() if np.isfinite(float(value))}


def metric_direction(metric_name: str) -> str:
    return "minimize" if metric_name in LOWER_IS_BETTER_METRICS else "maximize"


def _roc_curve_payload(target: np.ndarray, scores: np.ndarray, label: str) -> dict[str, Any]:
    false_positive, true_positive, thresholds = roc_curve(target, scores)
    indices = _sample_curve_indices(len(false_positive))
    return {
        "label": label,
        "points": [
            {
                "false_positive_rate": float(false_positive[index]),
                "true_positive_rate": float(true_positive[index]),
                "threshold": (
                    float(thresholds[index]) if np.isfinite(thresholds[index]) else None
                ),
            }
            for index in indices
        ],
    }


def _precision_recall_payload(
    target: np.ndarray,
    scores: np.ndarray,
    label: str,
) -> dict[str, Any]:
    precision, recall, thresholds = precision_recall_curve(target, scores)
    indices = _sample_curve_indices(len(precision))
    return {
        "label": label,
        "points": [
            {
                "recall": float(recall[index]),
                "precision": float(precision[index]),
                "threshold": (
                    float(thresholds[index])
                    if index < len(thresholds) and np.isfinite(thresholds[index])
                    else None
                ),
            }
            for index in indices
        ],
    }


def _sample_curve_indices(length: int, maximum: int = 300) -> np.ndarray:
    if length <= maximum:
        return np.arange(length)
    return np.unique(np.linspace(0, length - 1, maximum, dtype=int))


def _probabilities(fitted: Any, features: pd.DataFrame) -> np.ndarray | None:
    if not hasattr(fitted, "predict_proba"):
        return None
    return np.asarray(fitted.predict_proba(features))


def _decision_scores(fitted: Any, features: pd.DataFrame) -> np.ndarray | None:
    if not hasattr(fitted, "decision_function"):
        return None
    return np.asarray(fitted.decision_function(features))


def _summary(values: np.ndarray) -> dict[str, float]:
    return {
        "minimum": float(np.min(values)),
        "q1": float(np.quantile(values, 0.25)),
        "median": float(np.median(values)),
        "mean": float(np.mean(values)),
        "q3": float(np.quantile(values, 0.75)),
        "maximum": float(np.max(values)),
        "standard_deviation": float(np.std(values)),
    }
