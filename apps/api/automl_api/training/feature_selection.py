"""Bound feature-statistic fitting to a reproducible sample of each training fold."""

from __future__ import annotations

import numpy as np
from sklearn.feature_selection import SelectPercentile, mutual_info_classif, mutual_info_regression
from sklearn.model_selection import train_test_split

FEATURE_STATISTICS_MAX_ROWS = 50_000


def sample_training_rows(features, target, *, max_rows, classification=False):
    if max_rows < 2:
        raise ValueError("Feature statistics require a sample budget of at least two rows.")
    if len(features) <= max_rows:
        return features, target
    indices, _ = train_test_split(
        np.arange(len(features)),
        train_size=max_rows,
        random_state=42,
        stratify=target if classification else None,
    )

    def take(values):
        if values is None:
            return None
        return values.iloc[indices] if hasattr(values, "iloc") else values[indices]

    return take(features), take(target)


def classification_scores(features, target):
    return mutual_info_classif(features, target, random_state=42)


def regression_scores(features, target):
    return mutual_info_regression(features, target, random_state=42)


class BoundedFeatureSelector(SelectPercentile):
    """Fit scores on training rows only; transform every row supplied by the pipeline."""

    def __init__(
        self, *, task_type="regression", percentile=80, max_rows=FEATURE_STATISTICS_MAX_ROWS
    ):
        self.task_type = task_type
        self.max_rows = max_rows
        super().__init__(
            score_func=classification_scores
            if task_type == "classification"
            else regression_scores,
            percentile=percentile,
        )

    def fit(self, features, target):
        sampled, sampled_target = sample_training_rows(
            features,
            target,
            max_rows=self.max_rows,
            classification=self.task_type == "classification",
        )
        self.input_rows_ = len(features)
        self.statistics_rows_ = len(sampled)
        return super().fit(sampled, sampled_target)
