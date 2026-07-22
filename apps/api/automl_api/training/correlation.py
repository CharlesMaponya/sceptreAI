from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_selection import mutual_info_classif, mutual_info_regression
from sklearn.utils.validation import check_is_fitted


class CorrelatedFeatureFilter(TransformerMixin, BaseEstimator):
    """Remove redundant numeric features using task-appropriate training evidence."""

    def __init__(
        self,
        task_type: str,
        threshold: float = 0.9,
        evidence_feature_limit: int = 50,
    ) -> None:
        self.task_type = task_type
        self.threshold = threshold
        self.evidence_feature_limit = evidence_feature_limit

    def fit(self, features: pd.DataFrame, target: pd.Series | None = None) -> Any:
        if not isinstance(features, pd.DataFrame):
            raise TypeError("CorrelatedFeatureFilter requires a pandas DataFrame.")
        if not 0 < self.threshold <= 1:
            raise ValueError("Correlation threshold must be greater than 0 and at most 1.")

        numeric_columns = list(features.select_dtypes(include="number").columns)
        correlation = features[numeric_columns].corr(method="pearson")
        scores, score_method = self._scores(features[numeric_columns], target)
        position = {column: index for index, column in enumerate(numeric_columns)}
        ranked = sorted(numeric_columns, key=lambda column: (-scores[column], position[column]))

        retained_numeric: list[str] = []
        removed: list[dict[str, Any]] = []
        for column in ranked:
            conflicts = [
                kept
                for kept in retained_numeric
                if abs(float(correlation.at[column, kept])) >= self.threshold
            ]
            if not conflicts:
                retained_numeric.append(column)
                continue
            kept = max(conflicts, key=lambda item: abs(float(correlation.at[column, item])))
            removed.append(
                {
                    "feature": str(column),
                    "kept_feature": str(kept),
                    "correlation": round(float(correlation.at[column, kept]), 6),
                    "score": round(scores[column], 6),
                    "kept_score": round(scores[kept], 6),
                }
            )

        removed_columns = {item["feature"] for item in removed}
        self.feature_names_in_ = np.asarray(features.columns, dtype=object)
        self.retained_features_ = [
            column for column in features.columns if str(column) not in removed_columns
        ]
        self.removed_features_ = removed
        self.scores_ = scores
        self.score_method_ = score_method
        self.evidence_ = self._evidence(correlation, numeric_columns)
        return self

    def transform(self, features: pd.DataFrame) -> pd.DataFrame:
        check_is_fitted(self, "retained_features_")
        return features.loc[:, self.retained_features_].copy()

    def get_feature_names_out(self, input_features: Any = None) -> np.ndarray:
        check_is_fitted(self, "retained_features_")
        return np.asarray(self.retained_features_, dtype=object)

    def _scores(
        self,
        numeric: pd.DataFrame,
        target: pd.Series | None,
    ) -> tuple[dict[str, float], str]:
        if numeric.empty:
            return {}, "not_applicable"
        if self.task_type == "clustering":
            return (
                {column: float(numeric[column].notna().mean()) for column in numeric.columns},
                "non_missing_rate",
            )
        if target is None:
            raise ValueError(f"A target is required for {self.task_type} correlation filtering.")

        usable = numeric.replace([np.inf, -np.inf], np.nan)
        imputed = usable.fillna(usable.median()).fillna(0.0)
        if self.task_type == "classification" and target.nunique(dropna=True) == 2:
            return (
                {column: _information_value(usable[column], target) for column in numeric.columns},
                "information_value",
            )
        if self.task_type == "classification":
            values = mutual_info_classif(
                imputed,
                pd.factorize(target, sort=True)[0],
                random_state=42,
            )
            method = "mutual_information_classification"
        else:
            values = mutual_info_regression(
                imputed,
                pd.to_numeric(target, errors="raise"),
                random_state=42,
            )
            method = "mutual_information_regression"
        return (
            {column: float(value) for column, value in zip(numeric.columns, values, strict=True)},
            method,
        )

    def _evidence(
        self,
        correlation: pd.DataFrame,
        numeric_columns: list[Any],
    ) -> dict[str, Any]:
        columns_by_name = {str(column): column for column in numeric_columns}
        priority = [
            columns_by_name[feature]
            for item in self.removed_features_
            for feature in (item["kept_feature"], item["feature"])
        ]
        evidence_columns = list(dict.fromkeys([*priority, *numeric_columns]))[
            : self.evidence_feature_limit
        ]
        retained = [column for column in evidence_columns if column in self.retained_features_]
        return {
            "threshold": self.threshold,
            "score_method": self.score_method_,
            "numeric_feature_count": len(numeric_columns),
            "heatmap_truncated": len(numeric_columns) > len(evidence_columns),
            "retained_features": [str(column) for column in self.retained_features_],
            "removed_features": self.removed_features_,
            "before": _matrix(correlation, evidence_columns),
            "after": _matrix(correlation, retained),
        }


def _information_value(feature: pd.Series, target: pd.Series) -> float:
    target_codes = pd.Series(pd.factorize(target, sort=True)[0], index=target.index)
    non_missing = feature.dropna()
    if non_missing.nunique() < 2:
        return 0.0
    try:
        buckets = pd.qcut(feature, q=min(10, non_missing.nunique()), duplicates="drop")
    except ValueError:
        return 0.0
    bucket_labels = buckets.astype("string").fillna("__missing__")
    table = pd.crosstab(bucket_labels, target_codes).reindex(columns=[0, 1], fill_value=0)
    bin_count = max(1, len(table))
    non_event = (table[0] + 0.5) / (table[0].sum() + 0.5 * bin_count)
    event = (table[1] + 0.5) / (table[1].sum() + 0.5 * bin_count)
    return float(((non_event - event) * np.log(non_event / event)).sum())


def _matrix(correlation: pd.DataFrame, columns: list[Any]) -> dict[str, Any]:
    values = correlation.loc[columns, columns].fillna(0.0).round(4).to_numpy().tolist()
    return {"columns": [str(column) for column in columns], "values": values}
