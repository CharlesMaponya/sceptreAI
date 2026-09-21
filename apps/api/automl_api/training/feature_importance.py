"""Format feature contributions without importing the training worker runtime."""

from typing import Any

import numpy as np


def normalize_feature_importance(
    feature_importance: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not feature_importance:
        return []
    values = np.asarray(
        [item.get("mean_absolute_shap", 0.0) for item in feature_importance],
        dtype=float,
    )
    percentages = _percentage_contributions(values, feature_axis=0)
    normalized = [
        {
            **item,
            "contribution_percent": float(percent),
        }
        for item, percent in zip(feature_importance, percentages, strict=True)
    ]
    return sorted(
        normalized,
        key=lambda item: item["contribution_percent"],
        reverse=True,
    )


def _percentage_contributions(
    values: np.ndarray,
    *,
    feature_axis: int,
) -> np.ndarray:
    absolute = np.nan_to_num(
        np.abs(np.asarray(values, dtype=float)),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    totals = np.sum(absolute, axis=feature_axis, keepdims=True)
    normalized = np.divide(
        absolute,
        totals,
        out=np.zeros_like(absolute, dtype=float),
        where=totals > 0,
    )
    return np.clip(normalized * 100.0, 0.0, 100.0)
