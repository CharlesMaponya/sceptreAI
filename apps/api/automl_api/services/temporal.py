from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

UNIX_UNIT_SCALES = {
    "s": 1.0,
    "ms": 1_000.0,
    "us": 1_000_000.0,
    "ns": 1_000_000_000.0,
}
MIN_UNIX_SECONDS = datetime(1980, 1, 1, tzinfo=UTC).timestamp()
MAX_UNIX_SECONDS = datetime(2100, 1, 1, tzinfo=UTC).timestamp()


def infer_unix_timestamp_unit(
    values: Iterable[Any],
    *,
    minimum_ratio: float = 0.8,
) -> str | None:
    numeric_values = []
    for value in values:
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(numeric):
            numeric_values.append(numeric)
    if not numeric_values:
        return None

    required = max(1, math.ceil(len(numeric_values) * minimum_ratio))
    for unit, scale in UNIX_UNIT_SCALES.items():
        plausible = sum(
            MIN_UNIX_SECONDS <= value / scale <= MAX_UNIX_SECONDS for value in numeric_values
        )
        if plausible >= required:
            return unit
    return None


def unix_timestamp_iso(value: Any, unit: str) -> str:
    seconds = float(value) / UNIX_UNIT_SCALES[unit]
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()


def normalize_temporal_features(features: pd.DataFrame) -> pd.DataFrame:
    normalized = features.copy()
    for column in normalized.columns:
        series = normalized[column]
        timestamp_unit = series_unix_timestamp_unit(series)
        if timestamp_unit:
            numeric = pd.to_numeric(series, errors="coerce").astype("float64")
            normalized[column] = numeric / UNIX_UNIT_SCALES[timestamp_unit] / 86_400
            continue
        is_temporal_name = any(
            token in str(column).lower() for token in ("date", "time", "timestamp")
        )
        if not isinstance(series.dtype, pd.DatetimeTZDtype) and not (
            pd.api.types.is_datetime64_any_dtype(series) or is_temporal_name
        ):
            continue
        parsed = pd.to_datetime(
            series,
            errors="coerce",
            utc=True,
            format="mixed",
        )
        if parsed.notna().mean() < 0.8:
            continue
        numeric = parsed.astype("int64", copy=False).astype("float64")
        numeric[parsed.isna()] = np.nan
        normalized[column] = numeric / 86_400_000_000_000
    return normalized


def series_unix_timestamp_unit(series: pd.Series) -> str | None:
    present = series.dropna()
    if present.empty:
        return None
    numeric = pd.to_numeric(present, errors="coerce")
    if numeric.notna().mean() < 0.8:
        return None
    return infer_unix_timestamp_unit(numeric.dropna().head(1000).tolist())
