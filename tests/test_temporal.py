from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from automl_api.services.temporal import (
    infer_unix_timestamp_unit,
    normalize_temporal_features,
    series_unix_timestamp_unit,
    unix_timestamp_iso,
)


@pytest.mark.parametrize(
    ("values", "unit"),
    [
        ([1_704_067_200, 1_704_153_600], "s"),
        ([1_704_067_200_000, 1_704_153_600_000], "ms"),
        ([1_704_067_200_000_000, 1_704_153_600_000_000], "us"),
        (
            [
                1_704_067_200_000_000_000,
                1_704_153_600_000_000_000,
            ],
            "ns",
        ),
    ],
)
def test_infer_unix_timestamp_unit(values: list[int], unit: str) -> None:
    assert infer_unix_timestamp_unit(values) == unit


def test_small_identifiers_are_not_timestamps() -> None:
    assert infer_unix_timestamp_unit([1001, 1002, 1003]) is None


def test_timestamp_inference_skips_invalid_nonfinite_and_partial_values() -> None:
    assert infer_unix_timestamp_unit([None, "bad", np.inf]) is None
    assert infer_unix_timestamp_unit(["bad", 1_704_067_200], minimum_ratio=1) == "s"
    assert unix_timestamp_iso(1_704_067_200, "s").startswith("2024-01-01")


def test_temporal_normalization_preserves_plain_and_rejects_sparse_dates() -> None:
    frame = pd.DataFrame(
        {
            "event_time": ["2026-01-01", "bad"],
            "created_date": ["2026-01-01", "2026-01-02"],
            "identifier": [1, 2],
        }
    )
    result = normalize_temporal_features(frame)
    assert result["event_time"].tolist() == frame["event_time"].tolist()
    assert pd.api.types.is_float_dtype(result["created_date"])
    assert result["identifier"].tolist() == [1, 2]
    assert series_unix_timestamp_unit(pd.Series(dtype=float)) is None
    assert series_unix_timestamp_unit(pd.Series(["bad", "also bad", 1_704_067_200])) is None
