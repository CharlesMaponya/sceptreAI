from __future__ import annotations

import pytest
from automl_api.services.temporal import infer_unix_timestamp_unit


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
