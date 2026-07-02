from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

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
            MIN_UNIX_SECONDS <= value / scale <= MAX_UNIX_SECONDS
            for value in numeric_values
        )
        if plausible >= required:
            return unit
    return None


def unix_timestamp_iso(value: Any, unit: str) -> str:
    seconds = float(value) / UNIX_UNIT_SCALES[unit]
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat()
