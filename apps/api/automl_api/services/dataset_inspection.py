from __future__ import annotations

import csv
import io
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from automl_api.models.enums import DatasetFormat, DatasetStatus
from automl_api.services.temporal import infer_unix_timestamp_unit

SUPPORTED_EXTENSIONS = {
    ".csv": DatasetFormat.CSV,
    ".parquet": DatasetFormat.PARQUET,
    ".xlsx": DatasetFormat.EXCEL,
    ".xls": DatasetFormat.EXCEL,
    ".json": DatasetFormat.JSON,
    ".jsonl": DatasetFormat.JSON,
}
PREVIEW_SAMPLE_SIZE = 512


@dataclass(frozen=True)
class InspectionResult:
    format: DatasetFormat
    status: DatasetStatus
    row_count: int | None
    column_count: int | None
    schema_json: dict[str, Any]
    inferred_types_json: dict[str, Any]
    quality_report_json: dict[str, Any]


@dataclass
class ColumnAccumulator:
    name: str
    missing_count: int = 0
    present_count: int = 0
    numeric_count: int = 0
    temporal_count: int = 0
    unix_temporal_count: int = 0
    text_count: int = 0
    decimal_seen: bool = False
    distinct_values: set[str] | None = None
    sample_values: list[str] | None = None
    preview_values: list[str] | None = None

    def __post_init__(self) -> None:
        self.distinct_values = set()
        self.sample_values = []
        self.preview_values = []

    def add(self, value: Any) -> None:
        if _is_missing(value):
            self.missing_count += 1
            return

        assert self.distinct_values is not None
        assert self.sample_values is not None
        assert self.preview_values is not None
        value_text = str(value).strip()
        self.present_count += 1
        self.distinct_values.add(value_text)
        if len(self.sample_values) < 5:
            self.sample_values.append(value_text)
        if len(self.preview_values) < PREVIEW_SAMPLE_SIZE:
            self.preview_values.append(value_text)
        if _looks_numeric(value_text):
            self.numeric_count += 1
        if _looks_decimal(value_text):
            self.decimal_seen = True
        if _looks_temporal(value_text):
            self.temporal_count += 1
        if infer_unix_timestamp_unit([value_text]):
            self.unix_temporal_count += 1
        if len(value_text.split()) >= 6 or len(value_text) > 80:
            self.text_count += 1

    def profile(self) -> dict[str, Any]:
        assert self.distinct_values is not None
        assert self.sample_values is not None
        assert self.preview_values is not None
        semantic_type = _infer_type_from_counts(self)
        return {
            "name": self.name,
            "semantic_type": semantic_type,
            "missing_count": self.missing_count,
            "distinct_count": len(self.distinct_values),
            "sample_values": self.sample_values,
            **_preview_metadata(semantic_type, self.preview_values),
        }


def detect_dataset_format(filename: str) -> DatasetFormat:
    extension = Path(filename.lower()).suffix
    try:
        return SUPPORTED_EXTENSIONS[extension]
    except KeyError as exc:
        raise ValueError(
            "Unsupported dataset format. Use CSV, Parquet, Excel, JSON, or JSONL.",
        ) from exc


def inspect_tabular_bytes(filename: str, content: bytes) -> InspectionResult:
    dataset_format = detect_dataset_format(filename)
    if dataset_format == DatasetFormat.CSV:
        return _inspect_csv(content)
    if dataset_format == DatasetFormat.JSON:
        return _inspect_json(content)

    return InspectionResult(
        format=dataset_format,
        status=DatasetStatus.UPLOADED,
        row_count=None,
        column_count=None,
        schema_json={"columns": [], "parser": "deferred"},
        inferred_types_json={},
        quality_report_json={
            "completeness_score": None,
            "warnings": [
                "Stored successfully. Rich metadata extraction for this "
                "format requires the optional data stack.",
            ],
        },
    )


def _inspect_csv(content: bytes) -> InspectionResult:
    sample = content[:4096].decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    except csv.Error:
        dialect = csv.excel
    text_stream = io.TextIOWrapper(io.BytesIO(content), encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text_stream, dialect=dialect)
    columns = list(reader.fieldnames or [])
    return _inspect_row_iter(DatasetFormat.CSV, reader, columns)


def _inspect_json(content: bytes) -> InspectionResult:
    text = content.decode("utf-8-sig").strip()
    if not text:
        rows: list[dict[str, Any]] = []
    else:
        try:
            loaded = json.loads(text)
            if isinstance(loaded, list):
                rows = [row for row in loaded if isinstance(row, dict)]
            elif isinstance(loaded, dict):
                data = loaded.get("data")
                rows = (
                    [row for row in data if isinstance(row, dict)]
                    if isinstance(data, list)
                    else [loaded]
                )
            else:
                rows = []
        except json.JSONDecodeError:
            rows = [
                row
                for line in text.splitlines()
                if line.strip()
                for row in [json.loads(line)]
                if isinstance(row, dict)
            ]

    columns = sorted({key for row in rows for key in row.keys()})
    return _inspect_rows(DatasetFormat.JSON, rows, columns)


def _inspect_rows(
    dataset_format: DatasetFormat,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> InspectionResult:
    row_count = len(rows)
    column_count = len(columns)
    column_profiles = [_profile_column(column, rows) for column in columns]
    missing_cells = sum(profile["missing_count"] for profile in column_profiles)
    total_cells = row_count * column_count
    completeness_score = 1.0 if total_cells == 0 else round(1 - (missing_cells / total_cells), 4)
    duplicate_count = _duplicate_count(rows)

    return InspectionResult(
        format=dataset_format,
        status=DatasetStatus.READY,
        row_count=row_count,
        column_count=column_count,
        schema_json={"columns": column_profiles},
        inferred_types_json={
            profile["name"]: {
                "semantic_type": profile["semantic_type"],
                "nullable": profile["missing_count"] > 0,
            }
            for profile in column_profiles
        },
        quality_report_json={
            "completeness_score": completeness_score,
            "missing_cells": missing_cells,
            "duplicate_rows": duplicate_count,
            "warnings": [],
        },
    )


def _inspect_row_iter(
    dataset_format: DatasetFormat,
    rows: Any,
    columns: list[str],
) -> InspectionResult:
    row_count = 0
    accumulators = {column: ColumnAccumulator(column) for column in columns}
    fingerprints: Counter[str] = Counter()

    for raw_row in rows:
        row = {column: raw_row.get(column) for column in columns}
        row_count += 1
        for column in columns:
            accumulators[column].add(row.get(column))
        fingerprints[json.dumps(row, sort_keys=True, default=str)] += 1

    column_profiles = [accumulators[column].profile() for column in columns]
    missing_cells = sum(profile["missing_count"] for profile in column_profiles)
    column_count = len(columns)
    total_cells = row_count * column_count
    completeness_score = 1.0 if total_cells == 0 else round(1 - (missing_cells / total_cells), 4)
    duplicate_count = sum(count - 1 for count in fingerprints.values() if count > 1)

    return InspectionResult(
        format=dataset_format,
        status=DatasetStatus.READY,
        row_count=row_count,
        column_count=column_count,
        schema_json={"columns": column_profiles},
        inferred_types_json={
            profile["name"]: {
                "semantic_type": profile["semantic_type"],
                "nullable": profile["missing_count"] > 0,
            }
            for profile in column_profiles
        },
        quality_report_json={
            "completeness_score": completeness_score,
            "missing_cells": missing_cells,
            "duplicate_rows": duplicate_count,
            "warnings": [],
        },
    )


def _profile_column(column: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [row.get(column) for row in rows]
    present_values = [value for value in values if not _is_missing(value)]
    inferred_type = _infer_type(present_values)
    distinct_count = len({str(value) for value in present_values})

    return {
        "name": column,
        "semantic_type": inferred_type,
        "missing_count": len(values) - len(present_values),
        "distinct_count": distinct_count,
        "sample_values": [str(value) for value in present_values[:5]],
        **_preview_metadata(inferred_type, present_values[:PREVIEW_SAMPLE_SIZE]),
    }


def _preview_metadata(semantic_type: str, values: list[Any]) -> dict[str, Any]:
    if semantic_type == "numerical_continuous":
        numeric_values = [float(value) for value in values if _looks_numeric(str(value).strip())]
        ordered = sorted(numeric_values)
        midpoint = len(ordered) // 2
        median = (
            (ordered[midpoint - 1] + ordered[midpoint]) / 2
            if len(ordered) > 0 and len(ordered) % 2 == 0
            else ordered[midpoint] if ordered else None
        )
        return {
            "preview_kind": "histogram",
            "preview_values": numeric_values,
            "preview_distribution": [],
            "statistics": {
                "min": min(numeric_values) if numeric_values else None,
                "median": median,
                "mean": sum(numeric_values) / len(numeric_values) if numeric_values else None,
                "max": max(numeric_values) if numeric_values else None,
            },
        }
    if semantic_type == "temporal":
        temporal_values = [str(value) for value in values]
        return {
            "preview_kind": "histogram",
            "preview_values": temporal_values,
            "preview_distribution": [],
            "statistics": {
                "min": min(temporal_values) if temporal_values else None,
                "max": max(temporal_values) if temporal_values else None,
            },
        }

    counts = Counter(str(value) for value in values)
    return {
        "preview_kind": "bar",
        "preview_values": [],
        "preview_distribution": [
            {"label": label, "count": count}
            for label, count in counts.most_common(15)
        ],
        "statistics": {},
    }


def _infer_type(values: list[Any]) -> str:
    if not values:
        return "unknown"

    numeric_count = 0
    temporal_count = 0
    text_count = 0
    for value in values:
        value_text = str(value).strip()
        if _looks_numeric(value_text):
            numeric_count += 1
        if _looks_temporal(value_text):
            temporal_count += 1
        if len(value_text.split()) >= 6 or len(value_text) > 80:
            text_count += 1

    threshold = max(1, int(len(values) * 0.8))
    if numeric_count >= threshold and infer_unix_timestamp_unit(values):
        return "temporal"
    if temporal_count >= threshold:
        return "temporal"
    if numeric_count >= threshold:
        if any(_looks_decimal(str(value).strip()) for value in values):
            return "numerical_continuous"
        unique_numeric_values = len({str(value) for value in values})
        return "numerical_discrete" if unique_numeric_values <= 20 else "numerical_continuous"
    if text_count >= threshold:
        return "text"
    return "categorical"


def _infer_type_from_counts(accumulator: ColumnAccumulator) -> str:
    if accumulator.present_count == 0:
        return "unknown"

    threshold = max(1, int(accumulator.present_count * 0.8))
    if accumulator.unix_temporal_count >= threshold:
        return "temporal"
    if accumulator.temporal_count >= threshold:
        return "temporal"
    if accumulator.numeric_count >= threshold:
        if accumulator.decimal_seen:
            return "numerical_continuous"
        assert accumulator.distinct_values is not None
        return (
            "numerical_discrete"
            if len(accumulator.distinct_values) <= 20
            else "numerical_continuous"
        )
    if accumulator.text_count >= threshold:
        return "text"
    return "categorical"


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _looks_decimal(value: str) -> bool:
    try:
        number = float(value)
    except ValueError:
        return False
    return not number.is_integer()


def _looks_temporal(value: str) -> bool:
    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return True


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in {"na", "n/a", "null", "none"}
    return False


def _duplicate_count(rows: list[dict[str, Any]]) -> int:
    fingerprints = Counter(json.dumps(row, sort_keys=True, default=str) for row in rows)
    return sum(count - 1 for count in fingerprints.values() if count > 1)
