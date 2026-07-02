from __future__ import annotations

import csv
import io
import json
import math
import statistics
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import psutil
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import DatasetFormat, ProjectRole, TaskType
from automl_api.models.iam import User
from automl_api.schemas.profiling import (
    ColumnProfileRead,
    DatasetProfileRead,
    FeatureRelationshipRead,
    PreparationStepRead,
    ProfileRequest,
    TaskInferenceRead,
)
from automl_api.services.projects import require_project_role
from automl_api.services.temporal import (
    infer_unix_timestamp_unit,
    unix_timestamp_iso,
)
from automl_api.storage.object_store import get_object_store


def build_dataset_profile(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    payload: ProfileRequest,
) -> DatasetProfileRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    version = _get_project_dataset_version(db, project_id, dataset_id, dataset_version_id)
    target_column = payload.target_column.strip() if payload.target_column else None

    rows: list[dict[str, Any]] = []
    if _should_use_dask(version):
        from automl_api.services.dask_profiling import profile_dataset_with_dask

        row_count, column_profiles, relationships, warnings = profile_dataset_with_dask(
            version,
            target_column,
        )
        columns = [profile.name for profile in column_profiles]
    else:
        rows, warnings = _load_rows(version)
        columns = _columns_from_rows_or_version(rows, version)
        row_count = len(rows)
        column_profiles = [_profile_column(column, rows, row_count, version) for column in columns]
        relationships = []
    profile_by_name = {profile.name: profile for profile in column_profiles}

    if target_column and target_column not in profile_by_name:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Target column '{target_column}' was not found in the dataset.",
        )

    task_inference = _infer_task(target_column, profile_by_name)
    if rows:
        relationships = _relationships_against_target(rows, profile_by_name, target_column)
    preparation_plan = _build_preparation_plan(
        column_profiles,
        target_column,
        task_inference.task_type,
    )

    return DatasetProfileRead(
        project_id=project_id,
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
        row_count_analyzed=row_count,
        column_count=len(columns),
        target_column=target_column,
        task_inference=task_inference,
        columns=column_profiles,
        relationships=relationships,
        preparation_plan=preparation_plan,
        warnings=warnings,
    )


def _should_use_dask(
    version: DatasetVersion,
    available_memory_bytes: int | None = None,
) -> bool:
    if version.format == DatasetFormat.CSV:
        supported = True
    elif version.format == DatasetFormat.JSON:
        filename = (version.original_filename or "").lower()
        supported = filename.endswith((".jsonl", ".ndjson"))
    else:
        supported = False
    if not supported or not version.byte_size:
        return False

    available_memory_bytes = available_memory_bytes or _available_memory_bytes()
    projected_memory_bytes = version.byte_size * 12
    return (
        version.byte_size >= 256 * 1024 * 1024
        or projected_memory_bytes > available_memory_bytes * 0.35
    )


def _available_memory_bytes() -> int:
    available = int(psutil.virtual_memory().available)
    cgroup_limit_path = Path("/sys/fs/cgroup/memory.max")
    cgroup_usage_path = Path("/sys/fs/cgroup/memory.current")
    try:
        raw_limit = cgroup_limit_path.read_text(encoding="ascii").strip()
        if raw_limit != "max":
            cgroup_available = int(raw_limit) - int(
                cgroup_usage_path.read_text(encoding="ascii").strip()
            )
            available = min(available, max(1, cgroup_available))
    except (OSError, ValueError):
        pass
    return available


def _get_project_dataset_version(
    db: Session,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
) -> DatasetVersion:
    version = db.scalar(
        select(DatasetVersion).where(
            DatasetVersion.project_id == project_id,
            DatasetVersion.dataset_id == dataset_id,
            DatasetVersion.id == dataset_version_id,
        )
    )
    if version is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset version not found.",
        )
    return version


def _load_rows(version: DatasetVersion) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    if version.format not in {DatasetFormat.CSV, DatasetFormat.JSON}:
        return [], [
            "Rich profiling currently supports CSV and JSON/JSONL. "
            "This dataset version will use stored metadata only.",
        ]

    try:
        content = get_object_store().read_bytes(version.object_uri)
    except (OSError, ValueError) as exc:
        return [], [f"Could not read stored dataset object for rich profiling: {exc}"]

    if version.format == DatasetFormat.CSV:
        return _load_csv_rows(content), warnings
    return _load_json_rows(content), warnings


def _load_csv_rows(content: bytes) -> list[dict[str, Any]]:
    sample = content[:4096].decode("utf-8-sig", errors="replace")
    try:
        dialect = csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    except csv.Error:
        dialect = csv.excel
    text_stream = io.TextIOWrapper(io.BytesIO(content), encoding="utf-8-sig", newline="")
    reader = csv.DictReader(text_stream, dialect=dialect)
    return [dict(row) for row in reader]


def _load_json_rows(content: bytes) -> list[dict[str, Any]]:
    text = content.decode("utf-8-sig").strip()
    if not text:
        return []
    try:
        loaded = json.loads(text)
        if isinstance(loaded, list):
            return [row for row in loaded if isinstance(row, dict)]
        if isinstance(loaded, dict):
            data = loaded.get("data")
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
            return [loaded]
    except json.JSONDecodeError:
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        return rows
    return []


def _columns_from_rows_or_version(rows: list[dict[str, Any]], version: DatasetVersion) -> list[str]:
    if rows:
        return sorted({column for row in rows for column in row.keys()})
    return [
        column.get("name")
        for column in version.schema_json.get("columns", [])
        if column.get("name")
    ]


def _profile_column(
    column: str,
    rows: list[dict[str, Any]],
    row_count: int,
    version: DatasetVersion,
) -> ColumnProfileRead:
    if not rows:
        stored_column = _stored_column_profile(column, version)
        missing_count = int(stored_column.get("missing_count", 0))
        return ColumnProfileRead(
            name=column,
            semantic_type=version.inferred_types_json.get(column, {}).get(
                "semantic_type",
                stored_column.get("semantic_type", "unknown"),
            ),
            missing_count=missing_count,
            missing_ratio=0.0,
            distinct_count=int(stored_column.get("distinct_count", 0)),
            sample_values=[str(value) for value in stored_column.get("sample_values", [])],
            statistics={},
            distribution_type="bar",
            distribution=[],
            quality_flags=[],
        )

    values = [row.get(column) for row in rows]
    present_values = [value for value in values if not _is_missing(value)]
    missing_count = len(values) - len(present_values)
    semantic_type = _infer_semantic_type(present_values)
    statistics_json = _statistics_for_values(semantic_type, present_values)
    distribution_type, distribution = _distribution_for_values(semantic_type, present_values)
    quality_flags = _quality_flags(column, semantic_type, missing_count, row_count, present_values)

    return ColumnProfileRead(
        name=column,
        semantic_type=semantic_type,
        missing_count=missing_count,
        missing_ratio=0.0 if row_count == 0 else round(missing_count / row_count, 4),
        distinct_count=len({str(value) for value in present_values}),
        sample_values=[str(value) for value in present_values[:5]],
        statistics=statistics_json,
        distribution_type=distribution_type,
        distribution=distribution,
        quality_flags=quality_flags,
    )


def _stored_column_profile(column: str, version: DatasetVersion) -> dict[str, Any]:
    for stored_column in version.schema_json.get("columns", []):
        if stored_column.get("name") == column:
            return stored_column
    return {}


def _statistics_for_values(semantic_type: str, values: list[Any]) -> dict[str, Any]:
    if not values:
        return {}
    if semantic_type == "temporal":
        timestamp_unit = infer_unix_timestamp_unit(values)
        if timestamp_unit:
            numeric_values = sorted(_finite_numeric_values(values))
            return {
                "count": len(numeric_values),
                "timestamp_unit": timestamp_unit,
                "min": unix_timestamp_iso(numeric_values[0], timestamp_unit),
                "q1": unix_timestamp_iso(
                    _percentile(numeric_values, 0.25),
                    timestamp_unit,
                ),
                "median": unix_timestamp_iso(
                    _percentile(numeric_values, 0.5),
                    timestamp_unit,
                ),
                "q3": unix_timestamp_iso(
                    _percentile(numeric_values, 0.75),
                    timestamp_unit,
                ),
                "max": unix_timestamp_iso(numeric_values[-1], timestamp_unit),
            }
    if semantic_type.startswith("numerical"):
        numeric_values = _finite_numeric_values(values)
        if not numeric_values:
            return {}
        mean = statistics.fmean(numeric_values)
        variance = statistics.pvariance(numeric_values) if len(numeric_values) > 1 else 0.0
        stddev = math.sqrt(variance)
        return {
            "count": len(numeric_values),
            "mean": round(mean, 6),
            "median": round(statistics.median(numeric_values), 6),
            "stddev": round(stddev, 6),
            "variance": round(variance, 6),
            "min": min(numeric_values),
            "q1": round(_percentile(numeric_values, 0.25), 6),
            "q3": round(_percentile(numeric_values, 0.75), 6),
            "max": max(numeric_values),
            "skewness": round(_skewness(numeric_values, mean, stddev), 6),
            "kurtosis": round(_kurtosis(numeric_values, mean, stddev), 6),
        }
    if semantic_type == "text":
        lengths = [len(str(value)) for value in values]
        return {
            "avg_length": round(statistics.fmean(lengths), 2),
            "max_length": max(lengths),
        }
    counter = Counter(str(value) for value in values)
    return {"top_values": counter.most_common(10)}


def _distribution_for_values(
    semantic_type: str,
    values: list[Any],
) -> tuple[str, list[dict[str, Any]]]:
    if not values:
        return "bar", []
    if semantic_type == "temporal":
        timestamp_unit = infer_unix_timestamp_unit(values)
        if timestamp_unit:
            distribution = _histogram(_finite_numeric_values(values))
            for bucket in distribution:
                bucket["label"] = _temporal_histogram_label(
                    bucket["label"],
                    timestamp_unit,
                )
            return "histogram", distribution
    if semantic_type.startswith("numerical"):
        return "histogram", _histogram(_finite_numeric_values(values))
    if semantic_type == "text":
        return "histogram", _histogram([float(len(str(value))) for value in values])

    counts = Counter(str(value) for value in values)
    return "bar", [{"label": label, "count": count} for label, count in counts.most_common(15)]


def _histogram(values: list[float], maximum_bins: int = 12) -> list[dict[str, Any]]:
    if not values:
        return []
    minimum = min(values)
    maximum = max(values)
    if minimum == maximum:
        return [{"label": f"{minimum:g}", "count": len(values)}]

    bin_count = min(maximum_bins, max(1, math.ceil(math.sqrt(len(values)))))
    bin_width = (maximum - minimum) / bin_count
    counts = [0] * bin_count
    for value in values:
        index = min(int((value - minimum) / bin_width), bin_count - 1)
        counts[index] += 1
    return [
        {
            "label": f"{minimum + index * bin_width:g} - {minimum + (index + 1) * bin_width:g}",
            "count": count,
        }
        for index, count in enumerate(counts)
    ]


def _finite_numeric_values(values: list[Any]) -> list[float]:
    numeric_values = [float(value) for value in values if _looks_numeric(str(value).strip())]
    return [value for value in numeric_values if math.isfinite(value)]


def _percentile(values: list[float], percentile: float) -> float:
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = percentile * (len(sorted_values) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return sorted_values[lower_index]
    fraction = position - lower_index
    return (
        sorted_values[lower_index]
        + (sorted_values[upper_index] - sorted_values[lower_index]) * fraction
    )


def _infer_semantic_type(values: list[Any]) -> str:
    if not values:
        return "unknown"
    numeric_count = 0
    decimal_seen = False
    temporal_count = 0
    text_count = 0
    for value in values:
        value_text = str(value).strip()
        if _looks_numeric(value_text):
            numeric_count += 1
            decimal_seen = decimal_seen or not float(value_text).is_integer()
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
        if decimal_seen:
            return "numerical_continuous"
        if len({str(value) for value in values}) <= 20:
            return "numerical_discrete"
        return "numerical_continuous"
    if text_count >= threshold:
        return "text"
    return "categorical"


def _quality_flags(
    column: str,
    semantic_type: str,
    missing_count: int,
    row_count: int,
    values: list[Any],
) -> list[str]:
    flags = []
    if row_count and missing_count / row_count > 0.3:
        flags.append("high_missingness")
    if len({str(value) for value in values}) <= 1 and row_count > 1:
        flags.append("constant_or_near_constant")
    if semantic_type.startswith("numerical"):
        numeric_values = _finite_numeric_values(values)
        if _has_iqr_outliers(numeric_values):
            flags.append("possible_outliers")
    if column.lower() in {"id", "uuid", "guid"} or column.lower().endswith("_id"):
        flags.append("identifier_like")
    return flags


def _infer_task(
    target_column: str | None,
    profile_by_name: dict[str, ColumnProfileRead],
) -> TaskInferenceRead:
    if not target_column:
        return TaskInferenceRead(
            task_type=TaskType.CLUSTERING,
            target_column=None,
            confidence=0.95,
            rationale=(
                "No target column was provided, so the project is configured "
                "as unsupervised clustering."
            ),
        )

    target = profile_by_name[target_column]
    if target.semantic_type == "temporal":
        return TaskInferenceRead(
            task_type=TaskType.TIME_SERIES,
            target_column=target_column,
            confidence=0.78,
            rationale="The selected target is temporal.",
        )
    if target.semantic_type == "numerical_continuous":
        return TaskInferenceRead(
            task_type=TaskType.REGRESSION,
            target_column=target_column,
            confidence=0.86,
            rationale="The selected target is continuous numeric.",
        )
    return TaskInferenceRead(
        task_type=TaskType.CLASSIFICATION,
        target_column=target_column,
        confidence=0.82,
        rationale=("The selected target is categorical, text-like, or low-cardinality numeric."),
    )


def _relationships_against_target(
    rows: list[dict[str, Any]],
    profile_by_name: dict[str, ColumnProfileRead],
    target_column: str | None,
) -> list[FeatureRelationshipRead]:
    if not rows or not target_column:
        return []
    target_profile = profile_by_name[target_column]
    relationships: list[FeatureRelationshipRead] = []
    for column, profile in profile_by_name.items():
        if column == target_column:
            continue
        value = _relationship_value(rows, column, profile, target_column, target_profile)
        if value is not None:
            relationships.append(
                FeatureRelationshipRead(
                    source_column=column,
                    target_column=target_column,
                    method=(
                        "pearson" if profile.semantic_type.startswith("numerical") else "cramers_v"
                    ),
                    value=round(value, 6),
                )
            )
    return sorted(relationships, key=lambda item: abs(item.value), reverse=True)[:25]


def _relationship_value(
    rows: list[dict[str, Any]],
    column: str,
    profile: ColumnProfileRead,
    target_column: str,
    target_profile: ColumnProfileRead,
) -> float | None:
    if profile.semantic_type.startswith("numerical") and target_profile.semantic_type.startswith(
        "numerical"
    ):
        paired = [
            (float(row[column]), float(row[target_column]))
            for row in rows
            if _looks_numeric(str(row.get(column, "")).strip())
            and _looks_numeric(str(row.get(target_column, "")).strip())
        ]
        return _pearson(paired) if len(paired) >= 2 else None
    paired_categories = [
        (str(row.get(column)), str(row.get(target_column)))
        for row in rows
        if not _is_missing(row.get(column)) and not _is_missing(row.get(target_column))
    ]
    return _cramers_v(paired_categories) if len(paired_categories) >= 2 else None


def _build_preparation_plan(
    columns: list[ColumnProfileRead],
    target_column: str | None,
    task_type: TaskType,
) -> list[PreparationStepRead]:
    steps: list[PreparationStepRead] = []
    for column in columns:
        if column.name == target_column:
            continue
        if column.missing_count:
            strategy = (
                "median_imputation"
                if column.semantic_type.startswith("numerical")
                else "most_frequent_imputation"
            )
            steps.append(
                PreparationStepRead(
                    column=column.name,
                    action="impute_missing_values",
                    strategy=strategy,
                    reason=f"{column.missing_ratio:.1%} of values are missing.",
                )
            )
        if column.semantic_type == "temporal":
            steps.append(
                PreparationStepRead(
                    column=column.name,
                    action="extract_time_features",
                    strategy="day_of_week_month_hour_elapsed",
                    reason="Temporal columns should be expanded into model-friendly components.",
                )
            )
        elif column.semantic_type == "text":
            steps.append(
                PreparationStepRead(
                    column=column.name,
                    action="encode_text",
                    strategy="tfidf_plus_text_length",
                    reason=(
                        "Raw text needs lightweight NLP features instead of categorical encoding."
                    ),
                )
            )
        elif column.semantic_type == "categorical":
            steps.append(
                PreparationStepRead(
                    column=column.name,
                    action="encode_categorical",
                    strategy="one_hot_or_target_encoding",
                    reason="Non-ordinal categoricals need explicit encoding.",
                )
            )
        elif column.semantic_type.startswith("numerical"):
            steps.append(
                PreparationStepRead(
                    column=column.name,
                    action="scale_and_check_outliers",
                    strategy="robust_scaler_iqr_outlier_flags",
                    reason="Numeric features benefit from robust scaling and outlier safeguards.",
                )
            )
    steps.append(
        PreparationStepRead(
            column="__all_features__",
            action="feature_selection",
            strategy="iv_woe_rfe_thresholds",
            reason=f"Apply supervised feature selection for {task_type.value} after preprocessing.",
        )
    )
    return steps


def _pearson(paired: list[tuple[float, float]]) -> float | None:
    xs = [item[0] for item in paired]
    ys = [item[1] for item in paired]
    mean_x = statistics.fmean(xs)
    mean_y = statistics.fmean(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in paired)
    denominator_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denominator_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    denominator = denominator_x * denominator_y
    return None if denominator == 0 else numerator / denominator


def _cramers_v(paired: list[tuple[str, str]]) -> float:
    row_labels = sorted({item[0] for item in paired})
    col_labels = sorted({item[1] for item in paired})
    table = Counter(paired)
    total = len(paired)
    chi_square = 0.0
    for row_label in row_labels:
        row_total = sum(table[(row_label, col_label)] for col_label in col_labels)
        for col_label in col_labels:
            col_total = sum(table[(other_row, col_label)] for other_row in row_labels)
            expected = row_total * col_total / total if total else 0
            observed = table[(row_label, col_label)]
            if expected:
                chi_square += (observed - expected) ** 2 / expected
    denominator = total * max(1, min(len(row_labels) - 1, len(col_labels) - 1))
    return math.sqrt(chi_square / denominator) if denominator else 0.0


def _skewness(values: list[float], mean: float, stddev: float) -> float:
    if not values or stddev == 0:
        return 0.0
    return statistics.fmean(((value - mean) / stddev) ** 3 for value in values)


def _kurtosis(values: list[float], mean: float, stddev: float) -> float:
    if not values or stddev == 0:
        return 0.0
    return statistics.fmean(((value - mean) / stddev) ** 4 for value in values) - 3


def _has_iqr_outliers(values: list[float]) -> bool:
    if len(values) < 4:
        return False
    sorted_values = sorted(values)
    midpoint = len(sorted_values) // 2
    lower_half = sorted_values[:midpoint]
    upper_half = sorted_values[midpoint + (len(sorted_values) % 2) :]
    q1 = statistics.median(lower_half)
    q3 = statistics.median(upper_half)
    iqr = q3 - q1
    if iqr == 0:
        return False
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return any(value < lower_bound or value > upper_bound for value in values)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() in {"na", "n/a", "null", "none"}
    return False


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _looks_temporal(value: str) -> bool:
    from datetime import datetime

    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return True


def _temporal_histogram_label(label: str, unit: str) -> str:
    bounds = [part.strip() for part in label.split(" - ", maxsplit=1)]
    try:
        converted = [unix_timestamp_iso(float(bound), unit)[:10] for bound in bounds]
    except ValueError:
        return label
    return " - ".join(converted)
