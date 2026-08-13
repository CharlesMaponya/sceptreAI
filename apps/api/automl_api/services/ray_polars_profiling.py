from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from collections import Counter
from typing import Any
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.fs as pa_fs
import ray
from ray.data import DataContext, Dataset

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import DatasetFormat
from automl_api.schemas.profiling import (
    ColumnProfileRead,
    FeatureRelationshipRead,
    LeakageAnalysisRead,
)
from automl_api.services.leakage import LEAKAGE_SAMPLE_ROWS, detect_target_leakage
from automl_api.services.temporal import (
    MAX_UNIX_SECONDS,
    MIN_UNIX_SECONDS,
    UNIX_UNIT_SCALES,
    infer_unix_timestamp_unit,
    unix_timestamp_iso,
)
from automl_api.services.text_profiling import text_word_counter
from automl_api.storage.object_store import get_object_store

MISSING_MARKERS = ("", "na", "n/a", "null", "none")
RAY_BATCH_ROWS = 8_192
PER_BATCH_DISTINCT_LIMIT = 4_096
GLOBAL_DISTINCT_LIMIT = 100_000
PER_BATCH_TOP_VALUES = 32
PER_BATCH_WORDS = 256
HISTOGRAM_BINS = 12
INTERNAL_HISTOGRAM_BINS = 240
MAX_CRAMERS_V_CELLS = 1_000_000


def profile_dataset_with_ray(
    version: DatasetVersion,
    target_column: str | None,
) -> tuple[
    int,
    list[ColumnProfileRead],
    list[FeatureRelationshipRead],
    LeakageAnalysisRead,
    list[str],
]:
    dataset = _load_dataset(version)
    row_count = dataset.count()
    columns = [str(column) for column in dataset.schema().names]
    profiles = [_profile_column(dataset, column, row_count) for column in columns]
    profile_by_name = {profile.name: profile for profile in profiles}
    sample_rows = _sample_rows(dataset)
    relationships, relationship_warnings = _relationships_from_rows(
        sample_rows,
        profile_by_name,
        target_column,
        row_count,
    )
    leakage_analysis = detect_target_leakage(pd.DataFrame(sample_rows), target_column)
    warnings = [
        (
            f"Processed all {row_count} rows with Ray Data and bounded Arrow-to-Polars "
            "batches. Quartiles and high-cardinality summaries are deterministic "
            "approximations; leakage and relationships use a bounded sample."
        ),
        *relationship_warnings,
        *leakage_analysis.warnings,
    ]
    return row_count, profiles, relationships, leakage_analysis, warnings


def _ensure_ray() -> None:
    if not ray.is_initialized():
        ray.init(
            address=os.getenv("RAY_ADDRESS") or None,
            include_dashboard=False,
            ignore_reinit_error=True,
            log_to_driver=False,
        )
    DataContext.get_current().enable_progress_bars = False


def _load_dataset(version: DatasetVersion) -> Dataset:
    _ensure_ray()
    store = get_object_store()
    source, storage_options = store.dataframe_source(version.object_uri)
    source, filesystem = _ray_source(source, storage_options)
    if version.format == DatasetFormat.CSV:
        dialect = _detect_csv_dialect(store.read_head(version.object_uri))
        return ray.data.read_csv(
            source,
            filesystem=filesystem,
            parse_options=pa_csv.ParseOptions(
                delimiter=dialect.delimiter,
                quote_char=dialect.quotechar,
            ),
            convert_options=pa_csv.ConvertOptions(
                strings_can_be_null=True,
                null_values=list(MISSING_MARKERS),
            ),
        )
    if version.format == DatasetFormat.JSON:
        return ray.data.read_json(source, filesystem=filesystem, lines=True)
    raise ValueError(
        f"Ray/Polars profiling does not support {version.format.value} datasets."
    )


def _ray_source(
    source: str,
    storage_options: dict[str, object],
) -> tuple[str, pa_fs.FileSystem | None]:
    if not storage_options:
        return source, None
    if not source.startswith("s3://"):
        raise ValueError("Ray Data storage options are only supported for S3 URIs.")
    client_options = storage_options.get("client_kwargs") or {}
    endpoint_url = str(client_options.get("endpoint_url") or "")
    parsed_endpoint = urlparse(endpoint_url)
    filesystem = pa_fs.S3FileSystem(
        access_key=str(storage_options.get("key") or ""),
        secret_key=str(storage_options.get("secret") or ""),
        endpoint_override=parsed_endpoint.netloc or None,
        scheme=parsed_endpoint.scheme or "https",
    )
    return source.removeprefix("s3://"), filesystem


def _detect_csv_dialect(content_head: bytes) -> csv.Dialect:
    sample = content_head.decode("utf-8-sig", errors="replace")
    try:
        return csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    except csv.Error:
        return csv.excel


def _profile_column(
    dataset: Dataset,
    column: str,
    row_count: int,
) -> ColumnProfileRead:
    _ensure_ray()
    summary_rows = (
        dataset.select_columns([column])
        .map_batches(
            _summarize_column_batch,
            batch_format="pyarrow",
            batch_size=RAY_BATCH_ROWS,
            fn_kwargs={"column": column},
            zero_copy_batch=True,
        )
        .take_all()
    )
    summaries = [json.loads(str(row["summary_json"])) for row in summary_rows]
    merged = _merge_summaries(summaries)
    present_count = int(merged["present_count"])
    missing_count = max(0, row_count - present_count)
    distinct_count = int(merged["distinct_count"])
    semantic_type, timestamp_unit = _semantic_type(merged, distinct_count)

    if semantic_type.startswith("numerical"):
        statistics, distribution, has_outliers = _numeric_profile(
            dataset,
            column,
            merged["numeric"],
            mode="numeric",
        )
        distribution_type = "histogram"
    elif semantic_type == "temporal" and timestamp_unit:
        statistics, distribution, _ = _numeric_profile(
            dataset,
            column,
            merged["numeric"],
            mode="numeric",
        )
        statistics = _unix_temporal_statistics(statistics, timestamp_unit)
        distribution = [
            {**bucket, "label": _unix_histogram_label(bucket["label"], timestamp_unit)}
            for bucket in distribution
        ]
        distribution_type = "histogram"
        has_outliers = False
    elif semantic_type == "text":
        statistics, distribution, _ = _numeric_profile(
            dataset,
            column,
            merged["length"],
            mode="length",
        )
        statistics = {
            "count": statistics.get("count", 0),
            "avg_length": statistics.get("mean", 0.0),
            "max_length": int(statistics.get("max", 0)),
            "min": statistics.get("min", 0.0),
            "q1": statistics.get("q1", 0.0),
            "median": statistics.get("median", 0.0),
            "q3": statistics.get("q3", 0.0),
            "max": statistics.get("max", 0.0),
            "approximate_quantiles": True,
            "word_frequencies": [
                {"word": word, "count": count}
                for word, count in merged["words"].most_common(40)
            ],
        }
        distribution_type = "histogram"
        has_outliers = False
    else:
        top_values = merged["top_values"].most_common(15)
        statistics = {
            "top_values": [[str(value), int(count)] for value, count in top_values[:10]],
            "approximate_top_values": bool(merged["summary_count"] > 1),
        }
        distribution = [
            {"label": str(value), "count": int(count)} for value, count in top_values
        ]
        distribution_type = "bar"
        has_outliers = False

    return ColumnProfileRead(
        name=column,
        semantic_type=semantic_type,
        missing_count=missing_count,
        missing_ratio=0.0 if row_count == 0 else round(missing_count / row_count, 4),
        distinct_count=distinct_count,
        sample_values=list(merged["sample_values"]),
        statistics=statistics,
        distribution_type=distribution_type,
        distribution=distribution,
        quality_flags=_quality_flags(
            column,
            semantic_type,
            missing_count,
            row_count,
            distinct_count,
            has_outliers,
        ),
    )


def _summarize_column_batch(batch: pa.Table, *, column: str) -> pa.Table:
    frame = pl.from_arrow(batch)
    if not isinstance(frame, pl.DataFrame):
        frame = frame.to_frame()
    raw = frame.get_column(column).cast(pl.String, strict=False).str.strip_chars()
    lower = raw.str.to_lowercase()
    present = raw.filter(raw.is_not_null() & ~lower.is_in(MISSING_MARKERS))
    present_values = [str(value) for value in present.to_list()]
    unique_values = list(dict.fromkeys(present_values))
    distinct_overflow = len(unique_values) > PER_BATCH_DISTINCT_LIMIT
    distinct_hashes = [
        _stable_value_hash(value)
        for value in unique_values[:PER_BATCH_DISTINCT_LIMIT]
    ]

    numeric = present.cast(pl.Float64, strict=False).drop_nulls()
    numeric = numeric.filter(numeric.is_finite())
    numeric_values = numeric.to_numpy()
    lengths = present.str.len_chars().cast(pl.Float64).to_numpy()
    temporal_count = _temporal_value_count(present)
    text_count = sum(
        1 for value in present_values if len(value.split()) >= 6 or len(value) > 80
    )
    words = text_word_counter(present_values, maximum_terms=PER_BATCH_WORDS)
    summary = {
        "row_count": frame.height,
        "present_count": len(present_values),
        "sample_values": present_values[:5],
        "distinct_hashes": distinct_hashes,
        "distinct_overflow": distinct_overflow,
        "top_values": Counter(present_values).most_common(PER_BATCH_TOP_VALUES),
        "words": words.most_common(PER_BATCH_WORDS),
        "numeric": _moments(numeric_values),
        "numeric_sample": [float(value) for value in numeric_values[:128]],
        "decimal_seen": bool(
            numeric_values.size
            and np.any(np.abs(np.mod(numeric_values, 1.0)) > 1e-12)
        ),
        "temporal_count": temporal_count,
        "text_count": text_count,
        "length": _moments(lengths),
    }
    return pa.table({"summary_json": [json.dumps(summary, separators=(",", ":"))]})


def _temporal_value_count(values: pl.Series) -> int:
    try:
        return int(values.str.to_datetime(strict=False).is_not_null().sum())
    except pl.exceptions.ComputeError:
        try:
            return int(values.str.to_date(strict=False).is_not_null().sum())
        except pl.exceptions.ComputeError:
            return 0


def _moments(values: np.ndarray) -> dict[str, float | int | None]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0:
        return {
            "count": 0,
            "sum": 0.0,
            "sum2": 0.0,
            "sum3": 0.0,
            "sum4": 0.0,
            "min": None,
            "max": None,
        }
    return {
        "count": int(finite.size),
        "sum": float(np.sum(finite)),
        "sum2": float(np.sum(np.square(finite))),
        "sum3": float(np.sum(np.power(finite, 3))),
        "sum4": float(np.sum(np.power(finite, 4))),
        "min": float(np.min(finite)),
        "max": float(np.max(finite)),
    }


def _merge_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    distinct_hashes: set[int] = set()
    distinct_overflow = False
    top_values: Counter[str] = Counter()
    words: Counter[str] = Counter()
    samples: list[str] = []
    numeric_samples: list[float] = []
    numeric = _empty_moments()
    length = _empty_moments()
    present_count = 0
    temporal_count = 0
    text_count = 0
    decimal_seen = False
    for summary in summaries:
        present_count += int(summary["present_count"])
        temporal_count += int(summary["temporal_count"])
        text_count += int(summary["text_count"])
        decimal_seen = decimal_seen or bool(summary["decimal_seen"])
        samples.extend(str(value) for value in summary["sample_values"])
        numeric_samples.extend(float(value) for value in summary["numeric_sample"])
        top_values.update({str(value): int(count) for value, count in summary["top_values"]})
        words.update({str(value): int(count) for value, count in summary["words"]})
        _merge_moments(numeric, summary["numeric"])
        _merge_moments(length, summary["length"])
        if summary["distinct_overflow"]:
            distinct_overflow = True
        for value_hash in summary["distinct_hashes"]:
            if len(distinct_hashes) >= GLOBAL_DISTINCT_LIMIT:
                distinct_overflow = True
                break
            distinct_hashes.add(int(value_hash))
    return {
        "summary_count": len(summaries),
        "present_count": present_count,
        "sample_values": samples[:5],
        "distinct_count": (
            max(GLOBAL_DISTINCT_LIMIT + 1, len(distinct_hashes))
            if distinct_overflow
            else len(distinct_hashes)
        ),
        "top_values": top_values,
        "words": words,
        "numeric": numeric,
        "numeric_sample": numeric_samples[:1_000],
        "temporal_count": temporal_count,
        "text_count": text_count,
        "decimal_seen": decimal_seen,
        "length": length,
    }


def _empty_moments() -> dict[str, float | int | None]:
    return {
        "count": 0,
        "sum": 0.0,
        "sum2": 0.0,
        "sum3": 0.0,
        "sum4": 0.0,
        "min": None,
        "max": None,
    }


def _merge_moments(
    target: dict[str, float | int | None],
    source: dict[str, float | int | None],
) -> None:
    for name in ("count", "sum", "sum2", "sum3", "sum4"):
        target[name] = (target[name] or 0) + (source[name] or 0)
    source_min = source.get("min")
    source_max = source.get("max")
    if source_min is not None:
        target["min"] = source_min if target["min"] is None else min(target["min"], source_min)
    if source_max is not None:
        target["max"] = source_max if target["max"] is None else max(target["max"], source_max)


def _semantic_type(summary: dict[str, Any], distinct_count: int) -> tuple[str, str | None]:
    present_count = int(summary["present_count"])
    if present_count == 0:
        return "unknown", None
    threshold = max(1, math.ceil(present_count * 0.8))
    numeric = summary["numeric"]
    numeric_count = int(numeric["count"])
    if numeric_count >= threshold:
        unit = infer_unix_timestamp_unit(summary["numeric_sample"])
        if unit and _unix_range_is_valid(numeric, unit):
            return "temporal", unit
        if not summary["decimal_seen"] and distinct_count <= 20:
            return "numerical_discrete", None
        return "numerical_continuous", None
    if int(summary["temporal_count"]) >= threshold:
        return "temporal", None
    if int(summary["text_count"]) >= threshold:
        return "text", None
    return "categorical", None


def _unix_range_is_valid(moments: dict[str, Any], unit: str) -> bool:
    scale = UNIX_UNIT_SCALES[unit]
    minimum = float(moments["min"]) / scale
    maximum = float(moments["max"]) / scale
    return minimum >= MIN_UNIX_SECONDS and maximum <= MAX_UNIX_SECONDS


def _numeric_profile(
    dataset: Dataset,
    column: str,
    moments: dict[str, Any],
    *,
    mode: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
    count = int(moments["count"])
    if count == 0:
        return {}, [], False
    minimum = float(moments["min"])
    maximum = float(moments["max"])
    edges, counts = _distributed_histogram(dataset, column, minimum, maximum, count, mode)
    q1 = _histogram_quantile(edges, counts, 0.25)
    median = _histogram_quantile(edges, counts, 0.5)
    q3 = _histogram_quantile(edges, counts, 0.75)
    mean = float(moments["sum"]) / count
    second = float(moments["sum2"]) / count
    variance = max(0.0, second - mean**2)
    stddev = math.sqrt(variance)
    third = float(moments["sum3"]) / count - 3 * mean * second + 2 * mean**3
    fourth = (
        float(moments["sum4"]) / count
        - 4 * mean * (float(moments["sum3"]) / count)
        + 6 * mean**2 * second
        - 3 * mean**4
    )
    skewness = third / stddev**3 if stddev else 0.0
    kurtosis = fourth / variance**2 - 3 if variance else 0.0
    statistics = {
        "count": count,
        "mean": _rounded(mean),
        "median": _rounded(median),
        "stddev": _rounded(stddev),
        "variance": _rounded(variance),
        "min": minimum,
        "q1": _rounded(q1),
        "q3": _rounded(q3),
        "max": maximum,
        "skewness": _rounded(skewness),
        "kurtosis": _rounded(kurtosis),
        "approximate_quantiles": True,
    }
    iqr = q3 - q1
    has_outliers = False
    if iqr > 0:
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        for index, bucket_count in enumerate(counts):
            if bucket_count and (edges[index + 1] < lower_bound or edges[index] > upper_bound):
                has_outliers = True
                break
    return statistics, _display_histogram(edges, counts), has_outliers


def _distributed_histogram(
    dataset: Dataset,
    column: str,
    minimum: float,
    maximum: float,
    count: int,
    mode: str,
) -> tuple[np.ndarray, np.ndarray]:
    if minimum == maximum:
        return np.asarray([minimum, maximum], dtype=float), np.asarray([count], dtype=int)
    bin_count = min(
        INTERNAL_HISTOGRAM_BINS,
        max(HISTOGRAM_BINS, math.ceil(math.sqrt(count))),
    )
    edges = np.linspace(minimum, maximum, bin_count + 1)
    rows = (
        dataset.select_columns([column])
        .map_batches(
            _histogram_batch,
            batch_format="pyarrow",
            batch_size=RAY_BATCH_ROWS,
            fn_kwargs={"column": column, "edges": edges.tolist(), "mode": mode},
            zero_copy_batch=True,
        )
        .take_all()
    )
    counts = np.zeros(bin_count, dtype=np.int64)
    for row in rows:
        counts += np.asarray(json.loads(str(row["counts_json"])), dtype=np.int64)
    return edges, counts


def _histogram_batch(
    batch: pa.Table,
    *,
    column: str,
    edges: list[float],
    mode: str,
) -> pa.Table:
    frame = pl.from_arrow(batch)
    if not isinstance(frame, pl.DataFrame):
        frame = frame.to_frame()
    raw = frame.get_column(column).cast(pl.String, strict=False).str.strip_chars()
    lower = raw.str.to_lowercase()
    present = raw.filter(raw.is_not_null() & ~lower.is_in(MISSING_MARKERS))
    if mode == "length":
        values = present.str.len_chars().cast(pl.Float64).to_numpy()
    else:
        numeric = present.cast(pl.Float64, strict=False).drop_nulls()
        values = numeric.filter(numeric.is_finite()).to_numpy()
    counts, _ = np.histogram(np.asarray(values, dtype=float), bins=np.asarray(edges))
    return pa.table({"counts_json": [json.dumps(counts.tolist(), separators=(",", ":"))]})


def _histogram_quantile(edges: np.ndarray, counts: np.ndarray, quantile: float) -> float:
    total = int(np.sum(counts))
    if total == 0:
        return 0.0
    target = quantile * max(0, total - 1)
    cumulative = 0
    for index, bucket_count in enumerate(counts):
        next_cumulative = cumulative + int(bucket_count)
        if target < next_cumulative and bucket_count:
            fraction = (target - cumulative) / max(1, int(bucket_count))
            return float(edges[index] + fraction * (edges[index + 1] - edges[index]))
        cumulative = next_cumulative
    return float(edges[-1])


def _display_histogram(edges: np.ndarray, counts: np.ndarray) -> list[dict[str, Any]]:
    if len(counts) == 1:
        return [{"label": f"{edges[0]:g}", "count": int(counts[0])}]
    buckets = []
    for indices in np.array_split(np.arange(len(counts)), min(HISTOGRAM_BINS, len(counts))):
        start = int(indices[0])
        end = int(indices[-1]) + 1
        buckets.append(
            {
                "label": f"{edges[start]:g} - {edges[end]:g}",
                "count": int(np.sum(counts[indices])),
            }
        )
    return buckets


def _unix_temporal_statistics(statistics: dict[str, Any], unit: str) -> dict[str, Any]:
    temporal = {"count": statistics.get("count", 0), "timestamp_unit": unit}
    for name in ("min", "q1", "median", "q3", "max"):
        temporal[name] = unix_timestamp_iso(statistics[name], unit)
    temporal["approximate_quantiles"] = True
    return temporal


def _unix_histogram_label(label: str, unit: str) -> str:
    bounds = [part.strip() for part in label.split(" - ", maxsplit=1)]
    try:
        converted = [unix_timestamp_iso(float(bound), unit)[:10] for bound in bounds]
    except ValueError:
        return label
    return " - ".join(converted)


def _sample_rows(dataset: Dataset, limit: int = LEAKAGE_SAMPLE_ROWS) -> list[dict[str, Any]]:
    return [dict(row) for row in dataset.limit(limit).take_all()]


def _relationships(
    dataset: Dataset,
    profiles: dict[str, ColumnProfileRead],
    target_column: str | None,
) -> tuple[list[FeatureRelationshipRead], list[str]]:
    row_count = dataset.count()
    return _relationships_from_rows(
        _sample_rows(dataset),
        profiles,
        target_column,
        row_count,
    )


def _relationships_from_rows(
    rows: list[dict[str, Any]],
    profiles: dict[str, ColumnProfileRead],
    target_column: str | None,
    row_count: int,
) -> tuple[list[FeatureRelationshipRead], list[str]]:
    if not target_column or target_column not in profiles:
        return [], []
    warnings = []
    for column, profile in profiles.items():
        if column == target_column:
            continue
        possible_cells = profile.distinct_count * profiles[target_column].distinct_count
        if (
            not profile.semantic_type.startswith("numerical")
            or not profiles[target_column].semantic_type.startswith("numerical")
        ) and possible_cells > MAX_CRAMERS_V_CELLS:
            warnings.append(
                f"Skipped Cramer's V for {column}: the contingency table could contain "
                f"{possible_cells:,} cells."
            )
    from automl_api.services.profiling import _relationships_against_target

    relationships = _relationships_against_target(rows, profiles, target_column)
    if row_count > len(rows):
        warnings.append(
            f"Relationship and leakage analysis used the first {len(rows):,} rows of "
            f"{row_count:,}; full-column summaries still cover every row."
        )
    return relationships, warnings


def _quality_flags(
    column: str,
    semantic_type: str,
    missing_count: int,
    row_count: int,
    distinct_count: int,
    has_outliers: bool,
) -> list[str]:
    flags = []
    if row_count and missing_count / row_count > 0.3:
        flags.append("high_missingness")
    if distinct_count <= 1 and row_count > 1:
        flags.append("constant_or_near_constant")
    if semantic_type.startswith("numerical") and has_outliers:
        flags.append("possible_outliers")
    if column.lower() in {"id", "uuid", "guid"} or column.lower().endswith("_id"):
        flags.append("identifier_like")
    return flags


def _stable_value_hash(value: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest(),
        byteorder="big",
        signed=False,
    )


def _rounded(value: float) -> float:
    return round(value, 6) if math.isfinite(value) else 0.0
