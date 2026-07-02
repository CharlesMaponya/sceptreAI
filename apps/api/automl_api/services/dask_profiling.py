from __future__ import annotations

import csv
import math
from collections import Counter
from typing import Any

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import DatasetFormat
from automl_api.schemas.profiling import ColumnProfileRead, FeatureRelationshipRead
from automl_api.services.temporal import (
    MAX_UNIX_SECONDS,
    MIN_UNIX_SECONDS,
    UNIX_UNIT_SCALES,
    infer_unix_timestamp_unit,
    unix_timestamp_iso,
)
from automl_api.storage.object_store import get_object_store

MISSING_MARKERS = ["", "na", "n/a", "null", "none"]
HISTOGRAM_BINS = 12
MAX_CRAMERS_V_CELLS = 1_000_000


def profile_dataset_with_dask(
    version: DatasetVersion,
    target_column: str | None,
) -> tuple[int, list[ColumnProfileRead], list[FeatureRelationshipRead], list[str]]:
    dataframe = _load_dataframe(version)
    row_count = int(dataframe.shape[0].compute())
    profiles = [
        _profile_column(dataframe[column], str(column), row_count)
        for column in dataframe.columns
    ]
    profile_by_name = {profile.name: profile for profile in profiles}
    relationships, relationship_warnings = _relationships(
        dataframe,
        profile_by_name,
        target_column,
    )
    warnings = [
        (
            f"Processed all {row_count} rows with partitioned Dask execution. "
            "Quartiles use Dask's whole-dataset approximate quantile algorithm."
        ),
        *relationship_warnings,
    ]
    return row_count, profiles, relationships, warnings


def _load_dataframe(version: DatasetVersion) -> dd.DataFrame:
    store = get_object_store()
    source, storage_options = store.dataframe_source(version.object_uri)
    if version.format == DatasetFormat.CSV:
        dialect = _detect_csv_dialect(store.read_head(version.object_uri))
        return dd.read_csv(
            source,
            blocksize="64MB",
            dtype=str,
            keep_default_na=False,
            na_values=MISSING_MARKERS,
            sep=dialect.delimiter,
            quotechar=dialect.quotechar,
            storage_options=storage_options,
        )
    if version.format == DatasetFormat.JSON:
        return dd.read_json(
            source,
            blocksize="64MB",
            lines=True,
            storage_options=storage_options,
        )
    raise ValueError(f"Dask profiling does not support {version.format.value} datasets.")


def _detect_csv_dialect(content_head: bytes) -> csv.Dialect:
    sample = content_head.decode("utf-8-sig", errors="replace")
    try:
        return csv.Sniffer().sniff(sample) if sample.strip() else csv.excel
    except csv.Error:
        return csv.excel


def _clean_series(series: dd.Series) -> dd.Series:
    cleaned = series.astype("string").str.strip()
    return cleaned.mask(cleaned.str.lower().isin(MISSING_MARKERS))


def _profile_column(
    raw_series: dd.Series,
    column: str,
    row_count: int,
) -> ColumnProfileRead:
    series = _clean_series(raw_series)
    present = series.dropna()
    present_count, distinct_count = dask.compute(present.count(), present.nunique())
    present_count = int(present_count)
    distinct_count = int(distinct_count)
    missing_count = row_count - present_count
    sample_values = [str(value) for value in present.head(5, npartitions=-1).tolist()]

    semantic_type, numeric_values, timestamp_unit = _infer_semantic_type(
        present,
        present_count,
        distinct_count,
    )
    if semantic_type.startswith("numerical"):
        statistics_json, distribution, has_outliers = _numeric_profile(numeric_values)
        distribution_type = "histogram"
    elif semantic_type == "temporal":
        if timestamp_unit:
            statistics_json, distribution = _unix_temporal_profile(
                numeric_values,
                timestamp_unit,
            )
            distribution_type = "histogram"
        else:
            top_values = present.value_counts().nlargest(15).compute()
            distribution = [
                {"label": str(value), "count": int(count)}
                for value, count in top_values.items()
            ]
            statistics_json = {
                "top_values": [
                    [str(value), int(count)]
                    for value, count in top_values.head(10).items()
                ]
            }
            distribution_type = "bar"
        has_outliers = False
    elif semantic_type == "text":
        lengths = present.str.len().astype("float64")
        statistics_json, distribution, _ = _numeric_profile(lengths, text_lengths=True)
        distribution_type = "histogram"
        has_outliers = False
    else:
        top_values = present.value_counts().nlargest(15).compute()
        distribution = [
            {"label": str(value), "count": int(count)}
            for value, count in top_values.items()
        ]
        statistics_json = {
            "top_values": [
                [str(value), int(count)]
                for value, count in top_values.head(10).items()
            ]
        }
        distribution_type = "bar"
        has_outliers = False

    quality_flags = _quality_flags(
        column,
        semantic_type,
        missing_count,
        row_count,
        distinct_count,
        has_outliers,
    )
    return ColumnProfileRead(
        name=column,
        semantic_type=semantic_type,
        missing_count=missing_count,
        missing_ratio=0.0 if row_count == 0 else round(missing_count / row_count, 4),
        distinct_count=distinct_count,
        sample_values=sample_values,
        statistics=statistics_json,
        distribution_type=distribution_type,
        distribution=distribution,
        quality_flags=quality_flags,
    )


def _infer_semantic_type(
    series: dd.Series,
    present_count: int,
    distinct_count: int,
) -> tuple[str, dd.Series, str | None]:
    if present_count == 0:
        return "unknown", dd.to_numeric(series, errors="coerce"), None

    numeric_values = dd.to_numeric(series, errors="coerce").dropna()
    numeric_count = int(numeric_values.count().compute())
    threshold = max(1, math.ceil(present_count * 0.8))
    if numeric_count >= threshold:
        timestamp_unit = _dask_unix_timestamp_unit(
            numeric_values,
            present_count,
        )
        if timestamp_unit:
            return "temporal", numeric_values, timestamp_unit
        decimal_seen = bool(((numeric_values % 1).abs() > 1e-12).any().compute())
        if not decimal_seen and distinct_count <= 20:
            return "numerical_discrete", numeric_values, None
        return "numerical_continuous", numeric_values, None

    temporal_count = int(dd.to_datetime(series, errors="coerce").count().compute())
    if temporal_count >= threshold:
        return "temporal", numeric_values, None

    text_count = int(
        series.map_partitions(
            lambda partition: partition.map(
                lambda value: len(str(value).split()) >= 6 or len(str(value)) > 80
            ),
            meta=("text_like", "bool"),
        ).sum().compute()
    )
    if text_count >= threshold:
        return "text", numeric_values, None
    return "categorical", numeric_values, None


def _dask_unix_timestamp_unit(
    values: dd.Series,
    present_count: int,
) -> str | None:
    sample = values.head(min(1000, present_count), npartitions=-1).tolist()
    unit = infer_unix_timestamp_unit(sample)
    if not unit:
        return None
    seconds = values.astype("float64") / UNIX_UNIT_SCALES[unit]
    valid_count = int(
        (
            (seconds >= MIN_UNIX_SECONDS)
            & (seconds <= MAX_UNIX_SECONDS)
        ).sum().compute()
    )
    required = max(1, math.ceil(present_count * 0.8))
    return unit if valid_count >= required else None


def _unix_temporal_profile(
    values: dd.Series,
    unit: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    statistics_json, distribution, _ = _numeric_profile(values)
    temporal_statistics = {
        "count": statistics_json["count"],
        "timestamp_unit": unit,
    }
    for name in ("min", "q1", "median", "q3", "max"):
        temporal_statistics[name] = unix_timestamp_iso(
            statistics_json[name],
            unit,
        )
    for bucket in distribution:
        bucket["label"] = _unix_histogram_label(bucket["label"], unit)
    return temporal_statistics, distribution


def _unix_histogram_label(label: str, unit: str) -> str:
    bounds = [part.strip() for part in label.split(" - ", maxsplit=1)]
    try:
        converted = [
            unix_timestamp_iso(float(bound), unit)[:10]
            for bound in bounds
        ]
    except ValueError:
        return label
    return " - ".join(converted)


def _numeric_profile(
    values: dd.Series,
    *,
    text_lengths: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
    values = values.dropna().astype("float64")
    count = int(values.count().compute())
    if count == 0:
        return {}, [], False

    mean, variance, minimum, maximum, skewness, kurtosis, quantiles = dask.compute(
        values.mean(),
        values.var(ddof=0),
        values.min(),
        values.max(),
        values.skew(),
        values.kurtosis(),
        values.quantile([0.25, 0.5, 0.75]),
    )
    mean = float(mean)
    variance = float(variance)
    minimum = float(minimum)
    maximum = float(maximum)
    q1 = float(quantiles.loc[0.25])
    median = float(quantiles.loc[0.5])
    q3 = float(quantiles.loc[0.75])
    stddev = math.sqrt(max(0.0, variance))

    statistics_json: dict[str, Any]
    if text_lengths:
        statistics_json = {
            "count": count,
            "avg_length": round(mean, 2),
            "max_length": int(maximum),
            "min": minimum,
            "q1": round(q1, 6),
            "median": round(median, 6),
            "q3": round(q3, 6),
            "max": maximum,
        }
    else:
        statistics_json = {
            "count": count,
            "mean": _rounded(mean),
            "median": _rounded(median),
            "stddev": _rounded(stddev),
            "variance": _rounded(variance),
            "min": minimum,
            "q1": _rounded(q1),
            "q3": _rounded(q3),
            "max": maximum,
            "skewness": _rounded(float(skewness)),
            "kurtosis": _rounded(float(kurtosis)),
        }

    distribution = _histogram(values, minimum, maximum, count)
    iqr = q3 - q1
    has_outliers = False
    if iqr > 0:
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        has_outliers = bool(
            ((values < lower_bound) | (values > upper_bound)).any().compute()
        )
    return statistics_json, distribution, has_outliers


def _histogram(
    values: dd.Series,
    minimum: float,
    maximum: float,
    count: int,
) -> list[dict[str, Any]]:
    if minimum == maximum:
        return [{"label": f"{minimum:g}", "count": count}]

    bin_count = min(HISTOGRAM_BINS, max(1, math.ceil(math.sqrt(count))))
    edges = np.linspace(minimum, maximum, bin_count + 1)
    meta = pd.DataFrame(
        {
            "bin": pd.Series(dtype="int64"),
            "count": pd.Series(dtype="int64"),
        }
    )
    partition_counts = values.map_partitions(
        _partition_histogram,
        edges=edges,
        meta=meta,
    )
    counts = partition_counts.groupby("bin")["count"].sum().compute()
    return [
        {
            "label": f"{edges[index]:g} - {edges[index + 1]:g}",
            "count": int(counts.get(index, 0)),
        }
        for index in range(bin_count)
    ]


def _partition_histogram(partition: pd.Series, edges: np.ndarray) -> pd.DataFrame:
    counts, _ = np.histogram(partition.dropna().to_numpy(dtype=float), bins=edges)
    return pd.DataFrame({"bin": range(len(counts)), "count": counts})


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


def _relationships(
    dataframe: dd.DataFrame,
    profiles: dict[str, ColumnProfileRead],
    target_column: str | None,
) -> tuple[list[FeatureRelationshipRead], list[str]]:
    if not target_column or target_column not in profiles:
        return [], []

    relationships = []
    warnings = []
    target_profile = profiles[target_column]
    for column, profile in profiles.items():
        if column == target_column:
            continue
        if (
            profile.semantic_type.startswith("numerical")
            and target_profile.semantic_type.startswith("numerical")
        ):
            value = _numeric_relationship(dataframe, column, target_column)
            method = "pearson"
        else:
            possible_cells = profile.distinct_count * target_profile.distinct_count
            if possible_cells > MAX_CRAMERS_V_CELLS:
                warnings.append(
                    f"Skipped Cramer's V for {column}: the contingency table "
                    f"could contain {possible_cells:,} cells."
                )
                continue
            value = _categorical_relationship(dataframe, column, target_column)
            method = "cramers_v"
        if value is not None and math.isfinite(value):
            relationships.append(
                FeatureRelationshipRead(
                    source_column=column,
                    target_column=target_column,
                    method=method,
                    value=round(value, 6),
                )
            )
    return (
        sorted(relationships, key=lambda item: abs(item.value), reverse=True)[:25],
        warnings,
    )


def _numeric_relationship(
    dataframe: dd.DataFrame,
    column: str,
    target_column: str,
) -> float | None:
    pair = dd.concat(
        [
            dd.to_numeric(_clean_series(dataframe[column]), errors="coerce").rename(column),
            dd.to_numeric(
                _clean_series(dataframe[target_column]),
                errors="coerce",
            ).rename(target_column),
        ],
        axis=1,
    ).dropna()
    if int(pair.shape[0].compute()) < 2:
        return None
    correlation = pair.corr().compute()
    return float(correlation.loc[column, target_column])


def _categorical_relationship(
    dataframe: dd.DataFrame,
    column: str,
    target_column: str,
) -> float | None:
    pair = dd.concat(
        [
            _clean_series(dataframe[column]).rename("source"),
            _clean_series(dataframe[target_column]).rename("target"),
        ],
        axis=1,
    ).dropna()
    counts = pair.groupby(["source", "target"]).size().compute()
    if counts.empty or int(counts.sum()) < 2:
        return None
    table = Counter(
        {
            (str(source), str(target)): int(count)
            for (source, target), count in counts.items()
        }
    )
    row_labels = sorted({source for source, _ in table})
    column_labels = sorted({target for _, target in table})
    total = sum(table.values())
    row_totals = {
        source: sum(table[(source, target)] for target in column_labels)
        for source in row_labels
    }
    column_totals = {
        target: sum(table[(source, target)] for source in row_labels)
        for target in column_labels
    }
    chi_square = 0.0
    for source in row_labels:
        for target in column_labels:
            expected = row_totals[source] * column_totals[target] / total
            if expected:
                chi_square += (table[(source, target)] - expected) ** 2 / expected
    denominator = total * max(1, min(len(row_labels) - 1, len(column_labels) - 1))
    return math.sqrt(chi_square / denominator) if denominator else 0.0


def _rounded(value: float) -> float:
    return round(value, 6) if math.isfinite(value) else 0.0
