from __future__ import annotations

from types import SimpleNamespace

import dask.dataframe as dd
import pandas as pd
from automl_api.models.enums import DatasetFormat, TaskType
from automl_api.schemas.profiling import ColumnProfileRead
from automl_api.services.dask_profiling import _profile_column as _profile_dask_column
from automl_api.services.leakage import detect_target_leakage
from automl_api.services.profiling import (
    _build_preparation_plan,
    _distribution_for_values,
    _infer_semantic_type,
    _infer_task,
    _load_csv_rows,
    _relationships_against_target,
    _should_use_dask,
    _statistics_for_values,
)


def test_infer_task_without_target_defaults_to_clustering() -> None:
    result = _infer_task(None, {})

    assert result.task_type == TaskType.CLUSTERING


def test_infer_task_with_continuous_target_is_regression() -> None:
    profile_by_name = {
        "revenue": ColumnProfileRead(
            name="revenue",
            semantic_type="numerical_continuous",
            missing_count=0,
            missing_ratio=0,
            distinct_count=10,
            sample_values=["1.2"],
            statistics={},
            distribution_type="histogram",
            distribution=[],
            quality_flags=[],
        ),
        "customer_birth_date": ColumnProfileRead(
            name="customer_birth_date",
            semantic_type="temporal",
            missing_count=0,
            missing_ratio=0,
            distinct_count=10,
            sample_values=["1990-01-01"],
            statistics={},
            distribution_type="bar",
            distribution=[],
            quality_flags=[],
        ),
    }

    result = _infer_task("revenue", profile_by_name)

    assert result.task_type == TaskType.REGRESSION


def test_infer_task_with_discrete_target_ignores_unrelated_temporal_features() -> None:
    profile_by_name = {
        "loan_default": ColumnProfileRead(
            name="loan_default",
            semantic_type="numerical_discrete",
            missing_count=0,
            missing_ratio=0,
            distinct_count=2,
            sample_values=["0", "1"],
            statistics={"count": 10},
            distribution_type="histogram",
            distribution=[],
            quality_flags=[],
        ),
        "date_of_birth": ColumnProfileRead(
            name="date_of_birth",
            semantic_type="temporal",
            missing_count=0,
            missing_ratio=0,
            distinct_count=10,
            sample_values=["1985-04-12"],
            statistics={},
            distribution_type="bar",
            distribution=[],
            quality_flags=[],
        ),
    }

    result = _infer_task("loan_default", profile_by_name)

    assert result.task_type == TaskType.CLASSIFICATION


def test_infer_task_with_categorical_target_is_classification() -> None:
    profile_by_name = {
        "segment": ColumnProfileRead(
            name="segment",
            semantic_type="categorical",
            missing_count=0,
            missing_ratio=0,
            distinct_count=3,
            sample_values=["small"],
            statistics={"top_values": [["small", 4]]},
            distribution_type="bar",
            distribution=[{"label": "small", "count": 4}],
            quality_flags=[],
        )
    }

    result = _infer_task("segment", profile_by_name)

    assert result.task_type == TaskType.CLASSIFICATION


def test_preparation_plan_uses_type_aware_steps() -> None:
    columns = [
        ColumnProfileRead(
            name="comment",
            semantic_type="text",
            missing_count=1,
            missing_ratio=0.5,
            distinct_count=2,
            sample_values=["some long text"],
            statistics={},
            distribution_type="histogram",
            distribution=[],
            quality_flags=[],
        ),
        ColumnProfileRead(
            name="category",
            semantic_type="categorical",
            missing_count=0,
            missing_ratio=0,
            distinct_count=3,
            sample_values=["a"],
            statistics={},
            distribution_type="bar",
            distribution=[],
            quality_flags=[],
        ),
    ]

    steps = _build_preparation_plan(columns, None, TaskType.CLUSTERING)
    actions = {step.action for step in steps}

    assert "impute_missing_values" in actions
    assert "encode_text" in actions
    assert "encode_categorical" in actions
    assert "feature_selection" in actions


def test_relationships_compute_numeric_pearson() -> None:
    rows = [{"x": "1", "y": "2"}, {"x": "2", "y": "4"}, {"x": "3", "y": "6"}]
    profiles = {
        "x": ColumnProfileRead(
            name="x",
            semantic_type="numerical_continuous",
            missing_count=0,
            missing_ratio=0,
            distinct_count=3,
            sample_values=[],
            statistics={},
            distribution_type="histogram",
            distribution=[],
            quality_flags=[],
        ),
        "y": ColumnProfileRead(
            name="y",
            semantic_type="numerical_continuous",
            missing_count=0,
            missing_ratio=0,
            distinct_count=3,
            sample_values=[],
            statistics={},
            distribution_type="histogram",
            distribution=[],
            quality_flags=[],
        ),
    }

    relationships = _relationships_against_target(rows, profiles, "y")

    assert relationships[0].method == "pearson"
    assert relationships[0].value == 1.0


def test_leakage_detection_excludes_exact_and_encoded_target_proxies() -> None:
    dataframe = pd.DataFrame(
        {
            "safe_feature": list(range(20)),
            "target": ["yes", "no"] * 10,
            "target_copy": ["yes", "no"] * 10,
            "decision_label": [1, 0] * 10,
        }
    )

    analysis = detect_target_leakage(dataframe, "target")

    assert analysis.status == "leakage_detected"
    assert set(analysis.excluded_columns) == {"target_copy", "decision_label"}
    assert {finding.kind for finding in analysis.findings} == {
        "exact_target_copy",
        "encoded_target_proxy",
    }
    assert "safe_feature" not in analysis.excluded_columns


def test_leakage_detection_excludes_numeric_transform_and_reports_duplicates() -> None:
    base = pd.DataFrame(
        {
            "feature": [index % 3 for index in range(20)],
            "target": [float(index) for index in range(20)],
            "future_result": [float(index) * 10 + 7 for index in range(20)],
        }
    )
    dataframe = pd.concat([base, base.iloc[[0]]], ignore_index=True)

    analysis = detect_target_leakage(dataframe, "target")

    assert analysis.excluded_columns == ["future_result"]
    assert analysis.findings[0].kind == "deterministic_numeric_proxy"
    assert analysis.duplicate_row_count == 1
    assert analysis.duplicate_row_ratio > 0


def test_numeric_statistics_include_five_number_summary_and_shape() -> None:
    result = _statistics_for_values("numerical_continuous", [1, 2, 3, 4, 5])

    assert result["min"] == 1
    assert result["q1"] == 2
    assert result["median"] == 3
    assert result["q3"] == 4
    assert result["max"] == 5
    assert "skewness" in result
    assert "kurtosis" in result


def test_text_statistics_include_meaningful_word_frequencies() -> None:
    result = _statistics_for_values(
        "text",
        [
            "The graphics update makes Borderlands gameplay smoother",
            "Borderlands gameplay and graphics look excellent",
            "Excellent gameplay makes this update worth playing",
        ],
    )

    frequencies = {item["word"]: item["count"] for item in result["word_frequencies"]}
    assert frequencies["gameplay"] == 3
    assert frequencies["borderlands"] == 2
    assert "the" not in frequencies
    assert "and" not in frequencies


def test_unix_nanoseconds_are_profiled_as_temporal_dates() -> None:
    values = [
        1_704_067_200_000_000_000,
        1_704_153_600_000_000_000,
        1_704_240_000_000_000_000,
    ]

    assert _infer_semantic_type(values) == "temporal"
    statistics = _statistics_for_values("temporal", values)
    chart_type, distribution = _distribution_for_values("temporal", values)

    assert statistics["timestamp_unit"] == "ns"
    assert statistics["min"].startswith("2024-01-01")
    assert statistics["max"].startswith("2024-01-03")
    assert chart_type == "histogram"
    assert sum(bucket["count"] for bucket in distribution) == 3


def test_numeric_distribution_is_chart_ready_histogram() -> None:
    chart_type, distribution = _distribution_for_values(
        "numerical_continuous",
        [1, 2, 3, 4, 5],
    )

    assert chart_type == "histogram"
    assert sum(bucket["count"] for bucket in distribution) == 5


def test_categorical_distribution_uses_vertical_bar_counts() -> None:
    chart_type, distribution = _distribution_for_values(
        "categorical",
        ["small", "large", "small"],
    )

    assert chart_type == "bar"
    assert distribution[0] == {"label": "small", "count": 2}


def test_csv_loader_profiles_every_row() -> None:
    content = b"value\n1\n2\n3\n4\n5\n"

    rows = _load_csv_rows(content)

    assert len(rows) == 5
    assert rows[-1]["value"] == "5"


def test_memory_risk_switches_csv_to_dask() -> None:
    version = SimpleNamespace(
        format=DatasetFormat.CSV,
        original_filename="large.csv",
        byte_size=100 * 1024 * 1024,
    )

    assert _should_use_dask(version, available_memory_bytes=1024 * 1024 * 1024)


def test_small_csv_stays_on_exact_in_memory_path() -> None:
    version = SimpleNamespace(
        format=DatasetFormat.CSV,
        original_filename="small.csv",
        byte_size=10 * 1024 * 1024,
    )

    assert not _should_use_dask(version, available_memory_bytes=8 * 1024 * 1024 * 1024)


def test_dask_profile_aggregates_all_partitions() -> None:
    dataframe = dd.from_pandas(
        pd.DataFrame({"amount": list(range(1, 101))}),
        npartitions=7,
    )

    profile = _profile_dask_column(dataframe["amount"], "amount", 100)

    assert profile.statistics["count"] == 100
    assert profile.statistics["min"] == 1
    assert profile.statistics["max"] == 100
    assert sum(bucket["count"] for bucket in profile.distribution) == 100


def test_dask_profile_detects_unix_seconds() -> None:
    dataframe = dd.from_pandas(
        pd.DataFrame(
            {
                "event_epoch": [
                    1_704_067_200,
                    1_704_153_600,
                    1_704_240_000,
                ]
            }
        ),
        npartitions=2,
    )

    profile = _profile_dask_column(
        dataframe["event_epoch"],
        "event_epoch",
        3,
    )

    assert profile.semantic_type == "temporal"
    assert profile.statistics["timestamp_unit"] == "s"
    assert profile.statistics["min"].startswith("2024-01-01")


def test_dask_text_profile_includes_word_cloud_frequencies() -> None:
    dataframe = dd.from_pandas(
        pd.DataFrame(
            {
                "tweet": [
                    "Players love the smooth Borderlands gameplay update today",
                    "Borderlands gameplay remains smooth after this excellent update",
                    "Excellent gameplay and smooth graphics make players happy",
                ]
                * 4
            }
        ),
        npartitions=3,
    )

    profile = _profile_dask_column(dataframe["tweet"], "tweet", 12)

    frequencies = {
        item["word"]: item["count"]
        for item in profile.statistics["word_frequencies"]
    }
    assert profile.semantic_type == "text"
    assert frequencies["gameplay"] == 12
    assert frequencies["smooth"] == 12
    assert "the" not in frequencies
