from __future__ import annotations

from types import SimpleNamespace

import automl_api.services.profiling as profiling
import pytest
from automl_api.models.enums import DatasetFormat, TaskType
from automl_api.schemas.profiling import ColumnProfileRead, LeakageAnalysisRead
from automl_api.services.dataset_inspection import (
    ColumnAccumulator,
    _duplicate_count,
    _infer_type,
    _inspect_json,
    _looks_decimal,
    _preview_metadata,
    detect_dataset_format,
)
from automl_api.services.dataset_inspection import (
    _is_missing as inspection_missing,
)
from automl_api.services.dataset_inspection import (
    _looks_numeric as inspection_numeric,
)
from automl_api.services.dataset_inspection import (
    _looks_temporal as inspection_temporal,
)
from fastapi import HTTPException


def column(name: str, semantic: str, *, missing: int = 0, ratio: float = 0) -> ColumnProfileRead:
    return ColumnProfileRead(
        name=name,
        semantic_type=semantic,
        missing_count=missing,
        missing_ratio=ratio,
        distinct_count=2,
        sample_values=[],
        statistics={},
        distribution_type="bar",
        distribution=[],
        quality_flags=[],
    )


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"", []),
        (b'[1, {"a": 2}, "x"]', [{"a": 2}]),
        (b'{"data": [{"a": 1}, 2]}', [{"a": 1}]),
        (b'{"a": 1}', [{"a": 1}]),
        (b"1", []),
        (b'{"a":1}\n\n{"a":2}\n', [{"a": 1}, {"a": 2}]),
    ],
)
def test_json_loader_contract(payload: bytes, expected: list[dict[str, object]]) -> None:
    assert profiling._load_json_rows(payload) == expected


def test_csv_loader_falls_back_for_an_unknown_dialect() -> None:
    assert profiling._load_csv_rows(b"name\nalpha\n") == [{"name": "alpha"}]
    assert profiling._load_csv_rows(b"") == []


def test_row_loading_and_column_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    unsupported = SimpleNamespace(format=DatasetFormat.PARQUET)
    rows, warnings = profiling._load_rows(unsupported)
    assert rows == [] and "stored metadata" in warnings[0]

    broken = SimpleNamespace(format=DatasetFormat.CSV, object_uri="store://missing")
    monkeypatch.setattr(
        profiling,
        "get_object_store",
        lambda: SimpleNamespace(
            read_bytes=lambda _uri: (_ for _ in ()).throw(OSError("offline")),
        ),
    )
    rows, warnings = profiling._load_rows(broken)
    assert rows == [] and "offline" in warnings[0]

    version = SimpleNamespace(schema_json={"columns": [{"name": "b"}, {}, {"name": "a"}]})
    assert profiling._columns_from_rows_or_version([{"b": 1}, {"a": 2}], version) == ["a", "b"]
    assert profiling._columns_from_rows_or_version([], version) == ["b", "a"]


def test_stored_profile_contract_and_lookup() -> None:
    version = SimpleNamespace(
        schema_json={
            "columns": [
                {
                    "name": "age",
                    "missing_count": 3,
                    "distinct_count": 2,
                    "sample_values": [1, 2],
                    "semantic_type": "categorical",
                }
            ]
        },
        inferred_types_json={"age": {"semantic_type": "numerical_discrete"}},
    )
    profile = profiling._profile_column("age", [], 0, version)
    assert profile.semantic_type == "numerical_discrete"
    assert profile.missing_count == 3
    assert profile.sample_values == ["1", "2"]
    assert profiling._stored_column_profile("unknown", version) == {}


@pytest.mark.parametrize(
    ("semantic", "values", "key"),
    [
        ("unknown", [], None),
        ("numerical_continuous", ["bad", "inf"], None),
        ("numerical_discrete", [5], "mean"),
        ("text", ["one two three four five six"], "word_frequencies"),
        ("categorical", ["a", "a", "b"], "top_values"),
    ],
)
def test_statistics_edge_contracts(semantic: str, values: list[object], key: str | None) -> None:
    result = profiling._statistics_for_values(semantic, values)
    assert (key in result) if key else result == {}


def test_distribution_histogram_and_percentile_edges() -> None:
    assert profiling._distribution_for_values("unknown", []) == ("bar", [])
    assert profiling._distribution_for_values("numerical_continuous", [2, 2]) == (
        "histogram",
        [{"label": "2", "count": 2}],
    )
    chart, values = profiling._distribution_for_values("text", ["a", "long"])
    assert chart == "histogram" and sum(item["count"] for item in values) == 2
    assert profiling._percentile([9], 0.25) == 9
    assert profiling._percentile([1, 2, 3], 0.5) == 2
    assert profiling._percentile([0, 10], 0.25) == 2.5
    assert profiling._finite_numeric_values(["1", "nan", "inf", "bad", 2]) == [1.0, 2.0]


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([], "unknown"),
        (["2026-01-01", "2026-01-02"], "temporal"),
        (["1", "2"], "numerical_discrete"),
        ([str(index) for index in range(25)], "numerical_continuous"),
        (["1.5", "2.5"], "numerical_continuous"),
        (["this sentence contains at least six distinct words"], "text"),
        (["red", "blue"], "categorical"),
    ],
)
def test_semantic_inference_edges(values: list[object], expected: str) -> None:
    assert profiling._infer_semantic_type(values) == expected


def test_quality_flags_cover_missing_constant_outlier_and_identifier() -> None:
    flags = profiling._quality_flags(
        "customer_id", "numerical_continuous", 4, 10, [1, 2, 2, 3, 3, 4, 100]
    )
    assert set(flags) == {"high_missingness", "possible_outliers", "identifier_like"}
    assert "constant_or_near_constant" in profiling._quality_flags(
        "x", "categorical", 0, 3, ["a", "a"]
    )
    assert not profiling._has_iqr_outliers([1, 2, 3])
    assert not profiling._has_iqr_outliers([1, 1, 1, 1])


def test_relationship_and_preparation_edges() -> None:
    numeric = column("x", "numerical_continuous")
    category = column("kind", "categorical", missing=1, ratio=0.5)
    target = column("target", "categorical")
    profiles = {item.name: item for item in [numeric, category, target]}
    rows = [{"x": "bad", "kind": "a", "target": "yes"}, {"x": "1", "kind": "", "target": "no"}]
    relationships = profiling._relationships_against_target(rows, profiles, "target")
    assert relationships[0].method == "cramers_v"
    assert profiling._relationships_against_target([], profiles, "target") == []
    assert profiling._relationships_against_target(rows, profiles, None) == []
    assert profiling._pearson([(1, 2), (1, 3)]) is None
    assert profiling._cramers_v([]) == 0

    leakage = LeakageAnalysisRead(
        status="leakage_detected",
        target_column="target",
        analyzed_rows=2,
        excluded_columns=["proxy"],
        findings=[
            {
                "column": "proxy",
                "kind": "copy",
                "reason": "copy",
                "confidence": 1,
                "auto_excluded": True,
                "severity": "high",
            }
        ],
        duplicate_row_count=0,
        duplicate_row_ratio=0,
        warnings=[],
    )
    steps = profiling._build_preparation_plan(
        [
            column("proxy", "categorical"),
            category,
            column("when", "temporal"),
            column("notes", "text"),
            numeric,
            target,
        ],
        "target",
        TaskType.CLASSIFICATION,
        leakage,
    )
    actions = {step.action for step in steps}
    assert actions >= {
        "exclude_target_leakage",
        "impute_missing_values",
        "encode_categorical",
        "extract_time_features",
        "encode_text",
        "scale_and_check_outliers",
        "feature_selection",
    }


def test_scalar_parsing_and_temporal_label_edges() -> None:
    for value in (None, "", " NA ", "null", "None"):
        assert profiling._is_missing(value)
    assert not profiling._is_missing(0)
    assert profiling._looks_numeric("1.2") and not profiling._looks_numeric("x")
    assert profiling._looks_temporal("2026-01-01T00:00:00Z")
    assert not profiling._looks_temporal("tomorrow")
    assert profiling._temporal_histogram_label("bad - label", "s") == "bad - label"
    assert profiling._skewness([], 0, 0) == 0
    assert profiling._kurtosis([1], 1, 0) == 0


def test_dataset_inspection_json_and_type_edges() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        detect_dataset_format("records.txt")
    assert _inspect_json(b"").row_count == 0
    assert _inspect_json(b'[1, {"x": 2}]').row_count == 1
    assert _inspect_json(b'{"data": [{"x": 1}, 3]}').row_count == 1
    assert _inspect_json(b'{"x": 1}').row_count == 1
    assert _inspect_json(b"1").row_count == 0
    assert _inspect_json(b'{"x":1}\n{"x":2}\n').row_count == 2
    assert _duplicate_count([{"x": 1}, {"x": 1}, {"x": 2}]) == 1

    assert _infer_type([]) == "unknown"
    assert _infer_type(["one two three four five six"]) == "text"
    assert _infer_type(["red", "blue"]) == "categorical"
    assert _infer_type([str(i) for i in range(21)]) == "numerical_continuous"
    assert _infer_type(["1", "2"]) == "numerical_discrete"
    assert _preview_metadata("numerical_continuous", [1, 2])["statistics"]["median"] == 1.5
    assert _preview_metadata("numerical_continuous", [])["statistics"]["mean"] is None
    assert _preview_metadata("temporal", [])["statistics"] == {"min": None, "max": None}


def test_dataset_inspection_accumulator_and_value_helpers() -> None:
    accumulator = ColumnAccumulator("value")
    accumulator.add(None)
    for item in ["1.5", "2026-01-01", "this value has more than six words in it", "red"]:
        accumulator.add(item)
    result = accumulator.profile()
    assert result["missing_count"] == 1
    assert result["distinct_count"] == 4

    empty = ColumnAccumulator("empty")
    assert empty.profile()["semantic_type"] == "unknown"
    assert inspection_numeric("1") and not inspection_numeric("x")
    assert _looks_decimal("1.5") and not _looks_decimal("1") and not _looks_decimal("x")
    assert inspection_temporal("2026-01-01T00:00:00Z") and not inspection_temporal("x")
    assert (
        inspection_missing(None) and inspection_missing(" n/a ") and not inspection_missing(False)
    )


def test_dataset_version_lookup_fails_closed() -> None:
    db = SimpleNamespace(scalar=lambda _statement: None)
    with pytest.raises(HTTPException) as caught:
        profiling._get_project_dataset_version(db, object(), object(), object())
    assert caught.value.status_code == 404


@pytest.mark.parametrize(
    ("format", "filename", "expected"),
    [
        (DatasetFormat.CSV, "data.csv", True),
        (DatasetFormat.JSON, "data.jsonl", True),
        (DatasetFormat.JSON, "data.ndjson", True),
        (DatasetFormat.JSON, "data.json", False),
        (DatasetFormat.PARQUET, "data.parquet", False),
    ],
)
def test_ray_routing_is_format_deterministic(format, filename, expected) -> None:
    version = SimpleNamespace(format=format, original_filename=filename)
    assert profiling._should_use_ray(version) is expected


def test_build_profile_rejects_missing_target_on_local_path(monkeypatch) -> None:
    version = SimpleNamespace(
        format=DatasetFormat.PARQUET,
        original_filename="data.parquet",
        schema_json={"columns": [{"name": "feature"}]},
        inferred_types_json={},
    )
    monkeypatch.setattr(profiling, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(profiling, "_get_project_dataset_version", lambda *_args: version)
    with pytest.raises(HTTPException, match="Target column") as caught:
        profiling.build_dataset_profile(
            SimpleNamespace(),
            SimpleNamespace(),
            object(),
            object(),
            object(),
            profiling.ProfileRequest(target_column="missing"),
        )
    assert caught.value.status_code == 422


def test_task_inference_covers_all_task_types() -> None:
    profiles = {
        "time": column("time", "temporal"),
        "amount": column("amount", "numerical_continuous"),
        "label": column("label", "categorical"),
    }
    assert profiling._infer_task(None, profiles).task_type == TaskType.CLUSTERING
    assert profiling._infer_task("time", profiles).task_type == TaskType.TIME_SERIES
    assert profiling._infer_task("amount", profiles).task_type == TaskType.REGRESSION
    assert profiling._infer_task("label", profiles).task_type == TaskType.CLASSIFICATION


def test_temporal_statistics_and_distributions_accept_unix_timestamps() -> None:
    values = [1_700_000_000, 1_700_000_100]
    statistics = profiling._statistics_for_values("temporal", values)
    assert statistics["timestamp_unit"] == "s"
    chart, distribution = profiling._distribution_for_values("temporal", values)
    assert chart == "histogram" and distribution


def test_relationship_math_non_degenerate_paths() -> None:
    assert profiling._pearson([(1, 2), (2, 4), (3, 6)]) == pytest.approx(1.0)
    score = profiling._cramers_v(
        [("a", "yes"), ("a", "yes"), ("b", "no"), ("b", "no")]
    )
    assert score == pytest.approx(1.0)
    assert profiling._skewness([1, 2, 3], 2, 1) == 0
    assert profiling._kurtosis([1, 2, 3], 2, 1) == pytest.approx(-7 / 3)
