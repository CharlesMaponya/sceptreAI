from __future__ import annotations

import csv
import json
from collections import Counter
from types import SimpleNamespace

import numpy as np
import polars as pl
import pyarrow as pa
import pytest
from automl_api.models.enums import DatasetFormat
from automl_api.services import ray_polars_profiling as profiler


def test_ray_initialization_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []
    context = SimpleNamespace(enable_progress_bars=True)
    monkeypatch.setattr(profiler.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(profiler.ray, "init", lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(profiler.DataContext, "get_current", lambda: context)
    monkeypatch.delenv("RAY_ADDRESS", raising=False)
    profiler._ensure_ray()
    assert calls[0]["address"] is None and context.enable_progress_bars is False
    monkeypatch.setattr(profiler.ray, "is_initialized", lambda: True)
    profiler._ensure_ray()
    assert len(calls) == 1


def test_ray_source_local_s3_and_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    assert profiler._ray_source("/tmp/data.csv", {}) == ("/tmp/data.csv", None)
    with pytest.raises(ValueError, match="only supported for S3"):
        profiler._ray_source("/tmp/data.csv", {"key": "x"})
    captured: dict = {}
    monkeypatch.setattr(
        profiler.pa_fs, "S3FileSystem", lambda **kwargs: captured.update(kwargs) or "fs"
    )
    source, filesystem = profiler._ray_source(
        "s3://bucket/key.csv",
        {
            "key": "access",
            "secret": "secret",
            "client_kwargs": {"endpoint_url": "http://minio:9000"},
        },
    )
    assert (source, filesystem) == ("bucket/key.csv", "fs")
    assert captured["endpoint_override"] == "minio:9000" and captured["scheme"] == "http"
    assert captured["access_key"] == "access" and captured["secret_key"] == "secret"

    captured.clear()
    source, filesystem = profiler._ray_source(
        "s3://bucket/key.csv",
        {"client_kwargs": {"endpoint_url": "http://seaweedfs:8333"}},
    )
    assert (source, filesystem) == ("bucket/key.csv", "fs")
    assert "access_key" not in captured and "secret_key" not in captured


def test_load_dataset_dispatch_and_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    store = SimpleNamespace(
        dataframe_source=lambda _uri: ("/tmp/data", {}),
        read_head=lambda _uri: b"a;b\n1;2\n",
    )
    monkeypatch.setattr(profiler, "_ensure_ray", lambda: None)
    monkeypatch.setattr(profiler, "get_object_store", lambda: store)
    monkeypatch.setattr(profiler.ray.data, "read_csv", lambda source, **kwargs: (source, kwargs))
    monkeypatch.setattr(profiler.ray.data, "read_json", lambda source, **kwargs: (source, kwargs))
    monkeypatch.setattr(
        profiler.ray.data, "read_parquet", lambda source, **kwargs: (source, kwargs)
    )
    csv_version = SimpleNamespace(object_uri="uri", format=DatasetFormat.CSV)
    source, options = profiler._load_dataset(csv_version)
    assert source == "/tmp/data" and options["parse_options"].delimiter == ";"
    json_version = SimpleNamespace(object_uri="uri", format=DatasetFormat.JSON)
    assert profiler._load_dataset(json_version)[1]["lines"] is True
    assert profiler._load_dataset(
        SimpleNamespace(object_uri="uri", format=DatasetFormat.PARQUET)
    )[0] == "/tmp/data"
    with pytest.raises(ValueError, match="does not support"):
        profiler._load_dataset(SimpleNamespace(object_uri="uri", format=DatasetFormat.EXCEL))


def test_csv_dialect_empty_fallback_and_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    assert profiler._detect_csv_dialect(b"").delimiter == csv.excel.delimiter
    assert profiler._detect_csv_dialect(b"a|b\n1|2").delimiter == "|"
    monkeypatch.setattr(csv.Sniffer, "sniff", lambda *_args: (_ for _ in ()).throw(csv.Error()))
    assert profiler._detect_csv_dialect(b"invalid").delimiter == csv.excel.delimiter


def test_batch_summary_handles_missing_numeric_text_and_distinct_overflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(profiler, "PER_BATCH_DISTINCT_LIMIT", 2)
    table = pa.table(
        {"value": [" 1 ", "2.5", "NA", None, "a long sentence with six words here", "3"]}
    )
    result = profiler._summarize_column_batch(table, column="value")
    summary = json.loads(result["summary_json"][0].as_py())
    assert summary["row_count"] == 6 and summary["present_count"] == 4
    assert summary["distinct_overflow"] is True
    assert summary["numeric"]["count"] == 3 and summary["decimal_seen"] is True
    assert summary["text_count"] == 1


def test_default_distinct_sketch_is_bounded_for_wide_datasets() -> None:
    assert 20 < profiler.PER_BATCH_DISTINCT_LIMIT <= 256
    values = [f"value-{index}" for index in range(profiler.PER_BATCH_DISTINCT_LIMIT + 1)]
    result = profiler._summarize_column_batch(pa.table({"value": values}), column="value")
    summary = json.loads(result["summary_json"][0].as_py())
    assert summary["distinct_overflow"] is True
    assert len(summary["distinct_hashes"]) == profiler.PER_BATCH_DISTINCT_LIMIT


def test_temporal_count_fallback_and_empty_moments(monkeypatch: pytest.MonkeyPatch) -> None:
    series = pl.Series(["2026-01-01", "bad"])
    assert profiler._temporal_value_count(series) == 1
    date_result = SimpleNamespace(is_not_null=lambda: SimpleNamespace(sum=lambda: 1))
    fallback = SimpleNamespace(
        str=SimpleNamespace(
            to_datetime=lambda **_kwargs: (_ for _ in ()).throw(pl.exceptions.ComputeError("bad")),
            to_date=lambda **_kwargs: date_result,
        )
    )
    assert profiler._temporal_value_count(fallback) == 1
    fallback.str.to_date = lambda **_kwargs: (_ for _ in ()).throw(
        pl.exceptions.ComputeError("bad")
    )
    assert profiler._temporal_value_count(fallback) == 0
    assert profiler._moments(np.asarray([np.nan, np.inf])) == profiler._empty_moments()
    moments = profiler._moments(np.asarray([1, 2, 3], dtype=float))
    assert moments["count"] == 3 and moments["min"] == 1 and moments["max"] == 3


def _summary(**updates):
    value = {
        "present_count": 2,
        "temporal_count": 0,
        "text_count": 0,
        "decimal_seen": False,
        "sample_values": ["1"],
        "numeric_sample": [1, 2],
        "top_values": [["1", 2]],
        "words": [],
        "numeric": profiler._moments(np.asarray([1, 2])),
        "length": profiler._moments(np.asarray([1, 1])),
        "distinct_overflow": False,
        "distinct_hashes": [1, 2],
    }
    value.update(updates)
    return value


def test_merge_summaries_and_moments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(profiler, "GLOBAL_DISTINCT_LIMIT", 2)
    merged = profiler._merge_summaries(
        [
            _summary(),
            _summary(distinct_hashes=[3], distinct_overflow=True, decimal_seen=True),
        ]
    )
    assert merged["summary_count"] == 2 and merged["present_count"] == 4
    assert merged["distinct_count"] == 3 and merged["decimal_seen"] is True
    assert merged["numeric"]["count"] == 4

    target = profiler._empty_moments()
    profiler._merge_moments(target, profiler._moments(np.asarray([2, 4])))
    profiler._merge_moments(target, profiler._empty_moments())
    assert target["min"] == 2 and target["max"] == 4


@pytest.mark.parametrize(
    ("summary", "distinct", "expected"),
    [
        (_summary(present_count=0), 0, ("unknown", None)),
        (_summary(), 2, ("numerical_discrete", None)),
        (_summary(decimal_seen=True), 2, ("numerical_continuous", None)),
        (_summary(numeric=profiler._empty_moments(), temporal_count=2), 2, ("temporal", None)),
        (_summary(numeric=profiler._empty_moments(), text_count=2), 2, ("text", None)),
        (_summary(numeric=profiler._empty_moments()), 2, ("categorical", None)),
    ],
)
def test_semantic_type_contracts(summary, distinct, expected) -> None:
    assert profiler._semantic_type(summary, distinct) == expected


def test_numeric_profile_constant_empty_and_outlier(monkeypatch: pytest.MonkeyPatch) -> None:
    assert profiler._numeric_profile(object(), "x", profiler._empty_moments(), mode="numeric") == (
        {},
        [],
        False,
    )
    moments = profiler._moments(np.asarray([1, 1, 1]))
    monkeypatch.setattr(
        profiler, "_distributed_histogram", lambda *_args: (np.asarray([1, 1]), np.asarray([3]))
    )
    stats, distribution, outliers = profiler._numeric_profile(
        object(), "x", moments, mode="numeric"
    )
    assert stats["variance"] == 0 and distribution == [{"label": "1", "count": 3}] and not outliers

    moments = {
        "count": 100,
        "sum": 100,
        "sum2": 100,
        "sum3": 100,
        "sum4": 100,
        "min": 0,
        "max": 100,
    }
    monkeypatch.setattr(
        profiler,
        "_distributed_histogram",
        lambda *_args: (np.asarray([0, 1, 2, 4, 100]), np.asarray([49, 50, 0, 1])),
    )
    _, _, outliers = profiler._numeric_profile(object(), "x", moments, mode="numeric")
    assert outliers is True


def test_histogram_batch_quantile_display_and_labels() -> None:
    table = pa.table({"x": ["1", "2", "bad", None]})
    numeric = profiler._histogram_batch(table, column="x", edges=[0, 1, 2, 3], mode="numeric")
    lengths = profiler._histogram_batch(table, column="x", edges=[0, 1, 2, 3, 4], mode="length")
    assert sum(json.loads(numeric["counts_json"][0].as_py())) == 2
    assert sum(json.loads(lengths["counts_json"][0].as_py())) == 3
    assert profiler._histogram_quantile(np.asarray([0, 1]), np.asarray([0]), 0.5) == 0
    assert profiler._histogram_quantile(
        np.asarray([0, 1, 2]), np.asarray([0, 2]), 0.5
    ) == pytest.approx(1.25)
    assert profiler._histogram_quantile(np.asarray([0, 1]), np.asarray([2]), 2.0) == 1
    display = profiler._display_histogram(np.arange(0, 25), np.ones(24))
    assert len(display) == 12 and sum(item["count"] for item in display) == 24
    assert profiler._unix_histogram_label("bad label", "s") == "bad label"


def test_distributed_histogram_configurations_cover_all_semantics() -> None:
    summaries = {
        "numeric": _summary(),
        "constant": _summary(numeric=profiler._moments(np.asarray([4, 4]))),
        "temporal": _summary(numeric=profiler._moments(np.asarray([1, 2]))),
        "text": _summary(length=profiler._moments(np.asarray([10, 40]))),
        "empty": _summary(numeric=profiler._empty_moments()),
        "category": _summary(),
    }
    semantics = {
        "numeric": ("numerical_continuous", None),
        "constant": ("numerical_discrete", None),
        "temporal": ("temporal", "s"),
        "text": ("text", None),
        "empty": ("numerical_continuous", None),
        "category": ("categorical", None),
    }

    configurations = profiler._histogram_configurations(summaries, semantics)

    assert set(configurations) == {"numeric", "constant", "temporal", "text"}
    assert configurations["constant"]["edges"] == [4.0, 4.0]
    assert configurations["text"]["mode"] == "length"
    assert profiler._histograms_batch(
        pa.table({"category": ["a"]}), configurations={}
    ).num_rows == 0


def test_column_profiles_from_shared_summary_pass_cover_histogram_modes() -> None:
    numeric_summary = _summary(
        present_count=3,
        distinct_count=3,
        summary_count=1,
        words=Counter(),
        top_values=Counter({"1": 1, "2": 1, "3": 1}),
        numeric=profiler._moments(np.asarray([1, 2, 3])),
    )
    numeric = profiler._column_profile_from_summary(
        "amount",
        4,
        numeric_summary,
        ("numerical_continuous", None),
        {"mode": "numeric", "edges": [1, 2, 3]},
        np.asarray([1, 2]),
    )
    assert numeric.distribution_type == "histogram" and numeric.missing_count == 1

    temporal = profiler._column_profile_from_summary(
        "created_at",
        3,
        numeric_summary,
        ("temporal", "s"),
        {"mode": "numeric", "edges": [1, 2, 3]},
        np.asarray([1, 2]),
    )
    assert temporal.semantic_type == "temporal" and temporal.statistics["min"]

    text_summary = {
        **numeric_summary,
        "length": profiler._moments(np.asarray([10, 20, 30])),
        "words": Counter({"reliable": 3}),
    }
    text = profiler._column_profile_from_summary(
        "description",
        3,
        text_summary,
        ("text", None),
        {"mode": "length", "edges": [10, 20, 30]},
        np.asarray([1, 2]),
    )
    assert text.statistics["word_frequencies"] == [{"word": "reliable", "count": 3}]

    categorical = profiler._column_profile_from_summary(
        "segment",
        0,
        {**numeric_summary, "present_count": 0, "top_values": Counter({"a": 2})},
        ("categorical", None),
        None,
        None,
    )
    assert categorical.distribution_type == "bar" and categorical.missing_ratio == 0.0


def test_shared_distributed_pass_profiles_multiple_columns_without_column_rescans() -> None:
    dataset = profiler.ray.data.from_items(
        [
            {"amount": 1.5, "constant": 4, "segment": "a"},
            {"amount": 2.5, "constant": 4, "segment": "b"},
            {"amount": 3.5, "constant": 4, "segment": "a"},
        ]
    )
    empty_milestones: list[str] = []
    assert (
        profiler._profile_columns_in_distributed_passes(
            dataset,
            [],
            3,
            progress_callback=empty_milestones.append,
        )
        == []
    )
    assert empty_milestones == ["summaries_complete", "histograms_complete"]

    milestones: list[str] = []
    completed: list[tuple[str, int, int]] = []
    profiles = profiler._profile_columns_in_distributed_passes(
        dataset,
        ["amount", "constant", "segment"],
        3,
        progress_callback=milestones.append,
        feature_callback=lambda profile, count, total, _row_count: completed.append(
            (profile.name, count, total)
        ),
    )

    assert [item.name for item in profiles] == ["amount", "constant", "segment"]
    assert profiles[0].distribution_type == "histogram"
    assert profiles[1].statistics["variance"] == 0
    assert milestones == ["summaries_complete", "histograms_complete"]
    assert completed == [
        ("amount", 1, 3),
        ("constant", 2, 3),
        ("segment", 3, 3),
    ]


def test_relationships_sampling_warnings_and_quality_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    def profile(name: str, semantic: str, distinct: int) -> SimpleNamespace:
        return SimpleNamespace(name=name, semantic_type=semantic, distinct_count=distinct)

    profiles = {
        "target": profile("target", "categorical", 2000),
        "feature": profile("feature", "text", 2000),
    }
    relationship_profiles = None

    def relationships_against_target(_rows, filtered_profiles, _target):
        nonlocal relationship_profiles
        relationship_profiles = filtered_profiles
        return ["relationship"]

    monkeypatch.setattr(
        "automl_api.services.profiling._relationships_against_target",
        relationships_against_target,
    )
    relationships, warnings = profiler._relationships_from_rows(
        [{"target": "a", "feature": "b"}], profiles, "target", 10
    )
    assert relationships == ["relationship"] and len(warnings) == 2
    assert relationship_profiles == {"target": profiles["target"]}
    assert profiler._relationships_from_rows([], profiles, None, 0) == ([], [])
    flags = profiler._quality_flags("customer_id", "numerical_continuous", 4, 10, 1, True)
    assert flags == [
        "high_missingness",
        "constant_or_near_constant",
        "possible_outliers",
        "identifier_like",
    ]
    assert profiler._stable_value_hash("same") == profiler._stable_value_hash("same")
    assert profiler._rounded(float("inf")) == 0
