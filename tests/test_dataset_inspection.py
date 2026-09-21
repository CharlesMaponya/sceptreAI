from __future__ import annotations

import pytest
from automl_api.models.enums import DatasetFormat, DatasetStatus
from automl_api.services.dataset_inspection import (
    detect_dataset_format,
    inspect_column_sample,
    inspect_tabular_bytes,
)


def test_detect_dataset_format_from_extension() -> None:
    assert detect_dataset_format("customers.csv") == DatasetFormat.CSV
    assert detect_dataset_format("customers.parquet") == DatasetFormat.PARQUET
    assert detect_dataset_format("customers.xlsx") == DatasetFormat.EXCEL
    assert detect_dataset_format("customers.json") == DatasetFormat.JSON


def test_inspect_csv_extracts_schema_and_quality() -> None:
    result = inspect_tabular_bytes(
        "customers.csv",
        b"id,name,spend,created_at\n1,Ada,12.5,2026-01-01\n2,,15.0,2026-01-02\n2,,15.0,2026-01-02\n",
    )

    assert result.format == DatasetFormat.CSV
    assert result.status == DatasetStatus.READY
    assert result.row_count == 3
    assert result.column_count == 4
    assert result.quality_report_json["missing_cells"] == 2
    assert result.quality_report_json["duplicate_rows"] == 1
    assert result.inferred_types_json["spend"]["semantic_type"] == "numerical_continuous"
    assert result.inferred_types_json["created_at"]["semantic_type"] == "temporal"
    profiles = {column["name"]: column for column in result.schema_json["columns"]}
    assert profiles["spend"]["preview_kind"] == "histogram"
    assert profiles["spend"]["preview_values"] == [12.5, 15.0, 15.0]
    assert profiles["spend"]["statistics"]["median"] == 15.0
    assert profiles["name"]["preview_kind"] == "bar"
    assert profiles["name"]["preview_distribution"] == [{"label": "Ada", "count": 1}]


def test_parquet_upload_is_deferred_without_optional_parser() -> None:
    result = inspect_tabular_bytes("customers.parquet", b"PAR1")

    assert result.format == DatasetFormat.PARQUET
    assert result.status == DatasetStatus.UPLOADED
    assert result.quality_report_json["warnings"]


def test_unix_millisecond_column_is_temporal() -> None:
    result = inspect_tabular_bytes(
        "events.csv",
        (b"event_epoch,value\n1704067200000,1\n1704153600000,2\n1704240000000,3\n"),
    )

    assert result.inferred_types_json["event_epoch"]["semantic_type"] == "temporal"


def test_target_preview_is_bounded_and_reports_sampled_class_balance() -> None:
    content = b"feature,target\n" + b"".join(
        f"{index},{'yes' if index % 4 == 0 else 'no'}\n".encode()
        for index in range(1_000)
    )

    result = inspect_column_sample(
        "training.csv", content, "target", maximum_rows=512
    )

    assert result.sampled_rows == 512
    assert result.profile["semantic_type"] == "categorical"
    assert result.profile["preview_sample_size"] == 512
    assert result.profile["preview_distribution"] == [
        {"label": "no", "count": 384},
        {"label": "yes", "count": 128},
    ]


def test_target_preview_rejects_unknown_columns_and_handles_incomplete_jsonl() -> None:
    with pytest.raises(ValueError, match="not found"):
        inspect_column_sample("training.csv", b"feature,target\n1,yes\n", "missing")

    result = inspect_column_sample(
        "training.ndjson",
        b'{"target":"yes"}\n{"target":"no"}\n{"target":',
        "target",
    )
    assert result.sampled_rows == 2
    assert result.profile["preview_distribution"] == [
        {"label": "yes", "count": 1},
        {"label": "no", "count": 1},
    ]


def test_target_preview_fails_closed_for_invalid_or_unusable_samples() -> None:
    with pytest.raises(ValueError, match="positive"):
        inspect_column_sample(
            "training.csv", b"feature,target\n1,yes\n", "target", maximum_rows=0
        )
    with pytest.raises(ValueError, match="supports CSV"):
        inspect_column_sample("training.parquet", b"PAR1", "target")
    with pytest.raises(ValueError, match="no data rows"):
        inspect_column_sample("training.csv", b"feature,target\n", "target")
    with pytest.raises(ValueError, match="not found"):
        inspect_column_sample(
            "training.jsonl",
            b'\nnot-json\n[]\n{"other":"value"}\n',
            "target",
        )


def test_target_preview_bounds_jsonl_rows_and_counts_missing_values() -> None:
    result = inspect_column_sample(
        "training.jsonl",
        b'\nnot-json\n[]\n{"other":1}\n{"target":"yes"}\n{"target":"no"}\n',
        "target",
        maximum_rows=2,
    )

    assert result.sampled_rows == 2
    assert result.profile["missing_count"] == 1
    assert result.profile["preview_distribution"] == [{"label": "yes", "count": 1}]
