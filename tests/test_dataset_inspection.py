from __future__ import annotations

from automl_api.models.enums import DatasetFormat, DatasetStatus
from automl_api.services.dataset_inspection import detect_dataset_format, inspect_tabular_bytes


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
