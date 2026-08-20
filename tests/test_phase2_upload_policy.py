from __future__ import annotations

import hashlib
import io
import zipfile
from unittest.mock import MagicMock

import pytest
from automl_api.core.config import Settings
from automl_api.core.redaction import redact_mapping, redact_text, redact_url
from automl_api.services import upload_policy
from automl_api.services.upload_policy import (
    configured_upload_data_region,
    inspect_and_scan_stream,
    validate_scanner_configuration,
    validate_upload_data_region,
    validate_upload_manifest,
)


def _settings(**kwargs: object) -> Settings:
    values = {
        "max_upload_size_bytes": 200 * 1024 * 1024,
        "buffered_upload_max_bytes": 100 * 1024 * 1024,
        "upload_max_decompression_ratio": 100,
        "upload_max_decompressed_bytes": 1024 * 1024,
        "upload_scanner_version": "test-scanner-1.0.0",
        "upload_scanner_signature_version": "signatures-2026-08-20",
    }
    values.update(kwargs)
    return Settings(**values)


@pytest.mark.parametrize(
    ("filename", "content_type"),
    [
        ("data.csv", "text/csv; charset=utf-8"),
        ("data.json", "application/json"),
        ("data.jsonl", "application/x-ndjson"),
        ("data.ndjson", "application/jsonl"),
        ("data.parquet", "application/vnd.apache.parquet"),
        ("data.xlsx", "application/zip"),
        ("data.xls", "application/vnd.ms-excel"),
    ],
)
def test_manifest_allowlist(filename: str, content_type: str) -> None:
    assert validate_upload_manifest(
        filename=filename, content_type=content_type, byte_size=100, settings=_settings()
    ) == "." + filename.rsplit(".", 1)[-1]


def test_manifest_rejects_extension_type_size_and_spreadsheet_size() -> None:
    with pytest.raises(ValueError, match="Unsupported upload extension"):
        validate_upload_manifest(
            filename="payload.exe", content_type="application/octet-stream", byte_size=1,
            settings=_settings(),
        )
    with pytest.raises(ValueError, match="not allowed"):
        validate_upload_manifest(
            filename="data.csv", content_type="image/png", byte_size=1, settings=_settings()
        )
    with pytest.raises(ValueError, match="configured maximum"):
        validate_upload_manifest(
            filename="data.csv", content_type="text/csv", byte_size=201 * 1024 * 1024,
            settings=_settings(),
        )
    with pytest.raises(ValueError, match="spreadsheet"):
        validate_upload_manifest(
            filename="data.xlsx", content_type="application/zip", byte_size=101 * 1024 * 1024,
            settings=_settings(),
        )


def test_builtin_stream_inspection_hash_encoding_width_and_signatures() -> None:
    content = b"name,value\nalice,1\nbob,2\n"
    progress: list[int] = []
    result = inspect_and_scan_stream(
        io.BytesIO(content),
        filename="data.csv",
        expected_size=len(content),
        settings=_settings(),
        on_progress=progress.append,
    )
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.encoding == "utf-8" and result.maximum_row_width == 10
    assert result.scanner_status == "allowed"
    assert result.schema_columns == ("name", "value")
    assert result.evidence["schema_column_count"] == 2
    assert progress == [len(content)]

    for value, message in (
        (b"\xff\xfe", "valid UTF-8"),
        (b"MZ executable", "windows_executable"),
        (b"\x7fELF executable", "elf_executable"),
    ):
        with pytest.raises(ValueError, match=message):
            inspect_and_scan_stream(
                io.BytesIO(value), filename="data.csv", expected_size=len(value),
                settings=_settings(),
            )
    invalid_after_sample = b"a,b\n" + (b"1,2\n" * 20_000) + b"\xff"
    with pytest.raises(ValueError, match="valid UTF-8"):
        inspect_and_scan_stream(
            io.BytesIO(invalid_after_sample),
            filename="data.csv",
            expected_size=len(invalid_after_sample),
            settings=_settings(),
        )
    too_wide = b"x" * (16 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="row exceeds"):
        inspect_and_scan_stream(
            io.BytesIO(too_wide), filename="data.csv", expected_size=len(too_wide),
            settings=_settings(),
        )
    with pytest.raises(ValueError, match="declared byte size"):
        inspect_and_scan_stream(
            io.BytesIO(b"long"), filename="data.csv", expected_size=2, settings=_settings()
        )
    with pytest.raises(ValueError, match="differs from its manifest"):
        inspect_and_scan_stream(
            io.BytesIO(b"x"), filename="data.csv", expected_size=2, settings=_settings()
        )


def test_stream_inspection_extracts_bounded_text_schemas() -> None:
    semicolon = b'"account;id";amount\n1;2\n'
    csv_result = inspect_and_scan_stream(
        io.BytesIO(semicolon),
        filename="records.csv",
        expected_size=len(semicolon),
        settings=_settings(),
    )
    assert csv_result.schema_columns == ("account;id", "amount")

    jsonl = b'{"account": 1}\n{"amount": 2, "segment": "a"}\n'
    jsonl_result = inspect_and_scan_stream(
        io.BytesIO(jsonl),
        filename="records.jsonl",
        expected_size=len(jsonl),
        settings=_settings(),
    )
    assert jsonl_result.schema_columns == ("account", "amount", "segment")

    document = b'{"data": [{"account": 1}, {"amount": 2}]}'
    json_result = inspect_and_scan_stream(
        io.BytesIO(document),
        filename="records.json",
        expected_size=len(document),
        settings=_settings(),
    )
    assert json_result.schema_columns == ("account", "amount")


@pytest.mark.parametrize(
    ("filename", "content", "message"),
    [
        ("records.csv", b"a,a\n1,2\n", "unique"),
        ("records.csv", b"a,\n1,2\n", "non-empty"),
        ("records.jsonl", b'{"a": 1}\nnot-json\n', "invalid JSON record"),
        ("records.json", b'{"a":', "valid UTF-8 JSON"),
    ],
)
def test_stream_schema_extraction_rejects_ambiguous_text_contracts(
    filename: str, content: bytes, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        inspect_and_scan_stream(
            io.BytesIO(content),
            filename=filename,
            expected_size=len(content),
            settings=_settings(),
        )


def test_jsonl_schema_cardinality_is_bounded_while_streaming() -> None:
    content = b"\n".join(
        f'{{"column_{index}": {index}}}'.encode() for index in range(10_001)
    )
    with pytest.raises(ValueError, match="10,000-column"):
        inspect_and_scan_stream(
            io.BytesIO(content),
            filename="wide.jsonl",
            expected_size=len(content),
            settings=_settings(),
        )


@pytest.mark.parametrize(
    ("filename", "content", "message"),
    [
        ("data.parquet", b"not-parquet", "Parquet extension"),
        ("data.xlsx", b"not-zip", "XLSX extension"),
        ("data.xls", b"not-ole", "XLS extension"),
    ],
)
def test_magic_bytes_are_enforced(filename: str, content: bytes, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        inspect_and_scan_stream(
            io.BytesIO(content), filename=filename, expected_size=len(content), settings=_settings()
        )


def _xlsx(entries: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w", compression=compression) as archive:
        for name, value in entries.items():
            archive.writestr(name, value)
    return target.getvalue()


def test_xlsx_archive_safety_and_external_scanner() -> None:
    valid = _xlsx({"xl/workbook.xml": b"<workbook />"})
    result = inspect_and_scan_stream(
        io.BytesIO(valid), filename="data.xlsx", expected_size=len(valid),
            settings=_settings(upload_scanner_command="/usr/bin/sha256sum"),
    )
    assert result.scanner_name == "/usr/bin/sha256sum"

    unsafe = _xlsx({"../escape.xml": b"bad"})
    with pytest.raises(ValueError, match="unsafe member path"):
        inspect_and_scan_stream(
            io.BytesIO(unsafe), filename="data.xlsx", expected_size=len(unsafe),
            settings=_settings(),
        )
    bomb = _xlsx({"xl/huge.xml": b"0" * 50_000})
    with pytest.raises(ValueError, match="decompression ratio"):
        inspect_and_scan_stream(
            io.BytesIO(bomb), filename="data.xlsx", expected_size=len(bomb),
            settings=_settings(upload_max_decompression_ratio=2),
        )
    with pytest.raises(ValueError, match="valid ZIP"):
        inspect_and_scan_stream(
            io.BytesIO(b"PK-invalid"), filename="data.xlsx", expected_size=10,
            settings=_settings(),
        )
    with pytest.raises(ValueError, match="scanner denied"):
        inspect_and_scan_stream(
            io.BytesIO(b"a,b\n1,2\n"), filename="data.csv", expected_size=8,
            settings=_settings(upload_scanner_command="/bin/sh -c 'cat >/dev/null; exit 1'"),
        )


def test_scanner_is_mandatory_outside_local_and_redaction_is_recursive() -> None:
    with pytest.raises(ValueError, match="mandatory"):
        validate_scanner_configuration(_settings(environment="production"))
    validate_scanner_configuration(
        _settings(environment="production", upload_scanner_command="/usr/bin/sha256sum")
    )
    with pytest.raises(ValueError, match="not available"):
        validate_scanner_configuration(
            _settings(environment="production", upload_scanner_command="/missing/scanner")
        )
    with pytest.raises(ValueError, match="shell-style syntax"):
        validate_scanner_configuration(
            _settings(environment="production", upload_scanner_command="'unterminated")
        )
    with pytest.raises(ValueError, match="SCANNER_VERSION"):
        validate_scanner_configuration(
            _settings(
                environment="production",
                upload_scanner_command="/usr/bin/sha256sum",
                upload_scanner_version="unconfigured",
            )
        )
    with pytest.raises(ValueError, match="SIGNATURE_VERSION"):
        validate_scanner_configuration(
            _settings(
                environment="production",
                upload_scanner_command="/usr/bin/sha256sum",
                upload_scanner_signature_version="unconfigured",
            )
        )
    signed = "https://storage.test/key?X-Amz-Credential=a&X-Amz-Signature=b"
    assert redact_url(signed) == "https://storage.test/key?<redacted>"
    assert "Signature=<redacted>" in redact_text(f"failed {signed} Signature=secret")
    redacted = redact_mapping({
        "authorization": "Bearer secret",
        "nested": {"provider_id": "private", "message": signed},
        "events": [{"token": "private"}, signed, 3],
        "safe": True,
    })
    assert redacted["authorization"] == "<redacted>"
    assert redacted["nested"]["provider_id"] == "<redacted>"
    assert redacted["events"][0]["token"] == "<redacted>"
    assert redacted["events"][1].endswith("?<redacted>")
    assert redacted["safe"] is True


def test_scanner_process_is_secret_free_and_timeout_is_terminal(monkeypatch) -> None:
    popen = MagicMock()
    process = MagicMock()
    popen.return_value = process
    monkeypatch.setattr(upload_policy.subprocess, "Popen", popen)
    assert upload_policy._start_scanner(["/usr/bin/sha256sum"]) is process
    environment = popen.call_args.kwargs["env"]
    assert set(environment) == {"LANG", "PATH"}
    assert popen.call_args.kwargs["shell"] is False
    assert popen.call_args.kwargs["start_new_session"] is True

    process.stdin = MagicMock()
    process.wait.side_effect = [upload_policy.subprocess.TimeoutExpired("scanner", 1), 0]
    with pytest.raises(TimeoutError, match="timed out"):
        upload_policy._finish_scanner(process, _settings(upload_scanner_timeout_seconds=1))
    process.stdin.close.assert_called_once()
    process.kill.assert_called_once()


def test_scanner_timeout_includes_blocked_input_writes() -> None:
    content = b"a" * (2 * 1024 * 1024)
    with pytest.raises(TimeoutError, match="timed out"):
        inspect_and_scan_stream(
            io.BytesIO(content),
            filename="data.csv",
            expected_size=len(content),
            settings=_settings(
                upload_scanner_command="/bin/sh -c 'sleep 30'",
                upload_scanner_timeout_seconds=1,
            ),
        )


def test_upload_region_is_resolved_and_enforced_before_instructions() -> None:
    assert configured_upload_data_region(_settings()) == "local"
    governed = _settings(object_store_region="af-south-1")
    assert configured_upload_data_region(governed) == "af-south-1"
    assert validate_upload_data_region("af-south-1", governed) == "af-south-1"
    with pytest.raises(ValueError, match="does not match"):
        validate_upload_data_region("eu-west-1", governed)
    with pytest.raises(ValueError, match="mandatory"):
        configured_upload_data_region(
            _settings(
                environment="production",
                upload_scanner_command="/usr/bin/sha256sum",
            )
        )
