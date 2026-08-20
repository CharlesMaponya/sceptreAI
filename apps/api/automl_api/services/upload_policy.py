from __future__ import annotations

import codecs
import csv
import hashlib
import io
import json
import os
import select
import shlex
import shutil
import signal
import subprocess
import tempfile
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from automl_api.core.config import Settings

MIB = 1024 * 1024
MAX_SCHEMA_PREFIX_BYTES = 16 * MIB + 2
ALLOWED_CONTENT_TYPES = {
    ".csv": {"text/csv", "text/plain", "application/csv", "application/octet-stream"},
    ".json": {"application/json", "text/json", "application/octet-stream"},
    ".jsonl": {
        "application/jsonl",
        "application/x-ndjson",
        "application/json",
        "text/plain",
        "application/octet-stream",
    },
    ".ndjson": {
        "application/x-ndjson",
        "application/jsonl",
        "application/json",
        "text/plain",
        "application/octet-stream",
    },
    ".parquet": {"application/vnd.apache.parquet", "application/octet-stream"},
    ".xlsx": {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/zip",
        "application/octet-stream",
    },
    ".xls": {"application/vnd.ms-excel", "application/octet-stream"},
}
TEXT_EXTENSIONS = {".csv", ".json", ".jsonl", ".ndjson"}
DENIED_SIGNATURES = {
    b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!": "eicar",
    b"\x7fELF": "elf_executable",
    b"MZ": "windows_executable",
}


@dataclass(frozen=True)
class ContentInspection:
    byte_size: int
    sha256: str
    scanner_status: str
    scanner_name: str
    scanner_version: str
    signature_version: str
    encoding: str | None
    maximum_row_width: int | None
    schema_columns: tuple[str, ...]
    evidence: dict[str, object]


def validate_upload_manifest(
    *, filename: str, content_type: str, byte_size: int, settings: Settings
) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_CONTENT_TYPES:
        raise ValueError(
            "Unsupported upload extension. Expected CSV, JSON/JSONL, Parquet, XLS, or XLSX."
        )
    normalized_type = content_type.partition(";")[0].strip().lower()
    if normalized_type not in ALLOWED_CONTENT_TYPES[suffix]:
        raise ValueError(
            f"Content type '{normalized_type}' is not allowed for the '{suffix}' extension."
        )
    if byte_size > settings.max_upload_size_bytes:
        raise ValueError(
            f"Object size {byte_size} exceeds the configured maximum "
            f"{settings.max_upload_size_bytes}."
        )
    if suffix in {".xlsx", ".xls"} and byte_size > settings.buffered_upload_max_bytes:
        raise ValueError(
            "Compressed spreadsheet uploads above 100 MiB are rejected to bound "
            "decompression and parser risk; convert the file to CSV or Parquet."
        )
    return suffix


def validate_scanner_configuration(settings: Settings) -> None:
    configured = (settings.upload_scanner_command or "").strip()
    if settings.environment.lower() in {"staging", "production"} and not configured:
        raise ValueError(
            "UPLOAD_SCANNER_COMMAND is mandatory in staging and production; "
            "a no-op scanner is not permitted."
        )
    if not configured:
        return
    if settings.upload_scanner_version.strip().lower() in {"", "builtin-v1", "unconfigured"}:
        raise ValueError("UPLOAD_SCANNER_VERSION must identify the external scanner release.")
    if settings.upload_scanner_signature_version.strip().lower() in {
        "",
        "builtin-content-policy-v1",
        "unconfigured",
    }:
        raise ValueError(
            "UPLOAD_SCANNER_SIGNATURE_VERSION must identify the active signature database."
        )
    try:
        command = shlex.split(configured)
    except ValueError as exc:
        raise ValueError("UPLOAD_SCANNER_COMMAND is not valid shell-style syntax.") from exc
    if not command or shutil.which(command[0]) is None:
        raise ValueError("UPLOAD_SCANNER_COMMAND executable is not available in the runtime image.")


def configured_upload_data_region(settings: Settings) -> str:
    configured = (getattr(settings, "object_store_region", None) or "").strip()
    if configured:
        return configured
    if settings.environment.lower() in {"staging", "production"}:
        raise ValueError(
            "OBJECT_STORE_REGION is mandatory in staging and production so upload "
            "residency is resolved before issuing provider instructions."
        )
    return "local"


def validate_upload_data_region(requested: str, settings: Settings) -> str:
    configured = configured_upload_data_region(settings)
    if getattr(settings, "object_store_region", None) and requested != configured:
        raise ValueError(
            f"Upload data region '{requested}' does not match the configured storage "
            f"region '{configured}'."
        )
    return requested


def inspect_and_scan_stream(
    source: BinaryIO,
    *,
    filename: str,
    expected_size: int,
    settings: Settings,
    on_progress: Callable[[int], None] | None = None,
) -> ContentInspection:
    validate_scanner_configuration(settings)
    suffix = Path(filename).suffix.lower()
    command = shlex.split(settings.upload_scanner_command or "")
    process = _start_scanner(command) if command else None
    scanner_deadline = (
        time.monotonic() + settings.upload_scanner_timeout_seconds if process else None
    )
    digest = hashlib.sha256()
    total = 0
    sample = bytearray()
    schema_prefix = bytearray()
    json_line_carry = b""
    json_line_columns: set[str] = set()
    row_width = 0
    maximum_row_width = 0
    carry = b""
    denied: str | None = None
    text_decoder = codecs.getincrementaldecoder("utf-8")() if suffix in TEXT_EXTENSIONS else None
    archive = tempfile.SpooledTemporaryFile(max_size=8 * MIB) if suffix == ".xlsx" else None
    try:
        while chunk := source.read(MIB):
            total += len(chunk)
            if total > expected_size:
                _terminate(process)
                raise ValueError("The object stream exceeds its declared byte size.")
            digest.update(chunk)
            if text_decoder is not None:
                try:
                    text_decoder.decode(chunk, final=False)
                except UnicodeDecodeError as exc:
                    _terminate(process)
                    raise ValueError("Text uploads must use valid UTF-8 encoding.") from exc
            if archive is not None:
                archive.write(chunk)
            if len(sample) < 64 * 1024:
                sample.extend(chunk[: 64 * 1024 - len(sample)])
            if len(schema_prefix) < MAX_SCHEMA_PREFIX_BYTES:
                schema_prefix.extend(chunk[: MAX_SCHEMA_PREFIX_BYTES - len(schema_prefix)])
            combined = carry + chunk
            if denied is None:
                for signature, label in DENIED_SIGNATURES.items():
                    if signature in combined:
                        denied = label
                        break
            carry = combined[-128:]
            if suffix in TEXT_EXTENSIONS:
                pieces = chunk.split(b"\n")
                if len(pieces) == 1:
                    row_width += len(chunk)
                else:
                    maximum_row_width = max(maximum_row_width, row_width + len(pieces[0]))
                    if len(pieces) > 2:
                        maximum_row_width = max(
                            maximum_row_width, max(len(value) for value in pieces[1:-1])
                        )
                    row_width = len(pieces[-1])
                if maximum_row_width > 16 * MIB or row_width > 16 * MIB:
                    _terminate(process)
                    raise ValueError("A row exceeds the 16 MiB ingestion width limit.")
            if suffix in {".jsonl", ".ndjson"}:
                json_lines = (json_line_carry + chunk).split(b"\n")
                json_line_carry = json_lines.pop()
                for line in json_lines:
                    _merge_json_line_columns(json_line_columns, line)
            if process is not None and process.stdin is not None:
                _write_scanner(process, chunk, deadline=scanner_deadline)
            if on_progress is not None:
                on_progress(total)
        if archive is not None:
            _validate_magic(bytes(sample), suffix)
            _validate_xlsx_archive(archive, settings)
        if text_decoder is not None:
            try:
                text_decoder.decode(b"", final=True)
            except UnicodeDecodeError as exc:
                _terminate(process)
                raise ValueError("Text uploads must use valid UTF-8 encoding.") from exc
    except Exception:  # noqa: BLE001 - terminate the operator scanner before re-raising.
        _terminate(process)
        raise
    finally:
        if archive is not None:
            archive.close()
    maximum_row_width = max(maximum_row_width, row_width) if suffix in TEXT_EXTENSIONS else 0
    if total != expected_size:
        _terminate(process)
        raise ValueError(
            f"The object stream size differs from its manifest: {total} != {expected_size}."
        )
    external = (
        _finish_scanner(process, settings, deadline=scanner_deadline)
        if process is not None
        else None
    )
    if denied:
        raise ValueError(f"Content policy denied the object signature '{denied}'.")
    if external is not None and external[0] != 0:
        raise ValueError("The configured malware scanner denied the uploaded object.")
    encoding = _validate_encoding(bytes(sample), suffix)
    _validate_magic(bytes(sample), suffix)
    if suffix in {".jsonl", ".ndjson"} and json_line_carry.strip():
        _merge_json_line_columns(json_line_columns, json_line_carry)
    schema_columns = _extract_schema_columns(
        bytes(schema_prefix),
        suffix=suffix,
        complete_document=total <= len(schema_prefix),
        json_line_columns=json_line_columns,
    )
    scanner_name = command[0] if command else "sceptre-builtin-content-policy"
    scanner_version = settings.upload_scanner_version if command else "builtin-v1"
    scanner_output = external[1] if external else "builtin signature policy passed"
    return ContentInspection(
        byte_size=total,
        sha256=digest.hexdigest(),
        scanner_status="allowed",
        scanner_name=scanner_name,
        scanner_version=scanner_version,
        signature_version=settings.upload_scanner_signature_version,
        encoding=encoding,
        maximum_row_width=maximum_row_width or None,
        schema_columns=schema_columns,
        evidence={
            "policy_revision": "phase2-content-policy-v1",
            "scanner_output_sha256": hashlib.sha256(scanner_output.encode()).hexdigest(),
            "sample_bytes": len(sample),
            "schema_column_count": len(schema_columns),
        },
    )


def _extract_schema_columns(
    prefix: bytes,
    *,
    suffix: str,
    complete_document: bool,
    json_line_columns: set[str],
) -> tuple[str, ...]:
    if suffix == ".csv":
        # The bounded prefix may end midway through a later multibyte value. The
        # complete stream's encoding is validated independently; only the first
        # logical CSV record is consumed here.
        text = prefix.decode("utf-8-sig", errors="ignore")
        sample = text[: 64 * 1024]
        try:
            dialect = (
                csv.Sniffer().sniff(sample, delimiters=",;\t|")
                if sample.strip()
                else csv.excel
            )
        except csv.Error:
            dialect = csv.excel
        try:
            header = next(csv.reader(io.StringIO(text, newline=""), dialect=dialect, strict=True))
        except (StopIteration, csv.Error) as exc:
            raise ValueError(
                "CSV upload does not contain a complete, valid header record."
            ) from exc
        return _validated_column_names(header)
    if suffix in {".jsonl", ".ndjson"}:
        return tuple(sorted(json_line_columns))
    if suffix == ".json" and complete_document:
        try:
            loaded = json.loads(prefix.decode("utf-8-sig"))
        except json.JSONDecodeError as exc:
            raise ValueError("JSON upload is not a valid UTF-8 JSON document.") from exc
        rows = (
            loaded
            if isinstance(loaded, list)
            else loaded.get("data", [loaded])
            if isinstance(loaded, dict)
            else []
        )
        names = sorted({str(key) for row in rows if isinstance(row, dict) for key in row})
        return _validated_column_names(names) if names else ()
    return ()


def _json_line_columns(line: bytes) -> set[str]:
    if not line.strip():
        return set()
    try:
        value = json.loads(line.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("JSONL upload contains an invalid JSON record.") from exc
    return {str(key) for key in value} if isinstance(value, dict) else set()


def _merge_json_line_columns(columns: set[str], line: bytes) -> None:
    columns.update(_json_line_columns(line))
    if len(columns) > 10_000:
        raise ValueError("Tabular upload exceeds the 10,000-column safety limit.")


def _validated_column_names(values: list[str]) -> tuple[str, ...]:
    columns = tuple(str(value) for value in values)
    if not columns or any(not value.strip() for value in columns):
        raise ValueError("Tabular upload column names must be non-empty.")
    if len(columns) != len(set(columns)):
        raise ValueError("Tabular upload column names must be unique.")
    if len(columns) > 10_000:
        raise ValueError("Tabular upload exceeds the 10,000-column safety limit.")
    return columns


def _validate_encoding(sample: bytes, suffix: str) -> str | None:
    if suffix not in TEXT_EXTENSIONS:
        return None
    try:
        sample.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Text uploads must use valid UTF-8 encoding.") from exc
    return "utf-8"


def _validate_magic(sample: bytes, suffix: str) -> None:
    if suffix == ".parquet" and not sample.startswith(b"PAR1"):
        raise ValueError("Parquet extension does not match the object magic bytes.")
    if suffix == ".xlsx" and not sample.startswith(b"PK"):
        raise ValueError("XLSX extension does not match a ZIP container.")
    if suffix == ".xls" and not sample.startswith(bytes.fromhex("D0CF11E0A1B11AE1")):
        raise ValueError("XLS extension does not match an OLE compound document.")


def _validate_xlsx_archive(source: BinaryIO, settings: Settings) -> None:
    source.seek(0)
    try:
        with zipfile.ZipFile(source) as archive:
            members = archive.infolist()
            if len(members) > 10_000:
                raise ValueError("XLSX archive contains too many members.")
            compressed = 0
            expanded = 0
            for member in members:
                normalized = Path(member.filename)
                if normalized.is_absolute() or ".." in normalized.parts:
                    raise ValueError("XLSX archive contains an unsafe member path.")
                compressed += max(1, member.compress_size)
                expanded += member.file_size
            if expanded > settings.upload_max_decompressed_bytes:
                raise ValueError("XLSX decompressed bytes exceed the configured safety limit.")
            if expanded > compressed * settings.upload_max_decompression_ratio:
                raise ValueError("XLSX decompression ratio exceeds the configured safety limit.")
    except zipfile.BadZipFile as exc:
        raise ValueError("XLSX upload is not a valid ZIP container.") from exc


def _start_scanner(command: list[str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603 - command is operator-configured, never shell-expanded.
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        shell=False,
        close_fds=True,
        start_new_session=True,
        env={
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        },
    )


def _finish_scanner(
    process: subprocess.Popen[bytes],
    settings: Settings,
    *,
    deadline: float | None = None,
) -> tuple[int, str]:
    if process.stdin is not None:
        process.stdin.close()
    timeout = settings.upload_scanner_timeout_seconds
    if deadline is not None:
        timeout = max(0.0, deadline - time.monotonic())
    try:
        return_code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _kill_scanner(process)
        process.wait()
        raise TimeoutError("The configured malware scanner timed out; upload quarantined.") from exc
    return return_code, f"external scanner exited with status {return_code}"


def _write_scanner(
    process: subprocess.Popen[bytes],
    chunk: bytes,
    *,
    deadline: float | None,
) -> None:
    if process.stdin is None:
        raise ValueError("The configured malware scanner has no input pipe.")
    descriptor = process.stdin.fileno()
    os.set_blocking(descriptor, False)
    remaining = memoryview(chunk)
    while remaining:
        if process.poll() is not None:
            raise ValueError("The configured malware scanner closed its input early.")
        timeout = None if deadline is None else deadline - time.monotonic()
        if timeout is not None and timeout <= 0:
            _terminate(process)
            raise TimeoutError("The configured malware scanner timed out; upload quarantined.")
        _, writable, _ = select.select([], [descriptor], [], timeout)
        if not writable:
            _terminate(process)
            raise TimeoutError("The configured malware scanner timed out; upload quarantined.")
        try:
            written = os.write(descriptor, remaining)
        except BlockingIOError:
            continue
        remaining = remaining[written:]


def _terminate(process: subprocess.Popen[bytes] | None) -> None:
    if process is not None and process.poll() is None:
        _kill_scanner(process)
        process.wait()


def _kill_scanner(process: subprocess.Popen[bytes]) -> None:
    if not isinstance(process.pid, int) or process.pid <= 0:
        process.kill()
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (PermissionError, ProcessLookupError, TypeError):
        process.kill()
