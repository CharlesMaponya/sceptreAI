from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Literal, Protocol, runtime_checkable

UploadProtocol = Literal["multipart", "resumable_offset", "block_list", "single_put"]


class DeletionPending(RuntimeError):
    """Provider-retained data remains; recheck without consuming failure retries."""

    retry_after_seconds = 3600


class UploadContractError(ValueError):
    """A provider-neutral upload contract was violated."""


class UploadCapabilityUnavailable(UploadContractError):
    """The selected provider explicitly does not implement an optional capability."""


def immutable_object_version(version: str) -> str:
    if (
        not isinstance(version, str)
        or not version.strip()
        or version.strip().lower() == "null"
        or any(ord(c) < 32 or ord(c) == 127 for c in version)
    ):
        raise UploadContractError("An immutable object version is required.")
    return version


def embedded_object_version(version: str) -> str:
    if not isinstance(version, str) or not re.fullmatch(r"embedded-v1:[0-9a-f]{32}", version):
        raise UploadContractError("An immutable original embedded incarnation is required.")
    return version


class UploadExpired(UploadContractError):
    """The provider upload or transfer instruction is no longer valid."""


class UploadThrottled(RuntimeError):
    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True)
class UploadCapabilities:
    protocol: UploadProtocol
    parallel_chunks_per_object: int
    can_list_committed_units: bool
    supports_unit_checksum: bool
    supports_final_checksum: bool
    supports_provider_lifecycle: bool
    supports_abort: bool = True


@dataclass(frozen=True)
class BeginUpload:
    object_key: str
    byte_size: int
    content_type: str
    checksum_sha256: str
    transfer_unit_size: int
    expires_at: datetime
    origin: str


@dataclass(frozen=True)
class ProviderUpload:
    provider_id: str
    object_key: str
    protocol: UploadProtocol
    byte_size: int
    expires_at: datetime
    state: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TransferCursor:
    unit_number: int
    offset: int
    length: int
    checksum_sha256: str | None = None


@dataclass(frozen=True)
class UploadInstruction:
    instruction_id: str
    method: Literal["PUT", "POST"]
    url: str
    headers: dict[str, str]
    cursor: TransferCursor
    expires_at: datetime
    expose_response_headers: tuple[str, ...]
    enforced_controls: tuple[str, ...]
    unsupported_controls: tuple[str, ...] = ()


@dataclass(frozen=True)
class TransferReceipt:
    unit_number: int
    offset: int
    length: int
    etag: str | None = None
    checksum_sha256: str | None = None
    provider_headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class UploadProgress:
    confirmed_bytes: int
    total_bytes: int
    receipts: tuple[TransferReceipt, ...]
    complete: bool


@dataclass(frozen=True)
class CompletedObject:
    uri: str
    byte_size: int
    provider_checksum: str | None
    provider_headers: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ByteRange:
    start: int
    end_inclusive: int

    def __post_init__(self) -> None:
        if self.start < 0 or self.end_inclusive < self.start:
            raise ValueError("A byte range must be ordered and non-negative.")


@dataclass(frozen=True)
class ObjectMetadata:
    uri: str
    byte_size: int
    etag: str | None = None
    checksum: str | None = None
    content_type: str | None = None
    provider_headers: dict[str, str] = field(default_factory=dict)
    storage_path: Path | None = None


@dataclass(frozen=True)
class RayDataSourceDescriptor:
    path: str
    filesystem_options: dict[str, object]
    provider: str

    def __iter__(self) -> Iterator[object]:
        # Compatibility with the historical ``(path, filesystem_options)`` facade.
        yield self.path
        yield self.filesystem_options


@dataclass(frozen=True)
class HealthcheckResult:
    healthy: bool
    driver: str
    detail: str | None = None


@runtime_checkable
class UploadDriver(Protocol):
    driver_name: str

    def capabilities(self) -> UploadCapabilities: ...

    def begin_upload(self, request: BeginUpload) -> ProviderUpload: ...

    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction: ...

    def query_progress(self, upload: ProviderUpload) -> UploadProgress: ...

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject: ...

    def abort_upload(self, upload: ProviderUpload) -> None: ...


@runtime_checkable
class ObjectStoreDriver(UploadDriver, Protocol):
    def uri_for_key(self, object_key: str) -> str: ...

    def stat(self, uri: str, *, version: str | None = None) -> ObjectMetadata: ...

    def open_stream(
        self, uri: str, byte_range: ByteRange | None = None, *, version: str | None = None
    ) -> BinaryIO: ...

    def put_stream(self, uri: str, source: BinaryIO) -> ObjectMetadata: ...

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata: ...

    def create_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        """Create exclusively; return the original write identity without reading latest."""
        ...

    def read_bytes(self, uri: str) -> bytes: ...

    def read_head(self, uri: str, byte_count: int) -> bytes: ...

    def exists(self, uri: str) -> bool: ...

    def size(self, uri: str) -> int: ...

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor: ...

    def healthcheck(self) -> HealthcheckResult: ...

    def delete(self, uri: str, *, version: str | None = None) -> None: ...

    def delete_prefix(self, prefix: str) -> None: ...

    def prefix_absent(self, prefix: str) -> bool: ...
