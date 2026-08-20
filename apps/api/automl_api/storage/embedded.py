from __future__ import annotations

import io
import uuid
from pathlib import Path

from automl_api.storage.contracts import (
    BeginUpload,
    ByteRange,
    CompletedObject,
    HealthcheckResult,
    ObjectMetadata,
    ProviderUpload,
    RayDataSourceDescriptor,
    TransferCursor,
    TransferReceipt,
    UploadCapabilities,
    UploadCapabilityUnavailable,
    UploadInstruction,
    UploadProgress,
)


class EmbeddedObjectStoreDriver:
    driver_name = "embedded"

    def __init__(self, *, root: Path, bucket: str) -> None:
        self.root = root
        self.bucket = bucket

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities(
            protocol="single_put",
            parallel_chunks_per_object=1,
            can_list_committed_units=False,
            supports_unit_checksum=False,
            supports_final_checksum=False,
            supports_provider_lifecycle=False,
        )

    def begin_upload(self, request: BeginUpload) -> ProviderUpload:
        return ProviderUpload(
            provider_id=uuid.uuid4().hex,
            object_key=self._key(request.object_key),
            protocol="single_put",
            byte_size=request.byte_size,
            expires_at=request.expires_at,
        )

    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction:
        raise UploadCapabilityUnavailable(
            "Embedded storage cannot issue a browser-transfer credential."
        )

    def query_progress(self, upload: ProviderUpload) -> UploadProgress:
        uri = self._uri(upload.object_key)
        confirmed = self.size(uri) if self.exists(uri) else 0
        return UploadProgress(
            confirmed_bytes=confirmed,
            total_bytes=upload.byte_size,
            receipts=(),
            complete=confirmed == upload.byte_size,
        )

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject:
        if receipts:
            raise ValueError("Embedded single-put completion does not accept unit receipts.")
        metadata = self.stat(self._uri(upload.object_key))
        if metadata.byte_size != upload.byte_size:
            raise ValueError("Embedded object size does not match the upload manifest.")
        return CompletedObject(metadata.uri, metadata.byte_size, metadata.checksum)

    def abort_upload(self, upload: ProviderUpload) -> None:
        self.delete(self._uri(upload.object_key))

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str) -> ObjectMetadata:
        path = self._path_from_uri(uri)
        return ObjectMetadata(
            uri=self._canonical_uri(path), byte_size=path.stat().st_size, storage_path=path
        )

    def open_stream(self, uri: str, byte_range: ByteRange | None = None):
        path = self._path_from_uri(uri)
        if byte_range is None:
            return path.open("rb")
        with path.open("rb") as source:
            source.seek(byte_range.start)
            value = source.read(byte_range.end_inclusive - byte_range.start + 1)
        return io.BytesIO(value)

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        path = self._path_from_uri(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as destination:
            while chunk := source.read(1024 * 1024):
                destination.write(chunk)
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        path = self._path(self._key(key))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        return self.stat(self._uri(key))

    def read_bytes(self, uri: str) -> bytes:
        return self._path_from_uri(uri).read_bytes()

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        with self._path_from_uri(uri).open("rb") as source:
            return source.read(byte_count)

    def exists(self, uri: str) -> bool:
        try:
            return self._path_from_uri(uri).is_file()
        except ValueError:
            return False

    def size(self, uri: str) -> int:
        return self._path_from_uri(uri).stat().st_size

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor:
        return RayDataSourceDescriptor(
            path=str(self._path_from_uri(uri).resolve()),
            filesystem_options={},
            provider=self.driver_name,
        )

    def healthcheck(self) -> HealthcheckResult:
        (self.root / self.bucket).mkdir(parents=True, exist_ok=True)
        return HealthcheckResult(healthy=True, driver=self.driver_name)

    def delete(self, uri: str) -> None:
        self._path_from_uri(uri).unlink(missing_ok=True)

    def _key(self, value: str) -> str:
        key = value.strip("/")
        if not key or key.startswith("../") or "/../" in key:
            raise ValueError("Object keys must remain inside the configured bucket.")
        return key

    def _path(self, key: str) -> Path:
        return self.root / self.bucket / self._key(key)

    def _uri(self, key: str) -> str:
        return f"embedded://{self.bucket}/{self._key(key)}"

    def _path_from_uri(self, uri: str) -> Path:
        canonical = f"embedded://{self.bucket}/"
        legacy = f"minio://{self.bucket}/"
        if uri.startswith(canonical):
            key = uri.removeprefix(canonical)
        elif uri.startswith(legacy):
            key = uri.removeprefix(legacy)
        else:
            raise ValueError("Object URI does not belong to the configured embedded store.")
        return self._path(key)

    def _canonical_uri(self, path: Path) -> str:
        key = path.relative_to(self.root / self.bucket).as_posix()
        return self._uri(key)
