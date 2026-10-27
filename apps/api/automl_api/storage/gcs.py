from __future__ import annotations

import hashlib
import io
import json
from datetime import UTC, datetime, timedelta
from itertools import islice
from typing import Any

from automl_api.storage.contracts import (
    BeginUpload,
    ByteRange,
    CompletedObject,
    DeletionPending,
    HealthcheckResult,
    ObjectMetadata,
    ProviderUpload,
    RayDataSourceDescriptor,
    TransferCursor,
    TransferReceipt,
    UploadCapabilities,
    UploadContractError,
    UploadExpired,
    UploadInstruction,
    UploadProgress,
    UploadThrottled,
    immutable_object_version,
)


class GCSObjectStoreDriver:
    driver_name = "gcs"

    def __init__(
        self,
        *,
        bucket: str,
        project: str | None = None,
        client: Any | None = None,
        http_client: Any | None = None,
    ) -> None:
        self.bucket_name = bucket
        if client is None:
            from google.cloud import storage

            client = storage.Client(project=project)
        self.client = client
        self.bucket = client.bucket(bucket)
        if http_client is None:
            import httpx

            http_client = httpx.Client(timeout=30.0, follow_redirects=False)
        self.http = http_client

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities(
            protocol="resumable_offset",
            parallel_chunks_per_object=1,
            can_list_committed_units=False,
            supports_unit_checksum=False,
            supports_final_checksum=True,
            supports_provider_lifecycle=True,
        )

    def begin_upload(self, request: BeginUpload) -> ProviderUpload:
        blob = self.bucket.blob(self._key(request.object_key))
        session_uri = blob.create_resumable_upload_session(
            content_type=request.content_type,
            size=request.byte_size,
            origin=request.origin,
            if_generation_match=0,
            retry=None,
        )
        return ProviderUpload(
            provider_id=session_uri,
            object_key=self._key(request.object_key),
            protocol="resumable_offset",
            byte_size=request.byte_size,
            expires_at=request.expires_at,
            state={
                "content_type": request.content_type,
                "transfer_unit_size": request.transfer_unit_size,
            },
        )

    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction:
        self._validate_cursor(upload, cursor)
        expires_at = min(upload.expires_at, datetime.now(UTC) + timedelta(minutes=15))
        end = cursor.offset + cursor.length - 1
        return UploadInstruction(
            instruction_id=self._instruction_id(upload, cursor, expires_at),
            method="PUT",
            url=upload.provider_id,
            headers={
                "Content-Type": str(upload.state.get("content_type") or "application/octet-stream"),
                "Content-Length": str(cursor.length),
                "Content-Range": f"bytes {cursor.offset}-{end}/{upload.byte_size}",
            },
            cursor=cursor,
            expires_at=expires_at,
            expose_response_headers=("Range", "X-GUploader-UploadID"),
            enforced_controls=("object", "method", "offset", "size"),
            unsupported_controls=("unit_checksum", "instruction_expiry"),
        )

    def query_progress(self, upload: ProviderUpload) -> UploadProgress:
        return self._query_progress(upload)[0]

    def _query_progress(self, upload: ProviderUpload, *, allow_expired=False):
        if not allow_expired:
            self._ensure_active(upload)
        with self.http.stream(
            "PUT",
            upload.provider_id,
            headers={"Content-Length": "0", "Content-Range": f"bytes */{upload.byte_size}"},
            content=b"",
            timeout=30,
        ) as response:
            if response.status_code in {404, 410}:
                raise UploadExpired("The GCS resumable session is no longer usable.")
            if response.status_code == 429:
                raise UploadThrottled(
                    "GCS throttled the progress query.",
                    retry_after_seconds=self._retry_after(response.headers),
                )
            if response.status_code not in {200, 201, 308}:
                raise UploadContractError(
                    f"GCS returned unexpected progress status {response.status_code}."
                )
            confirmed = self._confirmed_offset(response.headers.get("Range"), upload.byte_size)
            completed = None
            if response.status_code in {200, 201}:
                body = bytearray()
                for block in response.iter_bytes(chunk_size=8192):
                    body.extend(block)
                    if len(body) > 65536:
                        raise UploadContractError("GCS completion exceeds its receipt budget.")
                try:
                    receipt = json.loads(body)
                    generation = receipt["generation"]
                    size = receipt["size"]
                    if (
                        receipt["bucket"] != self.bucket_name
                        or receipt["name"] != upload.object_key
                        or not isinstance(generation, str)
                        or not generation.isascii()
                        or not generation.isdecimal()
                        or len(generation) > 20
                        or not 0 < int(generation) < 2**64
                        or not isinstance(size, str)
                        or not size.isascii()
                        or not size.isdecimal()
                        or len(size) > 20
                        or int(size) != upload.byte_size
                    ):
                        raise ValueError("GCS completion has another resource, generation or size")
                    completed = CompletedObject(
                        self._uri(upload.object_key),
                        upload.byte_size,
                        receipt.get("crc32c") or receipt.get("md5Hash"),
                        {"generation": generation},
                    )
                except (ValueError, KeyError, TypeError) as exc:
                    raise UploadContractError(
                        "GCS completion has no original immutable receipt."
                    ) from exc
                confirmed = upload.byte_size
        return UploadProgress(
            confirmed_bytes=confirmed,
            total_bytes=upload.byte_size,
            receipts=(),
            complete=completed is not None,
        ), completed

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject:
        if receipts:
            raise UploadContractError("GCS offset uploads do not accept synthetic part receipts.")
        progress, completed = self._query_progress(upload)
        if not progress.complete or completed is None:
            raise UploadContractError("GCS has not confirmed the entire object.")
        return completed

    def abort_upload(self, upload: ProviderUpload) -> None:
        response = self.http.delete(upload.provider_id)
        # A missing session cannot prove that a browser-completed object was cancelled.
        if response.status_code != 499:
            raise UploadContractError(f"GCS session cancellation returned {response.status_code}.")

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str, *, version: str | None = None) -> ObjectMetadata:
        kwargs = {"generation": self._generation(version)} if version is not None else {}
        blob = self.bucket.get_blob(self._key_from_uri(uri), **kwargs)
        if blob is None:
            raise FileNotFoundError(uri)
        if version is not None and str(blob.generation) != version:
            raise UploadContractError("Object metadata returned a different or missing version.")
        checksum = blob.crc32c or blob.md5_hash
        return ObjectMetadata(
            uri=self._uri(blob.name),
            byte_size=int(blob.size or 0),
            etag=blob.etag,
            checksum=checksum,
            content_type=blob.content_type,
            provider_headers={
                "generation": str(blob.generation),
                "metageneration": str(blob.metageneration),
                **(
                    {"sceptre_grant": str((getattr(blob, "metadata", None) or {})["sceptre_grant"])}
                    if (getattr(blob, "metadata", None) or {}).get("sceptre_grant")
                    else {}
                ),
            },
        )

    def open_stream(
        self, uri: str, byte_range: ByteRange | None = None, *, version: str | None = None
    ):
        kwargs = {}
        if version is not None:
            kwargs["generation"] = self._generation(version)
        blob = self.bucket.blob(self._key_from_uri(uri), **kwargs)
        if byte_range is None:
            return blob.open("rb")
        value = blob.download_as_bytes(
            start=byte_range.start, end=byte_range.end_inclusive, checksum=None
        )
        return io.BytesIO(value)

    @staticmethod
    def _generation(version):
        immutable_object_version(version)
        if (
            not version.isascii()
            or not version.isdecimal()
            or len(version) > 20
            or not 0 < int(version) < 2**64
            or str(int(version)) != version
        ):
            raise UploadContractError("GCS reads require a positive immutable generation.")
        return int(version)

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        blob = self.bucket.blob(self._key_from_uri(uri))
        blob.upload_from_file(source, rewind=True)
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        blob = self.bucket.blob(self._key(key))
        blob.upload_from_string(value)
        return self.stat(self._uri(blob.name))

    def read_bytes(self, uri: str) -> bytes:
        return self.bucket.blob(self._key_from_uri(uri)).download_as_bytes()

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        if byte_count <= 0:
            return b""
        return self.bucket.blob(self._key_from_uri(uri)).download_as_bytes(
            start=0, end=byte_count - 1, checksum=None
        )

    def exists(self, uri: str) -> bool:
        return self.bucket.blob(self._key_from_uri(uri)).exists()

    def size(self, uri: str) -> int:
        return self.stat(uri).byte_size

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor:
        return RayDataSourceDescriptor(
            path=uri,
            filesystem_options={},
            provider=self.driver_name,
        )

    def healthcheck(self) -> HealthcheckResult:
        try:
            self.client.get_bucket(self.bucket_name)
        except Exception as exc:
            return HealthcheckResult(False, self.driver_name, type(exc).__name__)
        return HealthcheckResult(True, self.driver_name)

    def prefix_absent(self, prefix: str) -> bool:
        prefix = self._key(prefix).rstrip("/") + "/"
        for options in ({"versions": True}, {"soft_deleted": True}):
            for blob in self.client.list_blobs(
                self.bucket_name, prefix=prefix, page_size=1, **options
            ):
                if not isinstance(blob.name, str) or not blob.name.startswith(prefix):
                    raise UploadContractError("GCS prefix inventory escaped its original root.")
                return False
        return True

    def delete_prefix(self, prefix: str) -> None:
        self._delete_matching(self._key(prefix).rstrip("/") + "/", exact=False)

    def delete(self, uri: str, *, version: str | None = None) -> None:
        key = self._key_from_uri(uri)
        if version is None:
            self._delete_matching(key, exact=True)
            return
        generation = self._generation(version)
        blob = self.bucket.get_blob(key, generation=generation, retry=None)
        if blob is not None:
            self._require_generation(blob, key, generation)
            blob.delete(if_generation_match=generation, retry=None)
            remaining = self.bucket.get_blob(key, generation=generation, retry=None)
            if remaining is not None:
                self._require_generation(remaining, key, generation)
                raise DeletionPending("The original GCS generation is still stored.")
        retained = self.bucket.get_blob(key, generation=generation, soft_deleted=True, retry=None)
        if retained is not None:
            self._require_generation(retained, key, generation)
            raise DeletionPending("GCS retains the original soft-deleted generation.")

    @staticmethod
    def _require_generation(blob, key, generation):
        if blob.name != key or str(blob.generation) != str(generation):
            raise UploadContractError("GCS returned another deletion generation.")

    def _delete_matching(self, prefix: str, *, exact: bool) -> None:
        from google.api_core.exceptions import NotFound

        previous = None
        while True:
            blobs = list(
                islice(
                    (
                        blob
                        for blob in self.client.list_blobs(
                            self.bucket_name, prefix=prefix, versions=True, page_size=1000
                        )
                        if not exact or blob.name == prefix
                    ),
                    1000,
                )
            )
            if not blobs:
                break
            identities = frozenset((blob.name, blob.generation) for blob in blobs)
            if identities == previous:
                raise RuntimeError("GCS generation deletion made no progress.")
            previous = identities
            for blob in blobs:
                if not blob.name.startswith(prefix) or blob.generation is None:
                    raise RuntimeError("GCS returned an unscoped or unversioned deletion target.")
                try:
                    blob.delete(if_generation_match=blob.generation)
                except NotFound:
                    pass  # Another cleanup may have already removed this generation.
        # Disabled policies can still leave older retained data; never skip this query.
        for blob in self.client.list_blobs(
            self.bucket_name, prefix=prefix, soft_deleted=True, page_size=1000
        ):
            if not blob.name.startswith(prefix):
                raise RuntimeError("GCS returned an unscoped retention result.")
            if not exact or blob.name == prefix:
                raise DeletionPending("GCS retains soft-deleted object generations.")

    def _validate_cursor(self, upload: ProviderUpload, cursor: TransferCursor) -> None:
        self._ensure_active(upload)
        unit_size = int(upload.state.get("transfer_unit_size") or cursor.length)
        if cursor.unit_number < 1:
            raise UploadContractError("Transfer unit numbers are one-based.")
        if (
            not 0 <= cursor.offset < upload.byte_size
            or cursor.unit_number != cursor.offset // unit_size + 1
            or cursor.length != min(unit_size, upload.byte_size - cursor.offset)
        ):
            raise UploadContractError("GCS requires the next sequential confirmed byte offset.")

    @staticmethod
    def _ensure_active(upload: ProviderUpload) -> None:
        if upload.expires_at <= datetime.now(UTC):
            raise UploadExpired("The application upload session has expired.")

    @staticmethod
    def _confirmed_offset(range_header: str | None, total: int) -> int:
        if not range_header:
            return 0
        try:
            _, bounds = range_header.split("=", 1)
            _, end = bounds.split("-", 1)
            return min(total, int(end) + 1)
        except (TypeError, ValueError) as exc:
            raise UploadContractError("GCS returned an invalid confirmed Range header.") from exc

    @staticmethod
    def _retry_after(headers: Any) -> float | None:
        value = headers.get("Retry-After")
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None

    @staticmethod
    def _instruction_id(
        upload: ProviderUpload, cursor: TransferCursor, expires_at: datetime
    ) -> str:
        value = (
            f"{upload.object_key}:{cursor.unit_number}:{cursor.offset}:{cursor.length}:"
            f"{int(expires_at.timestamp())}"
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def _uri(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self._key(key)}"

    def _key_from_uri(self, uri: str) -> str:
        prefix = f"gs://{self.bucket_name}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured GCS bucket.")
        return self._key(uri.removeprefix(prefix))

    @staticmethod
    def _key(value: str) -> str:
        key = value.strip("/")
        if not key or key.startswith("../") or "/../" in key:
            raise ValueError("Object keys must remain inside the configured bucket.")
        return key
