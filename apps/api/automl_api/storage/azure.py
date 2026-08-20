from __future__ import annotations

import base64
import hashlib
import io
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

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
    UploadContractError,
    UploadExpired,
    UploadInstruction,
    UploadProgress,
)


class AzureBlobObjectStoreDriver:
    driver_name = "azure_blob"

    def __init__(
        self,
        *,
        account_url: str,
        container: str,
        account_name: str,
        credential: Any | None = None,
        service_client: Any | None = None,
        sas_factory: Any | None = None,
    ) -> None:
        self.account_url = account_url.rstrip("/")
        self.container_name = container
        self.account_name = account_name
        if credential is None and service_client is None:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
        self.credential = credential
        if service_client is None:
            from azure.storage.blob import BlobServiceClient

            service_client = BlobServiceClient(account_url=self.account_url, credential=credential)
        self.service = service_client
        self.container = service_client.get_container_client(container)
        if sas_factory is None:
            from azure.storage.blob import generate_blob_sas

            sas_factory = generate_blob_sas
        self.sas_factory = sas_factory

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities(
            protocol="block_list",
            parallel_chunks_per_object=4,
            can_list_committed_units=True,
            supports_unit_checksum=False,
            supports_final_checksum=True,
            supports_provider_lifecycle=True,
        )

    def begin_upload(self, request: BeginUpload) -> ProviderUpload:
        key = self._key(request.object_key)
        return ProviderUpload(
            provider_id=hashlib.sha256(f"{self.container_name}/{key}".encode()).hexdigest(),
            object_key=key,
            protocol="block_list",
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
        now = datetime.now(UTC)
        expires_at = min(upload.expires_at, now + timedelta(minutes=15))
        block_id = self._block_id(cursor.unit_number)
        delegation_key = self.service.get_user_delegation_key(
            now - timedelta(minutes=5), expires_at
        )
        from azure.storage.blob import BlobSasPermissions

        sas = self.sas_factory(
            account_name=self.account_name,
            container_name=self.container_name,
            blob_name=upload.object_key,
            user_delegation_key=delegation_key,
            permission=BlobSasPermissions(write=True, create=True),
            expiry=expires_at,
            start=now - timedelta(minutes=5),
        )
        query = urlencode({"comp": "block", "blockid": block_id})
        url = f"{self.account_url}/{self.container_name}/{upload.object_key}?{query}&{sas}"
        return UploadInstruction(
            instruction_id=self._instruction_id(upload, cursor, expires_at),
            method="PUT",
            url=url,
            headers={
                "Content-Length": str(cursor.length),
                "x-ms-version": "2023-11-03",
            },
            cursor=cursor,
            expires_at=expires_at,
            expose_response_headers=("ETag", "x-ms-request-id", "x-ms-version"),
            enforced_controls=("object", "method", "block_id", "expiry"),
            unsupported_controls=("exact_content_length", "unit_sha256"),
        )

    def query_progress(self, upload: ProviderUpload) -> UploadProgress:
        self._ensure_active(upload)
        blob = self.container.get_blob_client(upload.object_key)
        blocks = blob.get_block_list(block_list_type="all")
        values = list(getattr(blocks, "committed_blocks", []) or []) + list(
            getattr(blocks, "uncommitted_blocks", []) or []
        )
        receipts: list[TransferReceipt] = []
        unit_size = int(upload.state.get("transfer_unit_size") or 1)
        for block in values:
            unit_number = self._unit_number(str(block.id))
            length = int(block.size)
            receipts.append(
                TransferReceipt(
                    unit_number=unit_number,
                    offset=(unit_number - 1) * unit_size,
                    length=length,
                    etag=getattr(block, "etag", None),
                    provider_headers={"block_id": str(block.id)},
                )
            )
        ordered = tuple(sorted(receipts, key=lambda value: value.unit_number))
        confirmed = sum(value.length for value in ordered)
        return UploadProgress(
            confirmed_bytes=confirmed,
            total_bytes=upload.byte_size,
            receipts=ordered,
            complete=confirmed == upload.byte_size,
        )

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject:
        self._ensure_active(upload)
        ordered = sorted(receipts, key=lambda value: value.unit_number)
        if not ordered or [item.unit_number for item in ordered] != list(
            range(1, len(ordered) + 1)
        ):
            raise UploadContractError("Azure block receipts must be contiguous and one-based.")
        if sum(item.length for item in ordered) != upload.byte_size:
            raise UploadContractError("Azure block receipts do not match the object size.")
        block_ids = [
            item.provider_headers.get("block_id") or self._block_id(item.unit_number)
            for item in ordered
        ]
        self.container.get_blob_client(upload.object_key).commit_block_list(block_ids)
        metadata = self.stat(self._uri(upload.object_key))
        if metadata.byte_size != upload.byte_size:
            raise UploadContractError("The committed Azure blob has an unexpected size.")
        return CompletedObject(
            metadata.uri,
            metadata.byte_size,
            metadata.checksum,
            metadata.provider_headers,
        )

    def abort_upload(self, upload: ProviderUpload) -> None:
        blob = self.container.get_blob_client(upload.object_key)
        try:
            blob.delete_blob(delete_snapshots="include")
        except Exception as exc:
            if getattr(exc, "status_code", None) != 404:
                raise

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str) -> ObjectMetadata:
        blob = self.container.get_blob_client(self._key_from_uri(uri))
        properties = blob.get_blob_properties()
        content_settings = getattr(properties, "content_settings", None)
        checksum = None
        if content_settings is not None and getattr(content_settings, "content_md5", None):
            checksum = base64.b64encode(content_settings.content_md5).decode("ascii")
        return ObjectMetadata(
            uri=self._uri(blob.blob_name),
            byte_size=int(properties.size),
            etag=str(properties.etag).strip('"') if properties.etag else None,
            checksum=checksum,
            content_type=getattr(content_settings, "content_type", None),
            provider_headers={
                "version_id": str(properties.version_id)
                if getattr(properties, "version_id", None)
                else "",
            },
        )

    def open_stream(self, uri: str, byte_range: ByteRange | None = None):
        blob = self.container.get_blob_client(self._key_from_uri(uri))
        kwargs: dict[str, int] = {}
        if byte_range:
            kwargs = {
                "offset": byte_range.start,
                "length": byte_range.end_inclusive - byte_range.start + 1,
            }
        return io.BytesIO(blob.download_blob(**kwargs).readall())

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        self.container.get_blob_client(self._key_from_uri(uri)).upload_blob(
            source, overwrite=True
        )
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        self.container.get_blob_client(normalized).upload_blob(value, overwrite=True)
        return self.stat(self._uri(normalized))

    def read_bytes(self, uri: str) -> bytes:
        return self.container.get_blob_client(self._key_from_uri(uri)).download_blob().readall()

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        if byte_count <= 0:
            return b""
        return (
            self.container.get_blob_client(self._key_from_uri(uri))
            .download_blob(offset=0, length=byte_count)
            .readall()
        )

    def exists(self, uri: str) -> bool:
        return self.container.get_blob_client(self._key_from_uri(uri)).exists()

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
            self.container.get_container_properties()
        except Exception as exc:
            return HealthcheckResult(False, self.driver_name, type(exc).__name__)
        return HealthcheckResult(True, self.driver_name)

    def delete(self, uri: str) -> None:
        self.container.get_blob_client(self._key_from_uri(uri)).delete_blob(
            delete_snapshots="include"
        )

    def _validate_cursor(self, upload: ProviderUpload, cursor: TransferCursor) -> None:
        self._ensure_active(upload)
        if cursor.unit_number < 1 or cursor.unit_number > 50_000:
            raise UploadContractError("Azure block numbers must be between 1 and 50,000.")
        unit_size = int(upload.state.get("transfer_unit_size") or cursor.length)
        expected_offset = (cursor.unit_number - 1) * unit_size
        expected_length = min(unit_size, upload.byte_size - expected_offset)
        if cursor.offset != expected_offset or cursor.length != expected_length:
            raise UploadContractError("Azure transfer cursor does not match its block ID.")

    @staticmethod
    def _ensure_active(upload: ProviderUpload) -> None:
        if upload.expires_at <= datetime.now(UTC):
            raise UploadExpired("The application upload session has expired.")

    @staticmethod
    def _block_id(unit_number: int) -> str:
        return base64.b64encode(f"sceptre-{unit_number:08d}".encode()).decode("ascii")

    @staticmethod
    def _unit_number(block_id: str) -> int:
        try:
            decoded = base64.b64decode(block_id).decode("ascii")
            return int(decoded.removeprefix("sceptre-"))
        except (ValueError, UnicodeError) as exc:
            raise UploadContractError("Azure returned an unrecognized block ID.") from exc

    @staticmethod
    def _instruction_id(
        upload: ProviderUpload, cursor: TransferCursor, expires_at: datetime
    ) -> str:
        value = (
            f"{upload.provider_id}:{cursor.unit_number}:{cursor.offset}:{cursor.length}:"
            f"{int(expires_at.timestamp())}"
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def _uri(self, key: str) -> str:
        return f"az://{self.account_name}/{self.container_name}/{self._key(key)}"

    def _key_from_uri(self, uri: str) -> str:
        prefix = f"az://{self.account_name}/{self.container_name}/"
        if not uri.startswith(prefix):
            raise ValueError("Object URI does not belong to the configured Azure container.")
        return self._key(uri.removeprefix(prefix))

    @staticmethod
    def _key(value: str) -> str:
        key = value.strip("/")
        if not key or key.startswith("../") or "/../" in key:
            raise ValueError("Object keys must remain inside the configured container.")
        return key
