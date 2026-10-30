from __future__ import annotations

import base64
import hashlib
import io
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

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
    UploadCapabilityUnavailable,
    UploadContractError,
    UploadExpired,
    UploadInstruction,
    UploadProgress,
    immutable_object_version,
)


class _AzureReader(io.RawIOBase):
    def __init__(self, downloader):
        super().__init__()
        self.downloader = downloader

    def readable(self):
        return True

    def read(self, size=-1):
        if self.closed:
            raise ValueError("I/O operation on closed stream.")
        return self.downloader.read(-1 if size is None else size)

    def readinto(self, buffer):
        view = memoryview(buffer).cast("B")
        if view.readonly:
            raise TypeError("readinto requires a writable buffer")
        value = self.read(len(view))
        view[: len(value)] = value
        return len(value)

    def close(self):
        self.downloader = None
        super().close()


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
            supports_abort=False,
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
        committed, uncommitted = blob.get_block_list(block_list_type="all")
        values = list(committed) + list(uncommitted)
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
            item.provider_headers.get("block_id") or f"sceptre-{item.unit_number:08d}"
            for item in ordered
        ]
        if any(
            self._unit_number(block) != item.unit_number
            for block, item in zip(block_ids, ordered, strict=True)
        ):
            raise UploadContractError("Azure block receipt does not match its transfer unit.")
        from azure.core import MatchConditions

        response = self.container.get_blob_client(upload.object_key).commit_block_list(
            block_ids, match_condition=MatchConditions.IfMissing, retry_total=0
        )
        if not isinstance(response, dict):
            raise UploadContractError("Completion has no original immutable receipt.")
        version = immutable_object_version(response.get("version_id"))
        if len(version) > 256:
            raise UploadContractError("The completion version exceeds its receipt budget.")
        return CompletedObject(
            self._uri(upload.object_key),
            upload.byte_size,
            base64.b64encode(response["content_md5"]).decode("ascii")
            if response.get("content_md5")
            else None,
            {"version_id": version},
        )

    def abort_upload(self, upload: ProviderUpload) -> None:
        raise UploadCapabilityUnavailable(
            "Azure staged blocks require expiry or qualified original-version retirement."
        )

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str, *, version: str | None = None) -> ObjectMetadata:
        kwargs = {"version_id": immutable_object_version(version)} if version is not None else {}
        blob = self.container.get_blob_client(self._key_from_uri(uri), **kwargs)
        properties = blob.get_blob_properties()
        if version is not None and getattr(properties, "version_id", None) != version:
            raise UploadContractError("Object metadata returned a different or missing version.")
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
                **(
                    {
                        "sceptre_grant": str(
                            (getattr(properties, "metadata", None) or {})["sceptre_grant"]
                        )
                    }
                    if (getattr(properties, "metadata", None) or {}).get("sceptre_grant")
                    else {}
                ),
            },
        )

    def open_stream(
        self, uri: str, byte_range: ByteRange | None = None, *, version: str | None = None
    ):
        blob_kwargs = (
            {"version_id": immutable_object_version(version)} if version is not None else {}
        )
        blob = self.container.get_blob_client(self._key_from_uri(uri), **blob_kwargs)
        kwargs: dict[str, int] = {}
        if byte_range:
            kwargs = {
                "offset": byte_range.start,
                "length": byte_range.end_inclusive - byte_range.start + 1,
            }
        downloader = blob.download_blob(**kwargs)
        if version is not None and getattr(downloader.properties, "version_id", None) != version:
            raise UploadContractError("Object store returned a different or missing version.")
        return _AzureReader(downloader)

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        self.container.get_blob_client(self._key_from_uri(uri)).upload_blob(source, overwrite=True)
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        self.container.get_blob_client(normalized).upload_blob(value, overwrite=True)
        return self.stat(self._uri(normalized))

    def create_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        response = self.container.get_blob_client(normalized).upload_blob(
            value, overwrite=False, retry_total=0, max_concurrency=1
        )
        if not isinstance(response, dict):
            raise UploadContractError("Byte creation has no original immutable receipt.")
        version = immutable_object_version(response.get("version_id"))
        if len(version) > 256:
            raise UploadContractError("The byte creation version exceeds its receipt budget.")
        return ObjectMetadata(
            uri=self._uri(normalized),
            byte_size=len(value),
            etag=str(response.get("etag") or "").strip('"') or None,
            provider_headers={"version_id": version},
        )

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

    def prefix_absent(self, prefix: str) -> bool:
        prefix = self._key(prefix).rstrip("/") + "/"
        for blob in self.container.list_blobs(
            name_starts_with=prefix,
            include=["versions", "snapshots", "deleted", "uncommittedblobs"],
            results_per_page=1,
        ):
            if not isinstance(blob.name, str) or not blob.name.startswith(prefix):
                raise UploadContractError("Azure prefix inventory escaped its original root.")
            return False
        return True

    def delete_prefix(self, prefix: str) -> None:
        self._delete_matching(self._key(prefix).rstrip("/") + "/", exact=False)

    def delete(self, uri: str, *, version: str | None = None) -> None:
        key = self._key_from_uri(uri)
        if version is None:
            self._delete_matching(key, exact=True)
            return
        version = immutable_object_version(version)
        if not self._version_present(key, version):
            return
        self.container.get_blob_client(key, version_id=version).delete_blob(retry_total=0)
        if self._version_present(key, version):
            raise DeletionPending("The original Azure version is still stored.")

    def _version_present(self, key, version):
        found = False
        # shortcut: hold scans beyond 10,000 entries; add resumable scans for larger histories.
        for index, blob in enumerate(
            self.container.list_blobs(
                name_starts_with=key, include=["versions", "deleted"], results_per_page=1000
            )
        ):
            if index >= 10_000:
                raise DeletionPending(
                    "Azure version inventory exceeds the bounded qualification scan."
                )
            if not isinstance(blob.name, str) or not blob.name.startswith(key):
                raise UploadContractError("Azure version inventory escaped its prefix.")
            if blob.name == key and blob.version_id == version:
                if blob.deleted:
                    raise DeletionPending("Azure retains the original soft-deleted version.")
                found = True
        return found

    def _delete_matching(self, prefix: str, *, exact: bool) -> None:
        from azure.core.exceptions import ResourceNotFoundError

        previous = None
        while True:
            targets = []
            retained = False
            for blob in self.container.list_blobs(
                name_starts_with=prefix,
                include=["versions", "snapshots", "deleted", "uncommittedblobs"],
                results_per_page=1000,
            ):
                if not blob.name.startswith(prefix):
                    raise RuntimeError("Azure returned an unscoped deletion target.")
                if exact and blob.name != prefix:
                    continue
                if blob.deleted:
                    retained = True
                    continue
                targets.append((blob.name, blob.version_id, blob.snapshot, blob.is_current_version))
                if len(targets) == 1000:
                    break
            if not targets:
                if retained:
                    raise DeletionPending(
                        "Azure retains soft-deleted blobs, versions or snapshots."
                    )
                return
            identities = frozenset(targets)
            if identities == previous:
                raise RuntimeError("Azure blob deletion made no progress.")
            previous = identities
            for name, version, snapshot, current in targets:
                # Deleting the base demotes the current version; the next pass removes it.
                version = version if not current and not snapshot else None
                blob_client = self.container.get_blob_client(
                    name, version_id=version, snapshot=snapshot
                )
                try:
                    if version or snapshot:
                        blob_client.delete_blob()
                    else:
                        blob_client.delete_blob(delete_snapshots="include")
                except ResourceNotFoundError:
                    pass  # A base deletion or another cleanup may have removed this snapshot.

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
        match = re.fullmatch(r"sceptre-([0-9]{8})", block_id)
        if match is None or not 1 <= int(match[1]) <= 50_000:
            raise UploadContractError("Azure returned an unrecognized block ID.")
        return int(match[1])

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
