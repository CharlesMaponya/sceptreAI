from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
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


class S3ObjectStoreDriver:
    """AWS multipart upload using only the supported public boto3 API."""

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str | None = None,
        region: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        public_endpoint_url: str | None = None,
        compatible: bool = False,
        client: Any | None = None,
        presign_client: Any | None = None,
    ) -> None:
        self.driver_name = "s3_compatible" if compatible else "aws_s3"
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.public_endpoint_url = public_endpoint_url or endpoint_url
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key
        self.compatible = compatible
        if compatible and (not endpoint_url or not access_key or not secret_key):
            raise ValueError(
                "S3-compatible storage requires OBJECT_STORE_ENDPOINT, "
                "OBJECT_STORE_ACCESS_KEY, and OBJECT_STORE_SECRET_KEY."
            )
        # Workflow reconciliation owns retries after uncertain provider outcomes.
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4", retries={"total_max_attempts": 1}),
            )
        self.client = client
        self.presign_client = presign_client or client
        if presign_client is None and public_endpoint_url and public_endpoint_url != endpoint_url:
            import boto3
            from botocore.config import Config

            self.presign_client = boto3.client(
                "s3",
                endpoint_url=public_endpoint_url,
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4", retries={"total_max_attempts": 1}),
            )

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities(
            protocol="multipart",
            parallel_chunks_per_object=4,
            can_list_committed_units=True,
            supports_unit_checksum=not self.compatible,
            supports_final_checksum=True,
            supports_provider_lifecycle=not self.compatible,
        )

    def begin_upload(self, request: BeginUpload) -> ProviderUpload:
        object_key = self._key(request.object_key)
        if self.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError(
                "Upload admission requires single-attempt S3 SDK configuration."
            )
        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": object_key,
            "ContentType": request.content_type,
            "Metadata": {
                "sceptre-sha256": request.checksum_sha256,
                "sceptre-byte-size": str(request.byte_size),
            },
        }
        if not self.compatible:
            params["ChecksumAlgorithm"] = "SHA256"
        try:
            provider_id = self._existing_multipart_upload_id(object_key)
            if provider_id is None:
                response = self.client.create_multipart_upload(**params)
                if (
                    not isinstance(response, dict)
                    or response.get("Bucket", self.bucket) != self.bucket
                    or response.get("Key", object_key) != object_key
                ):
                    raise UploadContractError(
                        "S3 creation returned another or missing upload receipt."
                    )
                provider_id = self._multipart_id(response.get("UploadId"))
        except Exception as exc:
            self._raise_provider_error(exc)
            raise
        return ProviderUpload(
            provider_id=provider_id,
            object_key=object_key,
            protocol="multipart",
            byte_size=request.byte_size,
            expires_at=request.expires_at,
            state={
                "content_type": request.content_type,
                "transfer_unit_size": request.transfer_unit_size,
            },
        )

    def _existing_multipart_upload_id(self, object_key: str) -> str | None:
        params = dict(Bucket=self.bucket, Prefix=object_key, MaxUploads=1000)
        seen, matches = set(), set()
        for _ in range(100):
            response = self.client.list_multipart_uploads(**params)
            if (
                not isinstance(response, dict)
                or type(response.get("IsTruncated")) is not bool
                or response.get("Bucket", self.bucket) != self.bucket
                or response.get("Prefix", object_key) != object_key
                or response.get("KeyMarker", params.get("KeyMarker", ""))
                != params.get("KeyMarker", "")
                or response.get("UploadIdMarker", params.get("UploadIdMarker", ""))
                != params.get("UploadIdMarker", "")
                or not isinstance(response.get("Uploads", []), list)
                or len(response.get("Uploads", [])) > 1000
            ):
                raise UploadContractError("S3 multipart inventory is incomplete or foreign.")
            for row in response.get("Uploads", []):
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("Key"), str)
                    or not row["Key"].startswith(object_key)
                ):
                    raise UploadContractError("S3 multipart inventory contains a foreign key.")
                upload_id = self._multipart_id(row.get("UploadId"))
                if row["Key"] == object_key:
                    matches.add(upload_id)
                    if len(matches) > 1:
                        raise UploadContractError(
                            "Original multipart upload identity is ambiguous."
                        )
            if not response["IsTruncated"]:
                return next(iter(matches), None)
            key = response.get("NextKeyMarker")
            marker = self._multipart_id(response.get("NextUploadIdMarker"))
            if (
                not isinstance(key, str)
                or not key.startswith(object_key)
                or key < params.get("KeyMarker", object_key)
                or len(key) > 1024
                or (key, marker) in seen
            ):
                raise UploadContractError("S3 multipart inventory did not advance.")
            seen.add((key, marker))
            params.update(KeyMarker=key, UploadIdMarker=marker)
        raise UploadContractError("S3 multipart inventory exceeded its bounded page budget.")

    @staticmethod
    def _multipart_id(value):
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 1024
            or any(ord(c) < 32 or ord(c) == 127 for c in value)
        ):
            raise UploadContractError("S3 returned no qualified multipart upload identity.")
        return value

    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction:
        self._validate_cursor(upload, cursor)
        expires_at = min(upload.expires_at, datetime.now(UTC) + timedelta(minutes=15))
        expires_in = max(1, int((expires_at - datetime.now(UTC)).total_seconds()))
        params: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": upload.object_key,
            "UploadId": upload.provider_id,
            "PartNumber": cursor.unit_number,
        }
        headers: dict[str, str] = {"Content-Length": str(cursor.length)}
        enforced = ["object", "method", "unit_number", "expiry"]
        unsupported = ["exact_content_length"]
        if cursor.checksum_sha256 and self.capabilities().supports_unit_checksum:
            checksum = base64.b64encode(bytes.fromhex(cursor.checksum_sha256)).decode("ascii")
            params["ChecksumSHA256"] = checksum
            headers["x-amz-checksum-sha256"] = checksum
            enforced.append("unit_checksum")
        elif cursor.checksum_sha256:
            unsupported.append("unit_checksum")
        try:
            url = self.presign_client.generate_presigned_url(
                "upload_part", Params=params, ExpiresIn=expires_in, HttpMethod="PUT"
            )
        except Exception as exc:
            self._raise_provider_error(exc)
            raise
        return UploadInstruction(
            instruction_id=self._instruction_id(upload, cursor, expires_at),
            method="PUT",
            url=url,
            headers=headers,
            cursor=cursor,
            expires_at=expires_at,
            expose_response_headers=("ETag", "x-amz-checksum-sha256", "x-amz-request-id"),
            enforced_controls=tuple(enforced),
            unsupported_controls=tuple(unsupported),
        )

    def query_progress(self, upload: ProviderUpload) -> UploadProgress:
        self._ensure_active(upload)
        params = dict(
            Bucket=self.bucket, Key=upload.object_key, UploadId=upload.provider_id, MaxParts=1000
        )
        parts, last_part = [], 0
        for _ in range(100):
            try:
                response = self.client.list_parts(**params)
            except Exception as exc:
                self._raise_provider_error(exc)
                raise
            if (
                not isinstance(response, dict)
                or type(response.get("IsTruncated")) is not bool
                or response.get("Bucket", self.bucket) != self.bucket
                or response.get("Key", upload.object_key) != upload.object_key
                or response.get("UploadId", upload.provider_id) != upload.provider_id
                or response.get("PartNumberMarker", params.get("PartNumberMarker", 0))
                != params.get("PartNumberMarker", 0)
                or not isinstance(response.get("Parts", []), list)
                or len(response.get("Parts", [])) > 1000
            ):
                raise UploadContractError("S3 part inventory is incomplete or foreign.")
            page = response.get("Parts", [])
            if any(
                not isinstance(part, dict)
                or type(part.get("PartNumber")) is not int
                or type(part.get("Size")) is not int
                or part["Size"] <= 0
                for part in page
            ):
                raise UploadContractError("S3 part inventory contains malformed parts.")
            for part in sorted(page, key=lambda value: value["PartNumber"]):
                if not last_part < part["PartNumber"] <= 10_000:
                    raise UploadContractError("S3 part inventory repeated or exceeded its bounds.")
                last_part = part["PartNumber"]
                parts.append(part)
            if not response["IsTruncated"]:
                break
            marker = response.get("NextPartNumberMarker")
            if type(marker) is not int or not page or marker != last_part:
                raise UploadContractError("S3 part inventory did not advance.")
            params["PartNumberMarker"] = marker
        else:
            raise UploadContractError("S3 part inventory exceeded its bounded page budget.")
        unit_size = int(upload.state["transfer_unit_size"])
        receipts = tuple(
            TransferReceipt(
                unit_number=(unit_number := int(part["PartNumber"])),
                offset=(unit_number - 1) * unit_size,
                length=int(part.get("Size") or 0),
                etag=str(part.get("ETag") or "").strip('"') or None,
                checksum_sha256=part.get("ChecksumSHA256"),
            )
            for part in parts
        )
        confirmed = sum(receipt.length for receipt in receipts)
        return UploadProgress(
            confirmed_bytes=confirmed,
            total_bytes=upload.byte_size,
            receipts=receipts,
            complete=confirmed == upload.byte_size,
        )

    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject:
        self._ensure_active(upload)
        if not receipts:
            raise UploadContractError("Multipart completion requires provider receipts.")
        ordered = sorted(receipts, key=lambda value: value.unit_number)
        if [item.unit_number for item in ordered] != list(range(1, len(ordered) + 1)):
            raise UploadContractError("Multipart receipts must be contiguous and one-based.")
        if sum(item.length for item in ordered) != upload.byte_size:
            raise UploadContractError("Multipart receipt bytes do not match the object size.")
        parts: list[dict[str, Any]] = []
        for receipt in ordered:
            if not receipt.etag:
                raise UploadContractError("Every S3 transfer unit requires an ETag receipt.")
            part: dict[str, Any] = {
                "PartNumber": receipt.unit_number,
                "ETag": receipt.etag,
            }
            if receipt.checksum_sha256 and not self.compatible:
                part["ChecksumSHA256"] = receipt.checksum_sha256
            parts.append(part)
        if self.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError("Completion requires single-attempt S3 SDK configuration.")
        try:
            response = self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=upload.object_key,
                UploadId=upload.provider_id,
                MultipartUpload={"Parts": parts},
                IfNoneMatch="*",
            )
        except Exception as exc:
            self._raise_provider_error(exc)
            raise
        if not isinstance(response, dict):
            raise UploadContractError("Completion has no original immutable receipt.")
        version = immutable_object_version(response.get("VersionId"))
        if len(version) > 256:
            raise UploadContractError("The completion version exceeds its receipt budget.")
        return CompletedObject(
            self._uri(upload.object_key),
            upload.byte_size,
            response.get("ChecksumSHA256"),
            {"version_id": version},
        )

    def abort_upload(self, upload: ProviderUpload) -> None:
        key = self._key(upload.object_key)
        if (
            upload.protocol != "multipart"
            or not isinstance(upload.provider_id, str)
            or not upload.provider_id.strip()
            or any(ord(c) < 32 or ord(c) == 127 for c in upload.provider_id)
        ):
            raise UploadContractError("Cancellation requires the original multipart identity.")
        if self.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError("Cancellation requires single-attempt S3 SDK configuration.")
        if self._remaining_parts(key, upload.provider_id) is None:
            return
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=key, UploadId=upload.provider_id
            )
        except Exception as exc:
            if not self._missing_upload(exc):
                self._raise_provider_error(exc)
                raise
        remaining = self._remaining_parts(key, upload.provider_id)
        if remaining is not None:
            raise DeletionPending("The original S3 multipart session still exists.")

    @staticmethod
    def _missing_upload(exc):
        response = getattr(exc, "response", None) or {}
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return (
            (response.get("Error") or {}).get("Code") == "NoSuchUpload"
            and type(status) is int
            and status == 404
        )

    def _remaining_parts(self, key, upload_id):
        try:
            result = self.client.list_parts(
                Bucket=self.bucket, Key=key, UploadId=upload_id, MaxParts=1
            )
        except Exception as exc:
            if self._missing_upload(exc):
                return None
            self._raise_provider_error(exc)
            raise
        if (
            type(result.get("IsTruncated")) is not bool
            or result.get("UploadId", upload_id) != upload_id
            or result.get("Key", key) != key
        ):
            raise UploadContractError("S3 returned another or incomplete multipart inventory.")
        return result

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str, *, version: str | None = None) -> ObjectMetadata:
        key = self._key_from_uri(uri)
        params = {"Bucket": self.bucket, "Key": key}
        if version is not None:
            params["VersionId"] = immutable_object_version(version)
        response = self.client.head_object(**params)
        if version is not None and response.get("VersionId") != version:
            raise UploadContractError("Object metadata returned a different or missing version.")
        checksum = response.get("ChecksumSHA256") or (response.get("Metadata") or {}).get(
            "sceptre-sha256"
        )
        return ObjectMetadata(
            uri=self._uri(key),
            byte_size=int(response["ContentLength"]),
            etag=str(response.get("ETag") or "").strip('"') or None,
            checksum=checksum,
            content_type=response.get("ContentType"),
            provider_headers={
                key: str(value)
                for key, value in {
                    "version_id": response.get("VersionId"),
                    "request_charged": response.get("RequestCharged"),
                    "sceptre_grant": (response.get("Metadata") or {}).get("sceptre_grant"),
                }.items()
                if value is not None
            },
        )

    def open_stream(
        self, uri: str, byte_range: ByteRange | None = None, *, version: str | None = None
    ):
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": self._key_from_uri(uri)}
        if version is not None:
            params["VersionId"] = immutable_object_version(version)
        if byte_range:
            params["Range"] = f"bytes={byte_range.start}-{byte_range.end_inclusive}"
        response = self.client.get_object(**params)
        if version is not None and response.get("VersionId") != version:
            response["Body"].close()
            raise UploadContractError("Object store returned a different or missing version.")
        return response["Body"]

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        key = self._key_from_uri(uri)
        self.client.upload_fileobj(source, self.bucket, key)
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        self.client.put_object(Bucket=self.bucket, Key=normalized, Body=value)
        return self.stat(self._uri(normalized))

    def create_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        if self.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError("Byte creation requires single-attempt S3 SDK configuration.")
        response = self.client.put_object(
            Bucket=self.bucket, Key=normalized, Body=value, IfNoneMatch="*"
        )
        if not isinstance(response, dict):
            raise UploadContractError("Byte creation has no original immutable receipt.")
        version = immutable_object_version(response.get("VersionId"))
        if len(version) > 256:
            raise UploadContractError("The byte creation version exceeds its receipt budget.")
        return ObjectMetadata(
            uri=self._uri(normalized),
            byte_size=len(value),
            etag=str(response.get("ETag") or "").strip('"') or None,
            checksum=response.get("ChecksumSHA256"),
            provider_headers={"version_id": version},
        )

    def read_bytes(self, uri: str) -> bytes:
        response = self.open_stream(uri)
        try:
            return response.read()
        finally:
            response.close()

    def read_head(self, uri: str, byte_count: int = 4096) -> bytes:
        if byte_count <= 0:
            return b""
        response = self.open_stream(uri, ByteRange(0, byte_count - 1))
        try:
            return response.read()
        finally:
            response.close()

    def exists(self, uri: str) -> bool:
        try:
            self.stat(uri)
            return True
        except ValueError:
            return False
        except Exception as exc:
            if self._is_not_found(exc):
                return False
            raise

    def size(self, uri: str) -> int:
        return self.stat(uri).byte_size

    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor:
        key = self._key_from_uri(uri)
        options: dict[str, object] = {}
        if self.endpoint_url:
            options["client_kwargs"] = {"endpoint_url": self.endpoint_url}
        if self.access_key:
            options["key"] = self.access_key
        if self.secret_key:
            options["secret"] = self.secret_key
        if self.region:
            options["region"] = self.region
        return RayDataSourceDescriptor(
            path=f"s3://{self.bucket}/{key}",
            filesystem_options=options,
            provider=self.driver_name,
        )

    def healthcheck(self) -> HealthcheckResult:
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception as exc:
            return HealthcheckResult(False, self.driver_name, type(exc).__name__)
        return HealthcheckResult(True, self.driver_name)

    def ensure_bucket(self) -> None:
        if self.healthcheck().healthy:
            return
        params: dict[str, Any] = {"Bucket": self.bucket}
        if self.region and self.region != "us-east-1" and not self.compatible:
            params["CreateBucketConfiguration"] = {"LocationConstraint": self.region}
        self.client.create_bucket(**params)

    def prefix_absent(self, prefix: str) -> bool:
        prefix = self._key(prefix).rstrip("/") + "/"
        for operation, kinds, limits in (
            ("list_multipart_uploads", ("Uploads",), {"MaxUploads": 1}),
            ("list_object_versions", ("Versions", "DeleteMarkers"), {"MaxKeys": 1}),
        ):
            page = getattr(self.client, operation)(Bucket=self.bucket, Prefix=prefix, **limits)
            if (
                not isinstance(page, dict)
                or not isinstance(page.get("ResponseMetadata"), dict)
                or page["ResponseMetadata"].get("HTTPStatusCode") != 200
                or type(page.get("IsTruncated")) is not bool
            ):
                raise UploadContractError("S3 prefix absence needs a complete native inventory.")
            for kind in kinds:
                items = page.get(kind, [])
                if not isinstance(items, list):
                    raise UploadContractError("S3 prefix inventory is malformed.")
                for item in items:
                    if (
                        not isinstance(item, dict)
                        or not isinstance(item.get("Key"), str)
                        or not item["Key"].startswith(prefix)
                    ):
                        raise UploadContractError("S3 prefix inventory escaped its original root.")
                if items:
                    return False
            if page["IsTruncated"]:
                raise DeletionPending("S3 prefix absence inventory is incomplete.")
        return True

    def delete_prefix(self, prefix: str) -> None:
        self._delete_matching(self._key(prefix).rstrip("/") + "/", exact=False)

    def delete(self, uri: str, *, version: str | None = None) -> None:
        key = self._key_from_uri(uri)
        if version is None:
            self._delete_matching(key, exact=True)
            return
        version = immutable_object_version(version)
        if self.client.meta.config.retries.get("total_max_attempts") != 1:
            raise UploadContractError(
                "Version retirement requires single-attempt S3 SDK configuration."
            )
        if not self._version_present(key, version):
            return
        reply = self.client.delete_object(Bucket=self.bucket, Key=key, VersionId=version)
        if reply.get("VersionId") != version:
            raise UploadContractError("S3 returned another or missing deletion version.")
        if self._version_present(key, version):
            raise DeletionPending("The original S3 version is still stored.")

    def _version_present(self, key, version):
        markers = {}
        seen = set()
        # shortcut: hold inventories beyond 100 pages; add resumable scans for larger histories.
        for _ in range(100):
            page = self.client.list_object_versions(
                Bucket=self.bucket, Prefix=key, MaxKeys=1000, **markers
            )
            if type(page.get("IsTruncated")) is not bool:
                raise UploadContractError("S3 version inventory is incomplete.")
            for kind in ("Versions", "DeleteMarkers"):
                for item in page.get(kind, []):
                    name = item.get("Key")
                    if not isinstance(name, str) or not name.startswith(key):
                        raise UploadContractError("S3 version inventory escaped its prefix.")
                    if name == key:
                        found_version = item.get("VersionId")
                        if not isinstance(found_version, str) or not found_version:
                            raise UploadContractError("S3 returned an unversioned inventory row.")
                        if found_version == version:
                            return True
            if not page["IsTruncated"]:
                return False
            token = (page.get("NextKeyMarker"), page.get("NextVersionIdMarker"))
            if (
                not isinstance(token[0], str)
                or not token[0]
                or token[1] is not None
                and not isinstance(token[1], str)
                or token in seen
            ):
                raise UploadContractError("S3 version inventory did not advance.")
            seen.add(token)
            markers = dict(KeyMarker=token[0])
            if token[1]:
                markers["VersionIdMarker"] = token[1]
        raise DeletionPending("S3 version inventory exceeds the bounded qualification scan.")

    def _deletion_items(self, operation, prefix, exact, kinds, **limits):
        options = dict(Bucket=self.bucket, Prefix=prefix, **limits)
        pages = (
            self.client.get_paginator(operation).paginate(**options)
            if exact
            else [getattr(self.client, operation)(**options)]
        )
        for page in pages:
            items = [
                item
                for kind in kinds
                for item in page.get(kind, [])
                if not exact or item["Key"] == prefix
            ]
            if items:
                return items
            if not exact and page.get("IsTruncated"):
                raise RuntimeError("Cleanup listing is incomplete.")
        return []

    def _delete_matching(self, prefix, *, exact):
        previous = None
        while True:
            uploads = self._deletion_items(
                "list_multipart_uploads", prefix, exact, ("Uploads",), MaxUploads=1000
            )
            if not uploads:
                break
            pending = frozenset((item["Key"], item["UploadId"]) for item in uploads)
            if pending == previous or any(not key.startswith(prefix) for key, _ in pending):
                raise RuntimeError("Multipart cleanup made no progress or escaped its prefix.")
            previous = pending
            for key, upload_id in pending:
                try:
                    self.client.abort_multipart_upload(
                        Bucket=self.bucket, Key=key, UploadId=upload_id
                    )
                    parts = self.client.list_parts(
                        Bucket=self.bucket, Key=key, UploadId=upload_id, MaxParts=1
                    )
                    if parts.get("Parts") or parts.get("IsTruncated"):
                        raise RuntimeError(
                            "Multipart cleanup still has stored parts; retry required."
                        )
                except Exception as exc:
                    if not self._missing_upload(exc):
                        raise
        previous = None
        while True:
            objects = [
                {"Key": item["Key"], "VersionId": item["VersionId"]}
                for item in self._deletion_items(
                    "list_object_versions",
                    prefix,
                    exact,
                    ("Versions", "DeleteMarkers"),
                    MaxKeys=1000,
                )
            ]
            if not objects:
                return
            pending = frozenset((item["Key"], item["VersionId"]) for item in objects)
            if pending == previous or any(not key.startswith(prefix) for key, _ in pending):
                raise RuntimeError("Version cleanup made no progress or escaped its prefix.")
            previous = pending
            result = self.client.delete_objects(
                Bucket=self.bucket, Delete={"Objects": objects, "Quiet": True}
            )
            if result.get("Errors"):
                raise RuntimeError("Some stored versions could not be deleted; cleanup will retry.")

    def _validate_cursor(self, upload: ProviderUpload, cursor: TransferCursor) -> None:
        self._ensure_active(upload)
        if cursor.unit_number < 1 or cursor.unit_number > 10_000:
            raise UploadContractError("S3 part numbers must be between 1 and 10,000.")
        if cursor.offset < 0 or cursor.length <= 0:
            raise UploadContractError("Transfer byte ranges must be positive.")
        if cursor.offset + cursor.length > upload.byte_size:
            raise UploadContractError("Transfer unit exceeds the declared object size.")
        unit_size = int(upload.state.get("transfer_unit_size") or cursor.length)
        expected_offset = (cursor.unit_number - 1) * unit_size
        expected_length = min(unit_size, upload.byte_size - expected_offset)
        if cursor.offset != expected_offset or cursor.length != expected_length:
            raise UploadContractError("Transfer unit offset does not match its part number.")
        if cursor.checksum_sha256 and len(cursor.checksum_sha256) != 64:
            raise UploadContractError("Transfer-unit SHA-256 must be a 64-character hex digest.")

    @staticmethod
    def _ensure_active(upload: ProviderUpload) -> None:
        if upload.expires_at <= datetime.now(UTC):
            raise UploadExpired("The provider upload session has expired.")

    @staticmethod
    def _instruction_id(
        upload: ProviderUpload, cursor: TransferCursor, expires_at: datetime
    ) -> str:
        payload = (
            f"{upload.provider_id}:{cursor.unit_number}:{cursor.offset}:{cursor.length}:"
            f"{cursor.checksum_sha256}:{int(expires_at.timestamp())}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def _uri(self, key: str) -> str:
        scheme = "s3c" if self.compatible else "s3"
        return f"{scheme}://{self.bucket}/{self._key(key)}"

    def _key_from_uri(self, uri: str) -> str:
        accepted = [f"s3c://{self.bucket}/", f"minio://{self.bucket}/"]
        if not self.compatible:
            accepted = [f"s3://{self.bucket}/"]
        for prefix in accepted:
            if uri.startswith(prefix):
                return self._key(uri.removeprefix(prefix))
        raise ValueError("Object URI does not belong to the configured S3 store.")

    @staticmethod
    def _key(value: str) -> str:
        key = value.strip("/")
        if not key or key.startswith("../") or "/../" in key:
            raise ValueError("Object keys must remain inside the configured bucket.")
        return key

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        response = getattr(exc, "response", {}) or {}
        code = str((response.get("Error") or {}).get("Code") or "")
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        return code in {"404", "NoSuchKey", "NoSuchUpload", "NotFound"} or status == 404

    @staticmethod
    def _raise_provider_error(exc: Exception) -> None:
        response = getattr(exc, "response", {}) or {}
        code = str((response.get("Error") or {}).get("Code") or "")
        status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
        headers = (response.get("ResponseMetadata") or {}).get("HTTPHeaders") or {}
        if status == 429 or code in {"SlowDown", "Throttling", "ThrottlingException"}:
            raw_retry = headers.get("retry-after")
            retry_after = float(raw_retry) if raw_retry else None
            raise UploadThrottled(
                "The object provider throttled the transfer request.",
                retry_after_seconds=retry_after,
            ) from exc
        if code in {"ExpiredToken", "RequestExpired"} or S3ObjectStoreDriver._missing_upload(exc):
            raise UploadExpired("The provider upload session is no longer usable.") from exc
