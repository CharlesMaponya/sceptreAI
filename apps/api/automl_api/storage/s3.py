from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

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
    UploadThrottled,
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
        self.compatible = compatible
        if compatible and (not endpoint_url or not access_key or not secret_key):
            raise ValueError(
                "S3-compatible storage requires OBJECT_STORE_ENDPOINT, "
                "OBJECT_STORE_ACCESS_KEY, and OBJECT_STORE_SECRET_KEY."
            )
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
            )
        self.client = client
        self.presign_client = presign_client or client
        if (
            presign_client is None
            and public_endpoint_url
            and public_endpoint_url != endpoint_url
        ):
            import boto3
            from botocore.config import Config

            self.presign_client = boto3.client(
                "s3",
                endpoint_url=public_endpoint_url,
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=Config(signature_version="s3v4", retries={"max_attempts": 5}),
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
                provider_id = str(response["UploadId"])
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
        response = self.client.list_multipart_uploads(
            Bucket=self.bucket,
            Prefix=object_key,
        )
        matches = sorted(
            (
                value
                for value in response.get("Uploads", [])
                if value.get("Key") == object_key and value.get("UploadId")
            ),
            key=lambda value: (str(value.get("Initiated") or ""), str(value["UploadId"])),
        )
        return str(matches[0]["UploadId"]) if matches else None

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
        try:
            response = self.client.list_parts(
                Bucket=self.bucket, Key=upload.object_key, UploadId=upload.provider_id
            )
        except Exception as exc:
            self._raise_provider_error(exc)
            raise
        unit_size = int(upload.state["transfer_unit_size"])
        receipts = tuple(
            TransferReceipt(
                unit_number=(unit_number := int(part["PartNumber"])),
                offset=(unit_number - 1) * unit_size,
                length=int(part.get("Size") or 0),
                etag=str(part.get("ETag") or "").strip('"') or None,
                checksum_sha256=part.get("ChecksumSHA256"),
            )
            for part in sorted(response.get("Parts", []), key=lambda value: value["PartNumber"])
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
        try:
            self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=upload.object_key,
                UploadId=upload.provider_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception as exc:
            self._raise_provider_error(exc)
            raise
        metadata = self.stat(self._uri(upload.object_key))
        if metadata.byte_size != upload.byte_size:
            raise UploadContractError("The completed provider object has an unexpected size.")
        return CompletedObject(
            metadata.uri,
            metadata.byte_size,
            metadata.checksum,
            metadata.provider_headers,
        )

    def abort_upload(self, upload: ProviderUpload) -> None:
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=upload.object_key, UploadId=upload.provider_id
            )
        except Exception as exc:
            if not self._is_not_found(exc):
                self._raise_provider_error(exc)

    def uri_for_key(self, object_key: str) -> str:
        return self._uri(object_key)

    def stat(self, uri: str) -> ObjectMetadata:
        key = self._key_from_uri(uri)
        response = self.client.head_object(Bucket=self.bucket, Key=key)
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
                }.items()
                if value is not None
            },
        )

    def open_stream(self, uri: str, byte_range: ByteRange | None = None):
        params: dict[str, Any] = {"Bucket": self.bucket, "Key": self._key_from_uri(uri)}
        if byte_range:
            params["Range"] = f"bytes={byte_range.start}-{byte_range.end_inclusive}"
        return self.client.get_object(**params)["Body"]

    def put_stream(self, uri: str, source) -> ObjectMetadata:
        key = self._key_from_uri(uri)
        self.client.upload_fileobj(source, self.bucket, key)
        return self.stat(uri)

    def put_bytes(self, key: str, value: bytes) -> ObjectMetadata:
        normalized = self._key(key)
        self.client.put_object(Bucket=self.bucket, Key=normalized, Body=value)
        return self.stat(self._uri(normalized))

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

    def delete(self, uri: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key_from_uri(uri))

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
        if code in {"NoSuchUpload", "ExpiredToken", "RequestExpired"}:
            raise UploadExpired("The provider upload session is no longer usable.") from exc
