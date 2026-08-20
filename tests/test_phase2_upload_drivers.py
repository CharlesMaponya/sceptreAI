from __future__ import annotations

import base64
import io
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import (
    BeginUpload,
    ByteRange,
    ObjectMetadata,
    ProviderUpload,
    TransferCursor,
    TransferReceipt,
    UploadContractError,
    UploadExpired,
    UploadThrottled,
)
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver


def _begin(*, size: int = 10, part: int = 6) -> BeginUpload:
    return BeginUpload(
        object_key="projects/p/raw/file.csv",
        byte_size=size,
        content_type="text/csv",
        checksum_sha256="a" * 64,
        transfer_unit_size=part,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        origin="https://app.example.test",
    )


class ProviderError(Exception):
    def __init__(self, code: str, status: int, retry_after: str | None = None) -> None:
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {
                "HTTPStatusCode": status,
                "HTTPHeaders": {"retry-after": retry_after} if retry_after else {},
            },
        }


class S3Client:
    def __init__(self) -> None:
        self.parts: list[dict[str, object]] = []
        self.objects: dict[str, bytes] = {}
        self.aborts = 0
        self.completed: dict[str, object] | None = None
        self.fail: Exception | None = None
        self.bucket_exists = True
        self.multipart_uploads: list[dict[str, object]] = []
        self.create_count = 0

    def list_multipart_uploads(self, **_kwargs: object) -> dict[str, object]:
        if self.fail:
            raise self.fail
        return {"Uploads": list(self.multipart_uploads)}

    def create_multipart_upload(self, **kwargs: object) -> dict[str, str]:
        if self.fail:
            raise self.fail
        self.create_count += 1
        self.created = kwargs
        upload_id = f"upload-{self.create_count}"
        self.multipart_uploads.append(
            {"Key": kwargs["Key"], "UploadId": upload_id, "Initiated": self.create_count}
        )
        return {"UploadId": upload_id}

    def generate_presigned_url(self, *_args: object, **kwargs: object) -> str:
        self.presigned = kwargs
        return "https://objects.example.test/part?signature=secret"

    def list_parts(self, **_kwargs: object) -> dict[str, object]:
        if self.fail:
            raise self.fail
        return {"Parts": self.parts}

    def complete_multipart_upload(self, **kwargs: object) -> None:
        self.completed = kwargs
        self.objects[str(kwargs["Key"])] = b"0123456789"

    def abort_multipart_upload(self, **_kwargs: object) -> None:
        if self.fail:
            raise self.fail
        self.aborts += 1

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        del Bucket
        if Key not in self.objects:
            raise ProviderError("NoSuchKey", 404)
        return {
            "ContentLength": len(self.objects[Key]),
            "ETag": '"etag-final"',
            "ChecksumSHA256": "provider-sha",
            "ContentType": "text/csv",
            "VersionId": "v1",
        }

    def get_object(self, *, Bucket: str, Key: str, Range: str | None = None) -> dict[str, object]:
        del Bucket
        value = self.objects[Key]
        if Range:
            start, end = [int(item) for item in Range.removeprefix("bytes=").split("-")]
            value = value[start : end + 1]
        return {"Body": io.BytesIO(value)}

    def upload_fileobj(self, source: io.BytesIO, bucket: str, key: str) -> None:
        del bucket
        self.objects[key] = source.read()

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        del Bucket
        self.objects[Key] = Body

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        del Bucket
        self.objects.pop(Key, None)

    def head_bucket(self, **_kwargs: object) -> None:
        if self.fail:
            raise self.fail
        if not self.bucket_exists:
            raise ProviderError("NotFound", 404)

    def create_bucket(self, **_kwargs: object) -> None:
        self.bucket_exists = True


def test_s3_native_full_contract_and_scoped_instructions() -> None:
    client = S3Client()
    driver = S3ObjectStoreDriver(bucket="bucket", region="us-east-1", client=client)
    upload = driver.begin_upload(_begin())
    assert driver.begin_upload(_begin()).provider_id == upload.provider_id
    assert client.create_count == 1
    cursor = TransferCursor(1, 0, 6, "b" * 64)

    instruction = driver.create_transfer_instruction(upload, cursor)
    assert instruction.method == "PUT"
    assert instruction.url.startswith("https://objects.example.test/")
    checksum = base64.b64encode(bytes.fromhex("b" * 64)).decode()
    assert instruction.headers["x-amz-checksum-sha256"] == checksum
    assert "unit_checksum" in instruction.enforced_controls

    client.parts = [
        {"PartNumber": 2, "Size": 4, "ETag": '"e2"', "ChecksumSHA256": "c2"},
        {"PartNumber": 1, "Size": 6, "ETag": '"e1"', "ChecksumSHA256": "c1"},
    ]
    progress = driver.query_progress(upload)
    assert progress.complete and progress.confirmed_bytes == 10
    assert [receipt.offset for receipt in progress.receipts] == [0, 6]
    completed = driver.complete_upload(upload, list(progress.receipts))
    assert completed.uri == "s3://bucket/projects/p/raw/file.csv"
    assert driver.uri_for_key(upload.object_key) == completed.uri
    assert client.completed is not None

    assert driver.read_bytes(completed.uri) == b"0123456789"
    assert driver.read_head(completed.uri, 3) == b"012"
    assert driver.open_stream(completed.uri, ByteRange(2, 4)).read() == b"234"
    assert driver.size(completed.uri) == 10
    assert driver.exists(completed.uri)
    descriptor = driver.dataframe_source(completed.uri)
    assert descriptor.path == completed.uri and descriptor.provider == "aws_s3"
    driver.put_stream(completed.uri, io.BytesIO(b"replacement"))
    assert driver.put_bytes("another.csv", b"a").byte_size == 1
    driver.delete("s3://bucket/another.csv")
    assert not driver.exists("s3://bucket/another.csv")
    assert driver.healthcheck().healthy
    driver.abort_upload(upload)
    assert client.aborts == 1


def test_s3_compatible_and_failure_contracts() -> None:
    client = S3Client()
    presigner = S3Client()
    driver = S3ObjectStoreDriver(
        bucket="bucket",
        endpoint_url="http://internal:8333",
        public_endpoint_url="https://storage.test",
        access_key="key",
        secret_key="secret",
        compatible=True,
        client=client,
        presign_client=presigner,
    )
    upload = driver.begin_upload(_begin())
    instruction = driver.create_transfer_instruction(upload, TransferCursor(1, 0, 6, "c" * 64))
    assert "unit_checksum" in instruction.unsupported_controls
    assert presigner.presigned["Params"]["PartNumber"] == 1
    assert driver.capabilities().supports_provider_lifecycle is False

    for cursor in (
        TransferCursor(0, 0, 6),
        TransferCursor(1, 1, 6),
        TransferCursor(1, 0, 11),
        TransferCursor(1, 0, 6, "bad"),
    ):
        with pytest.raises(UploadContractError):
            driver.create_transfer_instruction(upload, cursor)
    with pytest.raises(UploadContractError, match="requires provider receipts"):
        driver.complete_upload(upload, [])
    with pytest.raises(UploadContractError, match="contiguous"):
        driver.complete_upload(upload, [TransferReceipt(2, 6, 4, "e2")])
    with pytest.raises(UploadContractError, match="ETag"):
        driver.complete_upload(upload, [TransferReceipt(1, 0, 10)])
    with pytest.raises(ValueError, match="configured S3"):
        driver.stat("s3c://other/key")

    expired = ProviderUpload(
        upload.provider_id,
        upload.object_key,
        upload.protocol,
        upload.byte_size,
        datetime.now(UTC) - timedelta(seconds=1),
        upload.state,
    )
    with pytest.raises(UploadExpired):
        driver.query_progress(expired)
    client.fail = ProviderError("SlowDown", 429, "2.5")
    with pytest.raises(UploadThrottled) as throttled:
        driver.query_progress(upload)
    assert throttled.value.retry_after_seconds == 2.5
    client.fail = ProviderError("NoSuchUpload", 404)
    with pytest.raises(UploadExpired):
        driver.query_progress(upload)
    driver.abort_upload(upload)
    client.fail = RuntimeError("offline")
    assert not driver.healthcheck().healthy


class GCSResponse:
    def __init__(self, status_code: int, headers: dict[str, str] | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


class GCSHttp:
    def __init__(self) -> None:
        self.response = GCSResponse(308, {"Range": "bytes=0-5"})
        self.deleted = 204

    def put(self, *_args: object, **_kwargs: object) -> GCSResponse:
        return self.response

    def delete(self, *_args: object, **_kwargs: object) -> GCSResponse:
        return GCSResponse(self.deleted)


class GCSBlob:
    def __init__(self, bucket: GCSBucket, name: str) -> None:
        self.bucket = bucket
        self.name = name
        self.size = len(bucket.objects.get(name, b""))
        self.etag = "etag"
        self.crc32c = "crc"
        self.md5_hash = "md5"
        self.content_type = "text/csv"
        self.generation = 1
        self.metageneration = 2

    def create_resumable_upload_session(self, **kwargs: object) -> str:
        self.session = kwargs
        return "https://storage.googleapis.test/session-secret"

    def open(self, _mode: str):
        return io.BytesIO(self.bucket.objects[self.name])

    def download_as_bytes(
        self, start: int | None = None, end: int | None = None, **_kwargs: object
    ) -> bytes:
        value = self.bucket.objects[self.name]
        return value[slice(start, None if end is None else end + 1)]

    def upload_from_file(self, source: io.BytesIO, **_kwargs: object) -> None:
        self.bucket.objects[self.name] = source.read()

    def upload_from_string(self, value: bytes) -> None:
        self.bucket.objects[self.name] = value

    def exists(self) -> bool:
        return self.name in self.bucket.objects

    def delete(self) -> None:
        self.bucket.objects.pop(self.name, None)


class GCSBucket:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {"projects/p/raw/file.csv": b"0123456789"}

    def blob(self, name: str) -> GCSBlob:
        return GCSBlob(self, name)

    def get_blob(self, name: str) -> GCSBlob | None:
        return self.blob(name) if name in self.objects else None


class GCSClient:
    def __init__(self) -> None:
        self.bucket_value = GCSBucket()
        self.healthy = True

    def bucket(self, _name: str) -> GCSBucket:
        return self.bucket_value

    def get_bucket(self, _name: str) -> GCSBucket:
        if not self.healthy:
            raise RuntimeError("offline")
        return self.bucket_value


def test_gcs_offset_contract_and_failures() -> None:
    client, http = GCSClient(), GCSHttp()
    driver = GCSObjectStoreDriver(bucket="bucket", client=client, http_client=http)
    upload = driver.begin_upload(_begin())
    instruction = driver.create_transfer_instruction(upload, TransferCursor(1, 0, 6))
    assert instruction.headers["Content-Range"] == "bytes 0-5/10"
    assert driver.query_progress(upload).confirmed_bytes == 6
    http.response = GCSResponse(200)
    assert driver.complete_upload(upload, []).byte_size == 10
    with pytest.raises(UploadContractError, match="synthetic"):
        driver.complete_upload(upload, [TransferReceipt(1, 0, 10)])
    with pytest.raises(UploadContractError):
        driver.create_transfer_instruction(upload, TransferCursor(2, 5, 4))

    uri = "gs://bucket/projects/p/raw/file.csv"
    assert driver.uri_for_key(upload.object_key) == uri
    assert driver.read_bytes(uri) == b"0123456789"
    assert driver.read_head(uri, 2) == b"01"
    assert driver.open_stream(uri, ByteRange(2, 3)).read() == b"23"
    assert driver.exists(uri) and driver.size(uri) == 10
    assert driver.dataframe_source(uri).provider == "gcs"
    driver.put_stream(uri, io.BytesIO(b"abc"))
    assert driver.put_bytes("other.csv", b"x").byte_size == 1
    driver.delete("gs://bucket/other.csv")
    assert not driver.exists("gs://bucket/other.csv")
    assert driver.healthcheck().healthy
    client.healthy = False
    assert not driver.healthcheck().healthy

    for response, error in (
        (GCSResponse(410), UploadExpired),
        (GCSResponse(429, {"Retry-After": "3"}), UploadThrottled),
        (GCSResponse(500), UploadContractError),
        (GCSResponse(308, {"Range": "invalid"}), UploadContractError),
    ):
        http.response = response
        with pytest.raises(error):
            driver.query_progress(upload)
    http.deleted = 500
    with pytest.raises(UploadContractError):
        driver.abort_upload(upload)
    with pytest.raises(ValueError, match="configured GCS"):
        driver.stat("gs://other/key")
    with pytest.raises(FileNotFoundError):
        driver.stat("gs://bucket/missing.csv")


class AzureBlob:
    def __init__(self, name: str, objects: dict[str, bytes]) -> None:
        self.blob_name, self.objects = name, objects
        self.blocks: list[SimpleNamespace] = []
        self.committed: list[str] = []

    def get_block_list(self, **_kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(committed_blocks=[], uncommitted_blocks=self.blocks)

    def commit_block_list(self, values: list[str]) -> None:
        self.committed = values
        self.objects[self.blob_name] = b"0123456789"

    def get_blob_properties(self) -> SimpleNamespace:
        value = self.objects[self.blob_name]
        return SimpleNamespace(
            size=len(value),
            etag='"etag"',
            version_id="v1",
            content_settings=SimpleNamespace(content_md5=b"md5", content_type="text/csv"),
        )

    def download_blob(self, offset: int = 0, length: int | None = None) -> SimpleNamespace:
        value = self.objects[self.blob_name][offset : offset + length if length else None]
        return SimpleNamespace(readall=lambda: value)

    def upload_blob(self, source: object, **_kwargs: object) -> None:
        self.objects[self.blob_name] = source.read() if hasattr(source, "read") else bytes(source)

    def exists(self) -> bool:
        return self.blob_name in self.objects

    def delete_blob(self, **_kwargs: object) -> None:
        self.objects.pop(self.blob_name, None)


class AzureContainer:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {"projects/p/raw/file.csv": b"0123456789"}
        self.blobs: dict[str, AzureBlob] = {}
        self.healthy = True

    def get_blob_client(self, name: str) -> AzureBlob:
        return self.blobs.setdefault(name, AzureBlob(name, self.objects))

    def get_container_properties(self) -> dict[str, object]:
        if not self.healthy:
            raise RuntimeError("offline")
        return {}


class AzureService:
    def __init__(self) -> None:
        self.container = AzureContainer()

    def get_container_client(self, _name: str) -> AzureContainer:
        return self.container

    def get_user_delegation_key(self, *_args: object) -> str:
        return "delegation"


def test_azure_block_contract_and_object_operations() -> None:
    service = AzureService()
    driver = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        account_name="account",
        container="data",
        credential=object(),
        service_client=service,
        sas_factory=lambda **_kwargs: "sig=secret",
    )
    upload = driver.begin_upload(_begin())
    instruction = driver.create_transfer_instruction(upload, TransferCursor(1, 0, 6))
    assert "comp=block" in instruction.url and "sig=secret" in instruction.url
    blob = service.container.get_blob_client(upload.object_key)
    blob.blocks = [
        SimpleNamespace(id=driver._block_id(2), size=4, etag="e2"),
        SimpleNamespace(id=driver._block_id(1), size=6, etag="e1"),
    ]
    progress = driver.query_progress(upload)
    assert progress.complete and progress.confirmed_bytes == 10
    completed = driver.complete_upload(upload, list(progress.receipts))
    assert completed.uri.startswith("az://account/data/")
    assert driver.uri_for_key(upload.object_key) == completed.uri
    assert blob.committed == [driver._block_id(1), driver._block_id(2)]
    assert driver.read_bytes(completed.uri) == b"0123456789"
    assert driver.read_head(completed.uri, 2) == b"01"
    assert driver.open_stream(completed.uri, ByteRange(3, 5)).read() == b"345"
    assert driver.exists(completed.uri) and driver.size(completed.uri) == 10
    assert driver.dataframe_source(completed.uri).provider == "azure_blob"
    driver.put_stream(completed.uri, io.BytesIO(b"stream"))
    assert driver.put_bytes("other.csv", b"x").byte_size == 1
    driver.delete("az://account/data/other.csv")
    assert not driver.exists("az://account/data/other.csv")
    assert driver.healthcheck().healthy
    service.container.healthy = False
    assert not driver.healthcheck().healthy
    driver.abort_upload(upload)

    with pytest.raises(UploadContractError):
        driver.create_transfer_instruction(upload, TransferCursor(2, 5, 4))
    with pytest.raises(UploadContractError, match="contiguous"):
        driver.complete_upload(upload, [TransferReceipt(2, 6, 4)])
    with pytest.raises(UploadContractError, match="unrecognized"):
        driver._unit_number("not-base64")
    with pytest.raises(ValueError, match="configured Azure"):
        driver.stat("az://other/data/key")


def test_gcs_contract_edge_branches(monkeypatch) -> None:
    client, http = GCSClient(), GCSHttp()
    driver = GCSObjectStoreDriver(bucket="bucket", client=client, http_client=http)
    upload = driver.begin_upload(_begin())
    uri = "gs://bucket/projects/p/raw/file.csv"

    assert driver.capabilities().protocol == "resumable_offset"
    assert driver.open_stream(uri).read() == b"0123456789"
    assert driver.read_head(uri, 0) == b""
    http.deleted = 404
    driver.abort_upload(upload)

    http.response = GCSResponse(308)
    assert driver.query_progress(upload).confirmed_bytes == 0
    with pytest.raises(UploadContractError, match="not confirmed"):
        driver.complete_upload(upload, [])

    http.response = GCSResponse(201)
    monkeypatch.setattr(
        driver,
        "stat",
        lambda object_uri: ObjectMetadata(uri=object_uri, byte_size=9),
    )
    with pytest.raises(UploadContractError, match="unexpected size"):
        driver.complete_upload(upload, [])

    with pytest.raises(UploadContractError, match="one-based"):
        driver.create_transfer_instruction(upload, TransferCursor(0, 0, 6))
    expired = ProviderUpload(
        upload.provider_id,
        upload.object_key,
        upload.protocol,
        upload.byte_size,
        datetime.now(UTC) - timedelta(seconds=1),
        upload.state,
    )
    with pytest.raises(UploadExpired):
        driver.create_transfer_instruction(expired, TransferCursor(1, 0, 6))

    http.response = GCSResponse(429, {"Retry-After": "invalid"})
    with pytest.raises(UploadThrottled) as throttled:
        driver.query_progress(upload)
    assert throttled.value.retry_after_seconds is None
    for key in ("", "../bad", "folder/../bad"):
        with pytest.raises(ValueError, match="inside"):
            driver._key(key)


class AzureDeleteError(Exception):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


def test_azure_contract_edge_branches(monkeypatch) -> None:
    service = AzureService()
    driver = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        account_name="account",
        container="data",
        credential=object(),
        service_client=service,
        sas_factory=lambda **_kwargs: "sig=secret",
    )
    upload = driver.begin_upload(_begin())
    uri = "az://account/data/projects/p/raw/file.csv"
    blob = service.container.get_blob_client(upload.object_key)

    assert driver.capabilities().protocol == "block_list"
    assert driver.open_stream(uri).read() == b"0123456789"
    assert driver.read_head(uri, 0) == b""

    properties = SimpleNamespace(
        size=10,
        etag=None,
        version_id=None,
        content_settings=None,
    )
    monkeypatch.setattr(blob, "get_blob_properties", lambda: properties)
    metadata = driver.stat(uri)
    assert metadata.checksum is None and metadata.etag is None
    assert metadata.provider_headers["version_id"] == ""

    monkeypatch.setattr(
        driver,
        "stat",
        lambda object_uri: ObjectMetadata(uri=object_uri, byte_size=9),
    )
    receipts = [TransferReceipt(1, 0, 6), TransferReceipt(2, 6, 4)]
    with pytest.raises(UploadContractError, match="unexpected size"):
        driver.complete_upload(upload, receipts)

    for unit_number in (0, 50_001):
        with pytest.raises(UploadContractError, match="block numbers"):
            driver.create_transfer_instruction(upload, TransferCursor(unit_number, 0, 6))
    expired = ProviderUpload(
        upload.provider_id,
        upload.object_key,
        upload.protocol,
        upload.byte_size,
        datetime.now(UTC) - timedelta(seconds=1),
        upload.state,
    )
    with pytest.raises(UploadExpired):
        driver.query_progress(expired)

    def fail_delete(status_code: int):
        def fail(**_kwargs: object) -> None:
            raise AzureDeleteError(status_code)

        return fail

    monkeypatch.setattr(blob, "delete_blob", fail_delete(404))
    driver.abort_upload(upload)
    monkeypatch.setattr(blob, "delete_blob", fail_delete(500))
    with pytest.raises(AzureDeleteError):
        driver.abort_upload(upload)
    for key in ("", "../bad", "folder/../bad"):
        with pytest.raises(ValueError, match="inside"):
            driver._key(key)
