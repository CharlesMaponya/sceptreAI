from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.core.config import Settings
from automl_api.storage.contracts import (
    BeginUpload,
    ByteRange,
    TransferCursor,
    UploadCapabilityUnavailable,
)
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.storage.object_store import (
    EmbeddedObjectStore,
    MinioObjectStore,
    ObjectStore,
    get_object_store,
)
from automl_api.storage.s3 import S3ObjectStoreDriver


class _MissingObject(Exception):
    response = {"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}}


class _Body(io.BytesIO):
    pass


class _BotoClient:
    def __init__(self) -> None:
        self.bucket_present = False
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail = False
        self.responses: list[_Body] = []

    def head_bucket(self, *, Bucket: str) -> None:
        if not self.bucket_present:
            raise RuntimeError(f"missing bucket {Bucket}")

    def create_bucket(self, *, Bucket: str, **_kwargs: object) -> None:
        self.bucket_present = True

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        self.bucket_present = True
        self.objects[(Bucket, Key)] = bytes(Body)

    def upload_fileobj(self, source: object, bucket: str, key: str) -> None:
        self.bucket_present = True
        self.objects[(bucket, key)] = source.read()  # type: ignore[attr-defined]

    def get_object(self, *, Bucket: str, Key: str, Range: str | None = None) -> dict[str, _Body]:
        if self.fail:
            raise RuntimeError("object service unavailable")
        content = self.objects[(Bucket, Key)]
        if Range:
            start, end = (int(value) for value in Range.removeprefix("bytes=").split("-"))
            content = content[start : end + 1]
        response = _Body(content)
        self.responses.append(response)
        return {"Body": response}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if self.fail:
            raise RuntimeError("object service unavailable")
        if (Bucket, Key) not in self.objects:
            raise _MissingObject(Key)
        value = self.objects[(Bucket, Key)]
        return {"ContentLength": len(value), "ETag": "etag", "Metadata": {}}

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        if self.fail:
            raise RuntimeError("object service unavailable")
        self.objects.pop((Bucket, Key), None)


def _settings(tmp_path: object, *, remote: bool = False) -> Settings:
    return Settings(
        object_store_type="minio" if remote else "embedded",
        object_store_endpoint="http://minio.test:9000" if remote else None,
        object_store_access_key="access" if remote else None,
        object_store_secret_key="secret" if remote else None,
        object_store_bucket="datasets",
        local_object_store_path=tmp_path,  # type: ignore[arg-type]
    )


def test_abstract_object_store_contract_is_not_silently_implemented() -> None:
    store = ObjectStore()
    calls = [
        lambda: store.put_bytes("key", b"value"),
        lambda: store.read_bytes("uri"),
        lambda: store.read_head("uri"),
        lambda: store.exists("uri"),
        lambda: store.dataframe_source("uri"),
        lambda: store.delete("uri"),
        store.healthcheck,
        lambda: store.size("uri"),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError):
            call()


def test_embedded_store_round_trip_and_uri_isolation(tmp_path) -> None:
    store = EmbeddedObjectStore(_settings(tmp_path))
    stored = store.put_bytes("/project/data.csv", b"a,b\n1,2\n")
    assert store.uri_for_key("project/data.csv") == stored.uri

    assert stored.storage_path is not None
    assert store.exists(stored.uri)
    assert store.read_bytes(stored.uri) == b"a,b\n1,2\n"
    assert store.read_head(stored.uri, 3) == b"a,b"
    assert store.size(stored.uri) == 8
    source, options = store.dataframe_source(stored.uri)
    assert source == str(stored.storage_path.resolve())
    assert options == {}
    store.healthcheck()

    assert not store.exists("minio://another-bucket/project/data.csv")
    for operation in (
        store.read_bytes,
        store.read_head,
        store.dataframe_source,
        store.delete,
        store.size,
    ):
        with pytest.raises(ValueError, match="configured embedded store"):
            operation("minio://another-bucket/project/data.csv")

    store.delete(stored.uri)
    assert not store.exists(stored.uri)


def test_embedded_driver_upload_contract_and_streaming(tmp_path) -> None:
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="datasets")
    request = BeginUpload(
        object_key="project/data.csv",
        byte_size=6,
        content_type="text/csv",
        checksum_sha256="a" * 64,
        transfer_unit_size=6,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        origin="http://localhost:5173",
    )
    upload = store.begin_upload(request)
    capabilities = store.capabilities()
    assert capabilities.protocol == "single_put"
    assert not capabilities.supports_provider_lifecycle
    assert store.query_progress(upload).confirmed_bytes == 0
    with pytest.raises(UploadCapabilityUnavailable):
        store.create_transfer_instruction(upload, TransferCursor(1, 0, 6))

    uri = f"embedded://datasets/{upload.object_key}"
    stored = store.put_stream(uri, io.BytesIO(b"abcdef"))
    assert stored.uri == uri
    assert store.open_stream(uri).read() == b"abcdef"
    assert store.open_stream(uri, ByteRange(1, 3)).read() == b"bcd"
    assert store.query_progress(upload).complete
    with pytest.raises(ValueError, match="does not accept"):
        store.complete_upload(upload, [object()])  # type: ignore[list-item]
    completed = store.complete_upload(upload, [])
    assert completed.byte_size == 6
    assert store.dataframe_source(f"minio://datasets/{upload.object_key}").provider == "embedded"

    wrong_size = store.begin_upload(
        BeginUpload(
            object_key="project/data.csv",
            byte_size=7,
            content_type="text/csv",
            checksum_sha256="b" * 64,
            transfer_unit_size=7,
            expires_at=request.expires_at,
            origin=request.origin,
        )
    )
    with pytest.raises(ValueError, match="size"):
        store.complete_upload(wrong_size, [])
    store.abort_upload(upload)
    assert not store.exists(uri)
    assert store.healthcheck().healthy

    for bad_key in ("", "../outside", "folder/../outside"):
        with pytest.raises(ValueError, match="inside"):
            store.put_bytes(bad_key, b"x")


def test_remote_store_round_trip_and_credentials(monkeypatch, tmp_path) -> None:
    client = _BotoClient()
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))

    store.ensure_bucket()
    stored = store.put_bytes("/project/data.parquet", b"parquet-bytes")
    assert client.bucket_present
    assert store.read_bytes(stored.uri) == b"parquet-bytes"
    assert store.read_head(stored.uri, 7) == b"parquet"
    assert all(response.closed for response in client.responses)
    assert store.exists(stored.uri)
    assert store.size(stored.uri) == 13
    source, options = store.dataframe_source(stored.uri)
    assert source == "s3://datasets/project/data.parquet"
    assert options == {"client_kwargs": {"endpoint_url": "http://minio.test:9000"}}
    assert store.healthcheck().healthy
    store.delete(stored.uri)
    assert not store.exists(stored.uri)


def test_remote_store_never_falls_back_to_local_storage(
    monkeypatch,
    tmp_path,
) -> None:
    client = _BotoClient()
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))
    uri = store.put_bytes("project/object.csv", b"abcdef").uri
    client.fail = True

    for operation in (store.read_bytes, store.read_head, store.delete, store.size):
        with pytest.raises(RuntimeError, match="object service unavailable"):
            operation(uri)


def test_remote_store_configuration_and_factory_fail_closed(monkeypatch, tmp_path) -> None:
    with pytest.raises(ValueError, match="ENDPOINT"):
        get_object_store(Settings(object_store_type="minio"))
    with pytest.raises(ValueError, match="ENDPOINT"):
        MinioObjectStore(Settings(object_store_type="minio"))
    with pytest.raises(ValueError, match="ACCESS_KEY"):
        MinioObjectStore(
            Settings(object_store_type="minio", object_store_endpoint="http://minio.test")
        )

    assert isinstance(get_object_store(_settings(tmp_path)), EmbeddedObjectStoreDriver)
    client = _BotoClient()
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: client)
    assert isinstance(get_object_store(_settings(tmp_path, remote=True)), S3ObjectStoreDriver)
    with pytest.raises(ValueError, match="Unknown object-store driver"):
        get_object_store(
            Settings(
                environment="production",
                object_store_type="unknown-driver",
                local_object_store_path=tmp_path,
            )
        )


def test_object_store_factory_selects_every_canonical_driver(monkeypatch, tmp_path) -> None:
    sentinels = {
        "s3": object(),
        "gcs": object(),
        "azure": object(),
    }
    monkeypatch.setattr(
        "automl_api.storage.object_store.S3ObjectStoreDriver",
        lambda **_kwargs: sentinels["s3"],
    )
    monkeypatch.setattr(
        "automl_api.storage.object_store.GCSObjectStoreDriver",
        lambda **_kwargs: sentinels["gcs"],
    )
    monkeypatch.setattr(
        "automl_api.storage.object_store.AzureBlobObjectStoreDriver",
        lambda **_kwargs: sentinels["azure"],
    )

    assert get_object_store(Settings(object_store_type="aws_s3")) is sentinels["s3"]
    assert get_object_store(Settings(object_store_type="gcs")) is sentinels["gcs"]
    azure = Settings(
        object_store_type="azure_blob",
        object_store_endpoint="https://account.blob.core.windows.net",
        azure_storage_account="account",
    )
    assert get_object_store(azure) is sentinels["azure"]
    with pytest.raises(ValueError, match="AZURE_STORAGE_ACCOUNT"):
        get_object_store(Settings(object_store_type="azure_blob"))
    with pytest.raises(ValueError, match="forbidden"):
        get_object_store(
            Settings(
                environment="staging",
                object_store_type="embedded",
                local_object_store_path=tmp_path,
            )
        )


def test_remote_store_rejects_foreign_bucket_uris(monkeypatch, tmp_path) -> None:
    client = _BotoClient()
    monkeypatch.setattr("boto3.client", lambda *_args, **_kwargs: client)
    store = MinioObjectStore(_settings(tmp_path, remote=True))
    foreign = "minio://another-bucket/project/data.csv"

    assert not store.exists(foreign)
    for operation in (
        store.read_bytes,
        store.read_head,
        store.dataframe_source,
        store.delete,
        store.size,
    ):
        with pytest.raises(ValueError, match="configured S3 store"):
            operation(foreign)
