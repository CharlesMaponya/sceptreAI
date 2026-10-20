"""Immutable object reads use provider versions and bounded SDK reads."""

import io
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import MagicMock

import boto3
import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import ByteRange, UploadCapabilityUnavailable, UploadContractError
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver
from azure.storage.blob import BlobClient, BlobServiceClient
from botocore.stub import Stubber
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage import Blob


class Downloader:
    def __init__(self, data=b"abcdefghij", version="v1"):
        self.source = io.BytesIO(data)
        self.properties = SimpleNamespace(version_id=version)
        self.sizes = []

    def read(self, size=-1):
        self.sizes.append(size)
        return self.source.read(size)

    def readall(self):
        raise AssertionError("Driver must not eagerly buffer the whole object")


@pytest.fixture(params=["s3", "gcs", "azure"])
def provider(request):
    if request.param == "s3":
        with closing(
            boto3.client(
                "s3",
                region_name="us-east-1",
                endpoint_url="https://store.test",
                aws_access_key_id="test-key",
                aws_secret_access_key="test-secret",
            )
        ) as client:
            yield S3ObjectStoreDriver(bucket="bucket", client=client), "s3://bucket/key", client
    elif request.param == "gcs":
        with storage.Client(project="tests", credentials=AnonymousCredentials()) as client:
            yield (
                GCSObjectStoreDriver(bucket="bucket", client=client, http_client=MagicMock()),
                "gs://bucket/key",
                client,
            )
    else:
        with BlobServiceClient(account_url="https://account.blob.core.windows.net") as client:
            yield (
                AzureBlobObjectStoreDriver(
                    account_url="https://account.blob.core.windows.net",
                    container="bucket",
                    account_name="account",
                    service_client=client,
                ),
                "az://account/bucket/key",
                client,
            )


@pytest.mark.parametrize("version", ["", " ", "null", " NULL ", True, 1, "v\r\n1"])
def test_invalid_immutable_version_is_rejected_before_any_provider_call(
    provider, monkeypatch, version
):
    store, uri, client = provider
    call = MagicMock(side_effect=AssertionError("invalid version cannot use provider"))
    if store.driver_name == "aws_s3":
        monkeypatch.setattr(client, "get_object", call)
    elif store.driver_name == "gcs":
        monkeypatch.setattr(store.bucket, "blob", call)
    else:
        monkeypatch.setattr(store.container, "get_blob_client", call)
    with pytest.raises(UploadContractError):
        store.open_stream(uri, version=version)
    call.assert_not_called()


@pytest.mark.parametrize("byte_range", [None, ByteRange(2, 5)])
@pytest.mark.parametrize("provider", ["s3"], indirect=True)
def test_s3_uses_exact_version_and_range_with_actual_sdk_contract(provider, byte_range):
    store, uri, client = provider
    body = io.BytesIO(b"cdef" if byte_range else b"abcdefghij")
    expected = {"Bucket": "bucket", "Key": "key", "VersionId": "v1"}
    if byte_range:
        expected["Range"] = "bytes=2-5"
    with Stubber(client) as stub:
        stub.add_response("get_object", {"Body": body, "VersionId": "v1"}, expected)
        with store.open_stream(uri, byte_range, version="v1") as source:
            assert source.read() == (b"cdef" if byte_range else b"abcdefghij")
        assert body.closed
        stub.assert_no_pending_responses()


@pytest.mark.parametrize("reported", [None, "different"])
def test_s3_rejects_ignored_version_and_closes_body(monkeypatch, reported):
    body = io.BytesIO(b"current")
    client = MagicMock()
    client.get_object.return_value = {"Body": body, **({"VersionId": reported} if reported else {})}
    store = S3ObjectStoreDriver(bucket="bucket", client=client)
    with pytest.raises(UploadContractError):
        store.open_stream("s3://bucket/key", version="v1")
    assert body.closed and client.get_object.call_count == 1


@pytest.mark.parametrize("byte_range", [None, ByteRange(2, 5)])
@pytest.mark.parametrize("provider", ["gcs"], indirect=True)
def test_gcs_actual_blob_is_bound_to_generation(provider, monkeypatch, byte_range):
    store, uri, _ = provider
    seen = []

    def open_blob(blob, mode):
        assert mode == "rb" and blob.generation == 123
        seen.append(blob.generation)
        return io.BytesIO(b"abcdefghij")

    def ranged(blob, **kwargs):
        assert blob.generation == 123
        assert kwargs == {"start": 2, "end": 5, "checksum": None}
        seen.append(blob.generation)
        return b"cdef"

    monkeypatch.setattr(Blob, "open", open_blob)
    monkeypatch.setattr(Blob, "download_as_bytes", ranged)
    with store.open_stream(uri, byte_range, version="123") as stream:
        assert stream.read() == (b"cdef" if byte_range else b"abcdefghij")
    assert seen == [123]


@pytest.mark.parametrize("version", ["0", "-1", "1.5", "1e3", "01", "٣"])
def test_gcs_requires_a_canonical_positive_generation(monkeypatch, version):
    client = MagicMock()
    store = GCSObjectStoreDriver(bucket="bucket", client=client, http_client=MagicMock())
    client.bucket.return_value.blob.reset_mock()
    with pytest.raises(UploadContractError):
        store.open_stream("gs://bucket/key", version=version)
    store.bucket.blob.assert_not_called()


@pytest.mark.parametrize("byte_range", [None, ByteRange(2, 5)])
@pytest.mark.parametrize("provider", ["azure"], indirect=True)
def test_azure_actual_blob_client_pins_version_and_streams_bounded_reads(
    provider, monkeypatch, byte_range
):
    store, uri, _ = provider
    downloader = Downloader(b"cdef" if byte_range else b"abcdefghij")
    seen = []

    def download(blob, **kwargs):
        assert blob.version_id == "v1"
        assert kwargs == ({"offset": 2, "length": 4} if byte_range else {})
        seen.append(True)
        return downloader

    monkeypatch.setattr(BlobClient, "download_blob", download)
    with store.open_stream(uri, byte_range, version="v1") as stream:
        assert downloader.sizes == []
        buffer = bytearray(2)
        assert stream.readinto(buffer) == 2 and bytes(buffer) == (b"cd" if byte_range else b"ab")
        assert stream.read(2) == (b"ef" if byte_range else b"cd")
        assert downloader.sizes == [2, 2]
    assert stream.closed and stream.downloader is None and seen == [True]
    with pytest.raises(ValueError):
        stream.read(1)


@pytest.mark.parametrize("provider", ["azure"], indirect=True)
def test_azure_current_read_stays_lazy_and_accepts_no_version(provider, monkeypatch):
    store, uri, _ = provider
    downloader = Downloader()

    def download(blob, **_kwargs):
        assert blob.version_id is None
        return downloader

    monkeypatch.setattr(BlobClient, "download_blob", download)
    with store.open_stream(uri) as stream:
        assert downloader.sizes == []
        assert stream.read(None) == b"abcdefghij"
    assert downloader.sizes == [-1]


@pytest.mark.parametrize("provider_kind", ["s3", "gcs", "azure"])
@pytest.mark.parametrize("error_type", [FileNotFoundError, PermissionError])
def test_missing_version_never_falls_back_to_current(provider_kind, monkeypatch, error_type):
    calls = []

    def missing(*args, **kwargs):
        calls.append((args, kwargs))
        raise error_type("exact version is unavailable")

    if provider_kind == "s3":
        client = MagicMock()
        client.get_object.side_effect = missing
        store = S3ObjectStoreDriver(bucket="bucket", client=client)
        uri = "s3://bucket/key"
        version = "v1"
    elif provider_kind == "gcs":
        client = storage.Client(project="tests", credentials=AnonymousCredentials())
        store = GCSObjectStoreDriver(bucket="bucket", client=client, http_client=MagicMock())
        uri = "gs://bucket/key"
        version = "123"
        monkeypatch.setattr(Blob, "open", missing)
    else:
        service = BlobServiceClient(account_url="https://account.blob.core.windows.net")
        store = AzureBlobObjectStoreDriver(
            account_url="https://account.blob.core.windows.net",
            container="bucket",
            account_name="account",
            service_client=service,
        )
        uri = "az://account/bucket/key"
        version = "v1"
        monkeypatch.setattr(BlobClient, "download_blob", missing)
    try:
        with pytest.raises(error_type):
            store.open_stream(uri, version=version)
        assert len(calls) == 1
    finally:
        if provider_kind == "gcs":
            client.close()
        elif provider_kind == "azure":
            service.close()


def test_embedded_rejects_version_without_reading_current_file(tmp_path):
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="bucket")
    item = store.put_bytes("key", b"current")
    with pytest.raises(UploadCapabilityUnavailable):
        store.open_stream(item.uri, version="v1")
    assert store.open_stream(item.uri).read() == b"current"


@pytest.mark.parametrize("reported", [None, "other"])
@pytest.mark.parametrize("provider", ["azure"], indirect=True)
def test_azure_rejects_ignored_version_without_reading_current(provider, monkeypatch, reported):
    store, uri, _ = provider
    downloader = Downloader(version=reported)
    calls = []

    def download(blob, **kwargs):
        assert blob.version_id == "v1"
        calls.append(True)
        return downloader

    monkeypatch.setattr(BlobClient, "download_blob", download)
    with pytest.raises(UploadContractError):
        store.open_stream(uri, version="v1")
    assert calls == [True] and downloader.sizes == []


@pytest.mark.parametrize("provider", ["azure"], indirect=True)
def test_azure_reader_supports_buffering_and_closes_on_read_error(provider, monkeypatch):
    store, uri, _ = provider
    downloader = Downloader()
    monkeypatch.setattr(BlobClient, "download_blob", lambda *_args, **_kwargs: downloader)
    with io.BufferedReader(store.open_stream(uri, version="v1"), buffer_size=4) as stream:
        assert stream.read(2) == b"ab"
        assert stream.read(2) == b"cd"
        assert stream.read() == b"efghij"
    assert stream.closed
    downloader = Downloader()
    monkeypatch.setattr(downloader, "read", MagicMock(side_effect=ConnectionError("read failed")))
    with pytest.raises(ConnectionError), store.open_stream(uri, version="v1") as stream:
        stream.read(2)
    assert stream.closed and stream.downloader is None
