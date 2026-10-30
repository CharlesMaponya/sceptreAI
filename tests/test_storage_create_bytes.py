"""Original write replies, never latest-object adoption, identify an exclusive create."""

import base64
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import google_crc32c
import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver

CONTENT = b"feature,target\n1,2\n3,4\n"
CHECKSUM = base64.b64encode(google_crc32c.value(CONTENT).to_bytes(4, "big")).decode("ascii")
KEY = "projects/owned/input.csv"


@pytest.fixture(params=["aws_s3", "s3_compatible", "gcs", "azure_blob"])
def creator(request):
    kind = request.param
    if kind in {"aws_s3", "s3_compatible"}:
        native = MagicMock()
        native.meta.config.retries = {"total_max_attempts": 1}
        reply = {"VersionId": "7", "ETag": '"original-etag"', "ChecksumSHA256": "original-sha"}
        native.put_object.return_value = reply
        store = S3ObjectStoreDriver(
            bucket="owned",
            client=native,
            compatible=kind == "s3_compatible",
            endpoint_url="https://objects.test",
            access_key="test",
            secret_key="test",
        )
        call = native.put_object
        version_key = "version_id"
    elif kind == "azure_blob":
        native = MagicMock()
        reply = {"version_id": "7", "etag": '"original-etag"'}
        blob = native.get_container_client.return_value.get_blob_client.return_value
        blob.upload_blob.return_value = reply
        store = AzureBlobObjectStoreDriver(
            account_url="https://account.blob.test",
            account_name="account",
            container="owned",
            service_client=native,
            sas_factory=lambda **_: "test-only",
        )
        call = native.get_container_client.return_value.get_blob_client.return_value.upload_blob
        version_key = "version_id"
    else:
        native = MagicMock()
        blob = native.bucket.return_value.blob.return_value
        blob.name, blob.generation, blob.size = KEY, 7, len(CONTENT)
        blob.id = f"owned/{KEY}/7"
        blob.etag, blob.crc32c, blob.md5_hash = "original-etag", CHECKSUM, None
        reply = blob
        store = GCSObjectStoreDriver(bucket="owned", client=native)
        call = blob.upload_from_string
        version_key = "generation"
    store.stat = MagicMock(side_effect=AssertionError("Latest metadata must never be read"))
    return SimpleNamespace(
        kind=kind, store=store, native=native, reply=reply, call=call, version_key=version_key
    )


def test_create_bytes_keeps_original_receipt_and_exclusive_write(creator):
    c = creator
    result = c.store.create_bytes(KEY, CONTENT)
    assert result.byte_size == len(CONTENT)
    assert result.provider_headers == {c.version_key: "7"}
    assert result.etag == "original-etag"
    assert result.uri == c.store.uri_for_key(KEY)
    if c.kind in {"aws_s3", "s3_compatible"}:
        c.call.assert_called_once_with(Bucket="owned", Key=KEY, Body=CONTENT, IfNoneMatch="*")
        c.native.head_object.assert_not_called()
        assert result.checksum == "original-sha"
    elif c.kind == "azure_blob":
        c.call.assert_called_once_with(CONTENT, overwrite=False, retry_total=0, max_concurrency=1)
        c.native.get_container_client.return_value.get_blob_client.return_value.get_blob_properties.assert_not_called()
    else:
        c.call.assert_called_once_with(CONTENT, if_generation_match=0, retry=None, checksum=None)
        c.native.bucket.return_value.get_blob.assert_not_called()
        assert result.checksum == CHECKSUM
    c.store.stat.assert_not_called()


@pytest.mark.parametrize("version", [None, "", "null", "NULL", " ", "\n", "x" * 257, True, [], 0])
def test_create_bytes_rejects_unqualified_original_version(creator, version):
    c = creator
    if c.kind in {"aws_s3", "s3_compatible"}:
        c.reply["VersionId"] = version
    elif c.kind == "azure_blob":
        c.reply["version_id"] = version
    else:
        c.reply.generation = version
    with pytest.raises(UploadContractError):
        c.store.create_bytes(KEY, CONTENT)
    assert c.call.call_count == 1
    c.store.stat.assert_not_called()


@pytest.mark.parametrize("reason", ["collision", "lost-reply", "forbidden", "throttled"])
def test_create_bytes_does_not_adopt_or_reissue_after_provider_failure(creator, reason):
    creator.call.side_effect = RuntimeError(reason)
    with pytest.raises(RuntimeError, match=reason):
        creator.store.create_bytes(KEY, CONTENT)
    assert creator.call.call_count == 1
    creator.store.stat.assert_not_called()


@pytest.mark.parametrize("compatible", [False, True])
def test_create_bytes_rejects_s3_hidden_retries_before_write(compatible):
    native = MagicMock()
    native.meta.config.retries = {"total_max_attempts": 2}
    store = S3ObjectStoreDriver(
        bucket="owned",
        client=native,
        compatible=compatible,
        endpoint_url="https://objects.test",
        access_key="test",
        secret_key="test",
    )
    with pytest.raises(UploadContractError, match="single-attempt"):
        store.create_bytes(KEY, CONTENT)
    native.put_object.assert_not_called()


@pytest.mark.parametrize(
    "patch", [{"id": "foreign/key/7"}, {"size": 0}, {"generation": "01"}, {"generation": 2**64}]
)
def test_gcs_create_bytes_validates_its_original_response_scope(patch):
    native = MagicMock()
    blob = native.bucket.return_value.blob.return_value
    blob.name, blob.generation, blob.size = KEY, 7, len(CONTENT)
    blob.id = f"owned/{KEY}/7"
    for name, value in patch.items():
        setattr(blob, name, value)
    store = GCSObjectStoreDriver(bucket="owned", client=native)
    with pytest.raises(UploadContractError):
        store.create_bytes(KEY, CONTENT)
    native.bucket.return_value.get_blob.assert_not_called()


@pytest.mark.parametrize("same_content", [False, True])
def test_embedded_create_bytes_has_one_atomic_winner(tmp_path, same_content):
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="owned")
    barrier = threading.Barrier(4)

    def create(index):
        value = CONTENT if same_content else bytes([index]) * 4096
        barrier.wait(timeout=5)
        try:
            return store.create_bytes(KEY, value), value
        except FileExistsError:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(create, range(4)))
    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    metadata, original = winners[0]
    assert metadata.byte_size == len(original)
    assert store.read_bytes(metadata.uri) == original
    assert not list(tmp_path.rglob(".upload-*"))
    with pytest.raises(FileExistsError):
        store.create_bytes(KEY, b"replacement")
    assert store.read_bytes(metadata.uri) == original


def test_embedded_create_bytes_cleans_owned_temporary_file_on_failure(tmp_path, monkeypatch):
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="owned")
    unrelated = tmp_path / "unrelated"
    unrelated.write_bytes(CONTENT)

    def unavailable(*_args):
        raise OSError("Injected link failure")

    monkeypatch.setattr("automl_api.storage.embedded.os.link", unavailable)
    with pytest.raises(OSError, match="Injected link failure"):
        store.create_bytes(KEY, CONTENT)
    assert not list(tmp_path.rglob(".upload-*"))
    assert not (tmp_path / "owned" / KEY).exists()
    assert unrelated.read_bytes() == CONTENT
