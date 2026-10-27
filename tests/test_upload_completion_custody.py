"""Native completion responses must not be replaced by latest-object metadata."""

import base64
import json
import os
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree as ET

import httpx
import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.contracts import TransferReceipt, UploadContractError
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver
from azure.core import MatchConditions
from test_phase2_upload_drivers import AzureService, GCSClient, S3Client, _begin


@pytest.mark.parametrize("compatible", [False, True])
def test_s3_completion_preserves_its_response_version_without_reading_latest(compatible):
    native = S3Client()
    native.meta = MagicMock()
    native.meta.config.retries = {"total_max_attempts": 1}
    complete = native.complete_multipart_upload

    def accepted(**kwargs):
        complete(**kwargs)
        return {"VersionId": "original-completion", "ChecksumSHA256": "original-checksum"}

    native.complete_multipart_upload = MagicMock(side_effect=accepted)
    native.head_object = MagicMock(wraps=native.head_object)
    driver = S3ObjectStoreDriver(
        bucket="bucket",
        client=native,
        compatible=compatible,
        endpoint_url="https://objects.test",
        access_key="test",
        secret_key="test",
    )
    upload = driver.begin_upload(_begin())
    result = driver.complete_upload(upload, [TransferReceipt(1, 0, 10, "part-etag")])
    assert result.provider_headers["version_id"] == "original-completion"
    assert result.provider_checksum == "original-checksum"
    assert native.complete_multipart_upload.call_count == 1
    assert native.completed["IfNoneMatch"] == "*"
    native.head_object.assert_not_called()


def test_azure_completion_preserves_its_response_version_without_reading_latest():
    native = AzureService()
    driver = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        account_name="account",
        container="data",
        service_client=native,
        sas_factory=lambda **_: "test-only",
    )
    upload = driver.begin_upload(_begin())
    blob = native.container.get_blob_client(upload.object_key)
    blob.commit_block_list = MagicMock(return_value={"version_id": "original-completion"})
    blob.get_blob_properties = MagicMock(wraps=blob.get_blob_properties)
    result = driver.complete_upload(upload, [TransferReceipt(1, 0, 10)])
    assert result.provider_headers["version_id"] == "original-completion"
    assert blob.commit_block_list.call_count == 1
    assert blob.commit_block_list.call_args.kwargs["retry_total"] == 0
    assert blob.commit_block_list.call_args.kwargs["match_condition"] == MatchConditions.IfMissing
    blob.get_blob_properties.assert_not_called()


def test_gcs_completion_preserves_original_session_generation_without_reading_latest():
    native = GCSClient()
    native.bucket_value.get_blob = MagicMock(wraps=native.bucket_value.get_blob)
    original = dict(
        bucket="bucket", name=_begin().object_key, generation="7", size="10", crc32c="original-crc"
    )
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=original))
    ) as http:
        driver = GCSObjectStoreDriver(bucket="bucket", client=native, http_client=http)
        upload = driver.begin_upload(_begin())
        result = driver.complete_upload(upload, [])
    assert result.provider_headers["generation"] == "7"
    assert result.provider_checksum == "original-crc"
    native.bucket_value.get_blob.assert_not_called()


@pytest.mark.parametrize("version", [None, "", "null", "bad\nversion", "v" * 257, False])
def test_s3_missing_or_unbounded_completion_receipt_never_adopts_latest(version):
    native = S3Client()
    native.complete_multipart_upload = MagicMock(return_value={"VersionId": version})
    native.head_object = MagicMock()
    driver = S3ObjectStoreDriver(bucket="bucket", client=native)
    upload = driver.begin_upload(_begin())
    with pytest.raises(UploadContractError):
        driver.complete_upload(upload, [TransferReceipt(1, 0, 10, "part")])
    native.head_object.assert_not_called()
    assert native.complete_multipart_upload.call_count == 1


def test_s3_completion_refuses_hidden_sdk_retries_before_any_write():
    native = S3Client()
    native.complete_multipart_upload = MagicMock()
    driver = S3ObjectStoreDriver(bucket="bucket", client=native)
    upload = driver.begin_upload(_begin())
    native.meta.config.retries = {"total_max_attempts": 9}
    with pytest.raises(UploadContractError, match="single-attempt"):
        driver.complete_upload(upload, [TransferReceipt(1, 0, 10, "part")])
    native.complete_multipart_upload.assert_not_called()


@pytest.mark.parametrize("version", [None, "", "null", "bad\nversion", "v" * 257, False])
def test_azure_missing_or_unbounded_completion_receipt_never_adopts_latest(version):
    native = AzureService()
    driver = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        account_name="account",
        container="data",
        service_client=native,
        sas_factory=lambda **_: "test-only",
    )
    upload = driver.begin_upload(_begin())
    blob = native.container.get_blob_client(upload.object_key)
    blob.commit_block_list = MagicMock(return_value={"version_id": version})
    blob.get_blob_properties = MagicMock()
    with pytest.raises(UploadContractError):
        driver.complete_upload(upload, [TransferReceipt(1, 0, 10)])
    blob.get_blob_properties.assert_not_called()
    assert blob.commit_block_list.call_count == 1


@pytest.mark.parametrize(
    "patch",
    [
        {"bucket": "foreign"},
        {"name": "foreign"},
        {"size": "9"},
        {"size": True},
        {"generation": None},
        {"generation": "0"},
        {"generation": "-1"},
        {"generation": str(2**64)},
        {"generation": "١"},
        {"generation": "v" * 257},
    ],
)
def test_gcs_completion_rejects_foreign_or_incomplete_native_receipt(patch):
    receipt = dict(bucket="bucket", name=_begin().object_key, generation="7", size="10") | patch
    native = GCSClient()
    native.bucket_value.get_blob = MagicMock()
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=receipt))
    ) as http:
        driver = GCSObjectStoreDriver(bucket="bucket", client=native, http_client=http)
        upload = driver.begin_upload(_begin())
        with pytest.raises(UploadContractError, match="immutable receipt"):
            driver.complete_upload(upload, [])
    native.bucket_value.get_blob.assert_not_called()


def test_gcs_byte_progress_without_completion_receipt_is_not_complete():
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(308, headers={"Range": "bytes=0-9"}))
    ) as http:
        driver = GCSObjectStoreDriver(bucket="bucket", client=GCSClient(), http_client=http)
        upload = driver.begin_upload(_begin())
        progress = driver.query_progress(upload)
        assert progress.confirmed_bytes == 10 and not progress.complete
        with pytest.raises(UploadContractError, match="not confirmed"):
            driver.complete_upload(upload, [])


def test_gcs_completion_budget_stops_reading_and_closes_oversized_response():
    class Oversized(httpx.SyncByteStream):
        def __init__(self):
            self.reads, self.closed = 0, False

        def __iter__(self):
            for _ in range(100):
                self.reads += 1
                yield b" " * 8192

        def close(self):
            self.closed = True

    body = Oversized()
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=body))
    ) as http:
        driver = GCSObjectStoreDriver(bucket="bucket", client=GCSClient(), http_client=http)
        upload = driver.begin_upload(_begin())
        with pytest.raises(UploadContractError, match="receipt budget"):
            driver.complete_upload(upload, [])
    assert body.reads == 9 and body.closed


@pytest.fixture
def azure_completion_http(monkeypatch):
    from azure.core.credentials import AzureNamedKeyCredential
    from azure.storage.blob import BlobServiceClient

    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    state = SimpleNamespace(fault=None, commits=[], latest_reads=0, headers=[])
    wire_id = base64.b64encode(b"sceptre-00000001").decode()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, code, body=b"", **headers):
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            self.close_connection = True

        def do_GET(self):
            assert parse_qs(urlsplit(self.path).query)["comp"] == ["blocklist"]
            self.respond(
                200,
                (
                    "<BlockList><CommittedBlocks/><UncommittedBlocks>"
                    f"<Block><Name>{wire_id}</Name><Size>10</Size></Block>"
                    "</UncommittedBlocks></BlockList>"
                ).encode(),
                **{"Content-Type": "application/xml"},
            )

        def do_PUT(self):
            assert parse_qs(urlsplit(self.path).query)["comp"] == ["blocklist"]
            body = self.rfile.read(int(self.headers["Content-Length"]))
            state.commits.append([item.text for item in ET.fromstring(body)])
            state.headers.append(self.headers.get("If-None-Match"))
            if state.fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            elif state.fault == "503":
                self.respond(
                    503,
                    b"<Error><Code>ServerBusy</Code>"
                    b"<Message>Accepted but response lost</Message></Error>",
                    **{"Content-Type": "application/xml"},
                )
            else:
                self.respond(
                    201,
                    **{
                        "ETag": '"original"',
                        "x-ms-version-id": "original-completion",
                        "Last-Modified": "Mon, 26 Oct 2026 00:00:00 GMT",
                    },
                )

        def do_HEAD(self):
            state.latest_reads += 1
            self.respond(200, **{"x-ms-version-id": "replacement"})

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = f"http://127.0.0.1:{server.server_port}/account"
    sdk = BlobServiceClient(
        address,
        credential=AzureNamedKeyCredential("account", base64.b64encode(b"x" * 32).decode()),
        retry_total=9,
    )
    state.store = AzureBlobObjectStoreDriver(
        account_url=address, account_name="account", container="data", service_client=sdk
    )
    state.wire_id = wire_id
    try:
        yield state
    finally:
        sdk.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("fault", [None, "503", "disconnect"])
def test_azure_sdk_completion_uses_original_blocks_and_does_not_replay_lost_response(
    azure_completion_http, fault
):
    state = azure_completion_http
    store = state.store
    upload = store.begin_upload(_begin())
    progress = store.query_progress(upload)
    assert progress.complete and progress.confirmed_bytes == 10
    assert progress.receipts[0].provider_headers == {"block_id": "sceptre-00000001"}
    state.fault = fault
    error, result = None, None
    try:
        result = store.complete_upload(upload, list(progress.receipts))
    except Exception as exc:
        error = exc
    assert state.commits == [[state.wire_id]], repr(error)
    assert state.headers == ["*"] and state.latest_reads == 0
    if fault is None:
        assert error is None, repr(error)
        assert result.provider_headers == {"version_id": "original-completion"}
    else:
        assert error is not None


@pytest.mark.parametrize("fault", [None, "503", "disconnect"])
def test_gcs_sdk_binds_original_session_and_does_not_replay_lost_creation(monkeypatch, fault):
    from google.auth.credentials import AnonymousCredentials
    from google.cloud.storage import Client

    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    state = SimpleNamespace(creates=[], queries=[], latest_reads=0, read_paths=[])

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, code, body=b"", **headers):
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def do_POST(self):
            query = parse_qs(urlsplit(self.path).query)
            metadata = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state.creates.append((query, metadata))
            if fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            elif fault == "503":
                self.respond(
                    503,
                    b'{"error":{"code":503,"message":"Accepted but response lost"}}',
                    **{"Content-Type": "application/json"},
                )
            else:
                self.respond(
                    200,
                    **{
                        "Location": f"http://127.0.0.1:{self.server.server_port}/resumable/original"
                    },
                )

        def do_PUT(self):
            state.queries.append(
                (self.path, self.headers.get("Content-Range"), self.headers.get("Content-Length"))
            )
            self.respond(
                200,
                json.dumps(
                    dict(
                        bucket="bucket",
                        name=_begin().object_key,
                        generation="7",
                        size="10",
                        crc32c="original",
                    )
                ).encode(),
                **{"Content-Type": "application/json"},
            )

        def do_GET(self):
            state.read_paths.append(self.path)
            if urlsplit(self.path).path == "/storage/v1/b/bucket":
                self.respond(
                    200,
                    json.dumps(
                        dict(kind="storage#bucket", name="bucket", versioning=dict(enabled=True))
                    ).encode(),
                    **{"Content-Type": "application/json"},
                )
                return
            state.latest_reads += 1
            self.respond(
                200,
                json.dumps(
                    dict(bucket="bucket", name=_begin().object_key, generation="99", size="10")
                ).encode(),
                **{"Content-Type": "application/json"},
            )

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sdk = Client(
        project="unit",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"http://127.0.0.1:{server.server_port}"},
    )
    http = httpx.Client(trust_env=False)
    try:
        store = GCSObjectStoreDriver(bucket="bucket", client=sdk, http_client=http)
        error, upload = None, None
        try:
            upload = store.begin_upload(_begin())
        except Exception as exc:
            error = exc
        assert len(state.creates) == 1
        assert state.creates[0][0]["ifGenerationMatch"] == ["0"]
        assert state.creates[0][1]["name"] == _begin().object_key
        if fault is None:
            assert error is None, repr(error)
            result = store.complete_upload(upload, [])
            assert result.provider_headers == {"generation": "7"}
            assert state.queries == [("/resumable/original", "bytes */10", "0")]
        else:
            assert error is not None
        assert state.latest_reads == 0, state.read_paths
        if fault is None:
            # Prove the counter detects a real latest-object read.
            latest = sdk.bucket("bucket").get_blob(_begin().object_key)
            assert latest.generation == 99 and state.latest_reads == 1
            assert result.provider_headers == {"generation": "7"}
    finally:
        http.close()
        sdk.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
