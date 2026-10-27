"""Actual SDK requests must not repeat accepted mutations after response loss."""

import os
import socket
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

import pytest
from automl_api.storage.contracts import BeginUpload, TransferReceipt
from automl_api.storage.s3 import S3ObjectStoreDriver


@pytest.fixture
def storage_http(monkeypatch):
    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_MAX_ATTEMPTS", "9")
    monkeypatch.setenv("AWS_RETRY_MODE", "standard")
    state = SimpleNamespace(
        fault=None, uploads=[], writes=[], objects={}, completions=[], inventory=None, listings=[]
    )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status, payload=b"", **headers):
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)
            self.close_connection = True

        def lost(self, count):
            if count != 1 or state.fault is None:
                return False
            if state.fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            else:
                self.respond(
                    503,
                    b"<Error><Code>SlowDown</Code>"
                    b"<Message>Accepted but response lost</Message></Error>",
                    **{"Content-Type": "application/xml"},
                )
            return True

        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            assert "uploads" in query
            state.listings.append(query)
            if state.inventory:
                self.respond(
                    200, state.inventory(query).encode(), **{"Content-Type": "application/xml"}
                )
                return
            entries = "".join(
                f"<Upload><Key>{escape(key)}</Key><UploadId>{value}</UploadId><Initiated>2026-01-01T00:00:00Z</Initiated></Upload>"
                for key, value in state.uploads
            )
            self.respond(
                200,
                (
                    "<ListMultipartUploadsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>"
                    + entries
                    + "<IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"
                ).encode(),
                **{"Content-Type": "application/xml"},
            )

        def do_POST(self):
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if "uploadId" in query:
                assert query["uploadId"] == ["upload-1"]
                assert self.headers["If-None-Match"] == "*"
                state.completions.append(self.rfile.read(int(self.headers["Content-Length"])))
                if not self.lost(len(state.completions)):
                    self.respond(
                        200,
                        b"<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>projects/owned/raw/file.csv</Key><ETag>original</ETag></CompleteMultipartUploadResult>",
                        **{
                            "Content-Type": "application/xml",
                            "x-amz-version-id": "original-completion",
                        },
                    )
                return
            assert "uploads" in query
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            key = urlsplit(self.path).path.removeprefix("/bucket/")
            upload_id = "upload-" + str(len(state.uploads) + 1)
            state.uploads.append((key, upload_id))
            if not self.lost(len(state.uploads)):
                self.respond(
                    200,
                    f"<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>{escape(key)}</Key><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>".encode(),
                    **{"Content-Type": "application/xml"},
                )

        def do_PUT(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            state.writes.append(payload)
            state.objects[urlsplit(self.path).path] = payload
            if not self.lost(len(state.writes)):
                self.respond(
                    200, **{"ETag": '"fixture"', "x-amz-version-id": "v" + str(len(state.writes))}
                )

        def do_HEAD(self):
            payload = state.objects[urlsplit(self.path).path]
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("x-amz-version-id", "v" + str(len(state.writes)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.endpoint = f"http://127.0.0.1:{server.server_port}"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def driver(state, compatible, *, public=False):
    return S3ObjectStoreDriver(
        bucket="bucket",
        region="us-east-1",
        endpoint_url=state.endpoint,
        public_endpoint_url=state.endpoint + "/public" if public else None,
        access_key="test-only-access",
        secret_key="test-only-secret-0123456789",
        compatible=compatible,
    )


def request():
    return BeginUpload(
        object_key="projects/owned/raw/file.csv",
        byte_size=12,
        content_type="text/csv",
        checksum_sha256="a" * 64,
        transfer_unit_size=12,
        expires_at=datetime.now(UTC) + timedelta(minutes=10),
        origin="https://test.invalid",
    )


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize("fault", [None, "503", "disconnect"])
def test_completion_response_loss_never_replays_an_accepted_sdk_commit(
    storage_http, compatible, fault
):
    state = storage_http
    store = driver(state, compatible)
    try:
        upload = store.begin_upload(request())
        state.fault = fault
        error, result = None, None
        try:
            result = store.complete_upload(upload, [TransferReceipt(1, 0, 12, "part")])
        except Exception as exc:
            error = exc
        assert len(state.completions) == 1
        if fault is None:
            assert error is None, repr(error)
            assert result.provider_headers == {"version_id": "original-completion"}
        else:
            assert error is not None
    finally:
        store.client.close()


@pytest.mark.parametrize("compatible", [False, True])
@pytest.mark.parametrize("operation", ["create", "put"])
@pytest.mark.parametrize("fault", [None, "503", "disconnect"])
def test_mutation_response_loss_never_causes_hidden_sdk_replay(
    storage_http, compatible, operation, fault
):
    state = storage_http
    state.fault = fault
    store = driver(state, compatible)
    error, value = None, None
    try:
        try:
            value = (
                store.begin_upload(request())
                if operation == "create"
                else store.put_bytes("projects/owned/model", b"model-bytes")
            )
        except Exception as exc:
            error = exc
        accepted = state.uploads if operation == "create" else state.writes
        assert len(accepted) == 1, "The SDK repeated a mutation accepted before response loss"
        if fault is None:
            assert error is None and value is not None
        else:
            assert error is not None, "The caller must observe the uncertain outcome"
        if operation == "create":
            # An explicit caller retry queries the same native ID, without another create.
            resumed = store.begin_upload(request())
            assert resumed.provider_id == "upload-1" and len(state.uploads) == 1
        else:
            assert state.writes == [b"model-bytes"]
            metadata = store.stat(store.uri_for_key("projects/owned/model"))
            assert (
                metadata.byte_size == len(b"model-bytes")
                and metadata.provider_headers["version_id"] == "v1"
            )
    finally:
        store.client.close()


@pytest.mark.parametrize("compatible", [False, True])
def test_separate_public_signing_client_cannot_enable_sdk_retries(storage_http, compatible):
    store = driver(storage_http, compatible, public=True)
    try:
        for client in (store.client, store.presign_client):
            assert client.meta.config.retries["total_max_attempts"] == 1
        assert store.client is not store.presign_client
    finally:
        store.client.close()
        store.presign_client.close()
