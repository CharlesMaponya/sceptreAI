"""A missing session or accepted abort is not by itself proof of cancellation."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from automl_api.storage.contracts import DeletionPending, UploadContractError
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver
from botocore.exceptions import ClientError
from test_phase2_upload_drivers import S3Client, _begin


def error(code, status=404):
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, "ListParts"
    )


@pytest.fixture
def staged():
    native = S3Client()
    store = S3ObjectStoreDriver(bucket="bucket", client=native)
    upload = store.begin_upload(_begin())
    native.list_parts = MagicMock(return_value={"Parts": [{"PartNumber": 1}], "IsTruncated": False})
    native.abort_multipart_upload = MagicMock()
    return store, upload, native


def test_s3_abort_waits_for_remaining_parts(staged):
    store, upload, native = staged
    with pytest.raises(DeletionPending):
        store.abort_upload(upload)
    assert native.abort_multipart_upload.call_count == 1
    assert native.list_parts.call_count >= 1


def test_s3_missing_bucket_cannot_mean_cancelled(staged):
    store, upload, native = staged
    native.list_parts.side_effect = error("NoSuchBucket")
    native.abort_multipart_upload.side_effect = error("NoSuchBucket")
    with pytest.raises(ClientError):
        store.abort_upload(upload)


@pytest.mark.parametrize("status", [200, 204, 404, 410, 499])
def test_gcs_only_qualified_cancellation_acknowledgement_succeeds(status):
    requests = []
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(status)
        )
    ) as http:
        store = object.__new__(GCSObjectStoreDriver)
        store.http = http
        upload = SimpleNamespace(provider_id="https://storage.test/resumable/original")
        if status == 499:
            store.abort_upload(upload)
        else:
            with pytest.raises(UploadContractError):
                store.abort_upload(upload)
    assert len(requests) == 1 and str(requests[0].url) == upload.provider_id
    assert requests[0].method == "DELETE"


def test_s3_already_absent_session_does_not_issue_another_abort(staged):
    store, upload, native = staged
    native.list_parts.side_effect = error("NoSuchUpload")
    store.abort_upload(upload)
    native.abort_multipart_upload.assert_not_called()


def test_s3_accepted_abort_requires_a_fresh_original_parts_check(staged):
    store, upload, native = staged
    native.list_parts.side_effect = [
        {"Parts": [{"PartNumber": 1}], "IsTruncated": False},
        error("NoSuchUpload"),
    ]
    store.abort_upload(upload)
    expected = {
        "Bucket": "bucket",
        "Key": upload.object_key,
        "UploadId": upload.provider_id,
        "MaxParts": 1,
    }
    assert all(call.kwargs == expected for call in native.list_parts.call_args_list)
    native.abort_multipart_upload.assert_called_once_with(
        Bucket="bucket", Key=upload.object_key, UploadId=upload.provider_id
    )


def test_s3_empty_but_truncated_confirmation_is_not_cancellation(staged):
    store, upload, native = staged
    native.list_parts.side_effect = [
        {"Parts": [], "IsTruncated": False},
        {"Parts": [], "IsTruncated": True},
    ]
    with pytest.raises(DeletionPending):
        store.abort_upload(upload)


def test_s3_surviving_empty_session_is_not_confirmed_cancellation(staged):
    store, upload, native = staged
    native.list_parts.return_value = {"Parts": [], "IsTruncated": False}
    with pytest.raises(DeletionPending):
        store.abort_upload(upload)
    assert native.list_parts.call_count == 2
    assert native.abort_multipart_upload.call_count == 1


def test_s3_confirmation_permission_failure_is_not_cancellation(staged):
    store, upload, native = staged
    native.list_parts.side_effect = [
        {"Parts": [], "IsTruncated": False},
        error("AccessDenied", 403),
    ]
    with pytest.raises(ClientError):
        store.abort_upload(upload)


@pytest.mark.parametrize("status", [None, 200, 204, 403, 410, 429, 500, 503, "404"])
@pytest.mark.parametrize("boundary", ["precheck", "abort", "confirmation"])
def test_s3_missing_error_requires_qualified_http_status(staged, status, boundary):
    from automl_api.storage.contracts import UploadThrottled

    store, upload, native = staged
    bad = error("NoSuchUpload", status)
    if status is None:
        bad.response.pop("ResponseMetadata")
    if boundary == "precheck":
        native.list_parts.side_effect = bad
    else:
        native.list_parts.side_effect = [
            {"Parts": [], "IsTruncated": False},
            bad if boundary == "confirmation" else error("NoSuchUpload", 404),
        ]
        if boundary == "abort":
            native.abort_multipart_upload.side_effect = bad
    with pytest.raises(UploadThrottled if status == 429 else ClientError):
        store.abort_upload(upload)
    assert native.abort_multipart_upload.call_count == int(boundary != "precheck")


def test_s3_abort_refuses_hidden_sdk_retries_before_any_native_call(staged):
    store, upload, native = staged
    native.meta.config.retries = {"total_max_attempts": 9}
    with pytest.raises(UploadContractError):
        store.abort_upload(upload)
    native.list_parts.assert_not_called()
    native.abort_multipart_upload.assert_not_called()


@pytest.mark.parametrize(
    "reply",
    [
        {"Parts": []},
        {"Parts": [], "IsTruncated": False, "Key": "foreign"},
        {"Parts": [], "IsTruncated": False, "UploadId": "replacement"},
    ],
)
def test_s3_unqualified_parts_inventory_cannot_authorize_abort(staged, reply):
    store, upload, native = staged
    native.list_parts.return_value = reply
    with pytest.raises(UploadContractError):
        store.abort_upload(upload)
    native.abort_multipart_upload.assert_not_called()


@pytest.mark.parametrize("identity", ["", " ", None, True, "foreign\nidentity"])
def test_s3_invalid_multipart_identity_cannot_reach_provider(staged, identity):
    from dataclasses import replace

    store, upload, native = staged
    with pytest.raises(UploadContractError):
        store.abort_upload(replace(upload, provider_id=identity))
    native.list_parts.assert_not_called()
    native.abort_multipart_upload.assert_not_called()


@pytest.mark.parametrize(
    "fault",
    [None, "disconnect", "503", "remaining", "empty-remaining"]
    + [
        f"false-missing:{boundary}:{status}"
        for boundary in ("precheck", "abort", "confirmation")
        for status in (403, 503)
    ],
)
def test_real_s3_sdk_abort_loss_and_remaining_parts(fault, monkeypatch):
    import os
    import socket
    from datetime import UTC, datetime, timedelta
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit

    from automl_api.storage.contracts import ProviderUpload, UploadThrottled
    from botocore.exceptions import BotoCoreError

    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    state = SimpleNamespace(
        active=True,
        reads=[],
        writes=[],
        remaining=fault in {"remaining", "empty-remaining"},
        false_missing=bool(fault and fault.startswith("false-missing:")),
    )
    boundary, status = fault.split(":")[1:] if state.false_missing else (None, None)
    missing_body = (
        b"<Error><Code>NoSuchUpload</Code><Message>Original session absent</Message></Error>"
    )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status, body=b""):
            self.send_response(status)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def do_GET(self):
            state.reads.append((urlsplit(self.path).path, parse_qs(urlsplit(self.path).query)))
            if state.false_missing and (
                boundary == "precheck" or boundary == "confirmation" and state.writes
            ):
                self.respond(int(status), missing_body)
                return
            if state.active:
                part = (
                    b""
                    if fault == "empty-remaining"
                    else b'<Part><PartNumber>1</PartNumber><Size>1</Size><ETag>"part"</ETag></Part>'
                )
                self.respond(
                    200,
                    b'<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>bucket</Bucket><Key>key</Key><UploadId>original</UploadId><IsTruncated>false</IsTruncated>'
                    + part
                    + b"</ListPartsResult>",
                )
            else:
                self.respond(404, missing_body)

        def do_DELETE(self):
            state.writes.append((urlsplit(self.path).path, parse_qs(urlsplit(self.path).query)))
            state.active = state.remaining
            if state.false_missing and boundary == "abort":
                self.respond(int(status), missing_body)
            elif fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            elif fault == "503":
                self.respond(
                    503,
                    (
                        b"<Error><Code>SlowDown</Code>"
                        b"<Message>Accepted response lost</Message></Error>"
                    ),
                )
            else:
                self.respond(204)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    store = S3ObjectStoreDriver(
        bucket="bucket",
        region="us-east-1",
        endpoint_url=f"http://127.0.0.1:{server.server_port}",
        access_key="unit",
        secret_key="unit",
    )
    upload = ProviderUpload(
        "original", "key", "multipart", 1, datetime.now(UTC) + timedelta(minutes=5)
    )
    try:
        if state.false_missing:
            with pytest.raises(ClientError):
                store.abort_upload(upload)
            state.false_missing = False
        elif fault in {"disconnect", "503"}:
            with pytest.raises((BotoCoreError, ClientError, UploadThrottled)):
                store.abort_upload(upload)
        elif fault in {"remaining", "empty-remaining"}:
            with pytest.raises(DeletionPending):
                store.abort_upload(upload)
            state.remaining = False
        else:
            store.abort_upload(upload)
        store.abort_upload(upload)
        assert not state.active
        assert len(state.writes) == (2 if fault in {"remaining", "empty-remaining"} else 1)
        assert all(
            path == "/bucket/key" and query["uploadId"] == ["original"]
            for path, query in state.writes
        )
        assert all(
            path == "/bucket/key"
            and query["uploadId"] == ["original"]
            and query["max-parts"] == ["1"]
            for path, query in state.reads
        )
    finally:
        store.client.close()
        store.presign_client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
