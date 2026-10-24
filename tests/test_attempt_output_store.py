"""Conditional native output writes preserve original objects and unknown outcomes."""

import hashlib
import io
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from automl_api.storage.attempt_outputs import S3_SINGLE_PUT_BYTES, write_attempt_output
from automl_api.storage.contracts import UploadContractError

DATA = b"one immutable model" * 5000
SHA = hashlib.sha256(DATA).hexdigest()
GRANT = "a" * 64
URI = "s3c://owned/projects/project/scopes/scope/refit/attempt/model/pipeline.joblib"


@pytest.fixture(params=["aws_s3", "s3_compatible", "gcs", "azure_blob"])
def native(request):
    c = SimpleNamespace(driver=request.param, calls=[], objects={}, version="v1", fault=None)
    c.blob = SimpleNamespace(generation=17, metadata=None)

    def accept(source, options):
        c.calls.append(options)
        if c.objects:
            raise FileExistsError("The conditional native write cannot overwrite")
        value = source.read()
        assert value == DATA
        c.objects["written"] = value
        if c.fault == "after_accept":
            raise ConnectionError("Native write accepted but reply lost")
        return {"VersionId": c.version, "version_id": c.version}

    client = SimpleNamespace(
        meta=SimpleNamespace(config=SimpleNamespace(retries={"total_max_attempts": 1})),
        put_object=Mock(side_effect=lambda **kw: accept(kw["Body"], kw)),
    )
    c.blob.upload_from_file = Mock(side_effect=lambda source, **kw: accept(source, kw))
    c.blob.upload_blob = Mock(side_effect=lambda source, **kw: accept(source, kw))
    bucket = Mock()
    bucket.blob.return_value = c.blob
    container = Mock()
    container.get_blob_client.return_value = c.blob
    reads = []

    def read(uri, *, version):
        reads.append((uri, version))
        return io.BytesIO(c.objects["written"] if c.fault != "readback" else b"corrupt")

    c.store = SimpleNamespace(
        driver_name=c.driver,
        client=client,
        bucket=bucket if c.driver == "gcs" else "owned",
        container=container,
        _key_from_uri=Mock(return_value="fixed/own/output"),
        open_stream=Mock(side_effect=read),
    )
    c.reads = reads
    return c


def write(c, **changes):
    options = {"byte_size": len(DATA), "sha256": SHA, "grant_digest": GRANT, **changes}
    return write_attempt_output(c.store, URI, io.BytesIO(DATA), **options)


def test_native_output_is_conditional_versioned_and_hash_verified(native):
    c = native
    result = write(c)
    assert result.uri == URI and result.byte_size == len(DATA)
    version = "17" if c.driver == "gcs" else "v1"
    key = "generation" if c.driver == "gcs" else "version_id"
    assert result.provider_headers == {key: version, "sceptre_grant": GRANT}
    assert c.reads == [(URI, version)] and len(c.calls) == 1
    options = c.calls[0]
    if c.driver in {"aws_s3", "s3_compatible"}:
        assert options["IfNoneMatch"] == "*" and options["ContentLength"] == len(DATA)
        assert options["Metadata"] == {"sceptre_grant": GRANT, "sceptre_sha256": SHA}
        assert options["ChecksumSHA256"]
    elif c.driver == "gcs":
        assert options["if_generation_match"] == 0 and options["retry"] is None
        assert options["checksum"] is None and options["size"] == len(DATA)
        assert c.blob.metadata == {"sceptre_grant": GRANT, "sceptre_sha256": SHA}
    else:
        assert options["blob_type"] == "BlockBlob" and options["overwrite"] is False
        assert options["retry_total"] == 0 and options["max_concurrency"] == 1
        assert options["length"] == len(DATA)
    with pytest.raises(FileExistsError):
        write(c)
    assert c.objects["written"] == DATA and len(c.calls) == 2


@pytest.mark.parametrize("fault", ["after_accept", "readback"])
def test_accepted_unknown_or_invalid_readback_never_rewrites_or_deletes(native, fault):
    c = native
    c.fault = fault
    with pytest.raises(ConnectionError if fault == "after_accept" else UploadContractError):
        write(c)
    assert len(c.calls) == 1 and c.objects["written"] == DATA
    assert c.reads == (
        [] if fault == "after_accept" else [(URI, "17" if c.driver == "gcs" else "v1")]
    )


@pytest.mark.parametrize("version", [None, "", "null", "v" * 257])
def test_unversioned_or_oversized_version_is_not_acknowledged(native, version):
    c = native
    if c.driver == "gcs":
        c.blob.generation = version
    else:
        c.version = version
    with pytest.raises(UploadContractError):
        write(c)
    assert len(c.calls) == 1 and c.objects["written"] == DATA and c.reads == []


@pytest.mark.parametrize(
    "changes",
    [
        {"byte_size": True},
        {"byte_size": 0},
        {"byte_size": -1},
        {"byte_size": len(DATA) - 1},
        {"byte_size": len(DATA) + 1},
        {"sha256": "f" * 64},
        {"sha256": "bad"},
        {"grant_digest": "bad"},
    ],
)
def test_invalid_declared_output_cannot_reach_native_storage(native, changes):
    with pytest.raises(UploadContractError):
        write(native, **changes)
    assert native.calls == [] and native.objects == {} and native.reads == []


@pytest.mark.parametrize("native", ["aws_s3", "s3_compatible"], indirect=True)
def test_s3_large_output_requires_tracked_multipart_not_untracked_transfer(native):
    with pytest.raises(UploadContractError, match="single-PUT"):
        write(native, byte_size=S3_SINGLE_PUT_BYTES + 1)
    assert native.calls == []


@pytest.mark.parametrize("native", ["aws_s3", "s3_compatible"], indirect=True)
def test_s3_injected_retrying_client_cannot_bypass_one_attempt(native):
    native.store.client.meta.config.retries = {"total_max_attempts": 2}
    with pytest.raises(UploadContractError, match="single-attempt"):
        write(native)
    assert native.calls == []


def test_unsupported_unversioned_storage_has_no_write_fallback(native):
    native.store.driver_name = "embedded"
    with pytest.raises(UploadContractError, match="immutable native"):
        write(native)
    assert native.calls == []


@pytest.fixture(params=["aws_s3", "s3_compatible", "gcs", "azure_blob"])
def sdk_http(request, monkeypatch):
    import base64
    import json
    import os
    import socket
    from email.parser import BytesParser
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit

    from automl_api.storage.azure import AzureBlobObjectStoreDriver
    from automl_api.storage.gcs import GCSObjectStoreDriver
    from automl_api.storage.s3 import S3ObjectStoreDriver

    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    state = SimpleNamespace(
        provider=request.param, fault=None, puts=[], stored=None, reads=0, events=[]
    )

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
            assert state.provider == "gcs"
            self.upload()

        def do_PUT(self):
            self.upload()

        def do_DELETE(self):
            state.events.append((self.command, self.path))
            self.respond(405)

        def upload(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            query = parse_qs(urlsplit(self.path).query)
            state.events.append((self.command, self.path))
            state.puts.append((dict(self.headers), query))
            if state.provider == "gcs":
                message = BytesParser().parsebytes(
                    (
                        "Content-Type: "
                        + self.headers["Content-Type"]
                        + "\r\nMIME-Version: 1.0\r\n\r\n"
                    ).encode()
                    + body
                )
                parts = message.get_payload()
                meta = json.loads(parts[0].get_payload(decode=True))
                value = parts[1].get_payload(decode=True)
                assert query["ifGenerationMatch"] == ["0"]
                tags = meta["metadata"]
            else:
                value = body
                assert self.headers["If-None-Match"] == "*"
                prefix = "x-ms-meta-" if state.provider == "azure_blob" else "x-amz-meta-"
                tags = {
                    key.lower().removeprefix(prefix): val
                    for key, val in self.headers.items()
                    if key.lower().startswith(prefix)
                }
            assert tags == {"sceptre_grant": GRANT, "sceptre_sha256": SHA}
            if state.stored is not None:
                error = (
                    b'{"error":{"code":412,"message":"exists"}}'
                    if state.provider == "gcs"
                    else b"<Error><Code>BlobAlreadyExists</Code><Message>exists</Message></Error>"
                )
                self.respond(
                    412,
                    error,
                    **{
                        "Content-Type": "application/json"
                        if state.provider == "gcs"
                        else "application/xml"
                    },
                )
                return
            state.stored = value
            assert value == DATA
            if state.fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            if state.fault == "503":
                error = (
                    b'{"error":{"code":503,"message":"lost"}}'
                    if state.provider == "gcs"
                    else b"<Error><Code>ServerBusy</Code><Message>lost</Message></Error>"
                )
                self.respond(
                    503,
                    error,
                    **{
                        "Content-Type": "application/json"
                        if state.provider == "gcs"
                        else "application/xml"
                    },
                )
                return
            if state.provider == "gcs":
                self.respond(200, self.properties(), **{"Content-Type": "application/json"})
            elif state.provider == "azure_blob":
                self.respond(
                    201,
                    **{
                        "ETag": '"native"',
                        "x-ms-version-id": "v1",
                        "Last-Modified": "Wed, 21 Oct 2026 00:00:00 GMT",
                    },
                )
            else:
                self.respond(200, **{"ETag": '"native"', "x-amz-version-id": "v1"})

        def properties(self):
            return json.dumps(
                {
                    "kind": "storage#object",
                    "name": "output",
                    "bucket": "owned",
                    "size": str(len(DATA)),
                    "generation": "17",
                    "metageneration": "1",
                    "etag": "native",
                }
            ).encode()

        def do_GET(self):
            state.events.append((self.command, self.path))
            state.reads += 1
            if state.provider == "gcs" and urlsplit(self.path).path == "/storage/v1/b/owned":
                self.respond(
                    200,
                    json.dumps(
                        {"kind": "storage#bucket", "name": "owned", "versioning": {"enabled": True}}
                    ).encode(),
                    **{"Content-Type": "application/json"},
                )
                return
            query = parse_qs(urlsplit(self.path).query)
            if state.provider == "gcs" and query.get("alt") != ["media"]:
                self.respond(200, self.properties(), **{"Content-Type": "application/json"})
                return
            version_header = {
                "aws_s3": "x-amz-version-id",
                "s3_compatible": "x-amz-version-id",
                "gcs": "x-goog-generation",
                "azure_blob": "x-ms-version-id",
            }[state.provider]
            headers = {
                version_header: "17" if state.provider == "gcs" else "v1",
                "ETag": '"native"',
                "Last-Modified": "Wed, 21 Oct 2026 00:00:00 GMT",
                "x-ms-blob-type": "BlockBlob",
            }
            selected = self.headers.get("Range") or self.headers.get("x-ms-range")
            value = state.stored
            if selected:
                start, end = selected.removeprefix("bytes=").split("-")
                start, end = int(start), min(int(end), len(value) - 1)
                value = value[start : end + 1]
                headers["Content-Range"] = f"bytes {start}-{end}/{len(state.stored)}"
            self.respond(206 if selected else 200, value, **headers)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    if state.provider == "gcs":
        from google.auth.credentials import AnonymousCredentials
        from google.cloud.storage import Client

        client = Client(
            project="unit",
            credentials=AnonymousCredentials(),
            client_options={"api_endpoint": base},
        )
        store = GCSObjectStoreDriver(bucket="owned", client=client)
    elif state.provider == "azure_blob":
        from azure.core.credentials import AzureNamedKeyCredential
        from azure.storage.blob import BlobServiceClient

        client = BlobServiceClient(
            base + "/account",
            credential=AzureNamedKeyCredential("account", base64.b64encode(b"x" * 32).decode()),
        )
        store = AzureBlobObjectStoreDriver(
            account_url=base + "/account",
            account_name="account",
            container="owned",
            service_client=client,
        )
    else:
        store = S3ObjectStoreDriver(
            bucket="owned",
            region="us-east-1",
            endpoint_url=base,
            access_key="unit",
            secret_key="unit-secret-only",
            compatible=state.provider == "s3_compatible",
        )
        client = store.client
    store.open_stream = Mock(wraps=store.open_stream)
    state.store = store
    try:
        yield state
    finally:
        client.close()
        if state.provider == "gcs":
            store.http.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("fault", [None, "503", "disconnect"])
def test_actual_sdk_conditional_write_and_response_loss(sdk_http, fault):
    state = sdk_http
    state.fault = fault
    uri = state.store.uri_for_key("output")
    error = None
    try:
        result = write_attempt_output(
            state.store, uri, io.BytesIO(DATA), byte_size=len(DATA), sha256=SHA, grant_digest=GRANT
        )
    except Exception as exc:
        error = exc
    assert state.stored == DATA and len(state.puts) == 1
    if fault is None:
        assert error is None, repr(error)
        assert result.byte_size == len(DATA) and result.uri == uri and state.reads >= 1
        from azure.core.exceptions import HttpResponseError
        from botocore.exceptions import ClientError
        from google.api_core.exceptions import GoogleAPICallError

        with pytest.raises((HttpResponseError, ClientError, GoogleAPICallError)):
            write_attempt_output(
                state.store,
                uri,
                io.BytesIO(DATA),
                byte_size=len(DATA),
                sha256=SHA,
                grant_digest=GRANT,
            )
        assert len(state.puts) == 2 and state.stored == DATA
    else:
        assert error is not None and state.store.open_stream.call_count == 0
        # GCS queues bucket metadata reads asynchronously; their order is not a write retry.
        from urllib.parse import urlsplit

        assert all(
            method in {"PUT", "POST"}
            or (
                state.provider == "gcs"
                and method == "GET"
                and urlsplit(path).path == "/storage/v1/b/owned"
            )
            for method, path in state.events
        ), state.events
