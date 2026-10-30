"""Real SDK traffic: exclusive writes retain original replies without hidden mutations."""

import base64
import json
import os
import socket
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import google_crc32c
import pytest
from automl_api.storage.azure import AzureBlobObjectStoreDriver
from automl_api.storage.gcs import GCSObjectStoreDriver
from automl_api.storage.s3 import S3ObjectStoreDriver

KEY = "projects/owned/input.csv"


@pytest.mark.parametrize(
    ("kind", "fault"),
    [
        (kind, fault)
        for kind in (
            "aws_s3",
            "s3_compatible",
            "azure_blob",
            "azure_chunks",
            "gcs",
            "gcs_resumable",
        )
        for fault in (None, "disconnect", "503", "missing-version", "collision", "corrupt-receipt")
        if fault != "corrupt-receipt" or kind.startswith("gcs")
    ],
)
def test_real_sdk_create_preserves_original_write_and_does_not_retry(monkeypatch, kind, fault):
    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    content = b"feature,target\n1,2\n3,4\n" if kind != "gcs_resumable" else b"x" * (9 * 1024 * 1024)
    crc = base64.b64encode(google_crc32c.value(content).to_bytes(4, "big")).decode("ascii")
    state = SimpleNamespace(commits=[], units=[], reads=[], deletes=[], metadata=[], condition=[])

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status, body=b"", **headers):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            self.close_connection = True

        def created(self, payload):
            state.commits.append(payload)
            state.condition.append(self.headers.get("If-None-Match"))
            if fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            if fault in {"503", "collision"}:
                status = 503 if fault == "503" else 412
                self.respond(
                    status,
                    b'{"error":{"code":503,"message":"Injected failure"}}',
                    **{"Content-Type": "application/json"},
                )
                return
            if kind.startswith("gcs"):
                response = dict(
                    bucket="owned",
                    name=KEY,
                    id=f"owned/{KEY}/7",
                    generation="7",
                    size=str(len(payload)),
                    crc32c=crc,
                    etag="original-etag",
                )
                if fault == "missing-version":
                    response.pop("generation")
                if fault == "corrupt-receipt":
                    response["crc32c"] = "AAAAAA=="
                self.respond(
                    200, json.dumps(response).encode(), **{"Content-Type": "application/json"}
                )
            else:
                version = "original-version"
                headers = {"ETag": '"original-etag"'}
                if fault != "missing-version":
                    headers[
                        "x-ms-version-id" if kind.startswith("azure") else "x-amz-version-id"
                    ] = version
                self.respond(201 if kind.startswith("azure") else 200, **headers)

        def do_POST(self):
            query = parse_qs(urlsplit(self.path).query)
            body = self.rfile.read(int(self.headers["Content-Length"]))
            assert query["ifGenerationMatch"] == ["0"]
            if query["uploadType"] == ["resumable"]:
                state.metadata.append(json.loads(body))
                self.respond(
                    200,
                    **{
                        "Location": f"http://127.0.0.1:{self.server.server_port}/resumable/original"
                    },
                )
            else:
                message = BytesParser(policy=policy.default).parsebytes(
                    b"Content-Type: " + self.headers["Content-Type"].encode() + b"\r\n\r\n" + body
                )
                metadata, data = list(message.iter_parts())
                state.metadata.append(json.loads(metadata.get_payload(decode=True)))
                self.created(data.get_payload(decode=True))

        def do_PUT(self):
            query = parse_qs(urlsplit(self.path).query)
            body = self.rfile.read(int(self.headers["Content-Length"]))
            if query.get("comp") == ["block"]:
                state.units.append(body)
                self.respond(201, **{"ETag": '"block-etag"'})
            elif query.get("comp") == ["blocklist"]:
                self.created(b"".join(state.units))
            else:
                self.created(body)

        def do_GET(self):
            if urlsplit(self.path).path == "/storage/v1/b/owned":
                self.respond(
                    200,
                    json.dumps(dict(name="owned", versioning=dict(enabled=True))).encode(),
                    **{"Content-Type": "application/json"},
                )
                return
            state.reads.append(self.path)
            self.respond(500)

        def do_HEAD(self):
            state.reads.append(self.path)
            self.respond(500)

        def do_DELETE(self):
            state.deletes.append(self.path)
            self.respond(204)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    sdk, http = None, None
    try:
        if kind.startswith("azure"):
            from azure.core.credentials import AzureNamedKeyCredential
            from azure.storage.blob import BlobServiceClient

            options = (
                {"max_single_put_size": 16, "max_block_size": 16} if kind == "azure_chunks" else {}
            )
            sdk = BlobServiceClient(
                endpoint + "/account",
                retry_total=9,
                credential=AzureNamedKeyCredential("account", base64.b64encode(b"x" * 32).decode()),
                **options,
            )
            store = AzureBlobObjectStoreDriver(
                account_url=endpoint + "/account",
                account_name="account",
                container="owned",
                service_client=sdk,
            )
        elif kind.startswith("gcs"):
            import httpx
            from google.auth.credentials import AnonymousCredentials
            from google.cloud.storage import Client

            sdk = Client(
                project="unit",
                credentials=AnonymousCredentials(),
                client_options={"api_endpoint": endpoint},
            )
            http = httpx.Client(trust_env=False)
            store = GCSObjectStoreDriver(bucket="owned", client=sdk, http_client=http)
        else:
            import boto3
            from botocore.config import Config

            sdk = boto3.client(
                "s3",
                endpoint_url=endpoint,
                region_name="us-east-1",
                aws_access_key_id="test",
                aws_secret_access_key="test",
                config=Config(retries={"total_max_attempts": 1}, connect_timeout=2, read_timeout=2),
            )
            store = S3ObjectStoreDriver(
                bucket="owned",
                client=sdk,
                endpoint_url=endpoint,
                access_key="test",
                secret_key="test",
                compatible=kind == "s3_compatible",
            )
        result, error = None, None
        try:
            result = store.create_bytes(KEY, content)
        except Exception as exc:
            error = exc
        assert state.commits == [content], repr(error)
        assert state.reads == [] and state.deletes == []
        if kind.startswith("gcs"):
            assert state.metadata == [{"name": KEY, "crc32c": crc}]
            assert len(state.condition) == 1
        else:
            assert state.condition == ["*"]
        if kind == "azure_chunks":
            assert len(state.units) == 2
        if fault in {"disconnect", "503", "missing-version", "collision"} or (
            fault == "corrupt-receipt" and kind.startswith("gcs")
        ):
            assert error is not None
        else:
            assert error is None, repr(error)
            assert result.byte_size == len(content)
            assert result.provider_headers == (
                {"generation": "7"}
                if kind.startswith("gcs")
                else {"version_id": "original-version"}
            )
    finally:
        if sdk:
            sdk.close()
        if http:
            http.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
