"""Retire one original provider version without touching a replacement or neighbor."""

from types import SimpleNamespace

import pytest
from automl_api.storage.contracts import (
    DeletionPending,
    UploadCapabilityUnavailable,
    UploadContractError,
)
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from test_versioned_streams import provider as provider


@pytest.fixture
def history(provider, monkeypatch):
    store, uri, native = provider
    original, replacement = ("7", "99") if store.driver_name == "gcs" else ("v1", "v2")
    c = SimpleNamespace(
        store=store,
        uri=uri,
        original=original,
        replacement=replacement,
        live={("key", original), ("key", replacement), ("key-extra", original)},
        soft=set(),
        writes=[],
        reads=[],
        retained=False,
        stuck=False,
        lost=False,
        error=None,
        error_after_write=False,
        foreign=False,
    )

    def erase(key, version):
        c.writes.append((key, str(version)))
        if not c.stuck:
            c.live.discard((key, str(version)))
            if c.retained:
                c.soft.add((key, str(version)))
        if c.lost:
            raise OSError("Original delete accepted; acknowledgement lost")

    def inventory():
        c.reads.append(True)
        if c.error and (not c.error_after_write or c.writes):
            raise c.error

    if store.driver_name == "aws_s3":
        native.meta.config.retries = {"total_max_attempts": 1}

        def listed(**kwargs):
            inventory()
            assert kwargs == {"Bucket": "bucket", "Prefix": "key", "MaxKeys": 1000}
            return {
                "IsTruncated": False,
                "Versions": [{"Key": key, "VersionId": version} for key, version in sorted(c.live)],
            }

        def deleted(**kwargs):
            assert kwargs == {"Bucket": "bucket", "Key": "key", "VersionId": original}
            erase("key", kwargs["VersionId"])
            return {"VersionId": original}

        monkeypatch.setattr(native, "list_object_versions", listed)
        monkeypatch.setattr(native, "delete_object", deleted)
    elif store.driver_name == "azure_blob":

        def listed(*, name_starts_with, include, results_per_page):
            inventory()
            assert name_starts_with == "key" and results_per_page == 1000
            assert set(include) == {"versions", "deleted"}
            return [
                SimpleNamespace(name=key, version_id=version, deleted=soft)
                for soft, rows in [(False, c.live), (True, c.soft)]
                for key, version in sorted(rows)
            ]

        def blob(key, *, version_id):
            assert (key, version_id) == ("key", original)

            def delete(**kwargs):
                assert kwargs == {"retry_total": 0}
                erase(key, version_id)

            return SimpleNamespace(delete_blob=delete)

        monkeypatch.setattr(store.container, "list_blobs", listed)
        monkeypatch.setattr(store.container, "get_blob_client", blob)
    else:

        def get(key, *, generation, retry, soft_deleted=False):
            inventory()
            assert key == "key" and generation == int(original) and retry is None
            rows = c.soft if soft_deleted else c.live
            if (key, str(generation)) not in rows:
                return None

            def delete(*, if_generation_match, retry):
                assert if_generation_match == generation and retry is None
                erase(key, generation)

            return SimpleNamespace(
                name=key, generation=99 if c.foreign else generation, delete=delete
            )

        monkeypatch.setattr(store.bucket, "get_blob", get)
    return c


def test_exact_delete_preserves_replacement_and_neighbor(history):
    c = history
    c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)]
    assert c.live == {("key", c.replacement), ("key-extra", c.original)}
    assert len(c.reads) >= 2
    c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)]


@pytest.mark.parametrize("version", ["", " ", "null", True, 1, "v\n1"])
def test_invalid_version_has_no_native_reads_or_writes(history, version):
    c = history
    with pytest.raises(UploadContractError):
        c.store.delete(c.uri, version=version)
    assert not c.reads and not c.writes


def test_unknown_native_delete_outcome_recovers_by_original_absence(history):
    c = history
    c.lost = True
    with pytest.raises(OSError, match="acknowledgement lost"):
        c.store.delete(c.uri, version=c.original)
    c.lost = False
    c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)]
    assert ("key", c.replacement) in c.live


def test_native_inventory_errors_cannot_mean_absence(history):
    c = history
    c.error = PermissionError("Original inventory denied")
    with pytest.raises(PermissionError):
        c.store.delete(c.uri, version=c.original)
    assert not c.writes


def test_still_live_original_is_not_a_retirement(history):
    c = history
    c.stuck = True
    with pytest.raises(DeletionPending):
        c.store.delete(c.uri, version=c.original)
    assert ("key", c.original) in c.live and ("key", c.replacement) in c.live


@pytest.mark.parametrize("provider", ["azure", "gcs"], indirect=True)
def test_soft_deleted_original_waits_without_deleting_replacement(history):
    c = history
    c.retained = True
    for _ in range(2):
        with pytest.raises(DeletionPending):
            c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)]
    assert c.soft == {("key", c.original)}
    c.soft.clear()
    c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)] and ("key", c.replacement) in c.live


@pytest.mark.parametrize("provider", ["gcs"], indirect=True)
def test_gcs_foreign_generation_response_cannot_authorize_delete(history):
    c = history
    c.foreign = True
    with pytest.raises(UploadContractError):
        c.store.delete(c.uri, version=c.original)
    assert not c.writes


def test_embedded_cannot_claim_versioned_retirement(tmp_path):
    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="bucket")
    store.put_bytes("key", b"original")
    with pytest.raises(UploadCapabilityUnavailable):
        store.delete(store.uri_for_key("key"), version="v1")
    assert store.read_bytes(store.uri_for_key("key")) == b"original"


@pytest.mark.parametrize("provider", ["s3"], indirect=True)
@pytest.mark.parametrize("fault", ["missing", "repeat", "later-page", "key-only-marker"])
def test_s3_absence_requires_complete_advancing_version_inventory(provider, monkeypatch, fault):
    store, uri, native = provider
    native.meta.config.retries = {"total_max_attempts": 1}
    requests, writes = [], []

    def listed(**kwargs):
        requests.append(kwargs)
        if fault in {"later-page", "key-only-marker"} and len(requests) == 2:
            assert kwargs["KeyMarker"] == "key"
            assert kwargs.get("VersionIdMarker") == ("older" if fault == "later-page" else None)
            return {"IsTruncated": False, "Versions": []}
        return {
            "IsTruncated": True,
            **(
                {"NextKeyMarker": "key", "NextVersionIdMarker": "older"}
                if fault in {"repeat", "later-page"}
                else {"NextKeyMarker": "key"}
                if fault == "key-only-marker"
                else {}
            ),
        }

    monkeypatch.setattr(native, "list_object_versions", listed)
    monkeypatch.setattr(native, "delete_object", lambda **kwargs: writes.append(kwargs))
    if fault in {"later-page", "key-only-marker"}:
        store.delete(uri, version="v1")
        assert len(requests) == 2
    else:
        with pytest.raises(UploadContractError):
            store.delete(uri, version="v1")
    assert not writes


@pytest.mark.parametrize("provider", ["s3"], indirect=True)
def test_s3_retirement_rejects_hidden_retries_before_inventory(provider, monkeypatch):
    store, uri, native = provider
    native.meta.config.retries = {"total_max_attempts": 9}
    reads = []
    monkeypatch.setattr(native, "list_object_versions", lambda **kwargs: reads.append(kwargs))
    with pytest.raises(UploadContractError):
        store.delete(uri, version="v1")
    assert not reads


@pytest.mark.parametrize(
    "kind,fault",
    [
        (kind, fault)
        for kind in ["s3", "s3c", "azure", "gcs"]
        for fault in [None, "disconnect", "503"]
    ]
    + [("azure", "retained"), ("gcs", "retained")],
)
def test_actual_sdk_exact_deletion_and_lost_acknowledgement(kind, fault, monkeypatch):
    import base64
    import json
    import os
    import socket
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit

    from automl_api.storage.azure import AzureBlobObjectStoreDriver
    from automl_api.storage.gcs import GCSObjectStoreDriver
    from automl_api.storage.s3 import S3ObjectStoreDriver
    from azure.core.credentials import AzureNamedKeyCredential
    from azure.core.exceptions import AzureError
    from azure.storage.blob import BlobServiceClient
    from botocore.exceptions import BotoCoreError, ClientError
    from google.api_core.exceptions import GoogleAPICallError
    from google.auth.credentials import AnonymousCredentials
    from google.cloud.storage import Client
    from requests.exceptions import RequestException

    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    original, replacement = ("7", "99") if kind == "gcs" else ("v1", "v2")
    live = {("key", original), ("key", replacement), ("key-extra", original)}
    requests, writes, soft = [], [], set()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def respond(self, status, body=b"", **headers):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
            self.close_connection = True

        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            requests.append((self.command, urlsplit(self.path).path, query))
            if kind in {"s3", "s3c"}:
                body = (
                    '<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    "<Name>bucket</Name><Prefix>key</Prefix><IsTruncated>false</IsTruncated>"
                    + "".join(
                        f"<Version><Key>{key}</Key><VersionId>{version}</VersionId></Version>"
                        for key, version in sorted(live)
                    )
                    + "</ListVersionsResult>"
                ).encode()
                self.respond(200, body, **{"Content-Type": "application/xml"})
            elif kind == "azure":
                body = (
                    "<EnumerationResults><Prefix>key</Prefix><Blobs>"
                    + "".join(
                        f"<Blob><Name>{key}</Name><VersionId>{version}</VersionId>"
                        f"<IsCurrentVersion>false</IsCurrentVersion><Deleted>{str(deleted).lower()}</Deleted>"
                        "<Properties><Last-Modified>Mon, 26 Oct 2026 00:00:00 GMT</Last-Modified>"
                        '<Etag>"original"</Etag><Content-Length>10</Content-Length>'
                        "<BlobType>BlockBlob</BlobType></Properties></Blob>"
                        for key, version, deleted in sorted(
                            [(key, version, False) for key, version in live]
                            + [(key, version, True) for key, version in soft]
                        )
                    )
                    + "</Blobs><NextMarker/></EnumerationResults>"
                ).encode()
                self.respond(200, body, **{"Content-Type": "application/xml"})
            else:
                if urlsplit(self.path).path == "/storage/v1/b/bucket":
                    self.respond(
                        200,
                        json.dumps(dict(kind="storage#bucket", name="bucket")).encode(),
                        **{"Content-Type": "application/json"},
                    )
                    return
                selected = query.get("generation", [replacement])[0]
                rows = soft if query.get("softDeleted", ["false"])[0].lower() == "true" else live
                if ("key", selected) in rows:
                    self.respond(
                        200,
                        json.dumps(
                            dict(bucket="bucket", name="key", generation=selected, size="10")
                        ).encode(),
                        **{"Content-Type": "application/json"},
                    )
                else:
                    self.respond(
                        404,
                        b'{"error":{"code":404,"message":"Original generation absent"}}',
                        **{"Content-Type": "application/json"},
                    )

        def do_DELETE(self):
            query = parse_qs(urlsplit(self.path).query)
            writes.append((urlsplit(self.path).path, query))
            version = query.get("versionId", query.get("versionid", query.get("generation")))
            if version != [original]:
                self.respond(400, b"Unqualified deletion")
                return
            live.discard(("key", original))
            if fault == "retained":
                soft.add(("key", original))
            if fault == "disconnect":
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            elif fault == "503":
                body = (
                    b'{"error":{"code":503,"message":"Accepted response lost"}}'
                    if kind == "gcs"
                    else (
                        b"<Error><Code>ServerBusy</Code>"
                        b"<Message>Accepted response lost</Message></Error>"
                    )
                )
                self.respond(
                    503,
                    body,
                    **{"Content-Type": "application/json" if kind == "gcs" else "application/xml"},
                )
            else:
                self.respond(202 if kind == "azure" else 204, **{"x-amz-version-id": original})

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    sdk = None
    try:
        if kind in {"s3", "s3c"}:
            store = S3ObjectStoreDriver(
                bucket="bucket",
                endpoint_url=endpoint,
                region="us-east-1",
                access_key="unit",
                secret_key="unit",
                compatible=kind == "s3c",
            )
            sdk = store.client
        elif kind == "azure":
            sdk = BlobServiceClient(
                endpoint + "/account",
                credential=AzureNamedKeyCredential("account", base64.b64encode(b"x" * 32).decode()),
                retry_total=9,
            )
            store = AzureBlobObjectStoreDriver(
                account_url=endpoint + "/account",
                account_name="account",
                container="bucket",
                service_client=sdk,
            )
        else:
            sdk = Client(
                project="unit",
                credentials=AnonymousCredentials(),
                client_options={"api_endpoint": endpoint},
            )
            store = GCSObjectStoreDriver(bucket="bucket", client=sdk, http_client=SimpleNamespace())
        uri = store.uri_for_key("key")
        if fault in {"disconnect", "503"}:
            with pytest.raises(
                (AzureError, BotoCoreError, ClientError, GoogleAPICallError, RequestException)
            ):
                store.delete(uri, version=original)
        elif fault == "retained":
            for _ in range(2):
                with pytest.raises(DeletionPending):
                    store.delete(uri, version=original)
            assert soft == {("key", original)} and len(writes) == 1
            soft.clear()
        else:
            store.delete(uri, version=original)
        store.delete(uri, version=original)
        assert live == {("key", replacement), ("key-extra", original)}
        assert len(writes) == 1
        if kind == "gcs":
            assert writes[0][1]["generation"] == writes[0][1]["ifGenerationMatch"] == [original]

            def pinned_reads():
                objects = [
                    query for _, path, query in requests if path == "/storage/v1/b/bucket/o/key"
                ]
                assert objects and all(query.get("generation") == [original] for query in objects)
                assert all(
                    path in {"/storage/v1/b/bucket", "/storage/v1/b/bucket/o/key"}
                    for _, path, _ in requests
                )
                assert any(
                    query.get("softDeleted", ["false"])[0].lower() == "true" for query in objects
                )

            pinned_reads()
            latest = sdk.bucket("bucket").get_blob("key", retry=None)
            assert str(latest.generation) == replacement
            with pytest.raises(AssertionError):
                pinned_reads()
        elif kind in {"s3", "s3c"}:
            assert writes[0][1] == {"versionId": [original]}
            assert all(query.get("prefix") == ["key"] for _, _, query in requests)
        else:
            assert writes[0][1] == {"versionid": [original]}
            assert all(query.get("include") == ["versions,deleted"] for _, _, query in requests)
    finally:
        if sdk:
            sdk.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_unscoped_uri_cannot_reach_native_deletion(history):
    c = history
    with pytest.raises(ValueError):
        c.store.delete("other://unowned-bucket/key", version=c.original)
    assert not c.writes and not c.reads


def test_failed_confirmation_after_accepted_delete_is_not_success(history):
    c = history
    c.error = PermissionError("Original absence confirmation denied")
    c.error_after_write = True
    with pytest.raises(PermissionError):
        c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)]
    c.error = None
    c.store.delete(c.uri, version=c.original)
    assert c.writes == [("key", c.original)] and ("key", c.replacement) in c.live


@pytest.mark.parametrize("provider", ["s3"], indirect=True)
def test_s3_original_on_later_page_is_the_only_deleted_version(provider, monkeypatch):
    store, uri, native = provider
    native.meta.config.retries = {"total_max_attempts": 1}
    pages = iter(
        [
            {
                "IsTruncated": True,
                "Versions": [{"Key": "key", "VersionId": "v2"}],
                "NextKeyMarker": "key",
                "NextVersionIdMarker": "v2",
            },
            {"IsTruncated": False, "Versions": [{"Key": "key", "VersionId": "v1"}]},
            {"IsTruncated": False, "Versions": [{"Key": "key", "VersionId": "v2"}]},
        ]
    )
    reads, writes = [], []

    def listed(**kwargs):
        reads.append(kwargs)
        return next(pages)

    def delete(**kwargs):
        writes.append(kwargs)
        return {"VersionId": "v1"}

    monkeypatch.setattr(native, "list_object_versions", listed)
    monkeypatch.setattr(native, "delete_object", delete)
    store.delete(uri, version="v1")
    assert len(reads) == 3 and reads[1]["VersionIdMarker"] == "v2"
    assert writes == [{"Bucket": "bucket", "Key": "key", "VersionId": "v1"}]


@pytest.mark.parametrize("provider", ["s3"], indirect=True)
def test_s3_missing_version_in_inventory_is_not_original_absence(provider, monkeypatch):
    store, uri, native = provider
    native.meta.config.retries = {"total_max_attempts": 1}
    monkeypatch.setattr(
        native,
        "list_object_versions",
        lambda **kwargs: {"IsTruncated": False, "Versions": [{"Key": "key"}]},
    )
    writes = []
    monkeypatch.setattr(native, "delete_object", lambda **kwargs: writes.append(kwargs))
    with pytest.raises(UploadContractError):
        store.delete(uri, version="v1")
    assert not writes


@pytest.mark.parametrize("provider", ["s3", "azure"], indirect=True)
def test_bounded_inventory_exhaustion_cannot_claim_absence(provider, monkeypatch):
    store, uri, native = provider
    reads = []
    if store.driver_name == "aws_s3":
        native.meta.config.retries = {"total_max_attempts": 1}

        def listed(**kwargs):
            reads.append(kwargs)
            return {
                "IsTruncated": True,
                "NextKeyMarker": "key",
                "NextVersionIdMarker": str(len(reads)),
            }

        monkeypatch.setattr(native, "list_object_versions", listed)
    else:

        def listed(**kwargs):
            for i in range(10_001):
                reads.append(i)
                yield SimpleNamespace(name="key-extra", version_id="neighbor", deleted=False)

        monkeypatch.setattr(store.container, "list_blobs", listed)
    with pytest.raises(DeletionPending):
        store.delete(uri, version="v1")
    assert len(reads) == (100 if store.driver_name == "aws_s3" else 10_001)
