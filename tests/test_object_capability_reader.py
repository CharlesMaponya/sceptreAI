"""HTTP capability-only reads: exact delegation, bounded bytes and no identity leakage."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx
import pytest
from automl_api.services.provider_copy_verification import verify_provider_objects
from automl_api.training import object_capabilities as caps
from test_provider_copy_verification import VersionStore, budget, manifest


@pytest.fixture
def reader_case(tmp_path, monkeypatch, request):
    source = VersionStore()
    value = manifest(source)
    provider = getattr(request, "param", "aws")
    raw = value.model_dump(mode="json")
    raw["provider"] = raw["safe_data"]["provider"] = provider
    raw["canonical_provider"] = "aws" if provider != "aws" else "gcp"
    scheme = {"aws": "s3c://safe/", "gcp": "gs://safe/", "azure": "az://account/safe/"}[provider]
    for item in raw["safe_data"]["objects"]:
        content = source.objects[(item["uri"], item["version"])]
        item["uri"] = item["uri"].replace("s3c://safe/", scheme)
        item["version"] = {
            "aws": item["version"],
            "gcp": "123456",
            "azure": "2026-10-19T00:00:00.0000000Z",
        }[provider]
        source.objects[(item["uri"], item["version"])] = content
    value = type(value).model_validate(raw)
    version_header = {
        "aws": "x-amz-version-id",
        "gcp": "x-goog-generation",
        "azure": "x-ms-version-id",
    }[provider]
    token = tmp_path / "token"
    token.write_text("rotating-token-a")
    state = {"fault": None, "requests": [], "rpc": [], "body_reads": 0}
    actual_client = httpx.Client

    def handler(request):
        state["requests"].append(request)
        if request.url.host == "control.invalid":
            assert request.method == "POST" and request.url.path.endswith("/objects/read")
            data = json.loads(request.content)
            state["rpc"].append(data)
            assert set(data) == {"request_id", "role", "object_index"}
            item = [i for i in value.safe_data.objects if i.role == data["role"]][
                data["object_index"]
            ]
            fault = state["fault"]
            query = {
                "X-Amz-Date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                "X-Amz-Expires": "30",
                "versionId": item.version,
                "unit-signature": "private",
            }
            if provider == "gcp":
                query = {
                    "X-Goog-Date": query["X-Amz-Date"],
                    "X-Goog-Expires": "30",
                    "generation": item.version,
                }
            elif provider == "azure":
                query = {
                    "se": (datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
                    "sp": "r",
                    "spr": "https",
                    "versionid": item.version,
                }
            if fault == "long_valid":
                query["X-Amz-Security-Token"] = "X" * 12000
            url = "https://objects.invalid/" + str(data["object_index"]) + "?" + urlencode(query)
            response = {
                "grant_id": str(uuid.uuid4()),
                "grant_digest": "a" * 64,
                "expires_at": (datetime.now(UTC) + timedelta(seconds=50)).isoformat(),
                "byte_limit": item.byte_size,
                "sha256": item.sha256,
                "url": url,
            }
            if fault == "size":
                response["byte_limit"] += 1
            elif fault == "sha":
                response["sha256"] = "b" * 64
            elif fault == "expiry":
                response["expires_at"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
            elif fault == "http":
                response["url"] = url.replace("https://", "http://")
            elif fault == "version":
                response["url"] = url.replace("versionId=", "versionId=other-")
            elif fault == "duplicate":
                response["url"] += "&X-Amz-Expires=30"
            elif fault == "large_reply":
                return httpx.Response(200, content=b"X" * (caps.MAX_GRANT_RESPONSE_BYTES + 1))
            elif fault == "redirect":
                return httpx.Response(307, headers={"Location": "https://evil.invalid"})
            elif fault == "unavailable":
                return httpx.Response(503)
            state["item"] = item
            return httpx.Response(200, json=response)
        assert request.url.host == "objects.invalid" and request.method == "GET"
        assert "authorization" not in request.headers
        assert request.headers["accept-encoding"] == "identity"
        item = state["item"]
        content = source.objects[(item.uri, item.version)]
        headers = {"Content-Length": str(len(content)), version_header: item.version}
        status = 200
        fault = state["fault"]
        if fault == "response_missing_version":
            del headers[version_header]
        elif fault == "response_empty_version":
            headers[version_header] = ""
        elif fault == "response_null_version":
            headers[version_header] = "null"
        elif fault == "response_wrong_version":
            headers[version_header] = "unapproved-version"
        elif fault == "response_wrong_provider_header":
            del headers[version_header]
            headers["x-goog-generation" if provider != "gcp" else "x-amz-version-id"] = item.version
        elif fault in {"response_repeated_version", "response_conflicting_version"}:
            headers = [
                *headers.items(),
                (
                    version_header,
                    item.version if fault == "response_repeated_version" else "unapproved-version",
                ),
            ]
        elif fault == "length":
            headers["Content-Length"] = "1"
        elif fault == "encoding":
            headers["Content-Encoding"] = "gzip"
        elif fault == "range":
            headers["Content-Range"] = "bytes 0-1/100"
        elif fault == "status":
            status = 403
        elif fault == "oversize":
            content += b"extra"
            headers = {version_header: item.version}
        elif fault == "truncated":
            content = content[:-1]
            headers = {version_header: item.version}
        elif fault == "stream_error":

            class BrokenStream(httpx.SyncByteStream):
                def __iter__(self):
                    yield content[:10]
                    raise httpx.ReadError("https://objects.invalid/?unit-signature=private")

            return httpx.Response(200, headers=headers, stream=BrokenStream())

        class Body(httpx.SyncByteStream):
            def __iter__(self):
                state["body_reads"] += 1
                yield content

        return httpx.Response(status, headers=headers, stream=Body())

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs["verify"].check_hostname
        return actual_client(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(caps.httpx, "Client", client)
    reader = caps.ProviderGrantReader(
        value,
        uuid.uuid4(),
        base_url="https://control.invalid/api/v1/internal/attempts",
        token_file=token,
    )
    return reader, source, value, token, state


def consume(reader, item):
    data = bytearray()
    with reader.open_stream(item.uri, version=item.version) as stream:
        while block := stream.read(65536):
            data.extend(block)
    return bytes(data)


def test_capabilities_verify_actual_signed_copy_bytes_and_reread_rotated_identity(reader_case):
    reader, source, value, token, state = reader_case
    proof = verify_provider_objects(value, reader, budget())
    assert proof.rows == {"train": 2, "validation": 2}
    assert len(state["rpc"]) == len(value.safe_data.objects)
    first = value.safe_data.objects[0]
    token.write_text("rotating-token-b")
    assert consume(reader, first) == source.objects[(first.uri, first.version)]
    rpc_headers = [
        r.headers["authorization"] for r in state["requests"] if r.url.host == "control.invalid"
    ]
    assert (
        rpc_headers[0] == "Bearer rotating-token-a" and rpc_headers[-1] == "Bearer rotating-token-b"
    )
    assert len({r["request_id"] for r in state["rpc"]}) == len(state["rpc"])
    assert not hasattr(reader, "put_bytes") and not hasattr(reader, "client")


@pytest.mark.parametrize(
    "fault",
    [
        "size",
        "sha",
        "expiry",
        "http",
        "version",
        "duplicate",
        "large_reply",
        "redirect",
        "unavailable",
    ],
)
def test_invalid_broker_response_never_reaches_native_storage(reader_case, fault):
    reader, source, value, _, state = reader_case
    item = value.safe_data.objects[0]
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
    state["requests"].clear()
    state["fault"] = fault
    with pytest.raises(caps.ProviderObjectUnavailable) as error:
        consume(reader, item)
    assert "private" not in str(error.value)
    assert all(r.url.host == "control.invalid" for r in state["requests"])


@pytest.mark.parametrize(
    "fault", ["length", "encoding", "range", "status", "oversize", "truncated", "stream_error"]
)
def test_native_response_cannot_change_size_encoding_or_disclose_signed_errors(reader_case, fault):
    reader, source, value, _, state = reader_case
    item = value.safe_data.objects[0]
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
    state["fault"] = fault
    with pytest.raises((ValueError, caps.ProviderObjectUnavailable)) as error:
        consume(reader, item)
    assert "private" not in str(error.value)


def test_unlisted_uri_version_and_unbounded_reads_fail_with_a_positive_control(reader_case):
    reader, source, value, token, state = reader_case
    item = value.safe_data.objects[0]
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
    state["requests"].clear()
    for uri, version in (("s3c://safe/raw", item.version), (item.uri, "unapproved")):
        with pytest.raises(ValueError, match="approved safe declaration"):
            with reader.open_stream(uri, version=version):
                pytest.fail("Forbidden source was opened")
    assert state["requests"] == []
    with reader.open_stream(item.uri, version=item.version) as stream:
        with pytest.raises(ValueError, match="bounded"):
            stream.read(-1)
    token.write_bytes(b"X" * 65537)
    state["requests"].clear()
    with pytest.raises(caps.ProviderObjectUnavailable):
        consume(reader, item)
    assert state["requests"] == []


def test_missing_broker_environment_never_falls_back_to_ambient_storage(reader_case, monkeypatch):
    from automl_api.storage import object_store

    _, _, value, _, _ = reader_case
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    monkeypatch.delenv("ATTEMPT_CONTROL_BASE_URL", raising=False)
    monkeypatch.setenv("OBJECT_STORE_ACCESS_KEY", "ambient-credential-must-not-be-used")
    monkeypatch.setattr(
        object_store,
        "get_object_store",
        lambda: pytest.fail("Missing broker identity must not recover ambient authority"),
    )
    with pytest.raises(caps.ProviderObjectUnavailable, match="broker identity"):
        caps.provider_grant_reader(value)


def test_long_temporary_provider_session_query_still_has_a_bounded_reply(reader_case):
    reader, source, value, _, state = reader_case
    state["fault"] = "long_valid"
    item = value.safe_data.objects[0]
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
    native = next(r for r in state["requests"] if r.url.host == "objects.invalid")
    assert len(native.url.params["X-Amz-Security-Token"]) == 12000
    assert "authorization" not in native.headers


@pytest.mark.parametrize("reader_case", ["aws", "gcp", "azure"], indirect=True)
@pytest.mark.parametrize(
    "fault",
    [
        "response_missing_version",
        "response_empty_version",
        "response_null_version",
        "response_wrong_version",
        "response_repeated_version",
        "response_conflicting_version",
        "response_wrong_provider_header",
    ],
)
def test_native_response_version_is_required_before_body_exposure(reader_case, fault):
    reader, source, value, _, state = reader_case
    item = value.safe_data.objects[0]
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
    body_reads = state["body_reads"]
    state["fault"] = fault
    with pytest.raises(caps.ProviderObjectUnavailable):
        with reader.open_stream(item.uri, version=item.version):
            pytest.fail("A response with unapproved version metadata reached the consumer")
    assert state["body_reads"] == body_reads
    state["fault"] = None
    assert consume(reader, item) == source.objects[(item.uri, item.version)]
