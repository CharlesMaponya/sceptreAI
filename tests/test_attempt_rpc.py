"""Control requests keep projected identity fresh, bounded and out of errors."""

import uuid

import httpx
import pytest
from automl_api.training import attempt_rpc as rpc


@pytest.fixture
def control(tmp_path, monkeypatch):
    token = tmp_path / "identity"
    token.write_text("first-token")
    requests = []
    reply = {"status": 200, "content": b'{"ok":true}', "headers": {}}
    original = httpx.Client

    def dispatch(request):
        requests.append(request)
        return httpx.Response(reply["status"], content=reply["content"], headers=reply["headers"])

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs["timeout"] == reply.get("timeout", 5) and kwargs["verify"] is not False
        return original(**kwargs, transport=httpx.MockTransport(dispatch))

    monkeypatch.setattr(rpc.httpx, "Client", client)
    value = rpc.AttemptRPC(
        uuid.uuid4(), base_url="https://control.invalid/internal/attempts", token_file=token
    )
    return value, token, requests, reply


def test_identity_is_read_again_for_each_control_request(control):
    value, token, requests, _ = control
    assert value.request("POST", "provider-import/start", max_bytes=64) == {"ok": True}
    token.write_text("second-token\n")
    assert value.request("POST", "provider-import/heartbeat", max_bytes=64) == {"ok": True}
    assert [r.headers["authorization"] for r in requests] == [
        "Bearer first-token",
        "Bearer second-token",
    ]
    assert all(r.headers["accept-encoding"] == "identity" for r in requests)
    assert str(value.attempt_id) in requests[0].url.path


@pytest.mark.parametrize("content", [b"", b" \n", b"\xff", b"first-token" + b" " * 65536])
def test_invalid_identity_is_rejected_before_network(control, content):
    value, token, requests, _ = control
    token.write_bytes(content)
    with pytest.raises(rpc.AttemptControlUnavailable, match="Workload control request unavailable"):
        value.request("POST", "provider-import/start", max_bytes=64)
    assert not requests


@pytest.mark.parametrize("fault", ["redirect", "unavailable", "large", "json", "encoding"])
def test_control_faults_are_bounded_and_sanitized(control, fault):
    value, _, requests, reply = control
    if fault == "redirect":
        reply.update(status=307, headers={"Location": "https://elsewhere.invalid/?secret"})
    elif fault == "unavailable":
        reply.update(status=503, content=b"private-provider-detail")
    elif fault == "large":
        reply["content"] = b"x" * 65
    elif fault == "json":
        reply["content"] = b"private-provider-detail"
    else:
        # Valid uncompressed JSON under an encoded header must still be rejected.
        reply["headers"] = {"Content-Encoding": "identity, identity"}
    with pytest.raises(rpc.AttemptControlUnavailable) as error:
        value.request("POST", "provider-import/start", max_bytes=64)
    assert str(error.value) == "Workload control request unavailable"
    assert error.value.__suppress_context__
    assert len(requests) == 1


@pytest.mark.parametrize("limit", [0, -1, True, "64"])
def test_invalid_budget_never_sends_identity(control, limit):
    value, _, requests, _ = control
    with pytest.raises(rpc.AttemptControlUnavailable):
        value.request("POST", "provider-import/start", max_bytes=limit)
    assert not requests


def test_binary_upload_binds_exact_bytes_nonce_and_fresh_projected_token(control):
    value, token, requests, reply = control
    reply["timeout"] = 60
    nonce = uuid.uuid4()
    payload = b"\x00binary-model\xff"
    for identity in ("first-token", "rotated-token"):
        token.write_text(identity)
        assert value.request(
            "PUT", "objects/pipeline/declared", content=payload, request_id=nonce, max_bytes=64
        ) == {"ok": True}
        request = requests[-1]
        assert request.content == payload
        assert request.headers["x-request-id"] == str(nonce)
        assert request.headers["authorization"] == "Bearer " + identity
        assert request.headers["content-type"] == "application/octet-stream"
        assert request.headers["content-length"] == str(len(payload))
    assert len(requests) == 2


@pytest.mark.parametrize(
    "options",
    [
        {"content": "text"},
        {"content": bytearray(b"binary")},
        {"content": b"binary", "payload": {"unexpected": True}},
        {"content": b"binary", "request_id": "invalid"},
    ],
)
def test_invalid_binary_declaration_fails_before_network(control, options):
    value, _, requests, _ = control
    with pytest.raises(rpc.AttemptControlUnavailable):
        value.request("PUT", "objects/pipeline/declared", max_bytes=64, **options)
    assert not requests
