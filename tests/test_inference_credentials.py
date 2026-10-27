"""Pod keys survive container restart; admission and model bytes stay bounded."""

import hashlib
import uuid
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest
from automl_api.inference import credentials
from automl_api.services.inference_tls import (
    PROTOCOL,
    PeerIdentity,
    new_private_key,
    private_key_pem,
    public_key_digest,
)
from test_inference_tls import authority as authority


@pytest.fixture
def worker(tmp_path, monkeypatch, authority):
    run_id, project_id = uuid.uuid4(), uuid.uuid4()
    base = f"https://control.invalid/api/v1/internal/deployments/{run_id}"
    model = b"verified model bytes"
    for name, value in dict(
        INFERENCE_IDENTITY_PROTOCOL=PROTOCOL,
        INFERENCE_DEPLOYMENT_ID=str(run_id),
        INFERENCE_PROJECT_ID=str(project_id),
        INFERENCE_CA_SHA256=authority.digest,
        MODEL_SHA256=hashlib.sha256(model).hexdigest(),
        INFERENCE_IDENTITY_URL=base + "/identity",
        MODEL_DOWNLOAD_URL=base + "/model",
    ).items():
        monkeypatch.setenv(name, value)
    token = tmp_path / "projected-token"
    token.write_text("pod-token-one")
    monkeypatch.setattr(credentials, "TOKEN_PATH", str(token))
    monkeypatch.setattr(credentials, "IDENTITY_DIRECTORY", str(tmp_path / "identity"))
    c = SimpleNamespace(
        peer=PeerIdentity(str(project_id), str(run_id), *(str(uuid.uuid4()) for _ in range(3))),
        model=model,
        token=token,
        calls=[],
        fault=None,
    )
    original_client = httpx.Client

    def handler(request):
        c.calls.append((request.method, str(request.url), dict(request.headers), request.content))
        if c.fault == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.invalid/token-theft"})
        if c.fault == "denied":
            return httpx.Response(401)
        if request.method == "GET":
            body = {
                "model-large": model + b"extra",
                "model-short": model[:-1],
                "model-corrupt": b"x" * len(model),
            }.get(c.fault, model)
            return httpx.Response(200, content=body)
        peer = replace(c.peer, pod_uid=str(uuid.uuid4())) if c.fault == "peer" else c.peer
        proof = dict(
            protocol=PROTOCOL,
            identity=asdict(peer),
            ca_certificate=authority.pem.decode(),
            certificate=authority.issue_server(
                request.content, peer, datetime.now(UTC) + timedelta(seconds=300)
            ).decode(),
            model_digest=hashlib.sha256(model).hexdigest(),
            model_byte_size=len(model),
        )
        if c.fault == "protocol":
            proof["protocol"] = "legacy"
        elif c.fault == "digest":
            proof["model_digest"] = "0" * 64
        elif c.fault == "size":
            proof["model_byte_size"] = True
        elif c.fault == "csr-key":
            from automl_api.services.inference_tls import make_csr

            proof["certificate"] = authority.issue_server(
                make_csr(new_private_key()), peer, datetime.now(UTC) + timedelta(seconds=300)
            ).decode()
        elif c.fault == "large-response":
            return httpx.Response(200, content=b"x" * (3 * 16384 + 1))
        return httpx.Response(200, json=proof)

    def client(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert kwargs["verify"].check_hostname
        return original_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(credentials.httpx, "Client", client)
    c.worker = credentials.ServingCredentials()
    return c


def test_container_restart_keeps_original_private_key_and_reloads_current_token(worker):
    c = worker
    c.worker.renew()
    original_context = c.worker.context
    original_key = public_key_digest(c.worker.key.public_key())
    assert c.worker.active
    c.token.write_text("pod-token-two")
    c.worker.renew()
    assert c.worker.context is original_context
    restarted = credentials.ServingCredentials()
    assert public_key_digest(restarted.key.public_key()) == original_key
    assert not restarted.active
    restarted.renew()
    assert restarted.active
    assert c.calls[0][2]["authorization"] == "Bearer pod-token-one"
    assert c.calls[1][2]["authorization"] == "Bearer pod-token-two"
    assert c.calls[2][2]["authorization"] == "Bearer pod-token-two"
    assert private_key_pem(restarted.key) not in b"".join(call[3] for call in c.calls)


@pytest.mark.parametrize(
    "fault",
    ["denied", "redirect", "protocol", "digest", "size", "csr-key", "peer", "large-response"],
)
def test_failed_renewal_immediately_withdraws_readiness(worker, fault):
    c = worker
    c.worker.renew()
    c.fault = fault
    with pytest.raises(RuntimeError, match="renewal unavailable"):
        c.worker.renew()
    assert not c.worker.active and len(c.calls) == 2
    with pytest.raises(RuntimeError, match="active identity"):
        c.worker.download_model()
    assert len(c.calls) == 2


@pytest.mark.parametrize(
    "fault", [None, "model-large", "model-short", "model-corrupt", "denied", "redirect"]
)
def test_model_download_uses_current_pod_token_and_verifies_bytes_before_loading(worker, fault):
    c = worker
    c.worker.renew()
    c.fault = fault
    c.token.write_text("rotated-model-token")
    if fault is None:
        with c.worker.download_model() as source:
            assert source.read() == c.model
    else:
        with pytest.raises(RuntimeError, match="model download unavailable"):
            c.worker.download_model()
    assert len(c.calls) == 2 and c.calls[1][0] == "GET"
    assert c.calls[1][2]["authorization"] == "Bearer rotated-model-token"


@pytest.mark.parametrize("fault", ["empty", "oversized", "whitespace"])
def test_invalid_projected_token_never_reaches_transport(worker, fault):
    c = worker
    c.token.write_text(
        {"empty": "", "oversized": "x" * 65537, "whitespace": "foreign token"}[fault]
    )
    with pytest.raises(RuntimeError):
        c.worker.renew()
    assert not c.calls and not c.worker.active


@pytest.mark.parametrize("fault", ["corrupt", "public", "symlink"])
def test_invalid_existing_key_is_never_overwritten_or_replaced(tmp_path, fault):
    directory = tmp_path / "identity"
    key = credentials.serving_key(directory)
    path = directory / "serving-key.pem"
    original = path.read_bytes()
    if fault == "corrupt":
        path.write_bytes(b"incomplete previous write")
    elif fault == "public":
        path.chmod(0o644)
    else:
        target = tmp_path / "foreign-key"
        target.write_bytes(original)
        path.unlink()
        path.symlink_to(target)
    before = path.read_bytes()
    with pytest.raises((ValueError, OSError)):
        credentials.serving_key(directory)
    assert path.read_bytes() == before
    assert public_key_digest(key.public_key())


@pytest.mark.parametrize(
    "field,value",
    [
        ("INFERENCE_IDENTITY_PROTOCOL", "legacy"),
        ("INFERENCE_IDENTITY_URL", "http://control.invalid/internal/deployments/id/identity"),
        (
            "INFERENCE_IDENTITY_URL",
            "https://user:password@control.invalid/internal/deployments/id/identity",
        ),
        ("INFERENCE_CA_SHA256", "0" * 63),
        ("MODEL_DOWNLOAD_URL", "https://foreign.invalid/model"),
    ],
)
def test_malformed_worker_scope_cannot_bootstrap(worker, monkeypatch, field, value):
    monkeypatch.setenv(field, value)
    with pytest.raises(ValueError):
        credentials.ServingCredentials()
    assert not worker.calls
