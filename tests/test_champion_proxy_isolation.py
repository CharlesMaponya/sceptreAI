"""Champion capabilities and control tokens cannot inherit ambient proxy routing."""

import hashlib
import os
import signal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

import pytest
from automl_api.training import evaluation_worker, refit_worker
from automl_api.training.champion_evaluation import GrantedFinalStore
from cryptography.hazmat.primitives import serialization
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_worker import broker_worker_case as broker_worker_case
from test_champion_evaluation_worker import worker_case as worker_case
from test_champion_refit_control import broker_control_case as broker_control_case
from test_champion_refit_control import control_case as control_case

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


@pytest.fixture(params=[False, True])
def ambient_proxy(request, monkeypatch):
    calls = []

    class Proxy(BaseHTTPRequestHandler):
        def do_CONNECT(self):
            calls.append(self.path)
            self.send_error(502, "Unexpected proxy route")

        def log_message(self, *_):
            pass

    for name in list(os.environ):
        if name.lower().endswith("_proxy"):
            monkeypatch.delenv(name)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    if request.param:
        url = f"http://127.0.0.1:{server.server_port}"
        for name in ("http_proxy", "https_proxy", "all_proxy"):
            monkeypatch.setenv(name, url)
            monkeypatch.setenv(name.upper(), url)
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
    try:
        yield calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_refit_reads_ignore_ambient_proxy(broker_control_case, ambient_proxy):
    client, _, _, plan, _, _, ca, _ = broker_control_case
    result = refit_worker.run(client, ca_file=ca)
    assert result["row_count"] == sum(part.rows for part in plan.partitions)
    assert ambient_proxy == []


def test_evaluator_pipeline_ignores_ambient_proxy(broker_worker_case, ambient_proxy, tmp_path):
    c = broker_worker_case
    c.control.post("start").raise_for_status()
    pipeline = evaluation_worker.CachedPipeline(
        c.plan, c.control, tmp_path / "pipeline", ca_file=c.ca, progress=lambda: None
    )
    with pipeline.open_stream(c.plan.frozen_pipeline.uri) as source:
        assert hashlib.sha256(source.read()).hexdigest() == c.plan.frozen_pipeline.sha256
    assert ambient_proxy == []


def test_evaluator_final_reads_ignore_ambient_proxy(worker_case, ambient_proxy):
    c = worker_case
    c.control.post("start").raise_for_status()
    reply = c.authority.post(
        f"allocations/{c.plan.allocation_id}/credentials",
        json={"provider_manifest_digest": c.plan.final_manifest.digest},
    )
    reply.raise_for_status()
    final = GrantedFinalStore(c.plan, reply.json(), c.public_key, ca_file=c.ca)
    for uri, (item, _) in final.objects.items():
        with final.open_stream(uri) as source:
            assert hashlib.sha256(source.read()).hexdigest() == item.sha256
        with pytest.raises(ValueError, match="consumed or expired"):
            final.open_stream(uri)
    assert ambient_proxy == []
    assert all(authorization is None for _, authorization in c.requests)


def test_refit_control_client_ignores_ambient_proxy(control_case, ambient_proxy, monkeypatch):
    client, _, _, _, _, _, ca, token = control_case
    client.post("start").raise_for_status()
    url = client.object_url
    parts = urlsplit(url)
    monkeypatch.setenv("REFIT_CONTROL_URL", f"{parts.scheme}://{parts.netloc}/")
    monkeypatch.setenv("REFIT_CONTROL_TOKEN", token)
    monkeypatch.setenv("REFIT_CA_FILE", ca)
    calls = []

    def run(control, *, ca_file):
        assert ca_file == ca
        reply = control.get(parts.path)
        reply.raise_for_status()
        calls.append(reply.content)

    monkeypatch.setattr(refit_worker, "run", run)
    previous = signal.getsignal(signal.SIGTERM)
    try:
        refit_worker.main()
    finally:
        signal.signal(signal.SIGTERM, previous)
    assert len(calls) == 1 and calls[0]
    assert ambient_proxy == []


@pytest.mark.parametrize("target", ["control", "authority"])
def test_evaluator_control_clients_ignore_ambient_proxy(
    worker_case, ambient_proxy, target, monkeypatch, tmp_path
):
    c = worker_case
    c.control.post("start").raise_for_status()
    url = c.object_url
    parts = urlsplit(url)
    for name in ("EVALUATION_CONTROL_URL", "EVALUATION_AUTHORITY_URL"):
        monkeypatch.setenv(name, f"{parts.scheme}://{parts.netloc}/")
    for name in ("EVALUATION_CONTROL_TOKEN", "EVALUATION_AUTHORITY_TOKEN"):
        monkeypatch.setenv(name, "synthetic-control-token")
    key_path = tmp_path / "result-key.pem"
    key_path.write_bytes(
        c.signer.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    public_path = tmp_path / "authority-key"
    public_path.write_text(c.public_key.public_bytes_raw().hex())
    monkeypatch.setenv("EVALUATION_RESULT_KEY_FILE", str(key_path))
    monkeypatch.setenv("EVALUATION_AUTHORITY_PUBLIC_KEY_FILE", str(public_path))
    monkeypatch.setenv("EVALUATION_CA_FILE", c.ca)
    calls = []

    def run(control, authority, **kwargs):
        assert kwargs["ca_file"] == c.ca
        reply = {"control": control, "authority": authority}[target].get(parts.path)
        reply.raise_for_status()
        calls.append(reply.content)

    monkeypatch.setattr(evaluation_worker, "run", run)
    previous = signal.getsignal(signal.SIGTERM)
    try:
        evaluation_worker.main()
    finally:
        signal.signal(signal.SIGTERM, previous)
    assert len(calls) == 1 and calls[0]
    assert ambient_proxy == []
