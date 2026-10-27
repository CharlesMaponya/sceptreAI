"""The maintained entrypoint uses mTLS for data and exposes only plain health probes."""

import asyncio
import ssl
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
from automl_api.inference import app as inference
from automl_api.inference import runtime
from fastapi.testclient import TestClient
from test_inference_credentials import worker as worker
from test_inference_tls import authority as authority


def test_tls_declaration_cannot_use_the_legacy_plaintext_entrypoint(monkeypatch):
    monkeypatch.setenv("INFERENCE_IDENTITY_PROTOCOL", "inference-pod-tls-v1")
    monkeypatch.setenv("INFERENCE_GATEWAY_TOKEN", "legacy")
    with TestClient(inference.create_app()) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 503
        for path in ("/docs", "/openapi.json", "/v1/metadata"):
            response = client.get(path, headers={"X-Sceptre-Deployment-Token": "legacy"})
            assert response.status_code == 503


def test_tls_runtime_cannot_fall_back_to_object_store_or_static_model_download(monkeypatch):
    inference._load_model.cache_clear()
    monkeypatch.setenv("INFERENCE_IDENTITY_PROTOCOL", "inference-pod-tls-v1")
    with pytest.raises(RuntimeError, match="authenticated runtime loader"):
        inference._load_model()
    inference._load_model.cache_clear()


def test_health_listener_exposes_only_nonsensitive_probes():
    credentials = SimpleNamespace(active=False)
    with TestClient(runtime.health_app(credentials)) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 503
        for path in ("/docs", "/redoc", "/openapi.json", "/v1/metadata", "/v1/predict"):
            assert client.get(path).status_code == 404
        credentials.active = True
        assert client.get("/health/ready").status_code == 200


def test_real_runtime_tls_and_health_listeners_withdraw_serving_on_expiry(
    worker, authority, monkeypatch
):
    c = worker
    monkeypatch.setattr(runtime, "ServingCredentials", lambda: c.worker)
    loaded = []

    def load(source):
        loaded.append(source.read())
        assert loaded == [c.model]
        return SimpleNamespace(predict=lambda frame: [7] * len(frame))

    monkeypatch.setattr(runtime.joblib, "load", load)
    original_config, original_server = runtime.uvicorn.Config, runtime.uvicorn.Server
    servers = []

    def config(*args, **kwargs):
        assert kwargs["host"] == "0.0.0.0" and kwargs["port"] in {8080, 8443}
        return original_config(
            *args, **{**kwargs, "host": "127.0.0.1", "port": 0, "log_level": "error"}
        )

    def server(config):
        value = original_server(config)
        servers.append(value)
        return value

    monkeypatch.setattr(runtime.uvicorn, "Config", config)
    monkeypatch.setattr(runtime.uvicorn, "Server", server)

    async def scenario():
        task = asyncio.create_task(runtime.serve())
        try:
            deadline = time.monotonic() + 10
            while len(servers) != 2 or not all(server.started for server in servers):
                if task.done():
                    await task
                assert time.monotonic() < deadline, "Owned listeners failed to start"
                await asyncio.sleep(0.02)
            data, health = servers
            assert data.config.ssl is c.worker.context and health.config.ssl is None
            data_port = data.servers[0].sockets[0].getsockname()[1]
            health_port = health.servers[0].sockets[0].getsockname()[1]
            data_url = f"https://127.0.0.1:{data_port}"
            health_url = f"http://127.0.0.1:{health_port}"
            async with httpx.AsyncClient(
                verify=authority.client_context(), trust_env=False
            ) as client:

                async def request(path, *, method="GET", **kwargs):
                    query = client.build_request(method, data_url + path, **kwargs)
                    query.extensions["sni_hostname"] = c.peer.hostname
                    return await client.send(query)

                assert (await request("/v1/metadata")).status_code == 200
                response = await request("/v1/predict", method="POST", json={"records": [{"x": 1}]})
                assert response.status_code == 200 and response.json()["predictions"] == [7]
                async with httpx.AsyncClient(trust_env=False) as health_client:
                    assert (
                        await health_client.get(health_url + "/health/ready")
                    ).status_code == 200
                    assert (await health_client.get(health_url + "/v1/metadata")).status_code == 404
                    root_only = ssl.create_default_context(cadata=authority.pem.decode())
                    async with httpx.AsyncClient(verify=root_only, trust_env=False) as foreign:
                        query = foreign.build_request("GET", data_url + "/v1/metadata")
                        query.extensions["sni_hostname"] = c.peer.hostname
                        with pytest.raises(httpx.HTTPError):
                            await foreign.send(query)
                    c.worker.expires = datetime.min.replace(tzinfo=UTC)
                    assert (await request("/v1/metadata")).status_code == 503
                    assert (await request("/openapi.json")).status_code == 503
                    assert (
                        await health_client.get(health_url + "/health/ready")
                    ).status_code == 503
        finally:
            if servers:
                servers[0].should_exit = True
            await asyncio.wait_for(task, 10)
        assert loaded == [c.model]
        assert all(server.should_exit for server in servers)
        assert all(not listener.is_serving() for server in servers for listener in server.servers)

    asyncio.run(scenario())


def test_bootstrap_failure_cannot_start_either_listener(monkeypatch):
    def unavailable():
        raise RuntimeError("Serving unavailable")

    calls = []
    monkeypatch.setattr(runtime, "ServingCredentials", unavailable)
    monkeypatch.setattr(runtime.uvicorn, "Server", lambda *_: calls.append(True))
    with pytest.raises(RuntimeError, match="Serving unavailable"):
        asyncio.run(runtime.serve())
    assert not calls


def test_health_startup_failure_cannot_start_prediction_listener(worker, monkeypatch):
    monkeypatch.setattr(runtime, "ServingCredentials", lambda: worker.worker)
    monkeypatch.setattr(runtime.joblib, "load", lambda _: object())
    calls = []

    class Server:
        started = False
        should_exit = False

        def __init__(self, config):
            self.config = config
            calls.append(self)

        def run(self):
            return

        async def serve(self):
            raise AssertionError("Prediction listener must not start")

    monkeypatch.setattr(runtime.uvicorn, "Server", Server)
    with pytest.raises(RuntimeError, match="health listener failed to start"):
        asyncio.run(runtime.serve())
    assert len(calls) == 2 and all(server.should_exit for server in calls)
