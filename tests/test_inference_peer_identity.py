"""Inference cannot forward data to a name-only or unqualified peer."""

import asyncio
import uuid
from types import SimpleNamespace

import httpx
import pytest
from automl_api.models.enums import RunStatus
from automl_api.services import inference_gateway as gateway
from fastapi import HTTPException
from starlette.requests import Request


def test_ready_sql_row_cannot_infer_an_unqualified_native_peer(monkeypatch):
    run_id, project_id = uuid.uuid4(), uuid.uuid4()
    run = SimpleNamespace(
        id=run_id,
        project_id=project_id,
        status=RunStatus.SUCCEEDED,
        k8s_job_name=f"automl-model-{str(run_id)[:8]}",
        k8s_namespace="sceptre",
        tags={},
    )
    monkeypatch.setattr(gateway, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        gateway, "get_settings", lambda: SimpleNamespace(training_namespace="sceptre")
    )
    with pytest.raises(HTTPException):
        gateway.resolve_deployment_inference_target(
            SimpleNamespace(scalar=lambda _: run),
            SimpleNamespace(id=uuid.uuid4()),
            project_id,
            run_id,
        )


def test_unbound_peer_cannot_receive_gateway_credentials_or_prediction_bytes(monkeypatch):
    received = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{}"

    async def handler(request):
        received.append((dict(request.headers), await request.aread()))
        return httpx.Response(200, stream=Body())

    monkeypatch.setattr(
        gateway,
        "_new_http_client",
        lambda *a, **kw: httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False),
    )

    async def attempted():
        sent = False

        async def receive():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {
                "type": "http.request",
                "body": b'{"records":[{"private_feature":7}]}',
                "more_body": False,
            }

        request = Request(
            dict(
                type="http",
                method="POST",
                path="/inference/v1/predict",
                raw_path=b"/inference/v1/predict",
                query_string=b"",
                headers=[(b"content-type", b"application/json")],
            ),
            receive,
        )
        target = gateway.DeploymentInferenceTarget(
            service_name="automl-model-owned", namespace="sceptre", deployment_id=uuid.uuid4()
        )
        response = await gateway.proxy_deployment_inference(request, target, "v1/predict")
        async for _ in response.body_iterator:
            pass

    with pytest.raises(HTTPException):
        asyncio.run(attempted())
    assert not received
