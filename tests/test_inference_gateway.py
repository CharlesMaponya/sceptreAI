from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import automl_api.services.inference_gateway as gateway
import httpx
import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes.operations import deployment_inference_gateway
from automl_api.models.enums import ProjectRole, RunKind, RunStatus
from fastapi import HTTPException, status
from fastapi.dependencies.utils import get_dependant
from starlette.requests import Request


class _RecordingSession:
    def __init__(self, scalar_value: Any) -> None:
        self.scalar_value = scalar_value
        self.statement = None

    def scalar(self, statement):
        self.statement = statement
        return self.scalar_value


class _TrackingStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


class _StreamingTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        handler: Callable[[httpx.Request], Awaitable[httpx.Response]],
    ) -> None:
        self.handler = handler

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self.handler(request)


def _run(awaitable: Awaitable[Any]) -> Any:
    return asyncio.run(awaitable)


def _request(
    method: str,
    path: str,
    *,
    body: bytes = b"",
    body_chunks: tuple[bytes, ...] | None = None,
    query_string: bytes = b"",
    headers: dict[str, str] | None = None,
    raw_path: bytes | None = None,
    event_log: list[str] | None = None,
) -> Request:
    pending_chunks = list(body_chunks if body_chunks is not None else (body,))

    async def receive() -> dict[str, Any]:
        if not pending_chunks:
            return {"type": "http.disconnect"}
        if event_log is not None:
            event_log.append("request-chunk-read")
        chunk = pending_chunks.pop(0)
        return {
            "type": "http.request",
            "body": chunk,
            "more_body": bool(pending_chunks),
        }

    raw_headers = [
        (name.lower().encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": raw_path or path.encode(),
            "query_string": query_string,
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        },
        receive,
    )


def _gateway_path(path: str, *, project_id: uuid.UUID, run_id: uuid.UUID) -> str:
    return (
        f"/api/v1/projects/{project_id}/operations/deployments/{run_id}"
        f"/inference/{path}"
    )


def _target() -> gateway.DeploymentInferenceTarget:
    return gateway.DeploymentInferenceTarget(
        service_name="automl-model-22222222",
        namespace="sceptre",
    )


def _mock_client(
    handler: Callable[[httpx.Request], Awaitable[httpx.Response]],
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=_StreamingTransport(handler),
        follow_redirects=False,
        trust_env=False,
    )


def test_gateway_route_requires_platform_authentication() -> None:
    dependant = get_dependant(
        path="/api/v1/projects/{project_id}/operations/deployments/{run_id}/inference/{path:path}",
        call=deployment_inference_gateway,
    )
    assert any(dependency.call is get_current_user for dependency in dependant.dependencies)

    with pytest.raises(HTTPException) as exc_info:
        get_current_user(None, SimpleNamespace())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Authentication required."


def test_resolver_checks_project_viewer_role_before_deployment_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    db = _RecordingSession(None)
    requested_roles: list[tuple[uuid.UUID, ProjectRole]] = []

    def deny_project(_db, _user, requested_project_id, role) -> None:
        requested_roles.append((requested_project_id, role))
        raise HTTPException(status_code=403, detail="denied")

    monkeypatch.setattr(gateway, "require_project_role", deny_project)

    with pytest.raises(HTTPException) as error:
        gateway.resolve_deployment_inference_target(
            db,
            SimpleNamespace(id=uuid.uuid4()),
            project_id,
            uuid.uuid4(),
        )

    assert error.value.status_code == status.HTTP_403_FORBIDDEN
    assert requested_roles == [(project_id, ProjectRole.VIEWER)]
    assert db.statement is None


def test_resolver_scopes_lookup_to_project_run_and_deployment_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4()
    db = _RecordingSession(None)
    monkeypatch.setattr(gateway, "require_project_role", lambda *_: None)

    with pytest.raises(HTTPException) as error:
        gateway.resolve_deployment_inference_target(
            db,
            SimpleNamespace(id=uuid.uuid4()),
            project_id,
            run_id,
        )

    assert error.value.status_code == status.HTTP_404_NOT_FOUND
    assert db.statement is not None
    parameters = list(db.statement.compile().params.values())
    assert project_id in parameters
    assert run_id in parameters
    assert RunKind.DEPLOYMENT in parameters


@pytest.mark.parametrize(
    "run_status",
    [
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.PREEMPTED,
    ],
)
def test_resolver_rejects_deployments_that_are_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    run_status: RunStatus,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4()
    run = SimpleNamespace(
        id=run_id,
        status=run_status,
        k8s_job_name=f"automl-model-{str(run_id)[:8]}",
        k8s_namespace="sceptre",
    )
    monkeypatch.setattr(gateway, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        gateway,
        "get_settings",
        lambda: SimpleNamespace(training_namespace="sceptre"),
    )

    with pytest.raises(HTTPException) as error:
        gateway.resolve_deployment_inference_target(
            _RecordingSession(run),
            SimpleNamespace(id=uuid.uuid4()),
            project_id,
            run_id,
        )

    assert error.value.status_code == status.HTTP_409_CONFLICT


def test_resolver_derives_fixed_service_target_for_ready_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    run = SimpleNamespace(
        id=run_id,
        status=RunStatus.SUCCEEDED,
        k8s_job_name="automl-model-22222222",
        k8s_namespace="sceptre",
        tags={"service_name": "attacker.example", "endpoint": "http://169.254.169.254"},
    )
    monkeypatch.setattr(gateway, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        gateway,
        "get_settings",
        lambda: SimpleNamespace(training_namespace="sceptre"),
    )

    target = gateway.resolve_deployment_inference_target(
        _RecordingSession(run),
        SimpleNamespace(id=uuid.uuid4()),
        project_id,
        run_id,
    )

    assert target.service_name == "automl-model-22222222"
    assert target.namespace == "sceptre"
    assert target.url_for("v1/predict") == (
        "http://automl-model-22222222.sceptre.svc:8080/v1/predict"
    )


@pytest.mark.parametrize(
    ("stored_service", "stored_namespace", "configured_namespace"),
    [
        ("attacker-service", "sceptre", "sceptre"),
        ("automl-model-22222222", "other-namespace", "sceptre"),
        ("automl-model-22222222", "bad.namespace", "bad.namespace"),
    ],
)
def test_resolver_rejects_inconsistent_or_invalid_routing_identity(
    monkeypatch: pytest.MonkeyPatch,
    stored_service: str,
    stored_namespace: str,
    configured_namespace: str,
) -> None:
    run_id = uuid.UUID("22222222-2222-4222-8222-222222222222")
    run = SimpleNamespace(
        id=run_id,
        status=RunStatus.SUCCEEDED,
        k8s_job_name=stored_service,
        k8s_namespace=stored_namespace,
    )
    monkeypatch.setattr(gateway, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        gateway,
        "get_settings",
        lambda: SimpleNamespace(training_namespace=configured_namespace),
    )

    with pytest.raises(HTTPException) as error:
        gateway.resolve_deployment_inference_target(
            _RecordingSession(run),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            run_id,
        )

    assert error.value.status_code == status.HTTP_502_BAD_GATEWAY


@pytest.mark.parametrize(
    "path",
    [
        "",
        "v1",
        "v1/predict/extra",
        "v1/predict/../metadata",
        "v1/predict/%2e%2e/metadata",
        "v1/predict%2foffline",
        "//v1/predict",
        "http://169.254.169.254/latest/meta-data",
    ],
)
def test_gateway_rejects_unknown_and_traversal_paths_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
) -> None:
    monkeypatch.setattr(
        gateway,
        "_new_http_client",
        lambda: pytest.fail("rejected paths must not open an upstream connection"),
    )
    request = _request("POST", f"/gateway/{path}")

    with pytest.raises(HTTPException) as error:
        _run(gateway.proxy_deployment_inference(request, _target(), path))

    assert error.value.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.parametrize(
    "encoded_suffix",
    [
        b"v1/predict%2foffline",
        b"v1/predict%2Foffline",
        b"v1/predict%252foffline",
        b"v1/predict/%2e%2e/v1/predict/offline",
    ],
)
def test_gateway_rejects_encoded_path_ambiguity_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
    encoded_suffix: bytes,
) -> None:
    monkeypatch.setattr(
        gateway,
        "_new_http_client",
        lambda: pytest.fail("ambiguous paths must not open an upstream connection"),
    )
    request = _request(
        "POST",
        "/gateway/v1/predict/offline",
        raw_path=b"/gateway/" + encoded_suffix,
    )

    with pytest.raises(HTTPException) as error:
        _run(
            gateway.proxy_deployment_inference(
                request,
                _target(),
                "v1/predict/offline",
            )
        )

    assert error.value.status_code in {
        status.HTTP_400_BAD_REQUEST,
        status.HTTP_404_NOT_FOUND,
    }


@pytest.mark.parametrize(
    ("path", "actual_method", "allowed_method"),
    [
        ("health/ready", "POST", "GET"),
        ("v1/metadata", "POST", "GET"),
        ("v1/predict", "GET", "POST"),
        ("v1/predict/offline", "GET", "POST"),
    ],
)
def test_gateway_enforces_exact_method_allowlist_before_connecting(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    actual_method: str,
    allowed_method: str,
) -> None:
    monkeypatch.setattr(
        gateway,
        "_new_http_client",
        lambda: pytest.fail("rejected methods must not open an upstream connection"),
    )
    request = _request(actual_method, f"/gateway/{path}")

    with pytest.raises(HTTPException) as error:
        _run(gateway.proxy_deployment_inference(request, _target(), path))

    assert error.value.status_code == status.HTTP_405_METHOD_NOT_ALLOWED
    assert error.value.headers == {"Allow": allowed_method}


def test_gateway_does_not_forward_credentials_host_or_hop_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4()
    path = _gateway_path("v1/predict", project_id=project_id, run_id=run_id)
    captured: list[httpx.Request] = []
    response_stream = _TrackingStream(b'{"predictions":[1]}')

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        assert await request.aread() == b'{"records":[{"x":1}]}'
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "set-cookie": "model_session=secret",
                "location": "http://attacker.example",
                "connection": "keep-alive",
                "server": "private-runtime",
                "x-prediction-row-count": "1",
            },
            stream=response_stream,
        )

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request(
        "POST",
        path,
        body=b'{"records":[{"x":1}]}',
        query_string=b"trace=allowed&target=http://169.254.169.254",
        headers={
            "accept": "application/json",
            "authorization": "Bearer platform-secret",
            "cookie": "session=platform-secret",
            "host": "attacker.example",
            "connection": "upgrade",
            "upgrade": "websocket",
            "te": "trailers",
            "proxy-authorization": "Basic c2VjcmV0",
            "x-forwarded-host": "attacker.example",
            "content-type": "application/json",
            "content-length": "999999",
        },
    )

    async def scenario() -> tuple[Any, bytes]:
        response = await gateway.proxy_deployment_inference(request, _target(), "v1/predict")
        body = b"".join([chunk async for chunk in response.body_iterator])
        return response, body

    response, body = _run(scenario())

    assert body == b'{"predictions":[1]}'
    assert len(captured) == 1
    upstream = captured[0]
    assert upstream.url.host == "automl-model-22222222.sceptre.svc"
    assert upstream.url.port == 8080
    assert upstream.url.params["target"] == "http://169.254.169.254"
    assert upstream.headers["host"] == "automl-model-22222222.sceptre.svc:8080"
    for header in (
        "authorization",
        "cookie",
        "upgrade",
        "te",
        "proxy-authorization",
        "x-forwarded-host",
    ):
        assert header not in upstream.headers
    assert upstream.headers.get("connection") != "upgrade"
    assert upstream.headers.get("content-length") != "999999"
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-prediction-row-count"] == "1"
    for header in ("set-cookie", "location", "connection", "server"):
        assert header not in response.headers
    assert response_stream.closed
    assert client.is_closed


@pytest.mark.parametrize(
    ("upstream_error", "expected_status"),
    [
        (httpx.ConnectError("connection refused"), status.HTTP_502_BAD_GATEWAY),
        (httpx.ConnectTimeout("timed out"), status.HTTP_504_GATEWAY_TIMEOUT),
    ],
)
def test_gateway_closes_client_and_sanitizes_upstream_connection_failures(
    monkeypatch: pytest.MonkeyPatch,
    upstream_error: httpx.RequestError,
    expected_status: int,
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        upstream_error.request = request
        raise upstream_error

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request("GET", "/gateway/health/ready")

    with pytest.raises(HTTPException) as error:
        _run(
            gateway.proxy_deployment_inference(
                request,
                _target(),
                "health/ready",
            )
        )

    assert error.value.status_code == expected_status
    assert "connection refused" not in str(error.value.detail)
    assert "timed out" not in str(error.value.detail)
    assert client.is_closed


def test_gateway_streams_status_and_body_then_closes_upstream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream_stream = _TrackingStream(b'{"detail":', b'"invalid input"}')

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            headers={"content-type": "application/json", "x-internal-debug": "secret"},
            stream=upstream_stream,
        )

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request("POST", "/gateway/v1/predict", body=b"{}")

    async def scenario() -> tuple[Any, bytes]:
        response = await gateway.proxy_deployment_inference(request, _target(), "v1/predict")
        body = b"".join([chunk async for chunk in response.body_iterator])
        return response, body

    response, body = _run(scenario())

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert body == b'{"detail":"invalid input"}'
    assert response.headers["content-type"] == "application/json"
    assert "x-internal-debug" not in response.headers
    assert upstream_stream.closed
    assert client.is_closed


def test_gateway_closes_upstream_when_response_consumer_stops_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream_stream = _TrackingStream(b"first", b"second")

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=upstream_stream)

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request("GET", "/gateway/health/live")

    async def scenario() -> bytes:
        response = await gateway.proxy_deployment_inference(request, _target(), "health/live")
        iterator = response.body_iterator
        first = await anext(iterator)
        await iterator.aclose()
        return first

    assert _run(scenario()) == b"first"
    assert upstream_stream.closed
    assert client.is_closed


def test_gateway_streams_offline_upload_without_trusting_content_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_log: list[str] = []
    received_chunks: list[bytes] = []
    upstream_stream = _TrackingStream(b"prediction\n", b"1\n")

    async def handler(request: httpx.Request) -> httpx.Response:
        event_log.append("upstream-handler-started")
        assert "content-length" not in request.headers
        assert request.headers["transfer-encoding"] == "chunked"
        async for chunk in request.stream:
            received_chunks.append(chunk)
        return httpx.Response(
            200,
            headers={
                "content-type": "text/csv",
                "content-disposition": 'attachment; filename="predictions.csv"',
            },
            stream=upstream_stream,
        )

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request(
        "POST",
        "/gateway/v1/predict/offline",
        body_chunks=(b"first-upload-chunk", b"second-upload-chunk"),
        headers={
            "content-type": "multipart/form-data; boundary=test",
            "content-length": "999999999",
        },
        event_log=event_log,
    )

    async def scenario() -> tuple[Any, bytes]:
        response = await gateway.proxy_deployment_inference(
            request,
            _target(),
            "v1/predict/offline",
        )
        body = b"".join([chunk async for chunk in response.body_iterator])
        return response, body

    response, body = _run(scenario())

    assert event_log[0] == "upstream-handler-started"
    assert event_log.count("request-chunk-read") == 2
    assert b"".join(received_chunks) == b"first-upload-chunksecond-upload-chunk"
    assert body == b"prediction\n1\n"
    assert response.headers["content-type"] == "text/csv"
    assert response.headers["content-disposition"] == (
        'attachment; filename="predictions.csv"'
    )
    assert upstream_stream.closed
    assert client.is_closed


def test_gateway_rewrites_openapi_server_and_bearer_security(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    run_id = uuid.uuid4()
    request_path = _gateway_path("openapi.json", project_id=project_id, run_id=run_id)
    upstream_stream = _TrackingStream(
        json.dumps(
            {
                "openapi": "3.1.0",
                "info": {"title": "Model", "version": "1"},
                "servers": [{"url": "http://internal-service:8080"}],
                "paths": {
                    "/v1/predict": {
                        "post": {
                            "security": [],
                            "servers": [{"url": "https://attacker.example"}],
                            "responses": {"200": {}},
                        }
                    }
                },
                "components": {"schemas": {"Prediction": {"type": "object"}}},
            }
        ).encode()
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "set-cookie": "runtime=secret",
            },
            stream=upstream_stream,
        )

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request(
        "GET",
        request_path,
        headers={"authorization": "Bearer platform-secret"},
    )

    response = _run(
        gateway.proxy_deployment_inference(
            request,
            _target(),
            "openapi.json",
        )
    )
    document = json.loads(response.body)
    expected_base = request_path.removesuffix("/openapi.json")

    assert response.status_code == status.HTTP_200_OK
    assert document["servers"] == [{"url": expected_base}]
    assert document["security"] == [{"PlatformBearer": []}]
    assert document["paths"]["/v1/predict"]["post"]["security"] == [
        {"PlatformBearer": []}
    ]
    assert "servers" not in document["paths"]["/v1/predict"]["post"]
    assert document["components"]["securitySchemes"]["PlatformBearer"] == {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Sceptre platform access token",
    }
    assert document["components"]["schemas"]["Prediction"] == {"type": "object"}
    assert "set-cookie" not in response.headers
    assert upstream_stream.closed
    assert client.is_closed


def test_gateway_docs_embed_sanitized_openapi_without_follow_up_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream_requests: list[httpx.Request] = []
    upstream_stream = _TrackingStream(
        json.dumps(
            {
                "openapi": "3.1.0",
                "info": {
                    "title": "Model",
                    "version": "1",
                    "description": "</script><script>window.pwned=true</script>",
                },
                "paths": {
                    "/v1/predict": {
                        "post": {
                            "security": [],
                            "servers": [{"url": "https://attacker.example"}],
                            "responses": {"200": {}},
                        }
                    }
                },
            }
        ).encode()
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        upstream_requests.append(request)
        return httpx.Response(200, stream=upstream_stream)

    client = _mock_client(handler)
    monkeypatch.setattr(gateway, "_new_http_client", lambda: client)
    request = _request(
        "GET",
        "/api/v1/projects/project/operations/deployments/run/inference/docs",
        headers={"authorization": "Bearer platform-secret"},
    )

    response = _run(gateway.proxy_deployment_inference(request, _target(), "docs"))
    html = response.body.decode()

    assert response.status_code == status.HTTP_200_OK
    assert response.media_type == "text/html"
    assert len(upstream_requests) == 1
    assert upstream_requests[0].url.path == "/openapi.json"
    assert "authorization" not in upstream_requests[0].headers
    assert "spec: {" in html
    assert "__SCEPTRE_INLINE_OPENAPI__" not in html
    assert "</script><script>window.pwned=true</script>" not in html
    assert r"\u003c/script\u003e\u003cscript\u003ewindow.pwned=true" in html
    assert "https://attacker.example" not in html
    assert upstream_stream.closed
    assert client.is_closed
