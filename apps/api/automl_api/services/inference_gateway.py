from __future__ import annotations

import json
import re
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass

import httpx
from fastapi import HTTPException, Request, Response, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.models.enums import ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.services.projects import require_project_role

_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_ACTIVE_DEPLOYMENT_STATUSES = {RunStatus.SUCCEEDED}
_ALLOWED_INFERENCE_PATHS = {
    "health/live": "GET",
    "health/ready": "GET",
    "openapi.json": "GET",
    "docs": "GET",
    "v1/metadata": "GET",
    "v1/predict": "POST",
    "v1/predict/online": "POST",
    "v1/predict/offline": "POST",
}
_FORWARDED_REQUEST_HEADERS = {
    "accept",
    "accept-encoding",
    "accept-language",
    "content-type",
}
_FORWARDED_RESPONSE_HEADERS = {
    "cache-control",
    "content-disposition",
    "content-encoding",
    "content-length",
    "content-type",
    "etag",
    "last-modified",
    "x-prediction-row-count",
}
_MAX_OPENAPI_BYTES = 5 * 1024 * 1024


@dataclass(frozen=True)
class DeploymentInferenceTarget:
    service_name: str
    namespace: str

    def url_for(self, path: str) -> str:
        return f"http://{self.service_name}.{self.namespace}.svc:8080/{path}"


def resolve_deployment_inference_target(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
) -> DeploymentInferenceTarget:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
        )
    )
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment not found.",
        )
    if run.status not in _ACTIVE_DEPLOYMENT_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The model deployment is not active.",
        )

    service_name = f"automl-model-{str(run.id)[:8]}"
    namespace = get_settings().training_namespace
    if run.k8s_job_name and run.k8s_job_name != service_name:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The deployment service identity is inconsistent. Redeploy the model.",
        )
    if run.k8s_namespace and run.k8s_namespace != namespace:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The deployment namespace no longer matches the configured namespace.",
        )
    if not _valid_dns_label(service_name) or not _valid_dns_label(namespace):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                "The deployment has invalid Kubernetes service routing metadata. "
                "Redeploy the registered model."
            ),
        )
    return DeploymentInferenceTarget(
        service_name=service_name,
        namespace=namespace,
    )


async def proxy_deployment_inference(
    request: Request,
    target: DeploymentInferenceTarget,
    path: str,
) -> Response:
    if b"%" in request.scope.get("raw_path", b""):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Encoded inference paths are not accepted by the platform gateway.",
        )
    expected_method = _ALLOWED_INFERENCE_PATHS.get(path)
    if expected_method is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="This inference path is not available through the platform gateway.",
        )
    if request.method != expected_method:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail=f"Use {expected_method} for this inference path.",
            headers={"Allow": expected_method},
        )

    gateway_base_path = _gateway_base_path(request, path)
    upstream_path = "openapi.json" if path == "docs" else path
    upstream_url = httpx.URL(target.url_for(upstream_path)).copy_with(
        query=request.scope.get("query_string", b""),
    )
    client = _new_http_client()
    upstream_request = client.build_request(
        request.method,
        upstream_url,
        headers=_selected_headers(request.headers, _FORWARDED_REQUEST_HEADERS),
        content=request.stream(),
    )
    try:
        upstream_response = await client.send(upstream_request, stream=True)
    except httpx.TimeoutException as exc:
        await client.aclose()
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=(
                "The deployed model did not respond before the platform gateway "
                "connection timeout."
            ),
        ) from exc
    except httpx.RequestError as exc:
        await client.aclose()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=(
                "The deployed model could not be reached through its Kubernetes "
                f"service '{target.service_name}'. Confirm the deployment is ready."
            ),
        ) from exc
    except BaseException:
        await client.aclose()
        raise

    if path in {"docs", "openapi.json"}:
        return await _rewritten_openapi_response(
            upstream_response,
            client,
            gateway_base_path,
            render_docs=path == "docs",
        )

    response_headers = _selected_headers(
        upstream_response.headers,
        _FORWARDED_RESPONSE_HEADERS,
    )

    async def response_body() -> AsyncIterator[bytes]:
        try:
            async for chunk in upstream_response.aiter_raw():
                yield chunk
        finally:
            await upstream_response.aclose()
            await client.aclose()

    return StreamingResponse(
        response_body(),
        status_code=upstream_response.status_code,
        headers=response_headers,
    )


def _new_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(
            connect=10.0,
            read=3600.0,
            write=3600.0,
            pool=10.0,
        ),
        trust_env=False,
    )


async def _rewritten_openapi_response(
    upstream_response: httpx.Response,
    client: httpx.AsyncClient,
    gateway_base_path: str,
    *,
    render_docs: bool,
) -> Response:
    body = bytearray()
    try:
        async for chunk in upstream_response.aiter_bytes():
            body.extend(chunk)
            if len(body) > _MAX_OPENAPI_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="The deployed model returned an unexpectedly large OpenAPI document.",
                )
    finally:
        await upstream_response.aclose()
        await client.aclose()

    if not 200 <= upstream_response.status_code < 300:
        return Response(
            content=bytes(body),
            status_code=upstream_response.status_code,
            headers=_selected_headers(
                upstream_response.headers,
                _FORWARDED_RESPONSE_HEADERS - {"content-length", "content-encoding"},
            ),
        )
    try:
        document = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The deployed model returned an invalid OpenAPI document.",
        ) from exc
    if not isinstance(document, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="The deployed model returned an invalid OpenAPI document.",
        )

    components = document.setdefault("components", {})
    if not isinstance(components, dict):
        components = {}
        document["components"] = components
    security_schemes = components.setdefault("securitySchemes", {})
    if not isinstance(security_schemes, dict):
        security_schemes = {}
        components["securitySchemes"] = security_schemes
    security_schemes["PlatformBearer"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Sceptre platform access token",
    }
    document["security"] = [{"PlatformBearer": []}]
    document["servers"] = [{"url": gateway_base_path}]
    _secure_openapi_operations(document)
    if render_docs:
        return _inline_swagger_ui(document)
    return JSONResponse(document)


def _selected_headers(headers: Mapping[str, str], allowed: set[str]) -> dict[str, str]:
    return {
        name: value
        for name, value in headers.items()
        if name.lower() in allowed
    }


def _gateway_base_path(request: Request, path: str) -> str:
    suffix = f"/{path}"
    if not request.url.path.endswith(suffix):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid inference gateway path.",
        )
    return request.url.path[: -len(suffix)]


def _secure_openapi_operations(document: dict[str, object]) -> None:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        document["paths"] = {}
        return
    for path_name in list(paths):
        expected_method = _ALLOWED_INFERENCE_PATHS.get(str(path_name).lstrip("/"))
        path_item = paths[path_name]
        if expected_method is None or not isinstance(path_item, dict):
            paths.pop(path_name, None)
            continue
        method_name = expected_method.lower()
        operation = path_item.get(method_name)
        if not isinstance(operation, dict):
            paths.pop(path_name, None)
            continue
        operation.pop("servers", None)
        operation["security"] = [{"PlatformBearer": []}]
        paths[path_name] = {
            key: value
            for key, value in path_item.items()
            if key == method_name or key in {"description", "parameters", "summary"}
        }


def _inline_swagger_ui(document: dict[str, object]) -> Response:
    placeholder = "__SCEPTRE_INLINE_OPENAPI__"
    template = get_swagger_ui_html(
        openapi_url=placeholder,
        title="Sceptre deployed model API",
        swagger_ui_parameters={"persistAuthorization": True},
    )
    serialized = json.dumps(document, separators=(",", ":"))
    serialized = (
        serialized.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    html = template.body.decode("utf-8").replace(
        f"url: '{placeholder}',",
        f"spec: {serialized},",
    )
    return Response(content=html, media_type="text/html")


def _valid_dns_label(value: str) -> bool:
    return bool(value) and len(value) <= 63 and _DNS_LABEL.fullmatch(value) is not None
