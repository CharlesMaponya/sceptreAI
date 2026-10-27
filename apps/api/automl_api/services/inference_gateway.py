from __future__ import annotations

import ipaddress
import json
import re
import secrets
import ssl
import time
import uuid
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from functools import partial

import anyio
import httpx
from fastapi import HTTPException, Request, Response, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, StreamingResponse
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.models.enums import ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.services import inference_identity, upload_verification
from automl_api.services.inference_tls import DATA_PORT, PROTOCOL, PeerIdentity
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.projects import require_project_role
from automl_api.services.uploads import get_upload_session, require_verified_upload_digest
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.object_store import get_object_store

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
_MAX_OBJECT_REQUEST_BYTES = 16 * 1024


@dataclass(frozen=True)
class DeploymentInferenceTarget:
    service_name: str
    namespace: str
    deployment_id: uuid.UUID | None = None
    peer: PeerIdentity | None = None
    address: str | None = None
    tls_context: ssl.SSLContext | None = None
    selection: InferenceSelection | None = None

    def require_admission(self):
        if (
            self.peer is None
            or self.selection is None
            or not self.address
            or not isinstance(self.tls_context, ssl.SSLContext)
            or self.tls_context.verify_mode != ssl.CERT_REQUIRED
            or not self.tls_context.check_hostname
            or self.tls_context.hostname_checks_common_name
            or str(self.deployment_id) != self.peer.run_id
            or self.selection.deployment_id != self.deployment_id
            or str(self.selection.project_id) != self.peer.project_id
        ):
            raise HTTPException(409, "The deployment requires original pod TLS admission.")

    def url_for(self, path: str) -> str:
        self.require_admission()
        address = ipaddress.ip_address(self.address)
        host = f"[{address}]" if address.version == 6 else str(address)
        return f"https://{host}:{DATA_PORT}/{path}"


@dataclass(frozen=True)
class InferenceSelection:
    project_id: uuid.UUID
    actor_id: uuid.UUID
    token_version: int
    deployment_id: uuid.UUID
    service_name: str
    namespace: str
    scope: inference_identity.IdentityScope


@dataclass(frozen=True)
class InferenceInspection:
    selection: InferenceSelection
    peer: PeerIdentity
    address: str
    tls_context: ssl.SSLContext


def resolve_deployment_inference_target(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    *,
    inspection: InferenceSelection | None = None,
) -> InferenceSelection:
    from automl_api.services.operations import _deployment_reader

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
    if (run.tags or {}).get("inference_identity_protocol") != PROTOCOL:
        raise HTTPException(409, "Redeploy this model to qualify its pod TLS identity.")
    current = _deployment_reader(db, user, project_id, inspection)
    try:
        scope = inference_identity.select_scope(db, run_id, None)
        if (
            scope.request["project_id"] != str(project_id)
            or run.status not in _ACTIVE_DEPLOYMENT_STATUSES
        ):
            raise StaleFence("Inference deployment changed during selection")
    except (StaleFence, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(409, "Original inference custody could not be qualified.") from exc
    return InferenceSelection(
        project_id=project_id,
        actor_id=current.id,
        token_version=current.token_version,
        service_name=service_name,
        namespace=namespace,
        deployment_id=run.id,
        scope=scope,
    )


def _ready_pod(pod):
    status = pod.get("status") or {}
    return (
        not pod["metadata"].get("deletionTimestamp")
        and status.get("phase") == "Running"
        and any(
            item.get("type") == "Ready" and item.get("status") == "True"
            for item in status.get("conditions") or []
        )
    )


def inspect_deployment_inference_target(selection: InferenceSelection) -> InferenceInspection:
    try:
        if canonical_request_hash(selection.scope.request) != selection.scope.digest:
            raise StaleFence("Inference selection changed")
        signer = inference_identity.authority()
        if signer.digest != selection.scope.request["ca_digest"]:
            raise StaleFence("Inference authority changed")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise RuntimeError("Native identity verification unavailable")
        deadline = time.monotonic() + 30
        result = k8s.core.list_namespaced_pod(
            namespace=selection.namespace,
            label_selector=f"automl.platform/deployment-id={selection.deployment_id}",
            limit=100,
            _request_timeout=5,
        )
        if getattr(result.metadata, "_continue", None) or len(result.items) > 100:
            raise StaleFence("Incomplete inference pod inventory")
        pods = [inference_identity._object(item) for item in result.items]
        uids = [pod["metadata"]["uid"] for pod in pods]
        if len(set(uids)) != len(uids):
            raise StaleFence("Repeated inference pod identity")
        for candidate in pods:
            if not _ready_pod(candidate):
                continue
            peer, pod = inference_identity.verify_pod(
                k8s,
                selection.scope,
                candidate["metadata"]["name"],
                candidate["metadata"]["uid"],
                deadline=deadline,
            )
            if not _ready_pod(pod):
                raise StaleFence("Inference pod readiness changed")
            address = ipaddress.ip_address(pod["status"]["podIP"])
            if getattr(address, "scope_id", None) is not None:
                raise StaleFence("Scoped inference address is not admissible")
            address = getattr(address, "ipv4_mapped", None) or address
            if (
                address.is_loopback
                or address.is_unspecified
                or address.is_link_local
                or address.is_multicast
            ):
                raise StaleFence("Invalid inference pod address")
            if inference_identity.authority().digest != signer.digest:
                raise StaleFence("Inference authority changed during inspection")
            return InferenceInspection(selection, peer, str(address), signer.client_context())
        raise StaleFence("No original ready inference pod")
    except (
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        RuntimeError,
        OSError,
        ApiException,
    ) as exc:
        raise HTTPException(503, "The original inference pod could not be admitted.") from exc


def finalize_deployment_inference_target(db, user, inspected: InferenceInspection):
    selected = inspected.selection
    if canonical_request_hash(selected.scope.request) != selected.scope.digest:
        raise HTTPException(409, "The inference selection changed during admission.")
    current = resolve_deployment_inference_target(
        db,
        user,
        selected.project_id,
        selected.deployment_id,
        inspection=selected,
    )
    if current.scope.digest != selected.scope.digest:
        raise HTTPException(409, "The deployment changed during inference admission.")
    try:
        inference_identity.bound_peer(db, current.scope, inspected.peer)
    except (StaleFence, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(409, "The inference pod has no original key binding.") from exc
    return DeploymentInferenceTarget(
        current.service_name,
        current.namespace,
        current.deployment_id,
        inspected.peer,
        inspected.address,
        inspected.tls_context,
        current,
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
    client = _new_http_client(target)
    upstream_request = client.build_request(
        request.method,
        upstream_url,
        headers=_selected_headers(request.headers, _FORWARDED_REQUEST_HEADERS),
        content=request.stream(),
    )
    upstream_request.extensions["sni_hostname"] = target.peer.hostname
    upstream_response = await _send_upstream(client, upstream_request, target)

    if path in {"docs", "openapi.json"}:
        return await _rewritten_openapi_response(
            upstream_response,
            client,
            gateway_base_path,
            render_docs=path == "docs",
        )

    return _streaming_upstream_response(upstream_response, client)


async def proxy_object_backed_offline_inference(
    request: Request,
    target: DeploymentInferenceTarget,
    db: Session,
    user: User,
    project_id: uuid.UUID,
) -> Response:
    """Stream a verified direct upload to inference without browser retransmission."""
    target.require_admission()
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_OBJECT_REQUEST_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail="The object-backed inference request is too large.",
            )
        body.extend(chunk)
    try:
        document = json.loads(body)
        session_id = uuid.UUID(str(document["upload_session_id"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="A valid offline-scoring upload_session_id is required.",
        ) from exc

    db.expire_all()
    target = finalize_deployment_inference_target(
        db,
        user,
        InferenceInspection(target.selection, target.peer, target.address, target.tls_context),
    )
    upload = get_upload_session(db, user, project_id, session_id)
    if upload.upload_kind != "offline_scoring":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The upload is not an offline-scoring input.",
        )
    if upload.status != "ready" or not upload.completed_object_uri:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The offline-scoring upload has not passed object verification.",
        )
    try:
        require_verified_upload_digest(upload)
    except UploadContractError as exc:
        raise HTTPException(
            status_code=409, detail="The offline-scoring upload lacks verified digest evidence."
        ) from exc
    try:
        selected = upload_verification.verified_object(db, upload, get_settings())
    except (
        UploadContractError,
        StaleFence,
        KeyError,
        TypeError,
        ValueError,
        AttributeError,
    ) as exc:
        raise HTTPException(409, "The upload lacks original versioned verification proof.") from exc
    db.commit()
    store = get_object_store()
    if selected.driver != store.driver_name:
        raise HTTPException(
            409, "The upload provider does not match the active object-store driver."
        )

    boundary = f"sceptre-{secrets.token_hex(16)}"
    upstream_url = httpx.URL(target.url_for("v1/predict/offline")).copy_with(
        query=request.scope.get("query_string", b""),
    )
    headers = _selected_headers(request.headers, {"accept", "accept-encoding", "accept-language"})
    headers["content-type"] = f"multipart/form-data; boundary={boundary}"
    client = _new_http_client(target)
    body = _multipart_object_body(store, selected, boundary=boundary)
    try:
        upstream_request = client.build_request("POST", upstream_url, headers=headers, content=body)
        upstream_request.extensions["sni_hostname"] = target.peer.hostname
        upstream_response = await _send_upstream(client, upstream_request, target)
    except BaseException:
        with anyio.CancelScope(shield=True):
            await client.aclose()
        raise
    finally:
        with anyio.CancelScope(shield=True):
            await body.aclose()
    return _streaming_upstream_response(upstream_response, client)


async def _send_upstream(
    client: httpx.AsyncClient,
    upstream_request: httpx.Request,
    target: DeploymentInferenceTarget,
) -> httpx.Response:
    try:
        return await client.send(upstream_request, stream=True)
    except httpx.TimeoutException as exc:
        await client.aclose()
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=(
                "The deployed model did not respond before the platform gateway connection timeout."
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


class _InferenceStreamingResponse(StreamingResponse):
    def __init__(self, content, *, upstream, client, **kwargs):
        self.upstream, self.client = upstream, client
        super().__init__(content, **kwargs)

    async def close_upstream(self):
        with anyio.CancelScope(shield=True):
            try:
                await self.upstream.aclose()
            finally:
                await self.client.aclose()

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.close_upstream()


def _streaming_upstream_response(
    upstream_response: httpx.Response,
    client: httpx.AsyncClient,
) -> StreamingResponse:
    response_headers = _selected_headers(
        upstream_response.headers,
        _FORWARDED_RESPONSE_HEADERS,
    )

    async def response_body() -> AsyncIterator[bytes]:
        try:
            async for chunk in upstream_response.aiter_raw():
                yield chunk
        finally:
            await response.close_upstream()

    response = _InferenceStreamingResponse(
        response_body(),
        upstream=upstream_response,
        client=client,
        status_code=upstream_response.status_code,
        headers=response_headers,
    )
    return response


async def _multipart_object_body(
    store, selected: upload_verification.VerifiedObject, *, boundary: str
) -> AsyncIterator[bytes]:
    source = None
    try:
        # HTTPX consumes this iterator only after authenticating the TLS peer.
        source = await anyio.to_thread.run_sync(
            partial(store.open_stream, selected.uri, version=selected.version)
        )
        filename = re.sub(r"[^A-Za-z0-9._-]+", "_", selected.filename).strip("._") or "input.csv"
        yield (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            f'filename="{filename}"\r\nContent-Type: {selected.content_type}\r\n\r\n'
        ).encode("ascii")
        while chunk := await anyio.to_thread.run_sync(source.read, 1024 * 1024):
            yield chunk
    finally:
        if source is not None:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(source.close)
    yield f"\r\n--{boundary}--\r\n".encode("ascii")


def _new_http_client(target: DeploymentInferenceTarget) -> httpx.AsyncClient:
    target.require_admission()
    return httpx.AsyncClient(
        verify=target.tls_context,
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
    return {name: value for name, value in headers.items() if name.lower() in allowed}


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
