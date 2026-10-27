from __future__ import annotations

import hmac
import tempfile
import uuid
from typing import Annotated

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from automl_api.db.session import get_db, get_session_factory
from automl_api.models.enums import RunKind, RunStatus
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.security.deployments import deployment_token
from automl_api.services import inference_identity
from automl_api.services.inference_tls import MAX_PEM_BYTES, PROTOCOL
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.model_custody import deployment_model_custody, verify_model_custody
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.object_store import get_object_store

router = APIRouter(prefix="/internal/deployments", include_in_schema=False)


@router.post("/{deployment_id}/identity")
async def admit_deployment_identity(
    deployment_id: uuid.UUID,
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    headers = {"Cache-Control": "no-store"}
    scheme, separator, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not separator or not 0 < len(token) <= 65536:
        raise HTTPException(401, "A pod-bound bearer token is required", headers=headers)
    csr = bytearray()
    async for block in request.stream():
        if len(csr) + len(block) > MAX_PEM_BYTES:
            raise HTTPException(413, "Inference CSR exceeds its byte budget", headers=headers)
        csr.extend(block)
    try:
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Inference identity verification unavailable", headers=headers)
        result = await run_in_threadpool(
            inference_identity.issue_identity, factory, deployment_id, token, bytes(csr), k8s
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale inference authority", headers=headers) from exc
    except ApiException as exc:
        raise HTTPException(
            401 if exc.status in {401, 404} else 503,
            "Inference identity verification unavailable",
            headers=headers,
        ) from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Inference identity unavailable", headers=headers) from exc
    response.headers.update(headers)
    return result


def _select_model(db, deployment_id, *, pod_identity=False):
    project_id = db.scalar(select(ModelRun.project_id).where(ModelRun.id == deployment_id))
    if project_id is None:
        raise HTTPException(status_code=404, detail="Deployment not found.")
    db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    run = db.get(ModelRun, deployment_id, with_for_update={"read": True}, populate_existing=True)
    if (
        run is None
        or run.project_id != project_id
        or run.run_kind != RunKind.DEPLOYMENT
        or run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.PREEMPTED}
    ):
        raise HTTPException(status_code=404, detail="Deployment not found.")
    if run.tags.get("inference_identity_protocol") == PROTOCOL and not pod_identity:
        raise HTTPException(401, "A pod-bound identity is required.")
    try:
        artifact_id = uuid.UUID((run.tags or {})["model_artifact_id"])
    except (KeyError, ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=404, detail="Model artifact not found.") from None
    artifact = db.get(RunArtifact, artifact_id, populate_existing=True)
    if artifact is None or artifact.project_id != run.project_id:
        raise HTTPException(status_code=404, detail="Model artifact not found.")

    custody = deployment_model_custody(db, run)
    return custody, canonical_request_hash(
        {"status": run.status, "tags": run.tags, "custody": custody}
    )


@router.get("/{deployment_id}/model")
def download_deployment_model(
    deployment_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    token: Annotated[str, Header(alias="X-Sceptre-Deployment-Token")] = "",
    authorization: Annotated[str, Header()] = "",
) -> StreamingResponse:
    if not authorization and not hmac.compare_digest(
        token.encode(), deployment_token(deployment_id).encode()
    ):
        raise HTTPException(status_code=401, detail="Authentication required.")
    model = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b")
    try:
        scope = peer = None
        if authorization:
            scheme, separator, pod_token = authorization.partition(" ")
            if scheme.lower() != "bearer" or not separator or not 0 < len(pod_token) <= 65536:
                raise HTTPException(401, "A pod-bound bearer token is required.")
            signer = inference_identity.authority()
            k8s = KubernetesTrainingClient()
            if not k8s._configured:
                raise HTTPException(503, "Inference identity verification unavailable.")
            scope = inference_identity.select_scope(db, deployment_id, signer.digest)
            db.commit()
            peer, _, _ = inference_identity.review_identity(k8s, scope, pod_token)
            if inference_identity.authority().digest != signer.digest:
                raise StaleFence("Inference authority changed during model admission")
            db.expire_all()
            current = inference_identity.select_scope(db, deployment_id, signer.digest)
            if current.digest != scope.digest:
                raise StaleFence("Inference source changed during model admission")
            inference_identity.bound_peer(db, current, peer)
        custody, original = _select_model(db, deployment_id, pod_identity=bool(scope))
        if scope is not None and (
            custody["completion"]["sha256"] != scope.request["model_digest"]
            or custody["completion"]["byte_size"] != scope.request["model_byte_size"]
        ):
            raise StaleFence("Inference model differs from the original key binding")
        db.commit()
        verify_model_custody(get_object_store(), custody, sink=model)
        if scope is not None and inference_identity.authority().digest != signer.digest:
            raise StaleFence("Inference authority changed during model download")
        db.expire_all()
        if scope is not None:
            current_scope = inference_identity.select_scope(db, deployment_id, signer.digest)
            if current_scope.digest != scope.digest:
                raise StaleFence("Inference source changed during model download")
            inference_identity.bound_peer(db, current_scope, peer)
        _, current = _select_model(db, deployment_id, pod_identity=bool(scope))
        if current != original:
            raise HTTPException(409, "Deployment model changed during download.")
        db.commit()
        model.seek(0)
    except Exception as exc:
        model.close()
        db.rollback()
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(
            409, "Original deployment model custody could not be verified."
        ) from None

    def chunks():
        try:
            while chunk := model.read(65536):
                yield chunk
        finally:
            model.close()

    return StreamingResponse(
        chunks(),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(custody["completion"]["byte_size"]),
            "Cache-Control": "no-store",
        },
        background=BackgroundTask(model.close),
    )
