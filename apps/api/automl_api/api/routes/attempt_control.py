"""Pod-bound attempt admission; user session tokens cannot authorize this route."""

import json
import uuid
from typing import Annotated, Literal

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from kubernetes.client import ApiException
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from starlette.concurrency import run_in_threadpool

from automl_api.db.session import get_session_factory
from automl_api.services.attempt_identity import admit_attempt
from automl_api.services.attempt_object_grants import (
    InputGrantRequest,
    ObjectGrantUnavailable,
    issue_input_grant,
)
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.provider_copy_verification import CopyVerification
from automl_api.services.provider_import_control import control_provider_import
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.object_store import get_object_store

router = APIRouter(prefix="/internal/attempts", include_in_schema=False)


class VerificationFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code: Literal["invalid_copy", "verifier_unavailable"]


@router.post("/{attempt_id}/provider-import/{action}")
async def provider_verification_control(
    attempt_id: uuid.UUID,
    action: Literal["start", "heartbeat", "result", "receipt", "failure"],
    request: Request,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    body = bytearray()
    async for block in request.stream():
        if len(body) + len(block) > 1024**2:
            raise HTTPException(413, "Verification metadata exceeds its byte budget")
        body.extend(block)
    try:
        payload = json.loads(body) if body else None
        proof = (
            CopyVerification.model_validate(payload) if action in {"result", "receipt"} else None
        )
        failure = VerificationFailure.model_validate(payload) if action == "failure" else None
        if action in {"start", "heartbeat"} and payload is not None:
            raise ValueError("Verification control does not accept execution arguments")
    except (ValueError, ValidationError) as exc:
        raise HTTPException(422, "Invalid verification metadata") from exc
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = await run_in_threadpool(
            control_provider_import,
            factory,
            attempt_id,
            token,
            k8s,
            action,
            proof=proof,
            failure_code=failure.code if failure else None,
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale verification authority") from exc
    except (OSError, SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(503, "Provider verification control unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale verification authority") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload


@router.post("/{attempt_id}/admission")
def admission(
    attempt_id: uuid.UUID,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = admit_attempt(factory, attempt_id, token, k8s)
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale workload identity") from exc
    except (OSError, SQLAlchemyError) as exc:
        raise HTTPException(503, "Workload authorization storage unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale workload identity") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload


@router.post("/{attempt_id}/objects/read")
def read_object_grant(
    attempt_id: uuid.UUID,
    request: InputGrantRequest,
    response: Response,
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
    authorization: Annotated[str, Header()] = "",
):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        payload = issue_input_grant(
            factory, attempt_id, token, k8s, request, store_factory=get_object_store
        )
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale workload object authority") from exc
    except ObjectGrantUnavailable as exc:
        raise HTTPException(503, "Object capability unavailable") from exc
    except (OSError, SQLAlchemyError) as exc:
        raise HTTPException(503, "Workload authorization storage unavailable") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale workload object authority") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload
