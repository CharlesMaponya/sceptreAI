"""Pod-bound attempt admission; user session tokens cannot authorize this route."""

import uuid
from typing import Annotated

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Response
from kubernetes.client import ApiException
from sqlalchemy.orm import Session

from automl_api.db.session import get_db
from automl_api.services.attempt_identity import admit_attempt
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence

router = APIRouter(prefix="/internal/attempts", include_in_schema=False)


@router.post("/{attempt_id}/admission")
def admission(
    attempt_id: uuid.UUID,
    response: Response,
    db: Annotated[Session, Depends(get_db)],
    authorization: Annotated[str, Header()] = "",
):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise StaleFence("A workload bearer token is required.")
        k8s = KubernetesTrainingClient()
        if not k8s._configured:
            raise HTTPException(503, "Workload identity verification unavailable")
        with db.begin():
            payload = admit_attempt(db, attempt_id, token, k8s)
    except (StaleFence, ValueError, KeyError, TypeError, jwt.InvalidTokenError) as exc:
        raise HTTPException(401, "Invalid or stale workload identity") from exc
    except ApiException as exc:
        if exc.status in {401, 404}:
            raise HTTPException(401, "Invalid or stale workload identity") from exc
        raise HTTPException(503, "Workload identity verification unavailable") from exc
    response.headers["Cache-Control"] = "no-store"
    return payload
