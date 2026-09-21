from __future__ import annotations

import hmac
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from automl_api.db.session import get_db
from automl_api.models.enums import RunKind, RunStatus
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.security.deployments import deployment_token
from automl_api.storage.object_store import get_object_store

router = APIRouter(prefix="/internal/deployments", include_in_schema=False)


@router.get("/{deployment_id}/model")
def download_deployment_model(
    deployment_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    token: Annotated[str, Header(alias="X-Sceptre-Deployment-Token")] = "",
) -> StreamingResponse:
    if not hmac.compare_digest(token, deployment_token(deployment_id)):
        raise HTTPException(status_code=401, detail="Authentication required.")
    run = db.get(ModelRun, deployment_id)
    if (
        run is None
        or run.run_kind != RunKind.DEPLOYMENT
        or run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.PREEMPTED}
    ):
        raise HTTPException(status_code=404, detail="Deployment not found.")
    artifact_id = (run.tags or {}).get("model_artifact_id")
    artifact = db.get(RunArtifact, uuid.UUID(artifact_id)) if artifact_id else None
    if artifact is None or artifact.project_id != run.project_id:
        raise HTTPException(status_code=404, detail="Model artifact not found.")

    def chunks():
        with get_object_store().open_stream(artifact.object_uri) as stream:
            while chunk := stream.read(1024 * 1024):
                yield chunk

    return StreamingResponse(chunks(), media_type="application/octet-stream")
