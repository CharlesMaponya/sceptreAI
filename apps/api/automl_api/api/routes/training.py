from __future__ import annotations

import asyncio
import uuid
from typing import Annotated, Literal

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.core.config import get_settings
from automl_api.db.session import get_db, get_session_factory
from automl_api.models.enums import RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.schemas.training import (
    EstimatorRead,
    ModelRunRead,
    TrainingAddModelsRequest,
    TrainingEstimateRead,
    TrainingEstimateRequest,
    TrainingLaunchRead,
    TrainingLaunchRequest,
    TrainingLeaderboardRead,
    TrainingLogsRead,
    TrainingResourceUsageRead,
)
from automl_api.security.tokens import TokenError, decode_token
from automl_api.services.model_audit import model_audit_document
from automl_api.services.training import (
    add_models_to_training_run,
    cancel_training_run,
    estimate_training_run,
    get_training_run,
    launch_training_run,
    list_training_estimators,
    list_training_runs,
    restart_training_run,
    training_leaderboard,
    training_logs,
    training_resources,
)

router = APIRouter(prefix="/projects/{project_id}/training", tags=["training"])


@router.get("/estimators", response_model=list[EstimatorRead])
def estimators(
    project_id: uuid.UUID,
    task_type: TaskType,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[EstimatorRead]:
    return list_training_estimators(
        db,
        current_user,
        project_id,
        task_type,
    )


@router.post("/estimate", response_model=TrainingEstimateRead)
def estimate(
    project_id: uuid.UUID,
    payload: TrainingEstimateRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingEstimateRead:
    return estimate_training_run(db, current_user, project_id, payload)


@router.post("/runs", response_model=TrainingLaunchRead, status_code=status.HTTP_202_ACCEPTED)
def launch(
    project_id: uuid.UUID,
    payload: TrainingLaunchRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingLaunchRead:
    result = launch_training_run(db, current_user, project_id, payload)
    db.commit()
    return result


@router.get("/runs", response_model=list[ModelRunRead])
def runs(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[ModelRunRead]:
    return [
        ModelRunRead.model_validate(run) for run in list_training_runs(db, current_user, project_id)
    ]


@router.get("/runs/{run_id}", response_model=ModelRunRead)
def run_status(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ModelRunRead:
    run = get_training_run(db, current_user, project_id, run_id)
    db.commit()
    db.refresh(run)
    return ModelRunRead.model_validate(run)


@router.post("/runs/{run_id}/cancel", response_model=ModelRunRead)
def cancel(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ModelRunRead:
    run = cancel_training_run(db, current_user, project_id, run_id)
    db.commit()
    db.refresh(run)
    return ModelRunRead.model_validate(run)


@router.post(
    "/runs/{run_id}/restart",
    response_model=TrainingLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def restart(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingLaunchRead:
    result = restart_training_run(db, current_user, project_id, run_id)
    db.commit()
    return result


@router.post(
    "/runs/{run_id}/models",
    response_model=TrainingLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def add_models(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    payload: TrainingAddModelsRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingLaunchRead:
    result = add_models_to_training_run(
        db,
        current_user,
        project_id,
        run_id,
        payload,
    )
    db.commit()
    return result


@router.get("/runs/{run_id}/logs", response_model=TrainingLogsRead)
def logs(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingLogsRead:
    result = training_logs(db, current_user, project_id, run_id)
    db.commit()
    return result


@router.get("/runs/{run_id}/leaderboard", response_model=TrainingLeaderboardRead)
def leaderboard(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingLeaderboardRead:
    result = training_leaderboard(db, current_user, project_id, run_id)
    db.commit()
    return result


@router.get("/runs/{run_id}/models/{model_name}/audit-document")
def audit_document(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    model_name: str,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    output_format: Annotated[Literal["html", "json"], Query(alias="format")] = "html",
) -> Response:
    content, media_type, filename, evidence_hash = model_audit_document(
        db,
        current_user,
        project_id,
        run_id,
        model_name,
        output_format,
    )
    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "ETag": f'"{evidence_hash}"',
        },
    )


@router.get("/runs/{run_id}/resources", response_model=TrainingResourceUsageRead)
def resources(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> TrainingResourceUsageRead:
    result = training_resources(db, current_user, project_id, run_id)
    db.commit()
    return result


@router.websocket("/runs/{run_id}/logs/ws")
async def logs_websocket(
    websocket: WebSocket,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    token: Annotated[str, Query()],
) -> None:
    user_id = _websocket_user_id(token)
    await websocket.accept()
    sent_lines = 0
    try:
        while True:
            with get_session_factory()() as db:
                user = db.get(User, user_id)
                if user is None or not user.is_active:
                    await websocket.send_json({"error": "User is inactive."})
                    await websocket.close(code=4401)
                    return
                result = training_logs(db, user, project_id, run_id)
                db.commit()
            new_lines = result.lines[sent_lines:]
            sent_lines = len(result.lines)
            await websocket.send_json(
                {
                    "run_id": str(run_id),
                    "status": result.status.value,
                    "lines": new_lines,
                }
            )
            if result.status in {
                RunStatus.SUCCEEDED,
                RunStatus.FAILED,
                RunStatus.CANCELLED,
            }:
                return
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return


def _websocket_user_id(token: str) -> uuid.UUID:
    try:
        payload = decode_token(
            token,
            secret=get_settings().jwt_secret_key,
            expected_type="access",
        )
        return uuid.UUID(str(payload["sub"]))
    except (KeyError, ValueError, TokenError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token.",
        ) from exc
