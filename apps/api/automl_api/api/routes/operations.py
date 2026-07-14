from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.operations import (
    ArtifactCleanupRead,
    ArtifactCleanupRequest,
    DeploymentStatusRead,
    DriftLaunchRead,
    DriftLaunchRequest,
    ModelDeploymentLaunchRead,
    ModelDeploymentRequest,
    PlatformHealthRead,
    RegistryCreateRequest,
    RegistryEntryRead,
    RegistryStageUpdateRequest,
)
from automl_api.schemas.training import ModelRunRead
from automl_api.services.inference_gateway import (
    proxy_deployment_inference,
    resolve_deployment_inference_target,
)
from automl_api.services.operations import (
    cleanup_project_resources,
    deploy_registered_model,
    launch_drift_check,
    list_drift_runs,
    list_model_deployments,
    list_registry_entries,
    platform_health,
    register_model,
    registry_entry_read,
    set_registry_fallback,
    stop_model_deployment,
    update_registry_stage,
)

router = APIRouter(prefix="/projects/{project_id}/operations", tags=["operations"])


@router.get("/health", response_model=PlatformHealthRead)
def health(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> PlatformHealthRead:
    return platform_health(db, current_user, project_id)


@router.get("/registry", response_model=list[RegistryEntryRead])
def registry_entries(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[RegistryEntryRead]:
    return list_registry_entries(db, current_user, project_id)


@router.post(
    "/registry",
    response_model=RegistryEntryRead,
    status_code=status.HTTP_201_CREATED,
)
def create_registry_entry(
    project_id: uuid.UUID,
    payload: RegistryCreateRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RegistryEntryRead:
    entry = register_model(db, current_user, project_id, payload)
    db.commit()
    db.refresh(entry)
    return registry_entry_read(entry)


@router.post("/registry/{entry_id}/stage", response_model=RegistryEntryRead)
def change_registry_stage(
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    payload: RegistryStageUpdateRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RegistryEntryRead:
    entry = update_registry_stage(
        db,
        current_user,
        project_id,
        entry_id,
        payload.stage,
    )
    db.commit()
    db.refresh(entry)
    return registry_entry_read(entry)


@router.post("/registry/{entry_id}/fallback", response_model=RegistryEntryRead)
def select_registry_fallback(
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RegistryEntryRead:
    entry = set_registry_fallback(db, current_user, project_id, entry_id)
    db.commit()
    db.refresh(entry)
    return registry_entry_read(entry)


@router.post(
    "/registry/{entry_id}/drift",
    response_model=DriftLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def drift(
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    payload: DriftLaunchRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> DriftLaunchRead:
    result = launch_drift_check(
        db,
        current_user,
        project_id,
        entry_id,
        payload,
    )
    db.commit()
    return result


@router.get("/drift-runs", response_model=list[ModelRunRead])
def drift_runs(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[ModelRunRead]:
    result = [
        ModelRunRead.model_validate(run)
        for run in list_drift_runs(db, current_user, project_id)
    ]
    db.commit()
    return result


@router.post(
    "/registry/{entry_id}/deployments",
    response_model=ModelDeploymentLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def deploy(
    project_id: uuid.UUID,
    entry_id: uuid.UUID,
    payload: ModelDeploymentRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ModelDeploymentLaunchRead:
    result = deploy_registered_model(
        db,
        current_user,
        project_id,
        entry_id,
        payload,
    )
    db.commit()
    return result


@router.get("/deployments", response_model=list[DeploymentStatusRead])
def deployments(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[DeploymentStatusRead]:
    result = list_model_deployments(db, current_user, project_id)
    db.commit()
    return result


@router.api_route(
    "/deployments/{run_id}/inference/{path:path}",
    methods=["GET", "POST"],
    include_in_schema=False,
)
async def deployment_inference_gateway(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    path: str,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> Response:
    target = resolve_deployment_inference_target(
        db,
        current_user,
        project_id,
        run_id,
    )
    return await proxy_deployment_inference(request, target, path)


@router.post("/deployments/{run_id}/stop", response_model=ModelRunRead)
def stop_deployment(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ModelRunRead:
    run = stop_model_deployment(db, current_user, project_id, run_id)
    db.commit()
    db.refresh(run)
    return ModelRunRead.model_validate(run)


@router.post("/cleanup", response_model=ArtifactCleanupRead)
def cleanup(
    project_id: uuid.UUID,
    payload: ArtifactCleanupRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ArtifactCleanupRead:
    result = cleanup_project_resources(
        db,
        current_user,
        project_id,
        payload,
    )
    db.commit()
    return result
