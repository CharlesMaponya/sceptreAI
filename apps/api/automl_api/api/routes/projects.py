from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.projects import (
    ProjectCreate,
    ProjectMemberRead,
    ProjectRead,
    ProjectShareAccept,
    ProjectShareLinkCreate,
    ProjectShareLinkRead,
    ProjectUpdate,
)
from automl_api.services.projects import (
    accept_project_share_link,
    create_project,
    create_project_share_link,
    get_project_for_user,
    list_project_members,
    list_visible_projects,
    update_project,
)

router = APIRouter(prefix="/projects", tags=["projects"])


@router.get("", response_model=list[ProjectRead])
def list_projects(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[ProjectRead]:
    return [ProjectRead.model_validate(project) for project in list_visible_projects(db, current_user)]


@router.post("", response_model=ProjectRead, status_code=status.HTTP_201_CREATED)
def create(
    payload: ProjectCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProjectRead:
    project = create_project(db, current_user, payload)
    db.commit()
    db.refresh(project)
    return ProjectRead.model_validate(project)


@router.get("/{project_id}", response_model=ProjectRead)
def get_project(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProjectRead:
    project = get_project_for_user(db, current_user, project_id)
    return ProjectRead.model_validate(project)


@router.patch("/{project_id}", response_model=ProjectRead)
def patch_project(
    project_id: uuid.UUID,
    payload: ProjectUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProjectRead:
    project = update_project(db, current_user, project_id, payload)
    db.commit()
    db.refresh(project)
    return ProjectRead.model_validate(project)


@router.get("/{project_id}/members", response_model=list[ProjectMemberRead])
def members(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[ProjectMemberRead]:
    get_project_for_user(db, current_user, project_id)
    return [
        ProjectMemberRead(
            id=membership.id,
            user_id=membership.user_id,
            email=membership.user.email,
            full_name=membership.user.full_name,
            role=membership.role,
            accepted_at=membership.accepted_at,
            expires_at=membership.expires_at,
        )
        for membership in list_project_members(db, project_id)
    ]


@router.post("/{project_id}/share-links", response_model=ProjectShareLinkRead)
def share_link(
    project_id: uuid.UUID,
    payload: ProjectShareLinkCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProjectShareLinkRead:
    share_link_record, plaintext_token = create_project_share_link(
        db,
        current_user,
        project_id,
        payload,
    )
    db.commit()
    db.refresh(share_link_record)
    return ProjectShareLinkRead(
        id=share_link_record.id,
        project_id=share_link_record.project_id,
        role=share_link_record.role,
        expires_at=share_link_record.expires_at,
        max_uses=share_link_record.max_uses,
        used_count=share_link_record.used_count,
        invite_token=plaintext_token,
    )


@router.post("/share-links/accept", response_model=ProjectRead)
def accept_share_link(
    payload: ProjectShareAccept,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProjectRead:
    try:
        project = accept_project_share_link(db, current_user, payload.invite_token)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    db.commit()
    db.refresh(project)
    return ProjectRead.model_validate(project)

