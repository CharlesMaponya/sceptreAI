from __future__ import annotations

import secrets
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import Select, or_, select
from sqlalchemy.orm import Session, joinedload

from automl_api.models.enums import GlobalRole, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership, ProjectShareLink
from automl_api.schemas.projects import ProjectCreate, ProjectShareLinkCreate, ProjectUpdate
from automl_api.security.tokens import token_hash

ROLE_RANK = {
    ProjectRole.VIEWER: 10,
    ProjectRole.EDITOR: 20,
    ProjectRole.ADMIN: 30,
    ProjectRole.OWNER: 40,
}


def _now() -> datetime:
    return datetime.now(UTC)


def _active_membership_clause() -> tuple:
    now = _now()
    return (
        ProjectMembership.accepted_at.is_not(None),
        or_(ProjectMembership.expires_at.is_(None), ProjectMembership.expires_at > now),
    )


def _membership_query(
    user_id: uuid.UUID, project_id: uuid.UUID
) -> Select[tuple[ProjectMembership]]:
    return select(ProjectMembership).where(
        ProjectMembership.user_id == user_id,
        ProjectMembership.project_id == project_id,
        *_active_membership_clause(),
    )


def user_has_project_role(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    minimum_role: ProjectRole = ProjectRole.VIEWER,
) -> bool:
    if user.global_role == GlobalRole.ADMIN:
        return True

    membership = db.scalar(_membership_query(user.id, project_id))
    if membership is None:
        return False
    return ROLE_RANK[membership.role] >= ROLE_RANK[minimum_role]


def require_project_role(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    minimum_role: ProjectRole = ProjectRole.VIEWER,
) -> None:
    if not user_has_project_role(db, user, project_id, minimum_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You do not have access to this project.",
        )


def list_visible_projects(db: Session, user: User) -> list[Project]:
    if user.global_role == GlobalRole.ADMIN:
        return list(db.scalars(select(Project).order_by(Project.created_at.desc())).all())

    query = (
        select(Project)
        .join(ProjectMembership, ProjectMembership.project_id == Project.id)
        .where(ProjectMembership.user_id == user.id, *_active_membership_clause())
        .order_by(Project.created_at.desc())
    )
    return list(db.scalars(query).unique().all())


def create_project(db: Session, user: User, payload: ProjectCreate) -> Project:
    project = Project(
        owner_id=user.id,
        created_by_id=user.id,
        name=payload.name,
        description=payload.description,
        settings=payload.settings,
    )
    db.add(project)
    db.flush()

    project.object_prefix = f"projects/{project.id}"
    db.add(
        ProjectMembership(
            project_id=project.id,
            user_id=user.id,
            role=ProjectRole.OWNER,
            accepted_at=_now(),
        )
    )
    return project


def get_project_for_user(db: Session, user: User, project_id: uuid.UUID) -> Project:
    project = db.get(Project, project_id)
    if project is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found.")

    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return project


def update_project(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ProjectUpdate,
) -> Project:
    project = get_project_for_user(db, user, project_id)
    require_project_role(db, user, project_id, ProjectRole.ADMIN)

    update_data = payload.model_dump(exclude_unset=True)
    for field_name, value in update_data.items():
        setattr(project, field_name, value)
    return project


def list_project_members(db: Session, project_id: uuid.UUID) -> list[ProjectMembership]:
    query = (
        select(ProjectMembership)
        .options(joinedload(ProjectMembership.user))
        .where(ProjectMembership.project_id == project_id)
        .order_by(ProjectMembership.created_at.asc())
    )
    return list(db.scalars(query).all())


def create_project_share_link(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ProjectShareLinkCreate,
) -> tuple[ProjectShareLink, str]:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)

    invite_token = secrets.token_urlsafe(32)
    share_link = ProjectShareLink(
        project_id=project_id,
        created_by_id=user.id,
        token_hash=token_hash(invite_token),
        role=payload.role,
        permissions=payload.permissions,
        expires_at=_now() + timedelta(days=payload.expires_in_days),
        max_uses=payload.max_uses,
    )
    db.add(share_link)
    db.flush()
    return share_link, invite_token


def accept_project_share_link(db: Session, user: User, invite_token: str) -> Project:
    share_link = db.scalar(
        select(ProjectShareLink).where(
            ProjectShareLink.token_hash == token_hash(invite_token),
            ProjectShareLink.revoked_at.is_(None),
        )
    )
    if share_link is None:
        raise ValueError("Invite link was not found.")
    if share_link.expires_at <= _now():
        raise ValueError("Invite link has expired.")
    if share_link.used_count >= share_link.max_uses:
        raise ValueError("Invite link has already reached its maximum number of uses.")

    existing_membership = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == share_link.project_id,
            ProjectMembership.user_id == user.id,
        )
    )
    if existing_membership is None:
        db.add(
            ProjectMembership(
                project_id=share_link.project_id,
                user_id=user.id,
                invited_by_id=share_link.created_by_id,
                role=share_link.role,
                permissions=share_link.permissions,
                accepted_at=_now(),
            )
        )
    else:
        existing_membership.role = share_link.role
        existing_membership.permissions = share_link.permissions
        existing_membership.accepted_at = existing_membership.accepted_at or _now()
        existing_membership.expires_at = None

    share_link.used_count += 1
    project = db.get(Project, share_link.project_id)
    if project is None:
        raise ValueError("Invite project no longer exists.")
    return project
