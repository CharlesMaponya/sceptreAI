from __future__ import annotations

import secrets
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import Session, joinedload

from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import GlobalRole, ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership, ProjectShareLink
from automl_api.models.runs import ModelRun
from automl_api.schemas.projects import (
    ProjectCreate,
    ProjectJourneyRead,
    ProjectShareLinkCreate,
    ProjectUpdate,
)
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

    if minimum_role != ProjectRole.VIEWER:
        project = db.get(Project, project_id)
        settings = getattr(project, "settings", None)
        if isinstance(settings, dict) and settings.get("deletion_in_progress"):
            raise HTTPException(
                status_code=409,
                detail="Deletion is in progress. Wait for cleanup to finish before making changes.",
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


def project_journey(db: Session, user: User, project_id: uuid.UUID) -> ProjectJourneyRead:
    get_project_for_user(db, user, project_id)
    active = (RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING)

    def run_status(*kinds: RunKind) -> RunStatus | None:
        return db.scalar(
            select(ModelRun.status)
            .where(ModelRun.project_id == project_id, ModelRun.run_kind.in_(kinds))
            .order_by(
                case((ModelRun.status.in_(active), 0), else_=1),
                ModelRun.created_at.desc(),
                ModelRun.id.desc(),
            )
            .limit(1)
        )

    return ProjectJourneyRead(
        dataset_uploaded=db.scalar(
            select(DatasetVersion.id).where(DatasetVersion.project_id == project_id).limit(1)
        )
        is not None,
        profile_status=db.scalar(
            select(ProfilingJob.status)
            .where(ProfilingJob.project_id == project_id)
            .order_by(
                case(
                    (ProfilingJob.status.not_in(("succeeded", "failed", "cancelled")), 0), else_=1
                ),
                ProfilingJob.created_at.desc(),
                ProfilingJob.id.desc(),
            )
            .limit(1)
        ),
        training_status=run_status(RunKind.TRAINING),
        analysis_status=run_status(RunKind.VALIDATION, RunKind.EXPLAINABILITY),
        deployment_status=run_status(RunKind.DEPLOYMENT),
    )


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


def list_project_members(
    db: Session, project_id: uuid.UUID, *, offset: int = 0, limit: int = 100
) -> list[ProjectMembership]:
    query = (
        select(ProjectMembership)
        .options(joinedload(ProjectMembership.user))
        .where(ProjectMembership.project_id == project_id, *_active_membership_clause())
        .order_by(ProjectMembership.created_at.asc(), ProjectMembership.id.asc())
        .offset(offset)
        .limit(limit)
    )
    return list(db.scalars(query).all())


def create_project_share_link(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ProjectShareLinkCreate,
) -> tuple[ProjectShareLink, str]:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)

    if payload.role == ProjectRole.OWNER:
        require_project_role(db, user, project_id, ProjectRole.OWNER)

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
        select(ProjectShareLink)
        .where(
            ProjectShareLink.token_hash == token_hash(invite_token),
            ProjectShareLink.revoked_at.is_(None),
        )
        .with_for_update()
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
        if existing_membership.accepted_at and (
            existing_membership.expires_at is None or existing_membership.expires_at > _now()
        ):
            raise ValueError("You already have access to this project.")
        existing_membership.role = share_link.role
        existing_membership.permissions = share_link.permissions
        existing_membership.accepted_at = existing_membership.accepted_at or _now()
        existing_membership.expires_at = None

    share_link.used_count += 1
    project = db.get(Project, share_link.project_id)
    if project is None:
        raise ValueError("Invite project no longer exists.")
    return project


def list_project_invitations(
    db: Session, user: User, project_id: uuid.UUID, *, offset: int = 0, limit: int = 100
) -> list[ProjectShareLink]:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    return list(
        db.scalars(
            select(ProjectShareLink)
            .where(
                ProjectShareLink.project_id == project_id,
            )
            .order_by(ProjectShareLink.created_at.desc(), ProjectShareLink.id.desc())
            .offset(offset)
            .limit(limit)
        ).all()
    )


def revoke_project_invitation(
    db: Session, user: User, project_id: uuid.UUID, invitation_id: uuid.UUID
) -> ProjectShareLink:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    invite = db.scalar(
        select(ProjectShareLink)
        .where(
            ProjectShareLink.project_id == project_id,
            ProjectShareLink.id == invitation_id,
        )
        .with_for_update()
    )
    if invite is None:
        raise HTTPException(status_code=404, detail="Invitation not found.")
    if invite.role == ProjectRole.OWNER:
        require_project_role(db, user, project_id, ProjectRole.OWNER)
    invite.revoked_at = invite.revoked_at or _now()
    return invite


def remove_project_member(
    db: Session, user: User, project_id: uuid.UUID, membership_id: uuid.UUID
) -> None:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    member = db.scalar(
        select(ProjectMembership)
        .where(
            ProjectMembership.project_id == project_id,
            ProjectMembership.id == membership_id,
        )
        .with_for_update()
    )
    if member is None:
        raise HTTPException(status_code=404, detail="Project member not found.")
    project = db.get(Project, project_id)
    if member.user_id == project.owner_id:
        raise HTTPException(
            status_code=409, detail="The project creator's access cannot be removed."
        )
    if member.role in {ProjectRole.OWNER, ProjectRole.ADMIN}:
        require_project_role(db, user, project_id, ProjectRole.OWNER)
    db.delete(member)


def page_visible_projects(db: Session, user: User, *, offset: int, limit: int, search: str) -> dict:
    query = select(Project)
    if user.global_role != GlobalRole.ADMIN:
        query = query.join(ProjectMembership, ProjectMembership.project_id == Project.id).where(
            ProjectMembership.user_id == user.id, *_active_membership_clause()
        )
    if search:
        query = query.where(
            or_(
                Project.name.icontains(search, autoescape=True),
                Project.description.icontains(search, autoescape=True),
            )
        )
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.order_by(Project.created_at.desc(), Project.id.desc()).offset(offset).limit(limit)
    ).all()
    return {"items": items, "total": total}
