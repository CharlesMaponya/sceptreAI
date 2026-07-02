from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.enums import ProjectRole, ProjectStatus

if TYPE_CHECKING:
    from automl_api.models.datasets import Dataset, DatasetVersion
    from automl_api.models.iam import User
    from automl_api.models.runs import Metric, ModelRegistryEntry, ModelRun, RunArtifact


class Project(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "projects"
    __table_args__ = (
        Index("ix_projects_owner_status", "owner_id", "status"),
        Index("ix_projects_created_by", "created_by_id"),
    )

    owner_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    created_by_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(180), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[ProjectStatus] = mapped_column(
        SQLEnum(ProjectStatus, name="project_status", native_enum=False),
        nullable=False,
        default=ProjectStatus.ACTIVE,
        server_default=ProjectStatus.ACTIVE.value,
    )
    object_prefix: Mapped[str | None] = mapped_column(String(512), unique=True)
    settings: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    owner: Mapped[User] = relationship(
        "User",
        back_populates="owned_projects",
        foreign_keys=[owner_id],
    )
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_projects",
        foreign_keys=[created_by_id],
    )
    memberships: Mapped[list[ProjectMembership]] = relationship(
        "ProjectMembership",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="ProjectMembership.project_id",
    )
    share_links: Mapped[list[ProjectShareLink]] = relationship(
        "ProjectShareLink",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="ProjectShareLink.project_id",
    )
    datasets: Mapped[list[Dataset]] = relationship(
        "Dataset",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="Dataset.project_id",
    )
    dataset_versions: Mapped[list[DatasetVersion]] = relationship(
        "DatasetVersion",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="DatasetVersion.project_id",
        overlaps="dataset,versions",
    )
    model_runs: Mapped[list[ModelRun]] = relationship(
        "ModelRun",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="ModelRun.project_id",
    )
    metrics: Mapped[list[Metric]] = relationship(
        "Metric",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="Metric.project_id",
    )
    artifacts: Mapped[list[RunArtifact]] = relationship(
        "RunArtifact",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="RunArtifact.project_id",
    )
    registry_entries: Mapped[list[ModelRegistryEntry]] = relationship(
        "ModelRegistryEntry",
        back_populates="project",
        cascade="all, delete-orphan",
        foreign_keys="ModelRegistryEntry.project_id",
    )


class ProjectMembership(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "project_memberships"
    __table_args__ = (
        UniqueConstraint("project_id", "user_id", name="uq_project_memberships_project_user"),
        Index("ix_project_memberships_user_role", "user_id", "role"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    invited_by_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    role: Mapped[ProjectRole] = mapped_column(
        SQLEnum(ProjectRole, name="project_role", native_enum=False),
        nullable=False,
        default=ProjectRole.VIEWER,
        server_default=ProjectRole.VIEWER.value,
    )
    permissions: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    project: Mapped[Project] = relationship(
        "Project",
        back_populates="memberships",
        foreign_keys=[project_id],
    )
    user: Mapped[User] = relationship(
        "User",
        back_populates="memberships",
        foreign_keys=[user_id],
    )


class ProjectShareLink(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "project_share_links"
    __table_args__ = (
        Index("ix_project_share_links_project_expires", "project_id", "expires_at"),
        Index("ix_project_share_links_token_hash", "token_hash"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    created_by_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    role: Mapped[ProjectRole] = mapped_column(
        SQLEnum(ProjectRole, name="project_share_role", native_enum=False),
        nullable=False,
        default=ProjectRole.VIEWER,
        server_default=ProjectRole.VIEWER.value,
    )
    permissions: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    max_uses: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    used_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    project: Mapped[Project] = relationship(
        "Project",
        back_populates="share_links",
        foreign_keys=[project_id],
    )
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_share_links",
        foreign_keys=[created_by_id],
    )
