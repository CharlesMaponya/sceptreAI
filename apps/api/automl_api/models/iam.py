from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, Text, text
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.enums import AuthProvider, GlobalRole

if TYPE_CHECKING:
    from automl_api.models.datasets import Dataset, DatasetVersion
    from automl_api.models.projects import Project, ProjectMembership, ProjectShareLink
    from automl_api.models.runs import ModelRun


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True, index=True)
    full_name: Mapped[str | None] = mapped_column(String(200))
    password_hash: Mapped[str | None] = mapped_column(Text)
    auth_provider: Mapped[AuthProvider] = mapped_column(
        SQLEnum(AuthProvider, name="auth_provider", native_enum=False),
        nullable=False,
        default=AuthProvider.SIMPLE,
        server_default=AuthProvider.SIMPLE.value,
    )
    global_role: Mapped[GlobalRole] = mapped_column(
        SQLEnum(GlobalRole, name="global_role", native_enum=False),
        nullable=False,
        default=GlobalRole.MEMBER,
        server_default=GlobalRole.MEMBER.value,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    is_verified: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    token_version: Mapped[int] = mapped_column(nullable=False, default=1, server_default="1")
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    preferences: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    owned_projects: Mapped[list[Project]] = relationship(
        "Project",
        back_populates="owner",
        foreign_keys="Project.owner_id",
    )
    created_projects: Mapped[list[Project]] = relationship(
        "Project",
        back_populates="created_by",
        foreign_keys="Project.created_by_id",
    )
    memberships: Mapped[list[ProjectMembership]] = relationship(
        "ProjectMembership",
        back_populates="user",
        cascade="all, delete-orphan",
        foreign_keys="ProjectMembership.user_id",
    )
    datasets: Mapped[list[Dataset]] = relationship(
        "Dataset",
        back_populates="created_by",
        foreign_keys="Dataset.created_by_id",
    )
    dataset_versions: Mapped[list[DatasetVersion]] = relationship(
        "DatasetVersion",
        back_populates="created_by",
        foreign_keys="DatasetVersion.created_by_id",
    )
    model_runs: Mapped[list[ModelRun]] = relationship(
        "ModelRun",
        back_populates="created_by",
        foreign_keys="ModelRun.created_by_id",
    )
    refresh_tokens: Mapped[list[RefreshToken]] = relationship(
        "RefreshToken",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    password_reset_tokens: Mapped[list[PasswordResetToken]] = relationship(
        "PasswordResetToken",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    created_share_links: Mapped[list[ProjectShareLink]] = relationship(
        "ProjectShareLink",
        back_populates="created_by",
        foreign_keys="ProjectShareLink.created_by_id",
    )


class RefreshToken(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "refresh_tokens"
    __table_args__ = (
        Index("ix_refresh_tokens_user_expires", "user_id", "expires_at"),
        Index("ix_refresh_tokens_family", "family_id"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    family_id: Mapped[uuid.UUID] = mapped_column(nullable=False, default=uuid.uuid4)
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    user_agent: Mapped[str | None] = mapped_column(Text)
    ip_address: Mapped[str | None] = mapped_column(String(64))

    user: Mapped[User] = relationship("User", back_populates="refresh_tokens")


class PasswordResetToken(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "password_reset_tokens"
    __table_args__ = (Index("ix_password_reset_tokens_user_expires", "user_id", "expires_at"),)

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped[User] = relationship("User", back_populates="password_reset_tokens")
