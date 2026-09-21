from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from automl_api.models.enums import ProjectRole, ProjectStatus, RunStatus


class ProjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=180)
    description: str | None = None
    settings: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        return value.strip()


class ProjectUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=180)
    description: str | None = None
    status: ProjectStatus | None = None
    settings: dict[str, Any] | None = None

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else value


class ProjectRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    owner_id: uuid.UUID
    created_by_id: uuid.UUID
    name: str
    description: str | None = None
    status: ProjectStatus
    object_prefix: str | None = None
    settings: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ProjectPageRead(BaseModel):
    items: list[ProjectRead]
    total: int


class ProjectJourneyRead(BaseModel):
    dataset_uploaded: bool
    profile_status: str | None = None
    training_status: RunStatus | None = None
    analysis_status: RunStatus | None = None
    deployment_status: RunStatus | None = None


class ProjectShareLinkCreate(BaseModel):
    role: ProjectRole = ProjectRole.VIEWER
    permissions: dict[str, Any] = Field(default_factory=dict)
    expires_in_days: int = Field(default=7, ge=1, le=30)
    max_uses: int = Field(default=1, ge=1, le=50)


class ProjectInvitationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    project_id: uuid.UUID
    role: ProjectRole
    expires_at: datetime
    max_uses: int
    used_count: int
    revoked_at: datetime | None = None


class ProjectShareLinkRead(ProjectInvitationRead):
    invite_token: str


class ProjectShareAccept(BaseModel):
    invite_token: str = Field(min_length=16)


class ProjectMemberRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    email: str
    full_name: str | None = None
    role: ProjectRole
    accepted_at: datetime | None = None
    expires_at: datetime | None = None
