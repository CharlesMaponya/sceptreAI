from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    and_,
    text,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.enums import DatasetFormat, DatasetStatus, ObjectStoreType

if TYPE_CHECKING:
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from automl_api.models.runs import ModelRun


class Dataset(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "datasets"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_datasets_project_id_id"),
        Index("ix_datasets_project_created_at", "project_id", "created_at"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    created_by_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    name: Mapped[str] = mapped_column(String(220), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    latest_version_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    tags: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="datasets",
        foreign_keys=[project_id],
    )
    created_by: Mapped["User"] = relationship(
        "User",
        back_populates="datasets",
        foreign_keys=[created_by_id],
    )
    versions: Mapped[list["DatasetVersion"]] = relationship(
        "DatasetVersion",
        back_populates="dataset",
        cascade="all, delete-orphan",
        primaryjoin=lambda: and_(
            Dataset.project_id == DatasetVersion.project_id,
            Dataset.id == DatasetVersion.dataset_id,
        ),
        foreign_keys=lambda: [DatasetVersion.project_id, DatasetVersion.dataset_id],
    )


class DatasetVersion(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_id"],
            ["datasets.project_id", "datasets.id"],
            ondelete="CASCADE",
            name="fk_dataset_versions_dataset_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_dataset_versions_project_id_id"),
        UniqueConstraint("dataset_id", "version_number", name="uq_dataset_versions_dataset_version"),
        Index("ix_dataset_versions_project_dataset", "project_id", "dataset_id"),
        Index("ix_dataset_versions_content_hash", "content_hash"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    dataset_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    created_by_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[DatasetStatus] = mapped_column(
        SQLEnum(DatasetStatus, name="dataset_status", native_enum=False),
        nullable=False,
        default=DatasetStatus.UPLOADED,
        server_default=DatasetStatus.UPLOADED.value,
    )
    format: Mapped[DatasetFormat] = mapped_column(
        SQLEnum(DatasetFormat, name="dataset_format", native_enum=False),
        nullable=False,
    )
    object_store_type: Mapped[ObjectStoreType] = mapped_column(
        SQLEnum(ObjectStoreType, name="object_store_type", native_enum=False),
        nullable=False,
        default=ObjectStoreType.MINIO,
        server_default=ObjectStoreType.MINIO.value,
    )
    object_uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    original_filename: Mapped[str | None] = mapped_column(String(512))
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    byte_size: Mapped[int | None] = mapped_column(BigInteger)
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    column_count: Mapped[int | None] = mapped_column(Integer)
    schema_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    inferred_types_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    quality_report_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    profile_artifact_uri: Mapped[str | None] = mapped_column(String(1024))

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="dataset_versions",
        foreign_keys=[project_id],
        overlaps="versions",
    )
    dataset: Mapped["Dataset"] = relationship(
        "Dataset",
        back_populates="versions",
        primaryjoin=lambda: and_(
            DatasetVersion.project_id == Dataset.project_id,
            DatasetVersion.dataset_id == Dataset.id,
        ),
        foreign_keys=lambda: [DatasetVersion.project_id, DatasetVersion.dataset_id],
        overlaps="project",
    )
    created_by: Mapped["User"] = relationship(
        "User",
        back_populates="dataset_versions",
        foreign_keys=[created_by_id],
    )
    model_runs: Mapped[list["ModelRun"]] = relationship(
        "ModelRun",
        back_populates="dataset_version",
        foreign_keys="ModelRun.dataset_version_id",
    )


class ProfilingJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "profiling_jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="CASCADE",
            name="fk_profiling_jobs_dataset_version_project",
        ),
        Index("ix_profiling_jobs_project_version", "project_id", "dataset_version_id"),
        Index("ix_profiling_jobs_status_updated", "status", "updated_at"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"),
        nullable=False,
    )
    dataset_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    dataset_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    created_by_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    target_column: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="queued",
        server_default="queued",
    )
    current_stage: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="overview",
        server_default="overview",
    )
    progress: Mapped[float] = mapped_column(
        Float,
        nullable=False,
        default=0.0,
        server_default="0",
    )
    total_columns: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    completed_columns: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    row_count: Mapped[int | None] = mapped_column(BigInteger)
    overview_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    feature_profiles_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    relationships_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    preparation_json: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    warnings_json: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    artifact_uris_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    failure_message: Mapped[str | None] = mapped_column(Text)
    auto_started: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @property
    def available_features(self) -> list[str]:
        return list(self.feature_profiles_json)
