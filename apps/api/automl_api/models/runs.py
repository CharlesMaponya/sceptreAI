from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum as SQLEnum,
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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import ArtifactKind, MetricKind, MetricSplit, ModelStage, RunKind, RunStatus, TaskType

if TYPE_CHECKING:
    from automl_api.models.iam import User
    from automl_api.models.projects import Project


class ModelRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "model_runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="RESTRICT",
            name="fk_model_runs_dataset_version_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_model_runs_project_id_id"),
        Index("ix_model_runs_project_status", "project_id", "status"),
        Index("ix_model_runs_project_created_at", "project_id", "created_at"),
        Index("ix_model_runs_mlflow_run_id", "mlflow_run_id"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    dataset_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    created_by_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    run_kind: Mapped[RunKind] = mapped_column(
        SQLEnum(RunKind, name="run_kind", native_enum=False),
        nullable=False,
        default=RunKind.TRAINING,
        server_default=RunKind.TRAINING.value,
    )
    status: Mapped[RunStatus] = mapped_column(
        SQLEnum(RunStatus, name="run_status", native_enum=False),
        nullable=False,
        default=RunStatus.QUEUED,
        server_default=RunStatus.QUEUED.value,
    )
    task_type: Mapped[TaskType] = mapped_column(
        SQLEnum(TaskType, name="task_type", native_enum=False),
        nullable=False,
        default=TaskType.UNSPECIFIED,
        server_default=TaskType.UNSPECIFIED.value,
    )
    target_column: Mapped[str | None] = mapped_column(String(255))
    run_name: Mapped[str | None] = mapped_column(String(255))
    pipeline_name: Mapped[str | None] = mapped_column(String(255))
    mlflow_run_id: Mapped[str | None] = mapped_column(String(255))
    zenml_run_id: Mapped[str | None] = mapped_column(String(255))
    k8s_namespace: Mapped[str | None] = mapped_column(String(255))
    k8s_job_name: Mapped[str | None] = mapped_column(String(255))
    gpu_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default=text("false"))
    cpu_request_cores: Mapped[float | None] = mapped_column(Float)
    memory_request_mb: Mapped[int | None] = mapped_column(Integer)
    cpu_limit_cores: Mapped[float | None] = mapped_column(Float)
    memory_limit_mb: Mapped[int | None] = mapped_column(Integer)
    estimated_core_hours: Mapped[float | None] = mapped_column(Float)
    params: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    tags: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    failure_code: Mapped[str | None] = mapped_column(String(120))
    failure_message: Mapped[str | None] = mapped_column(Text)
    plain_english_failure: Mapped[str | None] = mapped_column(Text)
    queued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="model_runs",
        foreign_keys=[project_id],
    )
    dataset_version: Mapped["DatasetVersion"] = relationship(
        "DatasetVersion",
        back_populates="model_runs",
        primaryjoin=lambda: and_(
            ModelRun.project_id == DatasetVersion.project_id,
            ModelRun.dataset_version_id == DatasetVersion.id,
        ),
        foreign_keys=lambda: [ModelRun.project_id, ModelRun.dataset_version_id],
        overlaps="model_runs,project",
    )
    created_by: Mapped["User"] = relationship(
        "User",
        back_populates="model_runs",
        foreign_keys=[created_by_id],
    )
    metrics: Mapped[list["Metric"]] = relationship(
        "Metric",
        back_populates="model_run",
        cascade="all, delete-orphan",
        foreign_keys="Metric.model_run_id",
    )
    artifacts: Mapped[list["RunArtifact"]] = relationship(
        "RunArtifact",
        back_populates="model_run",
        cascade="all, delete-orphan",
        foreign_keys="RunArtifact.model_run_id",
    )
    registry_entries: Mapped[list["ModelRegistryEntry"]] = relationship(
        "ModelRegistryEntry",
        back_populates="model_run",
        cascade="all, delete-orphan",
        foreign_keys="ModelRegistryEntry.model_run_id",
    )


class Metric(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "metrics"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_metrics_model_run_project",
        ),
        UniqueConstraint(
            "model_run_id",
            "name",
            "split",
            "step",
            name="uq_metrics_run_name_split_step",
        ),
        Index("ix_metrics_project_kind", "project_id", "kind"),
        Index("ix_metrics_model_run", "model_run_id"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    model_run_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    kind: Mapped[MetricKind] = mapped_column(
        SQLEnum(MetricKind, name="metric_kind", native_enum=False),
        nullable=False,
        default=MetricKind.PERFORMANCE,
        server_default=MetricKind.PERFORMANCE.value,
    )
    split: Mapped[MetricSplit] = mapped_column(
        SQLEnum(MetricSplit, name="metric_split", native_enum=False),
        nullable=False,
        default=MetricSplit.VALIDATION,
        server_default=MetricSplit.VALIDATION.value,
    )
    value: Mapped[float | None] = mapped_column(Float)
    value_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    higher_is_better: Mapped[bool | None] = mapped_column(Boolean)
    step: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="metrics",
        foreign_keys=[project_id],
    )
    model_run: Mapped["ModelRun"] = relationship(
        "ModelRun",
        back_populates="metrics",
        primaryjoin=lambda: and_(
            Metric.project_id == ModelRun.project_id,
            Metric.model_run_id == ModelRun.id,
        ),
        foreign_keys=lambda: [Metric.project_id, Metric.model_run_id],
        overlaps="metrics,project",
    )


class RunArtifact(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "run_artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_run_artifacts_model_run_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_run_artifacts_project_id_id"),
        Index("ix_run_artifacts_project_kind", "project_id", "kind"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    model_run_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    kind: Mapped[ArtifactKind] = mapped_column(
        SQLEnum(ArtifactKind, name="artifact_kind", native_enum=False),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(220), nullable=False)
    object_uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_hash: Mapped[str | None] = mapped_column(String(128))
    byte_size: Mapped[int | None] = mapped_column(Integer)
    artifact_metadata: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="artifacts",
        foreign_keys=[project_id],
    )
    model_run: Mapped["ModelRun"] = relationship(
        "ModelRun",
        back_populates="artifacts",
        primaryjoin=lambda: and_(
            RunArtifact.project_id == ModelRun.project_id,
            RunArtifact.model_run_id == ModelRun.id,
        ),
        foreign_keys=lambda: [RunArtifact.project_id, RunArtifact.model_run_id],
        overlaps="artifacts,project",
    )
    registry_entries: Mapped[list["ModelRegistryEntry"]] = relationship(
        "ModelRegistryEntry",
        back_populates="model_artifact",
        foreign_keys="ModelRegistryEntry.model_artifact_id",
    )


class ModelRegistryEntry(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "model_registry_entries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_model_registry_entries_model_run_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "model_artifact_id"],
            ["run_artifacts.project_id", "run_artifacts.id"],
            ondelete="RESTRICT",
            name="fk_model_registry_entries_artifact_project",
        ),
        Index("ix_model_registry_project_stage", "project_id", "stage"),
        Index("ix_model_registry_project_feature_hash", "project_id", "feature_space_hash"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    model_run_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    model_artifact_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    stage: Mapped[ModelStage] = mapped_column(
        SQLEnum(ModelStage, name="model_stage", native_enum=False),
        nullable=False,
        default=ModelStage.CANDIDATE,
        server_default=ModelStage.CANDIDATE.value,
    )
    model_name: Mapped[str] = mapped_column(String(220), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    feature_space_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    champion_metric_name: Mapped[str | None] = mapped_column(String(160))
    champion_metric_value: Mapped[float | None] = mapped_column(Float)
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    registry_metadata: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )

    project: Mapped["Project"] = relationship(
        "Project",
        back_populates="registry_entries",
        foreign_keys=[project_id],
    )
    model_run: Mapped["ModelRun"] = relationship(
        "ModelRun",
        back_populates="registry_entries",
        primaryjoin=lambda: and_(
            ModelRegistryEntry.project_id == ModelRun.project_id,
            ModelRegistryEntry.model_run_id == ModelRun.id,
        ),
        foreign_keys=lambda: [ModelRegistryEntry.project_id, ModelRegistryEntry.model_run_id],
        overlaps="project,registry_entries",
    )
    model_artifact: Mapped["RunArtifact"] = relationship(
        "RunArtifact",
        back_populates="registry_entries",
        primaryjoin=lambda: and_(
            ModelRegistryEntry.project_id == RunArtifact.project_id,
            ModelRegistryEntry.model_artifact_id == RunArtifact.id,
        ),
        foreign_keys=lambda: [ModelRegistryEntry.project_id, ModelRegistryEntry.model_artifact_id],
        overlaps="model_run,project,registry_entries",
    )
