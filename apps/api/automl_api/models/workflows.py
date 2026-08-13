from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from automl_api.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    OutboxStatus,
    ScopeStatus,
    WorkflowStage,
)

JSON_DEFAULT = text("'{}'::jsonb")
ENUM_VALUES = lambda enum: [item.value for item in enum]  # noqa: E731


class WorkflowCommand(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "workflow_commands"
    __table_args__ = (
        UniqueConstraint(
            "project_id", "operation", "idempotency_key", name="uq_command_idempotency"
        ),
        UniqueConstraint("project_id", "id", name="uq_workflow_commands_project_id_id"),
        Index("ix_workflow_commands_status_available", "status", "available_at"),
        CheckConstraint("retry_count >= 0 AND max_retries >= 0", name="command_retry_bounds"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    actor_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    operation: Mapped[str] = mapped_column(String(80), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    request_payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    status: Mapped[CommandStatus] = mapped_column(
        SQLEnum(
            CommandStatus,
            name="command_status",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
        default=CommandStatus.PENDING,
        server_default=CommandStatus.PENDING.value,
    )
    resource_type: Mapped[str | None] = mapped_column(String(80))
    resource_id: Mapped[uuid.UUID | None] = mapped_column()
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=5, server_default="5")
    terminal_reason: Mapped[str | None] = mapped_column(Text)
    replayed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))


class OutboxEntry(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "outbox_entries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="CASCADE",
            name="fk_outbox_command_project",
        ),
        UniqueConstraint("project_id", "event_key", name="uq_outbox_event_key"),
        Index("ix_outbox_status_available", "status", "available_at"),
        CheckConstraint("delivery_attempts >= 0", name="outbox_delivery_attempts"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    command_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    event_key: Mapped[str] = mapped_column(String(255), nullable=False)
    topic: Mapped[str] = mapped_column(String(160), nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(80), nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    status: Mapped[OutboxStatus] = mapped_column(
        SQLEnum(
            OutboxStatus,
            name="outbox_status",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
        default=OutboxStatus.PENDING,
        server_default=OutboxStatus.PENDING.value,
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class CapacityReservation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "capacity_reservations"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="CASCADE",
            name="fk_capacity_command_project",
        ),
        UniqueConstraint("project_id", "command_id", name="uq_capacity_command"),
        CheckConstraint(
            "cpu_millis >= 0 AND memory_bytes >= 0 AND gpu_count >= 0",
            name="capacity_nonnegative",
        ),
        Index("ix_capacity_project_expires", "project_id", "expires_at"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    command_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    resource_class: Mapped[str] = mapped_column(String(80), nullable=False)
    cpu_millis: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    memory_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    gpu_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="held")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ImmutableProjectRevisionMixin:
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    digest_algorithm: Mapped[str] = mapped_column(String(32), nullable=False, default="sha256")
    digest_scope: Mapped[str] = mapped_column(String(80), nullable=False)
    content_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    specification: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )


class PreparedArtifact(ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "prepared_artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="RESTRICT",
            name="fk_prepared_artifact_dataset_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_prepared_artifacts_project_id_id"),
        UniqueConstraint(
            "project_id", "dataset_version_id", "revision", name="uq_prepared_artifact_revision"
        ),
    )

    dataset_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    object_uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ready")


class DatasetSplitRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "dataset_split_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="RESTRICT",
            name="fk_split_dataset_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_split_revisions_project_id_id"),
        UniqueConstraint("project_id", "dataset_version_id", "revision", name="uq_split_revision"),
    )

    dataset_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    train_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    validation_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    final_test_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    sealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FeatureContractRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "feature_contract_revisions"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_feature_contracts_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_feature_contract_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_column: Mapped[str] = mapped_column(String(255), nullable=False)


class FeatureRegistryRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "feature_registry_revisions"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_feature_registries_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_feature_registry_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)


class FeatureRecipeRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "feature_recipe_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "registry_revision_id"],
            ["feature_registry_revisions.project_id", "feature_registry_revisions.id"],
            ondelete="RESTRICT",
            name="fk_recipe_registry_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_feature_recipes_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_feature_recipe_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    registry_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)


class EstimatorCatalogRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "estimator_catalog_revisions"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_estimator_catalogs_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_estimator_catalog_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    release_version: Mapped[str] = mapped_column(String(64), nullable=False)


class FeatureSearchSpaceRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "feature_search_space_revisions"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_search_spaces_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_search_space_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(160), nullable=False)
    metric_direction: Mapped[str] = mapped_column(String(16), nullable=False)


class SearchObjectiveRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "search_objective_revisions"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_search_objectives_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_search_objective_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(160), nullable=False)
    metric_direction: Mapped[str] = mapped_column(String(16), nullable=False)


class ExperimentSpecRevision(
    ImmutableProjectRevisionMixin, UUIDPrimaryKeyMixin, TimestampMixin, Base
):
    __tablename__ = "experiment_spec_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_dataset_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "split_revision_id"],
            ["dataset_split_revisions.project_id", "dataset_split_revisions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_split_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "feature_contract_revision_id"],
            ["feature_contract_revisions.project_id", "feature_contract_revisions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_contract_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "feature_search_space_revision_id"],
            ["feature_search_space_revisions.project_id", "feature_search_space_revisions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_search_space_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "search_objective_revision_id"],
            ["search_objective_revisions.project_id", "search_objective_revisions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_objective_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "catalog_revision_id"],
            ["estimator_catalog_revisions.project_id", "estimator_catalog_revisions.id"],
            ondelete="RESTRICT",
            name="fk_experiment_catalog_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_experiment_specs_project_id_id"),
        UniqueConstraint("project_id", "name", "revision", name="uq_experiment_spec_revision"),
    )

    name: Mapped[str] = mapped_column(String(220), nullable=False)
    dataset_version_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    split_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    feature_contract_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    feature_search_space_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    search_objective_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    catalog_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_column: Mapped[str | None] = mapped_column(String(255))
    primary_metric: Mapped[str] = mapped_column(String(160), nullable=False)


class PromotionalScope(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "promotional_scopes"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "split_revision_id"],
            ["dataset_split_revisions.project_id", "dataset_split_revisions.id"],
            ondelete="RESTRICT",
            name="fk_scope_split_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "experiment_spec_revision_id"],
            ["experiment_spec_revisions.project_id", "experiment_spec_revisions.id"],
            ondelete="RESTRICT",
            name="fk_scope_experiment_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_promotional_scopes_project_id_id"),
        UniqueConstraint("project_id", "scope_key", name="uq_promotional_scope_key"),
        CheckConstraint("expected_members > 0", name="scope_expected_members_positive"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    scope_key: Mapped[str] = mapped_column(String(255), nullable=False)
    split_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    canonical_provider: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(
        String(32), nullable=False, default="validation_only", server_default="validation_only"
    )
    expected_members: Mapped[int] = mapped_column(Integer, nullable=False)
    experiment_spec_revision_id: Mapped[uuid.UUID | None] = mapped_column()
    membership_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scope_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    scope_deadline_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    comparison_policy: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    final_threshold_revision: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[ScopeStatus] = mapped_column(
        SQLEnum(
            ScopeStatus,
            name="scope_status",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
        default=ScopeStatus.OPEN,
        server_default=ScopeStatus.OPEN.value,
    )
    membership_digest: Mapped[str | None] = mapped_column(String(128))
    sealed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cas_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


class PromotionalScopeMember(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "promotional_scope_members"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "scope_id"],
            ["promotional_scopes.project_id", "promotional_scopes.id"],
            ondelete="CASCADE",
            name="fk_scope_member_scope_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_scope_member_run_project",
        ),
        UniqueConstraint("scope_id", "model_run_id", name="uq_scope_member_run"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    scope_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    model_run_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class TrainingCandidate(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "training_candidates"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_candidate_run_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "catalog_revision_id"],
            ["estimator_catalog_revisions.project_id", "estimator_catalog_revisions.id"],
            ondelete="RESTRICT",
            name="fk_candidate_catalog_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "feature_recipe_revision_id"],
            ["feature_recipe_revisions.project_id", "feature_recipe_revisions.id"],
            ondelete="RESTRICT",
            name="fk_candidate_recipe_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_training_candidates_project_id_id"),
        UniqueConstraint("model_run_id", "candidate_key", name="uq_candidate_run_key"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    model_run_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    candidate_key: Mapped[str] = mapped_column(String(255), nullable=False)
    estimator_key: Mapped[str] = mapped_column(String(160), nullable=False)
    catalog_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    feature_recipe_revision_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    params: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")


class TrainingTrial(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "training_trials"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "candidate_id"],
            ["training_candidates.project_id", "training_candidates.id"],
            ondelete="CASCADE",
            name="fk_trial_candidate_project",
        ),
        UniqueConstraint("project_id", "id", name="uq_training_trials_project_id_id"),
        UniqueConstraint("candidate_id", "suggestion_id", name="uq_trial_suggestion"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    candidate_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    suggestion_id: Mapped[str] = mapped_column(String(255), nullable=False)
    params_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    result_digest: Mapped[str | None] = mapped_column(String(128))


class ProviderConformanceCampaign(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "provider_conformance_campaigns"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_conformance_campaigns_project_id_id"),
        UniqueConstraint("project_id", "campaign_key", name="uq_conformance_campaign_key"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    campaign_key: Mapped[str] = mapped_column(String(255), nullable=False)
    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    stage: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    profile_digest: Mapped[str] = mapped_column(String(128), nullable=False)


class WorkflowAttempt(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "workflow_attempts"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_workflow_attempts_project_id_id"),
        UniqueConstraint("project_id", "logical_key", "generation", name="uq_attempt_generation"),
        ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            ondelete="CASCADE",
            name="fk_attempt_dataset_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            ondelete="CASCADE",
            name="fk_attempt_run_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "trial_id"],
            ["training_trials.project_id", "training_trials.id"],
            ondelete="CASCADE",
            name="fk_attempt_trial_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "scope_id"],
            ["promotional_scopes.project_id", "promotional_scopes.id"],
            ondelete="CASCADE",
            name="fk_attempt_scope_project",
        ),
        ForeignKeyConstraint(
            ["project_id", "conformance_campaign_id"],
            ["provider_conformance_campaigns.project_id", "provider_conformance_campaigns.id"],
            ondelete="CASCADE",
            name="fk_attempt_conformance_project",
        ),
        Index("ix_attempt_claim", "status", "available_at", "lease_expires_at"),
        CheckConstraint("generation > 0 AND retry_count >= 0", name="attempt_generation_retry"),
        CheckConstraint(
            "(stage = 'training_run' AND model_run_id IS NOT NULL AND trial_id IS NULL "
            "AND scope_id IS NULL AND conformance_campaign_id IS NULL) OR "
            "(stage = 'training_trial' AND model_run_id IS NOT NULL AND trial_id IS NOT NULL "
            "AND scope_id IS NULL AND conformance_campaign_id IS NULL) OR "
            "(stage IN ('splitter', 'preparation') AND dataset_version_id IS NOT NULL "
            "AND model_run_id IS NULL AND trial_id IS NULL AND scope_id IS NULL "
            "AND conformance_campaign_id IS NULL) OR "
            "(stage IN ('champion_refit', 'champion_evaluation') AND scope_id IS NOT NULL "
            "AND trial_id IS NULL AND conformance_campaign_id IS NULL) OR "
            "(stage = 'provider_conformance' AND conformance_campaign_id IS NOT NULL "
            "AND model_run_id IS NULL AND trial_id IS NULL AND scope_id IS NULL)",
            name="attempt_typed_parent",
        ),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[WorkflowStage] = mapped_column(
        SQLEnum(
            WorkflowStage,
            name="workflow_stage",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
    )
    logical_key: Mapped[str] = mapped_column(String(255), nullable=False)
    dataset_version_id: Mapped[uuid.UUID | None] = mapped_column()
    model_run_id: Mapped[uuid.UUID | None] = mapped_column()
    trial_id: Mapped[uuid.UUID | None] = mapped_column()
    scope_id: Mapped[uuid.UUID | None] = mapped_column()
    conformance_campaign_id: Mapped[uuid.UUID | None] = mapped_column()
    workload_identity: Mapped[str] = mapped_column(String(255), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    fencing_token: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    predecessor_attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("workflow_attempts.id", ondelete="RESTRICT")
    )
    predecessor_checkpoint_allowlist: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )
    status: Mapped[AttemptStatus] = mapped_column(
        SQLEnum(
            AttemptStatus,
            name="attempt_status",
            native_enum=False,
            values_callable=ENUM_VALUES,
        ),
        nullable=False,
        default=AttemptStatus.PENDING,
        server_default=AttemptStatus.PENDING.value,
    )
    ray_job_name: Mapped[str | None] = mapped_column(String(255))
    ray_cluster_name: Mapped[str | None] = mapped_column(String(255))
    ray_submission_id: Mapped[str | None] = mapped_column(String(255))
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    retry_budget: Mapped[int] = mapped_column(
        Integer, nullable=False, default=5, server_default="5"
    )
    checkpoint_uri: Mapped[str | None] = mapped_column(String(1024))
    terminal_reason: Mapped[str | None] = mapped_column(Text)
    terminal_cas_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    cleanup_state: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", server_default="pending"
    )
    replayed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))


class WorkflowCheckpoint(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "workflow_checkpoints"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "attempt_id"],
            ["workflow_attempts.project_id", "workflow_attempts.id"],
            ondelete="CASCADE",
            name="fk_checkpoint_attempt_project",
        ),
        UniqueConstraint("attempt_id", "sequence", name="uq_checkpoint_sequence"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    attempt_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    object_uri: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    event_sequence: Mapped[int] = mapped_column(Integer, nullable=False)


class WorkflowEvent(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "workflow_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "attempt_id"],
            ["workflow_attempts.project_id", "workflow_attempts.id"],
            ondelete="CASCADE",
            name="fk_event_attempt_project",
        ),
        UniqueConstraint("attempt_id", "sequence", name="uq_workflow_event_sequence"),
        UniqueConstraint("attempt_id", "event_key", name="uq_workflow_event_key"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    attempt_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_key: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(80), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=JSON_DEFAULT
    )
    result_digest: Mapped[str | None] = mapped_column(String(128))


class DigestVerification(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "digest_verifications"
    __table_args__ = (
        UniqueConstraint(
            "project_id",
            "resource_type",
            "resource_id",
            "digest_scope",
            name="uq_digest_verification_resource",
        ),
        Index("ix_digest_verification_status", "project_id", "status"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    resource_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    digest_algorithm: Mapped[str] = mapped_column(String(32), nullable=False)
    digest_scope: Mapped[str] = mapped_column(String(80), nullable=False)
    expected_digest: Mapped[str] = mapped_column(String(128), nullable=False)
    observed_digest: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeletionTombstone(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "deletion_tombstones"
    __table_args__ = (
        UniqueConstraint("project_id", "id", name="uq_deletion_tombstones_project_id_id"),
        UniqueConstraint("project_id", "resource_type", "resource_id", name="uq_tombstone"),
        Index("ix_tombstone_status", "project_id", "status", "requested_at"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    requested_by_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    resource_type: Mapped[str] = mapped_column(String(80), nullable=False)
    resource_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    legal_hold: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DeletionStage(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "deletion_stages"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "tombstone_id"],
            ["deletion_tombstones.project_id", "deletion_tombstones.id"],
            ondelete="CASCADE",
            name="fk_deletion_stage_tombstone_project",
        ),
        UniqueConstraint("tombstone_id", "resource_class", name="uq_deletion_stage_class"),
        Index("ix_deletion_stage_claim", "status", "lease_expires_at"),
    )

    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    tombstone_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    resource_class: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    terminal_reason: Mapped[str | None] = mapped_column(Text)
    replayed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
