"""Create the initial application schema.

Revision ID: 0001_initial
Revises:
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("full_name", sa.String(length=200), nullable=True),
        sa.Column("password_hash", sa.Text(), nullable=True),
        sa.Column(
            "auth_provider",
            sa.Enum("SIMPLE", "SSO", name="auth_provider", native_enum=False),
            server_default="simple",
            nullable=False,
        ),
        sa.Column(
            "global_role",
            sa.Enum("MEMBER", "ADMIN", name="global_role", native_enum=False),
            server_default="member",
            nullable=False,
        ),
        sa.Column(
            "is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False
        ),
        sa.Column(
            "is_verified", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("token_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "preferences",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
    )
    op.create_index(op.f("ix_users_email"), "users", ["email"], unique=True)

    op.create_table(
        "password_reset_tokens",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_password_reset_tokens_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_password_reset_tokens")),
        sa.UniqueConstraint(
            "token_hash", name=op.f("uq_password_reset_tokens_token_hash")
        ),
    )
    op.create_index(
        "ix_password_reset_tokens_user_expires",
        "password_reset_tokens",
        ["user_id", "expires_at"],
        unique=False,
    )

    op.create_table(
        "projects",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("ACTIVE", "ARCHIVED", name="project_status", native_enum=False),
            server_default="active",
            nullable=False,
        ),
        sa.Column("object_prefix", sa.String(length=512), nullable=True),
        sa.Column(
            "settings",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_projects_created_by_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_projects_owner_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_projects")),
        sa.UniqueConstraint(
            "object_prefix", name=op.f("uq_projects_object_prefix")
        ),
    )
    op.create_index(
        "ix_projects_created_by", "projects", ["created_by_id"], unique=False
    )
    op.create_index(
        "ix_projects_owner_status",
        "projects",
        ["owner_id", "status"],
        unique=False,
    )

    op.create_table(
        "refresh_tokens",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("family_id", sa.Uuid(), nullable=False),
        sa.Column(
            "issued_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_refresh_tokens_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_refresh_tokens")),
        sa.UniqueConstraint(
            "token_hash", name=op.f("uq_refresh_tokens_token_hash")
        ),
    )
    op.create_index(
        "ix_refresh_tokens_family", "refresh_tokens", ["family_id"], unique=False
    )
    op.create_index(
        "ix_refresh_tokens_user_expires",
        "refresh_tokens",
        ["user_id", "expires_at"],
        unique=False,
    )

    op.create_table(
        "datasets",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=220), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "latest_version_number", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_datasets_created_by_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_datasets_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_datasets")),
        sa.UniqueConstraint("project_id", "id", name="uq_datasets_project_id_id"),
    )
    op.create_index(
        "ix_datasets_project_created_at",
        "datasets",
        ["project_id", "created_at"],
        unique=False,
    )

    op.create_table(
        "project_memberships",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("invited_by_id", sa.Uuid(), nullable=True),
        sa.Column(
            "role",
            sa.Enum(
                "OWNER",
                "ADMIN",
                "EDITOR",
                "VIEWER",
                name="project_role",
                native_enum=False,
            ),
            server_default="viewer",
            nullable=False,
        ),
        sa.Column(
            "permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["invited_by_id"],
            ["users.id"],
            name=op.f("fk_project_memberships_invited_by_id_users"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_project_memberships_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_project_memberships_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_project_memberships")),
        sa.UniqueConstraint(
            "project_id", "user_id", name="uq_project_memberships_project_user"
        ),
    )
    op.create_index(
        "ix_project_memberships_user_role",
        "project_memberships",
        ["user_id", "role"],
        unique=False,
    )

    op.create_table(
        "project_share_links",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column(
            "role",
            sa.Enum(
                "OWNER",
                "ADMIN",
                "EDITOR",
                "VIEWER",
                name="project_share_role",
                native_enum=False,
            ),
            server_default="viewer",
            nullable=False,
        ),
        sa.Column(
            "permissions",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("max_uses", sa.Integer(), server_default="1", nullable=False),
        sa.Column("used_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_project_share_links_created_by_id_users"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_project_share_links_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_project_share_links")),
        sa.UniqueConstraint(
            "token_hash", name=op.f("uq_project_share_links_token_hash")
        ),
    )
    op.create_index(
        "ix_project_share_links_project_expires",
        "project_share_links",
        ["project_id", "expires_at"],
        unique=False,
    )
    op.create_index(
        "ix_project_share_links_token_hash",
        "project_share_links",
        ["token_hash"],
        unique=False,
    )

    op.create_table(
        "dataset_versions",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "UPLOADED",
                "PROFILING",
                "READY",
                "FAILED",
                "ARCHIVED",
                name="dataset_status",
                native_enum=False,
            ),
            server_default="uploaded",
            nullable=False,
        ),
        sa.Column(
            "format",
            sa.Enum(
                "CSV",
                "PARQUET",
                "EXCEL",
                "JSON",
                name="dataset_format",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "object_store_type",
            sa.Enum(
                "MINIO",
                "S3",
                "AZURE",
                "GCS",
                name="object_store_type",
                native_enum=False,
            ),
            server_default="minio",
            nullable=False,
        ),
        sa.Column("object_uri", sa.String(length=1024), nullable=False),
        sa.Column("original_filename", sa.String(length=512), nullable=True),
        sa.Column("content_hash", sa.String(length=128), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("column_count", sa.Integer(), nullable=True),
        sa.Column(
            "schema_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "inferred_types_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "quality_report_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("profile_artifact_uri", sa.String(length=1024), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_dataset_versions_created_by_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "dataset_id"],
            ["datasets.project_id", "datasets.id"],
            name="fk_dataset_versions_dataset_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_dataset_versions_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dataset_versions")),
        sa.UniqueConstraint(
            "dataset_id",
            "version_number",
            name="uq_dataset_versions_dataset_version",
        ),
        sa.UniqueConstraint(
            "project_id", "id", name="uq_dataset_versions_project_id_id"
        ),
    )
    op.create_index(
        "ix_dataset_versions_content_hash",
        "dataset_versions",
        ["content_hash"],
        unique=False,
    )
    op.create_index(
        "ix_dataset_versions_project_dataset",
        "dataset_versions",
        ["project_id", "dataset_id"],
        unique=False,
    )

    op.create_table(
        "model_runs",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_version_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column(
            "run_kind",
            sa.Enum(
                "TRAINING",
                "VALIDATION",
                "EXPLAINABILITY",
                "DRIFT",
                "DEPLOYMENT",
                name="run_kind",
                native_enum=False,
            ),
            server_default="training",
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "QUEUED",
                "PRECHECK_RUNNING",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
                "PREEMPTED",
                name="run_status",
                native_enum=False,
            ),
            server_default="queued",
            nullable=False,
        ),
        sa.Column(
            "task_type",
            sa.Enum(
                "UNSPECIFIED",
                "REGRESSION",
                "CLASSIFICATION",
                "TIME_SERIES",
                "CLUSTERING",
                name="task_type",
                native_enum=False,
            ),
            server_default="unspecified",
            nullable=False,
        ),
        sa.Column("target_column", sa.String(length=255), nullable=True),
        sa.Column("run_name", sa.String(length=255), nullable=True),
        sa.Column("pipeline_name", sa.String(length=255), nullable=True),
        sa.Column("mlflow_run_id", sa.String(length=255), nullable=True),
        sa.Column("zenml_run_id", sa.String(length=255), nullable=True),
        sa.Column("k8s_namespace", sa.String(length=255), nullable=True),
        sa.Column("k8s_job_name", sa.String(length=255), nullable=True),
        sa.Column(
            "gpu_requested",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("cpu_request_cores", sa.Float(), nullable=True),
        sa.Column("memory_request_mb", sa.Integer(), nullable=True),
        sa.Column("cpu_limit_cores", sa.Float(), nullable=True),
        sa.Column("memory_limit_mb", sa.Integer(), nullable=True),
        sa.Column("estimated_core_hours", sa.Float(), nullable=True),
        sa.Column(
            "params",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("failure_code", sa.String(length=120), nullable=True),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column("plain_english_failure", sa.Text(), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_model_runs_created_by_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            name="fk_model_runs_dataset_version_project",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_model_runs_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_runs")),
        sa.UniqueConstraint(
            "project_id", "id", name="uq_model_runs_project_id_id"
        ),
    )
    op.create_index(
        "ix_model_runs_mlflow_run_id",
        "model_runs",
        ["mlflow_run_id"],
        unique=False,
    )
    op.create_index(
        "ix_model_runs_project_created_at",
        "model_runs",
        ["project_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_model_runs_project_status",
        "model_runs",
        ["project_id", "status"],
        unique=False,
    )

    op.create_table(
        "profiling_jobs",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_version_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("target_column", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), server_default="queued", nullable=False),
        sa.Column(
            "current_stage",
            sa.String(length=32),
            server_default="overview",
            nullable=False,
        ),
        sa.Column("progress", sa.Float(), server_default="0", nullable=False),
        sa.Column("total_columns", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "completed_columns", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column(
            "overview_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "feature_profiles_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "relationships_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "preparation_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "warnings_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "artifact_uris_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column(
            "auto_started",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name=op.f("fk_profiling_jobs_created_by_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            name="fk_profiling_jobs_dataset_version_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_profiling_jobs_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_profiling_jobs")),
    )
    op.create_index(
        "ix_profiling_jobs_project_version",
        "profiling_jobs",
        ["project_id", "dataset_version_id"],
        unique=False,
    )
    op.create_index(
        "ix_profiling_jobs_status_updated",
        "profiling_jobs",
        ["status", "updated_at"],
        unique=False,
    )

    op.create_table(
        "metrics",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("model_run_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column(
            "kind",
            sa.Enum(
                "PERFORMANCE",
                "DATA_QUALITY",
                "DRIFT",
                "RESOURCE",
                "DIAGNOSTIC",
                name="metric_kind",
                native_enum=False,
            ),
            server_default="performance",
            nullable=False,
        ),
        sa.Column(
            "split",
            sa.Enum(
                "TRAIN",
                "VALIDATION",
                "TEST",
                "EXTERNAL",
                "PRODUCTION",
                name="metric_split",
                native_enum=False,
            ),
            server_default="validation",
            nullable=False,
        ),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column(
            "value_json",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("higher_is_better", sa.Boolean(), nullable=True),
        sa.Column("step", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "recorded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            name="fk_metrics_model_run_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_metrics_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_metrics")),
        sa.UniqueConstraint(
            "model_run_id",
            "name",
            "split",
            "step",
            name="uq_metrics_run_name_split_step",
        ),
    )
    op.create_index(
        "ix_metrics_model_run", "metrics", ["model_run_id"], unique=False
    )
    op.create_index(
        "ix_metrics_project_kind", "metrics", ["project_id", "kind"], unique=False
    )

    op.create_table(
        "run_artifacts",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("model_run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "kind",
            sa.Enum(
                "DATASET_PROFILE",
                "DIAGNOSTIC_PLOT",
                "MODEL_OBJECT",
                "SHAP_VALUES",
                "DRIFT_REPORT",
                "LOG_BUNDLE",
                "DEPLOYMENT_IMAGE",
                name="artifact_kind",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=220), nullable=False),
        sa.Column("object_uri", sa.String(length=1024), nullable=False),
        sa.Column("content_hash", sa.String(length=128), nullable=True),
        sa.Column("byte_size", sa.Integer(), nullable=True),
        sa.Column(
            "artifact_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            name="fk_run_artifacts_model_run_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_run_artifacts_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_run_artifacts")),
        sa.UniqueConstraint(
            "project_id", "id", name="uq_run_artifacts_project_id_id"
        ),
    )
    op.create_index(
        "ix_run_artifacts_project_kind",
        "run_artifacts",
        ["project_id", "kind"],
        unique=False,
    )

    op.create_table(
        "model_registry_entries",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("model_run_id", sa.Uuid(), nullable=False),
        sa.Column("model_artifact_id", sa.Uuid(), nullable=False),
        sa.Column(
            "stage",
            sa.Enum(
                "CANDIDATE",
                "STAGING",
                "PRODUCTION",
                "ARCHIVED",
                "REJECTED",
                name="model_stage",
                native_enum=False,
            ),
            server_default="candidate",
            nullable=False,
        ),
        sa.Column("model_name", sa.String(length=220), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("feature_space_hash", sa.String(length=128), nullable=False),
        sa.Column("champion_metric_name", sa.String(length=160), nullable=True),
        sa.Column("champion_metric_value", sa.Float(), nullable=True),
        sa.Column("promoted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "registry_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "model_artifact_id"],
            ["run_artifacts.project_id", "run_artifacts.id"],
            name="fk_model_registry_entries_artifact_project",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "model_run_id"],
            ["model_runs.project_id", "model_runs.id"],
            name="fk_model_registry_entries_model_run_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_model_registry_entries_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_model_registry_entries")),
    )
    op.create_index(
        "ix_model_registry_project_feature_hash",
        "model_registry_entries",
        ["project_id", "feature_space_hash"],
        unique=False,
    )
    op.create_index(
        "ix_model_registry_project_stage",
        "model_registry_entries",
        ["project_id", "stage"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_model_registry_project_stage", table_name="model_registry_entries"
    )
    op.drop_index(
        "ix_model_registry_project_feature_hash", table_name="model_registry_entries"
    )
    op.drop_table("model_registry_entries")
    op.drop_index("ix_run_artifacts_project_kind", table_name="run_artifacts")
    op.drop_table("run_artifacts")
    op.drop_index("ix_metrics_project_kind", table_name="metrics")
    op.drop_index("ix_metrics_model_run", table_name="metrics")
    op.drop_table("metrics")
    op.drop_index("ix_profiling_jobs_status_updated", table_name="profiling_jobs")
    op.drop_index("ix_profiling_jobs_project_version", table_name="profiling_jobs")
    op.drop_table("profiling_jobs")
    op.drop_index("ix_model_runs_project_status", table_name="model_runs")
    op.drop_index("ix_model_runs_project_created_at", table_name="model_runs")
    op.drop_index("ix_model_runs_mlflow_run_id", table_name="model_runs")
    op.drop_table("model_runs")
    op.drop_index(
        "ix_dataset_versions_project_dataset", table_name="dataset_versions"
    )
    op.drop_index(
        "ix_dataset_versions_content_hash", table_name="dataset_versions"
    )
    op.drop_table("dataset_versions")
    op.drop_index(
        "ix_project_share_links_token_hash", table_name="project_share_links"
    )
    op.drop_index(
        "ix_project_share_links_project_expires", table_name="project_share_links"
    )
    op.drop_table("project_share_links")
    op.drop_index(
        "ix_project_memberships_user_role", table_name="project_memberships"
    )
    op.drop_table("project_memberships")
    op.drop_index("ix_datasets_project_created_at", table_name="datasets")
    op.drop_table("datasets")
    op.drop_index("ix_refresh_tokens_user_expires", table_name="refresh_tokens")
    op.drop_index("ix_refresh_tokens_family", table_name="refresh_tokens")
    op.drop_table("refresh_tokens")
    op.drop_index("ix_projects_owner_status", table_name="projects")
    op.drop_index("ix_projects_created_by", table_name="projects")
    op.drop_table("projects")
    op.drop_index(
        "ix_password_reset_tokens_user_expires", table_name="password_reset_tokens"
    )
    op.drop_table("password_reset_tokens")
    op.drop_index(op.f("ix_users_email"), table_name="users")
    op.drop_table("users")
