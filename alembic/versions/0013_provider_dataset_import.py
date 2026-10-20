"""Expand dataset source identity and protected signed provider-import provenance.

Revision ID: 0013_provider_dataset_import
Revises: 0012_final_test_revision_pair
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0013_provider_dataset_import"
down_revision = "0012_final_test_revision_pair"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dataset_versions",
        sa.Column("source_kind", sa.String(length=32), server_default="raw", nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_dataset_versions_source_kind"),
        "dataset_versions",
        "source_kind IN ('raw', 'provider_distribution')",
    )
    op.create_table(
        "dataset_provider_imports",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_version_id", sa.Uuid(), nullable=False),
        sa.Column("command_id", sa.Uuid(), nullable=False),
        sa.Column("local_split_revision_id", sa.Uuid(), nullable=True),
        sa.Column("prepared_artifact_id", sa.Uuid(), nullable=True),
        sa.Column("allocation_id", sa.Uuid(), nullable=False),
        sa.Column("authority_scope_id", sa.Uuid(), nullable=False),
        sa.Column("project_reference", sa.String(length=255), nullable=False),
        sa.Column("fixture_revision", sa.String(length=128), nullable=False),
        sa.Column("split_revision", sa.String(length=128), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("canonical_provider", sa.String(length=32), nullable=False),
        sa.Column("mode", sa.String(length=32), server_default="validation_only", nullable=False),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("manifest_digest", sa.String(length=128), nullable=False),
        sa.Column("authority_key_digest", sa.String(length=128), nullable=False),
        sa.Column("distribution_receipt", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "verification",
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
        sa.CheckConstraint(
            "(status = 'verified' AND local_split_revision_id IS NOT NULL "
            "AND prepared_artifact_id IS NOT NULL) OR "
            "(status::text <> 'verified'::text AND local_split_revision_id IS NULL "
            "AND prepared_artifact_id IS NULL)",
            name=op.f("ck_dataset_provider_imports_verified_lineage"),
        ),
        sa.CheckConstraint(
            "mode = 'validation_only'", name=op.f("ck_dataset_provider_imports_mode")
        ),
        sa.CheckConstraint(
            "provider IN ('aws', 'gcp', 'azure') "
            "AND canonical_provider IN ('aws', 'gcp', 'azure') "
            "AND provider::text <> canonical_provider::text",
            name=op.f("ck_dataset_provider_imports_provider"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'verified', 'failed')",
            name=op.f("ck_dataset_provider_imports_status"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            name="fk_provider_import_command_project",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "dataset_version_id"],
            ["dataset_versions.project_id", "dataset_versions.id"],
            name="fk_provider_import_dataset_project",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "local_split_revision_id"],
            ["dataset_split_revisions.project_id", "dataset_split_revisions.id"],
            name="fk_provider_import_split_project",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "prepared_artifact_id"],
            ["prepared_artifacts.project_id", "prepared_artifacts.id"],
            name="fk_provider_import_artifact_project",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["projects.id"],
            name=op.f("fk_dataset_provider_imports_project_id_projects"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dataset_provider_imports")),
        sa.UniqueConstraint(
            "allocation_id", "provider", name="uq_provider_import_allocation_provider"
        ),
        sa.UniqueConstraint("dataset_version_id", name="uq_provider_import_dataset_version"),
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM dataset_provider_imports) OR "
            "EXISTS (SELECT 1 FROM dataset_versions WHERE source_kind <> 'raw')"
        )
    ):
        raise RuntimeError("Cannot discard provider dataset source provenance")
    op.drop_table("dataset_provider_imports")
    op.drop_constraint(op.f("ck_dataset_versions_source_kind"), "dataset_versions", type_="check")
    op.drop_column("dataset_versions", "source_kind")
