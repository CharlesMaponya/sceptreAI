"""Add resumable direct-to-object-storage dataset uploads.

Revision ID: 0004_resumable_dataset_uploads
Revises: 0003_security_controls
Create Date: 2026-08-01
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0004_resumable_dataset_uploads"
down_revision = "0003_security_controls"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_upload_sessions",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_id", sa.Uuid(), nullable=False),
        sa.Column("dataset_name", sa.String(length=220), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "tags",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("original_filename", sa.String(length=512), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("part_size", sa.BigInteger(), nullable=False),
        sa.Column("total_parts", sa.Integer(), nullable=False),
        sa.Column("object_key", sa.String(length=1024), nullable=False),
        sa.Column("multipart_upload_id", sa.String(length=1024), nullable=False),
        sa.Column("resume_key", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), server_default="pending", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dataset_id", sa.Uuid(), nullable=True),
        sa.Column("dataset_version_id", sa.Uuid(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dataset_id"], ["datasets.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["dataset_version_id"], ["dataset_versions.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["project_id"], ["projects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dataset_upload_sessions")),
        sa.UniqueConstraint("object_key", name=op.f("uq_dataset_upload_sessions_object_key")),
    )
    op.create_index(
        "ix_dataset_upload_sessions_expires_status",
        "dataset_upload_sessions",
        ["expires_at", "status"],
    )
    op.create_index(
        "ix_dataset_upload_sessions_owner_status",
        "dataset_upload_sessions",
        ["created_by_id", "status"],
    )
    op.create_index(
        "ix_dataset_upload_sessions_project_created",
        "dataset_upload_sessions",
        ["project_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_dataset_upload_sessions_project_created",
        table_name="dataset_upload_sessions",
    )
    op.drop_index(
        "ix_dataset_upload_sessions_owner_status",
        table_name="dataset_upload_sessions",
    )
    op.drop_index(
        "ix_dataset_upload_sessions_expires_status",
        table_name="dataset_upload_sessions",
    )
    op.drop_table("dataset_upload_sessions")
