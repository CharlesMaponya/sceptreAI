"""make the provider-neutral storage identity the durable default

Revision ID: 0009_phase2_storage_default
Revises: 0008_phase2_cloud_ingestion
Create Date: 2026-08-20 18:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0009_phase2_storage_default"
down_revision = "0008_phase2_cloud_ingestion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "dataset_versions",
        "object_store_type",
        existing_type=sa.String(length=32),
        existing_nullable=False,
        existing_server_default="minio",
        server_default="s3_compatible",
    )


def downgrade() -> None:
    op.alter_column(
        "dataset_versions",
        "object_store_type",
        existing_type=sa.String(length=32),
        existing_nullable=False,
        existing_server_default="s3_compatible",
        server_default="minio",
    )
