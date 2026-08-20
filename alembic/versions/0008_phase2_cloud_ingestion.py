"""add Phase 2 provider-neutral resumable ingestion state

Revision ID: 0008_phase2_cloud_ingestion
Revises: 0007_phase1_attempt_lineage
Create Date: 2026-08-20 13:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0008_phase2_cloud_ingestion"
down_revision = "0007_phase1_attempt_lineage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    json_default = sa.text("'{}'::jsonb")
    list_default = sa.text("'[]'::jsonb")
    columns = [
        sa.Column("upload_kind", sa.String(32), server_default="dataset", nullable=False),
        sa.Column(
            "provider_driver", sa.String(32), server_default="s3_compatible", nullable=False
        ),
        sa.Column("protocol", sa.String(32), server_default="multipart", nullable=False),
        sa.Column(
            "provider_state", postgresql.JSONB(), server_default=json_default, nullable=False
        ),
        sa.Column(
            "transfer_receipts", postgresql.JSONB(), server_default=list_default, nullable=False
        ),
        sa.Column(
            "instruction_state", postgresql.JSONB(), server_default=json_default, nullable=False
        ),
        sa.Column("confirmed_bytes", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column(
            "content_type",
            sa.String(255),
            server_default="application/octet-stream",
            nullable=False,
        ),
        sa.Column("sensitivity", sa.String(32), server_default="internal", nullable=False),
        sa.Column("data_region", sa.String(64), server_default="local", nullable=False),
        sa.Column("retention_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("legal_hold", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column("provider_checksum", sa.String(256), nullable=True),
        sa.Column("completed_object_uri", sa.String(1024), nullable=True),
        sa.Column("checksum_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_progress_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("aborted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("quarantine_delete_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scanner_status", sa.String(32), server_default="pending", nullable=False),
        sa.Column("scanner_name", sa.String(120), nullable=True),
        sa.Column("scanner_version", sa.String(120), nullable=True),
        sa.Column("scanner_signature_version", sa.String(255), nullable=True),
        sa.Column(
            "scanner_evidence", postgresql.JSONB(), server_default=json_default, nullable=False
        ),
        sa.Column("content_policy_revision", sa.String(128), nullable=True),
        sa.Column(
            "target_metadata", postgresql.JSONB(), server_default=json_default, nullable=False
        ),
    ]
    for column in columns:
        op.add_column("dataset_upload_sessions", column)

    op.execute(
        "UPDATE dataset_upload_sessions SET provider_driver = 's3_compatible', "
        "protocol = 'multipart', confirmed_bytes = CASE WHEN status = 'completed' "
        "THEN byte_size ELSE 0 END, content_policy_revision = 'legacy-v1'"
    )
    op.create_check_constraint(
        "upload_confirmed_byte_bounds",
        "dataset_upload_sessions",
        "confirmed_bytes >= 0 AND confirmed_bytes <= byte_size",
    )
    op.create_check_constraint(
        "upload_part_bounds",
        "dataset_upload_sessions",
        "part_size > 0 AND total_parts > 0 AND byte_size > 0",
    )
    op.create_index(
        "ix_upload_sessions_reconcile",
        "dataset_upload_sessions",
        ["status", "last_progress_at", "expires_at"],
    )
    op.create_index(
        "ix_upload_sessions_project_storage",
        "dataset_upload_sessions",
        ["project_id", "status", "byte_size"],
    )

    version_columns = [
        sa.Column(
            "content_hash_algorithm", sa.String(32), server_default="sha256", nullable=False
        ),
        sa.Column(
            "content_hash_scope", sa.String(64), server_default="byte_stream", nullable=False
        ),
        sa.Column(
            "content_hash_verification_status",
            sa.String(32),
            server_default="verified_legacy",
            nullable=False,
        ),
        sa.Column("content_hash_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("data_region", sa.String(64), server_default="legacy", nullable=False),
        sa.Column("sensitivity", sa.String(32), server_default="internal", nullable=False),
        sa.Column("retention_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("legal_hold", sa.Boolean(), server_default=sa.false(), nullable=False),
    ]
    for column in version_columns:
        op.add_column("dataset_versions", column)
    op.alter_column(
        "dataset_versions",
        "object_store_type",
        existing_type=sa.String(length=5),
        type_=sa.String(length=32),
        existing_nullable=False,
        existing_server_default="minio",
    )
    op.execute(
        "UPDATE dataset_versions SET object_store_type = CASE object_store_type "
        "WHEN 'minio' THEN 's3_compatible' WHEN 's3' THEN 'aws_s3' "
        "WHEN 'azure' THEN 'azure_blob' ELSE object_store_type END"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE dataset_versions SET object_store_type = CASE object_store_type "
        "WHEN 's3_compatible' THEN 'minio' WHEN 'aws_s3' THEN 's3' "
        "WHEN 'azure_blob' THEN 'azure' ELSE object_store_type END"
    )
    op.alter_column(
        "dataset_versions",
        "object_store_type",
        existing_type=sa.String(length=32),
        type_=sa.String(length=5),
        existing_nullable=False,
        existing_server_default="minio",
    )
    for name in [
        "legal_hold",
        "retention_until",
        "sensitivity",
        "data_region",
        "content_hash_verified_at",
        "content_hash_verification_status",
        "content_hash_scope",
        "content_hash_algorithm",
    ]:
        op.drop_column("dataset_versions", name)

    op.drop_index("ix_upload_sessions_project_storage", table_name="dataset_upload_sessions")
    op.drop_index("ix_upload_sessions_reconcile", table_name="dataset_upload_sessions")
    op.drop_constraint(
        op.f("ck_dataset_upload_sessions_upload_part_bounds"),
        "dataset_upload_sessions",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_dataset_upload_sessions_upload_confirmed_byte_bounds"),
        "dataset_upload_sessions",
        type_="check",
    )
    for name in [
        "target_metadata",
        "content_policy_revision",
        "scanner_evidence",
        "scanner_signature_version",
        "scanner_version",
        "scanner_name",
        "scanner_status",
        "quarantine_delete_after",
        "aborted_at",
        "last_progress_at",
        "checksum_verified_at",
        "completed_object_uri",
        "provider_checksum",
        "legal_hold",
        "retention_until",
        "data_region",
        "sensitivity",
        "content_type",
        "confirmed_bytes",
        "instruction_state",
        "transfer_receipts",
        "provider_state",
        "protocol",
        "provider_driver",
        "upload_kind",
    ]:
        op.drop_column("dataset_upload_sessions", name)
