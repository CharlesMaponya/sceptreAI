"""attempt object credential grants

Revision ID: 0014_attempt_object_grants
Revises: 0013_provider_dataset_import
Create Date: 2026-10-18 10:08:08.743230
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0014_attempt_object_grants"
down_revision = "0013_provider_dataset_import"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_attempt_grant_authority",
        "workflow_attempts",
        ["project_id", "id", "stage", "fencing_token"],
    )
    op.create_table(
        "workflow_attempt_credential_grants",
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("fencing_token", sa.String(length=128), nullable=False),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("pod_uid", sa.String(length=128), nullable=False),
        sa.Column("token_sha256", sa.String(length=64), nullable=False),
        sa.Column("authority_digest", sa.String(length=64), nullable=False),
        sa.Column("grant_digest", sa.String(length=64), nullable=False),
        sa.Column("method", sa.String(length=8), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("object_uri", sa.String(length=2048), nullable=False),
        sa.Column("object_version", sa.String(length=256), nullable=True),
        sa.Column("byte_limit", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="pending", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("issuance_owner", sa.Uuid(), nullable=True),
        sa.Column("issuance_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.String(length=255), nullable=True),
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
            "(method = 'GET' AND object_version IS NOT NULL AND "
            "length(object_version::text) > 0 AND "
            "object_version::text <> 'null'::text) OR (method = 'PUT' AND object_version IS "
            "NULL)",
            name=op.f("ck_workflow_attempt_credential_grants_grant_version"),
        ),
        sa.CheckConstraint(
            (
                "(stage = 'splitter' AND ((method = 'GET' AND role = 'raw') OR (method = "
                "'PUT' AND role IN ('train', 'validation', 'final_input', 'final_label', "
                "'manifest')))) OR (stage = 'preparation' AND ((method = 'GET' AND role IN "
                "('train', 'validation')) OR (method = 'PUT' AND role IN ('prepared', "
                "'profile', 'features', 'checkpoint')))) OR (stage IN ('training_run', "
                "'training_trial') AND ((method = 'GET' AND role IN ('train', 'validation',"
                " 'prepared', 'features', 'checkpoint')) OR (method = 'PUT' AND role IN "
                "('metrics', 'checkpoint', 'model', 'validation_evidence')))) OR (stage = "
                "'champion_refit' AND ((method = 'GET' AND role IN ('candidate', 'train', "
                "'validation')) OR (method = 'PUT' AND role IN ('pipeline', "
                "'refit_evidence')))) OR (stage = 'champion_evaluation' AND ((method = "
                "'GET' AND role IN ('pipeline', 'final_input', 'final_label')) OR (method ="
                " 'PUT' AND role = 'result'))) OR (stage = 'provider_conformance' AND "
                "((method = 'GET' AND role IN ('synthetic', 'parity', "
                "'conformance_evidence')) OR (method = 'PUT' AND role = "
                "'conformance_evidence')))"
            ),
            name=op.f("ck_workflow_attempt_credential_grants_grant_stage_role"),
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND issuance_owner IS NULL AND issuance_started_at IS NULL AND"
            " issued_at IS NULL AND revoked_at IS NULL) OR (status = 'issuing' AND "
            "issuance_owner IS NOT NULL AND issuance_started_at IS NOT NULL AND issued_at IS "
            "NULL AND revoked_at IS NULL) OR (status = 'issued' AND issuance_owner IS NOT NULL "
            "AND issuance_started_at IS NOT NULL AND issued_at IS NOT NULL AND revoked_at IS "
            "NULL) OR (status IN ('failed', 'unknown') AND issuance_owner IS NOT NULL AND "
            "issuance_started_at IS NOT NULL AND issued_at IS NULL AND revoked_at IS NULL) OR "
            "(status = 'expired' AND revoked_at IS NULL) OR (status = 'revoked' AND revoked_at "
            "IS NOT NULL)",
            name=op.f("ck_workflow_attempt_credential_grants_grant_lifecycle"),
        ),
        sa.CheckConstraint(
            "grant_digest::text ~ '^[0-9a-f]{64}$'::text AND authority_digest::text ~ "
            "'^[0-9a-f]{64}$'::text AND content_sha256::text ~ '^[0-9a-f]{64}$'::text AND "
            "token_sha256::text ~ '^[0-9a-f]{64}$'::text",
            name=op.f("ck_workflow_attempt_credential_grants_grant_digests"),
        ),
        sa.CheckConstraint(
            "method IN ('GET', 'PUT')",
            name=op.f("ck_workflow_attempt_credential_grants_grant_method"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'issuing', 'issued', 'failed', 'unknown', 'expired', 'revoked')",
            name=op.f("ck_workflow_attempt_credential_grants_grant_status"),
        ),
        sa.CheckConstraint(
            "byte_limit >= 0", name=op.f("ck_workflow_attempt_credential_grants_grant_byte_limit")
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + '00:15:00'::interval",
            name=op.f("ck_workflow_attempt_credential_grants_grant_expiry"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "attempt_id", "stage", "fencing_token"],
            [
                "workflow_attempts.project_id",
                "workflow_attempts.id",
                "workflow_attempts.stage",
                "workflow_attempts.fencing_token",
            ],
            name="fk_grant_attempt_authority",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_attempt_credential_grants")),
        sa.UniqueConstraint("attempt_id", "request_id", name="uq_grant_attempt_request"),
        sa.UniqueConstraint("project_id", "id", name="uq_grant_project_id_id"),
    )
    op.create_index(
        "ix_grant_attempt_expiry",
        "workflow_attempt_credential_grants",
        ["attempt_id", "expires_at"],
        unique=False,
    )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        sa.text("LOCK TABLE workflow_attempt_credential_grants IN ACCESS EXCLUSIVE MODE")
    )
    if connection.scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM workflow_attempt_credential_grants)")
    ):
        raise RuntimeError("Cannot discard attempt credential grant evidence")
    op.drop_index("ix_grant_attempt_expiry", table_name="workflow_attempt_credential_grants")
    op.drop_table("workflow_attempt_credential_grants")
    op.drop_constraint("uq_attempt_grant_authority", "workflow_attempts", type_="unique")
