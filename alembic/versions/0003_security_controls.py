"""Add durable activity audit and distributed rate-limit state.

Revision ID: 0003_security_controls
Revises: 0002_expand_artifact_kind
Create Date: 2026-07-31
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0003_security_controls"
down_revision = "0002_expand_artifact_kind"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("sso_issuer", sa.String(length=512), nullable=True))
    op.add_column("users", sa.Column("sso_subject", sa.String(length=512), nullable=True))
    op.create_unique_constraint(
        op.f("uq_users_sso_subject"),
        "users",
        ["sso_subject"],
    )

    op.create_table(
        "audit_events",
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("request_id", sa.String(length=64), nullable=False),
        sa.Column("actor_user_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(length=96), nullable=False),
        sa.Column("method", sa.String(length=12), nullable=False),
        sa.Column("route", sa.String(length=512), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("client_hash", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=256), nullable=True),
        sa.Column(
            "details",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_events")),
    )
    op.create_index(
        "ix_audit_events_actor_occurred",
        "audit_events",
        ["actor_user_id", "occurred_at"],
    )
    op.create_index(
        op.f("ix_audit_events_actor_user_id"),
        "audit_events",
        ["actor_user_id"],
    )
    op.create_index(
        "ix_audit_events_event_occurred",
        "audit_events",
        ["event_type", "occurred_at"],
    )
    op.create_index(
        "ix_audit_events_occurred_at",
        "audit_events",
        ["occurred_at"],
    )

    op.create_table(
        "rate_limit_buckets",
        sa.Column("scope", sa.String(length=64), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("hit_count", sa.Integer(), server_default="1", nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rate_limit_buckets")),
        sa.UniqueConstraint(
            "scope",
            "key_hash",
            "window_started_at",
            name="uq_rate_limit_bucket_window",
        ),
    )
    op.create_index(
        "ix_rate_limit_buckets_window",
        "rate_limit_buckets",
        ["window_started_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_rate_limit_buckets_window", table_name="rate_limit_buckets")
    op.drop_table("rate_limit_buckets")
    op.drop_index("ix_audit_events_occurred_at", table_name="audit_events")
    op.drop_index("ix_audit_events_event_occurred", table_name="audit_events")
    op.drop_index(op.f("ix_audit_events_actor_user_id"), table_name="audit_events")
    op.drop_index("ix_audit_events_actor_occurred", table_name="audit_events")
    op.drop_table("audit_events")
    op.drop_constraint(op.f("uq_users_sso_subject"), "users", type_="unique")
    op.drop_column("users", "sso_subject")
    op.drop_column("users", "sso_issuer")
