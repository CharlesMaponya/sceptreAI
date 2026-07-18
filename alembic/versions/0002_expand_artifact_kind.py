"""Expand the artifact kind column for governance reports.

Revision ID: 0002_expand_artifact_kind
Revises: 0001_initial
Create Date: 2026-07-18
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0002_expand_artifact_kind"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


artifact_kind = sa.Enum(
    "DATASET_PROFILE",
    "DIAGNOSTIC_PLOT",
    "MODEL_OBJECT",
    "SHAP_VALUES",
    "DRIFT_REPORT",
    "LOG_BUNDLE",
    "DEPLOYMENT_IMAGE",
    "GOVERNANCE_REPORT",
    name="artifact_kind",
    native_enum=False,
)


def upgrade() -> None:
    op.alter_column(
        "run_artifacts",
        "kind",
        existing_type=sa.String(length=16),
        type_=artifact_kind,
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "run_artifacts",
        "kind",
        existing_type=artifact_kind,
        type_=sa.String(length=16),
        existing_nullable=False,
    )
