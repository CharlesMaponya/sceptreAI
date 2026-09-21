"""Allow an explicit target-free feature contract for clustering.

Revision ID: 0010_phase3_contract_target
Revises: 0009_phase2_storage_default
"""

import sqlalchemy as sa

from alembic import op

revision = "0010_phase3_contract_target"
down_revision = "0009_phase2_storage_default"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "feature_contract_revisions",
        "target_column",
        existing_type=sa.String(length=255),
        nullable=True,
    )


def downgrade() -> None:
    op.execute(
        "UPDATE feature_contract_revisions SET target_column = '' WHERE target_column IS NULL"
    )
    op.alter_column(
        "feature_contract_revisions",
        "target_column",
        existing_type=sa.String(length=255),
        nullable=False,
    )
