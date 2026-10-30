"""Rotate legacy analysis scans without changing run or startup timestamps."""

import sqlalchemy as sa

from alembic import op

revision = "0021_legacy_observer_clock"
down_revision = "0020_observer_scan_clock"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "model_runs",
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("model_runs", "last_observed_at")
