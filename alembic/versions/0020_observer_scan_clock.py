"""Track observer scans independently from worker progress and startup clocks."""

import sqlalchemy as sa

from alembic import op

revision = "0020_observer_scan_clock"
down_revision = "0019_deletion_stage_custody"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_attempts",
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("workflow_attempts", "last_observed_at")
