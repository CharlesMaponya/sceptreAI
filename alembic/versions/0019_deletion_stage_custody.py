"""Keep original native deletion custody after the live project is erased."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0019_deletion_stage_custody"
down_revision = "0018_attempt_predecessor_lineage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "deletion_stages",
        sa.Column(
            "custody", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
    )
    op.create_check_constraint(
        op.f("ck_deletion_stages_deletion_stage_custody_object"),
        "deletion_stages",
        "jsonb_typeof(custody) = 'object'::text",
    )


def downgrade() -> None:
    op.execute("LOCK TABLE deletion_stages IN ACCESS EXCLUSIVE MODE")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM deletion_stages WHERE custody <> '{}'::jsonb) THEN
            RAISE EXCEPTION 'Deletion custody must be preserved before schema rollback'
              USING ERRCODE = '23514', CONSTRAINT = 'deletion_stage_custody_preservation';
          END IF;
        END $$
        """
    )
    op.drop_constraint(
        op.f("ck_deletion_stages_deletion_stage_custody_object"),
        "deletion_stages",
        type_="check",
    )
    op.drop_column("deletion_stages", "custody")
