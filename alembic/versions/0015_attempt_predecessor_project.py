"""Keep attempt predecessor lineage within its owning project.

Revision ID: 0015_attempt_predecessor_project
Revises: 0014_attempt_object_grants
"""

import sqlalchemy as sa

from alembic import op

revision = "0015_attempt_predecessor_project"
down_revision = "0014_attempt_object_grants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_attempt_predecessor_project",
        "workflow_attempts",
        ["predecessor_attempt_id", "project_id"],
        postgresql_where=sa.text("predecessor_attempt_id IS NOT NULL"),
    )
    # Validate existing lineage; never rewrite a cross-project predecessor.
    op.create_foreign_key(
        "fk_attempt_predecessor_project",
        "workflow_attempts",
        "workflow_attempts",
        ["project_id", "predecessor_attempt_id"],
        ["project_id", "id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint("fk_attempt_predecessor_project", "workflow_attempts", type_="foreignkey")
    op.drop_index("ix_attempt_predecessor_project", table_name="workflow_attempts")
