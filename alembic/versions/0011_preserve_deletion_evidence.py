"""Retain deletion evidence after the live project is removed.

Revision ID: 0011_preserve_deletion_evidence
Revises: 0010_phase3_contract_target
"""

import sqlalchemy as sa

from alembic import op

revision = "0011_preserve_deletion_evidence"
down_revision = "0010_phase3_contract_target"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("deletion_stages", "deletion_tombstones"):
        op.drop_constraint(op.f(f"fk_{table}_project_id_projects"), table, type_="foreignkey")


def downgrade() -> None:
    connection = op.get_bind()
    for table in ("deletion_tombstones", "deletion_stages"):
        if connection.scalar(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {table} d "
                "WHERE NOT EXISTS (SELECT 1 FROM projects p WHERE p.id = d.project_id))"
            )
        ):
            raise RuntimeError(
                "Cannot downgrade while deletion evidence references removed projects"
            )
    for table in ("deletion_tombstones", "deletion_stages"):
        op.create_foreign_key(
            op.f(f"fk_{table}_project_id_projects"),
            table,
            "projects",
            ["project_id"],
            ["id"],
            ondelete="CASCADE",
        )
