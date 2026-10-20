"""Expand final-test allocations with globally unique fixture/split references.

Revision ID: 0012_final_test_revision_pair
Revises: 0011_preserve_deletion_evidence
"""

import sqlalchemy as sa

from alembic import op

revision = "0012_final_test_revision_pair"
down_revision = "0011_preserve_deletion_evidence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("final_test_allocations", sa.Column("fixture_revision", sa.String(128)))
    op.add_column("final_test_allocations", sa.Column("split_revision", sa.String(128)))
    op.create_unique_constraint(
        "uq_final_test_revision_pair",
        "final_test_allocations",
        ["fixture_revision", "split_revision"],
    )
    op.create_check_constraint(
        op.f("ck_final_test_allocations_allocation_revision_pair"),
        "final_test_allocations",
        "(fixture_revision IS NULL AND split_revision IS NULL) OR "
        "(fixture_revision IS NOT NULL AND split_revision IS NOT NULL "
        "AND fixture_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text "
        "AND split_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text)",
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM final_test_allocations "
            "WHERE fixture_revision IS NOT NULL)"
        )
    ):
        raise RuntimeError("Cannot discard bound final-test revision evidence")
    op.drop_constraint(
        op.f("ck_final_test_allocations_allocation_revision_pair"),
        "final_test_allocations",
        type_="check",
    )
    op.drop_constraint("uq_final_test_revision_pair", "final_test_allocations", type_="unique")
    op.drop_column("final_test_allocations", "split_revision")
    op.drop_column("final_test_allocations", "fixture_revision")
