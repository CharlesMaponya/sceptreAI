"""Bind upload result references to their project without discarding upload history."""

import sqlalchemy as sa

from alembic import op

revision = "0017_upload_session_project"
down_revision = "0016_trial_parent_integrity"
branch_labels = None
depends_on = None

LINKS = (
    ("dataset_id", "datasets", "fk_upload_session_dataset_project", "ix_upload_dataset_project"),
    (
        "dataset_version_id",
        "dataset_versions",
        "fk_upload_session_version_project",
        "ix_upload_version_project",
    ),
)


def upgrade() -> None:
    # Existing single-column SET NULL links retain sessions when results are deleted.
    for column, parent, constraint, index in LINKS:
        op.create_index(
            index,
            "dataset_upload_sessions",
            [column, "project_id"],
            postgresql_where=sa.text(f"{column} IS NOT NULL"),
        )
        op.create_foreign_key(
            constraint,
            "dataset_upload_sessions",
            parent,
            ["project_id", column],
            ["project_id", "id"],
        )


def downgrade() -> None:
    for _column, _parent, constraint, index in reversed(LINKS):
        op.drop_constraint(constraint, "dataset_upload_sessions", type_="foreignkey")
        op.drop_index(index, table_name="dataset_upload_sessions")
