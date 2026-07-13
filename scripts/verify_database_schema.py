from __future__ import annotations

from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.db.base import Base
from automl_api.db.session import get_engine
from sqlalchemy import inspect, text


def main() -> None:
    engine = get_engine()
    expected_tables = {table.name for table in Base.metadata.sorted_tables}
    with engine.connect() as connection:
        actual_tables = set(inspect(connection).get_table_names())
        database_revisions = set(
            connection.execute(text("SELECT version_num FROM alembic_version")).scalars()
        )

    missing_tables = sorted(expected_tables - actual_tables)
    if missing_tables:
        raise RuntimeError(
            "Alembic reached the database but did not create application tables: "
            + ", ".join(missing_tables)
        )

    migration_heads = set(ScriptDirectory.from_config(Config("alembic.ini")).get_heads())
    if database_revisions != migration_heads:
        raise RuntimeError(
            "Database migration revisions do not match the repository heads: "
            f"database={sorted(database_revisions)}, heads={sorted(migration_heads)}"
        )

    print(
        f"Database schema is current: {len(actual_tables)} tables, "
        f"revision(s) {', '.join(sorted(database_revisions))}."
    )


if __name__ == "__main__":
    main()
