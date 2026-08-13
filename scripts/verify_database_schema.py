from __future__ import annotations

import re

from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.db.base import Base
from automl_api.db.session import get_engine
from sqlalchemy import CheckConstraint, inspect, text


def _normalized_type(value: object, dialect) -> str:
    rendered = str(value.compile(dialect=dialect)).lower().replace("character varying", "varchar")
    if dialect.name == "postgresql" and rendered == "float":
        return "double precision"
    return rendered


def _column_signature(column: object, dialect) -> tuple[str, bool]:
    return (_normalized_type(column.type, dialect), bool(column.nullable))


def _reflected_column_signature(column: dict[str, object], dialect) -> tuple[str, bool]:
    return (_normalized_type(column["type"], dialect), bool(column["nullable"]))


def _normalize_sql(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(str(value).lower().replace('"', "").split())
    normalized = re.sub(
        r"::(?:character varying|double precision|timestamp with time zone|[a-z_]+)(?:\[\])?",
        "",
        normalized,
    )
    if len(normalized) >= 2 and normalized.startswith("'") and normalized.endswith("'"):
        normalized = normalized[1:-1]
    return normalized


def _model_default(column, dialect) -> str | None:
    default = column.server_default
    if default is None:
        return None
    argument = default.arg
    rendered = argument.compile(dialect=dialect) if hasattr(argument, "compile") else argument
    return _normalize_sql(rendered)


def _check_signature(value: object) -> str:
    normalized = _normalize_sql(value) or ""
    # PostgreSQL reflection adds casts and grouping parentheses without changing
    # the predicate. Compare a compact canonical representation.
    normalized = re.sub(r"=\s*any\s*\(\s*array\[([^]]+)]\s*\)", r" in (\1)", normalized)
    return "".join(normalized.translate(str.maketrans("", "", "() ")).split())


def _column_set(rows: list[dict[str, object]], key: str = "column_names") -> set[tuple[str, ...]]:
    return {tuple(row.get(key) or ()) for row in rows}


def schema_differences(engine) -> list[str]:
    inspector = inspect(engine)
    actual_tables = set(inspector.get_table_names())
    expected_tables = set(Base.metadata.tables)
    differences = [f"missing table {name}" for name in sorted(expected_tables - actual_tables)]

    for table_name in sorted(expected_tables & actual_tables):
        expected = Base.metadata.tables[table_name]
        actual_columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        for column in expected.columns:
            actual = actual_columns.get(column.name)
            if actual is None:
                differences.append(f"{table_name}: missing column {column.name}")
                continue
            expected_signature = _column_signature(column, engine.dialect)
            actual_signature = _reflected_column_signature(actual, engine.dialect)
            if expected_signature != actual_signature:
                differences.append(
                    f"{table_name}.{column.name}: expected {expected_signature}, "
                    f"found {actual_signature}"
                )
            expected_default = _model_default(column, engine.dialect)
            actual_default = _normalize_sql(actual.get("default"))
            if expected_default != actual_default:
                differences.append(
                    f"{table_name}.{column.name}: default expected={expected_default!r}, "
                    f"found={actual_default!r}"
                )

        expected_unique = {
            tuple(constraint.columns.keys())
            for constraint in expected.constraints
            if constraint.__class__.__name__ == "UniqueConstraint"
        }
        actual_unique = _column_set(inspector.get_unique_constraints(table_name))
        if expected_unique != actual_unique:
            differences.append(
                f"{table_name}: unique constraints expected={sorted(expected_unique)} "
                f"found={sorted(actual_unique)}"
            )

        expected_foreign_keys = {
            (
                tuple(constraint.columns.keys()),
                tuple(element.target_fullname for element in constraint.elements),
            )
            for constraint in expected.foreign_key_constraints
        }
        actual_foreign_keys = {
            (
                tuple(row.get("constrained_columns") or ()),
                tuple(
                    f"{row['referred_table']}.{column}"
                    for column in row.get("referred_columns") or ()
                ),
            )
            for row in inspector.get_foreign_keys(table_name)
        }
        if expected_foreign_keys != actual_foreign_keys:
            differences.append(f"{table_name}: foreign keys differ")

        expected_indexes = {
            (tuple(index.columns.keys()), bool(index.unique)) for index in expected.indexes
        }
        actual_indexes = {
            (tuple(row.get("column_names") or ()), bool(row.get("unique")))
            for row in inspector.get_indexes(table_name)
            if not row.get("duplicates_constraint")
        }
        if expected_indexes != actual_indexes:
            differences.append(
                f"{table_name}: indexes expected={sorted(expected_indexes)} "
                f"found={sorted(actual_indexes)}"
            )

        expected_checks = {
            _check_signature(constraint.sqltext)
            for constraint in expected.constraints
            if isinstance(constraint, CheckConstraint)
        }
        actual_checks = {
            _check_signature(row.get("sqltext"))
            for row in inspector.get_check_constraints(table_name)
        }
        if expected_checks != actual_checks:
            differences.append(
                f"{table_name}: check constraints expected={sorted(expected_checks)} "
                f"found={sorted(actual_checks)}"
            )

    return differences


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

    differences = schema_differences(engine)
    if differences:
        raise RuntimeError("Database schema differs from ORM metadata:\n" + "\n".join(differences))

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
