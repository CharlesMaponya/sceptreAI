from __future__ import annotations

import json
import re
from collections import Counter
from enum import Enum

from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.db.base import Base
from automl_api.db.session import get_engine
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Engine,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    inspect,
    text,
)
from sqlalchemy.schema import CreateIndex
from sqlalchemy.types import TypeDecorator
from sqlglot import exp, parse


def _normalized_type(value: object, dialect) -> str:
    rendered = str(value.compile(dialect=dialect)).lower().replace("character varying", "varchar")
    if dialect.name == "postgresql" and rendered == "float":
        return "double precision"
    return rendered


def _column_signature(column: object, dialect) -> tuple[str, bool]:
    return (_normalized_type(column.type, dialect), bool(column.nullable))


def _reflected_column_signature(column: dict[str, object], dialect) -> tuple[str, bool]:
    return (_normalized_type(column["type"], dialect), bool(column["nullable"]))


def _default_signature(value, column, dialect) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    literal = re.fullmatch(
        r"('(?:[^']|'')*'|[+-]?\d+(?:\.\d+)?|true|false|null)(?:::(.+))?",
        rendered,
        flags=re.IGNORECASE,
    )
    if not literal:
        return rendered  # Unknown expressions compare exactly; never erase their casts/grouping.
    value, cast = literal.groups()
    storage_type = column.type.dialect_impl(dialect)
    while isinstance(storage_type, TypeDecorator):
        storage_type = storage_type.impl
    if cast:
        compatible_types = {_normalized_type(column.type, dialect)}
        if isinstance(storage_type, String):
            # Reflection omits the length on an implicit varchar literal cast.
            compatible_types.add("varchar")
        normalized_cast = " ".join(cast.lower().split()).replace("character varying", "varchar")
        if normalized_cast not in compatible_types:
            return rendered
    if isinstance(storage_type, (Integer, Numeric)) and re.fullmatch(
        r"'[+-]?\d+(?:\.\d+)?'", value
    ):
        return value[1:-1]
    return value if value.startswith("'") else value.lower()


def _model_default(column, dialect) -> str | None:
    rendered = dialect.ddl_compiler(dialect, None).get_column_default_string(column)
    return _default_signature(rendered, column, dialect)


def _text_atom(node, table, dialect):
    """Recognize only the varchar/text operands PostgreSQL coerces for comparison."""
    node = node.unnest()
    if isinstance(node, exp.Cast):
        target = node.to
        if target.this not in {exp.DType.TEXT, exp.DType.VARCHAR} or target.expressions:
            return None
        node = node.this.unnest()
    if isinstance(node, exp.Literal) and node.is_string:
        return node
    if isinstance(node, exp.Column) and not node.table:
        name = node.name if node.this.args.get("quoted") else node.name.lower()
        column = table.columns.get(name)
        if column is not None and _normalized_type(column.type, dialect).split("(")[0] in {
            "text",
            "varchar",
        }:
            return node
    return None


def _expression_signature(value: object, table, dialect) -> str:
    statements = parse(str(value), read="postgres")
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Expected one schema expression")

    def canonical(node):
        if isinstance(node, exp.Paren):
            return canonical(node.this)
        if isinstance(node, exp.Identifier):
            return ("identifier", node.name if node.args.get("quoted") else node.name.lower())
        if isinstance(node, exp.Index):
            # The catalog query already binds this definition to its table.
            return (
                node.key,
                {
                    key: canonical(item)
                    for key, item in node.args.items()
                    if key not in {"this", "table"}
                },
            )
        if isinstance(node, exp.IndexParameters):
            arguments = {**node.args, "using": node.args.get("using") or exp.Var(this="btree")}
            return (node.key, {key: canonical(item) for key, item in arguments.items()})
        if isinstance(node, exp.Create) and node.args.get("kind") == "INDEX":
            return (
                node.key,
                {
                    key: canonical(item)
                    for key, item in node.args.items()
                    if key not in {"concurrently", "exists"}
                },
            )
        if isinstance(node, exp.Ordered):
            arguments = {**node.args, "desc": bool(node.args.get("desc"))}
            return (node.key, {key: canonical(item) for key, item in arguments.items()})
        if isinstance(node, (exp.And, exp.Or)):
            # Parentheses still determine the tree; only identical Boolean
            # operators are associative. Arithmetic grouping is never flattened.
            def operands(part):
                part = part.unnest()
                if type(part) is type(node):
                    return operands(part.this) + operands(part.expression)
                return [canonical(part)]

            return (node.key, operands(node))
        if isinstance(node, (exp.EQ, exp.In)):
            left = _text_atom(node.this, table, dialect)
            if isinstance(left, exp.Column):
                if isinstance(node, exp.In):
                    values = node.expressions if not node.args.get("query") else []
                else:
                    right = node.expression.unnest()
                    values = []
                    if isinstance(right, exp.Any):
                        array = right.this.unnest()
                        if isinstance(array, exp.Cast) and array.to == exp.DataType.build(
                            "text[]", dialect="postgres"
                        ):
                            array = array.this.unnest()
                        if isinstance(array, exp.Array):
                            values = array.expressions
                    else:
                        literal = _text_atom(right, table, dialect)
                        if isinstance(literal, exp.Literal):
                            return ("eq", canonical(left), canonical(literal))
                literals = [_text_atom(item, table, dialect) for item in values]
                if literals and all(isinstance(item, exp.Literal) for item in literals):
                    return ("in", canonical(left), [canonical(item) for item in literals])
        if isinstance(node, exp.Expression):
            return (node.key, {key: canonical(item) for key, item in node.args.items()})
        if isinstance(node, list):
            return [canonical(item) for item in node]
        if isinstance(node, Enum):
            return (type(node).__name__, node.name)
        return node

    return json.dumps(canonical(statements[0]), sort_keys=True)


def _constraint_timing(constraint) -> tuple[bool, bool]:
    return bool(constraint.deferrable), (constraint.initially or "IMMEDIATE").upper() == "DEFERRED"


def _identity_signature(identity, column_type):
    if identity is None:
        return None
    options = identity if isinstance(identity, dict) else vars(identity)
    bits = (
        64
        if isinstance(column_type, BigInteger)
        else 16
        if isinstance(column_type, SmallInteger)
        else 32
    )
    increment = options.get("increment") if options.get("increment") is not None else 1
    minimum = 1 if increment > 0 else -(2 ** (bits - 1))
    maximum = 2 ** (bits - 1) - 1 if increment > 0 else -1
    if options.get("minvalue") is not None and not options.get("nominvalue"):
        minimum = options["minvalue"]
    if options.get("maxvalue") is not None and not options.get("nomaxvalue"):
        maximum = options["maxvalue"]
    start = options.get("start")
    return (
        bool(options.get("always")),
        increment,
        minimum,
        maximum,
        start if start is not None else minimum if increment > 0 else maximum,
        options.get("cache") if options.get("cache") is not None else 1,
        bool(options.get("cycle")),
    )


def _foreign_key_options(options) -> tuple:
    return (
        (options.get("ondelete") or "NO ACTION").upper(),
        (options.get("onupdate") or "NO ACTION").upper(),
        bool(options.get("deferrable")),
        (options.get("initially") or "IMMEDIATE").upper(),
        (options.get("match") or "SIMPLE").upper(),
    )


def schema_differences(engine) -> list[str]:
    if isinstance(engine, Engine):
        with engine.connect() as connection:
            return schema_differences(connection)
    inspector = inspect(engine)
    # SQLAlchemy does not reflect unique/primary-key deferral or FK validation.
    # Inspect the visible relation, so another schema with the same table name
    # cannot supply its constraint state. This needs no DDL privileges.
    constraint_state = {}
    index_state = []
    if engine.dialect.name == "postgresql":
        constraint_state = {
            (row["table_name"], row["conname"]): row
            for row in engine.execute(
                text("""
                    SELECT r.relname AS table_name, c.conname, c.contype,
                           c.convalidated, c.condeferrable, c.condeferred
                    FROM pg_catalog.pg_constraint c
                    JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
                    WHERE pg_catalog.pg_table_is_visible(r.oid)
                """)
            ).mappings()
        }
        index_state = (
            engine.execute(
                text("""
            SELECT r.relname AS table_name, x.relname AS index_name,
                   pg_catalog.pg_get_indexdef(i.indexrelid) AS definition,
                   i.indisvalid, i.indisready, i.indislive,
                   EXISTS (
                       SELECT 1 FROM pg_catalog.pg_constraint c
                       WHERE c.conindid = i.indexrelid AND c.contype IN ('p', 'u', 'x')
                   ) AS constraint_owned
            FROM pg_catalog.pg_index i
            JOIN pg_catalog.pg_class r ON r.oid = i.indrelid
            JOIN pg_catalog.pg_class x ON x.oid = i.indexrelid
            WHERE pg_catalog.pg_table_is_visible(r.oid)
        """)
            )
            .mappings()
            .all()
        )
    actual_tables = set(inspector.get_table_names())
    expected_tables = set(Base.metadata.tables)
    differences = [f"missing table {name}" for name in sorted(expected_tables - actual_tables)]

    for table_name in sorted(expected_tables & actual_tables):
        expected = Base.metadata.tables[table_name]
        for (relation, name), state in constraint_state.items():
            if relation == table_name and not state["convalidated"]:
                differences.append(f"{table_name}: constraint {name} is not validated")
            if relation == table_name and state["contype"] not in {"c", "f", "p", "u"}:
                differences.append(
                    f"{table_name}: unmodeled constraint {name} ({state['contype']})"
                )
        actual_columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        for name in sorted(set(actual_columns) - set(expected.columns.keys())):
            differences.append(f"{table_name}: unexpected column {name}")
        expected_primary_key = tuple(expected.primary_key.columns.keys())
        primary_key = inspector.get_pk_constraint(table_name)
        actual_primary_key = tuple(primary_key["constrained_columns"])
        if expected_primary_key != actual_primary_key:
            differences.append(
                f"{table_name}: primary key expected={expected_primary_key}, "
                f"found={actual_primary_key}"
            )
        primary_state = constraint_state.get((table_name, primary_key.get("name")), {})
        actual_timing = (
            bool(primary_state.get("condeferrable")),
            bool(primary_state.get("condeferred")),
        )
        if _constraint_timing(expected.primary_key) != actual_timing:
            differences.append(f"{table_name}: primary key timing differs")
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
            actual_default = _default_signature(actual.get("default"), column, engine.dialect)
            if expected_default != actual_default:
                differences.append(
                    f"{table_name}.{column.name}: default expected={expected_default!r}, "
                    f"found={actual_default!r}"
                )
            if _identity_signature(column.identity, column.type) != _identity_signature(
                actual.get("identity"), actual["type"]
            ):
                differences.append(f"{table_name}.{column.name}: identity differs")
            computed = column.computed
            expected_computed = (
                None
                if computed is None
                else (
                    _expression_signature(computed.sqltext, expected, engine.dialect),
                    computed.persisted is not False,
                )
            )
            reflected_computed = actual.get("computed")
            actual_computed = (
                None
                if reflected_computed is None
                else (
                    _expression_signature(reflected_computed["sqltext"], expected, engine.dialect),
                    bool(reflected_computed["persisted"]),
                )
            )
            if expected_computed != actual_computed:
                differences.append(f"{table_name}.{column.name}: computed expression differs")

        expected_unique = Counter(
            (
                tuple(constraint.columns.keys()),
                _constraint_timing(constraint),
                bool(constraint.dialect_options["postgresql"].get("nulls_not_distinct")),
            )
            for constraint in expected.constraints
            if isinstance(constraint, UniqueConstraint)
        )
        actual_unique = Counter(
            (
                tuple(row["column_names"]),
                (
                    bool(constraint_state.get((table_name, row["name"]), {}).get("condeferrable")),
                    bool(constraint_state.get((table_name, row["name"]), {}).get("condeferred")),
                ),
                bool(row.get("dialect_options", {}).get("postgresql_nulls_not_distinct")),
            )
            for row in inspector.get_unique_constraints(table_name)
        )
        if expected_unique != actual_unique:
            differences.append(
                f"{table_name}: unique constraints expected={expected_unique!r} "
                f"found={actual_unique!r}"
            )

        expected_foreign_keys = Counter(
            (
                tuple(constraint.columns.keys()),
                tuple(element.target_fullname for element in constraint.elements),
                _foreign_key_options(
                    {
                        name: getattr(constraint, name)
                        for name in ("ondelete", "onupdate", "deferrable", "initially", "match")
                    }
                ),
            )
            for constraint in expected.foreign_key_constraints
        )
        actual_foreign_keys = Counter(
            (
                tuple(row.get("constrained_columns") or ()),
                tuple(
                    (f"{row['referred_schema']}." if row.get("referred_schema") else "")
                    + f"{row['referred_table']}.{column}"
                    for column in row.get("referred_columns") or ()
                ),
                _foreign_key_options(row.get("options") or {}),
            )
            for row in inspector.get_foreign_keys(table_name)
        )
        if expected_foreign_keys != actual_foreign_keys:
            differences.append(f"{table_name}: foreign keys differ")

        if engine.dialect.name == "postgresql":
            table_indexes = [row for row in index_state if row["table_name"] == table_name]
            for row in table_indexes:
                if not all(row[flag] for flag in ("indisvalid", "indisready", "indislive")):
                    differences.append(
                        f"{table_name}: index {row['index_name']} is not valid/ready/live"
                    )
            expected_indexes = Counter(
                _expression_signature(
                    CreateIndex(index).compile(dialect=engine.dialect), expected, engine.dialect
                )
                for index in expected.indexes
            )
            actual_indexes = Counter(
                _expression_signature(row["definition"], expected, engine.dialect)
                for row in table_indexes
                if not row["constraint_owned"]
            )
        else:
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
                f"{table_name}: indexes expected={expected_indexes!r} found={actual_indexes!r}"
            )

        expected_checks = Counter(
            (_expression_signature(constraint.sqltext, expected, engine.dialect), False)
            for constraint in expected.constraints
            if isinstance(constraint, CheckConstraint)
        )
        actual_checks = Counter(
            (
                _expression_signature(row.get("sqltext"), expected, engine.dialect),
                bool(row.get("dialect_options", {}).get("no_inherit")),
            )
            for row in inspector.get_check_constraints(table_name)
        )
        if expected_checks != actual_checks:
            differences.append(
                f"{table_name}: check constraints expected={expected_checks!r} "
                f"found={actual_checks!r}"
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
