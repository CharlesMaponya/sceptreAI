import importlib.util
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import CreateColumn

spec = importlib.util.spec_from_file_location(
    "schema_verifier", Path("scripts/verify_database_schema.py")
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.fixture
def schema_case(monkeypatch):
    url = os.environ["DATABASE_URL"]
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url)
    metadata = MetaData()
    Table("parent", metadata, Column("id", Integer, primary_key=True, autoincrement=False))
    Table(
        "child",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("parent_id", Integer),
        Column("Parent_id", Integer, nullable=False),
        Column("label", String(64), server_default="Ready Now"),
        CheckConstraint("parent_id > 0", name="positive_parent"),
        UniqueConstraint("parent_id", name="unique_parent", deferrable=True, initially="DEFERRED"),
        ForeignKeyConstraint(
            ["parent_id"],
            ["parent.id"],
            name="parent_link",
            ondelete="CASCADE",
            onupdate="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
            match="FULL",
        ),
    )
    monkeypatch.setattr(verifier, "Base", SimpleNamespace(metadata=metadata))
    with engine.connect() as connection, connection.begin():
        schema = "schema_probe_" + uuid.uuid4().hex
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
        metadata.create_all(connection)
        assert verifier.schema_differences(connection) == []
        yield connection
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.mark.parametrize(
    "change, expected",
    [
        ("ALTER TABLE child ADD COLUMN unexpected integer", "unexpected column"),
        ("ALTER TABLE child DROP CONSTRAINT child_pkey", "primary key"),
        (
            "ALTER TABLE child DROP CONSTRAINT parent_link; "
            "ALTER TABLE child ADD CONSTRAINT parent_link "
            "FOREIGN KEY (parent_id) REFERENCES parent(id) MATCH FULL ON DELETE RESTRICT "
            "ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED",
            "foreign keys",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT parent_link; "
            "ALTER TABLE child ADD CONSTRAINT parent_link "
            "FOREIGN KEY (parent_id) REFERENCES parent(id) MATCH FULL ON DELETE CASCADE "
            "ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED",
            "foreign keys",
        ),
        ("ALTER TABLE child ALTER CONSTRAINT parent_link NOT DEFERRABLE", "foreign keys"),
        ("ALTER TABLE child ALTER CONSTRAINT parent_link INITIALLY IMMEDIATE", "foreign keys"),
        (
            "ALTER TABLE child DROP CONSTRAINT parent_link; "
            "ALTER TABLE child ADD CONSTRAINT parent_link "
            "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE "
            "ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED",
            "foreign keys",
        ),
    ],
)
def test_rejects_structural_schema_drift(schema_case, change, expected):
    schema_case.execute(text(change))
    assert any(expected in difference for difference in verifier.schema_differences(schema_case))


@pytest.mark.parametrize(
    "change, expected",
    [
        (
            "ALTER TABLE child DROP CONSTRAINT parent_link; "
            "ALTER TABLE child ADD CONSTRAINT parent_link "
            "FOREIGN KEY (parent_id) REFERENCES parent(id) MATCH FULL ON DELETE CASCADE "
            "ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED NOT VALID",
            "not validated",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT positive_parent; "
            "ALTER TABLE child ADD CONSTRAINT positive_parent CHECK (parent_id > 0) NOT VALID",
            "not validated",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT unique_parent; "
            "ALTER TABLE child ADD CONSTRAINT unique_parent UNIQUE (parent_id) NOT DEFERRABLE",
            "unique constraints",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT unique_parent; "
            "ALTER TABLE child ADD CONSTRAINT unique_parent UNIQUE (parent_id) "
            "DEFERRABLE INITIALLY IMMEDIATE",
            "unique constraints",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT child_pkey; "
            "ALTER TABLE child ADD CONSTRAINT child_pkey PRIMARY KEY (id) DEFERRABLE",
            "primary key timing",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT child_pkey; "
            "ALTER TABLE child ADD CONSTRAINT child_pkey PRIMARY KEY (id) "
            "DEFERRABLE INITIALLY DEFERRED",
            "primary key timing",
        ),
    ],
)
def test_rejects_constraint_state_drift(schema_case, change, expected):
    schema_case.execute(text(change))
    assert any(expected in difference for difference in verifier.schema_differences(schema_case))


def test_ignores_constraints_on_hidden_relations(schema_case):
    hidden = "hidden_probe_" + uuid.uuid4().hex
    schema_case.execute(text(f'CREATE SCHEMA "{hidden}"'))
    schema_case.execute(text(f'CREATE TABLE "{hidden}".child (parent_id integer)'))
    schema_case.execute(
        text(
            f'ALTER TABLE "{hidden}".child ADD CONSTRAINT positive_parent '
            "CHECK (parent_id > 0) NOT VALID"
        )
    )
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text(f'DROP SCHEMA "{hidden}" CASCADE'))


def test_full_application_schema_can_be_verified_read_only():
    url = os.environ["DATABASE_URL"]
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url)
    try:
        with engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            assert verifier.schema_differences(connection) == []
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "changed", ["READY Now", "Ready  Now", "Ready\tNow", "Ready Now::text", '"Ready Now"']
)
def test_default_literal_changes_are_not_normalized_away(schema_case, changed):
    schema_case.execute(text(f"ALTER TABLE child ALTER COLUMN label SET DEFAULT '{changed}'"))
    assert any(
        "child.label: default" in difference
        for difference in verifier.schema_differences(schema_case)
    )


@pytest.mark.parametrize(
    "original, changed",
    [
        ("parent_id > 0 OR id > 0 AND id < 10", "(parent_id > 0 OR id > 0) AND id < 10"),
        ("parent_id + id * 2 > 0", "(parent_id + id) * 2 > 0"),
        ("parent_id / 2 > 1", "parent_id::numeric / 2 > 1"),
        ("length(label::text) > 63", "length(label::name) > 63"),
        ("parent_id > 0", '"Parent_id" > 0'),
        ("label = 'Ready Now'", "label = 'READY Now'"),
        ("label = 'Ready Now'", "label = 'Ready  Now'"),
        ("label = 'Ready Now'", "label = 'Ready Now::text'"),
        ("label = 'Ready Now'", "label = '\"Ready Now\"'"),
        ("label IN ('Ready', 'Done')", "label IN ('READY', 'Done')"),
    ],
)
def test_check_expression_semantic_drift(schema_case, original, changed):
    verifier.Base.metadata.tables["child"].append_constraint(
        CheckConstraint(original, name="semantic_probe")
    )
    schema_case.execute(text(f"ALTER TABLE child ADD CONSTRAINT semantic_probe CHECK ({original})"))
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text("ALTER TABLE child DROP CONSTRAINT semantic_probe"))
    schema_case.execute(text(f"ALTER TABLE child ADD CONSTRAINT semantic_probe CHECK ({changed})"))
    assert any(
        "check constraints" in difference for difference in verifier.schema_differences(schema_case)
    )


@pytest.mark.parametrize(
    "definition",
    [
        "(label)",
        "(label) WHERE label = 'READY'",
        "(label DESC) WHERE label = 'Ready'",
        "(label NULLS FIRST) WHERE label = 'Ready'",
        "(label) INCLUDE (parent_id) WHERE label = 'Ready'",
        "USING hash (label) WHERE label = 'Ready'",
        "(label varchar_pattern_ops) WHERE label = 'Ready'",
        "((lower(label::text))) WHERE label = 'Ready'",
    ],
)
def test_index_definition_drift(schema_case, definition):
    table = verifier.Base.metadata.tables["child"]
    index = Index("index_probe", table.c.label, postgresql_where=text("label = 'Ready'"))
    index.create(schema_case)
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text("DROP INDEX index_probe"))
    schema_case.execute(text(f"CREATE INDEX index_probe ON child {definition}"))
    assert any("indexes" in difference for difference in verifier.schema_differences(schema_case))


def test_equivalent_index_definition_and_duplicate_detection(schema_case):
    table = verifier.Base.metadata.tables["child"]
    index = Index("model_name", table.c.label, postgresql_where=text("label = 'Ready'"))
    index.dialect_options["postgresql"]["concurrently"] = True
    schema_case.execute(
        text(
            "CREATE INDEX actual_name ON child USING btree (label ASC NULLS LAST) "
            "WHERE label::text = 'Ready'::text"
        )
    )
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text("CREATE INDEX redundant_copy ON child (label) WHERE label = 'Ready'"))
    assert any("indexes" in difference for difference in verifier.schema_differences(schema_case))


def test_rejects_failed_concurrent_index_build(monkeypatch):
    url = os.environ["DATABASE_URL"]
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    metadata = MetaData()
    table = Table("index_failure", metadata, Column("value", Integer))
    monkeypatch.setattr(verifier, "Base", SimpleNamespace(metadata=metadata))
    schema = "index_probe_" + uuid.uuid4().hex
    try:
        with engine.connect() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            try:
                connection.execute(text(f'SET search_path TO "{schema}"'))
                metadata.create_all(connection)
                connection.execute(text("INSERT INTO index_failure VALUES (1), (1)"))
                Index("failed_index", table.c.value, unique=True)
                with pytest.raises(IntegrityError):
                    connection.execute(
                        text(
                            "CREATE UNIQUE INDEX CONCURRENTLY failed_index ON index_failure (value)"
                        )
                    )
                assert any(
                    "not valid/ready/live" in difference
                    for difference in verifier.schema_differences(connection)
                )
            finally:
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "change, expected",
    [
        ('ALTER TABLE child ALTER COLUMN "Parent_id" ADD GENERATED ALWAYS AS IDENTITY', "identity"),
        (
            'ALTER TABLE child DROP COLUMN "Parent_id"; ALTER TABLE child ADD COLUMN "Parent_id" '
            "integer GENERATED ALWAYS AS (id + 1) STORED NOT NULL",
            "computed",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT unique_parent; ALTER TABLE child ADD CONSTRAINT "
            "unique_parent UNIQUE NULLS NOT DISTINCT (parent_id) DEFERRABLE INITIALLY DEFERRED",
            "unique constraints",
        ),
        (
            "ALTER TABLE child DROP CONSTRAINT positive_parent; ALTER TABLE child ADD CONSTRAINT "
            "positive_parent CHECK (parent_id > 0) NO INHERIT",
            "check constraints",
        ),
    ],
)
def test_generated_columns_and_constraint_options(schema_case, change, expected):
    schema_case.execute(text(change))
    assert any(expected in item for item in verifier.schema_differences(schema_case))


@pytest.mark.parametrize(
    "option",
    [
        "SET GENERATED BY DEFAULT",
        "SET START WITH 10",
        "SET INCREMENT BY 2",
        "SET MINVALUE 0",
        "SET MAXVALUE 100",
        "SET CACHE 5",
        "SET CYCLE",
    ],
)
def test_identity_options(schema_case, option):
    column = Column("identity_probe", Integer, Identity(always=True), nullable=False)
    verifier.Base.metadata.tables["child"].append_column(column)
    schema_case.execute(
        text(
            "ALTER TABLE child ADD COLUMN "
            f"{CreateColumn(column).compile(dialect=schema_case.dialect)}"
        )
    )
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text(f"ALTER TABLE child ALTER COLUMN identity_probe {option}"))
    assert any("identity" in item for item in verifier.schema_differences(schema_case))


def test_computed_expression_drift(schema_case):
    column = Column("computed_probe", Integer, Computed("id + 1"))
    verifier.Base.metadata.tables["child"].append_column(column)
    schema_case.execute(
        text(
            "ALTER TABLE child ADD COLUMN "
            f"{CreateColumn(column).compile(dialect=schema_case.dialect)}"
        )
    )
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text("ALTER TABLE child DROP COLUMN computed_probe"))
    schema_case.execute(
        text(
            "ALTER TABLE child ADD COLUMN computed_probe integer "
            "GENERATED ALWAYS AS (id + 2) STORED"
        )
    )
    assert any("computed" in item for item in verifier.schema_differences(schema_case))


@pytest.mark.parametrize("column_type", [SmallInteger, Integer, BigInteger])
@pytest.mark.parametrize("increment", [1, -1])
def test_identity_defaults_and_advancing_values(schema_case, column_type, increment):
    column = Column("identity_probe", column_type, Identity(increment=increment), nullable=False)
    verifier.Base.metadata.tables["child"].append_column(column)
    ddl = CreateColumn(column).compile(dialect=schema_case.dialect)
    schema_case.execute(text(f"ALTER TABLE child ADD COLUMN {ddl}"))
    assert verifier.schema_differences(schema_case) == []
    schema_case.execute(text("SELECT nextval(pg_get_serial_sequence('child', 'identity_probe'))"))
    schema_case.execute(text("SELECT nextval(pg_get_serial_sequence('child', 'identity_probe'))"))
    assert verifier.schema_differences(schema_case) == []


def test_application_byte_columns_have_64_bit_storage():
    url = os.environ["DATABASE_URL"]
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url)
    byte_columns = [
        column
        for table in verifier.Base.metadata.sorted_tables
        for column in table.columns
        if column.name.endswith(("bytes", "byte_size")) or column.name == "part_size"
    ]
    assert byte_columns
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            for column in byte_columns:
                actual = next(
                    item
                    for item in inspector.get_columns(column.table.name)
                    if item["name"] == column.name
                )
                assert isinstance(column.type, BigInteger), str(column)
                assert isinstance(actual["type"], BigInteger), str(column)
                sql_type = actual["type"].compile(dialect=connection.dialect)
                for size in (10 * 1024**3, 2**63 - 1):
                    actual_size = connection.scalar(
                        text(f"SELECT CAST(:size AS {sql_type})"), {"size": size}
                    )
                    assert actual_size == size
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "definition",
    [
        "CHECK (parent_id > 0)",
        "UNIQUE (parent_id) DEFERRABLE INITIALLY DEFERRED",
        "FOREIGN KEY (parent_id) REFERENCES parent(id) MATCH FULL ON DELETE CASCADE "
        "ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED",
        "EXCLUDE USING gist (int4range(parent_id, parent_id + 2, '[]') WITH &&)",
    ],
)
def test_extra_constraint_is_not_ignored(schema_case, definition):
    schema_case.execute(text(f"ALTER TABLE child ADD CONSTRAINT extra_probe {definition}"))
    assert verifier.schema_differences(schema_case)
