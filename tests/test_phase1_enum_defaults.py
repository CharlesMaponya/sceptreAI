import os
import uuid

import pytest
from automl_api.db.base import Base
from sqlalchemy import Column, MetaData, Table, create_engine, delete, select, text
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import Session

LEGACY_COLUMNS = (
    "users.auth_provider",
    "users.global_role",
    "projects.status",
    "project_memberships.role",
    "project_share_links.role",
    "dataset_versions.status",
    "model_runs.run_kind",
    "model_runs.status",
    "model_runs.task_type",
    "metrics.kind",
    "metrics.split",
    "model_registry_entries.stage",
)


@pytest.mark.parametrize("identity", LEGACY_COLUMNS)
def test_database_enum_default_is_readable(identity):
    table, name = identity.split(".")
    original = Base.metadata.tables[table].c[name]
    engine = create_engine(os.environ["DATABASE_URL"])
    try:
        with engine.connect() as connection, connection.begin():
            probe = Table(
                "enum_probe_" + uuid.uuid4().hex,
                MetaData(),
                Column("value", original.type, server_default=original.server_default),
                prefixes=["TEMPORARY"],
            )
            probe.create(connection)
            connection.execute(text(f'INSERT INTO "{probe.name}" DEFAULT VALUES'))
            with Session(bind=connection) as db:
                result = db.scalar(select(probe.c.value))
                assert result.value == str(original.server_default.arg)
                enum_class = type(result)
                db.execute(delete(probe))
                for member in enum_class:
                    for spelling in (member.name, member.value):
                        db.execute(
                            text(f'INSERT INTO "{probe.name}" VALUES (:value)'), {"value": spelling}
                        )
                for member in enum_class:
                    for spelling in (member, member.name, member.value):
                        assert db.scalars(
                            select(probe.c.value).where(probe.c.value == spelling)
                        ).all() == [member, member]
                        assert db.scalars(
                            select(probe.c.value).where(probe.c.value.in_([spelling]))
                        ).all() == [member, member]
                    assert (
                        member
                        not in db.scalars(
                            select(probe.c.value).where(probe.c.value != member)
                        ).all()
                    )
                    assert (
                        member
                        not in db.scalars(
                            select(probe.c.value).where(probe.c.value.not_in([member]))
                        ).all()
                    )
                assert db.scalars(select(probe.c.value).where(probe.c.value.in_([]))).all() == []
                db.execute(delete(probe))
                db.execute(probe.insert().values(value=result))
                assert db.scalar(text(f'SELECT value FROM "{probe.name}"')) == result.name
                db.execute(delete(probe))
                db.execute(probe.insert().values(value=None))
                assert db.scalar(select(probe.c.value).where(probe.c.value.is_(None))) is None
                with pytest.raises(ValueError):
                    _ = probe.c.value == "unrecognized-enum-value"
                with pytest.raises(StatementError):
                    db.execute(probe.insert().values(value="unrecognized-enum-value"))
                db.execute(delete(probe))
                db.execute(
                    text(f'INSERT INTO "{probe.name}" VALUES (:value)'), {"value": "bad"}
                )
                with pytest.raises(ValueError):
                    db.scalar(select(probe.c.value))
            probe.drop(connection)
    finally:
        engine.dispose()
