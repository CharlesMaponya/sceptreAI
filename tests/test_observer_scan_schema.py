"""Bounded local expansion/rollback checks; no production-size claim."""

import os
import subprocess
import sys
import uuid

from automl_api.models.workflows import WorkflowAttempt
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from test_ray_observer_inspection import (
    committed_tenant as committed_tenant,
)
from test_ray_observer_inspection import (
    phase1_engine as phase1_engine,
)
from test_ray_observer_inspection import (
    profile_source as profile_source,
)
from test_ray_observer_inspection import (
    ray_scope as ray_scope,
)

from scripts.verify_database_schema import schema_differences


def test_nullable_scan_expansion_preserves_legacy_reads_and_domain_rows(ray_scope):
    source_engine, identity, *_ = ray_scope
    source = make_url(os.environ["DATABASE_URL"])
    assert source.host == "127.0.0.1" and source.database == "automl_branch_0e6a232bc7_tests"
    name = "sceptre_phase1_observer_schema_" + uuid.uuid4().hex
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    source_engine.dispose()
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{source.database}"'))
    url = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(url)

    def migrate(*args):
        result = subprocess.run(
            [sys.executable, "-m", "alembic", *args],
            env={**os.environ, "DATABASE_URL": url},
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert result.returncode == 0, result.stderr[-1800:]

    def domain():
        with engine.connect() as db:
            return list(
                db.execute(
                    text(
                        "SELECT to_jsonb(a)-'last_observed_at' FROM workflow_attempts a ORDER BY id"
                    )
                ).scalars()
            )

    try:
        migrate("downgrade", "0019_deletion_stage_custody")
        assert "last_observed_at" not in {
            c["name"] for c in inspect(engine).get_columns("workflow_attempts")
        }
        before = domain()
        old_columns = ", ".join(
            c.name for c in WorkflowAttempt.__table__.columns if c.name != "last_observed_at"
        )
        with engine.connect() as db:
            old = dict(
                db.execute(
                    text(f"SELECT {old_columns} FROM workflow_attempts WHERE id=:id"),
                    dict(id=identity),
                )
                .mappings()
                .one()
            )
        migrate("upgrade", "head")
        assert schema_differences(engine) == [] and domain() == before
        with engine.begin() as db:
            column = next(
                c
                for c in inspect(db).get_columns("workflow_attempts")
                if c["name"] == "last_observed_at"
            )
            assert column["nullable"] and column["default"] is None
            assert (
                db.scalar(
                    text("SELECT last_observed_at FROM workflow_attempts WHERE id=:id"),
                    dict(id=identity),
                )
                is None
            )
            assert (
                dict(
                    db.execute(
                        text(f"SELECT {old_columns} FROM workflow_attempts WHERE id=:id"),
                        dict(id=identity),
                    )
                    .mappings()
                    .one()
                )
                == old
            )
            db.execute(text("UPDATE workflow_attempts SET last_observed_at=clock_timestamp()"))
        assert domain() == before
        migrate("downgrade", "0019_deletion_stage_custody")
        assert domain() == before
        migrate("upgrade", "head")
        assert domain() == before and schema_differences(engine) == []
    finally:
        engine.dispose()
        with admin.connect() as db:
            db.execute(text(f'DROP DATABASE "{name}"'))
        admin.dispose()
