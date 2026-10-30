"""Bounded local expansion/rollback checks; no production-size claim."""

import os
import subprocess
import sys
import uuid

from automl_api.models.runs import ModelRun
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from test_analysis_admission import analysis_scope as analysis_scope
from test_legacy_analysis_custody import legacy_scope as legacy_scope
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication

from scripts.verify_database_schema import schema_differences


def test_legacy_run_clock_expansion_preserves_prior_columns_and_rows(legacy_scope):
    source_engine, identity, *_ = legacy_scope
    source = make_url(os.environ["DATABASE_URL"])
    assert source.host == "127.0.0.1" and source.database == "automl_branch_0e6a232bc7_tests"
    name = "sceptre_phase1_legacy_observer_schema_" + uuid.uuid4().hex
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
                    text("SELECT to_jsonb(a)-'last_observed_at' FROM model_runs a ORDER BY id")
                ).scalars()
            )

    try:
        migrate("downgrade", "0020_observer_scan_clock")
        assert "last_observed_at" not in {
            c["name"] for c in inspect(engine).get_columns("model_runs")
        }
        before = domain()
        old_columns = ", ".join(
            c.name for c in ModelRun.__table__.columns if c.name != "last_observed_at"
        )
        with engine.connect() as db:
            old = dict(
                db.execute(
                    text(f"SELECT {old_columns} FROM model_runs WHERE id=:id"),
                    dict(id=identity),
                )
                .mappings()
                .one()
            )
        migrate("upgrade", "head")
        assert schema_differences(engine) == [] and domain() == before
        with engine.begin() as db:
            column = next(
                c for c in inspect(db).get_columns("model_runs") if c["name"] == "last_observed_at"
            )
            assert column["nullable"] and column["default"] is None
            assert (
                db.scalar(
                    text("SELECT last_observed_at FROM model_runs WHERE id=:id"),
                    dict(id=identity),
                )
                is None
            )
            assert (
                dict(
                    db.execute(
                        text(f"SELECT {old_columns} FROM model_runs WHERE id=:id"),
                        dict(id=identity),
                    )
                    .mappings()
                    .one()
                )
                == old
            )
            db.execute(text("UPDATE model_runs SET last_observed_at=clock_timestamp()"))
        assert domain() == before
        migrate("downgrade", "0020_observer_scan_clock")
        assert domain() == before
        migrate("upgrade", "head")
        assert domain() == before and schema_differences(engine) == []
    finally:
        engine.dispose()
        with admin.connect() as db:
            db.execute(text(f'DROP DATABASE "{name}"'))
        admin.dispose()
