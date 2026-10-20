"""Predecessor IDs cannot cross project ownership at the PostgreSQL boundary."""

import os
import subprocess
import uuid

import pytest
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowAttempt
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_attempt_object_grant_schema import make_attempt
from test_phase1_migration_rehearsal import drop_database, rehearsal

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.mark.parametrize("stage", list(WorkflowStage))
@pytest.mark.parametrize("action", ["insert", "update"])
def test_attempt_predecessor_is_project_scoped(phase1_db, tenant, stage, action):
    db = phase1_db
    user, project = tenant
    other = Project(owner_id=user.id, created_by_id=user.id, name="other predecessor project")
    db.add(other)
    db.flush()
    foreign = make_attempt(db, user, other, stage)
    previous = make_attempt(db, user, project, stage)
    current = make_attempt(db, user, project, stage)
    assert current.predecessor_attempt_id is None
    previous.status = AttemptStatus.FAILED
    db.flush()
    current.logical_key = previous.logical_key
    for name in (
        "dataset_version_id",
        "model_run_id",
        "trial_id",
        "run_attempt_id",
        "scope_id",
        "conformance_campaign_id",
    ):
        setattr(current, name, getattr(previous, name))
    current.predecessor_attempt_id = previous.id
    current.generation = 2
    db.flush()
    with pytest.raises(IntegrityError, match="fk_attempt_predecessor_project"):
        with db.begin_nested():
            if action == "update":
                current.predecessor_attempt_id = foreign.id
            else:
                db.add(
                    WorkflowAttempt(
                        project_id=current.project_id,
                        stage=current.stage,
                        logical_key=uuid.uuid4().hex,
                        dataset_version_id=current.dataset_version_id,
                        model_run_id=current.model_run_id,
                        trial_id=current.trial_id,
                        run_attempt_id=current.run_attempt_id,
                        scope_id=current.scope_id,
                        conformance_campaign_id=current.conformance_campaign_id,
                        workload_identity="predecessor-project-test",
                        generation=3,
                        fencing_token=uuid.uuid4().hex,
                        predecessor_attempt_id=foreign.id,
                    )
                )
            db.flush()
    db.refresh(current)
    assert current.predecessor_attempt_id == previous.id
    current.predecessor_attempt_id = None
    db.flush()
    current.predecessor_attempt_id = previous.id
    db.flush()


@pytest.mark.parametrize("corrupt", [False, True])
def test_predecessor_expansion_preserves_rows_or_blocks_invalid_legacy_link(corrupt):
    source = make_url(os.environ["DATABASE_URL"])
    assert source.database.endswith("_tests")
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = "sceptre_phase1_rehearsal_predecessor_" + uuid.uuid4().hex
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(target)

    def migrate(revision):
        rehearsal._run(target, rehearsal.sys.executable, "-m", "alembic", "upgrade", revision)

    tables = [
        "users",
        "projects",
        "datasets",
        "dataset_versions",
        "model_runs",
        "workflow_attempts",
    ]
    try:
        migrate("0014_attempt_object_grants")
        with Session(engine) as db, db.begin():
            user = User(
                email=uuid.uuid4().hex + "@example.test",
                full_name="Predecessor fixture",
                auth_provider="simple",
                global_role="member",
            )
            db.add(user)
            db.flush()
            project = Project(owner_id=user.id, created_by_id=user.id, name="predecessor owner")
            other = Project(owner_id=user.id, created_by_id=user.id, name="foreign owner")
            db.add_all([project, other])
            db.flush()
            previous = make_attempt(db, user, project)
            previous.status = AttemptStatus.SUPERSEDED
            db.flush()
            current = WorkflowAttempt(
                project_id=project.id,
                stage=previous.stage,
                model_run_id=previous.model_run_id,
                logical_key=previous.logical_key,
                workload_identity="predecessor-project-test",
                generation=2,
                fencing_token=uuid.uuid4().hex,
                predecessor_attempt_id=make_attempt(db, user, other).id if corrupt else previous.id,
            )
            db.add(current)
            db.flush()
            current_id = current.id
        with engine.connect() as db:
            before = rehearsal._snapshot_seeded_rows(db, tables)
        if corrupt:
            rejected = subprocess.run(
                [
                    rehearsal.sys.executable,
                    "-m",
                    "alembic",
                    "upgrade",
                    "0015_attempt_predecessor_project",
                ],
                env={**os.environ, "DATABASE_URL": target},
                capture_output=True,
                text=True,
            )
            assert rejected.returncode != 0
            assert "fk_attempt_predecessor_project" in rejected.stderr
            with engine.connect() as db:
                assert (
                    db.scalar(text("select version_num from alembic_version"))
                    == "0014_attempt_object_grants"
                )
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                assert "fk_attempt_predecessor_project" not in {
                    v["name"] for v in inspect(db).get_foreign_keys("workflow_attempts")
                }
                assert "ix_attempt_predecessor_project" not in {
                    v["name"] for v in inspect(db).get_indexes("workflow_attempts")
                }
            # Repair only this deliberately corrupt disposable fixture to test resumption.
            with engine.begin() as db:
                db.execute(
                    text("update workflow_attempts set predecessor_attempt_id=null where id=:id"),
                    {"id": current_id},
                )
            with engine.connect() as db:
                before = rehearsal._snapshot_seeded_rows(db, tables)
        migrate("0015_attempt_predecessor_project")
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
            assert (
                db.scalar(text("select version_num from alembic_version"))
                == "0015_attempt_predecessor_project"
            )
            assert (
                db.scalar(
                    text(
                        "select convalidated from pg_constraint "
                        "where conname='fk_attempt_predecessor_project'"
                    )
                )
                is True
            )
            foreign_keys = inspect(db).get_foreign_keys("workflow_attempts")
            assert any(v["constrained_columns"] == ["predecessor_attempt_id"] for v in foreign_keys)
        rehearsal._run(
            target,
            rehearsal.sys.executable,
            "-m",
            "alembic",
            "downgrade",
            "0014_attempt_object_grants",
        )
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
            assert "fk_attempt_predecessor_project" not in {
                v["name"] for v in inspect(db).get_foreign_keys("workflow_attempts")
            }
        migrate("0015_attempt_predecessor_project")
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
    finally:
        engine.dispose()
        drop_database(admin, name)
        admin.dispose()
