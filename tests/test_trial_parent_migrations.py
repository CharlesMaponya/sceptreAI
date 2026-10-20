"""Corrupt trial lineage halts expansion; no production history is rewritten."""

import os
import subprocess
import sys
import uuid

import pytest
from automl_api.models.enums import WorkflowStage
from automl_api.models.iam import User
from automl_api.models.projects import Project
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session
from test_attempt_object_grant_schema import make_attempt
from test_phase1_migration_rehearsal import drop_database, rehearsal


@pytest.mark.parametrize("corrupt", [False, True])
def test_trial_integrity_expansion_blocks_bad_history_and_preserves_valid_rows(corrupt):
    source = make_url(os.environ["DATABASE_URL"])
    assert source.database.endswith("_tests")
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = "sceptre_phase1_rehearsal_trial_" + uuid.uuid4().hex
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(target)

    def migrate(action, revision):
        return subprocess.run(
            [sys.executable, "-m", "alembic", action, revision],
            env={**os.environ, "DATABASE_URL": target},
            capture_output=True,
            text=True,
        )

    try:
        result = migrate("upgrade", "0015_attempt_predecessor_project")
        assert result.returncode == 0, result.stderr
        with Session(engine) as db, db.begin():
            user = User(
                email=uuid.uuid4().hex + "@example.test",
                full_name="Trial history fixture",
                auth_provider="simple",
                global_role="member",
            )
            db.add(user)
            db.flush()
            project = Project(owner_id=user.id, created_by_id=user.id, name="Trial history")
            db.add(project)
            db.flush()
            child = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
            other = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
            original_trial = child.trial_id
            child_id = child.id
            if corrupt:
                child.trial_id = other.trial_id
        with engine.connect() as db:
            tables = [t for t in inspect(db).get_table_names() if t != "alembic_version"]
            before = rehearsal._snapshot_seeded_rows(db, tables)
            old_function = db.scalar(
                text("SELECT prosrc FROM pg_proc WHERE proname='validate_trial_attempt_run_parent'")
            )
            old_trigger = db.scalar(
                text(
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger "
                    "WHERE tgname='trg_validate_trial_attempt_run_parent'"
                )
            )
        if corrupt:
            result = migrate("upgrade", "0016_trial_parent_integrity")
            assert (
                result.returncode != 0
                and "Existing same-run trial ownership is inconsistent" in result.stderr
            )
            with engine.connect() as db:
                assert (
                    db.scalar(text("select version_num from alembic_version"))
                    == "0015_attempt_predecessor_project"
                )
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                assert (
                    db.scalar(
                        text(
                            "SELECT prosrc FROM pg_proc "
                            "WHERE proname='validate_trial_attempt_run_parent'"
                        )
                    )
                    == old_function
                )
                assert not db.scalar(
                    text(
                        "SELECT count(*) FROM pg_proc "
                        "WHERE proname='preserve_trial_attempt_ownership'"
                    )
                )
            # Restore this deliberately corrupt disposable fixture using its known original ID.
            with engine.begin() as db:
                db.execute(
                    text("UPDATE workflow_attempts SET trial_id=:trial WHERE id=:id"),
                    {"trial": original_trial, "id": child_id},
                )
            with engine.connect() as db:
                before = rehearsal._snapshot_seeded_rows(db, tables)
        result = migrate("upgrade", "0016_trial_parent_integrity")
        assert result.returncode == 0, result.stderr
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
            assert (
                db.scalar(text("select version_num from alembic_version"))
                == "0016_trial_parent_integrity"
            )
        result = migrate("downgrade", "0015_attempt_predecessor_project")
        assert result.returncode == 0, result.stderr
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
            assert (
                db.scalar(
                    text(
                        "SELECT prosrc FROM pg_proc "
                        "WHERE proname='validate_trial_attempt_run_parent'"
                    )
                )
                == old_function
            )
            assert (
                db.scalar(
                    text(
                        "SELECT pg_get_triggerdef(oid) FROM pg_trigger "
                        "WHERE tgname='trg_validate_trial_attempt_run_parent'"
                    )
                )
                == old_trigger
            )
        result = migrate("upgrade", "0016_trial_parent_integrity")
        assert result.returncode == 0, result.stderr
        with engine.connect() as db:
            assert rehearsal._snapshot_seeded_rows(db, tables) == before
    finally:
        engine.dispose()
        drop_database(admin, name)
        admin.dispose()
