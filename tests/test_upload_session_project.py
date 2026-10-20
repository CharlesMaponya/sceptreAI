"""Upload results retain project ownership and existing deletion semantics."""

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import Dataset, DatasetUploadSession, DatasetVersion
from automl_api.models.enums import AuthProvider, DatasetFormat, GlobalRole
from automl_api.models.iam import User
from automl_api.models.projects import Project
from sqlalchemy import create_engine, delete, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_phase1_migration_rehearsal import drop_database, rehearsal

pytest_plugins = ["test_phase1_postgres_contracts"]
LINKS = (
    ("dataset_id", "fk_upload_session_dataset_project"),
    ("dataset_version_id", "fk_upload_session_version_project"),
)


def seed(db):
    user = User(
        email=f"upload-project-{uuid.uuid4().hex}@example.test",
        full_name="Upload project fixture",
        auth_provider=AuthProvider.SIMPLE,
        global_role=GlobalRole.MEMBER,
    )
    db.add(user)
    db.flush()
    projects, datasets, versions = [], [], []
    for index in range(2):
        project = Project(owner_id=user.id, created_by_id=user.id, name=f"upload-owner-{index}")
        db.add(project)
        db.flush()
        dataset = Dataset(project_id=project.id, created_by_id=user.id, name="synthetic upload")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            project_id=project.id,
            dataset_id=dataset.id,
            created_by_id=user.id,
            version_number=1,
            format=DatasetFormat.CSV,
            object_uri=f"s3://no-store-call/{uuid.uuid4()}",
            content_hash="a" * 64,
            byte_size=1,
        )
        db.add(version)
        db.flush()
        projects.append(project)
        datasets.append(dataset)
        versions.append(version)
    return SimpleNamespace(
        db=db, user=user, projects=projects, datasets=datasets, versions=versions
    )


def upload(case, **values):
    return DatasetUploadSession(
        project_id=case.projects[0].id,
        created_by_id=case.user.id,
        dataset_name="synthetic",
        original_filename="fixture.csv",
        byte_size=1,
        part_size=1,
        total_parts=1,
        object_key=f"upload-project/{uuid.uuid4()}",
        multipart_upload_id=str(uuid.uuid4()),
        resume_key=uuid.uuid4().hex,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        **values,
    )


@pytest.fixture
def upload_case(phase1_db):
    return seed(phase1_db)


@pytest.mark.parametrize("field,constraint", LINKS)
@pytest.mark.parametrize("operation", ["insert", "update"])
def test_upload_outputs_cannot_cross_project(upload_case, field, constraint, operation):
    c = upload_case
    own = dict(dataset_id=c.datasets[0].id, dataset_version_id=c.versions[0].id)
    foreign = c.datasets[1].id if field == "dataset_id" else c.versions[1].id
    control = upload(c, **own)
    c.db.add(control)
    c.db.flush()
    with pytest.raises(IntegrityError, match=constraint):
        with c.db.begin_nested():
            if operation == "update":
                setattr(control, field, foreign)
            else:
                c.db.add(upload(c, **{**own, field: foreign}))
            c.db.flush()
    c.db.refresh(control)
    assert control.dataset_id == own["dataset_id"]
    assert control.dataset_version_id == own["dataset_version_id"]


@pytest.mark.parametrize("links", ["none", "dataset", "version", "both"])
def test_nullable_and_same_project_upload_outputs_remain_valid(upload_case, links):
    c = upload_case
    row = upload(
        c,
        dataset_id=c.datasets[0].id if links in {"dataset", "both"} else None,
        dataset_version_id=c.versions[0].id if links in {"version", "both"} else None,
    )
    c.db.add(row)
    c.db.flush()
    c.db.refresh(row)
    assert (row.dataset_id is not None) == (links in {"dataset", "both"})
    assert (row.dataset_version_id is not None) == (links in {"version", "both"})


@pytest.mark.parametrize("parent", [Dataset, DatasetVersion])
def test_parent_deletion_clears_only_upload_references(upload_case, parent):
    c = upload_case
    row = upload(
        c,
        dataset_id=c.datasets[0].id,
        dataset_version_id=c.versions[0].id,
        status="ready",
        confirmed_bytes=1,
        completed_at=datetime.now(UTC),
        completed_object_uri="s3://no-store-call/completed",
        provider_state={"retained": "completion-metadata"},
    )
    c.db.add(row)
    c.db.flush()
    before = {column.name: getattr(row, column.name) for column in row.__table__.columns}
    identity = c.datasets[0].id if parent is Dataset else c.versions[0].id
    c.db.execute(delete(parent).where(parent.id == identity))
    c.db.refresh(row)
    expected = {**before, "dataset_version_id": None}
    if parent is Dataset:
        expected["dataset_id"] = None
    assert {column.name: getattr(row, column.name) for column in row.__table__.columns} == expected


@pytest.mark.parametrize("bad_field", [None, "dataset_id", "dataset_version_id"])
def test_upload_project_migration_preserves_history_or_rejects_it_atomically(bad_field):
    source = make_url(os.environ["DATABASE_URL"])
    assert source.database.endswith("_tests")
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = "sceptre_phase1_rehearsal_upload_" + uuid.uuid4().hex
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
        result = migrate("upgrade", "0016_trial_parent_integrity")
        assert result.returncode == 0, result.stderr
        with Session(engine) as db, db.begin():
            c = seed(db)
            own = dict(dataset_id=c.datasets[0].id, dataset_version_id=c.versions[0].id)
            values = dict(own)
            if bad_field:
                values[bad_field] = (
                    c.datasets[1].id if bad_field == "dataset_id" else c.versions[1].id
                )
            row = upload(c, **values)
            db.add(row)
            db.flush()
            row_id = row.id
        with engine.connect() as db:
            tables = [
                table for table in inspect(db).get_table_names() if table != "alembic_version"
            ]
            before = rehearsal._snapshot_seeded_rows(db, tables)
            old_fks = inspect(db).get_foreign_keys("dataset_upload_sessions")
            old_indexes = inspect(db).get_indexes("dataset_upload_sessions")
        if bad_field:
            result = migrate("upgrade", "0017_upload_session_project")
            assert result.returncode != 0
            assert dict(LINKS)[bad_field] in result.stderr
            with engine.connect() as db:
                assert (
                    db.scalar(text("SELECT version_num FROM alembic_version"))
                    == "0016_trial_parent_integrity"
                )
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                assert inspect(db).get_foreign_keys("dataset_upload_sessions") == old_fks
                assert inspect(db).get_indexes("dataset_upload_sessions") == old_indexes
            # Restore only this deliberately corrupt disposable fixture to exercise resumption.
            with engine.begin() as db:
                db.execute(
                    text(f"UPDATE dataset_upload_sessions SET {bad_field}=:parent WHERE id=:id"),
                    {"parent": own[bad_field], "id": row_id},
                )
            with engine.connect() as db:
                before = rehearsal._snapshot_seeded_rows(db, tables)
        for action, revision in (
            ("upgrade", "0017_upload_session_project"),
            ("downgrade", "0016_trial_parent_integrity"),
            ("upgrade", "0017_upload_session_project"),
        ):
            result = migrate(action, revision)
            assert result.returncode == 0, result.stderr
            with engine.connect() as db:
                assert db.scalar(text("SELECT version_num FROM alembic_version")) == revision
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                if action == "downgrade":
                    assert inspect(db).get_foreign_keys("dataset_upload_sessions") == old_fks
                    assert inspect(db).get_indexes("dataset_upload_sessions") == old_indexes
    finally:
        engine.dispose()
        drop_database(admin, name)
        admin.dispose()
