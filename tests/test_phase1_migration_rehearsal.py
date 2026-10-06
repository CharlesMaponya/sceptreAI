import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import ObjectStoreType
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

spec = importlib.util.spec_from_file_location(
    "migration_rehearsal", Path("scripts/rehearse_phase1_migrations.py")
)
rehearsal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rehearsal)
REVISION = "0004_resumable_dataset_uploads"


def test_rehearsal_covers_every_prior_schema():
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))
    heads = set(scripts.get_heads())
    prior = {item.revision for item in scripts.walk_revisions()} - heads
    assert set(rehearsal.SUPPORTED_STARTS) == prior


def drop_database(admin, name):
    assert name.startswith("sceptre_phase1_rehearsal_")
    with admin.connect() as connection:
        connection.execute(
            text(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = :name AND pid <> pg_backend_pid()"
            ),
            {"name": name},
        )
        connection.execute(text(f'DROP DATABASE "{name}"'))


@pytest.fixture(scope="module")
def migrated_template():
    source = make_url(os.environ["DATABASE_URL"])
    assert source.database.endswith("_tests")
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = "sceptre_phase1_rehearsal_probe_" + uuid.uuid4().hex
    previous = rehearsal.UPLOAD_SEED_ROWS
    rehearsal.UPLOAD_SEED_ROWS = 4
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    try:
        rehearsal._run(target, rehearsal.sys.executable, "-m", "alembic", "upgrade", REVISION)
        expected = rehearsal._seed_legacy_rows(target, REVISION)
        rehearsal._run(target, rehearsal.sys.executable, "-m", "alembic", "upgrade", "head")
        yield admin, source, name, expected
    finally:
        drop_database(admin, name)
        admin.dispose()
        rehearsal.UPLOAD_SEED_ROWS = previous


@pytest.fixture
def migrated_case(migrated_template):
    admin, source, template, expected = migrated_template
    name = "sceptre_phase1_rehearsal_probe_" + uuid.uuid4().hex
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{template}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    try:
        rehearsal._verify_seeded_rows(target, expected)
        yield target, expected
    finally:
        drop_database(admin, name)


@pytest.mark.parametrize(
    "change",
    [
        "UPDATE users SET email = 'changed@example.test'",
        "UPDATE projects SET name = 'changed project'",
        "UPDATE dataset_upload_sessions SET byte_size = byte_size + 1 "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET id = '00000000-0000-0000-0000-000000000001' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET object_key = 'changed/object' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET tags = '{\"changed\": true}'::jsonb "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET expires_at = expires_at + interval '1 day' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET provider_driver = 'zz_wrong_driver' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET protocol = 'zz_wrong_protocol' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET content_policy_revision = NULL "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET confirmed_bytes = 1 "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET provider_state = '{\"bad\": 1}'::jsonb "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET legal_hold = true "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET scanner_name = 'unexpected' "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "UPDATE dataset_upload_sessions SET quarantine_reason = NULL "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
        "DELETE FROM dataset_upload_sessions "
        "WHERE id = (SELECT id FROM dataset_upload_sessions ORDER BY id LIMIT 1)",
    ],
)
def test_detects_migration_data_corruption(migrated_case, change):
    target, expected = migrated_case
    engine = create_engine(target)
    try:
        with engine.begin() as connection:
            connection.execute(text(change))
    finally:
        engine.dispose()
    with pytest.raises(RuntimeError):
        rehearsal._verify_seeded_rows(target, expected)


@pytest.mark.parametrize("count", [0, -1])
def test_rejects_empty_campaign_before_database_work(monkeypatch, count):
    monkeypatch.setattr(rehearsal, "UPLOAD_SEED_ROWS", count)
    monkeypatch.setattr(
        rehearsal, "create_engine", lambda *a, **kw: pytest.fail("Database touched")
    )
    with pytest.raises(ValueError, match="must be positive"):
        rehearsal.main()
    with pytest.raises(ValueError, match="must be positive"):
        rehearsal._seed_legacy_rows("unused", REVISION)


def test_seed_batches_cover_requested_count(migrated_case, monkeypatch):
    target, _ = migrated_case
    engine = create_engine(target)
    try:
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE users CASCADE"))
        monkeypatch.setattr(rehearsal, "UPLOAD_SEED_ROWS", 1001)
        expected = rehearsal._seed_legacy_rows(target, REVISION)
        assert {table: snapshot["rows"] for table, snapshot in expected.items()} == {
            "users": 1,
            "projects": 1,
            "dataset_upload_sessions": 1001,
        }
        rehearsal._verify_seeded_rows(target, expected)
        with engine.connect() as connection:
            assert dict(
                connection.execute(
                    text("SELECT status, count(*) FROM dataset_upload_sessions GROUP BY status")
                ).all()
            ) == {"pending": 501, "completed": 500}
    finally:
        engine.dispose()


def test_failed_verification_cleans_up_database(migrated_template, monkeypatch):
    admin, _, template, _ = migrated_template

    def databases():
        with admin.connect() as connection:
            return set(
                connection.execute(
                    text(
                        "SELECT datname FROM pg_database "
                        "WHERE datname LIKE 'sceptre_phase1_rehearsal_%'"
                    )
                ).scalars()
            )

    verify = rehearsal._verify_seeded_rows
    created = []

    def corrupt_then_verify(target, expected):
        created.append(make_url(target).database)
        engine = create_engine(target)
        try:
            with engine.begin() as connection:
                connection.execute(text("UPDATE users SET sso_subject = 'unexpected-identity'"))
        finally:
            engine.dispose()
        verify(target, expected)

    monkeypatch.setattr(rehearsal, "SUPPORTED_STARTS", ("0001_initial",))
    monkeypatch.setattr(rehearsal, "_verify_seeded_rows", corrupt_then_verify)
    with pytest.raises(RuntimeError, match="incorrect backfill: sso_subject"):
        rehearsal.main()
    remaining = databases()
    assert len(created) == 1
    assert created[0] not in remaining
    assert template in remaining


def test_storage_values_remain_readable_after_actual_migration(migrated_template):
    admin, source, _, _ = migrated_template
    name = "sceptre_phase1_rehearsal_probe_" + uuid.uuid4().hex
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(target)
    try:
        rehearsal._run(
            target,
            rehearsal.sys.executable,
            "-m",
            "alembic",
            "upgrade",
            "0007_phase1_attempt_lineage",
        )
        rehearsal._seed_legacy_rows(target, "0007_phase1_attempt_lineage")
        expected = {}
        with engine.begin() as connection:
            user = connection.scalar(text("SELECT id FROM users"))
            project = connection.scalar(text("SELECT id FROM projects"))
            dataset = uuid.uuid4()
            connection.execute(
                text(
                    "INSERT INTO datasets (id, project_id, created_by_id, name) "
                    "VALUES (:id, :project, :user, 'storage migration')"
                ),
                {"id": dataset, "project": project, "user": user},
            )
            for index, spelling in enumerate(
                ["MINIO", "S3", "AZURE", "GCS", "minio", "s3", "azure", "gcs"], 1
            ):
                identity = uuid.uuid4()
                uri = f"legacy://unchanged/{identity}"
                mapped = {"minio": "s3_compatible", "s3": "aws_s3", "azure": "azure_blob"}.get(
                    spelling, spelling
                )
                expected[identity] = (
                    ObjectStoreType[mapped] if mapped.isupper() else ObjectStoreType(mapped),
                    uri,
                )
                connection.execute(
                    text(
                        "INSERT INTO dataset_versions (id, project_id, dataset_id, created_by_id, "
                        "version_number, status, format, object_store_type, object_uri, "
                        "content_hash, "
                        "byte_size) VALUES (:id, :project, :dataset, :user, :number, 'UPLOADED', "
                        "'CSV', :driver, :uri, :digest, 10737418240)"
                    ),
                    {
                        "id": identity,
                        "project": project,
                        "dataset": dataset,
                        "user": user,
                        "number": index,
                        "driver": spelling,
                        "uri": uri,
                        "digest": "a" * 64,
                    },
                )
        rehearsal._run(target, rehearsal.sys.executable, "-m", "alembic", "upgrade", "head")
        with Session(engine) as db:
            for identity, (driver, uri) in expected.items():
                version = db.get(DatasetVersion, identity)
                assert version.object_store_type is driver
                assert version.object_uri == uri
                assert version.content_hash == "a" * 64
                assert version.byte_size == 10737418240
    finally:
        engine.dispose()
        drop_database(admin, name)
