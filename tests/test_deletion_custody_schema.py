"""Deletion history must persist after its live project and commands disappear."""

import json
import os
import subprocess
import sys
import time
import uuid
from types import SimpleNamespace

import pytest
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowCommand,
)
from automl_api.services import deletion
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session


def test_deletion_stage_can_retain_structured_native_custody():
    value = {
        "protocol": "deletion.stage.v1",
        "phase": "recorded",
        "receipt": {"native_uid": "original"},
    }
    stage = DeletionStage(
        project_id=uuid.uuid4(), tombstone_id=uuid.uuid4(), resource_class="runtime", custody=value
    )
    assert stage.custody == value


HEAD = "0023_private_buffered_chunks"
PREVIOUS = "0018_attempt_predecessor_lineage"
PREFIX = "sceptre_phase1_deletion_schema_"


def _migration(url, *args, success=True):
    assert make_url(url).host == "127.0.0.1" and make_url(url).database.startswith(PREFIX)
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        env={**os.environ, "DATABASE_URL": url},
        capture_output=True,
        text=True,
        timeout=45,
    )
    if success:
        assert result.returncode == 0, result.stderr[-1800:]
    return result


def _legacy(engine):
    with engine.connect() as db:
        return list(
            db.execute(
                text("SELECT to_jsonb(s)-'custody' FROM deletion_stages s ORDER BY id")
            ).scalars()
        )


def _seed(engine):
    with Session(engine) as db, db.begin():
        user = User(email=f"deletion-schema-{uuid.uuid4().hex}@example.test", password_hash="test")
        db.add(user)
        db.flush()
        project = Project(owner_id=user.id, created_by_id=user.id, name="Legacy deletion metadata")
        db.add(project)
        db.flush()
        tombstone = DeletionTombstone(
            project_id=project.id,
            requested_by_id=user.id,
            resource_type="project",
            resource_id=project.id,
            reason="preserve original reason",
        )
        db.add(tombstone)
        db.flush()
        for name, status in [("runtime", "completed"), ("object_store", "pending")]:
            db.execute(
                text(
                    "INSERT INTO deletion_stages "
                    "(id,project_id,tombstone_id,resource_class,status,"
                    "retry_count,terminal_reason) "
                    "VALUES (:id,:project,:tombstone,:name,:status,2,'historical unchanged')"
                ),
                dict(
                    id=uuid.uuid4(),
                    project=project.id,
                    tombstone=tombstone.id,
                    name=name,
                    status=status,
                ),
            )


@pytest.fixture(scope="module")
def custody_template():
    source = make_url(os.environ["DATABASE_URL"])
    assert source.host == "127.0.0.1" and source.database == "automl_branch_0e6a232bc7_tests"
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = PREFIX + "template_" + uuid.uuid4().hex
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}"'))
    url = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(url)
    try:
        _migration(url, "upgrade", PREVIOUS)
        _seed(engine)
        legacy = _legacy(engine)
        _migration(url, "upgrade", "head")
        engine.dispose()
        yield SimpleNamespace(admin=admin, source=source, name=name, legacy=legacy)
    finally:
        engine.dispose()
        with admin.connect() as db:
            db.execute(text(f'DROP DATABASE "{name}"'))
        admin.dispose()


@pytest.fixture
def custody_case(custody_template):
    c = custody_template
    name = PREFIX + uuid.uuid4().hex
    with c.admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}" TEMPLATE "{c.name}"'))
    url = c.source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(url)
    try:
        yield SimpleNamespace(engine=engine, url=url, legacy=c.legacy, name=name, admin=c.admin)
    finally:
        engine.dispose()
        with c.admin.connect() as db:
            db.execute(text(f'DROP DATABASE "{name}"'))


def test_expansion_preserves_legacy_stage_fields_without_fabricating_receipts(custody_case):
    c = custody_case
    assert _legacy(c.engine) == c.legacy
    with Session(c.engine) as db:
        stages = list(db.scalars(select(DeletionStage)))
        assert len(stages) == 2 and all(s.custody == {} for s in stages)
        assert {s.status for s in stages} == {"pending", "completed"}


def test_current_schema_strictly_matches_and_preserves_all_large_byte_types(custody_case):
    from scripts.verify_database_schema import schema_differences

    c = custody_case
    assert schema_differences(c.engine) == []
    pairs = [
        ("dataset_versions", "byte_size"),
        ("dataset_upload_sessions", "byte_size"),
        ("dataset_upload_sessions", "part_size"),
        ("dataset_upload_sessions", "confirmed_bytes"),
        ("run_artifacts", "byte_size"),
        ("prepared_artifacts", "byte_size"),
        ("capacity_reservations", "memory_bytes"),
        ("workflow_attempt_credential_grants", "byte_limit"),
    ]
    with c.engine.connect() as db:
        for table, column in pairs:
            assert (
                db.scalar(
                    text(
                        "SELECT data_type FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name=:table AND column_name=:column"
                    ),
                    dict(table=table, column=column),
                )
                == "bigint"
            )
        assert db.scalar(text("SELECT version_num FROM alembic_version")) == HEAD


@pytest.mark.parametrize("value", ["null", "[]", "1", '"not-an-object"'])
def test_stage_custody_rejects_nonobject_json_at_database_boundary(custody_case, value):
    c = custody_case
    with pytest.raises(IntegrityError) as denied:
        with c.engine.begin() as db:
            db.execute(
                text("UPDATE deletion_stages SET custody=CAST(:value AS jsonb)"), {"value": value}
            )
    assert denied.value.orig.sqlstate == "23514"
    assert _legacy(c.engine) == c.legacy


@pytest.mark.parametrize("phase", ["issued", "recorded", "published"])
def test_downgrade_refuses_to_erase_any_nonempty_native_history(custody_case, phase):
    c = custody_case
    value = dict(protocol="deletion.stage.v1", phase=phase, native_uid="original")
    with c.engine.begin() as db:
        db.execute(
            text("UPDATE deletion_stages SET custody=CAST(:value AS jsonb)"),
            {"value": json.dumps(value)},
        )
    result = _migration(c.url, "downgrade", PREVIOUS, success=False)
    assert result.returncode != 0 and "custody must be preserved" in result.stderr
    with c.engine.connect() as db:
        assert db.scalar(text("SELECT version_num FROM alembic_version")) == HEAD
        assert list(db.execute(text("SELECT custody FROM deletion_stages")).scalars()) == [
            value,
            value,
        ]
    assert _legacy(c.engine) == c.legacy


def test_empty_custody_can_downgrade_and_upgrade_without_legacy_data_loss(custody_case):
    c = custody_case
    _migration(c.url, "downgrade", PREVIOUS)
    assert "custody" not in {v["name"] for v in inspect(c.engine).get_columns("deletion_stages")}
    assert _legacy(c.engine) == c.legacy
    _migration(c.url, "upgrade", "head")
    assert _legacy(c.engine) == c.legacy
    with c.engine.connect() as db:
        assert list(db.execute(text("SELECT custody FROM deletion_stages")).scalars()) == [{}, {}]


def test_downgrade_locks_out_concurrent_custody_writes_before_its_empty_check(custody_case):
    c = custody_case
    writer = c.engine.connect()
    transaction = writer.begin()
    writer.execute(text('UPDATE deletion_stages SET custody=\'{"phase":"recorded"}\'::jsonb'))
    child = subprocess.Popen(
        [sys.executable, "-m", "alembic", "downgrade", PREVIOUS],
        env={**os.environ, "DATABASE_URL": c.url},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15
        waiting = False
        while time.monotonic() < deadline and child.poll() is None:
            with c.admin.connect() as db:
                waiting = db.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname=:name AND wait_event_type='Lock' "
                        "AND query LIKE 'LOCK TABLE deletion_stages IN ACCESS EXCLUSIVE MODE%')"
                    ),
                    {"name": c.name},
                )
            if waiting:
                break
            time.sleep(0.05)
        assert waiting and child.poll() is None
        transaction.commit()
        _, stderr = child.communicate(timeout=20)
        assert child.returncode != 0 and "custody must be preserved" in stderr
        with c.engine.connect() as db:
            assert db.scalar(text("SELECT version_num FROM alembic_version")) == HEAD
            assert db.scalar(text("SELECT custody FROM deletion_stages LIMIT 1")) == {
                "phase": "recorded"
            }
    finally:
        if transaction.is_active:
            transaction.rollback()
        writer.close()
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=10)


def test_structured_custody_survives_committed_project_command_and_outbox_erasure(custody_case):
    c = custody_case
    with Session(c.engine) as db, db.begin():
        user = User(email=f"deletion-custody-{uuid.uuid4().hex}@example.test", password_hash="test")
        db.add(user)
        db.flush()
        project = Project(owner_id=user.id, created_by_id=user.id, name="Erase live metadata only")
        db.add(project)
        db.flush()
        tombstone = DeletionTombstone(
            project_id=project.id,
            requested_by_id=user.id,
            resource_type="project",
            resource_id=project.id,
            reason="owned fixture",
        )
        db.add(tombstone)
        db.flush()
        value = dict(
            protocol="deletion.stage.v1",
            phase="recorded",
            source_project=str(project.id),
            receipt=dict(native_uid="original-uid", absent=True),
        )
        stage = DeletionStage(
            project_id=project.id,
            tombstone_id=tombstone.id,
            resource_class="runtime",
            custody=value,
        )
        command = WorkflowCommand(
            project_id=project.id,
            actor_id=user.id,
            operation="project.delete",
            idempotency_key=uuid.uuid4().hex,
            request_hash="owned-test-request",
            request_payload={},
        )
        db.add_all([stage, command])
        db.flush()
        entry = OutboxEntry(
            project_id=project.id,
            command_id=command.id,
            topic="project.delete",
            event_key="owned-deletion-schema-fixture",
            aggregate_type="project",
            aggregate_id=project.id,
            payload={},
        )
        db.add(entry)
        db.flush()
        ids = project.id, command.id, entry.id, stage.id, tombstone.id
        requester_id = user.id
        deletion._delete_metadata(db, entry, project, [])
    with Session(c.engine) as db:
        project_id, command_id, entry_id, stage_id, tombstone_id = ids
        assert db.get(Project, project_id) is None
        assert db.get(WorkflowCommand, command_id) is None and db.get(OutboxEntry, entry_id) is None
        assert db.get(DeletionStage, stage_id).custody == value
        assert db.get(DeletionTombstone, tombstone_id).requested_by_id == requester_id


def test_rehearsal_catalog_contains_every_supported_prior_schema():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    from scripts.rehearse_phase1_migrations import SUPPORTED_STARTS

    scripts = ScriptDirectory.from_config(Config("alembic.ini"))
    assert set(scripts.get_heads()) == {HEAD}
    assert set(SUPPORTED_STARTS) == {r.revision for r in scripts.walk_revisions()} - {HEAD}
