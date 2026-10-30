"""Private bytes and native custody survive expansion and reject destructive transitions."""

import hashlib
import os
import subprocess
import sys
import uuid

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import AuthProvider
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.services.workflow_state import begin_command, canonical_request_hash
from sqlalchemy import create_engine, delete, inspect, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from scripts.verify_database_schema import schema_differences

HEAD = "0023_private_buffered_chunks"
PREVIOUS = "0021_legacy_observer_clock"


@pytest.fixture(scope="module")
def schema_lab():
    source = make_url(os.environ["DATABASE_URL"])
    assert source.host == "127.0.0.1" and source.database == "automl_branch_0e6a232bc7_tests"
    name = "sceptre_phase1_buffered_schema_" + uuid.uuid4().hex
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}"'))
    url = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(url)

    def migrate(*args, success=True):
        result = subprocess.run(
            [sys.executable, "-m", "alembic", *args],
            env={**os.environ, "DATABASE_URL": url},
            capture_output=True,
            text=True,
            timeout=45,
        )
        if success:
            assert result.returncode == 0, result.stderr[-1600:]
        return result

    try:
        migrate("upgrade", "head")
        yield engine, migrate
    finally:
        engine.dispose()
        with admin.connect() as db:
            db.execute(text(f'DROP DATABASE "{name}"'))
        admin.dispose()


def _seed(db):
    user = User(
        email=f"buffered-schema-{uuid.uuid4().hex}@example.test", auth_provider=AuthProvider.SIMPLE
    )
    db.add(user)
    db.flush()
    project = Project(name="private schema", owner_id=user.id, created_by_id=user.id)
    db.add(project)
    db.flush()
    command, _ = begin_command(
        db,
        project_id=project.id,
        actor_id=user.id,
        operation="dataset.upload.complete",
        idempotency_key="private",
        payload={"sha256": "fixture"},
    )
    body = b"feature,target\nprivate-value,1\n"
    manifest = {"source": str(command.id)}
    row = BufferedUploadInput(
        command_id=command.id,
        project_id=project.id,
        content=body,
        byte_size=len(body),
        content_sha256=hashlib.sha256(body).hexdigest(),
        manifest=manifest,
        manifest_digest=canonical_request_hash(manifest),
    )
    db.add(row)
    db.flush()
    return row


@pytest.fixture
def private_row(schema_lab):
    engine, _ = schema_lab
    connection = engine.connect()
    transaction = connection.begin()
    with Session(bind=connection) as db:
        row = _seed(db)
        try:
            yield db, row
        finally:
            db.rollback()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


def _reject(db, row, values):
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            update(BufferedUploadInput)
            .where(BufferedUploadInput.command_id == row.command_id)
            .values(**values)
        )


@pytest.mark.parametrize(
    "values",
    [
        {"content": b"replacement"},
        {"content": None},
        {"byte_size": 0},
        {"byte_size": 1},
        {"content_sha256": "a" * 64},
        {"manifest": {"changed": True}},
        {"manifest_digest": "b" * 64},
        {"project_id": uuid.UUID(int=1)},
        {"command_id": uuid.UUID(int=2)},
        {"phase": "issued", "native_receipt": {"version": "foreign"}},
        {"phase": "recorded", "native_receipt": {"version": "unissued"}},
        {"phase": "published", "content": None, "native_receipt": {"version": "skipped"}},
        {"phase": "retired"},
        {"phase": "unknown"},
    ],
)
def test_private_input_rejects_original_source_replacement_or_skipped_custody(private_row, values):
    db, row = private_row
    _reject(db, row, values)
    assert row.phase == "admitted" and row.content.startswith(b"feature,target")


@pytest.mark.parametrize(
    "values",
    [
        {"content": b"wrong"},
        {"byte_size": 0},
        {"content_sha256": "BAD"},
        {"manifest_digest": "BAD"},
        {"manifest": []},
        {"native_receipt": []},
        {"content": None},
        {"project_id": uuid.UUID(int=1)},
        {"phase": "issued"},
        {"phase": "recorded", "native_receipt": {"version": "unissued"}},
        {"phase": "published", "content": None, "native_receipt": {"version": "unissued"}},
        {"phase": "retired", "content": None},
    ],
)
def test_private_input_insert_checks_exact_bytes_and_ownership(private_row, values):
    db, source = private_row
    from automl_api.models.workflows import WorkflowCommand

    command = db.get(WorkflowCommand, source.command_id)
    other, _ = begin_command(
        db,
        project_id=command.project_id,
        actor_id=command.actor_id,
        operation=command.operation,
        idempotency_key="other",
        payload={},
    )
    data = dict(
        command_id=other.id,
        project_id=source.project_id,
        content=source.content,
        byte_size=source.byte_size,
        content_sha256=source.content_sha256,
        manifest=source.manifest,
        manifest_digest=source.manifest_digest,
    )
    data.update(values)
    with pytest.raises(IntegrityError), db.begin_nested():
        db.add(BufferedUploadInput(**data))
        db.flush()


def test_private_input_retains_write_once_fact_then_clears_bytes_after_publication(private_row):
    db, row = private_row
    row.phase = "issued"
    db.flush()
    _reject(db, row, {"phase": "admitted"})
    _reject(db, row, {"phase": "retired", "content": None})
    row.phase, row.native_receipt = "recorded", {"version": "original"}
    db.flush()
    _reject(db, row, {"native_receipt": {"version": "replacement"}})
    _reject(db, row, {"native_receipt": {}})
    row.phase, row.content = "published", None
    db.flush()
    assert row.native_receipt == {"version": "original"}
    _reject(db, row, {"content": b"feature,target\nprivate-value,1\n"})
    row.phase = "retired"
    db.flush()
    _reject(db, row, {"phase": "published"})
    db.delete(row)
    db.flush()


@pytest.mark.parametrize("phase", ["admitted", "issued", "recorded", "published"])
def test_unretired_input_blocks_direct_and_project_cascade_deletion(private_row, phase):
    db, row = private_row
    if phase != "admitted":
        row.phase = "issued"
        db.flush()
    if phase in {"recorded", "published"}:
        row.phase, row.native_receipt = "recorded", {"version": "original"}
        db.flush()
    if phase == "published":
        row.phase, row.content = "published", None
        db.flush()
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            delete(BufferedUploadInput).where(BufferedUploadInput.command_id == row.command_id)
        )
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(delete(Project).where(Project.id == row.project_id))


@pytest.mark.parametrize("mutation", ["disable", "drop", "replace-function"])
def test_strict_schema_detects_changed_private_custody_trigger(private_row, mutation):
    db, _ = private_row
    db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    assert schema_differences(db.connection()) == []
    commands = {
        "disable": """ALTER TABLE buffered_upload_inputs
            DISABLE TRIGGER trg_preserve_buffered_input_custody""",
        "drop": "DROP TRIGGER trg_preserve_buffered_input_custody ON buffered_upload_inputs",
        "replace-function": (
            "CREATE OR REPLACE FUNCTION preserve_buffered_input_custody() RETURNS trigger "
            "LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
        ),
    }
    db.execute(text(commands[mutation]))
    assert any(
        "trg_preserve_buffered_input_custody" in item
        for item in schema_differences(db.connection())
    )


@pytest.mark.parametrize("phase", ["admitted", "issued", "recorded", "published", "retired"])
def test_buffered_schema_empty_rollback_and_populated_guard(schema_lab, phase):
    engine, migrate = schema_lab
    with Session(engine) as db:
        row = _seed(db)
        command_id, project_id = row.command_id, row.project_id
        from automl_api.models.workflows import WorkflowCommand

        actor_id = db.get(WorkflowCommand, command_id).actor_id
        if phase in {"issued", "recorded", "published"}:
            row.phase = "issued"
            db.flush()
        if phase in {"recorded", "published"}:
            row.phase, row.native_receipt = "recorded", {"version": "synthetic-schema-proof"}
            db.flush()
        if phase in {"published", "retired"}:
            row.phase, row.content = phase, None
        db.commit()
    failed = migrate("downgrade", PREVIOUS, success=False)
    assert failed.returncode != 0 and "buffered_input_downgrade_preservation" in failed.stderr
    with Session(engine) as db, db.begin():
        assert db.scalar(text("SELECT version_num FROM alembic_version")) == HEAD
        row = db.get(BufferedUploadInput, command_id)
        assert row.phase == phase
        assert (row.content is None) == (phase in {"published", "retired"})
        if row.phase == "issued":
            row.phase, row.native_receipt = "recorded", {"version": "synthetic-schema-proof"}
            db.flush()
        row.phase, row.content = "retired", None
        db.flush()
        db.delete(row)
        db.flush()
        db.execute(delete(Project).where(Project.id == project_id))
        db.execute(delete(User).where(User.id == actor_id))
    migrate("downgrade", PREVIOUS)
    assert not inspect(engine).has_table("buffered_upload_inputs")
    migrate("upgrade", "head")
    assert schema_differences(engine) == []
