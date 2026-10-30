"""Private part integrity and whole-stream boundaries are distinct, explicit checks."""

import hashlib
import uuid

import pytest
from automl_api.models.buffered_uploads import BufferedUploadChunk, BufferedUploadInput
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import buffered_chunks
from automl_api.services.workflow_state import begin_command, canonical_request_hash
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.exc import IntegrityError
from test_buffered_deletion import case as deletion_case
from test_buffered_execution import pending as pending
from test_buffered_input_admission import admission as base_admission
from test_buffered_input_schema import private_row as private_row
from test_buffered_input_schema import schema_lab as schema_lab
from test_buffered_retirement import case as case
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine

from scripts.verify_database_schema import schema_differences

__all__ = ["base_admission", "deletion_case"]

BODY = b"feature,target\nprivate-value,1\n"


def stage(db, base, *, omit=False, reorder=False, count=None, manifest=None, content_sha=None):
    original = db.get(WorkflowCommand, base.command_id)
    command, _ = begin_command(
        db,
        project_id=base.project_id,
        actor_id=original.actor_id,
        operation="dataset.upload.complete",
        idempotency_key="chunk-" + uuid.uuid4().hex,
        payload={},
    )
    parts = [BODY[i : i + 11] for i in range(0, len(BODY), 11)]
    metadata = [(len(part), hashlib.sha256(part).hexdigest()) for part in parts]
    for ordinal, part in enumerate(reversed(parts) if reorder else parts):
        if omit and ordinal == len(parts) - 1:
            continue
        db.execute(
            insert(BufferedUploadChunk).values(
                command_id=command.id,
                project_id=base.project_id,
                ordinal=ordinal,
                byte_size=len(part),
                content_sha256=hashlib.sha256(part).hexdigest(),
                content=part,
            )
        )
    row = BufferedUploadInput(
        command_id=command.id,
        project_id=base.project_id,
        content=None,
        byte_size=len(BODY),
        content_sha256=content_sha or hashlib.sha256(BODY).hexdigest(),
        manifest={"source": str(command.id)},
        manifest_digest=canonical_request_hash({"source": str(command.id)}),
        chunk_count=len(parts) if count is None else count,
        chunk_manifest_sha256=buffered_chunks.manifest_hash(metadata)
        if manifest is None
        else manifest,
    )
    db.add(row)
    db.flush()
    return row


def validate_deferred(db):
    db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    db.execute(text("SET CONSTRAINTS ALL DEFERRED"))


def test_complete_private_parts_reconstruct_exact_stream_and_preserve_inline_source(private_row):
    db, base = private_row
    row = stage(db, base)
    validate_deferred(db)
    assert row.content is None and base.content is not None
    assert b"".join(buffered_chunks.load_parts(db, row)) == BODY
    buffered_chunks.validate_layout(db, row)
    assert hashlib.sha256(BODY).hexdigest() == row.content_sha256


@pytest.mark.parametrize("mutation", ["omit", "reorder", "count", "manifest"])
def test_database_rejects_incomplete_or_changed_ordered_part_layout(private_row, mutation):
    db, base = private_row
    arguments = (
        {"omit": True}
        if mutation == "omit"
        else {"reorder": True}
        if mutation == "reorder"
        else {"count": 8}
        if mutation == "count"
        else {"manifest": "f" * 64}
    )
    with pytest.raises(IntegrityError), db.begin_nested():
        stage(db, base, **arguments)
        validate_deferred(db)


@pytest.mark.parametrize(
    "field", ["content", "ordinal", "byte_size", "content_sha256", "project_id", "command_id"]
)
def test_private_parts_cannot_be_updated_after_admission(private_row, field):
    db, base = private_row
    row = stage(db, base)
    validate_deferred(db)
    value = (
        uuid.uuid4()
        if field in {"project_id", "command_id"}
        else b"changed"
        if field == "content"
        else 7
        if field in {"ordinal", "byte_size"}
        else "a" * 64
    )
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            update(BufferedUploadChunk)
            .where(
                BufferedUploadChunk.command_id == row.command_id, BufferedUploadChunk.ordinal == 0
            )
            .values(**{field: value})
        )
    assert b"".join(buffered_chunks.load_parts(db, row)) == BODY


@pytest.mark.parametrize("action", ["delete", "append"])
def test_admitted_private_parts_cannot_be_removed_or_appended(private_row, action):
    db, base = private_row
    row = stage(db, base)
    validate_deferred(db)
    with pytest.raises(IntegrityError), db.begin_nested():
        if action == "delete":
            db.execute(
                delete(BufferedUploadChunk).where(BufferedUploadChunk.command_id == row.command_id)
            )
        else:
            db.execute(
                insert(BufferedUploadChunk).values(
                    command_id=row.command_id,
                    project_id=row.project_id,
                    ordinal=3,
                    byte_size=1,
                    content=b"x",
                    content_sha256=hashlib.sha256(b"x").hexdigest(),
                )
            )


@pytest.mark.parametrize("field", ["chunk_count", "chunk_manifest_sha256"])
def test_original_part_metadata_cannot_be_rebound(private_row, field):
    db, base = private_row
    row = stage(db, base)
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            update(BufferedUploadInput)
            .where(BufferedUploadInput.command_id == row.command_id)
            .values(**{field: 1 if field == "chunk_count" else "b" * 64})
        )


def test_closed_source_requires_part_clearing_in_the_same_transaction(private_row):
    db, base = private_row
    row = stage(db, base)
    validate_deferred(db)
    with pytest.raises(IntegrityError), db.begin_nested():
        row.phase = "retired"
        db.flush()
        validate_deferred(db)
    db.refresh(row)
    row.phase = "retired"
    buffered_chunks.clear_parts(db, row)
    validate_deferred(db)
    assert (
        db.scalar(
            select(BufferedUploadChunk.command_id).where(
                BufferedUploadChunk.command_id == row.command_id
            )
        )
        is None
    )
    db.delete(row)
    db.flush()


@pytest.mark.parametrize("mutation", ["defer-off", "disable", "drop", "replace-function"])
def test_strict_introspection_detects_layout_trigger_drift(private_row, mutation):
    db, _ = private_row
    validate_deferred(db)
    if mutation == "defer-off":
        db.execute(
            text("DROP TRIGGER trg_validate_buffered_chunk_layout ON buffered_upload_inputs")
        )
        db.execute(
            text(
                "CREATE TRIGGER trg_validate_buffered_chunk_layout AFTER INSERT OR UPDATE "
                "ON buffered_upload_inputs FOR EACH ROW "
                "EXECUTE FUNCTION validate_buffered_chunk_layout()"
            )
        )
    elif mutation == "disable":
        db.execute(
            text(
                "ALTER TABLE buffered_upload_inputs DISABLE TRIGGER "
                "trg_validate_buffered_chunk_layout"
            )
        )
    elif mutation == "drop":
        db.execute(
            text("DROP TRIGGER trg_preserve_buffered_chunk_custody ON buffered_upload_chunks")
        )
    else:
        db.execute(
            text(
                "CREATE OR REPLACE FUNCTION preserve_buffered_chunk_custody() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
            )
        )
    assert any("buffered_chunk" in difference for difference in schema_differences(db.connection()))


def test_chunked_history_blocks_destructive_downgrade_even_after_parts_cleared(schema_lab):
    from sqlalchemy.orm import Session
    from test_buffered_input_schema import _seed

    engine, migrate = schema_lab
    with Session(engine) as db, db.begin():
        base = _seed(db)
        row = stage(db, base)
        ids = (base.command_id, row.command_id, base.project_id)
        actor_id = db.get(WorkflowCommand, base.command_id).actor_id
    result = migrate("downgrade", "0022_private_buffered_input", success=False)
    assert result.returncode != 0 and "buffered_chunk_downgrade_preservation" in result.stderr
    with Session(engine) as db, db.begin():
        row = db.get(BufferedUploadInput, ids[1])
        row.phase = "retired"
        buffered_chunks.clear_parts(db, row)
    result = migrate("downgrade", "0022_private_buffered_input", success=False)
    assert result.returncode != 0 and "buffered_chunk_downgrade_preservation" in result.stderr
    with Session(engine) as db, db.begin():
        from automl_api.models.iam import User
        from automl_api.models.projects import Project

        row = db.get(BufferedUploadInput, ids[1])
        db.delete(row)
        base = db.get(BufferedUploadInput, ids[0])
        base.phase, base.content = "retired", None
        db.flush()
        db.delete(base)
        db.flush()
        db.execute(delete(Project).where(Project.id == ids[2]))
        db.execute(delete(User).where(User.id == actor_id))
    migrate("downgrade", "0022_private_buffered_input")
    migrate("upgrade", "head")
    with engine.connect() as db:
        assert schema_differences(db) == []


@pytest.fixture
def admission(base_admission, monkeypatch):
    monkeypatch.setattr(buffered_chunks, "PART_BYTES", 11)
    return base_admission


@pytest.fixture
def chunked(case):
    c = case
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.chunk_count > 1 and row.content is None
        assert b"".join(buffered_chunks.load_parts(db, row)) == c.body
    return c


def test_chunked_factory_uses_original_whole_hash_and_clears_parts_on_publication(chunked):
    c = chunked
    c.run()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "published" and row.content is None and row.chunk_count > 1
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is None
        )
        command = db.get(WorkflowCommand, c.identifier)
        assert (
            command.response_payload["version"]["content_hash"]
            == hashlib.sha256(c.body).hexdigest()
        )
    before = list(c.native.creates)
    c.run()
    assert c.native.creates == before and len(before) == 1


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_original_private_parts_retire_with_no_premature_clear_or_extra_native(chunked, phase):
    c = chunked
    c.prepare(phase)
    assert c.cancel()["status"] == "aborted"
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "retired" and row.chunk_count > 1
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is None
        )
    assert len(c.erases) == int(phase == "recorded")


def test_whole_stream_mismatch_holds_before_sdk_even_with_valid_part_layout(chunked, monkeypatch):
    from automl_api.services import buffered_execution
    from automl_api.storage.contracts import UploadContractError

    c = chunked
    monkeypatch.setattr(
        buffered_chunks, "load_parts", lambda *_: [b"incorrect reconstructed stream"]
    )
    with pytest.raises(UploadContractError, match="bytes changed"):
        buffered_execution.reconcile_buffered_upload(
            c.factory,
            c.entry_id,
            c.worker,
            settings=c.settings,
            store_factory=lambda *_: pytest.fail(
                "Whole-byte-stream verification precedes SDK construction"
            ),
        )
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "admitted"
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is not None
        )


@pytest.mark.parametrize("phase", ["admitted", "recorded"])
def test_private_part_retirement_rollback_keeps_all_original_parts(chunked, phase, monkeypatch):
    from automl_api.services import buffered_retirement

    c = chunked
    c.prepare(phase)
    publish = buffered_retirement._publish

    def rollback(*args):
        publish(*args)
        raise RuntimeError("Owned chunk-clear publication rollback")

    monkeypatch.setattr(buffered_retirement, "_publish", rollback)
    with pytest.raises(RuntimeError):
        c.cancel()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == phase and b"".join(buffered_chunks.load_parts(db, row)) == c.body
    monkeypatch.setattr(buffered_retirement, "_publish", publish)
    assert c.cancel()["status"] == "aborted" and len(c.erases) == int(phase == "recorded")


@pytest.mark.parametrize(
    "mode", ["before-write", "after-write", "after-fact", "backend-fact", "backend-publication"]
)
def test_chunked_process_and_backend_boundaries_preserve_original_custody(chunked, tmp_path, mode):
    from test_buffered_execution import (
        test_process_and_backend_loss_preserve_custody_without_blind_reissue as exercise,
    )

    c = chunked
    exercise(c, tmp_path, mode)
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        if row.phase == "published":
            assert (
                db.scalar(
                    select(BufferedUploadChunk.command_id).where(
                        BufferedUploadChunk.command_id == c.identifier
                    )
                )
                is None
            )
        else:
            assert b"".join(buffered_chunks.load_parts(db, row)) == c.body


@pytest.mark.parametrize(
    "mode", ["before-delete", "after-delete", "after-fact", "backend-fact", "backend-publication"]
)
def test_chunked_retirement_process_and_backend_boundaries_keep_parts_until_closed(
    chunked, tmp_path, mode
):
    from test_buffered_retirement import (
        test_process_and_backend_loss_keep_original_retirement_custody as exercise,
    )

    c = chunked
    exercise(c, tmp_path, mode)
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).phase == "retired"
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is None
        )


@pytest.mark.parametrize("phase", ["admitted", "recorded", "published"])
def test_chunked_project_deletion_keeps_parts_until_all_native_stages_qualify(deletion_case, phase):
    from test_buffered_deletion import (
        test_original_known_custody_closes_through_all_deletion_stages as exercise,
    )

    c = deletion_case
    with c.factory() as db:
        assert db.get(BufferedUploadInput, c.identifier).chunk_count > 1
    exercise(c, phase)
    with c.factory() as db:
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is None
        )


def test_chunked_publication_rollback_preserves_all_parts_and_known_original_reply(
    chunked, monkeypatch
):
    from automl_api.services import buffered_execution

    c = chunked
    publish = buffered_execution._publish

    def rollback(*args):
        publish(*args)
        raise RuntimeError("Owned chunked publication rollback")

    monkeypatch.setattr(buffered_execution, "_publish", rollback)
    with pytest.raises(RuntimeError):
        c.run()
    with c.factory() as db:
        row = db.get(BufferedUploadInput, c.identifier)
        assert row.phase == "recorded" and b"".join(buffered_chunks.load_parts(db, row)) == c.body
    monkeypatch.setattr(buffered_execution, "_publish", publish)
    c.claim()
    c.run()
    assert len(c.native.creates) == 1
    with c.factory() as db:
        assert (
            db.scalar(
                select(BufferedUploadChunk.command_id).where(
                    BufferedUploadChunk.command_id == c.identifier
                )
            )
            is None
        )


@pytest.mark.parametrize(
    "defect", ["sha", "size", "empty", "oversize", "foreign-project", "negative-ordinal"]
)
def test_database_rejects_malformed_part_payload_at_admission(private_row, defect):
    db, base = private_row
    actor_id = db.get(WorkflowCommand, base.command_id).actor_id
    with pytest.raises(IntegrityError), db.begin_nested():
        command, _ = begin_command(
            db,
            project_id=base.project_id,
            actor_id=actor_id,
            operation="dataset.upload.complete",
            idempotency_key="bad-part-" + uuid.uuid4().hex,
            payload={},
        )
        body = b"x" if defect != "oversize" else b"x" * (8 * 1024 * 1024 + 1)
        if defect == "empty":
            body = b""
        size = len(body) + (1 if defect == "size" else 0)
        sha = "f" * 64 if defect == "sha" else hashlib.sha256(body).hexdigest()
        db.execute(
            insert(BufferedUploadChunk).values(
                command_id=command.id,
                project_id=uuid.uuid4() if defect == "foreign-project" else base.project_id,
                ordinal=-1 if defect == "negative-ordinal" else 0,
                byte_size=size,
                content_sha256=sha,
                content=body,
            )
        )
        manifest = {"source": str(command.id)}
        db.add(
            BufferedUploadInput(
                command_id=command.id,
                project_id=base.project_id,
                content=None,
                byte_size=size,
                content_sha256=hashlib.sha256(body).hexdigest(),
                manifest=manifest,
                manifest_digest=canonical_request_hash(manifest),
                chunk_count=1,
                chunk_manifest_sha256=buffered_chunks.manifest_hash([(size, sha)]),
            )
        )
        db.flush()
        validate_deferred(db)


def test_chunked_birth_cannot_omit_content_manifest_hash(private_row):
    db, base = private_row
    with pytest.raises(IntegrityError), db.begin_nested():
        # Birth validation must fail independently of the deferred parent reference.
        command, _ = begin_command(
            db,
            project_id=base.project_id,
            actor_id=db.get(WorkflowCommand, base.command_id).actor_id,
            operation="dataset.upload.complete",
            idempotency_key="missing-manifest",
            payload={},
        )
        body = b"x"
        db.execute(
            insert(BufferedUploadChunk).values(
                command_id=command.id,
                project_id=base.project_id,
                ordinal=0,
                byte_size=1,
                content_sha256=hashlib.sha256(body).hexdigest(),
                content=body,
            )
        )
        manifest = {"source": str(command.id)}
        db.add(
            BufferedUploadInput(
                command_id=command.id,
                project_id=base.project_id,
                content=None,
                byte_size=1,
                content_sha256=hashlib.sha256(body).hexdigest(),
                manifest=manifest,
                manifest_digest=canonical_request_hash(manifest),
                chunk_count=1,
                chunk_manifest_sha256=None,
            )
        )
        db.flush()
        validate_deferred(db)
