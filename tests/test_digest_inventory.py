import hashlib
import io
import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import Dataset, DatasetUploadSession, DatasetVersion
from automl_api.models.enums import DatasetFormat, DatasetStatus, ObjectStoreType
from psycopg.errors import ReadOnlySqlTransaction
from sqlalchemy import event, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401

from scripts import inventory_dataset_digests as inventory_script


def _valid_record(**changes):
    return {
        "resource_type": "dataset_version",
        "algorithm": "sha256",
        "scope": "byte_stream",
        "byte_size": 4,
        "scanner_status": "allowed",
        "expected_digest": "a" * 64,
        "verified_at": datetime.now(UTC),
        "state": "verified",
        "legacy_upload_link": False,
        **changes,
    }


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"algorithm": "md5"}, "unsupported_algorithm"),
        ({"scope": "multipart_manifest"}, "legacy_manifest"),
        ({"scope": "unknown"}, "unsupported_scope"),
        ({"expected_digest": "A" * 64}, "missing_or_invalid_digest"),
        ({"expected_digest": None}, "missing_or_invalid_digest"),
        ({"state": "verified_legacy"}, "verification_not_proven"),
        ({"verified_at": None}, "verification_not_proven"),
        ({"byte_size": None}, "missing_or_invalid_byte_size"),
        ({"byte_size": True}, "missing_or_invalid_byte_size"),
        ({"byte_size": -1}, "missing_or_invalid_byte_size"),
        ({"verified_at": datetime.now()}, "verification_not_proven"),
        (
            {
                "resource_type": "upload",
                "state": "ready",
                "observed_digest": "a" * 64,
                "scanner_status": "pending",
            },
            "scanner_not_allowed",
        ),
        ({"legacy_upload_link": True}, "legacy_upload_provenance"),
        (
            {"resource_type": "upload", "state": "ready", "observed_digest": "b" * 64},
            "observed_digest_mismatch",
        ),
    ],
)
def test_inventory_classifies_review_reasons_with_valid_control(changes, reason):
    assert inventory_script.review_reasons(_valid_record()) == []
    assert reason in inventory_script.review_reasons(_valid_record(**changes))
    assert (
        inventory_script.review_reasons(
            _valid_record(resource_type="upload", state="ready", observed_digest="a" * 64)
        )
        == []
    )


def _upload(user_id, project_id, **changes):
    return DatasetUploadSession(
        project_id=project_id,
        created_by_id=user_id,
        dataset_name="inventory fixture",
        original_filename="original.csv",
        byte_size=4,
        part_size=4,
        total_parts=1,
        object_key="inventory/" + uuid.uuid4().hex,
        multipart_upload_id=uuid.uuid4().hex,
        resume_key=uuid.uuid4().hex,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        **changes,
    )


def test_postgres_inventory_preserves_rows_and_links_legacy_without_duplicates(
    phase1_engine,  # noqa: F811
    committed_tenant,  # noqa: F811
):
    user_id, project_id = committed_tenant
    now = datetime.now(UTC)
    with Session(phase1_engine) as db, db.begin():
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="inventory")
        db.add(dataset)
        db.flush()
        versions = []
        for number in (1, 2):
            version = DatasetVersion(
                project_id=project_id,
                dataset_id=dataset.id,
                created_by_id=user_id,
                version_number=number,
                status=DatasetStatus.READY,
                format=DatasetFormat.CSV,
                object_store_type=ObjectStoreType.EMBEDDED,
                object_uri=f"embedded://inventory/{number}",
                content_hash="a" * 64,
                byte_size=4,
                content_hash_algorithm="sha256",
                content_hash_scope="byte_stream",
                content_hash_verification_status="verified",
                content_hash_verified_at=now,
            )
            db.add(version)
            db.flush()
            versions.append(version.id)
        # Two legacy links must identify one affected version, not duplicate its inventory row.
        for _ in range(2):
            db.add(_upload(user_id, project_id, dataset_version_id=versions[0], status="completed"))
        db.add(
            _upload(
                user_id,
                project_id,
                dataset_version_id=versions[1],
                status="ready",
                digest_algorithm="sha256",
                digest_scope="byte_stream",
                expected_object_digest="a" * 64,
                observed_object_digest="a" * 64,
                checksum_verified_at=now,
                scanner_status="allowed",
            )
        )
    statements = []
    transaction_settings = []

    def observe(connection, cursor, statement, _parameters, context, _many):
        statements.append(statement)
        if statement.lstrip().upper().startswith("SELECT"):
            with connection.connection.cursor() as probe:
                probe.execute(
                    "SELECT current_setting('transaction_read_only'), "
                    "current_setting('transaction_isolation')"
                )
                read_only, isolation = probe.fetchone()
            transaction_settings.append(
                (read_only, isolation, context.execution_options.get("yield_per"))
            )

    event.listen(phase1_engine, "before_cursor_execute", observe)
    try:
        output = io.StringIO()
        summary = inventory_script.inventory(phase1_engine, output)
    finally:
        event.remove(phase1_engine, "before_cursor_execute", observe)
    lines = output.getvalue().splitlines(keepends=True)
    records = [json.loads(line) for line in lines]
    resources = [record for record in records if record["type"] == "resource"]
    ours = [record for record in resources if record["project_id"] == str(project_id)]
    assert len(ours) == 5
    assert len({(r["resource_type"], r["id"]) for r in ours}) == 5
    by_id = {r["id"]: r for r in ours}
    assert by_id[str(versions[0])]["review_reasons"] == ["legacy_upload_provenance"]
    assert by_id[str(versions[1])]["review_reasons"] == []
    assert all(settings[:2] == ("on", "repeatable read") for settings in transaction_settings)
    assert len(transaction_settings) == 3
    assert [settings[2] for settings in transaction_settings[1:]] == [500, 500]
    assert summary["complete"] and records[-1] == summary
    resource_lines = [line for line in lines if json.loads(line)["type"] == "resource"]
    assert (
        summary["resource_records_sha256"]
        == hashlib.sha256("".join(resource_lines).encode()).hexdigest()
    )
    assert summary["counts"]["dataset_version"] == sum(
        r["resource_type"] == "dataset_version" for r in resources
    )
    assert all(
        s.lstrip().upper().startswith(("SELECT", "SET TRANSACTION READ ONLY")) for s in statements
    )
    with Session(phase1_engine) as db:
        assert db.get(DatasetVersion, versions[0]).content_hash == "a" * 64
        uploads = list(
            db.scalars(
                select(DatasetUploadSession).where(DatasetUploadSession.project_id == project_id)
            )
        )
        assert len(uploads) == 3 and all(u.quarantine_delete_after is None for u in uploads)
        assert sum(u.digest_scope == "multipart_manifest" for u in uploads) == 2


def test_inventory_snapshot_ignores_later_commits_and_rejects_writes(
    phase1_engine,  # noqa: F811
    committed_tenant,  # noqa: F811
):
    user_id, project_id = committed_tenant
    inserted = []

    class Output(io.StringIO):
        def write(self, line):
            if json.loads(line)["type"] == "header":
                with Session(phase1_engine) as db, db.begin():
                    upload = _upload(user_id, project_id, status="completed")
                    db.add(upload)
                    db.flush()
                    inserted.append(str(upload.id))
            return super().write(line)

    output = Output()
    inventory_script.inventory(phase1_engine, output)
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert inserted and not any(r.get("id") in inserted for r in records)

    def attempt_write(_connection, cursor, statement, _parameters, _context, _many):
        if statement.lstrip().upper().startswith("SELECT txid_current_snapshot".upper()):
            cursor.execute("UPDATE dataset_upload_sessions SET status = status WHERE false")

    event.listen(phase1_engine, "before_cursor_execute", attempt_write)
    try:
        failed = io.StringIO()
        with pytest.raises(ReadOnlySqlTransaction, match="read-only"):
            inventory_script.inventory(phase1_engine, failed)
        assert '"complete": true' not in failed.getvalue()
    finally:
        event.remove(phase1_engine, "before_cursor_execute", attempt_write)


def test_cli_preserves_previous_report_and_creates_private_file(tmp_path, monkeypatch):
    target = tmp_path / "inventory.ndjson"
    calls = []
    monkeypatch.setattr(inventory_script, "get_engine", lambda: SimpleNamespace())
    monkeypatch.setattr(
        inventory_script,
        "inventory",
        lambda engine, output: (calls.append(engine), output.write("report\n")),
    )
    inventory_script.main(["--output", str(target)])
    assert target.read_text() == "report\n" and target.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        inventory_script.main(["--output", str(target)])
    assert len(calls) == 1 and target.read_text() == "report\n"


def test_inventory_rejects_non_postgres_before_connecting():
    engine = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    with pytest.raises(ValueError, match="PostgreSQL"):
        inventory_script.inventory(engine, io.StringIO())
