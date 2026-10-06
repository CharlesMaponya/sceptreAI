#!/usr/bin/env python3
"""Rehearse every checked-in prior schema in isolated PostgreSQL databases."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import uuid
from itertools import batched

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

SUPPORTED_STARTS = (
    "0001_initial",
    "0002_expand_artifact_kind",
    "0003_security_controls",
    "0004_resumable_dataset_uploads",
    "0005_phase1_durable_workflows",
    "0006_phase1_public_contracts",
    "0007_phase1_attempt_lineage",
    "0008_phase2_cloud_ingestion",
    "0009_phase2_storage_default",
)
UPLOAD_SEED_ROWS = int(os.environ.get("PHASE1_REHEARSAL_UPLOAD_ROWS", "10000"))

# Expected migration policy, independent of the database's resulting defaults.
UPLOAD_BACKFILLS = {
    "digest_algorithm": "sha256",
    "digest_scope": "multipart_manifest",
    "retry_count": 0,
    "retry_budget": 5,
    "upload_kind": "dataset",
    "provider_driver": "s3_compatible",
    "protocol": "multipart",
    "provider_state": {},
    "transfer_receipts": [],
    "instruction_state": {},
    "content_type": "application/octet-stream",
    "sensitivity": "internal",
    "data_region": "local",
    "legal_hold": False,
    "scanner_status": "pending",
    "scanner_evidence": {},
    "content_policy_revision": "legacy-v1",
    "target_metadata": {},
    **dict.fromkeys(
        (
            "expected_object_digest",
            "observed_object_digest",
            "lease_owner",
            "lease_expires_at",
            "heartbeat_at",
            "replayed_by",
            "retention_until",
            "provider_checksum",
            "completed_object_uri",
            "checksum_verified_at",
            "last_progress_at",
            "aborted_at",
            "quarantine_delete_after",
            "scanner_name",
            "scanner_version",
            "scanner_signature_version",
        )
    ),
}


def _fingerprint(connection, table, columns):
    quote = connection.dialect.identifier_preparer.quote
    query = text(
        f"SELECT {', '.join(quote(name) for name in columns)} FROM {quote(table)} ORDER BY id"
    )
    digest = hashlib.sha256()
    count = 0
    with connection.execution_options(yield_per=1000).execute(query) as rows:
        for row in rows:
            digest.update(json.dumps(list(row), sort_keys=True, default=str).encode())
            digest.update(b"\n")
            count += 1
    return {"columns": columns, "rows": count, "sha256": digest.hexdigest()}


def _snapshot_seeded_rows(connection, tables):
    inspector = inspect(connection)
    return {
        table: _fingerprint(
            connection, table, sorted(column["name"] for column in inspector.get_columns(table))
        )
        for table in tables
    }


def _run(database_url: str, *arguments: str) -> None:
    environment = {**os.environ, "DATABASE_URL": database_url}
    subprocess.run(arguments, env=environment, check=True)  # noqa: S603


def main() -> int:
    if UPLOAD_SEED_ROWS < 1:
        raise ValueError("PHASE1_REHEARSAL_UPLOAD_ROWS must be positive")
    source = make_url(os.environ["DATABASE_URL"])
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        prefix = f"sceptre_phase1_rehearsal_{uuid.uuid4().hex[:10]}"
        for index, revision in enumerate(SUPPORTED_STARTS):
            database_name = f"{prefix}_{index}"
            if not database_name.startswith("sceptre_phase1_rehearsal_"):
                raise RuntimeError("Refusing to operate on a non-rehearsal database.")
            with admin.connect() as connection:
                connection.execute(text(f'CREATE DATABASE "{database_name}"'))
            target_url = source.set(database=database_name).render_as_string(hide_password=False)
            try:
                _run(target_url, sys.executable, "-m", "alembic", "upgrade", revision)
                expected = _seed_legacy_rows(target_url, revision)
                _run(target_url, sys.executable, "-m", "alembic", "upgrade", "head")
                _verify_seeded_rows(target_url, expected)
                _run(target_url, sys.executable, "scripts/verify_database_schema.py")
                _run(target_url, sys.executable, "-m", "alembic", "check")
                print(f"migration rehearsal passed: {revision} -> head", flush=True)
                print(
                    json.dumps({"start": revision, "preserved": expected}, sort_keys=True),
                    flush=True,
                )
            finally:
                with admin.connect() as connection:
                    connection.execute(
                        text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE datname = :name AND pid <> pg_backend_pid()"
                        ),
                        {"name": database_name},
                    )
                    connection.execute(text(f'DROP DATABASE "{database_name}"'))
    finally:
        admin.dispose()
    return 0


def _seed_legacy_rows(database_url: str, revision: str) -> dict:
    """Capture every seeded legacy field before migration; bound client memory."""
    if UPLOAD_SEED_ROWS < 1:
        raise ValueError("PHASE1_REHEARSAL_UPLOAD_ROWS must be positive")
    engine = create_engine(database_url)
    user_id, project_id = uuid.uuid4(), uuid.uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO users (id, email, auth_provider, global_role) "
                    "VALUES (:id, :email, 'simple', 'member')"
                ),
                {"id": user_id, "email": f"migration-{user_id}@example.test"},
            )
            connection.execute(
                text(
                    "INSERT INTO projects (id, owner_id, created_by_id, name, status) "
                    "VALUES (:id, :user, :user, 'migration rehearsal', 'active')"
                ),
                {"id": project_id, "user": user_id},
            )
            has_uploads = inspect(connection).has_table("dataset_upload_sessions")
            if has_uploads:
                rows = (
                    {
                        "id": uuid.uuid4(),
                        "project": project_id,
                        "user": user_id,
                        "dataset_name": f"legacy-{index}",
                        "filename": f"legacy-{index}.csv",
                        "object_key": f"legacy/{uuid.uuid4()}",
                        "upload_id": uuid.uuid4().hex,
                        "resume_key": uuid.uuid4().hex,
                        "status": "pending" if index % 2 == 0 else "completed",
                    }
                    for index in range(UPLOAD_SEED_ROWS)
                )
                statement = text(
                    "INSERT INTO dataset_upload_sessions "
                    "(id, project_id, created_by_id, dataset_name, original_filename, "
                    "byte_size, part_size, total_parts, object_key, multipart_upload_id, "
                    "resume_key, status, expires_at) VALUES "
                    "(:id, :project, :user, :dataset_name, :filename, 10737418240, 8388608, "
                    "1280, :object_key, :upload_id, :resume_key, :status, "
                    "now() + interval '1 hour')"
                )
                for batch in batched(rows, 1000):
                    connection.execute(statement, list(batch))
            tables = ["users", "projects"]
            if has_uploads:
                tables.append("dataset_upload_sessions")
            expected = _snapshot_seeded_rows(connection, tables)
            for table, snapshot in expected.items():
                count = UPLOAD_SEED_ROWS if table == "dataset_upload_sessions" else 1
                if snapshot["rows"] != count:
                    raise RuntimeError(f"Seed row count differs for {table}")
    finally:
        engine.dispose()
    return expected


def _verify_seeded_rows(database_url: str, expected: dict) -> None:
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            for table, snapshot in expected.items():
                actual = _fingerprint(connection, table, snapshot["columns"])
                if actual != snapshot:
                    raise RuntimeError(f"Migration changed legacy data in {table}")
            inspector = inspect(connection)
            quote = connection.dialect.identifier_preparer.quote
            for table, snapshot in expected.items():
                new_columns = sorted(
                    {column["name"] for column in inspector.get_columns(table)}
                    - set(snapshot["columns"])
                )
                if not new_columns:
                    continue
                with connection.execution_options(yield_per=1000).execute(
                    text(f"SELECT * FROM {quote(table)} ORDER BY id")
                ) as rows:
                    for row in rows.mappings():
                        policy = (
                            {"sso_issuer": None, "sso_subject": None} if table == "users" else {}
                        )
                        if table == "dataset_upload_sessions":
                            quarantined = (
                                row["dataset_version_id"] is None or row["status"] != "completed"
                            )
                            policy = {
                                **UPLOAD_BACKFILLS,
                                "confirmed_bytes": row["byte_size"]
                                if row["status"] == "completed"
                                else 0,
                                "quarantine_reason": (
                                    "legacy_digest_scope_requires_object_verification"
                                    if quarantined
                                    else None
                                ),
                                "terminal_reason": (
                                    "legacy upload hash retained as multipart manifest"
                                    if quarantined
                                    else None
                                ),
                            }
                        for column in new_columns:
                            if column not in policy or row[column] != policy[column]:
                                raise RuntimeError(
                                    f"{table} {row['id']} has incorrect backfill: {column}"
                                )

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
