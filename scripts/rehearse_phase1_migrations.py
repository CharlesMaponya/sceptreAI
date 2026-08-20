#!/usr/bin/env python3
"""Rehearse every supported pre-Phase-1 schema in isolated PostgreSQL databases."""

from __future__ import annotations

import os
import subprocess
import sys
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

SUPPORTED_STARTS = (
    "0001_initial",
    "0002_expand_artifact_kind",
    "0003_security_controls",
    "0004_resumable_dataset_uploads",
    "0007_phase1_attempt_lineage",
)
UPLOAD_SEED_ROWS = int(os.environ.get("PHASE1_REHEARSAL_UPLOAD_ROWS", "10000"))


def _run(database_url: str, *arguments: str) -> None:
    environment = {**os.environ, "DATABASE_URL": database_url}
    subprocess.run(arguments, env=environment, check=True)  # noqa: S603


def main() -> int:
    source = make_url(os.environ["DATABASE_URL"])
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
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
            _seed_legacy_rows(target_url, revision)
            _run(target_url, sys.executable, "-m", "alembic", "upgrade", "head")
            _verify_seeded_rows(target_url, revision)
            _run(target_url, sys.executable, "scripts/verify_database_schema.py")
            _run(target_url, sys.executable, "-m", "alembic", "check")
            print(f"migration rehearsal passed: {revision} -> head")
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
    admin.dispose()
    return 0


def _seed_legacy_rows(database_url: str, revision: str) -> None:
    """Seed enough durable rows to prove data survives every upgrade path."""
    engine = create_engine(database_url)
    user_id, project_id = uuid.uuid4(), uuid.uuid4()
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
        if revision in {"0004_resumable_dataset_uploads", "0007_phase1_attempt_lineage"}:
            rows = [
                {
                    "id": uuid.uuid4(),
                    "project": project_id,
                    "user": user_id,
                    "dataset_name": f"legacy-{index}",
                    "filename": f"legacy-{index}.csv",
                    "object_key": f"legacy/{uuid.uuid4()}",
                    "upload_id": uuid.uuid4().hex,
                    "resume_key": uuid.uuid4().hex,
                }
                for index in range(UPLOAD_SEED_ROWS)
            ]
            connection.execute(
                text(
                    "INSERT INTO dataset_upload_sessions "
                    "(id, project_id, created_by_id, dataset_name, original_filename, "
                    "byte_size, part_size, total_parts, object_key, multipart_upload_id, "
                    "resume_key, status, expires_at) VALUES "
                    "(:id, :project, :user, :dataset_name, :filename, 10737418240, 8388608, "
                    "1280, :object_key, :upload_id, :resume_key, 'pending', "
                    "now() + interval '1 hour')"
                ),
                rows,
            )
    engine.dispose()


def _verify_seeded_rows(database_url: str, revision: str) -> None:
    if revision not in {"0004_resumable_dataset_uploads", "0007_phase1_attempt_lineage"}:
        return
    engine = create_engine(database_url)
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT count(*), min(provider_driver), min(protocol), "
                "min(content_policy_revision), min(byte_size) "
                "FROM dataset_upload_sessions"
            )
        ).one()
    engine.dispose()
    actual, driver, protocol, policy, byte_size = row
    if actual != UPLOAD_SEED_ROWS:
        raise RuntimeError(
            f"migration row-count mismatch: expected {UPLOAD_SEED_ROWS}, observed {actual}"
        )
    if (driver, protocol, policy, byte_size) != (
        "s3_compatible",
        "multipart",
        "legacy-v1",
        10 * 1024 * 1024 * 1024,
    ):
        raise RuntimeError("Phase 2 legacy upload backfill does not preserve its contract.")


if __name__ == "__main__":
    raise SystemExit(main())
