"""Retention workflow contract tests (task-index P7-W10)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from automl_api.services.retention import (
    AUDIT_RETENTION_DAYS,
    LOG_RETENTION_DAYS,
    purge_expired_audit_events,
    retention_cutoffs,
    retention_status,
)


def test_retention_windows_match_signed_policy() -> None:
    assert AUDIT_RETENTION_DAYS == 365
    assert LOG_RETENTION_DAYS == 30


def test_cutoffs_are_derived_from_now() -> None:
    now = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    audit_cutoff, log_cutoff = retention_cutoffs(now)

    assert audit_cutoff == now - timedelta(days=365)
    assert log_cutoff == now - timedelta(days=30)


def test_purge_deletes_only_rows_older_than_the_audit_window() -> None:
    now = datetime.now(UTC)
    db = MagicMock()
    execute_result = MagicMock(rowcount=3)
    db.execute.return_value = execute_result

    deleted = purge_expired_audit_events(db, now=now)

    assert deleted == 3
    statement = db.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": False}))
    assert "audit_events" in compiled
    # The WHERE clause must reference the occurred_at cutoff.
    assert "occurred_at" in compiled
    assert "LIMIT" in compiled.upper()


def test_purge_is_bounded_per_invocation() -> None:
    db = MagicMock()
    db.execute.return_value = MagicMock(rowcount=5_000)

    deleted = purge_expired_audit_events(db)

    assert deleted == 5_000
    statement = db.execute.call_args.args[0]
    compiled = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert "LIMIT 5000" in compiled.upper()


def test_retention_status_reports_posture() -> None:
    now = datetime.now(UTC)
    oldest_at = now - timedelta(days=AUDIT_RETENTION_DAYS + 5)
    db = MagicMock()
    db.query.return_value.order_by.return_value.first.return_value = (oldest_at,)

    status = retention_status(db, now=now)

    assert status["audit_retention_days"] == AUDIT_RETENTION_DAYS
    assert status["log_retention_days"] == LOG_RETENTION_DAYS
    assert status["oldest_audit_event_at"] == oldest_at.isoformat()
    assert status["purge_batch_limit"] == 5_000


def test_retention_status_handles_empty_table() -> None:
    db = MagicMock()
    db.query.return_value.order_by.return_value.first.return_value = None

    status = retention_status(db)

    assert status["oldest_audit_event_at"] is None


def test_purge_targets_the_audit_event_model() -> None:
    db = MagicMock()
    db.execute.return_value = MagicMock(rowcount=0)

    purge_expired_audit_events(db)

    statement = db.execute.call_args.args[0]
    # The delete statement is built from the AuditEvent model.
    assert hasattr(statement, "is_delete") and statement.is_delete
    assert statement.table.name == "audit_events"
