"""Durable scheduled audit/log retention (task-index P7-W10).

Retention and purge run through the reconciler's scheduled workflow — never
synchronously on API startup. The approved data policy enforces 30-day
searchable application logs and 365-day audit retention.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from automl_api.models.security import AuditEvent

AUDIT_RETENTION_DAYS = 365
LOG_RETENTION_DAYS = 30

# Reconciler cadence: the purge sweep is safe to repeat; it deletes only rows
# older than the retention window and is bounded per invocation.
PURGE_BATCH_LIMIT = 5_000


def retention_cutoffs(now: datetime | None = None) -> tuple[datetime, datetime]:
    """Return ``(audit_cutoff, log_cutoff)`` for the given instant."""
    moment = now or datetime.now(UTC)
    audit_cutoff = moment - timedelta(days=AUDIT_RETENTION_DAYS)
    log_cutoff = moment - timedelta(days=LOG_RETENTION_DAYS)
    return audit_cutoff, log_cutoff


def purge_expired_audit_events(db: Session, *, now: datetime | None = None) -> int:
    """Delete audit events older than the 365-day retention window.

    Returns the number of rows removed. Bounded per call so a large backlog
    drains across reconciler ticks instead of one long transaction.
    """
    audit_cutoff, _ = retention_cutoffs(now)
    expired_ids = (
        select(AuditEvent.id)
        .where(AuditEvent.occurred_at < audit_cutoff)
        .order_by(AuditEvent.occurred_at, AuditEvent.id)
        .limit(PURGE_BATCH_LIMIT)
    )
    result = db.execute(
        delete(AuditEvent)
        .where(AuditEvent.id.in_(expired_ids))
        .execution_options(synchronize_session=False)
    )
    return int(result.rowcount or 0)


def retention_status(db: Session, *, now: datetime | None = None) -> dict[str, object]:
    """Report current retention posture for dashboards and alerts (P7-W06)."""
    audit_cutoff, _log_cutoff = retention_cutoffs(now)
    oldest = db.query(AuditEvent.occurred_at).order_by(AuditEvent.occurred_at.asc()).first()
    oldest_audit_at = oldest[0] if oldest else None
    return {
        "audit_retention_days": AUDIT_RETENTION_DAYS,
        "log_retention_days": LOG_RETENTION_DAYS,
        "audit_cutoff": audit_cutoff.isoformat(),
        "oldest_audit_event_at": (
            oldest_audit_at.isoformat() if oldest_audit_at is not None else None
        ),
        "purge_batch_limit": PURGE_BATCH_LIMIT,
    }
