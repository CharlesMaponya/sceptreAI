from __future__ import annotations

import logging
import os
import socket
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import OutboxEntry, PromotionalScope, WorkflowAttempt
from automl_api.services.champion_planning import plan_scope_refit
from automl_api.services.evaluation_reconciler import reconcile_evaluation_jobs_once
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.reconciler import (
    observe_analysis_jobs,
    observe_dataset_ray_jobs,
    observe_training_ray_jobs,
    reconcile_entry,
)
from automl_api.services.refit_jobs import reconcile_refit_jobs_once
from automl_api.services.retention import purge_expired_audit_events
from automl_api.services.uploads import cleanup_abandoned_uploads
from automl_api.services.workflow_state import claim_outbox
from automl_api.storage.object_store import get_object_store

LOGGER = logging.getLogger(__name__)


def run_once(
    session_factory: Callable[[], Session],
    *,
    worker_id: str,
    limit: int = 1,
) -> int:
    try:
        with session_factory() as db:
            entry_ids = [row.id for row in claim_outbox(db, worker_id=worker_id, limit=limit)]
            db.commit()
    except Exception as exc:
        # A StatefulSet failover can invalidate every pooled connection at the
        # same instant.  The next checkout will reconnect, so keep the daemon
        # alive instead of turning a transient database outage into a restart
        # storm across every reconciler replica.
        LOGGER.error(
            "outbox claim cycle unavailable",
            extra={"error_type": type(exc).__name__},
        )
        return 0

    for entry_id in entry_ids:
        try:
            with session_factory() as db:
                entry = db.get(OutboxEntry, entry_id)
                if entry is None:
                    continue
                try:
                    reconcile_entry(db, entry, worker_id=worker_id)
                except Exception as exc:
                    LOGGER.error(
                        "outbox reconciliation failed",
                        extra={"entry_id": str(entry_id), "error_type": type(exc).__name__},
                    )
                    # ``reconcile_entry`` normally rolls back the failed side effect and
                    # records a durable retry before re-raising.  A concurrent delete or
                    # stale ORM row can make that recovery flush fail too, leaving the
                    # session in SQLAlchemy's partial-rollback state.  Committing such a
                    # session terminates the long-running reconciler.  Preserve a valid
                    # retry transaction, but roll back an invalid one so lease expiry can
                    # hand the row to another replica.
                    if db.is_active:
                        try:
                            db.commit()
                        except Exception:
                            db.rollback()
                            LOGGER.exception(
                                "outbox failure transaction could not be committed",
                                extra={"entry_id": str(entry_id)},
                            )
                    else:
                        db.rollback()
                else:
                    db.commit()
        except Exception as exc:
            # A committed claim remains protected by its lease and is safely
            # retried after expiry if the database disappears mid-delivery.
            LOGGER.error(
                "outbox processing transaction unavailable",
                extra={"entry_id": str(entry_id), "error_type": type(exc).__name__},
            )
    return len(entry_ids)


def observe_once(
    session_factory: Callable[[], Session],
    *,
    k8s: KubernetesTrainingClient | None = None,
) -> int:
    with session_factory() as db:
        try:
            client = k8s or KubernetesTrainingClient()
            observed = observe_training_ray_jobs(db, client)
            observed += observe_dataset_ray_jobs(db, client)
            observed += observe_analysis_jobs(db, client)
        except Exception:
            db.rollback()
            LOGGER.exception("Workflow job observation failed")
            return 0
        db.commit()
        return observed


def cleanup_uploads_once(session_factory: Callable[[], Session]) -> int:
    with session_factory() as db:
        try:
            result = cleanup_abandoned_uploads(db)
        except Exception as exc:
            db.rollback()
            LOGGER.error("upload cleanup failed", extra={"error_type": type(exc).__name__})
            return 0
        db.commit()
        return sum(result.values())


def plan_champions_once(session_factory: Callable[[], Session], *, store=None, limit=25) -> int:
    """Commit desired refit work independently of the workload observation loop."""
    try:
        with session_factory() as db:
            scope_ids = list(db.scalars(select(PromotionalScope.id).where(
                PromotionalScope.mode == "promotional",
                PromotionalScope.status.in_({ScopeStatus.SEALED, ScopeStatus.RUNNING}),
                ~select(WorkflowAttempt.id).where(
                    WorkflowAttempt.scope_id == PromotionalScope.id,
                    WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                    WorkflowAttempt.status != AttemptStatus.FAILED,
                ).exists(),
            ).order_by(PromotionalScope.updated_at, PromotionalScope.id).limit(limit)))
        if not scope_ids:
            return 0
        object_store = store or get_object_store()
    except Exception as exc:
        LOGGER.error("champion planning unavailable", extra={"error_type": type(exc).__name__})
        return 0
    planned = 0
    for scope_id in scope_ids:
        try:
            with session_factory() as db, db.begin():
                attempt = plan_scope_refit(db, scope_id, object_store)
                planned += int(attempt is not None)
        except Exception as exc:
            LOGGER.error("champion planning failed", extra={
                "scope_id": str(scope_id), "error_type": type(exc).__name__,
            })
            try:
                with session_factory() as db, db.begin():
                    scope = db.scalar(select(PromotionalScope).where(
                        PromotionalScope.id == scope_id,
                        PromotionalScope.status.in_({ScopeStatus.SEALED, ScopeStatus.RUNNING}),
                    ).with_for_update(skip_locked=True))
                    if scope is not None:
                        scope.updated_at = datetime.now(UTC)
                        if isinstance(exc, (ValueError, LookupError)):
                            scope.status = ScopeStatus.FAILED
                            scope.comparison_policy = {
                                **(scope.comparison_policy or {}),
                                "planning_error": type(exc).__name__,
                            }
            except Exception as recovery_error:
                LOGGER.error("champion planning recovery unavailable", extra={
                    "scope_id": str(scope_id), "error_type": type(recovery_error).__name__,
                })
    return planned


def cleanup_retention_once(session_factory: Callable[[], Session]) -> int:
    with session_factory() as db:
        try:
            deleted = purge_expired_audit_events(db)
        except Exception as exc:
            db.rollback()
            LOGGER.error("retention cleanup failed", extra={"error_type": type(exc).__name__})
            return 0
        db.commit()
        return deleted


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    worker_id = os.environ.get("RECONCILER_WORKER_ID") or (
        f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    )
    interval = max(0.1, float(os.environ.get("RECONCILER_POLL_SECONDS", "1")))
    session_factory = get_session_factory()
    k8s = KubernetesTrainingClient()
    cleanup_interval = max(60.0, float(os.environ.get("UPLOAD_RECONCILE_INTERVAL_SECONDS", "900")))
    retention_interval = max(
        300.0,
        float(os.environ.get("RETENTION_RECONCILE_INTERVAL_SECONDS", "86400")),
    )
    next_cleanup = time.monotonic()
    next_retention = time.monotonic()
    while True:
        processed = run_once(session_factory, worker_id=worker_id)
        observe_once(session_factory, k8s=k8s)
        plan_champions_once(session_factory)
        try:
            reconcile_refit_jobs_once(session_factory, k8s)
        except Exception as exc:
            LOGGER.error("refit observation unavailable", extra={"error_type": type(exc).__name__})
        try:
            reconcile_evaluation_jobs_once(session_factory, k8s)
        except Exception as exc:
            LOGGER.error(
                "evaluation observation unavailable", extra={"error_type": type(exc).__name__}
            )
        if time.monotonic() >= next_cleanup:
            cleanup_uploads_once(session_factory)
            next_cleanup = time.monotonic() + cleanup_interval
        if time.monotonic() >= next_retention:
            cleanup_retention_once(session_factory)
            next_retention = time.monotonic() + retention_interval
        # Active observations are not new work; rate-limit steady-state polling.
        if processed == 0:
            time.sleep(interval)


if __name__ == "__main__":
    main()
