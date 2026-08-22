from __future__ import annotations

import logging
import os
import socket
import time
import uuid
from collections.abc import Callable

from sqlalchemy.orm import Session

from automl_api.db.session import get_session_factory
from automl_api.models.workflows import OutboxEntry
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.reconciler import observe_training_ray_jobs, reconcile_entry
from automl_api.services.uploads import cleanup_abandoned_uploads
from automl_api.services.workflow_state import claim_outbox

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
            observed = observe_training_ray_jobs(db, k8s or KubernetesTrainingClient())
        except Exception:
            db.rollback()
            LOGGER.exception("RayJob observation failed")
            return 0
        db.commit()
        return observed


def cleanup_uploads_once(session_factory: Callable[[], Session]) -> int:
    with session_factory() as db:
        try:
            result = cleanup_abandoned_uploads(db)
        except Exception as exc:
            db.rollback()
            LOGGER.error(
                "upload cleanup failed", extra={"error_type": type(exc).__name__}
            )
            return 0
        db.commit()
        return sum(result.values())


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    worker_id = os.environ.get("RECONCILER_WORKER_ID") or (
        f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    )
    interval = max(0.1, float(os.environ.get("RECONCILER_POLL_SECONDS", "1")))
    session_factory = get_session_factory()
    k8s = KubernetesTrainingClient()
    cleanup_interval = max(
        60.0, float(os.environ.get("UPLOAD_RECONCILE_INTERVAL_SECONDS", "900"))
    )
    next_cleanup = time.monotonic()
    while True:
        processed = run_once(session_factory, worker_id=worker_id)
        observed = observe_once(session_factory, k8s=k8s)
        cleaned = 0
        if time.monotonic() >= next_cleanup:
            cleaned = cleanup_uploads_once(session_factory)
            next_cleanup = time.monotonic() + cleanup_interval
        if processed == 0 and observed == 0 and cleaned == 0:
            time.sleep(interval)


if __name__ == "__main__":
    main()
