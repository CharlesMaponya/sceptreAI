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
from automl_api.services.reconciler import reconcile_entry
from automl_api.services.workflow_state import claim_outbox

LOGGER = logging.getLogger(__name__)


def run_once(
    session_factory: Callable[[], Session],
    *,
    worker_id: str,
    limit: int = 1,
) -> int:
    with session_factory() as db:
        entry_ids = [row.id for row in claim_outbox(db, worker_id=worker_id, limit=limit)]
        db.commit()

    for entry_id in entry_ids:
        with session_factory() as db:
            entry = db.get(OutboxEntry, entry_id)
            if entry is None:
                continue
            try:
                reconcile_entry(db, entry, worker_id=worker_id)
            except Exception:
                LOGGER.exception("outbox reconciliation failed", extra={"entry_id": str(entry_id)})
            finally:
                db.commit()
    return len(entry_ids)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    worker_id = os.environ.get("RECONCILER_WORKER_ID") or (
        f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    )
    interval = max(0.1, float(os.environ.get("RECONCILER_POLL_SECONDS", "1")))
    session_factory = get_session_factory()
    while True:
        processed = run_once(session_factory, worker_id=worker_id)
        if processed == 0:
            time.sleep(interval)


if __name__ == "__main__":
    main()
