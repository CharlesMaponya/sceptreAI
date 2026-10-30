"""Legacy component boundary; actual custody/native SQL separation use factory tests."""

import uuid
from unittest.mock import Mock

from automl_api.models.workflows import WorkflowAttempt
from automl_api.services import analysis_cleanup
from automl_api.services.deletion_runtime import _read
from sqlalchemy import select


def cancel_analysis_job(db, entry, k8s):
    assert isinstance(k8s, Mock), "Legacy adapter accepts owned native mocks only"
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.id == uuid.UUID(entry.payload["attempt_id"]))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    target = analysis_cleanup.original_target(db, entry, attempt)
    if _read(k8s, target) is not None:
        k8s.delete_job(target["name"], namespace=target["namespace"], uid=target["uid"])
    attempt.cleanup_state = "completed"
    db.flush()
    return attempt
