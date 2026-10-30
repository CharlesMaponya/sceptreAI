from __future__ import annotations

import logging
import os
import socket
import time
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from functools import partial

from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.db.session import get_session_factory
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import OutboxEntry, PromotionalScope, WorkflowAttempt
from automl_api.services.analysis_cleanup import TOPIC as ANALYSIS_CLEANUP_TOPIC
from automl_api.services.analysis_cleanup import reconcile_analysis_cleanup
from automl_api.services.analysis_submission import TOPIC as ANALYSIS_SUBMISSION_TOPIC
from automl_api.services.analysis_submission import reconcile_analysis_submission
from automl_api.services.buffered_execution import TOPIC as BUFFERED_UPLOAD_TOPIC
from automl_api.services.buffered_execution import reconcile_buffered_upload
from automl_api.services.buffered_retirement import cleanup_inputs
from automl_api.services.champion_planning import RefitInspection, plan_scope_refit
from automl_api.services.deletion_workflow import reconcile_deletion
from automl_api.services.deployment_controls import TOPIC as DEPLOYMENT_CONTROL_TOPIC
from automl_api.services.deployment_controls import reconcile_control
from automl_api.services.deployment_objects import TOPIC as DEPLOYMENT_TOPIC
from automl_api.services.deployment_objects import reconcile_deployment_object
from automl_api.services.evaluation_reconciler import reconcile_evaluation_jobs_once
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.legacy_analysis import TOPIC as LEGACY_CLEANUP_TOPIC
from automl_api.services.legacy_analysis import observe_legacy_once, reconcile_legacy_cleanup
from automl_api.services.profiling_jobs import resume_incomplete_profiling_jobs
from automl_api.services.provider_import_jobs import reconcile_provider_imports_once
from automl_api.services.ray_cancellation import TOPICS as RAY_CANCELLATION_TOPICS
from automl_api.services.ray_cancellation import reconcile_ray_cancellation
from automl_api.services.ray_submission import TOPICS as RAY_SUBMISSION_TOPICS
from automl_api.services.ray_submission import reconcile_ray_submission
from automl_api.services.reconciler import (
    inspect_analysis_observation,
    inspect_ray_observation,
    observe_analysis_attempts,
    observe_dataset_ray_jobs,
    observe_training_ray_jobs,
    reconcile_entry,
)
from automl_api.services.refit_jobs import reconcile_refit_jobs_once
from automl_api.services.registry_inspection import TOPIC as REGISTRY_TOPIC
from automl_api.services.registry_inspection import reconcile_registry_artifact
from automl_api.services.retention import purge_expired_audit_events
from automl_api.services.tracking_reconciliation import reconcile_tracking_once
from automl_api.services.training_metadata import reconcile_tracking_metadata_once
from automl_api.services.upload_admission_cleanup import cleanup_pending_admissions
from automl_api.services.upload_admission_expiry import expire_pending_admissions
from automl_api.services.upload_admission_recovery import recover_pending_admissions
from automl_api.services.upload_retirement import cleanup_abandoned_uploads
from automl_api.services.upload_verification import TOPIC as UPLOAD_VERIFICATION_TOPIC
from automl_api.services.upload_verification import reconcile_upload_verification
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
            factory_dispatch = None
            with session_factory() as db:
                entry = db.get(OutboxEntry, entry_id)
                if entry is None:
                    continue
                if getattr(entry, "topic", None) == BUFFERED_UPLOAD_TOPIC:
                    factory_dispatch = partial(
                        reconcile_buffered_upload, store_factory=get_object_store
                    )
                elif getattr(entry, "topic", None) in RAY_SUBMISSION_TOPICS:
                    factory_dispatch = partial(
                        reconcile_ray_submission, k8s_factory=KubernetesTrainingClient
                    )
                elif getattr(entry, "topic", None) in RAY_CANCELLATION_TOPICS:
                    factory_dispatch = partial(
                        reconcile_ray_cancellation, k8s_factory=KubernetesTrainingClient
                    )
                elif getattr(entry, "topic", None) == ANALYSIS_SUBMISSION_TOPIC:
                    factory_dispatch = partial(
                        reconcile_analysis_submission, k8s_factory=KubernetesTrainingClient
                    )
                elif getattr(entry, "topic", None) == ANALYSIS_CLEANUP_TOPIC:
                    factory_dispatch = partial(
                        reconcile_analysis_cleanup, k8s_factory=KubernetesTrainingClient
                    )
                elif getattr(entry, "topic", None) in {"project.delete", "run.delete"}:
                    factory_dispatch = reconcile_deletion
                elif getattr(entry, "topic", None) == LEGACY_CLEANUP_TOPIC:
                    factory_dispatch = reconcile_legacy_cleanup
                elif getattr(entry, "topic", None) == UPLOAD_VERIFICATION_TOPIC:
                    factory_dispatch = reconcile_upload_verification
                elif getattr(entry, "topic", None) == REGISTRY_TOPIC:
                    factory_dispatch = reconcile_registry_artifact
                elif getattr(entry, "topic", None) == DEPLOYMENT_CONTROL_TOPIC:
                    factory_dispatch = reconcile_control
                elif getattr(entry, "topic", None) in {
                    DEPLOYMENT_TOPIC,
                    "deployment.reconcile.v3",
                    "deployment.reconcile",
                    "deployment.reconcile.v2",
                }:
                    factory_dispatch = reconcile_deployment_object
                else:
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
            if factory_dispatch is not None:
                factory_dispatch(session_factory, entry_id, worker_id)

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
    observed = 0
    try:
        client = k8s or KubernetesTrainingClient()
        for observer, inspect_job in (
            (observe_training_ray_jobs, inspect_ray_observation),
            (observe_dataset_ray_jobs, inspect_ray_observation),
            (partial(observe_analysis_attempts, k8s=client), inspect_analysis_observation),
        ):
            requests = []
            with session_factory() as db:
                try:
                    selected = observer(db, read=requests.append)
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
            observed += selected
            for request in requests:
                try:
                    result = inspect_job(client, request)
                    with session_factory() as db:
                        try:
                            observer(
                                db,
                                attempt_id=result.attempt_id,
                                read=lambda _current, captured=result: captured,
                            )
                            db.commit()
                        except Exception:
                            db.rollback()
                            raise
                except Exception as exc:
                    LOGGER.error(
                        "Workload observation unavailable",
                        extra={
                            "attempt_id": str(request.attempt_id),
                            "error_type": type(exc).__name__,
                        },
                    )
        observed += observe_legacy_once(session_factory, client)
    except Exception:
        LOGGER.exception("Workflow job observation failed")
    return observed


def cleanup_uploads_once(session_factory: Callable[[], Session]) -> int:
    expired = 0
    try:
        expired += cleanup_inputs(session_factory)
    except Exception as exc:
        LOGGER.error("buffered input cleanup failed", extra={"error_type": type(exc).__name__})
    try:
        expired += recover_pending_admissions(session_factory, now=datetime.now(UTC))
    except Exception as exc:
        LOGGER.error("upload admission recovery failed", extra={"error_type": type(exc).__name__})
    try:
        expired += expire_pending_admissions(session_factory, now=datetime.now(UTC))
    except Exception as exc:
        LOGGER.error("upload admission expiry failed", extra={"error_type": type(exc).__name__})
    try:
        expired += cleanup_pending_admissions(session_factory, now=datetime.now(UTC))
    except Exception as exc:
        LOGGER.error("native admission cleanup failed", extra={"error_type": type(exc).__name__})
    try:
        result = cleanup_abandoned_uploads(session_factory)
    except Exception as exc:
        LOGGER.error("upload cleanup failed", extra={"error_type": type(exc).__name__})
        return expired
    return expired + sum(result.values())


def plan_champions_once(session_factory: Callable[[], Session], *, store=None, limit=25) -> int:
    """Commit desired refit work independently of the workload observation loop."""
    try:
        with session_factory() as db:
            scope_ids = list(
                db.scalars(
                    select(PromotionalScope.id)
                    .where(
                        PromotionalScope.mode == "promotional",
                        PromotionalScope.status.in_({ScopeStatus.SEALED, ScopeStatus.RUNNING}),
                        ~select(WorkflowAttempt.id)
                        .where(
                            WorkflowAttempt.scope_id == PromotionalScope.id,
                            WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                            WorkflowAttempt.status != AttemptStatus.FAILED,
                        )
                        .exists(),
                    )
                    .order_by(PromotionalScope.updated_at, PromotionalScope.id)
                    .limit(limit)
                )
            )
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
            if isinstance(attempt, RefitInspection):
                try:
                    metadata = object_store.stat(attempt.uri)
                    if metadata.uri != attempt.uri:
                        raise ValueError("Candidate metadata belongs to another object")
                except Exception as exc:
                    LOGGER.error(
                        "champion candidate inspection unavailable",
                        extra={"scope_id": str(scope_id), "error_type": type(exc).__name__},
                    )
                    continue
                with session_factory() as db, db.begin():
                    attempt = plan_scope_refit(
                        db,
                        scope_id,
                        object_store,
                        inspection=replace(attempt, byte_size=metadata.byte_size),
                    )
            planned += int(attempt is not None)
        except Exception as exc:
            LOGGER.error(
                "champion planning failed",
                extra={
                    "scope_id": str(scope_id),
                    "error_type": type(exc).__name__,
                },
            )
            try:
                with session_factory() as db, db.begin():
                    scope = db.scalar(
                        select(PromotionalScope)
                        .where(
                            PromotionalScope.id == scope_id,
                            PromotionalScope.status.in_({ScopeStatus.SEALED, ScopeStatus.RUNNING}),
                            ~select(WorkflowAttempt.id)
                            .where(
                                WorkflowAttempt.scope_id == PromotionalScope.id,
                                WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
                                WorkflowAttempt.status != AttemptStatus.FAILED,
                            )
                            .exists(),
                        )
                        .with_for_update(skip_locked=True)
                    )
                    if scope is not None:
                        scope.updated_at = datetime.now(UTC)
                        if isinstance(exc, (ValueError, LookupError)):
                            scope.status = ScopeStatus.FAILED
                            scope.comparison_policy = {
                                **(scope.comparison_policy or {}),
                                "planning_error": type(exc).__name__,
                            }
            except Exception as recovery_error:
                LOGGER.error(
                    "champion planning recovery unavailable",
                    extra={
                        "scope_id": str(scope_id),
                        "error_type": type(recovery_error).__name__,
                    },
                )
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
        try:
            reconcile_provider_imports_once(session_factory, k8s)
        except Exception as exc:
            LOGGER.error(
                "provider verification unavailable", extra={"error_type": type(exc).__name__}
            )
        try:
            reconcile_tracking_once(session_factory)
        except Exception as exc:
            LOGGER.error("tracking recovery unavailable", extra={"error_type": type(exc).__name__})
        try:
            reconcile_tracking_metadata_once(session_factory)
        except Exception as exc:
            LOGGER.error(
                "tracking metadata recovery unavailable", extra={"error_type": type(exc).__name__}
            )
        if time.monotonic() >= next_cleanup:
            try:
                resume_incomplete_profiling_jobs()
            except Exception:
                LOGGER.exception("Local profiling recovery unavailable")
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
