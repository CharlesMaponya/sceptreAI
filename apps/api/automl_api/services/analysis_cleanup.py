"""Original-UID analysis Job retirement with independent native observations."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    ProjectRole,
    RunKind,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import ray_submission as submission
from automl_api.services import reconciler
from automl_api.services.deletion_runtime import cleanup_runtime
from automl_api.services.projects import user_has_project_role
from automl_api.services.workflow_state import (
    StaleFence,
    complete_outbox,
    defer_outbox,
    transition_command,
)
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import DeletionPending

TOPIC = "kubernetes.analysis.cancel"
INTENT = "analysis.cleanup.intent"
START = "analysis.cleanup.started:"
RESULT = "analysis.cleanup.result:"
TERMINAL = {
    AttemptStatus.SUCCEEDED,
    AttemptStatus.FAILED,
    AttemptStatus.CANCELLED,
    AttemptStatus.SUPERSEDED,
}


def cancellation_scope(entry, attempt):
    if (
        attempt is None
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.logical_key != f"analysis-run:{attempt.model_run_id}"
        or attempt.project_id != entry.project_id
        or entry.aggregate_type != "model_run"
        or attempt.model_run_id != entry.aggregate_id
        or entry.payload.get("attempt_id") != str(attempt.id)
        or attempt.fencing_token != entry.payload.get("fencing_token")
        or attempt.status not in TERMINAL
    ):
        raise StaleFence("Analysis cleanup scope, status or fence is stale.")


def original_target(db, entry, attempt):
    cancellation_scope(entry, attempt)
    from automl_api.services.analysis_submission import cleanup_source

    identity, source, context = cleanup_source(db, attempt)
    if any(entry.payload.get(key) != identity[key] for key in ("uid", "namespace", "job_name")):
        raise StaleFence("Analysis cleanup differs from its original Job identity.")
    expected = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/run-id": str(attempt.model_run_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
        "automl.platform/workflow-stage": attempt.stage.value,
    }
    if identity["uid"] is None:
        return None
    return dict(
        kind="Job",
        name=identity["job_name"],
        namespace=identity["namespace"],
        uid=identity["uid"],
        project_id=str(attempt.project_id),
        run_id=str(attempt.model_run_id),
        attempt_id=str(attempt.id),
        labels=expected,
    )


def _rows(db, entry_id, worker_id):
    original = db.get(OutboxEntry, entry_id, populate_existing=True)
    if original is None or original.status.value == "delivered":
        return None
    project = db.get(
        Project, original.project_id, with_for_update={"read": True}, populate_existing=True
    )
    attempt = db.get(
        WorkflowAttempt,
        uuid.UUID(original.payload["attempt_id"]),
        with_for_update=True,
        populate_existing=True,
    )
    run = db.get(ModelRun, original.aggregate_id, with_for_update=True, populate_existing=True)
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    if (
        project is None
        or attempt is None
        or run is None
        or command is None
        or entry.topic != TOPIC
        or entry.aggregate_type != "model_run"
        or attempt.project_id != project.id
        or run.project_id != project.id
        or entry.project_id != project.id
        or attempt.model_run_id != run.id
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.logical_key != f"analysis-run:{run.id}"
        or attempt.status not in TERMINAL
        or run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or command.operation != f"{run.run_kind.value}.launch"
        or entry.payload.get("attempt_id") != str(attempt.id)
        or entry.payload.get("fencing_token") != attempt.fencing_token
        or command.project_id != project.id
        or command.resource_id != run.id
        or command.resource_type not in {"model_run", command.operation}
        or command.request_hash != digest(command.request_payload)
        or command.status
        not in {CommandStatus.PENDING, CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
    ):
        raise StaleFence("Analysis cleanup command, source, status or fence changed.")
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    if (
        actor is None
        or not actor.is_active
        or not user_has_project_role(db, actor, project.id, ProjectRole.EDITOR)
    ):
        raise StaleFence("Current analysis cleanup actor is unavailable.")
    target = original_target(db, entry, attempt)
    from automl_api.services.analysis_submission import cleanup_source

    identity, source, context = cleanup_source(db, attempt)
    manifest = source.payload["manifest"]
    meta = manifest.get("metadata", {})
    expected = {
        "automl.platform/project-id": str(project.id),
        "automl.platform/run-id": str(run.id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
        "automl.platform/workflow-stage": attempt.stage.value,
    }
    if (
        source.command_id != command.id
        or source.aggregate_id != run.id
        or any(entry.payload.get(k) != identity[k] for k in ("uid", "namespace", "job_name"))
        or identity["namespace"] != run.k8s_namespace
        or meta.get("name") != identity["job_name"]
        or meta.get("namespace") != identity["namespace"]
        or any((meta.get("labels") or {}).get(k) != v for k, v in expected.items())
    ):
        raise StaleFence("Analysis cleanup lost its original Job identity or source binding.")
    intent = dict(
        protocol="analysis.cleanup.v1",
        project_id=str(project.id),
        attempt_id=str(attempt.id),
        outbox_id=str(entry.id),
        command_id=str(command.id),
        target=target,
        native_context_sha256=context,
        source_sha256=digest(
            dict(
                payload=entry.payload,
                identity=identity,
                source=source.payload,
                source_id=str(source.id),
                actor_id=str(command.actor_id),
                operation=command.operation,
                request_hash=command.request_hash,
                generation=attempt.generation,
                fencing_token=attempt.fencing_token,
                logical_key=attempt.logical_key,
                workload_identity=attempt.workload_identity,
                dataset_version_id=str(attempt.dataset_version_id),
                run_kind=run.run_kind.value,
            )
        ),
    )
    old = submission._event(db, attempt, INTENT)
    if old is not None and old.payload != intent:
        raise StaleFence("Original analysis cleanup intent or current source changed.")
    return entry, command, attempt, intent


def _wait_until(expires_at, message):
    remaining = (datetime.fromisoformat(expires_at) - datetime.now(UTC)).total_seconds()
    if remaining > 0:
        pending = DeletionPending(message)
        pending.retry_after_seconds = int(remaining) + 1
        raise pending


def _results(db, attempt, intent):
    keys = list(
        db.scalars(
            select(WorkflowEvent.event_key)
            .where(
                WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key.startswith(START)
            )
            .order_by(WorkflowEvent.created_at, WorkflowEvent.id)
        )
    )
    results = set(
        db.scalars(
            select(WorkflowEvent.event_key).where(
                WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key.startswith(RESULT)
            )
        )
    )
    if not results <= {RESULT + key.removeprefix(START) for key in keys}:
        raise StaleFence("Original Analysis cleanup result lost its native unit.")
    units = []
    for key in keys:
        unit = submission._event(db, attempt, key)
        result = submission._event(db, attempt, RESULT + key.removeprefix(START))
        if (
            unit.payload.get("intent_sha256") != digest(intent)
            or unit.payload.get("operation") != key
        ):
            raise StaleFence("Analysis cleanup unit lost its original intent.")
        if result is not None and (
            result.payload.get("start_sha256") != unit.result_digest
            or result.payload.get("intent_sha256") != digest(intent)
            or type(result.payload.get("absent")) is not bool
            or type(result.payload.get("observed_absent")) is not bool
            or result.payload["absent"]
            != (
                result.payload["observed_absent"]
                and result.payload.get("context_sha256") == unit.payload.get("context_sha256")
            )
        ):
            raise StaleFence("Analysis cleanup result lost its original native unit.")
        units.append((unit, result))
    return units


def _publish(db, rows, worker_id):
    entry, command, attempt, intent = rows
    if not any(
        result is not None and result.payload["absent"]
        for _, result in _results(db, attempt, intent)
    ):
        raise StaleFence("Analysis cleanup has no original physical retirement proof.")
    attempt.cleanup_state = "completed"
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    if command.status != CommandStatus.SUCCEEDED:
        if command.status == CommandStatus.PENDING:
            transition_command(command, CommandStatus.RUNNING)
        transition_command(command, CommandStatus.SUCCEEDED)


def reconcile_analysis_cleanup(factory, entry_id, worker_id, *, k8s_factory):
    try:
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            entry, _, attempt, intent = rows
            delivery = submission._delivery(entry)
            submission._put(db, attempt, INTENT, intent)
            units = _results(db, attempt, intent)
            known_absent = any(
                result is not None and result.payload["absent"] for _, result in units
            )
            for unit, result in units:
                if result is None:
                    _wait_until(
                        unit.payload["expires_at"],
                        "Original Analysis cleanup still owns its native lease.",
                    )
            inspect_only = bool(units and units[-1][1] is None)
            if not known_absent and entry.delivery_attempts > attempt.retry_budget:
                raise DeletionPending(
                    "Analysis cleanup retry budget requires operator qualification."
                )
            attempt_id = attempt.id
        k8s = k8s_factory() if intent["target"] is not None else None
        context = submission._context(k8s) if k8s is not None else intent["native_context_sha256"]
        if (
            intent["native_context_sha256"] is not None
            and context != intent["native_context_sha256"]
        ):
            raise StaleFence("Analysis cleanup native context differs from original creation.")
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if rows[3] != intent or submission._delivery(rows[0]) != delivery:
                raise StaleFence("Analysis cleanup source or delivery changed before native work.")
            units = _results(db, rows[2], intent)
            if any(unit.payload.get("context_sha256") != context for unit, _ in units):
                raise StaleFence("Analysis cleanup native context changed between units.")
            if known_absent:
                _publish(db, rows, worker_id)
                return
            for unit, result in units:
                if result is None:
                    _wait_until(
                        unit.payload["expires_at"],
                        "Another Analysis cleanup unit still owns its lease.",
                    )
            key = START + str(uuid.uuid4())
            start = dict(
                operation=key,
                intent_sha256=digest(intent),
                delivery=delivery,
                context_sha256=context,
                worker_id=worker_id,
                expires_at=rows[0].lease_expires_at.isoformat(),
                inspect_only=inspect_only,
            )
            submission._put(db, rows[2], key, start)
        if k8s is not None:
            submission._same_context(k8s, context)
        absent = (
            cleanup_runtime(k8s, [intent["target"]], inspect_only=inspect_only)
            if k8s is not None
            else True
        )
        observed_absent = absent
        observed_context = context
        if k8s is not None:
            try:
                observed_context = submission._context(k8s)
            except (OSError, ValueError, TypeError):
                observed_context = None
        absent = observed_absent and observed_context == context
        with factory() as db, db.begin():
            attempt = db.get(
                WorkflowAttempt, attempt_id, with_for_update=True, populate_existing=True
            )
            original = submission._event(db, attempt, INTENT) if attempt is not None else None
            if original is None or original.payload != intent:
                raise StaleFence(
                    "Original Analysis cleanup history disappeared before fact custody."
                )
            unit = submission._event(db, attempt, key)
            if unit is None or unit.payload != start:
                raise StaleFence("Original Analysis cleanup unit changed before fact custody.")
            submission._put(
                db,
                attempt,
                RESULT + key.removeprefix(START),
                dict(
                    intent_sha256=digest(intent),
                    start_sha256=digest(start),
                    absent=absent,
                    observed_absent=observed_absent,
                    context_sha256=observed_context,
                ),
            )
        if k8s is not None:
            submission._same_context(k8s, context)
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if rows[3] != intent or submission._delivery(rows[0]) != delivery:
                raise StaleFence("Late Analysis cleanup cannot acknowledge another delivery.")
            if absent:
                _publish(db, rows, worker_id)
            else:
                defer_outbox(db, rows[0], worker_id=worker_id, delay_seconds=5)
                rows[0].last_error = "Waiting for original Job and Pods to retire."
    except DeletionPending as exc:
        with factory() as db, db.begin():
            entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
            if entry is not None:
                defer_outbox(db, entry, worker_id=worker_id, delay_seconds=exc.retry_after_seconds)
                entry.last_error = str(exc)
    except Exception as exc:
        try:
            with factory() as db, db.begin():
                entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
                if entry is not None:
                    entry.last_error = redact_text(str(exc))
        except StaleFence:
            pass
        raise


def retirement_complete(db, attempt):
    """Only independently journaled physical retirement releases a capacity hold."""
    if attempt is None or attempt.cleanup_state != "completed":
        return False
    original = submission._event(db, attempt, INTENT)
    return original is not None and any(
        result is not None and result.payload["absent"]
        for _, result in _results(db, attempt, original.payload)
    )
