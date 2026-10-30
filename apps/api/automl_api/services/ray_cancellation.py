"""Original-UID Ray retirement with independent facts and SQL-free native work."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import AttemptStatus, CommandStatus, ProjectRole, WorkflowStage
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
from automl_api.services.workflow_state import (
    canonical_request_hash as digest,
)
from automl_api.storage.contracts import DeletionPending

TOPICS = {"ray.training.cancel", "ray.dataset.cancel"}
INTENT = "ray.cancel.intent"
START = "ray.cancel.started:"
RESULT = "ray.cancel.result:"
TERMINAL = {
    AttemptStatus.CANCELLED,
    AttemptStatus.SUPERSEDED,
    AttemptStatus.FAILED,
    AttemptStatus.SUCCEEDED,
}


def _wait_until(expires_at, message):
    remaining = (datetime.fromisoformat(expires_at) - datetime.now(UTC)).total_seconds()
    if remaining > 0:
        pending = DeletionPending(message)
        pending.retry_after_seconds = int(remaining) + 1
        raise pending


def original_target(db, attempt, resource, namespace):
    """Read original custody, including create replies not yet logically submitted."""
    original = submission._event(db, attempt, submission.INTENT)
    started = submission._event(db, attempt, submission.CREATE_START)
    sa = submission._event(db, attempt, submission.SA_START)
    result = submission._event(db, attempt, submission.CREATE_RESULT)
    ready = submission._event(db, attempt, submission.SA_RESULT)
    published = submission._event(db, attempt, "ray.submitted")
    target = None
    context = None
    if original is not None:
        intent = original.payload
        if (
            intent.get("protocol") != "ray.factory.v1"
            or intent.get("project_id") != str(attempt.project_id)
            or intent.get("attempt_id") != str(attempt.id)
            or intent.get("namespace") != namespace
        ):
            raise StaleFence("Original Ray retirement intent changed scope or namespace.")
        source = db.get(OutboxEntry, uuid.UUID(intent["outbox_id"]), populate_existing=True)
        command = db.get(WorkflowCommand, uuid.UUID(intent["command_id"]), populate_existing=True)
        expected_topic = (
            "ray.training.submit"
            if attempt.stage == WorkflowStage.TRAINING_RUN
            else "ray.dataset.split.submit"
            if attempt.stage == WorkflowStage.SPLITTER
            else "ray.dataset.prepare.submit"
        )
        if (
            source is None
            or command is None
            or source.command_id != command.id
            or source.project_id != attempt.project_id
            or source.aggregate_id != resource.id
            or source.topic != expected_topic
            or source.payload.get("attempt_id") != str(attempt.id)
            or source.payload.get("fencing_token") != attempt.fencing_token
            or digest(source.payload) != intent.get("request_sha256")
            or command.project_id != attempt.project_id
            or command.request_hash != digest(command.request_payload)
        ):
            raise StaleFence("Original Ray retirement source changed.")
        version = db.get(DatasetVersion, resource.dataset_version_id, populate_existing=True)
        if version is None:
            raise StaleFence("Original Ray retirement dataset disappeared.")
        plan = reconciler.RaySubmissionPlan(
            attempt=attempt,
            manifest={"metadata": {"name": intent["name"], "namespace": intent["namespace"]}},
            fingerprints={key: intent[key] for key in ("request_sha256", "manifest_sha256")},
        )
        if submission._intent((source, command, attempt, resource, version), plan) != intent:
            raise StaleFence("Original Ray retirement immutable source changed.")
        if sa is not None:
            ready = submission._event(db, attempt, submission.SA_RESULT)
            if ready is None or ready.payload != dict(
                intent_sha256=digest(intent), start_sha256=sa.result_digest, ready=True
            ):
                raise DeletionPending(
                    "Original Ray service-account outcome requires qualification."
                )
            context = sa.payload["context_sha256"]
        if ready is not None and sa is None:
            raise StaleFence("Original Ray service-account fact lost its unit.")
        created = submission._created(db, attempt, intent)
        if created is None:
            if started is not None:
                raise DeletionPending("Original Ray create outcome requires native qualification.")
            if attempt.ray_job_name or published is not None:
                raise StaleFence("Ray logical identity has no original create fact.")
        else:
            if sa is None or started.payload["context_sha256"] != context:
                raise StaleFence("Original Ray creation lost its native context.")
            target = dict(
                uid=created["metadata"]["uid"],
                job_name=intent["name"],
                namespace=intent["namespace"],
            )
            if attempt.ray_job_name or published is not None:
                submission._published(db, attempt, intent, created)
            submission._activated(db, attempt, intent, created)
    elif any(row is not None for row in (sa, started, result, ready)):
        raise StaleFence("Original Ray creation history lost its intent.")
    elif attempt.ray_job_name:
        target, source = reconciler._ray_submission(db, attempt)
        if target["namespace"] != namespace:
            raise StaleFence("Original Ray retirement namespace changed.")
    elif published is not None:
        raise StaleFence("Original Ray submitted identity lost its logical name.")
    if target is None:
        if (
            db.scalar(
                select(WorkflowEvent.id)
                .where(
                    WorkflowEvent.attempt_id == attempt.id,
                    WorkflowEvent.event_key.startswith(submission.ACT_START)
                    | WorkflowEvent.event_key.startswith(submission.ACT_RESULT),
                )
                .limit(1)
            )
            is not None
        ):
            raise StaleFence("Original Ray activation lost its creation identity.")
        return None, context
    units = db.scalars(
        select(WorkflowEvent.event_key).where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key.startswith(submission.ACT_START),
        )
    )
    for key in units:
        unit = submission._event(db, attempt, key)
        result = submission._event(
            db, attempt, submission.ACT_RESULT + key.removeprefix(submission.ACT_START)
        )
        if result is None:
            _wait_until(
                unit.payload["expires_at"], "Original Ray activation still owns its native lease."
            )
    labels = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
    }
    training = attempt.stage == WorkflowStage.TRAINING_RUN
    labels.update(
        {"automl.platform/run-id": str(resource.id)}
        if training
        else {
            "automl.platform/profile-job-id": str(resource.id),
            "automl.platform/workflow-stage": attempt.stage.value,
            "automl.platform/dataset-version-id": str(attempt.dataset_version_id),
        }
    )
    return dict(
        kind="RayJob",
        name=target["job_name"],
        namespace=target["namespace"],
        uid=target["uid"],
        project_id=str(attempt.project_id),
        attempt_id=str(attempt.id),
        run_id=str(resource.id) if training else None,
        labels=labels,
    ), context


def cancellation_scope(entry, attempt, resource):
    training = entry.aggregate_type == "model_run"
    if attempt is None:
        raise LookupError("The tracked Ray attempt no longer exists.")
    if (
        attempt.project_id != entry.project_id
        or attempt.fencing_token != entry.payload.get("fencing_token")
        or entry.payload.get("attempt_id") != str(attempt.id)
        or resource is None
        or resource.project_id != attempt.project_id
        or resource.id != entry.aggregate_id
        or (
            training
            and (attempt.stage != WorkflowStage.TRAINING_RUN or attempt.model_run_id != resource.id)
        )
        or (
            not training
            and (
                entry.aggregate_type != "profiling_job"
                or attempt.stage not in {WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}
                or attempt.status not in TERMINAL
                or attempt.logical_key != f"profile:{resource.id}:{attempt.stage.value}"
            )
        )
    ):
        raise StaleFence("Ray cancellation scope, status or fence is stale.")
    namespace = resource.k8s_namespace if training else entry.payload.get("namespace")
    if not isinstance(namespace, str) or not namespace:
        raise StaleFence("Ray cancellation requires its original namespace.")
    return namespace


def _rows(db, entry_id, worker_id, *, retired=False):
    original = db.get(OutboxEntry, entry_id, populate_existing=True)
    if original is None or (original.status.value == "delivered" and not retired):
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
    training = original.topic == "ray.training.cancel"
    resource = db.get(
        ModelRun if training else ProfilingJob,
        original.aggregate_id,
        with_for_update=True,
        populate_existing=True,
    )
    if retired:
        entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
        if entry is None or entry.status.value != "delivered":
            raise StaleFence("Ray cancellation retirement was not logically published.")
    else:
        entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
        if entry is None:
            return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    if (
        project is None
        or attempt is None
        or resource is None
        or command is None
        or entry.topic not in TOPICS
        or entry.project_id != project.id
        or attempt.project_id != project.id
        or resource.project_id != project.id
        or entry.payload.get("attempt_id") != str(attempt.id)
        or attempt.fencing_token != entry.payload.get("fencing_token")
        or attempt.status not in TERMINAL
        or command.project_id != project.id
        or command.resource_type != entry.aggregate_type
        or command.resource_id != resource.id
        or command.request_hash != digest(command.request_payload)
        or command.status
        not in {CommandStatus.PENDING, CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
        or command.operation
        not in ({"training.cancel", "training.launch"} if training else {"dataset.profile"})
        or entry.aggregate_type != ("model_run" if training else "profiling_job")
        or (
            training
            and (
                attempt.stage != WorkflowStage.TRAINING_RUN
                or attempt.model_run_id != resource.id
                or attempt.logical_key != f"training-run:{resource.id}"
            )
        )
        or (
            not training
            and (
                attempt.stage not in {WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}
                or attempt.dataset_version_id != resource.dataset_version_id
                or attempt.logical_key != f"profile:{resource.id}:{attempt.stage.value}"
            )
        )
    ):
        raise StaleFence("Ray cancellation command, source, terminal status or fence changed.")
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    if (
        actor is None
        or not actor.is_active
        or not user_has_project_role(db, actor, project.id, ProjectRole.EDITOR)
    ):
        raise StaleFence("Current Ray cancellation actor is unavailable.")
    namespace = resource.k8s_namespace if training else entry.payload.get("namespace")
    if not isinstance(namespace, str) or not namespace:
        raise StaleFence("Ray cancellation requires its original namespace.")
    namespace = cancellation_scope(entry, attempt, resource)
    target, context = original_target(db, attempt, resource, namespace)
    intent = dict(
        protocol="ray.cancel.v1",
        project_id=str(project.id),
        attempt_id=str(attempt.id),
        outbox_id=str(entry.id),
        command_id=str(command.id),
        target=target,
        native_context_sha256=context,
        source_sha256=digest(
            dict(
                payload=entry.payload,
                actor_id=str(command.actor_id),
                operation=command.operation,
                request_hash=command.request_hash,
                resource_id=str(resource.id),
                generation=attempt.generation,
                fencing_token=attempt.fencing_token,
                stage=attempt.stage.value,
                logical_key=attempt.logical_key,
            )
        ),
    )
    old = submission._event(db, attempt, INTENT)
    if old is not None and old.payload != intent:
        raise StaleFence("Original Ray cancellation intent or current source changed.")
    return entry, command, attempt, intent


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
        raise StaleFence("Original Ray cancellation result lost its native unit.")
    units = []
    for key in keys:
        unit = submission._event(db, attempt, key)
        result = submission._event(db, attempt, RESULT + key.removeprefix(START))
        if (
            unit.payload.get("intent_sha256") != digest(intent)
            or unit.payload.get("operation") != key
        ):
            raise StaleFence("Ray cancellation unit lost its original intent.")
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
            raise StaleFence("Ray cancellation result lost its original native unit.")
        units.append((unit, result))
    return units


def _publish(db, rows, worker_id):
    entry, command, attempt, intent = rows
    if not any(
        result is not None and result.payload["absent"]
        for _, result in _results(db, attempt, intent)
    ):
        raise StaleFence("Ray cancellation has no original physical retirement proof.")
    attempt.cleanup_state = "completed"
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    if command.status != CommandStatus.SUCCEEDED:
        if command.status == CommandStatus.PENDING:
            transition_command(command, CommandStatus.RUNNING)
        transition_command(command, CommandStatus.SUCCEEDED)


def retirement_complete(db, attempt):
    record = submission._event(db, attempt, INTENT)
    if record is None or attempt.cleanup_state != "completed":
        return False
    rows = _rows(db, uuid.UUID(record.payload["outbox_id"]), None, retired=True)
    if rows is None or rows[2].id != attempt.id:
        raise StaleFence("Original Ray retirement proof belongs to another attempt.")
    return any(
        result is not None and result.payload["absent"]
        for _, result in _results(db, rows[2], rows[3])
    )


def reconcile_ray_cancellation(factory, entry_id, worker_id, *, k8s_factory):
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
                        "Original Ray cancellation still owns its native lease.",
                    )
            inspect_only = bool(units and units[-1][1] is None)
            if not known_absent and entry.delivery_attempts > attempt.retry_budget:
                raise DeletionPending(
                    "Ray cancellation retry budget requires operator qualification."
                )
            attempt_id = attempt.id
        k8s = k8s_factory() if intent["target"] is not None else None
        context = submission._context(k8s) if k8s is not None else intent["native_context_sha256"]
        if (
            intent["native_context_sha256"] is not None
            and context != intent["native_context_sha256"]
        ):
            raise StaleFence("Ray cancellation native context differs from original creation.")
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if rows[3] != intent or submission._delivery(rows[0]) != delivery:
                raise StaleFence("Ray cancellation source or delivery changed before native work.")
            units = _results(db, rows[2], intent)
            if any(unit.payload.get("context_sha256") != context for unit, _ in units):
                raise StaleFence("Ray cancellation native context changed between units.")
            if known_absent:
                _publish(db, rows, worker_id)
                return
            for unit, result in units:
                if result is None:
                    _wait_until(
                        unit.payload["expires_at"],
                        "Another Ray cancellation unit still owns its lease.",
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
                    "Original Ray cancellation history disappeared before fact custody."
                )
            unit = submission._event(db, attempt, key)
            if unit is None or unit.payload != start:
                raise StaleFence("Original Ray cancellation unit changed before fact custody.")
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
                raise StaleFence("Late Ray cancellation cannot acknowledge another delivery.")
            if absent:
                _publish(db, rows, worker_id)
            else:
                defer_outbox(db, rows[0], worker_id=worker_id, delay_seconds=5)
                rows[0].last_error = "Waiting for original RayJob and descendants to retire."
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
