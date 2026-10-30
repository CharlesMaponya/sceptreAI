"""Factory-owned suspended Ray creation, original facts and guarded activation."""

import copy
import hashlib
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
    OutboxStatus,
    ProjectRole,
    ProjectStatus,
    RunStatus,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DeletionTombstone,
    OutboxEntry,
    WorkflowAttempt,
    WorkflowCommand,
    WorkflowEvent,
)
from automl_api.services import reconciler
from automl_api.services.evaluation_publication import _append
from automl_api.services.projects import user_has_project_role
from automl_api.services.provider_source import validate_attempt_source
from automl_api.services.workflow_state import (
    StaleFence,
    attempt_credentials_revoked,
    complete_outbox,
    defer_outbox,
    transition_command,
)
from automl_api.services.workflow_state import (
    canonical_request_hash as digest,
)
from automl_api.storage.contracts import DeletionPending

TOPICS = {"ray.training.submit", "ray.dataset.split.submit", "ray.dataset.prepare.submit"}
INTENT = "ray.create.intent"
SA_START, SA_RESULT = "ray.service_account.started", "ray.service_account.result"
CREATE_START, CREATE_RESULT = "ray.create.started", "ray.create.result"
ACT_START, ACT_RESULT = "ray.activate.started:", "ray.activate.result:"
ACTIVE = {
    AttemptStatus.PENDING,
    AttemptStatus.CLAIMED,
    AttemptStatus.SUBMITTED,
    AttemptStatus.RUNNING,
}
_DNS_LABEL = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?"
_DNS_SUBDOMAIN = _DNS_LABEL + r"(?:\." + _DNS_LABEL + r")*"


def _event(db, attempt, key):
    row = db.scalar(
        select(WorkflowEvent)
        .where(WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == key)
        .execution_options(populate_existing=True)
    )
    if row is not None and (
        row.project_id != attempt.project_id
        or row.event_type != key
        or not isinstance(row.payload, dict)
        or row.result_digest != digest(row.payload)
    ):
        raise StaleFence("Original Ray journal evidence changed.")
    if row is not None and (key in {SA_START, CREATE_START} or key.startswith(ACT_START)):
        if row.payload.get("operation") != key:
            raise StaleFence("Original Ray native unit lost its operation binding.")
    return row


def _put(db, attempt, key, payload):
    row = _event(db, attempt, key)
    if row is not None:
        if row.payload != payload:
            raise StaleFence("Original Ray native fact cannot be overwritten.")
        return row
    return _append(db, attempt, key, payload)


def _delivery(entry):
    return digest(
        {
            name: getattr(entry, name)
            for name in (
                "id",
                "command_id",
                "project_id",
                "topic",
                "aggregate_type",
                "aggregate_id",
                "payload",
                "status",
                "lease_owner",
                "lease_expires_at",
                "delivery_attempts",
            )
        }
    )


def _context(k8s):
    cfg = k8s.custom.api_client.configuration
    if (
        not isinstance(cfg.host, str)
        or not cfg.host
        or cfg.retries != 0
        or cfg.verify_ssl is not True
    ):
        raise StaleFence(
            "Ray native context requires verified HTTPS and disabled transport retries."
        )
    parts = urlsplit(cfg.host)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or getattr(cfg, "assert_hostname", None) is False
    ):
        raise StaleFence("Ray native context requires verified HTTPS without URL credentials.")
    return digest(
        dict(
            host=cfg.host,
            assert_hostname=getattr(cfg, "assert_hostname", None),
            tls_server_name=getattr(cfg, "tls_server_name", None),
            ca=(
                hashlib.sha256(Path(cfg.ssl_ca_cert).read_bytes()).hexdigest()
                if cfg.ssl_ca_cert
                else None
            ),
        )
    )


def _same_context(k8s, expected):
    if _context(k8s) != expected:
        raise StaleFence("Original Ray native cluster context changed.")


def _rows(db, entry_id, worker_id):
    original = db.get(OutboxEntry, entry_id, populate_existing=True)
    if original is None or original.status == OutboxStatus.DELIVERED:
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
    if project is None or attempt is None or attempt.project_id != project.id:
        raise StaleFence("Original Ray project or attempt disappeared.")
    training = original.topic == "ray.training.submit"
    resource = db.get(
        ModelRun if training else ProfilingJob,
        original.aggregate_id,
        with_for_update=True,
        populate_existing=True,
    )
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    if (
        entry.topic not in TOPICS
        or command is None
        or command.project_id != project.id
        or command.operation != ("training.launch" if training else "dataset.profile")
        or command.resource_type != entry.aggregate_type
        or command.resource_id != entry.aggregate_id
        or command.request_hash != digest(command.request_payload)
        or command.status not in {CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
        or resource is None
        or resource.project_id != project.id
        or attempt.fencing_token != entry.payload.get("fencing_token")
        or entry.payload.get("attempt_id") != str(attempt.id)
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
                entry.payload.get("profiling_job_id") != str(resource.id)
                or attempt.logical_key != f"profile:{resource.id}:{attempt.stage.value}"
                or attempt.stage
                != (
                    WorkflowStage.SPLITTER
                    if entry.topic == "ray.dataset.split.submit"
                    else WorkflowStage.PREPARATION
                )
                or attempt.dataset_version_id != resource.dataset_version_id
            )
        )
    ):
        raise StaleFence("Original Ray command, source or attempt scope changed.")
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == project.id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & (DeletionTombstone.resource_id == resource.id)
            ),
        )
        .limit(1)
    )
    if (
        project.status != ProjectStatus.ACTIVE
        or project.settings.get("deletion_in_progress")
        or deleting is not None
        or actor is None
        or not actor.is_active
        or not user_has_project_role(db, actor, project.id, ProjectRole.EDITOR)
        or attempt_credentials_revoked(db, attempt.id)
    ):
        raise StaleFence("Current Ray actor, project or attempt authority is unavailable.")
    version = db.get(DatasetVersion, resource.dataset_version_id, populate_existing=True)
    if (
        version is None
        or version.project_id != project.id
        or version.status in {DatasetStatus.ARCHIVED, DatasetStatus.FAILED}
    ):
        raise StaleFence("Original Ray dataset is unavailable.")
    validate_attempt_source(db, attempt, resource if training else None)
    return entry, command, attempt, resource, version


def _native_authority(rows):
    _, _, attempt, resource, _ = rows
    if attempt.status not in ACTIVE:
        raise StaleFence("The Ray attempt no longer permits native work.")
    if isinstance(resource, ModelRun):
        if resource.status not in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING}:
            raise StaleFence("The training run no longer permits submission.")
    elif resource.status not in {"queued", "running"} or resource.overview_json.get(
        "workflow_attempt_id"
    ) != str(attempt.id):
        raise StaleFence("The current profile no longer permits this Ray attempt.")


def _plan(db, rows):
    entry, _, attempt, resource, version = rows
    protocol = reconciler._ray_credential_protocol(entry)
    if attempt.status in {AttemptStatus.PENDING, AttemptStatus.CLAIMED}:
        prepared = (
            reconciler.prepare_training_ray_job(db, entry)
            if isinstance(resource, ModelRun)
            else reconciler.prepare_dataset_ray_job(db, entry)
        )
        manifest = prepared.manifest
    elif isinstance(resource, ModelRun):
        name = f"sceptre-run-{str(resource.id)[:8]}-g{attempt.generation}"
        manifest = reconciler.build_ray_job_manifest(
            resource, attempt, name=name, credential_protocol=protocol
        )
    else:
        name = f"sceptre-{attempt.stage.value}-{str(resource.id)[:8]}-g{attempt.generation}"
        manifest = reconciler.build_dataset_ray_job_manifest(
            resource, version, attempt, name=name, credential_protocol=protocol
        )
    for value, limit, pattern in (
        (manifest["metadata"].get("namespace"), 63, _DNS_LABEL),
        (manifest["metadata"].get("name"), 253, _DNS_SUBDOMAIN),
        (attempt.workload_identity, 253, _DNS_SUBDOMAIN),
    ):
        if not isinstance(value, str) or len(value) > limit or re.fullmatch(pattern, value) is None:
            raise StaleFence("Ray submission requires a valid, complete native target.")
    # Workload admission must follow the committed original UID.
    manifest["spec"]["suspend"] = True
    return reconciler.RaySubmissionPlan(
        attempt,
        manifest,
        dict(request_sha256=digest(entry.payload), manifest_sha256=digest(manifest)),
        resource,
        version,
    )


def _intent(rows, plan):
    entry, command, attempt, resource, version = rows
    fields = ("id", "project_id", "created_by_id", "dataset_version_id", "target_column")
    if isinstance(resource, ModelRun):
        fields += (
            "run_kind",
            "task_type",
            "params",
            "cpu_request_cores",
            "cpu_limit_cores",
            "memory_request_mb",
            "memory_limit_mb",
            "gpu_requested",
            "k8s_namespace",
        )
    else:
        fields += ("dataset_id",)
    return dict(
        protocol="ray.factory.v1",
        project_id=str(attempt.project_id),
        attempt_id=str(attempt.id),
        outbox_id=str(entry.id),
        command_id=str(command.id),
        source_sha256=digest(
            dict(
                actor_id=command.actor_id,
                operation=command.operation,
                request_hash=command.request_hash,
                request=command.request_payload,
                payload=entry.payload,
                attempt={
                    name: getattr(attempt, name)
                    for name in (
                        "stage",
                        "logical_key",
                        "model_run_id",
                        "dataset_version_id",
                        "generation",
                        "fencing_token",
                        "workload_identity",
                        "predecessor_attempt_id",
                        "predecessor_checkpoint_allowlist",
                        "retry_budget",
                    )
                },
                resource={name: getattr(resource, name) for name in fields},
                version={
                    name: getattr(version, name)
                    for name in (
                        "id",
                        "project_id",
                        "dataset_id",
                        "object_uri",
                        "format",
                        "byte_size",
                        "content_hash",
                        "content_hash_algorithm",
                        "content_hash_scope",
                        "content_hash_verification_status",
                    )
                },
            )
        ),
        **plan.fingerprints,
        name=plan.manifest["metadata"]["name"],
        namespace=plan.manifest["metadata"]["namespace"],
        workload_identity=attempt.workload_identity,
    )


def _original(db, rows, intent):
    row = _event(db, rows[2], INTENT)
    if row is None or row.payload != intent:
        raise StaleFence("Original Ray creation intent or current source changed.")


def _fact(factory, attempt_id, intent, key, payload):
    with factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True, populate_existing=True)
        if attempt is None:
            raise StaleFence("Original Ray attempt history disappeared.")
        _original(db, (None, None, attempt), intent)
        _put(db, attempt, key, payload)


def _created(db, attempt, intent):
    row = _event(db, attempt, CREATE_RESULT)
    if row is None:
        return None
    value = row.payload
    start = _event(db, attempt, CREATE_START)
    if (
        start is None
        or value.get("intent_sha256") != digest(intent)
        or value.get("start_sha256") != start.result_digest
        or not isinstance(value.get("reply"), dict)
    ):
        raise StaleFence("Original Ray creation fact lost its unit binding.")
    metadata = value["reply"].get("metadata", {})
    if (
        metadata.get("name") != intent["name"]
        or metadata.get("namespace") != intent["namespace"]
        or not isinstance(metadata.get("uid"), str)
        or not metadata["uid"].strip()
    ):
        raise StaleFence("Original Ray creation fact lost its native identity.")
    return value["reply"]


def _activated(db, attempt, intent, created):
    keys = db.scalars(
        select(WorkflowEvent.event_key).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key.startswith(ACT_RESULT)
        )
    )
    result = False
    for key in keys:
        fact = _event(db, attempt, key)
        unit = _event(db, attempt, ACT_START + key.removeprefix(ACT_RESULT))
        if unit is None or fact.payload != dict(
            intent_sha256=digest(intent),
            start_sha256=unit.result_digest,
            uid=created["metadata"]["uid"],
            active=True,
        ):
            raise StaleFence("Original Ray activation fact lost its native unit binding.")
        result = True
    return result


def _published(db, attempt, intent, created):
    identity, source = reconciler._ray_submission(db, attempt)
    if (
        {key: identity[key] for key in ("uid", "namespace", "job_name")}
        != {
            "uid": created["metadata"]["uid"],
            "namespace": intent["namespace"],
            "job_name": intent["name"],
        }
        or str(source.id) != intent["outbox_id"]
        or identity["request_sha256"] != intent["request_sha256"]
        or identity["manifest_sha256"] != intent["manifest_sha256"]
    ):
        raise StaleFence("Ray published identity differs from original creation.")


def assert_no_unpublished_creations(db, attempts):
    """An unresolved create cannot disappear behind an empty logical Job name."""
    candidates = [
        attempt
        for attempt in attempts
        if attempt.stage
        in {WorkflowStage.TRAINING_RUN, WorkflowStage.SPLITTER, WorkflowStage.PREPARATION}
    ]
    started_ids = set()
    for offset in range(0, len(candidates), 500):
        started_ids.update(
            db.scalars(
                select(WorkflowEvent.attempt_id).where(
                    WorkflowEvent.attempt_id.in_(
                        [row.id for row in candidates[offset : offset + 500]]
                    ),
                    WorkflowEvent.event_key.in_([SA_START, CREATE_START]),
                )
            )
        )
    for attempt in candidates:
        if attempt.id not in started_ids:
            continue
        from automl_api.services.ray_cancellation import retirement_complete

        if retirement_complete(db, attempt):
            continue
        started = _event(db, attempt, SA_START)
        if started is None:
            if _event(db, attempt, CREATE_START) is not None:
                raise DeletionPending("Original Ray service-account history is missing.")
            continue
        intent = _event(db, attempt, INTENT)
        try:
            if intent is None:
                raise StaleFence("Missing original Ray intent.")
            created = _created(db, attempt, intent.payload)
            identity, source = reconciler._ray_submission(db, attempt)
            if (
                created is None
                or identity["uid"] != created["metadata"]["uid"]
                or identity["manifest_sha256"] != intent.payload["manifest_sha256"]
                or str(source.id) != intent.payload["outbox_id"]
            ):
                raise StaleFence("Unpublished Ray creation.")
        except (StaleFence, KeyError, TypeError, ValueError) as exc:
            raise DeletionPending(
                "Original Ray creation requires native qualification before retirement."
            ) from exc


def _start(db, rows, intent, key, context):
    _original(db, rows, intent)
    _original(db, rows, _intent(rows, _plan(db, rows)))
    _native_authority(rows)
    entry, _, attempt, _, _ = rows
    if _event(db, attempt, key) is not None:
        raise DeletionPending("The original Ray native operation has an unknown outcome.")
    payload = dict(
        operation=key,
        intent_sha256=digest(intent),
        context_sha256=context,
        delivery=_delivery(entry),
        worker_id=entry.lease_owner,
        expires_at=entry.lease_expires_at.isoformat(),
    )
    _put(db, attempt, key, payload)
    return payload


def reconcile_ray_submission(factory, entry_id, worker_id, *, k8s_factory):
    try:
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            entry, command, attempt, _, _ = rows
            delivery = _delivery(entry)
            old = _event(db, attempt, INTENT)
            if old is not None and attempt.status in {
                AttemptStatus.CANCELLED,
                AttemptStatus.SUPERSEDED,
                AttemptStatus.FAILED,
            }:
                from automl_api.services.ray_cancellation import retirement_complete

                if retirement_complete(db, attempt):
                    complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
                    return
            if old is None and attempt.status not in {AttemptStatus.PENDING, AttemptStatus.CLAIMED}:
                if attempt.ray_job_name:
                    reconciler._ray_submission(db, attempt)
                elif attempt.status not in {
                    AttemptStatus.CANCELLED,
                    AttemptStatus.SUPERSEDED,
                    AttemptStatus.FAILED,
                }:
                    raise StaleFence("Legacy Ray submission lacks original native evidence.")
                complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
                return
            plan = _plan(db, rows)
            intent = _intent(rows, plan)
            if old is None:
                _native_authority(rows)
                reconciler._ray_submission_sequence(db, attempt)
                prior = db.scalars(
                    select(WorkflowAttempt).where(
                        WorkflowAttempt.project_id == attempt.project_id,
                        WorkflowAttempt.logical_key == attempt.logical_key,
                        WorkflowAttempt.id != attempt.id,
                    )
                )
                assert_no_unpublished_creations(db, prior)
                if attempt.status == AttemptStatus.PENDING:
                    from automl_api.services.workflow_state import transition_attempt

                    transition_attempt(
                        attempt, AttemptStatus.CLAIMED, fencing_token=attempt.fencing_token
                    )
                _put(db, attempt, INTENT, intent)
            else:
                _original(db, rows, intent)
            created = _created(db, attempt, intent)
            if created is not None and attempt.ray_job_name is not None:
                _published(db, attempt, intent, created)
            activated = created is not None and _activated(db, attempt, intent, created)
            if not activated:
                _native_authority(rows)
            if created is None and _event(db, attempt, CREATE_START) is not None:
                raise DeletionPending(
                    "The original Ray create outcome is unknown; name adoption is forbidden."
                )
            ready = _event(db, attempt, SA_RESULT)
            if ready is None and _event(db, attempt, SA_START) is not None:
                raise DeletionPending("The original service-account ensure outcome is unknown.")
            if not activated and entry.delivery_attempts > attempt.retry_budget:
                raise DeletionPending(
                    "Ray native retry budget exhausted; operator qualification is required."
                )
            attempt_id = attempt.id

        k8s = k8s_factory()
        context = _context(k8s)
        if ready is None:
            with factory() as db, db.begin():
                rows = _rows(db, entry_id, worker_id)
                if rows is None:
                    return
                if _delivery(rows[0]) != delivery:
                    raise StaleFence("Ray delivery changed before service-account custody.")
                start = _start(db, rows, intent, SA_START, context)
            _same_context(k8s, context)
            k8s.ensure_service_account(intent["workload_identity"], namespace=intent["namespace"])
            _fact(
                factory,
                attempt_id,
                intent,
                SA_RESULT,
                dict(intent_sha256=digest(intent), start_sha256=digest(start), ready=True),
            )
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if _delivery(rows[0]) != delivery:
                raise StaleFence("Ray delivery changed before creation.")
            _original(db, rows, _intent(rows, _plan(db, rows)))
            ready = _event(db, rows[2], SA_RESULT)
            sa = _event(db, rows[2], SA_START)
            if (
                sa is None
                or ready is None
                or sa.payload["context_sha256"] != context
                or ready.payload
                != dict(intent_sha256=digest(intent), start_sha256=sa.result_digest, ready=True)
            ):
                raise StaleFence("Original Ray service-account/context evidence changed.")
            created = _created(db, rows[2], intent)
            if created is not None and _activated(db, rows[2], intent, created):
                _published(db, rows[2], intent, created)
                complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
                if rows[1].status == CommandStatus.RUNNING:
                    transition_command(rows[1], CommandStatus.SUCCEEDED)
                return
            if created is None:
                start = _start(db, rows, intent, CREATE_START, context)
        if created is None:
            _same_context(k8s, context)
            reply = k8s.create_ray_job(copy.deepcopy(plan.manifest))
            metadata = reply.get("metadata", {})
            if (
                metadata.get("name") != intent["name"]
                or metadata.get("namespace") != intent["namespace"]
                or not isinstance(metadata.get("uid"), str)
                or not metadata["uid"].strip()
                or reply.get("spec") != plan.manifest["spec"]
            ):
                raise StaleFence("Original suspended Ray create reply differs from its intent.")
            created = dict(
                metadata={name: metadata[name] for name in ("uid", "name", "namespace")},
                status={
                    name: (reply.get("status") or {}).get(name)
                    for name in ("rayClusterName", "jobId")
                },
            )
            _fact(
                factory,
                attempt_id,
                intent,
                CREATE_RESULT,
                dict(intent_sha256=digest(intent), start_sha256=digest(start), reply=created),
            )
        _same_context(k8s, context)
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if _delivery(rows[0]) != delivery:
                raise StaleFence("Late Ray create reply cannot publish for another delivery.")
            current_plan = _plan(db, rows)
            _original(db, rows, _intent(rows, current_plan))
            _native_authority(rows)
            if rows[2].ray_job_name is None:
                reconciler.publish_ray_submission(db, rows[0], current_plan, created)
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if _delivery(rows[0]) != delivery:
                raise StaleFence("Ray delivery changed before activation.")
            _original(db, rows, _intent(rows, _plan(db, rows)))
            _native_authority(rows)
            _published(db, rows[2], intent, created)
            original = _event(db, rows[2], CREATE_START)
            if original.payload["context_sha256"] != context:
                raise StaleFence("Original Ray native cluster context changed.")
            units = list(
                db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt_id,
                        WorkflowEvent.event_key.startswith(ACT_START),
                    )
                )
            )
            if any(
                datetime.fromisoformat(unit.payload["expires_at"]) > datetime.now(UTC)
                for unit in units
            ):
                raise DeletionPending("The original Ray activation still owns its native lease.")
            unit_id = str(uuid.uuid4())
            start = _start(db, rows, intent, ACT_START + unit_id, context)
        _same_context(k8s, context)
        active = k8s.activate_ray_job(copy.deepcopy(plan.manifest), created["metadata"]["uid"])
        wanted = copy.deepcopy(plan.manifest["spec"])
        wanted["suspend"] = False
        if (
            active.get("metadata", {}).get("uid") != created["metadata"]["uid"]
            or active.get("spec") != wanted
        ):
            raise StaleFence("Ray activation did not confirm the original active resource.")
        _fact(
            factory,
            attempt_id,
            intent,
            ACT_RESULT + unit_id,
            dict(
                intent_sha256=digest(intent),
                start_sha256=digest(start),
                uid=created["metadata"]["uid"],
                active=True,
            ),
        )
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if _delivery(rows[0]) != delivery:
                raise StaleFence("Late Ray activation cannot acknowledge another delivery.")
            _original(db, rows, _intent(rows, _plan(db, rows)))
            if not _activated(db, rows[2], intent, created):
                raise StaleFence("Original Ray activation result disappeared.")
            complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
            if rows[1].status == CommandStatus.RUNNING:
                transition_command(rows[1], CommandStatus.SUCCEEDED)
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
