"""Factory-owned analysis creation, durable capacity and original Job activation."""

import copy
import re
import uuid
from datetime import UTC, datetime

from kubernetes import client
from kubernetes.utils.quantity import parse_quantity
from sqlalchemy import func, select

from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
    OutboxStatus,
    ProjectRole,
    ProjectStatus,
    RunKind,
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
from automl_api.services import ray_submission as journal
from automl_api.services import reconciler
from automl_api.services.dataset_digests import require_verified_digest
from automl_api.services.projects import user_has_project_role
from automl_api.services.workflow_state import (
    StaleFence,
    attempt_credentials_revoked,
    complete_outbox,
    defer_outbox,
    transition_attempt,
    transition_command,
)
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import DeletionPending

TOPIC = "kubernetes.analysis.submit"
INTENT = "analysis.create.intent"
RESERVATION = "analysis.capacity.reserved"
SA_START, SA_RESULT = "analysis.service_account.started", "analysis.service_account.result"
CREATE_START, CREATE_RESULT = "analysis.create.started", "analysis.create.result"
ACT_START, ACT_RESULT = "analysis.activate.started:", "analysis.activate.result:"
ACTIVE = {
    AttemptStatus.PENDING,
    AttemptStatus.CLAIMED,
    AttemptStatus.SUBMITTED,
    AttemptStatus.RUNNING,
}


def _rows(db, entry_id, worker_id):
    source = db.get(OutboxEntry, entry_id, populate_existing=True)
    if source is None or source.status == OutboxStatus.DELIVERED:
        return None
    project = db.get(
        Project, source.project_id, with_for_update={"read": True}, populate_existing=True
    )
    manifest = source.payload["manifest"]
    attempt_id = (
        (manifest.get("metadata") or {}).get("labels", {}).get("automl.platform/attempt-id")
    )
    if not attempt_id:
        raise StaleFence("Legacy analysis requires factory-owned first-delivery migration.")
    attempt = db.get(
        WorkflowAttempt, uuid.UUID(attempt_id), with_for_update=True, populate_existing=True
    )
    run = db.get(ModelRun, source.aggregate_id, with_for_update=True, populate_existing=True)
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    # Public launch commands name the operation; legacy tracked commands name model_run.
    if (
        project is None
        or attempt is None
        or run is None
        or command is None
        or entry.topic != TOPIC
        or entry.aggregate_type != "model_run"
        or entry.project_id != project.id
        or attempt.project_id != project.id
        or run.project_id != project.id
        or command.project_id != project.id
        or command.resource_id != run.id
        or command.resource_type not in {"model_run", command.operation}
        or command.request_hash != digest(command.request_payload)
        or command.status
        not in {CommandStatus.PENDING, CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
        or entry.payload.get("run_id") != str(run.id)
        or attempt.stage != WorkflowStage.TRAINING_RUN
        or attempt.model_run_id != run.id
        or attempt.logical_key != f"analysis-run:{run.id}"
        or run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or command.operation != f"{run.run_kind.value}.launch"
        or (run.tags.get("workflow_attempt_id") != str(attempt.id) and attempt.status in ACTIVE)
        or attempt.dataset_version_id not in {None, run.dataset_version_id}
    ):
        raise StaleFence("Original analysis command, source or attempt scope changed.")
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == project.id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & (DeletionTombstone.resource_id == run.id)
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
        raise StaleFence("Current analysis actor, project or attempt authority is unavailable.")
    version = db.get(DatasetVersion, run.dataset_version_id, populate_existing=True)
    if (
        version is None
        or version.project_id != project.id
        or version.status in {DatasetStatus.ARCHIVED, DatasetStatus.FAILED}
    ):
        raise StaleFence("Original analysis dataset is unavailable.")
    require_verified_digest(version)
    return entry, command, attempt, run, version


def _plan(rows):
    entry, _, attempt, run, _ = rows
    manifest = copy.deepcopy(entry.payload["manifest"])
    meta = manifest["metadata"]
    labels = {
        "automl.platform/" + key: value
        for key, value in {
            "project-id": str(run.project_id),
            "run-id": str(run.id),
            "attempt-id": str(attempt.id),
            "generation": str(attempt.generation),
            "workflow-stage": attempt.stage.value,
        }.items()
    }
    pod = manifest["spec"]["template"]
    env = {item["name"]: item.get("value") for item in pod["spec"]["containers"][0]["env"]}
    if (
        meta.get("namespace") != run.k8s_namespace
        or meta.get("name") != run.k8s_job_name
        or any(
            meta.get("labels", {}).get(k) != v
            or pod.get("metadata", {}).get("labels", {}).get(k) != v
            for k, v in labels.items()
        )
        or env.get("AUTOML_FENCING_TOKEN") != attempt.fencing_token
        or env.get("AUTOML_ATTEMPT_ID") != str(attempt.id)
        or pod["spec"].get("serviceAccountName") != attempt.workload_identity
        or pod["spec"].get("automountServiceAccountToken") is not False
    ):
        raise StaleFence("Analysis manifest no longer binds the original attempt.")
    for value, limit, pattern in (
        (meta.get("namespace"), 63, journal._DNS_LABEL),
        (meta.get("name"), 253, journal._DNS_SUBDOMAIN),
        (attempt.workload_identity, 253, journal._DNS_SUBDOMAIN),
    ):
        if not isinstance(value, str) or len(value) > limit or re.fullmatch(pattern, value) is None:
            raise StaleFence("Analysis submission requires a valid complete native target.")
    manifest["spec"]["suspend"] = True
    return manifest


def _payload(entry):
    payload = copy.deepcopy(entry.payload)
    legacy = payload.get("analysis_legacy_migration")
    if legacy is not None:
        from automl_api.services.analysis_migration import _state

        legacy = _state(entry)
        selected = legacy["selected_unit"]
        if (
            legacy["results"][selected]["absent"] is not True
            or legacy["intent"]["run"]["id"] != str(entry.aggregate_id)
            or legacy["intent"]["run"]["project_id"] != str(entry.project_id)
        ):
            raise StaleFence("Analysis migration lost original qualified absence/scope.")
        payload["analysis_legacy_migration"] = dict(
            intent=legacy["intent"],
            intent_sha256=legacy["intent_sha256"],
            selected_unit=selected,
            unit=legacy["units"][selected],
            result=legacy["results"][selected],
        )
    return payload


def _delivery(entry):
    return digest(
        dict(
            payload=_payload(entry),
            **{
                k: getattr(entry, k)
                for k in (
                    "id",
                    "command_id",
                    "project_id",
                    "topic",
                    "aggregate_type",
                    "aggregate_id",
                    "status",
                    "lease_owner",
                    "lease_expires_at",
                    "delivery_attempts",
                )
            },
        )
    )


def _intent(rows, manifest):
    entry, command, attempt, run, version = rows
    legacy = entry.payload.get("analysis_legacy_migration")
    if legacy is not None and digest(legacy["intent"]["command"]) != digest(
        {k: getattr(command, k) for k in legacy["intent"]["command"]}
    ):
        raise StaleFence("Analysis migration original command changed before submission.")
    return dict(
        protocol="analysis.factory.v1",
        project_id=str(attempt.project_id),
        attempt_id=str(attempt.id),
        outbox_id=str(entry.id),
        command_id=str(command.id),
        name=manifest["metadata"]["name"],
        namespace=manifest["metadata"]["namespace"],
        workload_identity=attempt.workload_identity,
        request_sha256=digest(_payload(entry)),
        manifest_sha256=digest(manifest),
        source_sha256=digest(
            dict(
                actor_id=command.actor_id,
                operation=command.operation,
                request_hash=command.request_hash,
                payload=_payload(entry),
                attempt={
                    k: getattr(attempt, k)
                    for k in (
                        "project_id",
                        "model_run_id",
                        "dataset_version_id",
                        "stage",
                        "logical_key",
                        "generation",
                        "fencing_token",
                        "workload_identity",
                        "predecessor_attempt_id",
                        "retry_budget",
                    )
                },
                run={
                    k: getattr(run, k)
                    for k in (
                        "id",
                        "project_id",
                        "created_by_id",
                        "dataset_version_id",
                        "run_kind",
                        "task_type",
                        "params",
                        "target_column",
                        "cpu_request_cores",
                        "cpu_limit_cores",
                        "memory_request_mb",
                        "memory_limit_mb",
                        "gpu_requested",
                        "k8s_namespace",
                    )
                },
                version={
                    k: getattr(version, k)
                    for k in (
                        "id",
                        "project_id",
                        "object_uri",
                        "byte_size",
                        "format",
                        "content_hash",
                        "content_hash_algorithm",
                        "content_hash_scope",
                        "content_hash_verification_status",
                    )
                },
            )
        ),
    )


def _authority(rows):
    if rows[2].status not in ACTIVE or rows[3].status not in {
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
    }:
        raise StaleFence("Analysis no longer permits native submission.")


def _current(db, entry_id, worker_id, intent, delivery):
    rows = _rows(db, entry_id, worker_id)
    if rows is None:
        return None
    if _delivery(rows[0]) != delivery or _intent(rows, _plan(rows)) != intent:
        raise StaleFence("Analysis source or delivery changed before publication/native work.")
    original = journal._event(db, rows[2], INTENT)
    if original is None or original.payload != intent:
        raise StaleFence("Original analysis intent changed.")
    return rows


def _start(db, rows, intent, key, context):
    _authority(rows)
    if journal._event(db, rows[2], key) is not None:
        raise DeletionPending("Original analysis native operation has an unknown outcome.")
    reservation = journal._event(db, rows[2], RESERVATION)
    if reservation is None or reservation.payload != dict(
        intent_sha256=digest(intent), namespace=intent["namespace"], context_sha256=context
    ):
        raise StaleFence("Original analysis capacity or native context changed.")
    value = dict(
        operation=key,
        intent_sha256=digest(intent),
        context_sha256=context,
        delivery=_delivery(rows[0]),
        worker_id=rows[0].lease_owner,
        expires_at=rows[0].lease_expires_at.isoformat(),
    )
    journal._put(db, rows[2], key, value)
    return value


def _fact(factory, attempt_id, intent, key, start_key, start, **fact):
    with factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True, populate_existing=True)
        original = journal._event(db, attempt, INTENT) if attempt else None
        unit = journal._event(db, attempt, start_key) if attempt else None
        if original is None or original.payload != intent or unit is None or unit.payload != start:
            raise StaleFence("Original analysis fact lost its intent or operation unit.")
        journal._put(
            db, attempt, key, dict(intent_sha256=digest(intent), start_sha256=digest(start), **fact)
        )


def _created(db, attempt, intent):
    fact = journal._event(db, attempt, CREATE_RESULT)
    if fact is None:
        return None
    start = journal._event(db, attempt, CREATE_START)
    if (
        start is None
        or start.payload.get("operation") != CREATE_START
        or start.payload.get("intent_sha256") != digest(intent)
        or fact.payload.get("intent_sha256") != digest(intent)
        or fact.payload.get("start_sha256") != start.result_digest
    ):
        raise StaleFence("Analysis create fact lost its original unit binding.")
    reservation = journal._event(db, attempt, RESERVATION)
    sa = journal._event(db, attempt, SA_START)
    ready = journal._event(db, attempt, SA_RESULT)
    if (
        reservation is None
        or reservation.payload
        != dict(
            intent_sha256=digest(intent),
            namespace=intent["namespace"],
            context_sha256=start.payload.get("context_sha256"),
        )
        or sa is None
        or sa.payload.get("operation") != SA_START
        or sa.payload.get("intent_sha256") != digest(intent)
        or sa.payload.get("context_sha256") != start.payload.get("context_sha256")
        or ready is None
        or ready.payload
        != dict(intent_sha256=digest(intent), start_sha256=sa.result_digest, ready=True)
    ):
        raise StaleFence("Analysis creation lost original capacity/service-account custody.")
    reply = fact.payload.get("reply", {})
    meta = reply.get("metadata", {})
    if (
        meta.get("name") != intent["name"]
        or meta.get("namespace") != intent["namespace"]
        or not isinstance(meta.get("uid"), str)
        or not meta["uid"].strip()
        or reply.get("spec", {}).get("suspend") is not True
    ):
        raise StaleFence("Original analysis creation fact lost its identity/specification.")
    return reply


def _activated(db, attempt, intent, created):
    keys = db.scalars(
        select(WorkflowEvent.event_key).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key.startswith(ACT_RESULT)
        )
    )
    result = False
    for key in keys:
        unit = journal._event(db, attempt, ACT_START + key.removeprefix(ACT_RESULT))
        fact = journal._event(db, attempt, key)
        if (
            unit is None
            or unit.payload.get("operation") != unit.event_key
            or unit.payload.get("intent_sha256") != digest(intent)
            or unit.payload.get("context_sha256")
            != journal._event(db, attempt, CREATE_START).payload.get("context_sha256")
            or fact.payload
            != dict(
                intent_sha256=digest(intent),
                start_sha256=unit.result_digest,
                uid=created["metadata"]["uid"],
                active=True,
            )
        ):
            raise StaleFence("Original analysis activation fact lost its unit.")
        result = True
    return result


def verify_published_identity(db, attempt, identity, source):
    original = journal._event(db, attempt, INTENT)
    if original is None:
        if (
            db.scalar(
                select(WorkflowEvent.id)
                .where(
                    WorkflowEvent.attempt_id == attempt.id,
                    WorkflowEvent.event_key.in_([CREATE_START, CREATE_RESULT]),
                )
                .limit(1)
            )
            is not None
        ):
            raise StaleFence("Original analysis factory creation lost its intent.")
        return
    _original_source(db, attempt, original.payload)
    created = _created(db, attempt, original.payload)
    if (
        created is None
        or identity.get("uid") != created["metadata"]["uid"]
        or identity.get("job_name") != created["metadata"]["name"]
        or identity.get("namespace") != created["metadata"]["namespace"]
        or str(source.id) != original.payload["outbox_id"]
        or identity.get("manifest_sha256") != digest(source.payload["manifest"])
    ):
        raise StaleFence("Analysis published receipt differs from original factory creation.")


def _original_source(db, attempt, intent):
    source = db.get(OutboxEntry, uuid.UUID(intent["outbox_id"]), populate_existing=True)
    command = db.get(WorkflowCommand, source.command_id, populate_existing=True) if source else None
    run = db.get(ModelRun, attempt.model_run_id, populate_existing=True)
    version = (
        db.get(DatasetVersion, run.dataset_version_id, populate_existing=True) if run else None
    )
    if (
        source is None
        or command is None
        or run is None
        or version is None
        or source.topic != TOPIC
        or source.aggregate_id != run.id
        or source.aggregate_type != "model_run"
        or source.project_id != attempt.project_id
        or command.request_hash != digest(command.request_payload)
    ):
        raise StaleFence("Original analysis source disappeared.")
    manifest = copy.deepcopy(source.payload["manifest"])
    manifest["spec"]["suspend"] = True
    if _intent((source, command, attempt, run, version), manifest) != intent:
        raise StaleFence("Analysis original source changed.")
    return source


def cleanup_source(db, attempt):
    """Return only original factory facts; unknown create outcomes stay unresolved."""
    old = journal._event(db, attempt, INTENT)
    if old is None:
        identity, source = reconciler._analysis_submission(db, attempt)
        return identity, source, None
    intent = old.payload
    source = _original_source(db, attempt, intent)
    created = _created(db, attempt, intent)
    if created is None and journal._event(db, attempt, CREATE_START) is not None:
        raise DeletionPending("Original analysis create outcome is unknown; no UID is inferred.")
    for key in db.scalars(
        select(WorkflowEvent.event_key).where(
            WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key.startswith(ACT_START)
        )
    ):
        unit = journal._event(db, attempt, key)
        result = journal._event(db, attempt, ACT_RESULT + key.removeprefix(ACT_START))
        if result is None:
            remaining = (
                datetime.fromisoformat(unit.payload["expires_at"]) - datetime.now(UTC)
            ).total_seconds()
            if remaining > 0:
                pending = DeletionPending(
                    "Original analysis activation still owns its native lease."
                )
                pending.retry_after_seconds = int(remaining) + 1
                raise pending
    reservation = journal._event(db, attempt, RESERVATION)
    context = reservation.payload["context_sha256"] if reservation else None
    if created is not None:
        unit = journal._event(db, attempt, CREATE_START)
        if unit.payload.get("context_sha256") != context:
            raise StaleFence("Original analysis create context lost its reservation.")
    identity = dict(
        namespace=intent["namespace"],
        job_name=intent["name"],
        uid=created["metadata"]["uid"] if created else None,
        outbox_id=str(source.id),
        manifest_sha256=digest(source.payload["manifest"]),
    )
    return identity, source, context


def _retire(db, rows, worker_id):
    entry, _, attempt, run, _ = rows
    if attempt.status in ACTIVE and run.status in {
        RunStatus.QUEUED,
        RunStatus.PRECHECK_RUNNING,
        RunStatus.RUNNING,
    }:
        return False
    old = journal._event(db, attempt, INTENT)
    if old is None:
        _finish(db, rows, worker_id)
        return True
    if attempt.status in ACTIVE:
        transition_attempt(
            attempt,
            AttemptStatus.CANCELLED,
            fencing_token=attempt.fencing_token,
            expected_cas_version=attempt.terminal_cas_version,
        )
    from automl_api.services.analysis_cleanup import retirement_complete

    identity, source, _ = cleanup_source(db, attempt)
    if retirement_complete(db, attempt):
        _finish(db, rows, worker_id)
        return True
    reconciler._queue_analysis_cleanup(db, attempt, identity, source)
    defer_outbox(db, entry, worker_id=worker_id)
    return True


def _publish(db, rows, intent, created):
    entry, _, attempt, run, _ = rows
    value = dict(
        namespace=intent["namespace"],
        job_name=intent["name"],
        uid=created["metadata"]["uid"],
        outbox_id=str(entry.id),
        manifest_sha256=digest(entry.payload["manifest"]),
    )
    old = journal._event(db, attempt, "analysis.submitted")
    if old is not None and old.payload != value:
        raise StaleFence("Analysis published UID differs from original creation.")
    journal._put(db, attempt, "analysis.submitted", value)
    if attempt.status == AttemptStatus.CLAIMED:
        transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token=attempt.fencing_token)
    run.tags = {**run.tags, "desired_state": "kubernetes_submitted", "waiting_for_capacity": False}


def _finish(db, rows, worker_id):
    complete_outbox(db, rows[0].id, worker_id=worker_id, delivered=True)
    command = rows[1]
    if command.status == CommandStatus.PENDING:
        transition_command(command, CommandStatus.RUNNING)
    if command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)


def _spec_matches(wanted, actual, uid, name, path=()):
    """Accept requested values plus known API defaults, never injected workloads/credentials."""
    defaults = {
        (): dict(
            parallelism=1,
            completions=1,
            completionMode="NonIndexed",
            manualSelector=False,
            podReplacementPolicy="TerminatingOrFailed",
        ),
        ("template", "metadata"): dict(creationTimestamp=None),
        ("template", "spec"): dict(
            dnsPolicy="ClusterFirst",
            enableServiceLinks=True,
            schedulerName="default-scheduler",
            securityContext={},
            terminationGracePeriodSeconds=30,
        ),
    }.get(path, {})
    if isinstance(wanted, dict):
        if not isinstance(actual, dict):
            return False
        defaults = dict(defaults)
        if path == ():
            defaults["selector"] = {"matchLabels": {"batch.kubernetes.io/controller-uid": uid}}
        if path == ("template", "metadata", "labels"):
            defaults.update(
                {
                    "batch.kubernetes.io/controller-uid": uid,
                    "controller-uid": uid,
                    "batch.kubernetes.io/job-name": name,
                    "job-name": name,
                }
            )
        if path == ("template", "spec") and wanted.get("serviceAccountName"):
            defaults["serviceAccount"] = wanted["serviceAccountName"]
        if path[-1:] == ("containers",):
            defaults.update(
                terminationMessagePath="/dev/termination-log",
                terminationMessagePolicy="File",
                resources={},
            )
        return all(
            (path[-1:] == ("env",) and k == "value" and v == "" and k not in actual)
            or (k in actual and _spec_matches(v, actual[k], uid, name, path + (k,)))
            for k, v in wanted.items()
        ) and all(k in wanted or (k in defaults and v == defaults[k]) for k, v in actual.items())
    if isinstance(wanted, list):
        return (
            isinstance(actual, list)
            and len(wanted) == len(actual)
            and all(
                _spec_matches(a, b, uid, name, path) for a, b in zip(wanted, actual, strict=True)
            )
        )
    if path[-1:] == ("sizeLimit",) or (
        len(path) > 2 and path[-3] == "resources" and path[-2] in {"requests", "limits"}
    ):
        try:
            return parse_quantity(wanted) == parse_quantity(actual)
        except (ValueError, TypeError):
            return False
    return type(wanted) is type(actual) and wanted == actual


def reconcile_analysis_submission(factory, entry_id, worker_id, *, k8s_factory):
    try:
        from automl_api.services.analysis_migration import migrate_legacy_delivery

        if migrate_legacy_delivery(factory, entry_id, worker_id, k8s_factory=k8s_factory):
            return
        with factory() as db, db.begin():
            rows = _rows(db, entry_id, worker_id)
            if rows is None:
                return
            if _retire(db, rows, worker_id):
                return
            manifest = _plan(rows)
            intent = _intent(rows, manifest)
            attempt = rows[2]
            attempt_id = attempt.id
            delivery = _delivery(rows[0])
            old = journal._event(db, attempt, INTENT)
            if old is None:
                if attempt.status not in {AttemptStatus.PENDING, AttemptStatus.CLAIMED}:
                    reconciler._analysis_submission(db, attempt)
                    _finish(db, rows, worker_id)
                    return
                _authority(rows)
                if journal._event(db, attempt, "analysis.submitted") is not None:
                    raise StaleFence(
                        "Legacy analysis native history requires original qualification."
                    )
                if attempt.status != AttemptStatus.PENDING or rows[0].delivery_attempts != 1:
                    raise StaleFence(
                        "Unjournaled legacy analysis delivery needs native qualification."
                    )
                if attempt.status == AttemptStatus.PENDING:
                    transition_attempt(
                        attempt, AttemptStatus.CLAIMED, fencing_token=attempt.fencing_token
                    )
                journal._put(db, attempt, INTENT, intent)
            elif old.payload != intent:
                raise StaleFence("Original analysis intent changed.")
            created = _created(db, attempt, intent)
            activated = created is not None and _activated(db, attempt, intent, created)
            if not activated:
                _authority(rows)
            if created is None and journal._event(db, attempt, CREATE_START) is not None:
                raise DeletionPending(
                    "Original analysis create is unknown; name adoption/reissue is forbidden."
                )
            if not activated and rows[0].delivery_attempts > attempt.retry_budget:
                raise DeletionPending("Analysis retry budget requires operator qualification.")
            ready = journal._event(db, attempt, SA_RESULT)
            if ready is None and journal._event(db, attempt, SA_START) is not None:
                raise DeletionPending("Original analysis service-account outcome is unknown.")
            reserved = journal._event(db, attempt, RESERVATION)
        k8s = k8s_factory()
        context = journal._context(k8s)
        if "analysis_legacy_migration" in rows[0].payload:
            legacy = rows[0].payload["analysis_legacy_migration"]
            if legacy["units"][legacy["selected_unit"]]["context_sha256"] != context:
                raise StaleFence("Analysis create context differs from original legacy preflight.")
        if k8s.settings.training_namespace != intent["namespace"]:
            raise StaleFence("Analysis capacity context uses another namespace.")
        if reserved is not None and reserved.payload != dict(
            intent_sha256=digest(intent), namespace=intent["namespace"], context_sha256=context
        ):
            raise StaleFence("Original analysis reservation/context changed.")
        if activated:
            with factory() as db, db.begin():
                rows = _current(db, entry_id, worker_id, intent, delivery)
                if rows is not None:
                    if _retire(db, rows, worker_id):
                        return
                    _publish(db, rows, intent, created)
                    _finish(db, rows, worker_id)
            return
        if reserved is None:
            capacity = k8s.capacity_snapshot().capacity
            with factory() as db, db.begin():
                from automl_api.services.training import _lock_training_admission

                _lock_training_admission(db)
                rows = _current(db, entry_id, worker_id, intent, delivery)
                if rows is None:
                    return
                _authority(rows)
                holds = list(
                    db.scalars(
                        select(WorkflowEvent).where(
                            WorkflowEvent.event_key == RESERVATION,
                            WorkflowEvent.payload["namespace"].astext == intent["namespace"],
                        )
                    )
                )
                count = 0
                reserved_runs = set()
                for hold in holds:
                    candidate = db.get(WorkflowAttempt, hold.attempt_id)
                    # A terminal run does not prove an uncertain physical create retired.
                    from automl_api.services.analysis_cleanup import retirement_complete

                    original = journal._event(db, candidate, INTENT) if candidate else None
                    if (
                        original is None
                        or hold.result_digest != digest(hold.payload)
                        or hold.payload.get("intent_sha256") != original.result_digest
                    ):
                        raise StaleFence("Analysis capacity reservation lost original custody.")
                    reserved_runs.add(candidate.model_run_id)
                    if not retirement_complete(db, candidate):
                        count += 1
                submitted = int(
                    db.scalar(
                        select(func.count(ModelRun.id)).where(
                            ModelRun.id != rows[3].id,
                            ModelRun.id.not_in(reserved_runs),
                            ModelRun.k8s_namespace == intent["namespace"],
                            ModelRun.run_kind.in_(
                                {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
                            ),
                            ModelRun.status.in_({RunStatus.QUEUED, RunStatus.RUNNING}),
                            ModelRun.tags["desired_state"].astext == "kubernetes_submitted",
                        )
                    )
                    or 0
                )
                if (
                    not capacity.connected
                    or max(capacity.active_training_jobs, submitted + count)
                    >= k8s.settings.max_concurrent_jobs
                ):
                    rows[3].tags = {**rows[3].tags, "waiting_for_capacity": True}
                    defer_outbox(db, rows[0], worker_id=worker_id)
                    return
                journal._put(
                    db,
                    rows[2],
                    RESERVATION,
                    dict(
                        intent_sha256=digest(intent),
                        namespace=intent["namespace"],
                        context_sha256=context,
                    ),
                )
        if ready is None:
            with factory() as db, db.begin():
                rows = _current(db, entry_id, worker_id, intent, delivery)
                if rows is None:
                    return
                start = _start(db, rows, intent, SA_START, context)
            journal._same_context(k8s, context)
            k8s.ensure_service_account(intent["workload_identity"], namespace=intent["namespace"])
            _fact(factory, attempt_id, intent, SA_RESULT, SA_START, start, ready=True)
        with factory() as db, db.begin():
            rows = _current(db, entry_id, worker_id, intent, delivery)
            if rows is None:
                return
            ready = journal._event(db, rows[2], SA_RESULT)
            sa = journal._event(db, rows[2], SA_START)
            if (
                sa is None
                or sa.payload.get("operation") != SA_START
                or sa.payload["context_sha256"] != context
                or ready is None
                or ready.payload
                != dict(intent_sha256=digest(intent), start_sha256=sa.result_digest, ready=True)
            ):
                raise StaleFence("Original analysis service-account/context changed.")
            created = _created(db, rows[2], intent)
            if created is None:
                start = _start(db, rows, intent, CREATE_START, context)
        if created is None:
            journal._same_context(k8s, context)
            native = k8s.create_job(copy.deepcopy(manifest))
            reply = client.ApiClient().sanitize_for_serialization(native)
            meta = reply.get("metadata", {})
            created = dict(metadata=meta, spec=reply.get("spec"))
            _fact(factory, attempt_id, intent, CREATE_RESULT, CREATE_START, start, reply=created)
            with factory() as db:
                created = _created(db, db.get(WorkflowAttempt, attempt_id), intent)
        if not _spec_matches(
            manifest["spec"], created["spec"], created["metadata"]["uid"], intent["name"]
        ):
            raise StaleFence(
                "Suspended analysis create reply differs from requested specification."
            )
        journal._same_context(k8s, context)
        with factory() as db, db.begin():
            rows = _current(db, entry_id, worker_id, intent, delivery)
            if rows is None:
                return
            _authority(rows)
            _publish(db, rows, intent, created)
        with factory() as db, db.begin():
            rows = _current(db, entry_id, worker_id, intent, delivery)
            if rows is None:
                return
            unit = journal._event(db, rows[2], CREATE_START)
            if unit.payload["context_sha256"] != context:
                raise StaleFence("Original analysis create context changed.")
            for key in db.scalars(
                select(WorkflowEvent.event_key).where(
                    WorkflowEvent.attempt_id == attempt_id,
                    WorkflowEvent.event_key.startswith(ACT_START),
                )
            ):
                old = journal._event(db, rows[2], key)
                seconds = (
                    datetime.fromisoformat(old.payload["expires_at"]) - datetime.now(UTC)
                ).total_seconds()
                if seconds > 0:
                    pending = DeletionPending("Original analysis activation still owns its lease.")
                    pending.retry_after_seconds = int(seconds) + 1
                    raise pending
            key = ACT_START + str(uuid.uuid4())
            start = _start(db, rows, intent, key, context)
        journal._same_context(k8s, context)
        active = k8s.activate_job(copy.deepcopy(created))
        wanted = copy.deepcopy(created["spec"])
        wanted["suspend"] = False
        if (
            active.get("metadata", {}).get("uid") != created["metadata"]["uid"]
            or active.get("spec") != wanted
        ):
            raise StaleFence("Analysis activation did not confirm original active resource.")
        _fact(
            factory,
            attempt_id,
            intent,
            ACT_RESULT + key.removeprefix(ACT_START),
            key,
            start,
            uid=created["metadata"]["uid"],
            active=True,
        )
        journal._same_context(k8s, context)
        with factory() as db, db.begin():
            rows = _current(db, entry_id, worker_id, intent, delivery)
            if rows is None:
                return
            if not _activated(db, rows[2], intent, created):
                raise StaleFence("Original analysis activation result disappeared.")
            if _retire(db, rows, worker_id):
                return
            _finish(db, rows, worker_id)
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
