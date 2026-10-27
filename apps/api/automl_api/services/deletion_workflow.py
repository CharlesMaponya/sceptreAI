"""Commit deletion intent and native facts separately from current-policy publication."""

import copy
import uuid
from datetime import UTC, datetime

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.enums import CommandStatus, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionStage, OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.services import deletion
from automl_api.services.deletion_runtime import cleanup_runtime, runtime_targets
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.projects import user_has_project_role
from automl_api.services.provider_import import assert_provider_uri_cleanup_safe
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

PROTOCOL = "deletion.stage.v1"


def _delivery(entry):
    return digest(
        {
            key: getattr(entry, key)
            for key in (
                "id",
                "project_id",
                "command_id",
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


def _select(db, entry_id, worker_id):
    from automl_api.services.reconciler import _lock_outbox_delivery

    project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
    if project_id is None:
        return None
    db.get(Project, project_id, with_for_update=True, populate_existing=True)
    entry = _lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    if (
        entry.topic not in {"project.delete", "run.delete"}
        or command is None
        or command.project_id != project_id
        or command.operation != entry.topic
        or command.status != CommandStatus.RUNNING
        or command.request_hash != digest(command.request_payload)
        or command.request_payload != {"id": str(entry.aggregate_id)}
    ):
        raise StaleFence("Original deletion command or delivery changed.")
    try:
        context = deletion.deletion_context(db, entry)
    except (DeletionPending, HTTPException) as exc:
        if isinstance(exc, HTTPException) and exc.status_code != 409:
            raise
        defer_outbox(
            db,
            entry,
            worker_id=worker_id,
            delay_seconds=getattr(exc, "retry_after_seconds", DeletionPending.retry_after_seconds),
        )
        entry.last_error = redact_text(exc.detail if isinstance(exc, HTTPException) else str(exc))
        return None
    if context is None:
        raise StaleFence("Deletion delivery lost its original project.")
    project, runs, run_ids, tombstone, stages, stage, done = context
    if (
        tombstone.resource_type != entry.aggregate_type
        or entry.topic != f"{tombstone.resource_type}.delete"
        or tombstone.resource_id != entry.aggregate_id
        or tombstone.requested_by_id != command.actor_id
        or len(run_ids) != len(set(run_ids))
        or {run.id for run in runs} != set(run_ids)
        or not project.settings.get("deletion_in_progress")
    ):
        raise StaleFence("Original deletion resource scope changed.")
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    if actor is None or not actor.is_active:
        raise StaleFence("Deletion actor is no longer active.")
    if not user_has_project_role(
        db,
        actor,
        project.id,
        ProjectRole.OWNER if tombstone.resource_type == "project" else ProjectRole.ADMIN,
    ):
        raise StaleFence("Deletion actor no longer has the required project role.")
    if stage is None:
        if done:
            raise StaleFence("Completed deletion flags lack an atomic metadata removal.")
        defer_outbox(db, entry, worker_id=worker_id)
        return None
    stage = db.get(DeletionStage, stage.id, with_for_update=True, populate_existing=True)
    return entry, command, context[:-2] + (stage, done)


def _source(db, entry, command, context):
    project, runs, run_ids, tombstone, _, _, _ = context
    attempts = select(WorkflowAttempt).where(WorkflowAttempt.project_id == project.id)
    if entry.topic == "run.delete":
        attempts = attempts.where(WorkflowAttempt.model_run_id.in_(run_ids))

    def snapshot(row):
        return {
            column.key: getattr(row, column.key)
            for column in row.__table__.columns
            if column.key not in {"created_at", "updated_at"}
        }

    return digest(
        jsonable_encoder(
            dict(
                project_id=project.id,
                deleting=project.settings.get("deletion_in_progress"),
                command_id=command.id,
                actor_id=command.actor_id,
                request_hash=command.request_hash,
                entry_id=entry.id,
                topic=entry.topic,
                aggregate_id=entry.aggregate_id,
                payload=entry.payload,
                tombstone_id=tombstone.id,
                resource_type=tombstone.resource_type,
                resource_id=tombstone.resource_id,
                runs=sorted([snapshot(run) for run in runs], key=lambda row: str(row["id"])),
                attempts=sorted(
                    [snapshot(attempt) for attempt in db.scalars(attempts)],
                    key=lambda row: str(row["id"]),
                ),
            )
        )
    )


def _store_digest(settings):
    return digest(
        {
            name: getattr(settings, name)
            for name in (
                "object_store_type",
                "local_object_store_path",
                "object_store_bucket",
                "object_store_endpoint",
                "object_store_public_endpoint",
                "object_store_region",
                "gcs_project",
                "azure_storage_account",
            )
        }
    )


def _intent(db, entry, command, context, stage, settings):
    project, runs, run_ids, tombstone, _, _, _ = context
    value = dict(
        protocol=PROTOCOL,
        project_id=str(project.id),
        command_id=str(command.id),
        tombstone_id=str(tombstone.id),
        stage_id=str(stage.id),
        stage=stage.resource_class,
        source_digest=_source(db, entry, command, context),
    )
    value["deployment_retirements"] = deletion.deployment_retirements(
        db, project.id, run_ids if entry.topic == "run.delete" else None
    )
    value["source"] = jsonable_encoder(
        dict(
            actor_id=command.actor_id,
            operation=command.operation,
            request_hash=command.request_hash,
            request_payload=command.request_payload,
            outbox_id=entry.id,
            topic=entry.topic,
            aggregate_type=entry.aggregate_type,
            aggregate_id=entry.aggregate_id,
            payload=entry.payload,
        )
    )
    if stage.resource_class == "runtime":
        value["targets"] = runtime_targets(
            db, project.id, runs, whole_project=entry.topic == "project.delete"
        )
    elif stage.resource_class in {"object_store", "prepared_store"}:
        value["store_digest"] = _store_digest(settings)
        value["prefixes"] = (
            [f"automl/projects/{project.id}"]
            if stage.resource_class == "prepared_store"
            else [f"projects/{project.id}"]
            if entry.topic == "project.delete"
            else [f"projects/{project.id}/runs/{run_id}" for run_id in sorted(run_ids)]
        )
    return value


def _record(stage, intent):
    value = copy.deepcopy(stage.custody)
    if not value:
        if stage.status == "completed":
            raise StaleFence("Legacy deletion completion requires original native qualification.")
        return dict(intent=intent, attempts=[])
    if value.get("intent") != intent or not isinstance(value.get("attempts"), list):
        raise StaleFence("Original deletion stage source or context changed.")
    return value


def _outcome(record, unit, field):
    value = unit.get(field)
    if (
        not isinstance(value, dict)
        or type(value.get("absent")) is not bool
        or value.get("intent_sha256") != digest(record["intent"])
        or value.get("binding_sha256") != digest(record.get("binding"))
        or unit.get("intent_sha256") != digest(record["intent"])
        or not isinstance(value.get("observed_at"), str)
    ):
        raise StaleFence("Original native deletion outcome lost its intent or target binding.")
    return value


def _proof(record):
    published = record.get("published")
    if not isinstance(published, dict) or published.get("intent_sha256") != digest(
        record["intent"]
    ):
        raise StaleFence("Deletion stage has no original published native proof.")
    unit = next(
        (unit for unit in record["attempts"] if unit["id"] == published.get("unit_id")), None
    )
    outcome = _outcome(record, unit, published.get("outcome_field")) if unit else None
    if (
        not isinstance(outcome, dict)
        or outcome.get("absent") is not True
        or published.get("outcome_sha256") != digest(outcome)
        or published.get("binding_sha256") != digest(record.get("binding"))
    ):
        raise StaleFence("Deletion stage native proof changed.")


def _binding_policy(db, record):
    if record["intent"]["stage"] not in {"object_store", "prepared_store"}:
        return
    binding = record.get("binding")
    if binding is None:
        return
    if (
        not isinstance(binding, dict)
        or not isinstance(binding.get("driver"), str)
        or not binding["driver"]
        or not isinstance(binding.get("uris"), list)
        or len(binding["uris"]) != len(record["intent"]["prefixes"])
        or any(not isinstance(uri, str) or not uri for uri in binding["uris"])
    ):
        raise StaleFence("Original deletion storage binding is invalid.")
    for uri in binding["uris"]:
        assert_provider_uri_cleanup_safe(db, uri)


def _authority(db, selected, settings):
    entry, command, context = selected
    if _store_digest(get_settings()) != _store_digest(settings):
        raise StaleFence("Configured deletion context changed during reconciliation.")
    stages, current = context[4], context[5]
    for name in deletion.STAGES[context[3].resource_type]:
        if name == current.resource_class:
            break
        previous = stages[name]
        record = _record(previous, _intent(db, entry, command, context, previous, settings))
        if previous.status != "completed":
            raise StaleFence("Previous deletion stage has not completed.")
        _proof(record)
        _binding_policy(db, record)
    intent = _intent(db, entry, command, context, current, settings)
    record = _record(current, intent)
    _binding_policy(db, record)
    return intent, record


def _fact(factory, stage_id, intent, unit_id, outcome, *, field="outcome"):
    with factory() as db, db.begin():
        stage = db.get(DeletionStage, stage_id, with_for_update=True, populate_existing=True)
        if stage is None:
            raise StaleFence("Historical deletion custody disappeared.")
        record = _record(stage, intent)
        unit = next((value for value in record["attempts"] if value["id"] == unit_id), None)
        if unit is None:
            raise StaleFence("Original native deletion attempt disappeared.")
        if unit.get(field) is not None and unit[field] != outcome:
            raise StaleFence("Original native deletion outcome cannot be overwritten.")
        unit[field] = outcome
        stage.custody = record


def _publish(db, selected, record, unit, field):
    entry, command, context = selected
    project, _, run_ids, tombstone, _, stage, _ = context
    _outcome(record, unit, field)
    record["published"] = dict(
        intent_sha256=digest(record["intent"]),
        unit_id=unit["id"],
        outcome_field=field,
        outcome_sha256=digest(unit[field]),
        binding_sha256=digest(record.get("binding")),
    )
    stage.custody = record
    stage.status = "completed"
    stage.heartbeat_at = datetime.now(UTC)
    stage.lease_owner = stage.lease_expires_at = None
    if stage.resource_class == "metadata":
        deletion._delete_metadata(db, entry, project, run_ids)
        tombstone.status, tombstone.completed_at = "complete", datetime.now(UTC)
        if entry.topic == "run.delete":
            complete_outbox(db, entry.id, worker_id=entry.lease_owner, delivered=True)
            transition_command(command, CommandStatus.SUCCEEDED)
    else:
        defer_outbox(db, entry, worker_id=entry.lease_owner)


def _settle(factory, entry_id, worker_id, exc):
    from automl_api.services.reconciler import _lock_outbox_delivery

    with factory() as db, db.begin():
        project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
        if project_id is None:
            return
        project = db.get(Project, project_id, with_for_update=True)
        entry = _lock_outbox_delivery(db, entry_id, worker_id)
        if entry is None:
            return
        if isinstance(exc, DeletionPending):
            defer_outbox(db, entry, worker_id=worker_id, delay_seconds=exc.retry_after_seconds)
            entry.last_error = redact_text(str(exc))
        else:
            complete_outbox(
                db, entry_id, worker_id=worker_id, delivered=False, error=redact_text(str(exc))
            )
            if project is not None:
                project.settings = {**project.settings, "deletion_error": redact_text(str(exc))}


def reconcile_deletion(factory, entry_id, worker_id, *, k8s=None):
    settings = copy.deepcopy(get_settings())
    try:
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, _, context = selected
            delivery = _delivery(entry)
            intent, record = _authority(db, selected, settings)
            stage = context[5]
            stage_id = stage.id
            stage.custody = record
            if record.get("published") is not None:
                _proof(record)
                raise StaleFence("Native stage publication lost its matching logical state.")
            for unit in record["attempts"]:
                for field in ("outcome", "reconciliation"):
                    if unit.get(field) is not None and _outcome(record, unit, field)["absent"]:
                        _publish(db, selected, record, unit, field)
                        return
            if stage.resource_class == "metadata":
                unit = dict(
                    id=str(uuid.uuid4()),
                    intent_sha256=digest(intent),
                    outcome=dict(
                        absent=True,
                        sql_only=True,
                        intent_sha256=digest(intent),
                        binding_sha256=digest(None),
                        observed_at=datetime.now(UTC).isoformat(),
                    ),
                )
                record["attempts"].append(unit)
                _publish(db, selected, record, unit, "outcome")
                return
            if stage.lease_expires_at is not None and stage.lease_expires_at > datetime.now(UTC):
                pending = DeletionPending(
                    "The original native deletion attempt still owns its lease."
                )
                pending.retry_after_seconds = max(
                    1, int((stage.lease_expires_at - datetime.now(UTC)).total_seconds()) + 1
                )
                raise pending
            inspect_only = any(unit.get("outcome") is None for unit in record["attempts"])

        if intent["stage"] == "runtime":
            if intent["targets"]:
                k8s = k8s or KubernetesTrainingClient()
            binding = dict(targets_sha256=digest(intent["targets"]))
            store = None
        else:
            if _store_digest(get_settings()) != intent["store_digest"]:
                raise StaleFence("Configured deletion store changed before construction.")
            store = deletion.get_object_store(settings)
            binding = dict(
                driver=store.driver_name,
                uris=[store.uri_for_key(prefix) for prefix in intent["prefixes"]],
            )
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            if _delivery(selected[0]) != delivery:
                raise StaleFence("Deletion delivery changed before native custody committed.")
            current, record = _authority(db, selected, settings)
            if current != intent or (
                record.get("binding") is not None and record["binding"] != binding
            ):
                raise StaleFence("Original deletion native target or source changed.")
            if intent["stage"] != "runtime":
                for uri in binding["uris"]:
                    assert_provider_uri_cleanup_safe(db, uri)
            stage = selected[2][5]
            if stage.lease_expires_at is not None and stage.lease_expires_at > datetime.now(UTC):
                pending = DeletionPending("Another native deletion attempt owns this stage.")
                pending.retry_after_seconds = max(
                    1, int((stage.lease_expires_at - datetime.now(UTC)).total_seconds()) + 1
                )
                raise pending
            unit_id = str(uuid.uuid4())
            record["binding"] = binding
            record["attempts"].append(
                dict(
                    id=unit_id,
                    delivery=delivery,
                    worker_id=worker_id,
                    lease_expires_at=selected[0].lease_expires_at.isoformat(),
                    intent_sha256=digest(intent),
                    started_at=datetime.now(UTC).isoformat(),
                    recovery=inspect_only,
                )
            )
            stage.custody = record
            stage.lease_owner, stage.lease_expires_at = unit_id, selected[0].lease_expires_at

        try:
            if intent["stage"] == "runtime":
                absent = cleanup_runtime(k8s, intent["targets"], inspect_only=inspect_only)
                if inspect_only and not absent:
                    absent = cleanup_runtime(k8s, intent["targets"])
            else:
                absent = True
                for prefix in intent["prefixes"]:
                    observed = store.prefix_absent(prefix) if inspect_only else False
                    if type(observed) is not bool:
                        raise StaleFence("Native prefix absence is not qualified.")
                    if not observed:
                        store.delete_prefix(prefix)
                    observed = store.prefix_absent(prefix)
                    if type(observed) is not bool:
                        raise StaleFence("Native prefix absence is not qualified.")
                    if not observed:
                        absent = False
            outcome = dict(
                absent=absent,
                intent_sha256=digest(intent),
                observed_at=datetime.now(UTC).isoformat(),
                binding_sha256=digest(binding),
            )
        except DeletionPending as exc:
            outcome = dict(
                absent=False,
                intent_sha256=digest(intent),
                observed_at=datetime.now(UTC).isoformat(),
                pending=redact_text(str(exc)),
                retry_after_seconds=exc.retry_after_seconds,
                binding_sha256=digest(binding),
            )
        field = "reconciliation" if inspect_only else "outcome"
        _fact(factory, stage_id, intent, unit_id, outcome, field=field)
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            if _delivery(selected[0]) != delivery:
                raise StaleFence(
                    "Late native outcome cannot acknowledge another deletion delivery."
                )
            current, record = _authority(db, selected, settings)
            if current != intent or record.get("binding") != binding:
                raise StaleFence("Deletion source or native target changed before publication.")
            stage = selected[2][5]
            if stage.lease_owner != unit_id:
                raise StaleFence("Native deletion attempt lost stage ownership.")
            unit = next(unit for unit in record["attempts"] if unit["id"] == unit_id)
            if _outcome(record, unit, field) != outcome:
                raise StaleFence("Original native deletion fact changed before publication.")
            stage.lease_owner = stage.lease_expires_at = None
            if outcome["absent"]:
                _publish(db, selected, record, unit, field)
            else:
                defer_outbox(
                    db,
                    selected[0],
                    worker_id=worker_id,
                    delay_seconds=outcome.get("retry_after_seconds", 5),
                )
                selected[0].last_error = (
                    outcome.get("pending")
                    or "Waiting for qualified original native deletion closure."
                )
    except StaleFence:
        raise
    except Exception as exc:
        _settle(factory, entry_id, worker_id, exc)
        if not isinstance(exc, DeletionPending):
            raise
