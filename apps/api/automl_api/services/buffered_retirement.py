"""Retire original unpublished buffered custody without adopting another object."""

import copy
import logging
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import String, and_, cast, select
from sqlalchemy.orm import aliased

from automl_api.core.config import get_settings
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import BufferedRetirementRead
from automl_api.services import buffered_chunks
from automl_api.services import buffered_execution as execution
from automl_api.services import buffered_uploads as admission
from automl_api.services.upload_admission import _authorize
from automl_api.services.workflow_state import (
    StaleFence,
    begin_command,
)
from automl_api.services.workflow_state import (
    canonical_request_hash as digest,
)
from automl_api.storage.contracts import (
    UploadContractError,
    embedded_object_version,
    immutable_object_version,
)
from automl_api.storage.object_store import get_object_store

PROTOCOL = "dataset.buffered.retire.v1"
MARKER = "Original buffered retirement:"
LOGGER = logging.getLogger(__name__)


def _snapshot(row, original, entry, phase):
    return jsonable_encoder(
        dict(
            input_id=row.command_id,
            project_id=row.project_id,
            phase=phase,
            manifest_digest=row.manifest_digest,
            byte_size=row.byte_size,
            content_sha256=row.content_sha256,
            native_receipt=copy.deepcopy(row.native_receipt),
            admitted_at=row.manifest.get("admitted_at"),
            expires_at=row.manifest.get("expires_at"),
            command={
                name: getattr(original, name)
                for name in (
                    "actor_id",
                    "operation",
                    "idempotency_key",
                    "request_hash",
                    "request_payload",
                    "resource_type",
                    "resource_id",
                )
            },
            delivery={
                name: getattr(entry, name)
                for name in (
                    "id",
                    "project_id",
                    "command_id",
                    "topic",
                    "aggregate_type",
                    "aggregate_id",
                    "payload",
                )
            },
        )
    )


def _target(row, phase):
    if phase == "admitted" and row.native_receipt == {}:
        return {"kind": "unissued"}
    if phase != "recorded":
        raise UploadContractError(
            "Original buffered outcome has no qualified immutable retirement target."
        )
    receipt = row.native_receipt
    if row.manifest["driver"] == "embedded":
        version = embedded_object_version(receipt.get("object_version"))
    else:
        version = immutable_object_version(receipt.get("object_version"))
    return dict(kind="version", uri=receipt["uri"], version=version, driver=receipt["driver"])


def _authorization(actor_id, token_version, purpose):
    return dict(
        kind="user" if purpose == "abort" else "expiry_policy",
        actor_id=str(actor_id),
        token_version=token_version if purpose == "abort" else None,
    )


def _valid_authorization(value, payload):
    if not isinstance(value, dict) or value.get("actor_id") != payload["actor_id"]:
        return False
    if payload["purpose"] == "expire":
        return value.get("kind") == "expiry_policy" and value.get("token_version") is None
    return (
        value.get("kind") == "user"
        and type(value.get("token_version")) is int
        and value["token_version"] >= 0
    )


def _journal(owner, payload):
    journal = copy.deepcopy(owner.response_payload)
    if (
        not isinstance(journal, dict)
        or journal.get("intent_digest") != digest(payload)
        or not isinstance(journal.get("attempts"), list)
    ):
        raise StaleFence("Original buffered retirement journal changed.")
    identifiers = set()
    for ordinal, unit in enumerate(journal["attempts"], 1):
        if (
            not isinstance(unit, dict)
            or not isinstance(unit.get("id"), str)
            or unit["id"] in identifiers
            or not _valid_authorization(unit.get("authorization"), payload)
            or unit.get("ordinal") != ordinal
            or type(unit.get("retry_budget")) is not int
            or unit["retry_budget"] < ordinal
            or unit.get("intent_digest") != digest(payload)
            or unit.get("target_digest") != digest(payload["target"])
        ):
            raise StaleFence("Original buffered retirement attempt changed.")
        identifiers.add(unit["id"])
        if "outcome" in unit:
            _outcome(unit, payload)
    return journal


def _outcome(unit, payload):
    value = unit["outcome"]
    if (
        not isinstance(value, dict)
        or type(value.get("absent")) is not bool
        or value.get("intent_digest") != digest(payload)
        or value.get("target_digest") != digest(payload["target"])
        or not isinstance(value.get("observed_at"), str)
    ):
        raise StaleFence("Original buffered absence lost its intent or target binding.")
    return value


def _bound(row, original, entry, owner):
    if any(value is None for value in (row, original, entry, owner)):
        raise UploadContractError("Original buffered retirement history is missing.")
    admission.validate_input_source(row, original)
    admission.validate_input_delivery(row, original, [entry])
    payload = owner.request_payload
    if not isinstance(payload, dict) or not isinstance(payload.get("source"), dict):
        raise StaleFence("Original buffered retirement request is malformed.")
    marker = MARKER + str(owner.id)
    if (
        owner.project_id != row.project_id
        or owner.resource_id != row.command_id
        or owner.operation != PROTOCOL
        or owner.resource_type != PROTOCOL
        or owner.request_hash != digest(payload)
        or payload.get("input_id") != str(row.command_id)
        or payload.get("actor_id") != str(owner.actor_id)
        or payload.get("purpose") not in {"abort", "expire"}
        or payload.get("source")
        != _snapshot(row, original, entry, payload.get("source", {}).get("phase"))
        or payload.get("target") != _target(row, payload["source"]["phase"])
        or original.status != CommandStatus.CANCELLED
        or original.terminal_reason != marker
        or original.response_status is not None
        or original.response_payload
        or entry.status != OutboxStatus.DEAD
        or entry.last_error != marker
        or entry.lease_owner is not None
        or entry.lease_expires_at is not None
    ):
        raise StaleFence("Original buffered retirement source or owner changed.")
    if payload.get("producer_quarantine") is not None:
        from automl_api.services.buffered_quarantine import record

        record(payload["producer_quarantine"], row, original, entry)
    journal = _journal(owner, payload)
    if owner.retry_count != len(journal["attempts"]):
        raise StaleFence("Original buffered retirement retry accounting changed.")
    return payload, journal


def _closure(row, original, entry, owner):
    payload, journal = _bound(row, original, entry, owner)
    published = journal.get("published", {})
    unit = next((u for u in journal["attempts"] if u["id"] == published.get("unit_id")), None)
    if (
        row.phase != "retired"
        or owner.status != CommandStatus.SUCCEEDED
        or owner.response_status != 200
        or owner.lease_owner is not None
        or owner.lease_expires_at is not None
        or unit is None
        or not _outcome(unit, payload)["absent"]
        or published.get("outcome_digest") != digest(unit["outcome"])
        or published.get("result_digest") != digest(published.get("result"))
        or not _valid_authorization(published.get("authorization"), payload)
    ):
        raise StaleFence("Original buffered retirement has no qualified terminal proof.")
    result = BufferedRetirementRead.model_validate(published.get("result"))
    if (
        result.id != row.command_id
        or result.status != ("aborted" if payload["purpose"] == "abort" else "expired")
        or result.retired_at.tzinfo is None
    ):
        raise StaleFence("Original buffered retirement response changed.")
    return dict(
        owner_id=str(owner.id),
        delivery_id=str(entry.id),
        request_hash=owner.request_hash,
        intent=copy.deepcopy(payload),
        attempts=copy.deepcopy(journal["attempts"]),
        retry_count=owner.retry_count,
        max_retries=owner.max_retries,
        target=payload["target"],
        native_outcome=copy.deepcopy(unit["outcome"]),
        outcome_digest=published["outcome_digest"],
        authorization=copy.deepcopy(published["authorization"]),
        result=result.model_dump(mode="json"),
    )


def require_retired(db, row):
    original = db.get(WorkflowCommand, row.command_id, populate_existing=True)
    if original is None or not (original.terminal_reason or "").startswith(MARKER):
        raise UploadContractError("Buffered retirement lost its original owner anchor.")
    try:
        owner_id = uuid.UUID(original.terminal_reason[len(MARKER) :])
    except ValueError as exc:
        raise UploadContractError("Buffered retirement owner anchor is invalid.") from exc
    entries = list(
        db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == row.project_id,
                OutboxEntry.command_id == row.command_id,
                OutboxEntry.topic == admission.TOPIC,
            )
        )
    )
    admission.validate_input_delivery(row, original, entries)
    owner = db.get(WorkflowCommand, owner_id, populate_existing=True)
    return _closure(row, original, entries[0], owner)


def released_reservations(db, project_id):
    """Stream metadata-only proofs in one query; ambiguous histories stay charged."""
    original, owner = aliased(WorkflowCommand), aliased(WorkflowCommand)
    query = (
        select(BufferedUploadInput, original, OutboxEntry, owner)
        .select_from(BufferedUploadInput)
        .outerjoin(
            original,
            and_(
                original.id == BufferedUploadInput.command_id,
                original.project_id == BufferedUploadInput.project_id,
            ),
        )
        .outerjoin(
            OutboxEntry,
            and_(
                OutboxEntry.project_id == BufferedUploadInput.project_id,
                OutboxEntry.command_id == BufferedUploadInput.command_id,
                OutboxEntry.topic == admission.TOPIC,
            ),
        )
        .outerjoin(owner, MARKER + cast(owner.id, String) == original.terminal_reason)
        .where(BufferedUploadInput.project_id == project_id, BufferedUploadInput.phase == "retired")
        .order_by(BufferedUploadInput.command_id)
        .execution_options(yield_per=100, populate_existing=True)
    )
    released, current, matches = 0, None, []

    def qualified(rows):
        if len(rows) != 1:
            return 0
        try:
            _closure(*rows[0])
        except (ValueError, KeyError, TypeError, AttributeError):
            return 0
        return rows[0][0].byte_size

    for values in db.execute(query):
        identifier = values[0].command_id
        if current is not None and identifier != current:
            released += qualified(matches)
            matches = []
        current = identifier
        if len(matches) < 2:
            matches.append(values)
    return released + qualified(matches)


def _rows(db, project_id, input_id, purpose, actor_id, token_version, settings):
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if project is None:
        raise HTTPException(404, "Project not found.")
    if project.status != ProjectStatus.ACTIVE or project.settings.get("deletion_in_progress"):
        raise StaleFence("Project deletion prevents buffered retirement.")
    if purpose == "abort":
        _authorize(db, project_id, actor_id, token_version)
    elif purpose != "expire":
        raise UploadContractError("Unknown buffered retirement purpose.")
    if (
        db.scalar(
            select(DeletionTombstone.id)
            .where(
                DeletionTombstone.project_id == project_id,
                DeletionTombstone.resource_type == "project",
            )
            .with_for_update()
            .limit(1)
        )
        is not None
    ):
        raise StaleFence("Project deletion retains original buffered custody.")
    original = db.get(WorkflowCommand, input_id, with_for_update=True, populate_existing=True)
    row = db.get(BufferedUploadInput, input_id, with_for_update=True, populate_existing=True)
    if row is None or row.project_id != project_id or original is None:
        raise HTTPException(404, "Buffered input not found.")
    admission.original_input(db, original)
    entry = db.scalar(
        select(OutboxEntry)
        .where(
            OutboxEntry.project_id == project_id,
            OutboxEntry.command_id == input_id,
            OutboxEntry.topic == admission.TOPIC,
        )
        .with_for_update()
    )
    execution._backend(settings, row.manifest)
    execution._backend(get_settings(), row.manifest)
    return row, original, entry


def _eligible(db, row, purpose, phase, settings):
    if phase not in {"admitted", "recorded"} or row.phase not in {phase, "retired"}:
        raise UploadContractError("Published or unknown buffered writes cannot be retired here.")
    target = _target(row, phase)
    uri = execution.original_uri(settings, row.manifest)
    if phase == "recorded":
        from automl_api.services.provider_import import assert_provider_uri_cleanup_safe

        execution.validate_receipt(row.manifest, row.native_receipt, uri)
        assert_provider_uri_cleanup_safe(db, uri)
    if (
        db.scalar(
            select(DatasetVersion.id)
            .where(DatasetVersion.object_uri == uri)
            .with_for_update(read=True)
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError("Referenced buffered data prevents retirement.")
    if purpose == "expire":
        value = row.manifest.get("admitted_at")
        try:
            admitted = datetime.fromisoformat(value)
            expires = datetime.fromisoformat(row.manifest.get("expires_at"))
        except (ValueError, TypeError) as exc:
            raise UploadContractError(
                "Original admission time is missing; expiry was not inferred."
            ) from exc
        ttl = settings.upload_session_ttl_seconds
        if ttl != get_settings().upload_session_ttl_seconds:
            raise StaleFence("Buffered expiry policy changed during retirement.")
        if (
            admitted.tzinfo is None
            or admitted.utcoffset() != timedelta(0)
            or expires.tzinfo is None
            or expires.utcoffset() != timedelta(0)
            or expires <= admitted
            or type(ttl) is not int
            or ttl < 1
            or max(expires, admitted + timedelta(seconds=ttl)) > datetime.now(UTC)
        ):
            raise UploadContractError(
                "Original buffered input has not expired under current policy."
            )
    return target


def _fresh(db, project_id, input_id, owner_id, payload, settings, actor_id, token_version):
    row, original, entry = _rows(
        db,
        project_id,
        input_id,
        payload["purpose"],
        actor_id,
        token_version,
        settings,
    )
    owner = db.get(WorkflowCommand, owner_id, with_for_update=True, populate_existing=True)
    actual, journal = _bound(row, original, entry, owner)
    if actual != payload:
        raise StaleFence("Original buffered retirement request changed.")
    if row.phase != "retired":
        if (
            owner.status != CommandStatus.RUNNING
            or owner.response_status is not None
            or journal.get("published")
        ):
            raise StaleFence("Original buffered retirement lost its running state.")
        _eligible(db, row, payload["purpose"], payload["source"]["phase"], settings)
    return row, owner, journal


def _fact(factory, project_id, owner_id, payload, unit_id, outcome):
    # Native facts survive revoked publication authority and replaced leases.
    with factory() as db, db.begin():
        db.get(Project, project_id, with_for_update=True)
        row = db.get(BufferedUploadInput, uuid.UUID(payload["input_id"]), with_for_update=True)
        if (
            row is None
            or row.project_id != project_id
            or row.manifest_digest != payload["source"]["manifest_digest"]
            or row.native_receipt != payload["source"]["native_receipt"]
        ):
            raise StaleFence("Original buffered retirement fact lost its immutable input.")
        owner = db.get(WorkflowCommand, owner_id, with_for_update=True, populate_existing=True)
        if owner is None or owner.project_id != project_id:
            raise StaleFence("Original buffered retirement owner disappeared.")
        journal = _journal(owner, payload)
        unit = next((u for u in journal["attempts"] if u["id"] == unit_id), None)
        if unit is None or ("outcome" in unit and unit["outcome"] != outcome):
            raise StaleFence("Original buffered retirement fact cannot be replaced.")
        unit["outcome"] = outcome
        owner.response_payload = journal


def _publish(db, row, owner, journal, unit_id, lease, authorization):
    if owner.lease_owner != lease or owner.lease_expires_at <= datetime.now(UTC):
        raise StaleFence("Original buffered retirement lost its publication lease.")
    unit = next(u for u in journal["attempts"] if u["id"] == unit_id)
    if not _outcome(unit, owner.request_payload)["absent"]:
        raise StaleFence("Original buffered version is not absent.")
    result = BufferedRetirementRead(
        id=row.command_id,
        status="aborted" if owner.request_payload["purpose"] == "abort" else "expired",
        retired_at=datetime.now(UTC),
    ).model_dump(mode="json")
    journal["published"] = dict(
        unit_id=unit_id,
        outcome_digest=digest(unit["outcome"]),
        result_digest=digest(result),
        authorization=authorization,
        result=result,
    )
    owner.response_payload, owner.response_status, owner.status = (
        journal,
        200,
        CommandStatus.SUCCEEDED,
    )
    owner.lease_owner = owner.lease_expires_at = None
    row.phase, row.content = "retired", None
    buffered_chunks.clear_parts(db, row)
    db.flush()
    require_retired(db, row)
    return result


def retire_input(
    factory, actor_id, token_version, project_id, input_id, key, *, purpose="abort", settings=None
):
    settings = settings or get_settings()
    authorization = _authorization(actor_id, token_version, purpose)
    if not isinstance(key, str) or not key.strip() or len(key) > 255:
        raise ValueError("Idempotency-Key must contain 1 to 255 characters.")
    with factory() as db, db.begin():
        row, original, entry = _rows(
            db, project_id, input_id, purpose, actor_id, token_version, settings
        )
        if (original.terminal_reason or "").startswith(MARKER):
            try:
                owner_id = uuid.UUID(original.terminal_reason[len(MARKER) :])
            except ValueError as exc:
                raise StaleFence("Original retirement owner anchor changed.") from exc
            owner = db.get(WorkflowCommand, owner_id, with_for_update=True, populate_existing=True)
            payload, journal = _bound(row, original, entry, owner)
            if (
                owner.idempotency_key != key
                or owner.actor_id != actor_id
                or payload["purpose"] != purpose
            ):
                raise HTTPException(
                    409, "The original buffered retirement request must resolve first."
                )
        else:
            phase = row.phase
            target = _eligible(db, row, purpose, phase, settings)
            producer_quarantine = None
            if entry.status == OutboxStatus.DEAD:
                from automl_api.services.buffered_quarantine import held

                held(row, original, entry)
                producer_quarantine = original.terminal_reason
            if original.status != {
                "admitted": CommandStatus.PENDING,
                "recorded": CommandStatus.RUNNING,
            }[phase] or entry.status not in {
                OutboxStatus.PENDING,
                OutboxStatus.CLAIMED,
                OutboxStatus.DEAD,
            }:
                raise StaleFence("Original buffered delivery has no qualified retirement source.")
            payload = dict(
                input_id=str(input_id),
                actor_id=str(actor_id),
                purpose=purpose,
                source=_snapshot(row, original, entry, phase),
                target=target,
            )
            if producer_quarantine is not None:
                payload["producer_quarantine"] = producer_quarantine
            owner, replayed = begin_command(
                db,
                project_id=project_id,
                actor_id=actor_id,
                operation=PROTOCOL,
                idempotency_key=key,
                payload=payload,
            )
            if replayed:
                raise StaleFence("Buffered retirement lost its independent source anchor.")
            owner.resource_type, owner.resource_id, owner.status = (
                PROTOCOL,
                input_id,
                CommandStatus.RUNNING,
            )
            owner.response_payload = dict(intent_digest=digest(payload), attempts=[])
            marker = MARKER + str(owner.id)
            original.status, original.terminal_reason = CommandStatus.CANCELLED, marker
            entry.status, entry.last_error = OutboxStatus.DEAD, marker
            entry.lease_owner = entry.lease_expires_at = None
            db.flush()
            owner_id = owner.id
            journal = _journal(owner, payload)
        if row.phase == "retired":
            return require_retired(db, row)["result"]
        _eligible(db, row, purpose, payload["source"]["phase"], settings)
        if owner.status != CommandStatus.RUNNING:
            raise StaleFence("Original buffered retirement is not running.")
        known = next(
            (u for u in journal["attempts"] if u.get("outcome", {}).get("absent") is True), None
        )
        if (
            owner.lease_expires_at is not None
            and owner.lease_expires_at > datetime.now(UTC)
            and (known is None or owner.lease_owner != known["id"])
        ):
            raise HTTPException(409, "The original native retirement still owns its lease.")
        if known:
            lease = str(uuid.uuid4())
            owner.lease_owner, owner.lease_expires_at = (
                lease,
                datetime.now(UTC) + timedelta(minutes=5),
            )
            return _publish(db, row, owner, journal, known["id"], lease, authorization)
        if owner.retry_count >= owner.max_retries:
            raise UploadContractError(
                "Original buffered retirement retry budget requires controlled recovery."
            )
        source = copy.deepcopy(row.manifest)
    store = None
    if payload["target"]["kind"] == "version":
        store = get_object_store(settings)
        if (
            store.driver_name != source["driver"]
            or store.uri_for_key(source["object_key"]) != payload["target"]["uri"]
        ):
            raise StaleFence("Original buffered retirement backend changed.")
    with factory() as db, db.begin():
        row, owner, journal = _fresh(
            db, project_id, input_id, owner_id, payload, settings, actor_id, token_version
        )
        if row.phase == "retired":
            return require_retired(db, row)["result"]
        if owner.lease_expires_at is not None and owner.lease_expires_at > datetime.now(UTC):
            raise HTTPException(409, "Another native retirement owns the original lease.")
        if owner.retry_count >= owner.max_retries:
            raise UploadContractError(
                "Original buffered retirement retry budget requires controlled recovery."
            )
        unit_id = str(uuid.uuid4())
        expiry = datetime.now(UTC) + timedelta(minutes=5)
        journal["attempts"].append(
            dict(
                id=unit_id,
                ordinal=owner.retry_count + 1,
                retry_budget=owner.max_retries,
                authorization=authorization,
                intent_digest=digest(payload),
                target_digest=digest(payload["target"]),
                expires_at=expiry.isoformat(),
            )
        )
        owner.response_payload = journal
        owner.retry_count += 1
        owner.lease_owner, owner.lease_expires_at = unit_id, expiry
    if store is not None:
        store.delete(payload["target"]["uri"], version=payload["target"]["version"])
    outcome = dict(
        absent=True,
        intent_digest=digest(payload),
        target_digest=digest(payload["target"]),
        observed_at=datetime.now(UTC).isoformat(),
    )
    _fact(factory, project_id, owner_id, payload, unit_id, outcome)
    with factory() as db, db.begin():
        row, owner, journal = _fresh(
            db, project_id, input_id, owner_id, payload, settings, actor_id, token_version
        )
        return _publish(db, row, owner, journal, unit_id, unit_id, authorization)


def cleanup_inputs(factory, *, settings=None, limit=100):
    settings = settings or get_settings()
    ttl = settings.upload_session_ttl_seconds
    if type(limit) is not int or limit < 0 or type(ttl) is not int or ttl < 1:
        raise ValueError("Buffered cleanup requires bounded nonnegative limits and a positive TTL.")
    observed = datetime.now(UTC)
    cutoff = (observed - timedelta(seconds=ttl)).isoformat()
    with factory() as db:
        candidates = list(
            db.execute(
                select(
                    BufferedUploadInput.project_id,
                    BufferedUploadInput.command_id,
                    BufferedUploadInput.updated_at,
                )
                .where(
                    BufferedUploadInput.phase.in_({"admitted", "recorded"}),
                    BufferedUploadInput.manifest["admitted_at"].astext <= cutoff,
                    BufferedUploadInput.manifest["expires_at"].astext <= observed.isoformat(),
                )
                .order_by(BufferedUploadInput.updated_at, BufferedUploadInput.command_id)
                .limit(limit)
            )
        )
    retired = 0
    for project_id, input_id, clock in candidates:
        with factory() as db, db.begin():
            db.get(Project, project_id, with_for_update=True)
            row = db.get(
                BufferedUploadInput, input_id, with_for_update=True, populate_existing=True
            )
            if row is None or row.updated_at != clock or row.phase not in {"admitted", "recorded"}:
                continue
            row.updated_at = observed
            original = db.get(WorkflowCommand, input_id)
            actor_id = original.actor_id
        try:
            retire_input(
                factory,
                actor_id,
                None,
                project_id,
                input_id,
                "buffered-expire:" + str(input_id),
                purpose="expire",
                settings=settings,
            )
        except Exception as exc:
            LOGGER.warning(
                "buffered expiry held",
                extra={"input_id": str(input_id), "error_type": type(exc).__name__},
            )
        else:
            retired += 1
    return retired
