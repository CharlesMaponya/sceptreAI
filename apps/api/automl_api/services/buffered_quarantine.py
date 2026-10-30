"""Bounded delivery holds and explicit authority grants keep original buffered custody."""

import json
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectRole
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import BufferedReplayRead, UploadCleanupReplayRequest
from automl_api.services import buffered_uploads as admission
from automl_api.services.projects import require_project_role
from automl_api.services.upload_admission import _authorize
from automl_api.services.workflow_state import StaleFence, begin_command
from automl_api.services.workflow_state import canonical_request_hash as digest

PREFIX = "Buffered quarantine v1:"
REPLAY = "dataset.buffered.replay.v1"


def source(row, command, entry):
    return digest(
        dict(
            input_id=str(row.command_id),
            project_id=str(row.project_id),
            manifest_digest=row.manifest_digest,
            request_hash=command.request_hash,
            delivery_id=str(entry.id),
            delivery_payload=entry.payload,
        )
    )


def marker(row, command, entry, reason):
    return PREFIX + json.dumps(
        dict(
            source=source(row, command, entry),
            phase=row.phase,
            attempt=entry.delivery_attempts,
            budget=command.max_retries,
            reason=reason,
        ),
        sort_keys=True,
        separators=(",", ":"),
    )


def record(value, row, command, entry):
    if not isinstance(value, str) or not value.startswith(PREFIX):
        raise StaleFence("Original buffered quarantine is missing.")
    try:
        held = json.loads(value[len(PREFIX) :])
    except (ValueError, TypeError) as exc:
        raise StaleFence("Original buffered quarantine changed.") from exc
    if (
        not isinstance(held, dict)
        or set(held) != {"source", "phase", "attempt", "budget", "reason"}
        or held["source"] != source(row, command, entry)
        or held["phase"] not in {"admitted", "issued", "recorded"}
        or not (
            row.phase in {held["phase"], "retired"}
            or held["phase"] == "issued"
            and row.phase == "recorded"
        )
        or type(held["attempt"]) is not int
        or held["attempt"] < 1
        or held["attempt"] != entry.delivery_attempts
        or held["attempt"] != command.retry_count
        or type(held["budget"]) is not int
        or held["budget"] < 0
        or held["budget"] != command.max_retries
        or held["reason"] not in {"unknown_outcome", "retry_budget_exhausted"}
    ):
        raise StaleFence("Original buffered quarantine lost its source or budget binding.")
    return held


def held(row, command, entry):
    if (
        entry.status != OutboxStatus.DEAD
        or entry.lease_owner is not None
        or entry.lease_expires_at is not None
        or entry.last_error != command.terminal_reason
    ):
        raise StaleFence("Original buffered quarantine lost its delivery anchor.")
    return record(command.terminal_reason, row, command, entry)


def history(db, row, command, entry):
    nodes = list(
        db.scalars(
            select(WorkflowCommand)
            .where(
                WorkflowCommand.project_id == row.project_id,
                WorkflowCommand.operation == REPLAY,
                WorkflowCommand.resource_id == row.command_id,
            )
            .execution_options(populate_existing=True)
        )
    )
    remaining = {str(node.id): node for node in nodes}
    chain = []
    previous = None
    while remaining:
        children = [
            node
            for node in remaining.values()
            if isinstance(node.response_payload, dict)
            and isinstance(node.response_payload.get("authorization"), dict)
            and node.response_payload["authorization"].get("previous") == previous
        ]
        if len(children) != 1:
            raise StaleFence("Original buffered replay chain is missing or ambiguous.")
        node = children[0]
        proof = node.response_payload["authorization"]
        request = node.request_payload
        quarantine = proof.get("quarantine", {})
        budget = proof.get("budget")
        if (
            node.resource_type != REPLAY
            or node.status != CommandStatus.SUCCEEDED
            or node.response_status != 202
            or node.request_hash != digest(request)
            or not isinstance(request, dict)
            or set(request) != {"input_id", "reason"}
            or request["input_id"] != str(row.command_id)
            or UploadCleanupReplayRequest(reason=request["reason"]).reason != request["reason"]
            or proof.get("actor_id") != str(node.actor_id)
            or type(proof.get("token_version")) is not int
            or proof["token_version"] < 0
            or proof.get("source") != source(row, command, entry)
            or not isinstance(quarantine, dict)
            or quarantine.get("source") != proof["source"]
            or quarantine.get("phase") not in {"admitted", "issued", "recorded"}
            or type(quarantine.get("attempt")) is not int
            or quarantine["attempt"] < 1
            or type(quarantine.get("budget")) is not int
            or quarantine["budget"] < 0
            or quarantine.get("reason") not in {"retry_budget_exhausted", "unknown_outcome"}
            or (quarantine.get("reason") == "unknown_outcome" and quarantine["phase"] != "issued")
            or (
                quarantine["phase"] != "admitted"
                and (not proof.get("known_receipt") or proof["known_receipt"] != row.native_receipt)
            )
            or (quarantine["phase"] == "admitted" and proof.get("known_receipt") != {})
            or (chain and quarantine["budget"] != chain[-1]["authorization"]["budget"])
            or type(budget) is not int
            or budget != max(quarantine["attempt"], quarantine["budget"]) + 5
            or budget > 2_147_483_647
            or node.response_payload.get("result")
            != BufferedReplayRead(
                id=node.id, input_id=row.command_id, attempt_budget=budget
            ).model_dump(mode="json")
        ):
            raise StaleFence("Original buffered replay grant changed.")
        try:
            timestamp = datetime.fromisoformat(proof["authorized_at"])
            assert timestamp.tzinfo is not None and timestamp <= datetime.now(UTC)
        except (KeyError, ValueError, TypeError, AssertionError) as exc:
            raise StaleFence("Original buffered replay authorization time changed.") from exc
        chain.append(
            dict(
                id=str(node.id),
                request_hash=node.request_hash,
                request=request,
                authorization=proof,
                result=node.response_payload["result"],
            )
        )
        previous = str(node.id)
        remaining.pop(previous)
    if chain:
        last = chain[-1]["authorization"]
        if command.max_retries != last["budget"] or str(command.replayed_by) != last["actor_id"]:
            raise StaleFence("Original buffered replay budget or actor changed.")
    elif command.replayed_by is not None:
        raise StaleFence("Original buffered replay history is missing.")
    return chain


def authorize(db, row, command, entry):
    approvals = history(db, row, command, entry)
    if approvals:
        grant = approvals[-1]["authorization"]
        actor = _authorize(db, row.project_id, uuid.UUID(grant["actor_id"]), grant["token_version"])
        require_project_role(db, actor, row.project_id, ProjectRole.ADMIN)
    else:
        _authorize(db, row.project_id, command.actor_id, row.manifest["token_version"])


def settle(db, row, command, entry):
    command.retry_count = entry.delivery_attempts
    if row.phase == "issued" or entry.delivery_attempts >= command.max_retries:
        reason = "unknown_outcome" if row.phase == "issued" else "retry_budget_exhausted"
        command.terminal_reason = entry.last_error = marker(row, command, entry, reason)
        entry.status = OutboxStatus.DEAD
    else:
        entry.status = OutboxStatus.PENDING
        entry.available_at = datetime.now(UTC) + timedelta(seconds=30)
        entry.last_error = "Buffered upload retry pending; original custody retained."
    entry.lease_owner = entry.lease_expires_at = None


def resume_known(db, row, command, entry):
    if not (command.terminal_reason or "").startswith(PREFIX):
        return
    approval = held(row, command, entry)
    if approval["reason"] != "unknown_outcome" or entry.delivery_attempts >= command.max_retries:
        return
    admission.original_input(db, command)
    history(db, row, command, entry)
    entry.status = OutboxStatus.PENDING
    entry.available_at = datetime.now(UTC)
    entry.last_error = command.terminal_reason = None


def replay(factory, actor_id, token_version, project_id, input_id, key, request):
    request = UploadCleanupReplayRequest.model_validate(request)
    if not isinstance(key, str) or not key.strip() or len(key) > 255:
        raise ValueError("Idempotency-Key must contain 1 to 255 characters.")
    with factory() as db, db.begin():
        actor = _authorize(db, project_id, actor_id, token_version)
        require_project_role(db, actor, project_id, ProjectRole.ADMIN)
        original = db.get(WorkflowCommand, input_id, with_for_update=True, populate_existing=True)
        row = db.get(BufferedUploadInput, input_id, with_for_update=True, populate_existing=True)
        if row is None or row.project_id != project_id or original is None:
            raise HTTPException(404, "Buffered input not found.")
        admission.original_input(db, original)
        entry = db.scalar(
            select(OutboxEntry)
            .where(
                OutboxEntry.command_id == input_id,
                OutboxEntry.project_id == project_id,
                OutboxEntry.topic == admission.TOPIC,
            )
            .with_for_update()
        )
        node, replayed = begin_command(
            db,
            project_id=project_id,
            actor_id=actor_id,
            operation=REPLAY,
            idempotency_key=key,
            payload=dict(input_id=str(input_id), reason=request.reason),
        )
        if node.actor_id != actor_id:
            raise HTTPException(409, "This replay key belongs to another actor.")
        approvals = history(db, row, original, entry)
        if replayed:
            if not any(approval["id"] == str(node.id) for approval in approvals):
                raise StaleFence("Original buffered replay receipt is missing.")
            return BufferedReplayRead.model_validate(node.response_payload["result"])
        quarantine = held(row, original, entry)
        if row.phase not in {"admitted", "recorded"}:
            raise HTTPException(
                409, "Unknown native writes require controlled original-outcome recovery."
            )
        if (
            original.status
            != {"admitted": CommandStatus.PENDING, "recorded": CommandStatus.RUNNING}[row.phase]
        ):
            raise StaleFence("Original buffered command no longer matches its native phase.")
        from automl_api.services.buffered_execution import _backend, original_uri, validate_receipt

        settings = get_settings()
        _backend(settings, row.manifest)
        if row.phase == "recorded":
            version = validate_receipt(
                row.manifest, row.native_receipt, original_uri(settings, row.manifest)
            )
            if row.manifest["driver"] == "embedded" and version is None:
                raise StaleFence(
                    "Legacy local outcomes require controlled original-incarnation recovery."
                )
        budget = max(original.max_retries, entry.delivery_attempts) + 5
        if budget > 2_147_483_647:
            raise ValueError("Buffered replay exceeds its attempt budget range.")
        result = BufferedReplayRead(id=node.id, input_id=input_id, attempt_budget=budget)
        node.resource_type, node.resource_id, node.status, node.response_status = (
            REPLAY,
            input_id,
            CommandStatus.SUCCEEDED,
            202,
        )
        node.response_payload = dict(
            authorization=dict(
                actor_id=str(actor_id),
                token_version=token_version,
                source=source(row, original, entry),
                previous=approvals[-1]["id"] if approvals else None,
                quarantine=quarantine,
                budget=budget,
                known_receipt=row.native_receipt,
                authorized_at=datetime.now(UTC).isoformat(),
            ),
            result=result.model_dump(mode="json"),
        )
        original.max_retries, original.replayed_by, original.terminal_reason = (
            budget,
            actor_id,
            None,
        )
        entry.status, entry.available_at, entry.last_error = (
            OutboxStatus.PENDING,
            datetime.now(UTC),
            None,
        )
        db.flush()
        history(db, row, original, entry)
        return result
