"""Original instruction custody around SQL-free provider credential work."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import TransferInstructionRead
from automl_api.services import upload_admission as admission
from automl_api.services import uploads
from automl_api.services.upload_completion import SOURCE_FIELDS
from automl_api.services.workflow_state import begin_command, transition_command
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import TransferCursor, UploadContractError

OPERATION = "dataset.upload.instruction"
PROTOCOL = "dataset.upload.instruction.v2"


def _source(upload):
    return jsonable_encoder(
        {name: getattr(upload, name) for name in (*SOURCE_FIELDS, "provider_state")}
    )


def _select(db, actor_id, token_version, project_id, session_id, cursor, origin, settings):
    upload = uploads._progress_row(db, actor_id, token_version, project_id, session_id, settings)
    uploads._require_active(upload)
    uploads.require_allowed_origin(origin, settings)
    uploads._validate_transfer_cursor(upload, cursor)
    return upload


def _lookup(db, project_id, actor_id, session_id, key, request):
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == project_id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.idempotency_key == key,
        )
        .execution_options(populate_existing=True)
    )
    if command is not None and (
        command.actor_id != actor_id
        or command.resource_id != session_id
        or command.resource_type != OPERATION
        or command.request_hash != digest(command.request_payload)
        or (
            command.request_payload.get("request")
            if command.request_payload.get("protocol") == PROTOCOL
            else command.request_payload
        )
        != request
    ):
        raise UploadContractError("Instruction key differs from its original actor or request.")
    return command


def _receipt(value, request, source):
    result = TransferInstructionRead.model_validate(value)
    cursor = {
        **request["cursor"],
        "checksum_sha256": (request["cursor"].get("checksum_sha256") or "").lower() or None,
    }
    if (
        digest(value) != digest(result.model_dump(mode="json"))
        or result.cursor.model_dump(mode="json") != cursor
        or not result.instruction_id.strip()
        or not result.url.strip()
        or urlsplit(result.url).scheme not in {"http", "https"}
        or not urlsplit(result.url).netloc
        or any(c in result.url for c in "\r\n")
        or result.expires_at.tzinfo is None
        or result.expires_at > datetime.fromisoformat(source["expires_at"])
        or source["protocol"] == "resumable_offset"
        and result.url != source["multipart_upload_id"]
    ):
        raise UploadContractError("Provider instruction differs from its original transfer intent.")
    return result


def _journal(upload, command):
    try:
        payload = command.request_payload
        record = copy.deepcopy(command.response_payload)
        attempt = record["attempt"]
        slot = upload.instruction_state[str(payload["request"]["cursor"]["unit_number"])]
        if (
            payload["protocol"] != PROTOCOL
            or payload["actor_id"] != str(command.actor_id)
            or payload["source"]["id"] != str(upload.id)
            or payload["source"]["project_id"] != str(command.project_id)
            or command.operation != OPERATION
            or command.resource_type != OPERATION
            or command.resource_id != upload.id
            or command.request_hash != digest(payload)
            or slot["command_id"] != str(command.id)
            or slot["request_digest"] != command.request_hash
            or slot["attempt_digest"] != digest(attempt)
            or command.lease_owner
            != (None if record["phase"] in {"recorded", "published"} else attempt["id"])
            or command.lease_expires_at
            != (
                None
                if record["phase"] in {"recorded", "published"}
                else datetime.fromisoformat(attempt["expires_at"])
            )
            or type(attempt["number"]) is not int
            or not 1 <= attempt["number"] <= 5
            or type(attempt["actor_token_version"]) is not int
            or record["phase"] not in {"issued", "unknown", "recorded", "published"}
            or command.status
            != (
                CommandStatus.SUCCEEDED if record["phase"] == "published" else CommandStatus.RUNNING
            )
            or command.response_status != (200 if record["phase"] == "published" else None)
        ):
            raise UploadContractError("Original instruction ownership or custody changed.")
        if record["phase"] in {"recorded", "published"}:
            result = _receipt(record["receipt"], payload["request"], payload["source"])
            if slot.get("receipt_digest") != digest(record["receipt"]):
                raise UploadContractError("Original instruction receipt changed.")
            if record["phase"] == "published" and any(
                slot.get(name) != value
                for name, value in dict(
                    instruction_id=result.instruction_id,
                    offset=result.cursor.offset,
                    length=result.cursor.length,
                    expires_at=result.expires_at.isoformat(),
                ).items()
            ):
                raise UploadContractError("Published instruction projection changed.")
        elif "receipt" in record or "receipt_digest" in slot:
            raise UploadContractError("Known instruction cannot return to issuance.")
        return record
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise UploadContractError("Original instruction history could not be qualified.") from exc


def _save(upload, command, record):
    slot = dict(
        command_id=str(command.id),
        request_digest=command.request_hash,
        attempt_digest=digest(record["attempt"]),
    )
    if "receipt" in record:
        slot["receipt_digest"] = digest(record["receipt"])
    if record["phase"] == "published":
        result = _receipt(
            record["receipt"], command.request_payload["request"], command.request_payload["source"]
        )
        slot.update(
            instruction_id=result.instruction_id,
            offset=result.cursor.offset,
            length=result.cursor.length,
            expires_at=result.expires_at.isoformat(),
        )
    unit = str(command.request_payload["request"]["cursor"]["unit_number"])
    upload.instruction_state = {**upload.instruction_state, unit: slot}
    command.response_payload = record


def _fresh(upload, command, settings):
    if (
        _source(upload) != command.request_payload["source"]
        or admission._store_digest(settings, upload.provider_driver)
        != command.request_payload["store_digest"]
    ):
        raise UploadContractError("Instruction source or store differs from its original intent.")


def _legacy(upload, command, request):
    if command.status != CommandStatus.SUCCEEDED or command.response_status != 200:
        raise UploadContractError("Legacy instruction has no qualified known response.")
    result = _receipt(command.response_payload, request, _source(upload))
    slot = upload.instruction_state.get(str(result.cursor.unit_number), {})
    if "command_id" in slot or any(
        slot.get(name) != value
        for name, value in dict(
            instruction_id=result.instruction_id,
            offset=result.cursor.offset,
            length=result.cursor.length,
            expires_at=result.expires_at.isoformat(),
        ).items()
    ):
        raise UploadContractError("Legacy instruction no longer owns its original transfer unit.")
    if result.expires_at <= datetime.now(UTC):
        raise UploadContractError("Original instruction expired; use a new instruction key.")
    return result


def _record(
    factory, project_id, upload_id, command_id, payload, attempt, *, receipt=None, error=None
):
    with factory() as db, db.begin():
        db.get(Project, project_id, with_for_update=True)
        upload = db.get(
            DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
        )
        command = db.get(WorkflowCommand, command_id, populate_existing=True)
        if (
            command is None
            or command.request_hash != digest(payload)
            or command.request_payload != payload
        ):
            raise UploadContractError("Original instruction command changed during native work.")
        record = copy.deepcopy(command.response_payload)
        slot = (upload.instruction_state if upload else {}).get(
            str(payload["request"]["cursor"]["unit_number"]), {}
        )
        if (
            record.get("attempt") != attempt
            or record.get("phase") != "issued"
            or slot.get("command_id") != str(command_id)
        ):
            unused = dict(record.get("superseded_outcomes", {}))
            unused[attempt["id"]] = dict(receipt=receipt, error_type=error)
            command.response_payload = {**record, "superseded_outcomes": unused}
            return False
        _journal(upload, command)
        record.update(phase="recorded" if receipt is not None else "unknown")
        if receipt is not None:
            record["receipt"] = receipt
            command.lease_owner, command.lease_expires_at = None, None
        else:
            record["error_type"] = error
        _save(upload, command, record)
        return True


def issue_upload_instruction(
    factory, actor_id, token_version, project_id, session_id, cursor, key, *, origin, settings=None
):
    settings = settings or uploads.get_settings()
    cursor = cursor.model_copy(deep=True)
    request = dict(session_id=str(session_id), origin=origin, cursor=cursor.model_dump(mode="json"))
    with factory() as db, db.begin():
        upload = _select(
            db, actor_id, token_version, project_id, session_id, cursor, origin, settings
        )
        command = _lookup(db, project_id, actor_id, session_id, key, request)
        if command is not None and command.request_payload.get("protocol") != PROTOCOL:
            return _legacy(upload, command, request)
        if command is None:
            previous = upload.instruction_state.get(str(cursor.unit_number), {})
            if previous.get("command_id"):
                owner = db.get(
                    WorkflowCommand, uuid.UUID(previous["command_id"]), populate_existing=True
                )
                if owner is None:
                    raise UploadContractError("Original transfer-unit owner is missing.")
                state = _journal(upload, owner)
                if state["phase"] in {
                    "issued",
                    "unknown",
                } and owner.lease_expires_at > datetime.now(UTC):
                    raise HTTPException(409, "Original transfer-unit instruction is still pending.")
            payload = dict(
                protocol=PROTOCOL,
                actor_id=str(actor_id),
                request=request,
                source=_source(upload),
                store_digest=admission._store_digest(settings, upload.provider_driver),
            )
            command, _ = begin_command(
                db,
                project_id=project_id,
                actor_id=actor_id,
                operation=OPERATION,
                idempotency_key=key,
                payload=payload,
            )
            command.resource_id, command.resource_type = upload.id, OPERATION
            record = {}
            transition_command(command, CommandStatus.RUNNING)
        else:
            record = _journal(upload, command)
            _fresh(upload, command, settings)
        if record.get("phase") not in {"recorded", "published"}:
            if record and command.lease_expires_at > datetime.now(UTC):
                raise HTTPException(409, "Original instruction issuance is still pending.")
            number = record.get("attempt", {}).get("number", 0) + 1
            if number > 5:
                raise UploadContractError("Original instruction exhausted its native retry budget.")
            attempt = dict(
                id=str(uuid.uuid4()),
                number=number,
                actor_token_version=token_version,
                expires_at=min(
                    datetime.now(UTC) + timedelta(minutes=5), upload.expires_at
                ).isoformat(),
            )
            command.lease_owner = attempt["id"]
            command.lease_expires_at = datetime.fromisoformat(attempt["expires_at"])
            command.retry_count, command.max_retries = number, 5
            record.update(phase="issued", attempt=attempt)
            _save(upload, command, record)
        command_id, payload = command.id, copy.deepcopy(command.request_payload)
        db.flush()
        db.expunge(upload)
    if record["phase"] not in {"recorded", "published"}:
        try:
            store = uploads.get_object_store(settings)
            uploads._require_session_driver(upload, store.driver_name, settings)
            if store.uri_for_key(upload.object_key) != upload.provider_state.get(
                "upload_object_uri"
            ):
                raise UploadContractError(
                    "Instruction provider target differs from original admission."
                )
            with factory() as db, db.begin():
                current = _select(
                    db, actor_id, token_version, project_id, session_id, cursor, origin, settings
                )
                owner = _lookup(db, project_id, actor_id, session_id, key, request)
                state = _journal(current, owner)
                _fresh(current, owner, settings)
                if state["phase"] != "issued" or state["attempt"] != attempt:
                    raise UploadContractError("Original instruction lost its issuance attempt.")
            value = store.create_transfer_instruction(
                uploads.provider_upload(upload),
                TransferCursor(
                    unit_number=cursor.unit_number,
                    offset=cursor.offset,
                    length=cursor.length,
                    checksum_sha256=(cursor.checksum_sha256 or "").lower() or None,
                ),
            )
            if any(
                type(getattr(value.cursor, name)) is not int
                for name in ("unit_number", "offset", "length")
            ):
                raise UploadContractError("Provider instruction returned a malformed cursor.")
            receipt = uploads.instruction_read(value).model_dump(mode="json")
            _receipt(receipt, request, payload["source"])
        except Exception as exc:
            _record(
                factory,
                project_id,
                session_id,
                command_id,
                payload,
                attempt,
                error=type(exc).__name__,
            )
            raise
        if not _record(
            factory, project_id, session_id, command_id, payload, attempt, receipt=receipt
        ):
            raise UploadContractError("Original instruction lost its native attempt.")
    with factory() as db, db.begin():
        current = _select(
            db, actor_id, token_version, project_id, session_id, cursor, origin, settings
        )
        command = _lookup(db, project_id, actor_id, session_id, key, request)
        record = _journal(current, command)
        _fresh(current, command, settings)
        if record["phase"] not in {"recorded", "published"}:
            raise UploadContractError("Original instruction has no known provider reply.")
        result = _receipt(record["receipt"], request, payload["source"])
        if result.expires_at <= datetime.now(UTC):
            raise UploadContractError("Original instruction expired; use a new instruction key.")
        if record["phase"] != "published":
            record["phase"] = "published"
            command.response_status = 200
            transition_command(command, CommandStatus.SUCCEEDED)
            _save(current, command, record)
            current.last_progress_at = datetime.now(UTC)
        return result
