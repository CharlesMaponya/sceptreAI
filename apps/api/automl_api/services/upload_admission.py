"""Reserve quota before native creation; retain the reply before fresh publication."""

import copy
import json
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus, ProjectRole, ProjectStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import ResumableUploadRead
from automl_api.services import uploads
from automl_api.services.projects import require_project_role
from automl_api.services.upload_completion import SOURCE_FIELDS
from automl_api.services.workflow_state import begin_command, transition_command
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import BeginUpload, UploadCapabilities, UploadContractError
from automl_api.storage.object_store import get_object_store

OPERATION = "dataset.upload.begin"
PROTOCOL = "dataset.upload.begin.v2"
RECORD = uploads.UPLOAD_ADMISSION_RECORD
STARTED = uploads.UPLOAD_ADMISSION_STARTED


def _authorize(db, project_id, actor_id, token_version):
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if project is None:
        raise HTTPException(404, "Project not found.")
    if project.status != ProjectStatus.ACTIVE or project.settings.get("deletion_in_progress"):
        raise HTTPException(409, "Upload admission is unavailable during project deletion.")
    actor = db.get(User, actor_id, with_for_update={"read": True}, populate_existing=True)
    if actor is None or not actor.is_active or actor.token_version != token_version:
        raise HTTPException(401, "Current authentication is required.")
    db.scalar(
        select(ProjectMembership)
        .where(ProjectMembership.project_id == project_id, ProjectMembership.user_id == actor_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    require_project_role(db, actor, project_id, ProjectRole.EDITOR)
    return actor


def _source(upload):
    return jsonable_encoder(
        {
            **{
                name: getattr(upload, name)
                for name in SOURCE_FIELDS
                if name != "multipart_upload_id"
            },
            "resume_key": upload.resume_key,
            "uri": upload.provider_state.get("upload_object_uri"),
        }
    )


def _lookup(db, project_id, actor_id, key, request):
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
        or command.request_hash != digest(request)
        or command.request_hash != digest(command.request_payload)
    ):
        raise HTTPException(409, "This Idempotency-Key belongs to another original request.")
    return command


def _store_digest(settings, driver_name):
    configured_driver = settings.object_store_type.strip().lower()
    if configured_driver == "minio":
        configured_driver = "s3_compatible"
    return digest(
        {
            "driver": driver_name,
            "configured_driver": configured_driver,
            **{
                name: getattr(settings, name)
                for name in (
                    "object_store_endpoint",
                    "object_store_public_endpoint",
                    "object_store_bucket",
                    "object_store_region",
                    "gcs_project",
                    "azure_storage_account",
                )
            },
        }
    )


def require_store(settings, provider_state, driver_name):
    record = provider_state.get(RECORD)
    if record is None and STARTED not in provider_state:
        return
    if not isinstance(record, dict) or record.get("intent", {}).get(
        "store_digest"
    ) != _store_digest(settings, driver_name):
        raise UploadContractError("Configured store differs from original upload admission.")


def _receipt(value, intent):
    source = intent["source"]
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("provider_id"), str)
        or not value["provider_id"].strip()
        or len(value["provider_id"]) > 1024
        or any(ord(c) < 32 or ord(c) == 127 for c in value["provider_id"])
        or value.get("object_key") != source["object_key"]
        or value.get("protocol") != source["protocol"]
        or type(value.get("byte_size")) is not int
        or value["byte_size"] != source["byte_size"]
        or value.get("expires_at") != source["expires_at"]
        or not isinstance(value.get("state"), dict)
        or set(value["state"])
        & {
            RECORD,
            STARTED,
            "completion_protocol",
            "upload_object_uri",
            uploads.UPLOAD_COMPLETION_RECORD,
            uploads.UPLOAD_COMPLETION_STARTED,
            uploads.UPLOAD_VERIFICATION_RECORD,
            uploads.UPLOAD_RETIREMENT_RECORD,
            uploads.UPLOAD_RETIREMENT_STARTED,
            "admission_cleanup",
        }
        or len(json.dumps(value).encode()) > 65_536
    ):
        raise UploadContractError(
            "Native upload admission returned an unqualified original receipt."
        )
    return copy.deepcopy(value)


def _journal(db, command):
    upload = db.get(
        DatasetUploadSession, command.resource_id, with_for_update=True, populate_existing=True
    )
    if upload is None or upload.project_id != command.project_id:
        raise UploadContractError("Original upload admission is missing.")
    value = copy.deepcopy(upload.provider_state.get(RECORD))
    if value is None:
        if (
            STARTED in upload.provider_state
            or command.status != CommandStatus.SUCCEEDED
            or command.response_status != 201
            or command.resource_type != OPERATION
        ):
            raise UploadContractError("Original upload admission history is missing.")
        try:
            result = ResumableUploadRead.model_validate(command.response_payload)
        except ValueError as exc:
            raise UploadContractError(
                "Original admission has no qualified legacy response."
            ) from exc
        if result.id != upload.id or result.project_id != upload.project_id:
            raise UploadContractError("Legacy upload response belongs to another resource.")
        return upload, None, result
    try:
        intent = value["intent"]
        if (
            intent["protocol"] != PROTOCOL
            or command.operation != OPERATION
            or command.request_hash != digest(command.request_payload)
            or intent["command_id"] != str(command.id)
            or intent["source"]["id"] != str(upload.id)
            or intent["source"]["project_id"] != str(command.project_id)
            or intent["source"]["created_by_id"] != str(command.actor_id)
            or intent["request_digest"] != command.request_hash
            or command.resource_type != OPERATION
            or command.response_payload.get("admission_intent_digest") != digest(intent)
            or upload.provider_state.get(STARTED) is not True
            or value["phase"] not in {"issued", "unknown", "recorded", "published", "retired"}
            or command.response_payload.get("admission_attempt_digest") != digest(value["attempt"])
            or command.status
            != (
                CommandStatus.SUCCEEDED
                if value["phase"] == "published"
                else CommandStatus.FAILED
                if value["phase"] == "retired"
                else CommandStatus.RUNNING
            )
        ):
            raise UploadContractError("Original upload admission custody changed.")
        if value["phase"] != "retired" and (
            "retirement" in value or "admission_retirement_digest" in command.response_payload
        ):
            raise UploadContractError("Original admission retirement cannot be reversed.")
        if value["phase"] in {"issued", "unknown"} and (
            "receipt" in value
            or "admission_receipt_digest" in command.response_payload
            or "result" in command.response_payload
        ):
            raise UploadContractError("Known native admission cannot return to issuance.")
        if value["phase"] in {"recorded", "published"} or "receipt" in value:
            _receipt(value["receipt"], intent)
            if command.response_payload.get("admission_receipt_digest") != digest(value["receipt"]):
                raise UploadContractError("Original native admission receipt changed.")
        elif "admission_receipt_digest" in command.response_payload:
            raise UploadContractError("Original native admission receipt is missing.")
        if value["phase"] == "retired":
            retirement = value["retirement"]
            if (
                upload.status != "expired"
                or command.response_status is not None
                or "result" in command.response_payload
                or retirement["intent_digest"] != digest(intent)
                or retirement["attempt_digest"] != digest(value["attempt"])
                or retirement["unpublished"] is not True
                or retirement["native_absence_confirmed"] is not False
                or command.response_payload.get("admission_retirement_digest") != digest(retirement)
            ):
                raise UploadContractError("Original unpublished admission retirement changed.")
        if value["phase"] == "published":
            if command.response_status != 201 or value["result_digest"] != digest(
                command.response_payload["result"]
            ):
                raise UploadContractError("Original upload admission response changed.")
            result = ResumableUploadRead.model_validate(command.response_payload["result"])
            if result.id != upload.id or result.project_id != upload.project_id:
                raise UploadContractError(
                    "Original admission response belongs to another resource."
                )
            return upload, value, result
        return upload, value, None
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise UploadContractError("Original upload admission could not be qualified.") from exc


def _fresh(upload, record):
    if record["phase"] == "retired":
        raise UploadContractError("Original upload admission expired and cannot be reissued.")
    source, original = _source(upload), record["intent"]["source"]
    mutable = {"legal_hold", "retention_until"} if record["phase"] == "published" else set()
    if source.keys() != original.keys() or any(
        source[k] != original[k] for k in source if k not in mutable
    ):
        raise UploadContractError("Original upload admission source changed.")
    if record["phase"] == "published" and (
        upload.multipart_upload_id != record["receipt"]["provider_id"]
        or any(upload.provider_state.get(k) != v for k, v in record["receipt"]["state"].items())
        or upload.provider_state.get("completion_protocol") != uploads.UPLOAD_COMPLETION_PROTOCOL
    ):
        raise UploadContractError("Published upload differs from its original admission receipt.")
    if record["phase"] != "published" and (
        upload.status != "initiated" or upload.expires_at <= datetime.now(UTC)
    ):
        raise UploadContractError("Original upload admission is no longer publishable.")


def _save(upload, record):
    upload.provider_state = {**upload.provider_state, RECORD: copy.deepcopy(record), STARTED: True}


def require_published(db, upload):
    command = db.scalar(
        select(WorkflowCommand).where(
            WorkflowCommand.project_id == upload.project_id,
            WorkflowCommand.resource_id == upload.id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.response_payload["admission_intent_digest"].as_string().is_not(None),
        )
    )
    if (
        command is None
        and RECORD not in upload.provider_state
        and STARTED not in upload.provider_state
    ):
        return
    if command is None:
        raise UploadContractError("Original upload admission command is missing.")
    _, record, _ = _journal(db, command)
    if record is None or record["phase"] != "published":
        raise UploadContractError("Original upload admission has not published.")
    _fresh(upload, record)


def _can_issue(record):
    if record["phase"] in {"issued", "unknown"} and datetime.fromisoformat(
        record["attempt"]["expires_at"]
    ) > datetime.now(UTC):
        raise HTTPException(409, "Original native upload admission is still pending.")
    if record["attempt"]["number"] >= 5:
        raise UploadContractError("Original upload admission exhausted its native retry budget.")


def begin_upload_session(
    factory, actor_id, token_version, project_id, payload, key, *, origin, settings=None
):
    payload = payload.model_copy(deep=True)
    settings = settings or get_settings()
    request = {**payload.model_dump(mode="json"), "origin": origin}
    with factory() as db, db.begin():
        _authorize(db, project_id, actor_id, token_version)
        command = _lookup(db, project_id, actor_id, key, request)
        record = None
        if command is not None:
            upload, record, result = _journal(db, command)
            if record:
                _fresh(upload, record)
            if result is not None:
                return result
            if record["phase"] != "recorded":
                _can_issue(record)
    store = None if record and record["phase"] == "recorded" else get_object_store(settings)
    capabilities = store.capabilities() if store else None
    with factory() as db, db.begin():
        actor = _authorize(db, project_id, actor_id, token_version)
        command = _lookup(db, project_id, actor_id, key, request)
        if command is None:
            command, _ = begin_command(
                db,
                project_id=project_id,
                actor_id=actor_id,
                operation=OPERATION,
                idempotency_key=key,
                payload=request,
            )
            session_id = uuid.uuid5(project_id, f"{OPERATION}:{actor_id}:{key}")
            # Fresh custody cannot adopt multipart state left by missing or legacy history.
            intent_id = str(uuid.uuid4())
            filename = uploads._safe_filename(payload.filename)
            object_key = f"projects/{project_id}/raw/{session_id}/{intent_id}/{filename}"
            upload = uploads._admit_upload_session(
                db,
                actor,
                project_id,
                payload,
                origin=origin,
                settings=settings,
                session_id=session_id,
                driver_name=store.driver_name,
                protocol=capabilities.protocol,
                object_uri=store.uri_for_key(object_key),
                object_key=object_key,
            )
            command.resource_id, command.resource_type = upload.id, OPERATION
            intent = dict(
                id=intent_id,
                protocol=PROTOCOL,
                command_id=str(command.id),
                request_digest=command.request_hash,
                store_digest=_store_digest(settings, store.driver_name),
                source=_source(upload),
                capabilities=asdict(capabilities),
            )
            record = dict(intent=intent)
            command.response_payload = dict(admission_intent_digest=digest(intent))
            transition_command(command, CommandStatus.RUNNING)
        else:
            upload, record, result = _journal(db, command)
            if record:
                _fresh(upload, record)
            if result is not None:
                return result
        session_id, command_id, intent_id = upload.id, command.id, record["intent"]["id"]
        if record.get("phase") != "recorded":
            if record.get("phase"):
                _can_issue(record)
            if (
                store is None
                or record["intent"]["store_digest"] != _store_digest(settings, store.driver_name)
                or store.driver_name != upload.provider_driver
                or store.uri_for_key(upload.object_key) != record["intent"]["source"]["uri"]
            ):
                raise UploadContractError("Native store differs from original upload admission.")
            attempt = dict(
                id=str(uuid.uuid4()),
                number=record.get("attempt", {}).get("number", 0) + 1,
                actor_token_version=actor.token_version,
                expires_at=min(
                    datetime.now(UTC) + timedelta(minutes=5), upload.expires_at
                ).isoformat(),
            )
            upload.lease_expires_at = datetime.fromisoformat(attempt["expires_at"])
            command.retry_count, command.max_retries = attempt["number"], 5
            record.update(phase="issued", attempt=attempt)
            command.response_payload = {
                **command.response_payload,
                "admission_attempt_digest": digest(attempt),
            }

            _save(upload, record)
    if record["phase"] != "recorded":
        # Unpublished native capabilities never reach clients; only the current attempt may publish.
        source = record["intent"]["source"]
        native_request = BeginUpload(
            object_key=source["object_key"],
            byte_size=source["byte_size"],
            content_type=request["content_type"],
            checksum_sha256=source["expected_object_digest"],
            transfer_unit_size=source["part_size"],
            expires_at=datetime.fromisoformat(source["expires_at"]),
            origin=origin,
        )
        try:
            value = _receipt(
                jsonable_encoder(asdict(store.begin_upload(native_request))), record["intent"]
            )
        except Exception as exc:
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                current_upload, current, _ = _journal(
                    db, db.get(WorkflowCommand, command_id, populate_existing=True)
                )
                if current["attempt"] == attempt and current["phase"] == "issued":
                    current.update(phase="unknown", error=redact_text(str(exc))[:1000])
                    _save(current_upload, current)
            raise
        with factory() as db, db.begin():
            db.get(Project, project_id, with_for_update=True)
            command = db.get(WorkflowCommand, command_id, populate_existing=True)
            current_upload, current, _ = _journal(db, command)
            stale = (
                current["intent"]["id"] != intent_id
                or current["attempt"] != attempt
                or current["phase"] != "issued"
            )
            if stale:
                unused = dict(command.response_payload.get("superseded_admission_receipts", {}))
                unused[attempt["id"]] = value
                command.response_payload = {
                    **command.response_payload,
                    "superseded_admission_receipts": unused,
                }
            else:
                current.update(phase="recorded", receipt=value)
                current_upload.lease_expires_at = None
                command.response_payload = {
                    **command.response_payload,
                    "admission_receipt_digest": digest(value),
                }
                _save(current_upload, current)
        if stale:
            raise UploadContractError("Original upload admission lost its native attempt.")
    with factory() as db, db.begin():
        _authorize(db, project_id, actor_id, token_version)
        command = _lookup(db, project_id, actor_id, key, request)
        upload, current, result = _journal(db, command)
        _fresh(upload, current)
        if result is not None:
            return result
        if current["intent"]["id"] != intent_id or current["phase"] != "recorded":
            raise UploadContractError("Original upload admission has no known native reply.")
        require_store(settings, upload.provider_state, upload.provider_driver)
        value = current["receipt"]
        upload.multipart_upload_id = value["provider_id"]
        upload.status = "uploading"
        upload.lease_owner, upload.lease_expires_at = None, None
        upload.provider_state = {
            **upload.provider_state,
            **value["state"],
            "completion_protocol": uploads.UPLOAD_COMPLETION_PROTOCOL,
        }
        current["phase"] = "published"
        result = uploads.upload_read(
            upload, capabilities=UploadCapabilities(**current["intent"]["capabilities"])
        )
        response = result.model_dump(mode="json")
        current["result_digest"] = digest(response)
        command.response_payload = {**command.response_payload, "result": response}
        command.response_status = 201
        transition_command(command, CommandStatus.SUCCEEDED)
        _save(upload, current)
        return result
