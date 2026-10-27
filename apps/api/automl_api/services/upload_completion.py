"""Original upload completion custody; provider I/O never holds SQL."""

import copy
import uuid
from datetime import datetime

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus, ProjectRole, ProjectStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import UploadCompletionRead
from automl_api.services import uploads
from automl_api.services.projects import require_project_role
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    begin_command,
    canonical_request_hash,
    enqueue_outbox,
    transition_command,
)
from automl_api.storage.contracts import (
    CompletedObject,
    ProviderUpload,
    TransferReceipt,
    UploadContractError,
    immutable_object_version,
)
from automl_api.storage.object_store import get_object_store

PROTOCOL = uploads.UPLOAD_COMPLETION_PROTOCOL
TOPIC = "upload.reconcile.v2"
RECORD = uploads.UPLOAD_COMPLETION_RECORD
SOURCE_FIELDS = (
    "id",
    "project_id",
    "created_by_id",
    "object_key",
    "multipart_upload_id",
    "provider_driver",
    "protocol",
    "byte_size",
    "part_size",
    "total_parts",
    "expected_object_digest",
    "digest_algorithm",
    "digest_scope",
    "upload_kind",
    "original_filename",
    "content_type",
    "dataset_name",
    "description",
    "tags",
    "target_metadata",
    "sensitivity",
    "data_region",
    "retention_until",
    "legal_hold",
    "content_policy_revision",
    "expires_at",
)


def _source(upload):
    value = {
        name: getattr(upload, name)
        for name in SOURCE_FIELDS
        if name not in {"legal_hold", "retention_until"}
    }
    value["provider_state"] = {
        k: v
        for k, v in upload.provider_state.items()
        if k
        not in {
            RECORD,
            uploads.UPLOAD_COMPLETION_STARTED,
            uploads.UPLOAD_VERIFICATION_RECORD,
            uploads.UPLOAD_RETIREMENT_RECORD,
            uploads.UPLOAD_RETIREMENT_STARTED,
        }
    }
    return jsonable_encoder(value)


def same_source(left, right):
    # Policy can change; original signed history is compared without rewriting it.
    return {k: v for k, v in left.items() if k not in {"legal_hold", "retention_until"}} == {
        k: v for k, v in right.items() if k not in {"legal_hold", "retention_until"}
    }


def _select(db, actor_id, token_version, project_id, session_id):
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if project is None:
        raise HTTPException(404, "Project not found.")
    if project.status != ProjectStatus.ACTIVE or project.settings.get("deletion_in_progress"):
        raise HTTPException(409, "Upload completion is unavailable during project deletion.")
    upload = db.get(DatasetUploadSession, session_id, with_for_update=True, populate_existing=True)
    actor = db.get(User, actor_id, with_for_update={"read": True}, populate_existing=True)
    if actor is None or not actor.is_active or actor.token_version != token_version:
        raise HTTPException(401, "Current authentication is required.")
    require_project_role(db, actor, project_id, ProjectRole.EDITOR)
    if upload is None or upload.project_id != project_id:
        raise HTTPException(404, "Upload session not found.")
    uploads.require_upload_byte_digest(upload)
    if upload.provider_state.get("completion_protocol") != PROTOCOL:
        raise UploadContractError("Upload requires original versioned completion admission.")
    from automl_api.services.upload_admission import require_published

    require_published(db, upload)
    original = upload.provider_state.get(RECORD)
    published = isinstance(original, dict) and original.get("phase") == "published"
    if not published and (
        upload.status == "retiring"
        or uploads.UPLOAD_RETIREMENT_RECORD in upload.provider_state
        or uploads.UPLOAD_RETIREMENT_STARTED in upload.provider_state
        or db.scalar(
            select(WorkflowCommand.id)
            .where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.operation == uploads.UPLOAD_RETIREMENT_PROTOCOL,
                WorkflowCommand.resource_id == session_id,
                WorkflowCommand.status != CommandStatus.PENDING,
            )
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError("Original upload retirement prevents new native completion.")
    return upload


def _journal(db, upload):
    if upload is None:
        raise UploadContractError("Original upload completion no longer exists.")
    record = copy.deepcopy(upload.provider_state.get(RECORD))
    if record is None:
        return None, None
    intent = record["intent"]
    source = intent["source"]
    command = db.get(WorkflowCommand, uuid.UUID(intent["command_id"]), populate_existing=True)
    if (
        upload.provider_state.get(uploads.UPLOAD_COMPLETION_STARTED) is not True
        or intent["protocol"] != PROTOCOL
        or source["id"] != str(upload.id)
        or source["project_id"] != str(upload.project_id)
        or intent["source_digest"] != canonical_request_hash(source)
        or command is None
        or command.project_id != upload.project_id
        or command.operation != PROTOCOL
        or command.resource_type != PROTOCOL
        or command.resource_id != upload.id
        or command.request_hash != canonical_request_hash(command.request_payload)
        or command.request_payload.get("source_digest") != intent["source_digest"]
        or command.request_payload.get("session_id") != str(upload.id)
        or command.request_payload.get("actor_id") != str(command.actor_id)
        or command.request_payload.get("sha256") != source["expected_object_digest"]
        or record["phase"] not in {"admitted", "issued", "recorded", "published"}
    ):
        raise UploadContractError("Original upload completion custody changed.")
    uuid.UUID(intent["id"])
    if record["phase"] == "admitted":
        if command.response_payload or command.response_status is not None:
            raise UploadContractError("Native completion start cannot be rolled back to admission.")
    elif command.response_payload.get("native_completion_intent_digest") != canonical_request_hash(
        intent
    ) or command.response_payload.get("native_completion_parts_digest") != canonical_request_hash(
        record["parts"]
    ):
        raise UploadContractError("Original native completion start changed.")
    if receipt := record.get("receipt"):
        version = immutable_object_version(receipt["object_version"])
        if (
            len(version) > 256
            or record["phase"] not in {"recorded", "published"}
            or receipt["intent_digest"] != canonical_request_hash(intent)
            or receipt["uri"] != intent["uri"]
            or type(receipt["byte_size"]) is not int
            or receipt["byte_size"] != source["byte_size"]
            or receipt["parts"] != record["parts"]
            or command.response_payload.get("native_completion_receipt_digest")
            != canonical_request_hash(receipt)
        ):
            raise UploadContractError("Original upload completion receipt changed.")
    elif record["phase"] in {"recorded", "published"}:
        raise UploadContractError("Original upload completion receipt is missing.")
    if record["phase"] == "published":
        result = UploadCompletionRead.model_validate(command.response_payload)
        if (
            command.response_status != 200
            or record["response_digest"] != canonical_request_hash(command.response_payload)
            or result.session.id != upload.id
            or (result.dataset.id if result.dataset else None) != upload.dataset_id
            or (result.version.id if result.version else None) != upload.dataset_version_id
        ):
            raise UploadContractError("Original upload completion publication changed.")
    return record, command


def journal(db, upload):
    try:
        return _journal(db, upload)
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise UploadContractError(
            "Original upload completion custody could not be qualified."
        ) from exc


def _save(upload, record):
    upload.provider_state = {
        **upload.provider_state,
        RECORD: copy.deepcopy(record),
        uploads.UPLOAD_COMPLETION_STARTED: True,
    }


def _command(db, actor_id, project_id, session_id, payload, key, source):
    try:
        command, replay = begin_command(
            db,
            actor_id=actor_id,
            project_id=project_id,
            operation=PROTOCOL,
            idempotency_key=key,
            payload={
                "session_id": str(session_id),
                "actor_id": str(actor_id),
                **payload.model_dump(mode="json"),
                "source_digest": canonical_request_hash(source),
            },
        )
    except IdempotencyConflict as exc:
        raise HTTPException(409, "This Idempotency-Key has another original request.") from exc
    if replay:
        if command.resource_type != PROTOCOL or command.resource_id != session_id:
            raise UploadContractError("Original upload command binding changed.")
    else:
        command.resource_type, command.resource_id = PROTOCOL, session_id
    return command


def _native_upload(source):
    return ProviderUpload(
        source["multipart_upload_id"],
        source["object_key"],
        source["protocol"],
        source["byte_size"],
        datetime.fromisoformat(source["expires_at"]),
        copy.deepcopy(source["provider_state"]),
    )


def _receipt(completed, intent, parts):
    if not isinstance(completed, CompletedObject):
        raise UploadContractError("Native completion has no original receipt.")
    key = "generation" if intent["source"]["provider_driver"] == "gcs" else "version_id"
    version = immutable_object_version(completed.provider_headers.get(key))
    if key == "generation" and (
        not version.isascii()
        or not version.isdecimal()
        or len(version) > 20
        or not 0 < int(version) < 2**64
    ):
        raise UploadContractError("Native completion has no immutable GCS generation.")
    if (
        len(version) > 256
        or completed.uri != intent["uri"]
        or type(completed.byte_size) is not int
        or completed.byte_size != intent["source"]["byte_size"]
        or completed.provider_checksum is not None
        and (
            not isinstance(completed.provider_checksum, str)
            or len(completed.provider_checksum) > 512
        )
    ):
        raise UploadContractError("Native completion differs from its original intent.")
    return dict(
        uri=completed.uri,
        byte_size=completed.byte_size,
        object_version=version,
        provider_checksum=completed.provider_checksum,
        parts=parts,
        intent_digest=canonical_request_hash(intent),
    )


def complete_upload_session(
    factory, actor_id, token_version, project_id, session_id, payload, key, *, settings=None
):
    from automl_api.services.upload_admission import require_store

    settings = settings or get_settings()
    payload = payload.model_copy(deep=True)
    with factory() as db, db.begin():
        upload = _select(db, actor_id, token_version, project_id, session_id)
        source = _source(upload)
        require_store(settings, source["provider_state"], source["provider_driver"])
        if payload.sha256.lower() != uploads.require_upload_byte_digest(upload):
            raise UploadContractError("Final client SHA-256 differs from the upload manifest.")
        record, owner = journal(db, upload)
        if record is None and (
            uploads.UPLOAD_COMPLETION_STARTED in upload.provider_state
            or upload.completed_object_uri is not None
            or db.scalar(
                select(WorkflowCommand.id)
                .where(
                    WorkflowCommand.project_id == project_id,
                    WorkflowCommand.operation == PROTOCOL,
                    WorkflowCommand.resource_id == session_id,
                    WorkflowCommand.status != CommandStatus.PENDING,
                )
                .limit(1)
            )
            is not None
        ):
            raise UploadContractError(
                "Original upload completion history is missing; no admission was inferred."
            )
        if record is not None:
            if not same_source(source, record["intent"]["source"]):
                raise UploadContractError("Upload completion source changed.")
            source = record["intent"]["source"]
        if record is None or record["phase"] != "published":
            uploads._require_active(upload)
        command = _command(db, actor_id, project_id, session_id, payload, key, source)
        command_id = command.id
        if record is not None and record["phase"] == "published":
            if command.id != owner.id:
                command.response_payload, command.response_status = owner.response_payload, 200
                if command.status == CommandStatus.PENDING:
                    transition_command(command, CommandStatus.RUNNING)
                    transition_command(command, CommandStatus.SUCCEEDED)
            return UploadCompletionRead.model_validate(owner.response_payload)
        if record is not None and command.id != owner.id:
            raise HTTPException(409, "The original completion request must resolve first.")
        if (
            record is not None
            and record["phase"] == "issued"
            and source["provider_driver"] != "gcs"
        ):
            raise HTTPException(
                409, "Original native completion outcome is unknown; no commit was reissued."
            )
    require_store(settings, source["provider_state"], source["provider_driver"])
    # SDK construction can refresh credentials; it also belongs outside SQL.
    store = get_object_store(settings)
    if store.driver_name != source["provider_driver"]:
        raise UploadContractError("Upload provider differs from its original admission.")
    uri = store.uri_for_key(source["object_key"])
    with factory() as db, db.begin():
        upload = _select(db, actor_id, token_version, project_id, session_id)
        if not same_source(_source(upload), source):
            raise UploadContractError("Upload completion source changed before native admission.")
        record, owner = journal(db, upload)
        if record is None:
            intent = dict(
                protocol=PROTOCOL,
                id=str(uuid.uuid4()),
                command_id=str(command_id),
                source=source,
                source_digest=canonical_request_hash(source),
                uri=uri,
            )
            record = dict(intent=intent, phase="admitted")
            owner = db.get(WorkflowCommand, command_id)
            transition_command(owner, CommandStatus.RUNNING)
            _save(upload, record)
        if record["intent"]["command_id"] != str(command_id) or record["intent"]["uri"] != uri:
            raise HTTPException(409, "Another original completion owns this upload.")
        if record["phase"] == "issued" and source["provider_driver"] != "gcs":
            raise HTTPException(
                409, "Original native completion outcome is unknown; no commit was reissued."
            )
        original_id = record["intent"]["id"]
    if record["phase"] == "admitted":
        progress = store.query_progress(_native_upload(source))
        if not progress.complete:
            raise UploadContractError("The provider has not confirmed every object byte.")
        parts = [uploads.receipt_to_dict(part) for part in progress.receipts]
        if payload.receipts:
            uploads._validate_client_receipts(payload.receipts, list(progress.receipts))
        with factory() as db, db.begin():
            upload = _select(db, actor_id, token_version, project_id, session_id)
            current, owner = journal(db, upload)
            if (
                not same_source(_source(upload), source)
                or current["intent"]["id"] != original_id
                or current["phase"] != "admitted"
            ):
                raise HTTPException(409, "Original native completion already started or changed.")
            current.update(phase="issued", parts=parts)
            owner.response_payload = dict(
                native_completion_intent_digest=canonical_request_hash(current["intent"]),
                native_completion_parts_digest=canonical_request_hash(parts),
            )
            _save(upload, current)
            record = current
    if record["phase"] == "issued":
        completed = store.complete_upload(
            _native_upload(source), [TransferReceipt(**part) for part in record["parts"]]
        )
        receipt = _receipt(completed, record["intent"], record["parts"])
        # Preserve a known native outcome even if caller authority was revoked in flight.
        with factory() as db, db.begin():
            db.get(Project, project_id, with_for_update=True)
            upload = db.get(
                DatasetUploadSession, session_id, with_for_update=True, populate_existing=True
            )
            current, original = journal(db, upload)
            if current["intent"]["id"] != original_id or current["phase"] not in {
                "issued",
                "recorded",
            }:
                raise UploadContractError("Original completion receipt lost its admission.")
            if current.get("receipt") is not None and current["receipt"] != receipt:
                raise UploadContractError("Original completion receipt cannot be rebound.")
            current.update(phase="recorded", receipt=receipt)
            original.response_payload = {
                **original.response_payload,
                "native_completion_receipt_digest": canonical_request_hash(receipt),
            }
            _save(upload, current)
    with factory() as db, db.begin():
        upload = _select(db, actor_id, token_version, project_id, session_id)
        current, owner = journal(db, upload)
        if current["intent"]["id"] != original_id or not same_source(_source(upload), source):
            raise UploadContractError("Upload completion source changed before publication.")
        if current["phase"] == "published":
            return UploadCompletionRead.model_validate(owner.response_payload)
        uploads._require_active(upload)
        receipt = current["receipt"]
        key_name = "generation" if source["provider_driver"] == "gcs" else "version_id"
        result = uploads.record_completed_upload(
            db,
            upload,
            CompletedObject(
                uri,
                receipt["byte_size"],
                receipt["provider_checksum"],
                {key_name: receipt["object_version"]},
            ),
            [TransferReceipt(**part) for part in current["parts"]],
            driver=store,
        )
        owner.response_payload = {
            **owner.response_payload,
            **result.model_dump(mode="json", by_alias=True),
            "native_completion_receipt_digest": canonical_request_hash(receipt),
        }
        owner.response_status = 200
        current.update(
            phase="published", response_digest=canonical_request_hash(owner.response_payload)
        )
        _save(upload, current)
        enqueue_outbox(
            db,
            owner,
            topic=TOPIC,
            aggregate_type="dataset_upload_session",
            aggregate_id=session_id,
            payload={"session_id": str(session_id), "intent_id": original_id},
        )
        return result
