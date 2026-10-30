"""Private durable source admission; this module never constructs a storage SDK."""

import hashlib
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import CommandStatus
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import DatasetUploadRequest, DatasetUploadResponse
from automl_api.services import buffered_chunks
from automl_api.services.dataset_inspection import inspect_tabular_bytes
from automl_api.services.upload_admission import _authorize, _store_digest
from automl_api.services.uploads import require_storage_quota
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    begin_command,
    enqueue_outbox,
)
from automl_api.services.workflow_state import (
    canonical_request_hash as digest,
)
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.object_store import validate_object_store_settings

OPERATION = "dataset.upload.complete"
PROTOCOL = "dataset.buffered.upload.v1"
TOPIC = PROTOCOL


def _request(payload, content):
    return {
        **payload.model_dump(mode="json"),
        "content_sha256": hashlib.sha256(content).hexdigest(),
        "byte_size": len(content),
    }


def _existing(db, actor_id, project_id, key, request):
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == project_id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.idempotency_key == key,
        )
        .execution_options(populate_existing=True)
    )
    if command is not None:
        if command.actor_id != actor_id or command.request_hash != digest(request):
            raise HTTPException(
                409,
                detail={
                    "code": "idempotency_key_reused",
                    "message": "This key belongs to another original request.",
                },
            )
        if command.request_hash != digest(command.request_payload):
            raise UploadContractError("Original buffered command source changed.")
    return command


def validate_input_source(row, command):
    if row is None:
        raise UploadContractError("Original buffered input is missing; no source was inferred.")
    source = row.manifest
    if (
        row.project_id != command.project_id
        or row.manifest_digest != digest(source)
        or source.get("protocol") != PROTOCOL
        or source.get("command_id") != str(command.id)
        or source.get("project_id") != str(command.project_id)
        or source.get("actor_id") != str(command.actor_id)
        or source.get("request_hash") != command.request_hash
        or source.get("idempotency_key") != command.idempotency_key
        or source.get("request") != command.request_payload
        or command.request_hash != digest(command.request_payload)
        or command.operation != OPERATION
        or command.resource_type != PROTOCOL
        or command.resource_id != command.id
        or row.content_sha256 != command.request_payload.get("content_sha256")
        or row.byte_size != command.request_payload.get("byte_size")
        or source.get("object_key")
        != f"projects/{command.project_id}/buffered-uploads/{command.id}/input"
    ):
        raise UploadContractError("Original buffered input binding changed.")
    expected_parts = (
        {"count": row.chunk_count, "manifest_sha256": row.chunk_manifest_sha256}
        if row.chunk_count
        else None
    )
    if source.get("body_chunks") != expected_parts:
        raise UploadContractError("Original buffered body layout changed.")
    return row


def validate_input_delivery(row, command, entries):
    if len(entries) != 1:
        raise UploadContractError("Original buffered delivery metadata is missing or ambiguous.")
    entry = entries[0]
    if (
        entry is None
        or entry.project_id != command.project_id
        or entry.command_id != command.id
        or entry.topic != TOPIC
        or entry.aggregate_type != PROTOCOL
        or entry.aggregate_id != command.id
        or entry.payload != {"input_id": str(command.id), "manifest_digest": row.manifest_digest}
    ):
        raise UploadContractError("Original buffered delivery metadata is missing or changed.")


def original_input(db, command):
    """Verify custody before any future worker can use a private source."""
    row = validate_input_source(
        db.get(BufferedUploadInput, command.id, populate_existing=True), command
    )
    entries = list(
        db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == command.project_id,
                OutboxEntry.command_id == command.id,
                OutboxEntry.topic == TOPIC,
            )
        )
    )
    validate_input_delivery(row, command, entries)
    if row.chunk_count:
        buffered_chunks.validate_layout(db, row)
    return row


def _replay(db, command):
    if command.response_status == 201 and command.status == CommandStatus.SUCCEEDED:
        result = DatasetUploadResponse.model_validate(command.response_payload)
        if (
            result.dataset.project_id != command.project_id
            or result.version.project_id != command.project_id
            or result.version.dataset_id != result.dataset.id
            or result.version.created_by_id != command.actor_id
            or result.version.content_hash != command.request_payload.get("content_sha256")
            or result.version.byte_size != command.request_payload.get("byte_size")
            or result.dataset.name != command.request_payload.get("dataset_name")
            or result.version.original_filename != command.request_payload.get("filename")
        ):
            raise UploadContractError("Cached buffered response differs from its original source.")
        if command.resource_type == PROTOCOL:
            row = original_input(db, command)
            if row.phase not in {"published", "retired"} or not row.native_receipt:
                raise UploadContractError("Cached buffered response has no original publication.")
        elif command.resource_type != OPERATION or command.resource_id is not None:
            raise UploadContractError(
                "Cached legacy buffered response has no qualified command type."
            )
        return command.id
    original_input(db, command)
    return command.id


def store_digest(settings, driver):
    return digest(
        {
            "backend": _store_digest(settings, driver),
            "local_root": str(settings.local_object_store_path.resolve())
            if driver == "embedded"
            else None,
        }
    )


def admit_input(
    factory, actor_id, token_version, project_id, payload, content, key, *, settings=None
):
    settings = settings or get_settings()
    payload = DatasetUploadRequest.model_validate(payload).model_copy(deep=True)
    if not isinstance(key, str) or not key.strip() or len(key) > 255:
        raise ValueError("Idempotency-Key must contain 1 to 255 characters.")
    if not isinstance(content, bytes) or not content:
        raise ValueError("Uploaded file must contain bytes.")
    if settings.environment.lower() in {"staging", "production"}:
        if (
            type(settings.buffered_upload_max_bytes) is not int
            or settings.buffered_upload_max_bytes < 1
        ):
            raise ValueError("A positive buffered upload bound is required.")
        if len(content) > settings.buffered_upload_max_bytes:
            raise ValueError(
                "Buffered upload exceeds its configured bound; use the resumable upload protocol."
            )
    request = _request(payload, content)
    with factory() as db, db.begin():
        _authorize(db, project_id, actor_id, token_version)
        command = _existing(db, actor_id, project_id, key, request)
        if command is not None:
            return _replay(db, command)
        require_storage_quota(db, project_id, len(content), settings)
    driver = validate_object_store_settings(settings)
    parts = buffered_chunks.describe(content)
    # Inspection can be expensive and must not retain a database checkout.
    inspection = jsonable_encoder(asdict(inspect_tabular_bytes(payload.filename, content)))
    backend_digest = store_digest(settings, driver)
    with factory() as db, db.begin():
        _authorize(db, project_id, actor_id, token_version)
        command = _existing(db, actor_id, project_id, key, request)
        if command is not None:
            return _replay(db, command)
        require_storage_quota(db, project_id, len(content), settings)
        try:
            command, replayed = begin_command(
                db,
                project_id=project_id,
                actor_id=actor_id,
                operation=OPERATION,
                idempotency_key=key,
                payload=request,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                409, detail={"code": "idempotency_key_reused", "message": str(exc)}
            ) from exc
        if replayed:
            return _replay(db, command)
        command.resource_type, command.resource_id = PROTOCOL, command.id
        admitted_at = datetime.now(UTC)
        ttl = settings.upload_session_ttl_seconds
        if type(ttl) is not int or ttl < 1:
            raise ValueError("Buffered admission requires a positive expiry interval.")
        try:
            expires_at = admitted_at + timedelta(seconds=ttl)
        except OverflowError as exc:
            raise ValueError("Buffered expiry interval exceeds its supported range.") from exc
        source = dict(
            protocol=PROTOCOL,
            intent_id=str(uuid.uuid4()),
            admitted_at=admitted_at.isoformat(),
            expires_at=expires_at.isoformat(),
            command_id=str(command.id),
            project_id=str(project_id),
            actor_id=str(actor_id),
            token_version=token_version,
            request_hash=command.request_hash,
            idempotency_key=key,
            request=request,
            driver=driver,
            store_digest=backend_digest,
            inspection=inspection,
            object_key=f"projects/{project_id}/buffered-uploads/{command.id}/input",
        )
        part_manifest = buffered_chunks.manifest_hash(parts) if parts else None
        if parts:
            source["body_chunks"] = {"count": len(parts), "manifest_sha256": part_manifest}
            buffered_chunks.store_parts(db, command.id, project_id, content, parts)
        row = BufferedUploadInput(
            command_id=command.id,
            project_id=project_id,
            content=None if parts else content,
            byte_size=len(content),
            content_sha256=request["content_sha256"],
            manifest=source,
            manifest_digest=digest(source),
            chunk_count=len(parts),
            chunk_manifest_sha256=part_manifest,
        )
        db.add(row)
        enqueue_outbox(
            db,
            command,
            topic=TOPIC,
            aggregate_type=PROTOCOL,
            aggregate_id=command.id,
            payload={"input_id": str(command.id), "manifest_digest": row.manifest_digest},
        )
        return command.id
