from __future__ import annotations

import math
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from automl_api.core.config import Settings, get_settings
from automl_api.models.datasets import Dataset, DatasetUploadSession, DatasetVersion
from automl_api.models.enums import DatasetFormat, DatasetStatus, ObjectStoreType, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.schemas.datasets import (
    AbortUploadRead,
    DatasetRead,
    DatasetVersionRead,
    ResumableUploadBeginRequest,
    ResumableUploadRead,
    TransferCursorRead,
    TransferInstructionRead,
    TransferReceiptRead,
    UploadCapabilitiesRead,
    UploadCompletionRead,
    UploadProgressRead,
)
from automl_api.services.projects import require_project_role
from automl_api.services.upload_policy import validate_upload_data_region, validate_upload_manifest
from automl_api.storage.contracts import (
    BeginUpload,
    CompletedObject,
    ProviderUpload,
    TransferCursor,
    TransferReceipt,
    UploadContractError,
)
from automl_api.storage.object_store import get_object_store

ACTIVE_UPLOAD_STATES = {"initiated", "uploading", "object_completed", "verifying"}
TERMINAL_UPLOAD_STATES = {"ready", "aborted", "expired", "quarantined", "failed"}
SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


def begin_upload_session(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ResumableUploadBeginRequest,
    *,
    origin: str,
    settings: Settings | None = None,
    session_id: uuid.UUID | None = None,
) -> ResumableUploadRead:
    settings = settings or get_settings()
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    require_allowed_origin(origin, settings)
    validate_upload_manifest(
        filename=payload.filename,
        content_type=payload.content_type,
        byte_size=payload.byte_size,
        settings=settings,
    )
    validate_upload_data_region(payload.data_region, settings)
    _lock_project(db, project_id)
    used_bytes = int(
        db.scalar(
            select(func.coalesce(func.sum(DatasetVersion.byte_size), 0)).where(
                DatasetVersion.project_id == project_id
            )
        )
        or 0
    )
    reserved_bytes = int(
        db.scalar(
            select(func.coalesce(func.sum(DatasetUploadSession.byte_size), 0)).where(
                DatasetUploadSession.project_id == project_id,
                DatasetUploadSession.status.in_(ACTIVE_UPLOAD_STATES),
            )
        )
        or 0
    )
    if used_bytes + reserved_bytes + payload.byte_size > settings.project_storage_quota_bytes:
        raise ValueError("The upload would exceed the project's storage quota.")

    driver = get_object_store(settings)
    capabilities = driver.capabilities()
    if capabilities.protocol == "single_put":
        raise ValueError("The selected embedded driver cannot issue browser upload instructions.")
    session_id = session_id or uuid.uuid4()
    filename = _safe_filename(payload.filename)
    object_key = f"projects/{project_id}/raw/{session_id}/{filename}"
    expires_at = datetime.now(UTC) + timedelta(seconds=settings.upload_session_ttl_seconds)
    part_size = settings.upload_part_size_bytes
    provider_upload = driver.begin_upload(
        BeginUpload(
            object_key=object_key,
            byte_size=payload.byte_size,
            content_type=payload.content_type,
            checksum_sha256=payload.sha256,
            transfer_unit_size=part_size,
            expires_at=expires_at,
            origin=origin,
        )
    )
    retention_until = (
        datetime.now(UTC) + timedelta(days=payload.retention_days)
        if payload.retention_days
        else None
    )
    upload = DatasetUploadSession(
        id=session_id,
        project_id=project_id,
        created_by_id=user.id,
        dataset_name=payload.dataset_name,
        upload_kind=payload.upload_kind,
        description=payload.description,
        tags=payload.tags,
        original_filename=payload.filename,
        byte_size=payload.byte_size,
        part_size=part_size,
        total_parts=math.ceil(payload.byte_size / part_size),
        object_key=object_key,
        multipart_upload_id=provider_upload.provider_id,
        provider_driver=driver.driver_name,
        protocol=capabilities.protocol,
        provider_state=dict(provider_upload.state),
        transfer_receipts=[],
        instruction_state={},
        confirmed_bytes=0,
        resume_key=secrets.token_urlsafe(48),
        status="uploading",
        expires_at=expires_at,
        digest_algorithm="sha256",
        digest_scope="byte_stream",
        expected_object_digest=payload.sha256,
        content_type=payload.content_type.partition(";")[0].strip().lower(),
        sensitivity=payload.sensitivity,
        data_region=payload.data_region,
        retention_until=retention_until,
        legal_hold=payload.legal_hold,
        content_policy_revision="phase2-content-policy-v1",
        target_metadata=payload.target_metadata,
        scanner_status="pending",
        retry_count=0,
        retry_budget=5,
        last_progress_at=datetime.now(UTC),
    )
    db.add(upload)
    db.flush()
    return upload_read(upload, driver=driver)


def get_upload_session(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    *,
    for_update: bool = False,
) -> DatasetUploadSession:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    statement = select(DatasetUploadSession).where(
        DatasetUploadSession.project_id == project_id,
        DatasetUploadSession.id == session_id,
    )
    if for_update:
        statement = statement.with_for_update()
    upload = db.scalar(statement)
    if upload is None:
        raise HTTPException(status_code=404, detail="Upload session not found.")
    return upload


def issue_transfer_instruction(
    upload: DatasetUploadSession,
    cursor: TransferCursorRead,
    *,
    origin: str,
    settings: Settings | None = None,
) -> TransferInstructionRead:
    settings = settings or get_settings()
    require_allowed_origin(origin, settings)
    _require_active(upload)
    expected_offset = (cursor.unit_number - 1) * upload.part_size
    expected_length = min(upload.part_size, upload.byte_size - expected_offset)
    if expected_offset < 0 or cursor.offset != expected_offset or cursor.length != expected_length:
        raise UploadContractError("Transfer cursor does not match the session transfer unit.")
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name)
    instruction = driver.create_transfer_instruction(
        provider_upload(upload),
        TransferCursor(
            unit_number=cursor.unit_number,
            offset=cursor.offset,
            length=cursor.length,
            checksum_sha256=(cursor.checksum_sha256 or "").lower() or None,
        ),
    )
    upload.instruction_state = {
        **upload.instruction_state,
        str(cursor.unit_number): {
            "instruction_id": instruction.instruction_id,
            "offset": cursor.offset,
            "length": cursor.length,
            "expires_at": instruction.expires_at.isoformat(),
        },
    }
    upload.last_progress_at = datetime.now(UTC)
    return TransferInstructionRead(
        instruction_id=instruction.instruction_id,
        method=instruction.method,
        url=instruction.url,
        headers=instruction.headers,
        cursor=TransferCursorRead.model_validate(instruction.cursor, from_attributes=True),
        expires_at=instruction.expires_at,
        expose_response_headers=list(instruction.expose_response_headers),
        enforced_controls=list(instruction.enforced_controls),
        unsupported_controls=list(instruction.unsupported_controls),
    )


def query_upload_progress(
    upload: DatasetUploadSession, *, settings: Settings | None = None
) -> UploadProgressRead:
    settings = settings or get_settings()
    if upload.status in {"aborted", "expired", "quarantined", "failed"}:
        return progress_read(upload, complete=False)
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name)
    progress = driver.query_progress(provider_upload(upload))
    upload.confirmed_bytes = progress.confirmed_bytes
    upload.transfer_receipts = [receipt_to_dict(value) for value in progress.receipts]
    upload.last_progress_at = datetime.now(UTC)
    if progress.complete and upload.status == "uploading":
        upload.status = "object_completed"
    return progress_read(upload, complete=progress.complete)


def complete_upload_session(
    db: Session,
    upload: DatasetUploadSession,
    *,
    sha256: str,
    receipts: list[TransferReceiptRead],
    settings: Settings | None = None,
) -> UploadCompletionRead:
    settings = settings or get_settings()
    if upload.status in {"object_completed", "verifying", "ready"} and upload.completed_object_uri:
        return completion_read(db, upload)
    _require_active(upload)
    normalized_digest = sha256.lower()
    if normalized_digest != upload.expected_object_digest:
        raise UploadContractError("Final client SHA-256 differs from the upload manifest.")
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name)
    object_uri = driver.uri_for_key(upload.object_key)
    if driver.exists(object_uri):
        metadata = driver.stat(object_uri)
        if metadata.byte_size != upload.byte_size:
            raise UploadContractError("The recovered provider object has an unexpected size.")
        completed = CompletedObject(
            uri=metadata.uri,
            byte_size=metadata.byte_size,
            provider_checksum=metadata.checksum,
            provider_headers=metadata.provider_headers,
        )
        provider_receipts = [TransferReceipt(**value) for value in upload.transfer_receipts]
        if receipts and provider_receipts:
            _validate_client_receipts(receipts, provider_receipts)
    else:
        progress = driver.query_progress(provider_upload(upload))
        if not progress.complete:
            raise UploadContractError("The provider has not confirmed every object byte.")
        provider_receipts = list(progress.receipts)
        if receipts:
            _validate_client_receipts(receipts, provider_receipts)
        completed = driver.complete_upload(provider_upload(upload), provider_receipts)
    upload.confirmed_bytes = completed.byte_size
    upload.transfer_receipts = [receipt_to_dict(value) for value in provider_receipts]
    upload.completed_object_uri = completed.uri
    upload.provider_checksum = completed.provider_checksum
    upload.observed_object_digest = None
    upload.status = "object_completed"
    upload.completed_at = datetime.now(UTC)
    upload.last_progress_at = upload.completed_at
    if (
        upload.upload_kind in {"dataset", "validation", "drift"}
        and upload.dataset_version_id is None
    ):
        dataset, version = _register_dataset_version(db, upload)
        upload.dataset_id = dataset.id
        upload.dataset_version_id = version.id
    db.flush()
    return completion_read(db, upload)


def abort_upload_session(
    upload: DatasetUploadSession, *, settings: Settings | None = None
) -> AbortUploadRead:
    settings = settings or get_settings()
    if upload.status == "aborted":
        return AbortUploadRead(id=upload.id, status="aborted", aborted_at=upload.aborted_at)
    if upload.status == "ready":
        raise UploadContractError("A ready immutable upload cannot be aborted.")
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name)
    driver.abort_upload(provider_upload(upload))
    upload.status = "aborted"
    upload.aborted_at = datetime.now(UTC)
    upload.terminal_reason = "aborted by user"
    upload.lease_owner = None
    upload.lease_expires_at = None
    return AbortUploadRead(id=upload.id, status="aborted", aborted_at=upload.aborted_at)


def cleanup_abandoned_uploads(
    db: Session,
    *,
    settings: Settings | None = None,
    now: datetime | None = None,
    limit: int = 100,
) -> dict[str, int]:
    settings = settings or get_settings()
    observed_at = now or datetime.now(UTC)
    rows = list(
        db.scalars(
            select(DatasetUploadSession)
            .where(
                (
                    DatasetUploadSession.status.in_({"initiated", "uploading"})
                    & (DatasetUploadSession.expires_at <= observed_at)
                )
                | (
                    DatasetUploadSession.status.in_({"object_completed", "verifying"})
                    & (DatasetUploadSession.expires_at <= observed_at)
                    & (
                        DatasetUploadSession.lease_expires_at.is_(None)
                        | (DatasetUploadSession.lease_expires_at <= observed_at)
                    )
                    & (DatasetUploadSession.legal_hold.is_(False))
                )
                | (
                    (DatasetUploadSession.status == "quarantined")
                    & (DatasetUploadSession.quarantine_delete_after <= observed_at)
                    & (DatasetUploadSession.legal_hold.is_(False))
                )
            )
            .order_by(DatasetUploadSession.expires_at, DatasetUploadSession.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
    )
    result = {"expired": 0, "completed_deleted": 0, "quarantine_deleted": 0}
    driver = get_object_store(settings)
    for upload in rows:
        if upload.provider_driver != driver.driver_name:
            raise RuntimeError(
                "Cleanup driver does not match the persisted upload provider identity."
            )
        if upload.status in {"quarantined", "object_completed", "verifying"}:
            if upload.legal_hold:
                continue
            previous_status = upload.status
            uri = upload.completed_object_uri
            if uri and driver.exists(uri):
                driver.delete(uri)
            upload.status = "failed"
            if upload.dataset_version_id:
                version = db.get(DatasetVersion, upload.dataset_version_id)
                if version is not None:
                    version.status = DatasetStatus.FAILED
            if previous_status == "quarantined":
                upload.terminal_reason = "quarantined object deleted after retention period"
                result["quarantine_deleted"] += 1
            else:
                upload.terminal_reason = "stale completed object deleted after session expiry"
                result["completed_deleted"] += 1
        else:
            driver.abort_upload(provider_upload(upload))
            upload.status = "expired"
            upload.terminal_reason = "abandoned upload expired and was aborted"
            result["expired"] += 1
        upload.lease_owner = None
        upload.lease_expires_at = None
    db.flush()
    return result


def upload_read(upload: DatasetUploadSession, *, driver=None) -> ResumableUploadRead:
    driver = driver or get_object_store()
    return ResumableUploadRead(
        id=upload.id,
        project_id=upload.project_id,
        upload_kind=upload.upload_kind,
        status=upload.status,
        provider_driver=upload.provider_driver,
        protocol=upload.protocol,
        byte_size=upload.byte_size,
        part_size=upload.part_size,
        total_parts=upload.total_parts,
        confirmed_bytes=upload.confirmed_bytes,
        resume_key=upload.resume_key,
        expires_at=upload.expires_at,
        original_filename=upload.original_filename,
        expected_object_digest=upload.expected_object_digest,
        sensitivity=upload.sensitivity,
        data_region=upload.data_region,
        legal_hold=upload.legal_hold,
        capabilities=UploadCapabilitiesRead.model_validate(
            driver.capabilities(), from_attributes=True
        ),
        next_cursor=next_cursor(upload),
    )


def next_cursor(upload: DatasetUploadSession) -> TransferCursorRead | None:
    if upload.confirmed_bytes >= upload.byte_size:
        return None
    if upload.protocol == "resumable_offset":
        unit_number = (upload.confirmed_bytes // upload.part_size) + 1
        return TransferCursorRead(
            unit_number=unit_number,
            offset=upload.confirmed_bytes,
            length=min(upload.part_size, upload.byte_size - upload.confirmed_bytes),
        )
    confirmed_units = {int(value["unit_number"]) for value in upload.transfer_receipts}
    unit_number = next(
        (value for value in range(1, upload.total_parts + 1) if value not in confirmed_units),
        upload.total_parts + 1,
    )
    if unit_number > upload.total_parts:
        return None
    offset = (unit_number - 1) * upload.part_size
    return TransferCursorRead(
        unit_number=unit_number,
        offset=offset,
        length=min(upload.part_size, upload.byte_size - offset),
    )


def progress_read(upload: DatasetUploadSession, *, complete: bool) -> UploadProgressRead:
    return UploadProgressRead(
        session_id=upload.id,
        status=upload.status,
        confirmed_bytes=upload.confirmed_bytes,
        total_bytes=upload.byte_size,
        receipts=[TransferReceiptRead.model_validate(value) for value in upload.transfer_receipts],
        next_cursor=next_cursor(upload),
        expires_at=upload.expires_at,
        complete=complete,
    )


def provider_upload(upload: DatasetUploadSession) -> ProviderUpload:
    return ProviderUpload(
        provider_id=upload.multipart_upload_id,
        object_key=upload.object_key,
        protocol=upload.protocol,
        byte_size=upload.byte_size,
        expires_at=upload.expires_at,
        state=dict(upload.provider_state),
    )


def receipt_to_dict(receipt: TransferReceipt) -> dict[str, object]:
    return {
        "unit_number": receipt.unit_number,
        "offset": receipt.offset,
        "length": receipt.length,
        "etag": receipt.etag,
        "checksum_sha256": receipt.checksum_sha256,
        "provider_headers": receipt.provider_headers,
    }


def require_allowed_origin(origin: str, settings: Settings) -> None:
    if origin not in settings.upload_allowed_origins:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "upload_origin_denied", "message": "Upload origin is not allowed."},
        )


def _register_dataset_version(
    db: Session, upload: DatasetUploadSession
) -> tuple[Dataset, DatasetVersion]:
    _lock_project(db, upload.project_id)
    dataset = db.scalar(
        select(Dataset)
        .options(selectinload(Dataset.versions))
        .where(
            Dataset.project_id == upload.project_id,
            Dataset.name == upload.dataset_name,
        )
        .with_for_update()
    )
    if dataset is None:
        dataset = Dataset(
            project_id=upload.project_id,
            created_by_id=upload.created_by_id,
            name=upload.dataset_name,
            description=upload.description,
            tags=upload.tags,
        )
        db.add(dataset)
        db.flush()
    next_version = dataset.latest_version_number + 1
    version = DatasetVersion(
        project_id=upload.project_id,
        dataset_id=dataset.id,
        created_by_id=upload.created_by_id,
        version_number=next_version,
        status=DatasetStatus.UPLOADED,
        format=_dataset_format(upload.original_filename),
        object_store_type=_object_store_type(upload.provider_driver),
        object_uri=str(upload.completed_object_uri),
        original_filename=upload.original_filename,
        content_hash=str(upload.expected_object_digest),
        content_hash_algorithm="sha256",
        content_hash_scope="byte_stream",
        content_hash_verification_status="pending",
        byte_size=upload.byte_size,
        data_region=upload.data_region,
        sensitivity=upload.sensitivity,
        retention_until=upload.retention_until,
        legal_hold=upload.legal_hold,
    )
    db.add(version)
    dataset.latest_version_number = next_version
    db.flush()
    return dataset, version


def completion_read(db: Session, upload: DatasetUploadSession) -> UploadCompletionRead:
    dataset = db.get(Dataset, upload.dataset_id) if upload.dataset_id else None
    version = (
        db.get(DatasetVersion, upload.dataset_version_id) if upload.dataset_version_id else None
    )
    return UploadCompletionRead(
        session=upload_read(upload),
        dataset=DatasetRead.model_validate(dataset) if dataset else None,
        version=DatasetVersionRead.model_validate(version) if version else None,
    )


def _validate_client_receipts(
    client_receipts: list[TransferReceiptRead], provider_receipts: list[TransferReceipt]
) -> None:
    provider = {value.unit_number: value for value in provider_receipts}
    for receipt in client_receipts:
        observed = provider.get(receipt.unit_number)
        if observed is None:
            raise UploadContractError("Client supplied a receipt absent from provider state.")
        if receipt.length != observed.length:
            raise UploadContractError("Client receipt length differs from provider state.")
        if receipt.etag and observed.etag and receipt.etag.strip('"') != observed.etag.strip('"'):
            raise UploadContractError("Client ETag differs from the provider receipt.")


def _require_active(upload: DatasetUploadSession) -> None:
    if upload.status in TERMINAL_UPLOAD_STATES:
        raise UploadContractError(f"Upload session is terminal with status '{upload.status}'.")
    if upload.expires_at <= datetime.now(UTC):
        raise UploadContractError("Upload session has expired.")


def _require_session_driver(upload: DatasetUploadSession, driver_name: str) -> None:
    if upload.provider_driver != driver_name:
        raise RuntimeError(
            f"Persisted upload driver '{upload.provider_driver}' does not match "
            f"configured driver '{driver_name}'."
        )


def _safe_filename(value: str) -> str:
    safe = SAFE_FILENAME.sub("-", value.strip()).strip(".-")
    if not safe:
        raise ValueError("Filename has no safe object-key representation.")
    return safe[:240]


def _dataset_format(filename: str) -> DatasetFormat:
    suffix = filename.rsplit(".", 1)[-1].lower()
    if suffix == "csv":
        return DatasetFormat.CSV
    if suffix == "parquet":
        return DatasetFormat.PARQUET
    if suffix in {"xls", "xlsx"}:
        return DatasetFormat.EXCEL
    return DatasetFormat.JSON


def _object_store_type(driver: str) -> ObjectStoreType:
    return ObjectStoreType(driver)


def _lock_project(db: Session, project_id: uuid.UUID) -> None:
    locked_project_id = db.scalar(
        select(Project.id).where(Project.id == project_id).with_for_update()
    )
    if locked_project_id is None:
        raise HTTPException(status_code=404, detail="Project not found.")
