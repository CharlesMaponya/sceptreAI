from __future__ import annotations

import math
import re
import secrets
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from automl_api.core.config import Settings, get_settings
from automl_api.models.datasets import Dataset, DatasetUploadSession, DatasetVersion
from automl_api.models.enums import (
    CommandStatus,
    DatasetFormat,
    DatasetStatus,
    ObjectStoreType,
    ProjectRole,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import (
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
    CompletedObject,
    ProviderUpload,
    TransferCursor,
    TransferReceipt,
    UploadContractError,
    UploadProgress,
)
from automl_api.storage.object_store import get_object_store

ACTIVE_UPLOAD_STATES = {"initiated", "uploading", "object_completed", "verifying", "retiring"}
TERMINAL_UPLOAD_STATES = {"ready", "aborted", "expired", "quarantined", "failed"}
SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")
UPLOAD_ADMISSION_RECORD = "admission_custody"
UPLOAD_ADMISSION_STARTED = "native_admission_started"
UPLOAD_COMPLETION_PROTOCOL = "dataset.upload.complete.v2"
UPLOAD_COMPLETION_RECORD = "completion_custody"
UPLOAD_COMPLETION_STARTED = "native_completion_started"
UPLOAD_VERIFICATION_RECORD = "verification_custody"
UPLOAD_RETIREMENT_PROTOCOL = "dataset.upload.retire.v1"
UPLOAD_RETIREMENT_RECORD = "retirement_custody"
UPLOAD_RETIREMENT_STARTED = "native_retirement_started"


def _admit_upload_session(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ResumableUploadBeginRequest,
    *,
    origin: str,
    settings: Settings | None = None,
    session_id: uuid.UUID,
    driver_name: str,
    protocol: str,
    object_uri: str,
    object_key: str,
) -> DatasetUploadSession:
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

    if protocol == "single_put":
        raise ValueError("The selected embedded driver cannot issue browser upload instructions.")
    expires_at = datetime.now(UTC) + timedelta(seconds=settings.upload_session_ttl_seconds)
    part_size = settings.upload_part_size_bytes
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
        multipart_upload_id="",
        provider_driver=driver_name,
        protocol=protocol,
        provider_state={"upload_object_uri": object_uri},
        transfer_receipts=[],
        instruction_state={},
        confirmed_bytes=0,
        resume_key=secrets.token_urlsafe(48),
        status="initiated",
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
    return upload


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
    if for_update:
        try:
            _require_admitted(upload)
        except UploadContractError as exc:
            raise HTTPException(409, str(exc)) from exc
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
    _validate_transfer_cursor(upload, cursor)
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name, settings)
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
    return instruction_read(instruction)


def _validate_transfer_cursor(upload, cursor):
    offset = (cursor.unit_number - 1) * upload.part_size
    if upload.protocol == "resumable_offset":
        offset = upload.confirmed_bytes
        if cursor.unit_number != offset // upload.part_size + 1:
            raise UploadContractError("Transfer cursor does not match the confirmed offset.")
    if (
        not 0 <= offset < upload.byte_size
        or cursor.offset != offset
        or cursor.length != min(upload.part_size, upload.byte_size - offset)
    ):
        raise UploadContractError("Transfer cursor does not match the session transfer unit.")


def instruction_read(instruction):
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
    _require_admitted(upload)
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name, settings)
    progress = driver.query_progress(provider_upload(upload))
    _validate_progress(upload, progress)
    upload.confirmed_bytes = progress.confirmed_bytes
    upload.transfer_receipts = [receipt_to_dict(value) for value in progress.receipts]
    upload.last_progress_at = datetime.now(UTC)
    if progress.complete and upload.status == "uploading":
        upload.status = "object_completed"
    return progress_read(upload, complete=progress.complete)


def _validate_progress(upload, progress):
    if not isinstance(progress, UploadProgress) or not isinstance(progress.receipts, (list, tuple)):
        raise UploadContractError("Provider returned malformed upload progress.")
    if (
        type(progress.confirmed_bytes) is not int
        or type(progress.total_bytes) is not int
        or type(progress.complete) is not bool
        or progress.total_bytes != upload.byte_size
        or not upload.confirmed_bytes <= progress.confirmed_bytes <= upload.byte_size
        or progress.complete
        and progress.confirmed_bytes != upload.byte_size
    ):
        raise UploadContractError("Provider progress differs from the admitted byte bounds.")
    units = set()
    for receipt in progress.receipts:
        if (
            not isinstance(receipt, TransferReceipt)
            or any(
                type(value) is not int
                for value in (receipt.unit_number, receipt.offset, receipt.length)
            )
            or receipt.unit_number in units
            or not 1 <= receipt.unit_number <= upload.total_parts
            or receipt.offset != (receipt.unit_number - 1) * upload.part_size
            or receipt.length != min(upload.part_size, upload.byte_size - receipt.offset)
        ):
            raise UploadContractError("Provider progress has an invalid transfer receipt.")
        units.add(receipt.unit_number)
    if upload.protocol == "resumable_offset":
        if progress.receipts:
            raise UploadContractError("Offset progress cannot adopt synthetic part receipts.")
    elif sum(receipt.length for receipt in progress.receipts) != progress.confirmed_bytes:
        raise UploadContractError("Provider receipts differ from the confirmed byte count.")


def _progress_row(db, actor_id, token_version, project_id, session_id, settings):
    from automl_api.services import upload_admission as admission

    admission._authorize(db, project_id, actor_id, token_version)
    upload = db.get(DatasetUploadSession, session_id, with_for_update=True, populate_existing=True)
    if upload is None or upload.project_id != project_id:
        raise HTTPException(404, "Upload session not found.")
    admission.require_published(db, upload)
    _require_admitted(upload)
    admission.require_store(settings, upload.provider_state, upload.provider_driver)
    if upload.status not in TERMINAL_UPLOAD_STATES:
        _require_active(upload)
        if (
            upload.status not in {"uploading", "object_completed"}
            or UPLOAD_COMPLETION_RECORD in upload.provider_state
            or UPLOAD_COMPLETION_STARTED in upload.provider_state
            or upload.completed_object_uri is not None
            or db.scalar(
                select(WorkflowCommand.id)
                .where(
                    WorkflowCommand.project_id == project_id,
                    WorkflowCommand.resource_id == session_id,
                    WorkflowCommand.operation.in_(
                        {UPLOAD_COMPLETION_PROTOCOL, UPLOAD_RETIREMENT_PROTOCOL}
                    ),
                    WorkflowCommand.status != CommandStatus.PENDING,
                )
                .limit(1)
            )
            is not None
        ):
            raise UploadContractError("Original completion prevents new progress observations.")
    return upload


def _progress_stamp(upload):
    from automl_api.services.upload_completion import SOURCE_FIELDS

    return jsonable_encoder(
        {
            name: getattr(upload, name)
            for name in (
                *SOURCE_FIELDS,
                "provider_state",
                "status",
                "confirmed_bytes",
                "transfer_receipts",
                "instruction_state",
                "completed_object_uri",
                "dataset_id",
                "dataset_version_id",
            )
        }
    )


def observe_upload_progress(
    factory, actor_id, token_version, project_id, session_id, *, settings=None
):
    settings = settings or get_settings()
    with factory() as db, db.begin():
        upload = _progress_row(db, actor_id, token_version, project_id, session_id, settings)
        if upload.status in TERMINAL_UPLOAD_STATES:
            return progress_read(
                upload,
                complete=upload.status == "ready" and upload.confirmed_bytes == upload.byte_size,
            )
        original = _progress_stamp(upload)
        db.expunge(upload)
    # SDK construction can refresh credentials over the network.
    driver = get_object_store(settings)
    _require_session_driver(upload, driver.driver_name, settings)
    expected_uri = upload.provider_state.get("upload_object_uri")
    if expected_uri is not None and driver.uri_for_key(upload.object_key) != expected_uri:
        raise UploadContractError("Provider target differs from the admitted upload URI.")
    with factory() as db, db.begin():
        current = _progress_row(db, actor_id, token_version, project_id, session_id, settings)
        if _progress_stamp(current) != original:
            raise UploadContractError("Upload changed before provider progress inspection.")
    progress = driver.query_progress(provider_upload(upload))
    _validate_progress(upload, progress)
    with factory() as db, db.begin():
        current = _progress_row(db, actor_id, token_version, project_id, session_id, settings)
        if _progress_stamp(current) != original:
            raise UploadContractError("Upload changed during provider progress inspection.")
        current.confirmed_bytes = progress.confirmed_bytes
        current.transfer_receipts = [receipt_to_dict(value) for value in progress.receipts]
        current.last_progress_at = datetime.now(UTC)
        if progress.complete and current.status == "uploading":
            current.status = "object_completed"
        return progress_read(current, complete=progress.complete)


def record_completed_upload(
    db: Session,
    upload: DatasetUploadSession,
    completed: CompletedObject,
    provider_receipts: list[TransferReceipt],
    *,
    driver,
) -> UploadCompletionRead:
    """Materialize an already recorded native completion; no provider I/O."""
    require_upload_byte_digest(upload)
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
    return completion_read(db, upload, driver=driver)


def _upload_retained(upload: DatasetUploadSession, now: datetime) -> bool:
    return upload.legal_hold or (
        upload.retention_until is not None and upload.retention_until > now
    )


def upload_read(
    upload: DatasetUploadSession, *, driver=None, capabilities=None
) -> ResumableUploadRead:
    capabilities = capabilities or (driver or get_object_store()).capabilities()
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
        capabilities=UploadCapabilitiesRead.model_validate(capabilities, from_attributes=True),
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


def completion_read(
    db: Session, upload: DatasetUploadSession, *, driver=None
) -> UploadCompletionRead:
    dataset = db.get(Dataset, upload.dataset_id) if upload.dataset_id else None
    version = (
        db.get(DatasetVersion, upload.dataset_version_id) if upload.dataset_version_id else None
    )
    return UploadCompletionRead(
        session=upload_read(upload, driver=driver),
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


def require_upload_byte_digest(upload: DatasetUploadSession) -> str:
    digest = getattr(upload, "expected_object_digest", None)
    if (
        getattr(upload, "digest_algorithm", None) != "sha256"
        or getattr(upload, "digest_scope", None) != "byte_stream"
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        # Legacy manifest hashes require reviewed migration, never automatic deletion.
        raise UploadContractError("Upload requires an explicit byte-stream SHA-256 manifest.")
    return digest


def require_verified_upload_digest(upload: DatasetUploadSession) -> str:
    digest = require_upload_byte_digest(upload)
    verified_at = getattr(upload, "checksum_verified_at", None)
    size = getattr(upload, "byte_size", None)
    if (
        getattr(upload, "status", None) != "ready"
        or getattr(upload, "observed_object_digest", None) != digest
        or getattr(upload, "scanner_status", None) != "allowed"
        or not isinstance(verified_at, datetime)
        or verified_at.tzinfo is None
        or verified_at.utcoffset() is None
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
    ):
        raise UploadContractError("Upload has not passed object digest verification.")
    return digest


def _require_admitted(upload: DatasetUploadSession) -> None:
    state = getattr(upload, "provider_state", None) or {}
    if (
        not upload.multipart_upload_id
        or (UPLOAD_ADMISSION_RECORD in state or UPLOAD_ADMISSION_STARTED in state)
        and (
            not isinstance(state.get(UPLOAD_ADMISSION_RECORD), dict)
            or state[UPLOAD_ADMISSION_RECORD].get("phase") != "published"
        )
    ):
        raise UploadContractError("Original upload admission must publish before transfer work.")


def _require_active(upload: DatasetUploadSession) -> None:
    _require_admitted(upload)
    state = getattr(upload, "provider_state", None) or {}
    if (
        upload.status == "retiring"
        or UPLOAD_RETIREMENT_RECORD in state
        or UPLOAD_RETIREMENT_STARTED in state
    ):
        raise UploadContractError(
            "Original upload retirement must resolve before more upload work."
        )
    if upload.status in TERMINAL_UPLOAD_STATES:
        raise UploadContractError(f"Upload session is terminal with status '{upload.status}'.")
    if upload.expires_at <= datetime.now(UTC):
        raise UploadContractError("Upload session has expired.")


def _require_session_driver(
    upload: DatasetUploadSession, driver_name: str, settings: Settings
) -> None:
    from automl_api.services.upload_admission import require_store

    require_store(settings, upload.provider_state, driver_name)
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
