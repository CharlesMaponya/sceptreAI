from __future__ import annotations

import hashlib
import json
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.core.config import get_settings
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.datasets import (
    AbortUploadRead,
    CompleteUploadRequest,
    DatasetRead,
    DatasetUploadRequest,
    DatasetUploadResponse,
    DatasetVersionRead,
    ResumableUploadBeginRequest,
    ResumableUploadRead,
    TransferInstructionRead,
    TransferInstructionRequest,
    UploadCompletionRead,
    UploadProgressRead,
)
from automl_api.services.datasets import (
    get_dataset_for_user,
    list_dataset_versions,
    list_project_datasets,
    upload_dataset_version,
)
from automl_api.services.idempotency import durable_mutation
from automl_api.services.uploads import (
    abort_upload_session,
    begin_upload_session,
    complete_upload_session,
    completion_read,
    get_upload_session,
    issue_transfer_instruction,
    query_upload_progress,
    upload_read,
)

router = APIRouter(prefix="/projects/{project_id}/datasets", tags=["datasets"])


@router.post("/uploads", response_model=ResumableUploadRead, status_code=status.HTTP_201_CREATED)
def begin_resumable_upload(
    project_id: uuid.UUID,
    payload: ResumableUploadBeginRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
    origin: Annotated[str, Header(alias="Origin", min_length=1)],
) -> ResumableUploadRead:
    response = durable_mutation(
        db,
        current_user,
        project_id,
        operation="dataset.upload.begin",
        idempotency_key=idempotency_key,
        payload={**payload.model_dump(mode="json"), "origin": origin},
        execute=lambda: begin_upload_session(
            db,
            current_user,
            project_id,
            payload,
            origin=origin,
            session_id=uuid.uuid5(
                project_id,
                f"dataset.upload.begin:{current_user.id}:{idempotency_key}",
            ),
        ),
        response_model=ResumableUploadRead,
        response_status=status.HTTP_201_CREATED,
    )
    db.commit()
    return response


@router.get("/uploads/{session_id}", response_model=ResumableUploadRead)
def resume_upload(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ResumableUploadRead:
    return upload_read(get_upload_session(db, current_user, project_id, session_id))


@router.get("/uploads/{session_id}/result", response_model=UploadCompletionRead)
def upload_result(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> UploadCompletionRead:
    upload = get_upload_session(db, current_user, project_id, session_id)
    return completion_read(db, upload)


@router.post(
    "/uploads/{session_id}/instructions", response_model=TransferInstructionRead
)
def create_upload_instruction(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    payload: TransferInstructionRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
    origin: Annotated[str, Header(alias="Origin", min_length=1)],
) -> TransferInstructionRead:
    upload = get_upload_session(
        db, current_user, project_id, session_id, for_update=True
    )
    response = durable_mutation(
        db,
        current_user,
        project_id,
        operation="dataset.upload.instruction",
        idempotency_key=idempotency_key,
        payload={
            "session_id": str(session_id),
            "origin": origin,
            "cursor": payload.cursor.model_dump(mode="json"),
        },
        execute=lambda: issue_transfer_instruction(upload, payload.cursor, origin=origin),
        response_model=TransferInstructionRead,
        response_status=status.HTTP_200_OK,
        resource_id=session_id,
    )
    db.commit()
    return response


@router.get("/uploads/{session_id}/progress", response_model=UploadProgressRead)
def upload_progress(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> UploadProgressRead:
    upload = get_upload_session(
        db, current_user, project_id, session_id, for_update=True
    )
    response = query_upload_progress(upload)
    db.commit()
    return response


@router.post("/uploads/{session_id}/complete", response_model=UploadCompletionRead)
def complete_resumable_upload(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    payload: CompleteUploadRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> UploadCompletionRead:
    upload = get_upload_session(
        db, current_user, project_id, session_id, for_update=True
    )
    response = durable_mutation(
        db,
        current_user,
        project_id,
        operation="dataset.upload.complete.direct",
        idempotency_key=idempotency_key,
        payload={"session_id": str(session_id), **payload.model_dump(mode="json")},
        execute=lambda: complete_upload_session(
            db,
            upload,
            sha256=payload.sha256,
            receipts=payload.receipts,
        ),
        response_model=UploadCompletionRead,
        response_status=status.HTTP_200_OK,
        outbox_topic="upload.reconcile",
        outbox_payload=lambda _: {"session_id": str(session_id)},
        aggregate_type="dataset_upload_session",
        resource_id=session_id,
    )
    db.commit()
    return response


@router.post("/uploads/{session_id}/abort", response_model=AbortUploadRead)
def abort_resumable_upload(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> AbortUploadRead:
    upload = get_upload_session(
        db, current_user, project_id, session_id, for_update=True
    )
    response = durable_mutation(
        db,
        current_user,
        project_id,
        operation="dataset.upload.abort",
        idempotency_key=idempotency_key,
        payload={"session_id": str(session_id)},
        execute=lambda: abort_upload_session(upload),
        response_model=AbortUploadRead,
        response_status=status.HTTP_200_OK,
        resource_id=session_id,
    )
    db.commit()
    return response


@router.get("", response_model=list[DatasetRead])
def list_datasets(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[DatasetRead]:
    return [
        DatasetRead.model_validate(dataset)
        for dataset in list_project_datasets(db, current_user, project_id)
    ]


@router.post("/upload", response_model=DatasetUploadResponse, status_code=status.HTTP_201_CREATED)
def upload_dataset(
    project_id: uuid.UUID,
    file: Annotated[UploadFile, File(description="Tabular dataset file")],
    dataset_name: Annotated[str, Form(min_length=1, max_length=220)],
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
    description: Annotated[str | None, Form()] = None,
    tags: Annotated[str, Form()] = "{}",
) -> DatasetUploadResponse:
    try:
        settings = get_settings()
        if (
            settings.environment.lower() in {"staging", "production"}
            and file.size is not None
            and file.size > settings.buffered_upload_max_bytes
        ):
            raise ValueError(
                "API-buffered uploads above 100 MiB are disabled; use the resumable "
                "direct-to-object-storage upload protocol."
            )
        governed_environment = settings.environment.lower() in {"staging", "production"}
        content = file.file.read(
            settings.buffered_upload_max_bytes + 1 if governed_environment else -1
        )
        if (
            governed_environment
            and len(content) > settings.buffered_upload_max_bytes
        ):
            raise ValueError(
                "API-buffered uploads above 100 MiB are disabled; use the resumable "
                "direct-to-object-storage upload protocol."
            )
        if not content:
            raise ValueError("Uploaded file must not be empty.")
        parsed_tags = json.loads(tags)
        if not isinstance(parsed_tags, dict):
            raise ValueError("Dataset tags must be a JSON object.")
        payload = DatasetUploadRequest(
            dataset_name=dataset_name,
            description=description,
            filename=file.filename or "",
            tags=parsed_tags,
        )
        response = durable_mutation(
            db,
            current_user,
            project_id,
            operation="dataset.upload.complete",
            idempotency_key=idempotency_key,
            payload={
                **payload.model_dump(mode="json"),
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "byte_size": len(content),
            },
            execute=lambda: _upload_response(
                db, current_user, project_id, payload, content
            ),
            response_model=DatasetUploadResponse,
            response_status=status.HTTP_201_CREATED,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=str(exc)
        ) from exc
    finally:
        file.file.close()

    db.commit()
    return response


def _upload_response(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: DatasetUploadRequest,
    content: bytes,
) -> DatasetUploadResponse:
    dataset, version = upload_dataset_version(db, user, project_id, payload, content)
    db.flush()
    return DatasetUploadResponse(
        dataset=DatasetRead.model_validate(dataset),
        version=DatasetVersionRead.model_validate(version),
    )


@router.get("/{dataset_id}", response_model=DatasetRead)
def get_dataset(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> DatasetRead:
    dataset = get_dataset_for_user(db, current_user, project_id, dataset_id)
    return DatasetRead.model_validate(dataset)


@router.get("/{dataset_id}/versions", response_model=list[DatasetVersionRead])
def versions(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[DatasetVersionRead]:
    return [
        DatasetVersionRead.model_validate(version)
        for version in list_dataset_versions(db, current_user, project_id, dataset_id)
    ]
