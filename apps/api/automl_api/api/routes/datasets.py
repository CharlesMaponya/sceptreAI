from __future__ import annotations

import hashlib
import json
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile, status
from sqlalchemy.orm import Session, sessionmaker

from automl_api.api.deps import get_current_user
from automl_api.core.config import get_settings
from automl_api.db.session import get_db, get_session_factory
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
    UploadCleanupReplayRead,
    UploadCleanupReplayRequest,
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
from automl_api.services.upload_admission import begin_upload_session
from automl_api.services.upload_admission_cleanup import replay_cleanup
from automl_api.services.upload_completion import complete_upload_session
from automl_api.services.upload_instructions import issue_upload_instruction
from automl_api.services.upload_retirement import abort_upload_session
from automl_api.services.uploads import (
    completion_read,
    get_upload_session,
    observe_upload_progress,
    upload_read,
)
from automl_api.storage.contracts import DeletionPending, UploadContractError

router = APIRouter(prefix="/projects/{project_id}/datasets", tags=["datasets"])


@router.post("/uploads", response_model=ResumableUploadRead, status_code=status.HTTP_201_CREATED)
def begin_resumable_upload(
    project_id: uuid.UUID,
    payload: ResumableUploadBeginRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S"),
    ],
    origin: Annotated[str, Header(alias="Origin", min_length=1)],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> ResumableUploadRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return begin_upload_session(
            factory, actor_id, token_version, project_id, payload, idempotency_key, origin=origin
        )
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


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


@router.post("/uploads/{session_id}/instructions", response_model=TransferInstructionRead)
def create_upload_instruction(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    payload: TransferInstructionRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S"),
    ],
    origin: Annotated[str, Header(alias="Origin", min_length=1)],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> TransferInstructionRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return issue_upload_instruction(
            factory,
            actor_id,
            token_version,
            project_id,
            session_id,
            payload.cursor,
            idempotency_key,
            origin=origin,
        )
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.get("/uploads/{session_id}/progress", response_model=UploadProgressRead)
def upload_progress(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> UploadProgressRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return observe_upload_progress(factory, actor_id, token_version, project_id, session_id)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/uploads/{session_id}/complete", response_model=UploadCompletionRead)
def complete_resumable_upload(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    payload: CompleteUploadRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S"),
    ],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> UploadCompletionRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return complete_upload_session(
            factory, actor_id, token_version, project_id, session_id, payload, idempotency_key
        )
    except UploadContractError as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post("/uploads/{session_id}/abort", response_model=AbortUploadRead)
def abort_resumable_upload(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S")
    ],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> AbortUploadRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return abort_upload_session(
            factory, actor_id, token_version, project_id, session_id, idempotency_key
        )
    except (DeletionPending, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc


@router.post(
    "/uploads/{session_id}/cleanup/replay",
    response_model=UploadCleanupReplayRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def replay_upload_cleanup(
    project_id: uuid.UUID,
    session_id: uuid.UUID,
    payload: UploadCleanupReplayRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S")
    ],
    factory: Annotated[sessionmaker[Session], Depends(get_session_factory)],
) -> UploadCleanupReplayRead:
    actor_id, token_version = current_user.id, current_user.token_version
    db.commit()
    try:
        return replay_cleanup(
            factory,
            actor_id,
            token_version,
            project_id,
            session_id,
            idempotency_key,
            payload.reason,
        )
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc


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
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"\S"),
    ],
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
        if governed_environment and len(content) > settings.buffered_upload_max_bytes:
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
            execute=lambda: _upload_response(db, current_user, project_id, payload, content),
            response_model=DatasetUploadResponse,
            response_status=status.HTTP_201_CREATED,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
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
