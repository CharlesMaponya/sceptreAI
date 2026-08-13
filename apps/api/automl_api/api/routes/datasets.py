from __future__ import annotations

import hashlib
import json
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.datasets import (
    DatasetRead,
    DatasetUploadRequest,
    DatasetUploadResponse,
    DatasetVersionRead,
)
from automl_api.services.datasets import (
    get_dataset_for_user,
    list_dataset_versions,
    list_project_datasets,
    upload_dataset_version,
)
from automl_api.services.idempotency import durable_mutation

router = APIRouter(prefix="/projects/{project_id}/datasets", tags=["datasets"])


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
        content = file.file.read()
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
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
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
