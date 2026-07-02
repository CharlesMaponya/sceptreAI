from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
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
from automl_api.schemas.profiling_jobs import ProfilingJobCreate
from automl_api.services.datasets import (
    get_dataset_for_user,
    list_dataset_versions,
    list_project_datasets,
    upload_dataset_version,
)
from automl_api.services.profiling_jobs import create_profiling_job, schedule_profiling_job

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
    payload: DatasetUploadRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> DatasetUploadResponse:
    try:
        dataset, version = upload_dataset_version(db, current_user, project_id, payload)
        profiling_job, _ = create_profiling_job(
            db,
            current_user,
            project_id,
            dataset.id,
            version.id,
            ProfilingJobCreate(),
            auto_started=True,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    db.commit()
    db.refresh(dataset)
    db.refresh(version)
    db.refresh(profiling_job)
    schedule_profiling_job(profiling_job.id)
    return DatasetUploadResponse(
        dataset=DatasetRead.model_validate(dataset),
        version=DatasetVersionRead.model_validate(version),
        profiling_job_id=profiling_job.id,
        profiling_job_status=profiling_job.status,
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
