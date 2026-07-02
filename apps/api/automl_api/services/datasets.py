from __future__ import annotations

import base64
import binascii
import hashlib
import uuid

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from automl_api.core.config import get_settings
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import ObjectStoreType, ProjectRole
from automl_api.models.iam import User
from automl_api.schemas.datasets import DatasetUploadRequest
from automl_api.services.dataset_inspection import inspect_tabular_bytes
from automl_api.services.projects import require_project_role
from automl_api.storage.object_store import get_object_store


def list_project_datasets(db: Session, user: User, project_id: uuid.UUID) -> list[Dataset]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    query = (
        select(Dataset)
        .where(Dataset.project_id == project_id)
        .order_by(Dataset.created_at.desc())
    )
    return list(db.scalars(query).all())


def list_dataset_versions(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
) -> list[DatasetVersion]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    _get_project_dataset(db, project_id, dataset_id)
    query = (
        select(DatasetVersion)
        .where(DatasetVersion.project_id == project_id, DatasetVersion.dataset_id == dataset_id)
        .order_by(DatasetVersion.version_number.desc())
    )
    return list(db.scalars(query).all())


def get_dataset_for_user(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
) -> Dataset:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return _get_project_dataset(db, project_id, dataset_id)


def upload_dataset_version(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: DatasetUploadRequest,
) -> tuple[Dataset, DatasetVersion]:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    content = _decode_upload_content(payload.content_base64)
    inspection = inspect_tabular_bytes(payload.filename, content)
    content_hash = hashlib.sha256(content).hexdigest()

    dataset = db.scalar(
        select(Dataset)
        .options(selectinload(Dataset.versions))
        .where(Dataset.project_id == project_id, Dataset.name == payload.dataset_name)
    )
    if dataset is None:
        dataset = Dataset(
            project_id=project_id,
            created_by_id=user.id,
            name=payload.dataset_name,
            description=payload.description,
            tags=payload.tags,
        )
        db.add(dataset)
        db.flush()
    else:
        dataset.description = payload.description if payload.description is not None else dataset.description
        dataset.tags = payload.tags or dataset.tags

    next_version = dataset.latest_version_number + 1
    object_key = (
        f"projects/{project_id}/datasets/{dataset.id}/versions/{next_version}/"
        f"{content_hash[:12]}-{payload.filename}"
    )
    stored_object = get_object_store().put_bytes(object_key, content)
    object_store_type = _object_store_type_from_settings()

    dataset_version = DatasetVersion(
        project_id=project_id,
        dataset_id=dataset.id,
        created_by_id=user.id,
        version_number=next_version,
        status=inspection.status,
        format=inspection.format,
        object_store_type=object_store_type,
        object_uri=stored_object.uri,
        original_filename=payload.filename,
        content_hash=content_hash,
        byte_size=len(content),
        row_count=inspection.row_count,
        column_count=inspection.column_count,
        schema_json=inspection.schema_json,
        inferred_types_json=inspection.inferred_types_json,
        quality_report_json=inspection.quality_report_json,
    )
    db.add(dataset_version)
    dataset.latest_version_number = next_version
    db.flush()
    return dataset, dataset_version


def _decode_upload_content(content_base64: str) -> bytes:
    try:
        return base64.b64decode(content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Uploaded content must be valid base64.",
        ) from exc


def _object_store_type_from_settings() -> ObjectStoreType:
    raw_value = get_settings().object_store_type.lower()
    try:
        return ObjectStoreType(raw_value)
    except ValueError:
        return ObjectStoreType.MINIO


def _get_project_dataset(db: Session, project_id: uuid.UUID, dataset_id: uuid.UUID) -> Dataset:
    dataset = db.scalar(
        select(Dataset).where(Dataset.project_id == project_id, Dataset.id == dataset_id)
    )
    if dataset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")
    return dataset

