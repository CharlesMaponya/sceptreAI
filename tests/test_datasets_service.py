from __future__ import annotations

import hashlib
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import Dataset
from automl_api.models.enums import DatasetFormat, DatasetStatus, ObjectStoreType
from automl_api.schemas.datasets import DatasetUploadRequest
from automl_api.services import datasets
from fastapi import HTTPException


class _ScalarResult:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def all(self) -> list[object]:
        return self.values


class _Session:
    def __init__(self, scalar_values: list[object | None] | None = None) -> None:
        self.scalar_values = list(scalar_values or [])
        self.scalar_lists: list[list[object]] = []
        self.added: list[object] = []
        self.flushes = 0

    def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    def scalars(self, _statement: object) -> _ScalarResult:
        return _ScalarResult(self.scalar_lists.pop(0))

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def flush(self) -> None:
        self.flushes += 1
        for instance in self.added:
            if getattr(instance, "id", None) is None:
                instance.id = uuid.uuid4()
            if isinstance(instance, Dataset) and instance.latest_version_number is None:
                instance.latest_version_number = 0


def _inspection() -> SimpleNamespace:
    return SimpleNamespace(
        status=DatasetStatus.READY,
        format=DatasetFormat.CSV,
        row_count=2,
        column_count=2,
        schema_json={"columns": [{"name": "a"}, {"name": "b"}]},
        inferred_types_json={"a": "integer", "b": "integer"},
        quality_report_json={"warnings": []},
    )


def test_dataset_queries_enforce_membership_and_scope(monkeypatch) -> None:
    authorized: list[tuple[object, ...]] = []
    monkeypatch.setattr(datasets, "require_project_role", lambda *args: authorized.append(args))
    project_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    dataset = SimpleNamespace(id=dataset_id)
    versions = [SimpleNamespace(version_number=2), SimpleNamespace(version_number=1)]

    list_db = _Session()
    list_db.scalar_lists = [[dataset]]
    assert datasets.list_project_datasets(list_db, user, project_id) == [dataset]

    version_db = _Session([dataset])
    version_db.scalar_lists = [versions]
    assert datasets.list_dataset_versions(version_db, user, project_id, dataset_id) == versions
    assert (
        datasets.get_dataset_for_user(_Session([dataset]), user, project_id, dataset_id) is dataset
    )
    assert len(authorized) == 3

    with pytest.raises(HTTPException, match="Dataset not found") as error:
        datasets._get_project_dataset(_Session([None]), project_id, dataset_id)
    assert error.value.status_code == 404


def test_upload_creates_immutable_dataset_version(monkeypatch) -> None:
    project_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    payload = DatasetUploadRequest(
        dataset_name="Customers",
        description="Qualified upload",
        filename="customers.csv",
        tags={"source": "crm"},
    )
    content = b"a,b\n1,2\n3,4\n"
    store = MagicMock()
    store.put_bytes.return_value = SimpleNamespace(uri="minio://datasets/object.csv")
    monkeypatch.setattr(datasets, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(datasets, "inspect_tabular_bytes", lambda *_args: _inspection())
    monkeypatch.setattr(datasets, "get_object_store", lambda: store)
    monkeypatch.setattr(
        datasets,
        "get_settings",
        lambda: SimpleNamespace(object_store_type="s3"),
    )
    db = _Session([None])

    dataset, version = datasets.upload_dataset_version(db, user, project_id, payload, content)

    assert isinstance(dataset, Dataset)
    assert dataset.latest_version_number == 1
    assert version.version_number == 1
    assert version.content_hash == hashlib.sha256(content).hexdigest()
    assert version.object_store_type == ObjectStoreType.S3
    assert version.object_uri == "minio://datasets/object.csv"
    key, stored_content = store.put_bytes.call_args.args
    assert key.startswith(f"projects/{project_id}/datasets/{dataset.id}/versions/1/")
    assert key.endswith("-customers.csv")
    assert stored_content == content
    assert db.flushes == 2


def test_upload_updates_existing_dataset_without_erasing_optional_metadata(monkeypatch) -> None:
    project_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    existing = SimpleNamespace(
        id=uuid.uuid4(),
        description="Original",
        tags={"owner": "finance"},
        latest_version_number=4,
    )
    store = MagicMock()
    store.put_bytes.return_value = SimpleNamespace(uri="minio://datasets/v5.csv")
    monkeypatch.setattr(datasets, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(datasets, "inspect_tabular_bytes", lambda *_args: _inspection())
    monkeypatch.setattr(datasets, "get_object_store", lambda: store)
    monkeypatch.setattr(
        datasets,
        "get_settings",
        lambda: SimpleNamespace(object_store_type="unexpected-store"),
    )
    db = _Session([existing])

    _, version = datasets.upload_dataset_version(
        db,
        user,
        project_id,
        DatasetUploadRequest(
            dataset_name="Customers",
            description=None,
            filename="customers.csv",
            tags={},
        ),
        b"a,b\n5,6\n",
    )

    assert existing.description == "Original"
    assert existing.tags == {"owner": "finance"}
    assert existing.latest_version_number == 5
    assert version.version_number == 5
    assert version.object_store_type == ObjectStoreType.MINIO
