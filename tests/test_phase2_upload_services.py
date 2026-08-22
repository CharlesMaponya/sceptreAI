from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import datasets as dataset_routes
from automl_api.core.config import Settings
from automl_api.models.datasets import Dataset, DatasetUploadSession
from automl_api.models.enums import DatasetStatus, ObjectStoreType
from automl_api.schemas.datasets import (
    ResumableUploadBeginRequest,
    TransferCursorRead,
    TransferReceiptRead,
)
from automl_api.services import uploads
from automl_api.storage.contracts import (
    CompletedObject,
    HealthcheckResult,
    ObjectMetadata,
    ProviderUpload,
    TransferReceipt,
    UploadCapabilities,
    UploadContractError,
    UploadInstruction,
    UploadProgress,
)
from fastapi import HTTPException
from starlette.datastructures import UploadFile


class ScalarRows:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def __iter__(self):
        return iter(self.values)


class Session:
    def __init__(self, scalars: list[object | None] | None = None) -> None:
        self.scalar_values = list(scalars or [])
        self.scalar_statements: list[object] = []
        self.row_values: list[list[object]] = []
        self.added: list[object] = []
        self.objects: dict[tuple[type, object], object] = {}
        self.flushes = 0

    def scalar(self, _statement: object) -> object | None:
        self.scalar_statements.append(_statement)
        return self.scalar_values.pop(0) if self.scalar_values else None

    def scalars(self, _statement: object) -> ScalarRows:
        return ScalarRows(self.row_values.pop(0) if self.row_values else [])

    def add(self, value: object) -> None:
        self.added.append(value)

    def flush(self) -> None:
        self.flushes += 1
        for value in self.added:
            if getattr(value, "id", None) is None:
                value.id = uuid.uuid4()
            if getattr(value, "created_at", None) is None:
                value.created_at = datetime.now(UTC)
            if getattr(value, "updated_at", None) is None:
                value.updated_at = datetime.now(UTC)
            if isinstance(value, Dataset) and value.latest_version_number is None:
                value.latest_version_number = 0

    def get(self, model: type, identity: object) -> object | None:
        return self.objects.get((model, identity))


class Driver:
    driver_name = "s3_compatible"

    def __init__(self) -> None:
        self.progress = UploadProgress(0, 10, (), False)
        self.aborted = 0
        self.completed = 0
        self.deleted: list[str] = []
        self.present: set[str] = set()

    def capabilities(self) -> UploadCapabilities:
        return UploadCapabilities("multipart", 4, True, False, True, False)

    def begin_upload(self, request) -> ProviderUpload:
        self.begin_request = request
        return ProviderUpload(
            "provider-1",
            request.object_key,
            "multipart",
            request.byte_size,
            request.expires_at,
            {"transfer_unit_size": request.transfer_unit_size},
        )

    def create_transfer_instruction(self, upload, cursor) -> UploadInstruction:
        return UploadInstruction(
            "instruction-1",
            "PUT",
            "https://storage.test/object?sig=secret",
            {},
            cursor,
            upload.expires_at,
            ("ETag",),
            ("object", "method"),
            ("exact_length",),
        )

    def query_progress(self, _upload) -> UploadProgress:
        return self.progress

    def complete_upload(self, upload, _receipts) -> CompletedObject:
        self.completed += 1
        uri = f"s3c://bucket/{upload.object_key}"
        self.present.add(uri)
        return CompletedObject(uri, upload.byte_size, "provider-checksum")

    def uri_for_key(self, object_key: str) -> str:
        return f"s3c://bucket/{object_key}"

    def stat(self, uri: str) -> ObjectMetadata:
        return ObjectMetadata(uri=uri, byte_size=10, checksum="provider-checksum")

    def abort_upload(self, _upload) -> None:
        self.aborted += 1

    def exists(self, uri: str) -> bool:
        return uri in self.present

    def delete(self, uri: str) -> None:
        self.present.discard(uri)
        self.deleted.append(uri)

    def healthcheck(self) -> HealthcheckResult:
        return HealthcheckResult(True, self.driver_name)


def _settings(**kwargs: object) -> Settings:
    values = {
        "object_store_type": "s3_compatible",
        "object_store_endpoint": "http://internal:8333",
        "object_store_access_key": "key",
        "object_store_secret_key": "secret",
        "upload_allowed_origins": ("https://app.test",),
        "upload_part_size_bytes": 6,
        "project_storage_quota_bytes": 100,
        "max_upload_size_bytes": 100,
    }
    values.update(kwargs)
    return Settings(**values)


def _payload(**kwargs: object) -> ResumableUploadBeginRequest:
    values = {
        "upload_kind": "offline_scoring",
        "dataset_name": "Scores",
        "filename": "scores.csv",
        "byte_size": 10,
        "content_type": "text/csv",
        "sha256": "a" * 64,
        "sensitivity": "confidential",
        "data_region": "eu-west-1",
        "retention_days": 30,
        "legal_hold": False,
    }
    values.update(kwargs)
    return ResumableUploadBeginRequest(**values)


def _upload(**kwargs: object) -> DatasetUploadSession:
    now = datetime.now(UTC)
    values = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "created_by_id": uuid.uuid4(),
        "dataset_name": "Scores",
        "upload_kind": "offline_scoring",
        "description": None,
        "tags": {},
        "original_filename": "scores.csv",
        "byte_size": 10,
        "part_size": 6,
        "total_parts": 2,
        "object_key": "projects/p/raw/scores.csv",
        "multipart_upload_id": "provider-1",
        "provider_driver": "s3_compatible",
        "protocol": "multipart",
        "provider_state": {"transfer_unit_size": 6},
        "transfer_receipts": [],
        "instruction_state": {},
        "confirmed_bytes": 0,
        "content_type": "text/csv",
        "sensitivity": "confidential",
        "data_region": "eu-west-1",
        "retention_until": now + timedelta(days=30),
        "legal_hold": False,
        "target_metadata": {},
        "resume_key": "resume-secret",
        "status": "uploading",
        "expires_at": now + timedelta(hours=1),
        "digest_algorithm": "sha256",
        "digest_scope": "byte_stream",
        "expected_object_digest": "a" * 64,
        "scanner_status": "pending",
        "retry_count": 0,
        "retry_budget": 5,
        "created_at": now,
        "updated_at": now,
    }
    values.update(kwargs)
    return DatasetUploadSession(**values)


def test_begin_route_uses_a_deterministic_provider_identity(monkeypatch) -> None:
    project_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    observed: dict[str, object] = {}
    expected = SimpleNamespace(id="result")

    def begin(*_args, **kwargs):
        observed.update(kwargs)
        return expected

    monkeypatch.setattr(dataset_routes, "begin_upload_session", begin)
    monkeypatch.setattr(
        dataset_routes,
        "durable_mutation",
        lambda *_args, **kwargs: (
            observed.update({"serialize_project": kwargs["serialize_project"]})
            or kwargs["execute"]()
        ),
    )
    result = dataset_routes.begin_resumable_upload(
        project_id,
        _payload(),
        db,
        user,
        "stable-key",
        "https://app.test",
    )

    assert result is expected
    assert observed["session_id"] == uuid.uuid5(
        project_id,
        f"dataset.upload.begin:{user.id}:stable-key",
    )
    assert observed["serialize_project"] is True
    db.commit.assert_called_once()


def test_begin_instruction_progress_complete_and_abort(monkeypatch) -> None:
    driver = Driver()
    monkeypatch.setattr(uploads, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    user = SimpleNamespace(id=uuid.uuid4())
    project_id = uuid.uuid4()
    db = Session([project_id, 20, 5])
    session_id = uuid.uuid4()

    result = uploads.begin_upload_session(
        db,
        user,
        project_id,
        _payload(),
        origin="https://app.test",
        settings=_settings(),
        session_id=session_id,
    )
    upload = db.added[0]
    assert upload.id == session_id
    assert result.total_parts == 2 and result.next_cursor.offset == 0
    assert upload.data_region == "eu-west-1"
    assert driver.begin_request.origin == "https://app.test"

    instruction = uploads.issue_transfer_instruction(
        upload,
        TransferCursorRead(unit_number=1, offset=0, length=6),
        origin="https://app.test",
        settings=_settings(),
    )
    assert instruction.instruction_id == "instruction-1"
    assert upload.instruction_state["1"]["length"] == 6
    with pytest.raises(UploadContractError, match="does not match"):
        uploads.issue_transfer_instruction(
            upload,
            TransferCursorRead(unit_number=1, offset=1, length=6),
            origin="https://app.test",
            settings=_settings(),
        )

    receipts = (
        TransferReceipt(1, 0, 6, "etag-1"),
        TransferReceipt(2, 6, 4, "etag-2"),
    )
    driver.progress = UploadProgress(10, 10, receipts, True)
    progress = uploads.query_upload_progress(upload, settings=_settings())
    assert progress.complete and upload.status == "object_completed"
    complete = uploads.complete_upload_session(
        db,
        upload,
        sha256="a" * 64,
        receipts=[
            TransferReceiptRead.model_validate(uploads.receipt_to_dict(value)) for value in receipts
        ],
        settings=_settings(),
    )
    assert complete.session.status == "object_completed"
    assert upload.completed_object_uri.startswith("s3c://")
    assert (
        uploads.complete_upload_session(
            db, upload, sha256="a" * 64, receipts=[], settings=_settings()
        ).session.id
        == upload.id
    )

    other = _upload()
    aborted = uploads.abort_upload_session(other, settings=_settings())
    assert aborted.status == "aborted" and driver.aborted == 1
    assert uploads.abort_upload_session(other, settings=_settings()).id == other.id


def test_upload_policy_origin_quota_terminal_and_receipt_failures(monkeypatch) -> None:
    driver = Driver()
    monkeypatch.setattr(uploads, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    settings = _settings()
    with pytest.raises(HTTPException) as denied:
        uploads.require_allowed_origin("https://evil.test", settings)
    assert denied.value.status_code == 403

    with pytest.raises(ValueError, match="quota"):
        uploads.begin_upload_session(
            Session([uuid.uuid4(), 80, 20]),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            _payload(),
            origin="https://app.test",
            settings=settings,
        )
    with pytest.raises(ValueError, match="does not match"):
        uploads.begin_upload_session(
            Session([0, 0]),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            _payload(data_region="eu-west-1"),
            origin="https://app.test",
            settings=_settings(object_store_region="af-south-1"),
        )
    embedded = Driver()
    embedded.capabilities = lambda: UploadCapabilities("single_put", 1, False, False, False, False)
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: embedded)
    with pytest.raises(ValueError, match="cannot issue"):
        project_id = uuid.uuid4()
        uploads.begin_upload_session(
            Session([project_id, 0, 0]),
            SimpleNamespace(id=uuid.uuid4()),
            project_id,
            _payload(),
            origin="https://app.test",
            settings=settings,
        )
    with pytest.raises(HTTPException, match="Project not found"):
        uploads.begin_upload_session(
            Session([None]),
            SimpleNamespace(id=uuid.uuid4()),
            uuid.uuid4(),
            _payload(),
            origin="https://app.test",
            settings=settings,
        )

    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    terminal = _upload(status="failed")
    with pytest.raises(UploadContractError, match="terminal"):
        uploads.issue_transfer_instruction(
            terminal,
            TransferCursorRead(unit_number=1, offset=0, length=6),
            origin="https://app.test",
            settings=settings,
        )
    expired = _upload(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(UploadContractError, match="expired"):
        uploads.issue_transfer_instruction(
            expired,
            TransferCursorRead(unit_number=1, offset=0, length=6),
            origin="https://app.test",
            settings=settings,
        )
    mismatch = _upload()
    with pytest.raises(UploadContractError, match="Final client"):
        uploads.complete_upload_session(
            Session(), mismatch, sha256="b" * 64, receipts=[], settings=settings
        )

    provider = [TransferReceipt(1, 0, 6, "provider")]
    with pytest.raises(UploadContractError, match="absent"):
        uploads._validate_client_receipts(
            [TransferReceiptRead(unit_number=2, offset=6, length=4, etag="e")], provider
        )
    with pytest.raises(UploadContractError, match="length"):
        uploads._validate_client_receipts(
            [TransferReceiptRead(unit_number=1, offset=0, length=5)], provider
        )
    with pytest.raises(UploadContractError, match="ETag"):
        uploads._validate_client_receipts(
            [TransferReceiptRead(unit_number=1, offset=0, length=6, etag="other")], provider
        )


def test_cursor_registration_cleanup_and_lookup(monkeypatch) -> None:
    assert uploads.next_cursor(_upload(confirmed_bytes=10)) is None
    offset = uploads.next_cursor(_upload(protocol="resumable_offset", confirmed_bytes=6))
    assert (offset.unit_number, offset.offset, offset.length) == (2, 6, 4)
    multipart = uploads.next_cursor(
        _upload(confirmed_bytes=6, transfer_receipts=[{"unit_number": 1}])
    )
    assert multipart.unit_number == 2
    assert uploads._safe_filename(" ../../unsafe name.csv ") == "unsafe-name.csv"
    with pytest.raises(ValueError, match="safe"):
        uploads._safe_filename("... ")

    project_id, user_id = uuid.uuid4(), uuid.uuid4()
    existing = Dataset(
        id=uuid.uuid4(),
        project_id=project_id,
        created_by_id=user_id,
        name="Scores",
        description=None,
        latest_version_number=2,
        tags={},
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    existing.versions = []
    db = Session([project_id, existing])
    upload = _upload(
        project_id=project_id,
        created_by_id=user_id,
        upload_kind="validation",
        completed_object_uri="s3c://bucket/file.csv",
    )
    dataset, version = uploads._register_dataset_version(db, upload)
    assert dataset.latest_version_number == 3
    assert version.object_store_type == ObjectStoreType.S3_COMPATIBLE
    assert version.status == DatasetStatus.UPLOADED

    driver = Driver()
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    expired = _upload(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    quarantined = _upload(
        status="quarantined",
        completed_object_uri="s3c://bucket/bad.csv",
        quarantine_delete_after=datetime.now(UTC) - timedelta(seconds=1),
    )
    driver.present.add(quarantined.completed_object_uri)
    cleanup_db = Session()
    cleanup_db.row_values = [[expired, quarantined]]
    result = uploads.cleanup_abandoned_uploads(cleanup_db, settings=_settings())
    assert result == {"expired": 1, "completed_deleted": 0, "quarantine_deleted": 1}
    assert driver.aborted == 1 and driver.deleted == ["s3c://bucket/bad.csv"]

    ready = _upload(status="ready")
    with pytest.raises(UploadContractError, match="cannot be aborted"):
        uploads.abort_upload_session(ready, settings=_settings())
    foreign = _upload(provider_driver="gcs")
    with pytest.raises(RuntimeError, match="does not match"):
        uploads.query_upload_progress(foreign, settings=_settings())

    lookup_db = Session([None])
    monkeypatch.setattr(uploads, "require_project_role", lambda *_args: None)
    with pytest.raises(HTTPException) as missing:
        uploads.get_upload_session(lookup_db, SimpleNamespace(id=user_id), project_id, uuid.uuid4())
    assert missing.value.status_code == 404

    locked_upload = _upload(project_id=project_id)
    lookup_db = Session([locked_upload])
    assert (
        uploads.get_upload_session(
            lookup_db,
            SimpleNamespace(id=user_id),
            project_id,
            locked_upload.id,
            for_update=True,
        )
        is locked_upload
    )
    assert "FOR UPDATE" in str(lookup_db.scalar_statements[-1])


def test_progress_completion_and_cleanup_failure_branches(monkeypatch) -> None:
    driver = Driver()
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    settings = _settings()

    terminal = _upload(status="quarantined", confirmed_bytes=3)
    result = uploads.query_upload_progress(terminal, settings=settings)
    assert result.status == "quarantined" and not result.complete

    active = _upload(status="initiated")
    driver.progress = UploadProgress(6, 10, (TransferReceipt(1, 0, 6, "e1"),), False)
    result = uploads.query_upload_progress(active, settings=settings)
    assert result.confirmed_bytes == 6 and active.status == "initiated"
    with pytest.raises(UploadContractError, match="not confirmed"):
        uploads.complete_upload_session(
            Session(), active, sha256="a" * 64, receipts=[], settings=settings
        )

    driver.progress = UploadProgress(
        10,
        10,
        (TransferReceipt(1, 0, 6, "e1"), TransferReceipt(2, 6, 4, "e2")),
        True,
    )
    complete = uploads.complete_upload_session(
        Session(), active, sha256="a" * 64, receipts=[], settings=settings
    )
    assert complete.session.status == "object_completed"

    recovered = _upload(
        transfer_receipts=[
            uploads.receipt_to_dict(TransferReceipt(1, 0, 6, "e1")),
            uploads.receipt_to_dict(TransferReceipt(2, 6, 4, "e2")),
        ]
    )
    recovered_uri = driver.uri_for_key(recovered.object_key)
    driver.present.add(recovered_uri)
    completed_before_recovery = driver.completed
    result = uploads.complete_upload_session(
        Session(), recovered, sha256="a" * 64, receipts=[], settings=settings
    )
    assert result.session.status == "object_completed"
    assert recovered.completed_object_uri == recovered_uri
    assert driver.completed == completed_before_recovery

    wrong_size = _upload()
    driver.present.add(driver.uri_for_key(wrong_size.object_key))
    monkeypatch.setattr(
        driver,
        "stat",
        lambda uri: ObjectMetadata(uri=uri, byte_size=9),
    )
    with pytest.raises(UploadContractError, match="recovered provider object"):
        uploads.complete_upload_session(
            Session(), wrong_size, sha256="a" * 64, receipts=[], settings=settings
        )

    all_parts = _upload(
        confirmed_bytes=9,
        transfer_receipts=[{"unit_number": 1}, {"unit_number": 2}],
    )
    assert uploads.next_cursor(all_parts) is None

    missing_object = _upload(
        status="quarantined",
        completed_object_uri="s3c://bucket/already-gone.csv",
        quarantine_delete_after=datetime.now(UTC) - timedelta(seconds=1),
    )
    cleanup_db = Session()
    cleanup_db.row_values = [[missing_object]]
    assert uploads.cleanup_abandoned_uploads(cleanup_db, settings=settings) == {
        "expired": 0,
        "completed_deleted": 0,
        "quarantine_deleted": 1,
    }
    assert driver.deleted == []

    cleanup_db.row_values = [[_upload(provider_driver="gcs")]]
    with pytest.raises(RuntimeError, match="Cleanup driver"):
        uploads.cleanup_abandoned_uploads(cleanup_db, settings=settings)


def test_cleanup_deletes_stale_completed_objects_but_preserves_legal_holds(
    monkeypatch,
) -> None:
    driver = Driver()
    monkeypatch.setattr(uploads, "get_object_store", lambda *_args: driver)
    version_id = uuid.uuid4()
    version = SimpleNamespace(status=DatasetStatus.UPLOADED)
    expired_at = datetime.now(UTC) - timedelta(seconds=1)
    stale = _upload(
        status="verifying",
        expires_at=expired_at,
        lease_expires_at=expired_at,
        completed_object_uri="s3c://bucket/stale.csv",
        dataset_version_id=version_id,
    )
    held = _upload(
        status="object_completed",
        expires_at=expired_at,
        lease_expires_at=None,
        legal_hold=True,
        completed_object_uri="s3c://bucket/held.csv",
    )
    driver.present.update({stale.completed_object_uri, held.completed_object_uri})
    db = Session()
    db.row_values = [[stale, held]]
    db.objects[(uploads.DatasetVersion, version_id)] = version

    assert uploads.cleanup_abandoned_uploads(db, settings=_settings()) == {
        "expired": 0,
        "completed_deleted": 1,
        "quarantine_deleted": 0,
    }
    assert stale.status == "failed"
    assert version.status == DatasetStatus.FAILED
    assert stale.completed_object_uri not in driver.present
    assert held.status == "object_completed"
    assert held.completed_object_uri in driver.present


def test_upload_result_route_returns_fresh_authorized_completion(monkeypatch) -> None:
    upload = _upload(status="ready")
    expected = object()
    db = Session()
    user = SimpleNamespace(id=upload.created_by_id)
    monkeypatch.setattr(dataset_routes, "get_upload_session", lambda *_args: upload)
    monkeypatch.setattr(dataset_routes, "completion_read", lambda *_args: expected)

    assert dataset_routes.upload_result(upload.project_id, upload.id, db, user) is expected


def test_legacy_api_upload_reads_only_the_governed_limit(monkeypatch) -> None:
    class TrackingStream(BytesIO):
        read_sizes: list[int]

        def __init__(self, value: bytes) -> None:
            super().__init__(value)
            self.read_sizes = []

        def read(self, size: int = -1) -> bytes:
            self.read_sizes.append(size)
            return super().read(size)

    stream = TrackingStream(b"x" * 11)
    file = UploadFile(file=stream, filename="records.csv", size=None)
    monkeypatch.setattr(
        dataset_routes,
        "get_settings",
        lambda: _settings(environment="production", buffered_upload_max_bytes=10),
    )

    with pytest.raises(HTTPException) as denied:
        dataset_routes.upload_dataset(
            uuid.uuid4(), file, "Records", Session(), SimpleNamespace(), "request-1"
        )

    assert denied.value.status_code == 422
    assert stream.read_sizes == [11]
    assert stream.closed


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("data.csv", "csv"),
        ("data.parquet", "parquet"),
        ("data.xlsx", "excel"),
        ("data.xls", "excel"),
        ("data.ndjson", "json"),
    ],
)
def test_dataset_format_mapping(filename: str, expected: str) -> None:
    assert uploads._dataset_format(filename).value == expected


def test_registration_creates_dataset_when_name_is_new() -> None:
    project_id, user_id = uuid.uuid4(), uuid.uuid4()
    db = Session([project_id, None])
    upload = _upload(
        project_id=project_id,
        created_by_id=user_id,
        upload_kind="dataset",
        original_filename="records.parquet",
        completed_object_uri="s3c://bucket/records.parquet",
    )
    dataset, version = uploads._register_dataset_version(db, upload)
    assert dataset in db.added
    assert dataset.latest_version_number == 1
    assert version.format.value == "parquet"


def test_contract_value_guards() -> None:
    from automl_api.storage.contracts import ByteRange, RayDataSourceDescriptor

    with pytest.raises(ValueError, match="ordered"):
        ByteRange(4, 2)
    assert tuple(RayDataSourceDescriptor("path", {"a": 1}, "gcs")) == ("path", {"a": 1})
