"""Create once, preserve the original reply, then publish verified buffered data."""

import copy
import hashlib
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import CommandStatus, DatasetFormat, DatasetStatus, ObjectStoreType
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import DatasetRead, DatasetUploadResponse, DatasetVersionRead
from automl_api.services import buffered_chunks
from automl_api.services import buffered_uploads as admission
from automl_api.services.reconciler import _lock_outbox_delivery
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    complete_outbox,
    transition_command,
)
from automl_api.storage.contracts import (
    UploadContractError,
    embedded_object_version,
    immutable_object_version,
)
from automl_api.storage.object_store import get_object_store, validate_object_store_settings

TOPIC = admission.TOPIC


def _delivery(entry):
    return canonical_request_hash(
        {column.key: getattr(entry, column.key) for column in entry.__table__.columns}
    )


def _backend(settings, source):
    driver = validate_object_store_settings(settings)
    if (
        driver != source["driver"]
        or admission.store_digest(settings, driver) != source["store_digest"]
    ):
        raise StaleFence("Configured backend differs from original buffered admission.")


def original_uri(settings, source):
    scheme = {
        "embedded": "embedded",
        "aws_s3": "s3",
        "s3_compatible": "s3c",
        "gcs": "gs",
        "azure_blob": "az",
    }[source["driver"]]
    authority = settings.object_store_bucket
    if source["driver"] == "azure_blob":
        authority = f"{settings.azure_storage_account}/{authority}"
    return f"{scheme}://{authority}/{source['object_key']}"


def _select(db, entry_id, worker_id, settings):
    project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
    if project_id is None:
        return None
    db.get(Project, project_id, with_for_update=True, populate_existing=True)
    entry = _lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    if entry.topic != TOPIC:
        raise StaleFence("Buffered execution requires its original delivery topic.")
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    if command is None or command.project_id != project_id:
        raise StaleFence("Original buffered command no longer exists.")
    db.get(BufferedUploadInput, command.id, with_for_update=True, populate_existing=True)
    row = admission.original_input(db, command)
    source = row.manifest
    from automl_api.services import buffered_quarantine

    if (command.terminal_reason or "").startswith(buffered_quarantine.PREFIX):
        raise StaleFence("Original buffered unknown or exhausted work is quarantined.")
    if command.max_retries < 1 or entry.delivery_attempts > command.max_retries:
        raise StaleFence("Original buffered delivery retry budget is exhausted.")
    buffered_quarantine.authorize(db, row, command, entry)
    if (
        db.scalar(
            select(DeletionTombstone.id)
            .where(
                DeletionTombstone.project_id == project_id,
                DeletionTombstone.resource_type == "project",
            )
            .limit(1)
        )
        is not None
    ):
        raise StaleFence("Original project deletion prevents buffered publication.")
    _backend(settings, source)
    expected = {
        "admitted": CommandStatus.PENDING,
        "issued": CommandStatus.RUNNING,
        "recorded": CommandStatus.RUNNING,
        "published": CommandStatus.SUCCEEDED,
    }
    if row.phase not in expected or command.status != expected[row.phase]:
        raise StaleFence("Buffered command no longer matches its original native phase.")
    return entry, command, row


def _record(factory, command_id, source, receipt):
    # Known original outcomes survive revoked authority and superseded delivery leases.
    with factory() as db, db.begin():
        db.get(Project, uuid.UUID(source["project_id"]), with_for_update=True)
        row = db.get(BufferedUploadInput, command_id, with_for_update=True, populate_existing=True)
        if (
            row is None
            or row.manifest != source
            or row.manifest_digest != canonical_request_hash(source)
        ):
            raise StaleFence("Original buffered receipt lost its immutable admission.")
        if row.native_receipt:
            if row.native_receipt != receipt:
                raise StaleFence("Original buffered native outcome cannot be replaced.")
            return
        if row.phase != "issued":
            raise StaleFence("Original buffered write was not issued.")
        row.phase, row.native_receipt = "recorded", receipt
        from automl_api.services import buffered_quarantine

        command = db.get(WorkflowCommand, command_id, with_for_update=True)
        entries = list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.command_id == command_id,
                    OutboxEntry.project_id == row.project_id,
                    OutboxEntry.topic == TOPIC,
                )
            )
        )
        if command is not None and len(entries) == 1:
            try:
                buffered_quarantine.resume_known(db, row, command, entries[0])
            except (ValueError, KeyError, TypeError):
                pass


def _receipt(value, source, uri):
    driver = source["driver"]
    version = None
    if driver == "embedded":
        version = embedded_object_version(value.provider_headers.get("version_id"))
    else:
        version = immutable_object_version(
            value.provider_headers.get("generation" if driver == "gcs" else "version_id")
        )
        if len(version) > 256:
            raise UploadContractError("Original buffered version exceeds its receipt budget.")
    if (
        value.uri != uri
        or type(value.byte_size) is not int
        or value.byte_size != source["request"]["byte_size"]
    ):
        raise UploadContractError("Buffered write returned another object identity.")
    return dict(
        manifest_digest=canonical_request_hash(source),
        driver=driver,
        uri=uri,
        byte_size=value.byte_size,
        object_version=version,
    )


def validate_receipt(source, receipt, uri):
    if (
        receipt.get("manifest_digest") != canonical_request_hash(source)
        or receipt.get("driver") != source["driver"]
        or receipt.get("uri") != uri
        or type(receipt.get("byte_size")) is not int
        or receipt["byte_size"] != source["request"]["byte_size"]
    ):
        raise StaleFence("Original buffered receipt differs from its immutable source.")
    version = receipt.get("object_version")
    if source["driver"] == "embedded":
        if version is not None:
            embedded_object_version(version)
    else:
        immutable_object_version(version)
    return version


def _verify(store, source, receipt, uri):
    version = validate_receipt(source, receipt, uri)
    if source["driver"] == "embedded" and version is None:
        raise UploadContractError("Legacy unversioned local data requires controlled recovery.")
    metadata = store.stat(uri, version=version)
    if (
        metadata.uri != uri
        or type(metadata.byte_size) is not int
        or metadata.byte_size != receipt["byte_size"]
    ):
        raise UploadContractError("Original buffered version metadata changed.")
    if (
        version is not None
        and metadata.provider_headers.get(
            "generation" if source["driver"] == "gcs" else "version_id"
        )
        != version
    ):
        raise UploadContractError("Provider did not read the original buffered version.")
    total, digest = 0, hashlib.sha256()
    with store.open_stream(uri, version=version) as stream:
        while chunk := stream.read(1024 * 1024):
            total += len(chunk)
            if total > receipt["byte_size"]:
                raise UploadContractError("Original buffered byte size changed.")
            digest.update(chunk)
    if total != receipt["byte_size"] or digest.hexdigest() != source["request"]["content_sha256"]:
        raise UploadContractError("Original buffered byte digest changed.")


def _publish(db, command, row):
    source, payload = row.manifest, row.manifest["request"]
    inspection = source["inspection"]
    dataset = db.scalar(
        select(Dataset).where(
            Dataset.project_id == command.project_id, Dataset.name == payload["dataset_name"]
        )
    )
    if dataset is None:
        dataset = Dataset(
            project_id=command.project_id,
            created_by_id=command.actor_id,
            name=payload["dataset_name"],
            description=payload["description"],
            tags=payload["tags"],
        )
        db.add(dataset)
        db.flush()
    else:
        if payload["description"] is not None:
            dataset.description = payload["description"]
        dataset.tags = payload["tags"] or dataset.tags
    version = DatasetVersion(
        project_id=command.project_id,
        dataset_id=dataset.id,
        created_by_id=command.actor_id,
        version_number=dataset.latest_version_number + 1,
        status=DatasetStatus(inspection["status"]),
        format=DatasetFormat(inspection["format"]),
        object_store_type=ObjectStoreType(source["driver"]),
        object_uri=row.native_receipt["uri"],
        original_filename=payload["filename"],
        content_hash=row.content_sha256,
        content_hash_algorithm="sha256",
        content_hash_scope="byte_stream",
        content_hash_verification_status="verified",
        content_hash_verified_at=datetime.now(UTC),
        byte_size=row.byte_size,
        row_count=inspection["row_count"],
        column_count=inspection["column_count"],
        schema_json=inspection["schema_json"],
        inferred_types_json=inspection["inferred_types_json"],
        quality_report_json=inspection["quality_report_json"],
    )
    db.add(version)
    dataset.latest_version_number = version.version_number
    db.flush()
    response = DatasetUploadResponse(
        dataset=DatasetRead.model_validate(dataset),
        version=DatasetVersionRead.model_validate(version),
    )
    command.response_payload, command.response_status = (
        response.model_dump(mode="json", by_alias=True),
        201,
    )
    transition_command(command, CommandStatus.SUCCEEDED)
    row.phase, row.content = "published", None
    buffered_chunks.clear_parts(db, row)


def _hold(factory, entry_id, worker_id, stamp, exc):
    try:
        with factory() as db, db.begin():
            project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
            if project_id is None:
                return
            db.get(Project, project_id, with_for_update=True)
            entry = _lock_outbox_delivery(db, entry_id, worker_id)
            if entry is None or entry.topic != TOPIC or _delivery(entry) != stamp:
                return
            from automl_api.services import buffered_quarantine

            command = db.get(WorkflowCommand, entry.command_id, with_for_update=True)
            row = db.get(BufferedUploadInput, entry.command_id, with_for_update=True)
            if command is not None and row is not None and row.project_id == project_id:
                buffered_quarantine.settle(db, row, command, entry)
            else:
                from automl_api.models.enums import OutboxStatus

                entry.status = OutboxStatus.DEAD
                entry.lease_owner = entry.lease_expires_at = None
                entry.last_error = (
                    "Original buffered history is unavailable; custody was not inferred."
                )
    except StaleFence:
        return


def reconcile_buffered_upload(
    factory, entry_id, worker_id, *, store_factory=get_object_store, settings=None
):
    settings = settings or get_settings()
    stamp = None
    try:
        with factory() as db, db.begin():
            initial = db.get(OutboxEntry, entry_id)
            if initial is None:
                return
            stamp = _delivery(initial)
            selected = _select(db, entry_id, worker_id, settings)
            if selected is None:
                return
            entry, command, row = selected
            source, command_id = copy.deepcopy(row.manifest), command.id
            phase, receipt = row.phase, copy.deepcopy(row.native_receipt)
            if phase == "published":
                admission._replay(db, command)
                complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
                return
            if phase == "issued":
                raise StaleFence("Original buffered outcome is unknown; no write was reissued.")
            parts = buffered_chunks.load_parts(db, row) if phase == "admitted" else None
        body = b"".join(parts) if parts is not None else None
        del parts
        if phase == "admitted" and (
            len(body) != source["request"]["byte_size"]
            or hashlib.sha256(body).hexdigest() != source["request"]["content_sha256"]
        ):
            raise UploadContractError("Private buffered source bytes changed.")
        store = store_factory(settings)
        if store.driver_name != source["driver"]:
            raise StaleFence("Native store differs from original buffered provider.")
        uri = store.uri_for_key(source["object_key"])
        if phase == "admitted":
            with factory() as db, db.begin():
                selected = _select(db, entry_id, worker_id, settings)
                if selected is None:
                    return
                entry, command, row = selected
                if _delivery(entry) != stamp or row.manifest != source or row.phase != "admitted":
                    raise StaleFence("Original buffered write admission changed.")
                row.phase = "issued"
                transition_command(command, CommandStatus.RUNNING)
            value = store.create_bytes(source["object_key"], body)
            receipt = _receipt(value, source, uri)
            _record(factory, command_id, source, receipt)
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id, settings)
            if selected is None:
                return
            entry, command, row = selected
            if (
                _delivery(entry) != stamp
                or row.manifest != source
                or row.phase != "recorded"
                or row.native_receipt != receipt
            ):
                raise StaleFence("Original buffered verification lost its delivery authority.")
        _verify(store, source, receipt, uri)
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id, settings)
            if selected is None:
                return
            entry, command, row = selected
            if (
                _delivery(entry) != stamp
                or row.manifest != source
                or row.phase != "recorded"
                or row.native_receipt != receipt
            ):
                raise StaleFence("Original buffered verification lost publication authority.")
            _publish(db, command, row)
            if entry.lease_expires_at <= datetime.now(UTC):
                raise StaleFence("Buffered publication exceeded its original delivery lease.")
            complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)
    except Exception as exc:
        _hold(factory, entry_id, worker_id, stamp, exc)
        raise
