"""Version-pinned upload inspection under original source and delivery fences."""

import copy
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from automl_api.models.enums import CommandStatus, DatasetStatus, OutboxStatus, ProjectStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import OutboxEntry
from automl_api.schemas.datasets import UploadCompletionRead
from automl_api.services import reconciler, upload_completion, uploads
from automl_api.services.upload_policy import inspect_and_scan_stream
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    complete_outbox,
    transition_command,
)
from automl_api.storage.contracts import UploadContractError
from automl_api.storage.object_store import get_object_store

TOPIC = upload_completion.TOPIC
RECORD = uploads.UPLOAD_VERIFICATION_RECORD


def _policy(settings):
    return canonical_request_hash(
        {
            key: getattr(settings, key)
            for key in (
                "environment",
                "upload_scanner_command",
                "upload_scanner_version",
                "upload_scanner_signature_version",
                "upload_scanner_timeout_seconds",
                "upload_max_decompressed_bytes",
                "upload_max_decompression_ratio",
            )
        }
    )


def _select(db, entry_id, worker_id, policy):
    project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
    if project_id is None:
        return None
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if (
        project is None
        or project.status != ProjectStatus.ACTIVE
        or project.settings.get("deletion_in_progress")
    ):
        raise StaleFence("Upload verification is unavailable during project deletion")
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    return _custody(db, entry, policy)


def _custody(db, entry, policy, *, for_update=True):
    upload = db.get(
        DatasetUploadSession,
        uuid.UUID(entry.payload["session_id"]),
        with_for_update=True if for_update else {"read": True},
        populate_existing=True,
    )
    record, command = upload_completion.journal(db, upload)
    if (
        record is None
        or record["phase"] != "published"
        or entry.topic != TOPIC
        or entry.command_id != command.id
        or entry.project_id != upload.project_id
        or entry.aggregate_type != "dataset_upload_session"
        or entry.aggregate_id != upload.id
        or entry.payload.get("intent_id") != record["intent"]["id"]
        or not upload_completion.same_source(
            upload_completion._source(upload), record["intent"]["source"]
        )
        or upload.completed_object_uri != record["receipt"]["uri"]
        or command.status not in {CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
    ):
        raise StaleFence("Upload verification lost its original completion custody")
    uploads.require_upload_byte_digest(upload)
    version = None
    version_source = None
    if upload.dataset_version_id is not None:
        version = db.get(
            DatasetVersion,
            upload.dataset_version_id,
            with_for_update=True if for_update else {"read": True},
            populate_existing=True,
        )
        if (
            version is None
            or version.project_id != upload.project_id
            or version.dataset_id != upload.dataset_id
            or version.object_uri != upload.completed_object_uri
            or version.byte_size != upload.byte_size
            or version.content_hash != upload.expected_object_digest
            or version.content_hash_algorithm != "sha256"
            or version.content_hash_scope != "byte_stream"
        ):
            raise StaleFence("Upload dataset version differs from its original completion")
        version_source = jsonable_encoder(
            {
                name: getattr(version, name)
                for name in (
                    "id",
                    "project_id",
                    "dataset_id",
                    "created_by_id",
                    "object_uri",
                    "original_filename",
                    "byte_size",
                    "content_hash",
                    "content_hash_algorithm",
                    "content_hash_scope",
                    "source_kind",
                    "object_store_type",
                    "format",
                    "retention_until",
                    "legal_hold",
                    "sensitivity",
                    "data_region",
                )
            }
        )
        original_version = UploadCompletionRead.model_validate(command.response_payload).version
        published = jsonable_encoder(
            {name: getattr(original_version, name) for name in version_source}
        )
        if any(
            published.get(name) != value
            for name, value in version_source.items()
            if name not in {"legal_hold", "retention_until"}
        ):
            raise StaleFence("Upload dataset version changed since its original publication")
        for name in ("legal_hold", "retention_until"):
            version_source[name] = published[name]
    elif upload.upload_kind in {"dataset", "validation", "drift"}:
        raise StaleFence("Upload verification has no original dataset version")
    scope = dict(
        receipt=copy.deepcopy(record["receipt"]),
        source=copy.deepcopy(record["intent"]["source"]),
        version=version_source,
        policy_digest=policy,
        command_id=str(command.id),
        outbox_id=str(entry.id),
    )
    return entry, command, upload, version, scope


def _scope_digest(scope):
    return canonical_request_hash(scope)


def _proof(upload, entry, scope):
    proof = upload.provider_state.get(RECORD)
    if (
        not isinstance(proof, dict)
        or entry.payload.get("verification_digest") != canonical_request_hash(proof)
        or proof.get("scope_digest") != _scope_digest(scope)
        or proof.get("protocol") != TOPIC
        or proof.get("object_version") != scope["receipt"]["object_version"]
    ):
        raise StaleFence("Original upload verification proof is missing or changed")
    return proof


@dataclass(frozen=True)
class VerifiedObject:
    uri: str
    version: str
    driver: str
    filename: str
    content_type: str


def verified_object(db, upload, settings):
    """Select the original delivered proof; the caller commits before any object I/O."""
    project = db.get(
        Project, upload.project_id, with_for_update={"read": True}, populate_existing=True
    )
    if (
        project is None
        or project.status != ProjectStatus.ACTIVE
        or project.settings.get("deletion_in_progress")
    ):
        raise StaleFence("Verified upload is unavailable during project deletion")
    record, command = upload_completion.journal(db, upload)
    if record is None or record["phase"] != "published":
        raise UploadContractError("Upload has no original completion proof")
    entry = db.scalar(
        select(OutboxEntry)
        .where(
            OutboxEntry.project_id == upload.project_id,
            OutboxEntry.event_key == f"{command.operation}:{command.id}:{TOPIC}",
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if entry is None or entry.status != OutboxStatus.DELIVERED:
        raise StaleFence("Upload has no delivered original verification")
    _, command, current, _, scope = _custody(db, entry, _policy(settings), for_update=False)
    from automl_api.services.upload_admission import require_store

    require_store(settings, current.provider_state, current.provider_driver)
    uploads.require_verified_upload_digest(current)
    proof = _proof(current, entry, scope)
    inspection = proof.get("inspection")
    if (
        command.status != CommandStatus.SUCCEEDED
        or proof.get("error") is not None
        or proof.get("receipt_digest") != canonical_request_hash(scope["receipt"])
        or not isinstance(inspection, dict)
        or inspection.get("sha256") != current.expected_object_digest
        or inspection.get("byte_size") != current.byte_size
        or inspection.get("scanner_status") != "allowed"
        or datetime.fromisoformat(proof["verified_at"]) != current.checksum_verified_at
    ):
        raise StaleFence("Upload has no original successful verification")
    return VerifiedObject(
        scope["receipt"]["uri"],
        scope["receipt"]["object_version"],
        scope["source"]["provider_driver"],
        scope["source"]["original_filename"],
        scope["source"]["content_type"].partition(";")[0].strip().lower(),
    )


def reconcile_upload_verification(factory, entry_id, worker_id):
    attempt = None
    try:
        settings = get_settings()
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id, _policy(settings))
            if selected is None:
                return
            entry, command, upload, version, scope = selected
            if upload.status in {"ready", "quarantined"}:
                _proof(upload, entry, scope)
                complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
                return
            if upload.status not in {"object_completed", "verifying"}:
                raise StaleFence("Upload is not eligible for original verification")
            attempt = entry.delivery_attempts
            lease_seconds = max(60, settings.upload_scanner_timeout_seconds + 120)
            now = datetime.now(UTC)
            upload.status = "verifying"
            upload.lease_owner = worker_id
            upload.lease_expires_at = now + timedelta(seconds=lease_seconds)
            upload.heartbeat_at = now
            entry.lease_expires_at = upload.lease_expires_at
            last_heartbeat = now
        receipt = scope["receipt"]
        from automl_api.services.upload_admission import require_store

        require_store(
            settings, scope["source"]["provider_state"], scope["source"]["provider_driver"]
        )
        store = get_object_store(settings)
        if store.driver_name != scope["source"]["provider_driver"]:
            raise StaleFence("Upload verification provider changed")
        metadata = store.stat(receipt["uri"], version=receipt["object_version"])
        key = "generation" if store.driver_name == "gcs" else "version_id"
        if (
            metadata.uri != receipt["uri"]
            or type(metadata.byte_size) is not int
            or metadata.provider_headers.get(key) != receipt["object_version"]
        ):
            raise StaleFence("Upload metadata has another native identity")
        inspection, error = None, None
        failure_status = "denied"
        if metadata.byte_size != receipt["byte_size"]:
            failure_status = "failed"
            error = (
                f"object byte size mismatch: expected {receipt['byte_size']}, "
                f"observed {metadata.byte_size}"
            )
        else:

            def heartbeat(processed):
                nonlocal last_heartbeat
                now = datetime.now(UTC)
                if processed < receipt["byte_size"] and (now - last_heartbeat).total_seconds() < 30:
                    return
                with factory() as db, db.begin():
                    current = _select(db, entry_id, worker_id, _policy(get_settings()))
                    if current is None:
                        raise StaleFence("Upload verification lost its delivery")
                    e, _, u, _, current_scope = current
                    if (
                        e.delivery_attempts != attempt
                        or _scope_digest(current_scope) != _scope_digest(scope)
                        or u.status != "verifying"
                        or u.lease_owner != worker_id
                    ):
                        raise StaleFence("Upload verification source or lease changed")
                    u.heartbeat_at = now
                    u.lease_expires_at = now + timedelta(seconds=lease_seconds)
                    e.lease_expires_at = u.lease_expires_at
                last_heartbeat = now

            reader = reconciler._BoundedObjectReader(
                store,
                receipt["uri"],
                receipt["byte_size"],
                version=receipt["object_version"],
                range_size=reconciler.UPLOAD_VERIFICATION_RANGE_BYTES,
            )
            try:
                inspection = inspect_and_scan_stream(
                    reader,
                    filename=scope["source"]["original_filename"],
                    expected_size=receipt["byte_size"],
                    settings=settings,
                    on_progress=heartbeat,
                )
            except StaleFence:
                raise
            except (TimeoutError, ValueError) as exc:
                error = redact_text(str(exc))
                failure_status = "failed" if isinstance(exc, TimeoutError) else "denied"
            finally:
                reader.close()
            if (
                inspection is not None
                and inspection.sha256 != scope["source"]["expected_object_digest"]
            ):
                error = "object SHA-256 differs from the original client manifest"
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id, _policy(get_settings()))
            if selected is None:
                return
            entry, command, upload, version, current_scope = selected
            if (
                entry.delivery_attempts != attempt
                or _scope_digest(current_scope) != _scope_digest(scope)
                or upload.status != "verifying"
                or upload.lease_owner != worker_id
            ):
                raise StaleFence("Upload verification source or lease changed before publication")
            now = datetime.now(UTC)
            proof = dict(
                protocol=TOPIC,
                scope_digest=_scope_digest(scope),
                object_version=receipt["object_version"],
                receipt_digest=canonical_request_hash(receipt),
                verified_at=now.isoformat(),
                inspection=jsonable_encoder(asdict(inspection)) if inspection else None,
                error=error,
            )
            if error is not None:
                upload.scanner_status = failure_status
                reconciler._quarantine_upload(upload, error, version)
            else:
                if inspection is None:
                    raise RuntimeError("Upload verification has no terminal inspection")
                upload.status = "ready"
                upload.observed_object_digest = inspection.sha256
                upload.checksum_verified_at = now
                upload.scanner_status = inspection.scanner_status
                upload.scanner_name = inspection.scanner_name
                upload.scanner_version = inspection.scanner_version
                upload.scanner_signature_version = inspection.signature_version
                upload.scanner_evidence = inspection.evidence
                upload.terminal_reason = None
                upload.lease_owner = None
                upload.lease_expires_at = None
                upload.heartbeat_at = now
                if version is not None:
                    version.status = DatasetStatus.READY
                    version.content_hash_verification_status = "verified"
                    version.content_hash_verified_at = now
                    if inspection.schema_columns and not version.schema_json.get("columns"):
                        version.column_count = len(inspection.schema_columns)
                        version.schema_json = {
                            "columns": [{"name": n} for n in inspection.schema_columns],
                            "source": "verified_upload_stream",
                        }
                        version.inferred_types_json = {
                            n: {"semantic_type": "pending", "nullable": None}
                            for n in inspection.schema_columns
                        }
                    version.quality_report_json = {
                        **version.quality_report_json,
                        "native_upload_custody": dict(
                            upload_session_id=str(upload.id),
                            outbox_id=str(entry.id),
                            object_version=receipt["object_version"],
                            uri=receipt["uri"],
                            sha256=inspection.sha256,
                            byte_size=receipt["byte_size"],
                        ),
                    }
            upload.provider_state = {**upload.provider_state, RECORD: proof}
            entry.payload = {**entry.payload, "verification_digest": canonical_request_hash(proof)}
            # Delivery refreshes the row; persist its proof anchor in this transaction first.
            db.flush()
            complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
            if command.status == CommandStatus.RUNNING:
                transition_command(command, CommandStatus.SUCCEEDED)
    except StaleFence:
        raise
    except Exception as exc:
        with factory() as db, db.begin():
            entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
            if entry is not None and (attempt is None or entry.delivery_attempts == attempt):
                complete_outbox(
                    db, entry.id, worker_id=worker_id, delivered=False, error=redact_text(str(exc))
                )
        raise
