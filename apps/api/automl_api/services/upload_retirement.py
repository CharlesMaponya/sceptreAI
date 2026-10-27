"""Original upload cancellation: durable intent, native work, fresh publication."""

import copy
import logging
import uuid
from datetime import UTC, datetime, timedelta

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, or_, select

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from automl_api.models.enums import (
    CommandStatus,
    DatasetStatus,
    OutboxStatus,
    ProjectRole,
    ProjectStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import AbortUploadRead
from automl_api.services import upload_completion as completion
from automl_api.services import uploads
from automl_api.services.projects import require_project_role
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    StaleFence,
    begin_command,
    transition_command,
)
from automl_api.services.workflow_state import (
    canonical_request_hash as digest,
)
from automl_api.storage.contracts import DeletionPending, UploadContractError
from automl_api.storage.object_store import get_object_store

PROTOCOL = uploads.UPLOAD_RETIREMENT_PROTOCOL
RECORD = uploads.UPLOAD_RETIREMENT_RECORD
STARTED = uploads.UPLOAD_RETIREMENT_STARTED
LOGGER = logging.getLogger(__name__)


def _rows(db, project_id, upload_id, actor_id, token_version, purpose):
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if project is None:
        raise HTTPException(404, "Project not found.")
    if project.status != ProjectStatus.ACTIVE or project.settings.get("deletion_in_progress"):
        raise StaleFence("Upload retirement is unavailable during project deletion.")
    if purpose == "abort":
        actor = db.get(User, actor_id, with_for_update={"read": True}, populate_existing=True)
        if actor is None or not actor.is_active or actor.token_version != token_version:
            raise HTTPException(401, "Current authentication is required.")
        db.scalar(
            select(ProjectMembership)
            .where(
                ProjectMembership.project_id == project_id, ProjectMembership.user_id == actor_id
            )
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        require_project_role(db, actor, project_id, ProjectRole.EDITOR)
    elif purpose not in {"expire", "quarantine"}:
        raise UploadContractError("Unknown upload retirement purpose.")
    upload = db.get(DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True)
    if upload is None or upload.project_id != project_id:
        raise HTTPException(404, "Upload session not found.")
    return upload


def _source(db, upload):
    if upload.provider_state.get("completion_protocol") != completion.PROTOCOL:
        raise UploadContractError("Unknown upload admission requires controlled recovery.")
    from automl_api.services.upload_admission import require_published

    require_published(db, upload)
    uploads.require_upload_byte_digest(upload)
    record, owner = completion.journal(db, upload)
    receipt = record.get("receipt") if record else None
    if record is not None and record["phase"] not in {"admitted", "recorded", "published"}:
        raise UploadContractError(
            "Original native completion outcome must resolve before retirement."
        )
    if record is None and (
        uploads.UPLOAD_COMPLETION_STARTED in upload.provider_state
        or upload.completed_object_uri
        or upload.status in {"object_completed", "verifying", "quarantined", "ready", "completed"}
        or db.scalar(
            select(WorkflowCommand.id)
            .where(
                WorkflowCommand.project_id == upload.project_id,
                WorkflowCommand.operation == completion.PROTOCOL,
                WorkflowCommand.resource_id == upload.id,
                WorkflowCommand.status != CommandStatus.PENDING,
            )
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError(
            "Original completion history is missing; no native target was inferred."
        )
    source = completion._source(upload)
    if record and not completion.same_source(source, record["intent"]["source"]):
        raise UploadContractError("Upload differs from its original completion source.")
    uri = (
        receipt["uri"]
        if receipt
        else record["intent"]["uri"]
        if record
        else source["provider_state"].get("upload_object_uri")
    )
    if not isinstance(uri, str) or not uri:
        raise UploadContractError("Original upload store identity is missing.")
    if not upload.object_key.startswith(f"projects/{upload.project_id}/raw/{upload.id}/"):
        raise UploadContractError("Upload key does not belong to its original session.")
    version = None
    if upload.dataset_version_id:
        row = db.get(
            DatasetVersion, upload.dataset_version_id, with_for_update=True, populate_existing=True
        )
        if (
            row is None
            or row.project_id != upload.project_id
            or row.dataset_id != upload.dataset_id
        ):
            raise UploadContractError("Original dataset version is missing.")
        if (
            row.object_uri != uri
            or row.byte_size != upload.byte_size
            or row.content_hash != upload.expected_object_digest
        ):
            raise UploadContractError("Dataset version differs from original upload custody.")
        version = jsonable_encoder(
            {
                name: getattr(row, name)
                for name in (
                    "id",
                    "project_id",
                    "dataset_id",
                    "object_uri",
                    "byte_size",
                    "content_hash",
                    "content_hash_algorithm",
                    "content_hash_scope",
                    "format",
                )
            }
        )
    return dict(
        upload=source,
        uri=uri,
        receipt=copy.deepcopy(receipt),
        completion_command=str(owner.id) if owner else None,
        version=version,
        completed_uri=upload.completed_object_uri,
        quarantine_delete_after=jsonable_encoder(upload.quarantine_delete_after),
    )


def _eligible(db, upload, purpose, now, original_status):
    if uploads._upload_retained(upload, now):
        raise UploadContractError("Upload retention or legal hold prevents retirement.")
    if upload.provider_driver == "azure_blob" and upload.expires_at > now:
        raise UploadContractError(
            "Azure transfer credentials must expire before version retirement."
        )
    if original_status == "ready":
        raise UploadContractError("A ready immutable upload cannot be aborted or expired.")
    if original_status == "quarantined" and (
        upload.quarantine_delete_after is None or upload.quarantine_delete_after > now
    ):
        raise UploadContractError("Quarantine retention prevents retirement.")
    if purpose == "expire" and upload.expires_at > now:
        raise UploadContractError("Upload has not expired.")
    if purpose == "quarantine" and original_status != "quarantined":
        raise UploadContractError("Upload is not an original quarantined input.")
    if upload.dataset_version_id:
        version = db.get(
            DatasetVersion, upload.dataset_version_id, with_for_update=True, populate_existing=True
        )
        if (
            version is None
            or version.status == DatasetStatus.READY
            or version.legal_hold
            or version.retention_until is not None
            and version.retention_until > now
            or db.scalar(
                select(ModelRun.id).where(ModelRun.dataset_version_id == version.id).limit(1)
            )
            is not None
        ):
            raise UploadContractError(
                "Dataset readiness, references or retention prevents retirement."
            )


def _journal(db, upload):
    value = copy.deepcopy(upload.provider_state.get(RECORD))
    if value is None:
        if (
            STARTED in upload.provider_state
            or db.scalar(
                select(WorkflowCommand.id)
                .where(
                    WorkflowCommand.project_id == upload.project_id,
                    WorkflowCommand.operation == PROTOCOL,
                    WorkflowCommand.resource_id == upload.id,
                    WorkflowCommand.status != CommandStatus.PENDING,
                )
                .limit(1)
            )
            is not None
        ):
            raise UploadContractError(
                "Original retirement history is missing; no restart was inferred."
            )
        return None, None
    try:
        intent = value["intent"]
        command = db.get(WorkflowCommand, uuid.UUID(intent["command_id"]), populate_existing=True)
        if (
            intent["protocol"] != PROTOCOL
            or command is None
            or command.project_id != upload.project_id
            or command.resource_id != upload.id
            or command.resource_type != PROTOCOL
            or command.operation != PROTOCOL
            or command.request_hash != digest(command.request_payload)
            or command.request_payload["source_digest"] != digest(intent["source"])
            or command.request_payload["session_id"] != str(upload.id)
            or command.request_payload["purpose"] != intent["purpose"]
            or command.request_payload["actor_id"] != intent["actor_id"]
            or command.status
            != (CommandStatus.SUCCEEDED if value["phase"] == "published" else CommandStatus.RUNNING)
            or command.actor_id != uuid.UUID(intent["actor_id"])
            or command.response_payload.get("retirement_intent_digest") != digest(intent)
            or value["phase"]
            not in {"admitted", "issued", "unknown", "waiting", "observed", "published"}
            or STARTED in upload.provider_state
            and value["phase"] == "admitted"
        ):
            raise UploadContractError("Original upload retirement custody changed.")
        if value["phase"] != "admitted" and upload.provider_state.get(STARTED) is not True:
            raise UploadContractError("Original native retirement start is missing.")
        if value["phase"] == "admitted" and any(
            k in value for k in ("attempt", "target", "receipt")
        ):
            raise UploadContractError("Original native retirement cannot return to admission.")
        if value.get("attempt") and command.response_payload.get(
            "retirement_attempt_digest"
        ) != digest(value["attempt"]):
            raise UploadContractError("Original retirement attempt changed.")
        if value.get("target") and command.response_payload.get(
            "retirement_target_digest"
        ) != digest(value["target"]):
            raise UploadContractError("Original retirement target changed.")
        if value["phase"] in {"observed", "published"} and (
            command.response_payload.get("retirement_receipt_digest") != digest(value["receipt"])
            or value["receipt"]["intent_digest"] != digest(intent)
            or value["receipt"]["target_digest"] != digest(value["target"])
        ):
            raise UploadContractError("Original native retirement outcome changed.")
        if (
            value["phase"] == "published"
            and command.response_payload.get("result") != value["result"]
        ):
            raise UploadContractError("Original retirement response changed.")
        return value, command
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise UploadContractError(
            "Original upload retirement custody could not be qualified."
        ) from exc


def _save(upload, record):
    upload.provider_state = {**upload.provider_state, RECORD: copy.deepcopy(record)}


def _fresh(db, upload, record, purpose):
    expected_status = (
        "aborted"
        if purpose == "abort"
        else "expired"
        if record.get("target", {}).get("kind") == "staged"
        else "failed"
    )
    if (
        record["phase"] == "published"
        and upload.status != expected_status
        or record["phase"] != "published"
        and upload.status != "retiring"
    ):
        raise StaleFence("Upload retirement status changed.")
    current, original = _source(db, upload), copy.deepcopy(record["intent"]["source"])
    for source in (current, original):
        for key in ("legal_hold", "retention_until"):
            source["upload"].pop(key, None)
            if source["version"]:
                source["version"].pop(key, None)
    if record["intent"]["purpose"] != purpose or current != original:
        raise StaleFence("Original retirement source or purpose changed.")
    _eligible(db, upload, purpose, datetime.now(UTC), record["intent"]["initial_status"])


def retire_upload(
    factory,
    project_id,
    upload_id,
    key,
    *,
    actor_id=None,
    token_version=None,
    purpose="abort",
    settings=None,
):
    from automl_api.services.upload_admission import require_store

    settings = settings or get_settings()
    with factory() as db, db.begin():
        upload = _rows(db, project_id, upload_id, actor_id, token_version, purpose)
        record, owner = _journal(db, upload)
        if record is None and upload.status == "aborted" and upload.aborted_at is not None:
            return AbortUploadRead(
                id=upload.id, status="aborted", aborted_at=upload.aborted_at
            ).model_dump(mode="json")
        if record is None:
            if upload.status not in {
                "initiated",
                "uploading",
                "object_completed",
                "verifying",
                "quarantined",
            }:
                raise UploadContractError("Upload is not eligible for original retirement.")
            _eligible(db, upload, purpose, datetime.now(UTC), upload.status)
            source = _source(db, upload)
            if upload.provider_driver == "azure_blob" and source["receipt"] is None:
                raise UploadContractError("Azure staged blocks have no supported cancellation.")
        else:
            _fresh(db, upload, record, purpose)
            source = record["intent"]["source"]
        require_store(
            settings, source["upload"]["provider_state"], source["upload"]["provider_driver"]
        )
        principal = actor_id if purpose == "abort" else upload.created_by_id
        try:
            command, _ = begin_command(
                db,
                project_id=project_id,
                actor_id=principal,
                operation=PROTOCOL,
                idempotency_key=key,
                payload=dict(
                    session_id=str(upload_id),
                    purpose=purpose,
                    actor_id=str(principal),
                    source_digest=digest(source),
                ),
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                409, "This Idempotency-Key belongs to another original request."
            ) from exc
        command.resource_id, command.resource_type = upload_id, PROTOCOL
        if record and record["phase"] == "published":
            if command.id != owner.id:
                command.response_payload, command.response_status = (
                    copy.deepcopy(owner.response_payload),
                    200,
                )
                if command.status == CommandStatus.PENDING:
                    transition_command(command, CommandStatus.RUNNING)
                    transition_command(command, CommandStatus.SUCCEEDED)
            return copy.deepcopy(record["result"])
        if record and command.id != owner.id:
            raise HTTPException(409, "The original retirement request must resolve first.")
        if record is None:
            intent = dict(
                id=str(uuid.uuid4()),
                command_id=str(command.id),
                protocol=PROTOCOL,
                actor_id=str(principal),
                purpose=purpose,
                source=source,
                initial_status=upload.status,
            )
            record = dict(intent=intent, phase="admitted")
            command.response_payload = dict(retirement_intent_digest=digest(intent))
            transition_command(command, CommandStatus.RUNNING)
            upload.status = "retiring"
            _save(upload, record)
        intent_id = record["intent"]["id"]
        if record["phase"] == "issued" and datetime.fromisoformat(
            record["attempt"]["expires_at"]
        ) > datetime.now(UTC):
            raise HTTPException(409, "Original native retirement is still in progress.")
        if record["phase"] == "waiting" and datetime.fromisoformat(
            record["retry_at"]
        ) > datetime.now(UTC):
            raise HTTPException(
                409, "Original native retirement is waiting for provider retention."
            )
    if record["phase"] != "observed":
        from automl_api.services.upload_admission import require_store

        require_store(
            settings, source["upload"]["provider_state"], source["upload"]["provider_driver"]
        )
        store = get_object_store(settings)
        if (
            store.driver_name != source["upload"]["provider_driver"]
            or store.uri_for_key(source["upload"]["object_key"]) != source["uri"]
        ):
            raise UploadContractError("Retirement store differs from the original upload store.")
        target = record.get("target")
        if target is None:
            target = (
                dict(kind="version", uri=source["uri"], version=source["receipt"]["object_version"])
                if source["receipt"]
                else dict(kind="staged")
            )
            if target["kind"] == "staged" and store.driver_name == "azure_blob":
                raise UploadContractError("Azure staged blocks have no supported cancellation.")
        if target["kind"] == "staged" and store.driver_name == "gcs":
            progress, completed = store._query_progress(
                completion._native_upload(source["upload"]), allow_expired=True
            )
            if progress.complete:
                native_intent = dict(source=source["upload"], uri=source["uri"])
                receipt = completion._receipt(completed, native_intent, [])
                target = dict(
                    kind="version",
                    uri=receipt["uri"],
                    version=receipt["object_version"],
                    original_session_receipt=receipt,
                )
        with factory() as db, db.begin():
            upload = _rows(db, project_id, upload_id, actor_id, token_version, purpose)
            current, command = _journal(db, upload)
            _fresh(db, upload, current, purpose)
            if current["intent"]["id"] != intent_id or current["phase"] == "observed":
                raise StaleFence("Original retirement changed before native start.")
            if current["phase"] == "issued" and datetime.fromisoformat(
                current["attempt"]["expires_at"]
            ) > datetime.now(UTC):
                raise HTTPException(409, "Original native retirement is still in progress.")
            if (
                current.get("target")
                and current["target"] != target
                and not (
                    store.driver_name == "gcs"
                    and current["target"] == {"kind": "staged"}
                    and target.get("kind") == "version"
                    and target.get("original_session_receipt")
                )
            ):
                raise StaleFence("Original retirement target cannot be rebound.")
            attempt = dict(
                id=str(uuid.uuid4()),
                expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            )
            current.update(phase="issued", attempt=attempt, target=target)
            command.response_payload = {
                **command.response_payload,
                "retirement_attempt_digest": digest(attempt),
                "retirement_target_digest": digest(target),
            }
            upload.provider_state = {**upload.provider_state, STARTED: True}
            upload.lease_expires_at = datetime.fromisoformat(attempt["expires_at"])
            _save(upload, current)
        try:
            if target["kind"] == "version":
                store.delete(target["uri"], version=target["version"])
            else:
                store.abort_upload(completion._native_upload(source["upload"]))
        except Exception as exc:
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                upload = db.get(
                    DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
                )
                current, _ = _journal(db, upload)
                if (
                    current["intent"]["id"] == intent_id
                    and current.get("attempt") == attempt
                    and current["phase"] == "issued"
                ):
                    current.update(
                        phase="waiting" if isinstance(exc, DeletionPending) else "unknown",
                        retry_at=(
                            datetime.now(UTC)
                            + timedelta(
                                seconds=exc.retry_after_seconds
                                if isinstance(exc, DeletionPending)
                                else 60
                            )
                        ).isoformat(),
                        error=redact_text(str(exc))[:1000],
                    )
                    upload.lease_expires_at = datetime.fromisoformat(current["retry_at"])
                    _save(upload, current)
            raise
        with factory() as db, db.begin():
            db.get(Project, project_id, with_for_update=True)
            upload = db.get(
                DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
            )
            current, command = _journal(db, upload)
            if (
                current["intent"]["id"] != intent_id
                or current.get("attempt") != attempt
                or current["phase"] != "issued"
            ):
                raise StaleFence("Original retirement lost its native attempt.")
            receipt = dict(
                intent_digest=digest(current["intent"]),
                target_digest=digest(target),
                observed_at=datetime.now(UTC).isoformat(),
            )
            current.update(phase="observed", receipt=receipt)
            command.response_payload = {
                **command.response_payload,
                "retirement_receipt_digest": digest(receipt),
            }
            _save(upload, current)
    with factory() as db, db.begin():
        upload = _rows(db, project_id, upload_id, actor_id, token_version, purpose)
        current, command = _journal(db, upload)
        _fresh(db, upload, current, purpose)
        if current["intent"]["id"] != intent_id or current["phase"] != "observed":
            raise StaleFence("Original retirement has no known native outcome.")
        now = datetime.now(UTC)
        upload.status = (
            "aborted"
            if purpose == "abort"
            else "expired"
            if current["target"]["kind"] == "staged"
            else "failed"
        )
        if purpose == "abort":
            upload.aborted_at = now
            result = AbortUploadRead(id=upload.id, status="aborted", aborted_at=now).model_dump(
                mode="json"
            )
        else:
            result = dict(
                result_class="expired"
                if current["target"]["kind"] == "staged"
                else "quarantine_deleted"
                if purpose == "quarantine"
                else "completed_deleted"
            )
        if upload.dataset_version_id:
            db.get(DatasetVersion, upload.dataset_version_id).status = DatasetStatus.FAILED
        if source["completion_command"]:
            original = db.get(WorkflowCommand, uuid.UUID(source["completion_command"]))
            if original.status == CommandStatus.RUNNING:
                entries = db.scalars(
                    select(OutboxEntry)
                    .where(
                        OutboxEntry.project_id == project_id,
                        OutboxEntry.command_id == original.id,
                        OutboxEntry.topic == completion.TOPIC,
                        OutboxEntry.status.in_({OutboxStatus.PENDING, OutboxStatus.CLAIMED}),
                    )
                    .with_for_update()
                )
                for entry in entries:
                    entry.status, entry.lease_owner, entry.lease_expires_at = (
                        OutboxStatus.DEAD,
                        None,
                        None,
                    )
                    entry.last_error = (
                        "Original upload verification superseded by confirmed retirement"
                    )
                transition_command(original, CommandStatus.FAILED)
        upload.lease_owner, upload.lease_expires_at = None, None
        upload.terminal_reason = "original native upload retirement confirmed"
        current.update(phase="published", result=result)
        command.response_payload = {**command.response_payload, "result": result}
        command.response_status = 200
        transition_command(command, CommandStatus.SUCCEEDED)
        _save(upload, current)
        return copy.deepcopy(result)


def abort_upload_session(
    factory, actor_id, token_version, project_id, upload_id, key, *, settings=None
):
    return AbortUploadRead.model_validate(
        retire_upload(
            factory,
            project_id,
            upload_id,
            key,
            actor_id=actor_id,
            token_version=token_version,
            settings=settings,
        )
    )


def cleanup_abandoned_uploads(factory, *, settings=None, now=None, limit=100):
    observed = now or datetime.now(UTC)
    result = dict(expired=0, completed_deleted=0, quarantine_deleted=0)
    with factory() as db:
        candidates = list(
            db.execute(
                select(DatasetUploadSession.project_id, DatasetUploadSession.id)
                .where(
                    or_(
                        and_(
                            DatasetUploadSession.status.in_(
                                {"initiated", "uploading", "object_completed", "verifying"}
                            ),
                            DatasetUploadSession.expires_at <= observed,
                        ),
                        and_(
                            DatasetUploadSession.status == "quarantined",
                            DatasetUploadSession.quarantine_delete_after <= observed,
                        ),
                        and_(
                            DatasetUploadSession.status == "retiring",
                            DatasetUploadSession.provider_state[RECORD]["intent"]["purpose"]
                            .as_string()
                            .in_({"expire", "quarantine"}),
                        ),
                    ),
                    DatasetUploadSession.provider_state["completion_protocol"].as_string()
                    == completion.PROTOCOL,
                    or_(
                        DatasetUploadSession.provider_state.bool_op("?")(RECORD),
                        DatasetUploadSession.provider_state[completion.RECORD]["receipt"][
                            "object_version"
                        ]
                        .as_string()
                        .is_not(None),
                        and_(
                            DatasetUploadSession.status.in_({"initiated", "uploading"}),
                            ~DatasetUploadSession.provider_state.bool_op("?")(completion.RECORD),
                            ~DatasetUploadSession.provider_state.bool_op("?")(
                                uploads.UPLOAD_COMPLETION_STARTED
                            ),
                            DatasetUploadSession.provider_state["upload_object_uri"]
                            .as_string()
                            .is_not(None),
                            DatasetUploadSession.provider_driver != "azure_blob",
                        ),
                    ),
                    DatasetUploadSession.legal_hold.is_(False),
                    or_(
                        DatasetUploadSession.retention_until.is_(None),
                        DatasetUploadSession.retention_until <= observed,
                    ),
                    or_(
                        DatasetUploadSession.lease_expires_at.is_(None),
                        DatasetUploadSession.lease_expires_at <= observed,
                    ),
                )
                .order_by(DatasetUploadSession.expires_at, DatasetUploadSession.id)
                .limit(limit)
            )
        )
        db.commit()
    for project_id, upload_id in candidates:
        try:
            with factory() as db:
                upload = db.get(DatasetUploadSession, upload_id)
                record, _ = _journal(db, upload)
                purpose = (record["intent"]["purpose"] if record else None) or (
                    "quarantine" if upload.status == "quarantined" else "expire"
                )
                if purpose == "abort":
                    continue
                key = f"cleanup:{upload.id}"
                db.commit()
            outcome = retire_upload(
                factory, project_id, upload_id, key, purpose=purpose, settings=settings
            )
            result[outcome["result_class"]] += 1
        except Exception as exc:
            LOGGER.warning(
                "upload retirement deferred",
                extra={"upload_id": str(upload_id), "error_type": type(exc).__name__},
            )
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                upload = db.get(
                    DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
                )
                if upload is not None and (
                    upload.lease_expires_at is None or upload.lease_expires_at <= observed
                ):
                    upload.lease_expires_at = observed + timedelta(hours=1)
    return result
