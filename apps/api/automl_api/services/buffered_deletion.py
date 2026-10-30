"""SQL-only transfer of original buffered custody into project deletion."""

import copy
import uuid

from sqlalchemy import or_, select

from automl_api.core.config import get_settings
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import CommandStatus, OutboxStatus
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import DatasetUploadResponse
from automl_api.services import buffered_execution as execution
from automl_api.services import buffered_uploads as admission
from automl_api.services.workflow_state import StaleFence
from automl_api.storage.contracts import UploadContractError

CANCELLATION = "Original buffered custody transferred to project deletion."


def _rows(db, project_id):
    missing = (
        ~select(BufferedUploadInput.command_id)
        .where(
            BufferedUploadInput.project_id == WorkflowCommand.project_id,
            BufferedUploadInput.command_id == WorkflowCommand.id,
        )
        .correlate(WorkflowCommand)
        .exists()
    )
    if (
        db.scalar(
            select(WorkflowCommand.id)
            .where(
                WorkflowCommand.project_id == project_id,
                or_(
                    WorkflowCommand.resource_type == admission.PROTOCOL,
                    (WorkflowCommand.operation == admission.OPERATION)
                    & (WorkflowCommand.resource_id == WorkflowCommand.id),
                ),
                missing,
            )
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError("Original buffered command lost its private custody.")
    missing = (
        ~select(BufferedUploadInput.command_id)
        .where(
            BufferedUploadInput.project_id == OutboxEntry.project_id,
            BufferedUploadInput.command_id == OutboxEntry.command_id,
        )
        .correlate(OutboxEntry)
        .exists()
    )
    if (
        db.scalar(
            select(OutboxEntry.id)
            .where(
                OutboxEntry.project_id == project_id,
                OutboxEntry.topic == admission.TOPIC,
                missing,
            )
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError("Original buffered delivery lost its private custody.")
    return list(
        db.scalars(
            select(BufferedUploadInput)
            .where(
                BufferedUploadInput.project_id == project_id,
            )
            .order_by(BufferedUploadInput.command_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )


def _snapshot(db, row, *, transferred, prior=None):
    command = db.get(WorkflowCommand, row.command_id, with_for_update=True, populate_existing=True)
    if command is None or command.project_id != row.project_id:
        raise UploadContractError("Original buffered command disappeared.")
    admission.original_input(db, command)
    if row.phase == "retired":
        from automl_api.services.buffered_retirement import require_retired

        proof = require_retired(db, row)
        execution._backend(get_settings(), row.manifest)
        result = dict(
            input_id=str(row.command_id),
            delivery_id=proof["delivery_id"],
            phase="retired",
            manifest_digest=row.manifest_digest,
            native_receipt=copy.deepcopy(row.native_receipt),
            retirement=proof,
        )
        from automl_api.services.buffered_quarantine import history

        entry = db.get(OutboxEntry, uuid.UUID(proof["delivery_id"]))
        approvals = history(db, row, command, entry)
        if approvals:
            result["producer_replays"] = approvals
        return result
    phase = row.phase
    original_status = {
        "admitted": CommandStatus.PENDING,
        "recorded": CommandStatus.RUNNING,
        "published": CommandStatus.SUCCEEDED,
    }
    if phase not in original_status:
        raise UploadContractError("Unknown or unqualified buffered outcomes prevent deletion.")
    expected_status = (
        CommandStatus.CANCELLED if transferred and phase != "published" else original_status[phase]
    )
    if command.status != expected_status:
        raise UploadContractError("Original buffered command lost its qualified closure.")
    entry = db.scalar(
        select(OutboxEntry)
        .where(
            OutboxEntry.command_id == row.command_id,
            OutboxEntry.project_id == row.project_id,
            OutboxEntry.topic == admission.TOPIC,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    allowed = (
        {OutboxStatus.DELIVERED}
        if phase == "published"
        else {OutboxStatus.DEAD}
        if transferred
        else {OutboxStatus.PENDING, OutboxStatus.CLAIMED}
    )
    if entry is None or entry.status not in allowed:
        if not transferred and entry is not None and entry.status == OutboxStatus.DEAD:
            from automl_api.services.buffered_quarantine import held

            held(row, command, entry)
        else:
            raise UploadContractError("Original buffered delivery is not qualified for transfer.")
    producer_quarantine = (
        prior.get("producer_quarantine")
        if transferred and prior is not None
        else command.terminal_reason
        if not transferred and entry.status == OutboxStatus.DEAD
        else None
    )
    if producer_quarantine is not None:
        from automl_api.services.buffered_quarantine import record

        record(producer_quarantine, row, command, entry)
    if (
        transferred
        and phase != "published"
        and (
            entry.lease_owner is not None
            or entry.lease_expires_at is not None
            or entry.last_error != CANCELLATION
            or command.terminal_reason != CANCELLATION
        )
    ):
        raise UploadContractError("Original buffered cancellation lost its deletion handoff.")
    settings, source = get_settings(), row.manifest
    execution._backend(settings, source)
    version_id = None
    if phase != "admitted":
        uri = execution.original_uri(settings, source)
        execution.validate_receipt(source, row.native_receipt, uri)
    if phase == "published":
        admission._replay(db, command)
        cached = DatasetUploadResponse.model_validate(command.response_payload).version
        version = db.get(
            DatasetVersion, cached.id, with_for_update={"read": True}, populate_existing=True
        )
        if version is None or any(
            (
                version.project_id != row.project_id,
                version.dataset_id != cached.dataset_id,
                version.created_by_id != command.actor_id,
                version.version_number != cached.version_number,
                version.object_uri != row.native_receipt["uri"],
                cached.object_uri != version.object_uri,
                version.object_store_type.value != source["driver"],
                version.byte_size != row.byte_size,
                version.content_hash != row.content_sha256,
                version.original_filename != source["request"]["filename"],
            )
        ):
            raise UploadContractError(
                "Published buffered metadata lost its original object binding."
            )
        version_id = str(version.id)
    elif command.response_status is not None or command.response_payload:
        raise UploadContractError(
            "Unpublished buffered command has a contradictory cached response."
        )
    result = dict(
        input_id=str(row.command_id),
        delivery_id=str(entry.id),
        phase=phase,
        manifest_digest=row.manifest_digest,
        request_hash=command.request_hash,
        content_sha256=row.content_sha256,
        byte_size=row.byte_size,
        native_receipt=copy.deepcopy(row.native_receipt),
        dataset_version_id=version_id,
    )
    if producer_quarantine is not None:
        result["producer_quarantine"] = producer_quarantine
    from automl_api.services.buffered_quarantine import history

    approvals = history(db, row, command, entry)
    if approvals:
        result["producer_replays"] = approvals
    return result


def plan(db, project_id):
    """The caller owns the project lock; planning does not cancel or clear input."""
    return [_snapshot(db, row, transferred=False) for row in _rows(db, project_id)]


def require_transfer(db, project_id, expected, *, transferred=True):
    if not isinstance(expected, list):
        raise UploadContractError("Original buffered deletion inventory is malformed.")
    prior = {item.get("input_id"): item for item in expected if isinstance(item, dict)}
    if len(prior) != len(expected) or None in prior:
        raise UploadContractError("Original buffered deletion inventory is malformed.")
    current = [
        _snapshot(db, row, transferred=transferred, prior=prior.get(str(row.command_id)))
        for row in _rows(db, project_id)
    ]
    if current != expected:
        raise StaleFence("Original buffered deletion inventory changed.")


def unsettled(db, project_id, expected=None, *, transferred=False):
    try:
        if expected is None:
            return bool(_rows(db, project_id))
        require_transfer(db, project_id, expected, transferred=transferred)
    except (UploadContractError, StaleFence, ValueError):
        return True
    return False


def transfer(db, project_id, expected):
    require_transfer(db, project_id, expected, transferred=False)
    for item in expected:
        if item["phase"] not in {"published", "retired"}:
            command = db.get(WorkflowCommand, uuid.UUID(item["input_id"]))
            command.status, command.terminal_reason = CommandStatus.CANCELLED, CANCELLATION
            entry = db.get(OutboxEntry, uuid.UUID(item["delivery_id"]))
            entry.status, entry.last_error = OutboxStatus.DEAD, CANCELLATION
            entry.lease_owner = entry.lease_expires_at = None
    db.flush()
    require_transfer(db, project_id, expected)


def retire(db, project_id, expected):
    """Called only after the deletion factory qualifies every prior native stage."""
    require_transfer(db, project_id, expected)
    for item in expected:
        row = db.get(BufferedUploadInput, uuid.UUID(item["input_id"]))
        row.phase, row.content = "retired", None
    db.flush()
    from automl_api.services.buffered_chunks import clear_parts

    for item in expected:
        clear_parts(db, db.get(BufferedUploadInput, uuid.UUID(item["input_id"])))
