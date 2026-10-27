"""SQL-only proof that original upload authority cannot outlive project deletion."""

import copy

from sqlalchemy import and_, or_, select

from automl_api.models.enums import CommandStatus
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_admission as admission
from automl_api.services import upload_admission_cleanup as cleanup
from automl_api.services import upload_completion as completion
from automl_api.services import upload_instructions as instructions
from automl_api.services import upload_retirement as retirement
from automl_api.services import uploads
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import UploadContractError

OPERATIONS = {
    admission.OPERATION,
    cleanup.OPERATION,
    cleanup.REPLAY,
    instructions.OPERATION,
    completion.PROTOCOL,
    retirement.PROTOCOL,
}


def _instruction_source(upload, command):
    payload = command.request_payload
    if (
        payload.get("protocol") != instructions.PROTOCOL
        or command.request_hash != digest(payload)
        or payload.get("actor_id") != str(command.actor_id)
        or payload.get("request", {}).get("session_id") != str(upload.id)
        or command.resource_type != instructions.OPERATION
    ):
        raise UploadContractError("Instruction authority requires original source qualification.")
    source = copy.deepcopy(payload["source"])
    # These journals are appended after instructions, not new native upload identities.
    for key in (
        uploads.UPLOAD_COMPLETION_RECORD,
        uploads.UPLOAD_COMPLETION_STARTED,
        uploads.UPLOAD_VERIFICATION_RECORD,
        uploads.UPLOAD_RETIREMENT_RECORD,
        uploads.UPLOAD_RETIREMENT_STARTED,
    ):
        source["provider_state"].pop(key, None)
    if not completion.same_source(completion._source(upload), source):
        raise UploadContractError("Instruction refers to another original upload source.")
    slot = upload.instruction_state.get(str(payload["request"]["cursor"]["unit_number"]), {})
    if slot.get("command_id") == str(command.id):
        instructions._journal(upload, command)
    else:
        # Superseded keys lose their unit pointer; their immutable request still binds the target.
        record = command.response_payload
        if record.get("phase") not in {"issued", "unknown", "recorded", "published"}:
            raise UploadContractError("Superseded instruction history is missing.")
        if record["phase"] in {"recorded", "published"}:
            instructions._receipt(record["receipt"], payload["request"], payload["source"])


def require_retired(db, upload, *, now, settings=None):
    if upload.status not in uploads.TERMINAL_UPLOAD_STATES:
        raise UploadContractError("Upload has no qualified terminal state.")
    scope = (
        WorkflowCommand.project_id == upload.project_id,
        WorkflowCommand.resource_id == upload.id,
        WorkflowCommand.operation.in_(OPERATIONS),
    )
    has_commands = db.scalar(select(WorkflowCommand.id).where(*scope).limit(1)) is not None
    unpublished = (
        db.scalar(
            select(WorkflowCommand.id)
            .where(
                *scope,
                or_(
                    WorkflowCommand.operation.in_({cleanup.OPERATION, cleanup.REPLAY}),
                    and_(
                        WorkflowCommand.operation == admission.OPERATION,
                        WorkflowCommand.status != CommandStatus.SUCCEEDED,
                    ),
                ),
            )
            .limit(1)
        )
        is not None
    )
    state = upload.provider_state
    native_markers = {
        admission.RECORD,
        admission.STARTED,
        cleanup.RECORD,
        uploads.UPLOAD_COMPLETION_RECORD,
        uploads.UPLOAD_COMPLETION_STARTED,
        uploads.UPLOAD_VERIFICATION_RECORD,
        uploads.UPLOAD_RETIREMENT_RECORD,
        uploads.UPLOAD_RETIREMENT_STARTED,
        "completion_protocol",
    }
    if (
        upload.provider_driver == "embedded"
        and not has_commands
        and not upload.instruction_state
        and not native_markers.intersection(state)
        and all(
            value is None or isinstance(value, str) and value.startswith("embedded://")
            for value in (state.get("upload_object_uri"), upload.completed_object_uri)
        )
    ):
        # Embedded storage cannot issue browser-transfer credentials.
        return
    if (
        state.get(admission.RECORD, {}).get("phase") == "retired"
        or cleanup.RECORD in state
        or unpublished
    ):
        cleanup.require_cleanup_complete(db, upload)
        return
    if admission.RECORD not in state or state.get(admission.STARTED) is not True:
        raise UploadContractError("Remote upload requires original admission qualification.")
    admission.require_published(db, upload)
    uploads.require_upload_byte_digest(upload)
    settings = settings or admission.get_settings()
    admission.require_store(settings, state, upload.provider_driver)
    if not upload.object_key.startswith(f"projects/{upload.project_id}/raw/{upload.id}/"):
        raise UploadContractError("Upload does not own its original project namespace.")
    record, _ = retirement._journal(db, upload)
    if record is not None:
        if record["phase"] != "published":
            raise UploadContractError("Original native retirement is still pending.")
        retirement._fresh(db, upload, record, record["intent"]["purpose"])
    else:
        record, _ = completion.journal(db, upload)
        if (
            record is None
            or record["phase"] != "published"
            or not completion.same_source(completion._source(upload), record["intent"]["source"])
            or upload.completed_object_uri != record["receipt"]["uri"]
            or upload.byte_size != record["receipt"]["byte_size"]
        ):
            raise UploadContractError("Original upload has no qualified native closure.")
    if upload.provider_driver == "azure_blob" and upload.expires_at > now:
        raise UploadContractError("Azure upload credentials must expire before project deletion.")
    for command in db.scalars(
        select(WorkflowCommand)
        .where(
            *scope,
            WorkflowCommand.operation == instructions.OPERATION,
        )
        .execution_options(populate_existing=True, yield_per=100)
    ):
        _instruction_source(upload, command)
