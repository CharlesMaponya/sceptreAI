"""Expire unissued transfer authority; preserve all native uncertainty for cleanup."""

import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select

from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import upload_admission as admission
from automl_api.services import upload_completion, uploads
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.services.workflow_state import transition_command
from automl_api.storage.contracts import UploadContractError

LOGGER = logging.getLogger(__name__)


def _unpublished(db, upload, command, record, *, retired=False):
    phases = {"retired"} if retired else {"issued", "unknown", "recorded"}
    if record is None or record["phase"] not in phases:
        raise UploadContractError("Admission is not an unpublished native request.")
    current, original = admission._source(upload), record["intent"]["source"]
    if not upload_completion.same_source(current, original):
        raise UploadContractError("Unpublished admission source changed.")
    namespace = f"projects/{upload.project_id}/raw/{upload.id}/{record['intent']['id']}/"
    if (
        upload.status != ("expired" if retired else "initiated")
        or upload.multipart_upload_id != ""
        or not upload.object_key.startswith(namespace)
        or upload.completed_object_uri is not None
        or upload.dataset_id is not None
        or upload.dataset_version_id is not None
        or upload.confirmed_bytes != 0
        or upload.transfer_receipts != []
        or upload.instruction_state != {}
        or command.response_status is not None
        or "result" in command.response_payload
        or not retired
        and "admission_cleanup" in upload.provider_state
        or any(
            key in upload.provider_state
            for key in (
                "completion_protocol",
                uploads.UPLOAD_COMPLETION_RECORD,
                uploads.UPLOAD_COMPLETION_STARTED,
                uploads.UPLOAD_VERIFICATION_RECORD,
                uploads.UPLOAD_RETIREMENT_RECORD,
                uploads.UPLOAD_RETIREMENT_STARTED,
            )
        )
        or db.scalar(
            select(WorkflowCommand.id)
            .where(
                WorkflowCommand.project_id == upload.project_id,
                WorkflowCommand.id != command.id,
                or_(
                    WorkflowCommand.resource_id == upload.id,
                    WorkflowCommand.request_payload["session_id"].as_string() == str(upload.id),
                ),
                WorkflowCommand.operation.in_(
                    {
                        "dataset.upload.instruction",
                        uploads.UPLOAD_COMPLETION_PROTOCOL,
                        uploads.UPLOAD_RETIREMENT_PROTOCOL,
                    }
                ),
                WorkflowCommand.status != CommandStatus.PENDING,
            )
            .limit(1)
        )
        is not None
    ):
        raise UploadContractError("Unpublished transfer authority could not be proved.")


def _expire(factory, project_id, upload_id, observed):
    with factory() as db, db.begin():
        db.get(Project, project_id, with_for_update=True)
        upload = db.get(
            DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
        )
        if upload is None or upload.project_id != project_id or upload.status != "initiated":
            return 0
        try:
            command_id = uuid.UUID(upload.provider_state[admission.RECORD]["intent"]["command_id"])
            command = db.get(WorkflowCommand, command_id, populate_existing=True)
            if (
                command is None
                or command.resource_id != upload_id
                or command.project_id != project_id
            ):
                raise UploadContractError("Original admission command is missing.")
            _, record, _ = admission._journal(db, command)
            _unpublished(db, upload, command, record)
            if upload.expires_at <= observed:
                reason = "session_expired"
            elif (
                record["phase"] in {"issued", "unknown"}
                and type(record["attempt"]["number"]) is int
                and record["attempt"]["number"] == 5
                and datetime.fromisoformat(record["attempt"]["expires_at"]) <= observed
            ):
                reason = "native_retry_budget_exhausted"
            else:
                if record["phase"] in {"issued", "unknown"}:
                    upload.lease_expires_at = datetime.fromisoformat(
                        record["attempt"]["expires_at"]
                    )
                return 0
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise UploadContractError(
                "Unpublished admission expiry could not be qualified."
            ) from exc
        retirement = dict(
            reason=reason,
            retired_at=observed.isoformat(),
            intent_digest=digest(record["intent"]),
            attempt_digest=digest(record["attempt"]),
            unpublished=True,
            native_absence_confirmed=False,
        )
        record.update(phase="retired", retirement=retirement)
        command.response_payload = {
            **command.response_payload,
            "admission_retirement_digest": digest(retirement),
        }
        transition_command(command, CommandStatus.FAILED)
        upload.status = "expired"
        upload.lease_owner, upload.lease_expires_at = None, None
        upload.terminal_reason = (
            "Unpublished admission retired; native session cleanup unqualified."
        )
        admission._save(upload, record)
        return 1


def expire_pending_admissions(factory, *, now=None, limit=100):
    observed = now or datetime.now(UTC)
    with factory() as db:
        candidates = list(
            db.execute(
                select(DatasetUploadSession.project_id, DatasetUploadSession.id)
                .where(
                    DatasetUploadSession.status == "initiated",
                    DatasetUploadSession.provider_state[admission.RECORD]["intent"][
                        "protocol"
                    ].as_string()
                    == admission.PROTOCOL,
                    or_(
                        DatasetUploadSession.expires_at <= observed,
                        and_(
                            DatasetUploadSession.provider_state[admission.RECORD]["phase"]
                            .as_string()
                            .in_({"issued", "unknown"}),
                            DatasetUploadSession.provider_state[admission.RECORD]["attempt"][
                                "number"
                            ].as_string()
                            == "5",
                        ),
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
    expired = 0
    for project_id, upload_id in candidates:
        try:
            expired += _expire(factory, project_id, upload_id, observed)
        except Exception as exc:
            # A held corrupt row must not consume every bounded cleanup batch.
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                upload = db.get(
                    DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
                )
                if upload is not None and upload.status == "initiated":
                    upload.lease_expires_at = observed + timedelta(minutes=1)
                    upload.terminal_reason = (
                        "Unpublished admission expiry requires controlled recovery."
                    )
            LOGGER.warning(
                "Unpublished admission expiry deferred", extra={"error_type": type(exc).__name__}
            )
    return expired
