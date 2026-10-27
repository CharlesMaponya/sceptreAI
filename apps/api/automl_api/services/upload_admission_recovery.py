"""Resume unpublished admissions using their original authenticated request."""

import copy
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, or_, select

from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import ResumableUploadBeginRequest
from automl_api.services import upload_admission as admission
from automl_api.services.upload_admission_expiry import _unpublished
from automl_api.storage.contracts import UploadContractError

LOGGER = logging.getLogger(__name__)


def _recover(factory, project_id, upload_id, observed):
    with factory() as db, db.begin():
        db.get(Project, project_id, with_for_update=True)
        upload = db.get(DatasetUploadSession, upload_id, with_for_update=True)
        if (
            upload is None
            or upload.project_id != project_id
            or upload.status != "initiated"
            or upload.expires_at <= observed
            or upload.lease_expires_at is not None
            and upload.lease_expires_at > observed
        ):
            return 0
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.resource_id == upload_id,
                WorkflowCommand.operation == admission.OPERATION,
            )
        )
        if command is None:
            raise UploadContractError("Original admission command is missing.")
        _, record, _ = admission._journal(db, command)
        _unpublished(db, upload, command, record)
        token_version = record["attempt"].get("actor_token_version")
        # Legacy attempts need a new authenticated API request, not guessed authority.
        if token_version is None:
            return 0
        if type(token_version) is not int or token_version < 0:
            raise UploadContractError("Original authentication snapshot is invalid.")
        if record["phase"] != "recorded":
            # Only S3 has bounded exact-key inventory for an unknown creation reply.
            if upload.provider_driver not in {"aws_s3", "s3_compatible"}:
                return 0
            if datetime.fromisoformat(record["attempt"]["expires_at"]) > observed:
                return 0
            if record["attempt"]["number"] == 5:
                return 0
            admission._can_issue(record)
        request = copy.deepcopy(command.request_payload)
        origin = request.pop("origin")
        payload = ResumableUploadBeginRequest.model_validate(request)
        actor_id, key = command.actor_id, command.idempotency_key
    admission.begin_upload_session(
        factory, actor_id, token_version, project_id, payload, key, origin=origin
    )
    return 1


def recover_pending_admissions(factory, *, now=None, limit=100):
    observed = now or admission.datetime.now(UTC)
    with factory() as db:
        candidates = list(
            db.execute(
                select(DatasetUploadSession.project_id, DatasetUploadSession.id)
                .where(
                    DatasetUploadSession.status == "initiated",
                    DatasetUploadSession.expires_at > observed,
                    DatasetUploadSession.provider_state[admission.RECORD]["intent"][
                        "protocol"
                    ].as_string()
                    == admission.PROTOCOL,
                    DatasetUploadSession.provider_state[admission.RECORD]["attempt"][
                        "actor_token_version"
                    ]
                    .as_string()
                    .is_not(None),
                    or_(
                        DatasetUploadSession.provider_state[admission.RECORD]["phase"].as_string()
                        == "recorded",
                        and_(
                            DatasetUploadSession.provider_driver.in_({"aws_s3", "s3_compatible"}),
                            DatasetUploadSession.provider_state[admission.RECORD]["phase"]
                            .as_string()
                            .in_({"issued", "unknown"}),
                        ),
                    ),
                    or_(
                        DatasetUploadSession.lease_expires_at.is_(None),
                        DatasetUploadSession.lease_expires_at <= observed,
                    ),
                )
                .order_by(DatasetUploadSession.updated_at, DatasetUploadSession.id)
                .limit(limit)
            )
        )
    recovered = 0
    for project_id, upload_id in candidates:
        try:
            recovered += _recover(factory, project_id, upload_id, observed)
        except Exception as exc:
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                upload = db.get(DatasetUploadSession, upload_id, with_for_update=True)
                if upload is not None and upload.status == "initiated":
                    upload.lease_expires_at = max(
                        upload.lease_expires_at or observed, observed + timedelta(minutes=1)
                    )
            LOGGER.warning(
                "Upload admission recovery deferred", extra={"error_type": type(exc).__name__}
            )
    return recovered
