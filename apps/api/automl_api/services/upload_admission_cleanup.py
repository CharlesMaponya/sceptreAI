"""Cancel authenticated unpublished native sessions without discarding uncertainty."""

import copy
import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select

from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus, ProjectRole, ProjectStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import UploadCleanupReplayRead, UploadCleanupReplayRequest
from automl_api.services import upload_admission as admission
from automl_api.services.projects import require_project_role
from automl_api.services.upload_admission_expiry import _unpublished
from automl_api.services.workflow_state import begin_command, transition_command
from automl_api.services.workflow_state import canonical_request_hash as digest
from automl_api.storage.contracts import ProviderUpload, UploadContractError

OPERATION = "dataset.upload.admission.cleanup.v1"
REPLAY = "dataset.upload.admission.cleanup.replay.v1"
RECORD = "admission_cleanup"
LOGGER = logging.getLogger(__name__)


def _budget(state):
    return 5 * (1 + len(state.get("replays", [])))


def _replay_result(command, original, upload, epoch):
    return UploadCleanupReplayRead(
        id=command.id,
        cleanup_command_id=original.id,
        session_id=upload.id,
        attempt_budget=5 * (epoch + 1),
    )


def _approvals(db, upload, command, state):
    approvals = state.get("replays", [])
    nodes = list(
        db.scalars(
            select(WorkflowCommand)
            .where(
                WorkflowCommand.project_id == upload.project_id,
                WorkflowCommand.operation == REPLAY,
                WorkflowCommand.request_payload["cleanup_command_id"].as_string()
                == str(command.id),
            )
            .execution_options(populate_existing=True)
        )
    )
    if type(approvals) is not list or len(nodes) != len(approvals):
        raise UploadContractError("Original cleanup replay history is missing or ambiguous.")
    if not approvals:
        if "cleanup_replays_digest" in command.response_payload:
            raise UploadContractError("Original cleanup replay history cannot be removed.")
        return
    if command.response_payload.get("cleanup_replays_digest") != digest(approvals):
        raise UploadContractError("Original cleanup replay authorization changed.")
    by_id = {str(node.id): node for node in nodes}
    seen = set()
    for index, approval in enumerate(approvals):
        node = by_id.get(approval["command_id"])
        request = node.request_payload if node is not None else {}
        prior = request.get("prior_attempt", {})
        if (
            node is None
            or node.id in seen
            or node.resource_type != REPLAY
            or node.resource_id != upload.id
            or node.status != CommandStatus.SUCCEEDED
            or node.response_status != 202
            or approval["actor_id"] != str(node.actor_id)
            or request.get("actor_id") != str(node.actor_id)
            or type(request.get("token_version")) is not int
            or request["token_version"] < 0
            or request.get("session_id") != str(upload.id)
            or request.get("cleanup_command_id") != str(command.id)
            or request.get("cleanup_intent_digest") != digest(state["intent"])
            or request.get("epoch") != index + 1
            or type(request["epoch"]) is not int
            or request.get("prior_replay_digest") != digest(approvals[:index])
            or type(prior.get("number")) is not int
            or prior["number"] != 5 * (index + 1)
            or state["attempt"]["number"] < prior["number"]
            or datetime.fromisoformat(prior["expires_at"])
            > datetime.fromisoformat(approval["authorized_at"])
            or node.request_hash != digest(request)
            or approval["request_digest"] != node.request_hash
            or node.response_payload.get("authorization") != approval
            or node.response_payload.get("result")
            != _replay_result(node, command, upload, index + 1).model_dump(mode="json")
        ):
            raise UploadContractError("Original cleanup replay chain could not be authenticated.")
        uuid.UUID(prior["id"])
        UploadCleanupReplayRequest(reason=request["reason"])
        seen.add(node.id)


def _rows(db, project_id, upload_id):
    project = db.get(Project, project_id, with_for_update=True, populate_existing=True)
    if project is None:
        raise UploadContractError("Original admission cleanup project is missing.")
    upload = db.get(DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True)
    try:
        original = db.get(
            WorkflowCommand,
            uuid.UUID(upload.provider_state[admission.RECORD]["intent"]["command_id"]),
            populate_existing=True,
        )
        if (
            original is None
            or original.project_id != project_id
            or original.resource_id != upload_id
        ):
            raise UploadContractError("Original admission cleanup binding changed.")
        _, record, _ = admission._journal(db, original)
        if record["phase"] != "retired":
            raise UploadContractError("Admission is not logically retired.")
        return upload, original, record
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise UploadContractError("Original admission cleanup history is unavailable.") from exc


def _target(db, upload, original, record, settings):
    _unpublished(db, upload, original, record, retired=True)
    if (
        type(record["attempt"]["number"]) is not int
        or record["attempt"]["number"] != 1
        or "receipt" not in record
        or original.response_payload.get("superseded_admission_receipts", {}) != {}
        or upload.provider_driver not in {"s3_compatible", "aws_s3", "gcs"}
        or upload.protocol
        != ("resumable_offset" if upload.provider_driver == "gcs" else "multipart")
    ):
        raise UploadContractError(
            "Ambiguous native admission cleanup requires controlled recovery."
        )
    admission.require_store(settings, upload.provider_state, upload.provider_driver)


def _fresh(db, upload, original, record, settings, observed):
    project = db.get(Project, upload.project_id)
    if project.status != ProjectStatus.ACTIVE or project.settings.get("deletion_in_progress"):
        raise UploadContractError("Admission cleanup requires an active project.")
    _target(db, upload, original, record, settings)
    if (
        upload.legal_hold
        or upload.retention_until is not None
        and upload.retention_until > observed
    ):
        raise UploadContractError("Admission cleanup is held by current retention policy.")


def _state(db, upload, original, record, *, create=False):
    value = copy.deepcopy(upload.provider_state.get(RECORD))
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == upload.project_id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.idempotency_key == str(upload.id),
        )
        .execution_options(populate_existing=True)
    )
    payload = dict(
        session_id=str(upload.id),
        admission_command_id=str(original.id),
        retirement_digest=digest(record["retirement"]),
        receipt_digest=digest(record.get("receipt")),
        store_digest=record["intent"]["store_digest"],
    )
    if value is None:
        if command is not None:
            raise UploadContractError("Original admission cleanup state is missing.")
        if not create:
            return None, None
        command, _ = begin_command(
            db,
            project_id=upload.project_id,
            actor_id=original.actor_id,
            operation=OPERATION,
            idempotency_key=str(upload.id),
            payload=payload,
        )
        command.resource_id, command.resource_type = upload.id, OPERATION
        intent = dict(
            command_id=str(command.id),
            request_digest=command.request_hash,
            target=copy.deepcopy(record["receipt"]),
        )
        value = dict(intent=intent)
        command.response_payload = dict(cleanup_intent_digest=digest(intent))
        transition_command(command, CommandStatus.RUNNING)
        return command, value
    try:
        intent, phase = value["intent"], value["phase"]
        if (
            command is None
            or command.resource_id != upload.id
            or command.resource_type != OPERATION
            or command.actor_id != original.actor_id
            or command.request_payload != payload
            or command.request_hash != digest(payload)
            or intent
            != dict(
                command_id=str(command.id),
                request_digest=command.request_hash,
                target=record["receipt"],
            )
            or command.response_payload.get("cleanup_intent_digest") != digest(intent)
            or command.response_payload.get("cleanup_attempt_digest") != digest(value["attempt"])
            or phase not in {"issued", "unknown", "recorded", "published", "exhausted"}
            or phase != "published"
            and command.response_status is not None
            or command.status
            != (
                CommandStatus.SUCCEEDED
                if phase == "published"
                else CommandStatus.FAILED
                if phase == "exhausted"
                else CommandStatus.RUNNING
            )
        ):
            raise UploadContractError("Original admission cleanup custody changed.")
        if phase in {"issued", "unknown", "exhausted"} and (
            "receipt" in value
            or "cleanup_receipt_digest" in command.response_payload
            or "result" in command.response_payload
        ):
            raise UploadContractError("Known admission cleanup cannot return to issuance.")
        if phase in {"recorded", "published"}:
            receipt = value["receipt"]
            if (
                receipt["cancelled"] is not True
                or receipt["target_digest"] != digest(intent["target"])
                or receipt["attempt_digest"] != digest(value["attempt"])
                or command.response_payload.get("cleanup_receipt_digest") != digest(receipt)
            ):
                raise UploadContractError("Original admission cleanup outcome changed.")
        if phase == "published" and (
            command.response_status != 200
            or command.response_payload.get("result")
            != dict(cancelled=True, target_digest=digest(intent["target"]))
        ):
            raise UploadContractError("Original admission cleanup result changed.")
        _approvals(db, upload, command, value)
        if type(value["attempt"]["number"]) is not int or not 1 <= value["attempt"][
            "number"
        ] <= _budget(value):
            raise UploadContractError("Original cleanup attempt exceeds its authorized budget.")
        return command, value
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise UploadContractError("Original admission cleanup could not be qualified.") from exc


def _save(upload, state):
    upload.provider_state = {**upload.provider_state, RECORD: copy.deepcopy(state)}


def require_cleanup_complete(db, upload):
    """Authenticate native cancellation; deletion separately enforces live retention."""
    current, original, record = _rows(db, upload.project_id, upload.id)
    _target(db, current, original, record, admission.get_settings())
    _, state = _state(db, current, original, record)
    if state is None or state["phase"] != "published":
        raise UploadContractError("Original native admission cleanup is not complete.")


def replay_cleanup(factory, actor_id, token_version, project_id, upload_id, key, reason):
    payload = UploadCleanupReplayRequest(reason=reason)
    observed = datetime.now(UTC)
    with factory() as db, db.begin():
        actor = admission._authorize(db, project_id, actor_id, token_version)
        require_project_role(db, actor, project_id, ProjectRole.OWNER)
        upload, original, record = _rows(db, project_id, upload_id)
        _fresh(db, upload, original, record, admission.get_settings(), observed)
        command, state = _state(db, upload, original, record)
        if state is None:
            raise UploadContractError("Original native cleanup has not started.")
        existing = db.scalar(
            select(WorkflowCommand)
            .where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.operation == REPLAY,
                WorkflowCommand.idempotency_key == key,
            )
            .execution_options(populate_existing=True)
        )
        if existing is not None:
            if (
                existing.actor_id != actor_id
                or existing.request_payload.get("reason") != payload.reason
                or existing.request_payload.get("session_id") != str(upload_id)
            ):
                raise UploadContractError("Replay idempotency key belongs to another request.")
            if str(existing.id) not in {entry["command_id"] for entry in state.get("replays", [])}:
                raise UploadContractError("Replay approval is not in the original cleanup chain.")
            return UploadCleanupReplayRead.model_validate(existing.response_payload["result"])
        if (
            state["phase"] != "exhausted"
            or state["attempt"]["number"] != _budget(state)
            or datetime.fromisoformat(state["attempt"]["expires_at"]) > observed
        ):
            raise UploadContractError(
                "Only an exhausted, unleased original cleanup can be replayed."
            )
        approvals = copy.deepcopy(state.get("replays", []))
        epoch = len(approvals) + 1
        request = dict(
            session_id=str(upload_id),
            reason=payload.reason,
            cleanup_command_id=str(command.id),
            cleanup_intent_digest=digest(state["intent"]),
            prior_attempt=copy.deepcopy(state["attempt"]),
            prior_replay_digest=digest(approvals),
            epoch=epoch,
            actor_id=str(actor_id),
            token_version=token_version,
            prior_error_type=state.get("error_type"),
            prior_terminal_reason=command.terminal_reason,
        )
        approval_command, _ = begin_command(
            db,
            project_id=project_id,
            actor_id=actor_id,
            operation=REPLAY,
            idempotency_key=key,
            payload=request,
        )
        approval_command.resource_id, approval_command.resource_type = upload_id, REPLAY
        authorization = dict(
            command_id=str(approval_command.id),
            actor_id=str(actor_id),
            request_digest=approval_command.request_hash,
            authorized_at=observed.isoformat(),
        )
        approvals.append(authorization)
        result = _replay_result(approval_command, command, upload, epoch)
        approval_command.response_payload = dict(
            authorization=authorization, result=result.model_dump(mode="json")
        )
        approval_command.response_status = 202
        transition_command(approval_command, CommandStatus.RUNNING)
        transition_command(approval_command, CommandStatus.SUCCEEDED)
        state.update(replays=approvals, phase="unknown")
        command.response_payload = {
            **command.response_payload,
            "cleanup_replays_digest": digest(approvals),
        }
        transition_command(command, CommandStatus.PENDING)
        transition_command(command, CommandStatus.RUNNING)
        command.replayed_by, command.terminal_reason = actor_id, None
        command.max_retries = _budget(state)
        upload.lease_owner, upload.lease_expires_at = None, None
        upload.terminal_reason = (
            "Native admission cleanup replay authorized for the original target."
        )
        _save(upload, state)
        return result


def _publish(factory, project_id, upload_id, settings, observed):
    with factory() as db, db.begin():
        upload, original, record = _rows(db, project_id, upload_id)
        command, state = _state(db, upload, original, record)
        _fresh(db, upload, original, record, settings, observed)
        if state is None or state["phase"] not in {"recorded", "published"}:
            raise UploadContractError("Original admission cleanup has no known outcome.")
        if state["phase"] == "published":
            return False
        command.response_payload = {
            **command.response_payload,
            "result": dict(cancelled=True, target_digest=digest(state["intent"]["target"])),
        }
        command.response_status = 200
        transition_command(command, CommandStatus.SUCCEEDED)
        state["phase"] = "published"
        _save(upload, state)
        upload.lease_expires_at = None
        return True


def cleanup_admission(factory, project_id, upload_id, *, settings=None, now=None):
    settings = settings or admission.get_settings()

    def clock():
        return now or datetime.now(UTC)

    with factory() as db, db.begin():
        upload, original, record = _rows(db, project_id, upload_id)
        _fresh(db, upload, original, record, settings, clock())
        command, state = _state(db, upload, original, record, create=True)
        if state.get("phase") == "published":
            return False
        known = state.get("phase") == "recorded"
        if not known:
            if state.get("phase") == "exhausted":
                return False
            if (
                state.get("attempt")
                and datetime.fromisoformat(state["attempt"]["expires_at"]) > clock()
            ):
                return False
            number = state.get("attempt", {}).get("number", 0) + 1
            if number > _budget(state):
                state["phase"] = "exhausted"
                transition_command(command, CommandStatus.FAILED)
                _save(upload, state)
                upload.lease_expires_at = None
                upload.terminal_reason = (
                    "Native admission cleanup exhausted; controlled recovery required."
                )
                command.terminal_reason = upload.terminal_reason
                return False
            attempt = dict(
                id=str(uuid.uuid4()),
                number=number,
                expires_at=(clock() + timedelta(minutes=5)).isoformat(),
            )
            state.update(phase="issued", attempt=attempt)
            command.retry_count, command.max_retries = number, _budget(state)
            command.response_payload = {
                **command.response_payload,
                "cleanup_attempt_digest": digest(attempt),
            }
            _save(upload, state)
            upload.lease_expires_at = datetime.fromisoformat(attempt["expires_at"])
    if not known:
        try:
            store = admission.get_object_store(settings)
            target = state["intent"]["target"]
            with factory() as db, db.begin():
                upload, original, record = _rows(db, project_id, upload_id)
                _, current = _state(db, upload, original, record)
                _fresh(db, upload, original, record, settings, clock())
                if (
                    current["attempt"] != attempt
                    or current["phase"] != "issued"
                    or datetime.fromisoformat(attempt["expires_at"]) <= clock()
                ):
                    raise UploadContractError("Admission cleanup lost its native lease.")
                if (
                    store.driver_name != upload.provider_driver
                    or store.uri_for_key(target["object_key"]) != record["intent"]["source"]["uri"]
                ):
                    raise UploadContractError(
                        "Native store differs from original admission cleanup."
                    )
            store.abort_upload(
                ProviderUpload(
                    target["provider_id"],
                    target["object_key"],
                    target["protocol"],
                    target["byte_size"],
                    datetime.fromisoformat(target["expires_at"]),
                    copy.deepcopy(target["state"]),
                )
            )
        except Exception as exc:
            with factory() as db, db.begin():
                upload, original, record = _rows(db, project_id, upload_id)
                _, current = _state(db, upload, original, record)
                if current["attempt"] == attempt and current["phase"] == "issued":
                    current.update(phase="unknown", error_type=type(exc).__name__)
                    _save(upload, current)
            raise
        with factory() as db, db.begin():
            upload, original, record = _rows(db, project_id, upload_id)
            command, current = _state(db, upload, original, record)
            receipt = dict(
                cancelled=True,
                target_digest=digest(state["intent"]["target"]),
                attempt_digest=digest(attempt),
                observed_at=clock().isoformat(),
            )
            if current["attempt"] != attempt or current["phase"] not in {"issued", "unknown"}:
                superseded = dict(command.response_payload.get("superseded_cleanup_receipts", {}))
                superseded[attempt["id"]] = receipt
                command.response_payload = {
                    **command.response_payload,
                    "superseded_cleanup_receipts": superseded,
                }
                return False
            current.update(phase="recorded", receipt=receipt)
            command.response_payload = {
                **command.response_payload,
                "cleanup_receipt_digest": digest(receipt),
            }
            _save(upload, current)
            upload.lease_expires_at = None
    return _publish(factory, project_id, upload_id, settings, clock())


def cleanup_pending_admissions(factory, *, settings=None, now=None, limit=100):
    observed = now or datetime.now(UTC)
    with factory() as db:
        candidates = list(
            db.execute(
                select(DatasetUploadSession.project_id, DatasetUploadSession.id)
                .where(
                    DatasetUploadSession.status == "expired",
                    DatasetUploadSession.provider_state[admission.RECORD]["phase"].as_string()
                    == "retired",
                    DatasetUploadSession.provider_state[admission.RECORD]["receipt"]["provider_id"]
                    .as_string()
                    .is_not(None),
                    DatasetUploadSession.provider_state[admission.RECORD]["attempt"][
                        "number"
                    ].as_string()
                    == "1",
                    DatasetUploadSession.provider_driver.in_({"s3_compatible", "aws_s3", "gcs"}),
                    or_(
                        DatasetUploadSession.provider_state[RECORD]["phase"].as_string().is_(None),
                        DatasetUploadSession.provider_state[RECORD]["phase"]
                        .as_string()
                        .not_in({"published", "exhausted"}),
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
    completed = 0
    for project_id, upload_id in candidates:
        try:
            completed += bool(
                cleanup_admission(factory, project_id, upload_id, settings=settings, now=now)
            )
        except Exception as exc:
            with factory() as db, db.begin():
                db.get(Project, project_id, with_for_update=True)
                upload = db.get(
                    DatasetUploadSession, upload_id, with_for_update=True, populate_existing=True
                )
                if upload is not None and upload.status == "expired":
                    upload.lease_expires_at = max(
                        upload.lease_expires_at or observed, observed + timedelta(minutes=1)
                    )
            LOGGER.warning(
                "Native admission cleanup deferred", extra={"error_type": type(exc).__name__}
            )
    return completed
