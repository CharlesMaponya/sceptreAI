"""One committed metadata intent, one native write, and read-only outcome recovery."""

import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from functools import partial

from mlflow.entities import Metric, Param, RunTag
from sqlalchemy import select

from automl_api.models.enums import CommandStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.schemas.training_metadata import (
    TRAINING_METADATA_PROTOCOL,
    TrainingMetadataReply,
    TrainingMetadataRequest,
)
from automl_api.schemas.training_tracking import TrainingTrackingRequest
from automl_api.services import training_tracking as tracking
from automl_api.services.attempt_identity import _admit, attempt_audience
from automl_api.services.parent_control import _rows
from automl_api.services.tracking_reconciliation import tracking_identity, validate_tracking_run
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    StaleFence,
    begin_command,
    canonical_request_hash,
    transition_command,
)

OPERATION = "mlflow.metadata.log"
INTENT = "training.metadata.intent."
READY = "training.metadata.ready."


class TrackingMetadataUnresolved(RuntimeError):
    pass


def _prepare(db, attempt_id, request, proof, captured):
    role = TrainingTrackingRequest(run_id=request.run_id, role=request.role)
    current = tracking._tracking_scope(db, attempt_id, request=role, lock=True)
    attempt, run = _rows(db, attempt_id, request.run_id, lock=True)
    owner = tracking._authorize(db, attempt, run, role, proof, current, captured, complete=False)
    if owner is None or owner.payload["pod_uid"] != proof["pod_uid"]:
        raise StaleFence("Metadata belongs to its original tracking producer")
    tracking.tracking_reference(db, attempt, run, request.role, live=True)
    return attempt, run, _snapshot(db, attempt, run, request)


def requires_metadata_receipts(db, attempt, run):
    from automl_api.services.reconciler import _ray_credential_protocol

    declarations = [
        row
        for row in db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == run.project_id,
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
            )
        )
        if row.payload.get("attempt_id") == str(attempt.id)
    ]
    if len(declarations) > 1:
        raise StaleFence("Metadata has ambiguous saved declarations")
    if not declarations:
        return False
    required = _ray_credential_protocol(declarations[0]) in (
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
    )
    if required and declarations[0].payload.get("fencing_token") != attempt.fencing_token:
        raise StaleFence("Metadata declaration fence changed")
    return required


def _snapshot(db, attempt, run, request):
    if not requires_metadata_receipts(db, attempt, run):
        raise StaleFence("Metadata control requires its saved metadata protocol")
    role = TrainingTrackingRequest(run_id=request.run_id, role=request.role)
    reference = tracking.tracking_reference(db, attempt, run, request.role)
    owner = tracking._owner(db, attempt, run, role)
    command = db.get(WorkflowCommand, uuid.UUID(reference["command_id"]))
    identity = tracking_identity(command, run)
    target, target_identity = reference["tracking_run_id"], identity
    request_json = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    data = json.loads(request_json)["data"]
    if request.kind == "tags":
        common = {
            "project_id": str(run.project_id),
            "dataset_version_id": str(run.dataset_version_id),
            "automl_run_id": str(run.id),
            "task_type": run.task_type.value,
        }
        allowed = (
            set(common)
            if request.role == "parent"
            else {
                "candidate_model",
                "cost_tier",
                "accelerator",
                "detected_gpu_vendor",
                "cpu_threads",
                "rapids_active",
                "external_clustering_evaluation",
            }
        )
        if request.role.startswith("failure:"):
            allowed |= {"candidate_status", "failure_message"}
        expected = (
            common
            if request.role == "parent"
            else {
                "candidate_model": request.role.split(":", 1)[1],
                "candidate_status": "failed",
            }
        )
        if set(data) - allowed or any(
            key in expected and data[key] != expected[key] for key in data
        ):
            raise StaleFence("Metadata cannot change tracking scope or reserved tags")
    if request.kind == "mirror":
        from automl_api.training.pipeline import _registered_model_name

        name = request.role.split(":", 1)[1]
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-")
        parent = tracking.tracking_reference(db, attempt, run, "parent")
        target = parent["tracking_run_id"]
        target_identity = tracking_identity(
            db.get(WorkflowCommand, uuid.UUID(parent["command_id"])), run
        )
        data = {
            f"candidate.{safe}.metric_count": str(data["metric_count"]),
            f"candidate.{safe}.run_id": reference["tracking_run_id"],
            f"candidate.{safe}.registered_model": _registered_model_name(run, name),
        }
    payload = dict(
        request_json=request_json,
        kind=request.kind,
        tracking=reference,
        actor_id=str(run.created_by_id),
        binding_digest=owner.payload["binding_digest"],
        pod_uid=owner.payload["pod_uid"],
        target=target,
        identity=identity,
        target_identity=target_identity,
        native_data={key: str(value) for key, value in data.items()},
    )
    return payload


def _key(attempt_id, request_id):
    return f"{attempt_id}:{request_id}"


def _validate_intent(event, command, run_id):
    try:
        request = TrainingMetadataRequest.model_validate_json(
            command.request_payload["request_json"]
        )
        expected = dict(
            command_id=str(command.id),
            request_hash=command.request_hash,
            run_id=str(run_id),
            role=request.role,
        )
        if (
            event is None
            or event.payload != expected
            or event.result_digest != canonical_request_hash(expected)
            or event.event_key != INTENT + str(request.request_id)
            or event.event_type != event.event_key
            or event.project_id != command.project_id
            or str(event.attempt_id) != command.request_payload["tracking"]["attempt_id"]
            or command.operation != OPERATION
            or command.resource_type != "model_run"
            or command.resource_id != run_id
            or request.run_id != run_id
            or command.idempotency_key != _key(event.attempt_id, request.request_id)
        ):
            raise ValueError("Intent binding changed")
        return request
    except (ValueError, KeyError, TypeError, AttributeError):
        raise StaleFence("Metadata has no original immutable command binding") from None


def _command(db, attempt_id, request, run):
    event = tracking._event(db, attempt_id, INTENT + str(request.request_id))
    if event is not None:
        command = db.get(
            WorkflowCommand,
            uuid.UUID(event.payload["command_id"]),
            with_for_update=True,
            populate_existing=True,
        )
        _validate_intent(event, command, run.id)
        return command
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == run.project_id,
            WorkflowCommand.operation == OPERATION,
            WorkflowCommand.idempotency_key == _key(attempt_id, request.request_id),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if command is not None:
        raise StaleFence("Unowned metadata history requires controlled recovery")
    return None


def _same(db, command, run, payload):
    if (
        command is None
        or command.actor_id != run.created_by_id
        or command.resource_type != "model_run"
        or command.resource_id != run.id
        or command.request_payload != payload
        or command.request_hash != canonical_request_hash(payload)
    ):
        raise StaleFence("Metadata intent changed after native ownership was claimed")
    request = TrainingMetadataRequest.model_validate_json(payload["request_json"])
    _validate_intent(
        tracking._event(
            db, uuid.UUID(payload["tracking"]["attempt_id"]), INTENT + str(request.request_id)
        ),
        command,
        run.id,
    )


def _reply(command, attempt, run, request, payload):
    return TrainingMetadataReply(
        protocol=TRAINING_METADATA_PROTOCOL,
        attempt_id=attempt.id,
        project_id=run.project_id,
        run_id=run.id,
        role=request.role,
        request_id=request.request_id,
        audience=attempt_audience(attempt.id, attempt.fencing_token),
        request_digest=canonical_request_hash(request.model_dump(mode="json")),
        tracking_run_id=payload["tracking"]["tracking_run_id"],
        command_id=command.id,
        committed=True,
    ).model_dump(mode="json")


def _receipt(command, expected):
    if command.status not in {CommandStatus.RUNNING, CommandStatus.SUCCEEDED}:
        raise StaleFence("Metadata command is no longer publishable")
    if command.status != CommandStatus.SUCCEEDED:
        return None
    if command.response_status != 200 or command.response_payload != expected:
        raise StaleFence("Metadata publication acknowledgement changed")
    return expected


def _native_run(native, payload):
    original = native.get_run(payload["tracking"]["tracking_run_id"])
    validate_tracking_run(original, payload["identity"])
    if payload["target"] == original.info.run_id:
        return original
    parent = native.get_run(payload["target"])
    validate_tracking_run(parent, payload["target_identity"])
    return parent


def _marker(command_id, payload, timestamp):
    digest = canonical_request_hash(
        {
            "command_id": str(command_id),
            "payload": payload,
            "timestamp": timestamp,
        }
    )
    return f"automl.metadata.{command_id}", digest


def _verified(command):
    try:
        _, digest = _marker(
            command.id, command.request_payload, int(command.created_at.timestamp() * 1000)
        )
        return (
            command.request_hash == canonical_request_hash(command.request_payload)
            and str(command.actor_id) == command.request_payload["actor_id"]
            and str(command.resource_id) == command.request_payload["identity"]["automl_run_id"]
            and command.resource_type == "model_run"
            and str(command.project_id) == command.request_payload["identity"]["project_id"]
            and command.terminal_reason == "metadata-native-verified:" + digest
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _record_verified(factory, command_id, payload, timestamp):
    with factory() as db, db.begin():
        command = db.get(WorkflowCommand, command_id, with_for_update=True, populate_existing=True)
        if (
            command is None
            or command.request_payload != payload
            or command.request_hash != canonical_request_hash(payload)
            or str(command.actor_id) != payload["actor_id"]
            or command.resource_type != "model_run"
            or str(command.resource_id) != payload["identity"]["automl_run_id"]
            or str(command.project_id) != payload["identity"]["project_id"]
            or int(command.created_at.timestamp() * 1000) != timestamp
        ):
            raise StaleFence("Native metadata observation lost its original intent")
        request = TrainingMetadataRequest.model_validate_json(payload["request_json"])
        _validate_intent(
            tracking._event(
                db, uuid.UUID(payload["tracking"]["attempt_id"]), INTENT + str(request.request_id)
            ),
            command,
            uuid.UUID(payload["identity"]["automl_run_id"]),
        )
        _, digest = _marker(command_id, payload, timestamp)
        command.terminal_reason = "metadata-native-verified:" + digest


def has_unsettled_tracking_metadata(db, attempt):
    commands = list(
        db.scalars(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == attempt.project_id,
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.resource_id == attempt.model_run_id,
                WorkflowCommand.operation == OPERATION,
            )
        )
    )
    events = list(
        db.scalars(
            select(WorkflowEvent)
            .join(
                WorkflowAttempt,
                (WorkflowAttempt.id == WorkflowEvent.attempt_id)
                & (WorkflowAttempt.project_id == WorkflowEvent.project_id),
            )
            .where(
                WorkflowEvent.project_id == attempt.project_id,
                WorkflowAttempt.model_run_id == attempt.model_run_id,
                WorkflowEvent.event_key.startswith(INTENT),
            )
        )
    )
    try:
        by_id = {uuid.UUID(event.payload["command_id"]): event for event in events}
        if len(events) != len(commands) or set(by_id) != {command.id for command in commands}:
            return True
        for command in commands:
            _validate_intent(by_id[command.id], command, attempt.model_run_id)
            if not _verified(command):
                return True
    except (ValueError, KeyError, TypeError):
        return True
    return False


def reconcile_tracking_metadata_once(factory, *, limit=25):
    now = datetime.now(UTC)
    with factory() as db:
        ids = list(
            db.scalars(
                select(WorkflowCommand.id)
                .where(
                    WorkflowCommand.operation == OPERATION,
                    WorkflowCommand.terminal_reason.is_(None),
                    WorkflowCommand.available_at <= now,
                )
                .order_by(WorkflowCommand.available_at, WorkflowCommand.id)
                .limit(limit)
            )
        )
    settled = 0
    for command_id in ids:
        try:
            with factory() as db, db.begin():
                command = db.scalar(
                    select(WorkflowCommand)
                    .where(
                        WorkflowCommand.id == command_id,
                        WorkflowCommand.terminal_reason.is_(None),
                        WorkflowCommand.available_at <= now,
                    )
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
                if command is None:
                    continue
                command.available_at = now + timedelta(seconds=60)
                try:
                    payload = command.request_payload
                    request = TrainingMetadataRequest.model_validate_json(payload["request_json"])
                    attempt = db.get(WorkflowAttempt, uuid.UUID(payload["tracking"]["attempt_id"]))
                    run = db.get(ModelRun, request.run_id, populate_existing=True)
                    if (
                        run is None
                        or attempt is None
                        or attempt.project_id != run.project_id
                        or attempt.model_run_id != run.id
                        or command.operation != OPERATION
                        or command.idempotency_key != _key(attempt.id, request.request_id)
                    ):
                        continue
                    _same(db, command, run, payload)
                    if _snapshot(db, attempt, run, request) != payload:
                        continue
                    timestamp = int(command.created_at.timestamp() * 1000)
                except (ValueError, KeyError, TypeError, AttributeError):
                    continue
            native = tracking.MlflowClient(tracking_uri=tracking.get_settings().mlflow_tracking_uri)
            key, digest = _marker(command_id, payload, timestamp)
            if _native_run(native, payload).data.tags.get(key) == digest:
                _record_verified(factory, command_id, payload, timestamp)
                settled += 1
        except Exception:
            # Unknown native effects retain their retirement barrier and original intent.
            continue
    return settled


def _ready_payload(command, payload):
    return dict(
        command_id=str(command.id),
        intent_digest=command.request_hash,
        response_digest=canonical_request_hash(command.response_payload),
        tracking_run_id=payload["tracking"]["tracking_run_id"],
        pod_uid=payload["pod_uid"],
    )


def metadata_reference(db, attempt, run, role, tracking_receipt):
    from automl_api.schemas.training_metadata import TrainingMetadataReference

    if not requires_metadata_receipts(db, attempt, run):
        return None
    event = tracking._event(db, attempt.id, READY + role)
    if event is None:
        raise StaleFence("New training publication has no original metadata readiness")
    command = db.get(
        WorkflowCommand, uuid.UUID(event.payload["command_id"]), populate_existing=True
    )
    if command is None:
        raise StaleFence("Metadata readiness lost its original command")
    payload = command.request_payload
    _same(db, command, run, payload)
    request = TrainingMetadataRequest.model_validate_json(payload["request_json"])
    if (
        request.run_id != run.id
        or request.role != role
        or request.kind != "tags"
        or payload["tracking"] != tracking_receipt
        or str(attempt.id) != payload["tracking"]["attempt_id"]
        or not _verified(command)
        or _receipt(command, _reply(command, attempt, run, request, payload)) is None
        or event.payload != _ready_payload(command, payload)
        or event.event_type != READY + role
        or event.result_digest != canonical_request_hash(event.payload)
    ):
        raise StaleFence("Metadata readiness no longer matches its original acknowledgement")
    return TrainingMetadataReference(
        attempt_id=attempt.id,
        command_id=command.id,
        role=role,
        tracking_run_id=payload["tracking"]["tracking_run_id"],
        pod_uid=payload["pod_uid"],
        intent_digest=command.request_hash,
        receipt_digest=event.result_digest,
    ).model_dump(mode="json")


def validate_metadata_reference(db, run, reference, role, tracking_receipt):
    from automl_api.schemas.training_metadata import TrainingMetadataReference

    reference = TrainingMetadataReference.model_validate(reference).model_dump(mode="json")
    attempt = db.get(WorkflowAttempt, uuid.UUID(reference["attempt_id"]), populate_existing=True)
    if (
        attempt is None
        or attempt.project_id != run.project_id
        or attempt.model_run_id != run.id
        or reference["role"] != role
        or metadata_reference(db, attempt, run, role, tracking_receipt) != reference
    ):
        raise StaleFence("Metadata reference changed after model reservation")
    return reference


def _record_ready(db, attempt, run, command, request, payload):
    if request.kind != "tags":
        return
    if tracking._event(db, attempt.id, READY + request.role) is None:
        tracking._append(db, attempt, READY + request.role, _ready_payload(command, payload))
    metadata_reference(db, attempt, run, request.role, payload["tracking"])


def control_training_metadata(factory, attempt_id, token, k8s, request, *, receipt_only=False):
    role = TrainingTrackingRequest(run_id=request.run_id, role=request.role)
    resolver = partial(tracking._tracking_scope, request=role)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
        if receipt_only:
            _, run = _rows(db, attempt_id, request.run_id, lock=True)
            existing = _command(db, attempt_id, request, run)
            if existing is None:
                raise StaleFence("Metadata has no original intent")
            stored = TrainingMetadataRequest.model_validate_json(
                existing.request_payload["request_json"]
            )
            if (stored.run_id, stored.role, stored.request_id) != (
                request.run_id,
                request.role,
                request.request_id,
            ):
                raise StaleFence("Metadata receipt belongs to another request")
            request = stored
    proof = _admit(
        factory, attempt_id, token, k8s, resolver, event_type="training.metadata.identity"
    )
    with factory() as db, db.begin():
        attempt, run, payload = _prepare(db, attempt_id, request, proof, captured)
        command = _command(db, attempt_id, request, run)
        if command is None:
            if receipt_only:
                raise StaleFence("Metadata has no original intent")
            command, replay = begin_command(
                db,
                project_id=run.project_id,
                actor_id=run.created_by_id,
                operation=OPERATION,
                idempotency_key=_key(attempt_id, request.request_id),
                payload=payload,
            )
            if replay:
                raise StaleFence("Metadata intent was concurrently claimed")
            command.resource_type, command.resource_id = "model_run", run.id
            transition_command(command, CommandStatus.RUNNING)
            tracking._append(
                db,
                attempt,
                INTENT + str(request.request_id),
                dict(
                    command_id=str(command.id),
                    request_hash=command.request_hash,
                    run_id=str(run.id),
                    role=request.role,
                ),
            )
        else:
            _same(db, command, run, payload)
            completed = _receipt(command, _reply(command, attempt, run, request, payload))
            if completed is not None:
                return completed
            if not receipt_only:
                raise TrackingMetadataUnresolved("Metadata was already claimed; use its receipt")
        command_id = command.id
        timestamp = int(command.created_at.timestamp() * 1000)
    key, digest = _marker(command_id, payload, timestamp)
    try:
        os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"
        native = tracking.MlflowClient(tracking_uri=tracking.get_settings().mlflow_tracking_uri)
        if not receipt_only:
            _native_run(native, payload)
            data, kind = payload["native_data"], request.kind
            native.log_batch(
                payload["target"],
                metrics=[Metric(key, float(value), timestamp, 0) for key, value in data.items()]
                if kind == "metrics"
                else [],
                params=[Param(key, str(value)) for key, value in data.items()]
                if kind == "params"
                else [],
                tags=[RunTag(key, str(value)) for key, value in data.items()]
                if kind in {"tags", "mirror"}
                else [],
                synchronous=True,
            )
            native.set_tag(payload["target"], key, digest, synchronous=True)
        if _native_run(native, payload).data.tags.get(key) != digest:
            raise TrackingMetadataUnresolved("Native metadata does not prove the original outcome")
    except Exception:
        raise TrackingMetadataUnresolved("Tracking metadata outcome remains unresolved") from None
    _record_verified(factory, command_id, payload, timestamp)
    fresh = _admit(
        factory, attempt_id, token, k8s, resolver, event_type="training.metadata.identity"
    )
    with factory() as db, db.begin():
        attempt, run, current = _prepare(db, attempt_id, request, fresh, captured)
        command = db.get(WorkflowCommand, command_id, with_for_update=True, populate_existing=True)
        _same(db, command, run, current)
        if current != payload:
            raise StaleFence("Metadata authority changed during native verification")
        reply = _reply(command, attempt, run, request, current)
        completed = _receipt(command, reply)
        if completed is not None:
            return completed
        command.response_payload, command.response_status = reply, 200
        transition_command(command, CommandStatus.SUCCEEDED)
        db.flush()
        _record_ready(db, attempt, run, command, request, current)
        return reply


def result_metadata(db, attempt, run, receipt, *, publish=False):
    if not requires_metadata_receipts(db, attempt, run):
        return None
    base = tracking.tracking_reference(db, attempt, run, "parent")
    metadata = metadata_reference(db, attempt, run, "parent", base)
    expected = {"result_digest": receipt["digest"], "parent_metadata": metadata}
    key = "training.result.metadata"
    event = tracking._event(db, attempt.id, key)
    if event is None and publish:
        tracking._append(db, attempt, key, expected)
        event = tracking._event(db, attempt.id, key)
    if (
        event is None
        or event.payload != expected
        or event.event_type != key
        or event.result_digest != canonical_request_hash(expected)
    ):
        raise StaleFence("Training result lost its original parent metadata binding")
    return metadata
