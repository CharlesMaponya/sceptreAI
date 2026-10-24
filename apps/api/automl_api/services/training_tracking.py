"""Pod-owned tracking roles reuse the durable native writer without worker SQL."""

import hashlib
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from functools import partial

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.enums import CommandStatus
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.training_tracking import TrainingTrackingReply, TrainingTrackingRequest
from automl_api.services.attempt_identity import _admit, attempt_audience
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.parent_control import _publisher, _rows
from automl_api.services.tracking_reconciliation import (
    ensure_tracking_run,
    tracking_identity,
    validate_tracking_run,
)
from automl_api.services.training_parent import _require_parent_lease
from automl_api.services.training_progress import _progress_scope
from automl_api.services.training_result import _stored_result
from automl_api.services.workflow_state import (
    RAY_CREDENTIAL_PROTOCOL,
    RAY_TRAINING_METADATA_PROTOCOL,
    RAY_TRAINING_TRACKING_PROTOCOL,
    StaleFence,
    canonical_request_hash,
)

OWNER = "training.tracking.owner."
PUBLISHED = "training.tracking.published."


@dataclass(frozen=True)
class TrackingPlan:
    role: str
    run_name: str
    parent_id: str | None
    tags: dict
    backend_hash: str
    parent_command_id: uuid.UUID | None
    parent_identity: dict | None


def _command_payload(run, attempt, role, parent_id, backend_hash):
    return dict(
        model_run_id=str(run.id),
        attempt_id=str(attempt.id),
        role=role,
        parent_run_id=parent_id,
        tracking_backend_hash=backend_hash,
    )


def _known_command(db, run, attempt, role, parent_id, backend_hash):
    from automl_api.training.pipeline import TrackingCreationUnresolved

    payload = _command_payload(run, attempt, role, parent_id, backend_hash)
    key = canonical_request_hash(
        {name: payload[name] for name in ("model_run_id", "attempt_id", "role")}
    )
    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == run.project_id,
            WorkflowCommand.operation == "mlflow.run.ensure",
            WorkflowCommand.idempotency_key == key,
        )
        .execution_options(populate_existing=True)
    )
    if command is None:
        raise TrackingCreationUnresolved("Tracking has no committed creation receipt")
    if (
        command.actor_id != run.created_by_id
        or command.resource_type != "model_run"
        or command.resource_id != run.id
        or command.request_payload != payload
        or command.request_hash != canonical_request_hash(payload)
    ):
        raise StaleFence("Tracking command lineage changed")
    if (
        command.status != CommandStatus.SUCCEEDED
        or command.response_status != 200
        or not isinstance(command.response_payload, dict)
        or set(command.response_payload) != {"run_id"}
    ):
        raise TrackingCreationUnresolved("Tracking creation has no verified committed outcome")
    return command


def _tracking_scope(db, attempt_id, *, request, lock=False, expected_project=None):
    from automl_api.services.reconciler import _ray_credential_protocol

    scope = _progress_scope(
        db, attempt_id, run_id=request.run_id, lock=lock, expected_project=expected_project
    )
    attempt, run = _rows(
        db, attempt_id, request.run_id, lock=True, expected_project=scope.project_id
    )
    entries = [
        row
        for row in db.scalars(
            select(OutboxEntry).where(
                OutboxEntry.project_id == run.project_id,
                OutboxEntry.aggregate_id == run.id,
                OutboxEntry.topic == "ray.training.submit",
            )
        )
        if row.payload.get("attempt_id") == str(attempt_id)
    ]
    if (
        len(entries) != 1
        or _ray_credential_protocol(entries[0])
        not in (
            RAY_TRAINING_TRACKING_PROTOCOL,
            RAY_TRAINING_METADATA_PROTOCOL,
            RAY_CREDENTIAL_PROTOCOL,
        )
        or entries[0].payload.get("fencing_token") != attempt.fencing_token
    ):
        raise StaleFence("Tracking RPC requires its saved tracking protocol")
    backend_hash = hashlib.sha256(get_settings().mlflow_tracking_uri.encode()).hexdigest()
    parent_id = parent_command_id = parent_identity = None
    if request.role == "parent":
        name = run.run_name or str(run.id)
        tags = {"task_type": run.task_type.value}
    else:
        kind, name = request.role.split(":", 1)
        if name not in run.params["candidate_models"]:
            raise StaleFence("Tracking role belongs to an unselected candidate")
        parent = _known_command(db, run, attempt, "parent", None, backend_hash)
        parent_request = TrainingTrackingRequest(run_id=run.id, role="parent")
        _published(db, attempt, run, parent_request, parent, required=True)
        parent_id, parent_command_id = parent.response_payload["run_id"], parent.id
        parent_identity = tracking_identity(parent, run)
        if run.mlflow_run_id != parent_id:
            raise StaleFence("Candidate tracking parent pointer changed")
        tags = {"candidate_model": name}
        if kind == "failure":
            tags["candidate_status"] = "failed"
    plan = TrackingPlan(
        request.role, name, parent_id, tags, backend_hash, parent_command_id, parent_identity
    )
    return replace(
        scope,
        source_plan=plan,
        binding_digest=canonical_request_hash(
            {
                "source": scope.binding_digest,
                "plan": vars(plan),
                "declaration": {"id": entries[0].id, "payload": entries[0].payload},
            }
        ),
    )


def _owner(db, attempt, run, request):
    event = _event(db, attempt.id, OWNER + request.role)
    if event is not None and (
        event.event_type != OWNER + request.role
        or not isinstance(event.payload, dict)
        or set(event.payload) != {"run_id", "role", "pod_uid", "request_digest", "binding_digest"}
        or event.payload["run_id"] != str(run.id)
        or event.payload["role"] != request.role
        or event.payload["request_digest"]
        != canonical_request_hash(request.model_dump(mode="json"))
        or event.result_digest != canonical_request_hash(event.payload)
    ):
        raise StaleFence("Tracking role ownership changed")
    return event


def _authorize(db, attempt, run, request, proof, current, captured, *, complete):
    publisher = _publisher(db, attempt, run)
    if (
        current != captured
        or proof["generation"] != current.generation
        or proof["audience"] != attempt_audience(attempt.id, attempt.fencing_token)
        or proof["expires_at"] <= int(datetime.now(UTC).timestamp())
        or publisher is None
        or (request.role == "parent" and publisher.payload["pod_uid"] != proof["pod_uid"])
    ):
        raise StaleFence("Tracking authority changed during native verification")
    owner = _owner(db, attempt, run, request)
    if owner is not None and (
        owner.payload["pod_uid"] != proof["pod_uid"]
        or (not complete and owner.payload["binding_digest"] != current.binding_digest)
    ):
        raise StaleFence("Tracking role belongs to another producer or source")
    if not complete:
        _require_parent_lease(db, attempt, publisher.payload["owner"])
    return owner


def _published(db, attempt, run, request, command, *, required):
    from automl_api.training.pipeline import TrackingCreationUnresolved

    owner = _owner(db, attempt, run, request)
    if owner is None:
        raise StaleFence("Tracking has no committed producer owner")
    expected = dict(
        command_id=str(command.id),
        request_hash=command.request_hash,
        owner_digest=owner.result_digest,
        tracking_run_id=command.response_payload["run_id"],
        parent_tracking_run_id=command.request_payload["parent_run_id"],
        tracking_backend_hash=command.request_payload["tracking_backend_hash"],
    )
    event = _event(db, attempt.id, PUBLISHED + request.role)
    if event is None:
        if required:
            raise TrackingCreationUnresolved("Tracking has no native publication acknowledgement")
        return expected
    if (
        event.event_type != PUBLISHED + request.role
        or event.payload != expected
        or event.result_digest != canonical_request_hash(expected)
    ):
        raise StaleFence("Tracking publication acknowledgement changed")
    return None


def _record_receipt(db, attempt, run, request, command):
    payload = _published(db, attempt, run, request, command, required=False)
    if payload is not None:
        _append(db, attempt, PUBLISHED + request.role, payload)


def _reply(db, attempt, run, request, plan):
    command = _known_command(db, run, attempt, request.role, plan.parent_id, plan.backend_hash)
    _published(db, attempt, run, request, command, required=True)
    tracking_id = command.response_payload["run_id"]
    if request.role == "parent":
        if run.mlflow_run_id not in {None, tracking_id}:
            raise StaleFence("Parent tracking pointer changed")
        # Read-only native recovery may have committed the ID before the HTTP acknowledgement.
        run.mlflow_run_id = tracking_id
    return TrainingTrackingReply(
        protocol="training-tracking-v1",
        attempt_id=attempt.id,
        project_id=run.project_id,
        run_id=run.id,
        role=request.role,
        audience=attempt_audience(attempt.id, attempt.fencing_token),
        command_id=command.id,
        request_digest=canonical_request_hash(request.model_dump(mode="json")),
        tracking_run_id=tracking_id,
        parent_tracking_run_id=plan.parent_id,
        tracking_backend_hash=plan.backend_hash,
    ).model_dump(mode="json")


def control_training_tracking(factory, attempt_id, token, k8s, request, *, receipt_only=False):
    request = TrainingTrackingRequest.model_validate(request)
    resolver = partial(_tracking_scope, request=request)
    with factory() as db, db.begin():
        captured = resolver(db, attempt_id)
    proof = _admit(
        factory, attempt_id, token, k8s, resolver, event_type="training.tracking.identity"
    )
    with factory() as db, db.begin():
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        complete = _stored_result(db, attempt, run) is not None
        owner = _authorize(db, attempt, run, request, proof, current, captured, complete=complete)
        recovery = None
        if receipt_only or complete:
            if owner is None:
                raise StaleFence("Tracking has no owned creation receipt")
            command = _known_command(
                db,
                run,
                attempt,
                request.role,
                current.source_plan.parent_id,
                current.source_plan.backend_hash,
            )
            missing = _published(db, attempt, run, request, command, required=False)
            if missing is None:
                return _reply(db, attempt, run, request, current.source_plan)
            recovery = (
                command.id,
                command.response_payload["run_id"],
                tracking_identity(command, run),
            )
        if owner is None:
            payload = _command_payload(
                run,
                attempt,
                request.role,
                current.source_plan.parent_id,
                current.source_plan.backend_hash,
            )
            key = canonical_request_hash(
                {name: payload[name] for name in ("model_run_id", "attempt_id", "role")}
            )
            existing = db.scalar(
                select(WorkflowCommand.id).where(
                    WorkflowCommand.project_id == run.project_id,
                    WorkflowCommand.operation == "mlflow.run.ensure",
                    WorkflowCommand.idempotency_key == key,
                )
            )
            if existing is not None:
                raise StaleFence("Unowned tracking history requires controlled recovery")
            _append(
                db,
                attempt,
                OWNER + request.role,
                dict(
                    run_id=str(run.id),
                    role=request.role,
                    pod_uid=proof["pod_uid"],
                    request_digest=canonical_request_hash(request.model_dump(mode="json")),
                    binding_digest=current.binding_digest,
                ),
            )
        plan = current.source_plan

    if recovery is not None:
        try:
            native = MlflowClient(tracking_uri=get_settings().mlflow_tracking_uri)
            if hashlib.sha256(native.tracking_uri.encode()).hexdigest() != plan.backend_hash:
                raise StaleFence("Tracking backend changed during receipt recovery")
            tracked = native.get_run(recovery[1])
            validate_tracking_run(tracked, recovery[2])
        except MlflowException:
            from automl_api.training.pipeline import TrackingCreationUnresolved

            raise TrackingCreationUnresolved(
                "Tracking native receipt verification is unavailable"
            ) from None
        with factory() as db, db.begin():
            current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
            attempt, run = _rows(
                db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
            )
            complete = _stored_result(db, attempt, run) is not None
            _authorize(db, attempt, run, request, proof, current, captured, complete=complete)
            command = _known_command(
                db,
                run,
                attempt,
                request.role,
                current.source_plan.parent_id,
                current.source_plan.backend_hash,
            )
            if (
                command.id != recovery[0]
                or command.response_payload["run_id"] != recovery[1]
                or tracking_identity(command, run) != recovery[2]
            ):
                raise StaleFence("Tracking receipt changed during native verification")
            _record_receipt(db, attempt, run, request, command)
            return _reply(db, attempt, run, request, current.source_plan)

    def live_run(db):
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        if _stored_result(db, attempt, run) is not None:
            raise StaleFence("Completed training cannot create new tracking work")
        if _authorize(db, attempt, run, request, proof, current, captured, complete=False) is None:
            raise StaleFence("Tracking producer ownership disappeared")
        return run

    def record_receipt(db, run, command):
        db.flush()
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        _authorize(db, attempt, run, request, proof, current, captured, complete=False)
        _record_receipt(db, attempt, run, request, command)

    def native_client():
        client = MlflowClient(tracking_uri=get_settings().mlflow_tracking_uri)
        if plan.parent_id is not None:
            parent = client.get_run(plan.parent_id)
            if parent.info.run_id != plan.parent_id:
                raise StaleFence("Native tracking parent identity changed")
            validate_tracking_run(parent, plan.parent_identity)
        return client

    try:
        ensure_tracking_run(
            request.run_id,
            factory=factory,
            live_run=live_run,
            client_factory=native_client,
            tracking_uri=get_settings().mlflow_tracking_uri,
            attempt_id=str(attempt_id),
            role=request.role,
            run_name=plan.run_name,
            parent_run_id=plan.parent_id,
            tags=plan.tags,
            record_receipt=record_receipt,
        )
    except MlflowException:
        from automl_api.training.pipeline import TrackingCreationUnresolved

        raise TrackingCreationUnresolved("Tracking native verification is unavailable") from None
    with factory() as db, db.begin():
        current = resolver(db, attempt_id, lock=True, expected_project=captured.project_id)
        attempt, run = _rows(
            db, attempt_id, request.run_id, lock=True, expected_project=current.project_id
        )
        _authorize(db, attempt, run, request, proof, current, captured, complete=False)
        return _reply(db, attempt, run, request, current.source_plan)


def requires_tracking_receipts(db, attempt, run):
    from automl_api.services.reconciler import _ray_credential_protocol

    entries = [
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
    if len(entries) > 1:
        raise StaleFence("Tracking references have ambiguous saved declarations")
    if not entries:
        return False
    required = _ray_credential_protocol(entries[0]) in (
        RAY_TRAINING_TRACKING_PROTOCOL,
        RAY_TRAINING_METADATA_PROTOCOL,
        RAY_CREDENTIAL_PROTOCOL,
    )
    if required and entries[0].payload.get("fencing_token") != attempt.fencing_token:
        raise StaleFence("Tracking reference declaration fence changed")
    return required


def tracking_reference(db, attempt, run, role, *, tracking_id=None, live=False):
    """Read original custody only; never invent a missing tracking acknowledgement."""
    from automl_api.schemas.training_tracking import TrainingTrackingReference
    from automl_api.training.pipeline import TrackingCreationUnresolved

    if not requires_tracking_receipts(db, attempt, run):
        raise StaleFence("Tracking reference requires its original saved protocol")
    request = TrainingTrackingRequest(run_id=run.id, role=role)
    backend_hash = hashlib.sha256(get_settings().mlflow_tracking_uri.encode()).hexdigest()
    try:
        parent_id = None
        if role != "parent":
            parent = _known_command(db, run, attempt, "parent", None, backend_hash)
            _published(
                db,
                attempt,
                run,
                TrainingTrackingRequest(run_id=run.id, role="parent"),
                parent,
                required=True,
            )
            parent_id = parent.response_payload["run_id"]
        command = _known_command(db, run, attempt, role, parent_id, backend_hash)
        _published(db, attempt, run, request, command, required=True)
    except TrackingCreationUnresolved:
        raise StaleFence("Tracking reference has no original verified receipt") from None
    owner = _owner(db, attempt, run, request)
    if live:
        scope = _tracking_scope(
            db, attempt.id, request=request, lock=True, expected_project=run.project_id
        )
        if owner.payload["binding_digest"] != scope.binding_digest:
            raise StaleFence("Tracking reference source authority changed")
    if role == "parent":
        publisher = _publisher(db, attempt, run)
        if publisher is None or publisher.payload["pod_uid"] != owner.payload["pod_uid"]:
            raise StaleFence("Parent tracking reference belongs to another publisher")
    if tracking_id is not None and command.response_payload["run_id"] != tracking_id:
        raise StaleFence("Tracking ID differs from its original receipt")
    event = _event(db, attempt.id, PUBLISHED + role)
    return TrainingTrackingReference(
        attempt_id=attempt.id,
        command_id=command.id,
        role=role,
        tracking_run_id=command.response_payload["run_id"],
        owner_digest=owner.result_digest,
        receipt_digest=event.result_digest,
    ).model_dump(mode="json")


def validate_tracking_reference(db, run, reference, role):
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.schemas.training_tracking import TrainingTrackingReference

    reference = TrainingTrackingReference.model_validate(reference).model_dump(mode="json")
    attempt = db.get(WorkflowAttempt, uuid.UUID(reference["attempt_id"]), populate_existing=True)
    if (
        attempt is None
        or attempt.project_id != run.project_id
        or attempt.model_run_id != run.id
        or reference["role"] != role
    ):
        raise StaleFence("Tracking reference belongs to another scope or role")
    if (
        tracking_reference(db, attempt, run, role, tracking_id=reference["tracking_run_id"])
        != reference
    ):
        raise StaleFence("Tracking reference changed after model reservation")
    return reference


def require_parent_tracking(db, attempt, run, tracking_id, *, live=False):
    if requires_tracking_receipts(db, attempt, run):
        if not tracking_id:
            raise StaleFence("Parent publication has no tracking ID")
        reference = tracking_reference(
            db, attempt, run, "parent", tracking_id=tracking_id, live=live
        )
        from automl_api.services.training_metadata import metadata_reference

        metadata_reference(db, attempt, run, "parent", reference)
        return reference
    return None


def require_run_parent_tracking(db, run, tracking_id, *, context=None, live=False):
    import os

    from automl_api.models.enums import WorkflowStage
    from automl_api.models.workflows import WorkflowAttempt

    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == run.project_id,
            WorkflowAttempt.model_run_id == run.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    if attempt is None or not requires_tracking_receipts(db, attempt, run):
        return
    expected = (
        context
        if context is not None
        else (os.getenv("AUTOML_ATTEMPT_ID"), os.getenv("AUTOML_FENCING_TOKEN"))
    )
    if str(expected[0]) != str(attempt.id) or expected[1] != attempt.fencing_token:
        raise StaleFence("Tracking parent publication has no current fenced owner")
    require_parent_tracking(db, attempt, run, tracking_id, live=live)
    return attempt
