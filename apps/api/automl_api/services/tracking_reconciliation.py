from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import or_, select, update

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.models.enums import CommandStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.services.workflow_state import transition_command


def tracking_identity(command: WorkflowCommand, run: ModelRun) -> dict[str, str]:
    payload = command.request_payload
    if command.project_id != run.project_id or payload.get("model_run_id") != str(run.id):
        raise ValueError("The tracking command does not belong to this run.")
    return {
        "automl_tracking_command": str(command.id),
        "automl_run_id": str(run.id),
        "project_id": str(run.project_id),
        "dataset_version_id": str(run.dataset_version_id),
        "automl_attempt_id": payload["attempt_id"],
        "automl_tracking_role": payload["role"],
        **(
            {"mlflow.parentRunId": payload["parent_run_id"]} if payload.get("parent_run_id") else {}
        ),
    }


def validate_tracking_run(tracked, identity: dict[str, str]) -> None:
    if (
        tracked.info.lifecycle_stage != "active"
        or tracked.data.tags.get("mlflow.parentRunId") != identity.get("mlflow.parentRunId")
        or any(tracked.data.tags.get(key) != value for key, value in identity.items())
    ):
        raise ValueError("The MLflow run no longer matches its durable tracking lineage.")


LEASE_SECONDS = 90
HEARTBEAT_SECONDS = 15


@dataclass(frozen=True)
class _TrackingClaim:
    command_id: uuid.UUID
    project_id: uuid.UUID
    resource_id: uuid.UUID
    owner: str
    identity: dict[str, str]
    payload: Any
    request_hash: str
    budget_exhausted: bool
    scope_error: str | None


def _claim_tracking(session_factory, now):
    with session_factory() as db:
        claimed = db.execute(
            select(WorkflowCommand, ModelRun)
            .join(
                ModelRun,
                (ModelRun.id == WorkflowCommand.resource_id)
                & (ModelRun.project_id == WorkflowCommand.project_id),
            )
            .where(
                WorkflowCommand.operation == "mlflow.run.ensure",
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.status.in_([CommandStatus.PENDING, CommandStatus.RUNNING]),
                WorkflowCommand.available_at <= now,
                WorkflowCommand.response_payload.contains({"creation_started": True}),
                or_(
                    WorkflowCommand.lease_expires_at.is_(None),
                    WorkflowCommand.lease_expires_at <= now,
                ),
            )
            .order_by(WorkflowCommand.available_at, WorkflowCommand.id)
            .with_for_update(of=(ModelRun, WorkflowCommand), skip_locked=True)
            .limit(1)
            .execution_options(populate_existing=True)
        ).one_or_none()
        if claimed is None:
            return None
        command, run = claimed
        transition_command(command, CommandStatus.RUNNING)
        exhausted = command.retry_count >= command.max_retries
        if not exhausted:
            command.retry_count += 1
        command.lease_owner = "tracking-" + uuid.uuid4().hex
        command.heartbeat_at = now
        command.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
        identity, scope_error = {}, None
        try:
            identity = tracking_identity(command, run)
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            scope_error = str(exc)
        claim = _TrackingClaim(
            command.id,
            command.project_id,
            command.resource_id,
            command.lease_owner,
            identity,
            deepcopy(command.request_payload),
            command.request_hash,
            exhausted,
            scope_error,
        )
        db.commit()
        return claim


def _renew_tracking(session_factory, claim, now):
    with session_factory() as db:
        result = db.execute(
            update(WorkflowCommand)
            .where(
                WorkflowCommand.id == claim.command_id,
                WorkflowCommand.project_id == claim.project_id,
                WorkflowCommand.resource_id == claim.resource_id,
                WorkflowCommand.operation == "mlflow.run.ensure",
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.status == CommandStatus.RUNNING,
                WorkflowCommand.lease_owner == claim.owner,
                WorkflowCommand.lease_expires_at > now,
                WorkflowCommand.request_hash == claim.request_hash,
                WorkflowCommand.request_payload == claim.payload,
            )
            .values(heartbeat_at=now, lease_expires_at=now + timedelta(seconds=LEASE_SECONDS))
        )
        db.commit()
        return result.rowcount == 1


def _release_tracking(session_factory, claim):
    with session_factory() as db:
        db.execute(
            update(WorkflowCommand)
            .where(
                WorkflowCommand.id == claim.command_id,
                WorkflowCommand.project_id == claim.project_id,
                WorkflowCommand.resource_id == claim.resource_id,
                WorkflowCommand.operation == "mlflow.run.ensure",
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.status == CommandStatus.RUNNING,
                WorkflowCommand.lease_owner == claim.owner,
                WorkflowCommand.request_hash == claim.request_hash,
                WorkflowCommand.request_payload == claim.payload,
            )
            .values(lease_owner=None, lease_expires_at=None)
        )
        db.commit()


@contextmanager
def _tracking_heartbeat(session_factory, claim, clock):
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                if not _renew_tracking(session_factory, claim, clock()):
                    return
            except Exception:
                # Publication independently checks the same live lease and immutable scope.
                return

    thread = threading.Thread(target=heartbeat, name="tracking-lookup-heartbeat", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=1)


def _publish_tracking(session_factory, claim, now, *, tracked=None, error=None):
    with session_factory() as db:
        current = db.execute(
            select(WorkflowCommand, ModelRun)
            .join(
                ModelRun,
                (ModelRun.id == WorkflowCommand.resource_id)
                & (ModelRun.project_id == WorkflowCommand.project_id),
            )
            .where(
                WorkflowCommand.id == claim.command_id,
                WorkflowCommand.project_id == claim.project_id,
                WorkflowCommand.resource_id == claim.resource_id,
                WorkflowCommand.operation == "mlflow.run.ensure",
                WorkflowCommand.resource_type == "model_run",
                WorkflowCommand.status == CommandStatus.RUNNING,
                WorkflowCommand.lease_owner == claim.owner,
                WorkflowCommand.lease_expires_at > now,
                WorkflowCommand.response_payload.contains({"creation_started": True}),
            )
            .with_for_update(of=(ModelRun, WorkflowCommand), skip_locked=True)
            .execution_options(populate_existing=True)
        ).one_or_none()
        if current is None:
            return False
        command, run = current
        if command.request_hash != claim.request_hash or json.dumps(
            command.request_payload, sort_keys=True
        ) != json.dumps(claim.payload, sort_keys=True):
            return False
        if claim.scope_error is not None:
            if error is None:
                return False
        else:
            try:
                if tracking_identity(command, run) != claim.identity:
                    return False
            except (ValueError, KeyError, TypeError, AttributeError):
                return False
        if error is None:
            validate_tracking_run(tracked, claim.identity)
            command.response_payload = {"run_id": tracked.info.run_id}
            command.response_status = 200
            command.terminal_reason = None
            transition_command(command, CommandStatus.SUCCEEDED)
        else:
            command.terminal_reason = redact_text(f"Tracking outcome unresolved: {error}")[:2000]
            command.available_at = now + timedelta(seconds=60)
            if command.retry_count >= command.max_retries:
                transition_command(command, CommandStatus.FAILED)
        command.lease_owner = None
        command.lease_expires_at = None
        command.heartbeat_at = now
        db.commit()
        return error is None


def reconcile_tracking_once(session_factory, *, client=None, limit=25, now=None) -> int:
    from mlflow import MlflowClient
    from mlflow.entities import ViewType

    observed_at = now or datetime.now(UTC)
    started = time.monotonic()

    def clock():
        return observed_at + timedelta(seconds=time.monotonic() - started)

    resolved = 0
    for _ in range(limit):
        claim = _claim_tracking(session_factory, clock())
        if claim is None:
            break
        tracked, error = None, None
        with _tracking_heartbeat(session_factory, claim, clock):
            try:
                if claim.budget_exhausted:
                    raise ValueError("Tracking lookup retry budget exhausted.")
                if claim.scope_error is not None:
                    raise ValueError(claim.scope_error)
                client = client or MlflowClient(tracking_uri=get_settings().mlflow_tracking_uri)
                if (
                    claim.payload["tracking_backend_hash"]
                    != hashlib.sha256(client.tracking_uri.encode()).hexdigest()
                ):
                    raise ValueError("Tracking backend differs from the committed intent.")
                experiment = client.get_experiment_by_name(
                    f"automl-project-{claim.identity['project_id']}"
                )
                matches = (
                    []
                    if experiment is None
                    else client.search_runs(
                        [experiment.experiment_id],
                        filter_string=f"tags.automl_tracking_command = '{claim.command_id}'",
                        run_view_type=ViewType.ALL,
                        max_results=2,
                    )
                )
                if len(matches) != 1:
                    raise ValueError("Tracking outcome is unresolved or has multiple matches.")
                tracked = matches[0]
                validate_tracking_run(tracked, claim.identity)
            except Exception as exc:
                error = exc
        resolved += int(
            _publish_tracking(session_factory, claim, clock(), tracked=tracked, error=error)
        )
    return resolved


def ensure_tracking_run(
    run_id,
    *,
    factory,
    live_run,
    client_factory,
    tracking_uri,
    attempt_id,
    role,
    run_name,
    parent_run_id=None,
    tags=None,
    record_receipt=None,
):
    """Commit exclusive ownership before native creation; never repeat an unknown write."""
    import os

    import mlflow

    from automl_api.services.workflow_state import StaleFence, begin_command, canonical_request_hash
    from automl_api.training.pipeline import TrackingCreationUnresolved

    # MLflow retries POST creation internally; only the durable command may retry it.
    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"
    payload = {
        "model_run_id": str(run_id),
        "attempt_id": attempt_id,
        "role": role,
        "parent_run_id": parent_run_id,
        "tracking_backend_hash": hashlib.sha256(tracking_uri.encode()).hexdigest(),
    }
    claim = None

    try:
        with factory() as db:
            current = live_run(db)
            command, replayed = begin_command(
                db,
                project_id=current.project_id,
                actor_id=current.created_by_id,
                operation="mlflow.run.ensure",
                idempotency_key=canonical_request_hash(
                    {key: payload[key] for key in ("model_run_id", "attempt_id", "role")}
                ),
                payload=payload,
            )
            if not replayed:
                command.resource_type, command.resource_id = "model_run", current.id
            if (
                command.resource_type != "model_run"
                or command.resource_id != current.id
                or command.status == CommandStatus.CANCELLED
            ):
                raise StaleFence("The tracking command scope no longer belongs to this run.")
            identity = tracking_identity(command, current)
            request_hash = command.request_hash
            response = command.response_payload
            if not isinstance(response, dict):
                raise TrackingCreationUnresolved(
                    "MLflow creation is unresolved; invalid stored state."
                )
            known_id = response.get("run_id")
            known = set(response) == {"run_id"} and isinstance(known_id, str) and bool(known_id)
            uncertain = (
                set(response) == {"creation_started"} and response["creation_started"] is True
            )
            if not (known or uncertain or response == {}):
                raise TrackingCreationUnresolved(
                    "MLflow creation is unresolved; invalid stored state."
                )
            if not known:
                now = datetime.now(UTC)
                active = command.lease_expires_at is not None and command.lease_expires_at > now
                owner = command.lease_owner or ""
                # A live parent may supersede read-only recovery, never another writer.
                if active and (
                    not uncertain
                    or not owner.startswith("tracking-")
                    or owner.startswith("tracking-writer-")
                ):
                    raise TrackingCreationUnresolved(
                        "MLflow creation is unresolved; another writer owns it."
                    )
                if command.status == CommandStatus.FAILED:
                    transition_command(command, CommandStatus.PENDING)
                transition_command(command, CommandStatus.RUNNING)
                command.lease_owner = "tracking-writer-" + uuid.uuid4().hex
                command.heartbeat_at = now
                command.lease_expires_at = now + timedelta(seconds=LEASE_SECONDS)
                claim = _TrackingClaim(
                    command.id,
                    command.project_id,
                    command.resource_id,
                    command.lease_owner,
                    identity,
                    dict(payload),
                    request_hash,
                    False,
                    None,
                )
            command_id = command.id
            db.commit()

        def owned(db):
            current = live_run(db)
            command = db.get(
                WorkflowCommand, command_id, with_for_update=True, populate_existing=True
            )
            if (
                command is None
                or command.project_id != current.project_id
                or command.resource_type != "model_run"
                or command.resource_id != current.id
                or command.operation != "mlflow.run.ensure"
                or command.request_hash != request_hash
                or canonical_request_hash(command.request_payload)
                != canonical_request_hash(payload)
                or tracking_identity(command, current) != identity
                or canonical_request_hash(command.response_payload)
                != canonical_request_hash(response)
            ):
                raise StaleFence("The tracking command changed while remote I/O was in flight.")
            if claim is not None:
                if (
                    command.status != CommandStatus.RUNNING
                    or command.lease_owner != claim.owner
                    or command.lease_expires_at is None
                    or command.lease_expires_at <= datetime.now(UTC)
                ):
                    raise StaleFence("The tracking writer no longer owns its live lease.")
            elif command.status == CommandStatus.CANCELLED:
                raise StaleFence("The tracking command was cancelled during remote I/O.")
            return current, command

        client = client_factory()
        if (
            hashlib.sha256(client.tracking_uri.encode()).hexdigest()
            != payload["tracking_backend_hash"]
        ):
            raise ValueError("Tracking backend differs from the committed intent.")
        if known:
            tracked = client.get_run(known_id)
            validate_tracking_run(tracked, identity)
        else:
            with _tracking_heartbeat(factory, claim, lambda: datetime.now(UTC)):
                experiment_name = f"automl-project-{identity['project_id']}"
                experiment = client.get_experiment_by_name(experiment_name)
                if experiment is None:
                    if uncertain:
                        raise TrackingCreationUnresolved(
                            "MLflow creation is unresolved; its experiment is unavailable."
                        )
                    with factory() as db:
                        owned(db)
                        db.commit()
                    try:
                        experiment_id = client.create_experiment(experiment_name)
                    except mlflow.exceptions.MlflowException:
                        experiment = client.get_experiment_by_name(experiment_name)
                        if experiment is None:
                            raise
                        experiment_id = experiment.experiment_id
                else:
                    experiment_id = experiment.experiment_id
                matches = client.search_runs(
                    [experiment_id],
                    filter_string=f"tags.automl_tracking_command = '{command_id}'",
                    run_view_type=mlflow.entities.ViewType.ALL,
                    max_results=2,
                )
                if len(matches) > 1:
                    raise ValueError("Multiple MLflow runs claim the same tracking command.")
                if matches:
                    tracked = matches[0]
                else:
                    if uncertain:
                        raise TrackingCreationUnresolved(
                            "MLflow creation is unresolved; refusing a duplicate request."
                        )
                    with factory() as db:
                        _, command = owned(db)
                        response = {"creation_started": True}
                        command.response_payload = response
                        db.commit()
                    try:
                        tracked = client.create_run(
                            experiment_id, run_name=run_name, tags={**(tags or {}), **identity}
                        )
                    except Exception as exc:
                        raise TrackingCreationUnresolved(
                            "MLflow creation outcome is unknown; refusing an unverified retry."
                        ) from exc
                validate_tracking_run(tracked, identity)

        with factory() as db:
            current, command = owned(db)
            command.response_payload = {"run_id": tracked.info.run_id}
            command.response_status = 200
            command.terminal_reason = None
            command.lease_owner = None
            command.lease_expires_at = None
            if command.status != CommandStatus.SUCCEEDED:
                if command.status == CommandStatus.FAILED:
                    transition_command(command, CommandStatus.PENDING)
                transition_command(command, CommandStatus.RUNNING)
                transition_command(command, CommandStatus.SUCCEEDED)
            if role == "parent":
                current.mlflow_run_id = tracked.info.run_id
            if record_receipt is not None:
                record_receipt(db, current, command)
            db.commit()
            tracking_id = tracked.info.run_id
        return tracking_id
    except Exception:
        if claim is not None:
            try:
                _release_tracking(factory, claim)
            except Exception:
                # A lost database connection leaves the committed lease to expire.
                pass
        raise
