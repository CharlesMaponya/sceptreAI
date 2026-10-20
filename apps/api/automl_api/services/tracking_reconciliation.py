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
