from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

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
    if tracked.info.lifecycle_stage != "active" or any(
        tracked.data.tags.get(key) != value for key, value in identity.items()
    ):
        raise ValueError("The MLflow run no longer matches its durable tracking lineage.")


def reconcile_tracking_once(session_factory, *, client=None, limit=25, now=None) -> int:
    from mlflow import MlflowClient
    from mlflow.entities import ViewType

    observed_at = now or datetime.now(UTC)
    resolved = 0
    for _ in range(limit):
        with session_factory() as db:
            # Skip contention on either row before applying the batch limit.
            claimed = db.execute(
                select(WorkflowCommand, ModelRun)
                .join(
                    ModelRun,
                    (ModelRun.id == WorkflowCommand.resource_id)
                    & (ModelRun.project_id == WorkflowCommand.project_id),
                )
                .where(
                    WorkflowCommand.operation == "mlflow.run.ensure",
                    WorkflowCommand.status.in_([CommandStatus.PENDING, CommandStatus.RUNNING]),
                    WorkflowCommand.available_at <= observed_at,
                    WorkflowCommand.response_payload["creation_started"].as_boolean().is_(True),
                )
                .order_by(WorkflowCommand.available_at, WorkflowCommand.id)
                .with_for_update(of=(ModelRun, WorkflowCommand), skip_locked=True)
                .limit(1)
                .execution_options(populate_existing=True)
            ).one_or_none()
            if claimed is None:
                break
            command, run = claimed
            client = client or MlflowClient(tracking_uri=get_settings().mlflow_tracking_uri)
            transition_command(command, CommandStatus.RUNNING)
            try:
                if command.retry_count >= command.max_retries:
                    raise ValueError("Tracking lookup retry budget exhausted.")
                command.retry_count += 1
                if (
                    command.request_payload["tracking_backend_hash"]
                    != hashlib.sha256(client.tracking_uri.encode()).hexdigest()
                ):
                    raise ValueError("Tracking backend differs from the committed intent.")
                identity = tracking_identity(command, run)
                experiment = client.get_experiment_by_name(f"automl-project-{run.project_id}")
                matches = (
                    []
                    if experiment is None
                    else client.search_runs(
                        [experiment.experiment_id],
                        filter_string=f"tags.automl_tracking_command = '{command.id}'",
                        run_view_type=ViewType.ALL,
                        max_results=2,
                    )
                )
                if len(matches) != 1:
                    raise ValueError("Tracking outcome is unresolved or has multiple matches.")
                validate_tracking_run(matches[0], identity)
                command.response_payload = {"run_id": matches[0].info.run_id}
                command.response_status = 200
                command.terminal_reason = None
                transition_command(command, CommandStatus.SUCCEEDED)
                resolved += 1
            except Exception as exc:
                command.terminal_reason = redact_text(f"Tracking outcome unresolved: {exc}")[:2000]
                command.available_at = observed_at + timedelta(seconds=60)
                if command.retry_count >= command.max_retries:
                    transition_command(command, CommandStatus.FAILED)
            db.commit()
    return resolved
