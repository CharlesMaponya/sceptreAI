"""Analysis workers must accept the durable command emitted by the real routes."""

import uuid
from datetime import UTC, datetime

from automl_api import workflow_reconciler
from automl_api.api.deps import get_current_user
from automl_api.api.routes import operations, validation
from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus, OutboxStatus, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.schemas.operations import DriftLaunchRead
from automl_api.schemas.training import ModelRunRead
from automl_api.schemas.validation import AnalysisLaunchRead
from automl_api.services import reconciler
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes import client
from sqlalchemy import select
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_migration import legacy as legacy
from test_analysis_submission_factory import factory_case as factory_case
from test_analysis_submission_legacy import migration_case as migration_case
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication
from test_validation import _estimate


def test_actual_http_command_dispatches_observes_and_retires_original_job(
    factory_case, monkeypatch
):
    c = factory_case
    with c.factory() as db, db.begin():
        db.get(OutboxEntry, c.entry_id).status = OutboxStatus.DELIVERED
        run = db.get(ModelRun, c.run_id)
        kind, version_id = run.run_kind.value, run.dataset_version_id
        run_read = ModelRunRead.model_validate(run)
    app = FastAPI()
    app.include_router(validation.router)
    app.include_router(operations.router)

    def database():
        with c.factory() as db:
            yield db

    def user():
        with c.factory() as db:
            return db.get(User, c.actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = user
    target = uuid.uuid4()
    if kind == "drift":
        result = DriftLaunchRead(run=run_read, estimate=_estimate(), manifest=c.manifest)
        monkeypatch.setattr(operations, "launch_drift_check", lambda *_args: result)
        path = f"/projects/{c.project_id}/operations/registry/{target}/drift"
        body = {"dataset_version_id": str(version_id)}
    else:
        result = AnalysisLaunchRead(run=run_read, manifest=c.manifest)
        name = "launch_validation_run" if kind == "validation" else "launch_explainability_run"
        monkeypatch.setattr(validation, name, lambda *_args: result)
        suffix = "validations" if kind == "validation" else "explanations"
        path = f"/projects/{c.project_id}/training/runs/{target}/{suffix}"
        body = {"model_name": "Ridge"}
        if kind == "validation":
            body["dataset_version_id"] = str(version_id)
    response = TestClient(app).post(
        path, json=body, headers={"Idempotency-Key": "actual-api-command"}
    )
    assert response.status_code == 202, response.text
    for _ in range(2):
        replay = TestClient(app).post(
            path, json=body, headers={"Idempotency-Key": "actual-api-command"}
        )
        assert replay.status_code == 202 and replay.json() == response.json()
    with c.factory() as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.idempotency_key == "actual-api-command",
            )
        )
        assert command.operation == command.resource_type == f"{kind}.launch"
        source = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == command.id))
        assert source.payload["run_id"] == str(c.run_id) and source.aggregate_type == "model_run"
        source.available_at = datetime(2000, 1, 1, tzinfo=UTC)
        c.entry_id = source.id
    assert workflow_reconciler.run_once(c.factory, worker_id="actual-api-submit") == 1
    with c.factory() as db, db.begin():
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).status == AttemptStatus.SUBMITTED
        db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED
    assert len(c.creations) == 1
    requests = []
    with c.factory() as db, db.begin():
        assert (
            reconciler.observe_analysis_attempts(
                db, c.k8s, attempt_id=c.attempt_id, read=requests.append
            )
            == 1
        )
        cleanup = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == c.run_id,
                OutboxEntry.topic == "kubernetes.analysis.cancel",
            )
        )
        assert cleanup is not None and cleanup.payload["uid"] == "owned-original-uid"
        cleanup.available_at = datetime(2000, 1, 1, tzinfo=UTC)
        cleanup_id = cleanup.id
    assert requests == []
    c.cleanup_phase = True
    c.k8s.core.list_namespaced_pod.return_value = client.V1PodList(items=[])
    deleted = []

    def delete(**kwargs):
        assert c.engine.pool.checkedout() == 0
        deleted.append(kwargs["body"].preconditions.uid)
        c.native = None
        return client.V1Status(status="Success")

    c.k8s.batch.delete_namespaced_job.side_effect = delete
    assert workflow_reconciler.run_once(c.factory, worker_id="actual-api-cleanup") == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, cleanup_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state == "completed"
    assert deleted == ["owned-original-uid"] and len(c.creations) == 1
    replay = TestClient(app).post(
        path, json=body, headers={"Idempotency-Key": "actual-api-command"}
    )
    assert replay.status_code == 202 and replay.json() == response.json()


def test_legacy_api_operation_label_migrates_before_native_creation(migration_case):
    c = migration_case
    with c.factory() as db, db.begin():
        source = db.get(OutboxEntry, c.entry_id)
        command = db.get(WorkflowCommand, source.command_id)
        command.resource_type = command.operation
    assert workflow_reconciler.run_once(c.factory, worker_id="api-legacy-qualify") == 1
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.run_id)
        assert (
            db.get(WorkflowAttempt, uuid.UUID(run.tags["workflow_attempt_id"])).status
            == AttemptStatus.PENDING
        )
        db.get(OutboxEntry, c.entry_id).available_at = datetime(2000, 1, 1, tzinfo=UTC)
    assert workflow_reconciler.run_once(c.factory, worker_id="api-legacy-create") == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    c.k8s.create_job.assert_called_once()
