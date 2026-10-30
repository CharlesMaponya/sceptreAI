import sys
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from analysis_submission_helpers import reconcile_entry as reconcile_analysis_entry
from analysis_submission_helpers import submit_analysis_job
from automl_api.core.config import Settings
from automl_api.models.enums import AttemptStatus, OutboxStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import reconciler
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import worker
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope  # noqa: F401
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_validation import _estimate


def create_legacy_delivery(engine, run_id, old_id, k8s, *, suspend=False):
    with Session(engine) as db, db.begin():
        db.delete(db.get(WorkflowAttempt, old_id))
        db.flush()
        run = db.get(ModelRun, run_id)
        run.tags = {"desired_state": "kubernetes_submission_pending"}
        run.status = RunStatus.QUEUED
        run.k8s_namespace = k8s.settings.training_namespace
        manifest = k8s.build_job_manifest(
            run_id=run_id, project_id=run.project_id, estimate=_estimate()
        )
        manifest["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "previous-analysis:unfenced"
        )
        if suspend:
            manifest["spec"]["suspend"] = True
        run.k8s_job_name = manifest["metadata"]["name"]
        command = WorkflowCommand(
            project_id=run.project_id,
            actor_id=run.created_by_id,
            operation=f"{run.run_kind.value}.launch",
            idempotency_key=uuid.uuid4().hex,
            request_hash=canonical_request_hash({}),
            resource_type="model_run",
            resource_id=run.id,
        )
        db.add(command)
        db.flush()
        entry = OutboxEntry(
            project_id=run.project_id,
            command_id=command.id,
            event_key="legacy-analysis",
            topic="kubernetes.analysis.submit",
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"run_id": str(run.id), "manifest": manifest},
            status=OutboxStatus.CLAIMED,
            delivery_attempts=1,
            lease_owner="migration",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
        )
        db.add(entry)
        db.flush()
        return entry.id, manifest


@pytest.fixture
def legacy(analysis_scope, monkeypatch):  # noqa: F811
    engine, run_id, old_id, _, _, _, _, _ = analysis_scope
    binder = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    binder.settings = Settings(
        environment="production",
        training_namespace="legacy-test",
        attempt_control_base_url="https://control.invalid/internal/attempts",
    )
    k8s = MagicMock()
    k8s.settings = binder.settings
    k8s.build_job_manifest = binder.build_job_manifest
    k8s.bind_analysis_attempt_manifest = binder.bind_analysis_attempt_manifest
    k8s.batch.read_namespaced_job.side_effect = ApiException(status=404)
    k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    k8s.capacity_snapshot.return_value.capacity = SimpleNamespace(
        connected=True, active_training_jobs=0
    )

    def create(manifest):
        return client.V1Job(
            metadata=client.V1ObjectMeta(
                name=manifest["metadata"]["name"],
                namespace=manifest["metadata"]["namespace"],
                uid="new-physical-job",
            )
        )

    k8s.create_job.side_effect = create
    entry_id, manifest = create_legacy_delivery(engine, run_id, old_id, k8s)
    monkeypatch.setattr(reconciler, "get_settings", lambda: k8s.settings)
    return engine, run_id, entry_id, manifest, k8s


def test_first_legacy_delivery_commits_tracking_before_job_creation(legacy):
    engine, run_id, entry_id, manifest, k8s = legacy
    with Session(engine) as db, db.begin():
        reconcile_analysis_entry(db, db.get(OutboxEntry, entry_id), worker_id="migration", k8s=k8s)
    k8s.create_job.assert_not_called()
    k8s.ensure_service_account.assert_not_called()
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        entry = db.get(OutboxEntry, entry_id)
        attempt = db.get(WorkflowAttempt, run.tags["workflow_attempt_id"])
        assert attempt.status == AttemptStatus.PENDING
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        assert entry.payload["manifest"]["metadata"]["name"] != manifest["metadata"]["name"]
        command = entry.payload["manifest"]["spec"]["template"]["spec"]["containers"][0]["command"]
        assert command[-2:] == ["--attempt-protocol", "analysis-v1"]
        assert (
            entry.payload["manifest"]["spec"]["template"]["spec"]["containers"][0]["image"]
            == k8s.settings.training_image
        )
        assert (
            manifest["spec"]["template"]["spec"]["containers"][0]["image"]
            == "previous-analysis:unfenced"
        )
        event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
        assert event.event_type == "analysis.legacy.upgraded"
        attempt_id = attempt.id
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "migration"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        entry.delivery_attempts = 1
        db.flush()
        reconcile_analysis_entry(db, entry, worker_id="migration", k8s=k8s)
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        reconcile_analysis_entry(db, entry, worker_id="migration", k8s=k8s)
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.SUBMITTED
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id)
                    )
                )
            )
            == 1
        )
    k8s.create_job.assert_called_once()


@pytest.mark.parametrize(
    "invalid",
    [
        "retry",
        "replay",
        "running",
        "submitted",
        "moved",
        "scope",
        "job",
        "pod",
        "entrypoint",
        "gpu",
        "report",
    ],
)
def test_ambiguous_legacy_work_never_hands_off(legacy, invalid):
    engine, run_id, entry_id, manifest, k8s = legacy
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        run = db.get(ModelRun, run_id)
        if invalid == "retry":
            entry.delivery_attempts = 2
        elif invalid == "replay":
            db.get(WorkflowCommand, entry.command_id).replayed_by = run.created_by_id
        elif invalid == "running":
            run.status = RunStatus.RUNNING
        elif invalid == "submitted":
            run.tags = {"desired_state": "kubernetes_submitted"}
        elif invalid == "moved":
            run.tags = {**run.tags, "workflow_attempt_id": str(uuid.uuid4())}
        elif invalid == "gpu":
            run.gpu_requested = True
        elif invalid == "report":
            run.tags = {**run.tags, "artifact_uri": "s3://existing/report.json"}
        elif invalid == "scope":
            changed = {
                **manifest,
                "metadata": {**manifest["metadata"], "namespace": "another-namespace"},
            }
            entry.payload = {**entry.payload, "manifest": changed}
        elif invalid == "job":
            k8s.batch.read_namespaced_job.side_effect = None
            k8s.batch.read_namespaced_job.return_value = client.V1Job()
        elif invalid == "pod":
            k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[object()])
        else:
            changed = deepcopy(manifest)
            changed["spec"]["template"]["spec"]["containers"][0]["command"] = [
                "bash",
                "-c",
                "unfenced-work",
            ]
            entry.payload = {**entry.payload, "manifest": changed}
    with Session(engine) as db, pytest.raises((StaleFence, ValueError)):
        submit_analysis_job(db, db.get(OutboxEntry, entry_id), k8s)
    k8s.create_job.assert_not_called()
    k8s.ensure_service_account.assert_not_called()
    with Session(engine) as db:
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        )
        assert (
            db.get(OutboxEntry, entry_id)
            .payload["manifest"]["metadata"]
            .get("labels", {})
            .get("automl.platform/attempt-id")
            is None
        )


def test_migration_rolls_back_as_one_unit(legacy):
    engine, run_id, entry_id, manifest, k8s = legacy
    with Session(engine) as db:
        submit_analysis_job(db, db.get(OutboxEntry, entry_id), k8s)
        db.rollback()
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).payload["manifest"] == manifest
        assert "workflow_attempt_id" not in db.get(ModelRun, run_id).tags
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        )


def test_bound_analysis_refuses_missing_worker_protocol(analysis_scope, monkeypatch):  # noqa: F811
    engine, run_id, attempt_id, _, _, _, _, post = analysis_scope
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(sys, "argv", ["worker", "--run-id", str(run_id)])
    with pytest.raises(StaleFence, match="protocol"):
        worker.main()
    post.assert_not_called()
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.QUEUED
        assert db.get(WorkflowAttempt, attempt_id).lease_owner is None


def test_unknown_worker_protocol_is_rejected_before_start(analysis_scope, monkeypatch):  # noqa: F811
    _, run_id, _, _, _, _, _, post = analysis_scope
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(
        sys, "argv", ["worker", "--run-id", str(run_id), "--attempt-protocol", "unknown-v0"]
    )
    with pytest.raises(SystemExit) as exc:
        worker.main()
    assert exc.value.code == 2
    post.assert_not_called()


@pytest.mark.parametrize("unavailable", ["job", "pods", "transport"])
def test_readonly_preflight_failure_defers_without_spending_first_delivery(legacy, unavailable):
    engine, run_id, entry_id, _, k8s = legacy
    if unavailable == "job":
        k8s.batch.read_namespaced_job.side_effect = ApiException(status=503)
    elif unavailable == "transport":
        from urllib3.exceptions import HTTPError

        k8s.batch.read_namespaced_job.side_effect = HTTPError("transport unavailable")
    else:
        k8s.core.list_namespaced_pod.side_effect = ApiException(status=503)
    with Session(engine) as db, db.begin():
        reconcile_analysis_entry(db, db.get(OutboxEntry, entry_id), worker_id="migration", k8s=k8s)
    with Session(engine) as db:
        entry = db.get(OutboxEntry, entry_id)
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        assert "preflight unavailable" in entry.last_error
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        )
    k8s.create_job.assert_not_called()


def test_protocol_without_attempt_context_is_rejected(analysis_scope, monkeypatch):  # noqa: F811
    _, run_id, _, _, _, _, _, post = analysis_scope
    monkeypatch.delenv("AUTOML_ATTEMPT_ID")
    monkeypatch.delenv("AUTOML_FENCING_TOKEN")
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(
        sys, "argv", ["worker", "--run-id", str(run_id), "--attempt-protocol", "analysis-v1"]
    )
    with pytest.raises(StaleFence, match="context"):
        worker.main()
    post.assert_not_called()
