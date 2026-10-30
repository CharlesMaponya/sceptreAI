import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import analysis_observer_fixture
import pytest
from analysis_cleanup_helpers import cancel_analysis_job
from analysis_submission_helpers import reconcile_entry as reconcile_analysis_entry
from analysis_submission_helpers import submit_analysis_job
from automl_api.core.config import Settings
from automl_api.models.enums import AttemptStatus, OutboxStatus, RunStatus
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training import analysis, worker
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope  # noqa: F401
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_validation import _estimate


def create_delivery(engine, run_id, attempt_id, k8s, *, namespace="analysis-test", suspend=False):
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.status = AttemptStatus.PENDING
        run.k8s_namespace = namespace
        run.tags = {**run.tags, "desired_state": "kubernetes_submission_pending"}
        manifest = k8s.build_job_manifest(
            run_id=run_id, project_id=run.project_id, estimate=_estimate(), attempt=attempt
        )
        manifest["metadata"]["namespace"] = namespace
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
            resource_id=run_id,
        )
        db.add(command)
        db.flush()
        entry = OutboxEntry(
            project_id=run.project_id,
            command_id=command.id,
            event_key=f"analysis:{attempt_id}:initial-submit",
            topic="kubernetes.analysis.submit",
            aggregate_type="model_run",
            aggregate_id=run_id,
            payload={"run_id": str(run_id), "manifest": manifest},
        )
        db.add(entry)
        db.flush()
        submit_analysis_job(db, entry, k8s)
        return entry.id, manifest


@pytest.fixture
def recovery(analysis_scope):  # noqa: F811
    engine, run_id, attempt_id, fence, _, _, _, _ = analysis_scope
    binder = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    binder.settings = Settings(training_namespace="analysis-test")
    k8s = MagicMock()
    k8s.settings = binder.settings
    k8s.build_job_manifest = binder.build_job_manifest
    k8s.bind_analysis_attempt_manifest = binder.bind_analysis_attempt_manifest
    k8s.capacity_snapshot.return_value.capacity = SimpleNamespace(
        connected=True, active_training_jobs=0
    )
    jobs = {}

    def create(manifest):
        meta = manifest["metadata"]
        job = client.V1Job(
            metadata=client.V1ObjectMeta(
                name=meta["name"],
                namespace=meta["namespace"],
                uid=uuid.uuid4().hex,
                labels=deepcopy(meta["labels"]),
            ),
            status=client.V1JobStatus(active=1),
        )
        jobs[meta["name"]] = job
        return job

    def read(name, **_):
        if name not in jobs:
            raise ApiException(status=404)
        return jobs[name]

    k8s.create_job.side_effect = create
    k8s.batch.read_namespaced_job.side_effect = read
    source_id, manifest = create_delivery(engine, run_id, attempt_id, k8s)
    worker._begin_fenced_attempt(run_id, worker_id="old-worker")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    k8s.reset_mock()
    return engine, run_id, attempt_id, fence, k8s, source_id, manifest, jobs


def cleanup_entry(db, run_id):
    return db.scalar(
        select(OutboxEntry).where(
            OutboxEntry.aggregate_id == run_id, OutboxEntry.topic == "kubernetes.analysis.cancel"
        )
    )


def test_expiry_queues_exact_cleanup_and_fresh_generation_atomically(recovery):
    engine, run_id, old_id, fence, k8s, source_id, manifest, _ = recovery
    with Session(engine) as db, db.begin():
        assert analysis_observer_fixture.observe_analysis_attempts(db, k8s) == 1
    k8s.batch.read_namespaced_job.assert_not_called()
    k8s.delete_job.assert_not_called()
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        new = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
        )
        assert old.status == AttemptStatus.SUPERSEDED and old.terminal_cas_version == 1
        assert old.cleanup_state == "queued" and new.generation == 2 and new.fencing_token != fence
        run = db.get(ModelRun, run_id)
        assert run.tags["workflow_attempt_id"] == str(new.id) and run.status == RunStatus.QUEUED
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id)))
        assert len(entries) == 3
        replacement = next(
            e for e in entries if e.topic == "kubernetes.analysis.submit" and e.id != source_id
        )
        env = {
            e["name"]: e.get("value")
            for e in replacement.payload["manifest"]["spec"]["template"]["spec"]["containers"][0][
                "env"
            ]
        }
        assert (
            env["AUTOML_ATTEMPT_ID"] == str(new.id)
            and env["AUTOML_FENCING_TOKEN"] == new.fencing_token
        )
        assert replacement.payload["manifest"]["metadata"]["name"] != manifest["metadata"]["name"]
        assert cleanup_entry(db, run_id).payload["uid"]
    with Session(engine) as db, db.begin():
        assert analysis_observer_fixture.observe_analysis_attempts(db, k8s) == 0


def test_expiry_rollback_preserves_old_generation(recovery):
    engine, run_id, old_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db:
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
        db.rollback()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.RUNNING
        assert cleanup_entry(db, run_id) is None


def test_exhausted_budget_still_queues_cleanup(recovery):
    engine, run_id, old_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, old_id).retry_budget = 1
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.FAILED
        assert db.get(ModelRun, run_id).failure_code == "ANALYSIS_RETRY_EXHAUSTED"
        assert cleanup_entry(db, run_id) is not None
        assert (
            db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            is None
        )


@pytest.mark.parametrize("cause", ["missing", "failed", "complete", "startup"])
def test_job_exit_without_report_cannot_certify_success(recovery, cause):
    engine, run_id, old_id, _, k8s, _, manifest, jobs = recovery
    with Session(engine) as db, db.begin():
        old = db.get(WorkflowAttempt, old_id)
        old.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        if cause == "startup":
            old.lease_expires_at = None
            old.updated_at = datetime.now(UTC) - timedelta(minutes=6)
    job = jobs[manifest["metadata"]["name"]]
    if cause == "missing":
        jobs.clear()
    elif cause == "failed":
        job.status = client.V1JobStatus(failed=1)
    elif cause == "complete":
        job.status = client.V1JobStatus(succeeded=1)
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.SUPERSEDED
        assert db.get(ModelRun, run_id).status == RunStatus.QUEUED
        assert cleanup_entry(db, run_id) is not None


@pytest.mark.parametrize("state", ["cancelled", "failed", "moved"])
def test_terminal_or_moved_run_is_preserved_without_replacement(recovery, state):
    engine, run_id, old_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.failure_message = "control-plane decision"
        if state == "moved":
            run.tags = {**run.tags, "workflow_attempt_id": str(uuid.uuid4())}
        else:
            run.status = RunStatus.CANCELLED if state == "cancelled" else RunStatus.FAILED
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.CANCELLED
        assert db.get(ModelRun, run_id).failure_message == "control-plane decision"
        assert (
            db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            is None
        )


def test_committed_report_only_queues_cleanup(recovery):
    engine, run_id, old_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, old_id).lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
    analysis._persist_analysis_result(run_id, {"metrics": {"rmse": 1}})
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        old = db.get(WorkflowAttempt, old_id)
        assert old.status == AttemptStatus.SUCCEEDED and old.terminal_cas_version == 1
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert cleanup_entry(db, run_id) is not None
        assert (
            db.scalar(
                select(func.count())
                .select_from(RunArtifact)
                .where(RunArtifact.model_run_id == run_id)
            )
            == 1
        )
        assert (
            db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            is None
        )


@pytest.mark.parametrize(
    "invalid", ["fence", "project", "run", "uid", "labels", "evidence", "manifest"]
)
def test_cleanup_rejects_stale_or_changed_identity(recovery, invalid):
    engine, run_id, old_id, _, k8s, source_id, manifest, jobs = recovery
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        saved = cleanup_entry(db, run_id)
        entry = SimpleNamespace(
            project_id=saved.project_id,
            aggregate_type=saved.aggregate_type,
            aggregate_id=saved.aggregate_id,
            payload=dict(saved.payload),
        )
        if invalid == "fence":
            entry.payload["fencing_token"] = "stale"
        elif invalid == "project":
            entry.project_id = uuid.uuid4()
        elif invalid == "run":
            entry.aggregate_id = uuid.uuid4()
        elif invalid == "uid":
            jobs[manifest["metadata"]["name"]].metadata.uid = "replacement-uid"
        elif invalid == "labels":
            jobs[manifest["metadata"]["name"]].metadata.labels = {}
        elif invalid == "evidence":
            event = db.scalar(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == old_id,
                    WorkflowEvent.event_key == "analysis.submitted",
                )
            )
            event.payload = {**event.payload, "uid": "tampered"}
            db.flush()
        else:
            source = db.get(OutboxEntry, source_id)
            source.payload = {**source.payload, "manifest": {}}
            db.flush()
        with pytest.raises(StaleFence):
            cancel_analysis_job(db, entry, k8s)
        k8s.delete_job.assert_not_called()


def test_lost_delete_ack_replays_without_changing_old_cas(recovery):
    engine, run_id, old_id, _, k8s, _, _, jobs = recovery
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)

    def delete(*_a, **_k):
        jobs.clear()
        raise RuntimeError("delete committed; response lost")

    k8s.delete_job.side_effect = delete
    with Session(engine) as db, pytest.raises(RuntimeError):
        cancel_analysis_job(db, cleanup_entry(db, run_id), k8s)
    with Session(engine) as db, db.begin():
        entry = cleanup_entry(db, run_id)
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "cleanup"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        entry_id = entry.id
        db.flush()
    from automl_api.services.analysis_cleanup import reconcile_analysis_cleanup
    from sqlalchemy.orm import sessionmaker

    k8s.custom.api_client.configuration = SimpleNamespace(
        host="https://owned-analysis.invalid", verify_ssl=True, retries=0, ssl_ca_cert=None
    )
    k8s.core.list_namespaced_pod.return_value = {"items": [], "metadata": {}}
    reconcile_analysis_cleanup(
        sessionmaker(engine, expire_on_commit=False), entry_id, "cleanup", k8s_factory=lambda: k8s
    )
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, old_id).cleanup_state == "completed"
        assert db.get(WorkflowAttempt, old_id).terminal_cas_version == 1


def test_replacement_can_submit_claim_and_publish_one_report(recovery, analysis_scope, monkeypatch):  # noqa: F811
    from automl_api.services.attempt_identity import attempt_audience

    engine, run_id, old_id, _, k8s, source_id, _, _ = recovery
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db, db.begin():
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run_id,
                OutboxEntry.topic == "kubernetes.analysis.submit",
                OutboxEntry.id != source_id,
            )
        )
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "replacement"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        db.flush()
        reconcile_analysis_entry(db, entry, worker_id="replacement", k8s=k8s)
        new = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == old_id)
        )
        new_id, new_fence = new.id, new.fencing_token
        assert new.status == AttemptStatus.SUBMITTED
    proof = analysis_scope[6]
    proof.update(attempt_id=str(new_id), generation=2, audience=attempt_audience(new_id, new_fence))
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(new_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", new_fence)
    worker._begin_fenced_attempt(run_id, worker_id="new-worker")
    analysis._persist_analysis_result(run_id, {"metrics": {"rmse": 1}})
    with Session(engine) as db, db.begin():
        analysis_observer_fixture.observe_analysis_attempts(db, k8s)
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(WorkflowAttempt, old_id).status == AttemptStatus.SUPERSEDED
        assert db.get(WorkflowAttempt, new_id).terminal_cas_version == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(RunArtifact)
                .where(RunArtifact.model_run_id == run_id)
            )
            == 1
        )


def test_concurrent_observers_make_one_replacement(recovery):
    from concurrent.futures import ThreadPoolExecutor

    engine, run_id, old_id, _, k8s, _, _, _ = recovery

    def observe():
        with Session(engine) as db, db.begin():
            return analysis_observer_fixture.observe_analysis_attempts(db, k8s)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(lambda _: observe(), range(2))) == 1
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowAttempt)
                .where(WorkflowAttempt.predecessor_attempt_id == old_id)
            )
            == 1
        )
        assert (
            len(list(db.scalars(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id))))
            == 3
        )


def test_observer_refreshes_cached_live_attempt(recovery):
    engine, run_id, old_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, old_id).lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
    with Session(engine) as cached:
        old = cached.get(WorkflowAttempt, old_id)
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, old_id).lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        analysis_observer_fixture.observe_analysis_attempts(cached, k8s)
        assert old.status == AttemptStatus.SUPERSEDED
        cached.commit()


@pytest.mark.parametrize("changed", [False, True])
def test_job_creation_conflict_never_adopts_even_matching_attempt_scope(changed):
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s.settings = Settings(training_namespace="default-neighbor")
    k8s.batch = MagicMock()
    labels = {
        f"automl.platform/{key}": key
        for key in ("project-id", "run-id", "attempt-id", "generation", "workflow-stage")
    }
    manifest = {"metadata": {"name": "job", "namespace": "recorded-scope", "labels": labels}}
    existing = client.V1Job(
        metadata=client.V1ObjectMeta(
            name="job", namespace="recorded-scope", uid="physical-uid", labels=dict(labels)
        )
    )
    if changed:
        existing.metadata.labels["automl.platform/attempt-id"] = "another-attempt"
    k8s.batch.create_namespaced_job.side_effect = ApiException(status=409)
    k8s.batch.read_namespaced_job.return_value = existing
    with pytest.raises(ApiException) as caught:
        k8s.create_job(manifest)
    assert caught.value.status == 409
    k8s.batch.create_namespaced_job.assert_called_once_with(
        namespace="recorded-scope", body=manifest, _request_timeout=5
    )
    k8s.batch.read_namespaced_job.assert_not_called()
