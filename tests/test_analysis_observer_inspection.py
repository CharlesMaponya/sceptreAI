"""Committed analysis reports and current submission state outrank old Job reads."""

import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowEvent
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.training import analysis
from automl_api.workflow_reconciler import observe_once
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_recovery import recovery as recovery
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.fixture
def inspection(recovery):
    engine, run_id, attempt_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        attempt.dataset_version_id = db.get(ModelRun, run_id).dataset_version_id
    return recovery


def factory(engine):
    return lambda: Session(engine)


def no_successor(engine, attempt_id):
    with Session(engine) as db:
        assert not list(
            db.scalars(
                select(WorkflowAttempt.id).where(
                    WorkflowAttempt.predecessor_attempt_id == attempt_id
                )
            )
        )


def locked_attempt(db, attempt_id):
    return db.scalar(
        select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update(nowait=True)
    )


def test_managed_analysis_read_releases_sql_connection_and_locks(inspection):
    engine, run_id, attempt_id, _, k8s, _, manifest, jobs = inspection
    counts = []

    def read(name, namespace, _request_timeout):
        counts.append(engine.pool.checkedout())
        assert counts[-1] == 0 and _request_timeout == 5
        assert name == manifest["metadata"]["name"]
        assert namespace == manifest["metadata"]["namespace"]
        with Session(engine) as db, db.begin():
            locked_attempt(db, attempt_id)
            db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))
        return jobs[name]

    k8s.batch.read_namespaced_job.side_effect = read
    assert observe_once(factory(engine), k8s=k8s) == 1
    assert counts == [0]
    no_successor(engine, attempt_id)
    k8s.delete_job.assert_not_called()


@pytest.mark.parametrize("change", ["fence", "lease", "owner", "run_cancel", "identity", "source"])
def test_changed_analysis_authority_defeats_old_missing_observation(inspection, change):
    engine, run_id, attempt_id, _, k8s, source_id, _, _ = inspection
    changed = []

    def read(**_):
        with Session(engine) as db, db.begin():
            attempt = locked_attempt(db, attempt_id)
            if change == "fence":
                attempt.fencing_token = uuid.uuid4().hex
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) + timedelta(hours=1)
            elif change == "owner":
                attempt.lease_owner = "renewed-worker"
            elif change == "run_cancel":
                db.get(ModelRun, run_id).status = RunStatus.CANCELLED
            else:
                event = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt_id,
                        WorkflowEvent.event_key == "analysis.submitted",
                    )
                )
                payload = dict(event.payload)
                if change == "identity":
                    payload["uid"] = "new-physical-uid"
                else:
                    source = db.get(OutboxEntry, source_id)
                    manifest = deepcopy(source.payload["manifest"])
                    manifest["spec"]["backoffLimit"] = 2
                    source.payload = {**source.payload, "manifest": manifest}
                    payload["manifest_sha256"] = canonical_request_hash(manifest)
                event.payload = payload
                event.result_digest = canonical_request_hash(payload)
        changed.append(True)
        raise ApiException(status=404)

    k8s.batch.read_namespaced_job.side_effect = read
    observe_once(factory(engine), k8s=k8s)
    assert changed == [True]
    no_successor(engine, attempt_id)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == (
            AttemptStatus.CANCELLED if change == "run_cancel" else AttemptStatus.RUNNING
        )
        if change == "run_cancel":
            assert db.get(ModelRun, run_id).status == RunStatus.CANCELLED
    k8s.delete_job.assert_not_called()


@pytest.mark.parametrize("fault", ["uid", "namespace", "labels"])
def test_observed_physical_job_must_match_recorded_identity(inspection, fault):
    engine, _, attempt_id, _, k8s, _, manifest, jobs = inspection
    job = jobs[manifest["metadata"]["name"]]
    if fault == "labels":
        job.metadata.labels = {}
    else:
        setattr(job.metadata, fault, "foreign")
    job.status.failed = 1
    observe_once(factory(engine), k8s=k8s)
    no_successor(engine, attempt_id)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.RUNNING
        assert db.get(WorkflowAttempt, attempt_id).cleanup_state == "pending"
    k8s.delete_job.assert_not_called()


def test_missing_job_replacement_preserves_nonnull_dataset_lineage(inspection):
    engine, _, attempt_id, _, k8s, _, _, _ = inspection
    k8s.batch.read_namespaced_job.side_effect = ApiException(status=404)
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        original = db.get(WorkflowAttempt, attempt_id)
        successor = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == attempt_id)
        )
        assert original.status == AttemptStatus.SUPERSEDED
        assert successor.dataset_version_id == original.dataset_version_id is not None
        assert successor.generation == original.generation + 1
    k8s.delete_job.assert_not_called()


def test_expiry_uses_no_native_read_and_preserves_dataset_lineage(inspection):
    engine, _, attempt_id, _, k8s, _, _, _ = inspection
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    assert observe_once(factory(engine), k8s=k8s) == 1
    k8s.batch.read_namespaced_job.assert_not_called()
    with Session(engine) as db:
        original = db.get(WorkflowAttempt, attempt_id)
        successor = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == attempt_id)
        )
        assert successor.dataset_version_id == original.dataset_version_id is not None


def test_competing_analysis_observers_create_one_successor(inspection):
    engine, _, attempt_id, _, k8s, _, _, _ = inspection
    competing = []

    def read(**_):
        if not competing:
            competing.append(None)
            competing[0] = observe_once(factory(engine), k8s=k8s)
        raise ApiException(status=404)

    k8s.batch.read_namespaced_job.side_effect = read
    observe_once(factory(engine), k8s=k8s)
    assert competing == [1]
    with Session(engine) as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowAttempt.id).where(
                            WorkflowAttempt.predecessor_attempt_id == attempt_id
                        )
                    )
                )
            )
            == 1
        )
    k8s.delete_job.assert_not_called()


def test_real_committed_analysis_report_wins_over_old_missing_job(inspection, analysis_scope):
    engine, run_id, attempt_id, _, k8s, _, _, _ = inspection
    committed = []

    def read(**_):
        with Session(engine) as db, db.begin():
            locked_attempt(db, attempt_id)
        analysis._persist_analysis_result(run_id, analysis_scope[5])
        committed.append(True)
        raise ApiException(status=404)

    k8s.batch.read_namespaced_job.side_effect = read
    observe_once(factory(engine), k8s=k8s)
    assert committed == [True]
    no_successor(engine, attempt_id)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.SUCCEEDED
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(ModelRun, run_id).tags["analysis_result_receipt"]
    k8s.delete_job.assert_not_called()


def test_unavailable_analysis_read_is_retryable_without_state_loss(inspection):
    engine, _, attempt_id, _, k8s, _, _, jobs = inspection
    original = k8s.batch.read_namespaced_job.side_effect
    k8s.batch.read_namespaced_job.side_effect = ApiException(status=503)
    observe_once(factory(engine), k8s=k8s)
    no_successor(engine, attempt_id)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.RUNNING
    assert jobs
    k8s.batch.read_namespaced_job.side_effect = original
    assert observe_once(factory(engine), k8s=k8s) == 1
