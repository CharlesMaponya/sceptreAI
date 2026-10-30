"""Original factory intent and current commands fence analysis retries."""

import copy
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import AttemptStatus, CommandStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import analysis_submission as submission
from automl_api.services import reconciler
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.workflow_reconciler import observe_once
from kubernetes.client import ApiException
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_recovery import recovery as recovery
from test_analysis_submission_factory import deliver
from test_analysis_submission_factory import factory_case as factory_case
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.fixture(params=["factory", "legacy_tracked"])
def scope(request, analysis_scope):
    if request.param == "factory":
        c = request.getfixturevalue("factory_case")
        deliver(c)
        c.factory_backed = True
    else:
        from types import SimpleNamespace

        engine, run_id, attempt_id, _, k8s, entry_id, _, _ = request.getfixturevalue("recovery")
        c = SimpleNamespace(
            engine=engine,
            run_id=run_id,
            attempt_id=attempt_id,
            k8s=k8s,
            entry_id=entry_id,
            factory=sessionmaker(engine),
            factory_backed=False,
        )
    with c.factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    c.k8s.batch.read_namespaced_job.reset_mock()
    c.k8s.batch.create_namespaced_job.reset_mock()
    return c


def event(db, c):
    return db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == c.attempt_id,
            WorkflowEvent.event_key == "analysis.submitted",
        )
    )


def unchanged_execution(c):
    with c.factory() as db:
        run = db.get(ModelRun, c.run_id)
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert run.status in {RunStatus.QUEUED, RunStatus.RUNNING}
        assert run.tags["workflow_attempt_id"] == str(attempt.id)
        assert attempt.status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
        assert attempt.terminal_cas_version == 0
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == attempt.id)
            )
        )
        assert not list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.aggregate_id == run.id,
                    OutboxEntry.topic == "kubernetes.analysis.cancel",
                )
            )
        )
    c.k8s.batch.create_namespaced_job.assert_not_called()


@pytest.mark.parametrize("change", ["request_hash", "operation", "resource_type", "payload_run_id"])
def test_changed_scoped_command_blocks_analysis_native_read_and_retry(scope, change):
    c = scope
    with c.factory() as db, db.begin():
        source = db.get(OutboxEntry, c.entry_id)
        command = db.get(WorkflowCommand, source.command_id)
        if change == "request_hash":
            command.request_hash = "0" * 64
        elif change == "operation":
            command.operation = "project.update"
        elif change == "resource_type":
            command.resource_type = "profiling_job"
        else:
            source.payload = {**source.payload, "run_id": "different-run"}
    c.k8s.batch.read_namespaced_job.side_effect = ApiException(status=404)
    observe_once(c.factory, k8s=c.k8s)
    c.k8s.batch.read_namespaced_job.assert_not_called()
    unchanged_execution(c)


@pytest.mark.parametrize(
    "change", ["command_request", "payload_extra", "target", "version", "intent_missing"]
)
def test_original_factory_source_blocks_rebound_observation(factory_case, change):
    c = factory_case
    deliver(c)
    with c.factory() as db, db.begin():
        db.get(WorkflowAttempt, c.attempt_id).lease_expires_at = datetime.now(UTC) + timedelta(
            minutes=5
        )
        source = db.get(OutboxEntry, c.entry_id)
        command = db.get(WorkflowCommand, source.command_id)
        run = db.get(ModelRun, c.run_id)
        if change == "command_request":
            command.request_payload = {**command.request_payload, "changed": True}
            command.request_hash = canonical_request_hash(command.request_payload)
        elif change == "payload_extra":
            source.payload = {**source.payload, "changed": True}
        elif change == "target":
            run.target_column = "changed-target"
        elif change == "version":
            db.get(DatasetVersion, run.dataset_version_id).byte_size += 1
        else:
            db.execute(
                delete(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.event_key == submission.INTENT,
                )
            )
    c.k8s.batch.read_namespaced_job.reset_mock()
    c.k8s.batch.create_namespaced_job.reset_mock()
    c.k8s.batch.read_namespaced_job.side_effect = ApiException(status=404)
    observe_once(c.factory, k8s=c.k8s)
    c.k8s.batch.read_namespaced_job.assert_not_called()
    unchanged_execution(c)


@pytest.mark.parametrize("change", ["request", "status"])
def test_inflight_analysis_command_change_discards_missing_job(scope, change):
    c = scope
    with c.factory() as db:
        original = copy.deepcopy(event(db, c).payload)
    calls = []

    def read(**_):
        calls.append(c.engine.pool.checkedout())
        assert calls[-1] == 0
        with c.factory() as db, db.begin():
            command = db.get(WorkflowCommand, db.get(OutboxEntry, c.entry_id).command_id)
            if change == "request":
                command.request_payload = {**command.request_payload, "changed": True}
                command.request_hash = canonical_request_hash(command.request_payload)
            else:
                command.status = (
                    CommandStatus.RUNNING
                    if command.status == CommandStatus.PENDING
                    else CommandStatus.PENDING
                )
        raise ApiException(status=404)

    c.k8s.batch.read_namespaced_job.side_effect = read
    observe_once(c.factory, k8s=c.k8s)
    assert calls == [0]
    unchanged_execution(c)
    with c.factory() as db:
        assert event(db, c).payload == original


def test_unchanged_original_missing_job_queues_one_successor_and_cleanup(scope):
    c = scope
    with c.factory() as db:
        original = copy.deepcopy(event(db, c).payload)
    calls = []

    def read(**_):
        calls.append(c.engine.pool.checkedout())
        assert calls[-1] == 0
        raise ApiException(status=404)

    c.k8s.batch.read_namespaced_job.side_effect = read
    observe_once(c.factory, k8s=c.k8s)
    assert calls == [0]
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        successors = list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == attempt.id)
            )
        )
        assert len(successors) == 1 and successors[0].generation == 2
        assert attempt.status == AttemptStatus.SUPERSEDED and attempt.cleanup_state == "queued"
        cleanup = list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.aggregate_id == c.run_id,
                    OutboxEntry.topic == "kubernetes.analysis.cancel",
                )
            )
        )
        assert len(cleanup) == 1 and cleanup[0].payload["uid"] == original["uid"]
        assert event(db, c).payload == original
    c.k8s.batch.create_namespaced_job.assert_not_called()


def test_orphaned_factory_receipt_cannot_bypass_shared_identity_check(factory_case):
    c = factory_case
    deliver(c)
    with c.factory() as db, db.begin():
        db.execute(
            delete(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key == submission.INTENT,
            )
        )
    with c.factory() as db, pytest.raises(StaleFence):
        reconciler._analysis_submission(db, db.get(WorkflowAttempt, c.attempt_id))


@pytest.mark.parametrize("field", ["job_name", "namespace"])
def test_factory_receipt_metadata_cannot_redirect_shared_identity(factory_case, field):
    c = factory_case
    deliver(c)
    with c.factory() as db, db.begin():
        fact = event(db, c)
        fact.payload = {**fact.payload, field: "foreign-target"}
        fact.result_digest = canonical_request_hash(fact.payload)
    with c.factory() as db, pytest.raises(StaleFence):
        reconciler._analysis_submission(db, db.get(WorkflowAttempt, c.attempt_id))
