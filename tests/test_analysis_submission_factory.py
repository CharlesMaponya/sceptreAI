"""Actual analysis dispatcher with committed PostgreSQL and the native SDK boundary."""

import copy
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api import workflow_reconciler
from automl_api.core.config import Settings
from automl_api.models.enums import AttemptStatus, CommandStatus, OutboxStatus, RunStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.services import analysis_submission as submission
from automl_api.services import reconciler, workflow_state
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import canonical_request_hash
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy.orm import sessionmaker
from test_analysis_admission import analysis_scope as analysis_scope
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication
from test_validation import _estimate


@pytest.fixture
def factory_case(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, fence, _, _, _, _ = analysis_scope
    factory = sessionmaker(engine, expire_on_commit=False)
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s.settings = Settings(training_namespace="analysis-test", max_concurrent_jobs=1000)
    k8s.batch = MagicMock()
    k8s.batch.api_client = client.ApiClient()
    k8s.core = MagicMock()
    k8s.custom = SimpleNamespace(
        api_client=SimpleNamespace(
            configuration=SimpleNamespace(
                host="https://owned-analysis-submission.invalid",
                verify_ssl=True,
                retries=0,
                ssl_ca_cert=None,
            )
        )
    )
    k8s.capacity_snapshot = MagicMock(
        return_value=SimpleNamespace(
            capacity=SimpleNamespace(connected=True, active_training_jobs=0)
        )
    )
    with factory() as db, db.begin():
        run = db.get(ModelRun, run_id)
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.status = AttemptStatus.PENDING
        run.tags = {**run.tags, "desired_state": "kubernetes_submission_pending"}
        manifest = k8s.build_job_manifest(
            run_id=run_id, project_id=run.project_id, estimate=_estimate(), attempt=attempt
        )
        run.k8s_namespace = manifest["metadata"]["namespace"]
        run.k8s_job_name = manifest["metadata"]["name"]
        command = WorkflowCommand(
            project_id=run.project_id,
            actor_id=run.created_by_id,
            operation=f"{run.run_kind.value}.launch",
            idempotency_key=uuid.uuid4().hex,
            request_hash=canonical_request_hash({}),
            request_payload={},
            resource_type="model_run",
            resource_id=run_id,
            status=CommandStatus.RUNNING,
        )
        db.add(command)
        db.flush()
        entry = OutboxEntry(
            project_id=run.project_id,
            command_id=command.id,
            event_key=uuid.uuid4().hex,
            topic=submission.TOPIC,
            aggregate_type="model_run",
            aggregate_id=run_id,
            payload=dict(run_id=str(run_id), manifest=manifest),
            available_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
        db.add(entry)
        db.flush()
        entry_id = entry.id
        actor_id = command.actor_id
        project_id = run.project_id
    c = SimpleNamespace(
        engine=engine,
        factory=factory,
        run_id=run_id,
        attempt_id=attempt_id,
        entry_id=entry_id,
        k8s=k8s,
        manifest=manifest,
        actor_id=actor_id,
        project_id=project_id,
        checkouts=[],
        creations=[],
        activations=[],
        native=None,
        callback=None,
    )

    c.sql_checkouts = lambda: engine.pool.checkedout()

    def checkpoint(stage):
        c.checkouts.append(c.sql_checkouts())
        assert c.checkouts[-1] == 0
        if c.callback:
            c.callback(stage)

    def created(body, namespace, **kw):
        checkpoint("create_before")
        assert body["spec"]["suspend"] is True
        with factory() as db:
            assert (
                submission.journal._event(
                    db,
                    db.get(
                        WorkflowAttempt,
                        uuid.UUID(body["metadata"]["labels"]["automl.platform/attempt-id"]),
                    ),
                    submission.CREATE_START,
                )
                is not None
            )
        c.native = copy.deepcopy(body)
        uid = "owned-original-uid" if not c.creations else f"owned-original-uid-{len(c.creations)}"
        c.native["metadata"].update(uid=uid, resourceVersion="1")
        c.creations.append(copy.deepcopy(c.native))
        checkpoint("create_after")
        return k8s.batch.api_client._ApiClient__deserialize(c.native, "V1Job")

    def read(**kw):
        checkpoint("read")
        if not getattr(c, "cleanup_phase", False):
            with factory() as db:
                a = db.get(
                    WorkflowAttempt,
                    uuid.UUID(c.native["metadata"]["labels"]["automl.platform/attempt-id"]),
                )
                original = submission.journal._event(db, a, submission.INTENT).payload
                assert (
                    reconciler._analysis_submission(db, a)[0]["uid"]
                    == submission._created(db, a, original)["metadata"]["uid"]
                )
        if c.native is None:
            raise ApiException(status=404)
        return k8s.batch.api_client._ApiClient__deserialize(c.native, "V1Job")

    def patch(**kw):
        checkpoint("activate_before")
        tests = kw["body"]
        assert tests[0]["value"] == c.native["metadata"]["uid"]
        assert tests[1]["value"] == c.native["metadata"]["resourceVersion"]
        assert tests[2]["value"] == c.native["spec"]
        c.native["spec"]["suspend"] = False
        c.native["metadata"]["resourceVersion"] = "2"
        c.activations.append(c.native["metadata"]["uid"])
        checkpoint("activate_after")
        return k8s.batch.api_client._ApiClient__deserialize(c.native, "V1Job")

    original_capacity = k8s.capacity_snapshot.return_value
    k8s.capacity_snapshot.side_effect = lambda: (checkpoint("capacity"), original_capacity)[1]
    k8s.core.create_namespaced_service_account.side_effect = lambda **kw: checkpoint(
        "service_account"
    )
    k8s.batch.create_namespaced_job.side_effect = created
    k8s.batch.read_namespaced_job.side_effect = read
    k8s.batch.patch_namespaced_job.side_effect = patch

    def construct():
        checkpoint("construct")
        return k8s

    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", construct)
    monkeypatch.setattr(reconciler, "KubernetesTrainingClient", construct)
    c.construct = construct
    return c


def deliver(c, worker="analysis-submit"):
    return workflow_reconciler.run_once(c.factory, worker_id=worker)


def retry(c):
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.available_at = datetime(2000, 1, 1, tzinfo=UTC)
        if entry.lease_expires_at is not None:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    return deliver(c, "recovery")


def test_actual_analysis_dispatch_commits_original_uid_before_sql_free_activation(factory_case):
    c = factory_case
    assert deliver(c) == 1
    assert len(c.creations) == len(c.activations) == 1
    assert c.checkouts and set(c.checkouts) == {0}
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert attempt.status == AttemptStatus.SUBMITTED
        intent = submission.journal._event(db, attempt, submission.INTENT).payload
        assert submission._created(db, attempt, intent)["spec"]["suspend"] is True
        assert submission._activated(db, attempt, intent, submission._created(db, attempt, intent))
    before = (len(c.creations), len(c.activations), len(c.checkouts))
    assert deliver(c) == 0
    assert before == (len(c.creations), len(c.activations), len(c.checkouts))


@pytest.mark.parametrize(
    "stage", ["create_before", "create_after", "activate_before", "activate_after"]
)
def test_lost_analysis_native_replies_never_recreate_or_adopt(factory_case, stage, monkeypatch):
    c = factory_case

    def fault(value):
        if value == stage:
            raise OSError("owned lost native reply")

    c.callback = fault
    deliver(c)
    c.callback = None
    if stage.startswith("create_"):
        retry(c)
        assert len(c.creations) == int(stage == "create_after") and not c.activations
    else:

        class ExpiredNativeClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(minutes=5)

        monkeypatch.setattr(submission, "datetime", ExpiredNativeClock)
        retry(c)
        assert len(c.creations) == len(c.activations) == 1
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("change", ["actor", "source", "owner", "terminal"])
@pytest.mark.parametrize("native_stage", ["create_after", "activate_after"])
def test_original_analysis_native_fact_survives_stale_publication(
    factory_case, change, native_stage
):
    c = factory_case

    def changed(stage):
        if stage != native_stage:
            return
        with c.factory() as db, db.begin():
            if change == "actor":
                db.get(User, c.actor_id).is_active = False
            elif change == "source":
                entry = db.get(OutboxEntry, c.entry_id)
                entry.payload = {**entry.payload, "changed": True}
            elif change == "owner":
                db.get(OutboxEntry, c.entry_id).lease_owner = "successor"
            else:
                db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED

    c.callback = changed
    deliver(c)
    assert len(c.creations) == 1 and len(c.activations) == int(native_stage == "activate_after")
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        intent = submission.journal._event(db, attempt, submission.INTENT).payload
        assert submission._created(db, attempt, intent)["metadata"]["uid"] == "owned-original-uid"
        assert (submission.journal._event(db, attempt, "analysis.submitted") is not None) == (
            native_stage == "activate_after"
        )
        assert submission._activated(
            db, attempt, intent, submission._created(db, attempt, intent)
        ) == (native_stage == "activate_after")
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


def test_replacement_job_cannot_be_activated(factory_case):
    c = factory_case

    def replacement(stage):
        if stage == "read":
            c.native["metadata"]["uid"] = "replacement"

    c.callback = replacement
    deliver(c)
    assert len(c.creations) == 1 and not c.activations
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


def test_capacity_wait_is_budget_neutral_and_never_creates(factory_case):
    c = factory_case
    c.k8s.settings = replace(c.k8s.settings, max_concurrent_jobs=0)
    deliver(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        assert db.get(ModelRun, c.run_id).tags["waiting_for_capacity"] is True
        assert (
            submission.journal._event(
                db, db.get(WorkflowAttempt, c.attempt_id), submission.RESERVATION
            )
            is None
        )
    assert not c.creations and not c.activations


@pytest.mark.parametrize("history", ["claimed", "retried"])
def test_unjournaled_legacy_history_cannot_be_treated_as_never_created(factory_case, history):
    c = factory_case
    with c.factory() as db, db.begin():
        if history == "claimed":
            db.get(WorkflowAttempt, c.attempt_id).status = AttemptStatus.CLAIMED
        else:
            db.get(OutboxEntry, c.entry_id).delivery_attempts = 1
    deliver(c)
    assert not c.creations and not c.activations and not c.checkouts
    with c.factory() as db:
        assert (
            submission.journal._event(db, db.get(WorkflowAttempt, c.attempt_id), submission.INTENT)
            is None
        )
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


@pytest.mark.parametrize("stage", ["construct", "service_account", "create_after"])
def test_terminal_analysis_retires_known_unpublished_or_never_started_job(factory_case, stage):
    from automl_api.services import analysis_cleanup

    c = factory_case

    def terminal(value):
        if value == stage:
            with c.factory() as db, db.begin():
                db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED

    c.callback = terminal
    deliver(c)
    c.callback = None
    retry(c)
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert attempt.status == AttemptStatus.CANCELLED
        assert not analysis_cleanup.retirement_complete(db, attempt)
    c.cleanup_phase = True

    def delete(name, namespace, body, **kw):
        assert body.preconditions.uid == c.native["metadata"]["uid"]
        c.native = None

    c.k8s.batch.delete_namespaced_job.side_effect = delete
    c.k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[], metadata=None)
    deliver(c, "cleanup")
    with c.factory() as db:
        assert analysis_cleanup.retirement_complete(db, db.get(WorkflowAttempt, c.attempt_id))
    retry(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    assert len(c.creations) == int(stage == "create_after") and not c.activations


def test_unknown_analysis_create_blocks_cleanup_and_database_history_erasure(factory_case):
    from automl_api.services.deletion_runtime import runtime_targets
    from automl_api.storage.contracts import DeletionPending

    c = factory_case

    def lost(stage):
        if stage == "create_after":
            raise OSError("lost original reply")

    c.callback = lost
    deliver(c)
    c.callback = None
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.run_id)
        run.status = RunStatus.CANCELLED
    retry(c)
    with c.factory() as db:
        run = db.get(ModelRun, c.run_id)
        with pytest.raises(DeletionPending, match="unknown"):
            runtime_targets(db, c.project_id, [run], whole_project=False)
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
    assert len(c.creations) == 1 and not c.activations


def test_known_unpublished_uid_is_available_to_authorized_deletion_with_creator_context(
    factory_case,
):
    from automl_api.services.deletion_runtime import cleanup_runtime, runtime_targets

    c = factory_case

    def cancelled(stage):
        if stage == "create_after":
            with c.factory() as db, db.begin():
                db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED

    c.callback = cancelled
    deliver(c)
    c.callback = None
    with c.factory() as db:
        targets = runtime_targets(
            db, c.project_id, [db.get(ModelRun, c.run_id)], whole_project=False
        )
    assert len(targets) == 1 and targets[0]["uid"] == "owned-original-uid"
    assert targets[0]["native_context_sha256"] == submission.journal._context(c.k8s)
    c.k8s.custom.api_client.configuration.host = "https://neighbor.invalid"
    before = c.k8s.batch.read_namespaced_job.call_count
    with pytest.raises(workflow_state.StaleFence, match="context changed"):
        cleanup_runtime(c.k8s, targets)
    assert c.k8s.batch.read_namespaced_job.call_count == before


def test_rehashed_analysis_receipt_cannot_replace_original_factory_uid(factory_case):
    from automl_api.models.workflows import WorkflowEvent
    from sqlalchemy import select

    c = factory_case
    deliver(c)
    with c.factory() as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key == "analysis.submitted",
            )
        )
        event.payload = {**event.payload, "uid": "replacement"}
        event.result_digest = canonical_request_hash(event.payload)
    with c.factory() as db, pytest.raises(workflow_state.StaleFence, match="original factory"):
        reconciler._analysis_submission(db, db.get(WorkflowAttempt, c.attempt_id))


def test_known_activation_can_settle_after_budget_without_repeating_native_effects(factory_case):
    c = factory_case

    def changed(stage):
        if stage == "activate_after":
            c.k8s.custom.api_client.configuration.host = "https://neighbor.invalid"

    c.callback = changed
    deliver(c)
    assert len(c.creations) == len(c.activations) == 1
    c.callback = None
    c.k8s.custom.api_client.configuration.host = "https://owned-analysis-submission.invalid"
    with c.factory() as db, db.begin():
        db.get(OutboxEntry, c.entry_id).delivery_attempts = 10
    retry(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    assert len(c.creations) == len(c.activations) == 1


def clone_pending_delivery(c):
    with c.factory() as db, db.begin():
        old = db.get(ModelRun, c.run_id)
        run = ModelRun(
            project_id=old.project_id,
            created_by_id=old.created_by_id,
            dataset_version_id=old.dataset_version_id,
            run_kind=old.run_kind,
            task_type=old.task_type,
            status=RunStatus.QUEUED,
            k8s_namespace=old.k8s_namespace,
            tags={},
        )
        db.add(run)
        db.flush()
        attempt = WorkflowAttempt(
            project_id=run.project_id,
            model_run_id=run.id,
            dataset_version_id=run.dataset_version_id,
            stage=db.get(WorkflowAttempt, c.attempt_id).stage,
            logical_key=f"analysis-run:{run.id}",
            workload_identity=db.get(WorkflowAttempt, c.attempt_id).workload_identity,
            generation=1,
            fencing_token=uuid.uuid4().hex,
        )
        db.add(attempt)
        db.flush()
        manifest = c.k8s.build_job_manifest(
            run_id=run.id, project_id=run.project_id, estimate=_estimate(), attempt=attempt
        )
        run.k8s_job_name = manifest["metadata"]["name"]
        run.tags = {
            "workflow_attempt_id": str(attempt.id),
            "desired_state": "kubernetes_submission_pending",
        }
        command = WorkflowCommand(
            project_id=run.project_id,
            actor_id=c.actor_id,
            operation=f"{run.run_kind.value}.launch",
            idempotency_key=uuid.uuid4().hex,
            request_payload={},
            request_hash=canonical_request_hash({}),
            resource_type="model_run",
            resource_id=run.id,
            status=CommandStatus.RUNNING,
        )
        db.add(command)
        db.flush()
        entry = OutboxEntry(
            project_id=run.project_id,
            command_id=command.id,
            event_key=uuid.uuid4().hex,
            topic=submission.TOPIC,
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"run_id": str(run.id), "manifest": manifest},
            available_at=datetime(2000, 1, 2, tzinfo=UTC),
        )
        db.add(entry)
        db.flush()
        return entry.id, attempt.id, run.id


def test_two_real_postgres_workers_cannot_reserve_the_same_last_slot(factory_case):
    from collections import Counter
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier, Lock, get_ident

    from sqlalchemy import event

    c = factory_case
    second_id, _, _ = clone_pending_delivery(c)
    c.k8s.settings = replace(c.k8s.settings, max_concurrent_jobs=1)
    barrier = Barrier(2)
    lock = Lock()
    held = Counter()

    def checkout(connection, record, proxy):
        with lock:
            record.info["owned_analysis_thread"] = get_ident()
            held[get_ident()] += 1

    def checkin(connection, record):
        with lock:
            held[record.info.pop("owned_analysis_thread")] -= 1

    def checked():
        with lock:
            return held[get_ident()]

    def rendezvous(stage):
        if stage == "capacity":
            barrier.wait(timeout=15)

    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)
    c.sql_checkouts = checked
    c.callback = rendezvous
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = [workers.submit(deliver, c, "worker-" + str(i)) for i in range(2)]
            assert [r.result(timeout=30) for r in results] == [1, 1]
    finally:
        c.callback = None
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)
    assert len(c.creations) == len(c.activations) == 1
    assert set(c.checkouts) == {0} and not any(held.values())
    with c.factory() as db:
        entries = [db.get(OutboxEntry, k) for k in (c.entry_id, second_id)]
        assert sorted(e.status.value for e in entries) == ["delivered", "pending"]
        waiting = next(e for e in entries if e.status == OutboxStatus.PENDING)
        assert waiting.delivery_attempts == 0


def test_unknown_create_keeps_its_slot_after_terminal_state(factory_case):
    c = factory_case
    c.k8s.settings = replace(c.k8s.settings, max_concurrent_jobs=1)

    def lost(stage):
        if stage == "create_after":
            raise OSError("lost original create reply")

    c.callback = lost
    deliver(c)
    c.callback = None
    with c.factory() as db, db.begin():
        db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED
    second_id, _, _ = clone_pending_delivery(c)
    deliver(c, "other-run")
    with c.factory() as db:
        other = db.get(OutboxEntry, second_id)
        assert other.status == OutboxStatus.PENDING and other.delivery_attempts == 0
    assert len(c.creations) == 1 and not c.activations


def test_a_slot_reopens_only_after_original_physical_cleanup(factory_case):
    from automl_api.services import analysis_cleanup

    c = factory_case
    c.k8s.settings = replace(c.k8s.settings, max_concurrent_jobs=1)

    def cancelled(stage):
        if stage == "create_after":
            with c.factory() as db, db.begin():
                db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED

    c.callback = cancelled
    deliver(c)
    c.callback = None
    second_id, _, _ = clone_pending_delivery(c)
    deliver(c, "other-run-waits")
    assert len(c.creations) == 1 and not c.activations
    retry(c)
    c.cleanup_phase = True

    def delete(name, namespace, body, **kw):
        assert body.preconditions.uid == c.native["metadata"]["uid"]
        c.native = None

    c.k8s.batch.delete_namespaced_job.side_effect = delete
    c.k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[], metadata=None)
    deliver(c, "physical-cleanup")
    with c.factory() as db:
        assert analysis_cleanup.retirement_complete(db, db.get(WorkflowAttempt, c.attempt_id))
    c.cleanup_phase = False
    with c.factory() as db, db.begin():
        db.get(OutboxEntry, second_id).available_at = datetime(2000, 1, 1, tzinfo=UTC)
    deliver(c, "other-run-starts")
    with c.factory() as db:
        assert db.get(OutboxEntry, second_id).status == OutboxStatus.DELIVERED
    assert len(c.creations) == 2 and c.activations == ["owned-original-uid-1"]


@pytest.mark.parametrize("bad", ["unverified", "digest", "verified_at"])
def test_analysis_source_digest_policy_fails_before_native_construction(factory_case, bad):
    from automl_api.models.datasets import DatasetVersion

    c = factory_case
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, db.get(ModelRun, c.run_id).dataset_version_id)
        if bad == "unverified":
            version.content_hash_verification_status = "pending"
        elif bad == "digest":
            version.content_hash = "not-a-digest"
        else:
            version.content_hash_verified_at = None
    deliver(c)
    assert not c.checkouts and not c.creations and not c.activations
    with c.factory() as db:
        assert (
            submission.journal._event(db, db.get(WorkflowAttempt, c.attempt_id), submission.INTENT)
            is None
        )
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


def test_retired_old_generation_ack_preserves_the_new_current_generation(factory_case):
    from automl_api.services import analysis_cleanup

    c = factory_case

    def cancelled(stage):
        if stage == "create_after":
            with c.factory() as db, db.begin():
                db.get(ModelRun, c.run_id).status = RunStatus.CANCELLED

    c.callback = cancelled
    deliver(c)
    c.callback = None
    retry(c)
    c.cleanup_phase = True

    def delete(name, namespace, body, **kw):
        assert body.preconditions.uid == c.native["metadata"]["uid"]
        c.native = None

    c.k8s.batch.delete_namespaced_job.side_effect = delete
    c.k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[], metadata=None)
    deliver(c, "retire-old")
    with c.factory() as db, db.begin():
        old = db.get(WorkflowAttempt, c.attempt_id)
        assert analysis_cleanup.retirement_complete(db, old)
        run = db.get(ModelRun, c.run_id)
        new = WorkflowAttempt(
            project_id=old.project_id,
            model_run_id=run.id,
            stage=old.stage,
            logical_key=old.logical_key,
            workload_identity=old.workload_identity,
            generation=old.generation + 1,
            predecessor_attempt_id=old.id,
            fencing_token=uuid.uuid4().hex,
        )
        db.add(new)
        db.flush()
        next_manifest = c.k8s.build_job_manifest(
            run_id=run.id, project_id=run.project_id, estimate=_estimate(), attempt=new
        )
        run.k8s_job_name = next_manifest["metadata"]["name"]
        run.tags = {
            **run.tags,
            "workflow_attempt_id": str(new.id),
            "desired_state": "kubernetes_submission_pending",
        }
        run.status = RunStatus.QUEUED
        source = db.get(OutboxEntry, c.entry_id)
        next_entry = OutboxEntry(
            project_id=run.project_id,
            command_id=source.command_id,
            event_key=uuid.uuid4().hex,
            topic=submission.TOPIC,
            aggregate_type="model_run",
            aggregate_id=run.id,
            payload={"run_id": str(run.id), "manifest": next_manifest},
            available_at=datetime(2000, 1, 2, tzinfo=UTC),
        )
        db.add(next_entry)
        db.flush()
        new_id, new_name, next_id = new.id, run.k8s_job_name, next_entry.id
    before = (len(c.creations), len(c.activations), len(c.checkouts))
    retry(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert db.get(OutboxEntry, next_id).status == OutboxStatus.PENDING
        run = db.get(ModelRun, c.run_id)
        assert run.tags["workflow_attempt_id"] == str(new_id) and run.k8s_job_name == new_name
        assert run.status == RunStatus.QUEUED
    assert before == (len(c.creations), len(c.activations), len(c.checkouts))


def test_active_moved_generation_never_authorizes_submission(factory_case):
    c = factory_case
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.run_id)
        run.tags = {**run.tags, "workflow_attempt_id": str(uuid.uuid4())}
    deliver(c)
    assert not c.checkouts and not c.creations and not c.activations
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert (
            submission.journal._event(db, db.get(WorkflowAttempt, c.attempt_id), submission.INTENT)
            is None
        )


@pytest.mark.parametrize("wrong", ["run.delete", "training.launch"])
def test_analysis_command_operation_must_match_the_analysis_kind(factory_case, wrong):
    c = factory_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        db.get(WorkflowCommand, entry.command_id).operation = wrong
    deliver(c)
    assert not c.checkouts and not c.creations and not c.activations
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert (
            submission.journal._event(db, db.get(WorkflowAttempt, c.attempt_id), submission.INTENT)
            is None
        )
