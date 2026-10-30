"""Factory-owned original-UID analysis Job and descendant retirement."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api import workflow_reconciler
from automl_api.models.enums import AttemptStatus, OutboxStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand
from automl_api.services import reconciler, workflow_state
from automl_api.services.workflow_state import canonical_request_hash
from sqlalchemy.orm import sessionmaker
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_recovery import recovery as recovery
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.fixture
def cleanup_case(recovery, monkeypatch):
    engine, run_id, attempt_id, fence, k8s, source_id, manifest, jobs = recovery
    factory = sessionmaker(engine, expire_on_commit=False)
    with factory() as db, db.begin():
        source = db.get(OutboxEntry, source_id)
        source.status = OutboxStatus.DELIVERED
        command = db.get(WorkflowCommand, source.command_id)
        command.request_hash = canonical_request_hash(command.request_payload)
        attempt = db.get(WorkflowAttempt, attempt_id)
        workflow_state.transition_attempt(attempt, AttemptStatus.CANCELLED, fencing_token=fence)
        run = db.get(ModelRun, run_id)
        run.status = RunStatus.CANCELLED
        identity, _ = reconciler._analysis_submission(db, attempt)
        cleanup_id = uuid.uuid4()
        db.add(
            OutboxEntry(
                id=cleanup_id,
                project_id=source.project_id,
                command_id=source.command_id,
                event_key=uuid.uuid4().hex,
                topic="kubernetes.analysis.cancel",
                aggregate_type="model_run",
                aggregate_id=run_id,
                payload={
                    "attempt_id": str(attempt_id),
                    "fencing_token": fence,
                    **{key: identity[key] for key in ("uid", "namespace", "job_name")},
                },
            )
        )
        actor_id, project_id = command.actor_id, source.project_id
    c = SimpleNamespace(
        engine=engine,
        factory=factory,
        run_id=run_id,
        attempt_id=attempt_id,
        entry_id=cleanup_id,
        k8s=k8s,
        jobs=jobs,
        manifest=manifest,
        identity=identity,
        actor_id=actor_id,
        project_id=project_id,
        checkouts=[],
        deletes=[],
        pods=[],
        callback=None,
    )

    def checkpoint(stage):
        c.checkouts.append(engine.pool.checkedout())
        if c.callback:
            c.callback(stage)

    original_read = k8s.batch.read_namespaced_job.side_effect

    def read(*a, **kw):
        checkpoint("read")
        return original_read(*a, **kw)

    def delete(name, *, namespace, uid):
        checkpoint("delete_before")
        assert jobs[name].metadata.uid == uid and namespace == identity["namespace"]
        c.deletes.append(uid)
        jobs.pop(name)
        checkpoint("delete_after")

    def pods(**_kw):
        checkpoint("inventory")
        return {"items": copy.deepcopy(c.pods), "metadata": {}}

    def construct():
        checkpoint("construct")
        return k8s

    k8s.batch.read_namespaced_job.side_effect = read
    k8s.delete_job.side_effect = delete
    k8s.core.list_namespaced_pod.side_effect = pods
    k8s.custom.api_client.configuration = SimpleNamespace(
        host="https://owned-analysis.invalid", verify_ssl=True, retries=0, ssl_ca_cert=None
    )
    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", construct)
    monkeypatch.setattr(reconciler, "KubernetesTrainingClient", construct)
    c.construct = construct
    return c


def deliver(c, worker="owned-analysis-cleaner"):
    return workflow_reconciler.run_once(c.factory, worker_id=worker)


def retry(c):
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        if entry.lease_expires_at is not None:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    deliver(c, "owned-analysis-successor")


def test_actual_dispatcher_retires_original_analysis_job_outside_sql(cleanup_case):
    c = cleanup_case
    deliver(c)
    assert c.deletes == [c.identity["uid"]] and c.checkouts and set(c.checkouts) == {0}
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state == "completed"
    deliver(c)
    assert c.deletes == [c.identity["uid"]]


def test_owned_pod_blocks_completed_cleanup_until_inventory_is_empty(cleanup_case):
    c = cleanup_case
    c.pods = [
        {
            "metadata": dict(
                name="owned-child",
                namespace=c.identity["namespace"],
                uid="child-uid",
                labels={},
                ownerReferences=[{"uid": c.identity["uid"]}],
            )
        }
    ]
    deliver(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.PENDING
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
    c.pods.clear()
    retry(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
    assert c.deletes == [c.identity["uid"]]


def test_same_name_replacement_never_authorizes_analysis_delete(cleanup_case):
    c = cleanup_case
    c.jobs[c.identity["job_name"]].metadata.uid = "replacement-uid"
    deliver(c)
    assert not c.deletes
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED


@pytest.mark.parametrize("window", ["delete_before", "delete_after"])
def test_lost_analysis_delete_reply_recovers_by_original_uid_inspection(
    cleanup_case, monkeypatch, window
):
    from automl_api.services import analysis_cleanup

    c = cleanup_case

    def lose(stage):
        if stage == window:
            c.callback = None
            raise ConnectionError("owned lost cleanup response")

    c.callback = lose
    deliver(c)
    assert len(c.deletes) == int(window == "delete_after")
    future = datetime.now(UTC) + timedelta(minutes=5)
    clock = type(
        "Clock",
        (),
        {"now": staticmethod(lambda _tz: future), "fromisoformat": datetime.fromisoformat},
    )
    for module in (analysis_cleanup, reconciler, workflow_state):
        monkeypatch.setattr(module, "datetime", clock)
    retry(c)
    if window == "delete_before":
        assert not c.deletes
        retry(c)
    assert c.deletes == [c.identity["uid"]]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("change", ["actor", "payload", "owner", "context"])
def test_analysis_native_fact_is_not_a_stale_publication_authority(cleanup_case, change):
    from automl_api.models.iam import User
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services import analysis_cleanup
    from sqlalchemy import select

    c = cleanup_case

    def change_source(stage):
        if stage != "delete_after":
            return
        c.callback = None
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            if change == "actor":
                db.get(User, c.actor_id).is_active = False
            elif change == "payload":
                entry.payload = {**entry.payload, "changed": True}
            elif change == "owner":
                entry.lease_owner = "another-worker"
            else:
                c.k8s.custom.api_client.configuration.host = "https://changed-analysis.invalid"

    c.callback = change_source
    deliver(c)
    assert c.deletes == [c.identity["uid"]]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
        facts = list(
            db.scalars(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == c.attempt_id,
                    WorkflowEvent.event_key.startswith(analysis_cleanup.RESULT),
                )
            )
        )
        assert len(facts) == 1 and facts[0].payload["observed_absent"]
        assert facts[0].payload["absent"] == (change != "context")


def test_live_analysis_cleanup_cannot_be_stolen_or_acknowledge_successor(cleanup_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = cleanup_case
    entered = Event()
    release = Event()

    def hold(stage):
        if stage == "delete_before":
            c.callback = None
            entered.set()
            assert release.wait(15)

    c.callback = hold
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(deliver, c)
        try:
            assert entered.wait(10)
            retry(c)
            assert not c.deletes
            with c.factory() as db:
                assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.PENDING
        finally:
            release.set()
        pending.result(timeout=15)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.PENDING
    retry(c)
    assert c.deletes == [c.identity["uid"]]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("bad", ["receipt", "source", "command", "namespace", "labels"])
def test_changed_original_analysis_binding_cannot_authorize_delete(cleanup_case, bad):
    from automl_api.models.workflows import WorkflowEvent
    from sqlalchemy import select

    c = cleanup_case
    with c.factory() as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key == "analysis.submitted",
            )
        )
        source = db.get(OutboxEntry, event.payload["outbox_id"])
        if bad == "receipt":
            event.result_digest = "0" * 64
        elif bad == "source":
            source.payload = {**source.payload, "manifest": {}}
        elif bad == "command":
            db.get(WorkflowCommand, source.command_id).request_hash = "0" * 64
        elif bad == "namespace":
            entry = db.get(OutboxEntry, c.entry_id)
            entry.payload = {**entry.payload, "namespace": "foreign"}
        else:
            c.jobs[c.identity["job_name"]].metadata.labels = {}
    deliver(c)
    assert not c.deletes
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
    if bad != "labels":
        assert not c.checkouts


def test_session_analysis_cleanup_entrypoint_fails_before_native(cleanup_case):
    c = cleanup_case
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        with pytest.raises(RuntimeError, match="factory-owned"):
            reconciler.cancel_analysis_job(db, entry, c.k8s)
    assert not c.checkouts and not c.deletes


def test_analysis_sdk_errors_are_redacted_and_bounded(cleanup_case, monkeypatch):
    c = cleanup_case
    with c.factory() as db:
        budget = db.get(WorkflowAttempt, c.attempt_id).retry_budget
    calls = []

    def fail():
        calls.append(True)
        raise ConnectionError("https://user:secret@owned.invalid/path?token=secret")

    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", fail)
    deliver(c)
    with c.factory() as db:
        assert "secret" not in db.get(OutboxEntry, c.entry_id).last_error
    for _ in range(budget + 2):
        retry(c)
    assert len(calls) == budget and not c.deletes
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.status == OutboxStatus.PENDING and "retry budget" in entry.last_error


def test_changed_context_absence_requires_a_fresh_original_context_read(cleanup_case):
    from automl_api.models.workflows import WorkflowEvent
    from automl_api.services import analysis_cleanup
    from sqlalchemy import select

    c = cleanup_case
    cfg = c.k8s.custom.api_client.configuration

    def changed(stage):
        if stage == "delete_after":
            c.callback = None
            cfg.verify_ssl = False

    c.callback = changed
    deliver(c)
    with c.factory() as db:
        fact = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key.startswith(analysis_cleanup.RESULT),
            )
        )
        assert fact.payload["observed_absent"] and not fact.payload["absent"]
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
    cfg.verify_ssl = True
    reads = len(c.checkouts)
    retry(c)
    assert len(c.checkouts) > reads and c.deletes == [c.identity["uid"]]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED


def test_analysis_cleanup_rejects_an_unrelated_command_operation(cleanup_case):
    c = cleanup_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        db.get(WorkflowCommand, entry.command_id).operation = "run.delete"
    deliver(c)
    assert not c.checkouts and not c.deletes
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
