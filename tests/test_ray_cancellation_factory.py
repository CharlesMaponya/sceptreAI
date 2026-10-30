"""Original native Ray retirement through the real factory dispatcher."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api import workflow_reconciler
from automl_api.models.enums import AttemptStatus, OutboxStatus
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowEvent
from automl_api.services import ray_cancellation as cancellation
from automl_api.services import ray_submission, reconciler
from automl_api.services.workflow_state import canonical_request_hash
from kubernetes.client import ApiException
from sqlalchemy import select
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_submission_factory import factory_case as factory_case
from test_ray_submission_receipts import committed_submission as committed_submission


def configure_cancellation(c):
    with c.factory() as db, db.begin():
        source = db.get(OutboxEntry, c.entry_id)
        original = ray_submission._event(
            db, db.get(WorkflowAttempt, c.attempt_id), ray_submission.INTENT
        )
        namespace = (
            original.payload["namespace"]
            if original is not None
            else (
                db.get(ray_submission.ModelRun, source.aggregate_id).k8s_namespace
                if source.topic == "ray.training.submit"
                else reconciler.get_settings().training_namespace
            )
        )
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        attempt.status = AttemptStatus.CANCELLED
        c.cancel_id = uuid.uuid4()
        db.add(
            OutboxEntry(
                id=c.cancel_id,
                project_id=source.project_id,
                command_id=source.command_id,
                event_key=uuid.uuid4().hex,
                topic="ray.training.cancel"
                if source.topic == "ray.training.submit"
                else "ray.dataset.cancel",
                aggregate_type=source.aggregate_type,
                aggregate_id=source.aggregate_id,
                payload=dict(
                    attempt_id=str(attempt.id),
                    fencing_token=attempt.fencing_token,
                    **({"namespace": namespace} if source.topic != "ray.training.submit" else {}),
                ),
            )
        )
    c.deletions, c.descendants, c.cancel_callback = [], [], None
    c.checkouts.clear()

    def checkpoint(stage):
        c.checkouts.append(c.engine.pool.checkedout())
        if c.cancel_callback:
            c.cancel_callback(stage)

    def read(*_args, **_kwargs):
        checkpoint("read")
        if c.native is None:
            raise ApiException(status=404)
        return copy.deepcopy(c.native)

    def delete(name, *, namespace, uid):
        checkpoint("delete_before")
        assert c.native["metadata"]["uid"] == uid
        assert c.native["metadata"]["name"] == name
        assert c.native["metadata"]["namespace"] == namespace
        c.deletions.append(uid)
        c.native = None
        checkpoint("delete_after")

    def pods(**_kwargs):
        checkpoint("inventory")
        return {"items": copy.deepcopy(c.descendants), "metadata": {}}

    def empty(**_kwargs):
        checkpoint("inventory")
        return {"items": [], "metadata": {}}

    c.k8s.ray_job.side_effect = read
    c.k8s.delete_ray_job.side_effect = delete
    c.k8s.core.list_namespaced_pod.side_effect = pods
    c.k8s.batch.list_namespaced_job.side_effect = empty
    c.k8s.custom.list_namespaced_custom_object.side_effect = empty
    c.checkpoint = checkpoint
    return c


@pytest.fixture
def cancellation_case(factory_case):
    c = factory_case
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-submitter")
    assert c.native and len(c.creations) == 1
    return configure_cancellation(c)


def deliver(c, worker="owned-ray-canceller"):
    return workflow_reconciler.run_once(c.factory, worker_id=worker)


def retry(c):
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.cancel_id)
        entry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        if entry.lease_expires_at is not None:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    deliver(c, "owned-ray-cancel-successor")


def history(c, db):
    return {
        row.event_key: row
        for row in db.scalars(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id,
                WorkflowEvent.event_key.startswith("ray.cancel."),
            )
        )
    }


def test_actual_cancellation_dispatcher_retires_original_uid_outside_sql(cancellation_case):
    c = cancellation_case
    deliver(c)
    assert c.deletions == ["original-factory-uid"]
    assert c.checkouts and set(c.checkouts) == {0}
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state == "completed"
        facts = history(c, db)
        assert cancellation.INTENT in facts
        assert len(facts) == 3
    deliver(c)
    assert len(c.deletions) == 1


def test_replacement_already_present_is_held_without_delete(cancellation_case):
    c = cancellation_case
    c.native["metadata"]["uid"] = "replacement-before-first-read"
    deliver(c)
    assert not c.deletions
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"


def test_descendant_blocks_cleanup_until_qualified_absent(cancellation_case):
    c = cancellation_case
    meta = c.native["metadata"]
    c.descendants = [
        {
            "metadata": dict(
                name="owned-child",
                namespace=meta["namespace"],
                uid="owned-child-uid",
                labels={},
                ownerReferences=[{"uid": meta["uid"]}],
            )
        }
    ]
    deliver(c)
    assert c.deletions == [meta["uid"]]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.PENDING
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
    c.descendants.clear()
    retry(c)
    assert len(c.deletions) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("window", ["delete_before", "delete_after"])
def test_lost_delete_response_recovers_by_inspection(cancellation_case, monkeypatch, window):
    c = cancellation_case

    def lose(stage):
        if stage == window:
            c.cancel_callback = None
            raise ConnectionError("owned cancellation response lost")

    c.cancel_callback = lose
    deliver(c)
    assert len(c.deletions) == int(window == "delete_after")
    # Expire the original native unit using the test clock, never by editing its evidence.
    future = datetime.now(UTC) + timedelta(minutes=5)
    clock = type(
        "Clock",
        (),
        {"now": staticmethod(lambda _tz: future), "fromisoformat": datetime.fromisoformat},
    )
    monkeypatch.setattr(cancellation, "datetime", clock)
    monkeypatch.setattr(reconciler, "datetime", clock)
    from automl_api.services import workflow_state

    monkeypatch.setattr(workflow_state, "datetime", clock)
    retry(c)
    if window == "delete_before":
        assert not c.deletions
        retry(c)
    assert c.deletions == ["original-factory-uid"]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("fault", ["actor", "payload", "claim", "context"])
def test_native_retirement_fact_survives_changed_publication_authority(cancellation_case, fault):
    from automl_api.models.iam import User

    c = cancellation_case

    def change(stage):
        if stage != "delete_after":
            return
        c.cancel_callback = None
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.cancel_id)
            if fault == "actor":
                db.get(User, c.actor_id).is_active = False
            elif fault == "payload":
                entry.payload = {**entry.payload, "changed": True}
            elif fault == "claim":
                entry.lease_owner = "another-live-owner"
            else:
                c.k8s.custom.api_client.configuration.host = "https://another-native.invalid"

    c.cancel_callback = change
    deliver(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED
        facts = history(c, db)
        result = next(fact for key, fact in facts.items() if key.startswith(cancellation.RESULT))
        assert result.payload["observed_absent"] is True
        assert result.payload["absent"] == (fault != "context")
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
    assert c.deletions == ["original-factory-uid"]


@pytest.mark.parametrize("topic", sorted(cancellation.TOPICS))
def test_session_dispatcher_rejects_ray_cancellation_before_native(topic):
    from unittest.mock import MagicMock

    entry = SimpleNamespace(
        id=uuid.uuid4(),
        topic=topic,
        status=OutboxStatus.CLAIMED,
        lease_owner="owned",
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=30),
    )
    db, k8s = MagicMock(), MagicMock()
    db.get.return_value = entry
    with pytest.raises(RuntimeError, match="factory-owned"):
        reconciler.reconcile_entry(db, entry, worker_id="owned", k8s=k8s)
    assert not k8s.mock_calls


@pytest.mark.parametrize(
    "window",
    [
        "known_unpublished",
        "unknown_create",
        "unknown_sa",
        "sa_known",
        "intent_only",
        "never_started",
    ],
)
def test_original_creation_history_governs_retirement_before_logical_submission(
    factory_case, monkeypatch, window
):
    c = factory_case
    if window == "known_unpublished":
        monkeypatch.setattr(
            reconciler,
            "publish_ray_submission",
            lambda *_a, **_k: (_ for _ in ()).throw(ConnectionError("owned lost publication")),
        )
    elif window == "unknown_create":

        def lose(stage):
            if stage == "create_after":
                raise ConnectionError("owned unknown native creation")

        c.callback = lose
    elif window == "unknown_sa":

        def lose(stage):
            if stage == "service_account":
                raise ConnectionError("owned unknown service account")

        c.callback = lose
    elif window == "sa_known":
        original_start = ray_submission._start

        def lose_before_create(db, rows, intent, key, context):
            if key == ray_submission.CREATE_START:
                raise ConnectionError("owned failure before create custody")
            return original_start(db, rows, intent, key, context)

        monkeypatch.setattr(ray_submission, "_start", lose_before_create)
    elif window == "intent_only":
        monkeypatch.setattr(
            workflow_reconciler,
            "KubernetesTrainingClient",
            lambda: (_ for _ in ()).throw(ConnectionError("owned unavailable client")),
        )
    if window != "never_started":
        workflow_reconciler.run_once(c.factory, worker_id="owned-before-cancellation")
    with c.factory() as db:
        assert db.get(WorkflowAttempt, c.attempt_id).ray_job_name is None
    configure_cancellation(c)
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.cancel_id)
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = "owned-direct-cancel"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts = 1
    cancellation.reconcile_ray_cancellation(
        c.factory, c.cancel_id, "owned-direct-cancel", k8s_factory=c.construct
    )
    with c.factory() as db:
        complete = db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED
        assert complete == (
            window in {"known_unpublished", "sa_known", "intent_only", "never_started"}
        )
        assert (db.get(WorkflowAttempt, c.attempt_id).cleanup_state == "completed") == complete
    assert len(c.deletions) == int(window == "known_unpublished")
    if window in {"unknown_create", "unknown_sa", "sa_known", "intent_only", "never_started"}:
        assert not c.checkouts


@pytest.mark.parametrize(
    "key", ["uid", "namespace", "job_name", "request_sha256", "outbox_id", "digest"]
)
def test_corrupt_original_receipt_blocks_cancellation_before_sdk(cancellation_case, key):
    c = cancellation_case
    with c.factory() as db, db.begin():
        fact = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == c.attempt_id, WorkflowEvent.event_key == "ray.submitted"
            )
        )
        if key == "digest":
            fact.result_digest = "0" * 64
        else:
            fact.payload = {**fact.payload, key: str(uuid.uuid4())}
            fact.result_digest = canonical_request_hash(fact.payload)
    deliver(c)
    assert not c.deletions and not c.checkouts
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED


def test_current_native_context_cannot_retire_another_cluster(cancellation_case):
    c = cancellation_case
    c.k8s.custom.api_client.configuration.host = "https://changed-native.invalid"
    deliver(c)
    assert not c.deletions and not c.k8s.ray_job.called
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED


def test_live_native_cancellation_cannot_be_stolen_or_acknowledge_successor(cancellation_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = cancellation_case
    entered, release = Event(), Event()

    def hold(stage):
        if stage == "delete_before":
            c.cancel_callback = None
            entered.set()
            assert release.wait(15)

    c.cancel_callback = hold
    with ThreadPoolExecutor(max_workers=1) as workers:
        future = workers.submit(deliver, c)
        try:
            assert entered.wait(10)
            retry(c)
            assert not c.deletions
            with c.factory() as db:
                assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.PENDING
                unit = next(
                    v for k, v in history(c, db).items() if k.startswith(cancellation.START)
                )
                assert db.get(OutboxEntry, c.cancel_id).available_at <= datetime.fromisoformat(
                    unit.payload["expires_at"]
                ) + timedelta(seconds=1)
        finally:
            release.set()
        future.result(timeout=15)
    assert c.deletions == ["original-factory-uid"]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.PENDING
        assert any(
            k.startswith(cancellation.RESULT) and v.payload["absent"]
            for k, v in history(c, db).items()
        )
    retry(c)
    assert c.deletions == ["original-factory-uid"]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("field", ["command", "workload", "target"])
def test_rehashed_original_creation_source_cannot_authorize_retirement(cancellation_case, field):
    c = cancellation_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        if field == "command":
            from automl_api.models.workflows import WorkflowCommand

            command = db.get(WorkflowCommand, entry.command_id)
            command.request_payload = {**command.request_payload, "changed": True}
            command.request_hash = canonical_request_hash(command.request_payload)
        elif field == "workload":
            db.get(WorkflowAttempt, c.attempt_id).workload_identity = "changed-workload"
        else:
            resource = db.get(
                ray_submission.ModelRun
                if entry.aggregate_type == "model_run"
                else ray_submission.ProfilingJob,
                entry.aggregate_id,
            )
            resource.target_column = "changed-target"
    deliver(c)
    assert not c.deletions and not c.checkouts
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED


@pytest.mark.parametrize("fault", ["changed_context", "invalid_context"])
def test_uncertain_context_result_cannot_be_reused_as_original_absence(cancellation_case, fault):
    c = cancellation_case
    original_cfg = c.k8s.custom.api_client.configuration

    def change(stage):
        if stage == "delete_after":
            c.cancel_callback = None
            if fault == "changed_context":
                original_cfg.host = "https://changed-context.invalid"
            else:
                original_cfg.verify_ssl = False

    c.cancel_callback = change
    deliver(c)
    with c.factory() as db:
        facts = history(c, db)
        result = next(v for k, v in facts.items() if k.startswith(cancellation.RESULT))
        assert result.payload["observed_absent"] and result.payload["absent"] is False
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED
    original_cfg.host, original_cfg.verify_ssl = "https://owned-factory.invalid", True
    before = len(c.checkouts)
    retry(c)
    assert len(c.checkouts) > before and c.deletions == ["original-factory-uid"]
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED


def test_sdk_construction_errors_are_redacted_and_bounded(cancellation_case, monkeypatch):
    c = cancellation_case
    calls = []

    def fail():
        calls.append(True)
        raise ConnectionError("https://user:secret@owned.invalid/token?token=secret")

    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", fail)
    deliver(c)
    with c.factory() as db:
        assert "secret" not in db.get(OutboxEntry, c.cancel_id).last_error
    for _ in range(8):
        retry(c)
    assert len(calls) == 5 and not c.deletions
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.cancel_id)
        assert entry.status == OutboxStatus.PENDING
        assert "secret" not in (entry.last_error or "")
        assert "retry budget" in entry.last_error
        assert not any(k.startswith(cancellation.START) for k in history(c, db))
