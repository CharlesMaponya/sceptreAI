"""Factory-owned first legacy analysis delivery; original preflight restrictions remain."""

import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api import workflow_reconciler
from automl_api.models.enums import AttemptStatus, OutboxStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import analysis_migration
from automl_api.services.workflow_state import canonical_request_hash
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_migration import legacy as legacy
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.fixture
def migration_case(legacy, monkeypatch):
    engine, run_id, entry_id, manifest, k8s = legacy
    factory = sessionmaker(engine, expire_on_commit=False)
    c = SimpleNamespace(
        engine=engine,
        factory=factory,
        run_id=run_id,
        entry_id=entry_id,
        manifest=manifest,
        k8s=k8s,
        callback=None,
        checkouts=[],
    )
    with factory() as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        entry.status = OutboxStatus.PENDING
        entry.delivery_attempts = 0
        entry.available_at = datetime(2000, 1, 1, tzinfo=UTC)
        entry.lease_owner = entry.lease_expires_at = None

    def checkpoint(stage):
        c.checkouts.append(engine.pool.checkedout())
        assert c.checkouts[-1] == 0
        if c.callback:
            c.callback(stage)

    def read(**kw):
        checkpoint("job")
        raise ApiException(status=404)

    def pods(**kw):
        checkpoint("pods")
        return SimpleNamespace(items=[])

    def create(body):
        checkpoint("create")
        native = copy.deepcopy(body)
        native["metadata"].update(uid="migrated-original-uid", resourceVersion="1")
        return client.ApiClient()._ApiClient__deserialize(native, "V1Job")

    def activate(created):
        checkpoint("activate")
        active = copy.deepcopy(created)
        active["spec"]["suspend"] = False
        return active

    k8s.batch.read_namespaced_job.side_effect = read
    k8s.core.list_namespaced_pod.side_effect = pods
    k8s.create_job.side_effect = create
    k8s.activate_job.side_effect = activate
    k8s.ensure_service_account.side_effect = lambda *a, **kw: checkpoint("sa")
    k8s.custom.api_client.configuration = SimpleNamespace(
        host="https://owned-legacy-analysis.invalid", verify_ssl=True, retries=0, ssl_ca_cert=None
    )

    def construct():
        checkpoint("construct")
        return k8s

    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", construct)
    return c


def deliver(c, worker="legacy-factory"):
    return workflow_reconciler.run_once(c.factory, worker_id=worker)


def retry(c):
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.available_at = datetime(2000, 1, 1, tzinfo=UTC)
        if entry.lease_expires_at is not None:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    return deliver(c, "legacy-recovery")


def test_actual_legacy_dispatch_commits_absence_before_tracking_and_defers_creation(migration_case):
    c = migration_case
    deliver(c)
    c.k8s.create_job.assert_not_called()
    c.k8s.ensure_service_account.assert_not_called()
    assert c.checkouts and set(c.checkouts) == {0}
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        run = db.get(ModelRun, c.run_id)
        state = analysis_migration._state(entry)
        assert len(state["units"]) == len(state["results"]) == 1
        assert state["results"][state["selected_unit"]]["absent"] is True
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        attempt = db.get(WorkflowAttempt, run.tags["workflow_attempt_id"])
        assert attempt.status == AttemptStatus.PENDING
        assert entry.payload["manifest"]["metadata"]["name"] != c.manifest["metadata"]["name"]
        assert (
            entry.payload["manifest"]["spec"]["template"]["spec"]["containers"][0]["image"]
            == c.k8s.settings.training_image
        )
    retry(c)
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        attempts = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == c.run_id))
        )
        assert len(attempts) == 1 and attempts[0].status == AttemptStatus.SUBMITTED
    assert c.k8s.create_job.call_count == 1 and c.k8s.activate_job.call_count == 1


@pytest.mark.parametrize("change", ["source", "owner", "actor"])
def test_legacy_absence_fact_survives_stale_tracking_publication(migration_case, change):
    c = migration_case

    def changed(stage):
        if stage != "pods":
            return
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            if change == "source":
                entry.payload = {**entry.payload, "changed": True}
            elif change == "owner":
                entry.lease_owner = "successor"
            else:
                db.get(User, db.get(ModelRun, c.run_id).created_by_id).is_active = False

    c.callback = changed
    deliver(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        state = analysis_migration._state(entry)
        assert any(r["absent"] for r in state["results"].values())
        assert entry.status != OutboxStatus.DELIVERED
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == c.run_id))
        )
    c.k8s.create_job.assert_not_called()


@pytest.mark.parametrize("present", ["job", "pod", "malformed"])
def test_legacy_existing_or_unqualified_inventory_never_creates_attempt(migration_case, present):
    c = migration_case
    if present == "job":
        c.k8s.batch.read_namespaced_job.side_effect = None
    else:
        c.k8s.core.list_namespaced_pod.side_effect = lambda **kw: SimpleNamespace(
            items=[object()] if present == "pod" else None
        )
    deliver(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        state = analysis_migration._state(entry)
        assert not any(r["absent"] for r in state["results"].values())
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == c.run_id))
        )
    c.k8s.create_job.assert_not_called()


def test_legacy_readonly_failure_is_recorded_and_budget_neutral(migration_case):
    c = migration_case
    c.k8s.batch.read_namespaced_job.side_effect = ApiException(status=503)
    deliver(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        state = analysis_migration._state(entry)
        assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
        assert len(state["results"]) == 1 and not next(iter(state["results"].values()))["absent"]
        assert "preflight unavailable" in entry.last_error
    c.k8s.batch.read_namespaced_job.side_effect = ApiException(status=404)
    retry(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert len(analysis_migration._state(entry)["results"]) == 2
        assert db.get(ModelRun, c.run_id).tags.get("workflow_attempt_id")


def test_legacy_source_digest_policy_precedes_migration_observation(migration_case):
    from automl_api.models.datasets import DatasetVersion

    c = migration_case
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, db.get(ModelRun, c.run_id).dataset_version_id)
        version.content_hash_verification_status = "pending"
    deliver(c)
    assert c.checkouts == []
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert analysis_migration.KEY not in entry.payload
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == c.run_id))
        )
    c.k8s.create_job.assert_not_called()


def test_late_legacy_observation_preserves_the_frozen_executable_request(migration_case):
    import uuid

    c = migration_case
    deliver(c)

    def late(stage):
        if stage != "create":
            return
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            state = analysis_migration._state(entry)
            selected = state["selected_unit"]
            key = str(uuid.uuid4())
            unit = copy.deepcopy(state["units"][selected])
            unit["operation"] = key
            fact = copy.deepcopy(state["results"][selected])
            fact["start_sha256"] = canonical_request_hash(unit)
            state["units"][key] = unit
            state["results"][key] = fact
            analysis_migration._put(entry, state)

    c.callback = late
    retry(c)
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.status == OutboxStatus.DELIVERED
        state = analysis_migration._state(entry)
        assert len(state["units"]) == len(state["results"]) == 2
    assert c.k8s.create_job.call_count == c.k8s.activate_job.call_count == 1


def test_legacy_command_operation_cannot_authorize_another_workflow(migration_case):
    from automl_api.models.workflows import WorkflowCommand

    c = migration_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        db.get(WorkflowCommand, entry.command_id).operation = "run.delete"
    deliver(c)
    assert not c.checkouts
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert analysis_migration.KEY not in entry.payload
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == c.run_id))
        )
