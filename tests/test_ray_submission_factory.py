"""Actual dispatcher creates outside SQL and commits identity before activation."""

import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api import workflow_reconciler
from automl_api.models.enums import AttemptStatus, CommandStatus, DatasetStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import ray_submission, reconciler, workflow_state
from automl_api.services.deletion_runtime import runtime_targets
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.contracts import DeletionPending
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_submission_receipts import committed_submission as committed_submission


@pytest.fixture
def factory_case(committed_submission, monkeypatch):
    c = committed_submission
    c.factory = sessionmaker(c.engine, expire_on_commit=False)
    c.checkouts, c.creations, c.activations, c.callback = [], [], [], None
    c.native = None

    def checkpoint(stage):
        c.checkouts.append(c.engine.pool.checkedout())
        if c.callback:
            c.callback(stage)

    def create(body):
        checkpoint("create_before")
        c.native = copy.deepcopy(body)
        c.native["metadata"].update(uid="original-factory-uid", resourceVersion="1")
        c.creations.append(copy.deepcopy(c.native))
        checkpoint("create_after")
        return copy.deepcopy(c.native)

    def activate(body, uid):
        checkpoint("activate_before")
        with c.factory() as db:
            attempt = db.get(WorkflowAttempt, c.attempt_id)
            assert attempt.status == AttemptStatus.SUBMITTED
            assert reconciler._ray_submission(db, attempt)[0]["uid"] == uid
        assert c.native["metadata"]["uid"] == uid
        if c.native["spec"]["suspend"]:
            c.native["spec"]["suspend"] = False
            c.activations.append(uid)
        checkpoint("activate_after")
        return copy.deepcopy(c.native)

    c.k8s.create_ray_job.side_effect = create
    c.k8s.ensure_service_account.side_effect = lambda *_args, **_kwargs: checkpoint(
        "service_account"
    )
    c.k8s.activate_ray_job.side_effect = activate
    c.k8s.custom.api_client.configuration = SimpleNamespace(
        host="https://owned-factory.invalid", verify_ssl=True, ssl_ca_cert=None, retries=0
    )

    def construct(*_args, **_kwargs):
        checkpoint("construct")
        return c.k8s

    monkeypatch.setattr(reconciler, "KubernetesTrainingClient", construct)
    # The new factory owns construction independently from the legacy Session dispatcher.
    monkeypatch.setattr(workflow_reconciler, "KubernetesTrainingClient", construct)
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        command = db.get(WorkflowCommand, entry.command_id)
        command.operation = (
            "training.launch" if entry.topic == "ray.training.submit" else "dataset.profile"
        )
        command.request_hash = canonical_request_hash(command.request_payload)
        command.resource_type, command.resource_id = entry.aggregate_type, entry.aggregate_id
        command.status = CommandStatus.RUNNING
        c.actor_id, c.project_id = command.actor_id, entry.project_id
    c.construct = construct
    return c


def test_actual_dispatcher_releases_sql_and_commits_identity_before_activation(factory_case):
    c = factory_case
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert c.checkouts and set(c.checkouts) == {0}
    assert len(c.creations) == 1 and c.creations[0]["spec"]["suspend"] is True
    assert c.activations == ["original-factory-uid"]
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert attempt.status == AttemptStatus.SUBMITTED
        assert reconciler._ray_submission(db, attempt)[0]["uid"] == "original-factory-uid"
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"
        units = [
            row
            for key, row in events(c, db).items()
            if key in {ray_submission.SA_START, ray_submission.CREATE_START}
            or key.startswith(ray_submission.ACT_START)
        ]
        assert len(units) == 3 and len({row.result_digest for row in units}) == 3
        assert all(row.payload["operation"] == row.event_key for row in units)
    assert (
        c.k8s.ensure_service_account.call_args.kwargs["namespace"]
        == c.creations[0]["metadata"]["namespace"]
    )
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert len(c.creations) == len(c.activations) == 1


def events(c, db):
    return {
        row.event_key: row
        for row in db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == c.attempt_id))
    }


def retry(c):
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.available_at = datetime.now(UTC) - timedelta(seconds=1)
        if entry.lease_expires_at is not None:
            entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-successor")


@pytest.mark.parametrize("fault", ["actor", "project", "source", "request", "version", "context"])
def test_known_create_fact_survives_current_policy_change_without_projection_or_recreate(
    factory_case, fault
):
    c = factory_case
    original = {}

    def change(stage):
        if stage != "create_after":
            return
        c.callback = None
        with c.factory() as db, db.begin():
            entry = db.get(OutboxEntry, c.entry_id)
            resource = db.get(
                ray_submission.ModelRun
                if entry.topic == "ray.training.submit"
                else ray_submission.ProfilingJob,
                entry.aggregate_id,
            )
            version = db.get(ray_submission.DatasetVersion, resource.dataset_version_id)
            if fault == "actor":
                db.get(User, c.actor_id).is_active = False
            elif fault == "project":
                project = db.get(Project, c.project_id)
                original["settings"] = copy.deepcopy(project.settings)
                project.settings = {**project.settings, "deletion_in_progress": True}
            elif fault == "source":
                original["target"] = resource.target_column
                resource.target_column = "changed-target"
            elif fault == "request":
                original["payload"] = copy.deepcopy(entry.payload)
                entry.payload = {**entry.payload, "changed": True}
            elif fault == "version":
                original["status"] = version.status
                version.status = DatasetStatus.ARCHIVED
            else:
                c.k8s.custom.api_client.configuration.host = "https://changed-cluster.invalid"

    c.callback = change
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    with c.factory() as db:
        history = events(c, db)
        assert ray_submission.CREATE_RESULT in history and "ray.submitted" not in history
        assert db.get(WorkflowAttempt, c.attempt_id).status == AttemptStatus.CLAIMED
    assert len(c.creations) == 1 and not c.activations
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        resource = db.get(
            ray_submission.ModelRun
            if entry.topic == "ray.training.submit"
            else ray_submission.ProfilingJob,
            entry.aggregate_id,
        )
        if fault == "actor":
            db.get(User, c.actor_id).is_active = True
        elif fault == "project":
            db.get(Project, c.project_id).settings = original["settings"]
        elif fault == "source":
            resource.target_column = original["target"]
        elif fault == "request":
            entry.payload = original["payload"]
        elif fault == "version":
            db.get(ray_submission.DatasetVersion, resource.dataset_version_id).status = original[
                "status"
            ]
        else:
            c.k8s.custom.api_client.configuration.host = "https://owned-factory.invalid"
    retry(c)
    assert len(c.creations) == len(c.activations) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"


@pytest.mark.parametrize("window", ["create_before", "create_after"])
def test_unknown_create_is_not_reissued_adopted_or_hidden_from_deletion(factory_case, window):
    c = factory_case

    def lose(stage):
        if stage == window:
            c.callback = None
            raise ConnectionError("owned unknown create outcome")

    c.callback = lose
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    for _ in range(2):
        retry(c)
    assert len(c.creations) == int(window == "create_after") and not c.activations
    c.k8s.ray_job.assert_not_called()
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert not attempt.ray_job_name
        assert ray_submission.CREATE_START in events(c, db)
        assert ray_submission.CREATE_RESULT not in events(c, db)
        with pytest.raises(DeletionPending, match="requires native qualification"):
            runtime_targets(db, c.project_id, [], whole_project=True)


@pytest.mark.parametrize("window", ["create_fact", "published"])
def test_known_reply_recovers_publication_loss_without_creating_again(
    factory_case, monkeypatch, window
):
    c = factory_case
    if window == "create_fact":
        original = ray_submission._fact

        def lose(*args, **kwargs):
            original(*args, **kwargs)
            if args[3] == ray_submission.CREATE_RESULT:
                raise ConnectionError("owned lost create-fact acknowledgement")

        monkeypatch.setattr(ray_submission, "_fact", lose)
    else:
        original = c.k8s.activate_ray_job.side_effect
        c.k8s.activate_ray_job.side_effect = ConnectionError("owned loss after publication")
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    if window == "create_fact":
        monkeypatch.setattr(ray_submission, "_fact", original)
    else:
        c.k8s.activate_ray_job.side_effect = original
        advance_native_clock(c, monkeypatch)
    retry(c)
    assert len(c.creations) == len(c.activations) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"


@pytest.mark.parametrize("topic", sorted(ray_submission.TOPICS))
def test_session_dispatcher_cannot_escape_native_factory(topic):
    from unittest.mock import MagicMock

    entry = SimpleNamespace(id="owned", topic=topic)
    db = MagicMock()
    entry.status = ray_submission.reconciler.OutboxStatus.CLAIMED
    entry.lease_owner, entry.lease_expires_at = "owned", datetime.now(UTC) + timedelta(seconds=60)
    db.get.return_value = entry
    k8s = MagicMock()
    with pytest.raises(RuntimeError, match="factory-owned"):
        reconciler.reconcile_entry(db, entry, worker_id="owned", k8s=k8s)
    assert not k8s.mock_calls


def advance_native_clock(c, monkeypatch):
    with c.factory() as db:
        expiry = max(
            datetime.fromisoformat(row.payload["expires_at"])
            for row in events(c, db).values()
            if row.event_key.startswith(ray_submission.ACT_START)
        ) + timedelta(seconds=1)

    class Advanced(datetime):
        @classmethod
        def now(cls, tz=None):
            return expiry if tz else expiry.replace(tzinfo=None)

    for module in (ray_submission, workflow_state, reconciler):
        monkeypatch.setattr(module, "datetime", Advanced)


def test_changed_cluster_cannot_publish_a_recorded_create_reply(factory_case, monkeypatch):
    c = factory_case
    original = ray_submission._fact

    def lose(*args, **kwargs):
        original(*args, **kwargs)
        if args[3] == ray_submission.CREATE_RESULT:
            raise ConnectionError("owned fact acknowledgement loss")

    monkeypatch.setattr(ray_submission, "_fact", lose)
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    monkeypatch.setattr(ray_submission, "_fact", original)
    c.k8s.custom.api_client.configuration.host = "https://changed-cluster.invalid"
    retry(c)
    with c.factory() as db:
        assert ray_submission.CREATE_RESULT in events(c, db) and "ray.submitted" not in events(
            c, db
        )
    assert len(c.creations) == 1 and not c.activations
    c.k8s.custom.api_client.configuration.host = "https://owned-factory.invalid"
    retry(c)
    assert len(c.creations) == len(c.activations) == 1


def test_activation_response_loss_inspects_original_without_second_patch(factory_case, monkeypatch):
    c = factory_case

    def lose(stage):
        if stage == "activate_after":
            c.callback = None
            raise ConnectionError("owned activation acknowledgement loss")

    c.callback = lose
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert len(c.creations) == len(c.activations) == 1 and c.native["spec"]["suspend"] is False
    advance_native_clock(c, monkeypatch)
    retry(c)
    assert len(c.creations) == len(c.activations) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.entry_id).status.value == "delivered"


def test_corrupt_published_uid_cannot_activate_or_acknowledge_another_native_target(
    factory_case, monkeypatch
):
    c = factory_case
    original = reconciler.publish_ray_submission
    saved = {}

    def corrupt(db, entry, plan, created):
        result = original(db, entry, plan, created)
        row = events(c, db)["ray.submitted"]
        saved["payload"] = copy.deepcopy(row.payload)
        row.payload = {**row.payload, "uid": "replacement"}
        row.result_digest = canonical_request_hash(row.payload)
        db.flush()
        return result

    monkeypatch.setattr(reconciler, "publish_ray_submission", corrupt)
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert len(c.creations) == 1 and not c.activations
    with c.factory() as db, db.begin():
        row = events(c, db)["ray.submitted"]
        row.payload = saved["payload"]
        row.result_digest = canonical_request_hash(row.payload)
        assert db.get(OutboxEntry, c.entry_id).status.value != "delivered"
    monkeypatch.setattr(reconciler, "publish_ray_submission", original)
    retry(c)
    assert len(c.creations) == len(c.activations) == 1


def test_construction_failure_is_redacted_and_bounded_before_native_issuance(factory_case):
    c = factory_case

    def fail(stage):
        if stage == "construct":
            raise RuntimeError("https://owned:local-test-secret@unavailable.invalid/api")

    c.callback = fail
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    with c.factory() as db:
        row = db.get(OutboxEntry, c.entry_id)
        assert row.last_error and "local-test-secret" not in row.last_error
        budget = db.get(WorkflowAttempt, c.attempt_id).retry_budget
    for _ in range(budget + 1):
        retry(c)
    assert len(c.checkouts) == budget
    assert not c.creations and not c.activations
    c.k8s.ensure_service_account.assert_not_called()
    with c.factory() as db:
        assert "budget exhausted" in db.get(OutboxEntry, c.entry_id).last_error
        assert ray_submission.SA_START not in events(c, db)


def test_live_unknown_create_is_not_stolen_and_late_fact_cannot_acknowledge_successor(factory_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = factory_case
    entered, release = Event(), Event()

    def hold(stage):
        if stage == "create_before":
            entered.set()
            assert release.wait(15)

    c.callback = hold
    with ThreadPoolExecutor(max_workers=1) as pool:
        old = pool.submit(workflow_reconciler.run_once, c.factory, worker_id="owned-ray-factory")
        try:
            assert entered.wait(15)
            with c.factory() as db, db.begin():
                entry = db.get(OutboxEntry, c.entry_id)
                entry.lease_owner = "successor"
                entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
                entry.delivery_attempts += 1
            ray_submission.reconcile_ray_submission(
                c.factory, c.entry_id, "successor", k8s_factory=c.construct
            )
            assert not c.creations and not c.activations
        finally:
            release.set()
        old.result(timeout=15)
    c.callback = None
    with c.factory() as db:
        assert ray_submission.CREATE_RESULT in events(c, db)
        assert "ray.submitted" not in events(c, db)
        assert db.get(OutboxEntry, c.entry_id).status.value != "delivered"
    retry(c)
    assert len(c.creations) == len(c.activations) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("namespace", None),
        ("namespace", ""),
        ("namespace", "bad/target"),
        ("name", ""),
        ("identity", ""),
        ("identity", "bad/target"),
    ],
)
def test_invalid_target_cannot_fall_back_to_client_defaults(
    factory_case, monkeypatch, field, value
):
    c = factory_case
    with c.factory() as db, db.begin():
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        if field == "identity":
            attempt.workload_identity = value
        training = attempt.stage.value == "training_run"
    if field != "identity":
        name = "build_ray_job_manifest" if training else "build_dataset_ray_job_manifest"
        original = getattr(reconciler, name)

        def invalid(*args, **kwargs):
            body = original(*args, **kwargs)
            body["metadata"][field] = value
            return body

        monkeypatch.setattr(reconciler, name, invalid)
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert not c.checkouts and not c.creations and not c.activations
    with c.factory() as db:
        assert ray_submission.SA_START not in events(c, db)
        assert "complete native target" in db.get(OutboxEntry, c.entry_id).last_error


@pytest.mark.parametrize(
    "bad", ["http", "credentials", "query", "fragment", "tls", "hostname", "retries"]
)
def test_unverified_native_context_never_starts_an_external_operation(factory_case, bad):
    c = factory_case
    cfg = c.k8s.custom.api_client.configuration
    values = {
        "http": "http://owned.invalid",
        "credentials": "https://user:secret@owned.invalid",
        "query": "https://owned.invalid/?token=secret",
        "fragment": "https://owned.invalid/#secret",
    }
    if bad in values:
        cfg.host = values[bad]
    elif bad == "tls":
        cfg.verify_ssl = False
    elif bad == "hostname":
        cfg.assert_hostname = False
    else:
        cfg.retries = 3
    workflow_reconciler.run_once(c.factory, worker_id="owned-ray-factory")
    assert c.checkouts == [0] and not c.creations and not c.activations
    with c.factory() as db:
        assert ray_submission.SA_START not in events(c, db)
        assert "verified HTTPS" in db.get(OutboxEntry, c.entry_id).last_error
