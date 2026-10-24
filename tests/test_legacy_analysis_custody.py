"""Legacy Job observation and cleanup use recorded physical identity, never name alone."""

import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt, WorkflowEvent
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.training import analysis
from automl_api.workflow_reconciler import observe_once
from kubernetes import client
from kubernetes.client import ApiException
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


class NativeJobs:
    def __init__(self, namespace, job, pod):
        self.jobs = {(namespace, job.metadata.name): job}
        self.pods = [pod]
        self.reads = []
        self.deletes = []
        self.on_read = None
        self.on_delete = None
        self.hold_deletion = False
        self.lose_delete_reply = False

    def read(self, name, namespace, **_):
        self.reads.append((namespace, name))
        hook, self.on_read = self.on_read, None
        if hook is not None:
            hook()
        if (namespace, name) not in self.jobs:
            raise ApiException(status=404)
        return deepcopy(self.jobs[(namespace, name)])

    def list_pods(self, namespace, label_selector="", **_):
        name = label_selector.split("=", 1)[1] if label_selector else None
        return client.V1PodList(
            items=[
                deepcopy(pod)
                for pod in self.pods
                if pod.metadata.namespace == namespace
                and (name is None or pod.metadata.labels.get("job-name") == name)
            ]
        )

    def delete(self, name, namespace, body, **_):
        hook = self.on_delete
        if hook is not None:
            hook()
        job = self.jobs.get((namespace, name))
        if job is None:
            raise ApiException(status=404)
        uid = body.preconditions.uid if body.preconditions is not None else None
        if uid is not None and uid != job.metadata.uid:
            raise ApiException(status=409)
        self.deletes.append((namespace, name, uid, body.propagation_policy))
        if self.hold_deletion:
            job.metadata.deletion_timestamp = datetime.now(UTC)
        else:
            del self.jobs[(namespace, name)]
            self.pods = [
                pod
                for pod in self.pods
                if not any(
                    ref.uid == job.metadata.uid for ref in pod.metadata.owner_references or []
                )
            ]
        if self.lose_delete_reply:
            self.lose_delete_reply = False
            raise ConnectionError("Native delete committed; acknowledgement lost")
        return client.V1Status(status="Success")


@pytest.fixture
def legacy_scope(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, _, _, result, *_ = analysis_scope
    namespace, name, uid = f"legacy-{uuid.uuid4().hex[:8]}", f"job-{run_id.hex}", uuid.uuid4().hex
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        db.execute(delete(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt_id))
        db.execute(delete(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id))
        run.status = RunStatus.QUEUED
        run.k8s_namespace, run.k8s_job_name = namespace, name
        run.tags = {"desired_state": "kubernetes_submitted"}
        project_id = run.project_id
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    monkeypatch.delenv("AUTOML_FENCING_TOKEN", raising=False)
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "")
    labels = {"automl.platform/project-id": str(project_id), "automl.platform/run-id": str(run_id)}
    job = client.V1Job(
        metadata=client.V1ObjectMeta(namespace=namespace, name=name, uid=uid, labels=labels),
        status=client.V1JobStatus(active=1),
    )
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            namespace=namespace,
            name=name + "-pod",
            uid=uuid.uuid4().hex,
            labels={**labels, "job-name": name},
            owner_references=[
                client.V1OwnerReference(
                    api_version="batch/v1", kind="Job", name=name, uid=uid, controller=True
                )
            ],
        ),
        status=client.V1PodStatus(phase="Running", container_statuses=[]),
    )
    backend = NativeJobs(namespace, job, pod)
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s._configured, k8s._configuration_error = True, None
    k8s.settings = Settings(training_namespace=namespace)
    k8s.batch = SimpleNamespace(
        read_namespaced_job_status=backend.read,
        read_namespaced_job=backend.read,
        delete_namespaced_job=backend.delete,
    )
    k8s.core = SimpleNamespace(list_namespaced_pod=backend.list_pods)
    return engine, run_id, project_id, k8s, backend, result


def factory(engine):
    return lambda: Session(engine)


def test_legacy_read_releases_connection_and_run_lock(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    counts = []

    def probe():
        counts.append(engine.pool.checkedout())
        assert counts[-1] == 0
        with Session(engine) as db, db.begin():
            db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))

    backend.on_read = probe
    assert observe_once(factory(engine), k8s=k8s) == 1
    assert counts == [0]
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING


def test_legacy_read_uses_stored_namespace(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    actual_namespace = next(iter(backend.jobs))[0]
    k8s.settings = replace(k8s.settings, training_namespace="wrong-default")
    assert observe_once(factory(engine), k8s=k8s) == 1
    assert backend.reads and all(namespace == actual_namespace for namespace, _ in backend.reads)
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING


def test_legacy_foreign_job_identity_cannot_change_run(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    next(iter(backend.jobs.values())).metadata.labels["automl.platform/project-id"] = str(
        uuid.uuid4()
    )
    observe_once(factory(engine), k8s=k8s)
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.QUEUED
    assert backend.deletes == []


def test_legacy_concurrent_cancellation_wins_over_observation(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    changed = []

    def cancel():
        with Session(engine) as db, db.begin():
            run = db.scalar(
                select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True)
            )
            run.status = RunStatus.CANCELLED
        changed.append(True)

    backend.on_read = cancel
    observe_once(factory(engine), k8s=k8s)
    assert changed == [True]
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.CANCELLED
    assert backend.deletes == []


def test_legacy_committed_report_wins_over_observation(legacy_scope):
    engine, run_id, _, k8s, backend, result = legacy_scope
    committed = []

    def publish():
        with Session(engine) as db, db.begin():
            db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True))
        analysis._persist_analysis_result(run_id, result)
        committed.append(True)

    backend.on_read = publish
    observe_once(factory(engine), k8s=k8s)
    assert committed == [True]
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(ModelRun, run_id).tags["artifact_uri"]
    assert backend.deletes == []


def fatal_waiting(backend):
    backend.pods[0].status.container_statuses = [
        client.V1ContainerStatus(
            image="missing",
            image_id="",
            name="worker",
            ready=False,
            restart_count=0,
            state=client.V1ContainerState(
                waiting=client.V1ContainerStateWaiting(
                    reason="ErrImageNeverPull", message="Image is not present"
                )
            ),
        )
    ]


def test_legacy_fatal_waiting_commits_intent_before_native_delete(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    target = next(iter(backend.jobs.values())).metadata
    fatal_waiting(backend)
    assert observe_once(factory(engine), k8s=k8s) == 1
    assert backend.deletes == []
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run_id,
                OutboxEntry.topic == "kubernetes.analysis.legacy.cancel",
            )
        )
        assert run.status == RunStatus.FAILED
        assert entry is not None
        assert entry.payload["uid"] == target.uid
        assert entry.payload["namespace"] == target.namespace
        assert entry.payload["job_name"] == target.name


def prepare_cleanup(scope):
    engine, run_id, _, k8s, backend, _ = scope
    fatal_waiting(backend)
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.aggregate_id == run_id,
                OutboxEntry.topic == "kubernetes.analysis.legacy.cancel",
            )
        )
        assert entry is not None
        return entry.id, entry.command_id


def claim_cleanup(engine, entry_id, owner="cleanup"):
    from datetime import timedelta

    from automl_api.models.enums import OutboxStatus

    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = owner
        entry.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
        entry.delivery_attempts += 1


def deliver(scope, entry_id, owner="cleanup"):
    from automl_api.services.legacy_analysis import reconcile_legacy_cleanup

    return reconcile_legacy_cleanup(factory(scope[0]), entry_id, owner, k8s=scope[3])


def test_native_cleanup_releases_sql_and_commits_start_before_uid_delete(legacy_scope):
    from automl_api.models.enums import CommandStatus, OutboxStatus
    from automl_api.models.workflows import WorkflowCommand

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    observed = []

    def native():
        assert engine.pool.checkedout() == 0
        with Session(engine) as db:
            command = db.get(WorkflowCommand, command_id)
            assert command.response_payload["native_nonce"]
            assert command.response_payload["native_started_at"]
            assert command.lease_owner == "cleanup"
            assert db.get(OutboxEntry, entry_id).status == OutboxStatus.CLAIMED
        observed.append(True)

    backend.on_delete = native
    deliver(legacy_scope, entry_id)
    assert observed == [True] and len(backend.deletes) == 1
    assert backend.deletes[0][2] is not None and backend.deletes[0][3] == "Foreground"
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        command = db.get(WorkflowCommand, command_id)
        assert command.status == CommandStatus.SUCCEEDED
        assert command.response_payload["retired"]["uid"] == command.request_payload["uid"]
    deliver(legacy_scope, entry_id)
    assert len(backend.deletes) == 1


def test_native_lost_delete_reply_reconciles_without_second_delete(legacy_scope):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import WorkflowCommand

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    backend.lose_delete_reply = True
    with pytest.raises(ConnectionError):
        deliver(legacy_scope, entry_id)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.PENDING
        nonce = db.get(WorkflowCommand, command_id).response_payload["native_nonce"]
        assert "retired" not in db.get(WorkflowCommand, command_id).response_payload
    assert backend.jobs == {} and len(backend.deletes) == 1
    claim_cleanup(engine, entry_id, "replay")
    deliver(legacy_scope, entry_id, "replay")
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowCommand, command_id).response_payload["native_nonce"] == nonce
    assert len(backend.deletes) == 1


def test_pending_finalizers_preserve_budget_and_owned_pods_block_retirement(legacy_scope):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import WorkflowCommand

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    backend.hold_deletion = True
    for _ in range(32):
        claim_cleanup(engine, entry_id)
        deliver(legacy_scope, entry_id)
        with Session(engine) as db:
            entry = db.get(OutboxEntry, entry_id)
            assert entry.status == OutboxStatus.PENDING and entry.delivery_attempts == 0
            assert "retired" not in db.get(WorkflowCommand, command_id).response_payload
    assert len(backend.deletes) == 1
    backend.jobs.clear()
    backend.pods[0].metadata.labels["job-name"] = "renamed-owned-pod"
    claim_cleanup(engine, entry_id)
    deliver(legacy_scope, entry_id)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.PENDING
    backend.pods.clear()
    claim_cleanup(engine, entry_id)
    deliver(legacy_scope, entry_id)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED


def test_replacement_uid_and_its_pods_are_never_deleted(legacy_scope):
    from automl_api.models.enums import OutboxStatus

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, _ = prepare_cleanup(legacy_scope)
    replacement = next(iter(backend.jobs.values()))
    replacement.metadata.uid = uuid.uuid4().hex
    backend.pods[0].metadata.owner_references[0].uid = replacement.metadata.uid
    claim_cleanup(engine, entry_id)
    deliver(legacy_scope, entry_id)
    assert backend.deletes == [] and backend.jobs and backend.pods
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED


def test_expired_owner_cannot_start_native_cleanup(legacy_scope):
    from datetime import timedelta

    from automl_api.services.workflow_state import StaleFence

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, _ = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    backend.reads.clear()
    with Session(engine) as db, db.begin():
        db.get(OutboxEntry, entry_id).lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(StaleFence):
        deliver(legacy_scope, entry_id)
    assert backend.reads == [] and backend.deletes == []
    claim_cleanup(engine, entry_id, "replacement")
    deliver(legacy_scope, entry_id, "replacement")
    assert len(backend.deletes) == 1


@pytest.mark.parametrize("takeover", [False, True])
def test_native_completion_is_recorded_after_expiry_without_stale_publication(
    legacy_scope, takeover
):
    from datetime import timedelta

    from automl_api.models.enums import CommandStatus, OutboxStatus
    from automl_api.models.workflows import WorkflowCommand

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)

    def replace_owner():
        with Session(engine) as db, db.begin():
            entry = db.get(OutboxEntry, entry_id)
            if takeover:
                entry.lease_owner = "replacement"
            else:
                entry.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)

    backend.on_delete = replace_owner
    deliver(legacy_scope, entry_id)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.CLAIMED
        command = db.get(WorkflowCommand, command_id)
        assert command.status == CommandStatus.RUNNING
        assert command.response_payload["retired"]
    reads = len(backend.reads)
    claim_cleanup(engine, entry_id, "replacement")
    deliver(legacy_scope, entry_id, "replacement")
    assert len(backend.reads) == reads and len(backend.deletes) == 1
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED


@pytest.mark.parametrize("tamper", ["target", "terminal_run", "retirement_receipt"])
def test_changed_cleanup_binding_never_reaches_native_delete(legacy_scope, tamper):
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.workflow_state import StaleFence

    engine, run_id, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    backend.reads.clear()
    with Session(engine) as db, db.begin():
        if tamper == "target":
            entry = db.get(OutboxEntry, entry_id)
            entry.payload = {**entry.payload, "uid": "foreign"}
        elif tamper == "terminal_run":
            db.get(ModelRun, run_id).status = RunStatus.SUCCEEDED
        else:
            command = db.get(WorkflowCommand, command_id)
            command.response_payload = {"retired": {"uid": "foreign"}}
    with pytest.raises(StaleFence):
        deliver(legacy_scope, entry_id)
    assert backend.reads == [] and backend.deletes == []


@pytest.mark.parametrize("whole_project", [True, False])
def test_unknown_dead_cleanup_blocks_deletion_until_verified_retirement(
    legacy_scope, whole_project
):
    from automl_api.models.enums import CommandStatus, OutboxStatus, RunKind
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services import deletion
    from automl_api.services.workflow_state import replay_dead_outbox
    from fastapi import HTTPException

    engine, run_id, project_id, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    backend.lose_delete_reply = True
    with pytest.raises(ConnectionError):
        deliver(legacy_scope, entry_id)
    with Session(engine) as db, db.begin():
        entry = db.get(OutboxEntry, entry_id)
        entry.status = OutboxStatus.DEAD
        db.get(WorkflowCommand, command_id).status = CommandStatus.FAILED
        run = db.get(ModelRun, run_id)
        parent = ModelRun(
            project_id=project_id,
            dataset_version_id=run.dataset_version_id,
            created_by_id=run.created_by_id,
            run_kind=RunKind.TRAINING,
            status=RunStatus.FAILED,
            task_type=run.task_type,
        )
        db.add(parent)
        db.flush()
        parent_id = parent.id
        run.tags = {**run.tags, "source_training_run_id": str(parent_id)}
    with Session(engine) as db:
        user = db.get(User, db.get(Project, project_id).owner_id)
        with pytest.raises(HTTPException) as caught:
            deletion.request_deletion(db, user, project_id, None if whole_project else parent_id)
        assert caught.value.status_code == 409 and "legacy Job cleanup" in caught.value.detail
        replay_dead_outbox(db, entry_id, actor_id=user.id)
        db.commit()
    claim_cleanup(engine, entry_id, "verified-replay")
    deliver(legacy_scope, entry_id, "verified-replay")
    with Session(engine) as db:
        user = db.get(User, db.get(Project, project_id).owner_id)
        assert (
            deletion.request_deletion(db, user, project_id, None if whole_project else parent_id)[
                "status"
            ]
            == "pending"
        )
        db.rollback()
    assert len(backend.deletes) == 1


@pytest.mark.parametrize("lost", ["start_commit", "completion_commit"])
def test_lost_sql_ack_preserves_same_operation_and_never_duplicates_delete(legacy_scope, lost):
    from automl_api.models.enums import CommandStatus, OutboxStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.legacy_analysis import reconcile_legacy_cleanup
    from sqlalchemy import event

    engine, _, _, k8s, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    dropped = []

    def unreliable_factory():
        db = Session(engine)
        drop = []

        def before_commit(session):
            for value in list(session.dirty) + list(session.identity_map.values()):
                if isinstance(value, WorkflowCommand) and value.id == command_id:
                    payload = value.response_payload
                    if lost == "start_commit":
                        match = (
                            "native_nonce" in payload
                            and "native_ack" not in payload
                            and "retired" not in payload
                        )
                    else:
                        match = value.status == CommandStatus.SUCCEEDED
                    if match and not dropped:
                        drop.append(True)

        def after_commit(_session):
            if drop and not dropped:
                dropped.append(True)
                raise ConnectionError("SQL commit succeeded; acknowledgement lost")

        event.listen(db, "before_commit", before_commit)
        event.listen(db, "after_commit", after_commit)
        return db

    with pytest.raises(ConnectionError):
        reconcile_legacy_cleanup(unreliable_factory, entry_id, "cleanup", k8s=k8s)
    assert dropped == [True]
    with Session(engine) as db:
        command = db.get(WorkflowCommand, command_id)
        nonce = command.response_payload["native_nonce"]
        if lost == "completion_commit":
            assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
    if lost == "start_commit":
        assert backend.deletes == []
        claim_cleanup(engine, entry_id, "replay")
        deliver(legacy_scope, entry_id, "replay")
    else:
        deliver(legacy_scope, entry_id)
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowCommand, command_id).response_payload["native_nonce"] == nonce
    assert len(backend.deletes) == 1


def test_uid_precondition_protects_replacement_after_native_preflight(legacy_scope):
    from automl_api.models.enums import OutboxStatus

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, _ = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id)
    new_uid = uuid.uuid4().hex

    def replace_job():
        next(iter(backend.jobs.values())).metadata.uid = new_uid
        backend.pods[0].metadata.owner_references[0].uid = new_uid

    backend.on_delete = replace_job
    deliver(legacy_scope, entry_id)
    assert backend.deletes == []
    assert next(iter(backend.jobs.values())).metadata.uid == new_uid
    assert backend.pods[0].metadata.owner_references[0].uid == new_uid
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED


def test_actual_sdk_wire_delete_and_process_death_recover_same_uid(legacy_scope, tmp_path):
    import json
    import subprocess
    import sys
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Event, Thread

    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.legacy_analysis import reconcile_legacy_cleanup

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id, "child")
    namespace, name = next(iter(backend.jobs))
    accepted, release = Event(), Event()
    wire_errors = []
    serializer = client.ApiClient()

    class API(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send(self, status, body):
            content = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path.split("?", 1)[0] == f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}":
                job = backend.jobs.get((namespace, name))
                self.send(200, serializer.sanitize_for_serialization(job)) if job else self.send(
                    404, {"kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404}
                )
            elif self.path.split("?", 1)[0] == f"/api/v1/namespaces/{namespace}/pods":
                self.send(
                    200,
                    {
                        "apiVersion": "v1",
                        "kind": "PodList",
                        "metadata": {},
                        "items": serializer.sanitize_for_serialization(backend.pods),
                    },
                )
            else:
                self.send(404, {})

        def do_DELETE(self):
            try:
                assert self.path == f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}"
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with Session(engine) as db, db.begin():
                    entry = db.scalar(
                        select(OutboxEntry)
                        .where(OutboxEntry.id == entry_id)
                        .with_for_update(nowait=True)
                    )
                    command = db.get(WorkflowCommand, command_id)
                    assert entry.lease_owner == "child"
                    assert command.response_payload["native_nonce"]
                    assert body["preconditions"]["uid"] == command.request_payload["uid"]
                    assert body["propagationPolicy"] == "Foreground"
                backend.delete(
                    name,
                    namespace,
                    SimpleNamespace(
                        preconditions=SimpleNamespace(uid=body["preconditions"]["uid"]),
                        propagation_policy=body["propagationPolicy"],
                    ),
                )
                accepted.set()
                release.wait(timeout=15)
                self.send(200, {"apiVersion": "v1", "kind": "Status", "status": "Success"})
            except BaseException as exc:
                wire_errors.append(repr(exc))
                accepted.set()
                self.send(500, {})

    server = ThreadingHTTPServer(("127.0.0.1", 0), API)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host = f"http://127.0.0.1:{server.server_port}"
    script = """
import os, sys
sys.path.insert(0, 'apps/api')
from types import SimpleNamespace
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from kubernetes import client
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.legacy_analysis import reconcile_legacy_cleanup
import uuid
cfg = client.Configuration()
cfg.host, cfg.verify_ssl, cfg.retries = sys.argv[2], False, 0
api = client.ApiClient(cfg)
k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
k8s.settings = SimpleNamespace(training_namespace='unused')
k8s.batch, k8s.core = client.BatchV1Api(api), client.CoreV1Api(api)
engine = create_engine(os.environ['DATABASE_URL'])
reconcile_legacy_cleanup(lambda: Session(engine), uuid.UUID(sys.argv[1]), 'child', k8s=k8s)
"""
    try:
        with (tmp_path / "child.log").open("w") as log:
            process = subprocess.Popen(
                [sys.executable, "-c", script, str(entry_id), host],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                assert accepted.wait(timeout=30), "Owned SDK child did not reach native deletion"
                assert wire_errors == []
                process.kill()
                assert process.wait(timeout=10) < 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                release.set()
        assert backend.jobs == {} and backend.pods == [] and len(backend.deletes) == 1
        with Session(engine) as db:
            assert "retired" not in db.get(WorkflowCommand, command_id).response_payload
        claim_cleanup(engine, entry_id, "replay")
        cfg = client.Configuration()
        cfg.host, cfg.verify_ssl, cfg.retries = host, False, 0
        with client.ApiClient(cfg) as api:
            k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
            k8s.settings = SimpleNamespace(training_namespace="unused")
            k8s.batch, k8s.core = client.BatchV1Api(api), client.CoreV1Api(api)
            reconcile_legacy_cleanup(factory(engine), entry_id, "replay", k8s=k8s)
        with Session(engine) as db:
            assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        assert len(backend.deletes) == 1
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        serializer.close()


def test_competing_native_owners_reconcile_one_immutable_uid(legacy_scope):
    from automl_api.models.enums import OutboxStatus
    from automl_api.models.workflows import WorkflowCommand

    engine, _, _, _, backend, _ = legacy_scope
    entry_id, command_id = prepare_cleanup(legacy_scope)
    claim_cleanup(engine, entry_id, "original")
    original_nonce = []

    def takeover():
        backend.on_delete = None
        with Session(engine) as db:
            original_nonce.append(
                db.get(WorkflowCommand, command_id).response_payload["native_nonce"]
            )
        claim_cleanup(engine, entry_id, "replacement")
        deliver(legacy_scope, entry_id, "replacement")

    backend.on_delete = takeover
    deliver(legacy_scope, entry_id, "original")
    assert len(backend.deletes) == 1
    with Session(engine) as db:
        assert db.get(OutboxEntry, entry_id).status == OutboxStatus.DELIVERED
        assert (
            db.get(WorkflowCommand, command_id).response_payload["native_nonce"]
            == original_nonce[0]
        )


def image_backoff(backend):
    fatal_waiting(backend)
    backend.pods[0].status.container_statuses[0].state.waiting.reason = "ImagePullBackOff"


def test_legacy_backoff_grace_and_recovery_preserve_healthy_work(legacy_scope):
    from automl_api.services.training import _IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG

    engine, run_id, _, k8s, backend, _ = legacy_scope
    image_backoff(backend)
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.status == RunStatus.QUEUED and run.tags[_IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG]
        assert not list(
            db.scalars(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id))
        )
    backend.pods[0].status.container_statuses = []
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert (
            run.status == RunStatus.RUNNING and _IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG not in run.tags
        )
    assert backend.deletes == []


def test_legacy_expired_backoff_commits_uid_bound_cleanup_without_inline_delete(legacy_scope):
    from datetime import timedelta

    from automl_api.services.training import (
        _IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG,
        _IMAGE_PULL_BACKOFF_GRACE,
    )

    engine, run_id, _, k8s, backend, _ = legacy_scope
    image_backoff(backend)
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.tags = {
            **run.tags,
            _IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG: (
                datetime.now(UTC) - _IMAGE_PULL_BACKOFF_GRACE - timedelta(seconds=1)
            ).isoformat(),
        }
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.FAILED
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.aggregate_id == run_id))
        assert entry.payload["uid"] == next(iter(backend.jobs.values())).metadata.uid
    assert backend.deletes == []


def test_foreign_pod_owner_cannot_trigger_cleanup_of_healthy_legacy_job(legacy_scope):
    engine, run_id, _, k8s, backend, _ = legacy_scope
    fatal_waiting(backend)
    backend.pods[0].metadata.owner_references[0].uid = uuid.uuid4().hex
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING
        assert not list(
            db.scalars(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id))
        )
    assert backend.deletes == [] and backend.pods


def test_competing_fatal_observers_commit_one_logical_cleanup_intent(legacy_scope):
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.legacy_analysis import OPERATION

    engine, run_id, _, k8s, backend, _ = legacy_scope
    fatal_waiting(backend)
    competing = []
    backend.on_read = lambda: competing.append(observe_once(factory(engine), k8s=k8s))
    assert observe_once(factory(engine), k8s=k8s) == 1
    assert competing == [1] and backend.deletes == []
    with Session(engine) as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowCommand.id).where(
                            WorkflowCommand.resource_id == run_id,
                            WorkflowCommand.operation == OPERATION,
                        )
                    )
                )
            )
            == 1
        )
        assert (
            len(list(db.scalars(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id))))
            == 1
        )


def test_terminal_projection_and_cleanup_intent_roll_back_together(legacy_scope, monkeypatch):
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services import training
    from automl_api.services.legacy_analysis import OPERATION

    engine, run_id, _, k8s, backend, _ = legacy_scope
    fatal_waiting(backend)
    original = training._sync_run_status
    interrupted = []

    def fail_before_commit(db, run, cached, **kwargs):
        original(db, run, cached, **kwargs)
        assert run.status == RunStatus.FAILED
        assert (
            db.scalar(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id)) is not None
        )
        interrupted.append(True)
        raise ConnectionError("Projection and cleanup were prepared; no outer commit")

    monkeypatch.setattr(training, "_sync_run_status", fail_before_commit)
    observe_once(factory(engine), k8s=k8s)
    assert interrupted == [True]
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.QUEUED
        assert not list(
            db.scalars(
                select(WorkflowCommand.id).where(
                    WorkflowCommand.resource_id == run_id, WorkflowCommand.operation == OPERATION
                )
            )
        )
        assert not list(
            db.scalars(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id))
        )
    assert backend.deletes == []
    monkeypatch.setattr(training, "_sync_run_status", original)
    assert observe_once(factory(engine), k8s=k8s) == 1
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.FAILED
        assert (
            db.scalar(select(OutboxEntry.id).where(OutboxEntry.aggregate_id == run_id)) is not None
        )
