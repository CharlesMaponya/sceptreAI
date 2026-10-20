"""Managed verification uses durable intent, one creator and UID-scoped cleanup."""

import copy
import importlib.util
import json
import sys
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from automl_api.core.config import get_settings
from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.enums import AttemptStatus, CommandStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import provider_import_jobs as jobs
from automl_api.services.provider_import import claim_provider_import
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.services.provider_import_workload import SUBMISSION
from automl_api.services.workflow_state import StaleFence
from kubernetes.client import ApiException
from sqlalchemy import select
from test_provider_import_attempts import pending_case as pending_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


class FakeKubernetes:
    def __init__(self, case):
        self.case = case
        self._configured = True
        self.settings = replace(
            get_settings(), attempt_control_base_url="https://control.invalid/internal/attempts"
        )
        self.objects = {kind: {} for kind in ("service_account", "network", "job")}
        self.creates, self.deletes, self.pods = [], [], []
        self.fault = None
        self.core = SimpleNamespace(
            create_namespaced_service_account=lambda **kw: self.create("service_account", **kw),
            read_namespaced_service_account=lambda **kw: self.read("service_account", **kw),
            delete_namespaced_service_account=lambda **kw: self.delete("service_account", **kw),
            list_namespaced_pod=self.list_pods,
        )
        self.networking = SimpleNamespace(
            create_namespaced_network_policy=lambda **kw: self.create("network", **kw),
            read_namespaced_network_policy=lambda **kw: self.read("network", **kw),
            delete_namespaced_network_policy=lambda **kw: self.delete("network", **kw),
            list_namespaced_network_policy=self.list_networks,
        )
        self.batch = SimpleNamespace(
            create_namespaced_job=lambda **kw: self.create("job", **kw),
            read_namespaced_job=lambda **kw: self.read("job", **kw),
            delete_namespaced_job=lambda **kw: self.delete("job", **kw),
        )

    def read(self, kind, *, name, namespace, **_kw):
        assert self.case.engine.pool.checkedout() == 0
        obj = self.objects[kind].get(name)
        if obj is None or obj["metadata"]["namespace"] != namespace:
            raise ApiException(status=404)
        return copy.deepcopy(obj)

    def create(self, kind, *, namespace, body, **_kw):
        assert self.case.engine.pool.checkedout() == 0
        with self.case.factory() as db:
            attempt = verification_attempt(db, db.get(DatasetProviderImport, self.case.import_id))
            assert (
                db.scalar(
                    select(WorkflowEvent.id).where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == f"provider_import.{kind}.requested",
                    )
                )
                is not None
            )
            assert (
                db.scalar(
                    select(WorkflowEvent.id).where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == SUBMISSION,
                    )
                )
                is not None
            )
        self.creates.append(kind)
        if self.fault == (kind, "absent"):
            raise OSError("Unknown create acknowledgement")
        obj = copy.deepcopy(body)
        assert obj["metadata"]["namespace"] == namespace
        obj["metadata"]["uid"] = str(uuid.uuid4())
        self.objects[kind][obj["metadata"]["name"]] = obj
        if self.fault == (kind, "committed"):
            raise OSError("Lost committed create acknowledgement")
        return copy.deepcopy(obj)

    def delete(self, kind, *, name, namespace, body, **_kw):
        assert self.case.engine.pool.checkedout() == 0
        actual = self.read(kind, name=name, namespace=namespace)
        assert body["preconditions"]["uid"] == actual["metadata"]["uid"]
        assert body["propagationPolicy"] == "Foreground"
        self.deletes.append((kind, actual["metadata"]["uid"]))
        del self.objects[kind][name]

    def list_pods(self, **_kw):
        assert self.case.engine.pool.checkedout() == 0
        return SimpleNamespace(items=list(self.pods))

    def list_networks(self, **_kw):
        assert self.case.engine.pool.checkedout() == 0
        return SimpleNamespace(
            items=list(self.objects["network"].values()), metadata=SimpleNamespace(_continue=None)
        )


@pytest.fixture
def job_case(pending_case, monkeypatch):
    c = pending_case
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "https://control.invalid/internal/attempts")
    monkeypatch.setenv("PROVIDER_IMPORT_IMAGE", "example.invalid/training@sha256:" + "a" * 64)
    monkeypatch.setenv(
        "PROVIDER_IMPORT_EGRESS_RULES",
        json.dumps(
            [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "trusted"}
                            },
                            "podSelector": {"matchLabels": {"app": "control"}},
                        }
                    ],
                    "ports": [{"port": 8443, "protocol": "TCP"}],
                }
            ]
        ),
    )
    monkeypatch.delenv("ATTEMPT_CONTROL_CA_SECRET", raising=False)
    monkeypatch.delenv("PROVIDER_IMPORT_MEMORY_MIB", raising=False)
    monkeypatch.delenv("PROVIDER_IMPORT_CPU_CORES", raising=False)
    monkeypatch.delenv("PROVIDER_IMPORT_NODE_SELECTOR", raising=False)
    c.k8s = FakeKubernetes(c)
    return c


def tick(c):
    assert jobs.reconcile_provider_imports_once(c.factory, c.k8s) == 1


def snapshot(c):
    with c.factory() as db:
        row = db.get(DatasetProviderImport, c.import_id)
        attempt = verification_attempt(db, row)
        command = db.get(WorkflowCommand, c.command_id)
        return SimpleNamespace(
            id=attempt.id,
            generation=attempt.generation,
            fence=attempt.fencing_token,
            status=attempt.status,
            cleanup=attempt.cleanup_state,
            predecessor=attempt.predecessor_attempt_id,
            retries=command.retry_count,
            command_status=command.status,
        )


def expire(c):
    with c.factory() as db, db.begin():
        row = db.get(DatasetProviderImport, c.import_id)
        attempt = verification_attempt(db, row)
        command = db.get(WorkflowCommand, c.command_id)
        command.lease_expires_at = attempt.lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )


def allow_retry(c):
    with c.factory() as db, db.begin():
        db.get(WorkflowCommand, c.command_id).available_at = datetime.now(UTC) - timedelta(
            seconds=1
        )


def test_controller_commits_one_desired_workload_before_remote_creation_and_replays(job_case):
    c = job_case
    tick(c)
    first = snapshot(c)
    assert first.generation == first.retries == 1
    assert c.k8s.creates == ["service_account", "network", "job"]
    tick(c)
    assert c.k8s.creates == ["service_account", "network", "job"]
    assert snapshot(c).id == first.id
    job = next(iter(c.k8s.objects["job"].values()))
    pod = job["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["serviceAccountName"] == "provider-verify-" + first.id.hex
    assert job["spec"]["backoffLimit"] == 0
    env = pod["containers"][0]["env"]
    assert pod["containers"][0]["name"] == "provider-import"
    assert all("valueFrom" not in item for item in env)
    assert not {
        "DATABASE_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_STORAGE_KEY",
    } & {item["name"] for item in env}


@pytest.mark.parametrize("kind", ["service_account", "network", "job"])
def test_lost_create_ack_adopts_the_observed_resource_without_another_create(job_case, kind):
    c = job_case
    c.k8s.fault = (kind, "committed")
    tick(c)
    original = snapshot(c)
    c.k8s.fault = None
    tick(c)
    assert snapshot(c).id == original.id
    assert c.k8s.creates == ["service_account", "network", "job"]
    assert all(len(values) == 1 for values in c.k8s.objects.values())


def test_unknown_absent_create_is_not_reissued_and_keeps_deny_policy(job_case):
    c = job_case
    c.k8s.fault = ("job", "absent")
    tick(c)
    c.k8s.fault = None
    tick(c)
    assert c.k8s.creates.count("job") == 1
    expire(c)
    tick(c)
    assert snapshot(c).cleanup == "resource_unknown"
    assert c.k8s.objects["network"] and c.k8s.objects["service_account"]
    assert c.k8s.deletes == [] and snapshot(c).retries == 1


def test_expired_generation_waits_for_uid_cleanup_before_one_bounded_replacement(job_case):
    c = job_case
    tick(c)
    original = snapshot(c)
    expire(c)
    tick(c)
    allow_retry(c)
    with pytest.raises(StaleFence, match="cleanup"):
        claim_provider_import(
            c.factory, c.version_id, policy=c.policy, public_key=c.key.public_key()
        )
    for _ in range(4):
        tick(c)
    replacement = snapshot(c)
    assert replacement.generation == replacement.retries == 2
    assert replacement.predecessor == original.id and replacement.fence != original.fence
    assert [kind for kind, _uid in c.k8s.deletes] == ["job", "service_account", "network"]
    with c.factory() as db:
        previous = db.get(WorkflowAttempt, original.id)
        assert previous.fencing_token == original.fence and previous.cleanup_state == "complete"


def test_replaced_resource_uid_is_never_adopted_or_deleted(job_case):
    c = job_case
    tick(c)
    name, job = next(iter(c.k8s.objects["job"].items()))
    original = copy.deepcopy(job)
    job["metadata"]["uid"] = str(uuid.uuid4())
    tick(c)
    tick(c)
    assert c.k8s.deletes == []
    assert snapshot(c).status == AttemptStatus.SUPERSEDED
    c.k8s.objects["job"][name] = original
    tick(c)
    assert c.k8s.deletes[0] == ("job", original["metadata"]["uid"])


@pytest.mark.parametrize(
    "variable,value",
    [
        ("PROVIDER_IMPORT_IMAGE", "mutable:latest"),
        ("PROVIDER_IMPORT_EGRESS_RULES", "[]"),
        (
            "PROVIDER_IMPORT_EGRESS_RULES",
            '[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0"}}],"ports":[{"port":443}]}]',
        ),
        ("PROVIDER_IMPORT_CPU_CORES", "0"),
    ],
)
def test_invalid_managed_configuration_has_no_claim_or_remote_side_effect(
    job_case, monkeypatch, variable, value
):
    c = job_case
    monkeypatch.setenv(variable, value)
    tick(c)
    with c.factory() as db:
        assert verification_attempt(db, db.get(DatasetProviderImport, c.import_id)) is None
        assert db.get(WorkflowCommand, c.command_id).retry_count == 0
    assert not c.k8s.creates


def test_pending_cli_only_queues_work_and_controller_submits_it(
    job_case, monkeypatch, capsys, tmp_path
):
    from automl_api.db import session
    from automl_api.storage import object_store

    c = job_case
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(c.receipt))
    budget = tmp_path / "budget.json"
    budget.write_text(c.limits.model_dump_json())
    spec = importlib.util.spec_from_file_location(
        "queued_provider_cli", "scripts/import_provider_dataset.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_provider_dataset.py",
            str(receipt),
            "--project-id",
            str(c.project_id),
            "--allocation-id",
            str(c.value.allocation_id),
            "--provider",
            "aws",
            "--actor-id",
            str(c.actor_id),
            "--budget",
            str(budget),
            "--name",
            "Signed safe fixture",
        ],
    )
    monkeypatch.setattr(session, "get_session_factory", lambda: c.factory)
    store = Mock(side_effect=AssertionError("CLI must not construct storage"))
    monkeypatch.setattr(object_store, "get_object_store", store)
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "verified": False,
        "status": "queued",
        "dataset_version_id": str(c.version_id),
    }
    store.assert_not_called()
    assert not c.store.calls and not c.store.writes
    from automl_api import workflow_reconciler as daemon

    monkeypatch.setattr(daemon, "get_session_factory", lambda: c.factory)
    monkeypatch.setattr(daemon, "KubernetesTrainingClient", lambda: c.k8s)
    for name in (
        "run_once",
        "observe_once",
        "plan_champions_once",
        "reconcile_refit_jobs_once",
        "reconcile_evaluation_jobs_once",
        "reconcile_tracking_once",
        "resume_incomplete_profiling_jobs",
        "cleanup_uploads_once",
        "cleanup_retention_once",
    ):
        monkeypatch.setattr(daemon, name, lambda *_a, **_kw: 0)

    def stop_after_one_cycle(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(daemon.time, "sleep", stop_after_one_cycle)
    with pytest.raises(KeyboardInterrupt):
        daemon.main()
    assert c.k8s.creates == ["service_account", "network", "job"]


def test_source_authority_loss_stops_the_current_generation_without_waiting_for_expiry(job_case):
    from automl_api.models.projects import Project

    c = job_case
    tick(c)
    with c.factory() as db, db.begin():
        project = db.get(Project, c.project_id)
        project.settings = {**(project.settings or {}), "deletion_in_progress": True}
    tick(c)
    assert snapshot(c).status == AttemptStatus.SUPERSEDED
    assert snapshot(c).command_status == CommandStatus.PENDING
    tick(c)
    assert c.k8s.deletes[0][0] == "job"


def test_preexisting_resource_without_owned_creation_intent_is_never_adopted_or_deleted(job_case):
    c = job_case
    execution = claim_provider_import(
        c.factory,
        c.version_id,
        policy=c.policy,
        public_key=c.key.public_key(),
        prepare=jobs._prepare(c.k8s.settings),
    )
    with c.factory() as db:
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == execution.attempt_id,
                WorkflowEvent.event_key == SUBMISSION,
            )
        )
        foreign = copy.deepcopy(event.payload["resources"]["service_account"])
    foreign["metadata"]["uid"] = str(uuid.uuid4())
    c.k8s.objects["service_account"][foreign["metadata"]["name"]] = foreign
    tick(c)
    tick(c)
    assert not c.k8s.creates and not c.k8s.deletes
    assert c.k8s.objects["service_account"][foreign["metadata"]["name"]] == foreign


def test_live_managed_cancel_is_fenced_and_cleanup_finishes_without_a_replacement(job_case):
    from automl_api.services.provider_import import cancel_provider_import

    c = job_case
    tick(c)
    with c.factory() as db, db.begin():
        cancel_provider_import(db, c.actor_id, c.version_id)
    assert snapshot(c).status == AttemptStatus.CANCELLED
    assert snapshot(c).command_status == CommandStatus.CANCELLED
    for _ in range(4):
        tick(c)
    assert snapshot(c).cleanup == "complete"
    assert snapshot(c).retries == 1
    assert jobs.reconcile_provider_imports_once(c.factory, c.k8s) == 0
    assert all(not objects for objects in c.k8s.objects.values())


def test_legacy_unknown_descriptor_write_still_blocks_cleanup_and_replacement(job_case):
    from automl_api.services.evaluation_publication import _append

    c = job_case
    tick(c)
    original = snapshot(c)
    with c.factory() as db, db.begin():
        _append(
            db,
            db.get(WorkflowAttempt, original.id),
            "provider_import.descriptor_requested",
            {"object_uri": "s3://owned/legacy-descriptor.json"},
        )
    expire(c)
    for _ in range(5):
        tick(c)
    current = snapshot(c)
    assert current.cleanup == "publication_unknown"
    assert current.id == original.id and current.retries == 1
    assert all(not objects for objects in c.k8s.objects.values())
    allow_retry(c)
    with pytest.raises(StaleFence, match="cleanup"):
        claim_provider_import(
            c.factory, c.version_id, policy=c.policy, public_key=c.key.public_key()
        )
