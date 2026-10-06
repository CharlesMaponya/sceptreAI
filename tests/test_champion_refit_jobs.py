from __future__ import annotations

import base64
import json
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from functools import partial
from types import SimpleNamespace

import pytest
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services import refit_access, refit_jobs
from automl_api.services.champion_planning import plan_scope_refit
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session

pytest_plugins = ["test_champion_planning"]


class FakeKubernetes:
    def __init__(self):
        self.settings = SimpleNamespace(
            training_namespace="qa-refit",
            training_image="registry/image@sha256:" + "a" * 64,
            workload_image_pull_secrets=(),
        )
        self.objects = {kind: {} for kind in ("job", "secret", "network_policy")}
        self.states = {}
        self.before_create = lambda: None
        self.lose_job_reply = False
        for attribute, kind in (
            ("batch", "job"),
            ("core", "secret"),
            ("networking", "network_policy"),
        ):
            api = SimpleNamespace()
            for operation in ("create", "read", "delete", "list"):
                setattr(
                    api, f"{operation}_namespaced_{kind}", partial(getattr(self, operation), kind)
                )
            setattr(self, attribute, api)

    def ensure_service_account(self, name):
        assert name == "sceptre-champion-refit"

    def create(self, kind, namespace, body):
        assert namespace == "qa-refit"
        self.before_create()
        name = body["metadata"]["name"]
        if name in self.objects[kind]:
            raise ApiException(status=409)
        self.objects[kind][name] = deepcopy(body)
        if kind == "job" and self.lose_job_reply:
            self.lose_job_reply = False
            raise ApiException(status=500, reason="Synthetic lost creation acknowledgement")

    def read(self, kind, name, namespace):
        if name not in self.objects[kind]:
            raise ApiException(status=404)
        body = self.objects[kind][name]
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                labels=body["metadata"].get("labels"),
                annotations=body["metadata"].get("annotations"),
            ),
            data={
                key: base64.b64encode(value.encode()).decode()
                for key, value in body.get("stringData", {}).items()
            },
        )

    def delete(self, kind, name, namespace, **_kwargs):
        if name not in self.objects[kind]:
            raise ApiException(status=404)
        del self.objects[kind][name]

    def list(self, kind, namespace, **_kwargs):
        return SimpleNamespace(
            items=[self.read(kind, name, namespace) for name in self.objects[kind]],
            metadata=SimpleNamespace(_continue=None),
        )

    def job_state(self, name):
        return self.states.get(name, "queued" if name in self.objects["job"] else "missing")


@pytest.fixture
def jobs_case(planning_case, monkeypatch):
    db, store, _, scope, _ = planning_case
    attempt = plan_scope_refit(db, scope.id, store)
    db.flush()
    k8s = FakeKubernetes()
    monkeypatch.setenv("REFIT_CONTROL_BASE_URL", "https://api.example/api/v1/internal/refits")
    monkeypatch.setenv(
        "REFIT_EGRESS_RULES",
        json.dumps(
            [
                {
                    "to": [{"podSelector": {"matchLabels": {"app": "refit-api"}}}],
                    "ports": [{"port": 8443}],
                }
            ]
        ),
    )
    monkeypatch.delenv("REFIT_IMAGE", raising=False)
    monkeypatch.setattr(
        refit_access,
        "get_settings",
        lambda: SimpleNamespace(
            jwt_secret_key="refit-job-tests-key-01234567890123456789",
        ),
    )
    monkeypatch.setattr(refit_jobs, "_next_sweep", float("inf"))

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    def before_create():
        assert not db.get_bind().in_nested_transaction()
        with factory() as session:
            assert session.get(WorkflowAttempt, attempt.id).status == AttemptStatus.SUBMITTED
            assert (
                session.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == "refit_submission",
                    )
                )
                is not None
            )

    k8s.before_create = before_create
    return db, store, scope, attempt, k8s, factory


def test_committed_submission_is_isolated_and_lost_reply_replays_same_attempt(jobs_case):
    db, _, _, attempt, k8s, factory = jobs_case
    k8s.lose_job_reply = True
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    name = f"refit-{attempt.id.hex}"
    assert name in k8s.objects["job"]
    # A transient missing observation exercises Secret/Job/NetworkPolicy 409 replay.
    k8s.states[name] = "missing"
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.SUBMITTED and attempt.generation == 1
    assert (
        len(
            list(
                db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == attempt.id,
                        WorkflowEvent.event_key == "refit_submission",
                    )
                )
            )
        )
        == 1
    )
    job = k8s.objects["job"][name]
    pod = job["spec"]["template"]["spec"]
    assert job["spec"]["backoffLimit"] == 0
    assert 0 < job["spec"]["activeDeadlineSeconds"] <= 3600
    assert pod["automountServiceAccountToken"] is False and pod["restartPolicy"] == "Never"
    container = pod["containers"][0]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert "envFrom" not in container
    names = {item["name"] for item in container["env"]}
    assert not names & {"DATABASE_URL", "AWS_SECRET_ACCESS_KEY", "OBJECT_STORE_SECRET_KEY"}
    assert (
        next(item for item in container["env"] if item["name"] == "REFIT_CONTROL_TOKEN")[
            "valueFrom"
        ]["secretKeyRef"]["name"]
        == name
    )
    assert k8s.objects["network_policy"][name]["spec"]["ingress"] == []


@pytest.mark.parametrize("failure", ["missing", "failed", "succeeded", "terminating", "lease"])
def test_lost_refit_fails_before_planner_creates_one_replacement(jobs_case, failure):
    db, store, scope, attempt, k8s, factory = jobs_case
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    attempt.status = AttemptStatus.RUNNING
    attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
    if failure == "lease":
        attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        k8s.states[f"refit-{attempt.id.hex}"] = failure
    db.flush()
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    replacement = plan_scope_refit(db, scope.id, store)
    assert replacement.generation == 2 and replacement.predecessor_attempt_id == attempt.id


def test_foreground_cleanup_keeps_credentials_and_policy_until_job_disappears(jobs_case):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    db.flush()
    name = f"refit-{attempt.id.hex}"
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    assert name not in k8s.objects["job"]
    assert name in k8s.objects["secret"] and name in k8s.objects["network_policy"]
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    assert all(not resources for resources in k8s.objects.values())
    db.refresh(attempt)
    assert attempt.cleanup_state == "complete"


def test_late_resources_after_cleanup_are_swept(jobs_case):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    resources = deepcopy(k8s.objects)
    db.refresh(attempt)
    attempt.status = AttemptStatus.FAILED
    db.flush()
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    k8s.objects = resources  # A lost external create completes after cleanup acknowledged.
    refit_jobs.sweep_refit_resources(factory, k8s)
    refit_jobs.sweep_refit_resources(factory, k8s)
    assert all(not resources for resources in k8s.objects.values())


def test_concurrent_worker_start_invalidates_stale_missing_observation(jobs_case):
    db, _, _, attempt, k8s, factory = jobs_case
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)

    def started_during_observation(_name):
        with factory() as session, session.begin():
            current = session.get(WorkflowAttempt, attempt.id)
            current.status = AttemptStatus.RUNNING
            current.lease_expires_at = datetime.now(UTC) + timedelta(seconds=90)
        return "missing"

    k8s.job_state = started_during_observation
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.RUNNING


def test_expired_pending_scope_never_creates_external_resources(jobs_case):
    db, _, scope, attempt, k8s, factory = jobs_case
    scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    refit_jobs.reconcile_refit_jobs_once(factory, k8s)
    db.refresh(attempt)
    db.refresh(scope)
    assert attempt.status == AttemptStatus.CANCELLED and scope.status == ScopeStatus.FAILED
    assert all(not resources for resources in k8s.objects.values())


def test_kubernetes_observation_recognizes_deletion_before_terminal_status():
    from automl_api.services.kubernetes_training import KubernetesTrainingClient

    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = SimpleNamespace(training_namespace="qa-refit")
    client.batch = SimpleNamespace(
        read_namespaced_job_status=lambda **_: SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp=datetime.now(UTC)),
            status=SimpleNamespace(succeeded=0, failed=0, active=1),
        )
    )
    assert client.job_state("terminating-job") == "terminating"
