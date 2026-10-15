import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import jwt
import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services import attempt_identity
from automl_api.services.workflow_state import StaleFence
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401


@pytest.fixture
def identity(publication, monkeypatch):  # noqa: F811
    engine, run_id, parent_id, fence, _ = publication
    namespace = "identity-test"
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.k8s_namespace = namespace
        parent = db.get(WorkflowAttempt, parent_id)
        parent.ray_job_name = "ray-job"
        labels = {
            "automl.platform/project-id": str(parent.project_id),
            "automl.platform/attempt-id": str(parent.id),
            "automl.platform/generation": str(parent.generation),
        }
        workload = parent.workload_identity
    token = jwt.encode({"exp": int(time.time()) + 600}, "unit-test-only-key", algorithm="HS256")
    audience = attempt_identity.attempt_audience(parent_id, fence)
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            uid="pod-uid",
            name="pod-name",
            namespace=namespace,
            deletion_timestamp=None,
            labels=labels,
            owner_references=[],
        ),
        spec=SimpleNamespace(service_account_name=workload),
    )
    user = SimpleNamespace(
        username=f"system:serviceaccount:{namespace}:{workload}",
        extra={
            "authentication.kubernetes.io/pod-name": ["pod-name"],
            "authentication.kubernetes.io/pod-uid": ["pod-uid"],
        },
    )
    review = SimpleNamespace(
        status=SimpleNamespace(authenticated=True, audiences=[audience], user=user)
    )
    auth = MagicMock()
    auth.create_token_review.return_value = review
    monkeypatch.setattr(attempt_identity.client, "AuthenticationV1Api", lambda: auth)
    k8s = SimpleNamespace(core=MagicMock(), custom=MagicMock(), _configured=True)
    k8s.core.read_namespaced_pod.return_value = pod
    return publication, token, k8s, review, pod, auth


def admit(identity):
    scope, token, k8s, *_ = identity
    with Session(scope[0]) as db, db.begin():
        return attempt_identity.admit_attempt(db, scope[2], token, k8s)


def test_pod_bound_admission_persists_only_digest_and_replays(identity):
    scope, token, _, _, _, auth = identity
    first = admit(identity)
    assert admit(identity) == first
    sent = auth.create_token_review.call_args.args[0]
    assert sent.spec.token == token and sent.spec.audiences == [first["audience"]]
    with Session(scope[0]) as db:
        events = list(db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == scope[2])))
        assert len(events) == 1 and events[0].event_type == "attempt.identity"
        assert token not in json.dumps(events[0].payload)


@pytest.mark.parametrize(
    "invalid",
    [
        "authenticated",
        "audience",
        "unbound",
        "pod_uid",
        "username",
        "project",
        "attempt",
        "generation",
        "deleted",
        "namespace",
        "service_account",
        "expired",
        "long_expiry",
        "bad_expiry",
        "parent",
        "lease",
        "run",
    ],
)
def test_invalid_workload_identity_is_rejected(identity, invalid):
    scope, _, _, review, pod, _ = identity
    if invalid == "authenticated":
        review.status.authenticated = False
    elif invalid == "audience":
        review.status.audiences = ["another-attempt"]
    elif invalid == "unbound":
        review.status.user.extra = {}
    elif invalid == "pod_uid":
        pod.metadata.uid = "recreated-pod"
    elif invalid == "username":
        review.status.user.username = "another-principal"
    elif invalid in {"project", "attempt", "generation"}:
        key = {"project": "project-id", "attempt": "attempt-id", "generation": "generation"}[
            invalid
        ]
        pod.metadata.labels[f"automl.platform/{key}"] = "another"
    elif invalid == "deleted":
        pod.metadata.deletion_timestamp = "deleting"
    elif invalid == "namespace":
        pod.metadata.namespace = "another-namespace"
    elif invalid == "service_account":
        pod.spec.service_account_name = "untrusted"
    elif invalid in {"expired", "long_expiry", "bad_expiry"}:
        expiry = {
            "expired": int(time.time()) - 1,
            "long_expiry": int(time.time()) + 3600,
            "bad_expiry": "later",
        }[invalid]
        identity = (
            scope,
            jwt.encode({"exp": expiry}, "unit-test-only-key", algorithm="HS256"),
            *identity[2:],
        )
    else:
        from datetime import UTC, datetime, timedelta

        with Session(scope[0]) as db, db.begin():
            if invalid == "parent":
                db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
            elif invalid == "lease":
                db.get(WorkflowAttempt, scope[2]).lease_expires_at = datetime.now(UTC) - timedelta(
                    seconds=1
                )
            else:
                db.get(ModelRun, scope[1]).status = RunStatus.CANCELLED
    with pytest.raises(StaleFence):
        admit(identity)
    with Session(scope[0]) as db:
        assert not list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == scope[2]))
        )


def head(identity):
    _, _, k8s, review, pod, _ = identity
    pod.spec.service_account_name = "ray-cluster"
    review.status.user.username = "system:serviceaccount:identity-test:ray-cluster"
    pod.metadata.labels["ray.io/node-type"] = "head"
    pod.metadata.owner_references = [
        SimpleNamespace(kind="RayCluster", controller=True, name="ray-cluster", uid="cluster-uid")
    ]
    cluster = {
        "metadata": {
            "name": "ray-cluster",
            "uid": "cluster-uid",
            "ownerReferences": [
                {"kind": "RayJob", "controller": True, "name": "ray-job", "uid": "job-uid"}
            ],
        }
    }
    job = {"metadata": {"uid": "job-uid", "labels": dict(pod.metadata.labels)}}
    k8s.custom.get_namespaced_custom_object.side_effect = [cluster, job]
    return cluster, job


def test_ray_head_requires_exact_controller_chain(identity):
    head(identity)
    assert admit(identity)["pod_uid"] == "pod-uid"


@pytest.mark.parametrize(
    "invalid", ["cluster_uid", "job_uid", "owner_name", "job_project", "not_head"]
)
def test_ray_head_cannot_cross_attempt_controller_chain(identity, invalid):
    cluster, job = head(identity)
    if invalid == "cluster_uid":
        cluster["metadata"]["uid"] = "recreated-cluster"
    elif invalid == "job_uid":
        job["metadata"]["uid"] = "recreated-job"
    elif invalid == "owner_name":
        cluster["metadata"]["ownerReferences"][0]["name"] = "another-job"
    elif invalid == "job_project":
        job["metadata"]["labels"]["automl.platform/project-id"] = "another-project"
    else:
        identity[4].metadata.labels["ray.io/node-type"] = "worker"
    with pytest.raises(StaleFence):
        admit(identity)


def test_admission_route_rejects_user_tokens_and_does_not_store_token(identity, monkeypatch):
    scope, token, k8s, *_ = identity
    app = FastAPI()
    app.include_router(attempt_control.router)

    def db():
        with Session(scope[0]) as session:
            yield session

    app.dependency_overrides[get_db] = db
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: k8s)
    with TestClient(app) as client:
        path = f"/internal/attempts/{scope[2]}/admission"
        assert client.post(path).status_code == 401
        assert (
            client.post(path, headers={"Authorization": "Basic user-password"}).status_code == 401
        )
        response = client.post(path, headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200, response.text
        assert response.headers["cache-control"] == "no-store"
        assert token not in response.text


def test_helm_grants_token_review_only_to_api_service_account():
    import subprocess

    import yaml

    result = subprocess.run(
        [
            "helm",
            "template",
            "identity-test",
            "infra/helm/sceptre",
            "--namespace",
            "identity-check",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    documents = [d for d in yaml.safe_load_all(result.stdout) if d]
    roles = [
        d
        for d in documents
        if d.get("kind") == "ClusterRole"
        and any("tokenreviews" in r.get("resources", []) for r in d.get("rules", []))
    ]
    assert len(roles) == 1
    assert roles[0]["rules"] == [
        {"apiGroups": ["authentication.k8s.io"], "resources": ["tokenreviews"], "verbs": ["create"]}
    ]
    bindings = [
        d
        for d in documents
        if d.get("kind") == "ClusterRoleBinding"
        and d.get("roleRef", {}).get("name") == roles[0]["metadata"]["name"]
    ]
    api_binding = next(
        d
        for d in documents
        if d.get("kind") == "RoleBinding" and d["metadata"]["name"].endswith("-workloads")
    )
    assert len(bindings) == 1 and bindings[0]["subjects"] == api_binding["subjects"]
    assert bindings[0]["subjects"][0]["namespace"] == "identity-check"


@pytest.mark.parametrize("tamper", ["payload", "type"])
def test_changed_admission_evidence_is_rejected(identity, tamper):
    admit(identity)
    scope = identity[0]
    with Session(scope[0]) as db, db.begin():
        event = db.scalar(select(WorkflowEvent).where(WorkflowEvent.attempt_id == scope[2]))
        if tamper == "payload":
            event.payload = {**event.payload, "pod_uid": "changed"}
        else:
            event.event_type = "unrelated.event"
    with pytest.raises(StaleFence):
        admit(identity)


def test_admission_route_reports_unavailable_cluster_without_admitting(identity, monkeypatch):
    scope, token, k8s, *_ = identity
    k8s._configured = False
    app = FastAPI()
    app.include_router(attempt_control.router)

    def db():
        with Session(scope[0]) as session:
            yield session

    app.dependency_overrides[get_db] = db
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: k8s)
    with TestClient(app) as http:
        response = http.post(
            f"/internal/attempts/{scope[2]}/admission", headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 503
    with Session(scope[0]) as db:
        assert not list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == scope[2]))
        )
