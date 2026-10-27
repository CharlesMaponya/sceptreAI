"""Serving credential ownership and immutable per-Pod key binding."""

import copy
import json
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import jwt
import pytest
from automl_api.models.enums import RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import inference_identity as identity
from automl_api.services.deployment_submission import lifecycle_custody
from automl_api.services.inference_tls import (
    PROTOCOL,
    PeerIdentity,
    audience,
    credential_mounts,
    credential_volumes,
    make_csr,
    new_private_key,
    private_key_pem,
    public_key_digest,
    validate_server_certificate,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from kubernetes import client
from sqlalchemy import select
from test_deployment_control_workflows import controls as controls
from test_deployment_lifecycle_identity import (
    bound_run as bound_run,
)
from test_deployment_lifecycle_identity import (
    capacity_client as capacity_client,
)
from test_deployment_lifecycle_identity import (
    committed_tenant as committed_tenant,
)
from test_deployment_lifecycle_identity import (
    context_api as context_api,
)
from test_deployment_lifecycle_identity import (
    deployment_object as deployment_object,
)
from test_deployment_lifecycle_identity import (
    identity_client,
)
from test_deployment_lifecycle_identity import (
    model_api as model_api,
)
from test_deployment_lifecycle_identity import (
    native_lifecycle as native_lifecycle,
)
from test_deployment_lifecycle_identity import (
    phase1_engine as phase1_engine,
)
from test_deployment_lifecycle_identity import (
    qualified_model as qualified_model,
)
from test_deployment_lifecycle_identity import (
    selected_scope as selected_scope,
)
from test_deployment_lifecycle_identity import (
    serving_model as serving_model,
)
from test_upload_completion_workflow import completion_case as completion_case
from test_upload_verification import verification_case as verification_case


@pytest.mark.parametrize("fault", ["none", "missing", "oversized", "mismatched"])
def test_authority_files_require_the_original_bounded_key_pair(tmp_path, fault):
    from automl_api.core.config import Settings
    from automl_api.services.inference_tls import Authority
    from test_inference_tls import authority_material

    cert, key = authority_material()
    certificate, private = tmp_path / "ca.crt", tmp_path / "ca.key"
    certificate.write_bytes(cert)
    private.write_bytes(key)
    settings = Settings(
        inference_identity_ca_certificate_file=certificate,
        inference_identity_ca_private_key_file=private,
    )
    if fault == "missing":
        private.unlink()
    elif fault == "oversized":
        certificate.write_bytes(b"x" * 16385)
    elif fault == "mismatched":
        private.write_bytes(authority_material()[1])
    if fault == "none":
        assert identity.authority(settings).digest == Authority.from_pem(cert, key).digest
    else:
        with pytest.raises(ValueError if fault == "mismatched" else StaleFence):
            identity.authority(settings)


def native_pod(k8s, resources, custody, project_id, run_id, ca_digest, record=lambda _: None):
    deployment = resources["deployment"]
    metadata = deployment["metadata"]
    pod_uid = str(uuid.uuid4())
    replica_uid = str(uuid.uuid4())
    pod = dict(
        metadata=dict(
            name="owned-inference-pod",
            uid=pod_uid,
            namespace=custody["namespace"],
            labels=copy.deepcopy(deployment["spec"]["template"]["metadata"]["labels"]),
            ownerReferences=[
                dict(kind="ReplicaSet", name="owned-inference-rs", uid=replica_uid, controller=True)
            ],
        ),
        spec=copy.deepcopy(deployment["spec"]["template"]["spec"]),
        status=dict(phase="Running"),
    )
    replica = dict(
        metadata=dict(
            name="owned-inference-rs",
            uid=replica_uid,
            namespace=custody["namespace"],
            ownerReferences=[
                dict(kind="Deployment", name=metadata["name"], uid=metadata["uid"], controller=True)
            ],
        ),
        spec=dict(template=copy.deepcopy(deployment["spec"]["template"])),
    )
    request = dict(
        run_id=str(run_id),
        project_id=str(project_id),
        actor_id=str(uuid.uuid4()),
        custody=custody,
        control=dict(generation=0, command_id=None, replicas=deployment["spec"]["replicas"]),
        model_digest="a" * 64,
        model_byte_size=4,
        model_custody_digest="b" * 64,
        ca_digest=ca_digest,
        service_account=pod["spec"]["serviceAccountName"],
        audience=audience(run_id, ca_digest),
    )
    scope = identity.IdentityScope(request, canonical_request_hash(request))
    state = SimpleNamespace(pod=pod, replica=replica, reviewed=[])

    def read_pod(**kwargs):
        record("pod")
        assert (
            kwargs["name"] == pod["metadata"]["name"]
            and kwargs["namespace"] == custody["namespace"]
        )
        return copy.deepcopy(state.pod)

    def read_replica(**kwargs):
        record("replica")
        assert (
            kwargs["name"] == replica["metadata"]["name"]
            and kwargs["namespace"] == custody["namespace"]
        )
        return copy.deepcopy(state.replica)

    k8s.core.read_namespaced_pod = read_pod
    k8s.apps.read_namespaced_replica_set = read_replica

    def review(body, **kwargs):
        record("review")
        state.reviewed.append(body.spec.token)
        assert body.spec.audiences == [request["audience"]] and kwargs["_request_timeout"] == 5
        return client.V1TokenReview(
            spec=body.spec,
            status=client.V1TokenReviewStatus(
                authenticated=True,
                audiences=[request["audience"]],
                user=client.V1UserInfo(
                    username=f"system:serviceaccount:{custody['namespace']}:{request['service_account']}",
                    extra={
                        "authentication.kubernetes.io/pod-name": [pod["metadata"]["name"]],
                        "authentication.kubernetes.io/pod-uid": [pod_uid],
                    },
                ),
            ),
        )

    k8s.authentication = SimpleNamespace(create_token_review=review)
    state.scope = scope
    state.token = jwt.encode(
        dict(exp=int((datetime.now(UTC) + timedelta(seconds=600)).timestamp())),
        "owned-token-review-fixture-at-least-32-bytes",
        algorithm="HS256",
    )
    state.peer = PeerIdentity(
        str(project_id), str(run_id), custody["namespace_uid"], metadata["uid"], pod_uid
    )
    return state


@pytest.mark.parametrize(
    "fault",
    [
        "pod-uid",
        "pod-owner",
        "replica-owner",
        "replica-template",
        "pod-init",
        "pod-lifecycle",
        "pod-volume",
        "terminating",
        "completed",
        "namespace",
        "marker",
    ],
)
def test_native_identity_refuses_rebound_or_extended_execution(fault):
    c = identity_client()
    state = native_pod(c.k8s, c.stored, c.target, c.project_id, c.run_id, "c" * 64)
    if fault == "pod-uid":
        state.pod["metadata"]["uid"] = "replacement"
    elif fault == "pod-owner":
        state.pod["metadata"]["ownerReferences"][0]["uid"] = "replacement"
    elif fault == "replica-owner":
        state.replica["metadata"]["ownerReferences"][0]["uid"] = "replacement"
    elif fault == "replica-template":
        state.replica["spec"]["template"]["spec"]["initContainers"] = [
            dict(name="foreign", image="foreign")
        ]
    elif fault == "pod-init":
        state.pod["spec"]["initContainers"] = [dict(name="foreign", image="foreign")]
    elif fault == "pod-lifecycle":
        state.pod["spec"]["containers"][0]["lifecycle"] = dict(
            postStart=dict(exec=dict(command=["foreign"]))
        )
    elif fault == "pod-volume":
        state.pod["spec"]["volumes"] = [
            dict(name="foreign", secret=dict(secretName="platform-signing-key"))
        ]
    elif fault == "terminating":
        state.pod["metadata"]["deletionTimestamp"] = "2026-10-25T00:00:00Z"
    elif fault == "completed":
        state.pod["status"]["phase"] = "Succeeded"
    elif fault == "namespace":
        c.namespace_uid = "replacement"
    elif fault == "marker":
        c.stored["deployment"]["metadata"]["annotations"]["automl.platform/control-generation"] = (
            "1"
        )
    with pytest.raises(ValueError if fault == "namespace" else StaleFence):
        identity.review_identity(c.k8s, state.scope, state.token)
    assert not c.mutations


@pytest.mark.parametrize(
    "fault", ["none", "denied", "audience", "service-account", "unbound", "expired", "long-lived"]
)
def test_token_review_must_authenticate_exact_bounded_pod_scope(fault):
    c = identity_client()
    state = native_pod(c.k8s, c.stored, c.target, c.project_id, c.run_id, "c" * 64)
    original = c.k8s.authentication.create_token_review

    def review(body, **kwargs):
        result = original(body, **kwargs)
        if fault == "denied":
            result.status.authenticated = False
        elif fault == "audience":
            result.status.audiences = ["other-audience"]
        elif fault == "service-account":
            result.status.user.username = "system:serviceaccount:other:default"
        elif fault == "unbound":
            result.status.user.extra = {}
        return result

    c.k8s.authentication.create_token_review = review
    if fault in {"expired", "long-lived"}:
        state.token = jwt.encode(
            dict(
                exp=int(
                    (
                        datetime.now(UTC) + timedelta(seconds=-1 if fault == "expired" else 901)
                    ).timestamp()
                )
            ),
            "owned-token-review-fixture-at-least-32-bytes",
            algorithm="HS256",
        )
    if fault == "none":
        peer, pod, expires = identity.review_identity(c.k8s, state.scope, state.token)
        assert (
            peer == state.peer
            and pod["metadata"]["uid"] == peer.pod_uid
            and expires > datetime.now(UTC)
        )
    else:
        with pytest.raises(StaleFence):
            identity.review_identity(c.k8s, state.scope, state.token)
    assert state.reviewed == [state.token]


@pytest.fixture
def binding_scope(controls):
    from automl_api.services.model_custody import deployment_model_custody

    c = controls
    authority = c.inference_authority
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.deployment_id)
        custody = lifecycle_custody(db, run)
        model = deployment_model_custody(db, run)
        model_digest = run.tags["model_custody_digest"]
        assert run.tags.get("inference_identity_protocol") == PROTOCOL
    state = native_pod(
        c.kubernetes,
        c.native_resources,
        custody,
        c.project_id,
        c.deployment_id,
        authority.digest,
        lambda label: c.native_reads.append((label, c.engine.pool.checkedout())),
    )
    state.scope.request["actor_id"] = str(c.user_id)
    state.scope.request.update(
        model_digest=model["completion"]["sha256"],
        model_byte_size=model["completion"]["byte_size"],
        model_custody_digest=model_digest,
    )

    c.identity = state
    return c


def test_sql_key_binding_is_immutable_and_renewal_rechecks_native_identity(binding_scope):
    c = binding_scope
    authority = c.inference_authority
    key = new_private_key()
    csr = make_csr(key)
    for _ in range(2):
        response = identity.issue_identity(
            c.factory, c.deployment_id, c.identity.token, csr, c.kubernetes
        )
        assert response["identity"] == asdict(c.identity.peer)
        validate_server_certificate(
            authority.pem, response["certificate"].encode(), c.identity.peer, key
        )
        assert c.identity.token not in json.dumps(response) and private_key_pem(
            key
        ).decode() not in json.dumps(response)
    with pytest.raises(ValueError):
        identity.issue_identity(
            c.factory, c.deployment_id, c.identity.token, make_csr(new_private_key()), c.kubernetes
        )
    with c.factory() as db:
        rows = list(
            db.scalars(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == identity.BINDING_OPERATION,
                )
            )
        )
        assert len(rows) == 1 and rows[0].request_payload["public_key_digest"] == public_key_digest(
            key.public_key()
        )
        assert private_key_pem(key).decode() not in json.dumps(rows[0].request_payload)
    assert all(held == 0 for _, held in c.native_reads)


def test_revocation_during_native_review_cannot_bind_or_sign(binding_scope):
    c = binding_scope
    original = c.kubernetes.authentication.create_token_review

    def revoked(body, **kwargs):
        response = original(body, **kwargs)
        with c.factory() as db, db.begin():
            db.get(ModelRun, c.deployment_id).status = RunStatus.CANCELLED
        return response

    c.kubernetes.authentication.create_token_review = revoked
    with pytest.raises(StaleFence):
        identity.issue_identity(
            c.factory, c.deployment_id, c.identity.token, make_csr(new_private_key()), c.kubernetes
        )
    with c.factory() as db:
        assert not list(
            db.scalars(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == identity.BINDING_OPERATION,
                )
            )
        )


def test_native_change_during_owner_lookup_cannot_issue_identity():
    c = identity_client()
    state = native_pod(c.k8s, c.stored, c.target, c.project_id, c.run_id, "c" * 64)
    original = c.k8s.apps.read_namespaced_replica_set

    def changed(**kwargs):
        result = original(**kwargs)
        c.stored["deployment"]["metadata"]["resourceVersion"] = "2"
        return result

    c.k8s.apps.read_namespaced_replica_set = changed
    with pytest.raises(StaleFence, match="changed during pod verification"):
        identity.review_identity(c.k8s, state.scope, state.token)


@pytest.mark.parametrize("fault", ["defaults", "sidecar", "init", "host-network", "lifecycle"])
def test_native_receipt_does_not_authorize_injected_execution(fault):
    c = identity_client()
    state = native_pod(c.k8s, c.stored, c.target, c.project_id, c.run_id, "c" * 64)
    spec = c.stored["deployment"]["spec"]["template"]["spec"]
    if fault == "defaults":
        spec.update(
            dnsPolicy="ClusterFirst",
            restartPolicy="Always",
            schedulerName="default-scheduler",
            terminationGracePeriodSeconds=30,
        )
        container = spec["containers"][0]
        container.update(
            terminationMessagePath="/dev/termination-log", terminationMessagePolicy="File"
        )
        for values in container["resources"].values():
            values.update(cpu="1000m", memory="1024Mi")
        for port in container["ports"]:
            port["protocol"] = "TCP"
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            container[probe].update(timeoutSeconds=1, successThreshold=1)
            container[probe]["httpGet"]["scheme"] = "HTTP"
        spec["volumes"][0]["projected"]["defaultMode"] = 420
    elif fault == "sidecar":
        spec["containers"].append(dict(name="foreign", image="foreign"))
    elif fault == "init":
        spec["initContainers"] = [dict(name="foreign", image="foreign")]
    elif fault == "host-network":
        spec["hostNetwork"] = True
    else:
        spec["containers"][0]["lifecycle"] = dict(postStart=dict(exec=dict(command=["foreign"])))
    state.pod["spec"] = copy.deepcopy(spec)
    state.replica["spec"]["template"]["spec"] = copy.deepcopy(spec)
    c.target["resources"]["deployment"]["spec_digest"] = canonical_request_hash(
        c.stored["deployment"]["spec"]
    )
    if fault == "defaults":
        assert identity.review_identity(c.k8s, state.scope, state.token)[0] == state.peer
    elif fault == "sidecar":
        with pytest.raises(ValueError, match="ownership or spec"):
            identity.review_identity(c.k8s, state.scope, state.token)
    else:
        with pytest.raises(StaleFence, match="unrequested execution"):
            identity.review_identity(c.k8s, state.scope, state.token)


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "protocol",
        "inactive-actor",
        "cancelled",
        "duplicate-env",
        "legacy-token",
        "ca",
        "project",
        "identity-url",
        "model-url",
        "model-digest",
        "model-uri",
        "automount",
        "command",
        "volumes",
        "mounts",
    ],
)
def test_serving_declaration_requires_the_exact_credential_protocol(monkeypatch, fault):
    from automl_api.services import model_custody, operations

    c = identity_client()
    run = SimpleNamespace(
        id=c.run_id,
        project_id=c.project_id,
        created_by_id=uuid.uuid4(),
        run_kind=RunKind.DEPLOYMENT,
        status=RunStatus.RUNNING,
        tags=dict(inference_identity_protocol=PROTOCOL, model_custody_digest="d" * 64),
    )
    origin = SimpleNamespace(is_active=True)
    base = "https://control.invalid/api/v1/internal/deployments"
    pod = c.target["manifests"]["deployment"]["spec"]["template"]["spec"]
    container = pod["containers"][0]
    values = dict(
        INFERENCE_IDENTITY_PROTOCOL=PROTOCOL,
        INFERENCE_CA_SHA256="c" * 64,
        INFERENCE_DEPLOYMENT_ID=str(run.id),
        INFERENCE_PROJECT_ID=str(run.project_id),
        INFERENCE_IDENTITY_URL=f"{base}/{run.id}/identity",
        MODEL_DOWNLOAD_URL=f"{base}/{run.id}/model",
        MODEL_SHA256="a" * 64,
        MODEL_URI="s3://owned/model",
    )
    container["env"] = [dict(name=k, value=v) for k, v in values.items()]
    container["command"] = ["python", "-m", "automl_api.inference.runtime"]
    pod["volumes"] = credential_volumes(run.id, "c" * 64)
    container["volumeMounts"] = credential_mounts()
    if fault == "protocol":
        run.tags["inference_identity_protocol"] = "legacy"
    elif fault == "inactive-actor":
        origin.is_active = False
    elif fault == "cancelled":
        run.status = RunStatus.CANCELLED
    elif fault == "duplicate-env":
        container["env"].append(dict(name="MODEL_URI", value="foreign"))
    elif fault == "legacy-token":
        container["env"].append(dict(name="INFERENCE_GATEWAY_TOKEN", value="foreign"))
    elif fault in {"ca", "project", "identity-url", "model-url", "model-digest", "model-uri"}:
        name = {
            "ca": "INFERENCE_CA_SHA256",
            "project": "INFERENCE_PROJECT_ID",
            "identity-url": "INFERENCE_IDENTITY_URL",
            "model-url": "MODEL_DOWNLOAD_URL",
            "model-digest": "MODEL_SHA256",
            "model-uri": "MODEL_URI",
        }[fault]
        next(item for item in container["env"] if item["name"] == name)["value"] = "foreign"
    elif fault == "automount":
        pod["automountServiceAccountToken"] = True
    elif fault == "command":
        container["command"] = ["foreign"]
    elif fault == "volumes":
        pod["volumes"][0]["projected"]["sources"][0]["serviceAccountToken"]["audience"] = "foreign"
    elif fault == "mounts":
        container["volumeMounts"][0]["readOnly"] = False

    def get(model, key, **kwargs):
        assert kwargs.get("populate_existing") is True
        assert kwargs.get("with_for_update")
        return {Project: SimpleNamespace(), ModelRun: run, User: origin}[model]

    db = SimpleNamespace(scalar=lambda _: c.project_id, get=get)
    monkeypatch.setattr(identity, "control_base_url", lambda: base)
    monkeypatch.setattr(
        operations,
        "_deployment_observation_request",
        lambda *_: dict(custody=c.target, control=dict(generation=0, command_id=None, replicas=2)),
    )
    monkeypatch.setattr(
        model_custody,
        "deployment_model_custody",
        lambda *_: dict(completion=dict(sha256="a" * 64, uri="s3://owned/model", byte_size=4)),
    )
    if fault == "none":
        scope = identity.select_scope(db, run.id, "c" * 64)
        assert scope.digest == canonical_request_hash(scope.request)
        assert scope.request["audience"] == audience(run.id, "c" * 64)
    else:
        with pytest.raises(StaleFence):
            identity.select_scope(db, run.id, "c" * 64)


@pytest.fixture
def identity_http(monkeypatch):
    from automl_api.api.routes import serving_artifacts
    from automl_api.db.session import get_session_factory
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()
    app.include_router(serving_artifacts.router)
    sentinel = object()
    app.dependency_overrides[get_session_factory] = lambda: sentinel
    k8s = SimpleNamespace(_configured=True)
    monkeypatch.setattr(serving_artifacts, "KubernetesTrainingClient", lambda: k8s)
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, k8s=k8s, factory=sentinel)


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "scheme",
        "empty",
        "large-token",
        "large-csr",
        "invalid-csr",
        "unconfigured",
        "stale",
        "provider",
        "database",
    ],
)
def test_identity_http_boundary_is_bounded_and_fails_closed(identity_http, monkeypatch, fault):
    from kubernetes.client import ApiException
    from sqlalchemy.exc import SQLAlchemyError

    c = identity_http
    calls = []

    def issue(*args):
        calls.append(args)
        raise {
            "provider": ApiException(status=503),
            "database": SQLAlchemyError("private"),
            "stale": StaleFence("private"),
            "invalid-csr": ValueError("private"),
        }.get(fault, AssertionError("Admission must not run"))

    monkeypatch.setattr(identity, "issue_identity", issue)
    header = {
        "missing": "",
        "scheme": "Basic token",
        "empty": "Bearer ",
        "large-token": "Bearer " + "x" * 65537,
    }.get(fault, "Bearer owned-pod-token")
    if fault == "unconfigured":
        c.k8s._configured = False
    response = c.client.post(
        f"/internal/deployments/{uuid.uuid4()}/identity",
        headers={"Authorization": header},
        content=b"x" * (16385 if fault == "large-csr" else 3),
    )
    expected = (
        413
        if fault == "large-csr"
        else 503
        if fault in {"unconfigured", "provider", "database"}
        else 401
    )
    assert response.status_code == expected and response.headers["cache-control"] == "no-store"
    assert "private" not in response.text
    assert bool(calls) == (fault in {"invalid-csr", "stale", "provider", "database"})


def test_identity_http_issues_real_certificate_without_native_sql_overlap(
    binding_scope, identity_http, monkeypatch
):
    from automl_api.api.routes import serving_artifacts
    from automl_api.db.session import get_session_factory

    c = binding_scope
    http = identity_http
    monkeypatch.setattr(serving_artifacts, "KubernetesTrainingClient", lambda: c.kubernetes)
    c.kubernetes._configured = True
    http.client.app.dependency_overrides[get_session_factory] = lambda: c.factory
    key = new_private_key()
    response = http.client.post(
        f"/internal/deployments/{c.deployment_id}/identity",
        headers={"Authorization": "Bearer " + c.identity.token},
        content=make_csr(key),
    )
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["identity"] == asdict(c.identity.peer)
    assert all(held == 0 for _, held in c.native_reads)


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "unbound",
        "binding-response",
        "binding-key",
        "revoke-during-review",
        "revoke-during-read",
        "corrupt-model",
        "legacy-token",
    ],
)
def test_pod_model_download_requires_current_bound_identity_and_original_bytes(
    binding_scope, identity_http, monkeypatch, fault
):
    from automl_api.api.routes import serving_artifacts
    from automl_api.db.session import get_db
    from automl_api.security.deployments import deployment_token

    c = binding_scope
    http = identity_http
    key = new_private_key()
    if fault != "unbound":
        identity.issue_identity(
            c.factory, c.deployment_id, c.identity.token, make_csr(key), c.kubernetes
        )
    monkeypatch.setattr(serving_artifacts, "KubernetesTrainingClient", lambda: c.kubernetes)
    c.kubernetes._configured = True

    def database():
        with c.factory() as db:
            yield db

    http.client.app.dependency_overrides[get_db] = database
    if fault in {"binding-response", "binding-key"}:
        with c.factory() as db, db.begin():
            command = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == identity.BINDING_OPERATION,
                )
            )
            if fault == "binding-response":
                command.response_payload = {}
            else:
                command.request_payload = {**command.request_payload, "public_key_digest": "0" * 64}
                command.request_hash = canonical_request_hash(command.request_payload)

    def revoke():
        with c.factory() as db, db.begin():
            db.get(ModelRun, c.deployment_id).status = RunStatus.CANCELLED

    if fault == "revoke-during-review":
        original = c.kubernetes.authentication.create_token_review

        def reviewed(*args, **kwargs):
            result = original(*args, **kwargs)
            revoke()
            return result

        c.kubernetes.authentication.create_token_review = reviewed
    elif fault == "revoke-during-read":
        c.state["during_read"] = revoke
    elif fault == "corrupt-model":
        c.state["body"] = b"x" * len(c.payload)
    headers = {"Authorization": "Bearer " + c.identity.token}
    if fault == "legacy-token":
        headers = {"X-Sceptre-Deployment-Token": deployment_token(c.deployment_id)}
    c.native_counts.clear()
    response = http.client.get(f"/internal/deployments/{c.deployment_id}/model", headers=headers)
    if fault == "none":
        assert response.status_code == 200, response.text
        assert response.content == c.payload
        assert response.headers["cache-control"] == "no-store"
        assert c.native_counts == [0] and all(c.closed)
    else:
        assert response.status_code in {401, 409}, response.text
        assert c.payload not in response.content
        if fault not in {"revoke-during-read", "corrupt-model"}:
            assert not c.native_counts
    assert all(held == 0 for _, held in c.native_reads)
    assert all(held == 0 for held in c.native_counts)


@pytest.fixture
def gateway_peer(binding_scope, monkeypatch):
    from automl_api.services import inference_gateway as gateway

    c = binding_scope
    c.gateway_key = new_private_key()
    c.gateway_certificate = identity.issue_identity(
        c.factory, c.deployment_id, c.identity.token, make_csr(c.gateway_key), c.kubernetes
    )["certificate"].encode()
    c.identity.pod["status"].update(
        podIP="10.42.0.5", conditions=[dict(type="Ready", status="True")]
    )
    c.inventory = dict(continued=False, duplicate=False)

    def inventory(**kwargs):
        c.native_reads.append(("gateway-pods", c.engine.pool.checkedout()))
        assert (
            kwargs["namespace"] == c.kubernetes.settings.training_namespace
            and kwargs["limit"] == 100
            and kwargs["_request_timeout"] == 5
        )
        pods = [copy.deepcopy(c.identity.pod)] * (2 if c.inventory["duplicate"] else 1)
        return SimpleNamespace(
            items=pods,
            metadata=SimpleNamespace(_continue="more" if c.inventory["continued"] else None),
        )

    c.kubernetes.core.list_namespaced_pod = inventory
    c.kubernetes._configured = True
    monkeypatch.setattr(gateway, "KubernetesTrainingClient", lambda: c.kubernetes)
    monkeypatch.setattr(gateway, "get_settings", lambda: c.kubernetes.settings)
    with c.factory() as db, db.begin():
        db.get(ModelRun, c.deployment_id).status = RunStatus.SUCCEEDED

    def select_target():
        with c.factory() as db:
            selected = gateway.resolve_deployment_inference_target(
                db, db.get(User, c.user_id), c.project_id, c.deployment_id
            )
            db.commit()
            return selected

    def finalize_target(inspected):
        with c.factory() as db:
            result = gateway.finalize_deployment_inference_target(
                db, db.get(User, c.user_id), inspected
            )
            db.commit()
            return result

    c.select_target, c.finalize_target = select_target, finalize_target
    return c


def test_gateway_requires_original_bound_ready_pod_without_sql_during_native_reads(gateway_peer):
    from automl_api.services import inference_gateway as gateway
    from fastapi import HTTPException

    c = gateway_peer
    with c.factory() as db, db.begin():
        run = db.get(ModelRun, c.deployment_id)
        run.tags = {**run.tags, "service_name": "foreign", "endpoint": "http://169.254.169.254"}
    selected = c.select_target()
    inspected = gateway.inspect_deployment_inference_target(selected)
    target = c.finalize_target(inspected)
    assert target.service_name == f"automl-model-{str(c.deployment_id)[:8]}"
    assert target.peer == c.identity.peer and target.address == "10.42.0.5"
    assert target.url_for("v1/predict") == "https://10.42.0.5:8443/v1/predict"
    assert target.tls_context.check_hostname and not target.tls_context.hostname_checks_common_name
    assert all(held == 0 for _, held in c.native_reads)
    selected.scope.request["model_digest"] = "changed"
    with pytest.raises(HTTPException) as failure:
        c.finalize_target(inspected)
    assert failure.value.status_code == 409


def test_gateway_rejects_incomplete_or_unready_native_inventory(gateway_peer):
    from automl_api.services import inference_gateway as gateway
    from fastapi import HTTPException

    c = gateway_peer
    selected = c.select_target()
    original = copy.deepcopy(c.identity.pod)
    for fault in (
        "continued",
        "duplicate",
        "not-ready",
        "loopback",
        "mapped-loopback",
        "scoped-ip",
        "owner",
    ):
        c.identity.pod = copy.deepcopy(original)
        c.inventory.update(continued=fault == "continued", duplicate=fault == "duplicate")
        if fault == "not-ready":
            c.identity.pod["status"]["conditions"][0]["status"] = "False"
        elif fault == "loopback":
            c.identity.pod["status"]["podIP"] = "127.0.0.1"
        elif fault == "mapped-loopback":
            c.identity.pod["status"]["podIP"] = "::ffff:127.0.0.1"
        elif fault == "scoped-ip":
            c.identity.pod["status"]["podIP"] = "2001:db8::1%interface"
        elif fault == "owner":
            c.identity.pod["metadata"]["ownerReferences"][0]["uid"] = "replacement"
        with pytest.raises(HTTPException) as failure:
            gateway.inspect_deployment_inference_target(selected)
        assert failure.value.status_code == 503, fault
    assert all(held == 0 for _, held in c.native_reads)


@pytest.mark.parametrize(
    "fault,code",
    [
        ("inactive", 401),
        ("token", 401),
        ("membership", 403),
        ("stop", 409),
        ("project-deleting", 409),
        ("binding", 409),
    ],
)
def test_gateway_revalidates_committed_reader_source_and_key_binding(gateway_peer, fault, code):
    from automl_api.models.projects import ProjectMembership
    from automl_api.services import inference_gateway as gateway
    from fastapi import HTTPException

    c = gateway_peer
    inspected = gateway.inspect_deployment_inference_target(c.select_target())
    with c.factory() as db, db.begin():
        if fault == "inactive":
            db.get(User, c.user_id).is_active = False
        elif fault == "token":
            db.get(User, c.user_id).token_version += 1
        elif fault == "membership":
            db.scalar(
                select(ProjectMembership).where(
                    ProjectMembership.project_id == c.project_id,
                    ProjectMembership.user_id == c.user_id,
                )
            ).expires_at = datetime.now(UTC) - timedelta(seconds=1)
        elif fault == "stop":
            db.get(ModelRun, c.deployment_id).status = RunStatus.CANCELLED
        elif fault == "project-deleting":
            project = db.get(Project, c.project_id)
            project.settings = {**project.settings, "deletion_in_progress": True}
        else:
            db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.project_id == c.project_id,
                    WorkflowCommand.operation == identity.BINDING_OPERATION,
                )
            ).response_payload = {}
    with pytest.raises(HTTPException) as failure:
        c.finalize_target(inspected)
    assert failure.value.status_code == code
    assert all(held == 0 for _, held in c.native_reads)


@pytest.mark.parametrize("foreign", [False, True])
def test_gateway_real_tls_authenticates_peer_before_headers_or_body(
    gateway_peer, monkeypatch, foreign
):
    import asyncio
    from dataclasses import replace

    from automl_api.services import inference_gateway as gateway
    from automl_api.services.inference_tls import server_context
    from fastapi import HTTPException
    from starlette.requests import Request

    c = gateway_peer
    admitted = c.finalize_target(gateway.inspect_deployment_inference_target(c.select_target()))
    peer = replace(c.identity.peer, pod_uid=str(uuid.uuid4())) if foreign else c.identity.peer
    certificate = c.inference_authority.issue_server(
        make_csr(c.gateway_key), peer, datetime.now(UTC) + timedelta(seconds=300)
    )
    context = server_context(c.inference_authority.pem, certificate, private_key_pem(c.gateway_key))
    received, consumed = [], []

    async def scenario():
        async def handler(reader, writer):
            header = await reader.readuntil(b"\r\n\r\n")
            body = bytearray()
            while True:
                size = int((await reader.readline()).strip(), 16)
                if size == 0:
                    await reader.readline()
                    break
                body.extend(await reader.readexactly(size))
                await reader.readexactly(2)
            received.append((header, bytes(body)))
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=context)
        monkeypatch.setattr(gateway, "DATA_PORT", server.sockets[0].getsockname()[1])
        target = replace(admitted, address="127.0.0.1")

        async def receive():
            consumed.append(True)
            return dict(
                type="http.request", body=b'{"records":[{"private_feature":7}]}', more_body=False
            )

        request = Request(
            dict(
                type="http",
                method="POST",
                path="/gateway/v1/predict",
                raw_path=b"/gateway/v1/predict",
                query_string=b"",
                headers=[
                    (b"authorization", b"Bearer browser-secret"),
                    (b"cookie", b"session=browser-secret"),
                    (b"content-type", b"application/json"),
                ],
            ),
            receive,
        )
        try:
            if foreign:
                with pytest.raises(HTTPException) as failure:
                    await gateway.proxy_deployment_inference(request, target, "v1/predict")
                assert failure.value.status_code == 502
                assert not received and not consumed
            else:
                response = await gateway.proxy_deployment_inference(request, target, "v1/predict")
                assert b"".join([part async for part in response.body_iterator]) == b"{}"
                assert received[0][1] == b'{"records":[{"private_feature":7}]}'
                assert (
                    b"browser-secret" not in received[0][0]
                    and b"X-Sceptre-Deployment-Token" not in received[0][0]
                )
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_actual_gateway_route_releases_sql_before_native_body_and_http(gateway_peer, monkeypatch):
    import asyncio

    import httpx
    from automl_api.api.routes.operations import deployment_inference_gateway
    from automl_api.services import inference_gateway as gateway
    from starlette.requests import Request

    c = gateway_peer
    events = []

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{}"

    async def handler(request):
        assert c.engine.pool.checkedout() == 0
        events.append("http")
        assert await request.aread() == b'{"records":[{"x":1}]}'
        assert "x-sceptre-deployment-token" not in request.headers
        return httpx.Response(200, stream=Body())

    def client(target):
        target.require_admission()
        assert c.engine.pool.checkedout() == 0
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)

    monkeypatch.setattr(gateway, "_new_http_client", client)

    async def receive():
        assert c.engine.pool.checkedout() == 0
        events.append("body")
        return dict(type="http.request", body=b'{"records":[{"x":1}]}', more_body=False)

    async def scenario():
        request = Request(
            dict(
                type="http",
                method="POST",
                path="/gateway/v1/predict",
                raw_path=b"/gateway/v1/predict",
                query_string=b"",
                headers=[],
            ),
            receive,
        )
        with c.factory() as db:
            response = await deployment_inference_gateway(
                c.project_id, c.deployment_id, "v1/predict", request, db, db.get(User, c.user_id)
            )
            assert c.engine.pool.checkedout() == 0
            assert b"".join([chunk async for chunk in response.body_iterator]) == b"{}"

    asyncio.run(scenario())
    assert events == ["body", "http"]
    assert all(held == 0 for _, held in c.native_reads)


def test_gateway_selection_cannot_lock_artifact_before_waiting_for_model(gateway_peer):
    import queue
    import time
    from concurrent.futures import ThreadPoolExecutor

    from automl_api.models.runs import RunArtifact
    from sqlalchemy import text

    c = gateway_peer
    backend = queue.Queue()

    def select_target():
        from automl_api.services import inference_gateway as gateway

        with c.factory() as db:
            backend.put(db.scalar(text("SELECT pg_backend_pid()")))
            result = gateway.resolve_deployment_inference_target(
                db, db.get(User, c.user_id), c.project_id, c.deployment_id
            )
            db.commit()
            return result

    with ThreadPoolExecutor(max_workers=1) as pool:
        with c.factory() as db:
            transaction = db.begin()
            try:
                owner = db.scalar(text("SELECT pg_backend_pid()"))
                db.get(ModelRun, c.deployment_id, with_for_update=True)
                future = pool.submit(select_target)
                waiter = backend.get(timeout=10)
                deadline = time.monotonic() + 10
                while owner not in db.scalar(
                    text("SELECT pg_blocking_pids(:pid)"), dict(pid=waiter)
                ):
                    assert time.monotonic() < deadline, "Gateway did not reach the held model fence"
                    time.sleep(0.02)
                db.scalar(
                    select(RunArtifact)
                    .where(RunArtifact.id == c.dockerfile_id)
                    .with_for_update(nowait=True)
                )
            finally:
                transaction.rollback()
        assert future.result(timeout=10).deployment_id == c.deployment_id


@pytest.mark.parametrize("completion_case", ["offline"], indirect=True)
@pytest.mark.parametrize("verification_case", ["offline"], indirect=True)
@pytest.mark.parametrize("foreign", [False, True])
def test_actual_offline_route_pins_verified_upload_after_real_tls(
    gateway_peer, verification_case, monkeypatch, foreign
):
    import asyncio
    from dataclasses import replace

    from automl_api.api.routes import operations as operation_routes
    from automl_api.api.routes.operations import deployment_inference_gateway
    from automl_api.services import inference_gateway as gateway
    from automl_api.services.inference_tls import server_context
    from automl_api.services.upload_verification import _policy
    from fastapi import HTTPException
    from starlette.requests import Request

    c, upload = gateway_peer, verification_case
    gateway_settings = replace(
        c.kubernetes.settings,
        object_store_endpoint=upload.settings.object_store_endpoint,
        object_store_type=upload.settings.object_store_type,
    )
    monkeypatch.setattr(gateway, "get_settings", lambda: gateway_settings)
    assert _policy(c.kubernetes.settings) == _policy(upload.settings), (
        c.kubernetes.settings.environment,
        upload.settings.environment,
    )
    assert c.project_id == upload.project_id and c.user_id == upload.actor_id
    upload.verify()
    seen, events = [], []

    class Source:
        def __init__(self):
            import io

            self.stream = io.BytesIO(upload.content)

        def read(self, size):
            assert c.engine.pool.checkedout() == 0
            events.append("read")
            return self.stream.read(size)

        def close(self):
            assert c.engine.pool.checkedout() == 0
            events.append("close")
            self.stream.close()

    def open_stream(uri, *, version=None):
        assert c.engine.pool.checkedout() == 0
        assert version == "original-v1"
        events.append("open")
        return Source()

    def store():
        assert c.engine.pool.checkedout() == 0
        events.append("store")
        return SimpleNamespace(driver_name="s3_compatible", open_stream=open_stream)

    monkeypatch.setattr(gateway, "get_object_store", store)
    peer = replace(c.identity.peer, pod_uid=str(uuid.uuid4())) if foreign else c.identity.peer
    certificate = c.inference_authority.issue_server(
        make_csr(c.gateway_key), peer, datetime.now(UTC) + timedelta(seconds=300)
    )
    context = server_context(c.inference_authority.pem, certificate, private_key_pem(c.gateway_key))

    async def scenario():
        async def handler(reader, writer):
            try:
                headers = await reader.readuntil(b"\r\n\r\n")
                body = bytearray()
                while True:
                    size = int((await reader.readline()).strip(), 16)
                    if not size:
                        await reader.readline()
                        break
                    body.extend(await reader.readexactly(size))
                    await reader.readexactly(2)
                seen.append((headers, bytes(body)))
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=context)
        monkeypatch.setattr(gateway, "DATA_PORT", server.sockets[0].getsockname()[1])
        real_inspect = gateway.inspect_deployment_inference_target
        monkeypatch.setattr(
            operation_routes,
            "inspect_deployment_inference_target",
            lambda selection: replace(real_inspect(selection), address="127.0.0.1"),
        )

        async def receive():
            assert c.engine.pool.checkedout() == 0
            events.append("body")
            return dict(
                type="http.request",
                body=json.dumps({"upload_session_id": str(upload.upload_id)}).encode(),
                more_body=False,
            )

        request = Request(
            dict(
                type="http",
                method="POST",
                path="/gateway/v1/predict/offline",
                raw_path=b"/gateway/v1/predict/offline",
                query_string=b"format=csv",
                headers=[
                    (b"content-type", b"application/json"),
                    (b"authorization", b"Bearer browser-secret"),
                    (b"cookie", b"session=browser-secret"),
                ],
            ),
            receive,
        )
        try:
            with c.factory() as db:
                if foreign:
                    with pytest.raises(HTTPException) as error:
                        await deployment_inference_gateway(
                            c.project_id,
                            c.deployment_id,
                            "v1/predict/offline",
                            request,
                            db,
                            db.get(User, c.user_id),
                        )
                    assert error.value.status_code == 502
                    assert not seen and not any(e in events for e in ["open", "read", "close"])
                else:
                    response = await deployment_inference_gateway(
                        c.project_id,
                        c.deployment_id,
                        "v1/predict/offline",
                        request,
                        db,
                        db.get(User, c.user_id),
                    )
                    assert c.engine.pool.checkedout() == 0
                    assert b"".join([chunk async for chunk in response.body_iterator]) == b"{}"
                    assert upload.content in seen[0][1]
                    assert b"browser-secret" not in seen[0][0]
                    assert events.count("open") == events.count("close") == 1
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


@pytest.mark.parametrize("completion_case", ["offline"], indirect=True)
@pytest.mark.parametrize("verification_case", ["offline"], indirect=True)
def test_actual_offline_route_rechecks_reader_model_and_custody_after_json(
    gateway_peer, verification_case, monkeypatch
):
    import asyncio
    from dataclasses import replace

    from automl_api.api.routes.operations import deployment_inference_gateway
    from automl_api.models.datasets import DatasetUploadSession
    from automl_api.models.enums import ProjectRole
    from automl_api.models.projects import ProjectMembership
    from automl_api.models.workflows import OutboxEntry
    from automl_api.services import inference_gateway as gateway
    from automl_api.services import upload_completion
    from automl_api.services.upload_verification import RECORD
    from fastapi import HTTPException
    from starlette.requests import Request

    c, upload = gateway_peer, verification_case
    upload.verify()
    with c.factory() as db:
        original = db.get(DatasetUploadSession, upload.upload_id)
        state = copy.deepcopy(original.provider_state)
        filename = original.original_filename
        token_version = db.get(User, c.user_id).token_version
        membership_id = db.scalar(
            select(ProjectMembership.id).where(
                ProjectMembership.project_id == c.project_id, ProjectMembership.user_id == c.user_id
            )
        )
        role = db.get(ProjectMembership, membership_id).role
        project_settings = copy.deepcopy(db.get(Project, c.project_id).settings)
        tags = copy.deepcopy(db.get(ModelRun, c.deployment_id).tags)
        payload = copy.deepcopy(db.get(OutboxEntry, upload.entry_id).payload)
    original_settings = replace(
        c.kubernetes.settings,
        object_store_endpoint=upload.settings.object_store_endpoint,
        object_store_type=upload.settings.object_store_type,
    )
    monkeypatch.setattr(gateway, "get_settings", lambda: original_settings)
    expected = {
        "token": 401,
        "role": 403,
        "project": 409,
        "model": 409,
        "source": 409,
        "receipt": 409,
        "proof": 409,
        "anchor": 409,
        "policy": 409,
    }
    cases = []

    def no_store():
        raise AssertionError("A revoked or changed request cannot construct an object store")

    monkeypatch.setattr(gateway, "get_object_store", no_store)

    async def scenario(fault, code):
        async def receive():
            assert c.engine.pool.checkedout() == 0
            with c.factory() as db, db.begin():
                if fault == "token":
                    db.get(User, c.user_id).token_version += 1
                elif fault == "role":
                    db.get(ProjectMembership, membership_id).role = ProjectRole.VIEWER
                elif fault == "project":
                    db.get(Project, c.project_id).settings = {
                        **project_settings,
                        "deletion_in_progress": True,
                    }
                elif fault == "model":
                    db.get(ModelRun, c.deployment_id).tags = {
                        **tags,
                        "desired_state": "deployment_stopped",
                    }
                elif fault == "source":
                    db.get(
                        DatasetUploadSession, upload.upload_id
                    ).original_filename = "replacement.csv"
                elif fault in {"receipt", "proof"}:
                    changed = copy.deepcopy(state)
                    if fault == "receipt":
                        changed[upload_completion.RECORD]["receipt"]["object_version"] = (
                            "replacement"
                        )
                    else:
                        changed.pop(RECORD)
                    db.get(DatasetUploadSession, upload.upload_id).provider_state = changed
                elif fault == "anchor":
                    db.get(OutboxEntry, upload.entry_id).payload = {
                        k: v for k, v in payload.items() if k != "verification_digest"
                    }
                else:
                    monkeypatch.setattr(
                        gateway,
                        "get_settings",
                        lambda: replace(
                            original_settings, upload_scanner_signature_version="changed"
                        ),
                    )
            return dict(
                type="http.request",
                body=json.dumps({"upload_session_id": str(upload.upload_id)}).encode(),
                more_body=False,
            )

        request = Request(
            dict(
                type="http",
                method="POST",
                path="/gateway/v1/predict/offline",
                raw_path=b"/gateway/v1/predict/offline",
                query_string=b"",
                headers=[(b"content-type", b"application/json")],
            ),
            receive,
        )
        with c.factory() as db:
            with pytest.raises(HTTPException) as error:
                await deployment_inference_gateway(
                    c.project_id,
                    c.deployment_id,
                    "v1/predict/offline",
                    request,
                    db,
                    db.get(User, c.user_id),
                )
            assert error.value.status_code == code, (fault, error.value.detail)
        cases.append(fault)

    for fault, code in expected.items():
        try:
            asyncio.run(scenario(fault, code))
        finally:
            monkeypatch.setattr(gateway, "get_settings", lambda: original_settings)
            with c.factory() as db, db.begin():
                db.get(User, c.user_id).token_version = token_version
                db.get(ProjectMembership, membership_id).role = role
                db.get(Project, c.project_id).settings = copy.deepcopy(project_settings)
                db.get(ModelRun, c.deployment_id).tags = copy.deepcopy(tags)
                current = db.get(DatasetUploadSession, upload.upload_id)
                current.provider_state = copy.deepcopy(state)
                current.original_filename = filename
                db.get(OutboxEntry, upload.entry_id).payload = copy.deepcopy(payload)
    assert cases == list(expected)
