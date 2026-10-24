"""Champion safe reads use committed pod- and version-bound grant evidence."""

import copy
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import jwt
import pytest
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowEvent,
)
from automl_api.services import attempt_identity
from automl_api.services import attempt_object_grants as grants
from automl_api.services.champion_workloads import DIGEST, isolated_workload
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.storage.contracts import ObjectMetadata
from kubernetes import client
from sqlalchemy import delete, select
from sqlalchemy.orm import sessionmaker
from test_champion_evaluation_control import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_control import control_case as evaluation_control_case  # noqa: F401
from test_champion_evaluation_control import handoff_case as handoff_case
from test_champion_evaluation_control import key_case as key_case
from test_champion_read_grants import grant_case as grant_case
from test_champion_refit_control import control_case as refit_control_case  # noqa: F401

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


def model(data, kind):
    return client.ApiClient().deserialize(SimpleNamespace(data=json.dumps(data)), kind)


@pytest.fixture
def champion(grant_case, monkeypatch):
    engine, route, claims, plan, _ = grant_case
    refit = hasattr(plan, "candidate")
    stage = "refit" if refit else "evaluation"
    plan = plan.model_copy(update={"credential_protocol": "pod-bound-object-v2"})
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    with factory() as db, db.begin():
        plan_event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == plan.attempt_id,
                WorkflowEvent.event_key == stage + "_plan",
            )
        )
        plan_event.payload = plan.model_dump(mode="json")
        plan_event.result_digest = plan.digest
        attempt = db.get(WorkflowAttempt, plan.attempt_id)
        attempt.workload_identity = f"project-{plan.project_id}-{stage}"
        attempt.lease_owner = stage + ":" + str(attempt.id)
        labels = {
            "automl.platform/project-id": str(plan.project_id),
            "automl.platform/attempt-id": str(attempt.id),
            "automl.platform/generation": str(attempt.generation),
            f"automl.platform/{stage}-attempt": str(attempt.id),
        }
        if not refit:
            labels["automl.platform/evaluation-scope"] = str(plan.scope_id)
        namespace = "broker-test"
        account = attempt.workload_identity
        desired = isolated_workload(
            metadata={
                "name": stage + "-" + attempt.id.hex,
                "namespace": namespace,
                "labels": labels,
            },
            image="registry/fixture@sha256:" + "a" * 64,
            seconds=600,
            env=[
                {"name": "AUTOML_IDENTITY_TOKEN_FILE", "value": "/var/run/sceptre/identity/token"}
            ],
            volumes=[
                {
                    "name": "identity",
                    "projected": {
                        "defaultMode": 0o444,
                        "sources": [
                            {
                                "serviceAccountToken": {
                                    "path": "token",
                                    "expirationSeconds": 600,
                                    "audience": attempt_identity.attempt_audience(
                                        attempt.id, attempt.fencing_token
                                    ),
                                }
                            }
                        ],
                    },
                }
            ],
            mounts=[
                {"name": "identity", "mountPath": "/var/run/sceptre/identity", "readOnly": True}
            ],
            disk=1024,
            memory_mib=128,
            cpu=1,
            settings=SimpleNamespace(workload_image_pull_secrets=()),
            egress=[
                {
                    "to": [{"podSelector": {"matchLabels": {"app": "fixture"}}}],
                    "ports": [{"port": 8443}],
                }
            ],
            worker=stage,
            service_account=account,
        )
        desired["object_grant_protocol"] = "pod-bound-object-v2"
        job = desired["job"]
        job["spec"]["template"]["spec"]["enableServiceLinks"] = False
        job["metadata"].pop("annotations")
        job["metadata"]["annotations"] = {DIGEST: canonical_request_hash(job)}
        db.add(
            WorkflowEvent(
                project_id=plan.project_id,
                attempt_id=attempt.id,
                sequence=100,
                event_key=stage + "_submission",
                event_type=stage + "_submission",
                payload=desired,
                result_digest=canonical_request_hash(desired),
            )
        )
        fence = attempt.fencing_token
    objects = copy.deepcopy(desired)
    for kind in ("job", "network"):
        objects[kind]["metadata"]["uid"] = str(uuid.uuid4())
    objects["service_account"] = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": account, "namespace": namespace, "uid": str(uuid.uuid4())},
        "automountServiceAccountToken": False,
    }
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "worker",
            "namespace": namespace,
            "uid": str(uuid.uuid4()),
            "labels": labels,
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": objects["job"]["metadata"]["name"],
                    "uid": objects["job"]["metadata"]["uid"],
                    "controller": True,
                }
            ],
        },
        "spec": copy.deepcopy(job["spec"]["template"]["spec"]),
    }
    token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 600},
        "test-champion-pod-token-key-long-enough",
        algorithm="HS256",
    )
    auth = MagicMock()
    auth.create_token_review.return_value = SimpleNamespace(
        status=SimpleNamespace(
            authenticated=True,
            audiences=[attempt_identity.attempt_audience(plan.attempt_id, fence)],
            user=SimpleNamespace(
                username=f"system:serviceaccount:{namespace}:{account}",
                extra={
                    "authentication.kubernetes.io/pod-name": ["worker"],
                    "authentication.kubernetes.io/pod-uid": [pod["metadata"]["uid"]],
                },
            ),
        )
    )
    monkeypatch.setattr(attempt_identity.client, "AuthenticationV1Api", lambda: auth)
    native = SimpleNamespace(core=MagicMock(), batch=MagicMock(), networking=MagicMock())

    def read(kind, model_type):
        def fetch(*_args, **_kwargs):
            assert engine.pool.checkedout() == 0
            return model(pod if kind == "pod" else objects[kind], model_type)

        return fetch

    native.core.read_namespaced_pod.side_effect = read("pod", "V1Pod")
    native.core.read_namespaced_service_account.side_effect = read(
        "service_account", "V1ServiceAccount"
    )
    native.batch.read_namespaced_job.side_effect = read("job", "V1Job")
    native.networking.read_namespaced_network_policy.side_effect = read(
        "network", "V1NetworkPolicy"
    )
    native.networking.list_namespaced_network_policy.return_value = SimpleNamespace(
        items=[], metadata=SimpleNamespace(_continue=None)
    )
    approved = (plan.candidate, *plan.partitions) if refit else (plan.frozen_pipeline,)
    items = {item.uri: item for item in approved}
    root = approved[0].uri.partition("/projects/")[0]
    calls = []

    class Store:
        driver_name = "s3_compatible"

        def uri_for_key(self, key):
            return root + "/" + key

        def stat(self, uri):
            assert engine.pool.checkedout() == 0
            return ObjectMetadata(
                uri=uri,
                byte_size=items[uri].byte_size,
                provider_headers={"version_id": "native-v1"},
            )

    def mint(store, item, deadline, *, object_version):
        assert engine.pool.checkedout() == 0
        assert object_version == "native-v1"
        calls.append((item.uri, deadline))
        return {
            "url": "https://storage.test/exact?versionId=native-v1",
            "expires_at": deadline.isoformat(),
        }

    monkeypatch.setattr(grants, "mint_refit_read_url", mint)
    c = SimpleNamespace(
        engine=engine,
        factory=factory,
        plan=plan,
        stage=stage,
        token=token,
        native=native,
        pod=pod,
        objects=objects,
        auth=auth,
        calls=calls,
        store=Store(),
        request={
            "request_id": uuid.uuid4(),
            "role": "candidate" if refit else "pipeline",
            "object_index": 0,
        },
    )
    try:
        yield c
    finally:
        # Doubled signer issued no native capability; remove only this fixture's grants.
        with factory() as db, db.begin():
            db.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == plan.attempt_id
                )
            )


def issue(c):
    return grants.issue_input_grant(
        c.factory, c.plan.attempt_id, c.token, c.native, c.request, store_factory=lambda: c.store
    )


def test_champion_read_is_durably_bound(champion):
    c = champion
    first = issue(c)
    assert first["url"] is not None
    assert issue(c) == {**first, "url": None}
    assert len(c.calls) == 1
    with c.factory() as db:
        row = db.scalar(
            select(WorkflowAttemptCredentialGrant).where(
                WorkflowAttemptCredentialGrant.attempt_id == c.plan.attempt_id
            )
        )
        assert row.pod_uid == c.pod["metadata"]["uid"]
        assert row.object_version == "native-v1" and row.role == c.request["role"]
        assert row.status == "issued" and row.expires_at.isoformat() == first["expires_at"]


def saved_grants(c):
    with c.factory() as db:
        return list(
            db.scalars(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.plan.attempt_id
                )
            )
        )


@pytest.mark.parametrize(
    "fault",
    [
        "job_uid",
        "pod_uid",
        "sa_uid",
        "policy_uid",
        "pod_env",
        "job_retries",
        "sa_annotation",
        "broad_policy",
        "wrong_audience",
        "unbound_token",
        "wrong_role",
    ],
)
def test_champion_read_rejects_identity_and_authority_changes(champion, fault):
    from automl_api.services.workflow_state import StaleFence

    c = champion
    first = issue(c)
    if fault == "job_uid":
        c.objects["job"]["metadata"]["uid"] = str(uuid.uuid4())
        c.pod["metadata"]["ownerReferences"][0]["uid"] = c.objects["job"]["metadata"]["uid"]
    elif fault == "pod_uid":
        c.pod["metadata"]["uid"] = str(uuid.uuid4())
        c.auth.create_token_review.return_value.status.user.extra[
            "authentication.kubernetes.io/pod-uid"
        ] = [c.pod["metadata"]["uid"]]
        c.token = jwt.encode(
            {"exp": int(datetime.now(UTC).timestamp()) + 600, "nonce": str(uuid.uuid4())},
            "test-champion-pod-token-key-long-enough",
            algorithm="HS256",
        )
    elif fault == "sa_uid":
        c.objects["service_account"]["metadata"]["uid"] = str(uuid.uuid4())
    elif fault == "policy_uid":
        c.objects["network"]["metadata"]["uid"] = str(uuid.uuid4())
    elif fault == "pod_env":
        c.pod["spec"]["containers"][0]["env"].append({"name": "DATABASE_URL", "value": "forbidden"})
    elif fault == "job_retries":
        c.objects["job"]["spec"]["backoffLimit"] = 1
    elif fault == "sa_annotation":
        c.objects["service_account"]["metadata"]["annotations"] = {"cloud-role": "ambient"}
    elif fault == "broad_policy":
        c.native.networking.list_namespaced_network_policy.return_value.items = [
            {"metadata": {"name": "broad"}, "spec": {"podSelector": {}, "egress": [{}]}}
        ]
    elif fault == "wrong_audience":
        c.auth.create_token_review.return_value.status.audiences = ["another-attempt"]
    elif fault == "unbound_token":
        c.auth.create_token_review.return_value.status.user.extra = {}
    elif fault == "wrong_role":
        c.request["role"] = "pipeline" if c.stage == "refit" else "candidate"
    c.request["request_id"] = uuid.uuid4()
    with pytest.raises(StaleFence):
        issue(c)
    rows = saved_grants(c)
    assert len(rows) == len(c.calls) == 1
    assert rows[0].status == "issued" and rows[0].expires_at.isoformat() == first["expires_at"]


@pytest.mark.parametrize("fault", ["missing", "null", "too_long", "size"])
def test_champion_read_requires_real_immutable_version_before_intent(champion, fault):
    c = champion
    original = c.store.stat

    def metadata(uri):
        value = original(uri)
        headers = {
            "missing": {},
            "null": {"version_id": "null"},
            "too_long": {"version_id": "v" * 257},
            "size": {"version_id": "native-v1"},
        }[fault]
        return ObjectMetadata(
            uri=uri, byte_size=value.byte_size + (fault == "size"), provider_headers=headers
        )

    c.store.stat = metadata
    with pytest.raises(grants.ObjectGrantUnavailable):
        issue(c)
    assert c.calls == [] and saved_grants(c) == []


@pytest.mark.parametrize("when", ["stat", "mint"])
def test_champion_read_revocation_during_external_io_never_discloses(champion, monkeypatch, when):
    from automl_api.services.workflow_state import StaleFence

    c = champion
    original = c.store.stat if when == "stat" else grants.mint_refit_read_url
    deadlines = []

    def remote(*args, **kwargs):
        value = original(*args, **kwargs)
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            if when == "mint":
                row = db.scalar(
                    select(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.attempt_id == c.plan.attempt_id
                    )
                )
                assert row.status == "issuing"
                deadlines.append(row.expires_at)
            grants.revoke_attempt_grants(db, c.plan.attempt_id)
        return value

    if when == "stat":
        c.store.stat = remote
    else:
        monkeypatch.setattr(grants, "mint_refit_read_url", remote)
    with pytest.raises(StaleFence if when == "stat" else grants.ObjectGrantUnavailable):
        issue(c)
    rows = saved_grants(c)
    if when == "stat":
        assert rows == [] and c.calls == []
    else:
        assert len(rows) == len(c.calls) == 1 and rows[0].status == "revoked"
        assert rows[0].expires_at == deadlines[0]
        with c.factory() as db:
            assert grants.unretired_grant_deadline(db, c.plan.attempt_id) == deadlines[0]


def test_champion_broker_keeps_role_boundaries_for_train_and_validation(champion):
    from automl_api.services.workflow_state import StaleFence

    c = champion
    for role in ("train", "validation"):
        c.request = {"request_id": uuid.uuid4(), "role": role, "object_index": 0}
        if c.stage == "refit":
            assert issue(c)["object_version"] == "native-v1"
        else:
            with pytest.raises(StaleFence):
                issue(c)
    rows = saved_grants(c)
    assert {r.role for r in rows} == ({"train", "validation"} if c.stage == "refit" else set())


def test_champion_broker_token_rotation_does_not_remint_or_extend_expiry(champion):
    import hashlib

    c = champion
    original_token_hash = hashlib.sha256(c.token.encode()).hexdigest()
    first = issue(c)
    c.token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 660, "rotation": str(uuid.uuid4())},
        "test-champion-pod-token-key-long-enough",
        algorithm="HS256",
    )
    assert issue(c) == {**first, "url": None}
    row = saved_grants(c)[0]
    assert row.token_sha256 == original_token_hash and len(c.calls) == 1
    assert row.expires_at.isoformat() == first["expires_at"]


def test_champion_broker_expired_original_mint_cannot_be_disclosed_after_renewal(
    champion, monkeypatch
):
    from datetime import timedelta

    c = champion
    original = grants.mint_refit_read_url
    deadlines = []

    def delayed(*args, **kwargs):
        value = original(*args, **kwargs)
        with c.factory() as db, db.begin():
            row = db.scalar(
                select(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.attempt_id == c.plan.attempt_id
                )
            )
            deadlines.append(row.expires_at)
            db.get(WorkflowAttempt, c.plan.attempt_id).lease_expires_at = datetime.now(
                UTC
            ) + timedelta(minutes=2)

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return deadlines[0] + timedelta(seconds=1)

        monkeypatch.setattr(grants, "datetime", Clock)
        return value

    monkeypatch.setattr(grants, "mint_refit_read_url", delayed)
    with pytest.raises(grants.ObjectGrantUnavailable):
        issue(c)
    row = saved_grants(c)[0]
    assert row.status == "expired" and row.expires_at == deadlines[0] and len(c.calls) == 1


def test_champion_worker_reads_through_real_http_broker_and_postgresql(
    champion, request, monkeypatch, tmp_path
):
    import hashlib
    from urllib.parse import urlencode

    import httpx
    from automl_api.api.routes import attempt_control, evaluation_control, refit_control
    from automl_api.db.session import get_session_factory
    from automl_api.models.enums import AttemptStatus
    from automl_api.training import evaluation_worker, refit_worker
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    c = champion
    original_case = request.getfixturevalue(c.stage + "_control_case")
    control = original_case[0]
    route = refit_control if c.stage == "refit" else evaluation_control
    backend = route.get_object_store()
    from champion_broker_fixture import install_native_output

    install_native_output(monkeypatch, c, backend)
    with c.factory() as db, db.begin():
        db.get(WorkflowAttempt, c.plan.attempt_id).status = AttemptStatus.SUBMITTED
    token = tmp_path / "projected-token"
    token.write_text(c.token)
    monkeypatch.setenv(
        "ATTEMPT_CONTROL_BASE_URL", "https://control.invalid/api/v1/internal/attempts"
    )
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token))
    c.native._configured = True
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.native)
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: c.store)
    app = FastAPI()
    app.include_router(attempt_control.router, prefix="/api/v1")
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    urls = {}
    requests = []
    original_mint = grants.mint_refit_read_url

    def mint(store, item, deadline, *, object_version):
        value = original_mint(store, item, deadline, object_version=object_version)
        key = hashlib.sha256(item.uri.encode()).hexdigest()
        urls[key] = item.uri
        value["url"] = (
            "https://objects.invalid/"
            + key
            + "?"
            + urlencode(
                {
                    "X-Amz-Date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                    "X-Amz-Expires": "30",
                    "versionId": object_version,
                }
            )
        )
        return value

    monkeypatch.setattr(grants, "mint_refit_read_url", mint)
    actual_client = httpx.Client
    with TestClient(app, base_url="https://control.invalid") as broker:

        def transport(req):
            requests.append(req)
            if req.url.host == "control.invalid":
                assert req.headers["authorization"] == "Bearer " + c.token
                response = broker.request(
                    req.method, str(req.url), headers=dict(req.headers), content=req.content
                )
                return httpx.Response(
                    response.status_code, content=response.content, headers=dict(response.headers)
                )
            assert req.url.host == "objects.invalid" and req.method == "GET"
            assert "authorization" not in req.headers
            with backend.open_stream(urls[req.url.path.lstrip("/")]) as source:
                data = source.read()
            return httpx.Response(
                200,
                stream=httpx.ByteStream(data),
                headers={
                    "Content-Length": str(len(data)),
                    "x-amz-version-id": "native-v1",
                },
            )

        def client(**kwargs):
            assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
            assert kwargs["verify"].check_hostname
            return actual_client(**kwargs, transport=httpx.MockTransport(transport))

        monkeypatch.setattr(httpx, "Client", client)
        if c.stage == "refit":
            result = refit_worker.run(control)
            assert result["row_count"] == sum(p.rows for p in c.plan.partitions)
            expected = 1 + len(c.plan.partitions)
        else:
            assert control.post("start").status_code == 200
            pipeline = evaluation_worker.CachedPipeline(
                c.plan,
                control,
                tmp_path / "pipeline.joblib",
                ca_file=None,
                progress=lambda: None,
            )
            with pipeline.open_stream(c.plan.frozen_pipeline.uri) as data:
                assert hashlib.sha256(data.read()).hexdigest() == c.plan.frozen_pipeline.sha256
            expected = 1
    rows = saved_grants(c)
    reads = [row for row in rows if row.method == "GET"]
    writes = [row for row in rows if row.method == "PUT"]
    assert len(reads) == len(c.calls) == expected
    assert len(writes) == (1 if c.stage == "refit" else 0)
    assert all(row.status == "issued" and row.object_version == "native-v1" for row in reads)
    for row in writes:
        with c.factory() as db:
            assert row.status == "issued" and row.object_version is None
            assert grants.output_completion(db, row)["object_version"] == "native-v1"
    assert sum(req.url.host == "objects.invalid" for req in requests) == expected
    assert sum(req.url.host == "control.invalid" for req in requests) == expected + len(writes)


def test_legacy_champion_read_endpoint_cannot_bypass_broker(champion, request):
    c = champion
    assert issue(c)["url"]
    control = request.getfixturevalue(c.stage + "_control_case")[0]
    response = control.get("inputs/0" if c.stage == "refit" else "pipeline")
    assert response.status_code == 410
    assert response.json()["detail"]["code"] == "pod_bound_grants_required"
    assert response.headers["cache-control"] == "no-store"
    assert len(saved_grants(c)) == len(c.calls) == 1


@pytest.mark.parametrize(
    "mutation",
    ["cancel", "supersede", "fence", "expiry", "deadline", "short_lease", "plan", "heartbeat"],
)
@pytest.mark.parametrize("when", ["stat", "mint"])
def test_champion_rechecks_authority_after_external_io(champion, monkeypatch, mutation, when):
    from datetime import timedelta

    from automl_api.models.enums import AttemptStatus, ScopeStatus
    from automl_api.models.workflows import PromotionalScope
    from automl_api.services.workflow_state import StaleFence

    c = champion
    # The grant FK prevents changing a fence once its intent exists; exercise it before intent.
    if mutation == "fence" and when == "mint":
        when = "stat"
    original = c.store.stat if when == "stat" else grants.mint_refit_read_url
    deadlines = []

    def remote(*args, **kwargs):
        value = original(*args, **kwargs)
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            scope = db.get(PromotionalScope, c.plan.scope_id)
            attempt = db.get(WorkflowAttempt, c.plan.attempt_id)
            if when == "mint":
                deadlines.append(saved_grants(c)[0].expires_at)
            if mutation == "cancel":
                scope.status = ScopeStatus.CANCELLED
            elif mutation == "supersede":
                attempt.status = AttemptStatus.SUPERSEDED
            elif mutation == "fence":
                attempt.fencing_token = uuid.uuid4().hex
            elif mutation == "expiry":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif mutation == "deadline":
                scope.scope_deadline_at = datetime.now(UTC) + timedelta(seconds=5)
            elif mutation == "short_lease":
                attempt.lease_expires_at = datetime.now(UTC) + timedelta(seconds=5)
            elif mutation == "heartbeat":
                attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
            elif mutation == "plan":
                event = db.scalar(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id == c.plan.attempt_id,
                        WorkflowEvent.event_key == c.stage + "_plan",
                    )
                )
                changed = c.plan.model_copy(update={"max_model_bytes": c.plan.max_model_bytes + 1})
                event.payload = changed.model_dump(mode="json")
                event.result_digest = changed.digest
        return value

    if when == "stat":
        c.store.stat = remote
    else:
        monkeypatch.setattr(grants, "mint_refit_read_url", remote)
    if mutation == "heartbeat" or (when == "stat" and mutation == "short_lease"):
        first = issue(c)
        row = saved_grants(c)[0]
        assert row.status == "issued" and len(c.calls) == 1
        assert row.expires_at.isoformat() == first["expires_at"]
        if deadlines:
            assert row.expires_at == deadlines[0]
    else:
        with pytest.raises((StaleFence, grants.ObjectGrantUnavailable)):
            issue(c)
        rows = saved_grants(c)
        if when == "stat":
            assert rows == [] and c.calls == []
        else:
            assert len(rows) == len(c.calls) == 1 and rows[0].status == "unknown"
            assert rows[0].expires_at == deadlines[0]
    assert c.engine.pool.checkedout() == 0


def test_champion_mint_failure_is_sanitized_unknown_and_never_reissued(champion, monkeypatch):
    from automl_api.services.workflow_state import StaleFence

    c = champion
    calls = []
    secret = "https://objects.test/private?credential=must-not-disclose"

    def failed(*args, **kwargs):
        assert c.engine.pool.checkedout() == 0
        calls.append(True)
        raise ConnectionError(secret)

    monkeypatch.setattr(grants, "mint_refit_read_url", failed)
    with pytest.raises(grants.ObjectGrantUnavailable) as denied:
        issue(c)
    assert secret not in str(denied.value)
    row = saved_grants(c)[0]
    assert row.status == "unknown" and secret not in row.failure_reason
    with pytest.raises(StaleFence):
        issue(c)
    assert calls == [True] and c.engine.pool.checkedout() == 0
