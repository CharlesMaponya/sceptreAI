import base64
import hashlib
import ipaddress
import ssl
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pandas as pd
import pytest
from automl_api.api.routes import evaluation_control
from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus, ScopeStatus
from automl_api.models.qualification import FinalTestAllocation
from automl_api.models.workflows import DatasetSplitRevision, WorkflowAttempt, WorkflowEvent
from automl_api.qualification_control import create_app
from automl_api.services import evaluation_access
from automl_api.services.champion_planning import plan_scope_refit
from automl_api.services.evaluation_keys import KEY_FIELD, prepare_evaluator_credentials
from automl_api.services.evaluation_planning import validate_evaluation_policy
from automl_api.services.evaluation_publication import recover_evaluation_result
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.training import evaluation_worker
from automl_api.training.champion_evaluation import verify_result
from automl_api.training.champion_refit import (
    FrozenPipeline,
    RefitPlan,
    execute_refit,
    publish_frozen_pipeline,
)
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from test_champion_evaluation_authority import compatible_uri_fixture as compatible_uri_fixture
from test_champion_evaluation_keys import SecretAPI

pytest_plugins = ["test_champion_evaluation_planning", "test_qualification_control"]


@pytest.fixture
def worker_case(policy_case, authority_api, tmp_path, monkeypatch):
    db, store, scope, _ = policy_case
    _, _, authority_options = authority_api
    objects, requests, entries = {}, [], []
    rows = [f"final-{i}" for i in range(10)]
    inputs = pd.DataFrame({"row_id": rows, "x": range(15, 25), "split_role": "final_test"})
    labels = pd.DataFrame({"row_id": rows, "target": [2 * i + 3 for i in range(15, 25)]})
    for role, frame in (("inputs", inputs), ("labels", labels.iloc[::-1])):
        content = frame.to_parquet(index=False)
        key = f"{role}/part.parquet"
        objects[f"/{key}?versionId=1"] = content
        entries.append(
            dict(
                role=role,
                key=key,
                version="1",
                byte_size=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )
        )
    row_xor = 0
    for row in rows:
        row_xor ^= int.from_bytes(hashlib.sha256(row.encode()).digest(), "big")
    split = db.get(DatasetSplitRevision, scope.split_revision_id)
    split.final_test_digest = f"{row_xor:064x}"
    previous = scope.comparison_policy["evaluation_policy"]
    scope.comparison_policy = {
        **scope.comparison_policy,
        "evaluation_policy": {
            **previous,
            "final_row_digest": split.final_test_digest,
            "final_data": {**previous["final_data"], "objects": entries},
        },
    }
    policy, digest = validate_evaluation_policy(db, scope)
    scope.comparison_policy = {**scope.comparison_policy, "evaluation_policy_digest": digest}
    scope.scope_deadline_at = datetime.now(UTC) + timedelta(minutes=5)
    refit = plan_scope_refit(db, scope.id, store)
    plan_event = db.scalar(
        select(WorkflowEvent).where(
            WorkflowEvent.attempt_id == refit.id, WorkflowEvent.event_key == "refit_plan"
        )
    )
    refit_plan = RefitPlan.model_validate(plan_event.payload)
    refit.status = AttemptStatus.RUNNING
    refit.lease_expires_at = scope.scope_deadline_at
    frozen = FrozenPipeline.model_validate(execute_refit(refit_plan, store))
    publish_frozen_pipeline(db, refit_plan, frozen, fencing_token=refit.fencing_token)
    db.flush()
    with store.open_stream(frozen.frozen_pipeline_uri) as stream:
        objects["/pipeline"] = stream.read()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, self.headers.get("Authorization")))
            if self.path not in objects:
                self.send_error(403)
                return
            self.send_response(200)
            self.end_headers()
            self.wfile.write(objects[self.path])

        def log_message(self, *_):
            pass

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "evaluation-fixture")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "ca.pem", tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert_path, key_path)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"https://127.0.0.1:{server.server_port}"
    manifest = FinalDataManifest(**policy.final_data.model_dump(), scope_id=scope.id)
    mint = Mock(
        side_effect=lambda actual, _expires: [
            f"{base}/{o.key}?versionId={o.version}" for o in actual.objects
        ]
    )
    allocator = authority_options["principals"][0].model_copy(
        update={"project_reference": manifest.project_reference}
    )
    app = create_app(
        **{**authority_options, "principals": [allocator], "manifests": [manifest], "mint": mint}
    )

    @contextmanager
    def factory():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    try:
        with TestClient(
            app, base_url="https://authority.test/", headers={"Authorization": "Bearer allocator"}
        ) as admin:
            k8s = SimpleNamespace(
                settings=SimpleNamespace(training_namespace="qa-evaluation"),
                core=SecretAPI(lambda: None),
            )
            public_key = authority_options["signing_secret"].public_key()
            plan, token, _ = prepare_evaluator_credentials(
                factory, scope.id, k8s, admin, authority_public_key=public_key
            )
            attempt = db.get(WorkflowAttempt, plan.attempt_id)
            attempt.status = AttemptStatus.SUBMITTED
            db.flush()
            monkeypatch.setattr(
                evaluation_access,
                "get_settings",
                lambda: SimpleNamespace(
                    jwt_secret_key="evaluator-worker-fixture-01234567890123456789"
                ),
            )
            control_token = evaluation_access.evaluation_token(attempt, scope.scope_deadline_at)
            monkeypatch.setattr(evaluation_control, "get_session_factory", lambda: factory)
            monkeypatch.setattr(evaluation_control, "get_object_store", lambda: store)
            monkeypatch.setattr(evaluation_control, "authority_public_key", lambda: public_key)
            monkeypatch.setattr(
                evaluation_control,
                "mint_refit_read_url",
                lambda *_: {
                    "url": base + "/pipeline",
                    "expires_at": scope.scope_deadline_at.isoformat(),
                },
            )
            control_app = FastAPI()
            control_app.include_router(evaluation_control.router, prefix="/api/v1")

            def database():
                with factory() as session:
                    yield session

            control_app.dependency_overrides[get_db] = database
            signer = serialization.load_pem_private_key(
                base64.b64decode(k8s.core.secret.data[KEY_FIELD]), password=None
            )
            with TestClient(
                control_app,
                base_url=f"https://app.test/api/v1/internal/evaluations/{attempt.id}/",
                headers={"Authorization": f"Bearer {control_token}"},
            ) as control:
                with TestClient(
                    app,
                    base_url="https://authority.test/",
                    headers={"Authorization": f"Bearer {token}"},
                ) as authority:
                    yield SimpleNamespace(
                        control=control,
                        authority=authority,
                        admin=admin,
                        db=db,
                        factory=factory,
                        scope=scope,
                        attempt=attempt,
                        plan=plan,
                        signer=signer,
                        public_key=public_key,
                        ca=str(cert_path),
                        requests=requests,
                        mint=mint,
                        store=store,
                        objects=objects,
                    )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        with authority_options["session_factory"]() as session, session.begin():
            session.execute(
                delete(FinalTestAllocation).where(
                    FinalTestAllocation.project_reference == manifest.project_reference
                )
            )


def run(case):
    return evaluation_worker.run(
        case.control,
        case.authority,
        signing_key=case.signer,
        authority_public_key=case.public_key,
        ca_file=case.ca,
    )


@pytest.mark.parametrize("lost", [None, "result", "commit", "publish"])
def test_connected_worker_evaluates_once_and_replays_only_acknowledgements(
    worker_case, lost, monkeypatch
):
    case = worker_case
    client, method = (
        (case.authority, "post")
        if lost == "commit"
        else (case.control, "put" if lost == "result" else "post")
    )
    original = getattr(client, method)
    lost_once = False

    def call(path, **kwargs):
        nonlocal lost_once
        response = original(path, **kwargs)
        if lost and path.endswith(lost) and not lost_once and response.status_code == 200:
            lost_once = True
            raise httpx.ReadError("Lost acknowledgement")
        return response

    monkeypatch.setattr(client, method, call)
    result = run(case)
    case.db.refresh(case.scope)
    assert case.scope.status == ScopeStatus.SUCCEEDED
    with case.store.open_stream(result["uri"]) as stream:
        verified = verify_result(stream.read(), case.plan, expected_digest=result["sha256"])
    assert verified.metrics["rmse"] == pytest.approx(0, abs=1e-10)
    case.mint.assert_called_once()
    assert len(case.requests) == 3 and all(auth is None for _, auth in case.requests)
    assert case.requests[0][0] == "/pipeline"
    assert case.control.post("start").status_code == 409


def test_lost_grant_reply_never_requests_or_evaluates_again(worker_case, monkeypatch):
    case = worker_case
    original = case.authority.post

    def lost(path, **kwargs):
        response = original(path, **kwargs)
        if path.endswith("/credentials"):
            assert response.status_code == 200
            raise httpx.ReadError("Grant response lost")
        return response

    monkeypatch.setattr(case.authority, "post", lost)
    with pytest.raises(httpx.ReadError):
        run(case)
    case.mint.assert_called_once()
    assert case.requests == [("/pipeline", None)]
    state = case.admin.get(f"allocations/{case.plan.allocation_id}").json()
    assert state["status"] == "opened"
    assert case.control.post("start").status_code == 409


def test_corrupt_pipeline_fails_before_final_grant(worker_case):
    case = worker_case
    case.objects["/pipeline"] = b"corrupt"
    with pytest.raises(ValueError):
        run(case)
    case.mint.assert_not_called()
    assert case.admin.get(f"allocations/{case.plan.allocation_id}").json()["status"] == "allocated"


@pytest.mark.parametrize("committed", [False, True])
def test_controller_recovers_stored_result_without_worker_or_final_reads(
    worker_case, monkeypatch, committed
):
    case = worker_case
    original = case.authority.post

    def unavailable(path, **kwargs):
        if path.endswith("/commit"):
            if committed:
                assert original(path, **kwargs).status_code == 200
            raise httpx.ReadError("Worker lost contact with authority")
        return original(path, **kwargs)

    monkeypatch.setattr(case.authority, "post", unavailable)
    with pytest.raises(httpx.ReadError):
        run(case)
    case.db.refresh(case.attempt)
    case.attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    case.db.flush()
    reads = list(case.requests)

    def commit(digest):
        response = case.admin.post(
            f"allocations/{case.plan.allocation_id}/recovery/commit",
            json=dict(
                provider_manifest_digest=case.plan.final_manifest.digest,
                expected_cas_version=1,
                evaluator_attempt_id=str(case.plan.attempt_id),
                frozen_pipeline_digest=case.plan.frozen_pipeline.sha256,
                generation=case.attempt.generation,
                result_digest=digest,
            ),
        )
        response.raise_for_status()
        return response.json()

    result = recover_evaluation_result(
        case.factory,
        case.plan,
        case.attempt.fencing_token,
        case.store,
        commit=commit,
        authority_public_key=case.public_key,
    )
    case.db.refresh(case.scope)
    assert case.scope.status == ScopeStatus.SUCCEEDED
    assert (
        result["sha256"]
        == case.admin.get(f"allocations/{case.plan.allocation_id}").json()["result_digest"]
    )
    assert case.requests == reads
    case.mint.assert_called_once()
