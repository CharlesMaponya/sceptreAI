from __future__ import annotations

import hashlib
import io
import ipaddress
import ssl
import uuid
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import refit_control
from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowEvent
from automl_api.security.tokens import TokenError, decode_token
from automl_api.services import refit_access
from automl_api.training import refit_worker
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

pytest_plugins = ["test_champion_refit_publication"]
SECRET = "refit-control-test-signing-key-0123456789"


@pytest.fixture
def control_case(durable_refit, refit_case, tmp_path, monkeypatch):
    db, plan, _, attempt, scope = durable_refit
    store, _ = refit_case
    attempt.status = AttemptStatus.SUBMITTED
    db.flush()
    monkeypatch.setattr(
        refit_access, "get_settings", lambda: SimpleNamespace(jwt_secret_key=SECRET),
    )
    token = refit_access.refit_token(attempt, scope.scope_deadline_at)
    monkeypatch.setattr(refit_control, "get_object_store", lambda: store)
    objects = {}
    paths = {}
    for item in (plan.candidate, *plan.partitions):
        path = "/" + uuid.uuid4().hex
        paths[item.uri] = path
        with store.open_stream(item.uri) as source:
            objects[path] = source.read()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in objects:
                self.send_error(403)
                return
            payload = objects[self.path]
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "refit-input-test")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
            .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
            .add_extension(x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
            ]), critical=False)
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .sign(key, hashes.SHA256()))
    cert_path, key_path = tmp_path / "ca.pem", tmp_path / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                                          serialization.PrivateFormat.PKCS8,
                                          serialization.NoEncryption()))
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def mint(_store, item, deadline):
        assert _store is store and item.uri in paths
        assert 0 < (deadline - datetime.now(UTC)).total_seconds() <= 90
        return {"url": f"https://127.0.0.1:{server.server_port}{paths[item.uri]}",
                "expires_at": deadline.isoformat()}

    monkeypatch.setattr(refit_control, "mint_refit_read_url", mint)
    app = FastAPI()
    app.include_router(refit_control.router, prefix="/api/v1")

    def database():
        with Session(bind=db.get_bind(), join_transaction_mode="create_savepoint") as session:
            yield session

    app.dependency_overrides[get_db] = database
    try:
        with TestClient(app, base_url=f"https://testserver/api/v1/internal/refits/{attempt.id}/",
                        headers={"Authorization": f"Bearer {token}"}) as client:
            yield client, db, store, plan, attempt, scope, str(cert_path), token
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_credential_free_worker_fits_uploads_and_replays_publication(control_case, monkeypatch):
    client, db, _, plan, attempt, _, ca, token = control_case
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(TokenError):
        decode_token(token, secret=SECRET, expected_type="access")
    result = refit_worker.run(client, ca_file=ca)
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.SUCCEEDED
    assert result["row_count"] == sum(part.rows for part in plan.partitions)
    assert client.post("publish", json=result).status_code == 200
    assert client.post("start").status_code == 409
    assert client.get("inputs/0").status_code == 409
    events = list(db.scalars(select(WorkflowEvent).where(
        WorkflowEvent.attempt_id == attempt.id,
    ).order_by(WorkflowEvent.sequence)))
    assert [event.event_key for event in events] == ["refit_plan", "refit_output", "refit_frozen"]


def test_input_capability_requires_current_attempt_and_cannot_name_arbitrary_objects(control_case):
    client, db, _, _, attempt, _, _, _ = control_case
    assert client.post("start").status_code == 200
    grant = client.get("inputs/0")
    assert grant.status_code == 200 and grant.headers["cache-control"] == "no-store"
    assert client.get("inputs/-1").status_code == 404
    assert client.get("inputs/3").status_code == 404
    assert client.get(
        f"https://testserver/api/v1/internal/refits/{uuid.uuid4()}/inputs/0",
    ).status_code == 401
    db.refresh(attempt)
    attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    db.flush()
    assert client.get("inputs/0").status_code == 409
    assert client.post("heartbeat").status_code == 409


def test_output_intent_precedes_storage_and_rejects_wrong_or_changed_bytes(control_case):
    client, db, store, plan, attempt, _, _, _ = control_case
    assert client.post("start").status_code == 200
    content = b"first model"
    digest = hashlib.sha256(content).hexdigest()
    assert client.put(f"model/{digest}", content=b"wrong bytes").status_code == 422
    intent = db.scalar(select(WorkflowEvent).where(
        WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "refit_output",
    ))
    assert intent is not None
    assert not store.exists(intent.payload["uri"])
    assert client.put(f"model/{digest}", content=content).status_code == 200
    assert client.put(f"model/{'d' * 64}", content=content).status_code == 409
    result = {"frozen_pipeline_uri": intent.payload["uri"], "frozen_pipeline_digest": digest,
              "refit_policy_digest": plan.policy_digest, "row_count": 10,
              "byte_size": len(content)}
    store.put_stream(intent.payload["uri"], io.BytesIO(b"corrupt"))
    assert client.post("publish", json=result).status_code == 409


def test_worker_failure_seals_attempt_for_reconciler_and_never_returns_secret_errors(control_case):
    client, db, _, _, attempt, _, _, _ = control_case
    assert client.post("start").status_code == 200
    assert client.post("heartbeat").json() == {"renewed": True}
    assert client.post("fail").status_code == 200
    assert client.post("fail").status_code == 200
    assert client.get("inputs/1").status_code == 409
    db.refresh(attempt)
    assert attempt.status == AttemptStatus.FAILED
    assert attempt.terminal_reason == "Refit worker reported execution failure"


@pytest.mark.parametrize("invalid", ["cancelled", "fence", "deadline", "signature"])
def test_replaced_cancelled_or_invalid_capabilities_cannot_read(control_case, invalid):
    from automl_api.models.enums import ScopeStatus

    client, db, _, _, attempt, scope, _, token = control_case
    assert client.post("start").status_code == 200
    db.refresh(attempt)
    if invalid == "cancelled":
        scope.status = ScopeStatus.CANCELLED
    elif invalid == "fence":
        attempt.fencing_token = uuid.uuid4().hex
    elif invalid == "deadline":
        scope.scope_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        client.headers["Authorization"] = f"Bearer {token}.invalid"
    db.flush()
    assert client.get("inputs/0").status_code == (401 if invalid == "signature" else 409)


def test_oversized_upload_is_rejected_before_storage_or_output_intent(control_case):
    client, db, _, plan, attempt, _, _, _ = control_case
    assert client.post("start").status_code == 200
    response = client.put(f"model/{'a' * 64}", content=b"x",
                          headers={"Content-Length": str(plan.max_model_bytes + 1)})
    assert response.status_code == 413
    assert db.scalar(select(WorkflowEvent).where(
        WorkflowEvent.attempt_id == attempt.id, WorkflowEvent.event_key == "refit_output",
    )) is None


def test_mint_failure_does_not_disclose_provider_capabilities(control_case, monkeypatch):
    client, *_ = control_case
    assert client.post("start").status_code == 200

    def failed_mint(*_args):
        raise RuntimeError("https://provider.example/object?secret=do-not-disclose")

    monkeypatch.setattr(refit_control, "mint_refit_read_url", failed_mint)
    response = client.get("inputs/0")
    assert response.status_code == 503
    assert "do-not-disclose" not in response.text


def test_native_s3_grant_is_exact_version_get_only(refit_case):
    from automl_api.storage.s3 import S3ObjectStoreDriver

    _, plan = refit_case
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 100, "VersionId": "version-1"}
    client.generate_presigned_url.return_value = "https://objects.example/input?signature=opaque"
    store = S3ObjectStoreDriver(bucket="test", client=client)
    item = SimpleNamespace(uri="s3://test/train/input.parquet", byte_size=100)
    grant = refit_access.mint_refit_read_url(store, item, datetime.now(UTC) + timedelta(seconds=60))
    assert grant["url"].startswith("https://objects.example/")
    call = client.generate_presigned_url.call_args
    assert call.args == ("get_object",)
    assert call.kwargs["HttpMethod"] == "GET"
    assert call.kwargs["Params"] == {
        "Bucket": "test", "Key": "train/input.parquet", "VersionId": "version-1",
    }
    assert 0 < call.kwargs["ExpiresIn"] <= 60
    client.generate_presigned_url.return_value = "http://objects.example/input"
    with pytest.raises(ValueError, match="HTTPS"):
        refit_access.mint_refit_read_url(store, item, datetime.now(UTC) + timedelta(seconds=60))
