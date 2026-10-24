"""Joined broker fixtures retain real TLS object transport and committed PostgreSQL."""

import hashlib
import math
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.orm import Session


def install_native_output(monkeypatch, case, backend):
    """Native SDKs are qualified separately; joined tests retain real stored bytes."""
    from automl_api.services import attempt_output_grants
    from automl_api.storage.contracts import ObjectMetadata

    calls = []

    def write(store, uri, source, *, byte_size, sha256, grant_digest):
        assert case.engine.pool.checkedout() == 0
        assert store is case.store
        calls.append((uri, grant_digest))
        if backend.exists(uri):
            raise FileExistsError("Conditional object already exists")
        source.seek(0)
        result = backend.put_stream(uri, source)
        with backend.open_stream(uri) as verified:
            assert hashlib.sha256(verified.read()).hexdigest() == sha256
        assert result.uri == uri and result.byte_size == byte_size
        return ObjectMetadata(
            uri=uri, byte_size=byte_size, provider_headers={"version_id": "native-v1"}
        )

    monkeypatch.setattr(attempt_output_grants, "write_attempt_output", write)
    return calls


def install_champion_broker(db, plan, route, claims, engine, monkeypatch, tmp_path, *, mint):
    from automl_api.api.routes import attempt_control
    from automl_api.db.session import get_session_factory
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from automl_api.models.workflows import WorkflowAttemptCredentialGrant
    from automl_api.services import attempt_object_grants as grants
    from automl_api.training.attempt_rpc import AttemptRPC
    from test_champion_read_broker import champion

    owner = db.get(Project, plan.project_id).owner_id
    connection = db.get_bind()
    db.commit()
    connection.commit()
    # This is the same committed, independently connected server fixture used by
    # the broker's authority/UID/expiry tests, rather than an alternate issuer.
    binding = champion.__wrapped__((engine, route, claims, plan, type(plan)), monkeypatch)
    c = next(binding)
    c.native._configured = True
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.native)
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: c.store)
    token_file = Path(tmp_path) / ("broker-token-" + plan.attempt_id.hex)
    token_file.write_text(c.token)
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(token_file))
    monkeypatch.setenv(
        "ATTEMPT_CONTROL_BASE_URL", "https://broker.fixture/api/v1/internal/attempts"
    )
    backend = route.get_object_store()
    writes = install_native_output(monkeypatch, c, backend)

    def issue(store, item, deadline, *, object_version):
        assert object_version == "native-v1"
        value = mint(backend, item, deadline)
        parts = urlsplit(value["url"])
        query = dict(parse_qsl(parts.query))
        query.update(
            {
                "X-Amz-Date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                "X-Amz-Expires": str(
                    min(30, math.floor((deadline - datetime.now(UTC)).total_seconds()))
                ),
                "versionId": object_version,
            }
        )
        return {
            "url": urlunsplit(parts._replace(query=urlencode(query))),
            "expires_at": deadline.isoformat(),
        }

    monkeypatch.setattr(grants, "mint_refit_read_url", issue)
    app = FastAPI()
    app.include_router(attempt_control.router, prefix="/api/v1")
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    broker = TestClient(app, base_url="https://broker.fixture")
    broker.__enter__()
    actual_client, rpc_request = httpx.Client, AttemptRPC.request
    errors = []

    def transport(request):
        assert request.url.host == "broker.fixture"
        response = broker.request(
            request.method, str(request.url), headers=dict(request.headers), content=request.content
        )
        if response.status_code >= 400:
            errors.append((request.method, request.url.path, response.status_code, response.text))
        return httpx.Response(
            response.status_code, content=response.content, headers=dict(response.headers)
        )

    def rpc(self, *args, **kwargs):
        # Only the control socket is substituted. Object GETs use the real TLS
        # client, preserving certificate and ambient-proxy regression checks.
        with monkeypatch.context() as scoped:
            scoped.setattr(
                httpx,
                "Client",
                lambda **options: actual_client(
                    **options, transport=httpx.MockTransport(transport)
                ),
            )
            return rpc_request(self, *args, **kwargs)

    monkeypatch.setattr(AttemptRPC, "request", rpc)

    def upload(payload, *, digest=None, request_id=None, headers=None):
        role = "pipeline" if c.stage == "refit" else "result"
        digest = digest or hashlib.sha256(payload).hexdigest()
        nonce = request_id or uuid.uuid4()
        return broker.put(
            f"/api/v1/internal/attempts/{plan.attempt_id}/objects/{role}/{digest}",
            content=payload,
            headers={
                "Authorization": "Bearer " + c.token,
                "X-Request-ID": str(nonce),
                **(headers or {}),
            },
        )

    def close():
        if errors:
            print("Broker denial details:", errors)
        broker.__exit__(None, None, None)
        db.close()
        try:
            next(binding)
        except StopIteration:
            pass
        with Session(engine) as cleanup, cleanup.begin():
            cleanup.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.project_id == plan.project_id
                )
            )
            cleanup.execute(delete(Project).where(Project.id == plan.project_id))
            cleanup.execute(delete(User).where(User.id == owner))

    return SimpleNamespace(
        close=close, case=c, broker=broker, upload=upload, writes=writes, errors=errors
    )
