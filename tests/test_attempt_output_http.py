"""The pod-bound output path shares committed admission with safe-object reads."""

import hashlib
import uuid
from types import SimpleNamespace

import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from champion_broker_fixture import install_native_output
from fastapi import FastAPI
from fastapi.testclient import TestClient
from test_champion_read_broker import champion as champion
from test_champion_read_broker import compatible_uri_fixture as compatible_uri_fixture
from test_champion_read_broker import evaluation_control_case as evaluation_control_case
from test_champion_read_broker import grant_case as grant_case
from test_champion_read_broker import handoff_case as handoff_case
from test_champion_read_broker import key_case as key_case
from test_champion_read_broker import refit_control_case as refit_control_case

pytest_plugins = [
    "test_champion_refit_publication",
    "test_champion_evaluation_planning",
    "test_qualification_control",
]


@pytest.fixture
def output_http(champion, monkeypatch, request):
    c = champion
    original = request.getfixturevalue(c.stage + "_control_case")
    if c.stage == "refit":
        backend = original[2]
    else:
        from automl_api.api.routes import evaluation_control

        backend = evaluation_control.get_object_store()
    writes = install_native_output(monkeypatch, c, backend)
    c.native._configured = True
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.native)
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: c.store)
    app = FastAPI()
    app.include_router(attempt_control.router, prefix="/api/v1")
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    headers = {"Authorization": "Bearer " + c.token}
    base = f"/api/v1/internal/attempts/{c.plan.attempt_id}/objects"
    with TestClient(app) as client:
        payload = b"declared-output" if c.stage == "refit" else original[5]
        nonce = uuid.uuid4()
        yield SimpleNamespace(
            case=c,
            client=client,
            base=base,
            headers=headers,
            payload=payload,
            digest=hashlib.sha256(payload).hexdigest(),
            role="pipeline" if c.stage == "refit" else "result",
            nonce=nonce,
            writes=writes,
            backend=backend,
        )


def upload(c, *, payload=None, nonce=None, digest=None, role=None, headers=None):
    return c.client.put(
        f"{c.base}/{role or c.role}/{digest or c.digest}",
        content=c.payload if payload is None else payload,
        headers={**c.headers, "X-Request-ID": str(nonce or c.nonce), **(headers or {})},
    )


def test_worker_output_route_shares_the_live_read_admission(output_http):
    c = output_http
    read = c.client.post(
        c.base + "/read",
        json={**c.case.request, "request_id": str(uuid.uuid4())},
        headers=c.headers,
    )
    assert read.status_code == 200
    uploaded = upload(c)
    assert uploaded.status_code == 200, uploaded.text
    assert (
        uploaded.json()["sha256"] == c.digest and uploaded.json()["object_version"] == "native-v1"
    )
    assert uploaded.headers["cache-control"] == "no-store"
    assert upload(c).json() == uploaded.json()
    receipt = c.client.get(c.base + f"/receipts/{c.nonce}", headers=c.headers)
    assert receipt.status_code == 200 and receipt.json() == uploaded.json()
    assert len(c.writes) == 1
