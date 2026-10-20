"""Pod-bound HTTP grant handoff uses factory-owned transactions and no-store replies."""

import uuid

import pytest
from automl_api.api.routes import attempt_control
from automl_api.db.session import get_session_factory
from automl_api.models.workflows import WorkflowAttemptCredentialGrant
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_attempt_object_grants import grant_case as grant_case  # noqa: F401
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def rpc(grant_case, monkeypatch):
    c = grant_case
    c.k8s._configured = True
    app = FastAPI()
    app.include_router(attempt_control.router)
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    monkeypatch.setattr(attempt_control, "KubernetesTrainingClient", lambda: c.k8s)
    monkeypatch.setattr(attempt_control, "get_object_store", lambda: c.capability_store)
    with TestClient(app) as http:
        c.path = f"/internal/attempts/{c.attempt_id}/objects/read"
        c.headers = {"Authorization": f"Bearer {c.token}"}
        yield http, c, app


def test_http_reply_is_uncached_and_replay_returns_only_a_receipt(rpc):
    http, c, _ = rpc
    body = c.request.model_dump(mode="json")
    response = http.post(c.path, headers=c.headers, json=body)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    receipt = response.json()
    assert receipt["url"].startswith("https://objects.invalid/")
    replay = http.post(c.path, headers=c.headers, json=body)
    assert replay.status_code == 200 and replay.json()["url"] is None
    assert replay.json()["grant_id"] == receipt["grant_id"]
    assert replay.json()["expires_at"] == receipt["expires_at"]
    c.signer.assert_called_once()
    with c.factory() as db:
        row = db.get(WorkflowAttemptCredentialGrant, uuid.UUID(receipt["grant_id"]))
        assert row.status == "issued"
        assert "private" not in str(vars(row)) and c.token not in str(vars(row))


@pytest.mark.parametrize(
    "update",
    [
        {"role": "raw"},
        {"role": "final_input"},
        {"role": "final_label"},
        {"method": "PUT"},
        {"object_uri": "s3c://safe/raw"},
        {"object_version": "other"},
        {"object_index": True},
        {"object_index": -1},
    ],
)
def test_untrusted_http_fields_are_rejected_before_identity_or_storage(rpc, update):
    http, c, _ = rpc
    body = {**c.request.model_dump(mode="json"), **update}
    assert http.post(c.path, headers=c.headers, json=body).status_code == 422
    c.auth.create_token_review.assert_not_called()
    c.signer.assert_not_called()


def test_http_rejects_missing_wrong_or_unavailable_identity_without_grants(rpc):
    http, c, _ = rpc
    body = c.request.model_dump(mode="json")
    assert http.post(c.path, json=body).status_code == 401
    assert (
        http.post(c.path, headers={"Authorization": "Basic password"}, json=body).status_code == 401
    )
    c.auth.create_token_review.assert_not_called()
    c.auth.create_token_review.return_value.status.authenticated = False
    assert http.post(c.path, headers=c.headers, json=body).status_code == 401
    c.k8s._configured = False
    assert http.post(c.path, headers=c.headers, json=body).status_code == 503
    c.signer.assert_not_called()
    with c.factory() as db:
        assert (
            list(
                db.scalars(
                    select(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.attempt_id == c.attempt_id
                    )
                )
            )
            == []
        )


def test_http_database_or_provider_outage_never_discloses_or_reissues(rpc):
    http, c, app = rpc
    body = c.request.model_dump(mode="json")

    def unavailable():
        raise OSError("Authorization database unavailable")

    app.dependency_overrides[get_session_factory] = lambda: unavailable
    assert http.post(c.path, headers=c.headers, json=body).status_code == 503
    c.signer.assert_not_called()
    app.dependency_overrides[get_session_factory] = lambda: c.factory
    c.signer.side_effect = OSError("https://objects.invalid/?private-provider-secret")
    response = http.post(c.path, headers=c.headers, json=body)
    assert response.status_code == 503 and "private-provider-secret" not in response.text
    assert http.post(c.path, headers=c.headers, json=body).status_code == 401
    c.signer.assert_called_once()
