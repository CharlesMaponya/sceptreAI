from __future__ import annotations

import hashlib
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import auth, serving_artifacts
from automl_api.core.config import Settings
from automl_api.inference import app as inference
from automl_api.models.enums import RunKind, RunStatus
from automl_api.schemas.auth import PasswordResetRequest
from automl_api.security.deployments import deployment_token
from fastapi import HTTPException
from fastapi.testclient import TestClient


def test_password_reset_never_exposes_token(monkeypatch):
    monkeypatch.setattr(auth, "get_settings", lambda: Settings(environment="local"))
    monkeypatch.setattr(auth, "create_password_reset_token", lambda *_: "private-reset-token")
    sender = MagicMock(return_value=False)
    monkeypatch.setattr(auth, "send_password_reset_email", sender)
    response = auth.request_password_reset(
        PasswordResetRequest(email="owner@example.test"), MagicMock()
    )
    assert "private-reset-token" not in response.model_dump_json()
    sender.assert_called_once_with("owner@example.test", "private-reset-token")


def test_disabled_password_auth_fails_before_account_lookup(monkeypatch):
    monkeypatch.setattr(auth, "get_settings", lambda: Settings(simple_auth_enabled=False))
    lookup = MagicMock()
    monkeypatch.setattr(auth, "authenticate_user", lookup)
    with pytest.raises(HTTPException) as error:
        auth.login(SimpleNamespace(), SimpleNamespace(), MagicMock())
    assert error.value.status_code == 403
    lookup.assert_not_called()


def test_inference_requires_deployment_credential_for_predictions_and_docs(monkeypatch):
    monkeypatch.setenv("INFERENCE_GATEWAY_TOKEN", "scoped-credential")
    client = TestClient(inference.create_app())
    assert client.get("/health/live").status_code == 200
    for path in ("/docs", "/openapi.json", "/v1/model"):
        assert client.get(path).status_code == 401
    assert client.post("/v1/predict", json={"records": [{"x": 1}]}).status_code == 401
    assert (
        client.get(
            "/openapi.json", headers={"X-Sceptre-Deployment-Token": "scoped-credential"}
        ).status_code
        == 200
    )


def test_model_digest_is_checked_before_deserialization(monkeypatch):
    inference._load_model.cache_clear()
    monkeypatch.setenv("MODEL_URI", "s3://models/one.joblib")
    monkeypatch.delenv("MODEL_DOWNLOAD_URL", raising=False)
    payload = b"untrusted serialized bytes"
    monkeypatch.setattr(
        inference, "get_object_store", lambda: SimpleNamespace(read_bytes=lambda _: payload)
    )
    load = MagicMock(return_value=object())
    monkeypatch.setattr(inference.joblib, "load", load)
    monkeypatch.setenv("MODEL_SHA256", "0" * 64)
    with pytest.raises(RuntimeError, match="integrity"):
        inference._load_model()
    load.assert_not_called()
    monkeypatch.setenv("MODEL_SHA256", hashlib.sha256(payload).hexdigest())
    assert inference._load_model() is load.return_value
    load.assert_called_once()
    inference._load_model.cache_clear()


def test_model_download_credential_and_project_are_scoped():
    deployment_id = uuid.uuid4()
    other_id = uuid.uuid4()
    db = MagicMock()
    with pytest.raises(HTTPException) as error:
        serving_artifacts.download_deployment_model(deployment_id, db, deployment_token(other_id))
    assert error.value.status_code == 401
    db.get.assert_not_called()
    db.get.side_effect = [
        SimpleNamespace(
            run_kind=RunKind.DEPLOYMENT,
            status=RunStatus.RUNNING,
            project_id=uuid.uuid4(),
            tags={"model_artifact_id": str(uuid.uuid4())},
        ),
        SimpleNamespace(project_id=uuid.uuid4()),
    ]
    with pytest.raises(HTTPException) as error:
        serving_artifacts.download_deployment_model(
            deployment_id, db, deployment_token(deployment_id)
        )
    assert error.value.status_code == 404
