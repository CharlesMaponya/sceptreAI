"""Exercise browser OIDC state, PKCE exchange, and provider failures."""

import base64
import hashlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from automl_api.api.routes import oidc
from automl_api.core.config import Settings
from automl_api.db.session import get_db
from automl_api.schemas.auth import TokenPair
from automl_api.security import browser_sessions
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def browser(monkeypatch):
    settings = Settings(
        oidc_issuer="https://identity.test",
        oidc_client_id="sceptre",
        oidc_client_secret="local-test-secret",
        public_app_url="https://app.test",
    )
    metadata = {
        "authorization_endpoint": "https://identity.test/authorize?organization=test",
        "token_endpoint": "https://identity.test/token",
        "jwks_uri": "https://identity.test/keys",
    }
    monkeypatch.setattr(oidc, "get_settings", lambda: settings)
    monkeypatch.setattr(browser_sessions, "get_settings", lambda: settings)
    monkeypatch.setattr(oidc, "discovery", lambda _: metadata)
    exchange = MagicMock()
    exchange.json.return_value = {"id_token": "signed-provider-token"}
    post = MagicMock(return_value=exchange)
    monkeypatch.setattr(oidc.httpx, "post", post)
    verify = MagicMock(return_value={"sub": "identity-1"})
    monkeypatch.setattr(oidc, "verify_identity", verify)
    monkeypatch.setattr(oidc, "identity_user", lambda *args: SimpleNamespace(id="user"))
    monkeypatch.setattr(
        oidc,
        "issue_token_pair",
        lambda *a, **k: TokenPair(access_token="access", refresh_token="refresh", expires_in=900),
    )
    db = MagicMock()
    app = FastAPI()
    app.include_router(oidc.router, prefix="/api/v1")
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app, base_url="https://app.test", follow_redirects=False) as client:
        yield client, settings, metadata, db, post, verify


def start(client):
    response = client.get("/api/v1/auth/oidc/login")
    assert response.status_code == 303
    return parse_qs(urlparse(response.headers["location"]).query)


@pytest.mark.parametrize("method", ["basic", "post", "public"])
def test_callback_exchanges_pkce_code_and_sets_scoped_browser_cookies(browser, monkeypatch, method):
    client, settings, metadata, db, post, verify = browser
    if method == "post":
        metadata["token_endpoint_auth_methods_supported"] = ["client_secret_post"]
    elif method == "public":
        monkeypatch.setattr(
            oidc, "get_settings", lambda: replace(settings, oidc_client_secret=None)
        )
    query = start(client)
    assert query["organization"] == ["test"]
    assert query["code_challenge_method"] == ["S256"]
    response = client.get(
        "/api/v1/auth/oidc/callback", params={"code": "one-use-code", "state": query["state"][0]}
    )
    assert response.status_code == 303
    assert response.headers["location"] == "https://app.test/auth?session=browser"
    db.commit.assert_called_once()
    db.rollback.assert_not_called()
    data = post.call_args.kwargs["data"]
    assert data["code"] == "one-use-code"
    assert data["redirect_uri"] == "https://app.test/api/v1/auth/oidc/callback"
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(data["code_verifier"].encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    assert [challenge] == query["code_challenge"]
    assert verify.call_args.args[1] == query["nonce"][0]
    assert bool(post.call_args.kwargs["auth"]) is (method == "basic")
    assert ("client_secret" in data) is (method == "post")
    cookies = response.headers.get_list("set-cookie")
    assert any(
        "sceptre_access=" in value and "HttpOnly" in value and "Secure" in value
        for value in cookies
    )
    assert any("Max-Age=0" in value and oidc.STATE_COOKIE in value for value in cookies)


@pytest.mark.parametrize(
    "failure", ["state", "code", "cookie", "provider", "identity", "method", "duplicate"]
)
def test_invalid_oidc_callback_never_creates_a_browser_session(browser, monkeypatch, failure):
    client, settings, metadata, db, post, verify = browser
    query = start(client)
    code, state = "one-use-code", query["state"][0]
    if failure == "state":
        state = "forged"
    elif failure == "code":
        code = ""
    elif failure == "cookie":
        client.cookies.clear()
    elif failure == "provider":
        post.side_effect = httpx.ConnectError("provider unavailable")
    elif failure == "identity":
        verify.side_effect = ValueError("invalid identity")
    elif failure == "method":
        metadata["token_endpoint_auth_methods_supported"] = ["private_key_jwt"]
    else:
        monkeypatch.setattr(
            oidc,
            "identity_user",
            MagicMock(side_effect=IntegrityError("identity", {}, Exception("duplicate"))),
        )
    response = client.get("/api/v1/auth/oidc/callback", params={"code": code, "state": state})
    assert response.status_code == (409 if failure == "duplicate" else 400)
    db.commit.assert_not_called()
    db.rollback.assert_called_once()
    assert not response.headers.get_list("set-cookie")
    if failure in {"state", "code", "cookie", "method"}:
        post.assert_not_called()


def test_oidc_configuration_and_unavailable_discovery(browser, monkeypatch):
    client, settings, _, _, _, _ = browser
    assert client.get("/api/v1/auth/configuration").json() == {
        "password_enabled": True,
        "oidc_enabled": True,
    }
    monkeypatch.setattr(oidc, "get_settings", lambda: replace(settings, oidc_client_id=None))
    assert client.get("/api/v1/auth/configuration").json()["oidc_enabled"] is False
    monkeypatch.setattr(oidc, "discovery", MagicMock(side_effect=ValueError("bad metadata")))
    response = client.get("/api/v1/auth/oidc/login")
    assert response.status_code == 503
    assert not response.headers.get_list("set-cookie")
