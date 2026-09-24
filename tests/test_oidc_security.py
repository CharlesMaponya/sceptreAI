from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import jwt
import pytest
from automl_api.core.config import Settings
from automl_api.schemas.auth import TokenPair
from automl_api.security import browser_sessions, oidc
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, Request, Response


@pytest.fixture
def identity(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client = SimpleNamespace(
        get_signing_key_from_jwt=lambda token: SimpleNamespace(key=key.public_key())
    )
    monkeypatch.setattr(oidc, "jwks_client", lambda uri: client)
    settings = Settings(oidc_issuer="https://issuer.example", oidc_client_id="sceptre")
    now = datetime.now(UTC)
    claims = {
        "iss": settings.oidc_issuer,
        "aud": settings.oidc_client_id,
        "sub": "provider-user",
        "iat": now,
        "exp": now + timedelta(minutes=5),
        "nonce": "nonce",
        "email": "USER@example.test",
        "email_verified": True,
        "amr": ["mfa"],
    }
    return key, settings, claims


def verify(identity, updates):
    key, settings, claims = identity
    token = jwt.encode({**claims, **updates}, key, algorithm="RS256")
    return oidc.verify_identity(
        token, "nonce", {"jwks_uri": "https://issuer.example/keys"}, settings
    )


def test_signed_identity_accepts_verified_mfa_and_normalizes_email(identity):
    assert verify(identity, {})["email"] == "user@example.test"
    assert (
        verify(identity, {"aud": ["sceptre", "second"], "azp": "sceptre"})["sub"] == "provider-user"
    )


@pytest.mark.parametrize(
    "claims",
    [
        {"iss": "https://other.example"},
        {"aud": "other"},
        {"nonce": "wrong"},
        {"exp": datetime.now(UTC) - timedelta(minutes=1)},
        {"aud": ["sceptre", "other"]},
        {"azp": "wrong"},
        {"sub": ""},
        {"sub": "x" * 513},
        {"email_verified": False},
        {"email_verified": "true"},
        {"email": "bad"},
        {"amr": []},
        {"amr": "not_mfa"},
        {"amr": None},
    ],
)
def test_signed_but_invalid_identity_is_rejected(identity, claims):
    with pytest.raises((ValueError, jwt.PyJWTError)):
        verify(identity, claims)


def test_invalid_signature_is_rejected(identity):
    key, settings, claims = identity
    attacker = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = jwt.encode(claims, attacker, algorithm="RS256")
    with pytest.raises(jwt.InvalidSignatureError):
        oidc.verify_identity(token, "nonce", {"jwks_uri": "keys"}, settings)


@pytest.mark.parametrize(
    "environment,endpoint,accepted",
    [
        ("local", "http://issuer.example/authorize", True),
        ("production", "https://issuer.example/authorize", True),
        ("production", "http://issuer.example/authorize", False),
        ("local", "http://different.example/authorize", False),
        ("local", "file:///etc/passwd", False),
        ("local", "https://user@issuer.example/authorize", False),
        ("local", "https://issuer.example/authorize#fragment", False),
    ],
)
def test_discovery_enforces_endpoint_boundaries(monkeypatch, environment, endpoint, accepted):
    settings = Settings(
        environment=environment, oidc_issuer="http://issuer.example", oidc_client_id="app"
    )
    metadata = {
        "issuer": settings.oidc_issuer,
        "authorization_endpoint": endpoint,
        "token_endpoint": "https://issuer.example/token",
        "jwks_uri": "https://issuer.example/keys",
    }
    response = MagicMock()
    response.json.return_value = metadata
    monkeypatch.setattr(oidc.httpx, "get", lambda *args, **kwargs: response)
    if accepted:
        assert oidc.discovery(settings) == metadata
    else:
        with pytest.raises(ValueError):
            oidc.discovery(settings)


def test_discovery_disabled_and_wrong_issuer_fail_closed(monkeypatch):
    with pytest.raises(HTTPException, match="not configured"):
        oidc.discovery(Settings())
    response = MagicMock()
    response.json.return_value = {"issuer": "https://wrong.example"}
    monkeypatch.setattr(oidc.httpx, "get", lambda *args, **kwargs: response)
    with pytest.raises(ValueError, match="issuer mismatch"):
        oidc.discovery(Settings(oidc_issuer="https://expected.example", oidc_client_id="app"))


def test_provider_identity_never_automatically_links_an_existing_email():
    claims = {"iss": "issuer", "sub": "subject", "email": "same@example.test"}
    db = MagicMock()
    db.scalar.side_effect = [None, object()]
    with pytest.raises(HTTPException) as error:
        oidc.identity_user(db, claims)
    assert error.value.status_code == 409
    db.add.assert_not_called()


def test_provider_identity_creates_a_verified_user_and_honors_disabled_accounts():
    claims = {"iss": "issuer", "sub": "subject", "email": "new@example.test", "name": "Name"}
    db = MagicMock()
    db.scalar.side_effect = [None, None]
    db.flush.side_effect = lambda: setattr(db.add.call_args.args[0], "is_active", True)
    user = oidc.identity_user(db, claims)
    assert user.is_verified is True
    assert user.sso_issuer == "issuer" and user.sso_subject == "subject"
    db.scalar.side_effect = [user]
    assert oidc.identity_user(db, claims) is user
    user.is_active = False
    db.scalar.side_effect = [user]
    with pytest.raises(HTTPException) as error:
        oidc.identity_user(db, claims)
    assert error.value.status_code == 403


@pytest.mark.parametrize(
    "method,origin,allowed",
    [
        ("GET", None, True),
        ("HEAD", None, True),
        ("OPTIONS", None, True),
        ("POST", "https://app.example", True),
        ("PATCH", "https://app.example", True),
        ("POST", "https://attacker.example", False),
        ("DELETE", None, False),
    ],
)
def test_cookie_authenticated_mutations_require_same_origin(monkeypatch, method, origin, allowed):
    monkeypatch.setattr(
        browser_sessions, "get_settings", lambda: Settings(public_app_url="https://app.example")
    )
    request = Request(
        {
            "type": "http",
            "method": method,
            "headers": [(b"origin", origin.encode())] if origin else [],
        }
    )
    if allowed:
        browser_sessions.require_same_origin(request)
    else:
        with pytest.raises(HTTPException) as error:
            browser_sessions.require_same_origin(request)
        assert error.value.status_code == 403


def test_browser_session_cookies_are_http_only_scoped_and_secure(monkeypatch):
    monkeypatch.setattr(
        browser_sessions, "get_settings", lambda: Settings(public_app_url="https://app.example")
    )
    response = Response()
    browser_sessions.set_browser_session(
        response, TokenPair(access_token="access", refresh_token="refresh", expires_in=900)
    )
    cookies = response.headers.getlist("set-cookie")
    assert len(cookies) == 2
    assert all(
        "HttpOnly" in value and "Secure" in value and "SameSite=lax" in value for value in cookies
    )
    assert "Path=/api/v1/auth" in cookies[1]
    assert response.headers["cache-control"] == "no-store"
    cleared = Response()
    browser_sessions.clear_browser_session(cleared)
    assert all("Max-Age=0" in value for value in cleared.headers.getlist("set-cookie"))
