from __future__ import annotations

from datetime import timedelta

from automl_api.core.config import Settings
from automl_api.security.passwords import hash_password, verify_password
from automl_api.security.tokens import TokenError, create_signed_token, decode_token


def test_password_hash_round_trip() -> None:
    password_hash = hash_password("correct horse battery staple")

    assert verify_password("correct horse battery staple", password_hash)
    assert not verify_password("wrong password", password_hash)


def test_signed_token_round_trip() -> None:
    token = create_signed_token(
        subject="user-1",
        email="user@example.com",
        token_version=1,
        secret="test-secret",
        token_type="access",
        expires_delta=timedelta(minutes=5),
    )

    payload = decode_token(token, secret="test-secret", expected_type="access")

    assert payload["sub"] == "user-1"
    assert payload["email"] == "user@example.com"
    assert payload["ver"] == 1


def test_signed_token_rejects_wrong_secret() -> None:
    token = create_signed_token(
        subject="user-1",
        email="user@example.com",
        token_version=1,
        secret="test-secret",
        token_type="access",
        expires_delta=timedelta(minutes=5),
    )

    try:
        decode_token(token, secret="other-secret", expected_type="access")
    except TokenError:
        return

    raise AssertionError("TokenError was not raised")


def test_default_session_lifetimes_cover_24_hours() -> None:
    settings = Settings()

    assert settings.jwt_access_token_minutes == 24 * 60
    assert settings.jwt_refresh_rotation_hours == 7 * 24
