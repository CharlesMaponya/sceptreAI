from __future__ import annotations

from datetime import timedelta

from automl_api.core.config import Settings
from automl_api.security.passwords import (
    hash_password,
    needs_rehash,
    verify_and_rehash,
    verify_password,
)
from automl_api.security.tokens import TokenError, create_signed_token, decode_token


def test_password_hash_round_trip() -> None:
    password_hash = hash_password("correct horse battery staple")

    assert verify_password("correct horse battery staple", password_hash)
    assert not verify_password("wrong password", password_hash)


def test_hashed_passwords_use_argon2id() -> None:
    """P6-W04: new hashes are Argon2id."""
    password_hash = hash_password("correct horse battery staple")

    assert password_hash.startswith("$argon2id$")


def test_rehash_on_login_upgrades_legacy_pbkdf2_hashes() -> None:
    """P6-W04: legacy pbkdf2 hashes verify once and upgrade to Argon2id."""
    import base64
    import hashlib
    import secrets

    from automl_api.security.passwords import PASSWORD_ALGORITHM, PASSWORD_ITERATIONS

    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", b"legacy secret", salt, PASSWORD_ITERATIONS
    )
    legacy = (
        f"{PASSWORD_ALGORITHM}${PASSWORD_ITERATIONS}$"
        f"{base64.urlsafe_b64encode(salt).decode('ascii')}$"
        f"{base64.urlsafe_b64encode(digest).decode('ascii')}"
    )

    ok, upgraded = verify_and_rehash("legacy secret", legacy)

    assert ok
    assert upgraded is not None
    assert upgraded.startswith("$argon2id$")
    assert verify_password("legacy secret", upgraded)
    assert not needs_rehash(upgraded)


def test_argon2id_verification_without_parameter_drift_returns_no_new_hash() -> None:
    current = hash_password("stable password")

    ok, upgraded = verify_and_rehash("stable password", current)

    assert ok
    assert upgraded is None


def test_needs_rehash_flags_legacy_hashes() -> None:
    assert needs_rehash(None)
    assert needs_rehash("pbkdf2_sha256$1$x$y")
    assert not needs_rehash(hash_password("fresh"))


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


def test_access_tokens_are_short_lived_and_refresh_supports_long_sessions() -> None:
    settings = Settings()

    assert settings.jwt_access_token_minutes == 15
    assert settings.jwt_refresh_rotation_hours == 7 * 24
