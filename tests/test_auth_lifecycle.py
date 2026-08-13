from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.main import app
from automl_api.schemas.auth import PasswordChangeRequest, RegisterRequest, UserUpdateRequest
from automl_api.security.passwords import hash_password, verify_password
from automl_api.security.tokens import create_signed_token
from automl_api.services import auth as auth_service
from automl_api.services import email as email_service
from automl_api.services.auth import change_user_password, update_user_profile
from fastapi import HTTPException


class FakeSession:
    def __init__(self) -> None:
        self.statements: list[object] = []
        self.flushes = 0

    def execute(self, statement: object) -> None:
        self.statements.append(statement)

    def flush(self) -> None:
        self.flushes += 1


class SequenceSession(FakeSession):
    def __init__(self, *scalar_values: object) -> None:
        super().__init__()
        self.scalar_values = list(scalar_values)
        self.added: list[object] = []

    def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    def add(self, instance: object) -> None:
        self.added.append(instance)


def test_account_lifecycle_routes_are_exposed() -> None:
    routes = app.openapi()["paths"]

    assert routes["/api/v1/auth/register"]["post"]["responses"]["201"]["content"]
    assert "patch" in routes["/api/v1/auth/me"]
    assert "post" in routes["/api/v1/auth/password/change"]
    assert "post" in routes["/api/v1/auth/password-reset/request"]
    assert "post" in routes["/api/v1/auth/password-reset/confirm"]


def test_profile_update_normalizes_identity_and_rotates_sessions() -> None:
    db = FakeSession()
    user = SimpleNamespace(
        id=uuid.uuid4(), email="old@example.com", full_name="Old Name", token_version=2
    )

    updated = update_user_profile(
        db,
        user,
        UserUpdateRequest(email="  ADA@EXAMPLE.COM ", full_name="  Ada Lovelace  "),
    )

    assert updated.email == "ada@example.com"
    assert updated.full_name == "Ada Lovelace"
    assert updated.token_version == 3
    assert len(db.statements) == 1
    assert db.flushes == 1


def test_password_change_requires_current_password_and_rotates_sessions() -> None:
    db = FakeSession()
    user = SimpleNamespace(
        id=uuid.uuid4(), password_hash=hash_password("current-password"), token_version=4
    )

    change_user_password(
        db,
        user,
        PasswordChangeRequest(
            current_password="current-password", new_password="a-new-secure-password"
        ),
    )

    assert verify_password("a-new-secure-password", user.password_hash)
    assert user.token_version == 5
    assert len(db.statements) == 1
    assert db.flushes == 1


def test_password_change_rejects_an_incorrect_current_password() -> None:
    db = FakeSession()
    user = SimpleNamespace(
        id=uuid.uuid4(), password_hash=hash_password("current-password"), token_version=1
    )

    with pytest.raises(HTTPException, match="current password is incorrect") as exc_info:
        change_user_password(
            db,
            user,
            PasswordChangeRequest(current_password="wrong-password", new_password="new-password"),
        )

    assert exc_info.value.status_code == 400
    assert user.token_version == 1
    assert db.statements == []


def test_registration_authentication_and_token_rotation() -> None:
    registration_db = SequenceSession()
    registered = auth_service.register_user(
        registration_db,
        RegisterRequest(
            email="  ADA@EXAMPLE.COM ",
            password="correct-horse-battery-staple",
            full_name="  Ada Lovelace  ",
        ),
    )
    assert registered.email == "ada@example.com"
    assert registered.full_name == "Ada Lovelace"
    assert verify_password("correct-horse-battery-staple", registered.password_hash or "")
    assert registration_db.added == [registered]
    assert registration_db.flushes == 1

    user = SimpleNamespace(
        id=uuid.uuid4(),
        email="ada@example.com",
        password_hash=registered.password_hash,
        is_active=True,
        token_version=3,
        last_login_at=None,
    )
    assert auth_service.authenticate_user(
        SequenceSession(user), " ADA@example.com ", "correct-horse-battery-staple"
    ) is user
    assert user.last_login_at is not None
    assert auth_service.authenticate_user(SequenceSession(None), user.email, "password") is None
    assert auth_service.authenticate_user(SequenceSession(user), user.email, "wrong") is None

    issue_db = SequenceSession()
    pair = auth_service.issue_token_pair(
        issue_db,
        user,
        user_agent="pytest",
        ip_address="127.0.0.1",
    )
    stored = issue_db.added[0]
    assert pair.expires_in > 0
    assert stored.user_agent == "pytest"
    assert stored.ip_address == "127.0.0.1"

    rotation_db = SequenceSession(stored, user)
    rotated = auth_service.rotate_refresh_token(rotation_db, pair.refresh_token)
    assert stored.rotated_at is not None
    assert rotated.refresh_token != pair.refresh_token
    assert rotation_db.added[0].family_id == stored.family_id


def test_authentication_and_refresh_fail_closed() -> None:
    inactive = SimpleNamespace(is_active=False, password_hash=hash_password("password"))
    assert auth_service.authenticate_user(SequenceSession(inactive), "a@b.co", "password") is None

    with pytest.raises(HTTPException, match="Invalid or expired") as invalid:
        auth_service.rotate_refresh_token(SequenceSession(), "not-a-token")
    assert invalid.value.status_code == 401

    user = SimpleNamespace(
        id=uuid.uuid4(), email="ada@example.com", token_version=2, is_active=True
    )
    token = create_signed_token(
        subject=str(user.id),
        email=user.email,
        token_version=user.token_version,
        secret=auth_service.get_settings().jwt_secret_key,
        token_type="refresh",
        expires_delta=timedelta(minutes=5),
    )
    expired_record = SimpleNamespace(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(HTTPException, match="already been used or revoked"):
        auth_service.rotate_refresh_token(SequenceSession(expired_record), token)


def test_logout_and_password_reset_lifecycle() -> None:
    refresh_record = SimpleNamespace(revoked_at=None)
    auth_service.logout_refresh_token(SequenceSession(refresh_record), "refresh-token")
    assert refresh_record.revoked_at is not None
    auth_service.logout_refresh_token(SequenceSession(None), "unknown-token")

    user = SimpleNamespace(
        id=uuid.uuid4(),
        email="ada@example.com",
        is_active=True,
        token_version=7,
        password_hash=hash_password("old-password"),
    )
    reset_db = SequenceSession(user)
    reset_token = auth_service.create_password_reset_token(reset_db, user.email)
    assert reset_token is not None
    stored_token = reset_db.added[0]

    confirm_db = SequenceSession(stored_token, user)
    auth_service.confirm_password_reset(confirm_db, reset_token, "new-secure-password")
    assert stored_token.used_at is not None
    assert user.token_version == 8
    assert verify_password("new-secure-password", user.password_hash)
    assert len(confirm_db.statements) == 1

    assert auth_service.create_password_reset_token(SequenceSession(None), user.email) is None
    with pytest.raises(HTTPException, match="Invalid or expired"):
        auth_service.confirm_password_reset(SequenceSession(), "bad-token", "new-password")


def test_password_reset_email_contains_a_single_use_browser_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delivered: list[object] = []

    class FakeSmtp:
        def __init__(self, host: str, port: int, timeout: int) -> None:
            assert (host, port, timeout) == ("smtp.example.com", 587, 10)

        def __enter__(self) -> FakeSmtp:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def starttls(self) -> None:
            return None

        def login(self, username: str, password: str) -> None:
            assert (username, password) == ("mailer", "smtp-secret")

        def send_message(self, message: object) -> None:
            delivered.append(message)

    monkeypatch.setattr(email_service.smtplib, "SMTP", FakeSmtp)
    monkeypatch.setattr(
        email_service,
        "get_settings",
        lambda: SimpleNamespace(
            smtp_host="smtp.example.com",
            smtp_port=587,
            smtp_username="mailer",
            smtp_password="smtp-secret",
            smtp_from_email="security@sceptre.example",
            smtp_starttls=True,
            smtp_use_ssl=False,
            public_app_url="https://sceptre.example/",
        ),
    )

    assert email_service.send_password_reset_email("ada@example.com", "one-time-token")
    assert len(delivered) == 1
    content = delivered[0].get_content()  # type: ignore[attr-defined]
    assert "https://sceptre.example/auth?mode=reset&token=one-time-token" in content
