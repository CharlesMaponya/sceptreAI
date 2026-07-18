from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from automl_api.main import app
from automl_api.schemas.auth import PasswordChangeRequest, UserUpdateRequest
from automl_api.security.passwords import hash_password, verify_password
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
