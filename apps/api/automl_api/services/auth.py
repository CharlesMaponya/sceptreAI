from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.models.enums import AuthProvider
from automl_api.models.iam import PasswordResetToken, RefreshToken, User
from automl_api.schemas.auth import RegisterRequest, TokenPair, normalize_email
from automl_api.security.passwords import hash_password, verify_password
from automl_api.security.tokens import TokenError, create_signed_token, decode_token, token_hash


def _now() -> datetime:
    return datetime.now(timezone.utc)


def register_user(db: Session, payload: RegisterRequest) -> User:
    user = User(
        email=normalize_email(payload.email),
        full_name=payload.full_name.strip() if payload.full_name else None,
        password_hash=hash_password(payload.password),
        auth_provider=AuthProvider.SIMPLE,
        is_verified=True,
    )
    db.add(user)
    db.flush()
    return user


def authenticate_user(db: Session, email: str, password: str) -> User | None:
    user = db.scalar(select(User).where(User.email == normalize_email(email)))
    if user is None or not user.is_active:
        return None
    if not verify_password(password, user.password_hash):
        return None
    user.last_login_at = _now()
    return user


def issue_token_pair(
    db: Session,
    user: User,
    *,
    user_agent: str | None = None,
    ip_address: str | None = None,
    family_id: uuid.UUID | None = None,
) -> TokenPair:
    settings = get_settings()
    access_delta = timedelta(minutes=settings.jwt_access_token_minutes)
    refresh_delta = timedelta(hours=settings.jwt_refresh_rotation_hours)
    family_id = family_id or uuid.uuid4()

    access_token = create_signed_token(
        subject=str(user.id),
        email=user.email,
        token_version=user.token_version,
        secret=settings.jwt_secret_key,
        token_type="access",
        expires_delta=access_delta,
    )
    refresh_token = create_signed_token(
        subject=str(user.id),
        email=user.email,
        token_version=user.token_version,
        secret=settings.jwt_secret_key,
        token_type="refresh",
        expires_delta=refresh_delta,
        extra={"family": str(family_id)},
    )
    db.add(
        RefreshToken(
            user_id=user.id,
            token_hash=token_hash(refresh_token),
            family_id=family_id,
            issued_at=_now(),
            expires_at=_now() + refresh_delta,
            user_agent=user_agent,
            ip_address=ip_address,
        )
    )

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=int(access_delta.total_seconds()),
    )


def rotate_refresh_token(
    db: Session,
    refresh_token: str,
    *,
    user_agent: str | None = None,
    ip_address: str | None = None,
) -> TokenPair:
    settings = get_settings()
    try:
        payload = decode_token(
            refresh_token,
            secret=settings.jwt_secret_key,
            expected_type="refresh",
        )
    except TokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token.",
        ) from exc

    existing = db.scalar(
        select(RefreshToken).where(
            RefreshToken.token_hash == token_hash(refresh_token),
            RefreshToken.revoked_at.is_(None),
            RefreshToken.rotated_at.is_(None),
        )
    )
    if existing is None or existing.expires_at <= _now():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token has already been used or revoked.",
        )

    user = db.scalar(select(User).where(User.id == uuid.UUID(str(payload["sub"]))))
    if user is None or not user.is_active or int(payload.get("ver", -1)) != user.token_version:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token user is no longer valid.",
        )

    existing.rotated_at = _now()
    return issue_token_pair(
        db,
        user,
        user_agent=user_agent,
        ip_address=ip_address,
        family_id=existing.family_id,
    )


def logout_refresh_token(db: Session, refresh_token: str) -> None:
    existing = db.scalar(select(RefreshToken).where(RefreshToken.token_hash == token_hash(refresh_token)))
    if existing is not None:
        existing.revoked_at = _now()


def create_password_reset_token(db: Session, email: str) -> str | None:
    user = db.scalar(select(User).where(User.email == normalize_email(email)))
    if user is None or not user.is_active:
        return None

    reset_token = create_signed_token(
        subject=str(user.id),
        email=user.email,
        token_version=user.token_version,
        secret=get_settings().jwt_secret_key,
        token_type="password_reset",
        expires_delta=timedelta(minutes=30),
    )
    db.add(
        PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash(reset_token),
            expires_at=_now() + timedelta(minutes=30),
        )
    )
    return reset_token


def confirm_password_reset(db: Session, reset_token: str, new_password: str) -> None:
    try:
        payload = decode_token(
            reset_token,
            secret=get_settings().jwt_secret_key,
            expected_type="password_reset",
        )
    except TokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid or expired password reset token.",
        ) from exc

    stored_token = db.scalar(
        select(PasswordResetToken).where(
            PasswordResetToken.token_hash == token_hash(reset_token),
            PasswordResetToken.used_at.is_(None),
        )
    )
    if stored_token is None or stored_token.expires_at <= _now():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password reset token has already been used or expired.",
        )

    user = db.scalar(select(User).where(User.id == uuid.UUID(str(payload["sub"]))))
    if user is None or int(payload.get("ver", -1)) != user.token_version:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password reset token is no longer valid.",
        )

    user.password_hash = hash_password(new_password)
    user.token_version += 1
    stored_token.used_at = _now()

