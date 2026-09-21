from __future__ import annotations

import logging
import smtplib
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.core.config import get_settings
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.auth import (
    AuthResponse,
    LoginRequest,
    LogoutRequest,
    PasswordChangeRequest,
    PasswordResetConfirm,
    PasswordResetRequest,
    PasswordResetResponse,
    RefreshRequest,
    RegisterRequest,
    RegistrationResponse,
    TokenPair,
    UserRead,
    UserUpdateRequest,
)
from automl_api.services.auth import (
    authenticate_user,
    change_user_password,
    confirm_password_reset,
    create_password_reset_token,
    issue_token_pair,
    logout_refresh_token,
    register_user,
    rotate_refresh_token,
    update_user_profile,
)
from automl_api.services.email import send_password_reset_email
from automl_api.security.browser_sessions import (
    REFRESH_COOKIE, clear_browser_session, require_same_origin, set_browser_session,
)

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger(__name__)


def _require_local_auth() -> None:
    if not get_settings().simple_auth_enabled:
        raise HTTPException(status_code=403, detail="Password authentication is disabled.")


def _client_context(request: Request) -> tuple[str | None, str | None]:
    user_agent = request.headers.get("user-agent")
    ip_address = request.client.host if request.client else None
    return user_agent, ip_address


@router.post("/register", response_model=RegistrationResponse, status_code=status.HTTP_201_CREATED)
def register(
    payload: RegisterRequest,
    db: Annotated[Session, Depends(get_db)],
) -> RegistrationResponse:
    settings = get_settings()
    if not settings.simple_auth_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Simple registration is disabled for this deployment.",
        )

    try:
        user = register_user(db, payload)
        db.commit()
        db.refresh(user)
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A user with this email already exists.",
        ) from exc

    return RegistrationResponse(user=UserRead.model_validate(user))


@router.post("/login", response_model=AuthResponse)
def login(
    payload: LoginRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    response: Response = None,
) -> AuthResponse:
    _require_local_auth()
    user = authenticate_user(db, payload.email, payload.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )

    user_agent, ip_address = _client_context(request)
    tokens = issue_token_pair(db, user, user_agent=user_agent, ip_address=ip_address)
    db.commit()
    if response is not None:
        set_browser_session(response, tokens)
    return AuthResponse(user=UserRead.model_validate(user), tokens=tokens)


@router.post("/refresh", response_model=TokenPair)
def refresh(
    payload: RefreshRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    response: Response = None,
) -> TokenPair:
    token = payload.refresh_token
    if not token:
        require_same_origin(request)
        token = request.cookies.get(REFRESH_COOKIE)
    if not token:
        raise HTTPException(status_code=401, detail="A refresh session is required.")
    user_agent, ip_address = _client_context(request)
    tokens = rotate_refresh_token(
        db,
        token,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    db.commit()
    if response is not None:
        set_browser_session(response, tokens)
    return tokens


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    payload: LogoutRequest,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[User, Depends(get_current_user)],
    request: Request = None,
) -> Response:
    token = payload.refresh_token or (request.cookies.get(REFRESH_COOKIE) if request else None)
    if token:
        logout_refresh_token(db, token)
        db.commit()
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    clear_browser_session(response)
    return response


@router.get("/me", response_model=UserRead)
def me(current_user: Annotated[User, Depends(get_current_user)]) -> UserRead:
    return UserRead.model_validate(current_user)


@router.patch("/me", response_model=AuthResponse)
def update_me(
    payload: UserUpdateRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> AuthResponse:
    try:
        user = update_user_profile(db, current_user, payload)
        user_agent, ip_address = _client_context(request)
        tokens = issue_token_pair(db, user, user_agent=user_agent, ip_address=ip_address)
        db.commit()
        db.refresh(user)
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A user with this email already exists.",
        ) from exc
    return AuthResponse(user=UserRead.model_validate(user), tokens=tokens)


@router.post("/password/change", response_model=AuthResponse)
def change_password(
    payload: PasswordChangeRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> AuthResponse:
    _require_local_auth()
    user = change_user_password(db, current_user, payload)
    user_agent, ip_address = _client_context(request)
    tokens = issue_token_pair(db, user, user_agent=user_agent, ip_address=ip_address)
    db.commit()
    db.refresh(user)
    return AuthResponse(user=UserRead.model_validate(user), tokens=tokens)


@router.post("/password-reset/request", response_model=PasswordResetResponse)
def request_password_reset(
    payload: PasswordResetRequest,
    db: Annotated[Session, Depends(get_db)],
) -> PasswordResetResponse:
    _require_local_auth()
    reset_token = create_password_reset_token(db, payload.email)
    db.commit()

    if reset_token:
        try:
            send_password_reset_email(payload.email, reset_token)
        except (OSError, smtplib.SMTPException):
            logger.exception("Password reset email delivery failed")

    return PasswordResetResponse()


@router.post("/password-reset/confirm", status_code=status.HTTP_204_NO_CONTENT)
def reset_password(
    payload: PasswordResetConfirm,
    db: Annotated[Session, Depends(get_db)],
) -> Response:
    _require_local_auth()
    confirm_password_reset(db, payload.token, payload.new_password)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
