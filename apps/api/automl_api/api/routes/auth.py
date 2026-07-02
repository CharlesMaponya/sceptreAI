from __future__ import annotations

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
    PasswordResetConfirm,
    PasswordResetRequest,
    PasswordResetResponse,
    RefreshRequest,
    RegisterRequest,
    TokenPair,
    UserRead,
)
from automl_api.services.auth import (
    authenticate_user,
    confirm_password_reset,
    create_password_reset_token,
    issue_token_pair,
    logout_refresh_token,
    register_user,
    rotate_refresh_token,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _client_context(request: Request) -> tuple[str | None, str | None]:
    user_agent = request.headers.get("user-agent")
    ip_address = request.client.host if request.client else None
    return user_agent, ip_address


@router.post("/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
def register(
    payload: RegisterRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
) -> AuthResponse:
    settings = get_settings()
    if not settings.simple_auth_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Simple registration is disabled for this deployment.",
        )

    try:
        user = register_user(db, payload)
        user_agent, ip_address = _client_context(request)
        tokens = issue_token_pair(db, user, user_agent=user_agent, ip_address=ip_address)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A user with this email already exists.",
        ) from exc

    return AuthResponse(user=UserRead.model_validate(user), tokens=tokens)


@router.post("/login", response_model=AuthResponse)
def login(
    payload: LoginRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
) -> AuthResponse:
    user = authenticate_user(db, payload.email, payload.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password.",
        )

    user_agent, ip_address = _client_context(request)
    tokens = issue_token_pair(db, user, user_agent=user_agent, ip_address=ip_address)
    db.commit()
    return AuthResponse(user=UserRead.model_validate(user), tokens=tokens)


@router.post("/refresh", response_model=TokenPair)
def refresh(
    payload: RefreshRequest,
    request: Request,
    db: Annotated[Session, Depends(get_db)],
) -> TokenPair:
    user_agent, ip_address = _client_context(request)
    tokens = rotate_refresh_token(
        db,
        payload.refresh_token,
        user_agent=user_agent,
        ip_address=ip_address,
    )
    db.commit()
    return tokens


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(
    payload: LogoutRequest,
    db: Annotated[Session, Depends(get_db)],
    _current_user: Annotated[User, Depends(get_current_user)],
) -> Response:
    if payload.refresh_token:
        logout_refresh_token(db, payload.refresh_token)
        db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/me", response_model=UserRead)
def me(current_user: Annotated[User, Depends(get_current_user)]) -> UserRead:
    return UserRead.model_validate(current_user)


@router.post("/password-reset/request", response_model=PasswordResetResponse)
def request_password_reset(
    payload: PasswordResetRequest,
    db: Annotated[Session, Depends(get_db)],
) -> PasswordResetResponse:
    reset_token = create_password_reset_token(db, payload.email)
    db.commit()

    response = PasswordResetResponse()
    if get_settings().environment != "production":
        response.reset_token_for_dev = reset_token
    return response


@router.post("/password-reset/confirm", status_code=status.HTTP_204_NO_CONTENT)
def reset_password(
    payload: PasswordResetConfirm,
    db: Annotated[Session, Depends(get_db)],
) -> Response:
    confirm_password_reset(db, payload.token, payload.new_password)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
