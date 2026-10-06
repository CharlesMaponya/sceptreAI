from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from datetime import timedelta
from typing import Annotated
from urllib.parse import urlencode

import httpx
import jwt
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from automl_api.core.config import get_settings
from automl_api.db.session import get_db
from automl_api.security.browser_sessions import set_browser_session
from automl_api.security.oidc import discovery, identity_user, verify_identity
from automl_api.security.tokens import TokenError, create_signed_token, decode_token
from automl_api.services.auth import issue_token_pair

router = APIRouter(prefix="/auth", tags=["auth"])
STATE_COOKIE = "sceptre_oidc_state"
STATE_PATH = "/api/v1/auth/oidc"


@router.get("/configuration")
def configuration() -> dict:
    settings = get_settings()
    return {"password_enabled": settings.simple_auth_enabled,
            "oidc_enabled": bool(settings.oidc_issuer and settings.oidc_client_id)}


@router.get("/oidc/login")
def login() -> RedirectResponse:
    settings = get_settings()
    try:
        metadata = discovery(settings)
    except (httpx.HTTPError, KeyError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="Organization sign-in is unavailable.") from exc
    state, nonce, verifier = (secrets.token_urlsafe(32) for _ in range(3))
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=").decode()
    )
    callback = f"{settings.public_app_url.rstrip('/')}/api/v1/auth/oidc/callback"
    query = urlencode({"client_id": settings.oidc_client_id, "response_type": "code",
                       "redirect_uri": callback, "scope": "openid profile email",
                       "state": state, "nonce": nonce, "code_challenge": challenge,
                       "code_challenge_method": "S256"})
    separator = "&" if "?" in metadata["authorization_endpoint"] else "?"
    response = RedirectResponse(metadata["authorization_endpoint"] + separator + query, 303)
    signed = create_signed_token(subject=state, email="", token_version=0,
                                 secret=settings.jwt_secret_key, token_type="oidc_state",
                                 expires_delta=timedelta(minutes=5),
                                 extra={"nonce": nonce, "verifier": verifier})
    response.set_cookie(STATE_COOKIE, signed, max_age=300, httponly=True, samesite="lax",
                        secure=settings.public_app_url.startswith("https://"), path=STATE_PATH)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/oidc/callback")
def callback(request: Request, db: Annotated[Session, Depends(get_db)],
             code: str = "", state: str = "") -> RedirectResponse:
    settings = get_settings()
    try:
        saved = decode_token(request.cookies.get(STATE_COOKIE, ""),
                             secret=settings.jwt_secret_key, expected_type="oidc_state")
        if not code or not state or not hmac.compare_digest(saved["sub"], state):
            raise ValueError("Invalid OIDC state.")
        metadata = discovery(settings)
        data = {"grant_type": "authorization_code", "code": code,
                "redirect_uri": f"{settings.public_app_url.rstrip('/')}/api/v1/auth/oidc/callback",
                "client_id": settings.oidc_client_id, "code_verifier": saved["verifier"]}
        auth = None
        if settings.oidc_client_secret:
            methods = metadata.get("token_endpoint_auth_methods_supported", ["client_secret_basic"])
            if "client_secret_basic" in methods:
                auth = (settings.oidc_client_id, settings.oidc_client_secret)
            elif "client_secret_post" in methods:
                data["client_secret"] = settings.oidc_client_secret
            else:
                raise ValueError("Unsupported OIDC client authentication method.")
        exchanged = httpx.post(metadata["token_endpoint"], data=data, auth=auth, timeout=15)
        exchanged.raise_for_status()
        claims = verify_identity(exchanged.json()["id_token"], saved["nonce"], metadata, settings)
        user = identity_user(db, claims)
        tokens = issue_token_pair(db, user, user_agent=request.headers.get("user-agent"),
                                  ip_address=request.client.host if request.client else None)
        db.commit()
    except (TokenError, jwt.PyJWTError, httpx.HTTPError, KeyError, ValueError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail="Organization sign-in could not be verified. Start sign-in again.",
        ) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409, detail="This identity conflicts with an existing account."
        ) from exc
    response = RedirectResponse(f"{settings.public_app_url.rstrip('/')}/auth?session=browser", 303)
    response.delete_cookie(STATE_COOKIE, path=STATE_PATH)
    set_browser_session(response, tokens)
    return response
