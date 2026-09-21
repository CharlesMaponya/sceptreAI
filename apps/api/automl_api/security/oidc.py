"""OIDC authorization-code verification with fixed algorithms and verified discovery."""
from __future__ import annotations

import hmac
from functools import lru_cache
from urllib.parse import urlsplit

import httpx
import jwt
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.core.config import Settings
from automl_api.models.enums import AuthProvider
from automl_api.models.iam import User
from automl_api.schemas.auth import normalize_email


def discovery(settings: Settings) -> dict:
    if not settings.oidc_issuer or not settings.oidc_client_id:
        raise HTTPException(status_code=404, detail="Organization sign-in is not configured.")
    response = httpx.get(f"{settings.oidc_issuer}/.well-known/openid-configuration", timeout=10)
    response.raise_for_status()
    metadata = response.json()
    if metadata.get("issuer") != settings.oidc_issuer:
        raise ValueError("OIDC discovery issuer mismatch.")
    for name in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
        parsed = urlsplit(metadata[name])
        if not parsed.netloc or parsed.username or parsed.fragment:
            raise ValueError("Invalid OIDC endpoint.")
        if parsed.scheme != "https" and (
            settings.environment.strip().lower() in {"staging", "production"}
            or parsed.scheme != "http"
            or parsed.netloc != urlsplit(settings.oidc_issuer).netloc
        ):
            raise ValueError("OIDC endpoints must use verified HTTPS outside local testing.")
    return metadata


@lru_cache(maxsize=8)
def jwks_client(uri: str) -> jwt.PyJWKClient:
    return jwt.PyJWKClient(uri, lifespan=300, timeout=10)


def verify_identity(token: str, nonce: str, metadata: dict, settings: Settings) -> dict:
    key = jwks_client(metadata["jwks_uri"]).get_signing_key_from_jwt(token)
    claims = jwt.decode(token, key.key, algorithms=["RS256", "ES256"],
                        audience=settings.oidc_client_id, issuer=settings.oidc_issuer,
                        options={"require": ["exp", "iat", "sub", "iss", "aud", "nonce"]})
    if not hmac.compare_digest(str(claims["nonce"]), nonce):
        raise ValueError("OIDC nonce mismatch.")
    audience = claims["aud"]
    if (isinstance(audience, list) and len(audience) > 1) or "azp" in claims:
        if claims.get("azp") != settings.oidc_client_id:
            raise ValueError("OIDC authorized party mismatch.")
    if not isinstance(claims["sub"], str) or not 1 <= len(claims["sub"]) <= 512:
        raise ValueError("OIDC subject is missing or too long.")
    if claims.get("email_verified") is not True:
        raise ValueError("Organization sign-in requires a verified email.")
    if settings.oidc_require_mfa and (
        not isinstance(claims.get("amr"), list) or "mfa" not in claims["amr"]
    ):
        raise ValueError("Organization sign-in requires MFA.")
    claims["email"] = normalize_email(claims.get("email", ""))
    return claims


def identity_user(db: Session, claims: dict) -> User:
    user = db.scalar(select(User).where(User.sso_issuer == claims["iss"],
                                       User.sso_subject == claims["sub"]))
    if user is None:
        # Email alone never links a local account or an identity from another issuer.
        if db.scalar(select(User).where(User.email == claims["email"])) is not None:
            raise HTTPException(status_code=409, detail="This email already belongs to another sign-in identity. Contact your administrator.")
        user = User(email=claims["email"], full_name=str(claims.get("name", ""))[:200] or None,
                    sso_issuer=claims["iss"], sso_subject=claims["sub"],
                    auth_provider=AuthProvider.SSO, is_verified=True)
        db.add(user)
        db.flush()
    if not user.is_active:
        raise HTTPException(status_code=403, detail="This account is disabled.")
    return user
