"""Host-only HttpOnly browser sessions; bearer authentication remains available to clients."""
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, Response

from automl_api.core.config import get_settings
from automl_api.schemas.auth import TokenPair

ACCESS_COOKIE = "sceptre_access"
REFRESH_COOKIE = "sceptre_refresh"


def require_same_origin(request: Request) -> None:
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return
    app = urlsplit(get_settings().public_app_url)
    expected = f"{app.scheme}://{app.netloc}"
    if request.headers.get("origin") != expected:
        raise HTTPException(status_code=403, detail="A same-origin browser request is required.")


def set_browser_session(response: Response, tokens: TokenPair) -> None:
    settings = get_settings()
    secure = settings.public_app_url.startswith("https://")
    response.set_cookie(ACCESS_COOKIE, tokens.access_token, max_age=tokens.expires_in,
                        httponly=True, secure=secure, samesite="lax", path="/api/v1")
    response.set_cookie(REFRESH_COOKIE, tokens.refresh_token,
                        max_age=settings.jwt_refresh_rotation_hours * 3600,
                        httponly=True, secure=secure, samesite="lax", path="/api/v1/auth")
    response.headers["Cache-Control"] = "no-store"


def clear_browser_session(response: Response) -> None:
    response.delete_cookie(ACCESS_COOKIE, path="/api/v1")
    response.delete_cookie(REFRESH_COOKIE, path="/api/v1/auth")
    response.headers["Cache-Control"] = "no-store"
