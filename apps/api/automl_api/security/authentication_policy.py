"""Authentication configuration policy (task-index P6-W01).

Production is OIDC-only: local simple-auth accounts exist only in evaluation
profiles. The API fails closed at startup when a governed environment tries
to boot with password login enabled.
"""

from __future__ import annotations

from automl_api.core.config import Settings
from urllib.parse import urlsplit

GOVERNED_ENVIRONMENTS = {"staging", "production"}


def validate_authentication_configuration(settings: Settings) -> None:
    """Reject simple-auth in governed environments (OIDC-only requirement)."""
    if (
        settings.environment.strip().lower() in GOVERNED_ENVIRONMENTS
        and settings.simple_auth_enabled
    ):
        raise ValueError(
            "SIMPLE_AUTH_ENABLED must be false in staging and production: "
            "governed environments authenticate through OIDC only. Local "
            "accounts are available in evaluation profiles."
        )
    if not settings.simple_auth_enabled:
        if not settings.oidc_issuer or not settings.oidc_client_id:
            raise ValueError("OIDC_ISSUER and OIDC_CLIENT_ID are required for OIDC authentication.")
    if settings.oidc_issuer:
        issuer = urlsplit(settings.oidc_issuer)
        if not issuer.netloc or issuer.query or issuer.fragment or issuer.username:
            raise ValueError("OIDC_ISSUER must be an absolute issuer URL.")
        if settings.environment.strip().lower() in GOVERNED_ENVIRONMENTS:
            if issuer.scheme != "https" or not settings.public_app_url.startswith("https://"):
                raise ValueError("Governed OIDC and application URLs must use HTTPS.")
            if not settings.oidc_require_mfa:
                raise ValueError("Governed OIDC requires MFA evidence from the issuer.")
        elif issuer.scheme not in {"http", "https"}:
            raise ValueError("OIDC_ISSUER must use HTTP or HTTPS.")
