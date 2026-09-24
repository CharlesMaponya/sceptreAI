"""Tests for authentication configuration policy (P6-W01/W02)."""

from __future__ import annotations

import pytest
from automl_api.core.config import Settings
from automl_api.security.authentication_policy import validate_authentication_configuration


def test_production_rejects_simple_auth() -> None:
    settings = Settings(environment="production", simple_auth_enabled=True)

    with pytest.raises(ValueError, match="OIDC only"):
        validate_authentication_configuration(settings)


def test_staging_rejects_simple_auth() -> None:
    settings = Settings(environment="staging", simple_auth_enabled=True)

    with pytest.raises(ValueError, match="OIDC only"):
        validate_authentication_configuration(settings)


@pytest.mark.parametrize("environment", ["local", "development", ""])
def test_evaluation_profiles_allow_simple_auth(environment: str) -> None:
    settings = Settings(environment=environment, simple_auth_enabled=True)

    validate_authentication_configuration(settings)


def test_missing_oidc_configuration_fails_closed() -> None:
    settings = Settings(environment="production", simple_auth_enabled=False)

    with pytest.raises(ValueError, match="OIDC"):
        validate_authentication_configuration(settings)


@pytest.mark.parametrize(
    "values,message",
    [
        ({"oidc_issuer": "relative"}, "absolute"),
        ({"oidc_issuer": "https://issuer.test?tenant=1"}, "absolute"),
        ({"oidc_issuer": "https://user@issuer.test"}, "absolute"),
        ({"oidc_issuer": "https://issuer.test#tenant"}, "absolute"),
        ({"oidc_issuer": "ftp://issuer.test"}, "HTTP or HTTPS"),
        ({"environment": "production", "oidc_issuer": "http://issuer.test"}, "HTTPS"),
        ({"environment": "staging", "public_app_url": "http://app.test"}, "HTTPS"),
        ({"environment": "production", "oidc_require_mfa": False}, "MFA"),
    ],
)
def test_oidc_configuration_rejects_unsafe_boundaries(values, message):
    settings = Settings(
        **{
            "simple_auth_enabled": False,
            "oidc_issuer": "https://issuer.test",
            "oidc_client_id": "test",
            "public_app_url": "https://app.test",
            **values,
        }
    )
    with pytest.raises(ValueError, match=message):
        validate_authentication_configuration(settings)


@pytest.mark.parametrize("environment", ["local", "staging", "production"])
def test_configured_provider_neutral_oidc_is_accepted(environment):
    validate_authentication_configuration(
        Settings(
            environment=environment,
            simple_auth_enabled=False,
            oidc_issuer="https://issuer.test",
            oidc_client_id="sceptre",
            public_app_url="https://app.test",
            oidc_require_mfa=True,
        )
    )
