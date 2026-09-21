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
