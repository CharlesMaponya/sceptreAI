from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from automl_shared.provider_credentials import (
    ProviderCredential,
    ProviderCredentialError,
    expected_subject,
)

NOW = datetime(2026, 8, 13, tzinfo=UTC)


@pytest.mark.parametrize("provider", ["aws-eks", "gcp-gke", "azure-aks"])
def test_provider_claims_are_attempt_scoped_and_expiring(provider: str) -> None:
    subject = expected_subject(  # type: ignore[arg-type]
        provider, namespace="project-a-attempt-7", service_account="ray-training"
    )
    credential = ProviderCredential(
        provider=provider,  # type: ignore[arg-type]
        project_id="project-a",
        attempt_id="attempt-7",
        subject=subject,
        audience="sceptre-object-store",
        expires_at=NOW + timedelta(minutes=15),
    )
    credential.validate(
        provider=provider,  # type: ignore[arg-type]
        project_id="project-a",
        attempt_id="attempt-7",
        subject=subject,
        audience="sceptre-object-store",
        now=NOW,
    )
    for changed in (
        {"project_id": "project-b"},
        {"attempt_id": "attempt-8"},
        {"audience": "release-final"},
        {"subject": subject + "-other"},
    ):
        arguments = {
            "provider": provider,
            "project_id": "project-a",
            "attempt_id": "attempt-7",
            "subject": subject,
            "audience": "sceptre-object-store",
            "now": NOW,
            **changed,
        }
        with pytest.raises(ProviderCredentialError, match="claim mismatch"):
            credential.validate(**arguments)  # type: ignore[arg-type]
    with pytest.raises(ProviderCredentialError, match="expired"):
        credential.validate(
            provider=provider,  # type: ignore[arg-type]
            project_id="project-a",
            attempt_id="attempt-7",
            subject=subject,
            audience="sceptre-object-store",
            now=NOW + timedelta(minutes=15),
        )


def test_provider_contract_rejects_invalid_time_and_provider() -> None:
    with pytest.raises(ProviderCredentialError, match="unsupported provider"):
        expected_subject(  # type: ignore[arg-type]
            "unsupported", namespace="namespace", service_account="account"
        )
    credential = ProviderCredential(
        provider="aws-eks",
        project_id="project-a",
        attempt_id="attempt-7",
        subject="subject",
        audience="audience",
        expires_at=NOW + timedelta(minutes=15),
    )
    with pytest.raises(ProviderCredentialError, match="realistic timezone-aware"):
        credential.validate(
            provider="aws-eks",
            project_id="project-a",
            attempt_id="attempt-7",
            subject="subject",
            audience="audience",
            now=datetime(1999, 1, 1),
        )

