from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

Provider = Literal["aws-eks", "gcp-gke", "azure-aks"]


class ProviderCredentialError(RuntimeError):
    """Raised when a provider token violates its workload-identity contract."""


@dataclass(frozen=True)
class ProviderCredential:
    provider: Provider
    project_id: str
    attempt_id: str
    subject: str
    audience: str
    expires_at: datetime

    def validate(
        self,
        *,
        provider: Provider,
        project_id: str,
        attempt_id: str,
        subject: str,
        audience: str,
        now: datetime,
    ) -> None:
        if now.tzinfo is None or now <= datetime(2000, 1, 1, tzinfo=UTC):
            raise ProviderCredentialError("now must be a realistic timezone-aware timestamp")
        if now >= self.expires_at:
            raise ProviderCredentialError("provider credential expired")
        expected = (
            self.provider,
            self.project_id,
            self.attempt_id,
            self.subject,
            self.audience,
        )
        supplied = (provider, project_id, attempt_id, subject, audience)
        if supplied != expected:
            raise ProviderCredentialError("provider credential claim mismatch")


def expected_subject(provider: Provider, *, namespace: str, service_account: str) -> str:
    if provider == "aws-eks":
        return f"system:serviceaccount:{namespace}:{service_account}"
    if provider == "gcp-gke":
        return f"{namespace}.svc.id.goog[{namespace}/{service_account}]"
    if provider == "azure-aks":
        return f"system:serviceaccount:{namespace}:{service_account}"
    raise ProviderCredentialError(f"unsupported provider: {provider}")

