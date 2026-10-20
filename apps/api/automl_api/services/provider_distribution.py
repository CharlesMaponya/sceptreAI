"""Signed role-filtered provider copies, issued by the qualification authority."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Annotated, Literal
from urllib.parse import unquote, urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select

from automl_api.models.enums import FinalTestStatus
from automl_api.models.qualification import FinalTestAuthorityReceipt
from automl_api.services import final_test_authority as authority
from automl_api.services.final_test_credentials import FinalDataManifest
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Provider = Literal["aws", "gcp", "azure"]
Revision = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")]
PROVIDERS = ("aws", "gcp", "azure")


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SafeObject(Contract):
    role: Literal["train", "validation"]
    uri: Annotated[str, Field(min_length=1, max_length=1024)]
    version: Annotated[str, Field(min_length=1, max_length=256)]
    sha256: Digest
    byte_size: Annotated[int, Field(gt=0, strict=True)]
    rows: Annotated[int, Field(gt=0, strict=True)]
    row_digest: Digest
    row_digest_revision: Literal["xor-sha256-row-id-v1"] = "xor-sha256-row-id-v1"

    @model_validator(mode="after")
    def safe_location(self):
        parts = urlsplit(self.uri)
        path = unquote(parts.path)
        if (
            not parts.netloc
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
            or any(c.isspace() or ord(c) < 32 for c in self.uri)
            or any(p in {".", "..", ""} for p in path.strip("/").split("/"))
            or f"/roles/split_role={self.role}/" not in path
            or self.version.strip().lower() in {"", "null"}
        ):
            raise ValueError("Provider copies require canonical versioned train/validation objects")
        return self


class ProviderCopy(Contract):
    provider: Provider
    schema_digest: Digest
    objects: Annotated[tuple[SafeObject, ...], Field(min_length=2, max_length=10000)]

    @model_validator(mode="after")
    def provider_objects(self):
        schemes = {"aws": {"s3", "s3c"}, "gcp": {"gs"}, "azure": {"az", "azure"}}
        if {o.role for o in self.objects} != {"train", "validation"}:
            raise ValueError("Provider copies require both safe roles")
        if len({o.uri for o in self.objects}) != len(self.objects):
            raise ValueError("Provider copy objects must be distinct")
        if any(urlsplit(o.uri).scheme not in schemes[self.provider] for o in self.objects):
            raise ValueError("Provider copy storage does not match its provider")
        if self.provider == "gcp" and any(not o.version.isdecimal() for o in self.objects):
            raise ValueError("GCS copies require numeric object generations")
        return self

    def logical_roles(self):
        result = {}
        for role in ("train", "validation"):
            objects = [o for o in self.objects if o.role == role]
            row_xor = 0
            for item in objects:
                row_xor ^= int(item.row_digest, 16)
            result[role] = (sum(o.rows for o in objects), f"{row_xor:064x}")
        return self.schema_digest, result


class DistributionRequest(Contract):
    fixture_revision: Revision
    split_revision: Revision
    copies: Annotated[tuple[ProviderCopy, ...], Field(min_length=3, max_length=3)]

    @model_validator(mode="after")
    def same_logical_data(self):
        if {c.provider for c in self.copies} != set(PROVIDERS):
            raise ValueError("Distribution requires exactly one copy per provider")
        if any(c.logical_roles() != self.copies[0].logical_roles() for c in self.copies):
            raise ValueError("Provider copies differ in logical rows or schema")
        return self


class FinalRoleDigests(Contract):
    inputs: Digest
    labels: Digest


def final_role_digests(manifest):
    return FinalRoleDigests(
        **{
            role: canonical_request_hash(
                sorted(o.sha256 for o in manifest.objects if o.role == role)
            )
            for role in ("inputs", "labels")
        }
    )


class ProviderDistribution(Contract):
    revision: Literal["provider-distribution-v1"] = "provider-distribution-v1"
    allocation_id: uuid.UUID
    project_reference: Annotated[str, Field(min_length=1, max_length=255)]
    scope_id: uuid.UUID
    fixture_revision: Revision
    split_revision: Revision
    split_digest: Digest
    canonical_provider: Provider
    provider: Provider
    mode: Literal["promotional", "validation_only"]
    safe_data: ProviderCopy
    final_role_digests: FinalRoleDigests
    final_data: FinalDataManifest | None = None

    @model_validator(mode="after")
    def role_boundary(self):
        canonical = self.provider == self.canonical_provider
        if self.safe_data.provider != self.provider or self.mode != (
            "promotional" if canonical else "validation_only"
        ):
            raise ValueError("Distribution mode/provider binding changed")
        if canonical != (self.final_data is not None):
            raise ValueError("Only the canonical provider may receive final object locations")
        if self.final_data is not None and (
            self.final_data.provider != self.provider
            or self.final_data.project_reference != self.project_reference
            or self.final_data.scope_id != self.scope_id
            or self.final_data.split_digest != self.split_digest
            or final_role_digests(self.final_data) != self.final_role_digests
        ):
            raise ValueError("Distribution final-data lineage changed")
        return self


class DistributionPayload(Contract):
    manifest: ProviderDistribution
    signing_key_id: Digest


class DistributionReceipt(Contract):
    allocation_id: uuid.UUID
    operation: Literal["distribution_aws", "distribution_gcp", "distribution_azure"]
    provider: Provider
    request_digest: Digest
    receipt_digest: Digest
    signature_algorithm: Literal["ed25519"]
    signature: Annotated[str, Field(pattern=r"^[0-9a-f]{128}$")]
    payload: DistributionPayload


def issue_distributions(db, *, allocation_id, project_reference, request, final_data, signing_key):
    if not isinstance(signing_key, Ed25519PrivateKey):
        raise ValueError("Provider distribution requires the authority's Ed25519 key")
    allocation = authority._locked_allocation(db, allocation_id)
    if allocation.project_reference != project_reference:
        raise authority.ProviderRejected("Allocation belongs to another project")
    if (
        allocation.fixture_revision != request.fixture_revision
        or allocation.split_revision != request.split_revision
    ):
        raise InvalidTransition(
            "Distribution requires the allocation's bound fixture/split revisions"
        )
    if (
        final_data.digest != allocation.provider_manifest_digest
        or final_data.scope_id != allocation.scope_id
        or final_data.project_reference != project_reference
        or final_data.split_digest != allocation.split_digest
        or final_data.provider != allocation.canonical_provider
    ):
        raise ValueError("Registered final manifest differs from the allocation")
    roots = {
        "aws": (f"s3://{final_data.bucket}/", f"s3c://{final_data.bucket}/"),
        "gcp": (f"gs://{final_data.bucket}/",),
        "azure": (
            f"az://{final_data.azure_account}/{final_data.bucket}/",
            f"azure://{final_data.azure_account}/{final_data.bucket}/",
        ),
    }
    final_uris = {root + o.key for root in roots[final_data.provider] for o in final_data.objects}
    final_hashes = {o.sha256 for o in final_data.objects}
    if any(
        item.uri in final_uris or unquote(item.uri) in final_uris or item.sha256 in final_hashes
        for copy in request.copies
        for item in copy.objects
    ):
        raise ValueError("Safe provider copies overlap registered locked final objects")
    copies = {c.provider: c for c in request.copies}
    manifests = [
        ProviderDistribution(
            allocation_id=allocation.id,
            project_reference=project_reference,
            scope_id=allocation.scope_id,
            fixture_revision=request.fixture_revision,
            split_revision=request.split_revision,
            split_digest=allocation.split_digest,
            canonical_provider=allocation.canonical_provider,
            provider=provider,
            mode="promotional" if provider == allocation.canonical_provider else "validation_only",
            safe_data=copies[provider],
            final_role_digests=final_role_digests(final_data),
            final_data=final_data if provider == allocation.canonical_provider else None,
        )
        for provider in PROVIDERS
    ]
    existing = [authority._receipt(db, allocation_id, "distribution_" + p) for p in PROVIDERS]
    if any(existing):
        if not all(existing):
            raise InvalidTransition("Incomplete provider distribution requires operator review")
        for receipt, manifest in zip(existing, manifests, strict=True):
            authority._same_request(
                receipt, canonical_request_hash(manifest.model_dump(mode="json"))
            )
            if receipt.payload.get("manifest") != manifest.model_dump(
                mode="json"
            ) or not authority.verify_receipt(receipt, signing_key.public_key()):
                raise InvalidTransition("Stored provider distribution is invalid")
        return existing
    if (
        allocation.status != FinalTestStatus.ALLOCATED
        or db.scalar(
            select(FinalTestAuthorityReceipt.id)
            .where(FinalTestAuthorityReceipt.allocation_id == allocation.id)
            .limit(1)
        )
        is not None
    ):
        raise InvalidTransition("Distribution must be registered before refit/evaluation evidence")
    return [
        authority._signed_receipt(
            db,
            allocation=allocation,
            operation="distribution_" + manifest.provider,
            provider=manifest.provider,
            request_digest=canonical_request_hash(manifest.model_dump(mode="json")),
            signing_secret=signing_key,
            payload={"manifest": manifest.model_dump(mode="json")},
        )
        for manifest in manifests
    ]


def verify_distribution(
    receipt,
    public_key,
    *,
    allocation_id,
    project_reference,
    scope_id,
    provider,
    canonical_provider,
    fixture_revision,
    split_revision,
    split_digest,
):
    """Validate a signed copy against independently trusted local references."""
    parsed = DistributionReceipt.model_validate(receipt)
    record = SimpleNamespace(**parsed.model_dump(mode="json"))
    if canonical_request_hash(receipt) != canonical_request_hash(parsed.model_dump(mode="json")):
        raise ValueError("Provider distribution is not in its signed canonical form")
    manifest = parsed.payload.manifest
    if (
        not authority.verify_receipt(record, public_key)
        or parsed.allocation_id != allocation_id
        or manifest.allocation_id != allocation_id
        or parsed.provider != provider
        or manifest.provider != provider
        or parsed.operation != "distribution_" + provider
        or manifest.canonical_provider != canonical_provider
        or manifest.project_reference != project_reference
        or manifest.scope_id != scope_id
        or manifest.fixture_revision != fixture_revision
        or manifest.split_revision != split_revision
        or manifest.split_digest != split_digest
        or parsed.request_digest != canonical_request_hash(manifest.model_dump(mode="json"))
    ):
        raise ValueError("Provider distribution signature or trusted references do not match")
    return manifest
