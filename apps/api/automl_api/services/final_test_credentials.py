"""One-shot, version-pinned final-data grants owned by the central authority."""

from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from urllib.parse import quote, urlencode, urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from automl_api.models.enums import FinalTestStatus
from automl_api.services import final_test_authority as authority
from automl_api.services.workflow_state import InvalidTransition, canonical_request_hash

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class FinalObject(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    role: Literal["inputs", "labels"]
    key: Annotated[str, Field(min_length=1, max_length=1024)]
    version: Annotated[str, Field(min_length=1, max_length=256)]
    sha256: Digest
    byte_size: Annotated[int, Field(gt=0)]

    @model_validator(mode="after")
    def immutable_key(self):
        if self.key.startswith("/") or any(p in {"", ".", ".."} for p in self.key.split("/")):
            raise ValueError("Final object keys must be canonical relative keys")
        if self.version.lower() == "null":
            raise ValueError("Final objects require immutable provider versions")
        return self


class FinalDataTemplate(BaseModel):
    """Exact data declaration before the application assigns a scope ID."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    project_reference: Annotated[str, Field(min_length=1, max_length=255)]
    split_digest: Digest
    provider: Literal["aws", "gcp", "azure"]
    bucket: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9._-]{1,221}[a-z0-9]$")]
    azure_account: Annotated[str, Field(pattern=r"^[a-z0-9]{3,24}$")] | None = None
    objects: Annotated[tuple[FinalObject, ...], Field(min_length=2, max_length=10000)]

    @model_validator(mode="after")
    def separate_roles(self):
        if {item.role for item in self.objects} != {"inputs", "labels"}:
            raise ValueError("Final inputs and labels must both be registered")
        if len({item.key for item in self.objects}) != len(self.objects):
            raise ValueError("Final inputs and labels must be distinct objects")
        if self.provider == "azure" and not self.azure_account:
            raise ValueError("Azure final data requires an account")
        if self.provider == "gcp" and any(not o.version.isdecimal() for o in self.objects):
            raise ValueError("GCS final data requires numeric object generations")
        return self

    @property
    def digest(self):
        return canonical_request_hash(self.model_dump(mode="json"))


class FinalDataManifest(FinalDataTemplate):
    """Installed by the qualification operator, never supplied by the evaluator."""

    scope_id: uuid.UUID


def mint_read_urls(manifest: FinalDataManifest, expires_at: datetime) -> list[str]:
    """Native read-only capabilities; no bucket listing, writing, or unversioned reads."""
    remaining_seconds = (expires_at - datetime.now(UTC)).total_seconds()
    remaining = math.floor(remaining_seconds)
    if remaining <= 0 or remaining_seconds > 900:
        raise ValueError("Final-data grant expiry is invalid")
    if manifest.provider == "aws":
        import boto3
        from botocore.config import Config

        endpoint = os.getenv("QUALIFICATION_S3_ENDPOINT")
        if endpoint and urlsplit(endpoint).scheme != "https":
            raise ValueError("Final-data object endpoints require HTTPS")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            config=Config(signature_version="s3v4", retries={"max_attempts": 0}),
        )
        return [
            client.generate_presigned_url(
                "get_object",
                Params={"Bucket": manifest.bucket, "Key": obj.key, "VersionId": obj.version},
                ExpiresIn=remaining,
                HttpMethod="GET",
            )
            for obj in manifest.objects
        ]
    if manifest.provider == "gcp":
        from google.auth.credentials import Signing
        from google.auth.transport.requests import Request
        from google.cloud import storage

        client = storage.Client()
        signing = {}
        if not isinstance(client._credentials, Signing):
            account = os.environ["QUALIFICATION_GCP_SIGNING_ACCOUNT"]
            client._credentials.refresh(Request())
            signing = {"service_account_email": account, "access_token": client._credentials.token}
        return [
            client.bucket(manifest.bucket)
            .blob(obj.key)
            .generate_signed_url(
                version="v4",
                expiration=expires_at,
                method="GET",
                generation=int(obj.version),
                **signing,
            )
            for obj in manifest.objects
        ]
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

    account_url = f"https://{manifest.azure_account}.blob.core.windows.net"
    now = datetime.now(UTC)
    with (
        DefaultAzureCredential() as credential,
        BlobServiceClient(account_url=account_url, credential=credential) as client,
    ):
        key = client.get_user_delegation_key(now - timedelta(minutes=1), expires_at)
        return [
            f"{account_url}/{manifest.bucket}/{quote(obj.key, safe='/')}?"
            + urlencode({"versionid": obj.version})
            + "&"
            + generate_blob_sas(
                account_name=manifest.azure_account,
                container_name=manifest.bucket,
                blob_name=obj.key,
                version_id=obj.version,
                user_delegation_key=key,
                permission=BlobSasPermissions(read=True),
                expiry=expires_at,
                start=now - timedelta(minutes=1),
                protocol="https",
            )
            for obj in manifest.objects
        ]


def issue_final_grant(
    session_factory,
    *,
    allocation_id,
    principal,
    manifest,
    signing_key,
    mint=mint_read_urls,
):
    """Commit consumption before minting, then commit its receipt before disclosure.

    A lost response sacrifices availability: neither this attempt nor a replacement
    may mint again. URLs are never persisted, logged, or returned by replay.
    """
    if principal.role != "provider":
        raise authority.ProviderRejected("Final-data credentials require an evaluator")
    binding = authority._evaluation_binding(
        principal.evaluator_attempt_id, principal.frozen_pipeline_digest
    )
    expires_at = min(principal.expires_at, datetime.now(UTC) + timedelta(minutes=15))
    request_digest = canonical_request_hash({"manifest": manifest.digest, **binding})
    with session_factory() as db, db.begin():
        allocation = authority._locked_allocation(db, allocation_id)
        if (
            allocation.project_reference != principal.project_reference
            or allocation.project_reference != manifest.project_reference
            or allocation.scope_id != principal.scope_id
            or allocation.scope_id != manifest.scope_id
            or allocation.split_digest != manifest.split_digest
        ):
            raise authority.ProviderRejected("Final-data grant lineage does not match")
        authority._authorize(allocation, principal.provider, manifest.digest)
        authority._require_registered_evaluator(db, allocation.id, binding)
        if manifest.provider != principal.provider:
            raise authority.ProviderRejected("Final-data provider does not match")
        if authority._receipt(db, allocation_id, "grant_claim") is not None:
            raise InvalidTransition("Final-data grant has already been consumed; never reissue")
        if allocation.status not in {FinalTestStatus.ALLOCATED, FinalTestStatus.OPENED}:
            raise InvalidTransition("Final-data allocation is terminal")
        if expires_at <= datetime.now(UTC):
            raise authority.ProviderRejected("Evaluator identity has expired")
        # Opening and consuming the mint permission share one durable transaction.
        opened = authority._receipt(db, allocation_id, "open")
        if opened is None:
            authority.open_allocation(
                db,
                allocation_id=allocation_id,
                provider=principal.provider,
                provider_manifest_digest=manifest.digest,
                request_digest=request_digest,
                signing_secret=signing_key,
                **binding,
            )
        else:
            authority._same_evaluator(opened, binding)
        authority._signed_receipt(
            db,
            allocation=allocation,
            operation="grant_claim",
            provider=principal.provider,
            request_digest=request_digest,
            signing_secret=signing_key,
            payload={"expires_at": expires_at, "manifest_digest": manifest.digest, **binding},
        )

    try:
        urls = mint(manifest, expires_at)
        if len(urls) != len(manifest.objects) or any(
            urlsplit(url).scheme != "https" or not urlsplit(url).hostname for url in urls
        ):
            raise ValueError("Object broker returned invalid final-data capabilities")
        with session_factory() as db, db.begin():
            allocation = authority._locked_allocation(db, allocation_id)
            if allocation.status != FinalTestStatus.OPENED or expires_at <= datetime.now(UTC):
                raise InvalidTransition("Final-data allocation closed or expired during mint")
            receipt = authority._signed_receipt(
                db,
                allocation=allocation,
                operation="grant_issued",
                provider=principal.provider,
                request_digest=request_digest,
                signing_secret=signing_key,
                payload={
                    "expires_at": expires_at,
                    "manifest_digest": manifest.digest,
                    **binding,
                    "capabilities_digest": hashlib.sha256(
                        json.dumps(urls, separators=(",", ":")).encode()
                    ).hexdigest(),
                },
            )
            result = {
                "expires_at": expires_at.isoformat(),
                "manifest": manifest.model_dump(mode="json"),
                "urls": urls,
                "receipt": {
                    field: getattr(receipt, field)
                    for field in (
                        "allocation_id",
                        "operation",
                        "provider",
                        "request_digest",
                        "payload",
                        "receipt_digest",
                        "signature_algorithm",
                        "signature",
                    )
                },
            }
        return result
    except Exception:
        # Never put a provider exception (which can contain a signed URL) in evidence.
        # If this transaction also fails, grant_claim still prevents a second mint.
        with session_factory() as db, db.begin():
            allocation = authority._locked_allocation(db, allocation_id)
            if allocation.status == FinalTestStatus.OPENED:
                authority.fail_allocation(
                    db,
                    allocation_id=allocation_id,
                    provider=principal.provider,
                    provider_manifest_digest=manifest.digest,
                    request_digest=request_digest,
                    signing_secret=signing_key,
                    expected_cas_version=allocation.cas_version,
                    reason="Final-data credential issuance failed or acknowledgement was lost",
                    **binding,
                )
        raise


def expire_unacknowledged_grants(db, *, signing_key, now=None, limit=100):
    """Reconcile mint-process death after claim without creating another capability."""
    from sqlalchemy import exists, select

    from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt

    now = now or datetime.now(UTC)
    allocations = db.scalars(
        select(FinalTestAllocation)
        .where(
            FinalTestAllocation.status == FinalTestStatus.OPENED,
            exists().where(
                FinalTestAuthorityReceipt.allocation_id == FinalTestAllocation.id,
                FinalTestAuthorityReceipt.operation == "grant_claim",
            ),
            ~exists().where(
                FinalTestAuthorityReceipt.allocation_id == FinalTestAllocation.id,
                FinalTestAuthorityReceipt.operation == "grant_issued",
            ),
        )
        .order_by(FinalTestAllocation.opened_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    expired = 0
    for allocation in allocations:
        claim = authority._receipt(db, allocation.id, "grant_claim")
        if datetime.fromisoformat(claim.payload["expires_at"]) > now:
            continue
        binding = {
            key: claim.payload[key]
            for key in (
                "evaluator_attempt_id",
                "frozen_pipeline_digest",
            )
        }
        authority.fail_allocation(
            db,
            allocation_id=allocation.id,
            provider=allocation.canonical_provider,
            provider_manifest_digest=allocation.provider_manifest_digest,
            request_digest=canonical_request_hash({"expired_grant": claim.receipt_digest}),
            signing_secret=signing_key,
            expected_cas_version=allocation.cas_version,
            reason="Final-data credential issuance was not acknowledged before expiry",
            **binding,
        )
        expired += 1
    return expired
