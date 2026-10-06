"""Attempt-bound refit control and exact-object read capabilities."""

from __future__ import annotations

import hashlib
import hmac
import math
import os
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import quote, urlencode, urlsplit

from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.enums import AttemptStatus, ScopeStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, WorkflowAttempt, WorkflowEvent
from automl_api.security.tokens import create_signed_token, decode_token
from automl_api.services.workflow_state import StaleFence
from automl_api.training.champion_refit import RefitPlan


def _key():
    secret = get_settings().jwt_secret_key
    if len(secret) < 32:
        raise ValueError("Refit control requires a signing key of at least 32 characters")
    return hmac.new(secret.encode(), b"sceptre-refit-control-v1", hashlib.sha256).hexdigest()


def refit_token(attempt, deadline):
    remaining = deadline - datetime.now(UTC)
    if remaining <= timedelta(0) or remaining > timedelta(hours=2):
        raise ValueError("Refit token requires the active scope deadline")
    return create_signed_token(
        subject=str(attempt.id), email="", token_version=0, secret=_key(),
        token_type="champion_refit", expires_delta=remaining,
        extra={"project_id": str(attempt.project_id), "fence": attempt.fencing_token},
    )


def refit_claims(token, attempt_id):
    claims = decode_token(token, secret=_key(), expected_type="champion_refit")
    if claims["sub"] != str(attempt_id):
        raise ValueError("Refit token belongs to another attempt")
    claims["project_id"] = uuid.UUID(claims["project_id"])
    return claims


def locked_refit(db, attempt_id, claims, statuses):
    project_id = claims["project_id"]
    scope_id = db.scalar(select(WorkflowAttempt.scope_id).where(
        WorkflowAttempt.id == attempt_id, WorkflowAttempt.project_id == project_id,
        WorkflowAttempt.stage == WorkflowStage.CHAMPION_REFIT,
    ))
    scope = db.scalar(select(PromotionalScope).where(
        PromotionalScope.id == scope_id, PromotionalScope.project_id == project_id,
    ).with_for_update().execution_options(populate_existing=True))
    attempt = db.scalar(select(WorkflowAttempt).where(
        WorkflowAttempt.id == attempt_id, WorkflowAttempt.project_id == project_id,
    ).with_for_update().execution_options(populate_existing=True))
    now = datetime.now(UTC)
    if (scope is None or attempt is None or attempt.fencing_token != claims["fence"]
        or scope.status != ScopeStatus.RUNNING or attempt.status not in statuses
        or scope.scope_deadline_at is None or scope.scope_deadline_at <= now
        or (attempt.status == AttemptStatus.RUNNING and (
            attempt.lease_expires_at is None or attempt.lease_expires_at <= now
        ))):
        raise StaleFence("Refit capability no longer owns an active attempt")
    event = db.scalar(select(WorkflowEvent).where(
        WorkflowEvent.attempt_id == attempt_id, WorkflowEvent.event_key == "refit_plan",
    ))
    if event is None:
        raise StaleFence("Refit execution plan is missing")
    plan = RefitPlan.model_validate(event.payload)
    if (plan.project_id != project_id or plan.attempt_id != attempt_id
        or plan.scope_id != scope_id or plan.run_id != attempt.model_run_id
        or plan.dataset_version_id != attempt.dataset_version_id
        or plan.digest != event.result_digest
        or plan.policy_digest != scope.comparison_policy.get("refit_policy_digest")):
        raise StaleFence("Refit capability no longer matches its execution plan")
    return scope, attempt, plan


def mint_refit_read_url(store, item, deadline):
    """Issue GET-only access to one approved input, pinning provider versions when present."""
    metadata = store.stat(item.uri)
    if metadata.byte_size != item.byte_size:
        raise ValueError("Refit input size changed")
    now = datetime.now(UTC)
    expires = min(deadline, now + timedelta(minutes=15))
    seconds = math.floor((expires - now).total_seconds())
    if seconds <= 0:
        raise StaleFence("Refit input grant expired")
    key = store._key_from_uri(item.uri)
    if store.driver_name in {"aws_s3", "s3_compatible"}:
        params = {"Bucket": store.bucket, "Key": key}
        version = metadata.provider_headers.get("version_id")
        if version and version != "null":
            params["VersionId"] = version
        url = store.presign_client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=seconds, HttpMethod="GET",
        )
    elif store.driver_name == "gcs":
        from google.auth.credentials import Signing
        from google.auth.transport.requests import Request

        signing = {}
        if not isinstance(store.client._credentials, Signing):
            account = os.environ["REFIT_GCP_SIGNING_ACCOUNT"]
            store.client._credentials.refresh(Request())
            signing = {"service_account_email": account,
                       "access_token": store.client._credentials.token}
        url = store.bucket.blob(key).generate_signed_url(
            version="v4", expiration=expires, method="GET",
            generation=int(metadata.provider_headers["generation"]), **signing,
        )
    elif store.driver_name == "azure_blob":
        from azure.storage.blob import BlobSasPermissions

        version = metadata.provider_headers.get("version_id")
        delegation = store.service.get_user_delegation_key(now - timedelta(minutes=1), expires)
        params = {"version_id": version} if version else {}
        sas = store.sas_factory(
            account_name=store.account_name, container_name=store.container_name,
            blob_name=key, user_delegation_key=delegation,
            permission=BlobSasPermissions(read=True), expiry=expires,
            start=now - timedelta(minutes=1), protocol="https", **params,
        )
        url = f"{store.account_url}/{store.container_name}/{quote(key, safe='/')}?"
        if version:
            url += urlencode({"versionid": version}) + "&"
        url += sas
    else:
        raise ValueError("Refit workloads require a cloud or S3-compatible object store")
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.netloc or parts.username or parts.fragment:
        raise ValueError("Refit inputs require HTTPS capabilities")
    return {"url": url, "expires_at": expires.isoformat()}
