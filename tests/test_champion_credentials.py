from __future__ import annotations

import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from automl_api.models.enums import FinalTestStatus
from automl_api.models.qualification import FinalTestAllocation, FinalTestAuthorityReceipt
from automl_api.qualification_control import create_app
from automl_api.services.final_test_authority import verify_receipt
from automl_api.services.final_test_credentials import FinalDataManifest, mint_read_urls
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_qualification_control import headers

pytest_plugins = ["test_qualification_control"]


def manifest_for(scope):
    return FinalDataManifest(
        project_reference=f"authority-test-{scope}",
        scope_id=scope,
        split_digest=hashlib.sha256(str(scope).encode()).hexdigest(),
        provider="aws",
        bucket="locked-final-data",
        objects=tuple(
            dict(
                role=role,
                key=f"final/{role}.parquet",
                version=f"version-{role}",
                sha256="a" * 64,
                byte_size=10,
            )
            for role in ("inputs", "labels")
        ),
    )


def grant_client(scope, kwargs, mint):
    manifest = manifest_for(scope)
    client = TestClient(create_app(**kwargs, manifests=[manifest], mint=mint))
    response = client.post(
        "/allocations",
        headers=headers("allocator"),
        json=dict(
            split_digest=manifest.split_digest,
            scope_id=str(scope),
            canonical_provider="aws",
            provider_manifest_digest=manifest.digest,
        ),
    )
    assert response.status_code == 200, response.text
    path = f"/allocations/{response.json()['allocation_id']}"
    return client, manifest, path


def test_one_shot_credentials_race_and_restart(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock(
        return_value=["https://objects.test/inputs?secret", "https://objects.test/labels?secret"]
    )
    client, manifest, path = grant_client(scope, kwargs, mint)
    payload = {"provider_manifest_digest": manifest.digest}
    # Positive control follows the rejected cross-project/provider/scope requests.
    for name in ("allocator", "gcp", "azure", "other-project", "other-scope"):
        assert (
            client.post(path + "/credentials", headers=headers(name), json=payload).status_code
            == 403
        )
    mint.assert_not_called()
    # A successful result must not be accepted before any data grant exists.
    assert (
        client.post(
            path + "/commit",
            headers=headers("aws"),
            json={
                **payload,
                "result_digest": "b" * 64,
            },
        ).status_code
        == 409
    )
    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(
            pool.map(
                lambda _: client.post(
                    path + "/credentials",
                    headers=headers("aws"),
                    json=payload,
                ),
                range(6),
            )
        )
    assert sorted(r.status_code for r in responses) == [200, 409, 409, 409, 409, 409]
    mint.assert_called_once()
    success = next(r for r in responses if r.status_code == 200)
    assert success.headers["Cache-Control"] == "no-store"
    grant = success.json()
    receipt = SimpleNamespace(**grant["receipt"])
    assert verify_receipt(receipt, kwargs["signing_secret"].public_key())
    assert receipt.payload["evaluator_attempt_id"] == str(scope)
    assert receipt.payload["frozen_pipeline_digest"] == "d" * 64
    assert grant["manifest"] == manifest.model_dump(mode="json")
    with kwargs["session_factory"]() as db:
        stored = list(
            db.scalars(
                select(FinalTestAuthorityReceipt).where(
                    FinalTestAuthorityReceipt.allocation_id == uuid.UUID(path.rsplit("/", 1)[1])
                )
            )
        )
        assert {row.operation for row in stored} == {"open", "grant_claim", "grant_issued"}
        assert "?secret" not in str([row.payload for row in stored])
    with TestClient(create_app(**kwargs, manifests=[manifest], mint=mint)) as restarted:
        replay = restarted.post(path + "/credentials", headers=headers("aws"), json=payload)
        assert replay.status_code == 409
        assert "urls" not in replay.json()
        assert (
            restarted.post(
                path + "/commit",
                headers=headers("aws"),
                json={
                    **payload,
                    "result_digest": "b" * 64,
                },
            ).status_code
            == 200
        )
    mint.assert_called_once()


def test_mint_failure_seals_scope_and_redacts_secrets(authority_api):
    _, scope, kwargs = authority_api
    mint = Mock(side_effect=RuntimeError("https://objects.test?secret=do-not-disclose"))
    client, manifest, path = grant_client(scope, kwargs, mint)
    payload = {"provider_manifest_digest": manifest.digest}
    response = client.post(path + "/credentials", headers=headers("aws"), json=payload)
    assert response.status_code == 503
    assert "do-not-disclose" not in response.text
    assert (
        client.post(path + "/credentials", headers=headers("aws"), json=payload).status_code == 409
    )
    with kwargs["session_factory"]() as db:
        row = db.get(FinalTestAllocation, uuid.UUID(path.rsplit("/", 1)[1]))
        assert row.status == FinalTestStatus.FAILED
        assert "do-not-disclose" not in row.terminal_reason
    mint.assert_called_once()


def test_crash_after_claim_never_reissues(authority_api):
    _, scope, kwargs = authority_api

    class ProcessDeath(BaseException):
        pass

    mint = Mock(side_effect=ProcessDeath())
    client, manifest, path = grant_client(scope, kwargs, mint)
    payload = {"provider_manifest_digest": manifest.digest}
    # BaseException bypasses the normal error handler, like a killed worker.
    with pytest.raises(ProcessDeath):
        client.post(path + "/credentials", headers=headers("aws"), json=payload)
    with TestClient(create_app(**kwargs, manifests=[manifest], mint=mint)) as restarted:
        assert (
            restarted.post(path + "/credentials", headers=headers("aws"), json=payload).status_code
            == 409
        )
    mint.assert_called_once()
    with kwargs["session_factory"]() as db:
        row = db.get(FinalTestAllocation, uuid.UUID(path.rsplit("/", 1)[1]))
        assert row.status == FinalTestStatus.OPENED
        assert row.opened_at is not None
        operations = list(
            db.scalars(
                select(FinalTestAuthorityReceipt.operation).where(
                    FinalTestAuthorityReceipt.allocation_id == row.id
                )
            )
        )
        assert set(operations) == {"open", "grant_claim"}
    # Removing the deployment manifest cannot bypass durable grant consumption.
    with TestClient(create_app(**kwargs)) as without_manifest:
        assert (
            without_manifest.post(
                path + "/commit",
                headers=headers("aws"),
                json={**payload, "result_digest": "b" * 64},
            ).status_code
            == 409
        )


def test_s3_grant_pins_versions_and_read_method(monkeypatch):
    import boto3

    client = Mock()
    client.generate_presigned_url.return_value = "https://example.test/version"
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    manifest = manifest_for(uuid.uuid4())
    assert len(mint_read_urls(manifest, datetime.now(UTC) + timedelta(minutes=5))) == 2
    for call, obj in zip(
        client.generate_presigned_url.call_args_list, manifest.objects, strict=True
    ):
        assert call.args == ("get_object",)
        assert call.kwargs["HttpMethod"] == "GET"
        assert call.kwargs["Params"] == {
            "Bucket": manifest.bucket,
            "Key": obj.key,
            "VersionId": obj.version,
        }
        assert 0 < call.kwargs["ExpiresIn"] <= 300


@pytest.mark.parametrize(
    "change",
    [
        {"version": "null"},
        {"key": "../labels"},
        {"key": "/absolute"},
    ],
)
def test_final_data_requires_canonical_immutable_objects(change):
    manifest = manifest_for(uuid.uuid4()).model_dump(mode="json")
    manifest["objects"][0].update(change)
    with pytest.raises(ValueError):
        FinalDataManifest.model_validate(manifest)


def test_expired_unacknowledged_grant_seals_without_remint(authority_api):
    from automl_api.services.final_test_credentials import expire_unacknowledged_grants

    _, scope, kwargs = authority_api

    class ProcessDeath(BaseException):
        pass

    mint = Mock(side_effect=ProcessDeath())
    client, manifest, path = grant_client(scope, kwargs, mint)
    with pytest.raises(ProcessDeath):
        client.post(
            path + "/credentials",
            headers=headers("aws"),
            json={
                "provider_manifest_digest": manifest.digest,
            },
        )
    with kwargs["session_factory"]() as db, db.begin():
        assert expire_unacknowledged_grants(db, signing_key=kwargs["signing_secret"]) == 0
        assert (
            expire_unacknowledged_grants(
                db,
                signing_key=kwargs["signing_secret"],
                now=datetime.now(UTC) + timedelta(minutes=16),
            )
            == 1
        )
    with kwargs["session_factory"]() as db, db.begin():
        assert (
            expire_unacknowledged_grants(
                db,
                signing_key=kwargs["signing_secret"],
                now=datetime.now(UTC) + timedelta(minutes=16),
            )
            == 0
        )
        row = db.get(FinalTestAllocation, uuid.UUID(path.rsplit("/", 1)[1]))
        assert row.status == FinalTestStatus.FAILED
    mint.assert_called_once()


@pytest.mark.parametrize("kill_operation", ["grant_claim", "grant_issued"])
def test_connection_loss_at_credential_commit(authority_api, kill_operation):
    from sqlalchemy import event, text

    _, scope, kwargs = authority_api
    factory = kwargs["session_factory"]
    mint = Mock(return_value=["https://objects.test/inputs", "https://objects.test/labels"])
    client, manifest, path = grant_client(scope, kwargs, mint)
    killed = False

    def disconnect(db):
        nonlocal killed
        if killed:
            return
        # Receipts are flushed before commit; inspect the current transaction.
        has_receipt = db.scalar(
            select(FinalTestAuthorityReceipt.id).where(
                FinalTestAuthorityReceipt.allocation_id == uuid.UUID(path.rsplit("/", 1)[1]),
                FinalTestAuthorityReceipt.operation == kill_operation,
            )
        )
        if has_receipt:
            killed = True
            pid = db.scalar(text("select pg_backend_pid()"))
            with (
                factory.kw["bind"]
                .connect()
                .execution_options(isolation_level="AUTOCOMMIT") as admin
            ):
                assert admin.scalar(text("select pg_terminate_backend(:pid)"), {"pid": pid})

    event.listen(factory.class_, "before_commit", disconnect)
    try:
        response = client.post(
            path + "/credentials",
            headers=headers("aws"),
            json={
                "provider_manifest_digest": manifest.digest,
            },
        )
    finally:
        event.remove(factory.class_, "before_commit", disconnect)
    assert killed
    assert response.status_code == 503
    assert "urls" not in response.json()
    retry = client.post(
        path + "/credentials",
        headers=headers("aws"),
        json={
            "provider_manifest_digest": manifest.digest,
        },
    )
    assert retry.status_code == (200 if kill_operation == "grant_claim" else 409)
    # Before durable consumption nothing was minted; after it, no retry can mint.
    mint.assert_called_once()


def test_azure_grant_is_read_only_and_bound_to_a_blob_version(monkeypatch):
    import base64
    from urllib.parse import parse_qs, urlsplit

    import azure.identity
    import azure.storage.blob

    manifest = manifest_for(uuid.uuid4()).model_dump(mode="json")
    manifest.update(provider="azure", azure_account="qualificationstore")
    manifest["objects"][0]["key"] = "final/input with spaces.parquet"
    delegation = azure.storage.blob.UserDelegationKey()
    delegation.signed_oid = str(uuid.uuid4())
    delegation.signed_tid = str(uuid.uuid4())
    delegation.signed_start = "2026-01-01T00:00:00Z"
    delegation.signed_expiry = "2027-01-01T00:00:00Z"
    delegation.signed_service = "b"
    delegation.signed_version = "2023-11-03"
    delegation.value = base64.b64encode(b"test-only-delegation-signing-key").decode()
    from unittest.mock import MagicMock

    service = MagicMock()
    service.__enter__.return_value.get_user_delegation_key.return_value = delegation
    monkeypatch.setattr(azure.storage.blob, "BlobServiceClient", lambda **kw: service)
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", MagicMock)
    manifest = FinalDataManifest.model_validate(manifest)
    urls = mint_read_urls(manifest, datetime.now(UTC) + timedelta(minutes=5))
    assert "%20" in urls[0]
    for url, obj in zip(urls, manifest.objects, strict=True):
        parts = urlsplit(url)
        params = parse_qs(parts.query)
        assert parts.scheme == "https"
        assert params["sp"] == ["r"]
        assert params["sr"] == ["bv"]
        assert params["spr"] == ["https"]
        assert params["versionid"] == [obj.version]
        assert params["sig"]


def test_gcp_workload_identity_uses_explicit_iam_signer_and_generation(monkeypatch):
    from google.cloud import storage

    manifest = manifest_for(uuid.uuid4()).model_dump(mode="json")
    manifest["provider"] = "gcp"
    for index, obj in enumerate(manifest["objects"]):
        obj["version"] = str(100 + index)
    client = Mock()
    client._credentials.token = "short-lived-iam-token"
    blob = client.bucket.return_value.blob.return_value
    blob.generate_signed_url.return_value = "https://storage.googleapis.com/test"
    monkeypatch.setattr(storage, "Client", lambda: client)
    monkeypatch.setenv(
        "QUALIFICATION_GCP_SIGNING_ACCOUNT", "broker@example.iam.gserviceaccount.com"
    )
    manifest = FinalDataManifest.model_validate(manifest)
    urls = mint_read_urls(manifest, datetime.now(UTC) + timedelta(minutes=5))
    assert len(urls) == 2
    client._credentials.refresh.assert_called_once()
    for call, obj in zip(blob.generate_signed_url.call_args_list, manifest.objects, strict=True):
        assert call.kwargs["method"] == "GET"
        assert call.kwargs["generation"] == int(obj.version)
        assert call.kwargs["version"] == "v4"
        assert call.kwargs["service_account_email"] == "broker@example.iam.gserviceaccount.com"
        assert call.kwargs["access_token"] == "short-lived-iam-token"
    monkeypatch.delenv("QUALIFICATION_GCP_SIGNING_ACCOUNT")
    with pytest.raises(KeyError, match="QUALIFICATION_GCP_SIGNING_ACCOUNT"):
        mint_read_urls(manifest, datetime.now(UTC) + timedelta(minutes=5))


@pytest.mark.parametrize("seconds", [-1, 901])
def test_broker_rejects_invalid_expiry_before_sdk_access(monkeypatch, seconds):
    import boto3

    client = Mock()
    monkeypatch.setattr(boto3, "client", client)
    with pytest.raises(ValueError, match="expiry"):
        mint_read_urls(manifest_for(uuid.uuid4()), datetime.now(UTC) + timedelta(seconds=seconds))
    client.assert_not_called()
