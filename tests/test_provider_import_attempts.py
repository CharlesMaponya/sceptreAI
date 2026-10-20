"""Pending signed imports have preparation authority before READY, without raw roles."""

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlencode

import jwt
import pytest
from automl_api.core.config import get_settings
from automl_api.models.datasets import DatasetProviderImport, DatasetVersion
from automl_api.models.enums import AttemptStatus, CommandStatus, DatasetStatus
from automl_api.models.workflows import (
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
    WorkflowCommand,
)
from automl_api.services import attempt_identity, attempt_object_grants, provider_source
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.storage.s3 import S3ObjectStoreDriver
from cryptography.hazmat.primitives import serialization
from sqlalchemy import delete, select
from test_provider_import_registration import begin, run
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def pending_case(registration_case, tmp_path, monkeypatch):
    c = registration_case
    c.version_id, c.import_id, c.command_id = begin(c)
    c.registry = tmp_path / "approved-policies.json"
    c.registry.write_text(json.dumps({"policies": [c.policy.model_dump(mode="json")]}))
    public = tmp_path / "authority.pem"
    public.write_bytes(
        c.key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    monkeypatch.setenv("PROVIDER_IMPORT_POLICIES_FILE", str(c.registry))
    monkeypatch.setenv("EVALUATION_AUTHORITY_PUBLIC_KEY_FILE", str(public))
    try:
        yield c
    finally:
        # The signer doubles never issued a real native credential.
        with c.factory() as db, db.begin():
            db.execute(
                delete(WorkflowAttemptCredentialGrant).where(
                    WorkflowAttemptCredentialGrant.project_id == c.project_id
                )
            )


def bind_identity(c, monkeypatch):
    with c.factory() as db:
        attempt = verification_attempt(db, db.get(DatasetProviderImport, c.import_id))
        c.attempt_id, c.fence = attempt.id, attempt.fencing_token
        service_account = attempt.workload_identity
    namespace = get_settings().training_namespace
    pod_uid = str(uuid.uuid4())
    c.token = jwt.encode(
        {"exp": int(datetime.now(UTC).timestamp()) + 600},
        "unit-import-token-at-least-32-bytes",
        algorithm="HS256",
    )
    review = SimpleNamespace(
        status=SimpleNamespace(
            authenticated=True,
            audiences=[attempt_identity.attempt_audience(c.attempt_id, c.fence)],
            user=SimpleNamespace(
                username=f"system:serviceaccount:{namespace}:{service_account}",
                extra={
                    "authentication.kubernetes.io/pod-name": ["verifier"],
                    "authentication.kubernetes.io/pod-uid": [pod_uid],
                },
            ),
        )
    )
    c.auth = MagicMock()
    c.auth.create_token_review.return_value = review
    monkeypatch.setattr(attempt_identity.client, "AuthenticationV1Api", lambda: c.auth)
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            uid=pod_uid,
            namespace=namespace,
            deletion_timestamp=None,
            labels={
                "automl.platform/project-id": str(c.project_id),
                "automl.platform/attempt-id": str(c.attempt_id),
                "automl.platform/generation": "1",
            },
        ),
        spec=SimpleNamespace(service_account_name=service_account),
    )
    c.k8s = SimpleNamespace(core=SimpleNamespace(read_namespaced_pod=lambda *_a, **_kw: pod))
    c.signer = MagicMock(
        side_effect=lambda *args, **kw: (
            "https://objects.invalid/input?"
            + urlencode(
                {
                    "X-Amz-Date": datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"),
                    "X-Amz-Expires": kw["ExpiresIn"],
                    "versionId": kw["Params"]["VersionId"],
                    "unit-signature": "never-persist",
                }
            )
        )
    )
    c.capability_store = S3ObjectStoreDriver(
        bucket="safe",
        compatible=True,
        endpoint_url="https://objects.invalid",
        access_key="unit-key",
        secret_key="unit-secret",
        client=SimpleNamespace(head_object=lambda **_kw: pytest.fail("Must not stat latest")),
        presign_client=SimpleNamespace(generate_presigned_url=c.signer),
    )


def mint(c):
    return attempt_object_grants.issue_input_grant(
        c.factory,
        c.attempt_id,
        c.token,
        c.k8s,
        {"request_id": uuid.uuid4(), "role": "train", "object_index": 0},
        store_factory=lambda: c.capability_store,
    )


def test_pending_signed_safe_grant_precedes_verified_registration_and_is_retired(
    pending_case, monkeypatch
):
    c = pending_case
    grants = []

    def during_read():
        if grants:
            return
        assert c.engine.pool.checkedout() == 0
        bind_identity(c, monkeypatch)
        with c.factory() as db, db.begin():
            row = db.get(DatasetProviderImport, c.import_id)
            command = db.get(WorkflowCommand, c.command_id)
            attempt = verification_attempt(db, row)
            assert attempt.lease_owner == command.lease_owner
            assert attempt.lease_expires_at == command.lease_expires_at
            assert row.status == "pending" and row.local_split_revision_id is None
            assert db.get(DatasetVersion, c.version_id).status == DatasetStatus.UPLOADED
            plan = provider_source.require_verification_source(db, attempt)
            assert plan.manifest.final_data is None and plan.manifest.mode == "validation_only"
            with pytest.raises(ValueError):
                provider_source.require_prepared_source(db, db.get(DatasetVersion, c.version_id))
        grants.append(mint(c))
        assert grants[0]["url"].startswith("https://objects.invalid/")
        item = next(o for o in c.value.safe_data.objects if o.role == "train")
        assert c.signer.call_args.kwargs["Params"] == {
            "Bucket": "safe",
            "Key": item.uri.split("/safe/", 1)[1],
            "VersionId": item.version,
        }
        assert c.signer.call_args.kwargs["HttpMethod"] == "GET"

    c.store.before_read = during_read
    assert run(c, c.version_id)
    assert len(grants) == 1
    with c.factory() as db:
        row = db.get(DatasetProviderImport, c.import_id)
        attempt = verification_attempt(db, row)
        grant = db.get(WorkflowAttemptCredentialGrant, uuid.UUID(grants[0]["grant_id"]))
        assert row.status == "verified" and attempt.status == AttemptStatus.SUCCEEDED
        assert grant.status == "revoked" and grant.issued_at is not None
        assert grant.expires_at.isoformat() == grants[0]["expires_at"]
        assert "never-persist" not in str(vars(grant))
    with pytest.raises(ValueError):
        mint(c)
    c.signer.assert_called_once()


def test_transient_verification_retries_append_attempt_history(pending_case):
    c = pending_case
    c.store.before_read = lambda: (_ for _ in ()).throw(OSError("Transient verifier I/O"))
    with pytest.raises(OSError):
        run(c, c.version_id)
    with c.factory() as db, db.begin():
        attempt = verification_attempt(db, db.get(DatasetProviderImport, c.import_id))
        original_id, original_fence = attempt.id, attempt.fencing_token
        assert attempt.status == AttemptStatus.SUPERSEDED and attempt.lease_owner is None
        db.get(WorkflowCommand, c.command_id).available_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    c.store.before_read = lambda: None
    assert run(c, c.version_id)
    with c.factory() as db:
        current = verification_attempt(db, db.get(DatasetProviderImport, c.import_id))
        old = db.get(WorkflowAttempt, original_id)
        assert current.id != old.id and current.generation == old.generation + 1
        assert current.predecessor_attempt_id == old.id and current.fencing_token != original_fence
        assert old.fencing_token == original_fence and old.status == AttemptStatus.SUPERSEDED
        assert current.status == AttemptStatus.SUCCEEDED
        assert db.get(WorkflowCommand, c.command_id).retry_count == 2


@pytest.mark.parametrize(
    "fault",
    [
        "command_owner",
        "command_expiry",
        "attempt_owner",
        "attempt_expiry",
        "identity",
        "source_kind",
        "status",
        "receipt",
        "policy",
    ],
)
def test_pending_authority_changes_cannot_mint(pending_case, monkeypatch, fault):
    c = pending_case

    def mutate_and_request():
        bind_identity(c, monkeypatch)
        with c.factory() as db, db.begin():
            attempt = db.get(WorkflowAttempt, c.attempt_id)
            command = db.get(WorkflowCommand, c.command_id)
            if fault == "command_owner":
                command.lease_owner = "other-owner"
            elif fault == "command_expiry":
                command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif fault == "attempt_owner":
                attempt.lease_owner = "other-owner"
            elif fault == "attempt_expiry":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif fault == "identity":
                attempt.workload_identity = "unapproved-verifier"
            elif fault == "source_kind":
                db.get(DatasetVersion, c.version_id).source_kind = "raw"
            elif fault == "status":
                db.get(DatasetProviderImport, c.import_id).status = "failed"
            elif fault == "receipt":
                row = db.get(DatasetProviderImport, c.import_id)
                row.distribution_receipt = {**row.distribution_receipt, "signature": "00" * 64}
            else:
                c.registry.write_text(json.dumps({"policies": []}))
        with pytest.raises(ValueError):
            mint(c)
        c.signer.assert_not_called()
        raise OSError("Stop this rejected verifier fixture")

    c.store.before_read = mutate_and_request
    with pytest.raises(OSError):
        run(c, c.version_id)
    assert not c.store.writes
    with c.factory() as db:
        assert db.get(DatasetProviderImport, c.import_id).status != "verified"
        assert (
            list(
                db.scalars(
                    select(WorkflowAttemptCredentialGrant).where(
                        WorkflowAttemptCredentialGrant.project_id == c.project_id
                    )
                )
            )
            == []
        )


@pytest.mark.parametrize("stop", ["cancel", "budget"])
def test_expired_process_loss_can_be_cancelled_or_exhausted_without_an_active_orphan(
    pending_case, stop
):
    from automl_api.services.provider_import import cancel_provider_import

    c = pending_case

    class ProcessDeath(BaseException):
        pass

    def die():
        raise ProcessDeath()

    with pytest.raises(ProcessDeath):
        run(c, c.version_id, store_factory=die)
    with c.factory() as db, db.begin():
        row = db.get(DatasetProviderImport, c.import_id)
        command = db.get(WorkflowCommand, c.command_id)
        attempt = verification_attempt(db, row)
        original_id, original_fence = attempt.id, attempt.fencing_token
        assert attempt.status == AttemptStatus.RUNNING
        assert attempt.lease_expires_at == command.lease_expires_at
        deadline = datetime.now(UTC) - timedelta(seconds=1)
        attempt.lease_expires_at = command.lease_expires_at = deadline
        if stop == "cancel":
            cancel_provider_import(db, c.actor_id, c.version_id)
        else:
            command.max_retries = command.retry_count
    if stop == "budget":
        assert not run(
            c,
            c.version_id,
            store_factory=lambda: pytest.fail("Exhausted verifier constructed storage"),
        )
    with c.factory() as db:
        row = db.get(DatasetProviderImport, c.import_id)
        command = db.get(WorkflowCommand, c.command_id)
        attempt = verification_attempt(db, row)
        assert (
            row.status == "failed"
            and db.get(DatasetVersion, c.version_id).status == DatasetStatus.FAILED
        )
        assert command.status == (
            CommandStatus.CANCELLED if stop == "cancel" else CommandStatus.FAILED
        )
        assert attempt.id == original_id and attempt.fencing_token == original_fence
        assert attempt.status == AttemptStatus.CANCELLED
        assert attempt.lease_owner is None and attempt.lease_expires_at is None
    assert not c.store.calls and not c.store.writes


def test_competing_import_execution_cannot_claim_another_attempt_or_construct_storage(pending_case):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    c = pending_case
    entered, release = Event(), Event()

    def before_read():
        entered.set()
        assert release.wait(10), "Concurrent fixture did not release verification"

    c.store.before_read = before_read
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(run, c, c.version_id)
        try:
            assert entered.wait(10), "First import never reached its committed claim"
            second = pool.submit(
                run,
                c,
                c.version_id,
                store_factory=lambda: pytest.fail("Competing verifier constructed storage"),
            )
            assert second.result(timeout=10) is False
            with c.factory() as db:
                row = db.get(DatasetProviderImport, c.import_id)
                attempt = verification_attempt(db, row)
                command = db.get(WorkflowCommand, c.command_id)
                assert attempt.lease_owner == command.lease_owner
                assert attempt.status == AttemptStatus.RUNNING
                assert command.retry_count == 1
                attempts = list(
                    db.scalars(
                        select(WorkflowAttempt).where(
                            WorkflowAttempt.project_id == c.project_id,
                            WorkflowAttempt.logical_key == attempt.logical_key,
                        )
                    )
                )
                assert len(attempts) == 1
        finally:
            release.set()
        assert first.result(timeout=10) is True


def test_expired_command_without_a_matching_attempt_does_not_authorize_cancellation(pending_case):
    from automl_api.services.provider_import import cancel_provider_import
    from automl_api.services.workflow_state import StaleFence

    c = pending_case
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, c.command_id)
        command.status = CommandStatus.RUNNING
        command.lease_owner = "unqualified-legacy-owner"
        command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        with pytest.raises(StaleFence, match="importer to stop"):
            cancel_provider_import(db, c.actor_id, c.version_id)
        assert command.status == CommandStatus.RUNNING
        assert db.get(DatasetProviderImport, c.import_id).status == "pending"
    assert not c.store.calls and not c.store.writes
