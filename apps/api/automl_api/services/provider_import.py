"""Protected operator registration of signed non-canonical safe dataset copies."""

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from urllib.parse import unquote, urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import Field
from sqlalchemy import select

from automl_api.models.datasets import Dataset, DatasetProviderImport, DatasetVersion
from automl_api.models.enums import (
    AttemptStatus,
    CommandStatus,
    DatasetStatus,
    GlobalRole,
    ObjectStoreType,
    ProjectStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import DatasetSplitRevision, PreparedArtifact, WorkflowCommand
from automl_api.services.provider_copy_verification import (
    CopyVerification,
    CopyVerificationBudget,
    verify_provider_objects,
)
from automl_api.services.provider_distribution import (
    Contract,
    Digest,
    Provider,
    ProviderDistribution,
    Revision,
    verify_distribution,
)
from automl_api.services.workflow_state import (
    InvalidTransition,
    StaleFence,
    begin_command,
    canonical_request_hash,
)

OPERATION = "dataset.provider.import"
SOURCE_KIND = "provider_distribution"
SPLIT_SCOPE = "provider-distribution-safe-v1"


class ImportReferences(Contract):
    allocation_id: uuid.UUID
    project_reference: Annotated[str, Field(min_length=1, max_length=255)]
    scope_id: uuid.UUID
    provider: Provider
    canonical_provider: Provider
    fixture_revision: Revision
    split_revision: Revision
    split_digest: Digest


class ImportPolicy(Contract):
    """Independently approved operator configuration, never a public API payload."""

    project_id: uuid.UUID
    authority_key_digest: Digest
    references: ImportReferences


class ImportIntent(Contract):
    revision: Literal["provider-dataset-import-v1"] = "provider-dataset-import-v1"
    policy: ImportPolicy
    dataset_version_id: uuid.UUID
    dataset_name: Annotated[str, Field(min_length=1, max_length=220)]
    manifest_digest: Digest
    receipt_digest: Digest
    budget: CopyVerificationBudget


def _key_digest(public_key):
    key = (
        serialization.load_pem_public_key(public_key.encode())
        if isinstance(public_key, str)
        else public_key
    )
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError("Provider import requires the trusted Ed25519 authority key")
    return hashlib.sha256(key.public_bytes_raw()).hexdigest()


def _manifest(receipt, policy, public_key):
    if _key_digest(public_key) != policy.authority_key_digest:
        raise ValueError("Provider import authority key differs from operator policy")
    manifest = verify_distribution(receipt, public_key, **policy.references.model_dump())
    if manifest.mode != "validation_only" or manifest.final_data is not None:
        raise ValueError(
            "Only signed non-canonical validation-only copies use the no-raw import path"
        )
    return manifest


def descriptor_bytes(manifest):
    return json.dumps(
        {
            "revision": "provider-distribution-descriptor-v1",
            "manifest": manifest.model_dump(mode="json"),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()


def _operator(db, actor_id, project_id, *, lock=True):
    user = db.get(User, actor_id, populate_existing=True)
    project = db.get(
        Project, project_id, with_for_update=True if lock else None, populate_existing=True
    )
    if (
        user is None
        or not user.is_active
        or user.global_role != GlobalRole.ADMIN
        or project is None
        or project.status != ProjectStatus.ACTIVE
        or (project.settings or {}).get("deletion_in_progress")
    ):
        raise InvalidTransition("Provider import requires an active operator and available project")


def begin_provider_import(db, actor_id, *, policy, receipt, public_key, budget, dataset_name):
    """Caller commits a durable intent before constructing any object-store client."""
    manifest = _manifest(receipt, policy, public_key)
    _operator(db, actor_id, policy.project_id)
    version_id = uuid.uuid5(
        policy.project_id, f"{OPERATION}:{manifest.allocation_id}:{manifest.provider}"
    )
    intent = ImportIntent(
        policy=policy,
        dataset_version_id=version_id,
        dataset_name=dataset_name,
        manifest_digest=canonical_request_hash(manifest.model_dump(mode="json")),
        receipt_digest=canonical_request_hash(receipt),
        budget=budget,
    )
    command, replay = begin_command(
        db,
        project_id=policy.project_id,
        actor_id=actor_id,
        operation=OPERATION,
        idempotency_key=f"{manifest.allocation_id}:{manifest.provider}",
        payload=intent.model_dump(mode="json"),
    )
    if replay:
        version, row, stored = _current(db, version_id)
        _validate(version, row, stored, policy, public_key)
        return row
    dataset_id = uuid.uuid5(version_id, "provider-dataset")
    db.add(
        Dataset(
            id=dataset_id,
            project_id=policy.project_id,
            created_by_id=actor_id,
            name=dataset_name,
            latest_version_number=1,
        )
    )
    db.flush()
    content = descriptor_bytes(manifest)
    if len(content) > 32 * 1024**2:
        raise ValueError("Provider import descriptor exceeds its byte budget")
    version = DatasetVersion(
        id=version_id,
        project_id=policy.project_id,
        dataset_id=dataset_id,
        created_by_id=actor_id,
        version_number=1,
        format="JSON",
        source_kind=SOURCE_KIND,
        object_uri=f"provider-import://{version_id}",
        original_filename="provider-distribution.json",
        object_store_type={
            "aws": ObjectStoreType.AWS_S3,
            "gcp": ObjectStoreType.GCS,
            "azure": ObjectStoreType.AZURE_BLOB,
        }[manifest.provider],
        content_hash=hashlib.sha256(content).hexdigest(),
        byte_size=len(content),
        content_hash_verification_status="pending",
        status=DatasetStatus.UPLOADED,
    )
    command.resource_type = "dataset_version"
    command.resource_id = version_id
    db.add(version)
    db.flush()
    row = DatasetProviderImport(
        project_id=policy.project_id,
        dataset_version_id=version_id,
        command_id=command.id,
        allocation_id=manifest.allocation_id,
        authority_scope_id=manifest.scope_id,
        project_reference=manifest.project_reference,
        fixture_revision=manifest.fixture_revision,
        split_revision=manifest.split_revision,
        provider=manifest.provider,
        canonical_provider=manifest.canonical_provider,
        manifest_digest=intent.manifest_digest,
        authority_key_digest=policy.authority_key_digest,
        distribution_receipt=receipt,
    )
    db.add(row)
    db.flush()
    return row


def _current(db, version_id):
    project_id = db.scalar(select(DatasetVersion.project_id).where(DatasetVersion.id == version_id))
    if project_id is None:
        raise InvalidTransition("Provider import dataset no longer exists")
    db.get(Project, project_id, with_for_update=True, populate_existing=True)
    version = db.get(DatasetVersion, version_id, with_for_update=True, populate_existing=True)
    row = db.scalar(
        select(DatasetProviderImport)
        .where(DatasetProviderImport.dataset_version_id == version_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    command = (
        db.get(WorkflowCommand, row.command_id, with_for_update=True, populate_existing=True)
        if row
        else None
    )
    if version is None or row is None or command is None or version.project_id != project_id:
        raise InvalidTransition("Provider import lineage is missing")
    return version, row, command


def _validate(version, row, command, policy, public_key):
    intent = ImportIntent.model_validate(command.request_payload)
    manifest = _manifest(row.distribution_receipt, policy, public_key)
    content = descriptor_bytes(manifest)
    if (
        intent.policy != policy
        or intent.dataset_version_id != version.id
        or command.request_hash != canonical_request_hash(intent.model_dump(mode="json"))
        or command.request_payload != intent.model_dump(mode="json")
        or command.project_id != row.project_id
        or row.project_id != policy.project_id
        or version.project_id != row.project_id
        or version.source_kind != SOURCE_KIND
        or command.operation != OPERATION
        or command.resource_type != "dataset_version"
        or command.resource_id != version.id
        or command.idempotency_key != f"{manifest.allocation_id}:{manifest.provider}"
        or row.authority_key_digest != policy.authority_key_digest
        or row.manifest_digest != intent.manifest_digest
        or row.manifest_digest != canonical_request_hash(manifest.model_dump(mode="json"))
        or intent.receipt_digest != canonical_request_hash(row.distribution_receipt)
        or row.allocation_id != manifest.allocation_id
        or row.authority_scope_id != manifest.scope_id
        or row.project_reference != manifest.project_reference
        or row.provider != manifest.provider
        or row.canonical_provider != manifest.canonical_provider
        or row.mode != "validation_only"
        or row.fixture_revision != manifest.fixture_revision
        or row.split_revision != manifest.split_revision
        or version.content_hash != hashlib.sha256(content).hexdigest()
        or version.byte_size != len(content)
    ):
        raise InvalidTransition("Provider import differs from its protected intent")
    return intent, manifest, content


def _owned(command, owner):
    if (
        not owner
        or command.status != CommandStatus.RUNNING
        or command.lease_owner != owner
        or command.lease_expires_at is None
        or command.lease_expires_at <= datetime.now(UTC)
    ):
        raise StaleFence("Provider import lease is no longer current")


@dataclass(frozen=True)
class ImportExecution:
    version_id: uuid.UUID
    dataset_id: uuid.UUID
    command_id: uuid.UUID
    actor_id: uuid.UUID
    import_id: uuid.UUID
    owner: str
    attempt_id: uuid.UUID
    fence: str
    intent: ImportIntent
    manifest: ProviderDistribution
    content: bytes


def claim_provider_import(factory, version_id, *, policy, public_key, prepare=None):
    """Commit one execution before any provider or Kubernetes operation."""
    from automl_api.services.provider_import_attempts import (
        claim_verification_attempt,
        stop_verification_attempt,
    )

    owner = uuid.uuid4().hex
    with factory() as db, db.begin():
        version, row, command = _current(db, version_id)
        intent, manifest, content = _validate(version, row, command, policy, public_key)
        _operator(db, command.actor_id, policy.project_id)
        if row.status == "verified" and command.status == CommandStatus.SUCCEEDED:
            _ready(db, version, row, command, manifest)
            return None
        if row.status != "pending" or version.status != DatasetStatus.UPLOADED:
            return None
        now = datetime.now(UTC)
        if (
            command.status not in (CommandStatus.PENDING, CommandStatus.RUNNING)
            or command.available_at > now
            or (command.lease_expires_at is not None and command.lease_expires_at > now)
        ):
            return None
        if command.retry_count >= command.max_retries:
            stop_verification_attempt(db, row, reason="Provider import retry budget exhausted")
            command.status = CommandStatus.FAILED
            row.status = "failed"
            version.status = DatasetStatus.FAILED
            command.terminal_reason = "Provider import retry budget exhausted"
            command.lease_owner = command.lease_expires_at = None
            return None
        command.status = CommandStatus.RUNNING
        command.retry_count += 1
        command.lease_owner = owner
        command.heartbeat_at = now
        command.lease_expires_at = now + timedelta(seconds=90)
        attempt = claim_verification_attempt(db, version, row, command, owner)
        attempt_id, fence = attempt.id, attempt.fencing_token
        execution = ImportExecution(
            version_id=version_id,
            dataset_id=version.dataset_id,
            command_id=command.id,
            actor_id=command.actor_id,
            import_id=row.id,
            owner=owner,
            attempt_id=attempt_id,
            fence=fence,
            intent=intent,
            manifest=manifest,
            content=content,
        )
        if prepare is not None:
            prepare(db, execution, attempt, command)
    return execution


def _execution_state(db, execution, policy, public_key):
    from automl_api.models.enums import WorkflowStage
    from automl_api.services.provider_import_attempts import verification_attempt

    version, row, command = _current(db, execution.version_id)
    intent, manifest, content = _validate(version, row, command, policy, public_key)
    _operator(db, command.actor_id, policy.project_id)
    attempt = verification_attempt(db, row)
    if (
        intent != execution.intent
        or manifest != execution.manifest
        or content != execution.content
        or version.dataset_id != execution.dataset_id
        or command.id != execution.command_id
        or command.actor_id != execution.actor_id
        or row.id != execution.import_id
        or attempt is None
        or attempt.id != execution.attempt_id
        or attempt.fencing_token != execution.fence
        or attempt.stage != WorkflowStage.PREPARATION
        or attempt.dataset_version_id != version.id
        or (
            attempt.status == AttemptStatus.RUNNING
            and (
                attempt.lease_owner != command.lease_owner
                or attempt.lease_expires_at != command.lease_expires_at
            )
        )
    ):
        raise StaleFence("Provider verification execution binding changed")
    return version, row, command


def renew_provider_import(factory, execution, *, policy, public_key, identity_deadline=None):
    from automl_api.services.provider_import_attempts import owned_verification_attempt

    owner = execution.owner
    attempt_id, fence = execution.attempt_id, execution.fence
    with factory() as db, db.begin():
        version, row, command = _execution_state(db, execution, policy, public_key)
        _owned(command, owner)
        attempt = owned_verification_attempt(db, row, attempt_id, fence, owner)
        if row.status != "pending" or version.status != DatasetStatus.UPLOADED:
            raise StaleFence("Provider import source was stopped")
        command.heartbeat_at = datetime.now(UTC)
        command.lease_expires_at = command.heartbeat_at + timedelta(seconds=90)
        if identity_deadline is not None:
            command.lease_expires_at = min(command.lease_expires_at, identity_deadline)
            if command.lease_expires_at <= command.heartbeat_at:
                raise StaleFence("Provider verification identity expired")
        attempt.heartbeat_at, attempt.lease_expires_at = (
            command.heartbeat_at,
            command.lease_expires_at,
        )


def _validate_copy_verification(manifest, proof, manifest_digest):
    roles = manifest.safe_data.logical_roles()[1]
    if (
        proof.manifest_digest != manifest_digest
        or proof.schema_digest != manifest.safe_data.schema_digest
        or canonical_request_hash(
            {
                "revision": "arrow-logical-schema-v1",
                "columns": [c.model_dump() for c in proof.columns],
            }
        )
        != proof.schema_digest
        or proof.objects != len(manifest.safe_data.objects)
        or proof.byte_size != sum(o.byte_size for o in manifest.safe_data.objects)
        or proof.rows != {role: values[0] for role, values in roles.items()}
        or proof.role_digests != {role: values[1] for role, values in roles.items()}
        or proof.verified_at.tzinfo is None
        or proof.verified_at.utcoffset() is None
    ):
        raise InvalidTransition("Provider verification differs from the signed source")


def verification_receipt_payload(version, row, command, attempt, publisher):
    return {
        "project_id": str(row.project_id),
        "attempt_id": str(attempt.id),
        "generation": attempt.generation,
        "fencing_token": attempt.fencing_token,
        "command_id": str(command.id),
        "actor_id": str(command.actor_id),
        "dataset_id": str(version.dataset_id),
        "import_id": str(row.id),
        "request_digest": command.request_hash,
        "publisher_digest": publisher.result_digest,
        "proof_digest": canonical_request_hash(row.verification),
        "response": dict(command.response_payload),
    }


def publish_provider_import(
    factory,
    execution,
    proof,
    *,
    policy,
    public_key,
    check_authority=lambda: None,
):
    """Commit canonical descriptor metadata and verification atomically; perform no storage I/O."""
    from automl_api.models.workflows import WorkflowAttempt
    from automl_api.services.evaluation_publication import _append, _event
    from automl_api.services.provider_import_attempts import (
        finish_verification_attempt,
        owned_verification_attempt,
    )

    version_id, owner = execution.version_id, execution.owner
    attempt_id, fence = execution.attempt_id, execution.fence
    manifest, content = execution.manifest, execution.content
    descriptor_uri = f"provider-import://{version_id}"
    proof = CopyVerification.model_validate(proof)
    _validate_copy_verification(manifest, proof, execution.intent.manifest_digest)
    check_authority()
    with factory() as db, db.begin():
        version, row, command = _execution_state(db, execution, policy, public_key)
        _validate_copy_verification(manifest, proof, row.manifest_digest)
        if row.status == "verified" and command.status == CommandStatus.SUCCEEDED:
            _ready(db, version, row, command, manifest)
            if row.verification != proof.model_dump(mode="json"):
                raise InvalidTransition("Provider verification replay changed its proof")
            return dict(command.response_payload)
        if _event(db, attempt_id, "provider_import.descriptor_requested") is not None:
            raise StaleFence("Legacy descriptor publication outcome must be recovered")
        _owned(command, owner)
        owned_verification_attempt(db, row, attempt_id, fence, owner)
        if row.status != "pending" or version.status != DatasetStatus.UPLOADED:
            raise StaleFence("Provider import source was stopped")
        specification = _specification(version, row, manifest, proof)
        artifact = PreparedArtifact(
            project_id=row.project_id,
            dataset_version_id=version_id,
            revision=1,
            digest_scope=SPLIT_SCOPE,
            content_digest=row.manifest_digest,
            object_uri=descriptor_uri,
            byte_size=len(content),
            row_count=sum(proof.rows.values()),
            specification=specification,
            status="ready",
        )
        split = DatasetSplitRevision(
            project_id=row.project_id,
            dataset_version_id=version_id,
            revision=1,
            digest_scope=SPLIT_SCOPE,
            content_digest=manifest.split_digest,
            train_digest=proof.role_digests["train"],
            validation_digest=proof.role_digests["validation"],
            final_test_digest=canonical_request_hash(
                manifest.final_role_digests.model_dump(mode="json")
            ),
            sealed_at=datetime.now(UTC),
            specification=specification,
        )
        db.add_all([artifact, split])
        db.flush()
        row.local_split_revision_id, row.prepared_artifact_id = split.id, artifact.id
        row.status, row.verification = "verified", proof.model_dump(mode="json")
        version.object_uri, version.status = descriptor_uri, DatasetStatus.READY
        version.content_hash_verification_status = "verified"
        version.content_hash_verified_at = datetime.now(UTC)
        version.row_count = sum(proof.rows.values())
        version.schema_json = {"columns": [c.model_dump() for c in proof.columns]}
        command.status = CommandStatus.SUCCEEDED
        command.response_status = 200
        command.response_payload = {
            "dataset_version_id": str(version_id),
            "split_revision_id": str(split.id),
            "prepared_artifact_id": str(artifact.id),
        }
        if not finish_verification_attempt(
            db, row, attempt_id, fence, owner, AttemptStatus.SUCCEEDED
        ):
            raise StaleFence("Provider verification publication lost its generation")
        command.lease_owner = command.lease_expires_at = None
        command.terminal_reason = None
        response = dict(command.response_payload)
        publisher = _event(db, attempt_id, "provider_import.publisher")
        if publisher is not None:
            if publisher.result_digest != canonical_request_hash(publisher.payload):
                raise StaleFence("Provider verification publisher evidence changed")
            attempt = db.get(WorkflowAttempt, attempt_id)
            _append(
                db,
                attempt,
                "provider_import.receipt",
                verification_receipt_payload(version, row, command, attempt, publisher),
            )
    return response


def fail_provider_import(factory, execution, exc):
    from automl_api.services.provider_import_attempts import finish_verification_attempt

    version_id, owner = execution.version_id, execution.owner
    attempt_id, fence = execution.attempt_id, execution.fence
    with factory() as db, db.begin():
        try:
            version, row, command = _current(db, version_id)
        except InvalidTransition:
            raise exc from None
        if command.lease_owner == owner and command.status == CommandStatus.RUNNING:
            terminal = (
                version.status != DatasetStatus.UPLOADED
                or row.status != "pending"
                or (isinstance(exc, ValueError) and not isinstance(exc, StaleFence))
                or command.retry_count >= command.max_retries
            )
            finish_verification_attempt(
                db,
                row,
                attempt_id,
                fence,
                owner,
                AttemptStatus.FAILED if terminal else AttemptStatus.SUPERSEDED,
            )
            command.status = CommandStatus.FAILED if terminal else CommandStatus.PENDING
            command.lease_owner = command.lease_expires_at = None
            command.available_at = datetime.now(UTC) + timedelta(seconds=60)
            command.terminal_reason = "Provider import failed; inspect protected evidence"
            if row.status != "verified":
                row.status = "failed" if terminal else "pending"
            if version.status == DatasetStatus.UPLOADED:
                version.status = DatasetStatus.FAILED if terminal else DatasetStatus.UPLOADED


def run_provider_import(factory, version_id, *, policy, public_key, store_factory):
    """Bounded operator execution; managed workers reuse claim/renew/publication."""
    execution = claim_provider_import(factory, version_id, policy=policy, public_key=public_key)
    if execution is None:
        return False
    last_renewal = time.monotonic()

    def progress(force=False):
        nonlocal last_renewal
        if force or time.monotonic() - last_renewal >= 15:
            renew_provider_import(factory, execution, policy=policy, public_key=public_key)
            last_renewal = time.monotonic()

    try:
        store = store_factory()
        reject_owned_source_namespace(store, execution.manifest)
        proof = verify_provider_objects(
            execution.manifest, store, execution.intent.budget, progress=progress
        )
        progress(force=True)
        publish_provider_import(
            factory,
            execution,
            proof,
            policy=policy,
            public_key=public_key,
        )
        return True
    except Exception as exc:
        fail_provider_import(factory, execution, exc)
        raise


def _specification(version, row, manifest, proof):
    return {
        "revision": SPLIT_SCOPE,
        "provider_import_id": str(row.id),
        "manifest_digest": row.manifest_digest,
        "source_digest": version.content_hash,
        "safe_objects": [o.model_dump(mode="json") for o in manifest.safe_data.objects],
        "split_counts": proof.rows,
        "identity": {"split_digests": proof.role_digests, "split_counts": proof.rows},
        "uris": {role: f"provider-import://{row.id}/{role}" for role in proof.rows},
        "final_test_digest_scope": "opaque-final-object-sha256-v1",
        "final_role_digests": manifest.final_role_digests.model_dump(mode="json"),
    }


def _ready(db, version, row, command, manifest):
    if (
        version.object_uri.startswith("provider-import:")
        and version.object_uri != f"provider-import://{version.id}"
    ):
        raise InvalidTransition("Provider descriptor identity differs from its dataset version")
    proof = CopyVerification.model_validate(row.verification)
    _validate_copy_verification(manifest, proof, row.manifest_digest)
    split = db.get(DatasetSplitRevision, row.local_split_revision_id, populate_existing=True)
    artifact = db.get(PreparedArtifact, row.prepared_artifact_id, populate_existing=True)
    expected = _specification(version, row, manifest, proof)
    if (
        row.status != "verified"
        or command.status != CommandStatus.SUCCEEDED
        or command.response_status != 200
        or command.lease_owner is not None
        or command.lease_expires_at is not None
        or version.schema_json.get("columns") != [c.model_dump() for c in proof.columns]
        or version.status not in (DatasetStatus.READY, DatasetStatus.PROFILING)
        or version.format.value != "json"
        or version.content_hash_algorithm != "sha256"
        or version.content_hash_scope != "byte_stream"
        or version.content_hash_verification_status != "verified"
        or version.content_hash_verified_at is None
        or version.content_hash_verified_at.tzinfo is None
        or version.content_hash_verified_at.utcoffset() is None
        or version.row_count != sum(proof.rows.values())
        or split is None
        or artifact is None
        or split.project_id != row.project_id
        or artifact.project_id != row.project_id
        or split.dataset_version_id != version.id
        or artifact.dataset_version_id != version.id
        or split.digest_scope != SPLIT_SCOPE
        or artifact.digest_scope != SPLIT_SCOPE
        or split.content_digest != manifest.split_digest
        or artifact.content_digest != row.manifest_digest
        or split.train_digest != proof.role_digests["train"]
        or split.validation_digest != proof.role_digests["validation"]
        or split.final_test_digest
        != canonical_request_hash(manifest.final_role_digests.model_dump(mode="json"))
        or split.sealed_at is None
        or artifact.status != "ready"
        or split.specification != expected
        or artifact.specification != expected
        or artifact.byte_size != version.byte_size
        or artifact.row_count != version.row_count
        or artifact.object_uri != version.object_uri
        or command.response_payload
        != {
            "dataset_version_id": str(version.id),
            "split_revision_id": str(split.id),
            "prepared_artifact_id": str(artifact.id),
        }
    ):
        raise InvalidTransition("Provider import is not a complete verified safe source")
    return proof, split, artifact


def reject_owned_source_namespace(store, manifest):
    """Externally owned copies must survive deletion of any application project."""

    def location(uri):
        parts = urlsplit(uri)
        return (
            {"s3c": "s3", "az": "azure"}.get(parts.scheme, parts.scheme),
            parts.netloc.lower(),
            unquote(parts.path).rstrip("/"),
        )

    prefixes = [location(store.uri_for_key(root)) for root in ("projects", "automl/projects")]
    for item in manifest.safe_data.objects:
        scheme, host, path = location(item.uri)
        if any(
            scheme == s and host == h and (path == p or path.startswith(p + "/"))
            for s, h, p in prefixes
        ):
            raise ValueError(
                "Provider safe copies must be outside application-owned project prefixes"
            )


def cancel_provider_import(db, actor_id, version_id):
    version, row, command = _current(db, version_id)
    _operator(db, actor_id, row.project_id)
    if row.status == "verified" or command.status == CommandStatus.SUCCEEDED:
        raise InvalidTransition("A completed provider source cannot be cancelled as pending work")
    from automl_api.services.provider_import_attempts import (
        stop_verification_attempt,
        verification_attempt,
    )

    if command.status == CommandStatus.RUNNING:
        attempt = verification_attempt(db, row)
        from automl_api.services.evaluation_publication import _event

        submission = (
            _event(db, attempt.id, "provider_import.submission") if attempt is not None else None
        )
        managed = (
            submission is not None
            and submission.event_type == "provider_import.submission"
            and submission.result_digest == canonical_request_hash(submission.payload)
            and submission.payload["authority"]["attempt_id"] == str(attempt.id)
            and submission.payload["authority"]["command_id"] == str(command.id)
            and submission.payload["authority"]["fence"] == attempt.fencing_token
        )
        if not managed and (
            command.lease_expires_at is None
            or command.lease_expires_at > datetime.now(UTC)
            or attempt is None
            or attempt.lease_owner != command.lease_owner
            or attempt.lease_expires_at != command.lease_expires_at
        ):
            raise StaleFence("Wait for the provider importer to stop before cancellation")
    stop_verification_attempt(db, row, reason="Provider import cancelled by operator")
    command.status = CommandStatus.CANCELLED
    command.lease_owner = command.lease_expires_at = None
    command.terminal_reason = "Provider import cancelled by operator"
    row.status = "failed"
    if version.status != DatasetStatus.ARCHIVED:
        version.status = DatasetStatus.FAILED
    return row


def assert_provider_cleanup_safe(db, store, prefix):
    """A later bucket/configuration change must not turn foreign copies into owned data."""
    from automl_api.services.provider_distribution import DistributionReceipt

    root = None
    receipts = db.execute(
        select(DatasetProviderImport.distribution_receipt).execution_options(yield_per=10)
    ).scalars()
    for receipt in receipts:
        manifest = DistributionReceipt.model_validate(receipt).payload.manifest
        if root is None:
            root = urlsplit(store.uri_for_key(prefix))
        for item in manifest.safe_data.objects:
            parts = urlsplit(item.uri)
            aliases = {"s3c": "s3", "az": "azure"}
            path, owned = unquote(parts.path).rstrip("/"), unquote(root.path).rstrip("/")
            if (
                aliases.get(parts.scheme, parts.scheme) == aliases.get(root.scheme, root.scheme)
                and parts.netloc.lower() == root.netloc.lower()
                and (path == owned or path.startswith(owned + "/"))
            ):
                raise ValueError("Protected provider copies intersect the requested cleanup prefix")
