"""Protected registration against committed PostgreSQL and real signed Parquet copies."""

import copy
import hashlib
import io
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import DatasetProviderImport, DatasetVersion
from automl_api.models.enums import CommandStatus, DatasetStatus, GlobalRole, ProjectStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import (
    DatasetSplitRevision,
    PreparedArtifact,
    WorkflowAttempt,
    WorkflowCommand,
)
from automl_api.services.dataset_digests import DatasetDigestError, require_verified_digest
from automl_api.services.provider_import import (
    ImportPolicy,
    ImportReferences,
    begin_provider_import,
    descriptor_bytes,
    run_provider_import,
)
from automl_api.services.workflow_state import IdempotencyConflict, InvalidTransition, StaleFence
from automl_api.storage.contracts import ObjectMetadata
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session, sessionmaker
from test_provider_copy_cli import signed
from test_provider_copy_verification import VersionStore, budget, manifest

pytest_plugins = ["test_phase1_postgres_contracts"]


class ImportStore(VersionStore):
    def __init__(self):
        super().__init__()
        self.descriptors = {}
        self.writes = []
        self.before_read = lambda: None
        self.after_write = lambda: None

    def uri_for_key(self, key):
        return f"s3c://operator-owned/{key}"

    def open_stream(self, uri, *, version=None):
        self.before_read()
        if version is not None:
            return super().open_stream(uri, version=version)
        return io.BytesIO(self.descriptors[uri])

    def put_bytes(self, key, content):
        uri = self.uri_for_key(key)
        self.descriptors[uri] = content
        self.writes.append(key)
        self.after_write()
        return ObjectMetadata(uri=uri, byte_size=len(content))


@pytest.fixture
def registration_case(phase1_engine, committed_tenant):
    actor_id, project_id = committed_tenant
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    with factory() as db, db.begin():
        db.get(User, actor_id).global_role = GlobalRole.ADMIN
    store = ImportStore()
    value = manifest(store)
    key = Ed25519PrivateKey.generate()
    policy = ImportPolicy(
        project_id=project_id,
        authority_key_digest=hashlib.sha256(key.public_key().public_bytes_raw()).hexdigest(),
        references=ImportReferences(
            **{name: getattr(value, name) for name in ImportReferences.model_fields}
        ),
    )
    return SimpleNamespace(
        engine=phase1_engine,
        factory=factory,
        actor_id=actor_id,
        project_id=project_id,
        store=store,
        value=value,
        key=key,
        policy=policy,
        receipt=signed(value, key),
        limits=budget(),
    )


def begin(case, **changes):
    values = dict(
        policy=case.policy,
        receipt=case.receipt,
        public_key=case.key.public_key(),
        budget=case.limits,
        dataset_name="Signed safe fixture",
    )
    values.update(changes)
    with case.factory() as db, db.begin():
        row = begin_provider_import(db, case.actor_id, **values)
        return row.dataset_version_id, row.id, row.command_id


def run(case, version_id, **changes):
    return run_provider_import(
        case.factory,
        version_id,
        policy=case.policy,
        public_key=case.key.public_key(),
        store_factory=changes.get("store_factory", lambda: case.store),
    )


def test_signed_safe_copy_becomes_explicit_descriptor_and_bound_prepared_source(registration_case):
    c = registration_case
    version_id, import_id, command_id = begin(c)
    before = copy.deepcopy(c.store.objects)

    def inspect_claim():
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            command = db.scalar(
                select(WorkflowCommand)
                .where(WorkflowCommand.id == command_id)
                .with_for_update(nowait=True)
            )
            assert command.status == CommandStatus.RUNNING and command.retry_count == 1
            assert command.lease_owner and command.lease_expires_at > datetime.now(UTC)
            db.scalar(
                select(DatasetVersion)
                .where(DatasetVersion.id == version_id)
                .with_for_update(nowait=True)
            )
            db.scalar(
                select(DatasetProviderImport)
                .where(DatasetProviderImport.id == import_id)
                .with_for_update(nowait=True)
            )

    c.store.before_read = inspect_claim
    assert run(c, version_id)
    with c.factory() as db:
        version = db.get(DatasetVersion, version_id)
        row = db.get(DatasetProviderImport, import_id)
        command = db.get(WorkflowCommand, command_id)
        split = db.get(DatasetSplitRevision, row.local_split_revision_id)
        artifact = db.get(PreparedArtifact, row.prepared_artifact_id)
        assert row.status == "verified" and row.mode == "validation_only"
        assert version.source_kind == "provider_distribution" and version.format.value == "json"
        assert version.status == DatasetStatus.READY and version.row_count == 4
        assert version.content_hash == hashlib.sha256(descriptor_bytes(c.value)).hexdigest()
        assert command.status == CommandStatus.SUCCEEDED and command.lease_owner is None
        assert (
            split.sealed_at
            and split.train_digest == c.value.safe_data.logical_roles()[1]["train"][1]
        )
        assert "final_input" not in split.specification["uris"]
        assert "final_label" not in split.specification["uris"]
        assert split.specification["final_test_digest_scope"] == "opaque-final-object-sha256-v1"
        assert artifact.object_uri == version.object_uri and artifact.row_count == 4
        with pytest.raises(DatasetDigestError, match="safe-copy reader"):
            require_verified_digest(version)
    assert before == c.store.objects
    assert version.object_uri == f"provider-import://{version_id}"
    assert not c.store.writes
    assert c.store.calls == [(o.uri, o.version) for o in c.value.safe_data.objects]
    assert not run(
        c, version_id, store_factory=lambda: pytest.fail("Completed replay read storage")
    )
    assert begin(c) == (version_id, import_id, command_id)


@pytest.mark.parametrize("fault", ["key", "signature", "reference", "mode", "operator"])
def test_untrusted_import_is_rejected_before_durable_registration(registration_case, fault):
    c = registration_case
    changes = {}
    if fault == "key":
        changes["public_key"] = Ed25519PrivateKey.generate().public_key()
    elif fault == "signature":
        receipt = copy.deepcopy(c.receipt)
        receipt["signature"] = "00" * 64
        changes["receipt"] = receipt
    elif fault == "reference":
        changes["policy"] = c.policy.model_copy(
            update={
                "references": c.policy.references.model_copy(
                    update={"project_reference": "another-project"}
                )
            }
        )
    elif fault == "mode":
        value = c.value.model_copy(update={"mode": "promotional"})
        changes["receipt"] = signed(value, c.key)
    else:
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).global_role = GlobalRole.MEMBER
    with pytest.raises((ValueError, InvalidTransition)):
        begin(c, **changes)
    with c.factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(DatasetProviderImport)
                .where(DatasetProviderImport.project_id == c.project_id)
            )
            == 0
        )
    assert not c.store.calls and not c.store.writes


def test_registration_replay_cannot_relabel_dataset(registration_case):
    c = registration_case
    begin(c)
    with pytest.raises(IdempotencyConflict):
        begin(c, dataset_name="Changed fixture")


@pytest.mark.parametrize("failure", ["bad_bytes", "lost_read"])
def test_failure_does_not_publish_ready_metadata_and_transient_replay_is_exact(
    registration_case, failure
):
    c = registration_case
    version_id, import_id, command_id = begin(c)
    original = copy.deepcopy(c.store.objects)
    if failure == "bad_bytes":
        item = c.value.safe_data.objects[0]
        c.store.objects[(item.uri, item.version)] = b"corrupt"
    elif failure == "lost_read":
        c.store.before_read = lambda: (_ for _ in ()).throw(OSError("private storage details"))
    with pytest.raises((ValueError, OSError)):
        run(c, version_id)
    with c.factory() as db, db.begin():
        row = db.get(DatasetProviderImport, import_id)
        command = db.get(WorkflowCommand, command_id)
        version = db.get(DatasetVersion, version_id)
        assert row.status != "verified" and row.local_split_revision_id is None
        assert command.status != CommandStatus.SUCCEEDED and command.lease_owner is None
        assert version.content_hash_verification_status == "pending"
        assert "private" not in command.terminal_reason
        if failure == "bad_bytes":
            assert row.status == "failed"
        else:
            command.available_at = datetime.now(UTC) - timedelta(seconds=1)
    if failure != "bad_bytes":
        c.store.before_read = lambda: None
        c.store.after_write = lambda: None
        assert run(c, version_id)
        assert c.store.objects == original
        assert c.store.writes == []


@pytest.mark.parametrize(
    "fault", ["owner", "lease_expired", "archive", "deletion", "project_archive"]
)
def test_stale_or_stopped_import_cannot_publish_descriptor(registration_case, fault):
    c = registration_case
    version_id, import_id, command_id = begin(c)
    changed = False

    def mutate():
        nonlocal changed
        if changed:
            return
        changed = True
        with c.factory() as db, db.begin():
            command = db.get(WorkflowCommand, command_id)
            if fault == "owner":
                command.lease_owner = "replacement"
            elif fault == "lease_expired":
                command.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif fault == "archive":
                db.get(DatasetVersion, version_id).status = DatasetStatus.ARCHIVED
            elif fault == "project_archive":
                db.get(Project, c.project_id).status = ProjectStatus.ARCHIVED
            else:
                db.get(Project, c.project_id).settings = {"deletion_in_progress": True}

    c.store.before_read = mutate
    with pytest.raises((StaleFence, InvalidTransition)):
        run(c, version_id)
    assert c.store.writes == []
    with c.factory() as db:
        row = db.get(DatasetProviderImport, import_id)
        assert row.local_split_revision_id is None and row.status != "verified"
        if fault == "owner":
            assert db.get(WorkflowCommand, command_id).lease_owner == "replacement"
        if fault == "archive":
            assert db.get(DatasetVersion, version_id).status == DatasetStatus.ARCHIVED


@pytest.mark.parametrize("fault", ["live_lease", "budget"])
def test_unavailable_claim_never_constructs_storage(registration_case, fault):
    c = registration_case
    version_id, _, command_id = begin(c)
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, command_id)
        if fault == "live_lease":
            command.status = CommandStatus.RUNNING
            command.lease_owner = "predecessor"
            command.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        else:
            command.retry_count = command.max_retries
    assert not run(
        c, version_id, store_factory=lambda: pytest.fail("Unavailable claim constructed storage")
    )


@pytest.mark.parametrize(
    "fault", ["proof", "byte_scope", "split", "response", "receipt", "archive"]
)
def test_completed_import_replay_revalidates_protected_evidence_before_storage(
    registration_case, fault
):
    c = registration_case
    version_id, import_id, command_id = begin(c)
    assert run(c, version_id)
    with c.factory() as db, db.begin():
        row = db.get(DatasetProviderImport, import_id)
        if fault == "proof":
            row.verification = {**row.verification, "schema_digest": "0" * 64}
        elif fault == "byte_scope":
            db.get(DatasetVersion, version_id).content_hash_scope = "legacy_unknown"
        elif fault == "split":
            db.get(DatasetSplitRevision, row.local_split_revision_id).specification = {}
        elif fault == "response":
            db.get(WorkflowCommand, command_id).response_payload = {}
        elif fault == "archive":
            db.get(DatasetVersion, version_id).status = DatasetStatus.ARCHIVED
        else:
            receipt = copy.deepcopy(row.distribution_receipt)
            receipt["signature"] = "00" * 64
            row.distribution_receipt = receipt
    with pytest.raises(ValueError):
        run(
            c,
            version_id,
            store_factory=lambda: pytest.fail("Invalid completed source read storage"),
        )


def test_canonical_descriptor_hash_is_validated_before_reading_copies(registration_case):
    c = registration_case
    version_id, import_id, _ = begin(c)
    with c.factory() as db, db.begin():
        version = db.get(DatasetVersion, version_id)
        original_hash = version.content_hash
        version.content_hash = "0" * 64
    with pytest.raises(ValueError, match="protected intent"):
        run(c, version_id)
    assert not c.store.calls and not c.store.writes
    with c.factory() as db, db.begin():
        assert db.get(DatasetProviderImport, import_id).local_split_revision_id is None
        db.get(DatasetVersion, version_id).content_hash = original_hash
    assert run(c, version_id)
    assert c.store.writes == []


def test_lost_owner_after_verification_cannot_publish_metadata(registration_case, monkeypatch):
    from automl_api.services import provider_import

    c = registration_case
    version_id, import_id, command_id = begin(c)
    before = copy.deepcopy(c.store.objects)
    verify = provider_import.verify_provider_objects

    def replace_after_verification(*args, **kwargs):
        proof = verify(*args, **kwargs)
        with c.factory() as db, db.begin():
            db.get(WorkflowCommand, command_id).lease_owner = "new-owner"
        return proof

    monkeypatch.setattr(provider_import, "verify_provider_objects", replace_after_verification)
    with pytest.raises(StaleFence):
        run(c, version_id)
    assert not c.store.writes and c.store.objects == before
    with c.factory() as db:
        assert db.get(DatasetProviderImport, import_id).local_split_revision_id is None
        assert db.get(WorkflowCommand, command_id).lease_owner == "new-owner"


@pytest.mark.parametrize("phase", ["claim", "publication"])
def test_actual_commit_with_lost_acknowledgement_recovers_without_duplicate_rows(
    registration_case, phase
):
    c = registration_case
    version_id, _, command_id = begin(c)
    fired = []
    target = CommandStatus.RUNNING if phase == "claim" else CommandStatus.SUCCEEDED

    def lose_ack(db):
        if not fired and any(
            isinstance(row, WorkflowCommand) and row.id == command_id and row.status == target
            for row in db.identity_map.values()
        ):
            fired.append(True)
            raise OSError("Lost committed metadata acknowledgement")

    event.listen(Session, "after_commit", lose_ack)
    try:
        with pytest.raises(OSError, match="acknowledgement"):
            run(c, version_id)
    finally:
        event.remove(Session, "after_commit", lose_ack)
    assert fired == [True]
    if phase == "claim":
        assert c.store.calls == []
        assert not run(
            c, version_id, store_factory=lambda: pytest.fail("Owned claim constructed storage")
        )
        with c.factory() as db, db.begin():
            command = db.get(WorkflowCommand, command_id)
            attempt = db.scalar(
                select(WorkflowAttempt).where(WorkflowAttempt.dataset_version_id == version_id)
            )
            assert command.status == CommandStatus.RUNNING and command.retry_count == 1
            command.lease_expires_at = attempt.lease_expires_at = datetime.now(UTC) - timedelta(
                seconds=1
            )
        assert run(c, version_id)
    with c.factory() as db:
        assert db.get(DatasetVersion, version_id).status == DatasetStatus.READY
        assert db.get(WorkflowCommand, command_id).status == CommandStatus.SUCCEEDED
        for model in (PreparedArtifact, DatasetSplitRevision):
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.dataset_version_id == version_id)
                )
                == 1
            )
    before = list(c.store.calls)
    assert not run(c, version_id, store_factory=lambda: pytest.fail("Replay constructed storage"))
    assert c.store.calls == before and c.store.writes == []
