"""Import provenance cannot be confused with raw dataset rows or another tenant."""

import importlib.util
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from automl_api.models.datasets import Dataset, DatasetProviderImport, DatasetVersion
from automl_api.models.projects import Project
from automl_api.models.workflows import DatasetSplitRevision, PreparedArtifact, WorkflowCommand
from automl_api.services.dataset_digests import DatasetDigestError, require_verified_digest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

pytest_plugins = ["test_phase1_postgres_contracts"]


def dataset_case(db, user, project):
    dataset = Dataset(project_id=project.id, created_by_id=user.id, name="safe copy")
    db.add(dataset)
    db.flush()
    version = DatasetVersion(
        project_id=project.id,
        dataset_id=dataset.id,
        created_by_id=user.id,
        version_number=1,
        format="JSON",
        object_uri="s3c://local/descriptor.json",
        content_hash="a" * 64,
        content_hash_verified_at=datetime.now(UTC),
        content_hash_verification_status="verified",
        byte_size=64,
    )
    command = WorkflowCommand(
        project_id=project.id,
        actor_id=user.id,
        operation="dataset.provider.import",
        idempotency_key=uuid.uuid4().hex,
        request_hash="b" * 64,
    )
    db.add_all([version, command])
    db.flush()
    row = DatasetProviderImport(
        project_id=project.id,
        dataset_version_id=version.id,
        command_id=command.id,
        allocation_id=uuid.uuid4(),
        authority_scope_id=uuid.uuid4(),
        project_reference="approved-fixture-project",
        fixture_revision="fixture-v1",
        split_revision="split-v1",
        provider="gcp",
        canonical_provider="aws",
        manifest_digest="c" * 64,
        authority_key_digest="d" * 64,
        distribution_receipt={},
    )
    db.add(row)
    db.flush()
    return version, command, row


@pytest.fixture
def import_schema_case(phase1_db, tenant):
    user, project = tenant
    return phase1_db, user, project, dataset_case(phase1_db, user, project)


def test_raw_source_default_preserves_verified_byte_contract(import_schema_case):
    db, _, _, (version, _, _) = import_schema_case
    assert version.source_kind == "raw"
    assert require_verified_digest(version) == "a" * 64
    assert (
        db.scalar(text("SELECT source_kind FROM dataset_versions WHERE id=:id"), {"id": version.id})
        == "raw"
    )


def test_manifest_source_never_passes_raw_reader_guard(import_schema_case):
    db, _, _, (version, _, _) = import_schema_case
    version.source_kind = "provider_distribution"
    db.flush()
    with pytest.raises(DatasetDigestError, match="safe-copy reader"):
        require_verified_digest(version)


def test_unknown_source_kind_rejected_in_database(import_schema_case):
    db, _, _, (version, _, _) = import_schema_case
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            text("UPDATE dataset_versions SET source_kind=:kind WHERE id=:id"),
            {"kind": "unsigned_import", "id": version.id},
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("mode", "promotional"),
        ("provider", "aws"),
        ("provider", "unknown"),
        ("canonical_provider", "unknown"),
        ("status", "verified"),
        ("status", "unknown"),
    ],
)
def test_import_database_boundary_rejects_unqualified_state(import_schema_case, field, value):
    db, _, _, (_, _, row) = import_schema_case
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            DatasetProviderImport.__table__.update()
            .where(DatasetProviderImport.id == row.id)
            .values({field: value})
        )


@pytest.mark.parametrize("binding", ["dataset", "command", "split", "artifact"])
def test_import_foreign_keys_are_project_bound(import_schema_case, binding):
    db, user, project, (_, _, row) = import_schema_case
    other = Project(owner_id=user.id, created_by_id=user.id, name="another tenant")
    db.add(other)
    db.flush()
    version, command, other_import = dataset_case(db, user, other)
    db.delete(other_import)
    db.flush()
    common = dict(
        project_id=other.id,
        dataset_version_id=version.id,
        revision=1,
        content_digest="e" * 64,
        digest_scope="fixture",
        specification={},
    )
    split = DatasetSplitRevision(
        **common, train_digest="a" * 64, validation_digest="b" * 64, final_test_digest="c" * 64
    )
    artifact = PreparedArtifact(
        **common, object_uri="s3c://other/manifest.json", byte_size=64, row_count=2
    )
    own = PreparedArtifact(
        project_id=project.id,
        dataset_version_id=row.dataset_version_id,
        revision=1,
        content_digest="f" * 64,
        digest_scope="fixture",
        specification={},
        object_uri="s3c://own/manifest.json",
        byte_size=64,
        row_count=2,
    )
    own_split = DatasetSplitRevision(
        project_id=project.id,
        dataset_version_id=row.dataset_version_id,
        revision=1,
        content_digest="1" * 64,
        digest_scope="fixture",
        train_digest="a" * 64,
        validation_digest="b" * 64,
        final_test_digest="c" * 64,
    )
    db.add_all([split, artifact, own, own_split])
    db.flush()
    changes = {
        "dataset": {"dataset_version_id": version.id},
        "command": {"command_id": command.id},
        "split": {
            "status": "verified",
            "local_split_revision_id": split.id,
            "prepared_artifact_id": own.id,
        },
        "artifact": {
            "status": "verified",
            "local_split_revision_id": own_split.id,
            "prepared_artifact_id": artifact.id,
        },
    }[binding]
    with pytest.raises(IntegrityError) as error, db.begin_nested():
        db.execute(
            DatasetProviderImport.__table__.update()
            .where(DatasetProviderImport.id == row.id)
            .values(**changes)
        )
    assert error.value.orig.diag.constraint_name == f"fk_provider_import_{binding}_project"


def test_downgrade_refuses_existing_provenance_before_any_ddl(import_schema_case, monkeypatch):
    db, *_ = import_schema_case
    spec = importlib.util.spec_from_file_location(
        "provider_import_migration", Path("alembic/versions/0013_provider_dataset_import.py")
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    monkeypatch.setattr(migration, "op", SimpleNamespace(get_bind=lambda: db.get_bind()))
    with pytest.raises(RuntimeError, match="Cannot discard"):
        migration.downgrade()
    assert db.scalar(select(DatasetProviderImport.id)) is not None
