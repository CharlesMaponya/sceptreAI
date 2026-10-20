"""Actual PostgreSQL enforcement of attempt capability lineage and lifecycle."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import AttemptStatus, TaskType, WorkflowStage
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    PromotionalScope,
    ProviderConformanceCampaign,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowAttemptCredentialGrant,
)
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

pytest_plugins = ["test_phase1_postgres_contracts"]


def make_attempt(db, user, project, stage=WorkflowStage.TRAINING_RUN):
    dataset = Dataset(project_id=project.id, created_by_id=user.id, name="grant source")
    db.add(dataset)
    db.flush()
    version = DatasetVersion(
        project_id=project.id,
        dataset_id=dataset.id,
        created_by_id=user.id,
        version_number=1,
        format="PARQUET",
        object_uri="s3c://safe/input.parquet",
        content_hash="a" * 64,
    )
    db.add(version)
    db.flush()
    parent = {}
    if stage == WorkflowStage.TRAINING_RUN:
        run = ModelRun(
            project_id=project.id,
            dataset_version_id=version.id,
            created_by_id=user.id,
            task_type=TaskType.REGRESSION,
        )
        db.add(run)
        db.flush()
        parent["model_run_id"] = run.id
    elif stage == WorkflowStage.TRAINING_TRIAL:
        root = make_attempt(db, user, project)
        common = dict(
            project_id=project.id,
            revision=1,
            digest_scope="schema-fixture",
            content_digest="a" * 64,
        )
        catalog = EstimatorCatalogRevision(
            **common, name=uuid.uuid4().hex, release_version="schema-fixture"
        )
        registry = FeatureRegistryRevision(**common, name=uuid.uuid4().hex)
        db.add_all([catalog, registry])
        db.flush()
        recipe = FeatureRecipeRevision(
            **common, name=uuid.uuid4().hex, registry_revision_id=registry.id
        )
        db.add(recipe)
        db.flush()
        candidate = TrainingCandidate(
            project_id=project.id,
            model_run_id=root.model_run_id,
            candidate_key=uuid.uuid4().hex,
            estimator_key="Ridge",
            catalog_revision_id=catalog.id,
            feature_recipe_revision_id=recipe.id,
        )
        db.add(candidate)
        db.flush()
        trial = TrainingTrial(
            project_id=project.id,
            candidate_id=candidate.id,
            suggestion_id=uuid.uuid4().hex,
            params_digest="b" * 64,
        )
        db.add(trial)
        db.flush()
        parent.update(model_run_id=root.model_run_id, trial_id=trial.id, run_attempt_id=root.id)
    elif stage in {WorkflowStage.CHAMPION_REFIT, WorkflowStage.CHAMPION_EVALUATION}:
        split = DatasetSplitRevision(
            project_id=project.id,
            dataset_version_id=version.id,
            revision=1,
            digest_scope="schema-fixture",
            content_digest="a" * 64,
            train_digest="b" * 64,
            validation_digest="c" * 64,
            final_test_digest="d" * 64,
        )
        db.add(split)
        db.flush()
        scope = PromotionalScope(
            project_id=project.id,
            split_revision_id=split.id,
            scope_key=uuid.uuid4().hex,
            canonical_provider="aws",
            expected_members=1,
        )
        db.add(scope)
        db.flush()
        parent["scope_id"] = scope.id
    elif stage == WorkflowStage.PROVIDER_CONFORMANCE:
        campaign = ProviderConformanceCampaign(
            project_id=project.id,
            campaign_key=uuid.uuid4().hex,
            provider="aws",
            stage="synthetic",
            profile_digest="a" * 64,
        )
        db.add(campaign)
        db.flush()
        parent["conformance_campaign_id"] = campaign.id
    else:
        parent["dataset_version_id"] = version.id
    attempt = WorkflowAttempt(
        project_id=project.id,
        stage=stage,
        logical_key=uuid.uuid4().hex,
        workload_identity="grant-schema",
        generation=1,
        fencing_token=uuid.uuid4().hex,
        status=AttemptStatus.RUNNING,
        **parent,
    )
    db.add(attempt)
    db.flush()
    return attempt


def grant_values(attempt, **updates):
    values = dict(
        project_id=attempt.project_id,
        attempt_id=attempt.id,
        stage=attempt.stage.value,
        fencing_token=attempt.fencing_token,
        request_id=uuid.uuid4(),
        pod_uid=str(uuid.uuid4()),
        token_sha256="1" * 64,
        authority_digest="2" * 64,
        grant_digest="3" * 64,
        method="GET",
        role="train",
        object_uri="s3c://safe/train.parquet",
        object_version="version-1",
        byte_limit=10 * 1024**3,
        content_sha256="4" * 64,
        expires_at=datetime.now(UTC) + timedelta(seconds=60),
    )
    return {**values, **updates}


@pytest.fixture
def grant_case(phase1_db, tenant):
    user, project = tenant
    attempt = make_attempt(phase1_db, user, project)
    return phase1_db, user, project, attempt


def test_valid_grant_preserves_large_byte_limit_and_contains_no_credential_material(grant_case):
    db, _, _, attempt = grant_case
    grant = WorkflowAttemptCredentialGrant(**grant_values(attempt))
    db.add(grant)
    db.flush()
    assert grant.byte_limit == 10 * 1024**3 and grant.status == "pending"
    assert (
        db.scalar(
            select(WorkflowAttemptCredentialGrant.byte_limit).where(
                WorkflowAttemptCredentialGrant.id == grant.id
            )
        )
        == 10 * 1024**3
    )
    assert not {"token", "credential", "signed_url", "private_key", "secret"} & set(
        WorkflowAttemptCredentialGrant.__table__.columns.keys()
    )


@pytest.mark.parametrize(
    "change",
    [
        {"stage": "preparation"},
        {"fencing_token": "another-fence"},
        {"attempt_id": uuid.UUID(int=1)},
        {"project_id": uuid.UUID(int=2)},
        {"method": "DELETE"},
        {"role": "raw"},
        {"role": "final_input"},
        {"role": "final_label"},
        {"byte_limit": -1},
        {"object_version": None},
        {"object_version": ""},
        {"object_version": "null"},
        {"method": "PUT", "role": "model"},
        {"expires_at": datetime.now(UTC) - timedelta(seconds=1)},
        {"expires_at": datetime.now(UTC) + timedelta(hours=2)},
        {"status": "available"},
        {"status": "issued"},
        {"status": "issuing"},
        {"status": "unknown"},
        {"status": "revoked"},
        {"grant_digest": "z" * 64},
        {"content_sha256": ""},
        {"authority_digest": "a" * 63},
        {"token_sha256": "private-material"},
    ],
)
def test_invalid_grant_cannot_persist_with_valid_positive_control(grant_case, change):
    db, _, _, attempt = grant_case
    db.add(WorkflowAttemptCredentialGrant(**grant_values(attempt)))
    db.flush()
    with pytest.raises(IntegrityError), db.begin_nested():
        db.add(WorkflowAttemptCredentialGrant(**grant_values(attempt, **change)))
        db.flush()


@pytest.mark.parametrize(
    "stage,role,method,valid",
    [
        (WorkflowStage.SPLITTER, "raw", "GET", True),
        (WorkflowStage.SPLITTER, "final_input", "PUT", True),
        (WorkflowStage.SPLITTER, "final_label", "GET", False),
        (WorkflowStage.SPLITTER, "model", "PUT", False),
        (WorkflowStage.PREPARATION, "validation", "GET", True),
        (WorkflowStage.PREPARATION, "features", "PUT", True),
        (WorkflowStage.PREPARATION, "raw", "GET", False),
        (WorkflowStage.PREPARATION, "final_input", "GET", False),
        (WorkflowStage.TRAINING_TRIAL, "checkpoint", "GET", True),
        (WorkflowStage.TRAINING_TRIAL, "model", "PUT", True),
        (WorkflowStage.TRAINING_TRIAL, "raw", "GET", False),
        (WorkflowStage.TRAINING_TRIAL, "final_label", "GET", False),
        (WorkflowStage.CHAMPION_REFIT, "candidate", "GET", True),
        (WorkflowStage.CHAMPION_REFIT, "pipeline", "PUT", True),
        (WorkflowStage.CHAMPION_REFIT, "raw", "GET", False),
        (WorkflowStage.CHAMPION_REFIT, "final_input", "GET", False),
        (WorkflowStage.CHAMPION_EVALUATION, "pipeline", "GET", True),
        (WorkflowStage.CHAMPION_EVALUATION, "final_input", "GET", True),
        (WorkflowStage.CHAMPION_EVALUATION, "result", "PUT", True),
        (WorkflowStage.CHAMPION_EVALUATION, "raw", "GET", False),
        (WorkflowStage.CHAMPION_EVALUATION, "train", "GET", False),
        (WorkflowStage.PROVIDER_CONFORMANCE, "synthetic", "GET", True),
        (WorkflowStage.PROVIDER_CONFORMANCE, "synthetic", "PUT", False),
        (WorkflowStage.PROVIDER_CONFORMANCE, "parity", "GET", True),
        (WorkflowStage.PROVIDER_CONFORMANCE, "parity", "PUT", False),
        (WorkflowStage.PROVIDER_CONFORMANCE, "conformance_evidence", "PUT", True),
        (WorkflowStage.PROVIDER_CONFORMANCE, "raw", "GET", False),
        (WorkflowStage.PROVIDER_CONFORMANCE, "final_input", "GET", False),
    ],
)
def test_stage_roles_are_enforced_in_database(grant_case, stage, role, method, valid):
    db, user, project, _ = grant_case
    attempt = make_attempt(db, user, project, stage)
    grant = WorkflowAttemptCredentialGrant(
        **grant_values(
            attempt,
            role=role,
            method=method,
            object_version=None if method == "PUT" else "version-1",
        )
    )
    if valid:
        db.add(grant)
        db.flush()
    else:
        with pytest.raises(IntegrityError), db.begin_nested():
            db.add(grant)
            db.flush()


def test_existing_other_project_cannot_reuse_attempt_authority(grant_case):
    db, user, project, attempt = grant_case
    other = Project(owner_id=user.id, created_by_id=user.id, name="other grant project")
    db.add(other)
    db.flush()
    foreign = make_attempt(db, user, other)
    with pytest.raises(IntegrityError), db.begin_nested():
        db.add(WorkflowAttemptCredentialGrant(**grant_values(foreign, project_id=project.id)))
        db.flush()
    db.add(WorkflowAttemptCredentialGrant(**grant_values(attempt)))
    db.flush()


def test_request_replay_conflict_and_parent_authority_rewrite_are_rejected(grant_case):
    db, _, _, attempt = grant_case
    first = WorkflowAttemptCredentialGrant(**grant_values(attempt))
    db.add(first)
    db.flush()
    with pytest.raises(IntegrityError), db.begin_nested():
        db.add(WorkflowAttemptCredentialGrant(**grant_values(attempt, request_id=first.request_id)))
        db.flush()
    with pytest.raises(IntegrityError), db.begin_nested():
        db.execute(
            text("UPDATE workflow_attempts SET fencing_token=:fence WHERE id=:id"),
            {"fence": uuid.uuid4().hex, "id": attempt.id},
        )
    with pytest.raises(IntegrityError), db.begin_nested():
        db.delete(attempt)
        db.flush()


def test_expired_and_revoked_grants_retain_issuance_history(grant_case):
    db, _, _, attempt = grant_case
    grant = WorkflowAttemptCredentialGrant(**grant_values(attempt))
    db.add(grant)
    db.flush()
    grant.status, grant.issuance_owner = "issuing", uuid.uuid4()
    grant.issuance_started_at = datetime.now(UTC)
    db.flush()
    grant.status, grant.issued_at = "issued", datetime.now(UTC)
    db.flush()
    original = grant.issued_at
    grant.status = "expired"
    db.flush()
    assert grant.issued_at == original
    grant.status, grant.revoked_at = "revoked", datetime.now(UTC)
    db.flush()
    assert grant.issued_at == original


def migration_for(db, monkeypatch):
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = Path("alembic/versions/0014_attempt_object_grants.py")
    spec = importlib.util.spec_from_file_location("grant_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "op", Operations(MigrationContext.configure(db.connection())))
    return module


def test_downgrade_refuses_existing_grant_evidence_before_ddl(grant_case, monkeypatch):
    db, _, _, attempt = grant_case
    grant = WorkflowAttemptCredentialGrant(**grant_values(attempt))
    db.add(grant)
    db.flush()
    migration = migration_for(db, monkeypatch)
    with pytest.raises(RuntimeError, match="Cannot discard"), db.begin_nested():
        migration.downgrade()
    assert db.get(WorkflowAttemptCredentialGrant, grant.id) is grant


def test_empty_expansion_round_trip_preserves_parent_fields(grant_case, monkeypatch):
    from sqlalchemy import inspect

    db, _, _, attempt = grant_case
    before = (attempt.id, attempt.project_id, attempt.stage, attempt.fencing_token)
    migration = migration_for(db, monkeypatch)
    migration.downgrade()
    assert not inspect(db.connection()).has_table("workflow_attempt_credential_grants")
    migration.upgrade()
    db.expire(attempt)
    assert (attempt.id, attempt.project_id, attempt.stage, attempt.fencing_token) == before
    db.add(WorkflowAttemptCredentialGrant(**grant_values(attempt)))
    db.flush()
