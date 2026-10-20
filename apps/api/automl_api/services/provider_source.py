"""Resolve protected provider provenance, never client-supplied source flags."""

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from pydantic import Field
from sqlalchemy import select

from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.enums import AttemptStatus, DatasetStatus, WorkflowStage
from automl_api.models.workflows import PromotionalScope, PromotionalScopeMember
from automl_api.services.dataset_digests import DatasetDigestError, require_verified_digest
from automl_api.services.provider_copy_verification import CopyVerificationBudget
from automl_api.services.provider_distribution import Contract, Digest, ProviderDistribution
from automl_api.services.provider_import import (
    OPERATION,
    SOURCE_KIND,
    ImportPolicy,
    _operator,
    _owned,
    _ready,
    _validate,
)


class ImportPolicies(Contract):
    policies: Annotated[tuple[ImportPolicy, ...], Field(min_length=1, max_length=1000)]


class ProviderReadPlan(Contract):
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    import_id: uuid.UUID
    split_revision_id: uuid.UUID
    prepared_artifact_id: uuid.UUID
    manifest: ProviderDistribution
    budget: CopyVerificationBudget


class ProviderVerificationPlan(Contract):
    project_id: uuid.UUID
    dataset_version_id: uuid.UUID
    import_id: uuid.UUID
    command_id: uuid.UUID
    actor_id: uuid.UUID
    owner: str
    request_digest: Digest
    manifest: ProviderDistribution
    budget: CopyVerificationBudget


def require_verification_source(db, attempt):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.provider_import_attempts import (
        verification_attempt,
        verification_identity,
        verification_key,
    )

    version = db.get(DatasetVersion, attempt.dataset_version_id, populate_existing=True)
    row = db.scalar(
        select(DatasetProviderImport)
        .where(DatasetProviderImport.dataset_version_id == attempt.dataset_version_id)
        .execution_options(populate_existing=True)
    )
    if version is None or row is None or attempt.logical_key != verification_key(row.id):
        raise DatasetDigestError("Pending verification import lineage is missing")
    policy, key = configured_policy(attempt.project_id, version.id)
    command = db.get(WorkflowCommand, row.command_id, populate_existing=True)
    if command is None:
        raise DatasetDigestError("Pending verification command is missing")
    intent, manifest, _ = _validate(version, row, command, policy, key)
    _operator(db, command.actor_id, row.project_id, lock=False)
    _owned(command, attempt.lease_owner)
    latest = verification_attempt(db, row, lock=False)
    if (
        attempt.project_id != row.project_id
        or attempt.stage != WorkflowStage.PREPARATION
        or attempt.workload_identity != verification_identity(attempt.id)
        or attempt.status != AttemptStatus.RUNNING
        or attempt.lease_expires_at is None
        or attempt.lease_expires_at <= datetime.now(UTC)
        or attempt.lease_expires_at != command.lease_expires_at
        or row.status != "pending"
        or version.status != DatasetStatus.UPLOADED
        or latest is None
        or latest.id != attempt.id
        or version.content_hash_verification_status != "pending"
    ):
        raise DatasetDigestError("Pending verification authority changed or expired")
    return ProviderVerificationPlan(
        project_id=row.project_id,
        dataset_version_id=version.id,
        import_id=row.id,
        command_id=command.id,
        actor_id=command.actor_id,
        owner=command.lease_owner,
        request_digest=command.request_hash,
        manifest=manifest,
        budget=intent.budget,
    )


def _bounded_file(path, limit):
    with Path(path).open("rb") as source:
        content = source.read(limit + 1)
    if len(content) > limit:
        raise ValueError("Provider source configuration exceeds its byte limit")
    return content


def configured_policy(project_id, version_id):
    """Both files are readonly server configuration, not public request arguments."""
    try:
        registry = ImportPolicies.model_validate_json(
            _bounded_file(os.environ["PROVIDER_IMPORT_POLICIES_FILE"], 1024**2)
        )
        matches = [
            p
            for p in registry.policies
            if p.project_id == project_id
            and uuid.uuid5(
                project_id, f"{OPERATION}:{p.references.allocation_id}:{p.references.provider}"
            )
            == version_id
        ]
        if len(matches) != 1:
            raise ValueError("Missing or ambiguous source binding")
        key = _bounded_file(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"], 4096).decode()
        return matches[0], key
    except (KeyError, ValueError, OSError, TypeError):
        raise DatasetDigestError("Approved provider source configuration is unavailable") from None


def require_prepared_source(db, version, *, split_revision_id=None, prepared_artifact_id=None):
    """Return a verified safe read plan or validate an ordinary raw dataset."""
    try:
        if getattr(version, "source_kind", None) in (None, "raw"):
            require_verified_digest(version)
        row = db.scalar(
            select(DatasetProviderImport)
            .where(DatasetProviderImport.dataset_version_id == version.id)
            .execution_options(populate_existing=True)
        )
        if row is None:
            require_verified_digest(version)
            return None
        if version.source_kind != SOURCE_KIND or row.project_id != version.project_id:
            raise DatasetDigestError("Provider source kind or project binding changed")
        policy, key = configured_policy(version.project_id, version.id)
        from automl_api.models.workflows import WorkflowCommand

        command = db.get(WorkflowCommand, row.command_id, populate_existing=True)
        if command is None:
            raise DatasetDigestError("Provider source registration is missing")
        intent, manifest, _ = _validate(version, row, command, policy, key)
        _, split, artifact = _ready(db, version, row, command, manifest)
        if (split_revision_id is not None and split.id != split_revision_id) or (
            prepared_artifact_id is not None and artifact.id != prepared_artifact_id
        ):
            raise DatasetDigestError("Provider source selected an unapproved prepared revision")
        return ProviderReadPlan(
            project_id=version.project_id,
            dataset_version_id=version.id,
            import_id=row.id,
            split_revision_id=split.id,
            prepared_artifact_id=artifact.id,
            manifest=manifest,
            budget=intent.budget,
        )
    except DatasetDigestError:
        raise
    except ValueError:
        raise DatasetDigestError(
            "Approved provider source evidence no longer matches this dataset"
        ) from None


def reject_provider_promotion(db, dataset_version_id):
    if (
        db.scalar(
            select(DatasetProviderImport.id).where(
                DatasetProviderImport.dataset_version_id == dataset_version_id
            )
        )
        is not None
    ):
        raise DatasetDigestError(
            "A validation-only provider source cannot join a promotional scope"
        )


def reject_promotional_membership(db, run):
    if (
        db.scalar(
            select(PromotionalScopeMember.id)
            .join(PromotionalScope, PromotionalScope.id == PromotionalScopeMember.scope_id)
            .where(
                PromotionalScopeMember.model_run_id == run.id,
                PromotionalScope.mode == "promotional",
            )
        )
        is not None
    ):
        reject_provider_promotion(db, run.dataset_version_id)


def validate_attempt_source(db, attempt, run=None):
    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.enums import WorkflowStage

    version_id = run.dataset_version_id if run is not None else attempt.dataset_version_id
    version = db.get(DatasetVersion, version_id, populate_existing=True)
    if version is None:
        raise DatasetDigestError("Attempt source dataset is missing")
    plan = require_prepared_source(db, version)
    if plan is not None and attempt.stage == WorkflowStage.SPLITTER:
        raise DatasetDigestError("A provider safe source cannot receive a raw splitter attempt")
    if run is not None:
        reject_promotional_membership(db, run)
        if plan is not None and str(plan.split_revision_id) != str(
            (run.params or {}).get("split_revision_id")
        ):
            raise DatasetDigestError("Attempt source uses an unapproved imported split")
    return plan
