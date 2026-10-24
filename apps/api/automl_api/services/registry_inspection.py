"""Read registry storage outside SQL; publish only under unchanged delivery authority."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.enums import ArtifactKind, CommandStatus, ProjectStatus
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.services.model_custody import registered_model_custody, verify_model_custody
from automl_api.services.reconciler import _lock_outbox_delivery
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    complete_outbox,
    transition_command,
)
from automl_api.storage.object_store import get_object_store

TOPIC = "registry.reconcile"


def _snapshot(row):
    return {column.key: getattr(row, column.key) for column in row.__table__.columns}


def _select(db, entry_id, worker_id, *, publish=False):
    project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
    if project_id is None:
        return None
    project = db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    entry = _lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    if project is None or project.status != ProjectStatus.ACTIVE or entry.project_id != project_id:
        raise StaleFence("Registry verification project is inactive or changed")
    if entry.topic != TOPIC:
        raise StaleFence("Registry verification requires its original outbox topic")
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    registry_id = uuid.UUID(str(entry.payload["registry_entry_id"]))
    run = db.scalar(
        select(ModelRun)
        .join(ModelRegistryEntry, ModelRegistryEntry.model_run_id == ModelRun.id)
        .where(
            ModelRegistryEntry.id == registry_id,
            ModelRegistryEntry.project_id == project_id,
        )
        .with_for_update(read=True, of=ModelRun)
        .execution_options(populate_existing=True)
    )
    registry = db.scalar(
        select(ModelRegistryEntry)
        .where(
            ModelRegistryEntry.project_id == project_id,
            ModelRegistryEntry.id == registry_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if registry is None:
        raise LookupError("The tracked registry entry no longer exists.")
    artifact = db.get(
        RunArtifact,
        registry.model_artifact_id,
        with_for_update=True if publish else {"read": True},
        populate_existing=True,
    )
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == project_id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & (DeletionTombstone.resource_id == registry.model_run_id)
            ),
        )
        .limit(1)
    )
    if (
        command is None
        or command.project_id != project_id
        or command.status not in {CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
        or run is None
        or run.project_id != project_id
        or registry.model_run_id != run.id
        or deleting is not None
        or artifact is None
        or artifact.project_id != project_id
        or artifact.model_run_id != run.id
        or artifact.kind != ArtifactKind.MODEL_OBJECT
    ):
        raise StaleFence("Registry model artifact or command authority changed")
    custody = registered_model_custody(db, registry, artifact)
    digest = canonical_request_hash(
        {
            type(row).__name__: _snapshot(row)
            for row in (project, entry, command, registry, run, artifact)
        }
    )
    return entry, command, registry, artifact, digest, custody


def _complete(db, entry, command, worker_id):
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    if command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)


def reconcile_registry_artifact(factory, entry_id, worker_id):
    """Duplicate read-only inspections are safe; only the live lease can publish."""
    delivery_digest = None
    try:
        with factory() as db, db.begin():
            original = db.get(OutboxEntry, entry_id)
            if original is None:
                return
            delivery_digest = canonical_request_hash(_snapshot(original))
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, command, registry, artifact, digest, custody = selected
            if canonical_request_hash(_snapshot(entry)) != delivery_digest:
                raise StaleFence("Registry verification delivery changed before selection")
            if registry.registry_metadata.get("desired_state") == "artifact_verified":
                _complete(db, entry, command, worker_id)
                return

        native_digest = verify_model_custody(get_object_store(), custody)
        observed_size = custody["completion"]["byte_size"]
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id, publish=True)
            if selected is None:
                return
            entry, command, registry, artifact, current_digest, current_custody = selected
            if current_digest != digest or current_custody != custody:
                raise StaleFence("Registry verification source or delivery authority changed")
            if artifact.byte_size is None:
                artifact.byte_size = observed_size
            registry.registry_metadata = {
                **registry.registry_metadata,
                "desired_state": "artifact_verified",
                "native_custody_digest": native_digest,
                "verified_byte_size": observed_size,
                "verified_at": datetime.now(UTC).isoformat(),
            }
            _complete(db, entry, command, worker_id)
    except StaleFence:
        raise
    except Exception as exc:
        with factory() as db, db.begin():
            entry = _lock_outbox_delivery(db, entry_id, worker_id)
            if entry is not None:
                if canonical_request_hash(_snapshot(entry)) != delivery_digest:
                    raise StaleFence(
                        "Registry verification failure lost its original delivery"
                    ) from None
                complete_outbox(
                    db, entry.id, worker_id=worker_id, delivered=False, error=redact_text(str(exc))
                )
        raise
