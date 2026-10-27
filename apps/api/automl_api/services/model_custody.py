"""Original model provenance and exact-version native byte verification."""

import time
import uuid
from types import SimpleNamespace

from sqlalchemy import select

from automl_api.models.enums import ArtifactKind, ProjectStatus, RunKind, WorkflowStage
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import DeletionTombstone, WorkflowAttempt
from automl_api.services.attempt_object_grants import output_completion
from automl_api.services.training_model_output import (
    _authorized_model_reference,
    _candidate_model_row,
    _model_reference,
    require_model_tracking,
)
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.storage.attempt_outputs import S3_SINGLE_PUT_BYTES, _verify_bytes
from automl_api.storage.contracts import immutable_object_version

PROTOCOL = "training-model-custody-v1"


def registration_model_custody(db, run, entry):
    candidate, row = _candidate_model_row(db, run, entry)
    parent = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.project_id == run.project_id,
            WorkflowAttempt.model_run_id == run.id,
            WorkflowAttempt.stage == WorkflowStage.TRAINING_RUN,
        )
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    if parent is None:
        raise StaleFence("Model custody has no original producer history; qualification required")
    if row.attempt_id == parent.id:
        reference = _model_reference(db, row, candidate.id)
        if row.fencing_token != parent.fencing_token:
            raise StaleFence("Original model producer fence changed")
    else:
        reference = _authorized_model_reference(db, parent, row, candidate.id)
    require_model_tracking(db, parent, run, candidate, row, entry)
    completion = output_completion(db, row)
    if completion is None or not 0 < completion["byte_size"] <= S3_SINGLE_PUT_BYTES:
        raise StaleFence("Model has no original bounded native completion")
    immutable_object_version(completion["object_version"])
    return {
        "protocol": PROTOCOL,
        "project_id": str(run.project_id),
        "run_id": str(run.id),
        "candidate_name": candidate.estimator_key,
        "reference": reference,
        "completion": completion,
    }


def registered_model_custody(db, registry, artifact):
    stored = (artifact.artifact_metadata or {}).get("model_custody")
    if (
        not isinstance(stored, dict)
        or registry.registry_metadata.get("artifact_policy") != PROTOCOL
    ):
        raise StaleFence("Original registry custody is missing; controlled qualification required")
    try:
        run = db.get(ModelRun, uuid.UUID(stored["run_id"]), populate_existing=True)
        if (
            run is None
            or run.project_id != registry.project_id
            or run.id != registry.model_run_id
            or artifact.model_run_id != run.id
        ):
            raise StaleFence("Registry custody belongs to another model run")
        current = registration_model_custody(
            db,
            run,
            {
                "model": registry.model_name,
                "status": "succeeded",
                "model_artifact_uri": artifact.object_uri,
                "model_artifact_sha256": artifact.content_hash,
                "mlflow_run_id": artifact.artifact_metadata.get("mlflow_run_id"),
            },
        )
    except (ValueError, KeyError, TypeError):
        raise StaleFence("Registry custody history is malformed") from None
    if canonical_request_hash(stored) != canonical_request_hash(
        current
    ) or artifact.byte_size not in (None, current["completion"]["byte_size"]):
        raise StaleFence("Registry custody differs from its original model completion")
    verified = registry.registry_metadata.get("native_custody_digest")
    if (
        verified is not None
        and verified != canonical_request_hash(current)
        or registry.registry_metadata.get("desired_state") == "artifact_verified"
        and verified is None
    ):
        raise StaleFence("Registry native verification acknowledgement changed or is missing")
    return current


def deployment_model_custody(db, run):
    try:
        registry_id = uuid.UUID(run.tags["registry_entry_id"])
        artifact_id = uuid.UUID(run.tags["model_artifact_id"])
    except (ValueError, KeyError, TypeError, AttributeError):
        raise StaleFence("Deployment model binding requires controlled qualification") from None
    project = db.get(
        Project, run.project_id, with_for_update={"read": True}, populate_existing=True
    )
    registry = db.get(
        ModelRegistryEntry, registry_id, with_for_update={"read": True}, populate_existing=True
    )
    artifact = db.get(
        RunArtifact, artifact_id, with_for_update={"read": True}, populate_existing=True
    )
    if (
        run.run_kind != RunKind.DEPLOYMENT
        or project is None
        or project.status != ProjectStatus.ACTIVE
        or registry is None
        or registry.project_id != run.project_id
        or registry.model_artifact_id != artifact_id
        or artifact is None
        or artifact.project_id != run.project_id
        or artifact.kind != ArtifactKind.MODEL_OBJECT
    ):
        raise StaleFence("Deployment model authority changed")
    deleting = db.scalar(
        select(DeletionTombstone.id)
        .where(
            DeletionTombstone.project_id == run.project_id,
            (DeletionTombstone.resource_type == "project")
            | (
                (DeletionTombstone.resource_type == "run")
                & DeletionTombstone.resource_id.in_([run.id, registry.model_run_id])
            )
            | (
                (DeletionTombstone.resource_type == "run_artifact")
                & (DeletionTombstone.resource_id == artifact_id)
            ),
        )
        .limit(1)
    )
    if deleting is not None:
        raise StaleFence("Deployment model is being deleted")
    custody = registered_model_custody(db, registry, artifact)
    digest = canonical_request_hash(custody)
    if (
        run.tags.get("model_custody_digest") != digest
        or registry.registry_metadata.get("native_custody_digest") != digest
    ):
        raise StaleFence("Deployment original model custody changed or is unqualified")
    return custody


def verify_model_custody(store, custody, *, sink=None):
    completion = custody["completion"]
    version = immutable_object_version(completion["object_version"])
    deadline = time.monotonic() + 60
    with store.open_stream(completion["uri"], version=version) as source:

        def read(size):
            if time.monotonic() >= deadline:
                raise TimeoutError("Registry model verification deadline elapsed")
            value = source.read(size)
            if time.monotonic() >= deadline:
                raise TimeoutError("Registry model verification deadline elapsed")
            if sink is not None:
                sink.write(value)
            return value

        _verify_bytes(SimpleNamespace(read=read), completion["byte_size"], completion["sha256"])
    return canonical_request_hash(custody)
