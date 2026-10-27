"""Original deployment object intents and receipts; no native writes under SQL."""

import hashlib
import io
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.enums import ArtifactKind, CommandStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.services import reconciler
from automl_api.services.inference_tls import PROTOCOL as INFERENCE_PROTOCOL
from automl_api.services.inference_tls import serving_declaration
from automl_api.services.model_custody import deployment_model_custody
from automl_api.services.workflow_state import (
    StaleFence,
    canonical_request_hash,
    complete_outbox,
    defer_outbox,
    transition_command,
)
from automl_api.storage.attempt_outputs import inspect_attempt_output, write_attempt_output
from automl_api.storage.contracts import immutable_object_version

TOPIC = "deployment.reconcile.v4"
# The journal format is unchanged; original v3 receipts remain valid for retirement.
CREATION_TOPICS = {"deployment.reconcile.v3", TOPIC}
PROTOCOL = "deployment-dockerfile-v1"
RECORD = "deployment_native_write"


def _descriptor(artifact):
    metadata = artifact.artifact_metadata
    body = metadata["content"].encode("utf-8")
    if (
        type(artifact.byte_size) is not int
        or artifact.byte_size != len(body)
        or not body
        or artifact.content_hash != hashlib.sha256(body).hexdigest()
    ):
        raise StaleFence("Deployment Dockerfile differs from its original bytes")
    return dict(
        project_id=str(artifact.project_id),
        run_id=str(artifact.model_run_id),
        artifact_id=str(artifact.id),
        byte_size=len(body),
        sha256=artifact.content_hash,
    ), body


def _record(artifact):
    record = artifact.artifact_metadata.get(RECORD)
    if record is None:
        if artifact.artifact_metadata.get("native_write_started"):
            raise StaleFence("Original deployment write history is missing")
        return None
    descriptor, _ = _descriptor(artifact)
    intent = record["intent"]
    expected = {
        "protocol",
        "write_id",
        "outbox_id",
        "source_digest",
        "driver",
        "uri",
        "pending_uri",
        *descriptor,
    }
    if (
        set(intent) != expected
        or intent["protocol"] != PROTOCOL
        or artifact.artifact_metadata.get("native_write_started") is not True
        or intent["driver"] not in {"s3_compatible", "aws_s3", "gcs", "azure_blob"}
        or any(intent[key] != value for key, value in descriptor.items())
        or type(intent["byte_size"]) is not int
        or artifact.object_uri not in {intent["uri"], intent["pending_uri"]}
        or not intent["uri"].endswith(
            f"/projects/{artifact.project_id}/deployments/{artifact.model_run_id}/Dockerfile"
        )
    ):
        raise StaleFence("Original deployment object intent changed")
    uuid.UUID(intent["write_id"])
    uuid.UUID(intent["outbox_id"])
    receipt = record.get("receipt")
    if receipt is not None:
        version = immutable_object_version(receipt["object_version"])
        if len(version) > 256:
            raise StaleFence("Deployment native version exceeds its receipt budget")
        expected_receipt = dict(
            uri=intent["uri"],
            byte_size=intent["byte_size"],
            sha256=intent["sha256"],
            intent_digest=canonical_request_hash(intent),
            object_version=version,
        )
        if canonical_request_hash(receipt) != canonical_request_hash(expected_receipt):
            raise StaleFence("Deployment native receipt differs from its original intent")
    return record


def unretired_deployment_write(artifact):
    metadata = getattr(artifact, "artifact_metadata", None) or {}
    if not isinstance(metadata, dict):
        return getattr(artifact, "kind", None) == ArtifactKind.DEPLOYMENT_IMAGE
    if RECORD not in metadata:
        if metadata.get("native_write_started"):
            return True
        return (
            metadata.get("desired_state") == "object_write_pending"
            and metadata.get("write_protocol") != PROTOCOL
        )
    try:
        record = _record(artifact)
        return record is None or record.get("receipt") is None
    except (StaleFence, ValueError, KeyError, TypeError, AttributeError):
        return True


def has_unretired_deployment_writes(db, project_id, run_ids=None):
    query = select(RunArtifact).where(
        RunArtifact.project_id == project_id, RunArtifact.kind == ArtifactKind.DEPLOYMENT_IMAGE
    )
    if run_ids is not None:
        query = query.where(RunArtifact.model_run_id.in_(run_ids))
    from automl_api.services.deployment_submission import unretired_submission

    return any(
        unretired_deployment_write(row) or unretired_submission(row) for row in db.scalars(query)
    )


def _snapshot(row):
    return {column.key: getattr(row, column.key) for column in row.__table__.columns}


def _select(db, entry_id, worker_id):
    project_id = db.scalar(select(OutboxEntry.project_id).where(OutboxEntry.id == entry_id))
    if project_id is None:
        return None
    from automl_api.models.projects import Project

    db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    run = db.get(
        ModelRun, uuid.UUID(entry.payload["run_id"]), with_for_update=True, populate_existing=True
    )
    if (
        entry.topic != TOPIC
        or command is None
        or command.project_id != project_id
        or command.status
        not in {
            CommandStatus.RUNNING,
            CommandStatus.SUCCEEDED,
            CommandStatus.CANCELLED,
            CommandStatus.FAILED,
        }
        or run is None
        or run.project_id != project_id
        or run.run_kind != RunKind.DEPLOYMENT
        or run.tags.get("deployment_write_protocol") != PROTOCOL
        or run.tags.get("inference_identity_protocol") != INFERENCE_PROTOCOL
    ):
        raise StaleFence("Deployment delivery requires its original qualified protocol and scope")
    if (
        entry.aggregate_type != "model_run"
        or entry.aggregate_id != run.id
        or command.operation != "deployment.launch"
        or command.resource_type != "deployment.launch"
        or command.resource_id != run.id
        or command.actor_id != run.created_by_id
        or command.request_hash != canonical_request_hash(command.request_payload)
    ):
        raise StaleFence("Deployment command no longer binds its original actor and resource")
    artifacts = list(
        db.scalars(
            select(RunArtifact)
            .where(
                RunArtifact.project_id == project_id,
                RunArtifact.model_run_id == run.id,
                RunArtifact.kind == ArtifactKind.DEPLOYMENT_IMAGE,
                RunArtifact.name == "Dockerfile",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if len(artifacts) != 1:
        raise LookupError("The original deployment Dockerfile is missing or ambiguous")
    artifact = artifacts[0]
    if artifact.artifact_metadata.get("write_protocol") != PROTOCOL:
        raise StaleFence("Deployment Dockerfile history requires controlled qualification")
    return entry, command, run, artifact


def _authority(db, entry, run, artifact):
    custody = deployment_model_custody(db, run)
    serving_declaration(
        entry.payload["manifests"],
        project_id=run.project_id,
        run_id=run.id,
        model_digest=custody["completion"]["sha256"],
        model_uri=custody["completion"]["uri"],
    )
    if (
        db.scalar(
            select(DeletionTombstone.id)
            .where(
                DeletionTombstone.project_id == run.project_id,
                DeletionTombstone.resource_type == "run_artifact",
                DeletionTombstone.resource_id == artifact.id,
            )
            .limit(1)
        )
        is not None
    ):
        raise StaleFence("Deployment Dockerfile is being deleted")
    descriptor, body = _descriptor(artifact)
    pending_uri = str(entry.payload["dockerfile_uri"])
    key = f"projects/{run.project_id}/deployments/{run.id}/Dockerfile"
    if pending_uri.split("/", 3)[-1] != key:
        raise StaleFence("Deployment Dockerfile target differs from its original scope")
    command = db.get(WorkflowCommand, entry.command_id)
    source = canonical_request_hash(
        dict(
            descriptor=descriptor,
            content=body.decode(),
            payload=entry.payload,
            command={
                key: getattr(command, key)
                for key in (
                    "id",
                    "actor_id",
                    "operation",
                    "resource_type",
                    "resource_id",
                    "request_hash",
                    "request_payload",
                )
            },
            custody=custody,
            params=run.params,
            namespace=run.k8s_namespace,
            job_name=run.k8s_job_name,
            tags={k: v for k, v in run.tags.items() if k != "desired_state"},
        )
    )
    return source, body, key


def _complete(db, entry, command, worker_id):
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    if command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)


def _fact(factory, artifact_id, intent, metadata):
    version_key = "generation" if intent["driver"] == "gcs" else "version_id"
    version = immutable_object_version(metadata.provider_headers.get(version_key))
    receipt = dict(
        uri=intent["uri"],
        byte_size=intent["byte_size"],
        sha256=intent["sha256"],
        intent_digest=canonical_request_hash(intent),
        object_version=version,
    )
    if metadata.uri != intent["uri"] or metadata.byte_size != intent["byte_size"]:
        raise StaleFence("Native deployment completion does not match its original write")
    with factory() as db, db.begin():
        artifact = db.get(RunArtifact, artifact_id, with_for_update=True, populate_existing=True)
        if artifact is None:
            raise StaleFence("Original deployment artifact no longer exists")
        original = _record(artifact)
        if original is None or canonical_request_hash(original["intent"]) != canonical_request_hash(
            intent
        ):
            raise StaleFence("Native completion lost its original deployment write record")
        if original.get("receipt") is not None and canonical_request_hash(
            original["receipt"]
        ) != canonical_request_hash(receipt):
            raise StaleFence("Original deployment native version cannot be rebound")
        artifact.artifact_metadata = {
            **artifact.artifact_metadata,
            "desired_state": "object_written",
            RECORD: {"intent": intent, "receipt": receipt},
        }
        artifact.object_uri = intent["uri"]


def reconcile_deployment_object(factory, entry_id, worker_id):
    delivery = None
    unresolved = False
    try:
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, command, run, artifact = selected
            delivery = canonical_request_hash(_snapshot(entry))
            record = _record(artifact)
            terminal = run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.PREEMPTED}
            terminal = terminal or command.status in {CommandStatus.CANCELLED, CommandStatus.FAILED}
            if terminal and record is None:
                _complete(db, entry, command, worker_id)
                return
            artifact_id = artifact.id
            if record is None:
                source, body, key = _authority(db, entry, run, artifact)
                descriptor, _ = _descriptor(artifact)
                pending_uri = str(entry.payload["dockerfile_uri"])
            else:
                intent = record["intent"]
                if intent["outbox_id"] != str(entry.id):
                    raise StaleFence("Deployment native intent belongs to another command delivery")
                source = intent["source_digest"]

        if record is None:
            store = reconciler.get_object_store()
            intent = dict(
                protocol=PROTOCOL,
                write_id=str(uuid.uuid4()),
                outbox_id=str(entry_id),
                source_digest=source,
                driver=store.driver_name,
                uri=store.uri_for_key(key),
                pending_uri=pending_uri,
                **descriptor,
            )
            if intent["driver"] not in {"s3_compatible", "aws_s3", "gcs", "azure_blob"}:
                raise StaleFence("Deployment objects require immutable native storage")
            with factory() as db, db.begin():
                selected = _select(db, entry_id, worker_id)
                if selected is None:
                    return
                entry, command, run, artifact = selected
                current_source, _, _ = _authority(db, entry, run, artifact)
                if (
                    canonical_request_hash(_snapshot(entry)) != delivery
                    or current_source != source
                    or _record(artifact) is not None
                    or run.status != RunStatus.QUEUED
                    or command.status not in {CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
                ):
                    raise StaleFence("Deployment changed before its native write intent committed")
                artifact.artifact_metadata = {
                    **artifact.artifact_metadata,
                    "native_write_started": True,
                    RECORD: {"intent": intent},
                }
            unresolved = True
            try:
                metadata = write_attempt_output(
                    store,
                    intent["uri"],
                    io.BytesIO(body),
                    byte_size=intent["byte_size"],
                    sha256=intent["sha256"],
                    grant_digest=canonical_request_hash(intent),
                )
            except Exception:
                metadata = inspect_attempt_output(
                    store,
                    intent["uri"],
                    byte_size=intent["byte_size"],
                    sha256=intent["sha256"],
                    grant_digest=canonical_request_hash(intent),
                )
            _fact(factory, artifact_id, intent, metadata)
        elif record.get("receipt") is None:
            unresolved = True
            store = reconciler.get_object_store()
            if (
                store.driver_name != intent["driver"]
                or store.uri_for_key(
                    f"projects/{intent['project_id']}/deployments/{intent['run_id']}/Dockerfile"
                )
                != intent["uri"]
            ):
                raise StaleFence("Original deployment storage binding changed")
            metadata = inspect_attempt_output(
                store,
                intent["uri"],
                byte_size=intent["byte_size"],
                sha256=intent["sha256"],
                grant_digest=canonical_request_hash(intent),
            )
            _fact(factory, artifact_id, intent, metadata)
        unresolved = True
        from automl_api.services.deployment_submission import _journal, submit_deployment

        submission_digest = submit_deployment(factory, entry_id, worker_id, delivery, source)
        unresolved = False
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, command, run, artifact = selected
            if canonical_request_hash(_snapshot(entry)) != delivery:
                raise StaleFence("Deployment native completion lost its original delivery")
            record = _record(artifact)
            if record is None or record.get("receipt") is None:
                raise StaleFence("Deployment has no original native completion")
            journal = _journal(artifact)
            if submission_digest != (
                canonical_request_hash(journal) if journal is not None else None
            ):
                raise StaleFence("Kubernetes completion receipts changed before publication")
            if journal is not None and journal["intent"][
                "object_receipt_digest"
            ] != canonical_request_hash(record["receipt"]):
                raise StaleFence(
                    "Original Dockerfile receipt changed before Kubernetes publication"
                )
            if run.status not in {
                RunStatus.CANCELLED,
                RunStatus.FAILED,
                RunStatus.PREEMPTED,
            } and command.status not in {CommandStatus.CANCELLED, CommandStatus.FAILED}:
                if (
                    run.status != RunStatus.QUEUED
                    or run.tags.get("desired_state") != "deployment_pending"
                    or journal is None
                    or set(journal["resources"]) != set(entry.payload["manifests"])
                    or any(value.get("receipt") is None for value in journal["resources"].values())
                ):
                    raise StaleFence(
                        "Deployment publication requires every original Kubernetes receipt"
                    )
                current_source, _, _ = _authority(db, entry, run, artifact)
                if current_source != source:
                    raise StaleFence("Deployment source changed during native object publication")
                run.status = RunStatus.RUNNING
                run.started_at = run.started_at or datetime.now(UTC)
                run.tags = {**run.tags, "desired_state": "deployment_active"}
            _complete(db, entry, command, worker_id)
    except StaleFence:
        raise
    except Exception as exc:
        with factory() as db, db.begin():
            entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
            if entry is not None:
                if canonical_request_hash(_snapshot(entry)) != delivery:
                    raise StaleFence("Deployment failure lost its original delivery") from None
                if unresolved:
                    defer_outbox(db, entry, worker_id=worker_id)
                    entry.last_error = (
                        "Deployment native operation requires original receipt recovery"
                    )
                else:
                    complete_outbox(
                        db,
                        entry.id,
                        worker_id=worker_id,
                        delivered=False,
                        error=redact_text(str(exc)),
                    )
        raise
