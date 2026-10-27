"""Committed one-shot Kubernetes creation and original native acknowledgement recovery."""

import copy
import uuid

from automl_api.models.enums import CommandStatus, RunStatus
from automl_api.models.runs import RunArtifact
from automl_api.services.workflow_state import StaleFence, canonical_request_hash

PROTOCOL = "deployment-kubernetes-v1"
RECORD = "deployment_kubernetes"
ANNOTATION = "automl.platform/creation-intent"


def _terminal(run, command):
    return run.status in {
        RunStatus.CANCELLED,
        RunStatus.FAILED,
        RunStatus.PREEMPTED,
    } or command.status in {CommandStatus.CANCELLED, CommandStatus.FAILED}


def _journal(artifact):
    journal = artifact.artifact_metadata.get(RECORD)
    if journal is None:
        if artifact.artifact_metadata.get("kubernetes_creation_started"):
            raise StaleFence("Original Kubernetes creation history is missing")
        return None
    intent = journal["intent"]
    if (
        set(intent)
        != {
            "protocol",
            "write_id",
            "outbox_id",
            "source_digest",
            "namespace",
            "namespace_uid",
            "manifest_digest",
            "object_receipt_digest",
        }
        or intent["protocol"] != PROTOCOL
    ):
        raise StaleFence("Original Kubernetes creation intent is malformed")
    uuid.UUID(intent["write_id"])
    uuid.UUID(intent["outbox_id"])
    if not all(isinstance(intent[key], str) and intent[key] for key in intent):
        raise StaleFence("Original Kubernetes creation identity is missing")
    if not isinstance(journal["resources"], dict) or set(journal["resources"]) - {
        "deployment",
        "service",
        "ingress",
    }:
        raise StaleFence("Original Kubernetes resource history is malformed")
    if (
        journal["resources"]
        and artifact.artifact_metadata.get("kubernetes_creation_started") is not True
    ):
        raise StaleFence("Original Kubernetes start marker is missing")
    if not journal["resources"] and artifact.artifact_metadata.get("kubernetes_creation_started"):
        raise StaleFence("Original Kubernetes started resource history is missing")
    for value in journal["resources"].values():
        if (
            set(value)
            not in ({"started", "request_digest"}, {"started", "request_digest", "receipt"})
            or not isinstance(value.get("request_digest"), str)
            or len(value["request_digest"]) != 64
            or value.get("started") is not True
        ):
            raise StaleFence("Original Kubernetes start history is malformed")
        receipt = value.get("receipt")
        if receipt is not None and (
            set(receipt) != {"uid", "spec_digest", "request_digest", "intent_digest"}
            or not all(isinstance(v, str) and v for v in receipt.values())
            or receipt["intent_digest"] != canonical_request_hash(intent)
        ):
            raise StaleFence("Original Kubernetes receipt is malformed")
    return journal


def unretired_submission(artifact):
    try:
        journal = _journal(artifact)
        if journal is None or not journal["resources"]:
            return False
        receipt = artifact.artifact_metadata.get(RETIREMENT)
        return not isinstance(receipt, dict) or canonical_request_hash(
            receipt
        ) != canonical_request_hash(retirement_receipt(journal))
    except (StaleFence, ValueError, KeyError, TypeError, AttributeError):
        return True


class _Custody:
    def __init__(self, factory, k8s, entry_id, worker_id, delivery, source, artifact_id, intent):
        self.receipts = {}
        self.factory, self.k8s, self.entry_id, self.worker_id = factory, k8s, entry_id, worker_id
        self.delivery, self.source, self.artifact_id, self.intent = (
            delivery,
            source,
            artifact_id,
            intent,
        )

    def check_namespace(self):
        # shortcut: namespace POST has no UID precondition; add admission before production.
        namespace = self.k8s.core.read_namespace(self.intent["namespace"], _request_timeout=5)
        metadata = namespace.metadata
        if (
            metadata.name != self.intent["namespace"]
            or metadata.uid != self.intent["namespace_uid"]
            or metadata.deletion_timestamp is not None
        ):
            raise StaleFence("Original Kubernetes namespace lifetime changed")

    def start(self, key, resource):
        from automl_api.services.deployment_objects import _authority, _record, _select, _snapshot

        with self.factory() as db, db.begin():
            selected = _select(db, self.entry_id, self.worker_id)
            if selected is None:
                raise StaleFence("Kubernetes creation lost its delivery")
            entry, command, run, artifact = selected
            if (
                canonical_request_hash(_snapshot(entry)) != self.delivery
                or artifact.id != self.artifact_id
            ):
                raise StaleFence("Kubernetes creation lost its original delivery")
            journal = _journal(artifact)
            if journal is None or journal["intent"] != self.intent:
                raise StaleFence("Original Kubernetes creation intent changed")
            if key in journal["resources"]:
                return False
            if _terminal(run, command):
                return None
            source, _, _ = _authority(db, entry, run, artifact)
            if (
                source != self.source
                or run.status != RunStatus.QUEUED
                or run.tags.get("desired_state") != "deployment_pending"
                or canonical_request_hash(_record(artifact)["receipt"])
                != self.intent["object_receipt_digest"]
            ):
                raise StaleFence("Kubernetes creation source changed")
            resources = {
                **journal["resources"],
                key: {"started": True, "request_digest": canonical_request_hash(resource)},
            }
            artifact.artifact_metadata = {
                **artifact.artifact_metadata,
                "kubernetes_creation_started": True,
                RECORD: {"intent": self.intent, "resources": resources},
            }
        return True

    def record(self, key, resource, existing):
        receipt = dict(
            uid=existing["metadata"]["uid"],
            spec_digest=canonical_request_hash(existing["spec"]),
            request_digest=canonical_request_hash(resource),
            intent_digest=canonical_request_hash(self.intent),
        )
        with self.factory() as db, db.begin():
            artifact = db.get(
                RunArtifact, self.artifact_id, with_for_update=True, populate_existing=True
            )
            if artifact is None:
                raise StaleFence("Original Kubernetes artifact disappeared")
            journal = _journal(artifact)
            if journal is None or journal["intent"] != self.intent:
                raise StaleFence("Original Kubernetes completion intent changed")
            started = journal["resources"].get(key)
            if started is None or started.get("request_digest") != receipt["request_digest"]:
                raise StaleFence("Native Kubernetes completion has no original start")
            if started.get("receipt") is not None and started["receipt"] != receipt:
                raise StaleFence("Original Kubernetes UID or spec cannot be rebound")
            if started.get("receipt") == receipt:
                self.receipts[key] = receipt
                return
            resources = {**journal["resources"], key: {**started, "receipt": receipt}}
            artifact.artifact_metadata = {
                **artifact.artifact_metadata,
                RECORD: {"intent": self.intent, "resources": resources},
            }

        self.receipts[key] = receipt


def submit_deployment(factory, entry_id, worker_id, delivery, source):
    from automl_api.services import reconciler
    from automl_api.services.deployment_objects import _authority, _record, _select, _snapshot

    with factory() as db, db.begin():
        selected = _select(db, entry_id, worker_id)
        if selected is None:
            raise StaleFence("Kubernetes submission lost its delivery")
        entry, command, run, artifact = selected
        if canonical_request_hash(_snapshot(entry)) != delivery:
            raise StaleFence("Kubernetes submission lost its original delivery")
        journal = _journal(artifact)
        if _terminal(run, command) and journal is None:
            return
        manifests = copy.deepcopy(entry.payload["manifests"])
        namespace, artifact_id = run.k8s_namespace, artifact.id
        record = _record(artifact)
        if record is None or record.get("receipt") is None:
            raise StaleFence("Kubernetes submission requires original Dockerfile custody")
        object_receipt_digest = canonical_request_hash(record["receipt"])
    k8s = reconciler.KubernetesTrainingClient()
    if k8s.settings.training_namespace != namespace:
        raise StaleFence("Kubernetes submission namespace configuration changed")
    k8s._model_deployment_plans(manifests)
    if journal is None:
        actual = k8s.core.read_namespace(namespace, _request_timeout=5)
        if (
            actual.metadata.name != namespace
            or not actual.metadata.uid
            or actual.metadata.deletion_timestamp is not None
        ):
            raise StaleFence("Kubernetes submission namespace is unavailable")
        intent = dict(
            protocol=PROTOCOL,
            write_id=str(uuid.uuid4()),
            outbox_id=str(entry_id),
            source_digest=source,
            namespace=namespace,
            namespace_uid=actual.metadata.uid,
            manifest_digest=canonical_request_hash(manifests),
            object_receipt_digest=object_receipt_digest,
        )
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                raise StaleFence("Kubernetes submission lost its delivery before intent")
            entry, command, run, artifact = selected
            current_source, _, _ = _authority(db, entry, run, artifact)
            if (
                canonical_request_hash(_snapshot(entry)) != delivery
                or current_source != source
                or _terminal(run, command)
                or run.status != RunStatus.QUEUED
                or run.tags.get("desired_state") != "deployment_pending"
                or _journal(artifact) is not None
                or canonical_request_hash(_record(artifact)["receipt"]) != object_receipt_digest
            ):
                raise StaleFence("Kubernetes submission changed before intent committed")
            journal = {"intent": intent, "resources": {}}
            artifact.artifact_metadata = {**artifact.artifact_metadata, RECORD: journal}
    intent = journal["intent"]
    if (
        intent["outbox_id"] != str(entry_id)
        or intent["source_digest"] != source
        or intent["namespace"] != namespace
        or intent["manifest_digest"] != canonical_request_hash(manifests)
        or intent["object_receipt_digest"] != object_receipt_digest
    ):
        raise StaleFence("Original Kubernetes submission binding changed")
    for key, resource in manifests.items():
        resource["metadata"]["annotations"] = {
            **resource["metadata"].get("annotations", {}),
            ANNOTATION: canonical_request_hash({"intent": intent, "resource": key}),
        }
    custody = _Custody(factory, k8s, entry_id, worker_id, delivery, source, artifact_id, intent)
    k8s.create_model_deployment(manifests, custody=custody)
    return canonical_request_hash(
        {
            "intent": intent,
            "resources": {
                key: {
                    "started": True,
                    "request_digest": receipt["request_digest"],
                    "receipt": receipt,
                }
                for key, receipt in custody.receipts.items()
            },
        }
    )


RETIREMENT = "deployment_kubernetes_retirement"


def retirement_receipt(journal):
    return {
        "journal_digest": canonical_request_hash(journal),
        "namespace_uid": journal["intent"]["namespace_uid"],
        "resource_uids": {
            key: value["receipt"]["uid"] for key, value in journal["resources"].items()
        },
        "workload_absent": True,
    }


def lifecycle_custody(db, run, *, allow_unstarted=False):
    from sqlalchemy import select

    from automl_api.models.enums import ArtifactKind, OutboxStatus, RunKind
    from automl_api.models.workflows import OutboxEntry, WorkflowCommand
    from automl_api.services.deployment_objects import CREATION_TOPICS, _record
    from automl_api.services.deployment_objects import PROTOCOL as OBJECT_PROTOCOL

    artifacts = list(
        db.scalars(
            select(RunArtifact)
            .where(
                RunArtifact.project_id == run.project_id,
                RunArtifact.model_run_id == run.id,
                RunArtifact.kind == ArtifactKind.DEPLOYMENT_IMAGE,
                RunArtifact.name == "Dockerfile",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    if len(artifacts) != 1 or run.run_kind != RunKind.DEPLOYMENT:
        raise StaleFence("Original deployment native artifact is missing or ambiguous")
    artifact = artifacts[0]
    journal = _journal(artifact)
    if allow_unstarted and (journal is None or not journal["resources"]):
        entries = list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.project_id == run.project_id,
                    OutboxEntry.topic.in_(CREATION_TOPICS),
                    OutboxEntry.aggregate_type == "model_run",
                    OutboxEntry.aggregate_id == run.id,
                )
            )
        )
        entry = entries[0] if len(entries) == 1 else None
        command = (
            db.get(WorkflowCommand, entry.command_id, populate_existing=True) if entry else None
        )
        if (
            entry is None
            or entry.payload["run_id"] != str(run.id)
            or artifact.artifact_metadata.get("write_protocol") != OBJECT_PROTOCOL
            or run.tags.get("deployment_write_protocol") != OBJECT_PROTOCOL
            or command is None
            or command.project_id != run.project_id
            or command.actor_id != run.created_by_id
            or command.operation != "deployment.launch"
            or command.resource_type != "deployment.launch"
            or command.resource_id != run.id
            or command.request_hash != canonical_request_hash(command.request_payload)
        ):
            raise StaleFence("Unstarted deployment requires its original modern admission")
        return {"unstarted": True, "retired": True}
    original_object = _record(artifact)
    if journal is None or original_object is None or original_object.get("receipt") is None:
        raise StaleFence("Deployment requires original native submission custody")
    intent = journal["intent"]
    entry = db.get(OutboxEntry, uuid.UUID(intent["outbox_id"]), populate_existing=True)
    if entry is None:
        raise StaleFence("Original deployment admission no longer exists")
    command = db.get(WorkflowCommand, entry.command_id, populate_existing=True)
    if (
        entry.project_id != run.project_id
        or entry.status != OutboxStatus.DELIVERED
        or entry.topic not in CREATION_TOPICS
        or entry.aggregate_type != "model_run"
        or entry.aggregate_id != run.id
        or entry.payload["run_id"] != str(run.id)
        or command is None
        or command.project_id != run.project_id
        or command.actor_id != run.created_by_id
        or command.operation != "deployment.launch"
        or command.resource_type != "deployment.launch"
        or command.resource_id != run.id
        or command.request_hash != canonical_request_hash(command.request_payload)
        or intent["namespace"] != run.k8s_namespace
        or intent["object_receipt_digest"] != canonical_request_hash(original_object["receipt"])
    ):
        raise StaleFence("Original deployment native admission binding changed")
    manifests = copy.deepcopy(entry.payload["manifests"])
    if (
        intent["manifest_digest"] != canonical_request_hash(manifests)
        or set(journal["resources"]) != set(manifests)
        or not {"deployment", "service"}.issubset(manifests)
        or manifests["deployment"]["metadata"]["name"] != run.k8s_job_name
        or run.params.get("replicas") != manifests["deployment"]["spec"]["replicas"]
    ):
        raise StaleFence("Original deployment native manifest binding changed")
    resources = {}
    intent_digest = canonical_request_hash(intent)
    for key, resource in manifests.items():
        resource["metadata"]["annotations"] = {
            **resource["metadata"].get("annotations", {}),
            ANNOTATION: canonical_request_hash({"intent": intent, "resource": key}),
        }
        record = journal["resources"][key]
        receipt = record.get("receipt")
        if (
            receipt is None
            or receipt["request_digest"] != canonical_request_hash(resource)
            or record["request_digest"] != receipt["request_digest"]
            or receipt["intent_digest"] != intent_digest
            or any(
                resource["metadata"]["labels"].get(k) != v
                for k, v in {
                    "automl.platform/project-id": str(run.project_id),
                    "automl.platform/deployment-id": str(run.id),
                }.items()
            )
        ):
            raise StaleFence("Original deployment native resource receipt changed")
        resources[key] = dict(receipt)
    return dict(
        namespace=intent["namespace"],
        namespace_uid=intent["namespace_uid"],
        intent_digest=intent_digest,
        journal_digest=canonical_request_hash(journal),
        artifact_id=str(artifact.id),
        manifests=manifests,
        resources=resources,
        retired=canonical_request_hash(artifact.artifact_metadata.get(RETIREMENT))
        == canonical_request_hash(retirement_receipt(journal)),
    )


def record_lifecycle_retirement(db, run, custody, receipt):
    artifact = db.get(
        RunArtifact, uuid.UUID(custody["artifact_id"]), with_for_update=True, populate_existing=True
    )
    if artifact is None or artifact.project_id != run.project_id or artifact.model_run_id != run.id:
        raise StaleFence("Original deployment retirement artifact disappeared")
    journal = _journal(artifact)
    if (
        journal is None
        or canonical_request_hash(journal) != custody["journal_digest"]
        or not isinstance(receipt, dict)
        or canonical_request_hash(receipt) != canonical_request_hash(retirement_receipt(journal))
    ):
        raise StaleFence("Native retirement does not match the original deployment")
    prior = artifact.artifact_metadata.get(RETIREMENT)
    if prior is not None and canonical_request_hash(prior) != canonical_request_hash(receipt):
        raise StaleFence("Original deployment retirement evidence cannot be rebound")
    artifact.artifact_metadata = {**artifact.artifact_metadata, RETIREMENT: receipt}
