"""SQL-only control admission and generation-fenced native reconciliation."""

import copy
import uuid
from datetime import UTC, datetime

from kubernetes.client import ApiException
from sqlalchemy.exc import DBAPIError
from urllib3.exceptions import HTTPError as KubernetesTransportError

from automl_api.core.redaction import redact_text
from automl_api.models.enums import CommandStatus, OutboxStatus, ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services.deployment_submission import lifecycle_custody, record_lifecycle_retirement
from automl_api.services.kubernetes_training import (
    KubernetesTrainingClient,
    ModelDeploymentRetirementPending,
)
from automl_api.services.model_custody import deployment_model_custody
from automl_api.services.projects import require_project_role
from automl_api.services.workflow_state import (
    StaleFence,
    begin_command,
    canonical_request_hash,
    complete_outbox,
    defer_outbox,
    enqueue_outbox,
    transition_command,
)

TOPIC = "deployment.control.v1"
OPERATION = "deployment.control.v1"
CONTROL = "deployment_control"
GENERATION = "automl.platform/control-generation"
COMMAND = "automl.platform/control-command"
PENDING = dict(
    stop="deployment_stop_pending",
    start="deployment_start_pending",
    cleanup="deployment_cleanup_pending",
)


def _digest(custody):
    return canonical_request_hash({k: v for k, v in custody.items() if k != "retired"})


def queue_control(db, user, run, action, custody):
    current = run.tags.get(CONTROL) or {}
    if current.get("action") == "cleanup" and action == "start":
        raise StaleFence("Cleanup was admitted; deploy the saved model again instead of restarting")
    if current.get("action") == action and current.get("state") == "pending":
        return
    previous = current.get("generation", 0)
    if type(previous) is not int or previous < 0:
        raise StaleFence("Original deployment control generation is malformed")
    generation = previous + 1
    if generation < 1:
        raise StaleFence("Original deployment control generation is malformed")
    request = dict(
        protocol=OPERATION,
        actor_id=str(user.id),
        run_id=str(run.id),
        generation=generation,
        action=action,
        custody_digest=_digest(custody),
        replicas=max(1, int(run.params.get("replicas", 1))) if action == "start" else 0,
    )
    command, replay = begin_command(
        db,
        project_id=run.project_id,
        actor_id=user.id,
        operation=OPERATION,
        idempotency_key=f"{run.id}:{generation}",
        payload=request,
    )
    if replay:
        raise StaleFence("Deployment control generation already has an original command")
    command.status = CommandStatus.RUNNING
    command.resource_type = OPERATION
    command.resource_id = run.id
    enqueue_outbox(
        db,
        command,
        topic=TOPIC,
        aggregate_type="model_run",
        aggregate_id=run.id,
        payload={"request": request},
    )
    run.status = RunStatus.QUEUED if action == "start" else RunStatus.CANCELLED
    run.finished_at = None if action == "start" else datetime.now(UTC)
    run.tags = {
        **run.tags,
        "desired_state": PENDING[action],
        CONTROL: dict(
            generation=generation, command_id=str(command.id), action=action, state="pending"
        ),
    }
    db.flush()


def _delivery(entry):
    return canonical_request_hash(
        {
            key: getattr(entry, key)
            for key in (
                "id",
                "project_id",
                "command_id",
                "topic",
                "aggregate_type",
                "aggregate_id",
                "status",
                "lease_owner",
                "lease_expires_at",
                "delivery_attempts",
            )
        }
    )


def _current(run, command, request):
    pointer = run.tags.get(CONTROL) or {}
    return all(
        pointer.get(k) == v
        for k, v in dict(
            generation=request["generation"], command_id=str(command.id), action=request["action"]
        ).items()
    )


def _select(db, entry_id, worker_id):
    from automl_api.services.reconciler import _lock_outbox_delivery

    original = db.get(OutboxEntry, entry_id)
    if original is None:
        return None
    project = db.get(
        Project, original.project_id, with_for_update={"read": True}, populate_existing=True
    )
    run = db.get(ModelRun, original.aggregate_id, with_for_update=True, populate_existing=True)
    entry = _lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    request = entry.payload["request"]
    if (
        project is None
        or run is None
        or run.project_id != entry.project_id
        or run.run_kind != RunKind.DEPLOYMENT
        or entry.topic != TOPIC
        or entry.aggregate_type != "model_run"
        or entry.aggregate_id != run.id
        or command is None
        or command.project_id != entry.project_id
        or command.operation != OPERATION
        or command.resource_type != OPERATION
        or command.resource_id != run.id
        or command.request_hash != canonical_request_hash(command.request_payload)
        or command.request_payload != request
        or request["actor_id"] != str(command.actor_id)
        or request["protocol"] != OPERATION
        or request["run_id"] != str(run.id)
        or request["action"] not in PENDING
        or type(request["generation"]) is not int
        or request["generation"] < 1
    ):
        raise StaleFence("Original deployment control command or scope changed")
    if command.status == CommandStatus.PENDING:
        transition_command(command, CommandStatus.RUNNING)
    return entry, command, run, request


def _authority(db, entry, command, run, request):
    if command.status != CommandStatus.RUNNING:
        raise StaleFence("Deployment control command is not active")
    if not _current(run, command, request):
        raise StaleFence("Deployment control was superseded")
    if run.tags.get("desired_state") != PENDING[request["action"]]:
        raise StaleFence("Deployment control lost its pending phase")
    custody = lifecycle_custody(db, run)
    if _digest(custody) != request["custody_digest"]:
        raise StaleFence("Original deployment control custody changed")
    if request["action"] == "start":
        actor = db.get(User, command.actor_id)
        if actor is None or actor.is_active is not True:
            raise StaleFence("Deployment restart actor is no longer active")
        require_project_role(db, actor, run.project_id, ProjectRole.ADMIN)
        deployment_model_custody(db, run)
        if run.status != RunStatus.QUEUED:
            raise StaleFence("Deployment restart lost its logical phase")
    elif run.status != RunStatus.CANCELLED:
        raise StaleFence("Deployment retirement lost logical revocation")
    return custody


def _marker(actual):
    annotations = actual["metadata"].get("annotations") or {}
    generation, command = annotations.get(GENERATION), annotations.get(COMMAND)
    if generation is None and command is None:
        return 0, None
    if not isinstance(generation, str) or str(int(generation)) != generation or int(generation) < 1:
        raise StaleFence("Native deployment control generation is malformed")
    uuid.UUID(command)
    return int(generation), command


def _patch_plan(actual, request, command_id):
    metadata = actual["metadata"]
    version = metadata.get("resourceVersion")
    if not isinstance(version, str) or not version:
        raise StaleFence("Native resourceVersion is missing")
    body = [
        dict(op="test", path="/metadata/uid", value=metadata["uid"]),
        dict(op="test", path="/metadata/resourceVersion", value=version),
        dict(
            op="add",
            path="/metadata/annotations/" + GENERATION.replace("/", "~1"),
            value=str(request["generation"]),
        ),
        dict(
            op="add",
            path="/metadata/annotations/" + COMMAND.replace("/", "~1"),
            value=str(command_id),
        ),
        dict(op="replace", path="/spec/replicas", value=request["replicas"]),
    ]
    return dict(uid=metadata["uid"], resource_version=version, body=body)


def _fact(factory, entry_id, request, observation):
    with factory() as db, db.begin():
        entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
        if entry is None or entry.payload.get("request") != request:
            raise StaleFence("Native control observation lost its original command")
        history = list(entry.payload.get("observations", []))
        if observation not in history:
            history.append(observation)
        entry.payload = {**entry.payload, "observations": history}


def _stopped(k8s, custody, actual):
    if actual is not None:
        generation = actual["metadata"].get("generation")
        observed = (actual.get("status") or {}).get("observedGeneration")
        if type(generation) is not int or type(observed) is not int or observed < generation:
            raise ModelDeploymentRetirementPending(
                "Waiting for the deployment controller to observe shutdown"
            )
    selector = (
        "automl.platform/deployment-id="
        + custody["manifests"]["deployment"]["metadata"]["labels"]["automl.platform/deployment-id"]
    )
    replicas = k8s.apps.list_namespaced_replica_set(
        namespace=custody["namespace"], label_selector=selector, limit=100, _request_timeout=5
    )
    if getattr(replicas.metadata, "_continue", None) or any(
        item.spec.replicas for item in replicas.items
    ):
        raise ModelDeploymentRetirementPending("Waiting for serving ReplicaSets to stop")
    pods = k8s.core.list_namespaced_pod(
        namespace=custody["namespace"], label_selector=selector, limit=1, _request_timeout=5
    )
    if pods.items or getattr(pods.metadata, "_continue", None):
        raise ModelDeploymentRetirementPending("Waiting for serving Pods to retire")


def _finish(db, entry, command, worker_id, *, superseded=False):
    complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
    if command.status == CommandStatus.RUNNING:
        transition_command(command, CommandStatus.SUCCEEDED)
    if superseded:
        command.terminal_reason = "Superseded by a newer deployment control generation"


def reconcile_control(factory, entry_id, worker_id):
    delivery = request = None
    try:
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, command, run, request = selected
            if not _current(run, command, request):
                _finish(db, entry, command, worker_id, superseded=True)
                return
            custody = copy.deepcopy(_authority(db, entry, command, run, request))
            delivery = _delivery(entry)
            command_id = command.id
            name = run.k8s_job_name
        k8s = KubernetesTrainingClient()
        plans = k8s._lifecycle_plans(name, custody)
        try:
            actual = k8s._lifecycle_resource(
                "deployment", "Deployment", "apps", custody, deleting=request["action"] == "cleanup"
            )
        except ApiException as exc:
            if exc.status != 404 or request["action"] == "start":
                raise
            actual = None
        if request["action"] == "start":
            for key, kind, _, api_name in plans:
                if key != "deployment":
                    k8s._lifecycle_resource(key, kind, api_name, custody)
        marker = _marker(actual) if actual is not None else (request["generation"], str(command_id))
        if (
            marker[0] > request["generation"]
            or marker[0] == request["generation"]
            and marker[1] != str(command_id)
        ):
            raise StaleFence("Native deployment belongs to a newer or foreign control")
        if actual is not None and marker[0] < request["generation"]:
            plan = _patch_plan(actual, request, command_id)
            with factory() as db, db.begin():
                selected = _select(db, entry_id, worker_id)
                if selected is None:
                    return
                entry, command, run, current = selected
                if _delivery(entry) != delivery or current != request:
                    raise StaleFence("Control inspection lost its delivery")
                _authority(db, entry, command, run, request)
                if marker[0]:
                    prior = db.get(WorkflowCommand, uuid.UUID(marker[1]))
                    if (
                        prior is None
                        or prior.project_id != run.project_id
                        or prior.operation != OPERATION
                        or prior.resource_id != run.id
                        or prior.request_payload.get("generation") != marker[0]
                    ):
                        raise StaleFence("Native prior generation has no original control command")
                history = list(entry.payload.get("plans", []))
                if not history or history[-1] != plan:
                    history.append(plan)
                entry.payload = {**entry.payload, "plans": history}
            k8s._lifecycle_namespace(custody)
            k8s.apps.patch_namespaced_deployment(
                name=name, namespace=custody["namespace"], body=plan["body"], _request_timeout=5
            )
            actual = k8s._lifecycle_resource(
                "deployment", "Deployment", "apps", custody, deleting=request["action"] == "cleanup"
            )
        if actual is not None and (
            _marker(actual) != (request["generation"], str(command_id))
            or actual["spec"]["replicas"] != request["replicas"]
        ):
            raise StaleFence(
                "Native control acknowledgement differs from its original desired state"
            )
        observation = dict(
            generation=request["generation"],
            command_id=str(command_id),
            uid=custody["resources"]["deployment"]["uid"],
            replicas=request["replicas"],
            resource_version=actual["metadata"].get("resourceVersion") if actual else None,
        )
        _fact(factory, entry_id, request, observation)
        if request["action"] == "stop":
            _stopped(k8s, custody, actual)
        retirement = None
        if request["action"] == "cleanup":
            with factory() as db, db.begin():
                selected = _select(db, entry_id, worker_id)
                if selected is None:
                    return
                entry, command, run, current = selected
                if _delivery(entry) != delivery:
                    raise StaleFence("Cleanup lost its original delivery")
                _authority(db, entry, command, run, request)
            retirement = k8s.delete_model_deployment(
                name, custody=custody, control=(request["generation"], str(command_id))
            )
            _fact(factory, entry_id, request, {"retirement": retirement})
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is None:
                return
            entry, command, run, current = selected
            if _delivery(entry) != delivery:
                raise StaleFence("Native completion lost its delivery")
            if not _current(run, command, request):
                _finish(db, entry, command, worker_id, superseded=True)
                return
            _authority(db, entry, command, run, request)
            tags = {**run.tags, CONTROL: {**run.tags[CONTROL], "state": "complete"}}
            if request["action"] == "start":
                run.status = RunStatus.RUNNING
                run.finished_at = None
                run.failure_code = None
                run.failure_message = None
                tags.update(
                    desired_state="deployment_active", resumed_at=datetime.now(UTC).isoformat()
                )
            elif request["action"] == "stop":
                tags["desired_state"] = "deployment_stopped"
            else:
                record_lifecycle_retirement(db, run, custody, retirement)
                tags.update(
                    desired_state="deployment_cleaned",
                    runtime_cleaned_at=datetime.now(UTC).isoformat(),
                )
            run.tags = tags
            _finish(db, entry, command, worker_id)
    except StaleFence:
        raise
    except Exception as exc:
        if delivery is None:
            raise
        with factory() as db, db.begin():
            selected = _select(db, entry_id, worker_id)
            if selected is not None:
                entry, command, run, current = selected
                if _delivery(entry) != delivery:
                    raise StaleFence("Control failure lost its delivery") from None
                if (
                    isinstance(
                        exc,
                        (
                            ModelDeploymentRetirementPending,
                            OSError,
                            DBAPIError,
                            KubernetesTransportError,
                        ),
                    )
                    or isinstance(exc, ApiException)
                    and exc.status in {0, 409, 422}
                ):
                    defer_outbox(db, entry, worker_id=worker_id)
                    entry.last_error = (
                        "Deployment control awaits native acknowledgement or retirement"
                    )
                else:
                    complete_outbox(
                        db,
                        entry.id,
                        worker_id=worker_id,
                        delivered=False,
                        error=redact_text(str(exc)),
                    )
                    if entry.status == OutboxStatus.DEAD and _current(run, command, request):
                        run.tags = {**run.tags, CONTROL: {**run.tags[CONTROL], "state": "failed"}}
        raise
