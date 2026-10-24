"""Namespace-bound legacy observation and durable, UID-conditional Job retirement."""

import logging
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace

from kubernetes.client import ApiException
from sqlalchemy import select

from automl_api.core.redaction import redact_text
from automl_api.models.enums import CommandStatus, OutboxStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import (
    StaleFence,
    begin_command,
    canonical_request_hash,
    complete_outbox,
    defer_outbox,
    enqueue_outbox,
    transition_command,
)

LOGGER = logging.getLogger(__name__)
TOPIC = "kubernetes.analysis.legacy.cancel"
OPERATION = "legacy.analysis.cleanup"


@dataclass(frozen=True)
class LegacyObservation:
    run_id: uuid.UUID
    project_id: uuid.UUID
    snapshot_digest: str
    namespace: str
    job_name: str
    uid: str | None = None
    state: str | None = None
    failure: tuple[str, str] | None = None


def _request(run):
    if not run.k8s_namespace or not run.k8s_job_name:
        raise StaleFence("Legacy observation requires its stored namespace and Job name.")
    return LegacyObservation(
        run_id=run.id,
        project_id=run.project_id,
        snapshot_digest=canonical_request_hash(
            {column.key: getattr(run, column.key) for column in ModelRun.__table__.columns}
        ),
        namespace=run.k8s_namespace,
        job_name=run.k8s_job_name,
    )


def _read_job(k8s, target):
    try:
        return k8s.batch.read_namespaced_job(
            name=target["job_name"],
            namespace=target["namespace"],
            _request_timeout=5,
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
        return None


def _verify_job(job, target):
    meta = job.metadata
    if (
        meta.name != target["job_name"]
        or meta.namespace != target["namespace"]
        or not isinstance(meta.uid, str)
        or not meta.uid
        or any(
            (meta.labels or {}).get("automl.platform/" + key) != target[value]
            for key, value in (("project-id", "project_id"), ("run-id", "run_id"))
        )
    ):
        raise StaleFence("The legacy Job identity differs from its recorded run.")


def inspect_legacy_observation(k8s, request):
    target = {
        "project_id": str(request.project_id),
        "run_id": str(request.run_id),
        "namespace": request.namespace,
        "job_name": request.job_name,
    }
    job = _read_job(k8s, target)
    if job is None:
        return replace(request, state="missing")
    _verify_job(job, target)
    uid = job.metadata.uid
    state = k8s.job_state(request.job_name, namespace=request.namespace, uid=uid)
    if state in {"image_pull_backoff", "terminal_waiting_failure"}:
        state = k8s.job_state(request.job_name, namespace=request.namespace, uid=uid)
    failure = (
        k8s.job_failure_details(request.job_name, namespace=request.namespace, uid=uid)
        if state in {"failed", "terminal_waiting_failure", "image_pull_backoff"}
        else None
    )
    current = _read_job(k8s, target)
    if current is None:
        return replace(request, uid=uid, state="missing")
    _verify_job(current, target)
    if current.metadata.uid != uid:
        raise StaleFence("The legacy Job was replaced during observation.")
    return replace(request, uid=uid, state=state, failure=failure)


def _enqueue_cleanup(db, run, observed):
    if not observed.uid:
        raise StaleFence("Legacy cleanup requires a verified native Job UID.")
    target = {
        "project_id": str(run.project_id),
        "run_id": str(run.id),
        "namespace": observed.namespace,
        "job_name": observed.job_name,
        "uid": observed.uid,
    }
    command, _ = begin_command(
        db,
        project_id=run.project_id,
        actor_id=run.created_by_id,
        operation=OPERATION,
        idempotency_key=f"{run.id}:{observed.uid}",
        payload=target,
    )
    command.resource_type, command.resource_id, command.max_retries = "model_run", run.id, 30
    enqueue_outbox(
        db,
        command,
        topic=TOPIC,
        aggregate_type="model_run",
        aggregate_id=run.id,
        payload=target,
        event_key=f"legacy-analysis:{run.id}:{observed.uid}:cleanup",
    )
    run.tags = {**run.tags, "legacy_cleanup_command_id": str(command.id)}


def observe_jobs(db, *, read, limit=25, run_id=None):
    from automl_api.services.training import _sync_run_status

    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.run_kind.in_({RunKind.EXPLAINABILITY, RunKind.VALIDATION, RunKind.DRIFT}),
                ModelRun.status.in_({RunStatus.QUEUED, RunStatus.RUNNING}),
                ModelRun.tags["desired_state"].astext == "kubernetes_submitted",
                ModelRun.tags["workflow_attempt_id"].astext.is_(None),
            )
            .where(ModelRun.id == run_id if run_id is not None else True)
            .order_by(ModelRun.updated_at, ModelRun.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
    )
    for run in runs:
        request = _request(run)
        observed = read(request)
        if observed is None or replace(observed, uid=None, state=None, failure=None) != request:
            continue
        cached = SimpleNamespace(
            job_state=lambda _name, current=observed: current.state,
            job_failure_details=lambda _name, current=observed: current.failure,
            delete_job=lambda _name, current=observed, resource=run: _enqueue_cleanup(
                db, resource, current
            ),
        )
        _sync_run_status(db, run, cached, observe_managed=True)
    return len(runs)


def observe_legacy_once(session_factory, k8s):
    from automl_api.services.reconciler import observe_analysis_jobs

    requests = []
    with session_factory() as db:
        selected = observe_analysis_jobs(db, read=requests.append)
        db.commit()
    for request in requests:
        try:
            observed = inspect_legacy_observation(k8s, request)
            with session_factory() as db:
                observe_analysis_jobs(
                    db, run_id=request.run_id, read=lambda _current, value=observed: value
                )
                db.commit()
        except Exception as exc:
            LOGGER.error(
                "Legacy analysis observation unavailable",
                extra={"run_id": str(request.run_id), "error_type": type(exc).__name__},
            )
    return selected


def has_unretired_cleanup(db, project_id, run_ids=None):
    db.flush()
    query = select(WorkflowCommand.id).where(
        WorkflowCommand.project_id == project_id,
        WorkflowCommand.operation == OPERATION,
        WorkflowCommand.status != CommandStatus.SUCCEEDED,
    )
    if run_ids is not None:
        query = query.where(WorkflowCommand.resource_id.in_(run_ids))
    return db.scalar(query.limit(1)) is not None


@dataclass(frozen=True)
class _CleanupClaim:
    entry_id: uuid.UUID
    command_id: uuid.UUID
    request_hash: str
    target: dict[str, str]
    retired: bool


def _live(entry, worker_id):
    return (
        entry is not None
        and entry.status == OutboxStatus.CLAIMED
        and entry.lease_owner == worker_id
        and entry.lease_expires_at is not None
        and entry.lease_expires_at > datetime.now(UTC)
    )


def _claim(db, entry_id, worker_id, *, start=False):
    entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
    if entry is None or entry.status == OutboxStatus.DELIVERED:
        return None
    if not _live(entry, worker_id):
        raise StaleFence("Legacy cleanup outbox ownership expired or changed.")
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    target = dict(entry.payload)
    if (
        entry.topic != TOPIC
        or entry.aggregate_type != "model_run"
        or set(target) != {"project_id", "run_id", "namespace", "job_name", "uid"}
        or any(not isinstance(value, str) or not value for value in target.values())
        or command is None
        or command.operation != OPERATION
        or command.project_id != entry.project_id
        or command.resource_type != "model_run"
        or command.resource_id != entry.aggregate_id
        or target["project_id"] != str(entry.project_id)
        or target["run_id"] != str(entry.aggregate_id)
        or command.request_payload != target
        or canonical_request_hash(target) != command.request_hash
        or command.status
        not in {CommandStatus.PENDING, CommandStatus.RUNNING, CommandStatus.SUCCEEDED}
    ):
        raise StaleFence("Legacy cleanup command or native target binding changed.")
    receipt = command.response_payload.get("retired")
    if receipt is not None and (
        not isinstance(receipt, dict)
        or receipt.get("request_hash") != command.request_hash
        or any(receipt.get(key) != target[key] for key in ("uid", "namespace", "job_name"))
        or not isinstance(receipt.get("observed_at"), str)
    ):
        raise StaleFence("Legacy cleanup retirement receipt has another target binding.")
    retired = receipt is not None
    if not retired:
        run = db.get(ModelRun, entry.aggregate_id, with_for_update=True, populate_existing=True)
        if (
            run is None
            or run.project_id != entry.project_id
            or run.status not in {RunStatus.FAILED, RunStatus.CANCELLED}
            or run.k8s_namespace != target["namespace"]
            or run.k8s_job_name != target["job_name"]
            or run.tags.get("legacy_cleanup_command_id") != str(command.id)
        ):
            raise StaleFence("Legacy cleanup no longer owns the terminal run.")
        transition_command(command, CommandStatus.RUNNING)
    command.lease_owner, command.lease_expires_at = worker_id, entry.lease_expires_at
    command.heartbeat_at = datetime.now(UTC)
    if start and not retired:
        payload = dict(command.response_payload)
        payload.setdefault("native_nonce", uuid.uuid4().hex)
        payload.setdefault("native_started_at", datetime.now(UTC).isoformat())
        command.response_payload = payload
    return _CleanupClaim(entry.id, command.id, command.request_hash, target, retired)


def _record_fact(session_factory, claim, key):
    with session_factory() as db, db.begin():
        command = db.get(
            WorkflowCommand, claim.command_id, with_for_update=True, populate_existing=True
        )
        if (
            command is None
            or command.operation != OPERATION
            or command.request_payload != claim.target
            or command.request_hash != claim.request_hash
            or canonical_request_hash(command.request_payload) != claim.request_hash
        ):
            raise StaleFence("Legacy cleanup native completion lost its immutable command binding.")
        payload = dict(command.response_payload)
        payload.setdefault(
            key,
            {
                "uid": claim.target["uid"],
                "request_hash": claim.request_hash,
                "namespace": claim.target["namespace"],
                "job_name": claim.target["job_name"],
                "observed_at": datetime.now(UTC).isoformat(),
            },
        )
        command.response_payload = payload


def _owned_pods(k8s, target):
    owned = []
    cursor, seen = None, set()
    while True:
        page = k8s.core.list_namespaced_pod(
            namespace=target["namespace"], limit=250, _continue=cursor, _request_timeout=5
        )
        for pod in page.items:
            if pod.metadata.namespace != target["namespace"]:
                raise StaleFence("Legacy retirement received a Pod from another namespace.")
            if any(ref.uid == target["uid"] for ref in pod.metadata.owner_references or []):
                owned.append(pod)
        cursor = getattr(page.metadata, "_continue", None) if page.metadata else None
        if not cursor:
            return owned
        if cursor in seen:
            raise RuntimeError("Legacy retirement Pod pagination did not advance.")
        seen.add(cursor)


def _physical_state(k8s, target):
    job = _read_job(k8s, target)
    if job is not None and job.metadata.uid == target["uid"]:
        _verify_job(job, target)
    else:
        job = None
    return job, _owned_pods(k8s, target)


def _settle(session_factory, entry_id, worker_id, *, pending=False, error=None):
    with session_factory() as db, db.begin():
        entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
        if not _live(entry, worker_id):
            return
        if pending:
            defer_outbox(db, entry, worker_id=worker_id, delay_seconds=5)
            entry.last_error = "Waiting for legacy Job and owned Pods to retire."
        elif error is not None:
            complete_outbox(
                db, entry_id, worker_id=worker_id, delivered=False, error=redact_text(str(error))
            )
        else:
            claim = _claim(db, entry_id, worker_id)
            if claim is None or not claim.retired:
                raise StaleFence("Legacy cleanup has no verified physical retirement receipt.")
            command = db.get(WorkflowCommand, claim.command_id)
            transition_command(command, CommandStatus.SUCCEEDED)
            command.lease_owner = command.lease_expires_at = None
            complete_outbox(db, entry_id, worker_id=worker_id, delivered=True)


def reconcile_legacy_cleanup(session_factory, entry_id, worker_id, *, k8s=None):
    """Reconcile one immutable UID target; expiry alone never proves retirement."""
    try:
        with session_factory() as db, db.begin():
            claim = _claim(db, entry_id, worker_id)
        if claim is None:
            return
        if claim.retired:
            _settle(session_factory, entry_id, worker_id)
            return
        k8s = k8s or KubernetesTrainingClient()
        job, pods = _physical_state(k8s, claim.target)
        if job is not None and not job.metadata.deletion_timestamp:
            with session_factory() as db, db.begin():
                current = _claim(db, entry_id, worker_id, start=True)
            if current is None or current.retired:
                if current is not None:
                    _settle(session_factory, entry_id, worker_id)
                return
            try:
                k8s.delete_job(
                    claim.target["job_name"],
                    namespace=claim.target["namespace"],
                    uid=claim.target["uid"],
                )
            except ApiException as exc:
                if exc.status not in {404, 409}:
                    raise
            else:
                _record_fact(session_factory, claim, "native_ack")
            job, pods = _physical_state(k8s, claim.target)
        if job is not None or pods:
            _settle(session_factory, entry_id, worker_id, pending=True)
            return
        _record_fact(session_factory, claim, "retired")
        _settle(session_factory, entry_id, worker_id)
    except StaleFence:
        raise
    except Exception as exc:
        _settle(session_factory, entry_id, worker_id, error=exc)
        raise
