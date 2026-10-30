"""First-delivery analysis migration with independent native absence facts."""

import copy
import json
import uuid

from kubernetes.client import ApiException
from sqlalchemy import select
from urllib3.exceptions import HTTPError

from automl_api.core.redaction import redact_text
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    DatasetStatus,
    OutboxStatus,
    ProjectRole,
    ProjectStatus,
    RunKind,
    RunStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionTombstone, OutboxEntry, WorkflowCommand
from automl_api.services import ray_submission as journal
from automl_api.services import reconciler
from automl_api.services.dataset_digests import require_verified_digest
from automl_api.services.projects import user_has_project_role
from automl_api.services.workflow_state import StaleFence, complete_outbox, defer_outbox
from automl_api.services.workflow_state import canonical_request_hash as digest

KEY = "analysis_legacy_migration"


def _scope(db, entry_id, worker_id):
    old = db.get(OutboxEntry, entry_id, populate_existing=True)
    if old is None or old.status == OutboxStatus.DELIVERED:
        return None
    project = db.get(
        Project, old.project_id, with_for_update={"read": True}, populate_existing=True
    )
    reconciler.prepare_legacy_analysis_delivery(db, old, first_delivery=KEY not in old.payload)
    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
    if entry is None:
        return None
    state = entry.payload.get(KEY)
    run = reconciler.prepare_legacy_analysis_delivery(db, entry, first_delivery=state is None)
    command = db.get(
        WorkflowCommand, entry.command_id, with_for_update=True, populate_existing=True
    )
    actor = db.get(User, command.actor_id, with_for_update={"read": True}, populate_existing=True)
    deleting = (
        db.scalar(
            select(DeletionTombstone.id)
            .where(
                DeletionTombstone.project_id == project.id,
                (DeletionTombstone.resource_type == "project")
                | (
                    (DeletionTombstone.resource_type == "run")
                    & (DeletionTombstone.resource_id == run.id)
                ),
            )
            .limit(1)
        )
        if project
        else None
    )
    if (
        entry.topic != "kubernetes.analysis.submit"
        or project is None
        or project.status != ProjectStatus.ACTIVE
        or project.settings.get("deletion_in_progress")
        or deleting is not None
        or actor is None
        or not actor.is_active
        or not user_has_project_role(db, actor, project.id, ProjectRole.EDITOR)
        or run.run_kind not in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}
        or command.operation != f"{run.run_kind.value}.launch"
        or command.resource_type not in {"model_run", command.operation}
        or command.resource_id != run.id
        or command.request_hash != digest(command.request_payload)
    ):
        raise StaleFence("Current legacy analysis actor, project or source is unavailable.")
    version = db.get(DatasetVersion, run.dataset_version_id, populate_existing=True)
    if (
        version is None
        or version.project_id != run.project_id
        or version.status in {DatasetStatus.ARCHIVED, DatasetStatus.FAILED}
    ):
        raise StaleFence("Legacy analysis dataset source is unavailable.")
    require_verified_digest(version)
    return entry, run, command


def _source(rows):
    entry, run, command = rows
    value = dict(
        protocol="analysis.legacy.factory.v1",
        source_payload={k: v for k, v in entry.payload.items() if k != KEY},
        command={
            k: getattr(command, k)
            for k in (
                "id",
                "project_id",
                "actor_id",
                "operation",
                "request_hash",
                "resource_type",
                "resource_id",
                "replayed_by",
            )
        },
        run={
            k: getattr(run, k)
            for k in (
                "id",
                "project_id",
                "created_by_id",
                "run_kind",
                "status",
                "dataset_version_id",
                "params",
                "gpu_requested",
                "k8s_namespace",
                "k8s_job_name",
                "tags",
            )
        },
    )
    return json.loads(json.dumps(value, default=str))


def _delivery(entry):
    return digest(
        {
            k: getattr(entry, k)
            for k in ("id", "status", "lease_owner", "lease_expires_at", "delivery_attempts")
        }
    )


def _state(entry):
    state = entry.payload.get(KEY)
    if (
        not isinstance(state, dict)
        or state.get("intent_sha256") != digest(state.get("intent"))
        or not isinstance(state.get("units"), dict)
        or not isinstance(state.get("results"), dict)
        or state.get("first_delivery") != 1
    ):
        raise StaleFence("Original legacy migration journal changed.")
    if not set(state["results"]) <= set(state["units"]):
        raise StaleFence("Legacy migration result lost its unit.")
    for key, unit in state["units"].items():
        if unit.get("operation") != key or unit.get("intent_sha256") != state["intent_sha256"]:
            raise StaleFence("Legacy migration unit lost original intent.")
        result = state["results"].get(key)
        if result is not None and (
            result.get("start_sha256") != digest(unit) or type(result.get("absent")) is not bool
        ):
            raise StaleFence("Legacy migration fact lost its unit.")
    return copy.deepcopy(state)


def _put(entry, state):
    entry.payload = {**entry.payload, KEY: state}


def migrate_legacy_delivery(factory, entry_id, worker_id, *, k8s_factory):
    """Return true when this delivery is legacy, terminal or already consumed."""
    with factory() as db:
        peek = db.get(OutboxEntry, entry_id)
        if peek is None or peek.status == OutboxStatus.DELIVERED:
            return True
        if (
            peek.payload.get("manifest", {})
            .get("metadata", {})
            .get("labels", {})
            .get("automl.platform/attempt-id")
        ):
            return False
    try:
        with factory() as db, db.begin():
            rows = _scope(db, entry_id, worker_id)
            if rows is None:
                return True
            entry, run, _ = rows
            if run.status in {RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED}:
                complete_outbox(db, entry.id, worker_id=worker_id, delivered=True)
                return True
            source = _source(rows)
            delivery = _delivery(entry)
            if KEY not in entry.payload:
                _put(
                    entry,
                    dict(
                        intent=source,
                        intent_sha256=digest(source),
                        first_delivery=1,
                        units={},
                        results={},
                    ),
                )
            state = _state(entry)
            if state["intent"] != source:
                raise StaleFence("Original legacy migration source changed.")
            namespace, name = run.k8s_namespace, run.k8s_job_name
        k8s = k8s_factory()
        context = journal._context(k8s)
        binding = {
            k: list(getattr(k8s.settings, k))
            if k == "workload_image_pull_secrets"
            else getattr(k8s.settings, k)
            for k in ("training_image", "training_image_pull_policy", "workload_image_pull_secrets")
        }
        with factory() as db, db.begin():
            rows = _scope(db, entry_id, worker_id)
            if rows is None:
                return True
            entry = rows[0]
            state = _state(entry)
            if _source(rows) != source or _delivery(entry) != delivery:
                raise StaleFence("Legacy migration source or delivery changed before inspection.")
            if any(
                u["context_sha256"] != context or u["binding"] != binding
                for u in state["units"].values()
            ):
                raise StaleFence("Original legacy migration context/config changed.")
            key = str(uuid.uuid4())
            unit = dict(
                operation=key,
                intent_sha256=state["intent_sha256"],
                context_sha256=context,
                binding=binding,
                delivery=delivery,
            )
            state["units"][key] = unit
            _put(entry, state)
        job_absent = pods_absent = None
        failure = None
        try:
            journal._same_context(k8s, context)
            try:
                k8s.batch.read_namespaced_job(name=name, namespace=namespace, _request_timeout=5)
            except ApiException as exc:
                if exc.status != 404:
                    raise
                job_absent = True
            else:
                job_absent = False
            journal._same_context(k8s, context)
            pods = k8s.core.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"automl.platform/run-id={source['run']['id']}",
                _request_timeout=5,
            )
            if not isinstance(pods.items, list):
                raise ValueError("Legacy Pod inventory did not return a complete list.")
            pods_absent = not pods.items
            journal._same_context(k8s, context)
        except Exception as exc:
            failure = exc
        fact = dict(
            start_sha256=digest(unit),
            absent=job_absent is True and pods_absent is True and failure is None,
            job_absent=job_absent,
            pods_absent=pods_absent,
            error_type=type(failure).__name__ if failure else None,
        )
        with factory() as db, db.begin():
            entry = db.get(OutboxEntry, entry_id, with_for_update=True, populate_existing=True)
            if entry is None:
                raise StaleFence("Original legacy migration journal disappeared.")
            state = _state(entry)
            if state["intent"] != source or state["units"].get(key) != unit:
                raise StaleFence("Original legacy migration unit changed before fact custody.")
            if key in state["results"]:
                raise StaleFence("Original legacy migration fact already exists.")
            state["results"][key] = fact
            _put(entry, state)
        if failure is not None:
            if isinstance(failure, (ApiException, HTTPError)):
                with factory() as db, db.begin():
                    entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
                    if entry is not None:
                        defer_outbox(db, entry, worker_id=worker_id)
                        entry.last_error = redact_text(
                            f"Legacy analysis preflight unavailable: {failure}"
                        )[:2000]
                return True
            raise failure
        if not fact["absent"]:
            raise StaleFence("Legacy Job or Pods remain; controlled replay is unavailable.")
        journal._same_context(k8s, context)
        with factory() as db, db.begin():
            rows = _scope(db, entry_id, worker_id)
            if rows is None:
                return True
            entry, run, _ = rows
            state = _state(entry)
            if (
                _source(rows) != source
                or _delivery(entry) != delivery
                or state["units"].get(key) != unit
                or state["results"].get(key) != fact
            ):
                raise StaleFence("Late legacy absence cannot publish a changed scope/delivery.")
            if binding != {
                k: list(getattr(k8s.settings, k))
                if k == "workload_image_pull_secrets"
                else getattr(k8s.settings, k)
                for k in binding
            }:
                raise StaleFence("Legacy migration binding configuration changed.")
            state["selected_unit"] = key
            _put(entry, state)
            reconciler.publish_legacy_analysis_delivery(
                db,
                entry,
                run,
                k8s,
                migration_fact=dict(
                    unit_sha256=digest(unit),
                    result_sha256=digest(fact),
                    source_sha256=digest(source),
                ),
            )
            defer_outbox(db, entry, worker_id=worker_id)
        return True
    except Exception as exc:
        try:
            with factory() as db, db.begin():
                entry = reconciler._lock_outbox_delivery(db, entry_id, worker_id)
                if entry is not None:
                    entry.last_error = redact_text(str(exc))
        except StaleFence:
            pass
        raise
