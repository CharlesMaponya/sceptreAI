"""Durable creation, observation and cleanup of provider verification Jobs."""

import logging
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from kubernetes.client import ApiException
from sqlalchemy import exists, or_, select

from automl_api.models.datasets import DatasetProviderImport
from automl_api.models.enums import AttemptStatus, CommandStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.attempt_identity import _scope
from automl_api.services.attempt_object_grants import (
    revoke_attempt_grants,
    unretired_grant_deadline,
)
from automl_api.services.evaluation_publication import _append, _event
from automl_api.services.provider_import import (
    _current,
    claim_provider_import,
    fail_provider_import,
)
from automl_api.services.provider_import_attempts import verification_attempt
from automl_api.services.provider_import_workload import (
    LABEL,
    SUBMISSION,
    check_network,
    check_resource,
    serialized,
    workload,
)
from automl_api.services.provider_source import configured_policy
from automl_api.services.workflow_state import StaleFence, canonical_request_hash

LOGGER = logging.getLogger(__name__)


def _prepare(settings):
    def prepare(db, execution, attempt, command):
        resources = workload(execution, attempt, settings)
        command.lease_expires_at = datetime.now(UTC) + timedelta(seconds=600)
        attempt.lease_expires_at = command.lease_expires_at
        authority = {
            "project_id": str(attempt.project_id),
            "dataset_version_id": str(execution.version_id),
            "attempt_id": str(attempt.id),
            "generation": attempt.generation,
            "fence": attempt.fencing_token,
            "identity": attempt.workload_identity,
            "command_id": str(command.id),
            "actor_id": str(command.actor_id),
            "owner": execution.owner,
            "request_digest": command.request_hash,
        }
        _append(db, attempt, SUBMISSION, {"authority": authority, "resources": resources})

    return prepare


def _load(db, version_id):
    version, row, command = _current(db, version_id)
    attempt = verification_attempt(db, row)
    if attempt is None:
        return None
    event = _event(db, attempt.id, SUBMISSION)
    if event is None:
        return SimpleNamespace(id=attempt.id, desired=None, status=attempt.status)
    desired = deepcopy(event.payload)
    authority = desired["authority"]
    if (
        event.event_type != SUBMISSION
        or event.result_digest != canonical_request_hash(desired)
        or authority["project_id"] != str(row.project_id)
        or authority["dataset_version_id"] != str(version.id)
        or authority["command_id"] != str(command.id)
        or authority["actor_id"] != str(command.actor_id)
        or authority["request_digest"] != command.request_hash
        or authority["attempt_id"] != str(attempt.id)
        or authority["fence"] != attempt.fencing_token
        or authority["generation"] != attempt.generation
        or authority["identity"] != attempt.workload_identity
    ):
        raise StaleFence("Provider verification submission no longer matches its execution")
    return SimpleNamespace(
        id=attempt.id,
        desired=desired,
        status=attempt.status,
        cleanup_state=attempt.cleanup_state,
        owner=authority["owner"],
        fence=attempt.fencing_token,
        lease=attempt.lease_expires_at,
        command_status=command.status,
        command_owner=command.lease_owner,
        row_status=row.status,
    )


def _guard(db, version_id, desired):
    snapshot = _load(db, version_id)
    if snapshot is None or snapshot.desired != desired or snapshot.command_owner != snapshot.owner:
        raise StaleFence("Provider verification resource creator lost its execution")
    _scope(
        db, snapshot.id, lock=True, expected_project=uuid.UUID(desired["authority"]["project_id"])
    )
    return db.get(WorkflowAttempt, snapshot.id)


def _read(read, expected):
    metadata = expected["metadata"]
    try:
        return read(name=metadata["name"], namespace=metadata["namespace"], _request_timeout=5)
    except ApiException as error:
        if error.status == 404:
            return None
        raise


def _key(kind, action):
    return f"provider_import.{kind}.{action}"


def _observed(factory, version_id, desired, kind, uid):
    with factory() as db, db.begin():
        snapshot = _load(db, version_id)
        if snapshot is None or snapshot.desired != desired:
            raise StaleFence("Provider verification resource intent changed")
        previous = _event(db, snapshot.id, _key(kind, "observed"))
        payload = {"uid": uid, "submission_digest": canonical_request_hash(desired)}
        if previous is None:
            requested = _event(db, snapshot.id, _key(kind, "requested"))
            if (
                requested is None
                or requested.event_type != _key(kind, "requested")
                or requested.result_digest != canonical_request_hash(requested.payload)
                or requested.payload != {"submission_digest": payload["submission_digest"]}
            ):
                raise StaleFence("Provider verification resource has no owned creation intent")
            _append(db, db.get(WorkflowAttempt, snapshot.id), _key(kind, "observed"), payload)
        elif (
            previous.event_type != _key(kind, "observed")
            or previous.payload != payload
            or previous.result_digest != canonical_request_hash(payload)
        ):
            raise StaleFence("Provider verification resource UID changed")


def _ensure(factory, k8s, version_id, desired):
    resources = desired["resources"]
    for kind, create, read in (
        (
            "service_account",
            k8s.core.create_namespaced_service_account,
            k8s.core.read_namespaced_service_account,
        ),
        (
            "network",
            k8s.networking.create_namespaced_network_policy,
            k8s.networking.read_namespaced_network_policy,
        ),
        ("job", k8s.batch.create_namespaced_job, k8s.batch.read_namespaced_job),
    ):
        expected = resources[kind]
        with factory() as db, db.begin():
            attempt = _guard(db, version_id, desired)
            observed = _event(db, attempt.id, _key(kind, "observed"))
            previous_uid = observed.payload["uid"] if observed is not None else None
        actual = _read(read, expected)
        if actual is not None:
            uid = check_resource(actual, expected)
            if previous_uid is not None and uid != previous_uid:
                raise StaleFence("Provider verification resource was replaced")
            _observed(factory, version_id, desired, kind, uid)
            continue
        if previous_uid is not None:
            raise StaleFence("An owned provider verification resource disappeared")
        if kind == "job":
            check_network(
                k8s, resources, resources["job"]["spec"]["template"]["metadata"]["labels"]
            )
        with factory() as db, db.begin():
            attempt = _guard(db, version_id, desired)
            if _event(db, attempt.id, _key(kind, "requested")) is not None:
                return False
            _append(
                db,
                attempt,
                _key(kind, "requested"),
                {"submission_digest": canonical_request_hash(desired)},
            )
        try:
            actual = create(
                namespace=expected["metadata"]["namespace"], body=expected, _request_timeout=5
            )
        except ApiException as error:
            if error.status == 409:
                actual = _read(read, expected)
                if actual is None:
                    return False
            else:
                if error.status in {400, 401, 403, 404, 422}:
                    with factory() as db, db.begin():
                        snapshot = _load(db, version_id)
                        _append(
                            db,
                            db.get(WorkflowAttempt, snapshot.id),
                            _key(kind, "rejected"),
                            {"status": error.status},
                        )
                raise
        uid = check_resource(actual, expected)
        _observed(factory, version_id, desired, kind, uid)
    return True


def _cleanup(factory, k8s, version_id, desired):
    with factory() as db, db.begin():
        snapshot = _load(db, version_id)
        if snapshot is None or snapshot.desired != desired:
            raise StaleFence("Provider verification cleanup intent changed")
        revoke_attempt_grants(db, snapshot.id)
        records = {
            kind: {
                action: _event(db, snapshot.id, _key(kind, action)) is not None
                for action in ("requested", "observed", "rejected")
            }
            for kind in ("job", "service_account", "network")
        }
    resources = desired["resources"]
    for kind, read, delete in (
        ("job", k8s.batch.read_namespaced_job, k8s.batch.delete_namespaced_job),
        (
            "service_account",
            k8s.core.read_namespaced_service_account,
            k8s.core.delete_namespaced_service_account,
        ),
        (
            "network",
            k8s.networking.read_namespaced_network_policy,
            k8s.networking.delete_namespaced_network_policy,
        ),
    ):
        if kind != "job":
            metadata = resources["job"]["metadata"]
            pods = k8s.core.list_namespaced_pod(
                namespace=metadata["namespace"],
                label_selector=f"{LABEL}={snapshot.id}",
                limit=1,
                _request_timeout=5,
            )
            if pods.items:
                return False
            with factory() as db, db.begin():
                if unretired_grant_deadline(db, snapshot.id) is not None:
                    return False
        expected = resources[kind]
        actual = _read(read, expected)
        if actual is not None:
            uid = check_resource(actual, expected)
            _observed(factory, version_id, desired, kind, uid)
            delete(
                name=expected["metadata"]["name"],
                namespace=expected["metadata"]["namespace"],
                body={"propagationPolicy": "Foreground", "preconditions": {"uid": uid}},
                _request_timeout=5,
            )
            return False
        record = records[kind]
        if record["requested"] and not record["observed"] and not record["rejected"]:
            with factory() as db, db.begin():
                db.get(WorkflowAttempt, snapshot.id).cleanup_state = "resource_unknown"
            return False
    with factory() as db, db.begin():
        current = _load(db, version_id)
        if current is None or current.id != snapshot.id or current.desired != desired:
            raise StaleFence("Provider verification cleanup generation changed")
        publication = _event(db, snapshot.id, "provider_import.descriptor_requested")
        receipt = _event(db, snapshot.id, "provider_import.receipt")
        db.get(WorkflowAttempt, snapshot.id).cleanup_state = (
            "publication_unknown" if publication is not None and receipt is None else "complete"
        )
        return publication is None or receipt is not None


def _stop(factory, version_id, snapshot):
    fail_provider_import(
        factory,
        SimpleNamespace(
            version_id=version_id,
            owner=snapshot.owner,
            attempt_id=snapshot.id,
            fence=snapshot.fence,
        ),
        RuntimeError("Provider verification workload stopped or expired"),
    )


def _reconcile(factory, k8s, version_id):
    with factory() as db, db.begin():
        snapshot = _load(db, version_id)
        version, row, _ = _current(db, version_id)
        project_id, row_status = row.project_id, row.status
    if snapshot is None:
        if row_status != "pending":
            return
        policy, key = configured_policy(project_id, version_id)
        execution = claim_provider_import(
            factory, version_id, policy=policy, public_key=key, prepare=_prepare(k8s.settings)
        )
        if execution is None:
            return
        with factory() as db, db.begin():
            snapshot = _load(db, version_id)
    if snapshot.desired is None:
        # Legacy work has no durable native-resource identity to adopt or remove.
        return
    if (
        snapshot.status == AttemptStatus.RUNNING
        and snapshot.command_status == CommandStatus.RUNNING
        and snapshot.lease is not None
        and snapshot.lease > datetime.now(UTC)
    ):
        try:
            if not _ensure(factory, k8s, version_id, snapshot.desired):
                return
            actual = _read(k8s.batch.read_namespaced_job, snapshot.desired["resources"]["job"])
            status = serialized(actual).get("status", {}) if actual is not None else {}
            terminal = (
                status.get("failed", 0)
                or status.get("succeeded", 0)
                or any(
                    c.get("status") == "True" and c.get("type") in {"Complete", "Failed"}
                    for c in status.get("conditions", [])
                )
            )
            if not terminal:
                return
        except ValueError:
            _stop(factory, version_id, snapshot)
            return
        except ApiException as error:
            if error.status not in {400, 401, 403, 404, 422}:
                raise
            _stop(factory, version_id, snapshot)
            return
        _stop(factory, version_id, snapshot)
    elif snapshot.status == AttemptStatus.RUNNING:
        _stop(factory, version_id, snapshot)
    if not _cleanup(factory, k8s, version_id, snapshot.desired):
        return
    if row_status == "pending":
        policy, key = configured_policy(project_id, version_id)
        claim_provider_import(
            factory, version_id, policy=policy, public_key=key, prepare=_prepare(k8s.settings)
        )


def reconcile_provider_imports_once(factory, k8s, *, limit=25):
    if not k8s._configured:
        raise RuntimeError("Provider verification Kubernetes control unavailable")
    unfinished = exists(
        select(WorkflowAttempt.id).where(
            WorkflowAttempt.dataset_version_id == DatasetProviderImport.dataset_version_id,
            WorkflowAttempt.stage == WorkflowStage.PREPARATION,
            WorkflowAttempt.logical_key.like("provider-import:%:verify"),
            WorkflowAttempt.cleanup_state != "complete",
        )
    )
    with factory() as db:
        ids = list(
            db.scalars(
                select(DatasetProviderImport.dataset_version_id)
                .where(
                    or_(DatasetProviderImport.status == "pending", unfinished),
                )
                .order_by(
                    DatasetProviderImport.updated_at, DatasetProviderImport.dataset_version_id
                )
                .limit(limit)
            )
        )
    for version_id in ids:
        try:
            _reconcile(factory, k8s, version_id)
        except Exception as error:
            LOGGER.error(
                "Provider verification reconciliation unavailable",
                extra={"dataset_version_id": str(version_id), "error_type": type(error).__name__},
            )
        finally:
            with factory() as db, db.begin():
                row = db.scalar(
                    select(DatasetProviderImport).where(
                        DatasetProviderImport.dataset_version_id == version_id
                    )
                )
                if row is not None:
                    row.updated_at = datetime.now(UTC)
    return len(ids)
