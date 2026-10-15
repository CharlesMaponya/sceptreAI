"""Verify Kubernetes pod-bound identities before admitting an attempt."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import jwt
from kubernetes import client
from sqlalchemy import func, select

from automl_api.core.config import get_settings
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, RunStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services.workflow_state import StaleFence, canonical_request_hash


def attempt_audience(attempt_id, fence):
    return f"sceptre-attempt:{attempt_id}:{hashlib.sha256(fence.encode()).hexdigest()}"


def _owner(metadata, kind):
    owners = [o for o in metadata.owner_references or [] if o.kind == kind and o.controller]
    if len(owners) != 1:
        raise StaleFence("Attempt pod has no unique controller owner.")
    return owners[0]


def _head_owner(k8s, pod, attempt, namespace):
    owner = _owner(pod.metadata, "RayCluster")
    cluster = k8s.custom.get_namespaced_custom_object(
        "ray.io",
        "v1",
        namespace,
        "rayclusters",
        owner.name,
        _request_timeout=5,
    )
    refs = [
        r
        for r in cluster["metadata"].get("ownerReferences", [])
        if r.get("kind") == "RayJob" and r.get("controller")
    ]
    if (
        cluster["metadata"]["uid"] != owner.uid
        or cluster["metadata"].get("deletionTimestamp") is not None
        or len(refs) != 1
        or refs[0]["name"] != attempt.ray_job_name
        or pod.spec.service_account_name != cluster["metadata"]["name"]
        or pod.metadata.labels.get("ray.io/node-type") != "head"
    ):
        raise StaleFence("Ray head identity is outside the attempt's controller chain.")
    job = k8s.custom.get_namespaced_custom_object(
        "ray.io",
        "v1",
        namespace,
        "rayjobs",
        attempt.ray_job_name,
        _request_timeout=5,
    )
    if (
        job["metadata"]["uid"] != refs[0]["uid"]
        or job["metadata"].get("deletionTimestamp") is not None
    ):
        raise StaleFence("Ray head controller identity changed.")
    return job["metadata"].get("labels", {})


def admit_attempt(db, attempt_id, token, k8s):
    """Caller commits admission evidence; this never mints object credentials."""
    if not isinstance(token, str) or not token or len(token) > 65536:
        raise StaleFence("A bounded pod-bound identity token is required.")
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(
            WorkflowAttempt.id == attempt_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        attempt is None
        or attempt.stage
        not in {
            WorkflowStage.TRAINING_RUN,
            WorkflowStage.SPLITTER,
            WorkflowStage.PREPARATION,
        }
        or attempt.status not in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
        or (attempt.lease_expires_at is not None and attempt.lease_expires_at <= datetime.now(UTC))
    ):
        raise StaleFence("The attempt no longer admits workload identities.")
    namespace = get_settings().training_namespace
    if attempt.stage == WorkflowStage.TRAINING_RUN:
        run = db.get(ModelRun, attempt.model_run_id, populate_existing=True)
        if (
            run is None
            or run.project_id != attempt.project_id
            or not run.k8s_namespace
            or run.status not in {RunStatus.QUEUED, RunStatus.PRECHECK_RUNNING, RunStatus.RUNNING}
        ):
            raise StaleFence("The training workload namespace is missing.")
        namespace = run.k8s_namespace
    else:
        parts = attempt.logical_key.split(":")
        if len(parts) != 3 or parts[0] != "profile" or parts[2] != attempt.stage.value:
            raise StaleFence("Dataset admission has invalid profile lineage.")
        profile = db.get(
            ProfilingJob, uuid.UUID(parts[1]), with_for_update=True, populate_existing=True
        )
        if (
            profile is None
            or profile.project_id != attempt.project_id
            or profile.dataset_version_id != attempt.dataset_version_id
            or profile.status not in {"queued", "running"}
            or str(profile.overview_json.get("workflow_attempt_id", "")) != str(attempt.id)
        ):
            raise StaleFence("The dataset profile no longer admits this attempt.")
    audience = attempt_audience(attempt.id, attempt.fencing_token)
    review = client.AuthenticationV1Api().create_token_review(
        client.V1TokenReview(spec=client.V1TokenReviewSpec(token=token, audiences=[audience])),
        _request_timeout=5,
    )
    status = review.status
    if not status or not status.authenticated or audience not in (status.audiences or []):
        raise StaleFence("The Kubernetes identity is invalid or has the wrong audience.")
    if status.user is None:
        raise StaleFence("The reviewed workload identity is missing.")
    extra = status.user.extra or {}
    pod_names = extra.get("authentication.kubernetes.io/pod-name", [])
    pod_uids = extra.get("authentication.kubernetes.io/pod-uid", [])
    if len(pod_names) != 1 or len(pod_uids) != 1:
        raise StaleFence("A pod-bound identity is required.")
    pod = k8s.core.read_namespaced_pod(pod_names[0], namespace, _request_timeout=5)
    labels = pod.metadata.labels or {}
    expected = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
    }
    if (
        pod.metadata.uid != pod_uids[0]
        or pod.metadata.deletion_timestamp is not None
        or pod.metadata.namespace != namespace
        or status.user.username
        != f"system:serviceaccount:{namespace}:{pod.spec.service_account_name}"
        or any(labels.get(k) != v for k, v in expected.items())
    ):
        raise StaleFence("The pod identity does not match this attempt.")
    if pod.spec.service_account_name != attempt.workload_identity:
        job_labels = _head_owner(k8s, pod, attempt, namespace)
        if any(job_labels.get(k) != v for k, v in expected.items()):
            raise StaleFence("The Ray head belongs to another attempt.")
    # TokenReview authenticated these exact bytes; decode only to cap their lifetime.
    claims = jwt.decode(token, options={"verify_signature": False})
    now = int(datetime.now(UTC).timestamp())
    expires = claims.get("exp")
    if type(expires) is not int or not now < expires <= now + 900:
        raise StaleFence("The pod identity expiry is invalid or exceeds fifteen minutes.")
    payload = {
        "attempt_id": str(attempt.id),
        "project_id": str(attempt.project_id),
        "generation": attempt.generation,
        "pod_uid": pod_uids[0],
        "audience": audience,
        "expires_at": expires,
        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
    }
    key = f"attempt.identity:{payload['token_sha256']}"
    existing = db.scalar(
        select(WorkflowEvent)
        .where(
            WorkflowEvent.attempt_id == attempt.id,
            WorkflowEvent.event_key == key,
        )
        .execution_options(populate_existing=True)
    )
    digest = canonical_request_hash(payload)
    if existing is not None:
        if (
            existing.event_type != "attempt.identity"
            or existing.result_digest != digest
            or canonical_request_hash(existing.payload) != digest
        ):
            raise StaleFence("The admission evidence changed.")
    else:
        sequence = (
            db.scalar(
                select(func.max(WorkflowEvent.sequence)).where(
                    WorkflowEvent.attempt_id == attempt.id,
                )
            )
            or 0
        ) + 1
        db.add(
            WorkflowEvent(
                project_id=attempt.project_id,
                attempt_id=attempt.id,
                sequence=sequence,
                event_key=key,
                event_type="attempt.identity",
                payload=payload,
                result_digest=digest,
            )
        )
    return payload
