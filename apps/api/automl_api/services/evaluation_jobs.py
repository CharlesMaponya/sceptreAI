"""Durable submission of isolated evaluator Jobs; no final-data grant is requested."""

import base64
import hashlib
import json
import os
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from kubernetes.client import ApiException

from automl_api.models.enums import AttemptStatus
from automl_api.services.champion_workloads import DIGEST, isolated_workload
from automl_api.services.evaluation_access import (
    evaluation_claims,
    evaluation_token,
    locked_evaluation,
)
from automl_api.services.evaluation_authority import _record
from automl_api.services.evaluation_keys import KEY_FIELD
from automl_api.services.evaluation_publication import _event
from automl_api.services.workflow_state import (
    InvalidTransition,
    canonical_request_hash,
    transition_attempt,
)

LABEL = "automl.platform/evaluation-attempt"


def _url(value):
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise ValueError("Evaluator endpoints require HTTPS without credentials or query strings")
    return value.rstrip("/")


def submission(plan, scope, settings, key_secret):
    base = _url(os.environ.get("EVALUATION_CONTROL_BASE_URL", ""))
    authority = _url(os.environ.get("EVALUATION_AUTHORITY_URL", ""))
    image = os.environ.get("EVALUATION_IMAGE", settings.training_image)
    if not re.fullmatch(r".+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Evaluator image must be digest-pinned")
    public_secret = os.environ.get("EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET", "")
    if not public_secret:
        raise ValueError("Evaluator requires an authority public-key Secret")
    egress = json.loads(os.environ.get("EVALUATION_EGRESS_RULES", "[]"))
    if not isinstance(egress, list) or not egress:
        raise ValueError("Evaluator requires explicit TLS endpoint egress")
    for rule in egress:
        if (
            not rule.get("to")
            or not rule.get("ports")
            or any(
                port.get("protocol", "TCP") != "TCP" or port.get("port") not in {443, 8443, 8334}
                for port in rule["ports"]
            )
        ):
            raise ValueError("Evaluator endpoint rules require explicit peers and TLS ports")
    memory_mib = int(os.environ.get("EVALUATION_MEMORY_MIB", "2048"))
    cpu = int(os.environ.get("EVALUATION_CPU_CORES", "1"))
    if min(memory_mib, cpu) <= 0 or plan.max_decoded_bytes > memory_mib * 1024 * 1024:
        raise ValueError("Evaluator inputs exceed workload memory")
    seconds = int((scope.scope_deadline_at - datetime.now(UTC)).total_seconds())
    if seconds <= 0:
        raise ValueError("Evaluator scope deadline expired")
    name = f"evaluation-{plan.attempt_id.hex}"
    metadata = dict(
        name=name,
        namespace=settings.training_namespace,
        labels={
            LABEL: str(plan.attempt_id),
            "automl.platform/workflow-stage": "champion-evaluation",
            "automl.platform/evaluation-scope": str(plan.scope_id),
            "automl.platform/project-id": str(plan.project_id),
        },
    )
    disk = max(256 * 1024 * 1024, plan.max_model_bytes + plan.max_input_bytes)
    env = [
        {"name": "EVALUATION_CONTROL_URL", "value": f"{base}/{plan.attempt_id}/"},
        {"name": "EVALUATION_AUTHORITY_URL", "value": authority + "/"},
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
        {"name": "OMP_NUM_THREADS", "value": str(cpu)},
    ]
    for variable, key in (
        ("EVALUATION_CONTROL_TOKEN", "control"),
        ("EVALUATION_AUTHORITY_TOKEN", "authority"),
    ):
        env.append({"name": variable, "valueFrom": {"secretKeyRef": {"name": name, "key": key}}})
    volumes = [{"name": "tmp", "emptyDir": {"sizeLimit": str(disk)}}]
    mounts = [{"name": "tmp", "mountPath": "/tmp"}]
    secrets = [
        ("result-key", key_secret, KEY_FIELD, "EVALUATION_RESULT_KEY_FILE"),
        ("authority-key", public_secret, "public-key.pem", "EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"),
    ]
    ca = os.environ.get("EVALUATION_CA_SECRET")
    if ca:
        secrets.append(("ca", ca, "ca.crt", "EVALUATION_CA_FILE"))
    for volume, secret, key, variable in secrets:
        path = f"/evaluation-{volume}"
        volumes.append(
            {
                "name": volume,
                "secret": {
                    "secretName": secret,
                    "defaultMode": 0o440,
                    "items": [{"key": key, "path": key}],
                },
            }
        )
        mounts.append({"name": volume, "mountPath": path, "readOnly": True})
        env.append({"name": variable, "value": path + "/" + key})
    return isolated_workload(
        metadata=metadata,
        image=image,
        seconds=seconds,
        env=env,
        volumes=volumes,
        mounts=mounts,
        disk=disk,
        memory_mib=memory_mib,
        cpu=cpu,
        settings=settings,
        egress=egress,
        worker="evaluation",
        service_account="sceptre-champion-evaluation",
    )


def submit_evaluator(session_factory, k8s, plan, authority_token, key_secret, *, fence):
    """Persist the manifest and startup lease before delivering credentials or a Job.

    The caller obtains the plan/token/key from prepare_evaluator_credentials.
    Replay only an unstarted, live attempt. Observation, recovery and replacement
    are controller responsibilities; this function never opens final data.
    """
    with session_factory() as db, db.begin():
        scope, attempt, saved = locked_evaluation(
            db,
            plan.attempt_id,
            {"project_id": plan.project_id, "fence": fence},
            (AttemptStatus.PENDING, AttemptStatus.SUBMITTED),
        )
        if saved != plan:
            raise InvalidTransition("Evaluator submission changed its registered plan")
        registered = _event(db, attempt.id, "authority_registered")
        if (
            registered.payload["token_sha256"]
            != hashlib.sha256(authority_token.encode()).hexdigest()
        ):
            raise InvalidTransition("Evaluator authority token differs from registration")
        # Key custody was committed on the successful refit before authority registration.
        expected_key = f"evaluation-key-{scope.id.hex}-{attempt.generation}"
        if key_secret != expected_key:
            raise InvalidTransition("Evaluator result-key Secret differs from its generation")
        now = datetime.now(UTC)
        if attempt.status == AttemptStatus.PENDING:
            desired = submission(plan, scope, k8s.settings, key_secret)
            _record(db, attempt, "evaluation_submission", desired)
            transition_attempt(attempt, AttemptStatus.CLAIMED, fencing_token=fence)
            transition_attempt(attempt, AttemptStatus.SUBMITTED, fencing_token=fence)
            attempt.lease_expires_at = min(scope.scope_deadline_at, now + timedelta(minutes=5))
        else:
            event = _event(db, attempt.id, "evaluation_submission")
            if event is None or event.result_digest != canonical_request_hash(event.payload):
                raise InvalidTransition("Evaluator has no valid durable submission")
            if attempt.lease_expires_at is None or attempt.lease_expires_at <= now:
                raise InvalidTransition("Evaluator startup lease expired")
            desired = event.payload
        control_token = evaluation_token(attempt, scope.scope_deadline_at)
    _ensure_resources(k8s, desired, plan, fence, control_token, authority_token)
    return desired


def _ensure_resources(k8s, desired, plan, fence, control_token, authority_token):
    metadata = desired["job"]["metadata"]
    name, namespace = metadata["name"], metadata["namespace"]
    k8s.ensure_service_account("sceptre-champion-evaluation")
    secret = dict(
        apiVersion="v1",
        kind="Secret",
        immutable=True,
        metadata={k: metadata[k] for k in ("name", "namespace", "labels")},
        stringData={"control": control_token, "authority": authority_token},
    )
    try:
        k8s.core.create_namespaced_secret(namespace, secret)
    except ApiException as error:
        if error.status != 409:
            raise
        existing = k8s.core.read_namespaced_secret(name, namespace)
        claims = evaluation_claims(
            base64.b64decode(existing.data["control"]).decode(), plan.attempt_id
        )
        if (
            existing.immutable is not True
            or (existing.metadata.labels or {}).get(LABEL) != str(plan.attempt_id)
            or claims["project_id"] != plan.project_id
            or claims["fence"] != fence
            or base64.b64decode(existing.data["authority"]).decode() != authority_token
        ):
            raise InvalidTransition(
                "Evaluator token Secret has different ownership or credentials"
            ) from None
    for resource, create, read in (
        (
            desired["network"],
            k8s.networking.create_namespaced_network_policy,
            k8s.networking.read_namespaced_network_policy,
        ),
        (desired["job"], k8s.batch.create_namespaced_job, k8s.batch.read_namespaced_job),
    ):
        try:
            create(namespace=namespace, body=resource)
        except ApiException as error:
            if error.status != 409:
                raise
            existing = read(name=name, namespace=namespace)
            if (existing.metadata.labels or {}).get(LABEL) != str(plan.attempt_id) or (
                existing.metadata.annotations or {}
            ).get(DIGEST) != resource["metadata"]["annotations"][DIGEST]:
                raise InvalidTransition(
                    "Evaluator resource differs from durable submission"
                ) from None
