"""Admit serving credentials only for the original, pod-bound native workload."""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import jwt
from cryptography.hazmat.primitives import serialization
from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from automl_api.core.config import get_settings
from automl_api.models.enums import CommandStatus, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.services.inference_tls import (
    MAX_CERTIFICATE_SECONDS,
    MAX_PEM_BYTES,
    PROTOCOL,
    Authority,
    PeerIdentity,
    _p256,
    audience,
    csr_public_key,
    public_key_digest,
    serving_declaration,
)
from automl_api.services.workflow_state import (
    StaleFence,
    begin_command,
    canonical_request_hash,
    transition_command,
)

BINDING_OPERATION = "inference.identity.bind.v1"


def authority(settings=None):
    settings = settings or get_settings()
    paths = (
        settings.inference_identity_ca_certificate_file,
        settings.inference_identity_ca_private_key_file,
    )
    if any(
        path is None or not Path(path).is_file() or Path(path).stat().st_size > MAX_PEM_BYTES
        for path in paths
    ):
        raise StaleFence("Inference identity authority is not configured")
    data = [Path(path).read_bytes() for path in paths]
    return Authority.from_pem(*data)


def control_base_url(settings=None):
    settings = settings or get_settings()
    source = urlsplit(settings.attempt_control_base_url.rstrip("/"))
    if (
        source.scheme != "https"
        or not source.hostname
        or source.username is not None
        or source.password is not None
        or source.query
        or source.fragment
        or not source.path.endswith("/internal/attempts")
    ):
        raise StaleFence("Inference identity requires the authenticated HTTPS control URL")
    return urlunsplit(source._replace(path=source.path.removesuffix("/attempts") + "/deployments"))


@dataclass(frozen=True)
class IdentityScope:
    request: dict
    digest: str


def select_scope(db, run_id, ca_digest: str | None):
    from sqlalchemy import select

    from automl_api.services.model_custody import deployment_model_custody
    from automl_api.services.operations import _deployment_observation_request

    project_id = db.scalar(select(ModelRun.project_id).where(ModelRun.id == run_id))
    if project_id is None:
        raise StaleFence("Inference deployment no longer exists")
    db.get(Project, project_id, with_for_update={"read": True}, populate_existing=True)
    run = db.get(ModelRun, run_id, with_for_update=True, populate_existing=True)
    if (
        run is None
        or run.project_id != project_id
        or run.run_kind != RunKind.DEPLOYMENT
        or run.status not in {RunStatus.RUNNING, RunStatus.SUCCEEDED}
        or run.tags.get("inference_identity_protocol") != PROTOCOL
    ):
        raise StaleFence("Inference identity requires an active qualified deployment")
    origin = db.get(User, run.created_by_id, with_for_update={"read": True}, populate_existing=True)
    if origin is None or not origin.is_active:
        raise StaleFence("Inference identity lost its original admission actor")
    original = _deployment_observation_request(db, origin, run)
    model = deployment_model_custody(db, run)
    custody = original["custody"]
    pod_spec = custody["manifests"]["deployment"]["spec"]["template"]["spec"]
    try:
        values = serving_declaration(
            custody["manifests"],
            project_id=project_id,
            run_id=run_id,
            model_digest=model["completion"]["sha256"],
            model_uri=model["completion"]["uri"],
            ca_digest=ca_digest,
            base_url=control_base_url(),
        )
    except ValueError as exc:
        raise StaleFence(
            "Inference credential declaration differs from its original scope"
        ) from exc
    ca_digest = values["INFERENCE_CA_SHA256"]
    request = copy.deepcopy(
        dict(
            run_id=str(run.id),
            project_id=str(project_id),
            actor_id=str(run.created_by_id),
            custody=custody,
            control=original["control"],
            model_digest=values.get("MODEL_SHA256"),
            model_byte_size=model["completion"]["byte_size"],
            model_custody_digest=run.tags.get("model_custody_digest"),
            ca_digest=ca_digest,
            service_account=pod_spec["serviceAccountName"],
            audience=audience(run.id, ca_digest),
        )
    )
    return IdentityScope(request, canonical_request_hash(request))


def _object(value):
    if isinstance(value, dict):
        return value
    with client.ApiClient() as serializer:
        return serializer.sanitize_for_serialization(value)


def _owner(metadata, kind):
    owners = [
        owner for owner in metadata.get("ownerReferences") or [] if owner.get("controller") is True
    ]
    if len(owners) != 1 or owners[0].get("kind") != kind:
        raise StaleFence("Inference workload has no original controller chain")
    return owners[0]


def _pod_spec_matches(actual, wanted):
    actual = copy.deepcopy(actual)
    wanted = copy.deepcopy(wanted)

    def defaults(target, requested, values):
        for key, value in values.items():
            if key not in requested and target.get(key) == value:
                target.pop(key, None)

    for container, requested in zip(
        actual.get("containers", []), wanted.get("containers", []), strict=False
    ):
        defaults(
            container,
            requested,
            dict(
                terminationMessagePath="/dev/termination-log",
                terminationMessagePolicy="File",
                stdin=False,
                stdinOnce=False,
                tty=False,
            ),
        )
        for port, original in zip(
            container.get("ports", []), requested.get("ports", []), strict=False
        ):
            defaults(port, original, dict(protocol="TCP"))
        for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
            if probe in container and probe in requested:
                defaults(
                    container[probe], requested[probe], dict(timeoutSeconds=1, successThreshold=1)
                )
                if "httpGet" in container[probe] and "httpGet" in requested[probe]:
                    defaults(
                        container[probe]["httpGet"],
                        requested[probe]["httpGet"],
                        dict(scheme="HTTP"),
                    )
        for spec in (container, requested):
            for values in spec.get("resources", {}).values():
                if isinstance(values, dict):
                    for key, value in values.items():
                        values[key] = str(parse_quantity(value).normalize())
    for volume, original in zip(actual.get("volumes", []), wanted.get("volumes", []), strict=False):
        if "projected" in volume and "projected" in original:
            defaults(volume["projected"], original["projected"], dict(defaultMode=420))
    ordinary = dict(
        dnsPolicy="ClusterFirst",
        restartPolicy="Always",
        schedulerName="default-scheduler",
        enableServiceLinks=True,
        terminationGracePeriodSeconds=30,
        preemptionPolicy="PreemptLowerPriority",
        serviceAccount=wanted.get("serviceAccountName"),
    )
    for name, value in ordinary.items():
        if name not in wanted and actual.get(name) == value:
            actual.pop(name, None)
    for name in ("nodeName", "priority"):
        if name not in wanted:
            actual.pop(name, None)
    if "tolerations" not in wanted:
        expected = [
            dict(
                key=f"node.kubernetes.io/{key}",
                operator="Exists",
                effect="NoExecute",
                tolerationSeconds=300,
            )
            for key in ("not-ready", "unreachable")
        ]
        if actual.get("tolerations") == expected:
            actual.pop("tolerations")
    return canonical_request_hash(actual) == canonical_request_hash(wanted)


def verify_pod(k8s, scope, pod_name, pod_uid, *, deadline):
    def bounded(call, *args, **kwargs):
        if time.monotonic() + 5 > deadline:
            raise TimeoutError("Inference identity native verification budget exhausted")
        return call(*args, **kwargs)

    request = scope.request
    custody, control = request["custody"], request["control"]
    bounded(k8s._lifecycle_namespace, custody)
    deployment = bounded(k8s._lifecycle_resource, "deployment", "Deployment", "apps", custody)
    submitted = custody["manifests"]["deployment"]["spec"]["template"]
    if not _pod_spec_matches(deployment["spec"]["template"]["spec"], submitted["spec"]):
        raise StaleFence("Inference native receipt contains unrequested execution fields")
    annotations = deployment["metadata"].get("annotations") or {}
    marker = (
        annotations.get("automl.platform/control-generation"),
        annotations.get("automl.platform/control-command"),
    )
    expected = (
        (str(control["generation"]), control["command_id"])
        if control["generation"]
        else (None, None)
    )
    if marker != expected or deployment["spec"]["replicas"] != control["replicas"]:
        raise StaleFence("Inference native control generation changed")
    pod = _object(
        bounded(
            k8s.core.read_namespaced_pod,
            name=pod_name,
            namespace=custody["namespace"],
            _request_timeout=5,
        )
    )
    metadata = pod["metadata"]
    if (
        metadata.get("uid") != pod_uid
        or metadata.get("namespace") != custody["namespace"]
        or metadata.get("deletionTimestamp")
        or (pod.get("status") or {}).get("phase") not in {"Pending", "Running"}
    ):
        raise StaleFence("Inference pod lifetime changed")
    wanted = deployment["spec"]["template"]
    if (
        any(
            (metadata.get("labels") or {}).get(key) != value
            for key, value in wanted["metadata"]["labels"].items()
        )
        or pod["spec"].get("serviceAccountName") != request["service_account"]
        or not _pod_spec_matches(pod["spec"], wanted["spec"])
    ):
        raise StaleFence("Inference pod execution declaration changed")
    owner = _owner(metadata, "ReplicaSet")
    replica = _object(
        bounded(
            k8s.apps.read_namespaced_replica_set,
            name=owner["name"],
            namespace=custody["namespace"],
            _request_timeout=5,
        )
    )
    rmeta = replica["metadata"]
    parent = _owner(rmeta, "Deployment")
    if (
        rmeta.get("uid") != owner["uid"]
        or rmeta.get("namespace") != custody["namespace"]
        or rmeta.get("deletionTimestamp")
        or parent.get("uid") != deployment["metadata"]["uid"]
        or parent.get("name") != deployment["metadata"]["name"]
    ):
        raise StaleFence("Inference ReplicaSet is outside the original deployment")
    template = copy.deepcopy(replica["spec"]["template"])
    if "pod-template-hash" not in wanted["metadata"]["labels"]:
        label = (template["metadata"].get("labels") or {}).pop("pod-template-hash", None)
        if label is not None and (
            (replica["spec"]["selector"]["matchLabels"].get("pod-template-hash") != label)
            or (metadata.get("labels") or {}).get("pod-template-hash") != label
        ):
            raise StaleFence("Inference pod has another ReplicaSet template hash")
    if canonical_request_hash(template) != canonical_request_hash(wanted):
        raise StaleFence("Inference ReplicaSet execution template changed")
    current = bounded(k8s._lifecycle_resource, "deployment", "Deployment", "apps", custody)
    version = deployment["metadata"].get("resourceVersion")
    if not version or current["metadata"].get("resourceVersion") != version:
        raise StaleFence("Inference deployment changed during pod verification")
    bounded(k8s._lifecycle_namespace, custody)
    return PeerIdentity(
        request["project_id"],
        request["run_id"],
        custody["namespace_uid"],
        deployment["metadata"]["uid"],
        pod_uid,
    ), pod


def review_identity(k8s, scope, token):
    if not isinstance(token, str) or not 0 < len(token) <= 65536:
        raise StaleFence("A bounded pod identity token is required")
    deadline = time.monotonic() + 30
    request = scope.request
    review = k8s.authentication.create_token_review(
        client.V1TokenReview(
            spec=client.V1TokenReviewSpec(token=token, audiences=[request["audience"]])
        ),
        _request_timeout=5,
    )
    status = review.status
    if (
        status is None
        or status.authenticated is not True
        or request["audience"] not in (status.audiences or [])
        or status.user is None
        or status.user.username
        != f"system:serviceaccount:{request['custody']['namespace']}:{request['service_account']}"
    ):
        raise StaleFence("Inference workload identity or audience changed")
    extra = status.user.extra or {}
    names, uids = (
        extra.get("authentication.kubernetes.io/pod-name", []),
        extra.get("authentication.kubernetes.io/pod-uid", []),
    )
    if len(names) != 1 or len(uids) != 1:
        raise StaleFence("Inference requires a pod-bound identity")
    peer, pod = verify_pod(k8s, scope, names[0], uids[0], deadline=deadline)
    claims = jwt.decode(token, options={"verify_signature": False})
    expires = claims.get("exp")
    now = int(datetime.now(UTC).timestamp())
    if type(expires) is not int or not now + 30 < expires <= now + 900:
        raise StaleFence("Inference identity token has no bounded active lifetime")
    return peer, pod, datetime.fromtimestamp(expires, UTC)


def _binding_request(scope, peer, key):
    _p256(key)
    return dict(
        protocol=PROTOCOL,
        identity=asdict(peer),
        ca_digest=scope.request["ca_digest"],
        model_digest=scope.request["model_digest"],
        model_custody_digest=scope.request["model_custody_digest"],
        journal_digest=scope.request["custody"]["journal_digest"],
        public_key_digest=public_key_digest(key),
        public_key_pem=key.public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("ascii"),
    )


def _validate_binding(command, scope, peer, request):
    if (
        command is None
        or command.status != CommandStatus.SUCCEEDED
        or command.project_id != uuid.UUID(peer.project_id)
        or command.actor_id != uuid.UUID(scope.request["actor_id"])
        or command.operation != BINDING_OPERATION
        or command.idempotency_key != f"{peer.run_id}:{peer.pod_uid}"
        or command.resource_type != BINDING_OPERATION
        or command.resource_id != uuid.UUID(peer.run_id)
        or command.request_payload != request
        or command.request_hash != canonical_request_hash(request)
        or command.response_status != 200
        or command.response_payload
        != dict(identity=asdict(peer), public_key_digest=request["public_key_digest"])
    ):
        raise StaleFence("Inference pod has no current original key binding")


def bound_peer(db, scope, peer):
    from sqlalchemy import select

    from automl_api.models.workflows import WorkflowCommand

    command = db.scalar(
        select(WorkflowCommand)
        .where(
            WorkflowCommand.project_id == uuid.UUID(peer.project_id),
            WorkflowCommand.operation == BINDING_OPERATION,
            WorkflowCommand.idempotency_key == f"{peer.run_id}:{peer.pod_uid}",
        )
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if command is None:
        raise StaleFence("Inference pod was not admitted")
    key = serialization.load_pem_public_key(
        command.request_payload["public_key_pem"].encode("ascii")
    )
    request = _binding_request(scope, peer, key)
    _validate_binding(command, scope, peer, request)
    return request


def issue_identity(factory, run_id, token, csr, k8s):
    signer = authority()
    key = csr_public_key(csr)
    with factory() as db, db.begin():
        scope = select_scope(db, run_id, signer.digest)
    peer, _pod, expires = review_identity(k8s, scope, token)
    if authority().digest != signer.digest:
        raise StaleFence("Inference authority changed during admission")
    request = _binding_request(scope, peer, key)
    with factory() as db, db.begin():
        current = select_scope(db, run_id, signer.digest)
        if current.digest != scope.digest:
            raise StaleFence("Inference source or control changed during admission")
        command, replay = begin_command(
            db,
            project_id=uuid.UUID(peer.project_id),
            actor_id=uuid.UUID(scope.request["actor_id"]),
            operation=BINDING_OPERATION,
            idempotency_key=f"{run_id}:{peer.pod_uid}",
            payload=request,
        )
        if replay:
            _validate_binding(command, scope, peer, request)
        else:
            command.resource_type = BINDING_OPERATION
            command.resource_id = run_id
            transition_command(command, CommandStatus.RUNNING)
            transition_command(command, CommandStatus.SUCCEEDED)
            command.response_status = 200
            command.response_payload = dict(
                identity=asdict(peer), public_key_digest=request["public_key_digest"]
            )
        certificate = signer.issue_server(
            csr,
            peer,
            min(expires, datetime.now(UTC) + timedelta(seconds=MAX_CERTIFICATE_SECONDS - 1)),
        )
    return dict(
        protocol=PROTOCOL,
        identity=asdict(peer),
        ca_certificate=signer.pem.decode("ascii"),
        certificate=certificate.decode("ascii"),
        model_digest=request["model_digest"],
        model_byte_size=scope.request["model_byte_size"],
    )
