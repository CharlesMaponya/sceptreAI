"""Approved verifier Job and checks against actual Kubernetes workload authority."""

import ipaddress
import json
import os
import re
from copy import deepcopy

from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from automl_api.services.champion_workloads import DIGEST, isolated_workload
from automl_api.services.workflow_state import StaleFence, canonical_request_hash
from automl_api.training.attempt_admission import TOKEN_PATH, control_base_url

LABEL = "automl.platform/provider-verification"
SUBMISSION = "provider_import.submission"


def _node_selector():
    raw = os.environ.get("PROVIDER_IMPORT_NODE_SELECTOR", "{}")
    if len(raw.encode()) > 4096:
        raise ValueError("Provider verifier node selector exceeds its byte limit")
    selector = json.loads(raw)
    if not isinstance(selector, dict) or len(selector) > 32:
        raise ValueError("Provider verifier node selector requires bounded node labels")
    name_pattern = r"[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?"
    dns_pattern = r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*"
    for key, value in selector.items():
        prefix, slash, name = key.rpartition("/")
        if not slash:
            name = key
        if (
            not 0 < len(name) <= 63
            or not re.fullmatch(name_pattern, name)
            or (slash and (not 0 < len(prefix) <= 253 or not re.fullmatch(dns_pattern, prefix)))
            or not isinstance(value, str)
            or len(value) > 63
            or (value and not re.fullmatch(name_pattern, value))
        ):
            raise ValueError("Provider verifier node selector has an invalid label")
    return selector


def workload(execution, attempt, settings):
    from automl_api.services.attempt_identity import attempt_audience

    selector = _node_selector()
    base = control_base_url(settings.attempt_control_base_url)
    image = os.environ.get("PROVIDER_IMPORT_IMAGE", settings.training_image)
    if not re.fullmatch(r".+@sha256:[0-9a-f]{64}", image):
        raise ValueError("Provider verification requires a digest-pinned image")
    egress = json.loads(os.environ.get("PROVIDER_IMPORT_EGRESS_RULES", "[]"))
    if not isinstance(egress, list) or not egress:
        raise ValueError("Provider verification requires explicit TLS endpoint peers")
    for rule in egress:
        if set(rule) != {"to", "ports"} or not rule["to"] or not rule["ports"]:
            raise ValueError("Provider verification egress requires explicit peers and ports")
        for port in rule["ports"]:
            if (
                set(port) - {"port", "protocol"}
                or port.get("protocol", "TCP") != "TCP"
                or port.get("port") not in {443, 8443, 8334}
            ):
                raise ValueError("Provider verification permits only explicit TLS endpoint ports")
        for peer in rule["to"]:
            if set(peer) == {"ipBlock"}:
                block = peer["ipBlock"]
                network = ipaddress.ip_network(block["cidr"], strict=True)
                if set(block) != {"cidr"} or not network.is_global or network.num_addresses != 1:
                    raise ValueError("Verifier IP egress requires one public endpoint address")
            elif set(peer) == {"namespaceSelector", "podSelector"}:
                if any(
                    set(selector) != {"matchLabels"} or not selector["matchLabels"]
                    for selector in peer.values()
                ):
                    raise ValueError(
                        "Verifier service peers require explicit namespace and pod labels"
                    )
            else:
                raise ValueError("Provider verification egress peer is not explicit")
    memory_mib = int(os.environ.get("PROVIDER_IMPORT_MEMORY_MIB", "2048"))
    cpu = int(os.environ.get("PROVIDER_IMPORT_CPU_CORES", "1"))
    if (
        cpu <= 0
        or memory_mib <= 0
        or execution.intent.budget.max_decoded_group_bytes > memory_mib * 1024**2 // 4
    ):
        raise ValueError("Provider verification exceeds its process memory budget")
    disk = execution.intent.budget.max_temporary_bytes + 64 * 1024**2
    labels = {
        LABEL: str(attempt.id),
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
        "automl.platform/workflow-stage": attempt.stage.value,
    }
    metadata = {
        "name": attempt.workload_identity,
        "namespace": settings.training_namespace,
        "labels": labels,
    }
    env = [
        {"name": "AUTOML_ATTEMPT_ID", "value": str(attempt.id)},
        {"name": "AUTOML_PROJECT_ID", "value": str(attempt.project_id)},
        {"name": "AUTOML_FENCING_TOKEN", "value": attempt.fencing_token},
        {"name": "AUTOML_IDENTITY_TOKEN_FILE", "value": TOKEN_PATH},
        {"name": "ATTEMPT_CONTROL_BASE_URL", "value": base},
        {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
        {"name": "POLARS_MAX_THREADS", "value": "1"},
        {"name": "OMP_NUM_THREADS", "value": str(cpu)},
    ]
    volumes = [
        {"name": "tmp", "emptyDir": {"sizeLimit": str(disk)}},
        {
            "name": "identity",
            "projected": {
                "defaultMode": 0o444,
                "sources": [
                    {
                        "serviceAccountToken": {
                            "path": "token",
                            "expirationSeconds": 600,
                            "audience": attempt_audience(attempt.id, attempt.fencing_token),
                        }
                    }
                ],
            },
        },
    ]
    mounts = [
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "identity", "mountPath": "/var/run/sceptre/identity", "readOnly": True},
    ]
    ca = os.environ.get("ATTEMPT_CONTROL_CA_SECRET")
    if ca:
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", ca) or len(ca) > 253:
            raise ValueError("Provider verification CA Secret name is invalid")
        volumes.append(
            {
                "name": "ca",
                "secret": {
                    "secretName": ca,
                    "defaultMode": 0o444,
                    "items": [{"key": "ca.crt", "path": "ca.crt"}],
                },
            }
        )
        mounts.append({"name": "ca", "mountPath": "/var/run/sceptre/ca", "readOnly": True})
        env.append({"name": "ATTEMPT_CONTROL_CA_FILE", "value": "/var/run/sceptre/ca/ca.crt"})
    desired = isolated_workload(
        metadata=metadata,
        image=image,
        seconds=execution.intent.budget.timeout_seconds + 600,
        env=env,
        volumes=volumes,
        mounts=mounts,
        disk=disk,
        memory_mib=memory_mib,
        cpu=cpu,
        settings=settings,
        egress=egress,
        worker="provider_import",
        service_account=attempt.workload_identity,
    )
    desired["job"]["spec"]["template"]["spec"]["enableServiceLinks"] = False
    desired["job"]["spec"]["template"]["spec"]["containers"][0]["name"] = "provider-import"
    if selector:
        desired["job"]["spec"]["template"]["spec"]["nodeSelector"] = selector
    # The final digest covers the entire approved body, including service-link policy.
    job = desired["job"]
    job["metadata"].pop("annotations")
    job["metadata"]["annotations"] = {DIGEST: canonical_request_hash(job)}
    service_account = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": deepcopy(metadata),
        "automountServiceAccountToken": False,
    }
    service_account["metadata"]["annotations"] = {DIGEST: canonical_request_hash(service_account)}
    return {"service_account": service_account, "network": desired["network"], "job": job}


def serialized(resource):
    return (
        resource
        if isinstance(resource, dict)
        else client.ApiClient().sanitize_for_serialization(resource)
    )


def _normal(value):
    if isinstance(value, dict):
        return {
            k: str(parse_quantity(v).normalize())
            if k in {"cpu", "memory", "ephemeral-storage", "sizeLimit"}
            else _normal(v)
            for k, v in value.items()
            if v is not None
        }
    if isinstance(value, list):
        return [_normal(v) for v in value]
    return value


def check_pod_spec(actual, expected):
    actual, expected = _normal(actual), _normal(expected)
    actual.setdefault("imagePullSecrets", [])
    if (
        (actual.get("nodeSelector") or {}) != (expected.get("nodeSelector") or {})
        or actual.get("schedulerName", "default-scheduler")
        != expected.get("schedulerName", "default-scheduler")
        or any(
            actual.get(k)
            for k in (
                "hostNetwork",
                "hostPID",
                "hostIPC",
                "shareProcessNamespace",
                "initContainers",
                "ephemeralContainers",
            )
        )
        or any(
            actual.get(k) != expected.get(k)
            for k in (
                "serviceAccountName",
                "automountServiceAccountToken",
                "enableServiceLinks",
                "restartPolicy",
                "terminationGracePeriodSeconds",
                "securityContext",
                "volumes",
                "imagePullSecrets",
            )
        )
        or len(actual.get("containers", [])) != 1
    ):
        raise StaleFence("Provider verifier pod authority differs from its approved workload")
    container, wanted = actual["containers"][0], expected["containers"][0]
    if (
        container.get("envFrom")
        or container.get("args")
        or any(
            container.get(k) != wanted.get(k)
            for k in (
                "name",
                "image",
                "imagePullPolicy",
                "command",
                "env",
                "volumeMounts",
                "resources",
                "securityContext",
                "lifecycle",
                "livenessProbe",
                "readinessProbe",
                "startupProbe",
                "volumeDevices",
            )
        )
    ):
        raise StaleFence("Provider verifier container differs from its approved workload")


def check_resource(actual, expected):
    actual = _normal(serialized(actual))
    expected = _normal(expected)
    metadata = actual["metadata"]
    if (
        metadata.get("name") != expected["metadata"]["name"]
        or metadata.get("namespace") != expected["metadata"]["namespace"]
        or metadata.get("deletionTimestamp")
        or metadata.get("annotations", {}).get(DIGEST)
        != expected["metadata"]["annotations"][DIGEST]
        or any(
            metadata.get("labels", {}).get(k) != v
            for k, v in expected["metadata"]["labels"].items()
        )
        or not metadata.get("uid")
    ):
        raise StaleFence("Verifier Kubernetes resource ownership changed")
    kind = expected["kind"]
    if kind == "Job":
        if actual["spec"]["template"]["spec"].get("nodeName"):
            raise StaleFence("Provider verifier Job bypasses approved scheduling")
        if any(actual["spec"].get(k, 1) != 1 for k in ("parallelism", "completions")):
            raise StaleFence("Provider verifier Job cannot create nested retries or replicas")
        if any(
            actual["spec"].get(k) != expected["spec"][k]
            for k in ("backoffLimit", "activeDeadlineSeconds")
        ):
            raise StaleFence("Provider verifier Job retry or deadline changed")
        check_pod_spec(actual["spec"]["template"]["spec"], expected["spec"]["template"]["spec"])
    elif kind == "ServiceAccount":
        if (
            actual.get("automountServiceAccountToken") is not False
            or actual.get("secrets")
            or actual.get("imagePullSecrets")
            or metadata.get("annotations") != expected["metadata"]["annotations"]
        ):
            raise StaleFence("Provider verification service account has ambient authority")
    else:
        spec = actual["spec"]
        spec.setdefault("ingress", [])
        if spec != expected["spec"]:
            raise StaleFence("Provider verification network policy changed")
    return metadata["uid"]


def submission_for_scope(db, attempt, source):
    from automl_api.services.evaluation_publication import _event

    event = _event(db, attempt.id, SUBMISSION)
    if event is None:
        return None
    authority = {
        "project_id": str(attempt.project_id),
        "dataset_version_id": str(attempt.dataset_version_id),
        "attempt_id": str(attempt.id),
        "generation": attempt.generation,
        "fence": attempt.fencing_token,
        "identity": attempt.workload_identity,
        "command_id": str(source.command_id),
        "actor_id": str(source.actor_id),
        "owner": source.owner,
        "request_digest": source.request_digest,
    }
    if (
        event.event_type != SUBMISSION
        or event.result_digest != canonical_request_hash(event.payload)
        or event.payload.get("authority") != authority
    ):
        raise StaleFence("Provider verifier submission changed its protected execution")
    resources = deepcopy(event.payload["resources"])
    observed_uids = {}
    for kind in ("service_account", "network", "job"):
        observed = _event(db, attempt.id, f"provider_import.{kind}.observed")
        if observed is not None and (
            observed.result_digest != canonical_request_hash(observed.payload)
            or observed.payload.get("submission_digest") != event.result_digest
        ):
            raise StaleFence("Provider verifier resource observation changed")
        observed_uids[kind] = observed.payload["uid"] if observed is not None else None
    resources["observed_uids"] = observed_uids
    return resources


def _selects(selector, labels):
    if any(labels.get(k) != v for k, v in selector.get("matchLabels", {}).items()):
        return False
    for expression in selector.get("matchExpressions", []):
        key, operator = expression["key"], expression["operator"]
        values = expression.get("values", [])
        if operator == "In":
            if labels.get(key) not in values:
                return False
        elif operator == "NotIn":
            if labels.get(key) in values:
                return False
        elif operator == "Exists":
            if key not in labels:
                return False
        elif operator == "DoesNotExist":
            if key in labels:
                return False
        else:
            raise StaleFence("Provider verification policy selector is unsupported")
    return True


def check_network(k8s, resources, pod_labels):
    network = resources["network"]
    if not _selects(network["spec"]["podSelector"], pod_labels):
        raise StaleFence("The approved network policy does not select the verifier pod")
    name, namespace = network["metadata"]["name"], network["metadata"]["namespace"]
    actual = k8s.networking.read_namespaced_network_policy(
        name=name, namespace=namespace, _request_timeout=5
    )
    uid = check_resource(actual, network)
    page = None
    while True:
        result = k8s.networking.list_namespaced_network_policy(
            namespace=namespace, limit=100, _continue=page, _request_timeout=5
        )
        for item in result.items:
            value = serialized(item)
            if value["metadata"]["name"] == name:
                continue
            spec = value["spec"]
            if _selects(spec.get("podSelector", {}), pod_labels) and (
                spec.get("egress") or spec.get("ingress")
            ):
                raise StaleFence("Another network policy broadens verifier access")
        page = result.metadata._continue
        if not page:
            return uid


def check_verifier_pod(k8s, pod, resources):
    value = serialized(pod)
    job = resources["job"]
    name, namespace = job["metadata"]["name"], job["metadata"]["namespace"]
    owners = [
        o
        for o in value["metadata"].get("ownerReferences", [])
        if o.get("kind") == "Job" and o.get("controller")
    ]
    actual = k8s.batch.read_namespaced_job(name=name, namespace=namespace, _request_timeout=5)
    uid = check_resource(actual, job)
    if (
        uid != resources["observed_uids"]["job"]
        or len(owners) != 1
        or owners[0]["name"] != name
        or owners[0]["uid"] != uid
    ):
        raise StaleFence("Provider verifier pod has another Job controller")
    check_pod_spec(value["spec"], job["spec"]["template"]["spec"])
    sa = resources["service_account"]
    actual_sa = k8s.core.read_namespaced_service_account(
        name=sa["metadata"]["name"], namespace=namespace, _request_timeout=5
    )
    if check_resource(actual_sa, sa) != resources["observed_uids"]["service_account"]:
        raise StaleFence("Provider verification service account UID changed")
    if (
        check_network(k8s, resources, value["metadata"].get("labels", {}))
        != resources["observed_uids"]["network"]
    ):
        raise StaleFence("Provider verification network policy UID changed")
