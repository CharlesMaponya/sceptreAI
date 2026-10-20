"""Shared native isolation profile for the refit and evaluator workers."""

from kubernetes.client import ApiException

from automl_api.services.workflow_state import canonical_request_hash

DIGEST = "automl.platform/manifest-digest"


def delete_owned_resource(read, delete, metadata, *, foreground=False):
    """Confirm absence, or delete only the observed identity and wait for its removal."""
    name, namespace = metadata["name"], metadata["namespace"]
    labels = metadata.get("labels") or {}
    if not labels or any(not isinstance(value, str) or not value for value in labels.values()):
        return False
    try:
        resource = read(name=name, namespace=namespace, _request_timeout=5)
    except ApiException as error:
        if error.status == 404:
            return True
        raise
    actual = getattr(resource, "metadata", None)
    uid = getattr(actual, "uid", None)
    if (
        not isinstance(uid, str)
        or not uid
        or getattr(actual, "name", None) != name
        or getattr(actual, "namespace", None) != namespace
        or ("uid" in metadata and metadata["uid"] != uid)
        or any(
            (getattr(actual, "labels", None) or {}).get(key) != value
            for key, value in labels.items()
        )
    ):
        return False
    body = {"preconditions": {"uid": uid}}
    if foreground:
        body["propagationPolicy"] = "Foreground"
    try:
        delete(name=name, namespace=namespace, body=body, _request_timeout=5)
    except ApiException as error:
        if error.status == 409:
            return False
        if error.status != 404:
            raise
    if foreground:
        return False
    try:
        read(name=name, namespace=namespace, _request_timeout=5)
    except ApiException as error:
        if error.status == 404:
            return True
        raise
    return False


def isolated_workload(
    *,
    metadata,
    image,
    seconds,
    env,
    volumes,
    mounts,
    disk,
    memory_mib,
    cpu,
    settings,
    egress,
    worker,
    service_account,
):
    labels = metadata["labels"]
    resources = {"cpu": str(cpu), "memory": f"{memory_mib}Mi", "ephemeral-storage": str(disk)}
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": seconds,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "serviceAccountName": service_account,
                    "automountServiceAccountToken": False,
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 5,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "fsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": worker,
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["python", "-m", f"automl_api.training.{worker}_worker"],
                            "env": env,
                            "volumeMounts": mounts,
                            "resources": {"requests": resources, "limits": resources},
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                        }
                    ],
                    "volumes": volumes,
                    "imagePullSecrets": [
                        {"name": item} for item in settings.workload_image_pull_secrets
                    ],
                },
            },
        },
    }
    network = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": dict(metadata),
        "spec": {
            "podSelector": {"matchLabels": labels},
            "policyTypes": ["Ingress", "Egress"],
            "ingress": [],
            "egress": [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                            },
                            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                        }
                    ],
                    "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}],
                },
                *egress,
            ],
        },
    }
    for resource in (job, network):
        resource["metadata"] = {
            **resource["metadata"],
            "annotations": {
                DIGEST: canonical_request_hash(resource),
            },
        }
    return {"job": job, "network": network}
