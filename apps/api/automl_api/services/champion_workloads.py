"""Shared native isolation profile for the refit and evaluator workers."""

from kubernetes.client import ApiException

from automl_api.services.workflow_state import canonical_request_hash

DIGEST = "automl.platform/manifest-digest"


def project_attempt_identity(metadata, env, volumes, mounts, attempt, base_url):
    from automl_api.models.enums import WorkflowStage
    from automl_api.services.attempt_identity import attempt_audience
    from automl_api.training.attempt_admission import TOKEN_PATH, control_base_url

    if attempt.stage not in {WorkflowStage.CHAMPION_REFIT, WorkflowStage.CHAMPION_EVALUATION}:
        raise ValueError("Champion token projection requires a champion attempt")
    stage = "refit" if attempt.stage == WorkflowStage.CHAMPION_REFIT else "evaluation"
    expected = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
        "automl.platform/workflow-stage": "champion-" + stage,
    }
    if (
        metadata["name"] != stage + "-" + attempt.id.hex
        or attempt.workload_identity != f"project-{attempt.project_id}-{stage}"
        or any(k in metadata["labels"] and metadata["labels"][k] != v for k, v in expected.items())
    ):
        raise ValueError("Champion workload plan differs from its attempt")
    metadata["labels"] = {**metadata["labels"], **expected}
    env.extend(
        [
            {"name": "AUTOML_IDENTITY_TOKEN_FILE", "value": TOKEN_PATH},
            {"name": "ATTEMPT_CONTROL_BASE_URL", "value": control_base_url(base_url)},
        ]
    )
    volumes.append(
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
                        },
                    }
                ],
            },
        }
    )
    mounts.append({"name": "identity", "mountPath": "/var/run/sceptre/identity", "readOnly": True})


def check_champion_pod(k8s, pod, resources):
    from automl_api.services.attempt_identity import _owner
    from automl_api.services.provider_import_workload import (
        check_network,
        check_pod_spec,
        check_resource,
        serialized,
    )
    from automl_api.services.workflow_state import StaleFence

    job = resources["job"]
    name, namespace = job["metadata"]["name"], job["metadata"]["namespace"]
    owner = _owner(pod.metadata, "Job")
    actual_job = k8s.batch.read_namespaced_job(name=name, namespace=namespace, _request_timeout=5)
    uid = check_resource(actual_job, job)
    if owner.name != name or owner.uid != uid:
        raise StaleFence("Champion pod has another Job controller")
    value = serialized(pod)
    check_pod_spec(value["spec"], job["spec"]["template"]["spec"])
    account = job["spec"]["template"]["spec"]["serviceAccountName"]
    sa = serialized(
        k8s.core.read_namespaced_service_account(
            name=account,
            namespace=namespace,
            _request_timeout=5,
        )
    )
    metadata = sa["metadata"]
    if (
        metadata.get("name") != account
        or metadata.get("namespace") != namespace
        or not metadata.get("uid")
        or metadata.get("deletionTimestamp")
        or metadata.get("annotations")
        or sa.get("secrets")
        or sa.get("imagePullSecrets")
        or sa.get("automountServiceAccountToken") is not False
    ):
        raise StaleFence("Champion service account has changed or has ambient authority")
    return {
        "job": uid,
        "pod": pod.metadata.uid,
        "service_account": metadata["uid"],
        "network": check_network(k8s, resources, value["metadata"].get("labels", {})),
    }


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
                    "enableServiceLinks": False,
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
