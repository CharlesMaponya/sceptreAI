"""Shared native isolation profile for the refit and evaluator workers."""

from automl_api.services.workflow_state import canonical_request_hash

DIGEST = "automl.platform/manifest-digest"


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
