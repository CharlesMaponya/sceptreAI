from __future__ import annotations

import ast
import math
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from kubernetes import client, config
from kubernetes.client import ApiException
from kubernetes.utils.quantity import parse_quantity

from automl_api.core.config import Settings, get_settings
from automl_api.models.enums import TaskType
from automl_api.schemas.training import ClusterCapacityRead, TrainingEstimateRead

_FATAL_CONTAINER_WAITING_REASONS = {
    "CreateContainerConfigError",
    "ErrImageNeverPull",
    "InvalidImageName",
}
_IMAGE_PULL_BACKOFF_REASONS = {"ImagePullBackOff"}
_REPORTED_CONTAINER_WAITING_FAILURE_REASONS = {
    *_FATAL_CONTAINER_WAITING_REASONS,
    *_IMAGE_PULL_BACKOFF_REASONS,
    "CreateContainerError",
    "RunContainerError",
}


@dataclass(frozen=True)
class NodeCapability:
    name: str
    gpu_vendor: str | None = None
    gpu_resource: str | None = None
    gpu_count: int = 0


@dataclass(frozen=True)
class CapacitySnapshot:
    capacity: ClusterCapacityRead
    nodes: list[NodeCapability]
    pvc_ready: bool
    priority_class_ready: bool
    runtime_dependencies_ready: bool


class KubernetesTrainingClient:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._configured = False
        self._configuration_error: str | None = None
        try:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            self.core = client.CoreV1Api()
            self.batch = client.BatchV1Api()
            self.apps = client.AppsV1Api()
            self.networking = client.NetworkingV1Api()
            self.scheduling = client.SchedulingV1Api()
            self.custom = client.CustomObjectsApi()
            self._configured = True
        except Exception as exc:
            self._configuration_error = str(exc)

    def capacity_snapshot(self) -> CapacitySnapshot:
        if not self._configured:
            warning = self._configuration_error or "Kubernetes client is not configured."
            return CapacitySnapshot(
                capacity=ClusterCapacityRead(
                    connected=False,
                    source="unavailable",
                    total_cpu_cores=0,
                    requested_cpu_cores=0,
                    available_cpu_cores=0,
                    total_memory_mb=0,
                    requested_memory_mb=0,
                    available_memory_mb=0,
                    ready_nodes=0,
                    gpu_available=False,
                    active_training_jobs=0,
                    warnings=[warning],
                ),
                nodes=[],
                pvc_ready=False,
                priority_class_ready=False,
                runtime_dependencies_ready=False,
            )

        namespace = self.settings.training_namespace
        warnings: list[str] = []
        pods = self.core.list_namespaced_pod(
            namespace=namespace,
            label_selector="automl.platform/workload=training",
        ).items
        active_training_jobs = sum(
            pod.status.phase not in {"Succeeded", "Failed"} for pod in pods
        )

        total_cpu = requested_cpu = available_cpu = 0.0
        total_memory = requested_memory = available_memory = 0
        source = "kubernetes_scheduler"
        try:
            quotas = self.core.list_namespaced_resource_quota(namespace=namespace).items
            quota_capacity = _namespace_quota_capacity(quotas)
            if quota_capacity is not None:
                (
                    total_cpu,
                    requested_cpu,
                    available_cpu,
                    total_memory,
                    requested_memory,
                    available_memory,
                ) = quota_capacity
                source = "namespace_resource_quota"
            else:
                warnings.append(
                    "No namespace ResourceQuota exposes CPU and memory capacity; "
                    "Kubernetes remains the scheduling authority."
                )
        except ApiException as exc:
            warnings.append(
                "Namespace quota discovery is unavailable; Kubernetes remains the "
                f"scheduling authority ({exc.reason or exc.status})."
            )

        ready_nodes: list[NodeCapability] = []
        ready_node_count = 0
        if self.settings.cluster_observer_enabled:
            try:
                for node in self.core.list_node().items:
                    if node.spec.unschedulable or not _node_is_ready(node):
                        continue
                    ready_node_count += 1
                    vendor, resource, count = _node_gpu(
                        node.status.allocatable or {},
                        resources=self._gpu_resources(),
                    )
                    if resource:
                        ready_nodes.append(
                            NodeCapability(
                                name=str(node.metadata.name),
                                gpu_vendor=vendor,
                                gpu_resource=resource,
                                gpu_count=count,
                            )
                        )
            except ApiException as exc:
                warnings.append(
                    "Optional cluster observer cannot list nodes; GPU discovery is "
                    f"disabled ({exc.reason or exc.status})."
                )
        elif self.settings.gpu_enabled:
            warnings.append(
                "GPU discovery is disabled because the optional cluster observer is not enabled."
            )

        pvc_ready = (
            not self.settings.dataset_cache_pvc_name
            or self._pvc_is_bound(self.settings.dataset_cache_pvc_name)
        )
        priority_ready = (
            not self.settings.training_priority_class_name
            or not self.settings.cluster_observer_enabled
            or self._priority_class_is_ready(self.settings.training_priority_class_name)
        )
        runtime_dependencies_ready = self._secret_exists(
            self.settings.database_secret_name
        ) and self._secret_exists(self.settings.object_store_secret_name)
        return CapacitySnapshot(
            capacity=ClusterCapacityRead(
                connected=True,
                source=source,
                total_cpu_cores=round(total_cpu, 3),
                requested_cpu_cores=round(requested_cpu, 3),
                used_cpu_cores=0,
                available_cpu_cores=round(available_cpu, 3),
                total_memory_mb=total_memory,
                requested_memory_mb=requested_memory,
                used_memory_mb=0,
                available_memory_mb=available_memory,
                ready_nodes=ready_node_count,
                gpu_available=bool(ready_nodes),
                active_training_jobs=active_training_jobs,
                warnings=warnings,
            ),
            nodes=ready_nodes,
            pvc_ready=pvc_ready,
            priority_class_ready=priority_ready,
            runtime_dependencies_ready=runtime_dependencies_ready,
        )

    def _gpu_resources(self) -> tuple[tuple[str, str], ...]:
        return (
            ("nvidia", self.settings.nvidia_gpu_resource),
            ("intel", self.settings.intel_gpu_resource),
        )

    def estimate(
        self,
        *,
        dataset_bytes: int,
        column_count: int,
        expected_minutes: int,
        prefer_gpu: bool,
        dataset_rows: int = 0,
        task_type: TaskType = TaskType.UNSPECIFIED,
        candidate_limit: int = 5,
        optimization_iterations: int = 5,
        model_cost_factor: float = 1.0,
        gpu_compatible_vendors: set[str] | None = None,
    ) -> TrainingEstimateRead:
        snapshot = self.capacity_snapshot()
        capacity = snapshot.capacity
        dataset_mb = max(1.0, dataset_bytes / 1024 / 1024)
        dense_matrix_mb = max(0, dataset_rows) * max(1, column_count) * 8 / 1024 / 1024
        task_multiplier = {
            TaskType.CLASSIFICATION: 1.35,
            TaskType.REGRESSION: 1.25,
            TaskType.TIME_SERIES: 1.4,
            TaskType.CLUSTERING: 1.6,
        }.get(task_type, 1.3)
        search_multiplier = (
            1
            + min(max(candidate_limit, 1), 20) * 0.03
            + min(max(optimization_iterations, 1), 25) * 0.01
        ) * max(1.0, min(model_cost_factor, 2.0))
        data_working_set = max(dataset_mb * 6, dense_matrix_mb * 3)
        estimated_working_set = math.ceil(
            700 + data_working_set * task_multiplier * search_multiplier
        )
        desired_memory = max(768, estimated_working_set)

        compatible_vendors = (
            gpu_compatible_vendors
            if gpu_compatible_vendors is not None
            else {"nvidia", "intel"}
        )
        gpu_nodes = [
            node
            for node in snapshot.nodes
            if node.gpu_resource and node.gpu_vendor in compatible_vendors
        ]
        gpu_requested = bool(prefer_gpu and self.settings.gpu_enabled and gpu_nodes)
        gpu_capability = gpu_nodes[0] if gpu_requested else None
        blockers = []
        warnings = list(capacity.warnings)
        gpu_fallback_reason = None
        if prefer_gpu and not gpu_requested:
            if not self.settings.gpu_enabled:
                gpu_fallback_reason = "GPU_ENABLED is false; using CPU training."
            elif gpu_compatible_vendors is not None and not gpu_compatible_vendors:
                gpu_fallback_reason = (
                    "The selected estimators do not expose a supported GPU backend; "
                    "using all allocated CPU threads."
                )
            elif not capacity.gpu_available:
                gpu_fallback_reason = (
                    "No schedulable node exposes nvidia.com/gpu, gpu.intel.com/xe, "
                    "or gpu.intel.com/i915; using CPU training."
                )
            else:
                gpu_fallback_reason = (
                    "Available GPU vendors are incompatible with the selected estimators; "
                    "using CPU training."
                )
            warnings.append(gpu_fallback_reason)

        if not capacity.connected:
            blockers.append("Kubernetes API is unavailable.")
        if not snapshot.pvc_ready:
            warnings.append(
                f"Optional dataset cache PVC {self.settings.dataset_cache_pvc_name} is not "
                "Bound; switch to ephemeral cache or repair the claim."
            )
        if not snapshot.priority_class_ready:
            warnings.append(
                f"Optional PriorityClass {self.settings.training_priority_class_name} is "
                "unavailable; omit it or grant observer access."
            )
        if not snapshot.runtime_dependencies_ready:
            blockers.append(
                "Training runtime database or object-store Secret is missing."
            )

        cpu_request = max(0.0, self.settings.training_cpu_request_cores)
        cpu_limit = max(cpu_request, self.settings.training_cpu_limit_cores)
        memory_limit = max(
            self.settings.training_memory_request_mb,
            self.settings.training_memory_limit_mb,
        )
        memory_request = min(
            memory_limit,
            max(self.settings.training_memory_request_mb, desired_memory),
        )
        if cpu_request < 0.25:
            blockers.append("TRAINING_CPU_REQUEST_CORES must be at least 0.25.")
        if memory_request < 256:
            blockers.append("TRAINING_MEMORY_REQUEST_MB must be at least 256 MiB.")
        if desired_memory > memory_limit:
            blockers.append(
                f"Estimated training working set is {desired_memory} MiB, above the "
                f"configured {memory_limit} MiB per-job limit. Increase the Helm training "
                "memory limit or reduce the dataset/model budget."
            )

        cpu_request = round(cpu_request, 3)
        cpu_limit = round(cpu_limit, 3)
        if capacity.source == "namespace_resource_quota":
            if capacity.available_cpu_cores < cpu_request:
                blockers.append(
                    "The namespace ResourceQuota has insufficient CPU for this request."
                )
            if capacity.available_memory_mb < memory_request:
                blockers.append(
                    "The namespace ResourceQuota has insufficient memory for this request."
                )
        max_parallel = self.settings.max_concurrent_jobs
        if capacity.source == "namespace_resource_quota":
            max_parallel = min(
                max_parallel,
                math.floor(capacity.total_cpu_cores / cpu_request) if cpu_request else 0,
                math.floor(capacity.total_memory_mb / memory_request) if memory_request else 0,
            )
        if capacity.active_training_jobs >= max_parallel:
            blockers.append(
                "Concurrent training limit reached "
                f"({capacity.active_training_jobs}/{max_parallel})."
            )

        return TrainingEstimateRead(
            capacity=capacity,
            estimated_working_set_mb=estimated_working_set,
            cpu_request_cores=cpu_request,
            cpu_limit_cores=cpu_limit,
            memory_request_mb=memory_request,
            memory_limit_mb=memory_limit,
            gpu_requested=gpu_requested,
            gpu_fallback_reason=gpu_fallback_reason,
            gpu_vendor=gpu_capability.gpu_vendor if gpu_capability else None,
            gpu_resource=gpu_capability.gpu_resource if gpu_capability else None,
            selected_node=None,
            expected_minutes=expected_minutes,
            active_deadline_seconds=_active_deadline_seconds(
                expected_minutes,
                self.settings,
            ),
            estimated_core_hours=round(cpu_request * expected_minutes / 60, 4),
            max_concurrent_jobs=max_parallel,
            can_launch=not blockers,
            blockers=blockers,
            warnings=warnings,
        )

    def build_job_manifest(
        self,
        *,
        run_id: uuid.UUID,
        project_id: uuid.UUID,
        estimate: TrainingEstimateRead,
    ) -> dict[str, Any]:
        name = f"automl-train-{str(run_id)[:8]}"
        settings = self.settings
        cpu_threads = max(1, math.floor(estimate.cpu_request_cores))
        shared_memory_mb = max(512, min(8192, estimate.memory_request_mb // 4))
        image = settings.training_image
        if estimate.gpu_vendor == "nvidia":
            image = settings.training_image_nvidia
        elif estimate.gpu_vendor == "intel":
            image = settings.training_image_intel
        container: dict[str, Any] = {
            "name": "trainer",
            "image": image,
            "imagePullPolicy": settings.training_image_pull_policy,
            "command": [
                "python",
                "-m",
                "automl_api.training.worker",
                "--run-id",
                str(run_id),
            ],
            "env": [
                {"name": "AUTOML_RUN_ID", "value": str(run_id)},
                {"name": "AUTOML_PROJECT_ID", "value": str(project_id)},
                {"name": "TRAINING_EXECUTION_MODE", "value": "direct"},
                {"name": "AUTOML_CPU_THREADS", "value": str(cpu_threads)},
                {"name": "OMP_NUM_THREADS", "value": str(cpu_threads)},
                {"name": "MKL_NUM_THREADS", "value": str(cpu_threads)},
                {"name": "OPENBLAS_NUM_THREADS", "value": str(cpu_threads)},
                {"name": "NUMEXPR_NUM_THREADS", "value": str(cpu_threads)},
                {
                    "name": "DATABASE_URL",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": settings.database_secret_name,
                            "key": settings.database_secret_key,
                        }
                    },
                },
                {"name": "OBJECT_STORE_TYPE", "value": "minio"},
                {
                    "name": "OBJECT_STORE_ENDPOINT",
                    "value": settings.object_store_endpoint or "",
                },
                {"name": "OBJECT_STORE_BUCKET", "value": settings.object_store_bucket},
                {
                    "name": "OBJECT_STORE_ACCESS_KEY",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": settings.object_store_secret_name,
                            "key": settings.object_store_access_key_secret_key,
                        }
                    },
                },
                {
                    "name": "OBJECT_STORE_SECRET_KEY",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": settings.object_store_secret_name,
                            "key": settings.object_store_secret_key_secret_key,
                        }
                    },
                },
                {
                    "name": "MLFLOW_TRACKING_URI",
                    "value": settings.mlflow_tracking_uri,
                },
                {"name": "MLFLOW_ENABLE_ASYNC_LOGGING", "value": "false"},
            ],
            "resources": {
                "requests": {
                    "cpu": str(estimate.cpu_request_cores),
                    "memory": f"{estimate.memory_request_mb}Mi",
                },
                "limits": {
                    "cpu": str(estimate.cpu_limit_cores),
                    "memory": f"{estimate.memory_limit_mb}Mi",
                },
            },
            "volumeMounts": [
                {"name": "dataset-cache", "mountPath": "/cache"},
                {"name": "shared-memory", "mountPath": "/dev/shm"},
            ],
        }
        pod_spec: dict[str, Any] = {
            "restartPolicy": "Never",
            "serviceAccountName": settings.training_service_account,
            "automountServiceAccountToken": False,
            "containers": [container],
            "volumes": [
                {
                    "name": "dataset-cache",
                    "emptyDir": {"sizeLimit": f"{settings.dataset_cache_size_gb}Gi"},
                },
                {
                    "name": "shared-memory",
                    "emptyDir": {
                        "medium": "Memory",
                        "sizeLimit": f"{shared_memory_mb}Mi",
                    },
                },
            ],
        }
        if settings.dataset_cache_pvc_name:
            pod_spec["volumes"][0] = {
                "name": "dataset-cache",
                "persistentVolumeClaim": {"claimName": settings.dataset_cache_pvc_name},
            }
        if settings.training_priority_class_name:
            pod_spec["priorityClassName"] = settings.training_priority_class_name
        if settings.workload_image_pull_secrets:
            pod_spec["imagePullSecrets"] = [
                {"name": name} for name in settings.workload_image_pull_secrets
            ]
        if estimate.gpu_requested and estimate.gpu_resource and estimate.gpu_vendor:
            container["resources"]["requests"][estimate.gpu_resource] = "1"
            container["resources"]["limits"][estimate.gpu_resource] = "1"
            container["env"].extend(
                [
                    {"name": "AUTOML_GPU_VENDOR", "value": estimate.gpu_vendor},
                    {"name": "AUTOML_GPU_RESOURCE", "value": estimate.gpu_resource},
                ]
            )
            if estimate.gpu_vendor == "nvidia":
                container["env"].append({"name": "CUML_ACCEL_ENABLED", "value": "1"})

        job_spec: dict[str, Any] = {
            "backoffLimit": 0,
            "activeDeadlineSeconds": estimate.active_deadline_seconds,
            "template": {
                "metadata": {
                    "labels": {
                        "automl.platform/workload": "training",
                        "automl.platform/run-id": str(run_id),
                    }
                },
                "spec": pod_spec,
            },
        }
        if settings.training_job_ttl_seconds > 0:
            job_spec["ttlSecondsAfterFinished"] = settings.training_job_ttl_seconds
        manifest: dict[str, Any] = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": settings.training_namespace,
                "labels": {
                    "automl.platform/workload": "training",
                    "automl.platform/run-id": str(run_id),
                    "automl.platform/project-id": str(project_id),
                },
            },
            "spec": job_spec,
        }
        return manifest

    def create_job(self, manifest: dict[str, Any]) -> None:
        self.batch.create_namespaced_job(
            namespace=self.settings.training_namespace,
            body=manifest,
        )

    def build_model_deployment_manifest(
        self,
        *,
        deployment_id: uuid.UUID,
        project_id: uuid.UUID,
        project_name: str,
        environment: str,
        model_name: str,
        model_uri: str,
        image: str,
        replicas: int,
        cpu_request: str,
        memory_request: str,
    ) -> dict[str, Any]:
        suffix = str(deployment_id)[:8]
        name = f"automl-model-{suffix}"
        labels = {
            "app.kubernetes.io/name": "automl-model-serving",
            "automl.platform/workload": "inference",
            "automl.platform/deployment-id": str(deployment_id),
            "automl.platform/project-id": str(project_id),
        }
        manifests: dict[str, Any] = {
            "deployment": {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {
                    "name": name,
                    "namespace": self.settings.training_namespace,
                    "labels": labels,
                },
                "spec": {
                    "replicas": replicas,
                    "selector": {
                        "matchLabels": {
                            "automl.platform/deployment-id": str(deployment_id),
                        }
                    },
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "serviceAccountName": self.settings.inference_service_account,
                            "automountServiceAccountToken": False,
                            **(
                                {
                                    "imagePullSecrets": [
                                        {"name": secret}
                                        for secret in self.settings.workload_image_pull_secrets
                                    ]
                                }
                                if self.settings.workload_image_pull_secrets
                                else {}
                            ),
                            "containers": [
                                {
                                    "name": "model",
                                    "image": image,
                                    "imagePullPolicy": self.settings.inference_image_pull_policy,
                                    "ports": [{"name": "http", "containerPort": 8080}],
                                    "env": [
                                        {"name": "MODEL_URI", "value": model_uri},
                                        {"name": "MODEL_NAME", "value": model_name},
                                        {
                                            "name": "PROJECT_NAME",
                                            "value": project_name,
                                        },
                                        {
                                            "name": "DEPLOYMENT_ENVIRONMENT",
                                            "value": environment,
                                        },
                                        {"name": "OBJECT_STORE_TYPE", "value": "minio"},
                                        {
                                            "name": "OBJECT_STORE_ENDPOINT",
                                            "value": self.settings.object_store_endpoint or "",
                                        },
                                        {
                                            "name": "OBJECT_STORE_BUCKET",
                                            "value": self.settings.object_store_bucket,
                                        },
                                        {
                                            "name": "OBJECT_STORE_ACCESS_KEY",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": self.settings.object_store_secret_name,
                                                    "key": (
                                                        self.settings
                                                        .object_store_access_key_secret_key
                                                    ),
                                                }
                                            },
                                        },
                                        {
                                            "name": "OBJECT_STORE_SECRET_KEY",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": self.settings.object_store_secret_name,
                                                    "key": (
                                                        self.settings
                                                        .object_store_secret_key_secret_key
                                                    ),
                                                }
                                            },
                                        },
                                    ],
                                    "resources": {
                                        "requests": {
                                            "cpu": cpu_request,
                                            "memory": memory_request,
                                        },
                                        "limits": {
                                            "cpu": cpu_request,
                                            "memory": memory_request,
                                        },
                                    },
                                    "startupProbe": {
                                        "httpGet": {"path": "/health/ready", "port": "http"},
                                        "periodSeconds": 5,
                                        "failureThreshold": 60,
                                    },
                                    "readinessProbe": {
                                        "httpGet": {"path": "/health/ready", "port": "http"},
                                        "periodSeconds": 5,
                                        "failureThreshold": 3,
                                    },
                                    "livenessProbe": {
                                        "httpGet": {"path": "/health/live", "port": "http"},
                                        "periodSeconds": 15,
                                        "failureThreshold": 3,
                                    },
                                    "securityContext": {
                                        "allowPrivilegeEscalation": False,
                                        "capabilities": {"drop": ["ALL"]},
                                        "runAsNonRoot": True,
                                    },
                                }
                            ],
                        },
                    },
                },
            },
            "service": {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "name": name,
                    "namespace": self.settings.training_namespace,
                    "labels": labels,
                },
                "spec": {
                    "type": self.settings.inference_service_type,
                    "selector": {
                        "automl.platform/deployment-id": str(deployment_id),
                    },
                    "ports": [{"name": "http", "port": 8080, "targetPort": "http"}],
                },
            },
        }
        if (
            self.settings.inference_ingress_enabled
            and self.settings.inference_ingress_host_template
        ):
            host = self.settings.inference_ingress_host_template.format(
                name=name,
                deployment_id=deployment_id,
            )
            ingress_spec: dict[str, Any] = {
                "rules": [
                    {
                        "host": host,
                        "http": {
                            "paths": [
                                {
                                    "path": "/",
                                    "pathType": "Prefix",
                                    "backend": {
                                        "service": {
                                            "name": name,
                                            "port": {"name": "http"},
                                        }
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
            if self.settings.inference_ingress_class_name:
                ingress_spec["ingressClassName"] = (
                    self.settings.inference_ingress_class_name
                )
            if self.settings.inference_ingress_tls_secret_name:
                ingress_spec["tls"] = [
                    {
                        "hosts": [host],
                        "secretName": self.settings.inference_ingress_tls_secret_name,
                    }
                ]
            manifests["ingress"] = {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "Ingress",
                "metadata": {
                    "name": name,
                    "namespace": self.settings.training_namespace,
                    "labels": labels,
                },
                "spec": ingress_spec,
            }
        return manifests

    def create_model_deployment(self, manifests: dict[str, Any]) -> None:
        deployment = manifests["deployment"]
        service = manifests["service"]
        namespace = self.settings.training_namespace
        self.apps.create_namespaced_deployment(namespace=namespace, body=deployment)
        try:
            self.core.create_namespaced_service(namespace=namespace, body=service)
            if ingress := manifests.get("ingress"):
                self.networking.create_namespaced_ingress(
                    namespace=namespace,
                    body=ingress,
                )
        except Exception:
            try:
                self.core.delete_namespaced_service(
                    name=service["metadata"]["name"],
                    namespace=namespace,
                )
            except Exception:
                pass
            self.apps.delete_namespaced_deployment(
                name=deployment["metadata"]["name"],
                namespace=namespace,
                propagation_policy="Foreground",
            )
            raise

    def model_deployment_state(self, name: str) -> str:
        try:
            deployment = self.apps.read_namespaced_deployment_status(
                name=name,
                namespace=self.settings.training_namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return "missing"
            raise
        desired = deployment.spec.replicas or 0
        available = deployment.status.available_replicas or 0
        if desired > 0 and available >= desired:
            return "ready"
        selector = deployment.spec.selector.match_labels or {}
        label_selector = ",".join(
            f"{key}={value}" for key, value in selector.items()
        )
        pods = self.core.list_namespaced_pod(
            namespace=self.settings.training_namespace,
            label_selector=label_selector,
        ).items
        waiting_reasons = {
            status.state.waiting.reason
            for pod in pods
            for status in (pod.status.container_statuses or [])
            if status.state and status.state.waiting
        }
        terminated_reasons = {
            status.state.terminated.reason
            for pod in pods
            for status in (pod.status.container_statuses or [])
            if status.state and getattr(status.state, "terminated", None)
        }
        previous_terminated_reasons = {
            status.last_state.terminated.reason
            for pod in pods
            for status in (pod.status.container_statuses or [])
            if getattr(status, "last_state", None)
            and status.last_state.terminated
        }
        if "OOMKilled" in terminated_reasons | previous_terminated_reasons:
            return "out_of_memory"
        if waiting_reasons & {"ImagePullBackOff", "ErrImagePull"}:
            return "image_pull_error"
        if "CrashLoopBackOff" in waiting_reasons:
            return "crash_loop"
        if waiting_reasons & {
            "CreateContainerConfigError",
            "CreateContainerError",
            "InvalidImageName",
        }:
            return "configuration_error"
        if deployment.status.unavailable_replicas:
            return "progressing"
        return "pending"

    def model_deployment_urls(self, name: str) -> dict[str, str] | None:
        service = self.core.read_namespaced_service(
            name=name,
            namespace=self.settings.training_namespace,
        )
        port = next(
            (
                item
                for item in (service.spec.ports or [])
                if item.name == "http"
            ),
            None,
        )
        if port is None:
            return None
        base_url: str | None = None
        if self.settings.inference_ingress_enabled:
            try:
                ingress = self.networking.read_namespaced_ingress_status(
                    name=name,
                    namespace=self.settings.training_namespace,
                )
                load_balancer = getattr(ingress.status, "load_balancer", None)
                admitted = getattr(load_balancer, "ingress", None) or []
                rules = ingress.spec.rules or []
                if admitted and rules and rules[0].host:
                    scheme = (
                        "https"
                        if self.settings.inference_ingress_tls_secret_name
                        else "http"
                    )
                    base_url = f"{scheme}://{rules[0].host}"
            except ApiException as exc:
                if exc.status != 404:
                    raise
        elif service.spec.type == "LoadBalancer":
            load_balancer = getattr(getattr(service, "status", None), "load_balancer", None)
            ingress = getattr(load_balancer, "ingress", None) or []
            if ingress:
                host = getattr(ingress[0], "hostname", None) or getattr(
                    ingress[0], "ip", None
                )
                if host:
                    base_url = (
                        f"{self.settings.inference_external_scheme}://{host}:{port.port}"
                    )
        elif (
            service.spec.type == "NodePort"
            and port.node_port
            and self.settings.inference_external_host
        ):
            base_url = (
                f"{self.settings.inference_external_scheme}://"
                f"{self.settings.inference_external_host}:{port.node_port}"
            )
        if not base_url:
            return None
        return {
            "base_url": base_url,
            "endpoint": f"{base_url}/v1/predict",
            "docs_url": f"{base_url}/docs",
            "openapi_url": f"{base_url}/openapi.json",
        }

    def delete_model_deployment(self, name: str) -> None:
        namespace = self.settings.training_namespace
        if self.settings.inference_ingress_enabled:
            try:
                self.networking.delete_namespaced_ingress(name=name, namespace=namespace)
            except ApiException as exc:
                if exc.status != 404:
                    raise
        try:
            self.core.delete_namespaced_service(name=name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise
        self.apps.delete_namespaced_deployment(
            name=name,
            namespace=namespace,
            propagation_policy="Foreground",
        )

    def cleanup_finished_jobs(self, project_id: uuid.UUID) -> list[str]:
        deleted: list[str] = []
        jobs = self.batch.list_namespaced_job(
            namespace=self.settings.training_namespace,
            label_selector=(
                "automl.platform/workload=training,"
                f"automl.platform/project-id={project_id}"
            ),
        ).items
        for job in jobs:
            if not (job.status.succeeded or job.status.failed):
                continue
            name = str(job.metadata.name)
            try:
                self.delete_job(name)
            except ApiException as exc:
                if exc.status != 404:
                    raise
            deleted.append(name)
        return deleted

    def delete_job(self, job_name: str) -> None:
        self.batch.delete_namespaced_job(
            name=job_name,
            namespace=self.settings.training_namespace,
            propagation_policy="Foreground",
        )

    def job_state(self, job_name: str) -> str:
        try:
            job = self.batch.read_namespaced_job_status(
                name=job_name,
                namespace=self.settings.training_namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return "missing"
            raise
        if job.status.succeeded:
            return "succeeded"
        if job.status.failed:
            return "failed"
        pods = self.core.list_namespaced_pod(
            namespace=self.settings.training_namespace,
            label_selector=f"job-name={job_name}",
        ).items
        if _container_waiting_failure(pods, _FATAL_CONTAINER_WAITING_REASONS) is not None:
            return "terminal_waiting_failure"
        if _container_waiting_failure(pods, _IMAGE_PULL_BACKOFF_REASONS) is not None:
            return "image_pull_backoff"
        if job.status.active:
            return "running"
        return "queued"

    def job_failure_details(self, job_name: str) -> tuple[str, str]:
        try:
            job = self.batch.read_namespaced_job_status(
                name=job_name,
                namespace=self.settings.training_namespace,
            )
            for condition in job.status.conditions or []:
                if condition.type == "Failed" and condition.reason == "DeadlineExceeded":
                    return (
                        "JOB_DEADLINE_EXCEEDED",
                        condition.message or "The Kubernetes Job exceeded its active deadline.",
                    )
        except (ApiException, AttributeError):
            pass
        pods = self.core.list_namespaced_pod(
            namespace=self.settings.training_namespace,
            label_selector=f"job-name={job_name}",
        ).items
        if not pods:
            return (
                "KUBERNETES_JOB_FAILED",
                "The Kubernetes Job failed before a pod status was available.",
            )
        waiting_failure = _container_waiting_failure(
            pods,
            _REPORTED_CONTAINER_WAITING_FAILURE_REASONS,
        )
        if waiting_failure is not None:
            return waiting_failure
        pod = pods[0]
        status = pod.status
        messages: list[str] = []
        failure_code = "KUBERNETES_JOB_FAILED"
        if status.reason:
            messages.append(str(status.reason))
            if status.reason.lower() == "evicted":
                failure_code = "POD_EVICTED"
        if status.message:
            messages.append(str(status.message))
        for container_status in status.container_statuses or []:
            terminated = getattr(container_status.state, "terminated", None)
            waiting = getattr(container_status.state, "waiting", None)
            if terminated:
                reason = terminated.reason or "Terminated"
                messages.append(
                    f"Container {container_status.name}: {reason} "
                    f"(exit code {terminated.exit_code})."
                )
                if reason == "OOMKilled" or terminated.exit_code == 137:
                    failure_code = "POD_OOM_KILLED"
            elif waiting and waiting.reason:
                messages.append(
                    f"Container {container_status.name}: {waiting.reason}"
                    + (f" - {waiting.message}" if waiting.message else "")
                )
        for condition in status.conditions or []:
            if condition.status == "False" and condition.message:
                messages.append(str(condition.message))
        return failure_code, " ".join(dict.fromkeys(messages)) or "Kubernetes Job failed."

    def job_logs(self, run_id: uuid.UUID, tail_lines: int = 500) -> list[str]:
        pods = self.core.list_namespaced_pod(
            namespace=self.settings.training_namespace,
            label_selector=f"automl.platform/run-id={run_id}",
        ).items
        if not pods:
            return []
        logs = self.core.read_namespaced_pod_log(
            name=pods[0].metadata.name,
            namespace=self.settings.training_namespace,
            tail_lines=tail_lines,
            timestamps=True,
        )
        if isinstance(logs, bytes):
            logs = logs.decode("utf-8", errors="replace")
        elif logs.startswith(("b'", 'b"')):
            try:
                raw_logs = ast.literal_eval(logs)
                if isinstance(raw_logs, bytes):
                    logs = raw_logs.decode("utf-8", errors="replace")
            except (SyntaxError, ValueError):
                pass
        return logs.splitlines()

    def training_resource_usage(self, run_id: uuid.UUID) -> dict[str, Any]:
        """Return the current pod snapshot without requiring metrics-server to exist."""
        if not self._configured:
            return {"telemetry_available": False, "status_reason": self._configuration_error}
        pods = self.core.list_namespaced_pod(
            namespace=self.settings.training_namespace,
            label_selector=f"automl.platform/run-id={run_id}",
        ).items
        if not pods:
            return {
                "telemetry_available": False,
                "status_reason": "Training pod is no longer available.",
            }
        pod = max(pods, key=lambda item: item.metadata.creation_timestamp)
        statuses = pod.status.container_statuses or []
        reason = pod.status.reason
        restart_count = sum(int(item.restart_count or 0) for item in statuses)
        for item in statuses:
            waiting = getattr(item.state, "waiting", None)
            terminated = getattr(item.state, "terminated", None)
            if waiting and waiting.reason:
                reason = waiting.reason
            elif terminated and terminated.reason:
                reason = terminated.reason
        for condition in pod.status.conditions or []:
            if condition.type == "PodScheduled" and condition.status == "False":
                reason = condition.reason or "Unschedulable"
                if condition.message:
                    reason = f"{reason}: {condition.message}"
        result: dict[str, Any] = {
            "pod_name": pod.metadata.name,
            "pod_phase": pod.status.phase,
            "node_name": pod.spec.node_name,
            "restart_count": restart_count,
            "status_reason": reason,
            "telemetry_available": False,
        }
        try:
            metrics = self.custom.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=self.settings.training_namespace,
                plural="pods",
                name=pod.metadata.name,
            )
            cpu = 0.0
            memory = 0
            for container_metrics in metrics.get("containers", []):
                usage = container_metrics.get("usage", {})
                cpu += _cpu_cores(usage.get("cpu", "0"))
                memory += _memory_mb(usage.get("memory", "0"))
            result.update(
                telemetry_available=True,
                cpu_usage_cores=round(cpu, 4),
                memory_usage_mb=memory,
            )
        except (ApiException, AttributeError, TypeError, ValueError) as exc:
            result["status_reason"] = reason or f"Resource metrics unavailable: {exc}"
        return result

    def _pvc_is_bound(self, name: str) -> bool:
        try:
            pvc = self.core.read_namespaced_persistent_volume_claim(
                name=name,
                namespace=self.settings.training_namespace,
            )
            return pvc.status.phase == "Bound"
        except ApiException:
            return False

    def _priority_class_is_ready(self, name: str) -> bool:
        try:
            self.scheduling.read_priority_class(name)
            return True
        except ApiException:
            return False

    def _secret_exists(self, name: str) -> bool:
        try:
            self.core.read_namespaced_secret(
                name=name,
                namespace=self.settings.training_namespace,
            )
            return True
        except ApiException:
            return False

    def _service_exists(self, name: str) -> bool:
        try:
            self.core.read_namespaced_service(
                name=name,
                namespace=self.settings.training_namespace,
            )
            return True
        except ApiException:
            return False


def _container_waiting_failure(
    pods: list[Any],
    reasons: set[str],
) -> tuple[str, str] | None:
    """Return an actionable failure for container states that cannot make progress."""
    for pod in pods:
        pod_status = getattr(pod, "status", None)
        if getattr(pod_status, "phase", None) in {"Failed", "Succeeded"}:
            continue
        statuses = [
            *(getattr(pod_status, "init_container_statuses", None) or []),
            *(getattr(pod_status, "container_statuses", None) or []),
        ]
        for container_status in statuses:
            state = getattr(container_status, "state", None)
            waiting = getattr(state, "waiting", None)
            reason = str(getattr(waiting, "reason", None) or "")
            if reason not in reasons:
                continue
            return _container_waiting_failure_details(
                reason,
                container_status,
                waiting,
            )
    return None


def _container_waiting_failure_details(
    reason: str,
    container_status: Any,
    waiting: Any,
) -> tuple[str, str]:
    image = str(getattr(container_status, "image", None) or "the configured training image")
    reported_message = str(getattr(waiting, "message", None) or "").strip()
    reported = f" Kubernetes reported: {reported_message}" if reported_message else ""
    if reason == "ErrImageNeverPull":
        return (
            "TRAINING_IMAGE_NOT_PRESENT",
            f"Kubernetes cannot start training because image '{image}' is not present "
            "on the node and its pull policy prevents downloading it. Import the image "
            "into every local-cluster node, or publish it to a registry reachable by "
            f"the cluster and use IfNotPresent or Always.{reported}",
        )
    if reason == "ImagePullBackOff":
        return (
            "TRAINING_IMAGE_PULL_FAILED",
            f"Kubernetes repeatedly failed to pull training image '{image}'. Verify the "
            "image name and tag, registry reachability, and imagePullSecrets. For a local "
            "cluster, import the image into every node or use a cluster-visible "
            f"registry.{reported}",
        )
    if reason == "InvalidImageName":
        return (
            "TRAINING_IMAGE_INVALID",
            f"Kubernetes rejected training image reference '{image}'. Configure a valid "
            f"registry, repository, and tag before starting another run.{reported}",
        )
    if reason == "CreateContainerConfigError":
        return (
            "TRAINING_CONTAINER_CONFIG_INVALID",
            "Kubernetes cannot create the training container. Verify its required Secrets, "
            f"ConfigMaps, environment variables, and volume mounts.{reported}",
        )
    return (
        "TRAINING_CONTAINER_START_FAILED",
        f"Kubernetes cannot start the training container ({reason}). Verify the image, "
        f"runtime configuration, and pod events before starting another run.{reported}",
    )


def _active_deadline_seconds(
    expected_minutes: int,
    settings: Settings,
) -> int:
    estimated_deadline = expected_minutes * 60 * max(1, settings.training_deadline_multiplier)
    return min(
        max(
            settings.training_active_deadline_seconds,
            estimated_deadline,
        ),
        max(
            settings.training_active_deadline_seconds,
            settings.training_max_active_deadline_seconds,
        ),
    )


def _node_is_ready(node: Any) -> bool:
    return any(
        condition.type == "Ready" and condition.status == "True"
        for condition in (node.status.conditions or [])
    )


def _node_gpu(
    allocatable: dict[str, Any],
    requested: dict[str, int] | None = None,
    resources: tuple[tuple[str, str], ...] | None = None,
) -> tuple[str | None, str | None, int]:
    configured_resources = resources or (
        ("nvidia", "nvidia.com/gpu"),
        ("intel", "gpu.intel.com/xe"),
        ("intel", "gpu.intel.com/i915"),
    )
    for vendor, resource in configured_resources:
        raw_count = allocatable.get(resource)
        if raw_count is None:
            continue
        count = int(Decimal(parse_quantity(str(raw_count)))) - (requested or {}).get(resource, 0)
        if count > 0:
            return vendor, resource, count
    return None, None, 0


def _namespace_quota_capacity(
    quotas: list[Any],
) -> tuple[float, float, float, int, int, int] | None:
    """Return the strictest namespace CPU and memory ResourceQuota constraints."""

    cpu_constraints: list[tuple[float, float]] = []
    memory_constraints: list[tuple[int, int]] = []
    for quota in quotas:
        hard = getattr(quota.status, "hard", None) or {}
        used = getattr(quota.status, "used", None) or {}
        for key in ("requests.cpu", "limits.cpu", "cpu"):
            if key in hard:
                cpu_constraints.append(
                    (_cpu_cores(hard[key]), _cpu_cores(used.get(key, "0")))
                )
                break
        for key in ("requests.memory", "limits.memory", "memory"):
            if key in hard:
                memory_constraints.append(
                    (_memory_mb(hard[key]), _memory_mb(used.get(key, "0")))
                )
                break
    if not cpu_constraints or not memory_constraints:
        return None
    cpu_total, cpu_used = min(
        cpu_constraints,
        key=lambda item: max(0.0, item[0] - item[1]),
    )
    memory_total, memory_used = min(
        memory_constraints,
        key=lambda item: max(0, item[0] - item[1]),
    )
    return (
        cpu_total,
        cpu_used,
        max(0.0, cpu_total - cpu_used),
        memory_total,
        memory_used,
        max(0, memory_total - memory_used),
    )


def _cpu_cores(value: str) -> float:
    return float(Decimal(parse_quantity(str(value))))


def _memory_mb(value: str) -> int:
    quantity = Decimal(parse_quantity(str(value)))
    return int(quantity / Decimal(1024 * 1024))
