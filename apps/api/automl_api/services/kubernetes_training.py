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


@dataclass(frozen=True)
class NodeHeadroom:
    name: str
    available_cpu: float
    available_memory_mb: int
    gpu_present: bool
    gpu_vendor: str | None = None
    gpu_resource: str | None = None
    gpu_count: int = 0


@dataclass(frozen=True)
class CapacitySnapshot:
    capacity: ClusterCapacityRead
    nodes: list[NodeHeadroom]
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

        nodes = self.core.list_node().items
        pods = self.core.list_pod_for_all_namespaces().items
        pod_usage_by_node, metrics_warning = self._pod_usage_by_node(pods)
        pod_requests_by_node: dict[str, tuple[float, int]] = {}
        gpu_requests_by_node: dict[str, dict[str, int]] = {}
        active_training_jobs = 0
        for pod in pods:
            if pod.status.phase in {"Succeeded", "Failed"}:
                continue
            labels = pod.metadata.labels or {}
            if labels.get("automl.platform/workload") == "training":
                active_training_jobs += 1
            node_name = pod.spec.node_name
            if not node_name:
                continue
            cpu, memory = _pod_requests(pod)
            current_cpu, current_memory = pod_requests_by_node.get(node_name, (0.0, 0))
            pod_requests_by_node[node_name] = (
                current_cpu + cpu,
                current_memory + memory,
            )
            node_gpu_requests = gpu_requests_by_node.setdefault(node_name, {})
            for resource, count in _pod_gpu_requests(pod).items():
                node_gpu_requests[resource] = node_gpu_requests.get(resource, 0) + count

        ready_nodes = []
        total_cpu = 0.0
        total_memory = 0
        requested_cpu = 0.0
        requested_memory = 0
        used_cpu_total = 0.0
        used_memory_total = 0
        available_cpu_total = 0.0
        available_memory_total = 0
        gpu_available = False
        for node in nodes:
            if node.spec.unschedulable or not _node_is_ready(node):
                continue
            allocatable = node.status.allocatable or {}
            node_cpu = _cpu_cores(allocatable.get("cpu", "0"))
            node_memory = _memory_mb(allocatable.get("memory", "0"))
            node_requested_cpu, node_requested_memory = pod_requests_by_node.get(
                node.metadata.name,
                (0.0, 0),
            )
            node_used_cpu, node_used_memory = pod_usage_by_node.get(
                node.metadata.name,
                (0.0, 0),
            )
            reserved_cpu = max(node_requested_cpu, node_used_cpu)
            reserved_memory = max(node_requested_memory, node_used_memory)
            node_available_cpu = max(0.0, node_cpu - reserved_cpu)
            node_available_memory = max(0, node_memory - reserved_memory)
            gpu_vendor, gpu_resource, gpu_count = _node_gpu(
                allocatable,
                gpu_requests_by_node.get(node.metadata.name, {}),
            )
            gpu_present = gpu_resource is not None
            ready_nodes.append(
                NodeHeadroom(
                    name=node.metadata.name,
                    available_cpu=node_available_cpu,
                    available_memory_mb=node_available_memory,
                    gpu_present=gpu_present,
                    gpu_vendor=gpu_vendor,
                    gpu_resource=gpu_resource,
                    gpu_count=gpu_count,
                )
            )
            total_cpu += node_cpu
            total_memory += node_memory
            requested_cpu += node_requested_cpu
            requested_memory += node_requested_memory
            used_cpu_total += node_used_cpu
            used_memory_total += node_used_memory
            available_cpu_total += node_available_cpu
            available_memory_total += node_available_memory
            gpu_available = gpu_available or gpu_present

        warnings = [metrics_warning] if metrics_warning else []
        source = (
            "metrics_api_conservative_headroom"
            if not metrics_warning
            else "allocatable_minus_requests"
        )
        pvc_ready = self._pvc_is_bound("automl-dataset-cache")
        priority_ready = self._priority_class_is_ready("automl-low")
        runtime_dependencies_ready = (
            self._secret_exists("automl-platform-secrets")
            and self._secret_exists("automl-minio-credentials")
            and self._service_exists("automl-mlflow")
        )
        return CapacitySnapshot(
            capacity=ClusterCapacityRead(
                connected=True,
                source=source,
                total_cpu_cores=round(total_cpu, 3),
                requested_cpu_cores=round(requested_cpu, 3),
                used_cpu_cores=round(used_cpu_total, 3),
                available_cpu_cores=round(available_cpu_total, 3),
                total_memory_mb=total_memory,
                requested_memory_mb=requested_memory,
                used_memory_mb=used_memory_total,
                available_memory_mb=available_memory_total,
                ready_nodes=len(ready_nodes),
                gpu_available=gpu_available,
                active_training_jobs=active_training_jobs,
                warnings=warnings,
            ),
            nodes=ready_nodes,
            pvc_ready=pvc_ready,
            priority_class_ready=priority_ready,
            runtime_dependencies_ready=runtime_dependencies_ready,
        )

    def _pod_usage_by_node(
        self,
        pods: list[Any],
    ) -> tuple[dict[str, tuple[float, int]], str | None]:
        pod_nodes = {
            (pod.metadata.namespace, pod.metadata.name): pod.spec.node_name
            for pod in pods
            if pod.spec.node_name
        }
        try:
            response = self.custom.list_cluster_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                plural="pods",
            )
        except Exception:
            return (
                {},
                "Metrics API is unavailable; headroom is calculated from "
                "allocatable capacity minus declared pod requests.",
            )
        usage_by_node: dict[str, tuple[float, int]] = {}
        for item in response.get("items", []):
            metadata = item.get("metadata", {})
            node_name = pod_nodes.get((metadata.get("namespace"), metadata.get("name")))
            if not node_name:
                continue
            cpu = 0.0
            memory = 0
            for container_usage in item.get("containers", []):
                usage = container_usage.get("usage", {})
                cpu += _cpu_cores(usage.get("cpu", "0"))
                memory += _memory_mb(usage.get("memory", "0"))
            current_cpu, current_memory = usage_by_node.get(node_name, (0.0, 0))
            usage_by_node[node_name] = (
                current_cpu + cpu,
                current_memory + memory,
            )
        return usage_by_node, None

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
        eligible_nodes = gpu_nodes if gpu_requested else snapshot.nodes
        best_node = max(
            eligible_nodes,
            key=lambda node: (node.available_cpu, node.available_memory_mb),
            default=None,
        )
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
            blockers.append("Dataset cache PVC automl-dataset-cache is not Bound.")
        if not snapshot.priority_class_ready:
            blockers.append(
                "PriorityClass automl-low is missing or does not allow PreemptLowerPriority."
            )
        if not snapshot.runtime_dependencies_ready:
            blockers.append(
                "Training runtime Secret/MinIO credentials or MLflow Service is missing."
            )
        if best_node is None:
            blockers.append("No schedulable Ready Kubernetes node is available.")

        fraction = min(1.0, max(0.01, self.settings.max_node_available_fraction_per_job))
        if best_node:
            cpu_ceiling = best_node.available_cpu * fraction
            memory_ceiling = int(best_node.available_memory_mb * fraction)
            cpu_request = cpu_ceiling
            memory_request = memory_ceiling
        else:
            cpu_ceiling = 0.0
            memory_ceiling = 0
            cpu_request = 0.0
            memory_request = 0

        if cpu_request < 0.25:
            blockers.append("Insufficient CPU headroom for the minimum 0.25-core request.")
        if memory_request < 256:
            blockers.append("Insufficient memory headroom for the minimum 256 MiB request.")
        if best_node and desired_memory > memory_ceiling:
            blockers.append(
                f"Estimated training working set is {desired_memory} MiB, above the "
                f"{memory_ceiling} MiB per-job safety ceiling on the best node. "
                "Reduce the dataset, model budget, or search iterations."
            )

        cpu_request = round(max(0.0, cpu_request), 3)
        memory_request = max(0, memory_request)
        cpu_limit = cpu_request
        memory_limit = memory_request
        max_parallel = min(
            self.settings.max_concurrent_jobs,
            math.floor(capacity.total_cpu_cores / cpu_request) if cpu_request else 0,
            (math.floor(capacity.total_memory_mb / memory_request) if memory_request else 0),
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
            gpu_vendor=best_node.gpu_vendor if gpu_requested and best_node else None,
            gpu_resource=best_node.gpu_resource if gpu_requested and best_node else None,
            selected_node=best_node.name if best_node else None,
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
        container: dict[str, Any] = {
            "name": "trainer",
            "image": settings.training_image,
            "imagePullPolicy": "IfNotPresent",
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
                            "name": "automl-platform-secrets",
                            "key": "DATABASE_URL",
                        }
                    },
                },
                {"name": "OBJECT_STORE_TYPE", "value": "minio"},
                {"name": "OBJECT_STORE_ENDPOINT", "value": "http://automl-minio:9000"},
                {"name": "OBJECT_STORE_BUCKET", "value": "automl"},
                {
                    "name": "OBJECT_STORE_ACCESS_KEY",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "automl-minio-credentials",
                            "key": "MINIO_ROOT_USER",
                        }
                    },
                },
                {
                    "name": "OBJECT_STORE_SECRET_KEY",
                    "valueFrom": {
                        "secretKeyRef": {
                            "name": "automl-minio-credentials",
                            "key": "MINIO_ROOT_PASSWORD",
                        }
                    },
                },
                {
                    "name": "MLFLOW_TRACKING_URI",
                    "value": "http://automl-mlflow:5000",
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
            "priorityClassName": "automl-low",
            "serviceAccountName": settings.training_service_account,
            "containers": [container],
            "volumes": [
                {
                    "name": "dataset-cache",
                    "persistentVolumeClaim": {"claimName": "automl-dataset-cache"},
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
        if estimate.selected_node:
            pod_spec["nodeSelector"] = {"kubernetes.io/hostname": estimate.selected_node}
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

        return {
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
            "spec": {
                "backoffLimit": 0,
                "activeDeadlineSeconds": estimate.active_deadline_seconds,
                "ttlSecondsAfterFinished": 30,
                "template": {
                    "metadata": {
                        "labels": {
                            "automl.platform/workload": "training",
                            "automl.platform/run-id": str(run_id),
                        }
                    },
                    "spec": pod_spec,
                },
            },
        }

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
        return {
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
                            "containers": [
                                {
                                    "name": "model",
                                    "image": image,
                                    "imagePullPolicy": "IfNotPresent",
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
                                            "value": "http://automl-minio:9000",
                                        },
                                        {"name": "OBJECT_STORE_BUCKET", "value": "automl"},
                                        {
                                            "name": "OBJECT_STORE_ACCESS_KEY",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": "automl-minio-credentials",
                                                    "key": "MINIO_ROOT_USER",
                                                }
                                            },
                                        },
                                        {
                                            "name": "OBJECT_STORE_SECRET_KEY",
                                            "valueFrom": {
                                                "secretKeyRef": {
                                                    "name": "automl-minio-credentials",
                                                    "key": "MINIO_ROOT_PASSWORD",
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

    def create_model_deployment(self, manifests: dict[str, Any]) -> None:
        deployment = manifests["deployment"]
        service = manifests["service"]
        namespace = self.settings.training_namespace
        self.apps.create_namespaced_deployment(namespace=namespace, body=deployment)
        try:
            self.core.create_namespaced_service(namespace=namespace, body=service)
        except Exception:
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
        if service.spec.type == "NodePort" and port.node_port:
            nodes = self.core.list_node().items
            addresses = [
                address
                for node in nodes
                for address in (node.status.addresses or [])
            ]
            host = next(
                (
                    address.address
                    for address in addresses
                    if address.type == "ExternalIP"
                ),
                None,
            ) or next(
                (
                    address.address
                    for address in addresses
                    if address.type == "InternalIP"
                ),
                None,
            )
            if not host:
                return None
            base_url = f"http://{host}:{port.node_port}"
        else:
            base_url = (
                f"http://{name}.{self.settings.training_namespace}.svc:"
                f"{port.port}"
            )
        return {
            "base_url": base_url,
            "endpoint": f"{base_url}/v1/predict",
            "docs_url": f"{base_url}/docs",
            "openapi_url": f"{base_url}/openapi.json",
        }

    def delete_model_deployment(self, name: str) -> None:
        namespace = self.settings.training_namespace
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
            priority = self.scheduling.read_priority_class(name)
            return priority.preemption_policy == "PreemptLowerPriority"
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
) -> tuple[str | None, str | None, int]:
    resources = (
        ("nvidia", "nvidia.com/gpu"),
        ("intel", "gpu.intel.com/xe"),
        ("intel", "gpu.intel.com/i915"),
    )
    for vendor, resource in resources:
        raw_count = allocatable.get(resource)
        if raw_count is None:
            continue
        count = int(Decimal(parse_quantity(str(raw_count)))) - (requested or {}).get(resource, 0)
        if count > 0:
            return vendor, resource, count
    return None, None, 0


def _pod_gpu_requests(pod: Any) -> dict[str, int]:
    requested: dict[str, int] = {}
    for container in pod.spec.containers or []:
        limits = container.resources.limits or {}
        resources = {**limits, **(container.resources.requests or {})}
        for _, resource in (
            ("nvidia", "nvidia.com/gpu"),
            ("intel", "gpu.intel.com/xe"),
            ("intel", "gpu.intel.com/i915"),
        ):
            if resource in resources:
                requested[resource] = requested.get(resource, 0) + int(
                    Decimal(parse_quantity(str(resources[resource])))
                )
    return requested


def _pod_requests(pod: Any) -> tuple[float, int]:
    cpu = 0.0
    memory = 0
    for container in pod.spec.containers or []:
        requests = container.resources.requests or {}
        cpu += _cpu_cores(requests.get("cpu", "0"))
        memory += _memory_mb(requests.get("memory", "0"))
    return cpu, memory


def _cpu_cores(value: str) -> float:
    return float(Decimal(parse_quantity(str(value))))


def _memory_mb(value: str) -> int:
    quantity = Decimal(parse_quantity(str(value)))
    return int(quantity / Decimal(1024 * 1024))
