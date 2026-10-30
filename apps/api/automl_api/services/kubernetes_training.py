from __future__ import annotations

import ast
import logging
import math
import os
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from kubernetes import client, config
from kubernetes.client import ApiException
from kubernetes.utils.quantity import parse_quantity

from automl_api.core.config import Settings, get_settings
from automl_api.models.enums import TaskType, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from automl_api.schemas.training import ClusterCapacityRead, TrainingEstimateRead
from automl_api.services.inference_tls import (
    DATA_PORT,
    HEALTH_PORT,
    credential_mounts,
    credential_volumes,
    serving_declaration,
)
from automl_api.services.inference_tls import (
    PROTOCOL as INFERENCE_PROTOCOL,
)
from automl_api.services.workflow_state import canonical_request_hash

logger = logging.getLogger(__name__)


def _model_deployment_state(deployment, pods):
    status = deployment.get("status") or {}
    desired = deployment["spec"].get("replicas") or 0
    if desired > 0 and (status.get("availableReplicas") or 0) >= desired:
        return "ready"
    containers = [
        container
        for pod in pods
        for container in ((pod.get("status") or {}).get("containerStatuses") or [])
    ]
    waiting = {
        (container.get("state") or {}).get("waiting", {}).get("reason")
        for container in containers
        if (container.get("state") or {}).get("waiting")
    }
    terminated = {
        value.get("terminated", {}).get("reason")
        for container in containers
        for value in (container.get("state") or {}, container.get("lastState") or {})
        if value.get("terminated")
    }
    if "OOMKilled" in terminated:
        return "out_of_memory"
    if waiting & {"ImagePullBackOff", "ErrImagePull"}:
        return "image_pull_error"
    if "CrashLoopBackOff" in waiting:
        return "crash_loop"
    if waiting & {"CreateContainerConfigError", "CreateContainerError", "InvalidImageName"}:
        return "configuration_error"
    return "progressing" if status.get("unavailableReplicas") else "pending"


def _model_deployment_urls(service, ingress, settings):
    port = next((p for p in service["spec"].get("ports", []) if p.get("name") == "http"), None)
    if port is None:
        return {}
    base_url = None
    if ingress is not None:
        rules = ingress["spec"].get("rules") or []
        if (
            ((ingress.get("status") or {}).get("loadBalancer") or {}).get("ingress")
            and rules
            and rules[0].get("host")
        ):
            scheme = "https" if ingress["spec"].get("tls") else "http"
            base_url = f"{scheme}://{rules[0]['host']}"
    elif service["spec"].get("type") == "LoadBalancer":
        admitted = ((service.get("status") or {}).get("loadBalancer") or {}).get("ingress") or []
        if admitted:
            host = admitted[0].get("hostname") or admitted[0].get("ip")
            if host:
                base_url = f"{settings.inference_external_scheme}://{host}:{port['port']}"
    elif (
        service["spec"].get("type") == "NodePort"
        and port.get("nodePort")
        and settings.inference_external_host
    ):
        base_url = f"{settings.inference_external_scheme}://{settings.inference_external_host}:{port['nodePort']}"
    if not base_url:
        return {}
    return dict(
        base_url=base_url,
        endpoint=f"{base_url}/v1/predict",
        docs_url=f"{base_url}/docs",
        openapi_url=f"{base_url}/openapi.json",
    )


class ModelDeploymentRetirementPending(RuntimeError):
    pass


def _requested_inference_spec(actual, wanted, path=()):
    if isinstance(wanted, dict):
        return isinstance(actual, dict) and all(
            key in actual and _requested_inference_spec(actual[key], value, (*path, key))
            for key, value in wanted.items()
        )
    if isinstance(wanted, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(wanted)
            and all(
                _requested_inference_spec(value, expected, (*path, str(index)))
                for index, (value, expected) in enumerate(zip(actual, wanted, strict=True))
            )
        )
    if len(path) >= 3 and path[-3] == "resources" and path[-2] in {"requests", "limits"}:
        try:
            return parse_quantity(str(actual)) == parse_quantity(str(wanted))
        except (ValueError, TypeError):
            return False
    return type(actual) is type(wanted) and actual == wanted


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


def mount_provider_source_configuration(pod_spec: dict[str, Any]) -> None:
    """Share the protected public source configuration across all workload pods."""
    container = pod_spec["containers"][0]
    policies = os.getenv("PROVIDER_IMPORT_POLICIES_CONFIG_MAP", "")
    if policies:
        public_key = os.getenv("EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET", "")
        if not public_key:
            raise ValueError("Provider source policies require the authority public key Secret")
        container["env"].extend(
            [
                {
                    "name": "PROVIDER_IMPORT_POLICIES_FILE",
                    "value": "/provider-import/policies.json",
                },
                {
                    "name": "EVALUATION_AUTHORITY_PUBLIC_KEY_FILE",
                    "value": "/evaluation-authority/public-key.pem",
                },
            ]
        )
        container["volumeMounts"].extend(
            [
                {"name": "provider-import", "mountPath": "/provider-import", "readOnly": True},
                {
                    "name": "evaluation-authority",
                    "mountPath": "/evaluation-authority",
                    "readOnly": True,
                },
            ]
        )
        pod_spec["volumes"].extend(
            [
                {
                    "name": "provider-import",
                    "configMap": {
                        "name": policies,
                        "items": [{"key": "policies.json", "path": "policies.json"}],
                    },
                },
                {
                    "name": "evaluation-authority",
                    "secret": {
                        "secretName": public_key,
                        "items": [{"key": "public-key.pem", "path": "public-key.pem"}],
                    },
                },
            ]
        )


def object_store_workload_environment(settings: Settings) -> list[dict[str, Any]]:
    driver = settings.object_store_type.strip().lower()
    if driver == "minio":
        driver = "s3_compatible"
    environment: list[dict[str, Any]] = [
        {"name": "OBJECT_STORE_TYPE", "value": driver},
        {"name": "OBJECT_STORE_ENDPOINT", "value": settings.object_store_endpoint or ""},
        {"name": "OBJECT_STORE_BUCKET", "value": settings.object_store_bucket},
        {"name": "OBJECT_STORE_REGION", "value": settings.object_store_region or ""},
        {"name": "GCS_PROJECT", "value": settings.gcs_project or ""},
        {
            "name": "AZURE_STORAGE_ACCOUNT",
            "value": settings.azure_storage_account or "",
        },
    ]
    if driver == "s3_compatible":
        environment.extend(
            [
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
            ]
        )
    return environment


@dataclass(frozen=True)
class NodeCapability:
    name: str
    gpu_vendor: str | None = None
    gpu_resource: str | None = None
    gpu_count: int = 0
    allocatable_cpu_cores: float = 0
    allocatable_memory_mb: int = 0
    available_memory_mb: int | None = None
    available_cpu_cores: float | None = None


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
            configuration = client.Configuration.get_default_copy()
            configuration.retries = 0
            api_client = client.ApiClient(configuration)
            self.core = client.CoreV1Api(api_client)
            self.batch = client.BatchV1Api(api_client)
            self.apps = client.AppsV1Api(api_client)
            self.networking = client.NetworkingV1Api(api_client)
            self.scheduling = client.SchedulingV1Api(api_client)
            self.custom = client.CustomObjectsApi(api_client)
            self.authentication = client.AuthenticationV1Api(api_client)
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
        active_training_jobs = sum(pod.status.phase not in {"Succeeded", "Failed"} for pod in pods)

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
                    allocatable = node.status.allocatable or {}
                    vendor, resource, count = _node_gpu(
                        allocatable,
                        resources=self._gpu_resources(),
                    )
                    ready_nodes.append(
                        NodeCapability(
                            name=str(node.metadata.name),
                            gpu_vendor=vendor,
                            gpu_resource=resource,
                            gpu_count=count,
                            allocatable_cpu_cores=_cpu_cores(allocatable.get("cpu", "0")),
                            allocatable_memory_mb=_memory_mb(allocatable.get("memory", "0")),
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

        if ready_nodes:
            try:
                reservations: dict[str, tuple[float, int]] = {}
                for pod in self.core.list_pod_for_all_namespaces().items:
                    if pod.status.phase in {"Succeeded", "Failed"} or not pod.spec.node_name:
                        continue
                    cpu, memory = _pod_resource_requests(pod.spec)
                    previous_cpu, previous_memory = reservations.get(pod.spec.node_name, (0, 0))
                    reservations[pod.spec.node_name] = (
                        previous_cpu + cpu,
                        previous_memory + memory,
                    )
                ready_nodes = [
                    replace(
                        node,
                        available_cpu_cores=max(
                            0, node.allocatable_cpu_cores - reservations.get(node.name, (0, 0))[0]
                        ),
                        available_memory_mb=max(
                            0, node.allocatable_memory_mb - reservations.get(node.name, (0, 0))[1]
                        ),
                    )
                    for node in ready_nodes
                ]
            except (ApiException, AttributeError) as exc:
                warnings.append(
                    "Node pod reservations are unavailable; adaptive sizing uses the configured "
                    "fallback and Kubernetes will queue unavailable capacity "
                    f"({type(exc).__name__})."
                )

        pvc_ready = not self.settings.dataset_cache_pvc_name or self._pvc_is_bound(
            self.settings.dataset_cache_pvc_name
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

    def create_ray_job(self, manifest: dict[str, Any]) -> dict[str, Any]:
        if not self._configured:
            raise RuntimeError(self._configuration_error or "Kubernetes client is not configured.")
        namespace = (
            manifest.get("metadata", {}).get("namespace") or self.settings.training_namespace
        )
        # A conflict cannot attest the original UID after an unknown create outcome.
        created = self.custom.create_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace,
            plural="rayjobs",
            body=manifest,
            _request_timeout=5,
        )
        metadata = created.get("metadata") if isinstance(created, dict) else None
        uid = metadata.get("uid") if isinstance(metadata, dict) else None
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        if (
            not isinstance(uid, str)
            or not uid.strip()
            or metadata.get("name") != manifest["metadata"]["name"]
            or metadata.get("namespace") != namespace
            or not isinstance(labels, dict)
            or any(
                labels.get(key) != value
                for key, value in manifest["metadata"].get("labels", {}).items()
            )
            or created.get("spec") != manifest.get("spec")
        ):
            raise ValueError("RayJob identity or submitted specification differs from the command.")
        return created

    def activate_ray_job(self, manifest: dict[str, Any], uid: str) -> dict[str, Any]:
        name, namespace = (manifest["metadata"][key] for key in ("name", "namespace"))

        def verify(value):
            metadata = value.get("metadata", {})
            wanted = deepcopy(manifest["spec"])
            suspended = value.get("spec", {}).get("suspend")
            wanted["suspend"] = suspended
            if (
                metadata.get("uid") != uid
                or metadata.get("name") != name
                or metadata.get("namespace") != namespace
                or not isinstance(metadata.get("resourceVersion"), str)
                or not metadata["resourceVersion"]
                or type(suspended) is not bool
                or value.get("spec") != wanted
                or any(
                    metadata.get("labels", {}).get(key) != expected
                    for key, expected in manifest["metadata"].get("labels", {}).items()
                )
            ):
                raise ValueError("Original Ray activation target or specification changed.")
            return suspended

        original = self.ray_job(name, namespace=namespace)
        if not verify(original):
            return original
        activated = self.custom.patch_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace,
            plural="rayjobs",
            name=name,
            body=[
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": original["metadata"]["resourceVersion"],
                },
                {"op": "test", "path": "/spec", "value": original["spec"]},
                {"op": "replace", "path": "/spec/suspend", "value": False},
            ],
            _content_type="application/json-patch+json",
            _request_timeout=5,
        )
        if verify(activated):
            raise ValueError("Original RayJob remains suspended after activation.")
        return activated

    def ray_job(self, name: str, namespace: str | None = None) -> dict[str, Any]:
        if not self._configured:
            raise RuntimeError(self._configuration_error or "Kubernetes client is not configured.")
        return self.custom.get_namespaced_custom_object(
            group="ray.io",
            version="v1",
            namespace=namespace or self.settings.training_namespace,
            plural="rayjobs",
            name=name,
            _request_timeout=5,
        )

    def delete_ray_job(
        self, name: str, namespace: str | None = None, *, uid: str | None = None
    ) -> None:
        if not self._configured:
            raise RuntimeError(self._configuration_error or "Kubernetes client is not configured.")
        try:
            self.custom.delete_namespaced_custom_object(
                group="ray.io",
                version="v1",
                namespace=namespace or self.settings.training_namespace,
                plural="rayjobs",
                name=name,
                body=client.V1DeleteOptions(
                    propagation_policy="Foreground",
                    preconditions=client.V1Preconditions(uid=uid) if uid is not None else None,
                ),
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

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
        # Candidates and search iterations execute serially (n_jobs=1), so they
        # increase duration but not the peak number of resident datasets/models.
        # The runner also releases each losing model after its durable artifact
        # is written. Estimate the largest selected model, not the sum of the
        # whole catalog.
        search_multiplier = (1 + min(max(optimization_iterations, 1), 25) * 0.01) * max(
            1.0, min(model_cost_factor, 2.0)
        )
        data_working_set = (
            max(dataset_mb * 2, dense_matrix_mb * 3)
            if dataset_rows > 0 and column_count > 0
            else dataset_mb * 6
        )
        estimated_working_set = math.ceil(
            700 + data_working_set * task_multiplier * search_multiplier
        )
        desired_memory = max(768, estimated_working_set)

        compatible_vendors = (
            gpu_compatible_vendors if gpu_compatible_vendors is not None else {"nvidia", "intel"}
        )
        gpu_nodes = [
            node
            for node in snapshot.nodes
            if node.gpu_resource and node.gpu_vendor in compatible_vendors
        ]
        gpu_requested = bool(prefer_gpu and self.settings.gpu_enabled and gpu_nodes)
        gpu_capability = gpu_nodes[0] if gpu_requested else None
        eligible_nodes = gpu_nodes if gpu_requested else snapshot.nodes
        capacity_node = max(
            eligible_nodes,
            key=lambda item: (
                item.available_memory_mb
                if item.available_memory_mb is not None
                else item.allocatable_memory_mb
            ),
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
            blockers.append("Training runtime database or object-store Secret is missing.")

        cpu_request = max(0.0, self.settings.training_cpu_request_cores)
        cpu_limit = max(cpu_request, self.settings.training_cpu_limit_cores)
        configured_memory_limit = max(
            self.settings.training_memory_request_mb,
            self.settings.training_memory_limit_mb,
        )
        memory_limit = configured_memory_limit
        selected_node = None
        if (
            self.settings.training_adaptive_node_sizing_enabled
            and capacity_node is not None
            and capacity_node.allocatable_memory_mb > 0
        ):
            fraction = min(max(self.settings.training_node_memory_fraction, 0.5), 0.9)
            memory_limit = max(
                self.settings.training_memory_request_mb,
                math.floor(capacity_node.allocatable_memory_mb * fraction),
            )
            if capacity_node.available_memory_mb is not None:
                free = sorted(
                    (node.available_memory_mb or 0 for node in eligible_nodes), reverse=True
                )
                # The coordinator still loads frames and publishes the winner.
                # Budget for two pods plus its autoscaler and the submitter.
                first = max(0, free[0] - 512)
                second = free[1] if len(free) > 1 else 0
                per_pod = max(first / 2, min(first, second))
                memory_limit = min(memory_limit, math.floor(per_pod * fraction))
                if memory_limit < 768:
                    blockers.append(
                        "Insufficient unreserved memory for the coordinator and a model worker."
                    )
                memory_limit = max(256, memory_limit)
            if capacity.source == "namespace_resource_quota":
                memory_limit = min(
                    memory_limit, max(256, (capacity.available_memory_mb - 512) // 2)
                )
            selected_node = capacity_node.name
            warnings.append(
                f"Training memory adapts to node {selected_node}: {memory_limit} MiB "
                + (
                    "per model pod, allowing for existing reservations and the coordinator."
                    if capacity_node.available_memory_mb is not None
                    else f"({fraction:.0%} of {capacity_node.allocatable_memory_mb} MiB "
                    "allocatable; "
                    "free memory could not be observed)."
                )
            )
        elif self.settings.training_adaptive_node_sizing_enabled:
            warnings.append(
                "Adaptive node sizing could not observe allocatable node memory; using the "
                f"configured {configured_memory_limit} MiB fallback."
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
            selected_node=selected_node,
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
        attempt: WorkflowAttempt | None = None,
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
                            # P6-W12: workers get the restricted run-scoped
                            # credential, never the full application secret.
                            "name": settings.worker_database_secret_name,
                            "key": settings.worker_database_secret_key,
                        }
                    },
                },
                *object_store_workload_environment(settings),
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
                # Memory-backed emptyDir volume: pod-scoped, never a shared
                # host path, so the fixed mount point is safe.
                {"name": "shared-memory", "mountPath": "/dev/shm"},  # nosec B108
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
        mount_provider_source_configuration(pod_spec)
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
            **(
                {"activeDeadlineSeconds": estimate.active_deadline_seconds}
                if estimate.active_deadline_seconds is not None
                else {}
            ),
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
        if attempt is not None:
            if attempt.model_run_id != run_id or attempt.project_id != project_id:
                raise ValueError("The analysis manifest attempt scope is invalid.")
            manifest = self.bind_analysis_attempt_manifest(manifest, attempt)
        return manifest

    def bind_analysis_attempt_manifest(self, manifest, attempt):
        manifest = deepcopy(manifest)
        run_id, project_id = attempt.model_run_id, attempt.project_id
        if (
            attempt.stage != WorkflowStage.TRAINING_RUN
            or attempt.logical_key != f"analysis-run:{run_id}"
            or manifest["metadata"]["labels"].get("automl.platform/run-id") != str(run_id)
            or manifest["metadata"]["labels"].get("automl.platform/project-id") != str(project_id)
        ):
            raise ValueError("The analysis manifest attempt scope is invalid.")
        settings = self.settings
        job_spec = manifest["spec"]
        pod_spec = job_spec["template"]["spec"]
        container = pod_spec["containers"][0]
        from automl_api.schemas.parent_control import PARENT_CONTROL_PROTOCOL
        from automl_api.services.attempt_identity import attempt_audience
        from automl_api.training.attempt_admission import (
            ANALYSIS_PROTOCOL,
            TOKEN_PATH,
            control_base_url,
        )

        command = ["python", "-m", "automl_api.training.worker", "--run-id", str(run_id)]
        if container.get("command") not in (
            command,
            command + ["--attempt-protocol", ANALYSIS_PROTOCOL],
        ):
            raise ValueError("The analysis entrypoint is not a supported worker command.")
        container["command"] = command + ["--attempt-protocol", ANALYSIS_PROTOCOL]
        url = settings.attempt_control_base_url
        if url:
            url = control_base_url(url)
        elif settings.environment in {"production", "staging"}:
            raise ValueError("Production analysis requires an HTTPS attempt admission URL.")
        name = f"automl-analysis-{run_id.hex}-g{attempt.generation}"
        manifest["metadata"]["name"] = name
        labels = {
            "automl.platform/project-id": str(project_id),
            "automl.platform/attempt-id": str(attempt.id),
            "automl.platform/generation": str(attempt.generation),
            "automl.platform/workflow-stage": attempt.stage.value,
        }
        manifest["metadata"]["labels"].update(labels)
        job_spec["template"]["metadata"]["labels"].update(labels)
        pod_spec["serviceAccountName"] = attempt.workload_identity
        bindings = {
            "AUTOML_PARENT_CONTROL_PROTOCOL",
            "ENVIRONMENT",
            "ATTEMPT_CONTROL_BASE_URL",
            "AUTOML_ATTEMPT_ID",
            "AUTOML_FENCING_TOKEN",
            "AUTOML_IDENTITY_TOKEN_FILE",
        }
        container["env"] = [e for e in container["env"] if e["name"] not in bindings]
        container["env"].extend(
            [
                {"name": "ENVIRONMENT", "value": settings.environment},
                {"name": "ATTEMPT_CONTROL_BASE_URL", "value": url or ""},
                {"name": "AUTOML_ATTEMPT_ID", "value": str(attempt.id)},
                {"name": "AUTOML_FENCING_TOKEN", "value": attempt.fencing_token},
                {"name": "AUTOML_IDENTITY_TOKEN_FILE", "value": TOKEN_PATH},
            ]
        )
        if url:
            container["env"].append(
                {"name": "AUTOML_PARENT_CONTROL_PROTOCOL", "value": PARENT_CONTROL_PROTOCOL}
            )
        pod_spec["volumes"] = [v for v in pod_spec["volumes"] if v["name"] != "attempt-identity"]
        container["volumeMounts"] = [
            v for v in container["volumeMounts"] if v["name"] != "attempt-identity"
        ]
        pod_spec["volumes"].append(
            {
                "name": "attempt-identity",
                "projected": {
                    "defaultMode": 0o444,
                    "sources": [
                        {
                            "serviceAccountToken": {
                                "path": "token",
                                "audience": attempt_audience(attempt.id, attempt.fencing_token),
                                "expirationSeconds": 600,
                            }
                        }
                    ],
                },
            }
        )
        container["volumeMounts"].append(
            {
                "name": "attempt-identity",
                "mountPath": str(Path(TOKEN_PATH).parent),
                "readOnly": True,
            }
        )
        return manifest

    def create_job(self, manifest: dict[str, Any]):
        from automl_api.services.workflow_state import StaleFence

        metadata = manifest.get("metadata") or {}
        labels = metadata.get("labels") or {}
        bound = "automl.platform/attempt-id" in labels
        namespace = metadata.get("namespace") or self.settings.training_namespace
        required = ("project-id", "run-id", "attempt-id", "generation", "workflow-stage")
        if bound and (
            not isinstance(metadata.get("name"), str)
            or not metadata["name"].strip()
            or not isinstance(metadata.get("namespace"), str)
            or not metadata["namespace"].strip()
            or any(
                not isinstance(labels.get(f"automl.platform/{key}"), str)
                or not labels[f"automl.platform/{key}"].strip()
                for key in required
            )
        ):
            raise StaleFence("The bound analysis Job scope is incomplete.")
        # A name conflict cannot attest the original UID after an unknown create outcome.
        created = self.batch.create_namespaced_job(
            namespace=namespace, body=manifest, _request_timeout=5
        )
        if bound:
            reply = getattr(created, "metadata", None)
            uid = getattr(reply, "uid", None)
            actual_labels = getattr(reply, "labels", None)
            if (
                not isinstance(uid, str)
                or not uid.strip()
                or getattr(reply, "name", None) != metadata["name"]
                or getattr(reply, "namespace", None) != namespace
                or not isinstance(actual_labels, dict)
                or any(actual_labels.get(key) != value for key, value in labels.items())
            ):
                raise StaleFence("The original analysis Job identity differs from its command.")
        return created

    def activate_job(self, original: dict[str, Any]) -> dict[str, Any]:
        """Activate only the original suspended server specification and UID."""
        metadata = original["metadata"]
        name, namespace, uid = (metadata[key] for key in ("name", "namespace", "uid"))
        if (
            not isinstance(uid, str)
            or not uid.strip()
            or original["spec"].get("suspend") is not True
        ):
            raise ValueError("Original analysis activation identity/specification is incomplete.")

        def verify(value):
            meta = value.get("metadata", {})
            wanted = deepcopy(original["spec"])
            suspended = value.get("spec", {}).get("suspend")
            wanted["suspend"] = suspended
            if (
                any(meta.get(k) != metadata[k] for k in ("uid", "name", "namespace"))
                or not isinstance(meta.get("resourceVersion"), str)
                or not meta["resourceVersion"]
                or type(suspended) is not bool
                or value.get("spec") != wanted
                or any(meta.get("labels", {}).get(k) != v for k, v in metadata["labels"].items())
            ):
                raise ValueError("Original analysis activation target or specification changed.")
            return suspended

        observed = self.batch.read_namespaced_job(
            name=name, namespace=namespace, _request_timeout=5
        )
        observed = self.batch.api_client.sanitize_for_serialization(observed)
        if not verify(observed):
            return observed
        active = self.batch.patch_namespaced_job(
            name=name,
            namespace=namespace,
            body=[
                {"op": "test", "path": "/metadata/uid", "value": uid},
                {
                    "op": "test",
                    "path": "/metadata/resourceVersion",
                    "value": observed["metadata"]["resourceVersion"],
                },
                {"op": "test", "path": "/spec", "value": observed["spec"]},
                {"op": "replace", "path": "/spec/suspend", "value": False},
            ],
            _content_type="application/json-patch+json",
            _request_timeout=5,
        )
        active = self.batch.api_client.sanitize_for_serialization(active)
        if verify(active):
            raise ValueError("Original analysis Job remains suspended after activation.")
        return active

    def ensure_service_account(self, name: str, namespace: str | None = None) -> None:
        namespace = namespace or self.settings.training_namespace
        body = {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {"name": name, "namespace": namespace},
            "automountServiceAccountToken": False,
        }
        try:
            self.core.create_namespaced_service_account(
                namespace=namespace, body=body, _request_timeout=5
            )
        except ApiException as exc:
            if exc.status != 409:
                raise

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
        model_digest: str,
        identity_ca_digest: str,
        identity_control_base_url: str,
    ) -> dict[str, Any]:
        identity_base = identity_control_base_url + f"/{deployment_id}"
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
                            "securityContext": {
                                "fsGroup": 10001,
                                "seccompProfile": {"type": "RuntimeDefault"},
                            },
                            "volumes": credential_volumes(deployment_id, identity_ca_digest),
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
                                    "command": ["python", "-m", "automl_api.inference.runtime"],
                                    "ports": [
                                        {"name": "https", "containerPort": DATA_PORT},
                                        {"name": "health", "containerPort": HEALTH_PORT},
                                    ],
                                    "volumeMounts": credential_mounts(),
                                    "env": [
                                        {"name": "MODEL_URI", "value": model_uri},
                                        {"name": "MODEL_SHA256", "value": model_digest},
                                        {
                                            "name": "INFERENCE_IDENTITY_PROTOCOL",
                                            "value": INFERENCE_PROTOCOL,
                                        },
                                        {
                                            "name": "INFERENCE_CA_SHA256",
                                            "value": identity_ca_digest,
                                        },
                                        {
                                            "name": "INFERENCE_DEPLOYMENT_ID",
                                            "value": str(deployment_id),
                                        },
                                        {"name": "INFERENCE_PROJECT_ID", "value": str(project_id)},
                                        {
                                            "name": "INFERENCE_IDENTITY_URL",
                                            "value": identity_base + "/identity",
                                        },
                                        {
                                            "name": "MODEL_DOWNLOAD_URL",
                                            "value": identity_base + "/model",
                                        },
                                        {"name": "MODEL_NAME", "value": model_name},
                                        {
                                            "name": "PROJECT_NAME",
                                            "value": project_name,
                                        },
                                        {
                                            "name": "DEPLOYMENT_ENVIRONMENT",
                                            "value": environment,
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
                                        "httpGet": {"path": "/health/ready", "port": "health"},
                                        "periodSeconds": 5,
                                        "failureThreshold": 60,
                                    },
                                    "readinessProbe": {
                                        "httpGet": {"path": "/health/ready", "port": "health"},
                                        "periodSeconds": 5,
                                        "failureThreshold": 3,
                                    },
                                    "livenessProbe": {
                                        "httpGet": {"path": "/health/live", "port": "health"},
                                        "periodSeconds": 15,
                                        "failureThreshold": 3,
                                    },
                                    "securityContext": {
                                        "allowPrivilegeEscalation": False,
                                        "capabilities": {"drop": ["ALL"]},
                                        "runAsNonRoot": True,
                                        "runAsUser": 10001,
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
                    "ports": [{"name": "https", "port": DATA_PORT, "targetPort": "https"}],
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
                                            "port": {"name": "https"},
                                        }
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
            if self.settings.inference_ingress_class_name:
                ingress_spec["ingressClassName"] = self.settings.inference_ingress_class_name
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
        serving_declaration(
            manifests,
            project_id=project_id,
            run_id=deployment_id,
            model_digest=model_digest,
            model_uri=model_uri,
            ca_digest=identity_ca_digest,
            base_url=identity_control_base_url,
        )
        return manifests

    def _model_deployment_plans(self, manifests: dict[str, Any]):
        namespace = self.settings.training_namespace
        plans = [
            ("deployment", "Deployment", "apps/v1", "apps"),
            ("service", "Service", "v1", "core"),
        ]
        if "ingress" in manifests:
            plans.append(("ingress", "Ingress", "networking.k8s.io/v1", "networking"))
        owner_keys = ("automl.platform/project-id", "automl.platform/deployment-id")
        try:
            identity = None
            for key, kind, api_version, _ in plans:
                resource = manifests[key]
                metadata = resource["metadata"]
                labels = metadata["labels"]
                owner = tuple(labels[name] for name in owner_keys)
                if (
                    resource["kind"] != kind
                    or resource["apiVersion"] != api_version
                    or not isinstance(metadata["name"], str)
                    or not metadata["name"]
                    or metadata.get("namespace", namespace) != namespace
                    or not isinstance(resource["spec"], dict)
                    or not resource["spec"]
                    or any(str(uuid.UUID(value)) != value for value in owner)
                    or identity is not None
                    and owner != identity
                ):
                    raise ValueError("Inference manifest target or ownership is invalid")
                identity = owner
            deployment, service = manifests["deployment"], manifests["service"]
            labels = deployment["spec"]["template"]["metadata"]["labels"]
            if (
                tuple(labels[name] for name in owner_keys) != identity
                or deployment["spec"]["selector"]["matchLabels"][owner_keys[1]] != identity[1]
                or service["spec"]["selector"][owner_keys[1]] != identity[1]
            ):
                raise ValueError("Inference selectors do not bind the requested deployment")
            if "ingress" in manifests:
                spec = manifests["ingress"]["spec"]
                backends = [
                    path["backend"]
                    for rule in spec.get("rules", [])
                    for path in rule["http"]["paths"]
                ]
                if "defaultBackend" in spec:
                    backends.append(spec["defaultBackend"])
                if not backends or any(
                    backend["service"]["name"] != service["metadata"]["name"]
                    for backend in backends
                ):
                    raise ValueError("Inference ingress does not bind the requested Service")
        except (KeyError, TypeError, AttributeError) as exc:
            raise ValueError("Inference manifest target or ownership is malformed") from exc

        return plans

    def _verify_model_resource(self, existing, resource, kind):
        namespace = self.settings.training_namespace
        if not isinstance(existing, dict):
            with client.ApiClient() as serializer:
                existing = serializer.sanitize_for_serialization(existing)
        if not isinstance(existing, dict) or not isinstance(existing.get("metadata"), dict):
            raise ValueError(f"Existing inference {kind} has malformed ownership or spec")
        metadata = existing["metadata"]
        labels = metadata.get("labels") or {}
        if (
            existing.get("kind") != kind
            or existing.get("apiVersion") != resource["apiVersion"]
            or not isinstance(labels, dict)
            or not isinstance(metadata.get("uid"), str)
            or not metadata["uid"]
            or metadata.get("name") != resource["metadata"]["name"]
            or metadata.get("namespace") != namespace
            or metadata.get("deletionTimestamp") is not None
            or any(
                labels.get(name) != value for name, value in resource["metadata"]["labels"].items()
            )
            or not _requested_inference_spec(existing.get("spec"), resource["spec"])
        ):
            raise ValueError(f"Existing inference {kind} has different ownership or spec")
        if any(
            existing["metadata"].get("annotations", {}).get(key) != value
            for key, value in resource["metadata"].get("annotations", {}).items()
        ):
            raise ValueError(f"Existing inference {kind} has different creation intent")
        return existing

    def create_model_deployment(self, manifests: dict[str, Any], *, custody=None) -> None:
        plans = self._model_deployment_plans(manifests)
        namespace = self.settings.training_namespace
        for key, kind, _, api_name in plans:
            resource = manifests[key]
            api = getattr(self, api_name)
            first = True
            if custody is not None:
                custody.check_namespace()
                first = custody.start(key, resource)
                if first is None:
                    continue
                custody.check_namespace()
            existing = None
            conflicted = False
            if first:
                try:
                    existing = getattr(api, f"create_namespaced_{key}")(
                        namespace=namespace,
                        body=resource,
                        **({"_request_timeout": 5} if custody is not None else {}),
                    )
                except ApiException as exc:
                    if custody is None and exc.status != 409:
                        raise
                    conflicted = True
                except Exception:
                    if custody is None:
                        raise
                else:
                    if custody is not None:
                        existing = self._verify_model_resource(existing, resource, kind)
                        custody.record(key, resource, existing)
            if custody is not None or conflicted:
                existing = getattr(api, f"read_namespaced_{key}")(
                    name=resource["metadata"]["name"],
                    namespace=namespace,
                    **({"_request_timeout": 5} if custody is not None else {}),
                )
                existing = self._verify_model_resource(existing, resource, kind)
            if custody is not None:
                custody.check_namespace()
                custody.record(key, resource, existing)

    def observe_model_deployment(self, name, *, custody, control, deadline):
        def bounded(call, *args, **kwargs):
            if time.monotonic() + 5 > deadline:
                raise TimeoutError("Deployment observation budget exhausted")
            return call(*args, **kwargs)

        bounded(self._lifecycle_plans, name, custody)
        try:
            deployment = bounded(
                self._lifecycle_resource, "deployment", "Deployment", "apps", custody
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
            bounded(self._lifecycle_namespace, custody)
            return "missing", {}
        version = deployment["metadata"].get("resourceVersion")
        if not isinstance(version, str) or not version:
            raise ValueError("Deployment observation requires a native resourceVersion")
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
        if (
            marker != expected
            or type(deployment["spec"].get("replicas")) is not int
            or deployment["spec"]["replicas"] != control["replicas"]
        ):
            raise ValueError("Deployment observation lost its original control generation")
        resources = {"deployment": deployment}
        for key, kind, _, api_name in self._model_deployment_plans(custody["manifests"]):
            if key != "deployment":
                resources[key] = bounded(self._lifecycle_resource, key, kind, api_name, custody)
        selector = (
            "automl.platform/deployment-id="
            + deployment["metadata"]["labels"]["automl.platform/deployment-id"]
        )
        replicas = bounded(
            self.apps.list_namespaced_replica_set,
            namespace=custody["namespace"],
            label_selector=selector,
            limit=100,
            _request_timeout=5,
        )
        pods = bounded(
            self.core.list_namespaced_pod,
            namespace=custody["namespace"],
            label_selector=selector,
            limit=100,
            _request_timeout=5,
        )
        if getattr(replicas.metadata, "_continue", None) or getattr(
            pods.metadata, "_continue", None
        ):
            raise ValueError("Deployment workload observation is incomplete")
        with client.ApiClient() as serializer:
            replica_items = [
                item if isinstance(item, dict) else serializer.sanitize_for_serialization(item)
                for item in replicas.items
            ]
            pod_items = [
                item if isinstance(item, dict) else serializer.sanitize_for_serialization(item)
                for item in pods.items
            ]

        def owned(item, kind, uid):
            metadata = item["metadata"]
            owners = metadata.get("ownerReferences") or []
            return (
                metadata.get("namespace") == custody["namespace"]
                and isinstance(metadata.get("uid"), str)
                and bool(metadata["uid"])
                and len([owner for owner in owners if owner.get("controller") is True]) == 1
                and any(
                    owner.get("controller") is True
                    and owner.get("kind") == kind
                    and owner.get("uid") == uid
                    for owner in owners
                )
            )

        if any(
            len({item["metadata"].get("uid") for item in items}) != len(items)
            for items in (replica_items, pod_items)
        ):
            raise ValueError("Deployment workload observation repeats a native UID")
        replica_uids = set()
        template = custody["manifests"]["deployment"]["spec"]["template"]
        for item in replica_items:
            if not owned(
                item, "Deployment", deployment["metadata"]["uid"]
            ) or not _requested_inference_spec(item["spec"]["template"], template):
                raise ValueError("ReplicaSet does not belong to the original deployment template")
            replica_uids.add(item["metadata"]["uid"])
        for item in pod_items:
            if not any(
                owned(item, "ReplicaSet", uid) for uid in replica_uids
            ) or not _requested_inference_spec(item["spec"], template["spec"]):
                raise ValueError("Pod does not belong to an original deployment ReplicaSet")
        current = bounded(self._lifecycle_resource, "deployment", "Deployment", "apps", custody)
        if current["metadata"].get("resourceVersion") != deployment["metadata"].get(
            "resourceVersion"
        ):
            raise ValueError("Deployment changed during native observation")
        bounded(self._lifecycle_namespace, custody)
        live = [item for item in pod_items if not item["metadata"].get("deletionTimestamp")]
        state = _model_deployment_state(deployment, live)
        if state == "ready":
            generation = deployment["metadata"].get("generation")
            observed = (deployment.get("status") or {}).get("observedGeneration")
            ready = sum(
                any(
                    c.get("type") == "Ready" and c.get("status") == "True"
                    for c in ((item.get("status") or {}).get("conditions") or [])
                )
                for item in live
            )
            if (
                type(generation) is not int
                or type(observed) is not int
                or observed < generation
                or ready < control["replicas"]
            ):
                incomplete = deepcopy(deployment)
                incomplete["status"].update(availableReplicas=0, unavailableReplicas=1)
                state = _model_deployment_state(incomplete, live)
        urls = (
            _model_deployment_urls(resources["service"], resources.get("ingress"), self.settings)
            if state == "ready"
            else {}
        )
        return state, urls

    def _lifecycle_namespace(self, custody):
        if (
            not isinstance(custody, dict)
            or custody.get("namespace") != self.settings.training_namespace
        ):
            raise ValueError("Deployment requires original namespace custody")
        namespace = self.core.read_namespace(custody["namespace"], _request_timeout=5)
        if (
            namespace.metadata.uid != custody.get("namespace_uid")
            or namespace.metadata.name != custody["namespace"]
            or namespace.metadata.deletion_timestamp is not None
        ):
            raise ValueError("Original deployment namespace lifetime changed")

    def _lifecycle_resource(self, key, kind, api_name, custody, *, missing=False, deleting=False):
        resource = custody["manifests"][key]
        receipt = custody["resources"][key]
        if (
            receipt["request_digest"] != canonical_request_hash(resource)
            or receipt["intent_digest"] != custody["intent_digest"]
        ):
            raise ValueError("Original deployment request custody changed")
        try:
            actual = getattr(getattr(self, api_name), f"read_namespaced_{key}")(
                name=resource["metadata"]["name"],
                namespace=custody["namespace"],
                _request_timeout=5,
            )
        except ApiException as exc:
            if missing and exc.status == 404:
                return None
            raise
        if not isinstance(actual, dict):
            with client.ApiClient() as serializer:
                actual = serializer.sanitize_for_serialization(actual)
        normalized = deepcopy(actual)
        if key == "deployment":
            normalized["spec"]["replicas"] = resource["spec"]["replicas"]
        if deleting:
            normalized["metadata"].pop("deletionTimestamp", None)
        normalized = self._verify_model_resource(normalized, resource, kind)
        if (
            normalized["metadata"]["uid"] != receipt["uid"]
            or canonical_request_hash(normalized["spec"]) != receipt["spec_digest"]
        ):
            raise ValueError("Original deployment native UID or spec changed")
        return actual

    def _lifecycle_plans(self, name, custody):
        self._lifecycle_namespace(custody)
        plans = self._model_deployment_plans(custody["manifests"])
        if custody["manifests"]["deployment"]["metadata"]["name"] != name or set(
            custody["resources"]
        ) != {key for key, _, _, _ in plans}:
            raise ValueError("Original deployment lifecycle targets changed")
        return plans

    def _scale_model_deployment(self, name, replicas, custody):
        plans = self._lifecycle_plans(name, custody)
        if type(replicas) is not int or replicas < 0:
            raise ValueError("Deployment replicas must be a nonnegative integer")
        deployment = None
        for key, kind, _, api_name in plans:
            if replicas == 0 and key != "deployment":
                continue
            actual = self._lifecycle_resource(key, kind, api_name, custody)
            if key == "deployment":
                deployment = actual
        metadata = deployment["metadata"]
        version = metadata.get("resourceVersion")
        if not isinstance(version, str) or not version:
            raise ValueError("Original deployment native resourceVersion is missing")
        self._lifecycle_namespace(custody)
        self.apps.patch_namespaced_deployment(
            name=name,
            namespace=custody["namespace"],
            _request_timeout=5,
            body=[
                {"op": "test", "path": "/metadata/uid", "value": metadata["uid"]},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "replace", "path": "/spec/replicas", "value": replicas},
            ],
        )
        self._lifecycle_namespace(custody)

    def start_model_deployment(self, name: str, replicas: int, *, custody=None) -> None:
        self._scale_model_deployment(name, replicas, custody)

    def shutdown_model_deployment(self, name: str, *, custody=None) -> None:
        self._scale_model_deployment(name, 0, custody)

    def delete_model_deployment(self, name: str, *, custody=None, control=None):
        plans = self._lifecycle_plans(name, custody)
        targets = [
            (
                key,
                self._lifecycle_resource(key, kind, api_name, custody, missing=True, deleting=True),
                api_name,
            )
            for key, kind, _, api_name in plans
        ]
        if control is not None:
            deployment = next(actual for key, actual, _ in targets if key == "deployment")
            if deployment is not None:
                annotations = deployment["metadata"].get("annotations") or {}
                if annotations.get("automl.platform/control-generation") != str(
                    control[0]
                ) or annotations.get("automl.platform/control-command") != str(control[1]):
                    raise ValueError("Cleanup lost its native control generation fence")
        for key, actual, api_name in reversed(targets):
            if actual is None:
                continue
            self._lifecycle_namespace(custody)
            receipt = custody["resources"][key]
            try:
                getattr(getattr(self, api_name), f"delete_namespaced_{key}")(
                    name=custody["manifests"][key]["metadata"]["name"],
                    namespace=custody["namespace"],
                    body=client.V1DeleteOptions(
                        preconditions=client.V1Preconditions(
                            uid=receipt["uid"],
                            resource_version=actual["metadata"]["resourceVersion"],
                        ),
                        propagation_policy="Foreground",
                    ),
                    _request_timeout=5,
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise
        for key, kind, _, api_name in plans:
            if (
                self._lifecycle_resource(key, kind, api_name, custody, missing=True, deleting=True)
                is not None
            ):
                raise ModelDeploymentRetirementPending(
                    "Original deployment resources are still retiring"
                )
        selector = (
            "automl.platform/deployment-id="
            + custody["manifests"]["deployment"]["metadata"]["labels"][
                "automl.platform/deployment-id"
            ]
        )
        for method in (self.apps.list_namespaced_replica_set, self.core.list_namespaced_pod):
            remaining = method(
                namespace=custody["namespace"], label_selector=selector, limit=1, _request_timeout=5
            )
            if remaining.items or getattr(remaining.metadata, "_continue", None):
                raise ModelDeploymentRetirementPending(
                    "Original deployment workloads are still retiring"
                )
        self._lifecycle_namespace(custody)
        return dict(
            journal_digest=custody["journal_digest"],
            namespace_uid=custody["namespace_uid"],
            resource_uids={key: value["uid"] for key, value in custody["resources"].items()},
            workload_absent=True,
        )

    def cleanup_finished_jobs(self, project_id: uuid.UUID) -> list[str]:
        deleted: list[str] = []
        jobs = self.batch.list_namespaced_job(
            namespace=self.settings.training_namespace,
            label_selector=(
                f"automl.platform/workload=training,automl.platform/project-id={project_id}"
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

    def delete_job(
        self, job_name: str, namespace: str | None = None, *, uid: str | None = None
    ) -> None:
        self.batch.delete_namespaced_job(
            _request_timeout=5,
            name=job_name,
            namespace=namespace or self.settings.training_namespace,
            body=client.V1DeleteOptions(
                propagation_policy="Foreground",
                preconditions=client.V1Preconditions(uid=uid) if uid is not None else None,
            ),
        )

    def job_state(
        self, job_name: str, *, namespace: str | None = None, uid: str | None = None
    ) -> str:
        try:
            job = self.batch.read_namespaced_job_status(
                name=job_name,
                namespace=namespace or self.settings.training_namespace,
                _request_timeout=5,
            )
        except ApiException as exc:
            if exc.status == 404:
                return "missing"
            raise
        if uid is not None and getattr(job.metadata, "uid", None) != uid:
            raise ValueError("The Kubernetes Job UID changed during observation.")
        if getattr(getattr(job, "metadata", None), "deletion_timestamp", None):
            return "terminating"
        if job.status.succeeded:
            return "succeeded"
        if job.status.failed:
            return "failed"
        pods = self.core.list_namespaced_pod(
            namespace=namespace or self.settings.training_namespace,
            _request_timeout=5,
            label_selector=f"job-name={job_name}",
        ).items
        if uid is not None:
            pods = [
                pod
                for pod in pods
                if any(ref.uid == uid for ref in pod.metadata.owner_references or [])
            ]
        if _container_waiting_failure(pods, _FATAL_CONTAINER_WAITING_REASONS) is not None:
            return "terminal_waiting_failure"
        if _container_waiting_failure(pods, _IMAGE_PULL_BACKOFF_REASONS) is not None:
            return "image_pull_backoff"
        if job.status.active:
            return "running"
        return "queued"

    def job_failure_details(
        self, job_name: str, *, namespace: str | None = None, uid: str | None = None
    ) -> tuple[str, str]:
        try:
            job = self.batch.read_namespaced_job_status(
                name=job_name,
                namespace=namespace or self.settings.training_namespace,
                _request_timeout=5,
            )
            if uid is not None and getattr(job.metadata, "uid", None) != uid:
                raise ValueError("The Kubernetes Job UID changed during observation.")
            for condition in job.status.conditions or []:
                if condition.type == "Failed" and condition.reason == "DeadlineExceeded":
                    return (
                        "JOB_DEADLINE_EXCEEDED",
                        condition.message or "The Kubernetes Job exceeded its active deadline.",
                    )
        except (ApiException, AttributeError):
            pass
        pods = self.core.list_namespaced_pod(
            namespace=namespace or self.settings.training_namespace,
            _request_timeout=5,
            label_selector=f"job-name={job_name}",
        ).items
        if uid is not None:
            pods = [
                pod
                for pod in pods
                if any(ref.uid == uid for ref in pod.metadata.owner_references or [])
            ]
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

        # Candidate-isolated releases fit on workers; older releases fit on the
        # head. Always select the newest generation before preferring its model
        # pod, so a retained predecessor never supplies the displayed telemetry.
        def model_pod_priority(item):
            labels = getattr(item.metadata, "labels", None) or {}
            containers = getattr(item.spec, "containers", None) or []
            isolated = any(
                env.name == "AUTOML_MODEL_PODS" and env.value == "1"
                for container in containers
                for env in (getattr(container, "env", None) or [])
            )
            role = labels.get("ray.io/node-type")
            return (
                int(labels.get("automl.platform/generation", "0")),
                role == ("worker" if isolated else "head"),
                item.metadata.creation_timestamp,
            )

        pod = max(pods, key=model_pod_priority)
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
                cpu_constraints.append((_cpu_cores(hard[key]), _cpu_cores(used.get(key, "0"))))
                break
        for key in ("requests.memory", "limits.memory", "memory"):
            if key in hard:
                memory_constraints.append((_memory_mb(hard[key]), _memory_mb(used.get(key, "0"))))
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


def _pod_resource_requests(spec) -> tuple[float, int]:
    def request(container):
        resources = getattr(container, "resources", None)
        values = getattr(resources, "requests", None) or {}
        return _cpu_cores(values.get("cpu", "0")), _memory_mb(values.get("memory", "0"))

    cpu, memory = 0.0, 0
    for container in spec.containers or []:
        c, m = request(container)
        cpu, memory = cpu + c, memory + m
    sidecar_cpu, sidecar_memory = 0.0, 0
    init_cpu, init_memory = 0.0, 0
    for container in getattr(spec, "init_containers", None) or []:
        c, m = request(container)
        if getattr(container, "restart_policy", None) == "Always":
            sidecar_cpu, sidecar_memory = sidecar_cpu + c, sidecar_memory + m
            init_cpu, init_memory = max(init_cpu, sidecar_cpu), max(init_memory, sidecar_memory)
        else:
            init_cpu, init_memory = (
                max(init_cpu, sidecar_cpu + c),
                max(init_memory, sidecar_memory + m),
            )
    cpu, memory = max(cpu + sidecar_cpu, init_cpu), max(memory + sidecar_memory, init_memory)
    pod_resources = getattr(spec, "resources", None)
    pod_requests = getattr(pod_resources, "requests", None) or {}
    cpu = _cpu_cores(pod_requests["cpu"]) if "cpu" in pod_requests else cpu
    memory = _memory_mb(pod_requests["memory"]) if "memory" in pod_requests else memory
    overhead = getattr(spec, "overhead", None) or {}
    return cpu + _cpu_cores(overhead.get("cpu", "0")), memory + _memory_mb(
        overhead.get("memory", "0")
    )
