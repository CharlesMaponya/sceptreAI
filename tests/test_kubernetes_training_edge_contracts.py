from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import TaskType
from automl_api.schemas.training import ClusterCapacityRead
from automl_api.services import kubernetes_training as kube
from kubernetes.client import ApiException


def _client(settings: Settings | None = None) -> kube.KubernetesTrainingClient:
    value = kube.KubernetesTrainingClient.__new__(kube.KubernetesTrainingClient)
    value.settings = settings or Settings()
    value._configured = True
    value._configuration_error = None
    return value


def _capacity(**overrides: object) -> ClusterCapacityRead:
    values = {
        "connected": True,
        "source": "namespace_resource_quota",
        "total_cpu_cores": 8,
        "requested_cpu_cores": 0,
        "available_cpu_cores": 8,
        "total_memory_mb": 16_384,
        "requested_memory_mb": 0,
        "available_memory_mb": 16_384,
        "ready_nodes": 1,
        "gpu_available": False,
        "active_training_jobs": 0,
        "warnings": [],
    }
    values.update(overrides)
    return ClusterCapacityRead(**values)


def _snapshot(**overrides: object) -> kube.CapacitySnapshot:
    values = {
        "capacity": _capacity(),
        "nodes": [],
        "pvc_ready": True,
        "priority_class_ready": True,
        "runtime_dependencies_ready": True,
    }
    values.update(overrides)
    return kube.CapacitySnapshot(**values)


def _raise(status: int, reason: str = "failure"):
    raise ApiException(status=status, reason=reason)


def test_unconfigured_capacity_is_explicitly_unavailable() -> None:
    client = _client()
    client._configured = False
    client._configuration_error = "no context"
    snapshot = client.capacity_snapshot()
    assert snapshot.capacity.connected is False
    assert snapshot.capacity.warnings == ["no context"]
    assert snapshot.runtime_dependencies_ready is False


def test_capacity_snapshot_handles_quota_and_observer_denials() -> None:
    client = _client(Settings(cluster_observer_enabled=True, gpu_enabled=True))
    pods = [
        SimpleNamespace(status=SimpleNamespace(phase="Running")),
        SimpleNamespace(status=SimpleNamespace(phase="Succeeded")),
    ]
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=pods),
        list_namespaced_resource_quota=lambda **_kw: _raise(403, "Forbidden"),
        list_node=lambda: _raise(403, "Forbidden"),
        read_namespaced_secret=lambda **_kw: object(),
    )
    client._pvc_is_bound = lambda _name: True
    client._priority_class_is_ready = lambda _name: True
    snapshot = client.capacity_snapshot()
    assert snapshot.capacity.active_training_jobs == 1
    assert snapshot.capacity.source == "kubernetes_scheduler"
    assert len(snapshot.capacity.warnings) == 2


def test_capacity_snapshot_discovers_quota_ready_nodes_and_gpu() -> None:
    client = _client(Settings(cluster_observer_enabled=True, gpu_enabled=True))
    quota = SimpleNamespace(
        status=SimpleNamespace(
            hard={"requests.cpu": "4", "requests.memory": "8Gi"},
            used={"requests.cpu": "1", "requests.memory": "2Gi"},
        )
    )
    ready = SimpleNamespace(
        spec=SimpleNamespace(unschedulable=False),
        metadata=SimpleNamespace(name="gpu"),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True")],
            allocatable={"nvidia.com/gpu": "2"},
        ),
    )
    skipped = SimpleNamespace(
        spec=SimpleNamespace(unschedulable=True), status=SimpleNamespace(conditions=[])
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]),
        list_namespaced_resource_quota=lambda **_kw: SimpleNamespace(items=[quota]),
        list_node=lambda: SimpleNamespace(items=[ready, skipped]),
        read_namespaced_secret=lambda **_kw: object(),
    )
    client._pvc_is_bound = lambda _name: True
    client._priority_class_is_ready = lambda _name: True
    snapshot = client.capacity_snapshot()
    assert snapshot.capacity.source == "namespace_resource_quota"
    assert snapshot.capacity.available_cpu_cores == 3
    assert snapshot.capacity.available_memory_mb == 6144
    assert snapshot.nodes[0].gpu_count == 2


def test_capacity_snapshot_iterates_across_multiple_ready_nodes() -> None:
    client = _client(Settings(cluster_observer_enabled=True, gpu_enabled=True))

    def node(name: str, allocatable: dict[str, str]):
        return SimpleNamespace(
            spec=SimpleNamespace(unschedulable=False),
            metadata=SimpleNamespace(name=name),
            status=SimpleNamespace(
                conditions=[SimpleNamespace(type="Ready", status="True")],
                allocatable=allocatable,
            ),
        )

    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]),
        list_namespaced_resource_quota=lambda **_kw: SimpleNamespace(items=[]),
        list_node=lambda: SimpleNamespace(
            items=[node("cpu", {"cpu": "4"}), node("gpu", {"nvidia.com/gpu": "1"})]
        ),
        read_namespaced_secret=lambda **_kw: object(),
    )
    client._pvc_is_bound = lambda _name: True
    client._priority_class_is_ready = lambda _name: True
    snapshot = client.capacity_snapshot()
    assert snapshot.capacity.ready_nodes == 2
    assert [item.name for item in snapshot.nodes] == ["gpu"]


def test_estimate_accumulates_capacity_and_dependency_blockers() -> None:
    client = _client(
        Settings(gpu_enabled=True, max_concurrent_jobs=1, training_memory_limit_mb=1024)
    )
    client.capacity_snapshot = lambda: _snapshot(
        capacity=_capacity(
            connected=False,
            available_cpu_cores=0,
            available_memory_mb=0,
            active_training_jobs=2,
            total_cpu_cores=1,
            total_memory_mb=1024,
        ),
        pvc_ready=False,
        priority_class_ready=False,
        runtime_dependencies_ready=False,
    )
    estimate = client.estimate(
        dataset_bytes=2 * 1024**3,
        dataset_rows=1_000_000,
        column_count=50,
        expected_minutes=1,
        prefer_gpu=True,
        task_type=TaskType.CLUSTERING,
    )
    assert estimate.can_launch is False
    assert any("Kubernetes API" in item for item in estimate.blockers)
    assert any("Secret" in item for item in estimate.blockers)
    assert any("working set" in item for item in estimate.blockers)
    assert any("ResourceQuota" in item for item in estimate.blockers)
    assert any("Concurrent" in item for item in estimate.blockers)
    assert len(estimate.warnings) >= 3


def test_estimate_gpu_vendor_incompatibility_is_actionable() -> None:
    client = _client(Settings(gpu_enabled=True))
    client.capacity_snapshot = lambda: _snapshot(
        capacity=_capacity(gpu_available=True),
        nodes=[kube.NodeCapability("intel", "intel", "gpu.intel.com/xe", 1)],
    )
    estimate = client.estimate(
        dataset_bytes=1,
        column_count=1,
        expected_minutes=1,
        prefer_gpu=True,
        gpu_compatible_vendors={"nvidia"},
    )
    assert estimate.gpu_requested is False
    assert "incompatible" in (estimate.gpu_fallback_reason or "")


def test_create_job_delegates_exact_manifest() -> None:
    client = _client()
    calls: list[dict] = []
    client.batch = SimpleNamespace(create_namespaced_job=lambda **kwargs: calls.append(kwargs))
    manifest = {"kind": "Job"}
    client.create_job(manifest)
    assert calls[0]["body"] is manifest


def test_model_deployment_creation_rolls_back_on_service_or_ingress_failure() -> None:
    client = _client()
    deleted: list[str] = []
    client.apps = SimpleNamespace(
        create_namespaced_deployment=lambda **_kw: None,
        delete_namespaced_deployment=lambda **kw: deleted.append(kw["name"]),
    )
    client.core = SimpleNamespace(
        create_namespaced_service=lambda **_kw: _raise(500),
        delete_namespaced_service=lambda **_kw: _raise(404),
    )
    manifests = {
        "deployment": {"metadata": {"name": "dep"}},
        "service": {"metadata": {"name": "svc"}},
    }
    with pytest.raises(ApiException):
        client.create_model_deployment(manifests)
    assert deleted == ["dep"]


@pytest.mark.parametrize(
    ("waiting", "terminated", "previous", "unavailable", "expected"),
    [
        ("ImagePullBackOff", None, None, 0, "image_pull_error"),
        ("CrashLoopBackOff", None, None, 0, "crash_loop"),
        ("InvalidImageName", None, None, 0, "configuration_error"),
        (None, "OOMKilled", None, 0, "out_of_memory"),
        (None, None, "OOMKilled", 0, "out_of_memory"),
        (None, None, None, 1, "progressing"),
        (None, None, None, 0, "pending"),
    ],
)
def test_model_deployment_state_matrix(
    waiting, terminated, previous, unavailable, expected
) -> None:
    client = _client()
    state = SimpleNamespace(
        waiting=SimpleNamespace(reason=waiting) if waiting else None,
        terminated=SimpleNamespace(reason=terminated) if terminated else None,
    )
    last = (
        SimpleNamespace(terminated=SimpleNamespace(reason=previous))
        if previous
        else SimpleNamespace(terminated=None)
    )
    pod = SimpleNamespace(
        status=SimpleNamespace(container_statuses=[SimpleNamespace(state=state, last_state=last)])
    )
    deployment = SimpleNamespace(
        spec=SimpleNamespace(replicas=1, selector=SimpleNamespace(match_labels={"app": "x"})),
        status=SimpleNamespace(available_replicas=0, unavailable_replicas=unavailable),
    )
    client.apps = SimpleNamespace(read_namespaced_deployment_status=lambda **_kw: deployment)
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod]))
    assert client.model_deployment_state("model") == expected


def test_model_deployment_state_ready_missing_and_api_error() -> None:
    client = _client()
    client.apps = SimpleNamespace(
        read_namespaced_deployment_status=lambda **_kw: SimpleNamespace(
            spec=SimpleNamespace(replicas=1), status=SimpleNamespace(available_replicas=1)
        )
    )
    assert client.model_deployment_state("model") == "ready"
    client.apps = SimpleNamespace(read_namespaced_deployment_status=lambda **_kw: _raise(404))
    assert client.model_deployment_state("model") == "missing"
    client.apps = SimpleNamespace(read_namespaced_deployment_status=lambda **_kw: _raise(500))
    with pytest.raises(ApiException):
        client.model_deployment_state("model")


def _service(service_type: str, *, host: str | None = None, node_port: int | None = None):
    port = SimpleNamespace(name="http", port=8080, node_port=node_port)
    ingress = [SimpleNamespace(hostname=host, ip=None)] if host else []
    return SimpleNamespace(
        spec=SimpleNamespace(ports=[port], type=service_type),
        status=SimpleNamespace(load_balancer=SimpleNamespace(ingress=ingress)),
    )


def test_model_deployment_url_modes() -> None:
    client = _client(Settings(inference_service_type="LoadBalancer"))
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_kw: _service("LoadBalancer", host="model.test")
    )
    assert client.model_deployment_urls("model")["base_url"].endswith(":8080")

    client.settings = Settings(
        inference_service_type="NodePort", inference_external_host="127.0.0.1"
    )
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_kw: _service("NodePort", node_port=30123)
    )
    assert "30123" in client.model_deployment_urls("model")["base_url"]

    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_kw: SimpleNamespace(
            spec=SimpleNamespace(ports=[], type="ClusterIP")
        )
    )
    assert client.model_deployment_urls("model") is None


def test_delete_deployment_ignores_404_and_cleanup_finished_jobs() -> None:
    client = _client(Settings(inference_ingress_enabled=True))
    calls: list[str] = []
    client.networking = SimpleNamespace(delete_namespaced_ingress=lambda **_kw: _raise(404))
    client.core = SimpleNamespace(delete_namespaced_service=lambda **_kw: _raise(404))
    client.apps = SimpleNamespace(
        delete_namespaced_deployment=lambda **kw: calls.append(kw["name"])
    )
    client.delete_model_deployment("model")
    assert calls == ["model"]

    jobs = [
        SimpleNamespace(
            metadata=SimpleNamespace(name="active"), status=SimpleNamespace(succeeded=0, failed=0)
        ),
        SimpleNamespace(
            metadata=SimpleNamespace(name="done"), status=SimpleNamespace(succeeded=1, failed=0)
        ),
        SimpleNamespace(
            metadata=SimpleNamespace(name="gone"), status=SimpleNamespace(succeeded=0, failed=1)
        ),
    ]
    client.batch = SimpleNamespace(list_namespaced_job=lambda **_kw: SimpleNamespace(items=jobs))
    client.delete_job = lambda name: _raise(404) if name == "gone" else calls.append(name)
    assert client.cleanup_finished_jobs(uuid.uuid4()) == ["done", "gone"]


@pytest.mark.parametrize(
    ("status_values", "expected"),
    [
        ({"succeeded": 1, "failed": 0, "active": 0}, "succeeded"),
        ({"succeeded": 0, "failed": 1, "active": 0}, "failed"),
        ({"succeeded": 0, "failed": 0, "active": 1}, "running"),
        ({"succeeded": 0, "failed": 0, "active": 0}, "queued"),
    ],
)
def test_job_state_terminal_and_active_paths(status_values, expected) -> None:
    client = _client()
    client.batch = SimpleNamespace(
        read_namespaced_job_status=lambda **_kw: SimpleNamespace(
            status=SimpleNamespace(**status_values)
        )
    )
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]))
    assert client.job_state("job") == expected
    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: _raise(404))
    assert client.job_state("job") == "missing"


def test_job_failure_details_handles_no_pod_eviction_and_waiting() -> None:
    client = _client()
    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: _raise(500))
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]))
    assert client.job_failure_details("job")[0] == "KUBERNETES_JOB_FAILED"

    waiting = SimpleNamespace(reason="CreateContainerError", message="bad config")
    status = SimpleNamespace(name="trainer", image="image", state=SimpleNamespace(waiting=waiting))
    pod = SimpleNamespace(
        status=SimpleNamespace(
            phase="Pending", init_container_statuses=[], container_statuses=[status]
        )
    )
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod]))
    assert client.job_failure_details("job")[0] == "TRAINING_CONTAINER_START_FAILED"

    pod.status = SimpleNamespace(
        phase="Failed",
        reason="Evicted",
        message="pressure",
        container_statuses=[],
        conditions=[SimpleNamespace(status="False", message="unschedulable")],
        init_container_statuses=[],
    )
    assert client.job_failure_details("job")[0] == "POD_EVICTED"


def test_job_logs_plain_literal_invalid_and_empty() -> None:
    client = _client()
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod"))
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]),
        read_namespaced_pod_log=lambda **_kw: "unused",
    )
    assert client.job_logs(uuid.uuid4()) == []
    client.core.list_namespaced_pod = lambda **_kw: SimpleNamespace(items=[pod])
    client.core.read_namespaced_pod_log = lambda **_kw: "b'one\\ntwo'"
    assert client.job_logs(uuid.uuid4()) == ["one", "two"]
    client.core.read_namespaced_pod_log = lambda **_kw: "b'broken"
    assert client.job_logs(uuid.uuid4()) == ["b'broken"]


def test_training_resource_usage_unavailable_empty_metrics_and_failure() -> None:
    client = _client()
    client._configured = False
    client._configuration_error = "offline"
    assert client.training_resource_usage(uuid.uuid4())["status_reason"] == "offline"
    client._configured = True
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]))
    assert "no longer" in client.training_resource_usage(uuid.uuid4())["status_reason"]

    state = SimpleNamespace(waiting=SimpleNamespace(reason="Starting"), terminated=None)
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="pod", creation_timestamp=datetime.now(UTC)),
        spec=SimpleNamespace(node_name="node"),
        status=SimpleNamespace(
            phase="Pending",
            reason=None,
            container_statuses=[SimpleNamespace(restart_count=2, state=state)],
            conditions=[
                SimpleNamespace(
                    type="PodScheduled", status="False", reason="Unschedulable", message="no cpu"
                )
            ],
        ),
    )
    client.core = SimpleNamespace(list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod]))
    client.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda **_kw: {
            "containers": [{"usage": {"cpu": "250m", "memory": "128Mi"}}, {"usage": {}}]
        }
    )
    usage = client.training_resource_usage(uuid.uuid4())
    assert usage["telemetry_available"] is True
    assert usage["cpu_usage_cores"] == 0.25 and usage["memory_usage_mb"] == 128
    assert "no cpu" in usage["status_reason"]

    client.custom = SimpleNamespace(get_namespaced_custom_object=lambda **_kw: _raise(404))
    assert client.training_resource_usage(uuid.uuid4())["telemetry_available"] is False


def test_resource_existence_helpers_fail_closed() -> None:
    client = _client()
    client.core = SimpleNamespace(
        read_namespaced_persistent_volume_claim=lambda **_kw: SimpleNamespace(
            status=SimpleNamespace(phase="Bound")
        ),
        read_namespaced_secret=lambda **_kw: object(),
        read_namespaced_service=lambda **_kw: object(),
    )
    client.scheduling = SimpleNamespace(read_priority_class=lambda _name: object())
    assert client._pvc_is_bound("pvc") and client._priority_class_is_ready("priority")
    assert client._secret_exists("secret") and client._service_exists("service")
    client.core = SimpleNamespace(
        read_namespaced_persistent_volume_claim=lambda **_kw: _raise(404),
        read_namespaced_secret=lambda **_kw: _raise(404),
        read_namespaced_service=lambda **_kw: _raise(404),
    )
    client.scheduling = SimpleNamespace(read_priority_class=lambda _name: _raise(404))
    assert not client._pvc_is_bound("pvc") and not client._priority_class_is_ready("priority")
    assert not client._secret_exists("secret") and not client._service_exists("service")


def test_low_level_node_quota_and_waiting_helpers() -> None:
    not_ready = SimpleNamespace(status=SimpleNamespace(conditions=None))
    assert kube._node_is_ready(not_ready) is False
    assert kube._node_gpu({"nvidia.com/gpu": "1"}, {"nvidia.com/gpu": 1}) == (None, None, 0)
    assert kube._namespace_quota_capacity([]) is None
    incomplete = SimpleNamespace(status=SimpleNamespace(hard={"requests.cpu": "1"}, used={}))
    assert kube._namespace_quota_capacity([incomplete]) is None

    finished = SimpleNamespace(status=SimpleNamespace(phase="Failed"))
    assert kube._container_waiting_failure([finished], {"ImagePullBackOff"}) is None
    details = kube._container_waiting_failure_details(
        "InvalidImageName", SimpleNamespace(image=None), SimpleNamespace(message="invalid")
    )
    assert details[0] == "TRAINING_IMAGE_INVALID"
    details = kube._container_waiting_failure_details(
        "CreateContainerConfigError", SimpleNamespace(image="image"), SimpleNamespace(message=None)
    )
    assert details[0] == "TRAINING_CONTAINER_CONFIG_INVALID"


def test_deadline_helper_obeys_floor_multiplier_and_ceiling() -> None:
    settings = Settings(
        training_active_deadline_seconds=100,
        training_max_active_deadline_seconds=1000,
        training_deadline_multiplier=2,
    )
    assert kube._active_deadline_seconds(1, settings) == 120
    assert kube._active_deadline_seconds(100, settings) == 1000


@pytest.mark.parametrize(
    ("settings", "vendors", "capacity", "expected"),
    [
        (Settings(gpu_enabled=False), {"nvidia"}, _capacity(), "GPU_ENABLED is false"),
        (Settings(gpu_enabled=True), set(), _capacity(gpu_available=True), "do not expose"),
        (Settings(gpu_enabled=True), {"nvidia"}, _capacity(), "No schedulable node"),
    ],
)
def test_estimate_explains_each_gpu_fallback(settings, vendors, capacity, expected) -> None:
    client = _client(settings)
    client.capacity_snapshot = lambda: _snapshot(capacity=capacity)
    estimate = client.estimate(
        dataset_bytes=1,
        column_count=1,
        expected_minutes=1,
        prefer_gpu=True,
        gpu_compatible_vendors=vendors,
    )
    assert expected in (estimate.gpu_fallback_reason or "")


def test_capacity_warns_when_gpu_observer_is_disabled() -> None:
    client = _client(Settings(gpu_enabled=True, cluster_observer_enabled=False))
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[]),
        list_namespaced_resource_quota=lambda **_kw: SimpleNamespace(items=[]),
        read_namespaced_secret=lambda **_kw: object(),
    )
    client._pvc_is_bound = lambda _name: True
    snapshot = client.capacity_snapshot()
    assert any("observer is not enabled" in item for item in snapshot.capacity.warnings)


def test_estimate_rejects_minimum_cpu_and_memory_requests() -> None:
    client = _client(
        Settings(
            training_cpu_request_cores=0.1,
            training_cpu_limit_cores=0.1,
            training_memory_request_mb=128,
            training_memory_limit_mb=128,
        )
    )
    client.capacity_snapshot = lambda: _snapshot()
    estimate = client.estimate(
        dataset_bytes=0, column_count=0, expected_minutes=1, prefer_gpu=False
    )
    assert any("at least 0.25" in item for item in estimate.blockers)
    assert any("at least 256" in item for item in estimate.blockers)


def test_build_job_manifest_optional_storage_gpu_and_ttl_contract() -> None:
    settings = Settings(
        dataset_cache_pvc_name="cache",
        training_priority_class_name="batch-low",
        workload_image_pull_secrets=("registry",),
        training_job_ttl_seconds=0,
    )
    client = _client(settings)
    estimate = kube.TrainingEstimateRead(
        capacity=_capacity(gpu_available=True),
        estimated_working_set_mb=512,
        cpu_request_cores=1,
        cpu_limit_cores=2,
        memory_request_mb=512,
        memory_limit_mb=1024,
        gpu_requested=True,
        gpu_vendor="nvidia",
        gpu_resource="nvidia.com/gpu",
        expected_minutes=1,
        active_deadline_seconds=120,
        estimated_core_hours=0.1,
        max_concurrent_jobs=1,
        can_launch=True,
    )
    manifest = client.build_job_manifest(
        run_id=uuid.uuid4(), project_id=uuid.uuid4(), estimate=estimate
    )
    spec = manifest["spec"]
    pod = spec["template"]["spec"]
    container = pod["containers"][0]
    assert "ttlSecondsAfterFinished" not in spec
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "cache"
    assert pod["priorityClassName"] == "batch-low"
    assert pod["imagePullSecrets"] == [{"name": "registry"}]
    assert container["resources"]["requests"]["nvidia.com/gpu"] == "1"
    assert {item["name"] for item in container["env"]} >= {"CUML_ACCEL_ENABLED"}


def test_model_manifest_and_creation_include_ingress_tls() -> None:
    client = _client(
        Settings(
            inference_ingress_enabled=True,
            inference_ingress_host_template="{name}.example.test",
            inference_ingress_class_name="traefik",
            inference_ingress_tls_secret_name="tls",
        )
    )
    manifests = client.build_model_deployment_manifest(
        deployment_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        project_name="Project",
        environment="staging",
        model_uri="minio://models/model.joblib",
        model_name="qualified",
        image="model:local",
        replicas=1,
        cpu_request="250m",
        memory_request="256Mi",
    )
    assert manifests["ingress"]["spec"]["ingressClassName"] == "traefik"
    assert manifests["ingress"]["spec"]["tls"][0]["secretName"] == "tls"
    calls: list[str] = []
    client.apps = SimpleNamespace(
        create_namespaced_deployment=lambda **_kw: calls.append("deployment")
    )
    client.core = SimpleNamespace(
        create_namespaced_service=lambda **_kw: calls.append("service")
    )
    client.networking = SimpleNamespace(
        create_namespaced_ingress=lambda **_kw: calls.append("ingress")
    )
    client.create_model_deployment(manifests)
    assert calls == ["deployment", "service", "ingress"]


def test_model_deployment_urls_ingress_and_error_paths() -> None:
    client = _client(
        Settings(
            inference_ingress_enabled=True,
            inference_ingress_tls_secret_name="tls",
        )
    )
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_kw: _service("ClusterIP")
    )
    ingress = SimpleNamespace(
        status=SimpleNamespace(load_balancer=SimpleNamespace(ingress=[object()])),
        spec=SimpleNamespace(rules=[SimpleNamespace(host="model.example.test")]),
    )
    client.networking = SimpleNamespace(
        read_namespaced_ingress_status=lambda **_kw: ingress
    )
    assert client.model_deployment_urls("model")["base_url"] == "https://model.example.test"
    client.networking.read_namespaced_ingress_status = lambda **_kw: _raise(404)
    assert client.model_deployment_urls("model") is None
    client.networking.read_namespaced_ingress_status = lambda **_kw: _raise(500)
    with pytest.raises(ApiException):
        client.model_deployment_urls("model")


def test_delete_and_cleanup_propagate_non_not_found_errors() -> None:
    client = _client(Settings(inference_ingress_enabled=True))
    client.networking = SimpleNamespace(
        delete_namespaced_ingress=lambda **_kw: _raise(500)
    )
    with pytest.raises(ApiException):
        client.delete_model_deployment("model")

    client = _client()
    job = SimpleNamespace(
        metadata=SimpleNamespace(name="done"),
        status=SimpleNamespace(succeeded=1, failed=0),
    )
    client.batch = SimpleNamespace(
        list_namespaced_job=lambda **_kw: SimpleNamespace(items=[job])
    )
    client.delete_job = lambda _name: _raise(500)
    with pytest.raises(ApiException):
        client.cleanup_finished_jobs(uuid.uuid4())


def test_job_state_detects_fatal_and_retriable_waiting_states() -> None:
    client = _client()
    job = SimpleNamespace(status=SimpleNamespace(succeeded=0, failed=0, active=0))
    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: job)

    def pod(reason: str):
        waiting = SimpleNamespace(reason=reason, message="detail")
        status = SimpleNamespace(state=SimpleNamespace(waiting=waiting), image="image")
        return SimpleNamespace(
            status=SimpleNamespace(
                phase="Pending", init_container_statuses=[], container_statuses=[status]
            )
        )

    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod("InvalidImageName")])
    )
    assert client.job_state("job") == "terminal_waiting_failure"
    client.core.list_namespaced_pod = lambda **_kw: SimpleNamespace(
        items=[pod("ImagePullBackOff")]
    )
    assert client.job_state("job") == "image_pull_backoff"


def test_job_failure_details_deadline_oom_waiting_and_conditions() -> None:
    client = _client()
    deadline = SimpleNamespace(
        status=SimpleNamespace(
            conditions=[
                SimpleNamespace(
                    type="Failed", reason="DeadlineExceeded", message=None
                )
            ]
        )
    )
    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: deadline)
    assert client.job_failure_details("job")[0] == "JOB_DEADLINE_EXCEEDED"

    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: _raise(500))
    terminated = SimpleNamespace(reason=None, exit_code=137)
    waiting = SimpleNamespace(reason="Waiting", message="why")
    pod = SimpleNamespace(
        status=SimpleNamespace(
            reason="Failed",
            message="pod failed",
            init_container_statuses=[],
            container_statuses=[
                SimpleNamespace(
                    name="trainer",
                    state=SimpleNamespace(terminated=terminated, waiting=None),
                ),
                SimpleNamespace(
                    name="sidecar",
                    state=SimpleNamespace(terminated=None, waiting=waiting),
                ),
            ],
            conditions=[SimpleNamespace(status="False", message="condition")],
        )
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod])
    )
    code, message = client.job_failure_details("job")
    assert code == "POD_OOM_KILLED"
    assert all(item in message for item in ("exit code 137", "why", "condition"))


def test_job_logs_decode_bytes_and_resource_usage_terminated_reason() -> None:
    client = _client()
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod"))
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod]),
        read_namespaced_pod_log=lambda **_kw: b"one\ntwo",
    )
    assert client.job_logs(uuid.uuid4()) == ["one", "two"]

    terminated = SimpleNamespace(reason="Completed")
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="pod", creation_timestamp=datetime.now(UTC)),
        spec=SimpleNamespace(node_name="node"),
        status=SimpleNamespace(
            phase="Succeeded",
            reason=None,
            container_statuses=[
                SimpleNamespace(
                    restart_count=0,
                    state=SimpleNamespace(waiting=None, terminated=terminated),
                )
            ],
            conditions=[],
        ),
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod])
    )
    client.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda **_kw: {"containers": []}
    )
    assert client.training_resource_usage(uuid.uuid4())["status_reason"] == "Completed"


def test_non_not_found_api_errors_propagate_and_ingress_delete_is_optional() -> None:
    client = _client(Settings(inference_ingress_enabled=False))
    deleted: list[str] = []
    client.core = SimpleNamespace(
        delete_namespaced_service=lambda **_kw: deleted.append("service")
    )
    client.apps = SimpleNamespace(
        delete_namespaced_deployment=lambda **_kw: deleted.append("deployment")
    )
    client.delete_model_deployment("model")
    assert deleted == ["service", "deployment"]

    client.core = SimpleNamespace(delete_namespaced_service=lambda **_kw: _raise(500))
    with pytest.raises(ApiException):
        client.delete_model_deployment("model")

    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: _raise(500))
    with pytest.raises(ApiException):
        client.job_state("job")


def test_failure_details_skips_non_deadline_condition_and_default_message() -> None:
    client = _client()
    job = SimpleNamespace(
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Complete", reason="Done", message=None)]
        )
    )
    client.batch = SimpleNamespace(read_namespaced_job_status=lambda **_kw: job)
    pod = SimpleNamespace(
        status=SimpleNamespace(
            reason=None,
            message=None,
            init_container_statuses=[],
            container_statuses=[],
            conditions=[],
        )
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod])
    )
    assert client.job_failure_details("job") == (
        "KUBERNETES_JOB_FAILED",
        "Kubernetes Job failed.",
    )


def test_resource_usage_handles_waiting_without_reason_and_unscheduled_without_message() -> None:
    client = _client()
    statuses = [
        SimpleNamespace(
            restart_count=None,
            state=SimpleNamespace(
                waiting=SimpleNamespace(reason=None), terminated=None
            ),
        )
    ]
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="pod", creation_timestamp=datetime.now(UTC)),
        spec=SimpleNamespace(node_name=None),
        status=SimpleNamespace(
            phase="Pending",
            reason=None,
            container_statuses=statuses,
            conditions=[
                SimpleNamespace(
                    type="PodScheduled", status="False", reason=None, message=None
                )
            ],
        ),
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod])
    )
    client.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda **_kw: {"containers": []}
    )
    usage = client.training_resource_usage(uuid.uuid4())
    assert usage["status_reason"] == "Unschedulable"
    assert usage["restart_count"] == 0


def test_quota_parser_accepts_limit_keys_and_gpu_skips_exhausted_resource() -> None:
    quota = SimpleNamespace(
        status=SimpleNamespace(
            hard={"limits.cpu": "4", "limits.memory": "8Gi"},
            used={"limits.cpu": "1", "limits.memory": "2Gi"},
        )
    )
    assert kube._namespace_quota_capacity([quota]) == (
        4.0,
        1.0,
        3.0,
        8192,
        2048,
        6144,
    )
    assert kube._node_gpu(
        {"nvidia.com/gpu": "1", "gpu.intel.com/xe": "2"},
        {"nvidia.com/gpu": 1},
    ) == ("intel", "gpu.intel.com/xe", 2)


def test_manifest_creation_without_ingress_and_tls_without_class() -> None:
    client = _client()
    calls: list[str] = []
    client.apps = SimpleNamespace(
        create_namespaced_deployment=lambda **_kw: calls.append("deployment")
    )
    client.core = SimpleNamespace(
        create_namespaced_service=lambda **_kw: calls.append("service")
    )
    client.create_model_deployment(
        {
            "deployment": {"metadata": {"name": "model"}},
            "service": {"metadata": {"name": "model"}},
        }
    )
    assert calls == ["deployment", "service"]

    client = _client(
        Settings(
            inference_ingress_enabled=True,
            inference_ingress_host_template="{name}.example.test",
            inference_ingress_class_name=None,
            inference_ingress_tls_secret_name="tls",
        )
    )
    manifests = client.build_model_deployment_manifest(
        deployment_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        project_name="Project",
        environment="staging",
        model_name="qualified",
        model_uri="minio://models/model.joblib",
        image="model:local",
        replicas=1,
        cpu_request="250m",
        memory_request="256Mi",
    )
    assert "ingressClassName" not in manifests["ingress"]["spec"]
    assert manifests["ingress"]["spec"]["tls"]


def test_external_url_requires_admitted_host() -> None:
    client = _client(Settings(inference_service_type="LoadBalancer"))
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_kw: _service("LoadBalancer")
    )
    assert client.model_deployment_urls("model") is None
    service = _service("LoadBalancer", host="ignored")
    service.status.load_balancer.ingress[0].hostname = None
    service.status.load_balancer.ingress[0].ip = None
    client.core.read_namespaced_service = lambda **_kw: service
    assert client.model_deployment_urls("model") is None


def test_failure_detail_and_log_branch_fallthroughs(monkeypatch) -> None:
    client = _client()
    client.batch = SimpleNamespace(
        read_namespaced_job_status=lambda **_kw: SimpleNamespace(
            status=SimpleNamespace(conditions=[])
        )
    )
    ordinary = SimpleNamespace(reason="Error", exit_code=1)
    pod = SimpleNamespace(
        status=SimpleNamespace(
            reason=None,
            message=None,
            init_container_statuses=[],
            container_statuses=[
                SimpleNamespace(
                    name="trainer",
                    state=SimpleNamespace(terminated=ordinary, waiting=None),
                ),
                SimpleNamespace(
                    name="empty",
                    state=SimpleNamespace(terminated=None, waiting=None),
                ),
            ],
            conditions=[
                SimpleNamespace(status="True", message="not an error"),
                SimpleNamespace(status="False", message=None),
            ],
        )
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod]),
        read_namespaced_pod_log=lambda **_kw: "ordinary log",
    )
    code, message = client.job_failure_details("job")
    assert code == "KUBERNETES_JOB_FAILED" and "exit code 1" in message
    pod.metadata = SimpleNamespace(name="pod")
    assert client.job_logs(uuid.uuid4()) == ["ordinary log"]
    client.core.read_namespaced_pod_log = lambda **_kw: "123"
    assert client.job_logs(uuid.uuid4()) == ["123"]
    client.core.read_namespaced_pod_log = lambda **_kw: "b'encoded'"
    monkeypatch.setattr(kube.ast, "literal_eval", lambda _value: "not bytes")
    assert client.job_logs(uuid.uuid4()) == ["b'encoded'"]


def test_resource_usage_ignores_unrelated_conditions_and_partial_quota() -> None:
    client = _client()
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="pod", creation_timestamp=datetime.now(UTC)),
        spec=SimpleNamespace(node_name="node"),
        status=SimpleNamespace(
            phase="Running",
            reason=None,
            container_statuses=[],
            conditions=[SimpleNamespace(type="Ready", status="True", reason=None, message=None)],
        ),
    )
    client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_kw: SimpleNamespace(items=[pod])
    )
    client.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda **_kw: {"containers": []}
    )
    assert client.training_resource_usage(uuid.uuid4())["status_reason"] is None

    memory_only = SimpleNamespace(
        status=SimpleNamespace(hard={"memory": "1Gi"}, used={"memory": "0"})
    )
    assert kube._namespace_quota_capacity([memory_only]) is None
