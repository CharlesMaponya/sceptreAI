from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from automl_api.core.config import Settings
from automl_api.models.enums import TaskType
from automl_api.schemas.training import (
    ClusterCapacityRead,
    TrainingEstimateRequest,
)
from automl_api.services.kubernetes_training import (
    CapacitySnapshot,
    KubernetesTrainingClient,
    NodeCapability,
    _cpu_cores,
    _memory_mb,
    _namespace_quota_capacity,
    _node_gpu,
)
from kubernetes.client import ApiException
from pydantic import ValidationError


class FakeTrainingClient(KubernetesTrainingClient):
    def __init__(self, snapshot: CapacitySnapshot, settings: Settings) -> None:
        self.snapshot = snapshot
        self.settings = settings

    def capacity_snapshot(self) -> CapacitySnapshot:
        return self.snapshot


def capacity_snapshot() -> CapacitySnapshot:
    return CapacitySnapshot(
        capacity=ClusterCapacityRead(
            connected=True,
            source="test",
            total_cpu_cores=8,
            requested_cpu_cores=2,
            available_cpu_cores=6,
            total_memory_mb=16_000,
            requested_memory_mb=4_000,
            available_memory_mb=12_000,
            ready_nodes=1,
            gpu_available=False,
            active_training_jobs=0,
        ),
        nodes=[
            NodeCapability(
                name="worker",
            )
        ],
        pvc_ready=True,
        priority_class_ready=True,
        runtime_dependencies_ready=True,
    )


def test_kubernetes_quantities_are_normalized() -> None:
    assert _cpu_cores("500m") == 0.5
    assert _memory_mb("2Gi") == 2048
    assert _node_gpu({"nvidia.com/gpu": "1"}) == ("nvidia", "nvidia.com/gpu", 1)
    assert _node_gpu({"gpu.intel.com/i915": "2"}) == ("intel", "gpu.intel.com/i915", 2)


def test_default_estimate_uses_configured_resources_without_pinning_a_node() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())

    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**2,
        column_count=10,
        expected_minutes=10,
        prefer_gpu=False,
    )

    assert estimate.cpu_request_cores == 1
    assert estimate.cpu_limit_cores == 2
    assert estimate.memory_request_mb == 1024
    assert estimate.memory_limit_mb == 4096
    assert estimate.selected_node is None


def test_estimate_enforces_configured_limit_and_gpu_fallback() -> None:
    settings = Settings(
        gpu_enabled=True,
    )
    training_client = FakeTrainingClient(capacity_snapshot(), settings)

    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**3,
        column_count=500,
        expected_minutes=15,
        prefer_gpu=True,
    )

    assert estimate.cpu_request_cores == 1
    assert estimate.memory_request_mb == 4096
    assert not estimate.gpu_requested
    assert "No schedulable node" in (estimate.gpu_fallback_reason or "")


def test_training_manifest_has_optional_priority_ephemeral_cache_and_timeout() -> None:
    settings = Settings(
        training_image="automl-training:test",
        training_priority_class_name="automl-low",
        training_job_ttl_seconds=30,
        workload_image_pull_secrets=("registry-one", "registry-two"),
    )
    training_client = FakeTrainingClient(capacity_snapshot(), settings)
    estimate = training_client.estimate(
        dataset_bytes=50 * 1024**2,
        column_count=20,
        expected_minutes=10,
        prefer_gpu=False,
    )

    manifest = training_client.build_job_manifest(
        run_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        project_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        estimate=estimate,
    )
    pod_spec = manifest["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    resources = container["resources"]

    assert pod_spec["priorityClassName"] == "automl-low"
    assert manifest["spec"]["activeDeadlineSeconds"] == 21_600
    assert resources["requests"]["cpu"] == str(estimate.cpu_request_cores)
    assert resources["limits"]["memory"] == f"{estimate.memory_limit_mb}Mi"
    assert "nodeSelector" not in pod_spec
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["imagePullSecrets"] == [
        {"name": "registry-one"},
        {"name": "registry-two"},
    ]
    assert pod_spec["volumes"][0] == {
        "name": "dataset-cache",
        "emptyDir": {"sizeLimit": "5Gi"},
    }
    assert {
        "name": "TRAINING_EXECUTION_MODE",
        "value": "direct",
    } in container["env"]
    assert manifest["spec"]["backoffLimit"] == 0
    assert manifest["spec"]["ttlSecondsAfterFinished"] == 30
    assert {"name": "MLFLOW_ENABLE_ASYNC_LOGGING", "value": "false"} in container["env"]


@pytest.mark.parametrize(
    ("vendor", "resource"),
    [
        ("nvidia", "nvidia.com/gpu"),
        ("intel", "gpu.intel.com/xe"),
        ("intel", "gpu.intel.com/i915"),
    ],
)
def test_gpu_vendor_and_resource_are_propagated_to_training_job(
    vendor: str,
    resource: str,
) -> None:
    snapshot = capacity_snapshot()
    snapshot = CapacitySnapshot(
        capacity=snapshot.capacity.model_copy(update={"gpu_available": True}),
        nodes=[
            NodeCapability(
                name="gpu-worker",
                gpu_vendor=vendor,
                gpu_resource=resource,
                gpu_count=1,
            )
        ],
        pvc_ready=True,
        priority_class_ready=True,
        runtime_dependencies_ready=True,
    )
    training_client = FakeTrainingClient(snapshot, Settings(gpu_enabled=True))

    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**2,
        column_count=10,
        expected_minutes=10,
        prefer_gpu=True,
    )
    manifest = training_client.build_job_manifest(
        run_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        estimate=estimate,
    )
    container = manifest["spec"]["template"]["spec"]["containers"][0]

    assert estimate.gpu_requested
    assert estimate.gpu_vendor == vendor
    assert estimate.gpu_resource == resource
    assert container["resources"]["requests"][resource] == "1"
    assert container["resources"]["limits"][resource] == "1"
    assert {"name": "AUTOML_GPU_VENDOR", "value": vendor} in container["env"]
    if vendor == "nvidia":
        assert {"name": "CUML_ACCEL_ENABLED", "value": "1"} in container["env"]
    assert {"name": "shared-memory", "mountPath": "/dev/shm"} in container["volumeMounts"]


def test_cpu_only_estimators_do_not_reserve_an_available_gpu() -> None:
    snapshot = capacity_snapshot()
    snapshot = CapacitySnapshot(
        capacity=snapshot.capacity.model_copy(update={"gpu_available": True}),
        nodes=[
            NodeCapability(
                name="gpu-worker",
                gpu_vendor="nvidia",
                gpu_resource="nvidia.com/gpu",
                gpu_count=1,
            )
        ],
        pvc_ready=True,
        priority_class_ready=True,
        runtime_dependencies_ready=True,
    )
    training_client = FakeTrainingClient(snapshot, Settings(gpu_enabled=True))

    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**2,
        column_count=10,
        expected_minutes=10,
        prefer_gpu=True,
        gpu_compatible_vendors=set(),
    )

    assert not estimate.gpu_requested
    assert "selected estimators" in (estimate.gpu_fallback_reason or "")


def test_memory_estimate_scales_with_dataset_and_search_budget() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())

    small = training_client.estimate(
        dataset_bytes=10 * 1024**2,
        dataset_rows=10_000,
        column_count=10,
        task_type=TaskType.CLASSIFICATION,
        candidate_limit=3,
        optimization_iterations=2,
        expected_minutes=5,
        prefer_gpu=False,
    )
    large = training_client.estimate(
        dataset_bytes=500 * 1024**2,
        dataset_rows=1_000_000,
        column_count=50,
        task_type=TaskType.CLASSIFICATION,
        candidate_limit=8,
        optimization_iterations=15,
        expected_minutes=30,
        prefer_gpu=False,
    )

    assert large.estimated_working_set_mb > small.estimated_working_set_mb
    assert small.memory_request_mb == 1024
    assert large.memory_request_mb == 4096
    assert not large.can_launch


def test_training_request_accepts_up_to_twenty_models() -> None:
    request = TrainingEstimateRequest(
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.CLASSIFICATION,
        candidate_limit=20,
        candidate_models=[f"Model{index}" for index in range(20)],
    )

    assert request.candidate_limit == 20
    assert len(request.candidate_models) == 20
    with pytest.raises(ValidationError):
        TrainingEstimateRequest(
            dataset_version_id=uuid.uuid4(),
            task_type=TaskType.CLASSIFICATION,
            candidate_limit=21,
            candidate_models=[f"Model{index}" for index in range(21)],
        )


def test_job_deadline_scales_beyond_the_six_hour_floor() -> None:
    settings = Settings(
        training_active_deadline_seconds=21_600,
        training_max_active_deadline_seconds=86_400,
        training_deadline_multiplier=6,
    )
    training_client = FakeTrainingClient(capacity_snapshot(), settings)
    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**2,
        column_count=10,
        expected_minutes=120,
        prefer_gpu=False,
    )

    manifest = training_client.build_job_manifest(
        run_id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        estimate=estimate,
    )

    assert manifest["spec"]["activeDeadlineSeconds"] == 43_200


def test_oom_failure_is_reported_from_pod_termination_state() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())
    terminated = SimpleNamespace(reason="OOMKilled", exit_code=137)
    container_status = SimpleNamespace(
        name="trainer",
        state=SimpleNamespace(terminated=terminated, waiting=None),
    )
    pod = SimpleNamespace(
        status=SimpleNamespace(
            reason=None,
            message=None,
            container_statuses=[container_status],
            conditions=[],
        )
    )
    training_client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[pod])
    )

    code, message = training_client.job_failure_details("automl-train-test")

    assert code == "POD_OOM_KILLED"
    assert "exit code 137" in message


def test_deadline_failure_is_reported_from_job_condition() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())
    condition = SimpleNamespace(
        type="Failed",
        reason="DeadlineExceeded",
        message="Job was active longer than specified deadline.",
    )
    training_client.batch = SimpleNamespace(
        read_namespaced_job_status=lambda **_: SimpleNamespace(
            status=SimpleNamespace(conditions=[condition])
        )
    )

    code, message = training_client.job_failure_details("automl-train-test")

    assert code == "JOB_DEADLINE_EXCEEDED"
    assert "deadline" in message


def test_byte_literal_pod_logs_are_split_into_lines() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())
    pod = SimpleNamespace(metadata=SimpleNamespace(name="trainer-pod"))
    training_client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[pod]),
        read_namespaced_pod_log=lambda **_: "b'first line\\nsecond line\\n'",
    )

    lines = training_client.job_logs(uuid.uuid4())

    assert lines == ["first line", "second line"]


def test_namespace_quota_capacity_uses_strictest_available_constraint() -> None:
    quotas = [
        SimpleNamespace(
            status=SimpleNamespace(
                hard={"requests.cpu": "8", "requests.memory": "16Gi"},
                used={"requests.cpu": "3", "requests.memory": "4Gi"},
            )
        ),
        SimpleNamespace(
            status=SimpleNamespace(
                hard={"limits.cpu": "6", "limits.memory": "10Gi"},
                used={"limits.cpu": "2", "limits.memory": "3Gi"},
            )
        ),
    ]

    assert _namespace_quota_capacity(quotas) == (6.0, 2.0, 4.0, 10_240, 3072, 7168)


def test_capacity_snapshot_uses_namespace_scope_without_node_access() -> None:
    training_client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    training_client._configured = True
    training_client.settings = Settings(
        training_namespace="team-a",
        gpu_enabled=False,
    )
    quota = SimpleNamespace(
        status=SimpleNamespace(
            hard={"requests.cpu": "8", "requests.memory": "16Gi"},
            used={"requests.cpu": "2", "requests.memory": "4Gi"},
        )
    )
    training_client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[]),
        list_namespaced_resource_quota=lambda **_: SimpleNamespace(items=[quota]),
        read_namespaced_secret=lambda **_: SimpleNamespace(),
    )

    snapshot = training_client.capacity_snapshot()

    assert snapshot.capacity.connected
    assert snapshot.capacity.source == "namespace_resource_quota"
    assert snapshot.capacity.available_cpu_cores == 6
    assert snapshot.capacity.available_memory_mb == 12_288
    assert snapshot.capacity.ready_nodes == 0
    assert snapshot.runtime_dependencies_ready


def test_optional_cluster_observer_forbidden_degrades_to_cpu() -> None:
    training_client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    training_client._configured = True
    training_client.settings = Settings(
        training_namespace="team-a",
        cluster_observer_enabled=True,
    )

    def forbidden_nodes():
        raise ApiException(status=403, reason="Forbidden")

    training_client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[]),
        list_namespaced_resource_quota=lambda **_: SimpleNamespace(items=[]),
        list_node=forbidden_nodes,
        read_namespaced_secret=lambda **_: SimpleNamespace(),
    )

    snapshot = training_client.capacity_snapshot()

    assert snapshot.capacity.connected
    assert not snapshot.capacity.gpu_available
    assert any("observer cannot list nodes" in item for item in snapshot.capacity.warnings)


def test_resource_usage_exposes_unschedulable_reason_without_metrics_server() -> None:
    training_client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    training_client._configured = True
    training_client.settings = Settings(training_namespace="team-a")
    pod = SimpleNamespace(
        metadata=SimpleNamespace(name="trainer", creation_timestamp=1),
        spec=SimpleNamespace(node_name=None),
        status=SimpleNamespace(
            phase="Pending",
            reason=None,
            container_statuses=[],
            conditions=[
                SimpleNamespace(
                    type="PodScheduled",
                    status="False",
                    reason="Unschedulable",
                    message="0/1 nodes are available: insufficient memory.",
                )
            ],
        ),
    )
    training_client.core = SimpleNamespace(
        list_namespaced_pod=lambda **_: SimpleNamespace(items=[pod])
    )
    training_client.custom = SimpleNamespace(
        get_namespaced_custom_object=lambda **_: (_ for _ in ()).throw(
            ApiException(status=404, reason="metrics API unavailable")
        )
    )

    usage = training_client.training_resource_usage(uuid.uuid4())

    assert not usage["telemetry_available"]
    assert "insufficient memory" in usage["status_reason"]
