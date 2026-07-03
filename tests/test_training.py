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
    NodeHeadroom,
    _cpu_cores,
    _memory_mb,
)
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
            NodeHeadroom(
                name="worker",
                available_cpu=6,
                available_memory_mb=12_000,
                gpu_present=False,
            )
        ],
        pvc_ready=True,
        priority_class_ready=True,
        runtime_dependencies_ready=True,
    )


def test_kubernetes_quantities_are_normalized() -> None:
    assert _cpu_cores("500m") == 0.5
    assert _memory_mb("2Gi") == 2048


def test_estimate_enforces_node_headroom_fraction_and_gpu_fallback() -> None:
    settings = Settings(
        gpu_enabled=True,
        max_node_available_fraction_per_job=0.6,
    )
    training_client = FakeTrainingClient(capacity_snapshot(), settings)

    estimate = training_client.estimate(
        dataset_bytes=10 * 1024**3,
        column_count=500,
        expected_minutes=15,
        prefer_gpu=True,
    )

    assert estimate.cpu_request_cores == 3.6
    assert estimate.memory_request_mb <= int(12_000 * 0.6)
    assert not estimate.gpu_requested
    assert "No node has" in (estimate.gpu_fallback_reason or "")


def test_training_manifest_has_low_priority_limits_and_timeout() -> None:
    settings = Settings(training_image="automl-training:test")
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
    assert {
        "name": "TRAINING_EXECUTION_MODE",
        "value": "direct",
    } in container["env"]
    assert manifest["spec"]["backoffLimit"] == 0
    assert manifest["spec"]["ttlSecondsAfterFinished"] == 30


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
    assert large.memory_request_mb > small.memory_request_mb


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


def test_metrics_api_usage_is_grouped_by_scheduled_node() -> None:
    training_client = FakeTrainingClient(capacity_snapshot(), Settings())
    pod = SimpleNamespace(
        metadata=SimpleNamespace(namespace="automl", name="trainer"),
        spec=SimpleNamespace(node_name="worker"),
    )
    training_client.custom = SimpleNamespace(
        list_cluster_custom_object=lambda **_: {
            "items": [
                {
                    "metadata": {
                        "namespace": "automl",
                        "name": "trainer",
                    },
                    "containers": [{"usage": {"cpu": "250m", "memory": "512Mi"}}],
                }
            ]
        }
    )

    usage, warning = training_client._pod_usage_by_node([pod])

    assert usage == {"worker": (0.25, 512)}
    assert warning is None
