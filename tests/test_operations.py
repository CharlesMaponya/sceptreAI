from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import automl_api.services.operations as operations_service
import numpy as np
import pandas as pd
import pytest
from automl_api import __version__
from automl_api.api.routes.operations import router
from automl_api.core.config import Settings
from automl_api.inference import app as inference_app
from automl_api.inference.app import PointPredictionRequest, PredictionRequest, predict_records
from automl_api.models.enums import (
    GlobalRole,
    ModelStage,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.schemas.operations import (
    ArtifactCleanupRequest,
    DriftLaunchRequest,
    ModelDeploymentRequest,
    RegistryCreateRequest,
)
from automl_api.schemas.training import ClusterCapacityRead, TrainingEstimateRead
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.operations import (
    _adaptive_inference_memory,
    _apply_monitoring_resource_floor,
    _generated_model_dockerfile,
    _internal_model_deployment_urls,
    _platform_model_deployment_urls,
    cleanup_project_resources,
    list_model_deployments,
    update_registry_stage,
)
from automl_api.services.training import BATCH_RUN_KINDS
from automl_api.storage.object_store import EmbeddedObjectStore
from automl_api.training.analysis import _drift_summary
from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.routing import APIRoute
from sklearn.linear_model import LinearRegression


def _endpoint(app: FastAPI, path: str, method: str):
    return next(
        route.endpoint
        for route in app.routes
        if isinstance(route, APIRoute) and route.path == path and method in route.methods
    )


def test_phase_seven_routes_are_registered() -> None:
    paths = {route.path for route in router.routes}

    assert {
        "/projects/{project_id}/operations/health",
        "/projects/{project_id}/operations/registry",
        "/projects/{project_id}/operations/registry/{entry_id}/stage",
        "/projects/{project_id}/operations/registry/{entry_id}/fallback",
        "/projects/{project_id}/operations/registry/{entry_id}/drift",
        "/projects/{project_id}/operations/drift-runs",
        "/projects/{project_id}/operations/registry/{entry_id}/deployments",
        "/projects/{project_id}/operations/deployments",
        "/projects/{project_id}/operations/deployments/{run_id}/inference/{path:path}",
        "/projects/{project_id}/operations/deployments/{run_id}/stop",
        "/projects/{project_id}/operations/cleanup",
    }.issubset(paths)


def test_long_lived_deployments_do_not_consume_batch_training_slots() -> None:
    assert RunKind.DEPLOYMENT not in BATCH_RUN_KINDS
    assert RunKind.DRIFT in BATCH_RUN_KINDS


def test_monitoring_resource_class_scales_drift_job_and_respects_capacity() -> None:
    estimate = SimpleNamespace(
        cpu_request_cores=1.0,
        cpu_limit_cores=2.0,
        memory_request_mb=1024,
        memory_limit_mb=2048,
        capacity=SimpleNamespace(available_cpu_cores=3.0, available_memory_mb=6000),
        blockers=[],
        can_launch=True,
    )

    _apply_monitoring_resource_floor(estimate, "large")

    assert estimate.cpu_request_cores == 2.0
    assert estimate.memory_request_mb == 4096
    assert estimate.can_launch is True

    _apply_monitoring_resource_floor(estimate, "xlarge")

    assert estimate.cpu_request_cores == 4.0
    assert estimate.memory_request_mb == 8192
    assert estimate.can_launch is False
    assert "available CPU" in " ".join(estimate.blockers)


def test_drift_rejects_external_data_missing_training_features(monkeypatch) -> None:
    project_id = uuid.uuid4()
    source_version_id = uuid.uuid4()
    current_version = SimpleNamespace(
        id=uuid.uuid4(),
        schema_json={"columns": [{"name": "age"}]},
    )
    source = SimpleNamespace(
        dataset_version_id=source_version_id,
        target_column="target",
        params={},
        dataset_version=SimpleNamespace(
            schema_json={
                "columns": [{"name": "age"}, {"name": "income"}, {"name": "target"}],
            }
        ),
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        operations_service, "_registry_entry", lambda *_: SimpleNamespace(model_run=source)
    )
    monkeypatch.setattr(operations_service, "_dataset_version", lambda *_: current_version)

    with pytest.raises(HTTPException, match="Missing columns: income"):
        operations_service.launch_drift_check(
            SimpleNamespace(),
            SimpleNamespace(),
            project_id,
            uuid.uuid4(),
            DriftLaunchRequest(dataset_version_id=current_version.id),
        )


def test_generated_model_dockerfile_is_pinned_to_supplied_runtime() -> None:
    entry_id = uuid.uuid4()

    dockerfile = _generated_model_dockerfile(
        base_image="registry.example/inference@sha256:abc123",
        model_uri="minio://automl/projects/p/model.joblib",
        model_name="Approved Model",
        project_name="Credit Risk",
        environment="production",
        registry_entry_id=entry_id,
    )

    assert dockerfile.startswith("FROM registry.example/inference@sha256:abc123\n")
    assert f'ai.sceptre.registry-entry-id"="{entry_id}' in dockerfile
    assert 'ENV MODEL_NAME="Approved Model"' in dockerfile
    assert 'ENV PROJECT_NAME="Credit Risk"' in dockerfile
    assert 'ENV DEPLOYMENT_ENVIRONMENT="production"' in dockerfile


def test_generated_model_dockerfile_rejects_line_break_injection() -> None:
    with pytest.raises(ValueError, match="line breaks"):
        _generated_model_dockerfile(
            base_image="safe-image\nRUN whoami",
            model_uri="minio://automl/model.joblib",
            model_name="Model",
            project_name="Credit Risk",
            environment="production",
            registry_entry_id=uuid.uuid4(),
        )


def test_inference_runtime_returns_predictions_and_probabilities() -> None:
    class ProbabilityModel:
        def predict(self, frame):
            return np.asarray(["yes"] * len(frame))

        def predict_proba(self, frame):
            return np.asarray([[0.25, 0.75]] * len(frame))

    predictions, probabilities = predict_records(
        ProbabilityModel(),
        [{"amount": 1.0}, {"amount": 2.0}],
        include_probabilities=True,
    )

    assert predictions == ["yes", "yes"]
    assert probabilities == [[0.25, 0.75], [0.25, 0.75]]


def test_inference_runtime_supports_sklearn_model() -> None:
    model = LinearRegression().fit(
        pd.DataFrame({"feature": [0.0, 1.0, 2.0]}),
        [0.0, 2.0, 4.0],
    )

    predictions, probabilities = predict_records(
        model,
        [{"feature": 3.0}],
        include_probabilities=False,
    )

    assert predictions == pytest.approx([6.0])
    assert probabilities is None


def test_inference_http_contract(monkeypatch) -> None:
    class Model:
        def predict(self, frame):
            return np.asarray([len(frame)] * len(frame))

    monkeypatch.setattr(inference_app, "_load_model", lambda: Model())
    app = inference_app.app
    ready = _endpoint(app, "/health/ready", "GET")()
    root = _endpoint(app, "/", "GET")()
    online = _endpoint(app, "/v1/predict/online", "POST")(
        PointPredictionRequest(record={"feature": 1.0})
    )
    response = _endpoint(app, "/v1/predict", "POST")(
        PredictionRequest(records=[{"feature": 1.0}, {"feature": 2.0}])
    )
    openapi = app.openapi()

    assert ready == {"status": "ok"}
    assert root.status_code == 307
    assert root.headers["location"] == "/docs"
    assert any(route.path == "/docs" for route in app.routes)
    assert "/v1/predict" in openapi["paths"]
    assert "/v1/predict/online" in openapi["paths"]
    assert "/v1/predict/offline" in openapi["paths"]
    assert online.prediction == 1
    assert response.predictions == [2, 2]


def test_inference_offline_upload_returns_prediction_file(monkeypatch) -> None:
    class Model:
        def predict(self, frame):
            return np.asarray([value * 2 for value in frame["feature"]])

    monkeypatch.setattr(inference_app, "_load_model", lambda: Model())
    app = inference_app.create_app()
    response = _endpoint(app, "/v1/predict/offline", "POST")(
        file=UploadFile(filename="scoring.csv", file=io.BytesIO(b"feature\n2\n4\n")),
        include_probabilities=False,
    )

    assert response.headers["x-prediction-row-count"] == "2"
    assert "scoring-predictions.csv" in response.headers["content-disposition"]
    assert Path(response.path).read_text(encoding="utf-8").splitlines() == [
        "feature,prediction",
        "2,4",
        "4,8",
    ]
    Path(response.path).unlink(missing_ok=True)


def test_inference_offline_upload_rejects_unknown_file_type(
    monkeypatch,
) -> None:
    monkeypatch.setattr(inference_app, "_load_model", lambda: object())
    endpoint = _endpoint(inference_app.create_app(), "/v1/predict/offline", "POST")

    with pytest.raises(HTTPException) as exc_info:
        endpoint(
            file=UploadFile(filename="scoring.xlsx", file=io.BytesIO(b"invalid")),
            include_probabilities=False,
        )

    assert exc_info.value.status_code == 422
    assert "Unsupported file type" in exc_info.value.detail


def test_inference_model_load_and_error_contracts(monkeypatch) -> None:
    inference_app._load_model.cache_clear()
    monkeypatch.delenv("MODEL_URI", raising=False)
    with pytest.raises(RuntimeError, match="MODEL_URI is required"):
        inference_app._load_model()

    app = inference_app.create_app()
    monkeypatch.setattr(
        inference_app,
        "_load_model",
        lambda: (_ for _ in ()).throw(RuntimeError("artifact unavailable")),
    )
    with pytest.raises(HTTPException) as ready_error:
        _endpoint(app, "/health/ready", "GET")()
    assert ready_error.value.status_code == 503

    with pytest.raises(HTTPException, match="Prediction failed") as online_error:
        _endpoint(app, "/v1/predict/online", "POST")(PointPredictionRequest(record={"feature": 1}))
    assert online_error.value.status_code == 422
    with pytest.raises(HTTPException, match="Prediction failed"):
        _endpoint(app, "/v1/predict", "POST")(PredictionRequest(records=[{"feature": 1}]))


def test_inference_uploaded_json_formats_empty_limit_and_probabilities(tmp_path) -> None:
    json_upload = UploadFile(
        filename="rows.json", file=io.BytesIO(b'[{"feature":1},{"feature":2}]')
    )
    frames = list(inference_app._uploaded_frames(json_upload, 10))
    assert frames[0]["feature"].tolist() == [1, 2]

    jsonl_upload = UploadFile(
        filename="rows.ndjson", file=io.BytesIO(b'{"feature":1}\n{"feature":2}\n')
    )
    assert sum(len(frame) for frame in inference_app._uploaded_frames(jsonl_upload, 1)) == 2

    class ProbabilityModel:
        def predict(self, frame):
            return np.ones(len(frame), dtype=int)

        def predict_proba(self, frame):
            return np.asarray([[0.25, 0.75]] * len(frame))

    output = inference_app._prediction_output_frame(
        ProbabilityModel(), pd.DataFrame({"feature": [1]}), include_probabilities=True
    )
    assert output[["probability_0", "probability_1"]].iloc[0].tolist() == [0.25, 0.75]

    empty = UploadFile(filename="empty.csv", file=io.BytesIO(b"feature\n"))
    with pytest.raises(ValueError, match="contains no rows"):
        inference_app.create_offline_prediction_file(
            ProbabilityModel(), empty, include_probabilities=False
        )
    oversized = UploadFile(filename="rows.csv", file=io.BytesIO(b"feature\n1\n2\n"))
    with pytest.raises(ValueError, match="exceeds"):
        inference_app.create_offline_prediction_file(
            ProbabilityModel(), oversized, include_probabilities=False, max_rows=1
        )


def test_inference_docs_use_project_and_environment_not_platform_name(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROJECT_NAME", "Credit Risk")
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "staging")
    monkeypatch.setenv("MODEL_NAME", "ExtraTreesClassifier")
    app = inference_app.create_app()
    openapi = app.openapi()
    metadata = _endpoint(app, "/v1/metadata", "GET")()

    assert openapi["info"]["title"] == "Credit Risk model API (staging)"
    assert "Sceptre" not in openapi["info"]["title"]
    assert metadata.model_dump() == {
        "project_name": "Credit Risk",
        "environment": "staging",
        "model_name": "ExtraTreesClassifier",
    }


def test_model_deployment_manifest_is_isolated_and_probe_enabled() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(
        training_namespace="automl",
        inference_service_account="automl-inference",
        workload_image_pull_secrets=("registry",),
    )
    deployment_id = uuid.uuid4()

    manifests = client.build_model_deployment_manifest(
        deployment_id=deployment_id,
        project_id=uuid.uuid4(),
        project_name="Credit Risk",
        environment="staging",
        model_name="RandomForestClassifier",
        model_uri="minio://automl/model.joblib",
        image="automl-inference@sha256:abc",
        replicas=2,
        cpu_request="500m",
        memory_request="1Gi",
    )

    pod_spec = manifests["deployment"]["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["serviceAccountName"] == "automl-inference"
    assert pod_spec["imagePullSecrets"] == [{"name": "registry"}]
    assert container["image"] == "automl-inference@sha256:abc"
    assert container["startupProbe"]["httpGet"]["path"] == "/health/ready"
    environment = {item["name"]: item["value"] for item in container["env"] if "value" in item}
    assert environment["PROJECT_NAME"] == "Credit Risk"
    assert environment["DEPLOYMENT_ENVIRONMENT"] == "staging"
    assert manifests["service"]["spec"]["type"] == "ClusterIP"


class _FakeDeploymentApps:
    def __init__(self, *, available=0, unavailable=1):
        self.deployment = SimpleNamespace(
            spec=SimpleNamespace(
                replicas=1,
                selector=SimpleNamespace(
                    match_labels={"automl.platform/deployment-id": "deployment"}
                ),
            ),
            status=SimpleNamespace(
                available_replicas=available,
                unavailable_replicas=unavailable,
            ),
        )

    def read_namespaced_deployment_status(self, **_):
        return self.deployment


class _FakeDeploymentCore:
    def __init__(self, waiting_reason=None, previous_reason=None):
        waiting = SimpleNamespace(reason=waiting_reason) if waiting_reason else None
        previous = SimpleNamespace(reason=previous_reason) if previous_reason else None
        self.pods = [
            SimpleNamespace(
                status=SimpleNamespace(
                    container_statuses=[
                        SimpleNamespace(
                            state=SimpleNamespace(
                                waiting=waiting,
                                terminated=None,
                            ),
                            last_state=SimpleNamespace(
                                terminated=previous,
                            ),
                        )
                    ]
                )
            )
        ]

    def list_namespaced_pod(self, **_):
        return SimpleNamespace(items=self.pods)


@pytest.mark.parametrize(
    ("waiting_reason", "expected_state"),
    [
        ("ImagePullBackOff", "image_pull_error"),
        ("ErrImagePull", "image_pull_error"),
        ("CrashLoopBackOff", "crash_loop"),
        ("CreateContainerConfigError", "configuration_error"),
    ],
)
def test_model_deployment_state_reports_container_failures(
    waiting_reason,
    expected_state,
) -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(training_namespace="automl")
    client.apps = _FakeDeploymentApps()
    client.core = _FakeDeploymentCore(waiting_reason)

    assert client.model_deployment_state("model") == expected_state


def test_model_deployment_state_reports_previous_oom_kill() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(training_namespace="automl")
    client.apps = _FakeDeploymentApps()
    client.core = _FakeDeploymentCore(previous_reason="OOMKilled")

    assert client.model_deployment_state("model") == "out_of_memory"


def test_inference_memory_adapts_to_serialized_model_size() -> None:
    assert _adaptive_inference_memory(64 * 1024 * 1024, "1Gi") == "2Gi"
    assert _adaptive_inference_memory(10 * 1024 * 1024, "3Gi") == "3Gi"


def test_model_deployment_urls_use_node_port() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(
        training_namespace="automl",
        inference_service_type="NodePort",
        inference_external_host="models.local",
    )
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_: SimpleNamespace(
            spec=SimpleNamespace(
                type="NodePort",
                ports=[
                    SimpleNamespace(
                        name="http",
                        port=8080,
                        node_port=31234,
                    )
                ],
            )
        ),
    )

    assert client.model_deployment_urls("model") == {
        "base_url": "http://models.local:31234",
        "endpoint": "http://models.local:31234/v1/predict",
        "docs_url": "http://models.local:31234/docs",
        "openapi_url": "http://models.local:31234/openapi.json",
    }


def test_cluster_ip_model_deployment_does_not_report_an_external_url() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(training_namespace="automl")
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_: SimpleNamespace(
            spec=SimpleNamespace(
                type="ClusterIP",
                ports=[SimpleNamespace(name="http", port=8080, node_port=None)],
            )
        )
    )

    assert client.model_deployment_urls("model") is None


def test_internal_model_deployment_urls_use_portable_service_dns() -> None:
    assert _internal_model_deployment_urls("automl-model-1234", "sceptre") == {
        "internal_endpoint": ("http://automl-model-1234.sceptre.svc:8080/v1/predict"),
        "internal_docs_url": "http://automl-model-1234.sceptre.svc:8080/docs",
        "internal_openapi_url": ("http://automl-model-1234.sceptre.svc:8080/openapi.json"),
    }


def test_platform_model_deployment_urls_use_authenticated_api_paths() -> None:
    project_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
    run_id = uuid.UUID("22222222-2222-2222-2222-222222222222")
    base_url = f"/api/v1/projects/{project_id}/operations/deployments/{run_id}/inference"

    assert _platform_model_deployment_urls(project_id, run_id) == {
        "platform_endpoint": f"{base_url}/v1/predict",
        "platform_online_endpoint": f"{base_url}/v1/predict/online",
        "platform_offline_endpoint": f"{base_url}/v1/predict/offline",
        "platform_metadata_url": f"{base_url}/v1/metadata",
        "platform_docs_url": f"{base_url}/docs",
        "platform_openapi_url": f"{base_url}/openapi.json",
        "platform_live_url": f"{base_url}/health/live",
        "platform_ready_url": f"{base_url}/health/ready",
    }


def test_model_ingress_url_is_reported_only_after_admission() -> None:
    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client.settings = Settings(
        training_namespace="automl",
        inference_ingress_enabled=True,
        inference_ingress_class_name="nginx",
        inference_ingress_host_template="{name}.models.local",
    )
    manifests = client.build_model_deployment_manifest(
        deployment_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        project_id=uuid.uuid4(),
        project_name="Credit Risk",
        environment="local",
        model_name="RandomForestClassifier",
        model_uri="minio://automl/model.joblib",
        image=f"docker.io/maponyacharles/sceptreai:inference-{__version__}",
        replicas=1,
        cpu_request="500m",
        memory_request="1Gi",
    )
    assert manifests["ingress"]["spec"]["ingressClassName"] == "nginx"
    assert manifests["ingress"]["spec"]["rules"][0]["host"] == "automl-model-11111111.models.local"
    client.core = SimpleNamespace(
        read_namespaced_service=lambda **_: SimpleNamespace(
            spec=SimpleNamespace(
                type="ClusterIP",
                ports=[SimpleNamespace(name="http", port=8080, node_port=None)],
            )
        )
    )
    ingress = SimpleNamespace(
        status=SimpleNamespace(load_balancer=SimpleNamespace(ingress=None)),
        spec=SimpleNamespace(rules=[SimpleNamespace(host="automl-model-11111111.models.local")]),
    )
    client.networking = SimpleNamespace(read_namespaced_ingress_status=lambda **_: ingress)

    assert client.model_deployment_urls("automl-model-11111111") is None
    ingress.status.load_balancer.ingress = [SimpleNamespace(ip="127.0.0.1")]
    assert client.model_deployment_urls("automl-model-11111111") == {
        "base_url": "http://automl-model-11111111.models.local",
        "endpoint": "http://automl-model-11111111.models.local/v1/predict",
        "docs_url": "http://automl-model-11111111.models.local/docs",
        "openapi_url": "http://automl-model-11111111.models.local/openapi.json",
    }


def test_drift_summary_extracts_stable_metrics_from_nested_report() -> None:
    report = {
        "metrics": [
            {
                "result": {
                    "dataset_drift": True,
                    "number_of_drifted_columns": 1,
                    "share_of_drifted_columns": 0.5,
                    "drift_by_columns": {
                        "amount": {"drift_detected": True},
                        "segment": {"drift_detected": False},
                    },
                }
            }
        ]
    }

    metrics, diagnostics = _drift_summary(report, feature_count=2)

    assert metrics == {
        "dataset_drift": 1.0,
        "drift_share": 0.5,
        "drifted_feature_count": 1.0,
    }
    assert diagnostics["drift_share_percent"] == 50.0
    assert diagnostics["drifted_features"] == ["amount"]


def test_embedded_object_store_delete_is_idempotent(tmp_path) -> None:
    store = EmbeddedObjectStore(
        Settings(
            object_store_bucket="automl",
            local_object_store_path=tmp_path,
        )
    )
    stored = store.put_bytes("cleanup/artifact.json", b"{}")

    assert store.size(stored.uri) == 2
    store.delete(stored.uri)
    store.delete(stored.uri)

    assert not store.exists(stored.uri)


def test_drift_summary_respects_dataset_threshold() -> None:
    metrics, diagnostics = _drift_summary(
        {
            "dataset_drift": False,
            "number_of_drifted_columns": 1,
            "share_of_drifted_columns": 0.1,
        },
        feature_count=10,
    )

    assert metrics["dataset_drift"] == 0.0
    assert diagnostics["drift_share_percent"] == 10.0


class _ScalarResult:
    def __init__(self, values):
        self.values = values

    def all(self):
        return self.values


class _SequenceSession:
    bind = None

    def __init__(self, *, scalars=None, scalar=None):
        self.scalar_values = list(scalar or [])
        self.scalars_values = list(scalars or [])
        self.deleted = []

    def scalar(self, _):
        return self.scalar_values.pop(0)

    def scalars(self, _):
        return _ScalarResult(self.scalars_values.pop(0))

    def flush(self):
        return None

    def delete(self, value):
        self.deleted.append(value)


def _deployment_run(*, status: RunStatus = RunStatus.RUNNING) -> ModelRun:
    now = datetime.now(UTC)
    return ModelRun(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        run_kind=RunKind.DEPLOYMENT,
        status=status,
        task_type=TaskType.REGRESSION,
        run_name="Deploy model",
        pipeline_name="kubernetes_model_deployment_v1",
        k8s_namespace="sceptre",
        k8s_job_name="automl-model-1234",
        gpu_requested=False,
        params={},
        tags={"service_name": "automl-model-1234"},
        queued_at=now,
        started_at=now,
        created_at=now,
        updated_at=now,
    )


def test_ready_deployment_reports_internal_and_external_access_metadata(
    monkeypatch,
) -> None:
    run = _deployment_run()
    db = _SequenceSession(scalars=[[run]])
    client = SimpleNamespace(
        settings=Settings(training_namespace="fallback"),
        model_deployment_state=lambda _: "ready",
        model_deployment_urls=lambda _: {
            "base_url": "https://model.example.test",
            "endpoint": "https://model.example.test/v1/predict",
            "docs_url": "https://model.example.test/docs",
            "openapi_url": "https://model.example.test/openapi.json",
        },
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)

    deployment = list_model_deployments(
        db,
        SimpleNamespace(),
        run.project_id,
        client,
    )[0]

    assert deployment.status == RunStatus.SUCCEEDED
    assert deployment.service_name == "automl-model-1234"
    assert deployment.namespace == "sceptre"
    assert deployment.endpoint == "https://model.example.test/v1/predict"
    assert deployment.docs_url == "https://model.example.test/docs"
    assert deployment.openapi_url == "https://model.example.test/openapi.json"
    assert deployment.internal_endpoint == ("http://automl-model-1234.sceptre.svc:8080/v1/predict")
    assert deployment.internal_docs_url == ("http://automl-model-1234.sceptre.svc:8080/docs")
    assert deployment.internal_openapi_url == (
        "http://automl-model-1234.sceptre.svc:8080/openapi.json"
    )
    platform_base = f"/api/v1/projects/{run.project_id}/operations/deployments/{run.id}/inference"
    assert deployment.platform_endpoint == f"{platform_base}/v1/predict"
    assert deployment.platform_online_endpoint == (f"{platform_base}/v1/predict/online")
    assert deployment.platform_offline_endpoint == (f"{platform_base}/v1/predict/offline")
    assert deployment.platform_metadata_url == f"{platform_base}/v1/metadata"
    assert deployment.platform_docs_url == f"{platform_base}/docs"
    assert deployment.platform_openapi_url == f"{platform_base}/openapi.json"
    assert deployment.platform_live_url == f"{platform_base}/health/live"
    assert deployment.platform_ready_url == f"{platform_base}/health/ready"


def test_non_ready_deployment_hides_internal_access_urls(monkeypatch) -> None:
    run = _deployment_run()
    db = _SequenceSession(scalars=[[run]])
    client = SimpleNamespace(
        settings=Settings(training_namespace="fallback"),
        model_deployment_state=lambda _: "progressing",
        model_deployment_urls=lambda _: pytest.fail(
            "external URLs must not be resolved before the deployment is ready"
        ),
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)

    deployment = list_model_deployments(
        db,
        SimpleNamespace(),
        run.project_id,
        client,
    )[0]

    assert deployment.status == RunStatus.RUNNING
    assert deployment.service_name == "automl-model-1234"
    assert deployment.namespace == "sceptre"
    assert deployment.internal_endpoint is None
    assert deployment.internal_docs_url is None
    assert deployment.internal_openapi_url is None
    assert deployment.platform_endpoint is None
    assert deployment.platform_online_endpoint is None
    assert deployment.platform_offline_endpoint is None
    assert deployment.platform_metadata_url is None
    assert deployment.platform_docs_url is None
    assert deployment.platform_openapi_url is None
    assert deployment.platform_live_url is None
    assert deployment.platform_ready_url is None


def test_production_promotion_preserves_previous_model_as_fallback() -> None:
    project_id = uuid.uuid4()
    entry = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=project_id,
        stage=ModelStage.STAGING,
        feature_space_hash="feature-space",
        registry_metadata={"fallback": False},
        promoted_at=None,
        retired_at=None,
    )
    previous = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=project_id,
        stage=ModelStage.PRODUCTION,
        feature_space_hash="feature-space",
        registry_metadata={"fallback": False},
    )
    db = _SequenceSession(
        scalar=[entry],
        scalars=[[previous], [previous, entry]],
    )
    user = SimpleNamespace(id=uuid.uuid4(), global_role=GlobalRole.ADMIN)

    updated = update_registry_stage(
        db,
        user,
        project_id,
        entry.id,
        ModelStage.PRODUCTION,
    )

    assert updated.stage == ModelStage.PRODUCTION
    assert previous.stage == ModelStage.STAGING
    assert previous.registry_metadata["fallback"] is True
    assert previous.registry_metadata["replaced_by_entry_id"] == str(entry.id)


def test_cleanup_preview_protects_active_deployment_artifacts() -> None:
    project_id = uuid.uuid4()
    active_run_id = uuid.uuid4()
    model_artifact_id = uuid.uuid4()
    active = SimpleNamespace(
        id=active_run_id,
        tags={"model_artifact_id": str(model_artifact_id)},
    )
    protected_model = SimpleNamespace(
        id=model_artifact_id,
        model_run_id=uuid.uuid4(),
        registry_entries=[],
        byte_size=10,
    )
    protected_dockerfile = SimpleNamespace(
        id=uuid.uuid4(),
        model_run_id=active_run_id,
        registry_entries=[],
        byte_size=20,
    )
    eligible = SimpleNamespace(
        id=uuid.uuid4(),
        model_run_id=uuid.uuid4(),
        registry_entries=[],
        byte_size=30,
        created_at=datetime.now(UTC) - timedelta(days=100),
    )
    db = _SequenceSession(
        scalars=[
            [active],
            [protected_model, protected_dockerfile, eligible],
        ]
    )
    user = SimpleNamespace(id=uuid.uuid4(), global_role=GlobalRole.ADMIN)

    result = cleanup_project_resources(
        db,
        user,
        project_id,
        ArtifactCleanupRequest(older_than_days=30, dry_run=True),
    )

    assert result.artifact_ids == [eligible.id]
    assert result.artifact_bytes == 30
    assert not db.deleted


def test_register_model_persists_artifact_and_version(monkeypatch) -> None:
    project_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    parent = SimpleNamespace(
        id=uuid.uuid4(),
        status=RunStatus.SUCCEEDED,
        project_id=project_id,
        target_column="target",
        task_type=TaskType.CLASSIFICATION,
        params={},
        dataset_version=SimpleNamespace(
            schema_json={"columns": [{"name": "x"}, {"name": "target"}]}
        ),
    )
    parent.tags = {
        "leaderboard_primary_metric": "accuracy",
        "leaderboard": [
            {
                "model": "LogisticRegression",
                "status": "succeeded",
                "model_artifact_uri": "s3://models/model.joblib",
                "metrics": {"accuracy": 0.91},
            }
        ],
    }

    class DB:
        calls = 0
        added = []

        def scalar(self, _):
            self.calls += 1
            return None if self.calls == 1 else 2

        def add(self, value):
            value.id = value.id or uuid.uuid4()
            now = datetime.now(UTC)
            value.created_at = value.created_at or now
            value.updated_at = value.updated_at or now
            self.added.append(value)

        def flush(self):
            pass

    db = DB()
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(operations_service, "_training_run", lambda *_: parent)
    monkeypatch.setattr(operations_service, "_leaderboard_parent", lambda *_: parent)
    monkeypatch.setattr(operations_service, "_candidate_run", lambda *_: parent)
    monkeypatch.setattr(
        operations_service,
        "get_object_store",
        lambda: SimpleNamespace(size=lambda _: 123),
    )

    entry = operations_service.register_model(
        db,
        user,
        project_id,
        RegistryCreateRequest(training_run_id=parent.id, model_name="LogisticRegression"),
    )

    assert entry.version == 3
    assert entry.champion_metric_value == 0.91
    assert entry.stage == ModelStage.CANDIDATE
    assert isinstance(db.added[0], RunArtifact)
    assert db.added[0].byte_size == 123


@pytest.mark.parametrize(
    ("parent_status", "leaderboard", "message"),
    [
        (RunStatus.RUNNING, [], "must succeed"),
        (RunStatus.SUCCEEDED, [], "not found"),
        (
            RunStatus.SUCCEEDED,
            [{"model": "M", "status": "succeeded"}],
            "durable model artifact",
        ),
    ],
)
def test_register_model_rejects_invalid_source(
    monkeypatch, parent_status, leaderboard, message
) -> None:
    parent = SimpleNamespace(status=parent_status, tags={"leaderboard": leaderboard})
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(operations_service, "_training_run", lambda *_: parent)
    monkeypatch.setattr(operations_service, "_leaderboard_parent", lambda *_: parent)
    monkeypatch.setattr(
        operations_service,
        "_candidate_run",
        lambda *_: SimpleNamespace(id=uuid.uuid4()),
    )
    with pytest.raises(HTTPException, match=message):
        operations_service.register_model(
            SimpleNamespace(scalar=lambda _: None),
            SimpleNamespace(),
            uuid.uuid4(),
            RegistryCreateRequest(training_run_id=uuid.uuid4(), model_name="M"),
        )


def test_registry_stage_and_fallback_transitions_fail_closed(monkeypatch) -> None:
    entry = SimpleNamespace(
        stage=ModelStage.CANDIDATE,
        registry_metadata={},
        feature_space_hash="x",
    )
    db = SimpleNamespace(flush=lambda: None)
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(operations_service, "_registry_entry", lambda *_: entry)
    with pytest.raises(HTTPException, match="Cannot move"):
        operations_service.update_registry_stage(
            db, SimpleNamespace(), uuid.uuid4(), uuid.uuid4(), ModelStage.PRODUCTION
        )
    with pytest.raises(HTTPException, match="Only a staging model"):
        operations_service.set_registry_fallback(db, SimpleNamespace(), uuid.uuid4(), uuid.uuid4())

    entry.stage = ModelStage.STAGING
    monkeypatch.setattr(operations_service, "_clear_fallbacks", lambda *_args, **_kwargs: None)
    selected = operations_service.set_registry_fallback(
        db, SimpleNamespace(id=uuid.uuid4()), uuid.uuid4(), uuid.uuid4()
    )
    assert selected.registry_metadata["fallback"] is True


def test_operation_lookup_helpers_and_candidate_extension() -> None:
    project_id, item_id = uuid.uuid4(), uuid.uuid4()
    missing = SimpleNamespace(scalar=lambda _: None)
    for helper, message in (
        (operations_service._registry_entry, "Registry entry not found"),
        (operations_service._training_run, "Training run not found"),
        (operations_service._dataset_version, "Dataset version not found"),
    ):
        with pytest.raises(HTTPException, match=message):
            helper(missing, project_id, item_id)

    parent = SimpleNamespace(project_id=project_id)
    assert operations_service._candidate_run(missing, parent, {}) is parent
    assert (
        operations_service._candidate_run(
            SimpleNamespace(get=lambda *_: None), parent, {"extension_run_id": "bad"}
        )
        is parent
    )


def test_stop_deployment_is_idempotent_for_kubernetes_404(monkeypatch) -> None:
    run = _deployment_run()
    db = SimpleNamespace(scalar=lambda _: run, flush=lambda: None)

    def missing(_):
        raise operations_service.ApiException(status=404)

    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    stopped = operations_service.stop_model_deployment(
        db,
        SimpleNamespace(),
        run.project_id,
        run.id,
        SimpleNamespace(delete_model_deployment=missing),
    )
    assert stopped.status == RunStatus.CANCELLED
    assert stopped.finished_at is not None


def test_cleanup_executes_object_and_job_deletion_with_error_capture(monkeypatch) -> None:
    project_id = uuid.uuid4()
    good = SimpleNamespace(
        id=uuid.uuid4(),
        model_run_id=uuid.uuid4(),
        registry_entries=[],
        byte_size=10,
        object_uri="s3://good",
        created_at=datetime.now(UTC) - timedelta(days=90),
    )
    bad = SimpleNamespace(
        id=uuid.uuid4(),
        model_run_id=uuid.uuid4(),
        registry_entries=[],
        byte_size=None,
        object_uri="s3://bad",
        created_at=datetime.now(UTC) - timedelta(days=90),
    )
    db = _SequenceSession(scalars=[[], [good, bad]])

    class Store:
        def delete(self, uri):
            if uri.endswith("bad"):
                raise RuntimeError("denied")

    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "get_object_store", Store)
    result = cleanup_project_resources(
        db,
        SimpleNamespace(),
        project_id,
        ArtifactCleanupRequest(dry_run=False),
        SimpleNamespace(cleanup_finished_jobs=lambda _: ["job-1"]),
    )
    assert result.deleted_object_uris == ["s3://good"]
    assert result.deleted_kubernetes_jobs == ["job-1"]
    assert "denied" in result.errors[0]
    assert db.deleted == [good]


def _operations_estimate(*, can_launch=True):
    return TrainingEstimateRead(
        capacity=ClusterCapacityRead(
            connected=True,
            source="test",
            total_cpu_cores=8,
            requested_cpu_cores=0,
            available_cpu_cores=8,
            total_memory_mb=16384,
            requested_memory_mb=0,
            available_memory_mb=16384,
            ready_nodes=2,
            gpu_available=False,
            active_training_jobs=0,
        ),
        estimated_working_set_mb=256,
        cpu_request_cores=1,
        cpu_limit_cores=2,
        memory_request_mb=1024,
        memory_limit_mb=2048,
        gpu_requested=False,
        expected_minutes=10,
        active_deadline_seconds=900,
        estimated_core_hours=0.2,
        max_concurrent_jobs=15,
        can_launch=can_launch,
        blockers=[] if can_launch else ["capacity"],
    )


def test_drift_launch_submits_a_resource_bounded_job(monkeypatch) -> None:
    project_id = uuid.uuid4()
    source_version_id = uuid.uuid4()
    source = SimpleNamespace(
        id=uuid.uuid4(),
        dataset_version_id=source_version_id,
        target_column="target",
        task_type=TaskType.CLASSIFICATION,
        params={"excluded_leakage_columns": ["leak"]},
        dataset_version=SimpleNamespace(
            schema_json={
                "columns": [
                    {"name": "x"},
                    {"name": "target"},
                    {"name": "leak"},
                ]
            }
        ),
    )
    entry = SimpleNamespace(id=uuid.uuid4(), model_run=source, model_name="M", version=1)
    current = SimpleNamespace(id=uuid.uuid4(), schema_json={"columns": [{"name": "x"}]})
    added = []

    class DB:
        def scalars(self, _):
            return SimpleNamespace(all=lambda: [])

        def add(self, value):
            value.id = uuid.uuid4()
            now = datetime.now(UTC)
            value.created_at = now
            value.updated_at = now
            added.append(value)

        def flush(self):
            pass

    class Client:
        settings = SimpleNamespace(training_namespace="ray-jobs")
        created = []

        def build_job_manifest(self, **kwargs):
            return {"metadata": {"name": f"drift-{kwargs['run_id']}"}}

        def create_job(self, manifest):
            self.created.append(manifest)

    client = Client()
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_registry_entry", lambda *_: entry)
    monkeypatch.setattr(operations_service, "_dataset_version", lambda *_: current)
    monkeypatch.setattr(
        operations_service, "estimate_training_run", lambda *_: _operations_estimate()
    )

    result = operations_service.launch_drift_check(
        DB(),
        SimpleNamespace(id=uuid.uuid4()),
        project_id,
        entry.id,
        DriftLaunchRequest(dataset_version_id=current.id),
        client,
    )

    assert result.run.status == RunStatus.QUEUED
    assert result.run.gpu_requested is False
    assert result.run.params["monitoring_resource_class"] == "standard"
    assert client.created == [result.manifest]
    assert added[0].run_kind == RunKind.DRIFT


def test_drift_launch_rejects_same_dataset_and_failed_precheck(monkeypatch) -> None:
    project_id = uuid.uuid4()
    version = SimpleNamespace(id=uuid.uuid4(), schema_json={"columns": []})
    source = SimpleNamespace(
        dataset_version_id=version.id,
        target_column=None,
        task_type=TaskType.CLUSTERING,
        params={},
        dataset_version=version,
    )
    entry = SimpleNamespace(id=uuid.uuid4(), model_run=source)
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_registry_entry", lambda *_: entry)
    monkeypatch.setattr(operations_service, "_dataset_version", lambda *_: version)
    with pytest.raises(HTTPException, match="different from"):
        operations_service.launch_drift_check(
            SimpleNamespace(),
            SimpleNamespace(),
            project_id,
            entry.id,
            DriftLaunchRequest(dataset_version_id=version.id),
        )

    source.dataset_version_id = uuid.uuid4()
    monkeypatch.setattr(
        operations_service,
        "estimate_training_run",
        lambda *_: _operations_estimate(can_launch=False),
    )
    db = SimpleNamespace(scalars=lambda _: SimpleNamespace(all=lambda: []))
    with pytest.raises(HTTPException, match="Drift precheck failed"):
        operations_service.launch_drift_check(
            db,
            SimpleNamespace(),
            project_id,
            entry.id,
            DriftLaunchRequest(dataset_version_id=version.id),
            SimpleNamespace(settings=SimpleNamespace(training_namespace="ray")),
        )


def _deployable_entry(project_id):
    source = SimpleNamespace(
        dataset_version_id=uuid.uuid4(),
        task_type=TaskType.CLASSIFICATION,
        target_column="target",
    )
    artifact = SimpleNamespace(
        object_uri="s3://models/model.joblib",
        byte_size=1024,
    )
    return SimpleNamespace(
        id=uuid.uuid4(),
        stage=ModelStage.STAGING,
        model_name="LogisticRegression",
        version=1,
        model_run=source,
        model_artifact=artifact,
        model_artifact_id=uuid.uuid4(),
        project_id=project_id,
    )


class _DeploymentDB:
    def __init__(self, project, active=None):
        self.project = project
        self.active = active or []
        self.added = []

    def scalars(self, _):
        return SimpleNamespace(all=lambda: self.active)

    def get(self, *_):
        return self.project

    def add(self, value):
        value.id = value.id or uuid.uuid4()
        now = datetime.now(UTC)
        value.created_at = value.created_at or now
        value.updated_at = value.updated_at or now
        self.added.append(value)

    def flush(self):
        pass


class _DeploymentClient:
    def __init__(self, error=None):
        self.error = error
        self.created = []
        self.settings = Settings(
            training_namespace="models",
            environment="local",
            inference_image="inference@sha256:abc",
        )

    def build_model_deployment_manifest(self, **_):
        return {
            "deployment": {"metadata": {"name": "model-deployment"}},
            "service": {"metadata": {"name": "model-service"}},
        }

    def create_model_deployment(self, manifests):
        if self.error:
            raise self.error
        self.created.append(manifests)

    def model_deployment_urls(self, _):
        return {"endpoint": "https://model.test/v1/predict"}


def test_deploy_registered_model_persists_manifest_and_dockerfile(monkeypatch) -> None:
    project_id = uuid.uuid4()
    project = SimpleNamespace(id=project_id, name="Risk")
    entry = _deployable_entry(project_id)
    db = _DeploymentDB(project)
    client = _DeploymentClient()
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(operations_service, "_registry_entry", lambda *_: entry)
    monkeypatch.setattr(
        operations_service,
        "get_object_store",
        lambda: SimpleNamespace(
            put_bytes=lambda key, data: SimpleNamespace(uri=f"s3://artifacts/{key}")
        ),
    )

    result = operations_service.deploy_registered_model(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        project_id,
        entry.id,
        ModelDeploymentRequest(),
        client,
    )

    assert result.run.status == RunStatus.QUEUED
    assert result.run.gpu_requested is False
    assert result.run.tags["service_name"] == "model-service"
    assert result.run.tags["desired_state"] == "deployment_pending"
    assert result.dockerfile_uri.startswith("pending://automl/projects/")
    assert client.created == []
    assert any(isinstance(item, RunArtifact) for item in db.added)


def test_deployment_rejects_stage_duplicate_and_missing_project(monkeypatch) -> None:
    project_id = uuid.uuid4()
    entry = _deployable_entry(project_id)
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(operations_service, "_registry_entry", lambda *_: entry)

    entry.stage = ModelStage.CANDIDATE
    with pytest.raises(HTTPException, match="staging or production"):
        operations_service.deploy_registered_model(
            _DeploymentDB(SimpleNamespace()),
            SimpleNamespace(),
            project_id,
            entry.id,
            ModelDeploymentRequest(),
            _DeploymentClient(),
        )

    entry.stage = ModelStage.STAGING
    active = SimpleNamespace(tags={"registry_entry_id": str(entry.id)})
    with pytest.raises(HTTPException, match="already has an active deployment"):
        operations_service.deploy_registered_model(
            _DeploymentDB(SimpleNamespace(), [active]),
            SimpleNamespace(),
            project_id,
            entry.id,
            ModelDeploymentRequest(),
            _DeploymentClient(),
        )

    with pytest.raises(HTTPException, match="Project not found"):
        operations_service.deploy_registered_model(
            _DeploymentDB(None),
            SimpleNamespace(),
            project_id,
            entry.id,
            ModelDeploymentRequest(),
            _DeploymentClient(),
        )


def test_platform_health_reports_each_unavailable_dependency(monkeypatch) -> None:
    capacity = ClusterCapacityRead(
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
    )
    snapshot = SimpleNamespace(
        capacity=capacity,
        pvc_ready=False,
        priority_class_ready=False,
        runtime_dependencies_ready=False,
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(
        operations_service,
        "get_object_store",
        lambda: SimpleNamespace(healthcheck=lambda: (_ for _ in ()).throw(RuntimeError("down"))),
    )

    result = operations_service.platform_health(
        SimpleNamespace(scalar=lambda _: 2),
        SimpleNamespace(),
        uuid.uuid4(),
        SimpleNamespace(capacity_snapshot=lambda: snapshot),
    )

    assert result.active_deployments == 2
    assert set(result.components.values()) == {"ok", "unavailable"}
    assert result.components["object_store"] == "unavailable"


@pytest.mark.parametrize(
    ("runtime_state", "expected_code"),
    [
        ("missing", "KUBERNETES_DEPLOYMENT_MISSING"),
        ("image_pull_error", "INFERENCE_IMAGE_PULL_FAILED"),
        ("crash_loop", "INFERENCE_CONTAINER_CRASH_LOOP"),
        ("configuration_error", "INFERENCE_CONTAINER_CONFIGURATION_FAILED"),
        ("out_of_memory", "INFERENCE_CONTAINER_OUT_OF_MEMORY"),
    ],
)
def test_deployment_listing_persists_terminal_runtime_failures(
    monkeypatch,
    runtime_state,
    expected_code,
) -> None:
    run = _deployment_run()
    db = _SequenceSession(scalars=[[run]])
    client = SimpleNamespace(
        settings=Settings(training_namespace="fallback"),
        model_deployment_state=lambda _: runtime_state,
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)

    result = list_model_deployments(db, SimpleNamespace(), run.project_id, client)

    assert result[0].status == RunStatus.FAILED
    assert run.failure_code == expected_code


def test_deployment_listing_degrades_when_cluster_lookup_fails(monkeypatch) -> None:
    run = _deployment_run()
    db = _SequenceSession(scalars=[[run]])
    client = SimpleNamespace(
        settings=Settings(training_namespace="fallback"),
        model_deployment_state=lambda _: (_ for _ in ()).throw(RuntimeError("down")),
    )
    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)

    result = list_model_deployments(db, SimpleNamespace(), run.project_id, client)

    assert result[0].runtime_state == "unavailable"
    assert result[0].status == RunStatus.RUNNING


def test_drift_listing_syncs_active_runs_and_stops_after_api_failure(monkeypatch) -> None:
    active = _deployment_run(status=RunStatus.RUNNING)
    active.run_kind = RunKind.DRIFT
    queued = _deployment_run(status=RunStatus.QUEUED)
    queued.run_kind = RunKind.DRIFT
    db = _SequenceSession(scalars=[[active, queued]])
    calls = []

    def fail_first(_db, run, _client):
        calls.append(run.id)
        raise operations_service.ApiException(status=503)

    monkeypatch.setattr(operations_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(operations_service, "_sync_run_status", fail_first)

    result = operations_service.list_drift_runs(
        db, SimpleNamespace(), active.project_id, SimpleNamespace()
    )

    assert result == [active, queued]
    assert calls == [active.id]
