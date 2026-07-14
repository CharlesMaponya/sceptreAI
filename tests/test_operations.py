from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import automl_api.services.operations as operations_service
import numpy as np
import pandas as pd
import pytest
from automl_api.api.routes.operations import router
from automl_api.core.config import Settings
from automl_api.inference import app as inference_app
from automl_api.inference.app import predict_records
from automl_api.models.enums import (
    GlobalRole,
    ModelStage,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.runs import ModelRun
from automl_api.schemas.operations import ArtifactCleanupRequest, DriftLaunchRequest
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.operations import (
    _adaptive_inference_memory,
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
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sklearn.linear_model import LinearRegression


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
        dataset_version=SimpleNamespace(schema_json={
            "columns": [{"name": "age"}, {"name": "income"}, {"name": "target"}],
        }),
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

    assert dockerfile.startswith(
        "FROM registry.example/inference@sha256:abc123\n"
    )
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
    client = TestClient(inference_app.app)

    ready = client.get("/health/ready")
    root = client.get("/", follow_redirects=False)
    docs = client.get("/docs")
    openapi = client.get("/openapi.json")
    online = client.post(
        "/v1/predict/online",
        json={"record": {"feature": 1.0}},
    )
    response = client.post(
        "/v1/predict",
        json={"records": [{"feature": 1.0}, {"feature": 2.0}]},
    )

    assert ready.status_code == 200
    assert root.status_code == 307
    assert root.headers["location"] == "/docs"
    assert docs.status_code == 200
    assert openapi.status_code == 200
    assert "/v1/predict" in openapi.json()["paths"]
    assert "/v1/predict/online" in openapi.json()["paths"]
    assert "/v1/predict/offline" in openapi.json()["paths"]
    assert online.status_code == 200
    assert online.json()["prediction"] == 1
    assert response.status_code == 200
    assert response.json()["predictions"] == [2, 2]


def test_inference_offline_upload_returns_prediction_file(monkeypatch) -> None:
    class Model:
        def predict(self, frame):
            return np.asarray([value * 2 for value in frame["feature"]])

    monkeypatch.setattr(inference_app, "_load_model", lambda: Model())
    client = TestClient(inference_app.create_app())

    response = client.post(
        "/v1/predict/offline",
        files={"file": ("scoring.csv", b"feature\n2\n4\n", "text/csv")},
        data={"include_probabilities": "false"},
    )

    assert response.status_code == 200
    assert response.headers["x-prediction-row-count"] == "2"
    assert "scoring-predictions.csv" in response.headers["content-disposition"]
    assert response.text.splitlines() == [
        "feature,prediction",
        "2,4",
        "4,8",
    ]


def test_inference_offline_upload_rejects_unknown_file_type(
    monkeypatch,
) -> None:
    monkeypatch.setattr(inference_app, "_load_model", lambda: object())
    client = TestClient(inference_app.create_app())

    response = client.post(
        "/v1/predict/offline",
        files={"file": ("scoring.xlsx", b"invalid", "application/octet-stream")},
    )

    assert response.status_code == 422
    assert "Unsupported file type" in response.json()["detail"]


def test_inference_docs_use_project_and_environment_not_platform_name(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROJECT_NAME", "Credit Risk")
    monkeypatch.setenv("DEPLOYMENT_ENVIRONMENT", "staging")
    monkeypatch.setenv("MODEL_NAME", "ExtraTreesClassifier")
    client = TestClient(inference_app.create_app())

    openapi = client.get("/openapi.json").json()
    metadata = client.get("/v1/metadata")

    assert openapi["info"]["title"] == "Credit Risk model API (staging)"
    assert "Sceptre" not in openapi["info"]["title"]
    assert metadata.json() == {
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
    environment = {
        item["name"]: item["value"]
        for item in container["env"]
        if "value" in item
    }
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
        waiting = (
            SimpleNamespace(reason=waiting_reason)
            if waiting_reason
            else None
        )
        previous = (
            SimpleNamespace(reason=previous_reason)
            if previous_reason
            else None
        )
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
    base_url = (
        f"/api/v1/projects/{project_id}/operations/deployments/{run_id}/inference"
    )

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
        image="sceptre-inference:0.1.0",
        replicas=1,
        cpu_request="500m",
        memory_request="1Gi",
    )
    assert manifests["ingress"]["spec"]["ingressClassName"] == "nginx"
    assert (
        manifests["ingress"]["spec"]["rules"][0]["host"]
        == "automl-model-11111111.models.local"
    )
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
        spec=SimpleNamespace(
            rules=[SimpleNamespace(host="automl-model-11111111.models.local")]
        ),
    )
    client.networking = SimpleNamespace(
        read_namespaced_ingress_status=lambda **_: ingress
    )

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
    platform_base = (
        f"/api/v1/projects/{run.project_id}/operations/deployments/"
        f"{run.id}/inference"
    )
    assert deployment.platform_endpoint == f"{platform_base}/v1/predict"
    assert deployment.platform_online_endpoint == (
        f"{platform_base}/v1/predict/online"
    )
    assert deployment.platform_offline_endpoint == (
        f"{platform_base}/v1/predict/offline"
    )
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
