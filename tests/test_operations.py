from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from automl_api.api.routes.operations import router
from automl_api.core.config import Settings
from automl_api.inference import app as inference_app
from automl_api.inference.app import predict_records
from automl_api.models.enums import GlobalRole, ModelStage, RunKind
from automl_api.schemas.operations import ArtifactCleanupRequest
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.operations import (
    _adaptive_inference_memory,
    _generated_model_dockerfile,
    cleanup_project_resources,
    update_registry_stage,
)
from automl_api.services.training import BATCH_RUN_KINDS
from automl_api.storage.object_store import EmbeddedObjectStore
from automl_api.training.analysis import _drift_summary
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
        "/projects/{project_id}/operations/deployments/{run_id}/stop",
        "/projects/{project_id}/operations/cleanup",
    }.issubset(paths)


def test_long_lived_deployments_do_not_consume_batch_training_slots() -> None:
    assert RunKind.DEPLOYMENT not in BATCH_RUN_KINDS
    assert RunKind.DRIFT in BATCH_RUN_KINDS


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
    assert container["image"] == "automl-inference@sha256:abc"
    assert container["startupProbe"]["httpGet"]["path"] == "/health/ready"
    environment = {
        item["name"]: item["value"]
        for item in container["env"]
        if "value" in item
    }
    assert environment["PROJECT_NAME"] == "Credit Risk"
    assert environment["DEPLOYMENT_ENVIRONMENT"] == "staging"
    assert manifests["service"]["spec"]["type"] == "NodePort"


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
    client.settings = Settings(training_namespace="automl")
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
        list_node=lambda: SimpleNamespace(
            items=[
                SimpleNamespace(
                    status=SimpleNamespace(
                        addresses=[
                            SimpleNamespace(
                                type="InternalIP",
                                address="192.168.49.2",
                            )
                        ]
                    )
                )
            ]
        ),
    )

    assert client.model_deployment_urls("model") == {
        "base_url": "http://192.168.49.2:31234",
        "endpoint": "http://192.168.49.2:31234/v1/predict",
        "docs_url": "http://192.168.49.2:31234/docs",
        "openapi_url": "http://192.168.49.2:31234/openapi.json",
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
