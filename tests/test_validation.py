from __future__ import annotations

import io
import sys
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import automl_api.services.validation as validation_service
import joblib
import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import ArtifactKind, RunKind, RunStatus, TaskType
from automl_api.models.runs import ModelRun, RunArtifact
from automl_api.schemas.training import ClusterCapacityRead, TrainingEstimateRead
from automl_api.schemas.validation import ExplainabilityLaunchRequest, ValidationLaunchRequest
from automl_api.services.validation import _reusable_explainability_run
from automl_api.training import analysis, pipeline
from fastapi import HTTPException
from sklearn.cluster import AgglomerativeClustering
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer


def test_external_classification_validation_returns_diagnostics(
    monkeypatch,
) -> None:
    training = pd.DataFrame(
        {
            "x": list(range(20)),
            "target": ["a", "b"] * 10,
        }
    )
    model = RandomForestClassifier(n_estimators=10, random_state=42).fit(
        training[["x"]],
        training["target"],
    )
    monkeypatch.setattr(analysis, "_load_model", lambda _: model)
    monkeypatch.setattr(
        analysis,
        "_load_dataframe",
        lambda _: training.copy(),
    )
    run = SimpleNamespace(
        task_type=TaskType.CLASSIFICATION,
        target_column="target",
        params={},
    )

    result = analysis._execute_validation(run, SimpleNamespace())

    assert "balanced_accuracy" in result["metrics"]
    assert result["diagnostics"]["external_rows"] == 20
    assert result["diagnostics"]["confusion_matrix"]


def test_external_regression_validation_returns_residuals(
    monkeypatch,
) -> None:
    training = pd.DataFrame(
        {
            "x": list(range(20)),
            "target": [float(value * 2) for value in range(20)],
        }
    )
    model = RandomForestRegressor(n_estimators=10, random_state=42).fit(
        training[["x"]],
        training["target"],
    )
    monkeypatch.setattr(analysis, "_load_model", lambda _: model)
    monkeypatch.setattr(
        analysis,
        "_load_dataframe",
        lambda _: training.copy(),
    )
    run = SimpleNamespace(
        task_type=TaskType.REGRESSION,
        target_column="target",
        params={},
    )

    result = analysis._execute_validation(run, SimpleNamespace())

    assert "rmse" in result["metrics"]
    assert result["diagnostics"]["prediction_samples"]
    assert result["diagnostics"]["external_rows"] == 20


def test_model_loading_falls_back_to_minio_mirror(monkeypatch) -> None:
    buffer = io.BytesIO()
    joblib.dump({"model": "persisted"}, buffer)
    store = SimpleNamespace(read_bytes=lambda _: buffer.getvalue())
    monkeypatch.setattr(analysis, "get_object_store", lambda: store)
    monkeypatch.setattr(
        analysis.mlflow_sklearn,
        "load_model",
        lambda _: (_ for _ in ()).throw(RuntimeError("artifact missing")),
    )
    run = SimpleNamespace(
        params={
            "model_mlflow_run_id": "missing-run",
            "model_artifact_uri": "minio://automl/model.joblib",
        }
    )

    assert analysis._load_model(run) == {"model": "persisted"}


def test_candidate_model_is_mirrored_with_stable_key(monkeypatch) -> None:
    captured = {}

    def put_bytes(key, content):
        captured.update(key=key, content=content)
        return SimpleNamespace(uri=f"minio://automl/{key}")

    monkeypatch.setattr(
        pipeline,
        "get_object_store",
        lambda: SimpleNamespace(put_bytes=put_bytes),
    )
    run = SimpleNamespace(project_id="project", id="run")

    uri = pipeline._persist_candidate_model(
        run,
        "Model With Spaces",
        {"fitted": True},
    )

    assert uri.endswith("/models/Model-With-Spaces.joblib")
    assert joblib.load(io.BytesIO(captured["content"])) == {"fitted": True}


def test_shap_encoding_round_trips_mixed_features() -> None:
    features = pd.DataFrame(
        {
            "amount": [1.5, 2.5, 3.5],
            "segment": ["retail", "business", None],
        }
    )

    encoded, decode = analysis._encode_shap_features(features)
    restored = decode(encoded.to_numpy(dtype=float))

    assert all(pd.api.types.is_numeric_dtype(encoded[column]) for column in encoded.columns)
    assert restored["amount"].tolist() == features["amount"].tolist()
    assert restored["segment"].iloc[:2].tolist() == ["retail", "business"]
    assert pd.isna(restored["segment"].iloc[2])


def test_shap_feature_importance_is_normalized_to_percentage() -> None:
    normalized = analysis.normalize_feature_importance(
        [
            {"feature": "small", "mean_absolute_shap": 1.0},
            {"feature": "large", "mean_absolute_shap": 3.0},
        ]
    )

    assert [item["feature"] for item in normalized] == ["large", "small"]
    assert [item["contribution_percent"] for item in normalized] == [75.0, 25.0]
    assert sum(item["contribution_percent"] for item in normalized) == 100.0


def test_shap_sample_contributions_normalize_each_output_across_features() -> None:
    values = np.asarray(
        [
            [[-1.0, 2.0], [3.0, -2.0]],
            [[0.0, 0.0], [0.0, 0.0]],
        ]
    )

    percentages = analysis._percentage_contributions(values, feature_axis=1)

    assert percentages.min() == 0.0
    assert percentages.max() <= 100.0
    np.testing.assert_allclose(percentages[0].sum(axis=0), [100.0, 100.0])
    np.testing.assert_allclose(percentages[1].sum(axis=0), [0.0, 0.0])


def test_explainability_result_includes_global_and_sample_percentages(
    monkeypatch,
) -> None:
    class Explainer:
        def __init__(self, *_args, **_kwargs):
            pass

        def __call__(self, values, **_kwargs):
            return SimpleNamespace(
                values=np.asarray(
                    [
                        [-1.0, 3.0],
                        [2.0, -2.0],
                        [0.0, 4.0],
                        [1.0, 1.0],
                    ][: len(values)]
                )
            )

    frame = pd.DataFrame(
        {
            "first": [1.0, 2.0, 3.0, 4.0],
            "second": [4.0, 3.0, 2.0, 1.0],
            "target": [2.0, 4.0, 6.0, 8.0],
        }
    )
    monkeypatch.setitem(sys.modules, "shap", SimpleNamespace(Explainer=Explainer))
    monkeypatch.setattr(
        analysis,
        "_load_model",
        lambda *_: SimpleNamespace(predict=lambda values: values["first"].to_numpy()),
    )
    monkeypatch.setattr(analysis, "_load_dataframe", lambda _: frame.copy())
    run = SimpleNamespace(
        target_column="target",
        params={"max_rows": 4},
        task_type=TaskType.REGRESSION,
    )

    result = analysis._execute_explainability(run, SimpleNamespace())

    assert sum(
        item["contribution_percent"] for item in result["feature_importance"]
    ) == pytest.approx(100.0)
    sample_percentages = np.asarray(result["shap_contribution_percent"])
    np.testing.assert_allclose(sample_percentages.sum(axis=1), [100.0] * 4)
    assert result["diagnostics"]["contribution_normalization"]["scale"] == "percent"


def test_completed_historical_explanation_is_reused() -> None:
    project_id = uuid.uuid4()
    source_id = uuid.uuid4()
    completed = SimpleNamespace(
        status=RunStatus.SUCCEEDED,
        tags={"source_training_run_id": str(source_id)},
        params={"model_name": "RandomForestClassifier"},
    )
    failed = SimpleNamespace(
        status=RunStatus.FAILED,
        tags={"source_training_run_id": str(source_id)},
        params={"model_name": "RandomForestClassifier"},
    )
    db = SimpleNamespace(scalars=lambda _: SimpleNamespace(all=lambda: [failed, completed]))
    source = SimpleNamespace(project_id=project_id, id=source_id)

    result = _reusable_explainability_run(
        db,
        source,
        "RandomForestClassifier",
    )

    assert result is completed


def test_failed_training_run_allows_shap_for_successful_candidate(monkeypatch) -> None:
    source = SimpleNamespace(
        status=RunStatus.FAILED,
        tags={
            "leaderboard": [
                {"model": "RandomForestClassifier", "status": "succeeded", "metrics": {}}
            ]
        },
    )
    monkeypatch.setattr(validation_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(validation_service, "_training_run", lambda *_: source)
    monkeypatch.setattr(validation_service, "_leaderboard_parent", lambda _, run: run)

    selected_source, entry = validation_service._source_model(
        SimpleNamespace(),
        SimpleNamespace(),
        uuid.uuid4(),
        uuid.uuid4(),
        "RandomForestClassifier",
        require_artifact=False,
        require_source_complete=False,
    )

    assert selected_source is source
    assert entry["status"] == "succeeded"


def test_external_validation_rejects_missing_training_columns() -> None:
    source = SimpleNamespace(
        dataset_version=SimpleNamespace(
            schema_json={
                "columns": [{"name": "age"}, {"name": "income"}, {"name": "target"}],
            }
        )
    )
    external = SimpleNamespace(
        schema_json={
            "columns": [{"name": "age"}, {"name": "target"}],
        }
    )

    with pytest.raises(HTTPException, match="Missing columns: income"):
        validation_service._require_matching_validation_columns(source, external)


def test_non_predictive_cluster_model_uses_fitted_centroids() -> None:
    features = pd.DataFrame(
        {
            "x": [0.0, 0.1, 10.0, 10.1],
            "y": [0.0, 0.2, 10.0, 10.2],
        }
    )
    estimator = AgglomerativeClustering(n_clusters=2).fit(features)
    model = Pipeline(
        [
            ("prepare", FunctionTransformer()),
            ("model", estimator),
        ]
    )

    predictor = analysis._clustering_predictor(model, features)
    predictions = predictor(features)

    assert len(predictions) == len(features)
    assert len(set(predictions)) == 2


def _source_run() -> ModelRun:
    now = datetime.now(UTC)
    return ModelRun(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        run_kind=RunKind.TRAINING,
        status=RunStatus.SUCCEEDED,
        task_type=TaskType.CLASSIFICATION,
        target_column="target",
        mlflow_run_id="winner-run",
        params={"positive_label": "yes"},
        tags={
            "winner": "LogisticRegression",
            "leaderboard": [
                {
                    "model": "LogisticRegression",
                    "status": "succeeded",
                    "model_artifact_uri": "s3://models/model.joblib",
                }
            ],
        },
        created_at=now,
        updated_at=now,
    )


def _estimate(*, can_launch: bool = True) -> TrainingEstimateRead:
    return TrainingEstimateRead(
        capacity=ClusterCapacityRead(
            connected=True,
            source="test",
            total_cpu_cores=4,
            requested_cpu_cores=0,
            available_cpu_cores=4,
            total_memory_mb=8192,
            requested_memory_mb=0,
            available_memory_mb=8192,
            ready_nodes=1,
            gpu_available=False,
            active_training_jobs=0,
        ),
        estimated_working_set_mb=256,
        cpu_request_cores=1,
        cpu_limit_cores=2,
        memory_request_mb=512,
        memory_limit_mb=1024,
        gpu_requested=False,
        expected_minutes=5,
        active_deadline_seconds=600,
        estimated_core_hours=0.1,
        max_concurrent_jobs=15,
        can_launch=can_launch,
        blockers=[] if can_launch else ["capacity"],
    )


class _AnalysisDB:
    def __init__(self):
        self.added = []
        self.flushes = 0

    def add(self, value):
        now = datetime.now(UTC)
        value.id = value.id or uuid.uuid4()
        value.created_at = value.created_at or now
        value.updated_at = value.updated_at or now
        self.added.append(value)

    def flush(self):
        self.flushes += 1


class _AnalysisClient:
    def __init__(self, *, error: Exception | None = None):
        self.settings = SimpleNamespace(training_namespace="analysis")
        self.error = error
        self.created = []

    def build_job_manifest(self, *, run_id, project_id, estimate):
        return {"metadata": {"name": f"analysis-{run_id}"}}

    def create_job(self, manifest):
        if self.error:
            raise self.error
        self.created.append(manifest)


def test_analysis_launch_persists_desired_state_without_inline_side_effect(monkeypatch) -> None:
    source = _source_run()
    version = SimpleNamespace(id=uuid.uuid4())
    db = _AnalysisDB()
    client = _AnalysisClient()
    monkeypatch.setattr(validation_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(validation_service, "estimate_training_run", lambda *_: _estimate())

    result = validation_service._launch_analysis_run(
        db,
        SimpleNamespace(id=uuid.uuid4()),
        source,
        version,
        source.tags["leaderboard"][0],
        RunKind.VALIDATION,
        expected_minutes=5,
        extra_params={"evaluation_column": None},
        client=client,
    )

    assert result.run.status == RunStatus.QUEUED
    assert result.run.params["model_mlflow_run_id"] == "winner-run"
    assert result.manifest["metadata"]["name"].startswith("analysis-")
    assert db.added[0].tags["desired_state"] == "kubernetes_submission_pending"
    assert client.created == []


def test_analysis_launch_rejects_precheck_without_contacting_cluster(monkeypatch) -> None:
    source = _source_run()
    version = SimpleNamespace(id=uuid.uuid4())
    monkeypatch.setattr(validation_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(
        validation_service, "estimate_training_run", lambda *_: _estimate(can_launch=False)
    )
    with pytest.raises(HTTPException, match="Analysis precheck failed"):
        validation_service._launch_analysis_run(
            _AnalysisDB(),
            SimpleNamespace(id=uuid.uuid4()),
            source,
            version,
            source.tags["leaderboard"][0],
            RunKind.VALIDATION,
            expected_minutes=5,
            extra_params={},
            client=_AnalysisClient(),
        )

    client = _AnalysisClient(error=RuntimeError("cluster rejected"))
    assert client.created == []


def test_validation_launch_enforces_target_and_evaluation_columns(monkeypatch) -> None:
    source = _source_run()
    entry = source.tags["leaderboard"][0]
    missing_target = SimpleNamespace(id=uuid.uuid4(), schema_json={"columns": [{"name": "x"}]})
    monkeypatch.setattr(validation_service, "_source_model", lambda *_: (source, entry))
    monkeypatch.setattr(validation_service, "_dataset_version", lambda *_: missing_target)
    monkeypatch.setattr(validation_service, "_require_matching_validation_columns", lambda *_: None)
    request = ValidationLaunchRequest(
        model_name="LogisticRegression",
        dataset_version_id=missing_target.id,
    )
    with pytest.raises(HTTPException, match="does not contain target"):
        validation_service.launch_validation_run(
            SimpleNamespace(), SimpleNamespace(), source.project_id, source.id, request
        )

    source.target_column = None
    request.evaluation_column = "segment"
    with pytest.raises(HTTPException, match="evaluation column is missing"):
        validation_service.launch_validation_run(
            SimpleNamespace(), SimpleNamespace(), source.project_id, source.id, request
        )


def test_analysis_queries_are_source_scoped(monkeypatch) -> None:
    source = _source_run()
    matching = SimpleNamespace(tags={"source_training_run_id": str(source.id)})
    foreign = SimpleNamespace(tags={"source_training_run_id": str(uuid.uuid4())})
    db = SimpleNamespace(
        scalars=lambda _: SimpleNamespace(all=lambda: [matching, foreign]),
        scalar=lambda _: None,
    )
    monkeypatch.setattr(validation_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(validation_service, "_training_run", lambda *_: source)
    monkeypatch.setattr(validation_service, "_leaderboard_parent", lambda _, run: run)

    assert validation_service.list_analysis_runs(
        db, SimpleNamespace(), source.project_id, source.id
    ) == [matching]
    with pytest.raises(HTTPException, match="run not found"):
        validation_service.get_analysis_result(
            db, SimpleNamespace(), source.project_id, source.id, uuid.uuid4()
        )


def test_analysis_result_normalizes_features_and_artifacts(monkeypatch) -> None:
    source = _source_run()
    now = datetime.now(UTC)
    run = ModelRun(
        id=uuid.uuid4(),
        project_id=source.project_id,
        dataset_version_id=source.dataset_version_id,
        created_by_id=source.created_by_id,
        run_kind=RunKind.EXPLAINABILITY,
        status=RunStatus.SUCCEEDED,
        task_type=source.task_type,
        params={"model_name": "LogisticRegression"},
        tags={
            "source_training_run_id": str(source.id),
            "metrics": {"accuracy": 0.9},
            "feature_importance": [{"feature": "x", "mean_absolute_shap": 2.0}],
        },
        created_at=now,
        updated_at=now,
    )
    artifact = RunArtifact(
        id=uuid.uuid4(),
        project_id=run.project_id,
        model_run_id=run.id,
        kind=ArtifactKind.SHAP_VALUES,
        name="shap.json",
        object_uri="s3://analysis/shap.json",
        artifact_metadata={},
        created_at=now,
        updated_at=now,
    )

    class DB:
        def scalar(self, _):
            return run

        def scalars(self, _):
            return SimpleNamespace(all=lambda: [artifact])

    monkeypatch.setattr(validation_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(validation_service, "_training_run", lambda *_: source)
    monkeypatch.setattr(validation_service, "_leaderboard_parent", lambda _, value: value)

    result = validation_service.get_analysis_result(
        DB(), SimpleNamespace(), source.project_id, source.id, run.id
    )
    assert result.model_name == "LogisticRegression"
    assert result.feature_importance[0]["contribution_percent"] == 100.0
    assert result.artifacts[0].name == "shap.json"


def test_validation_lookup_helpers_fail_closed(monkeypatch) -> None:
    project_id, item_id = uuid.uuid4(), uuid.uuid4()
    db = SimpleNamespace(scalar=lambda _: None)
    with pytest.raises(HTTPException, match="Training run not found"):
        validation_service._training_run(db, project_id, item_id)
    with pytest.raises(HTTPException, match="Dataset version not found"):
        validation_service._dataset_version(db, project_id, item_id)

    version = SimpleNamespace(id=item_id, object_uri="s3://missing", schema_json={})
    monkeypatch.setattr(
        validation_service, "get_object_store", lambda: SimpleNamespace(exists=lambda _: False)
    )
    with pytest.raises(HTTPException, match="missing from object storage"):
        validation_service._dataset_version(
            SimpleNamespace(scalar=lambda _: version), project_id, item_id
        )


def test_source_model_rejects_incomplete_missing_and_unpersisted_candidates(monkeypatch) -> None:
    source = _source_run()
    monkeypatch.setattr(validation_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(validation_service, "_training_run", lambda *_: source)
    monkeypatch.setattr(validation_service, "_leaderboard_parent", lambda _, run: run)
    source.status = RunStatus.RUNNING
    with pytest.raises(HTTPException, match="must be complete"):
        validation_service._source_model(
            SimpleNamespace(), SimpleNamespace(), source.project_id, source.id, "missing"
        )

    source.status = RunStatus.SUCCEEDED
    with pytest.raises(HTTPException, match="completed successfully"):
        validation_service._source_model(
            SimpleNamespace(), SimpleNamespace(), source.project_id, source.id, "missing"
        )

    source.tags["leaderboard"][0].pop("model_artifact_uri")
    source.mlflow_run_id = None
    with pytest.raises(HTTPException, match="no persisted artifact"):
        validation_service._source_model(
            SimpleNamespace(),
            SimpleNamespace(),
            source.project_id,
            source.id,
            "LogisticRegression",
        )


def test_explainability_launch_reuses_active_attempt(monkeypatch) -> None:
    source = _source_run()
    existing = _source_run()
    existing.run_kind = RunKind.EXPLAINABILITY
    existing.status = RunStatus.RUNNING
    existing.gpu_requested = False
    entry = source.tags["leaderboard"][0]
    monkeypatch.setattr(
        validation_service,
        "_source_model",
        lambda *_args, **_kwargs: (source, entry),
    )
    monkeypatch.setattr(validation_service, "_lock_training_admission", lambda *_: None)
    monkeypatch.setattr(validation_service, "_reusable_explainability_run", lambda *_: existing)

    result = validation_service.launch_explainability_run(
        SimpleNamespace(),
        SimpleNamespace(),
        source.project_id,
        source.id,
        ExplainabilityLaunchRequest(model_name="LogisticRegression", force=True),
    )

    assert result.cached is True
    assert result.run.id == existing.id
