from __future__ import annotations

import io
import sys
import uuid
from types import SimpleNamespace

import joblib
import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import RunStatus, TaskType
from automl_api.services.validation import _reusable_explainability_run
from automl_api.training import analysis, pipeline
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
    monkeypatch.setattr(analysis, "_load_model", lambda *_: SimpleNamespace(
        predict=lambda values: values["first"].to_numpy()
    ))
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
