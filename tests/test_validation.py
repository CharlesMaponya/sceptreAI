from __future__ import annotations

import io
from types import SimpleNamespace

import joblib
import pandas as pd
from automl_api.models.enums import TaskType
from automl_api.training import analysis, pipeline
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor


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
