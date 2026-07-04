from __future__ import annotations

import hashlib
import io
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import joblib
import mlflow
import mlflow.sklearn as mlflow_sklearn
import numpy as np
import pandas as pd

from automl_api.core.config import get_settings
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    ArtifactKind,
    MetricKind,
    MetricSplit,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.runs import Metric, ModelRun, RunArtifact
from automl_api.storage.object_store import get_object_store
from automl_api.training.evaluation import (
    classification_evaluation,
    clustering_evaluation,
    metric_direction,
    regression_evaluation,
)
from automl_api.training.pipeline import (
    _load_dataframe,
    _normalize_temporal_features,
    _persist_candidate_model,
    rebuild_candidate_model,
)


def execute_analysis_run(run_id: uuid.UUID) -> dict[str, Any]:
    session_factory = get_session_factory()
    with session_factory() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Analysis run {run_id} was not found.")
        version = db.get(DatasetVersion, run.dataset_version_id)
        if version is None:
            raise ValueError("Analysis dataset version was not found.")
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(UTC)
        db.commit()

    try:
        if run.run_kind == RunKind.VALIDATION:
            result = _execute_validation(run, version)
        elif run.run_kind == RunKind.EXPLAINABILITY:
            result = _execute_explainability(run, version)
        elif run.run_kind == RunKind.DRIFT:
            result = _execute_drift(run, version)
        else:
            raise ValueError(f"Unsupported analysis kind: {run.run_kind.value}")
        _persist_analysis_result(run_id, result)
        return result
    except Exception as exc:
        _mark_analysis_failed(run_id, exc)
        raise


def _execute_validation(
    run: ModelRun,
    version: DatasetVersion,
) -> dict[str, Any]:
    model = _load_model(run)
    dataframe = _load_dataframe(version)
    if run.task_type == TaskType.CLUSTERING:
        evaluation_column = run.params.get("evaluation_column")
        reference_labels = None
        if evaluation_column:
            reference_labels = dataframe.pop(evaluation_column).to_numpy()
        features = _normalize_temporal_features(dataframe)
        if hasattr(model, "predict"):
            labels = model.predict(features)
            transformed = _transformed_features(model, features)
            mode = "external_predict"
        else:
            labels = model.fit_predict(features)
            transformed = _transformed_features(model, features)
            mode = "external_refit"
        metrics = clustering_evaluation(
            np.asarray(transformed),
            np.asarray(labels),
            reference_labels,
        )
        unique_labels, counts = np.unique(labels, return_counts=True)
        diagnostics = {
            "validation_mode": mode,
            "external_rows": len(features),
            "cluster_sizes": {
                str(label): int(count) for label, count in zip(unique_labels, counts, strict=True)
            },
            "external_evaluation": reference_labels is not None,
        }
    else:
        target_column = run.target_column
        if not target_column or target_column not in dataframe.columns:
            raise ValueError("The external target column is missing.")
        target = dataframe.pop(target_column)
        valid = target.notna()
        target = target.loc[valid]
        features = _normalize_temporal_features(dataframe.loc[valid])
        predictions = model.predict(features)
        if run.task_type == TaskType.CLASSIFICATION:
            metrics, diagnostics = classification_evaluation(
                model,
                features,
                target,
                predictions,
            )
        else:
            numeric_target = pd.to_numeric(target, errors="raise")
            metrics, diagnostics = regression_evaluation(
                numeric_target,
                numeric_target,
                predictions,
                run.task_type,
            )
        diagnostics["external_rows"] = len(features)
    return {
        "metrics": metrics,
        "diagnostics": diagnostics,
        "feature_importance": [],
    }


def _execute_explainability(
    run: ModelRun,
    version: DatasetVersion,
) -> dict[str, Any]:
    import shap

    model = _load_model(run, version)
    dataframe = _load_dataframe(version)
    excluded = {
        value
        for value in (
            run.target_column,
            run.params.get("evaluation_column"),
        )
        if value
    }
    features = _normalize_temporal_features(dataframe.drop(columns=list(excluded), errors="ignore"))
    max_rows = min(int(run.params.get("max_rows", 200)), len(features))
    if max_rows < 2:
        raise ValueError("At least two rows are required for SHAP analysis.")
    sample = features.sample(n=max_rows, random_state=42)
    background = features.sample(
        n=min(50, len(features)),
        random_state=17,
    )
    columns = list(features.columns)
    encoded_features, decode = _encode_shap_features(features)
    encoded_sample = encoded_features.loc[sample.index]
    encoded_background = encoded_features.loc[background.index]
    cluster_predict = (
        _clustering_predictor(model, features) if run.task_type == TaskType.CLUSTERING else None
    )

    def predict(values: np.ndarray) -> np.ndarray:
        frame = decode(values)
        if cluster_predict is not None:
            return np.asarray(cluster_predict(frame))
        if run.task_type == TaskType.CLASSIFICATION and hasattr(
            model,
            "predict_proba",
        ):
            return np.asarray(model.predict_proba(frame))
        return np.asarray(model.predict(frame))

    explainer = shap.Explainer(
        predict,
        encoded_background.to_numpy(dtype=float),
        feature_names=columns,
        algorithm="permutation",
    )
    explanation = explainer(
        encoded_sample.to_numpy(dtype=float),
        max_evals=max(2 * len(columns) + 1, 10),
    )
    values = np.asarray(explanation.values, dtype=float)
    if values.ndim == 1:
        values = values.reshape(-1, 1)
    if values.ndim == 3:
        mean_absolute = np.mean(np.abs(values), axis=(0, 2))
    else:
        mean_absolute = np.mean(np.abs(values), axis=0)
    contribution_percent = _percentage_contributions(mean_absolute, feature_axis=0)
    feature_importance = sorted(
        [
            {
                "feature": column,
                "mean_absolute_shap": float(value),
                "contribution_percent": float(percent),
            }
            for column, value, percent in zip(
                columns,
                mean_absolute,
                contribution_percent,
                strict=True,
            )
        ],
        key=lambda item: item["contribution_percent"],
        reverse=True,
    )
    sample_values = values[: min(100, len(values))]
    return {
        "metrics": {},
        "diagnostics": {
            "sample_rows": max_rows,
            "background_rows": len(background),
            "feature_count": len(columns),
            "algorithm": "permutation",
            "model_reconstructed": bool(run.params.get("model_reconstructed")),
            "contribution_normalization": {
                "scale": "percent",
                "minimum": 0.0,
                "maximum": 100.0,
                "feature_importance_sum": float(np.sum(contribution_percent)),
                "direction_preserved_in": "shap_values",
            },
        },
        "feature_importance": feature_importance,
        "shap_values": sample_values.tolist(),
        "shap_contribution_percent": _percentage_contributions(
            sample_values,
            feature_axis=1,
        ).tolist(),
    }


def _execute_drift(
    run: ModelRun,
    current_version: DatasetVersion,
) -> dict[str, Any]:
    reference_version_id = run.params.get("reference_dataset_version_id")
    if not reference_version_id:
        raise ValueError("A reference dataset version is required for drift analysis.")
    with get_session_factory()() as db:
        reference_version = db.get(
            DatasetVersion,
            uuid.UUID(str(reference_version_id)),
        )
        if reference_version is None or reference_version.project_id != run.project_id:
            raise ValueError("The reference dataset version was not found.")
    reference = _load_dataframe(reference_version)
    current = _load_dataframe(current_version)
    excluded = {
        value
        for value in (
            run.target_column,
            run.params.get("evaluation_column"),
        )
        if value
    }
    common_columns = [
        column
        for column in reference.columns
        if column in current.columns and column not in excluded
    ]
    if not common_columns:
        raise ValueError("Reference and current datasets have no common feature columns.")
    max_rows = max(100, int(run.params.get("max_rows", 10_000)))
    reference_sample = reference[common_columns].sample(
        n=min(max_rows, len(reference)),
        random_state=42,
    )
    current_sample = current[common_columns].sample(
        n=min(max_rows, len(current)),
        random_state=43,
    )
    report = _run_evidently_report(reference_sample, current_sample)
    metrics, summary = _drift_summary(report, len(common_columns))
    return {
        "metrics": metrics,
        "diagnostics": {
            **summary,
            "reference_rows": len(reference_sample),
            "current_rows": len(current_sample),
            "feature_count": len(common_columns),
            "reference_dataset_version_id": str(reference_version.id),
            "current_dataset_version_id": str(current_version.id),
            "evidently_report": report,
        },
        "feature_importance": [],
    }


def _run_evidently_report(
    reference: pd.DataFrame,
    current: pd.DataFrame,
) -> dict[str, Any]:
    try:
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report
    except ImportError:
        try:
            from evidently import Report
            from evidently.presets import DataDriftPreset
        except ImportError as exc:
            raise RuntimeError(
                "Evidently is not installed in the analysis worker image."
            ) from exc
    try:
        report = Report(metrics=[DataDriftPreset()])
    except TypeError:
        report = Report([DataDriftPreset()])
    snapshot = report.run(
        reference_data=reference,
        current_data=current,
    )
    if hasattr(snapshot, "as_dict"):
        return snapshot.as_dict()
    if hasattr(snapshot, "dict"):
        return snapshot.dict()
    if hasattr(report, "as_dict"):
        return report.as_dict()
    raise RuntimeError("Evidently did not return a serializable drift report.")


def _drift_summary(
    report: dict[str, Any],
    feature_count: int,
) -> tuple[dict[str, float], dict[str, Any]]:
    dataset_drift_value = _first_nested_value(report, "dataset_drift")
    drift_share = _first_nested_value(report, "share_of_drifted_columns")
    drifted_count = _first_nested_value(report, "number_of_drifted_columns")
    drift_by_columns = _first_nested_value(report, "drift_by_columns", {})
    drifted_features: list[str] = []
    if isinstance(drift_by_columns, dict):
        drifted_features = sorted(
            str(name)
            for name, details in drift_by_columns.items()
            if isinstance(details, dict)
            and bool(
                details.get("drift_detected")
                or details.get("drifted")
                or details.get("detected")
            )
        )
    if drifted_count is None:
        drifted_count = len(drifted_features)
    drifted_count = int(drifted_count)
    if drift_share is None:
        drift_share = drifted_count / max(1, feature_count)
    drift_share = float(drift_share)
    if drift_share > 1:
        drift_share /= 100.0
    drift_share = min(1.0, max(0.0, drift_share))
    dataset_drift = (
        bool(dataset_drift_value)
        if dataset_drift_value is not None
        else drift_share >= 0.5
    )
    return (
        {
            "dataset_drift": float(dataset_drift),
            "drift_share": drift_share,
            "drifted_feature_count": float(drifted_count),
        },
        {
            "dataset_drift": dataset_drift,
            "drift_share_percent": drift_share * 100.0,
            "drifted_feature_count": drifted_count,
            "drifted_features": drifted_features,
        },
    )


def _first_nested_value(
    value: Any,
    key: str,
    default: Any = None,
) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for nested in value.values():
            found = _first_nested_value(nested, key, None)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = _first_nested_value(nested, key, None)
            if found is not None:
                return found
    return default


def normalize_feature_importance(
    feature_importance: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not feature_importance:
        return []
    values = np.asarray(
        [item.get("mean_absolute_shap", 0.0) for item in feature_importance],
        dtype=float,
    )
    percentages = _percentage_contributions(values, feature_axis=0)
    normalized = [
        {
            **item,
            "contribution_percent": float(percent),
        }
        for item, percent in zip(feature_importance, percentages, strict=True)
    ]
    return sorted(
        normalized,
        key=lambda item: item["contribution_percent"],
        reverse=True,
    )


def _percentage_contributions(
    values: np.ndarray,
    *,
    feature_axis: int,
) -> np.ndarray:
    absolute = np.nan_to_num(
        np.abs(np.asarray(values, dtype=float)),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    totals = np.sum(absolute, axis=feature_axis, keepdims=True)
    normalized = np.divide(
        absolute,
        totals,
        out=np.zeros_like(absolute, dtype=float),
        where=totals > 0,
    )
    return np.clip(normalized * 100.0, 0.0, 100.0)


def _encode_shap_features(
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, Any]:
    encoded = pd.DataFrame(index=features.index)
    categories: dict[str, list[Any]] = {}
    numeric_columns: set[str] = set()
    for column in features.columns:
        series = features[column]
        if pd.api.types.is_numeric_dtype(series):
            encoded[column] = pd.to_numeric(series, errors="coerce")
            numeric_columns.add(column)
            continue
        category = pd.Categorical(series)
        categories[column] = list(category.categories)
        encoded[column] = category.codes.astype(float)

    columns = list(features.columns)

    def decode(values: np.ndarray) -> pd.DataFrame:
        numeric = pd.DataFrame(values, columns=columns)
        decoded = pd.DataFrame(index=numeric.index)
        for column in columns:
            if column in numeric_columns:
                decoded[column] = numeric[column]
                continue
            options = categories[column]
            codes = np.rint(numeric[column].to_numpy()).astype(int)
            decoded[column] = [
                options[code] if 0 <= code < len(options) else np.nan for code in codes
            ]
        return decoded

    return encoded, decode


def _load_model(
    run: ModelRun,
    version: DatasetVersion | None = None,
) -> Any:
    model_run_id = run.params.get("model_mlflow_run_id")
    mlflow_error: Exception | None = None
    if model_run_id:
        try:
            mlflow.set_tracking_uri(get_settings().mlflow_tracking_uri)
            return mlflow_sklearn.load_model(f"runs:/{model_run_id}/model")
        except Exception as exc:
            mlflow_error = exc
    model_artifact_uri = run.params.get("model_artifact_uri")
    if model_artifact_uri:
        return joblib.load(io.BytesIO(get_object_store().read_bytes(model_artifact_uri)))
    if run.run_kind == RunKind.EXPLAINABILITY and version is not None:
        return _rebuild_historical_model(run, version)
    if mlflow_error is not None:
        raise ValueError(
            f"MLflow model loading failed and no MinIO model mirror exists: {mlflow_error}"
        ) from mlflow_error
    raise ValueError("The source model has no persisted artifact.")


def _rebuild_historical_model(
    run: ModelRun,
    version: DatasetVersion,
) -> Any:
    source_run_id = run.params.get("source_training_run_id")
    model_name = str(run.params.get("model_name", ""))
    if not source_run_id or not model_name:
        raise ValueError("Historical model reconstruction metadata is missing.")
    with get_session_factory()() as db:
        source = db.get(ModelRun, uuid.UUID(str(source_run_id)))
        if source is None:
            raise ValueError("The historical source training run is missing.")
        source_version = db.get(DatasetVersion, source.dataset_version_id)
        if source_version is None:
            source_version = version
        entry = next(
            (
                item
                for item in source.tags.get("leaderboard", [])
                if item.get("model") == model_name and item.get("status") == "succeeded"
            ),
            None,
        )
        if entry is None:
            raise ValueError("The historical leaderboard entry is missing.")
        model = rebuild_candidate_model(
            _load_dataframe(source_version),
            task_type=source.task_type,
            target_column=source.target_column,
            model_name=model_name,
            best_params=dict(entry.get("best_params") or {}),
            evaluation_column=source.params.get("evaluation_column"),
        )
        artifact_uri = _persist_candidate_model(
            source,
            model_name,
            model,
        )
        source.tags = {
            **source.tags,
            "leaderboard": [
                (
                    {
                        **item,
                        "model_artifact_uri": artifact_uri,
                        "artifact_reconstructed": True,
                    }
                    if item.get("model") == model_name
                    else item
                )
                for item in source.tags.get("leaderboard", [])
            ],
        }
        persisted_run = db.get(ModelRun, run.id)
        if persisted_run is not None:
            persisted_run.params = {
                **persisted_run.params,
                "model_artifact_uri": artifact_uri,
                "model_reconstructed": True,
            }
        db.commit()
    run.params = {
        **run.params,
        "model_artifact_uri": artifact_uri,
        "model_reconstructed": True,
    }
    return model


def _clustering_predictor(
    model: Any,
    training_features: pd.DataFrame,
) -> Any:
    if hasattr(model, "predict"):
        return model.predict
    if not hasattr(model, "named_steps") or "model" not in model.named_steps:
        raise ValueError("The clustering model cannot assign perturbed samples.")
    estimator = model.named_steps["model"]
    labels = np.asarray(getattr(estimator, "labels_", []))
    transformed = _dense_array(_transformed_features(model, training_features))
    if len(labels) != len(transformed) or not len(labels):
        raise ValueError("The clustering model has no reusable fitted labels.")
    label_values = np.unique(labels)
    centroids = np.vstack([transformed[labels == label].mean(axis=0) for label in label_values])

    def predict(features: pd.DataFrame) -> np.ndarray:
        values = _dense_array(_transformed_features(model, features))
        distances = np.linalg.norm(
            values[:, np.newaxis, :] - centroids[np.newaxis, :, :],
            axis=2,
        )
        return label_values[np.argmin(distances, axis=1)]

    return predict


def _dense_array(values: Any) -> np.ndarray:
    if hasattr(values, "toarray"):
        values = values.toarray()
    return np.asarray(values, dtype=float)


def _transformed_features(model: Any, features: pd.DataFrame) -> Any:
    if hasattr(model, "named_steps") and "prepare" in model.named_steps:
        return model.named_steps["prepare"].transform(features)
    return features


def _persist_analysis_result(
    run_id: uuid.UUID,
    result: dict[str, Any],
) -> None:
    payload = json.dumps(result, default=_json_default).encode("utf-8")
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            return
        suffix = {
            RunKind.EXPLAINABILITY: "shap.json",
            RunKind.DRIFT: "drift.json",
        }.get(run.run_kind, "external-validation.json")
        key = f"projects/{run.project_id}/runs/{run.id}/{suffix}"
        stored = get_object_store().put_bytes(key, payload)
        artifact_kind = {
            RunKind.EXPLAINABILITY: ArtifactKind.SHAP_VALUES,
            RunKind.DRIFT: ArtifactKind.DRIFT_REPORT,
        }.get(run.run_kind, ArtifactKind.DIAGNOSTIC_PLOT)
        db.add(
            RunArtifact(
                project_id=run.project_id,
                model_run_id=run.id,
                kind=artifact_kind,
                name=suffix,
                object_uri=stored.uri,
                content_hash=hashlib.sha256(payload).hexdigest(),
                byte_size=len(payload),
                artifact_metadata={
                    "source_training_run_id": run.params.get("source_training_run_id"),
                    "model_name": run.params.get("model_name"),
                },
            )
        )
        for name, value in result.get("metrics", {}).items():
            db.add(
                Metric(
                    project_id=run.project_id,
                    model_run_id=run.id,
                    name=name,
                    kind=(
                        MetricKind.DRIFT
                        if run.run_kind == RunKind.DRIFT
                        else MetricKind.PERFORMANCE
                    ),
                    split=(
                        MetricSplit.PRODUCTION
                        if run.run_kind == RunKind.DRIFT
                        else MetricSplit.EXTERNAL
                    ),
                    value=float(value),
                    higher_is_better=metric_direction(name) == "maximize",
                )
            )
        run.tags = {
            **run.tags,
            "metrics": result.get("metrics", {}),
            "diagnostics": result.get("diagnostics", {}),
            "feature_importance": result.get("feature_importance", []),
            "artifact_uri": stored.uri,
        }
        run.status = RunStatus.SUCCEEDED
        run.finished_at = datetime.now(UTC)
        db.commit()


def _mark_analysis_failed(run_id: uuid.UUID, exc: Exception) -> None:
    message = str(exc)
    lower = message.lower()
    if "column" in lower or "feature" in lower:
        remediation = (
            "The validation feature space does not match the trained model. "
            "Use a dataset with the same input columns and compatible types."
        )
    elif "mlflow" in lower or "artifact" in lower:
        remediation = (
            "The model artifact could not be loaded from MLflow. Check MLflow "
            "connectivity and the candidate run artifact."
        )
    elif "memory" in lower or "oom" in lower:
        remediation = (
            "The analysis exceeded its memory budget. Reduce the SHAP sample "
            "size or use a smaller validation dataset."
        )
    else:
        remediation = (
            "Validation or explainability failed. Review dataset compatibility "
            "and the selected model artifact."
        )
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            return
        run.status = RunStatus.FAILED
        run.failure_code = "ANALYSIS_FAILED"
        run.failure_message = message[:4000]
        run.plain_english_failure = remediation
        run.finished_at = datetime.now(UTC)
        db.commit()


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return str(value)
