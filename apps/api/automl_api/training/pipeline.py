import io
import json
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import joblib
import mlflow
import mlflow.sklearn as mlflow_sklearn
import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.feature_selection import (
    SelectPercentile,
    mutual_info_classif,
    mutual_info_regression,
)
from sklearn.impute import SimpleImputer
from sklearn.model_selection import KFold, TimeSeriesSplit, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from skopt import BayesSearchCV
from zenml import pipeline, step

from automl_api.core.config import get_settings
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import MetricKind, MetricSplit, RunStatus, TaskType
from automl_api.models.runs import Metric, ModelRun
from automl_api.services.temporal import (
    UNIX_UNIT_SCALES,
    infer_unix_timestamp_unit,
)
from automl_api.storage.object_store import get_object_store
from automl_api.training.evaluation import (
    aggregate_fold_metrics,
    classification_evaluation,
    clustering_evaluation,
    metric_direction,
    regression_evaluation,
)
from automl_api.training.model_catalog import CandidateSpec, select_candidates


@dataclass
class TournamentResult:
    metrics: dict[str, float]
    model: Any
    params: dict[str, Any]
    leaderboard: list[dict[str, Any]]
    primary_metric: str


@step
def train_run_step(run_id: str) -> dict[str, float]:
    return execute_training_run(uuid.UUID(run_id))


@pipeline
def tabular_automl_pipeline(run_id: str) -> None:
    train_run_step(run_id=run_id)


def execute_training_run(run_id: uuid.UUID) -> dict[str, float]:
    session_factory = get_session_factory()
    with session_factory() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Model run {run_id} was not found.")
        version = db.get(DatasetVersion, run.dataset_version_id)
        if version is None:
            raise ValueError("Dataset version was not found.")
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(UTC)
        db.commit()

    try:
        dataframe = _load_dataframe(version)
        settings = get_settings()
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        mlflow.set_experiment(f"automl-project-{run.project_id}")
        with mlflow.start_run(run_name=run.run_name or str(run.id)) as mlflow_run:
            mlflow.set_tags(
                {
                    "project_id": str(run.project_id),
                    "dataset_version_id": str(run.dataset_version_id),
                    "automl_run_id": str(run.id),
                    "task_type": run.task_type.value,
                }
            )
            result = _fit_model(dataframe, run)
            mlflow.log_params(_json_safe(result.params))
            mlflow.log_metrics(result.metrics)
            mlflow.log_dict(
                {
                    "primary_metric": result.primary_metric,
                    "entries": result.leaderboard,
                },
                "leaderboard.json",
            )
            mlflow_sklearn.log_model(result.model, artifact_path="model")
            mlflow_run_id = mlflow_run.info.run_id

        with session_factory() as db:
            persisted_run = db.get(ModelRun, run_id)
            assert persisted_run is not None
            persisted_run.status = RunStatus.SUCCEEDED
            persisted_run.mlflow_run_id = mlflow_run_id
            persisted_run.finished_at = datetime.now(UTC)
            persisted_run.tags = {
                **persisted_run.tags,
                "winner": result.leaderboard[0]["model"],
                "winner_mlflow_run_id": result.leaderboard[0].get("mlflow_run_id"),
                "leaderboard_primary_metric": result.primary_metric,
                "leaderboard": result.leaderboard,
            }
            for name, value in result.metrics.items():
                db.add(
                    Metric(
                        project_id=persisted_run.project_id,
                        model_run_id=persisted_run.id,
                        name=name,
                        kind=MetricKind.PERFORMANCE,
                        split=MetricSplit.VALIDATION,
                        value=float(value),
                        higher_is_better=metric_direction(name) == "maximize",
                    )
                )
            db.commit()
        return result.metrics
    except Exception as exc:
        _mark_failed(run_id, exc)
        raise


def _load_dataframe(version: DatasetVersion) -> pd.DataFrame:
    content = get_object_store().read_bytes(version.object_uri)
    filename = (version.original_filename or "").lower()
    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content))
    if filename.endswith((".json", ".jsonl", ".ndjson")):
        return pd.read_json(
            io.BytesIO(content),
            lines=filename.endswith((".jsonl", ".ndjson")),
        )
    if filename.endswith(".parquet"):
        return pd.read_parquet(io.BytesIO(content))
    if filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(content))
    raise ValueError(f"Unsupported training dataset format: {filename}")


def _fit_model(dataframe: pd.DataFrame, run: ModelRun) -> TournamentResult:
    if run.task_type == TaskType.CLUSTERING:
        return _fit_clustering(dataframe, run)
    if not run.target_column or run.target_column not in dataframe.columns:
        raise ValueError("The configured target column is missing from the dataset.")

    target = dataframe[run.target_column]
    features = dataframe.drop(columns=[run.target_column])
    valid_target = target.notna()
    features = features.loc[valid_target]
    target = target.loc[valid_target]
    if len(features) < 10:
        raise ValueError("At least 10 rows with a non-missing target are required.")
    if run.task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
        target = pd.to_numeric(target, errors="raise")
    features = _normalize_temporal_features(features)

    train_x, test_x, train_y, test_y = _supervised_split(features, target, run.task_type)
    candidate_limit = int(run.params.get("candidate_limit", 5))
    requested_names = run.params.get("candidate_models")
    candidates = select_candidates(
        run.task_type,
        requested_names if isinstance(requested_names, list) else None,
        candidate_limit,
    )
    if not candidates:
        raise ValueError(f"No supported candidates are configured for {run.task_type.value}.")

    iterations = max(1, min(int(run.params.get("optimization_iterations", 5)), 25))
    cv_folds = max(2, min(int(run.params.get("cv_folds", 3)), 5))
    cv = _cross_validation_strategy(train_y, run.task_type, cv_folds)
    primary_metric = "balanced_accuracy" if run.task_type == TaskType.CLASSIFICATION else "rmse"
    scoring = (
        "balanced_accuracy"
        if run.task_type == TaskType.CLASSIFICATION
        else "neg_root_mean_squared_error"
    )
    leaderboard: list[dict[str, Any]] = []
    fitted: dict[str, tuple[Any, dict[str, Any], dict[str, float]]] = {}
    for candidate in candidates:
        entry = _fit_candidate(
            candidate,
            train_x,
            train_y,
            test_x,
            test_y,
            run.task_type,
            iterations,
            cv,
            scoring,
            run,
        )
        leaderboard.append(entry)
        if entry["status"] == "succeeded":
            fitted[candidate.name] = (
                entry.pop("_model"),
                entry["best_params"],
                entry["metrics"],
            )
        _persist_partial_leaderboard(run.id, leaderboard, primary_metric)

    leaderboard = rank_leaderboard(leaderboard, primary_metric)
    successful = [entry for entry in leaderboard if entry["status"] == "succeeded"]
    if not successful:
        failures = "; ".join(
            f"{entry['model']}: {entry.get('error', 'failed')}" for entry in leaderboard
        )
        raise RuntimeError(f"Every candidate model failed. {failures}")

    winner = successful[0]
    best_model, best_params, best_metrics = fitted[winner["model"]]
    return TournamentResult(
        metrics=best_metrics,
        model=best_model,
        params={"winner": winner["model"], **best_params},
        leaderboard=leaderboard,
        primary_metric=primary_metric,
    )


def _fit_candidate(
    candidate: CandidateSpec,
    train_x: pd.DataFrame,
    train_y: pd.Series,
    test_x: pd.DataFrame,
    test_y: pd.Series,
    task_type: TaskType,
    iterations: int,
    cv: Any,
    scoring: str,
    run: ModelRun,
) -> dict[str, Any]:
    started = time.monotonic()
    print(f"Training candidate {candidate.name}", flush=True)
    try:
        score_function = (
            mutual_info_classif if task_type == TaskType.CLASSIFICATION else mutual_info_regression
        )
        model = Pipeline(
            [
                ("prepare", _preprocessor(train_x)),
                ("select", SelectPercentile(score_func=score_function, percentile=80)),
                ("model", clone(candidate.estimator)),
            ]
        )
        if candidate.search_space:
            search = BayesSearchCV(
                model,
                candidate.search_space,
                n_iter=iterations,
                cv=cv,
                scoring=scoring,
                n_jobs=1,
                random_state=42,
                error_score="raise",
            )
            search.fit(train_x, train_y)
            fitted = search.best_estimator_
            params = _json_safe(search.best_params_)
            cv_mean = float(search.best_score_)
            cv_std = float(search.cv_results_["std_test_score"][search.best_index_])
        else:
            cv_scores = cross_val_score(
                model,
                train_x,
                train_y,
                cv=cv,
                scoring=scoring,
                n_jobs=1,
                error_score="raise",
            )
            model.fit(train_x, train_y)
            fitted = model
            params = {}
            cv_mean = float(np.mean(cv_scores))
            cv_std = float(np.std(cv_scores))

        predictions = fitted.predict(test_x)
        if task_type == TaskType.CLASSIFICATION:
            metrics, diagnostics = classification_evaluation(
                fitted,
                test_x,
                test_y,
                predictions,
            )
        else:
            metrics, diagnostics = regression_evaluation(
                train_y,
                test_y,
                predictions,
                task_type,
            )
        diagnostics["cross_validation"] = {
            "folds": int(cv.n_splits) if hasattr(cv, "n_splits") else int(cv),
            "scoring": scoring,
            "mean": cv_mean,
            "standard_deviation": cv_std,
        }
        duration = round(time.monotonic() - started, 3)
        with mlflow.start_run(run_name=candidate.name, nested=True) as candidate_run:
            mlflow.set_tags(
                {
                    "candidate_model": candidate.name,
                    "cost_tier": candidate.cost_tier,
                }
            )
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.log_metric("cv_primary_mean", cv_mean)
            mlflow.log_metric("cv_primary_standard_deviation", cv_std)
            mlflow.log_metric("fit_duration_seconds", duration)
            mlflow.log_dict(_json_safe(diagnostics), "evaluation.json")
            mlflow_sklearn.log_model(fitted, artifact_path="model")
        model_artifact_uri = _persist_candidate_model(
            run,
            candidate.name,
            fitted,
        )
        print(f"Candidate {candidate.name} completed", flush=True)
        return {
            "rank": None,
            "model": candidate.name,
            "status": "succeeded",
            "cost_tier": candidate.cost_tier,
            "primary_score": None,
            "metrics": metrics,
            "diagnostics": _json_safe(diagnostics),
            "best_params": params,
            "duration_seconds": duration,
            "error": None,
            "mlflow_run_id": candidate_run.info.run_id,
            "model_artifact_uri": model_artifact_uri,
            "_model": fitted,
        }
    except Exception as exc:
        return _failed_candidate(candidate, started, exc)


def _fit_clustering(dataframe: pd.DataFrame, run: ModelRun) -> TournamentResult:
    evaluation_column = run.params.get("evaluation_column")
    reference_labels = None
    if evaluation_column:
        if evaluation_column not in dataframe.columns:
            raise ValueError(f"Clustering evaluation column '{evaluation_column}' is missing.")
        valid_reference = dataframe[evaluation_column].notna()
        reference_labels = dataframe.loc[valid_reference, evaluation_column].to_numpy()
        dataframe = dataframe.loc[valid_reference].drop(columns=[evaluation_column])
    dataframe = _normalize_temporal_features(dataframe)
    preprocessor = _preprocessor(dataframe)
    transformed = np.asarray(preprocessor.fit_transform(dataframe))

    candidate_limit = max(1, min(int(run.params.get("candidate_limit", 5)), 12))
    requested_names = run.params.get("candidate_models")
    candidates = select_candidates(
        TaskType.CLUSTERING,
        requested_names if isinstance(requested_names, list) else None,
        candidate_limit,
    )
    if not candidates:
        raise ValueError("No supported clustering candidates were selected.")
    requested_folds = max(2, min(int(run.params.get("cv_folds", 3)), 5))
    folds = min(requested_folds, max(2, len(dataframe) // 10))
    splitter = KFold(n_splits=folds, shuffle=True, random_state=42)

    leaderboard: list[dict[str, Any]] = []
    fitted: dict[str, tuple[Any, dict[str, Any], dict[str, float]]] = {}
    for candidate in candidates:
        entry = _fit_clustering_candidate(
            candidate,
            transformed,
            reference_labels,
            splitter,
            preprocessor,
            run,
        )
        leaderboard.append(entry)
        if entry["status"] == "succeeded":
            fitted[candidate.name] = (
                entry.pop("_model"),
                entry["best_params"],
                entry["metrics"],
            )
        _persist_partial_leaderboard(run.id, leaderboard, "silhouette")

    leaderboard = rank_leaderboard(leaderboard, "silhouette")
    successful = [entry for entry in leaderboard if entry["status"] == "succeeded"]
    if not successful:
        failures = "; ".join(
            f"{entry['model']}: {entry.get('error', 'failed')}" for entry in leaderboard
        )
        raise RuntimeError(f"Every clustering candidate failed. {failures}")
    winner = successful[0]
    model, params, metrics = fitted[winner["model"]]
    return TournamentResult(
        metrics=metrics,
        model=model,
        params={
            "winner": winner["model"],
            "evaluation_column": evaluation_column,
            **params,
        },
        leaderboard=leaderboard,
        primary_metric="silhouette",
    )


def _fit_clustering_candidate(
    candidate: CandidateSpec,
    transformed: np.ndarray,
    reference_labels: np.ndarray | None,
    splitter: KFold,
    preprocessor: ColumnTransformer,
    run: ModelRun,
) -> dict[str, Any]:
    started = time.monotonic()
    print(f"Training clustering candidate {candidate.name}", flush=True)
    try:
        parameter_options: list[dict[str, Any]] = [{}]
        available_parameters = candidate.estimator.get_params(deep=False)
        if "n_clusters" in available_parameters:
            maximum_clusters = min(8, max(2, len(transformed) - 1))
            parameter_options = [
                {"n_clusters": cluster_count} for cluster_count in range(2, maximum_clusters + 1)
            ]
        best_metrics = None
        best_standard_deviations = None
        best_fold_metrics = None
        best_params: dict[str, Any] = {}
        for parameters in parameter_options:
            fold_results = []
            for train_index, test_index in splitter.split(transformed):
                estimator = clone(candidate.estimator).set_params(**parameters)
                test_features = transformed[test_index]
                if hasattr(estimator, "predict"):
                    estimator.fit(transformed[train_index])
                    labels = estimator.predict(test_features)
                else:
                    labels = estimator.fit_predict(test_features)
                fold_reference = (
                    reference_labels[test_index] if reference_labels is not None else None
                )
                fold_results.append(clustering_evaluation(test_features, labels, fold_reference))
            means, standard_deviations = aggregate_fold_metrics(fold_results)
            if "silhouette" not in means:
                continue
            if best_metrics is None or means["silhouette"] > best_metrics["silhouette"]:
                best_metrics = means
                best_standard_deviations = standard_deviations
                best_fold_metrics = fold_results
                best_params = parameters
        if best_metrics is None:
            raise ValueError("No cross-validation fold produced valid clusters.")

        final_estimator = clone(candidate.estimator).set_params(**best_params)
        full_labels = final_estimator.fit_predict(transformed)
        unique_labels, counts = np.unique(full_labels, return_counts=True)
        diagnostics = {
            "cross_validation": {
                "folds": splitter.n_splits,
                "metric_standard_deviations": best_standard_deviations,
                "fold_metrics": best_fold_metrics,
            },
            "cluster_sizes": {
                str(label): int(count) for label, count in zip(unique_labels, counts, strict=True)
            },
            "cluster_count": int(len(unique_labels[unique_labels != -1])),
            "noise_rows": int(np.sum(full_labels == -1)),
            "external_evaluation": reference_labels is not None,
        }
        pipeline_model = Pipeline(
            [
                ("prepare", preprocessor),
                ("model", final_estimator),
            ]
        )
        params = {f"model__{name}": value for name, value in best_params.items()}
        duration = round(time.monotonic() - started, 3)
        with mlflow.start_run(run_name=candidate.name, nested=True) as candidate_run:
            mlflow.set_tags(
                {
                    "candidate_model": candidate.name,
                    "cost_tier": candidate.cost_tier,
                    "external_clustering_evaluation": reference_labels is not None,
                }
            )
            mlflow.log_params(_json_safe(params))
            mlflow.log_metrics(best_metrics)
            mlflow.log_metric("fit_duration_seconds", duration)
            mlflow.log_dict(_json_safe(diagnostics), "evaluation.json")
            mlflow_sklearn.log_model(pipeline_model, artifact_path="model")
        model_artifact_uri = _persist_candidate_model(
            run,
            candidate.name,
            pipeline_model,
        )
        return {
            "rank": None,
            "model": candidate.name,
            "status": "succeeded",
            "cost_tier": candidate.cost_tier,
            "primary_score": None,
            "metrics": best_metrics,
            "diagnostics": _json_safe(diagnostics),
            "best_params": _json_safe(params),
            "duration_seconds": duration,
            "error": None,
            "mlflow_run_id": candidate_run.info.run_id,
            "model_artifact_uri": model_artifact_uri,
            "_model": pipeline_model,
        }
    except Exception as exc:
        return _failed_candidate(candidate, started, exc)


def _failed_candidate(
    candidate: CandidateSpec,
    started: float,
    exc: Exception,
) -> dict[str, Any]:
    duration = round(time.monotonic() - started, 3)
    message = str(exc)[:1000]
    print(f"Candidate {candidate.name} failed: {message}", flush=True)
    with mlflow.start_run(run_name=candidate.name, nested=True):
        mlflow.set_tags(
            {
                "candidate_model": candidate.name,
                "candidate_status": "failed",
                "cost_tier": candidate.cost_tier,
                "failure_message": message[:250],
            }
        )
        mlflow.log_metric("fit_duration_seconds", duration)
    return {
        "rank": None,
        "model": candidate.name,
        "status": "failed",
        "cost_tier": candidate.cost_tier,
        "primary_score": None,
        "metrics": {},
        "diagnostics": {},
        "best_params": {},
        "duration_seconds": duration,
        "error": message,
    }


def _persist_candidate_model(
    run: ModelRun,
    model_name: str,
    model: Any,
) -> str:
    buffer = io.BytesIO()
    joblib.dump(model, buffer, compress=3)
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", model_name).strip("-")
    key = f"projects/{run.project_id}/runs/{run.id}/models/{safe_name or 'model'}.joblib"
    return get_object_store().put_bytes(key, buffer.getvalue()).uri


def rank_leaderboard(
    entries: list[dict[str, Any]],
    primary_metric: str,
) -> list[dict[str, Any]]:
    successful = [entry for entry in entries if entry["status"] == "succeeded"]
    failed = [entry for entry in entries if entry["status"] != "succeeded"]
    reverse = metric_direction(primary_metric) == "maximize"
    successful.sort(
        key=lambda entry: float(entry["metrics"][primary_metric]),
        reverse=reverse,
    )
    for rank, entry in enumerate(successful, start=1):
        entry["rank"] = rank
        entry["primary_score"] = entry["metrics"][primary_metric]
    return [*successful, *failed]


def merge_leaderboard_entries(
    existing: list[dict[str, Any]],
    additions: list[dict[str, Any]],
    primary_metric: str,
) -> list[dict[str, Any]]:
    by_model = {entry["model"]: dict(entry) for entry in existing}
    for entry in additions:
        by_model[entry["model"]] = dict(entry)
    return rank_leaderboard(list(by_model.values()), primary_metric)


def _persist_partial_leaderboard(
    run_id: uuid.UUID,
    entries: list[dict[str, Any]],
    primary_metric: str,
) -> None:
    ranked = rank_leaderboard(entries, primary_metric)
    successful = [entry for entry in ranked if entry["status"] == "succeeded"]
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            return
        run.tags = {
            **run.tags,
            "leaderboard_primary_metric": primary_metric,
            "leaderboard": _json_safe(ranked),
            "winner": successful[0]["model"] if successful else None,
            "winner_mlflow_run_id": (successful[0].get("mlflow_run_id") if successful else None),
            "completed_candidates": len(ranked),
            "leaderboard_updated_at": datetime.now(UTC).isoformat(),
        }
        parent_id = run.tags.get("leaderboard_parent_run_id")
        if parent_id:
            try:
                parent = db.get(ModelRun, uuid.UUID(str(parent_id)))
            except ValueError:
                parent = None
            if parent is not None and parent.project_id == run.project_id:
                parent_metric = parent.tags.get(
                    "leaderboard_primary_metric",
                    primary_metric,
                )
                additions = [
                    {
                        **entry,
                        "extension_run_id": str(run.id),
                    }
                    for entry in ranked
                ]
                merged = merge_leaderboard_entries(
                    parent.tags.get("leaderboard", []),
                    additions,
                    parent_metric,
                )
                merged_successful = [entry for entry in merged if entry["status"] == "succeeded"]
                parent.tags = {
                    **parent.tags,
                    "leaderboard": _json_safe(merged),
                    "winner": (merged_successful[0]["model"] if merged_successful else None),
                    "winner_mlflow_run_id": (
                        merged_successful[0].get("mlflow_run_id") if merged_successful else None
                    ),
                    "completed_candidates": len(merged),
                    "leaderboard_updated_at": datetime.now(UTC).isoformat(),
                }
        db.commit()


def _cross_validation_strategy(
    target: pd.Series,
    task_type: TaskType,
    requested_folds: int,
) -> Any:
    if task_type == TaskType.CLASSIFICATION:
        minimum_class_size = int(target.value_counts().min())
        if minimum_class_size < 2:
            raise ValueError("Each target class needs at least two training rows.")
        return min(requested_folds, minimum_class_size)
    if task_type == TaskType.TIME_SERIES:
        if len(target) < 6:
            raise ValueError("At least six training rows are required for time-series validation.")
        return TimeSeriesSplit(n_splits=min(requested_folds, max(2, len(target) // 20)))
    if len(target) < 4:
        raise ValueError("At least four training rows are required for cross-validation.")
    return min(requested_folds, max(2, len(target) // 50))


def _supervised_split(
    features: pd.DataFrame,
    target: pd.Series,
    task_type: TaskType,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    if task_type == TaskType.TIME_SERIES:
        order_column = _time_order_column(features)
        if order_column:
            order = features[order_column].sort_values(kind="stable").index
            features = features.loc[order]
            target = target.loc[order]
        split_at = max(1, min(len(features) - 1, int(len(features) * 0.8)))
        return (
            features.iloc[:split_at],
            features.iloc[split_at:],
            target.iloc[:split_at],
            target.iloc[split_at:],
        )
    return train_test_split(
        features,
        target,
        test_size=0.2,
        random_state=42,
        stratify=target if task_type == TaskType.CLASSIFICATION else None,
    )


def _normalize_temporal_features(features: pd.DataFrame) -> pd.DataFrame:
    normalized = features.copy()
    for column in normalized.columns:
        series = normalized[column]
        timestamp_unit = _series_unix_timestamp_unit(series)
        if timestamp_unit:
            numeric = pd.to_numeric(series, errors="coerce").astype("float64")
            normalized[column] = numeric / UNIX_UNIT_SCALES[timestamp_unit] / 86_400
            continue
        is_temporal_name = any(
            token in str(column).lower() for token in ("date", "time", "timestamp")
        )
        if not isinstance(series.dtype, pd.DatetimeTZDtype) and not (
            pd.api.types.is_datetime64_any_dtype(series) or is_temporal_name
        ):
            continue
        parsed = pd.to_datetime(series, errors="coerce", utc=True)
        if parsed.notna().mean() < 0.8:
            continue
        numeric = parsed.astype("int64", copy=False).astype("float64")
        numeric[parsed.isna()] = np.nan
        normalized[column] = numeric / 86_400_000_000_000
    return normalized


def _time_order_column(features: pd.DataFrame) -> str | None:
    temporal_names = []
    for column in features.columns:
        series = features[column]
        is_temporal_name = any(
            token in str(column).lower() for token in ("date", "time", "timestamp")
        )
        if (
            is_temporal_name
            or pd.api.types.is_datetime64_any_dtype(series)
            or _series_unix_timestamp_unit(series)
        ):
            temporal_names.append(column)
    return temporal_names[0] if temporal_names else None


def _series_unix_timestamp_unit(series: pd.Series) -> str | None:
    present = series.dropna()
    if present.empty:
        return None
    numeric = pd.to_numeric(present, errors="coerce")
    if numeric.notna().mean() < 0.8:
        return None
    return infer_unix_timestamp_unit(numeric.dropna().head(1000).tolist())


def _preprocessor(features: pd.DataFrame) -> ColumnTransformer:
    numeric_columns = list(features.select_dtypes(include="number").columns)
    categorical_columns = [column for column in features.columns if column not in numeric_columns]
    transformers = []
    if numeric_columns:
        transformers.append(
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                numeric_columns,
            )
        )
    if categorical_columns:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        (
                            "encode",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value",
                                unknown_value=-1,
                            ),
                        ),
                    ]
                ),
                categorical_columns,
            )
        )
    return ColumnTransformer(transformers=transformers, remainder="drop")


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def _mark_failed(run_id: uuid.UUID, exc: Exception) -> None:
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            return
        run.status = RunStatus.FAILED
        run.failure_code = "TRAINING_PIPELINE_FAILED"
        run.failure_message = str(exc)
        run.plain_english_failure = (
            "Model training failed. Review the run logs for data quality, "
            "memory, model, or MLflow connectivity errors."
        )
        run.finished_at = datetime.now(UTC)
        db.commit()
