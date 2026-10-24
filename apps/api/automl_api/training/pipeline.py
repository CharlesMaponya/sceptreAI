import hashlib
import hmac
import io
import json
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from importlib.metadata import distributions, version
from typing import Any
from urllib.parse import unquote, urlsplit

import joblib
import mlflow
import mlflow.sklearn as mlflow_sklearn
import numpy as np
import pandas as pd
import polars as pl
import ray
from fastapi import HTTPException
from mlflow import MlflowClient
from pydantic import ValidationError
from sklearn.base import clone
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.impute import SimpleImputer
from sklearn.model_selection import (
    KFold,
    TimeSeriesSplit,
    cross_val_score,
    learning_curve,
    train_test_split,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    KBinsDiscretizer,
    MinMaxScaler,
    OrdinalEncoder,
    StandardScaler,
)
from skopt import BayesSearchCV
from sqlalchemy import select
from sqlalchemy.orm import Session
from zenml import pipeline, step

from automl_api.core.config import get_settings
from automl_api.core.redaction import redact_text
from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import (
    AttemptStatus,
    MetricKind,
    MetricSplit,
    RunStatus,
    TaskType,
)
from automl_api.models.projects import Project
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    TrainingCandidate,
    WorkflowAttempt,
)
from automl_api.schemas.training import TrainingEstimateRequest
from automl_api.services.dataset_digests import (
    require_distinct_training_roles,
    require_verified_digest,
    verify_dataset_bytes,
)
from automl_api.services.experiment_identity import BINDINGS, ROLES, resolve_training_identity
from automl_api.services.feature_contracts import bound_row, validate_compiled_view
from automl_api.services.leakage import detect_target_leakage
from automl_api.services.ray_polars_profiling import _ensure_ray, _ray_source
from automl_api.services.temporal import (
    normalize_temporal_features as _normalize_temporal_features,
)
from automl_api.services.temporal import (
    series_unix_timestamp_unit as _series_unix_timestamp_unit,
)
from automl_api.services.tracking_reconciliation import ensure_tracking_run
from automl_api.services.workflow_state import (
    StaleFence,
    attempt_credentials_revoked,
    canonical_request_hash,
)
from automl_api.storage.object_store import get_object_store
from automl_api.training import training_metadata
from automl_api.training.catalog_contract import validate_catalog_revision
from automl_api.training.correlation import CorrelatedFeatureFilter
from automl_api.training.evaluation import (
    aggregate_fold_metrics,
    classification_evaluation,
    clustering_evaluation,
    cross_validation_scoring,
    default_binary_positive_label,
    metric_direction,
    regression_evaluation,
    resolve_primary_metric,
)
from automl_api.training.feature_selection import BoundedFeatureSelector
from automl_api.training.model_catalog import (
    PAIRWISE_SAMPLE_MODELS,
    CandidateSpec,
    candidate_catalog,
    configure_estimator_for_training,
    select_candidates,
)
from automl_api.training.parameter_overrides import (
    fixed_candidate,
    merge_fixed,
    validate_bound_overrides,
    validate_conditionals,
)
from automl_api.training.training_context import training_context

_TERMINAL_RUN_STATUSES = frozenset(
    {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.PREEMPTED,
    }
)

# Exact non-core types created by Sceptre's supported sklearn pipelines and
# third-party estimator wrappers. MLflow stores this allowlist with the skops
# model so later loads apply the same narrow trust boundary.
_SKOPS_TRUSTED_TYPES = (
    "automl_api.training.correlation.CorrelatedFeatureFilter",
    "automl_api.training.feature_selection.BoundedFeatureSelector",
    "automl_api.training.feature_selection.classification_scores",
    "automl_api.training.feature_selection.regression_scores",
    "automl_api.training.model_catalog.XGBLabelEncodingClassifier",
    "automl_api.training.pipeline._shift_nonnegative",
    "catboost.core.CatBoostClassifier",
    "catboost.core.CatBoostRegressor",
    "collections.OrderedDict",
    "datetime.time",
    "lightgbm.basic.Booster",
    "lightgbm.sklearn.LGBMClassifier",
    "lightgbm.sklearn.LGBMRegressor",
    "numpy.dtype",
    "sklearn.compose._column_transformer.make_column_selector",
    "sklearn.feature_selection._mutual_info.mutual_info_classif",
    "sklearn.feature_selection._mutual_info.mutual_info_regression",
    "xgboost.core.Booster",
    "xgboost.sklearn.XGBClassifier",
    "xgboost.sklearn.XGBRegressor",
)


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


@lru_cache(maxsize=1)
def _model_pip_requirements() -> tuple[str, ...]:
    # MLflow's automatic inference loads another copy of the fitted model in a
    # subprocess. Record the pinned worker environment without duplicating it.
    names = {
        re.sub(r"[-_.]+", "-", distribution.metadata["Name"]).lower()
        for distribution in distributions()
        if distribution.metadata.get("Name")
        and distribution.metadata["Name"].lower() != "smme-tabular-automl"
    }
    requirements = [f"{name}=={version(name)}" for name in sorted(names)]
    if any("+cpu" in requirement for requirement in requirements):
        requirements.insert(0, "--extra-index-url https://download.pytorch.org/whl/cpu")
    return tuple(requirements)


def _log_sklearn_model(model: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("pip_requirements", list(_model_pip_requirements()))
    return mlflow_sklearn.log_model(
        model,
        skops_trusted_types=list(_SKOPS_TRUSTED_TYPES),
        **kwargs,
    )


class TrackingCreationUnresolved(RuntimeError):
    pass


def _tracked_mlflow_run(run: ModelRun, *, role: str, run_name: str, parent_run_id=None, tags=None):
    from automl_api.training.training_tracking import ensure_remote_tracking

    metadata_enabled = training_metadata.enabled()

    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"
    remote_id = ensure_remote_tracking(run.id, role, parent_run_id=parent_run_id)
    if remote_id is not None:
        active = mlflow.start_run(run_id=remote_id, nested=parent_run_id is not None)
        return (
            training_metadata.metadata_context(active, run.id, role) if metadata_enabled else active
        )

    def live_run(db):
        current = _locked_run(db, run.id)
        if (
            current is None
            or current.status in _TERMINAL_RUN_STATUSES
            or _has_committed_training_result(current)
        ):
            raise StaleFence("This run no longer owns tracking creation.")
        return current

    tracking_id = ensure_tracking_run(
        run.id,
        factory=get_session_factory(),
        live_run=live_run,
        client_factory=MlflowClient,
        tracking_uri=mlflow.get_tracking_uri(),
        attempt_id=os.getenv("AUTOML_ATTEMPT_ID") or "legacy",
        role=role,
        run_name=run_name,
        parent_run_id=parent_run_id,
        tags=tags,
    )
    return mlflow.start_run(run_id=tracking_id, nested=parent_run_id is not None)


def _prepare_training_context(db, run_id, *, context=None, require_bound=False, mark_running=True):
    run = _locked_run(db, run_id, **({"context": context} if context is not None else {}))
    if run is None:
        raise ValueError(f"Model run {run_id} was not found.")
    if run.status in _TERMINAL_RUN_STATUSES or _has_committed_training_result(
        run, **({"attempt_id": str(context[0])} if context is not None else {})
    ):
        return None
    _validate_bound_catalog(db, run, **({"require_bound": True} if require_bound else {}))
    approved_columns = _bound_feature_columns(
        db, run, **({"require_bound": True} if require_bound else {})
    )
    validate_bound_overrides(
        db,
        run,
        select_candidates(
            run.task_type,
            run.params.get("candidate_models"),
            int(run.params.get("candidate_limit") or 5),
        ),
    )
    version = db.get(DatasetVersion, run.dataset_version_id)
    if version is None:
        raise ValueError("Dataset version was not found.")
    from automl_api.services.provider_source import (
        reject_promotional_membership,
        require_prepared_source,
    )

    split = _bound_split_revision(db, run)
    source_plan = require_prepared_source(db, version, split_revision_id=split.id)
    reject_promotional_membership(db, run)
    if mark_running:
        run.status = RunStatus.RUNNING
        run.started_at = run.started_at or datetime.now(UTC)
        run.failure_code = None
        run.failure_message = None
        run.plain_english_failure = None
        run.finished_at = None
    return run, split, approved_columns, source_plan


def execute_training_run(run_id: uuid.UUID) -> dict[str, float]:
    prepared = training_context(run_id)
    if prepared is None:
        with get_session_factory()() as db:
            prepared = _prepare_training_context(db, run_id)
            if prepared is None:
                return {}
            db.commit()
    run, split, approved_columns, source_plan = prepared

    try:
        dataframe, validation_dataframe = _load_prepared_training_frames(
            split,
            sample_rows=int(run.params.get("sample_tier_rows") or 0),
            validation_sample_rows=int(run.params.get("validation_sample_rows") or 0),
            task_type=run.task_type,
            approved_columns=approved_columns,
            **({"source_plan": source_plan} if source_plan is not None else {}),
        )
        settings = get_settings()
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        with _tracked_mlflow_run(
            run,
            role="parent",
            run_name=run.run_name or str(run.id),
            tags={"task_type": run.task_type.value},
        ) as mlflow_run:
            training_metadata.log_tags(
                {
                    "project_id": str(run.project_id),
                    "dataset_version_id": str(run.dataset_version_id),
                    "automl_run_id": str(run.id),
                    "task_type": run.task_type.value,
                }
            )
            result = _fit_model(dataframe, run, validation_dataframe)
            training_metadata.log_params(_json_safe(result.params))
            _log_metrics_synchronously(result.metrics)
            mlflow.log_dict(
                {
                    "primary_metric": result.primary_metric,
                    "entries": result.leaderboard,
                },
                "leaderboard.json",
            )
            _log_sklearn_model(result.model, artifact_path="model")
            mlflow_run_id = mlflow_run.info.run_id

        if not _persist_training_success(run_id, result, mlflow_run_id):
            return {}
        return result.metrics
    except Exception as exc:
        if not os.getenv("AUTOML_ATTEMPT_ID"):
            _mark_failed(run_id, exc)
        raise


def _load_dataframe(version: DatasetVersion) -> pd.DataFrame:
    require_verified_digest(version)
    content = get_object_store().read_bytes(version.object_uri)
    verify_dataset_bytes(version, content)
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


def _validate_bound_catalog(db: Session, run: ModelRun, *, require_bound=False) -> None:
    revision_id = (run.params or {}).get("estimator_catalog_revision_id")
    if revision_id is None:
        # shortcut: allow unbound local calls until legacy migration retires them.
        if (
            require_bound
            or os.getenv("AUTOML_ATTEMPT_ID")
            or get_settings().environment
            in {
                "production",
                "staging",
            }
        ):
            raise ValueError("Training requires a bound catalog revision")
        return
    try:
        revision_id = uuid.UUID(str(revision_id))
    except ValueError as exc:
        raise ValueError("Invalid bound catalog revision") from exc
    revision = db.scalar(
        select(EstimatorCatalogRevision).where(
            EstimatorCatalogRevision.id == revision_id,
            EstimatorCatalogRevision.project_id == run.project_id,
        )
    )
    if revision is None:
        raise ValueError("Bound catalog revision is unavailable")
    specification = validate_catalog_revision(revision, run.task_type)
    if (
        run.params.get("catalog_content_digest") != revision.content_digest
        or run.params.get("catalog_runtime_digest") != specification["runtime_lock_digest"]
    ):
        raise ValueError("Training catalog identity differs from admitted intent")
    selected = run.params.get("candidate_models")
    allowed = {row["name"] for row in specification["estimators"]}
    if (
        not isinstance(selected, list)
        or not selected
        or any(not isinstance(name, str) or name not in allowed for name in selected)
        or len(selected) != len(set(selected))
        or run.params.get("candidate_limit") != len(selected)
    ):
        raise ValueError("Training estimator selection differs from its bound catalog")


def _bound_feature_columns(db: Session, run: ModelRun, *, require_bound=False) -> list[str] | None:
    identity = run.params.get("experiment_spec_revision_id")
    if identity is None:
        # shortcut: local unbound calls remain until their legacy migration is qualified.
        if (
            require_bound
            or os.getenv("AUTOML_ATTEMPT_ID")
            or get_settings().environment
            in {
                "production",
                "staging",
            }
        ):
            raise ValueError("Training requires an immutable experiment specification")
        return None
    try:
        frozen = resolve_training_identity(
            db, run.project_id, TrainingEstimateRequest(experiment_spec_revision_id=identity)
        )
        if (run.dataset_version_id, run.task_type, run.target_column) != (
            frozen.dataset_version_id,
            frozen.task_type,
            frozen.target_column,
        ):
            raise ValueError("Training data or task differs from its immutable specification")
        for key in (*BINDINGS, "search_objective_revision_id"):
            stored = "estimator_catalog_revision_id" if key == "catalog_revision_id" else key
            if run.params.get(stored) != str(getattr(frozen, key)):
                raise ValueError("Training revision differs from its immutable specification")
        for key in ("primary_metric", "optimization_iterations", "cv_folds", *ROLES):
            expected = getattr(frozen, key)
            if key == "positive_label" and expected is None:
                continue  # The bound automatic-label policy records its chosen class after fitting.
            if run.params.get(key) != expected:
                raise ValueError("Training policy differs from its immutable specification")
        if sorted(set(run.params.get("excluded_columns") or [])) != frozen.excluded_columns:
            raise ValueError("Training exclusions differ from its immutable specification")
        graph = {
            key: bound_row(db, model, run.project_id, getattr(frozen, key))
            for key, model in BINDINGS.items()
        }
        compiled = validate_compiled_view(
            graph["feature_contract_revision_id"],
            graph["feature_registry_revision_id"],
            graph["feature_recipe_revision_id"],
            graph["feature_search_space_revision_id"],
        )
    except (HTTPException, ValidationError) as exc:
        raise ValueError("Training feature specification is unavailable or incompatible") from exc
    columns = [column for column in compiled.input_columns if column not in frozen.excluded_columns]
    for role in (frozen.target_column, frozen.evaluation_column):
        if role is not None and role not in columns:
            columns.append(role)
    return columns


def _bound_split_revision(db: Session, run: ModelRun) -> DatasetSplitRevision:
    value = (run.params or {}).get("split_revision_id")
    if not value:
        raise ValueError("Training requires an immutable prepared split revision.")
    try:
        split_id = uuid.UUID(str(value))
    except ValueError as exc:
        raise ValueError("The training split revision binding is invalid.") from exc
    split = db.get(DatasetSplitRevision, split_id)
    if (
        split is None
        or split.project_id != run.project_id
        or split.dataset_version_id != run.dataset_version_id
    ):
        raise ValueError("The prepared split revision is missing or has different lineage.")
    uris = (split.specification or {}).get("uris") or {}
    if not uris.get("train") or not uris.get("validation"):
        raise ValueError("The prepared split revision has no sealed train/validation roles.")
    require_distinct_training_roles(uris)
    if (
        run.task_type == TaskType.TIME_SERIES
        and split.specification.get("split_strategy") != "temporal"
    ):
        raise ValueError(
            "Time-series training requires preparation with a chronological time column."
        )
    return split


def _load_prepared_training_frames(
    split: DatasetSplitRevision,
    *,
    source_plan=None,
    sample_rows: int = 0,
    validation_sample_rows: int = 0,
    task_type: TaskType = TaskType.UNSPECIFIED,
    approved_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load only the sealed train and validation roles; final-test stays unreachable."""
    if split.specification.get("provider_import_id") and source_plan is None:
        raise ValueError("Provider prepared roles require their approved versioned source plan")
    if source_plan is not None and (
        source_plan.split_revision_id != split.id
        or source_plan.dataset_version_id != split.dataset_version_id
        or source_plan.project_id != split.project_id
    ):
        raise ValueError("Provider source plan does not belong to this split")
    uris = split.specification["uris"]
    counts = split.specification.get("split_counts") or (
        (split.specification.get("identity") or {}).get("split_counts") or {}
    )
    train_count = int(counts.get("train") or 0)
    validation_count = int(counts.get("validation") or 0)
    validation_rows = validation_sample_rows
    if validation_rows <= 0 and sample_rows > 0 and train_count > 0 and validation_count > 0:
        validation_rows = max(1_000, round(sample_rows * validation_count / train_count))
    order_column = (
        str(split.specification.get("time_column") or "source_ordinal")
        if task_type == TaskType.TIME_SERIES
        else "row_id"
    )
    seed_material = (
        f"provider-safe-sample-v1:{source_plan.manifest.split_digest}:"
        f"{source_plan.manifest.safe_data.schema_digest}"
        if source_plan is not None
        else f"{getattr(split, 'id', '')}:{split.specification.get('row_set_digest', '')}"
    )
    seed = int(hashlib.sha256(seed_material.encode()).hexdigest()[:8], 16)
    frames = (
        _load_prepared_role(
            str(uris["train"]),
            source_plan=source_plan,
            role="train",
            max_rows=sample_rows,
            source_rows=train_count,
            order_column=order_column,
            random_seed=seed,
            approved_columns=approved_columns,
        ),
        _load_prepared_role(
            str(uris["validation"]),
            source_plan=source_plan,
            role="validation",
            max_rows=validation_rows,
            source_rows=validation_count,
            order_column=order_column,
            random_seed=seed + 1,
            approved_columns=approved_columns,
        ),
    )
    return frames[0], frames[1]


def _load_prepared_role(
    uri: str,
    *,
    source_plan=None,
    role=None,
    max_rows: int = 0,
    source_rows: int = 0,
    order_column: str = "row_id",
    random_seed: int = 42,
    approved_columns: list[str] | None = None,
) -> pd.DataFrame:
    _ensure_ray()
    if source_plan is not None:
        from automl_api.services.provider_reader import read_provider_role

        dataset = read_provider_role(source_plan, role)
    else:
        if uri.startswith("provider-import:"):
            raise ValueError("Provider prepared roles cannot fall back to prefix reads")
        descriptor = get_object_store().dataframe_source(uri)
        path, filesystem = _ray_source(descriptor.path, descriptor.filesystem_options)
        dataset = ray.data.read_parquet(path, filesystem=filesystem)
    identity_columns = {
        "row_id",
        "source_ordinal",
        "content_fingerprint",
        "split_role",
        "__sceptre_sample_rank__",
    }
    names = set(dataset.schema().names)
    if order_column != "row_id" and order_column in names:
        dataset = dataset.sort(order_column)
        if max_rows > 0:
            dataset = dataset.limit(max_rows)
    elif max_rows > 0 and source_rows > max_rows:
        # Rank immutable row IDs before limiting: Ray task/block completion
        # order must never choose which rows enter a comparable experiment.
        # Filter first so the distributed sort handles only the bounded sample.
        fraction = min(1.0, max_rows * 1.10 / source_rows)
        dataset = (
            dataset.map_batches(
                _rank_sample_batch,
                batch_format="pyarrow",
                batch_size=16_384,
                fn_kwargs={"fraction": fraction, "seed": random_seed},
            )
            .sort(["__sceptre_sample_rank__", "row_id"])
            .limit(max_rows)
        )
        names.add("__sceptre_sample_rank__")
    elif source_plan is not None and "row_id" in names:
        dataset = dataset.sort("row_id")
        if max_rows > 0:
            dataset = dataset.limit(max_rows)
    elif max_rows > 0:
        dataset = dataset.limit(max_rows)
    removable = sorted(identity_columns & names)
    if removable:
        dataset = dataset.drop_columns(removable)
    if approved_columns is not None:
        if not approved_columns or set(approved_columns) - (names - identity_columns):
            raise ValueError("Prepared data is missing the approved feature view")
        dataset = dataset.select_columns(approved_columns)
    return dataset.to_pandas()


def _rank_sample_batch(batch, *, fraction: float, seed: int):
    frame = pl.from_arrow(batch)
    name = "__sceptre_sample_rank__"
    if name in frame.columns:
        raise ValueError(f"Rename reserved training column '{name}'.")
    salt = int(hashlib.sha256(str(seed).encode()).hexdigest()[:16], 16)
    rank = pl.col("row_id").str.slice(0, 16).str.to_integer(base=16, dtype=pl.UInt64)
    frame = frame.with_columns((rank ^ pl.lit(salt, dtype=pl.UInt64)).alias(name))
    if fraction < 1:
        frame = frame.filter(pl.col(name) < int((1 << 64) * fraction))
    return frame.to_arrow()


def _candidate_training_sample(
    features: pd.DataFrame,
    target: pd.Series,
    *,
    max_rows: int,
    task_type: TaskType,
) -> tuple[pd.DataFrame, pd.Series]:
    if max_rows <= 0 or len(features) <= max_rows:
        return features, target
    if task_type == TaskType.TIME_SERIES:
        return features.iloc[:max_rows], target.iloc[:max_rows]

    stratify: pd.Series | None = None
    if task_type == TaskType.CLASSIFICATION:
        counts = target.value_counts(dropna=False)
        if len(counts) > 1 and int(counts.min()) >= 2 and max_rows >= len(counts):
            stratify = target
    elif task_type == TaskType.REGRESSION:
        try:
            bins = pd.qcut(target.rank(method="first"), q=20, labels=False, duplicates="drop")
            if bins.nunique() > 1 and int(bins.value_counts().min()) >= 2:
                stratify = bins
        except (TypeError, ValueError):
            stratify = None

    selected, _ = train_test_split(
        np.arange(len(features)),
        train_size=max_rows,
        random_state=42,
        stratify=stratify,
    )
    return features.iloc[selected], target.iloc[selected]


def _fit_model(
    dataframe: pd.DataFrame,
    run: ModelRun,
    validation_dataframe: pd.DataFrame | None = None,
) -> TournamentResult:
    if run.task_type == TaskType.CLUSTERING:
        return _fit_clustering(dataframe, run)
    if not run.target_column or run.target_column not in dataframe.columns:
        raise ValueError("The configured target column is missing from the dataset.")

    duplicate_row_count = int(dataframe.duplicated(keep="first").sum())
    if duplicate_row_count:
        dataframe = dataframe.drop_duplicates(keep="first")
    leakage_analysis = detect_target_leakage(dataframe, run.target_column)
    excluded_leakage_columns = sorted(
        {
            str(column)
            for column in [
                *list(run.params.get("excluded_leakage_columns") or []),
                *list(run.params.get("excluded_columns") or []),
                *leakage_analysis.excluded_columns,
            ]
            if column and str(column) != run.target_column
        }
    )
    target = dataframe[run.target_column]
    features = dataframe.drop(
        columns=[run.target_column, *excluded_leakage_columns],
        errors="ignore",
    )
    if features.shape[1] == 0:
        raise ValueError("No training features remain after target-leakage removal.")
    valid_target = target.notna()
    features = features.loc[valid_target]
    target = target.loc[valid_target]
    if len(features) < 10:
        raise ValueError("At least 10 rows with a non-missing target are required.")
    if run.task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
        target = pd.to_numeric(target, errors="raise")
    elif run.params.get("positive_label") is None and target.nunique() == 2:
        run.params = {
            **run.params,
            "positive_label": default_binary_positive_label(target),
        }
    features = _normalize_temporal_features(features)

    if validation_dataframe is None:
        train_x, test_x, train_y, test_y = _supervised_split(features, target, run.task_type)
    else:
        if run.target_column not in validation_dataframe.columns:
            raise ValueError("The prepared validation role is missing the configured target.")
        validation_target = validation_dataframe[run.target_column]
        validation_features = validation_dataframe.drop(
            columns=[run.target_column, *excluded_leakage_columns],
            errors="ignore",
        )
        missing_features = sorted(set(features.columns) - set(validation_features.columns))
        if missing_features:
            raise ValueError(
                "The prepared validation role is missing training features: "
                + ", ".join(missing_features)
            )
        valid_validation_target = validation_target.notna()
        validation_features = validation_features.loc[valid_validation_target, features.columns]
        validation_target = validation_target.loc[valid_validation_target]
        if validation_features.empty:
            raise ValueError("The prepared validation role has no rows with a target.")
        if run.task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
            validation_target = pd.to_numeric(validation_target, errors="raise")
        train_x, train_y = features, target
        test_x = _normalize_temporal_features(validation_features)
        test_y = validation_target
    candidate_limit = int(run.params.get("candidate_limit", 5))
    requested_names = run.params.get("candidate_models")
    candidates = select_candidates(
        run.task_type,
        requested_names if isinstance(requested_names, list) else None,
        candidate_limit,
    )
    if not candidates:
        raise ValueError(f"No supported candidates are configured for {run.task_type.value}.")

    iterations = int(run.params.get("optimization_iterations", 5))
    cv_folds = int(run.params.get("cv_folds", 3))
    cv = _cross_validation_strategy(train_y, run.task_type, cv_folds)
    primary_metric = resolve_primary_metric(
        run.task_type,
        str(run.params.get("primary_metric")) if run.params.get("primary_metric") else None,
    )
    scoring = cross_validation_scoring(
        run.task_type,
        primary_metric,
        target_classes=int(train_y.nunique()) if run.task_type == TaskType.CLASSIFICATION else None,
    )
    pairwise_limit = int(run.params.get("pairwise_sample_rows") or 0)
    completed = _completed_candidates(run)
    leaderboard: list[dict[str, Any]] = [
        {
            "training_rows": (
                min(len(train_x), pairwise_limit)
                if candidate.name in PAIRWISE_SAMPLE_MODELS and pairwise_limit > 0
                else len(train_x)
            ),
            "validation_rows": len(test_x),
            **completed.get(candidate.name, _pending_candidate(candidate)),
        }
        for candidate in candidates
    ]
    best_model: Any | None = None
    best_score: float | None = None
    maximize = metric_direction(primary_metric) == "maximize"
    _persist_partial_leaderboard(run.id, leaderboard, primary_metric)
    arguments = []
    for candidate in candidates:
        candidate_x, candidate_y = train_x, train_y
        if candidate.name in PAIRWISE_SAMPLE_MODELS and pairwise_limit > 0:
            candidate_x, candidate_y = _candidate_training_sample(
                train_x,
                train_y,
                max_rows=pairwise_limit,
                task_type=run.task_type,
            )
        arguments.append(
            (candidate_x, candidate_y, test_x, test_y, run.task_type, iterations, cv, scoring)
        )
    for index, entry in _candidate_entries("supervised", candidates, arguments, run, completed):
        entry.setdefault("training_rows", len(arguments[index][0]))
        entry.setdefault("validation_rows", len(test_x))
        leaderboard[index] = entry
        if entry["status"] == "succeeded":
            candidate_model = entry.pop("_model", None)
            score = entry["metrics"].get(primary_metric)
            if score is not None and (
                best_score is None
                or (maximize and float(score) > best_score)
                or (not maximize and float(score) < best_score)
            ):
                best_model = candidate_model
                best_score = float(score)
            del candidate_model
        _persist_partial_leaderboard(run.id, leaderboard, primary_metric)

    leaderboard = rank_leaderboard(leaderboard, primary_metric)
    successful = [entry for entry in leaderboard if entry["status"] == "succeeded"]
    if not successful:
        failures = "; ".join(
            f"{entry['model']}: {entry.get('error', 'failed')}" for entry in leaderboard
        )
        raise RuntimeError(f"Every candidate model failed. {failures}")
    if best_model is None and best_score is not None:
        best_model = _restore_candidate(run, successful[0])["_model"]
    if best_model is None:
        raise RuntimeError(f"No candidate produced the primary metric '{primary_metric}'.")

    winner = successful[0]
    return TournamentResult(
        metrics=winner["metrics"],
        model=best_model,
        params={
            "winner": winner["model"],
            "positive_label": run.params.get("positive_label"),
            "excluded_leakage_columns": excluded_leakage_columns,
            "deduplicated_rows": duplicate_row_count,
            **winner["best_params"],
        },
        leaderboard=leaderboard,
        primary_metric=primary_metric,
    )


def _candidate_entries(kind, candidates, arguments, run, completed):
    from automl_api.training import candidate_runtime

    jobs = []
    for index, candidate in enumerate(candidates):
        if candidate.name in completed:
            yield (
                index,
                (
                    dict(completed[candidate.name])
                    if candidate_runtime.enabled()
                    else _restore_candidate(run, completed[candidate.name])
                ),
            )
        elif candidate_runtime.enabled():
            jobs.append((index, candidate, arguments[index]))
        else:
            _persist_candidate_phase(run.id, candidate.name, "preparing_data")
            fit = _fit_candidate if kind == "supervised" else _fit_clustering_candidate
            yield index, fit(candidate, *arguments[index], run)
    if jobs:
        yield from candidate_runtime.results(kind, jobs, run)


def rebuild_candidate_model(
    dataframe: pd.DataFrame,
    *,
    task_type: TaskType,
    target_column: str | None,
    model_name: str,
    best_params: dict[str, Any],
    evaluation_column: str | None = None,
    excluded_columns: list[str] | None = None,
) -> Any:
    candidate = next(
        (item for item in candidate_catalog(task_type) if item.name == model_name),
        None,
    )
    if candidate is None:
        raise ValueError(f"Historical estimator '{model_name}' is no longer available.")
    if task_type == TaskType.CLUSTERING:
        features = dataframe.drop(
            columns=[evaluation_column] if evaluation_column else [],
            errors="ignore",
        )
        features = _normalize_temporal_features(features)
        estimator = clone(candidate.estimator)
        model = Pipeline(
            [
                ("correlation", CorrelatedFeatureFilter(task_type=task_type.value)),
                ("prepare", _preprocessor()),
                ("model", estimator),
            ]
        )
        model.set_params(**best_params)
        model.fit(features)
        return model

    if not target_column or target_column not in dataframe.columns:
        raise ValueError("The historical training target is unavailable.")
    target = dataframe[target_column]
    features = dataframe.drop(
        columns=[target_column, *(excluded_columns or [])],
        errors="ignore",
    )
    valid_target = target.notna()
    features = _normalize_temporal_features(features.loc[valid_target])
    target = target.loc[valid_target]
    if task_type in {TaskType.REGRESSION, TaskType.TIME_SERIES}:
        target = pd.to_numeric(target, errors="raise")
    train_x, _, train_y, _ = _supervised_split(
        features,
        target,
        task_type,
    )
    model = _supervised_model_pipeline(
        candidate.name,
        clone(candidate.estimator),
        task_type,
    )
    model.set_params(**best_params)
    model.fit(train_x, train_y)
    return model


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
    _force_cpu: bool = False,
    _fallback_from: str | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    print(f"Training candidate {candidate.name}", flush=True)
    try:
        candidate, fixed = fixed_candidate(
            candidate,
            run.params.get("parameter_overrides", []),
            target_classes=int(train_y.nunique()) if task_type == TaskType.CLASSIFICATION else None,
        )
        if "model__n_neighbors" in fixed:
            minimum_rows = (
                min(len(indices) for indices, _ in cv.split(train_x, train_y))
                if hasattr(cv, "split")
                else len(train_x) - math.ceil(len(train_x) / int(cv))
            )
            validate_conditionals(
                candidate.name,
                {key.removeprefix("model__"): value for key, value in fixed.items()},
                minimum_rows=minimum_rows,
            )
        cpu_threads, detected_gpu_vendor, rapids_active = _runtime_training_resources()
        gpu_vendor = None if _force_cpu else detected_gpu_vendor
        estimator, accelerator = configure_estimator_for_training(
            candidate,
            cpu_threads=cpu_threads,
            gpu_vendor=gpu_vendor,
            rapids_active=rapids_active and not _force_cpu,
        )
        print(
            f"Candidate {candidate.name} accelerator={accelerator} cpu_threads={cpu_threads}",
            flush=True,
        )
        model = _supervised_model_pipeline(candidate.name, estimator, task_type)
        search_evidence = None
        if (
            candidate.search_space
            and os.getenv("TRAINING_EXECUTION_MODE") == "ray"
            and not gpu_vendor
            and not _force_cpu
        ):
            from automl_api.training.tune_runtime import search_candidate

            _persist_candidate_phase(run.id, candidate.name, "hyperparameter_search")
            attempt_id = uuid.UUID(os.environ["AUTOML_ATTEMPT_ID"])
            safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", candidate.name)
            store = get_object_store()
            uri = store.uri_for_key(
                f"projects/{run.project_id}/runs/{run.id}/attempts/{attempt_id}/tune"
            )
            descriptor = store.dataframe_source(uri)
            path, filesystem = _ray_source(descriptor.path, descriptor.filesystem_options)
            search_evidence = search_candidate(
                model,
                candidate.search_space,
                train_x,
                train_y,
                cv,
                scoring,
                iterations=iterations,
                storage_path=path,
                storage_filesystem=filesystem,
                name=safe_name,
                cpus_per_trial=cpu_threads,
                run=run,
                candidate_name=candidate.name,
                base_parameters=_json_safe(estimator.get_params(deep=False)),
                max_concurrent=max(1, int(os.getenv("AUTOML_TUNE_CONCURRENCY", "1"))),
            )
            params = merge_fixed(_json_safe(search_evidence["params"]), fixed)
            cv_mean, cv_std = search_evidence["mean"], search_evidence["std"]
            _persist_candidate_phase(run.id, candidate.name, "fitting_final_model")
            fitted = clone(model).set_params(**params).fit(train_x, train_y)
        elif candidate.search_space:
            _persist_candidate_phase(run.id, candidate.name, "hyperparameter_search")
            from automl_api.training.trial_runtime import requires_durable_trials

            if requires_durable_trials():
                from automl_api.training.tune_runtime import search_candidate_local

                search_evidence = search_candidate_local(
                    model,
                    candidate.search_space,
                    train_x,
                    train_y,
                    cv,
                    scoring,
                    run=run,
                    candidate_name=candidate.name,
                    iterations=iterations,
                    base_parameters=_json_safe(estimator.get_params(deep=False)),
                    accelerator=accelerator,
                    fallback_from=_fallback_from,
                )
                params = merge_fixed(_json_safe(search_evidence["params"]), fixed)
                cv_mean, cv_std = search_evidence["mean"], search_evidence["std"]
                _persist_candidate_phase(run.id, candidate.name, "fitting_final_model")
                fitted = clone(model).set_params(**params).fit(train_x, train_y)
            else:
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
                params = merge_fixed(_json_safe(search.best_params_), fixed)
                cv_mean = float(search.best_score_)
                cv_std = float(search.cv_results_["std_test_score"][search.best_index_])
        else:
            _persist_candidate_phase(run.id, candidate.name, "cross_validating")

            def evaluate_cv():
                return {
                    "scores": cross_val_score(
                        model,
                        train_x,
                        train_y,
                        cv=cv,
                        scoring=scoring,
                        n_jobs=1,
                        error_score="raise",
                    ).tolist()
                }

            from automl_api.training.trial_runtime import evaluate_trial

            prefix = (
                "cpu-fallback:"
                if _fallback_from is not None
                else (f"{accelerator}:" if accelerator != "cpu" else "")
            )
            specification = {
                "parameters": _json_safe(estimator.get_params(deep=False)),
                "cv": repr(cv),
                "scoring": scoring,
                "accelerator": accelerator,
            }
            if _fallback_from is not None:
                specification["fallback_from"] = _fallback_from
            cv_result = evaluate_trial(
                run,
                candidate.name,
                f"{prefix}fixed-cv",
                specification,
                evaluate_cv,
            )
            cv_scores = np.asarray(cv_result["scores"])
            _persist_candidate_phase(run.id, candidate.name, "fitting_final_model")
            model.fit(train_x, train_y)
            fitted = model
            params = fixed
            cv_mean = float(np.mean(cv_scores))
            cv_std = float(np.std(cv_scores))

        _persist_candidate_phase(run.id, candidate.name, "evaluating")
        predictions = fitted.predict(test_x)
        if task_type == TaskType.CLASSIFICATION:
            metrics, diagnostics = classification_evaluation(
                fitted,
                test_x,
                test_y,
                predictions,
                positive_label=(
                    str(run.params["positive_label"])
                    if run.params.get("positive_label") is not None
                    else None
                ),
            )
        else:
            metrics, diagnostics = regression_evaluation(
                train_y,
                test_y,
                predictions,
                task_type,
            )
        diagnostics["correlated_features"] = fitted.named_steps["correlation"].evidence_
        selector = fitted.named_steps.get("select")
        if hasattr(selector, "statistics_rows_"):
            diagnostics["feature_selection_sampling"] = {
                "input_rows": selector.input_rows_,
                "statistics_rows": selector.statistics_rows_,
                "policy": "seeded_training_fold_sample",
            }
        diagnostics["cross_validation"] = {
            "folds": int(cv.n_splits) if hasattr(cv, "n_splits") else int(cv),
            "scoring": scoring,
            "mean": cv_mean,
            "standard_deviation": cv_std,
        }
        _persist_candidate_phase(run.id, candidate.name, "learning_curve")
        learning = _learning_curve_diagnostics(
            fitted,
            train_x,
            train_y,
            cv=cv,
            scoring=scoring,
        )
        if learning:
            diagnostics["learning_curve"] = learning
        diagnostics["runtime"] = {
            "search_engine": (
                search_evidence.get("engine", "ray_tune") if search_evidence else "local_sklearn"
            ),
            "search_evidence": search_evidence,
            "accelerator": accelerator,
            "detected_gpu_vendor": detected_gpu_vendor,
            "cpu_threads": cpu_threads,
            "rapids_active": rapids_active,
        }
        duration = round(time.monotonic() - started, 3)
        parent_run = mlflow.active_run()
        parent_run_id = (
            parent_run.info.run_id if parent_run else run.tags.get("candidate_parent_run_id")
        )
        registered_model_name = _registered_model_name(run, candidate.name)
        _persist_candidate_phase(run.id, candidate.name, "logging_to_mlflow")
        with _tracked_mlflow_run(
            run,
            role=f"candidate:{candidate.name}",
            run_name=candidate.name,
            parent_run_id=parent_run_id,
            tags={"candidate_model": candidate.name},
        ) as candidate_run:
            training_metadata.log_tags(
                {
                    "candidate_model": candidate.name,
                    "cost_tier": candidate.cost_tier,
                    "accelerator": accelerator,
                    "detected_gpu_vendor": detected_gpu_vendor or "none",
                    "cpu_threads": cpu_threads,
                    "rapids_active": rapids_active,
                }
            )
            training_metadata.log_params(params)
            _log_metrics_synchronously(metrics)
            _log_metric_synchronously("cv_primary_mean", cv_mean)
            _log_metric_synchronously("cv_primary_standard_deviation", cv_std)
            _log_metric_synchronously("fit_duration_seconds", duration)
            mlflow.log_dict(_json_safe(diagnostics), "evaluation.json")
            _log_sklearn_model(
                fitted,
                artifact_path="model",
                registered_model_name=registered_model_name,
                await_registration_for=60,
            )
        _mirror_candidate_evidence_to_parent(
            parent_run_id,
            candidate.name,
            metrics,
            candidate_run.info.run_id,
            registered_model_name,
            run_id=run.id,
        )
        _persist_candidate_phase(run.id, candidate.name, "saving_model")
        model_artifact_uri, model_artifact_sha256 = _persist_candidate_model(
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
            "model_artifact_sha256": model_artifact_sha256,
            "_model": fitted,
        }
    except (StaleFence, TrackingCreationUnresolved):
        raise
    except Exception as exc:
        if not _force_cpu and locals().get("accelerator") not in {None, "cpu"}:
            print(
                f"Candidate {candidate.name} GPU training failed; retrying on CPU: {exc}",
                flush=True,
            )
            return _fit_candidate(
                candidate,
                train_x,
                train_y,
                test_x,
                test_y,
                task_type,
                iterations,
                cv,
                scoring,
                run,
                _force_cpu=True,
                _fallback_from=accelerator,
            )
        return _failed_candidate(candidate, started, exc, run)


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

    candidate_limit = int(run.params.get("candidate_limit", 5))
    requested_names = run.params.get("candidate_models")
    candidates = select_candidates(
        TaskType.CLUSTERING,
        requested_names if isinstance(requested_names, list) else None,
        candidate_limit,
    )
    if not candidates:
        raise ValueError("No supported clustering candidates were selected.")
    folds = int(run.params.get("cv_folds", 3))
    if len(dataframe) < folds * 2:
        raise ValueError("Each requested clustering fold needs at least two validation rows.")
    splitter = KFold(n_splits=folds, shuffle=True, random_state=42)

    primary_metric = resolve_primary_metric(
        run.task_type,
        str(run.params.get("primary_metric")) if run.params.get("primary_metric") else None,
    )
    completed = _completed_candidates(run)
    leaderboard: list[dict[str, Any]] = [
        completed.get(candidate.name, _pending_candidate(candidate)) for candidate in candidates
    ]
    best_model: Any | None = None
    best_score: float | None = None
    maximize = metric_direction(primary_metric) == "maximize"
    _persist_partial_leaderboard(run.id, leaderboard, primary_metric)
    arguments = [(dataframe, reference_labels, splitter) for _ in candidates]
    for index, entry in _candidate_entries("clustering", candidates, arguments, run, completed):
        leaderboard[index] = entry
        if entry["status"] == "succeeded":
            candidate_model = entry.pop("_model", None)
            score = entry["metrics"].get(primary_metric)
            if score is not None and (
                best_score is None
                or (maximize and float(score) > best_score)
                or (not maximize and float(score) < best_score)
            ):
                best_model = candidate_model
                best_score = float(score)
            del candidate_model
        _persist_partial_leaderboard(run.id, leaderboard, primary_metric)

    leaderboard = rank_leaderboard(leaderboard, primary_metric)
    successful = [entry for entry in leaderboard if entry["status"] == "succeeded"]
    if not successful:
        failures = "; ".join(
            f"{entry['model']}: {entry.get('error', 'failed')}" for entry in leaderboard
        )
        raise RuntimeError(f"Every clustering candidate failed. {failures}")
    if best_model is None and best_score is not None:
        best_model = _restore_candidate(run, successful[0])["_model"]
    if best_model is None:
        raise RuntimeError(f"No candidate produced the primary metric '{primary_metric}'.")
    winner = successful[0]
    return TournamentResult(
        metrics=winner["metrics"],
        model=best_model,
        params={
            "winner": winner["model"],
            "evaluation_column": evaluation_column,
            **winner["best_params"],
        },
        leaderboard=leaderboard,
        primary_metric=primary_metric,
    )


def _fit_clustering_candidate(
    candidate: CandidateSpec,
    features: pd.DataFrame,
    reference_labels: np.ndarray | None,
    splitter: KFold,
    run: ModelRun,
) -> dict[str, Any]:
    started = time.monotonic()
    print(f"Training clustering candidate {candidate.name}", flush=True)
    try:
        minimum_rows = len(features) - math.ceil(len(features) / splitter.n_splits)
        if not hasattr(candidate.estimator, "predict"):
            minimum_rows = min(minimum_rows, len(features) // splitter.n_splits)
        candidate, fixed = fixed_candidate(
            candidate, run.params.get("parameter_overrides", []), minimum_rows=minimum_rows
        )
        cpu_threads, gpu_vendor, rapids_active = _runtime_training_resources()
        base_estimator, accelerator = configure_estimator_for_training(
            candidate,
            cpu_threads=cpu_threads,
            gpu_vendor=gpu_vendor,
            rapids_active=rapids_active,
        )
        parameter_options: list[dict[str, Any]] = [{}]
        available_parameters = base_estimator.get_params(deep=False)
        if "n_clusters" in available_parameters and "model__n_clusters" not in fixed:
            maximum_clusters = min(8, max(2, len(features) - 1))
            parameter_options = [
                {"n_clusters": cluster_count} for cluster_count in range(2, maximum_clusters + 1)
            ]
        from automl_api.training.trial_runtime import evaluate_trial

        prepared_folds = []

        def evaluate_parameters(parameters: dict[str, Any]) -> dict[str, Any]:
            if not prepared_folds:
                for train_index, test_index in splitter.split(features):
                    correlation_filter = CorrelatedFeatureFilter(
                        task_type=TaskType.CLUSTERING.value
                    )
                    train_features = correlation_filter.fit_transform(features.iloc[train_index])
                    test_features = correlation_filter.transform(features.iloc[test_index])
                    preprocessor = _preprocessor()
                    prepared_folds.append(
                        (
                            np.asarray(preprocessor.fit_transform(train_features)),
                            np.asarray(preprocessor.transform(test_features)),
                            test_index,
                        )
                    )
            fold_results = []
            for train_features, test_features, test_index in prepared_folds:
                estimator = clone(base_estimator).set_params(**parameters)
                if hasattr(estimator, "predict"):
                    estimator.fit(train_features)
                    labels = estimator.predict(test_features)
                else:
                    labels = estimator.fit_predict(test_features)
                fold_reference = (
                    reference_labels[test_index] if reference_labels is not None else None
                )
                fold_results.append(clustering_evaluation(test_features, labels, fold_reference))
            means, standard_deviations = aggregate_fold_metrics(fold_results)
            return {
                "means": means,
                "standard_deviations": standard_deviations,
                "fold_metrics": fold_results,
            }

        best_metrics = None
        best_standard_deviations = None
        best_fold_metrics = None
        best_params: dict[str, Any] = {}
        _persist_candidate_phase(run.id, candidate.name, "cross_validating")
        for index, parameters in enumerate(parameter_options):
            result = evaluate_trial(
                run,
                candidate.name,
                f"cluster:{index}",
                {
                    "base_parameters": _json_safe(available_parameters),
                    "parameter_options": parameter_options,
                    "parameters": parameters,
                    "cv": repr(splitter),
                    "accelerator": accelerator,
                    "external_evaluation": reference_labels is not None,
                },
                lambda parameters=parameters: evaluate_parameters(parameters),
            )
            means = result["means"]
            if "silhouette" not in means:
                continue
            if best_metrics is None or means["silhouette"] > best_metrics["silhouette"]:
                best_metrics = means
                best_standard_deviations = result["standard_deviations"]
                best_fold_metrics = result["fold_metrics"]
                best_params = parameters
        if best_metrics is None:
            raise ValueError("No cross-validation fold produced valid clusters.")

        _persist_candidate_phase(run.id, candidate.name, "fitting_final_model")
        correlation_filter = CorrelatedFeatureFilter(task_type=TaskType.CLUSTERING.value)
        filtered = correlation_filter.fit_transform(features)
        preprocessor = _preprocessor()
        transformed = np.asarray(preprocessor.fit_transform(filtered))
        final_estimator = clone(base_estimator).set_params(**best_params)
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
            "correlated_features": correlation_filter.evidence_,
            "runtime": {
                "accelerator": accelerator,
                "detected_gpu_vendor": gpu_vendor,
                "cpu_threads": cpu_threads,
                "rapids_active": rapids_active,
            },
        }
        pipeline_model = Pipeline(
            [
                ("correlation", correlation_filter),
                ("prepare", preprocessor),
                ("model", final_estimator),
            ]
        )
        params = merge_fixed(
            {f"model__{name}": value for name, value in best_params.items()}, fixed
        )
        duration = round(time.monotonic() - started, 3)
        parent_run = mlflow.active_run()
        parent_run_id = (
            parent_run.info.run_id if parent_run else run.tags.get("candidate_parent_run_id")
        )
        registered_model_name = _registered_model_name(run, candidate.name)
        _persist_candidate_phase(run.id, candidate.name, "logging_to_mlflow")
        with _tracked_mlflow_run(
            run,
            role=f"candidate:{candidate.name}",
            run_name=candidate.name,
            parent_run_id=parent_run_id,
            tags={"candidate_model": candidate.name},
        ) as candidate_run:
            training_metadata.log_tags(
                {
                    "candidate_model": candidate.name,
                    "cost_tier": candidate.cost_tier,
                    "external_clustering_evaluation": reference_labels is not None,
                    "accelerator": accelerator,
                    "detected_gpu_vendor": gpu_vendor or "none",
                    "cpu_threads": cpu_threads,
                    "rapids_active": rapids_active,
                }
            )
            training_metadata.log_params(_json_safe(params))
            _log_metrics_synchronously(best_metrics)
            _log_metric_synchronously("fit_duration_seconds", duration)
            mlflow.log_dict(_json_safe(diagnostics), "evaluation.json")
            _log_sklearn_model(
                pipeline_model,
                artifact_path="model",
                registered_model_name=registered_model_name,
                await_registration_for=60,
            )
        _mirror_candidate_evidence_to_parent(
            parent_run_id,
            candidate.name,
            best_metrics,
            candidate_run.info.run_id,
            registered_model_name,
            run_id=run.id,
        )
        _persist_candidate_phase(run.id, candidate.name, "saving_model")
        model_artifact_uri, model_artifact_sha256 = _persist_candidate_model(
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
            "model_artifact_sha256": model_artifact_sha256,
            "_model": pipeline_model,
        }
    except (StaleFence, TrackingCreationUnresolved):
        raise
    except Exception as exc:
        return _failed_candidate(candidate, started, exc, run)


def _failed_candidate(
    candidate: CandidateSpec,
    started: float,
    exc: Exception,
    run: ModelRun,
) -> dict[str, Any]:
    duration = round(time.monotonic() - started, 3)
    message = redact_text(str(exc))[:1000]
    print(f"Candidate {candidate.name} failed: {message}", flush=True)
    parent = mlflow.active_run()
    parent_run_id = parent.info.run_id if parent else run.tags.get("candidate_parent_run_id")
    with _tracked_mlflow_run(
        run,
        role=f"failure:{candidate.name}",
        run_name=candidate.name,
        parent_run_id=parent_run_id,
        tags={"candidate_model": candidate.name, "candidate_status": "failed"},
    ):
        training_metadata.log_tags(
            {
                "candidate_model": candidate.name,
                "candidate_status": "failed",
                "cost_tier": candidate.cost_tier,
                "failure_message": message[:250],
            }
        )
        _log_metric_synchronously("fit_duration_seconds", duration)
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


def _completed_candidates(run: ModelRun) -> dict[str, dict[str, Any]]:
    """A replacement attempt reuses durable successes from this immutable run."""
    return {
        entry["model"]: dict(entry)
        for entry in (getattr(run, "tags", None) or {}).get("leaderboard", [])
        if entry.get("status") == "succeeded"
        and entry.get("model_artifact_uri")
        and entry.get("model_artifact_sha256")
    }


def _restore_candidate(run: ModelRun, entry: dict[str, Any]) -> dict[str, Any]:
    if os.getenv("AUTOML_TRAINING_MODEL_READ_PROTOCOL") is not None:
        from automl_api.training.model_read import restore_model

        return {**entry, "resumed": True, "_model": restore_model(run, entry)}
    store = get_object_store()
    uri = str(entry["model_artifact_uri"])
    prefix = store.uri_for_key(f"projects/{run.project_id}/runs/{run.id}/")
    if not uri.startswith(prefix) or ".." in unquote(urlsplit(uri).path).split("/"):
        raise ValueError("The candidate checkpoint does not belong to this training run.")
    payload = store.read_bytes(uri)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), entry["model_artifact_sha256"]):
        raise ValueError("Candidate checkpoint integrity verification failed.")
    return {**entry, "resumed": True, "_model": joblib.load(io.BytesIO(payload))}


def _persist_candidate_model(
    run: ModelRun,
    model_name: str,
    model: Any,
) -> tuple[str, str]:
    buffer = io.BytesIO()
    joblib.dump(model, buffer, compress=3)
    if os.getenv("AUTOML_MODEL_OUTPUT_PROTOCOL") is not None:
        from automl_api.training.model_output import persist_model_output

        return persist_model_output(run, model_name, buffer.getvalue())
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", model_name).strip("-")
    attempt_id = os.getenv("AUTOML_ATTEMPT_ID")
    attempt_prefix = f"attempts/{uuid.UUID(attempt_id)}/" if attempt_id else ""
    payload = buffer.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    key = (
        f"projects/{run.project_id}/runs/{run.id}/{attempt_prefix}"
        f"models/{safe_name or 'model'}/{digest}.joblib"
    )
    return get_object_store().put_bytes(key, payload).uri, digest


def _registered_model_name(run: ModelRun, candidate_name: str) -> str:
    safe_candidate = re.sub(r"[^A-Za-z0-9_.-]+", "-", candidate_name).strip("-")
    return f"sceptre-{run.project_id}-{run.task_type.value}-{safe_candidate}"[:250]


def _mirror_candidate_evidence_to_parent(
    parent_run_id: str | None,
    candidate_name: str,
    metrics: dict[str, float],
    candidate_run_id: str,
    registered_model_name: str,
    *,
    run_id=None,
) -> None:
    if not parent_run_id:
        return
    if training_metadata.mirror(run_id, candidate_name, candidate_run_id, len(metrics)):
        return
    client = MlflowClient()
    safe_candidate = re.sub(r"[^A-Za-z0-9_.-]+", "-", candidate_name).strip("-")
    # Metrics belong to the nested candidate run. Mirroring the same points to
    # the parent can make MLflow retries violate its metric primary key.
    client.set_tag(parent_run_id, f"candidate.{safe_candidate}.metric_count", str(len(metrics)))
    client.set_tag(parent_run_id, f"candidate.{safe_candidate}.run_id", candidate_run_id)
    client.set_tag(
        parent_run_id,
        f"candidate.{safe_candidate}.registered_model",
        registered_model_name,
    )


def _log_metrics_synchronously(metrics: dict[str, float]) -> None:
    """Avoid MLflow batch retries that can duplicate partially committed rows."""
    for name, value in metrics.items():
        _log_metric_synchronously(name, value)


def _log_metric_synchronously(name: str, value: float) -> None:
    try:
        training_metadata.log_metric(name, float(value))
    except Exception as exc:
        message = str(exc).lower()
        if "duplicate key" in message and "metric_pk" in message:
            print(
                f"MLflow already persisted metric {name}; continuing after a safe retry.",
                flush=True,
            )
            return
        raise


def rank_leaderboard(
    entries: list[dict[str, Any]],
    primary_metric: str,
) -> list[dict[str, Any]]:
    successful = [
        entry
        for entry in entries
        if entry["status"] == "succeeded" and primary_metric in entry.get("metrics", {})
    ]
    unranked = [
        entry
        for entry in entries
        if entry["status"] == "succeeded" and primary_metric not in entry.get("metrics", {})
    ]
    failed = [entry for entry in entries if entry["status"] != "succeeded"]
    for entry in failed:
        entry["rank"] = None
        entry["primary_score"] = None
    for entry in unranked:
        entry["rank"] = None
        entry["primary_score"] = None
    reverse = metric_direction(primary_metric) == "maximize"
    successful.sort(
        key=lambda entry: float(entry["metrics"][primary_metric]),
        reverse=reverse,
    )
    for rank, entry in enumerate(successful, start=1):
        entry["rank"] = rank
        entry["primary_score"] = entry["metrics"][primary_metric]
    return [*successful, *unranked, *failed]


def _pending_candidate(candidate: CandidateSpec) -> dict[str, Any]:
    return {
        "rank": None,
        "model": candidate.name,
        "status": "pending",
        "phase": "waiting_for_worker" if os.getenv("AUTOML_MODEL_PODS") == "1" else None,
        "cost_tier": candidate.cost_tier,
        "primary_score": None,
        "metrics": {},
        "diagnostics": {},
        "best_params": {},
        "duration_seconds": None,
        "error": None,
        "mlflow_run_id": None,
    }


def merge_leaderboard_entries(
    existing: list[dict[str, Any]],
    additions: list[dict[str, Any]],
    primary_metric: str,
) -> list[dict[str, Any]]:
    by_model = {entry["model"]: dict(entry) for entry in existing}
    for entry in additions:
        by_model[entry["model"]] = dict(entry)
    return rank_leaderboard(list(by_model.values()), primary_metric)


def _sync_candidate_statuses(
    db: Session, run: ModelRun, entries: list[dict], *, context=None
) -> None:
    from automl_api.services.training_model_output import (
        require_published_models,
        require_tracking_entry_shapes,
    )

    require_tracking_entry_shapes(db, run, entries)
    terminal = {"succeeded", "failed", "cancelled", "skipped"}
    by_name = {
        entry["model"]: entry
        for entry in entries
        if entry.get("status") in terminal | {"pending", "running"}
    }
    rows = list(
        db.scalars(
            select(TrainingCandidate).where(
                TrainingCandidate.project_id == run.project_id,
                TrainingCandidate.model_run_id == run.id,
                TrainingCandidate.estimator_key.in_(by_name),
            )
        )
    )
    if len({row.estimator_key for row in rows}) != len(rows):
        raise StaleFence("Duplicate durable candidate identities require controlled migration")
    previous = {
        entry["model"]: entry
        for entry in (run.tags or {}).get("leaderboard", [])
        if (entry.get("extension_run_id") is None or entry.get("extension_run_id") == str(run.id))
    }
    for row in rows:
        entry = by_name[row.estimator_key]
        if row.status in terminal:
            if entry["status"] in terminal and entry["status"] != row.status:
                raise StaleFence("A terminal candidate cannot change its recorded outcome")
            if entry["status"] not in terminal:
                old = previous.get(row.estimator_key)
                if old is None or old.get("status") != row.status:
                    raise StaleFence("Terminal candidate evidence is missing or inconsistent")
                entry.clear()
                entry.update(old)
    require_published_models(
        db, run, entries, **({"context": context} if context is not None else {})
    )
    for row in rows:
        if row.status not in terminal:
            row.status = by_name[row.estimator_key]["status"]


def _persist_partial_leaderboard(run_id, entries, primary_metric):
    from automl_api.training.training_progress import publish_training_progress

    if (
        publish_training_progress(run_id, "partial", entries=entries, primary_metric=primary_metric)
        is not None
    ):
        return
    with get_session_factory()() as db:
        run = _locked_run(db, run_id)
        if (
            run is None
            or run.status in _TERMINAL_RUN_STATUSES
            or _has_committed_training_result(run)
        ):
            return
        _store_partial_leaderboard(db, run, entries, primary_metric)
        db.commit()


def _store_partial_leaderboard(db, run, entries, primary_metric, *, context=None, model_pods=None):
    from automl_api.services.training_model_output import require_tracking_entry_shapes

    require_tracking_entry_shapes(db, run, entries)
    ranked = rank_leaderboard(entries, primary_metric)
    successful = [entry for entry in ranked if entry["status"] == "succeeded"]
    if model_pods if model_pods is not None else os.getenv("AUTOML_MODEL_PODS") == "1":
        stored = {entry["model"]: entry for entry in run.tags.get("leaderboard", [])}
        ranked = rank_leaderboard(
            [
                {
                    **entry,
                    **stored.get(entry["model"], {}),
                    "phase": stored.get(entry["model"], {}).get("phase") or entry.get("phase"),
                }
                if entry["status"] in {"pending", "running"}
                else entry
                for entry in ranked
            ],
            primary_metric,
        )
        successful = [entry for entry in ranked if entry["status"] == "succeeded"]
    _sync_candidate_statuses(
        db, run, ranked, **({"context": context} if context is not None else {})
    )
    ranked = rank_leaderboard(ranked, primary_metric)
    successful = [entry for entry in ranked if entry["status"] == "succeeded"]
    active = next((entry for entry in ranked if entry["status"] == "running"), None)
    run.tags = {
        **run.tags,
        "leaderboard_primary_metric": primary_metric,
        "leaderboard": _json_safe(ranked),
        "winner": successful[0]["model"] if successful else None,
        "winner_mlflow_run_id": (successful[0].get("mlflow_run_id") if successful else None),
        "completed_candidates": sum(entry["status"] in {"succeeded", "failed"} for entry in ranked),
        "current_candidate": active["model"] if active else None,
        "candidate_phase": (
            (active.get("phase") or "training") if active else "between_candidates"
        ),
        "leaderboard_updated_at": datetime.now(UTC).isoformat(),
    }
    parent_id = run.tags.get("leaderboard_parent_run_id")
    if parent_id:
        try:
            parent = _locked_run(db, uuid.UUID(str(parent_id)), check_fence=False)
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
                "completed_candidates": sum(
                    entry["status"] in {"succeeded", "failed"} for entry in merged
                ),
                "leaderboard_updated_at": datetime.now(UTC).isoformat(),
            }


def _persist_candidate_phase(run_id, candidate, phase):
    from automl_api.training.training_progress import publish_training_progress

    if (
        publish_training_progress(run_id, "phase", candidate_name=candidate, phase=phase)
        is not None
    ):
        return
    with get_session_factory()() as db:
        run = _locked_run(db, run_id)
        if (
            run is None
            or run.status in _TERMINAL_RUN_STATUSES
            or _has_committed_training_result(run)
        ):
            return
        if _store_candidate_phase(db, run, candidate, phase):
            db.commit()


def _store_candidate_phase(db, run, candidate, phase, *, context=None):
    leaderboard = [
        (
            {
                **entry,
                "status": "running",
                "phase": phase,
                "phase_updated_at": datetime.now(UTC).isoformat(),
            }
            if entry.get("model") == candidate and entry.get("status") in {"pending", "running"}
            else entry
        )
        for entry in (run.tags or {}).get("leaderboard", [])
    ]
    _sync_candidate_statuses(
        db, run, leaderboard, **({"context": context} if context is not None else {})
    )
    if any(
        entry.get("model") == candidate
        and entry.get("status") in {"succeeded", "failed", "cancelled", "skipped"}
        for entry in leaderboard
    ):
        return False
    run.tags = {
        **(run.tags or {}),
        "current_candidate": candidate,
        "candidate_phase": phase,
        "candidate_phase_updated_at": datetime.now(UTC).isoformat(),
        "leaderboard": leaderboard,
    }
    return True


def _cross_validation_strategy(
    target: pd.Series,
    task_type: TaskType,
    requested_folds: int,
) -> Any:
    if task_type == TaskType.CLASSIFICATION:
        minimum_class_size = int(target.value_counts().min())
        if minimum_class_size < 2:
            raise ValueError("Each target class needs at least two training rows.")
        if minimum_class_size < requested_folds:
            raise ValueError(
                f"Each target class needs at least {requested_folds} training rows "
                "for the requested cross-validation folds."
            )
        return requested_folds
    if task_type == TaskType.TIME_SERIES:
        if len(target) < 6:
            raise ValueError("At least six training rows are required for time-series validation.")
        if len(target) <= requested_folds:
            raise ValueError("Time-series validation needs more rows than the requested folds.")
        return TimeSeriesSplit(n_splits=requested_folds)
    if len(target) < 4:
        raise ValueError("At least four training rows are required for cross-validation.")
    if len(target) < requested_folds:
        raise ValueError("There are fewer training rows than the requested validation folds.")
    return requested_folds


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


def _supervised_model_pipeline(model_name: str, estimator: Any, task_type: TaskType) -> Pipeline:
    return Pipeline(
        [
            ("correlation", CorrelatedFeatureFilter(task_type=task_type.value)),
            ("prepare", _preprocessor_for_model(model_name)),
            ("select", BoundedFeatureSelector(task_type=task_type.value, percentile=80)),
            ("model", estimator),
        ]
    )


def _preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ]
                ),
                make_column_selector(dtype_include="number"),
            ),
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
                make_column_selector(dtype_exclude="number"),
            ),
        ],
        remainder="drop",
    )


def _preprocessor_for_model(model_name: str) -> ColumnTransformer:
    if model_name == "CategoricalNB":
        return _categorical_nb_preprocessor()
    if model_name in {"ComplementNB", "MultinomialNB"}:
        return _non_negative_preprocessor()
    return _preprocessor()


def _categorical_nb_preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        (
                            "discretize",
                            KBinsDiscretizer(
                                n_bins=10,
                                encode="ordinal",
                                strategy="quantile",
                            ),
                        ),
                    ]
                ),
                make_column_selector(dtype_include="number"),
            ),
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
                        (
                            "non_negative",
                            FunctionTransformer(_shift_nonnegative, feature_names_out="one-to-one"),
                        ),
                    ]
                ),
                make_column_selector(dtype_exclude="number"),
            ),
        ],
        remainder="drop",
    )


def _non_negative_preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", MinMaxScaler(clip=True)),
                    ]
                ),
                make_column_selector(dtype_include="number"),
            ),
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
                        (
                            "non_negative",
                            FunctionTransformer(_shift_nonnegative, feature_names_out="one-to-one"),
                        ),
                    ]
                ),
                make_column_selector(dtype_exclude="number"),
            ),
        ],
        remainder="drop",
    )


def _shift_nonnegative(values: Any) -> np.ndarray:
    return np.asarray(values) + 1


def _runtime_training_resources() -> tuple[int, str | None, bool]:
    raw_threads = os.getenv("AUTOML_CPU_THREADS", "1")
    try:
        cpu_threads = max(1, int(float(raw_threads)))
    except ValueError:
        cpu_threads = 1
    gpu_vendor = os.getenv("AUTOML_GPU_VENDOR", "").strip().lower() or None
    if gpu_vendor not in {None, "nvidia", "intel"}:
        gpu_vendor = None
    rapids_active = os.getenv("AUTOML_RAPIDS_ACTIVE", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    return cpu_threads, gpu_vendor, rapids_active


def _learning_curve_diagnostics(
    estimator: Any,
    features: pd.DataFrame,
    target: pd.Series,
    *,
    cv: Any,
    scoring: str,
) -> dict[str, Any] | None:
    try:
        sizes, training_scores, validation_scores = learning_curve(
            estimator,
            features,
            target,
            train_sizes=np.linspace(0.25, 1.0, 4),
            cv=cv,
            scoring=scoring,
            n_jobs=1,
            error_score=np.nan,
        )
    except (TypeError, ValueError):
        return None
    if scoring.startswith("neg_"):
        training_scores = -training_scores
        validation_scores = -validation_scores
    points = []
    for index, size in enumerate(sizes):
        train_values = training_scores[index]
        validation_values = validation_scores[index]
        if np.all(np.isnan(train_values)) or np.all(np.isnan(validation_values)):
            continue
        points.append(
            {
                "training_rows": int(size),
                "training_mean": float(np.nanmean(train_values)),
                "training_std": float(np.nanstd(train_values)),
                "validation_mean": float(np.nanmean(validation_values)),
                "validation_std": float(np.nanstd(validation_values)),
            }
        )
    if not points:
        return None
    return {
        "scoring": scoring.removeprefix("neg_"),
        "points": points,
    }


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, np.generic):
        return value.item()
    return str(value)


def _has_committed_training_result(run: ModelRun, *, attempt_id=None) -> bool:
    receipt = (run.tags or {}).get("training_result_receipt")
    return bool(
        receipt
        and receipt.get("attempt_id")
        == (attempt_id if attempt_id is not None else os.getenv("AUTOML_ATTEMPT_ID"))
    )


def _mark_failed(run_id: uuid.UUID, exc: Exception) -> None:
    with get_session_factory()() as db:
        run = _locked_run(db, run_id)
        if run is None:
            return
        if run.status in _TERMINAL_RUN_STATUSES or _has_committed_training_result(run):
            return
        run.status = RunStatus.FAILED
        run.failure_code = "TRAINING_PIPELINE_FAILED"
        run.failure_message = redact_text(str(exc))
        run.plain_english_failure = (
            "Model training failed. Review the run logs for data quality, "
            "memory, model, or MLflow connectivity errors."
        )
        run.finished_at = datetime.now(UTC)
        db.commit()


def _persist_training_success(
    run_id: uuid.UUID,
    result: TournamentResult,
    mlflow_run_id: str,
) -> bool:
    from automl_api.training.training_result import publish_training_result

    published = publish_training_result(run_id, result, mlflow_run_id)
    if published is not None:
        return published
    with get_session_factory()() as db:
        persisted_run = _locked_run(db, run_id)
        if persisted_run is None or persisted_run.status in _TERMINAL_RUN_STATUSES:
            return False
        if _store_training_success(db, persisted_run, result, mlflow_run_id):
            db.commit()
    return True


def _store_training_success(db, persisted_run, result, mlflow_run_id, *, context=None):
    attempt_id = str(context[0]) if context is not None else os.getenv("AUTOML_ATTEMPT_ID")
    fenced_attempt = bool(attempt_id)
    from automl_api.services.training_tracking import require_run_parent_tracking

    tracking_attempt = require_run_parent_tracking(
        db,
        persisted_run,
        mlflow_run_id,
        context=context,
        live=not _has_committed_training_result(persisted_run),
    )
    receipt = None
    if fenced_attempt:
        receipt = {
            "attempt_id": str(uuid.UUID(attempt_id)),
            "digest": canonical_request_hash(
                _json_safe(
                    {
                        "metrics": result.metrics,
                        "params": result.params,
                        "leaderboard": result.leaderboard,
                        "primary_metric": result.primary_metric,
                        "mlflow_run_id": mlflow_run_id,
                    }
                )
            ),
        }
        previous = persisted_run.tags.get("training_result_receipt")
        if previous and previous.get("attempt_id") == receipt["attempt_id"]:
            if previous != receipt:
                raise StaleFence("This attempt already committed a different training result.")
            if tracking_attempt is not None:
                from automl_api.services.training_model_output import require_stored_tracking

                require_stored_tracking(db, tracking_attempt, persisted_run)
                from automl_api.services.training_metadata import result_metadata

                result_metadata(db, tracking_attempt, persisted_run, receipt)
            return False
    _sync_candidate_statuses(
        db,
        persisted_run,
        result.leaderboard,
        **({"context": context} if context is not None else {}),
    )
    persisted_run.status = RunStatus.RUNNING if fenced_attempt else RunStatus.SUCCEEDED
    persisted_run.mlflow_run_id = mlflow_run_id
    persisted_run.finished_at = None if fenced_attempt else datetime.now(UTC)
    persisted_run.tags = {
        **persisted_run.tags,
        "winner": result.leaderboard[0]["model"],
        "winner_mlflow_run_id": result.leaderboard[0].get("mlflow_run_id"),
        "winner_model_artifact_uri": result.leaderboard[0].get("model_artifact_uri"),
        **({"training_result_receipt": receipt} if receipt is not None else {}),
        "leaderboard_primary_metric": result.primary_metric,
        "leaderboard": result.leaderboard,
    }
    persisted_run.params = {
        **persisted_run.params,
        "excluded_leakage_columns": result.params.get(
            "excluded_leakage_columns",
            persisted_run.params.get("excluded_leakage_columns", []),
        ),
        "deduplicated_rows": result.params.get("deduplicated_rows", 0),
        "positive_label": result.params.get(
            "positive_label",
            persisted_run.params.get("positive_label"),
        ),
    }
    if tracking_attempt is not None:
        from automl_api.services.training_metadata import result_metadata

        result_metadata(db, tracking_attempt, persisted_run, receipt, publish=True)
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
    return True


def _locked_run(
    db: Session, run_id: uuid.UUID, *, check_fence: bool = True, context=None
) -> ModelRun | None:
    # Project deletion takes the project before its attempts and runs.
    db.execute(
        select(Project)
        .join(ModelRun, ModelRun.project_id == Project.id)
        .where(ModelRun.id == run_id)
        .with_for_update(read=True, of=Project)
    )
    attempt_id = str(context[0]) if context is not None else os.getenv("AUTOML_ATTEMPT_ID")
    if check_fence and attempt_id:
        attempt = db.scalar(
            select(WorkflowAttempt)
            .where(WorkflowAttempt.id == uuid.UUID(attempt_id))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            attempt is None
            or attempt.model_run_id != run_id
            or attempt.fencing_token
            != (context[1] if context is not None else os.getenv("AUTOML_FENCING_TOKEN"))
            or attempt.status not in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
            or (
                getattr(attempt, "lease_expires_at", None) is not None
                and attempt.lease_expires_at <= datetime.now(UTC)
            )
            or attempt_credentials_revoked(db, attempt.id)
        ):
            raise StaleFence("The training attempt no longer owns this run.")
    return db.scalar(select(ModelRun).where(ModelRun.id == run_id).with_for_update())
