from __future__ import annotations

import builtins
import uuid
from types import SimpleNamespace

import automl_api.training.pipeline as training_pipeline
import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.services.training import _rank_combined_leaderboard
from automl_api.training.evaluation import (
    cross_validation_scoring,
    resolve_primary_metric,
)
from automl_api.training.model_catalog import (
    CandidateSpec,
    _external_estimators,
    candidate_catalog,
    configure_estimator_for_training,
    select_candidates,
    supported_gpu_vendors,
)
from automl_api.training.pipeline import (
    _log_metrics_synchronously,
    _normalize_temporal_features,
    _pending_candidate,
    _preprocessor_for_model,
    _registered_model_name,
    _supervised_split,
    merge_leaderboard_entries,
    rank_leaderboard,
    rebuild_candidate_model,
)
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import CategoricalNB
from sklearn.pipeline import Pipeline


def test_supervised_tasks_have_bounded_candidate_catalogs() -> None:
    classification = candidate_catalog(TaskType.CLASSIFICATION)
    regression = candidate_catalog(TaskType.REGRESSION)
    time_series = candidate_catalog(TaskType.TIME_SERIES)

    assert len(classification) >= 8
    assert len(regression) >= 8
    assert [candidate.name for candidate in time_series] == [
        candidate.name for candidate in regression
    ]
    assert len(select_candidates(TaskType.CLASSIFICATION, None, 3)) == 3
    clustering = candidate_catalog(TaskType.CLUSTERING)
    assert {candidate.name for candidate in clustering}.issuperset({"KMeans", "Birch", "DBSCAN"})


def test_external_boosting_estimators_are_available_for_supervised_tasks() -> None:
    expected = {
        TaskType.CLASSIFICATION: {
            "XGBClassifier",
            "LGBMClassifier",
            "CatBoostClassifier",
        },
        TaskType.REGRESSION: {
            "XGBRegressor",
            "LGBMRegressor",
            "CatBoostRegressor",
        },
        TaskType.TIME_SERIES: {
            "XGBRegressor",
            "LGBMRegressor",
            "CatBoostRegressor",
        },
    }

    for task_type, model_names in expected.items():
        catalog_names = {candidate.name for candidate in candidate_catalog(task_type)}
        assert model_names.issubset(catalog_names)

    clustering_names = {candidate.name for candidate in candidate_catalog(TaskType.CLUSTERING)}
    assert not any(name.startswith(("XGB", "LGBM", "CatBoost")) for name in clustering_names)


def test_one_broken_optional_estimator_library_does_not_hide_the_others(monkeypatch) -> None:
    original_import = builtins.__import__

    class StubEstimator:
        def __init__(self, **_: object) -> None:
            pass

    modules = {
        "xgboost": SimpleNamespace(XGBRegressor=StubEstimator),
        "catboost": SimpleNamespace(
            CatBoostClassifier=StubEstimator,
            CatBoostRegressor=StubEstimator,
        ),
    }

    def controlled_import(name, *args, **kwargs):
        if name == "lightgbm":
            raise OSError("libgomp.so.1 is unavailable")
        if name in modules:
            return modules[name]
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", controlled_import)

    model_names = {
        name for name, _ in _external_estimators(TaskType.REGRESSION)
    }

    assert "XGBRegressor" in model_names
    assert "CatBoostRegressor" in model_names
    assert "LGBMRegressor" not in model_names


@pytest.mark.skip(
    reason="Disabled pending stable cross-version historical estimator reconstruction."
)
def test_historical_candidate_can_be_reconstructed_for_explainability() -> None:
    dataframe = pd.DataFrame(
        {
            "amount": [float(index) for index in range(30)],
            "segment": ["retail", "business"] * 15,
            "target": ["yes", "no"] * 15,
        }
    )

    model = rebuild_candidate_model(
        dataframe,
        task_type=TaskType.CLASSIFICATION,
        target_column="target",
        model_name="LogisticRegression",
        best_params={"model__C": 1.0, "model__max_iter": 200},
    )

    predictions = model.predict(dataframe.drop(columns=["target"]))
    assert len(predictions) == len(dataframe)


def test_leaderboard_ranks_higher_and_lower_metrics_correctly() -> None:
    classification = rank_leaderboard(
        [
            {
                "model": "A",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.7},
            },
            {
                "model": "B",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.8},
            },
            {"model": "C", "status": "failed", "metrics": {}},
        ],
        "balanced_accuracy",
    )
    regression = rank_leaderboard(
        [
            {"model": "A", "status": "succeeded", "metrics": {"rmse": 2.0}},
            {"model": "B", "status": "succeeded", "metrics": {"rmse": 1.0}},
        ],
        "rmse",
    )

    assert [entry["model"] for entry in classification] == ["B", "A", "C"]
    assert classification[0]["rank"] == 1
    assert [entry["model"] for entry in regression] == ["B", "A"]


def test_pending_candidate_is_visible_without_metrics_or_rank() -> None:
    candidate = CandidateSpec(
        name="ExtraTreesClassifier",
        estimator=RandomForestClassifier(),
        search_space={},
        cost_tier="medium",
        default_selected=True,
    )

    entry = _pending_candidate(candidate)
    ranked = rank_leaderboard([entry], "balanced_accuracy")

    assert ranked == [entry]
    assert entry["status"] == "pending"
    assert entry["metrics"] == {}
    assert entry["primary_score"] is None
    assert entry["rank"] is None


def test_successful_model_without_selected_metric_remains_unranked() -> None:
    entries = rank_leaderboard(
        [
            {"model": "A", "status": "succeeded", "metrics": {"accuracy": 0.9}},
            {
                "model": "B",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.8},
            },
        ],
        "balanced_accuracy",
    )

    assert [entry["model"] for entry in entries] == ["B", "A"]
    assert entries[0]["rank"] == 1
    assert entries[1]["rank"] is None


def test_primary_metric_resolution_is_task_specific() -> None:
    assert resolve_primary_metric(TaskType.REGRESSION, None) == "rmse"
    assert cross_validation_scoring(TaskType.REGRESSION, "mae") == "neg_mean_absolute_error"
    assert (
        cross_validation_scoring(TaskType.CLASSIFICATION, "roc_auc", target_classes=3)
        == "roc_auc_ovr_weighted"
    )
    with pytest.raises(ValueError, match="not supported"):
        resolve_primary_metric(TaskType.REGRESSION, "accuracy")


def test_categorical_nb_preprocessing_never_produces_negative_values() -> None:
    features = pd.DataFrame(
        {
            "amount": [-20.5, -3.0, 0.0, 4.5, 8.0, 12.0, 30.0, 45.0],
            "segment": ["a", "b", "a", "c", "b", "a", "c", "b"],
        }
    )
    target = pd.Series([0, 1, 0, 1, 1, 0, 1, 1])
    model = Pipeline(
        [
            ("prepare", _preprocessor_for_model("CategoricalNB")),
            ("model", CategoricalNB()),
        ]
    )

    model.fit(features, target)
    transformed = model.named_steps["prepare"].transform(
        pd.DataFrame({"amount": [-100.0], "segment": ["unseen"]})
    )

    assert transformed.min() >= 0
    assert model.predict(features).shape == (len(features),)


def test_rapids_accelerator_is_selected_for_supported_sklearn_models() -> None:
    candidate = CandidateSpec(
        name="RandomForestClassifier",
        estimator=RandomForestClassifier(n_jobs=1),
        search_space={},
        cost_tier="medium",
        default_selected=True,
    )

    estimator, accelerator = configure_estimator_for_training(
        candidate,
        cpu_threads=6,
        gpu_vendor="nvidia",
        rapids_active=True,
    )

    assert accelerator == "rapids_cuml"
    assert estimator.n_jobs == 6
    assert supported_gpu_vendors(candidate.name) == {"nvidia"}


def test_candidate_parent_linkage_does_not_duplicate_mlflow_metrics(monkeypatch) -> None:
    metrics: list[tuple[str, str, float]] = []
    tags: list[tuple[str, str, str]] = []
    client = SimpleNamespace(
        log_metric=lambda run_id, key, value: metrics.append((run_id, key, value)),
        set_tag=lambda run_id, key, value: tags.append((run_id, key, value)),
    )
    monkeypatch.setattr(training_pipeline, "MlflowClient", lambda: client)
    run = SimpleNamespace(project_id=uuid.uuid4(), task_type=TaskType.CLASSIFICATION)
    registry_name = _registered_model_name(run, "Random Forest/Classifier")

    training_pipeline._mirror_candidate_evidence_to_parent(
        "parent-run",
        "Random Forest/Classifier",
        {"accuracy": 0.91, "roc_auc": 0.94},
        "candidate-run",
        registry_name,
    )

    assert metrics == []
    assert ("parent-run", "candidate.Random-Forest-Classifier.metric_count", "2") in tags
    assert any(
        key.endswith("registered_model") and value == registry_name
        for _, key, value in tags
    )


def test_mlflow_metrics_are_logged_individually_and_synchronously(monkeypatch) -> None:
    calls: list[tuple[str, float, int, bool]] = []
    monkeypatch.setattr(
        training_pipeline.mlflow,
        "log_metric",
        lambda name, value, *, step, synchronous: calls.append(
            (name, value, step, synchronous)
        ),
    )

    _log_metrics_synchronously({"mse": 4.2, "r2": 0.98})

    assert calls == [
        ("mse", 4.2, 0, True),
        ("r2", 0.98, 0, True),
    ]


def test_duplicate_mlflow_metric_retry_is_treated_as_already_persisted(monkeypatch) -> None:
    def duplicate(*args, **kwargs):
        raise RuntimeError("duplicate key value violates unique constraint metric_pk")

    monkeypatch.setattr(training_pipeline.mlflow, "log_metric", duplicate)

    _log_metrics_synchronously({"mse": 4.2})


def test_incremental_models_replace_failed_entries_and_rerank() -> None:
    merged = merge_leaderboard_entries(
        [
            {
                "model": "RandomForestClassifier",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.82},
            },
            {
                "model": "XGBClassifier",
                "status": "failed",
                "metrics": {},
            },
        ],
        [
            {
                "model": "XGBClassifier",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.87},
                "extension_run_id": "11111111-1111-1111-1111-111111111111",
            }
        ],
        "balanced_accuracy",
    )

    assert [entry["model"] for entry in merged] == [
        "XGBClassifier",
        "RandomForestClassifier",
    ]
    assert merged[0]["rank"] == 1
    assert merged[1]["rank"] == 2
    assert merged[0]["extension_run_id"] == ("11111111-1111-1111-1111-111111111111")


def test_ranking_replaces_child_local_ranks_and_clears_pending_ranks() -> None:
    ranked = rank_leaderboard(
        [
            {
                "model": "OriginalWinner",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.91},
                "rank": 1,
            },
            {
                "model": "ExtensionWinner",
                "status": "succeeded",
                "metrics": {"balanced_accuracy": 0.95},
                "rank": 1,
            },
            {
                "model": "NotStarted",
                "status": "pending",
                "metrics": {},
                "rank": 2,
                "primary_score": 0.8,
            },
        ],
        "balanced_accuracy",
    )

    assert [entry["rank"] for entry in ranked] == [1, 2, None]
    assert [entry["model"] for entry in ranked[:2]] == [
        "ExtensionWinner",
        "OriginalWinner",
    ]
    assert ranked[2]["primary_score"] is None


def test_api_combined_leaderboard_has_one_entry_per_rank() -> None:
    combined = _rank_combined_leaderboard(
        [
            {
                "model": "ParentA",
                "status": "succeeded",
                "metrics": {"rmse": 2.0},
                "rank": 1,
            },
            {
                "model": "ChildA",
                "status": "succeeded",
                "metrics": {"rmse": 1.0},
                "rank": 1,
            },
            {
                "model": "ChildB",
                "status": "succeeded",
                "metrics": {"rmse": 1.5},
                "rank": 2,
            },
        ],
        "rmse",
    )

    assert [entry["model"] for entry in combined] == ["ChildA", "ChildB", "ParentA"]
    assert [entry["rank"] for entry in combined] == [1, 2, 3]


def test_time_series_split_is_chronological() -> None:
    features = pd.DataFrame(
        {
            "event_date": [5.0, 1.0, 4.0, 2.0, 3.0, 6.0, 8.0, 7.0, 9.0, 10.0],
            "value": range(10),
        }
    )
    target = pd.Series(range(10))

    train_x, test_x, train_y, test_y = _supervised_split(
        features,
        target,
        TaskType.TIME_SERIES,
    )

    assert train_x["event_date"].max() < test_x["event_date"].min()
    assert list(train_y.index) == list(train_x.index)
    assert list(test_y.index) == list(test_x.index)


def test_unix_timestamp_features_are_normalized_to_epoch_days() -> None:
    features = pd.DataFrame(
        {
            "event_epoch": [
                1_704_067_200_000,
                1_704_153_600_000,
            ],
            "amount": [10.0, 20.0],
        }
    )

    normalized = _normalize_temporal_features(features)

    assert normalized["event_epoch"].tolist() == [19723.0, 19724.0]
    assert normalized["amount"].tolist() == [10.0, 20.0]
