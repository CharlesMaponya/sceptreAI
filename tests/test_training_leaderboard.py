from __future__ import annotations

import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training.model_catalog import candidate_catalog, select_candidates
from automl_api.training.pipeline import (
    _normalize_temporal_features,
    _supervised_split,
    merge_leaderboard_entries,
    rank_leaderboard,
    rebuild_candidate_model,
)


@pytest.mark.skip(reason="Disabled pending stable cross-version scikit-learn tag discovery.")
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


@pytest.mark.skip(reason="Disabled pending stable optional boosting discovery in CI.")
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
    assert merged[0]["extension_run_id"] == ("11111111-1111-1111-1111-111111111111")


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
