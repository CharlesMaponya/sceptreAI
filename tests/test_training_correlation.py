from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from automl_api.training.correlation import CorrelatedFeatureFilter


def test_binary_classification_removes_the_correlated_feature_with_lower_iv() -> None:
    random = np.random.default_rng(7)
    target = pd.Series(np.tile([0, 1], 100))
    strong = target + random.normal(0, 0.18, len(target))
    features = pd.DataFrame(
        {
            "strong": strong,
            "weak": strong + random.normal(0, 0.18, len(target)),
            "other": random.normal(size=len(target)),
            "segment": ["a", "b"] * 100,
        }
    )

    fitted = CorrelatedFeatureFilter("classification").fit(features, target)

    assert fitted.score_method_ == "information_value"
    assert fitted.scores_["strong"] > fitted.scores_["weak"]
    assert fitted.removed_features_[0]["feature"] == "weak"
    assert fitted.transform(features).columns.tolist() == ["strong", "other", "segment"]
    assert fitted.evidence_["before"]["columns"]
    assert fitted.evidence_["after"]["columns"]


@pytest.mark.parametrize(
    ("task_type", "target", "score_method"),
    [
        (
            "classification",
            pd.Series([0, 1, 2, 0, 1, 2] * 10),
            "mutual_information_classification",
        ),
        (
            "regression",
            pd.Series(np.linspace(0, 10, 60)),
            "mutual_information_regression",
        ),
        (
            "time_series",
            pd.Series(np.linspace(0, 10, 60)),
            "mutual_information_regression",
        ),
    ],
)
def test_supervised_task_types_use_their_supported_relevance_score(
    task_type: str,
    target: pd.Series,
    score_method: str,
) -> None:
    feature = np.linspace(0, 1, len(target))
    features = pd.DataFrame({"first": feature, "duplicate": feature})

    fitted = CorrelatedFeatureFilter(task_type).fit(features, target)

    assert fitted.score_method_ == score_method
    assert len(fitted.removed_features_) == 1
    assert fitted.transform(features).shape[1] == 1


def test_clustering_keeps_the_more_complete_correlated_feature() -> None:
    complete = np.arange(20, dtype=float)
    incomplete = complete.copy()
    incomplete[[2, 9]] = np.nan
    features = pd.DataFrame({"complete": complete, "incomplete": incomplete})

    fitted = CorrelatedFeatureFilter("clustering").fit(features)

    assert fitted.score_method_ == "non_missing_rate"
    assert fitted.removed_features_[0]["feature"] == "incomplete"
    assert fitted.transform(features).columns.tolist() == ["complete"]
