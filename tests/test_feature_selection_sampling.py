import io

import joblib
import numpy as np
import pandas as pd
from automl_api.training.correlation import CorrelatedFeatureFilter
from automl_api.training.feature_selection import BoundedFeatureSelector, sample_training_rows
from sklearn.base import clone
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline


def test_fold_sample_is_reproducible_and_keeps_features_aligned_with_target():
    features = pd.DataFrame({"value": np.arange(2000)}, index=np.arange(2000) + 3000)
    target = features["value"] * 2
    first, labels = sample_training_rows(features, target, max_rows=100)
    second, _ = sample_training_rows(features, target, max_rows=100)
    assert first.equals(second)
    assert len(first) == 100
    assert labels.equals(first["value"] * 2)
    _, stratified = sample_training_rows(
        features, pd.Series([0] * 1800 + [1] * 200), max_rows=100, classification=True
    )
    assert stratified.value_counts().to_dict() == {0: 90, 1: 10}


def test_bounded_selection_keeps_every_training_and_prediction_row():
    rng = np.random.default_rng(42)
    features = rng.normal(size=(2000, 4))
    target = features[:, 0] * 2 + rng.normal(size=2000)
    pipeline = Pipeline([("select", BoundedFeatureSelector(max_rows=100)), ("model", Ridge())])
    fitted = clone(pipeline).fit(features, target)
    selector = fitted.named_steps["select"]
    assert selector.input_rows_ == 2000
    assert selector.statistics_rows_ == 100
    assert selector.transform(features).shape[0] == 2000
    buffer = io.BytesIO()
    joblib.dump(fitted, buffer)
    buffer.seek(0)
    restored = joblib.load(buffer)
    np.testing.assert_allclose(restored.predict(features), fitted.predict(features))
    np.testing.assert_allclose(
        clone(pipeline).fit(features, target).predict(features), fitted.predict(features)
    )


def test_correlation_evidence_reports_its_bounded_fit_sample():
    rng = np.random.default_rng(42)
    values = rng.normal(size=2000)
    features = pd.DataFrame({"signal": values, "duplicate": values, "noise": rng.normal(size=2000)})
    fitted = CorrelatedFeatureFilter("regression", statistics_max_rows=100).fit(
        features, pd.Series(values)
    )
    assert fitted.evidence_["input_rows"] == 2000
    assert fitted.evidence_["statistics_rows"] == 100
    assert fitted.evidence_["statistics_policy"] == "seeded_training_fold_sample"
    assert fitted.transform(features).shape == (2000, 2)
