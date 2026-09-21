import numpy as np
import pandas as pd
import pytest
from automl_api.training.analysis import _drift_summary, _run_evidently_report


@pytest.mark.parametrize("shift,expected_share", [(0, 0.0), (100, 1.0)])
def test_installed_evidently_report_preserves_obvious_distribution_changes(shift, expected_share):
    rng = np.random.default_rng(42)
    reference = pd.DataFrame(
        {"distance": rng.normal(1, 0.1, 200), "duration": rng.normal(10, 1, 200)}
    )
    current = reference + shift
    report = _run_evidently_report(reference, current)
    metrics, diagnostics = _drift_summary(report, 2)
    assert metrics["drift_share"] == expected_share
    assert metrics["drifted_feature_count"] == expected_share * 2
    assert diagnostics["dataset_drift"] == bool(shift)


def test_unrecognized_drift_report_is_not_reported_as_zero_drift():
    with pytest.raises(ValueError, match="no recognized drift measurements"):
        _drift_summary({"metrics": []}, 2)


@pytest.mark.parametrize("shift_days,expected_share", [(0, 0.0), (100, 0.5)])
def test_drift_compares_parquet_timestamps_with_csv_dates(shift_days, expected_share):
    timestamps = pd.Series(pd.date_range("2015-01-01", periods=200, freq="h"))
    reference = pd.DataFrame({
        "pickup_datetime": timestamps.astype("datetime64[us]"),
        "distance": np.random.default_rng(42).normal(1, 0.1, len(timestamps)),
    })
    current = reference.copy()
    current["pickup_datetime"] = (timestamps + pd.Timedelta(days=shift_days)).astype(str)
    metrics, _ = _drift_summary(_run_evidently_report(reference, current), 2)
    assert metrics["drift_share"] == expected_share
    assert metrics["drifted_feature_count"] == expected_share * 2
