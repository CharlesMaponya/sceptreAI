from __future__ import annotations

import pandas as pd
from automl_api.services import leakage


def test_leakage_is_not_applicable_or_unavailable() -> None:
    frame = pd.DataFrame({"feature": range(12)})
    assert leakage.detect_target_leakage(frame, None).status == "not_applicable"
    result = leakage.detect_target_leakage(frame, "target")
    assert result.status == "unavailable"
    assert result.warnings


def test_leakage_short_and_empty_pairs_are_ignored() -> None:
    short = pd.DataFrame({"feature": range(9), "target": range(9)})
    assert leakage._feature_leakage_finding(short, "feature", "target") is None
    blank = pd.DataFrame({"feature": [""] * 12, "target": [""] * 12})
    assert leakage._feature_leakage_finding(blank, "feature", "target") is None


def test_post_outcome_warning_is_reported_without_auto_exclusion() -> None:
    target = ["yes"] * 10 + ["no"] * 10
    feature = ["approved"] * 9 + ["rejected"] + ["rejected"] * 10
    result = leakage.detect_target_leakage(
        pd.DataFrame({"target": target, "decision_status": feature}), "target"
    )
    assert result.findings[0].kind == "post_outcome_proxy"
    assert result.findings[0].auto_excluded is False
    assert any("Review possible" in warning for warning in result.warnings)


def test_duplicate_warning_empty_mapping_and_name_tokens() -> None:
    frame = pd.DataFrame(
        {"target": ["yes"] * 10 + ["no"] * 10, "feature": [1] * 10 + [2] * 10}
    )
    result = leakage.detect_target_leakage(pd.concat([frame, frame.iloc[[0]]]), "target")
    assert result.duplicate_row_count >= 1
    assert any("duplicate" in item for item in result.warnings)
    assert leakage._mapping_purity(pd.Series(dtype=str), pd.Series(dtype=str)) == 0
    assert leakage._has_post_outcome_name("final_decision", "target")
    assert leakage._has_post_outcome_name("target_probability", "target")
    assert not leakage._has_post_outcome_name("ordinary_feature", "target")


def test_ordinary_finite_numeric_correlation_is_not_mislabeled_leakage() -> None:
    target = list(range(20))
    feature = [value % 5 for value in target]
    finding = leakage._feature_leakage_finding(
        pd.DataFrame({"target": target, "feature": feature}), "feature", "target"
    )
    assert finding is None
