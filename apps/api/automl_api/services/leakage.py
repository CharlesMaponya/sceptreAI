from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd

from automl_api.schemas.profiling import LeakageAnalysisRead, LeakageFindingRead

LEAKAGE_SAMPLE_ROWS = 25_000
_POST_OUTCOME_TOKENS = {
    "actual",
    "after",
    "approved",
    "closed",
    "decision",
    "final",
    "future",
    "label",
    "outcome",
    "paid",
    "prediction",
    "resolved",
    "result",
    "settled",
    "status",
}


def detect_target_leakage(
    dataframe: pd.DataFrame,
    target_column: str | None,
) -> LeakageAnalysisRead:
    """Find high-confidence target proxies without treating ordinary correlation as leakage."""
    if not target_column:
        return LeakageAnalysisRead(status="not_applicable")
    if target_column not in dataframe.columns:
        return LeakageAnalysisRead(
            status="unavailable",
            target_column=target_column,
            warnings=[f"Target column '{target_column}' was unavailable for leakage analysis."],
        )

    sample = dataframe.head(LEAKAGE_SAMPLE_ROWS).copy()
    analyzed_rows = len(sample)
    duplicate_count = int(sample.duplicated(keep="first").sum()) if analyzed_rows else 0
    findings: list[LeakageFindingRead] = []
    for column in sample.columns:
        column_name = str(column)
        if column_name == target_column:
            continue
        finding = _feature_leakage_finding(sample, column_name, target_column)
        if finding is not None:
            findings.append(finding)

    findings.sort(key=lambda item: (-item.confidence, item.column))
    excluded = [item.column for item in findings if item.auto_excluded]
    warnings: list[str] = []
    if duplicate_count:
        warnings.append(
            f"Detected {duplicate_count:,} duplicate rows in the leakage sample; "
            "training will deduplicate rows before the holdout split."
        )
    suspected = [item.column for item in findings if not item.auto_excluded]
    if suspected:
        warnings.append(
            "Review possible post-outcome features before training: " + ", ".join(suspected) + "."
        )
    return LeakageAnalysisRead(
        status="leakage_detected" if findings else "clear",
        target_column=target_column,
        analyzed_rows=analyzed_rows,
        duplicate_row_count=duplicate_count,
        duplicate_row_ratio=(duplicate_count / analyzed_rows if analyzed_rows else 0.0),
        findings=findings,
        excluded_columns=excluded,
        warnings=warnings,
    )


def _feature_leakage_finding(
    dataframe: pd.DataFrame,
    column: str,
    target_column: str,
) -> LeakageFindingRead | None:
    paired = dataframe[[column, target_column]].dropna()
    if len(paired) < 10:
        return None
    feature = paired[column]
    target = paired[target_column]
    usable = (_canonical(feature) != "") & (_canonical(target) != "")
    feature = feature.loc[usable]
    target = target.loc[usable]
    if len(feature) < 10:
        return None

    coverage = len(feature) / max(1, int(dataframe[target_column].notna().sum()))
    exact_ratio = float((_canonical(feature) == _canonical(target)).mean())
    evidence: dict[str, Any] = {
        "paired_rows": int(len(feature)),
        "coverage": round(coverage, 6),
        "exact_match_ratio": round(exact_ratio, 6),
    }
    if coverage >= 0.9 and exact_ratio >= 0.995:
        return LeakageFindingRead(
            column=column,
            kind="exact_target_copy",
            severity="critical",
            confidence=round(min(1.0, exact_ratio * coverage), 6),
            reason="The feature reproduces the target value row by row.",
            evidence=evidence,
            auto_excluded=True,
        )

    numeric_feature = pd.to_numeric(feature, errors="coerce")
    numeric_target = pd.to_numeric(target, errors="coerce")
    numeric_valid = numeric_feature.notna() & numeric_target.notna()
    if int(numeric_valid.sum()) >= 10 and float(numeric_valid.mean()) >= 0.95:
        correlation = float(
            np.corrcoef(numeric_feature.loc[numeric_valid], numeric_target.loc[numeric_valid])[0, 1]
        )
        if np.isfinite(correlation):
            evidence["pearson"] = round(correlation, 6)
            if coverage >= 0.9 and abs(correlation) >= 0.9999:
                return LeakageFindingRead(
                    column=column,
                    kind="deterministic_numeric_proxy",
                    severity="critical",
                    confidence=round(min(0.999999, abs(correlation) * coverage), 6),
                    reason="The feature is an almost exact numeric transform of the target.",
                    evidence=evidence,
                    auto_excluded=True,
                )

    mapping_purity = _mapping_purity(feature, target)
    feature_distinct = int(feature.nunique(dropna=True))
    target_distinct = int(target.nunique(dropna=True))
    evidence.update(
        {
            "mapping_purity": round(mapping_purity, 6),
            "feature_distinct": feature_distinct,
            "target_distinct": target_distinct,
        }
    )
    clean_pairs = pd.DataFrame({"feature": feature, "target": target})
    target_maps_back = bool(
        target_distinct > 1
        and clean_pairs.groupby("target", dropna=False)["feature"]
        .nunique(dropna=False)
        .max()
        == 1
    )
    compact_encoding = feature_distinct <= max(20, target_distinct * 2)
    if (
        coverage >= 0.9
        and target_distinct >= 2
        and mapping_purity >= 0.999
        and target_maps_back
        and compact_encoding
    ):
        return LeakageFindingRead(
            column=column,
            kind="encoded_target_proxy",
            severity="critical",
            confidence=round(min(0.999, mapping_purity * coverage), 6),
            reason="The feature is a deterministic alternate encoding of the target.",
            evidence=evidence,
            auto_excluded=True,
        )

    suspicious_name = _has_post_outcome_name(column, target_column)
    if suspicious_name and coverage >= 0.8 and mapping_purity >= 0.95:
        auto_excluded = mapping_purity >= 0.995 and coverage >= 0.9
        return LeakageFindingRead(
            column=column,
            kind="post_outcome_proxy",
            severity="critical" if auto_excluded else "warning",
            confidence=round(min(0.995, mapping_purity * coverage), 6),
            reason=(
                "The feature name suggests post-outcome information and it nearly determines "
                "the target."
            ),
            evidence=evidence,
            auto_excluded=auto_excluded,
        )
    return None


def _canonical(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.casefold().replace({"nan": "", "none": ""})


def _mapping_purity(feature: pd.Series, target: pd.Series) -> float:
    counts = pd.crosstab(feature, target, dropna=False)
    if counts.empty:
        return 0.0
    return float(counts.max(axis=1).sum() / counts.to_numpy().sum())


def _has_post_outcome_name(column: str, target_column: str) -> bool:
    tokens = set(filter(None, re.split(r"[^a-z0-9]+", column.casefold())))
    target_tokens = set(filter(None, re.split(r"[^a-z0-9]+", target_column.casefold())))
    return bool(tokens & _POST_OUTCOME_TOKENS or (target_tokens and target_tokens < tokens))
