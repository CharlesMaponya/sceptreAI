from __future__ import annotations

from automl_api.api.routes.training import router
from automl_api.models.enums import TaskType
from automl_api.services.model_audit import _audit_html, _waterfall
from automl_api.services.model_evidence import (
    build_model_pipeline,
    feature_processing_contract,
    model_mathematics,
)


def test_audit_download_route_is_model_scoped() -> None:
    assert (
        "/projects/{project_id}/training/runs/{run_id}/models/"
        "{model_name}/audit-document"
    ) in {route.path for route in router.routes}


def test_pipeline_records_naive_bayes_processing_and_completed_stages() -> None:
    pipeline = build_model_pipeline(
        "CategoricalNB",
        TaskType.CLASSIFICATION,
        "succeeded",
        excluded_columns=["target_copy"],
    )

    assert all(stage["status"] == "completed" for stage in pipeline["stages"])
    assert pipeline["feature_processing"]["branch"] == "categorical_naive_bayes"
    assert "discretization" in " ".join(
        pipeline["feature_processing"]["numeric_features"]
    ).lower()
    assert "1 detected leakage" in pipeline["stages"][1]["summary"]


def test_non_negative_nb_contract_and_random_forest_maths_are_specific() -> None:
    contract = feature_processing_contract("MultinomialNB")
    maths = model_mathematics("RandomForestRegressor", TaskType.REGRESSION)

    assert contract["branch"] == "non_negative_naive_bayes"
    assert "[0, 1]" in contract["numeric_features"][1]
    assert maths["family"] == "Bagged decision-tree ensemble"
    assert "Σ" in maths["equation"]


def test_waterfall_preserves_direction_and_normalizes_magnitude() -> None:
    waterfall = _waterfall(
        {
            "feature_names": ["income", "debt"],
            "sample_feature_values": [{"income": 75_000, "debt": 12_000}],
            "base_values": [0.42],
            "prediction_values": [0.73],
            "shap_values": [[0.3, -0.1]],
        },
    )

    assert waterfall["status"] == "available"
    assert waterfall["base_value"] == 0.42
    assert waterfall["features"][0]["direction"] == "increases_output"
    assert waterfall["features"][1]["direction"] == "decreases_output"
    assert sum(item["absolute_percent"] for item in waterfall["features"]) == 100.0


def test_audit_html_is_portable_readable_and_escapes_evidence() -> None:
    pipeline = build_model_pipeline(
        "Ridge<script>",
        TaskType.REGRESSION,
        "succeeded",
    )
    report = {
        "document": {
            "generated_at": "2026-07-18T12:00:00Z",
            "evidence_sha256": "abc123",
            "missing_evidence": [],
            "regulatory_note": "Evidence only.",
        },
        "model_identity": {
            "model_name": "Ridge<script>",
            "task_type": "regression",
            "target_column": "performance",
            "candidate_status": "succeeded",
            "rank": 1,
            "training_run_id": "run-1",
        },
        "dataset_and_target": {
            "target_visualization": {
                "semantic_type": "continuous",
                "missing_count": 0,
                "missing_ratio": 0,
                "distinct_count": 10,
                "statistics": {"median": 50},
                "distribution_type": "histogram",
                "distribution": [{"label": "0–10", "count": 4}],
            }
        },
        "feature_processing": {
            "executable_training_contract": feature_processing_contract("Ridge"),
            "profiling_recommendations": [],
        },
        "training_pipeline": pipeline,
        "model_training": {"status": "succeeded"},
        "model_mathematics": model_mathematics("Ridge", TaskType.REGRESSION),
        "model_metrics": {"values": {"r2": 0.91}, "diagnostics": {}},
        "feature_contributions": {
            "global_normalized_contributions": [
                {"feature": "hours", "contribution_percent": 100}
            ],
            "waterfall": {
                "status": "available",
                "base_value": 0.4,
                "prediction_value": 0.7,
                "features": [
                    {
                        "feature": "hours",
                        "shap_value": 0.3,
                        "absolute_percent": 100,
                    }
                ],
            },
        },
    }

    rendered = _audit_html(report)

    assert "Ridge&lt;script&gt;" in rendered
    assert "Ridge<script>" not in rendered
    assert "Training pipeline" in rendered
    assert "Normalized feature contributions" in rendered
    assert "Representative SHAP waterfall" in rendered
    assert "@media print" in rendered
