from __future__ import annotations

import copy
import io
import re

from automl_api.api.routes.training import router
from automl_api.models.enums import TaskType
from automl_api.services.model_audit import (
    EvidenceChartFlowable,
    TargetDistributionFlowable,
    _audit_pdf,
    _diagnostic_chart_specs,
    _feature_action_rows,
    _logo_path,
    _waterfall,
)
from automl_api.services.model_evidence import (
    build_model_pipeline,
    feature_processing_contract,
    model_mathematics,
)
from reportlab.pdfgen import canvas


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
    assert pipeline["diagram"]["transformer"]["type"] == "ColumnTransformer"
    assert [branch["key"] for branch in pipeline["diagram"]["transformer"]["branches"]] == [
        "numeric",
        "categorical",
    ]
    assert pipeline["diagram"]["estimator"]["type"] == "CategoricalNB"


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


def test_governance_visuals_use_persisted_profile_and_model_diagnostics() -> None:
    processing = {
        "target_column": "Performance Index",
        "feature_profiles": {
            "Hours Studied": {"semantic_type": "numerical_continuous"},
            "Performance Index": {"semantic_type": "numerical_continuous"},
        },
        "profiling_recommendations": [
            {
                "column": "Hours Studied",
                "action": "scale_and_check_outliers",
                "strategy": "robust_scaler_iqr_outlier_flags",
                "reason": "Numeric features benefit from robust scaling.",
            }
        ],
    }
    rows = _feature_action_rows(processing)
    samples = [
        {"order": 0, "actual": 42.0, "predicted": 41.5, "residual": 0.5},
        {"order": 1, "actual": 55.0, "predicted": 56.0, "residual": -1.0},
    ]
    diagnostics = {
        "prediction_samples": samples,
        "learning_curve": {
            "scoring": "root_mean_squared_error",
            "points": [
                {
                    "training_rows": 50,
                    "training_mean": 2.0,
                    "validation_mean": 2.2,
                }
            ],
        },
        "cross_validation": {"mean": -2.1, "standard_deviation": 0.08},
    }
    specs = _diagnostic_chart_specs("regression", diagnostics)
    target = TargetDistributionFlowable(
        {
            "distribution": [
                {"label": "10–20", "count": 12},
                {"label": "20–30", "count": 18},
            ]
        },
        task="regression",
        target_name="Performance Index",
    )

    assert rows == [
        [
            "Hours Studied",
            "Numerical continuous",
            "Robust scaler iqr outlier flags",
            "Numeric features benefit from robust scaling.",
            "Scale and check outliers",
        ]
    ]
    assert target.orientation == "vertical"
    assert target.items == [("10–20", 12.0), ("20–30", 18.0)]
    assert [spec[0] for spec in specs] == [
        "Actual vs predicted",
        "Residual distribution",
        "Learning curve · Root mean squared error",
        "Cross-validation stability",
    ]
    assert specs[0][2] is samples


def test_classification_governance_visuals_match_leaderboard_evidence() -> None:
    diagnostics = {
        "labels": ["negative", "positive"],
        "confusion_matrix": [[18, 2], [3, 17]],
        "roc_curves": [{"label": "positive", "points": []}],
        "precision_recall_curves": [{"label": "positive", "points": []}],
        "classification_report": {
            "negative": {"precision": 0.86, "recall": 0.9, "f1-score": 0.88}
        },
    }

    specs = _diagnostic_chart_specs("classification", diagnostics)

    assert [spec[0] for spec in specs] == [
        "Confusion matrix",
        "ROC curve",
        "Precision–recall curve",
        "Per-class quality",
    ]
    assert specs[0][2]["matrix"] == [[18, 2], [3, 17]]
    assert _logo_path() is not None
    assert _logo_path().name == "sceptre-icon.png"
    for title, kind, payload in specs:
        output = io.BytesIO()
        pdf_canvas = canvas.Canvas(output)
        chart = EvidenceChartFlowable(title, kind, payload)
        assert chart.border_visible is False
        chart.canv = pdf_canvas
        chart.draw()
        pdf_canvas.save()
        assert output.getvalue().startswith(b"%PDF-")


def test_audit_pdf_is_branded_and_contains_complete_model_evidence() -> None:
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
            "project_name": "Student performance",
            "project_description": "Predict outcomes while documenting preprocessing.",
        },
        "dataset_and_target": {
            "dataset_version_id": "version-1",
            "content_hash": "dataset-hash",
            "rows": 100,
            "columns": 5,
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
            "target_column": "performance",
            "feature_profiles": {
                "hours": {"semantic_type": "numerical_continuous"},
                "performance": {"semantic_type": "numerical_continuous"},
            },
            "profiling_recommendations": [
                {
                    "column": "hours",
                    "action": "scale_and_check_outliers",
                    "strategy": "robust_scaler_iqr_outlier_flags",
                    "reason": "Numeric features benefit from robust scaling.",
                }
            ],
            "leakage_analysis": {"status": "complete", "findings": []},
        },
        "training_pipeline": pipeline,
        "model_training": {"status": "succeeded", "primary_metric": "r2"},
        "model_metrics": {
            "values": {"r2": 0.91, "mae": 1.2},
            "diagnostics": {
                "prediction_samples": [
                    {"order": 0, "actual": 50.0, "predicted": 49.5, "residual": 0.5},
                    {"order": 1, "actual": 60.0, "predicted": 61.0, "residual": -1.0},
                ],
                "learning_curve": {
                    "scoring": "rmse",
                    "points": [
                        {
                            "training_rows": 50,
                            "training_mean": 1.1,
                            "validation_mean": 1.3,
                        },
                        {
                            "training_rows": 100,
                            "training_mean": 1.2,
                            "validation_mean": 1.25,
                        },
                    ],
                },
                "cross_validation": {"mean": -1.28, "standard_deviation": 0.04},
            },
        },
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
        "evidence_provenance": {
            "profile_job_id": "profile-1",
            "explainability_run_id": "explain-1",
            "source_dataset_hash": "dataset-hash",
        },
    }

    rendered = _audit_pdf(report)

    assert rendered.startswith(b"%PDF-")
    assert len(rendered) > 10_000
    assert b"/Subtype /Image" in rendered
    assert b"/Annots" in rendered
    assert b"Model governance document" in rendered
    assert b"Actual vs predicted" in rendered
    assert b"0.91" in rendered
    assert len(re.findall(rb"/Type\s*/Page\b", rendered)) == 10
    assert b"Task type and target visualization" in rendered
    assert b"Team leader name" in rendered
    assert b"Team leader signature" in rendered
    assert b"10. Change log" in rendered
    assert b"Recorded diagnostics" not in rendered
    ordered_sections = [
        b"1. Overview",
        b"2. Task type and target visualization",
        b"3. Feature preparation and recorded actions",
        b"4. Training pipeline, record and tuning",
        b"5. Model performance",
        b"6. Explainability and feature contributions",
        b"7. Monitoring and thresholds",
        b"8. Risk assessment",
        b"9. Approval and sign-off",
        b"10. Change log",
    ]
    positions = [rendered.index(section) for section in ordered_sections]
    assert positions == sorted(positions)

    without_shap = copy.deepcopy(report)
    without_shap["feature_contributions"] = {
        "status": "not_calculated",
        "global_normalized_contributions": [],
        "waterfall": {
            "status": "not_available",
            "reason": "No persisted sample-level SHAP values.",
        },
    }

    partial = _audit_pdf(without_shap)

    assert partial.startswith(b"%PDF-")
    assert len(re.findall(rb"/Type\s*/Page\b", partial)) == 10
    assert b"Global SHAP feature contributions were not available" in partial
    assert b"Waterfall omitted" in partial
    assert b"The remaining audit evidence is still valid" in partial
