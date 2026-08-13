from __future__ import annotations

import io
import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from automl_api.models.enums import RunKind, RunStatus, TaskType
from automl_api.services import model_audit as audit
from fastapi import HTTPException
from reportlab.pdfgen import canvas


def _draw(flowable: object) -> bytes:
    output = io.BytesIO()
    pdf = canvas.Canvas(output, pagesize=(700, 700))
    flowable.drawOn(pdf, 10, 10)  # type: ignore[attr-defined]
    pdf.save()
    result = output.getvalue()
    assert result.startswith(b"%PDF")
    return result


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        ("actual_predicted", [{"actual": 2, "predicted": 2}, {"actual": "bad"}, None]),
        ("actual_predicted", []),
        ("histogram", [1, 1, "bad", float("nan")]),
        ("histogram", []),
        (
            "chronological",
            [
                {"order": 1, "actual": 2, "predicted": 2.2},
                {"order": 2, "actual": None, "predicted": 3},
                "ignored",
            ],
        ),
        ("confusion_matrix", {"matrix": [[10, 0], [2, "bad"]], "labels": ["long-label", "b"]}),
        ("confusion_matrix", {}),
        (
            "roc",
            [
                {
                    "points": [
                        {"false_positive_rate": 0, "true_positive_rate": 0},
                        {"false_positive_rate": 1, "true_positive_rate": 1},
                        {"false_positive_rate": "bad", "true_positive_rate": 1},
                    ]
                }
            ],
        ),
        (
            "precision_recall",
            [{"points": [{"recall": 0, "precision": 1}, {"recall": 1, "precision": 0.4}]}],
        ),
        (
            "per_class",
            {
                "zero": {"precision": 0.8, "recall": 0.7, "f1-score": 0.75},
                "accuracy": 0.8,
            },
        ),
        ("per_class", {}),
        (
            "learning_curve",
            {
                "scoring": "root_mean_squared_error",
                "points": [
                    {"training_rows": 10, "training_mean": 2, "validation_mean": 3},
                    {"training_rows": 20, "training_mean": 1, "validation_mean": "bad"},
                ],
            },
        ),
        ("cross_validation", {"mean": -2, "standard_deviation": 0.3}),
        ("cross_validation", {"mean": 0, "standard_deviation": 0}),
        ("cluster_sizes", {"0": 12, "long-cluster": 4, "bad": "nope"}),
        (
            "fold_metrics",
            {"fold_metrics": [{"silhouette": 0.3, "db": 2}, {"silhouette": 0.4, "db": None}]},
        ),
        ("fold_metrics", {"fold_metrics": []}),
        ("unknown", {}),
    ],
)
def test_every_persisted_evidence_chart_renders(kind: str, payload: object) -> None:
    assert (
        len(
            _draw(
                audit.EvidenceChartFlowable("A deliberately long audit chart title", kind, payload)
            )
        )
        > 500
    )


@pytest.mark.parametrize(
    ("target", "task"),
    [
        ({}, "classification"),
        (
            {"distribution": [{"label": "a", "count": 0}, {"label": "b", "count": -2}]},
            "classification",
        ),
        (
            {
                "distribution": [{"label": "0-10", "count": 5}],
                "statistics": {"min": 0.0, "median": None, "mean": 4.2, "max": 10.0},
            },
            "regression",
        ),
    ],
)
def test_target_distribution_variants_render(target: dict, task: str) -> None:
    _draw(audit.TargetDistributionFlowable(target, task=task, target_name="target"))


def test_audit_bars_and_pipeline_variants_render() -> None:
    _draw(audit.AuditBarsFlowable([], value_suffix="%"))
    _draw(audit.AuditBarsFlowable([("positive", 2), ("negative", -1)], signed=True))
    _draw(
        audit.PipelinePdfFlowable(
            {
                "input_gates": ["schema", "leakage"],
                "transformer": {
                    "name": "features",
                    "type": "ColumnTransformer",
                    "branches": [
                        {"label": "Numeric", "steps": ["impute", "scale"]},
                        {"label": "Categorical", "steps": []},
                    ],
                },
                "selector": {"name": "selector", "type": "SelectKBest"},
                "estimator": {"name": "model", "type": "Ridge"},
            }
        )
    )
    _draw(audit.PipelinePdfFlowable({}))
    _draw(audit.SceptreMarkFlowable())


def test_numeric_and_text_rendering_helpers_cover_boundaries() -> None:
    assert audit._finite_number("2.5") == 2.5
    assert audit._finite_number("bad", 7) == 7
    assert audit._finite_number(float("inf")) is None
    assert audit._scale(4, 4, 4) == 0.5
    assert audit._scale(2, 0, 4) == 0.5
    assert audit._axis_value(1_500_000) == "1.5m"
    assert audit._axis_value(-2_000) == "-2.0k"
    assert audit._axis_value(2.25) == "2.25"
    assert audit._histogram_counts([], 3) == []
    assert audit._histogram_counts([2, 2], 3) == [2.0]
    assert sum(audit._histogram_counts([0, 1, 2, 3], 3)) == 4
    assert audit._fit_text("short", 10) == "short"
    assert audit._fit_text("too long", 4) == "too…"


def test_waterfall_shape_and_scalar_contracts() -> None:
    unavailable = [
        {},
        {"shap_values": ["bad"]},
        {"shap_values": [[1]], "feature_names": []},
    ]
    assert all(audit._waterfall(item)["status"] == "not_available" for item in unavailable)

    available = audit._waterfall(
        {
            "shap_values": [[[1, 2], [], -1]],
            "feature_names": ["first", "second"],
            "sample_feature_values": ["not-a-mapping"],
            "base_values": [[0.1, 0.2]],
            "prediction_values": [[float("inf"), 0.8]],
        }
    )
    assert available["status"] == "available"
    assert available["output_index"] == -1
    assert available["base_value"] == 0.2
    assert available["prediction_value"] == 0.8
    assert available["features"][-1]["feature"] == "second"
    assert audit._sample_output_scalar(None, False) is None
    assert audit._sample_output_scalar("not-a-number", False) is None
    assert audit._sample_output_scalar(float("nan"), False) is None


def test_contribution_and_missing_evidence_contracts() -> None:
    class Run:
        tags = {
            "feature_importance": [
                {"feature": "a", "mean_absolute_shap": 2},
                {"feature": "b", "mean_absolute_shap": 0, "contribution_percent": 17},
            ],
            "diagnostics": {"runtime": 1},
        }

    evidence = audit._contribution_evidence(Run(), {})  # type: ignore[arg-type]
    assert evidence["status"] == "calculated"
    assert evidence["global_normalized_contributions"][0]["contribution_percent"] == 100
    assert audit._contribution_evidence(None, {})["status"] == "not_calculated"
    assert audit._missing_evidence(None, None, {}, evidence) == [
        "Completed dataset profile",
        "Target distribution visualization",
        "Successful candidate metrics",
        "Sample-level SHAP waterfall",
    ]


def test_flatten_display_metric_and_feature_action_helpers() -> None:
    assert audit._flatten_rows({"outer": {"inner": [1, None]}}) == [
        ("Outer · Inner", "1, Not recorded")
    ]
    assert audit._flatten_rows([], "Rows") == [("Rows", "None recorded")]
    assert audit._flatten_rows([{"a": True}, [3.14159265]]) == [
        ("Row 1 · A", "Yes"),
        ("Row 2", "3.14159"),
    ]
    assert audit._display_value(False) == "No"
    assert audit._metric_label("roc_auc") == "ROC AUC"
    assert audit._metric_label("custom_score") == "Custom score"
    assert audit._metric_direction("rmse") == "Lower is better"
    assert audit._metric_direction("accuracy") == "Higher is better"

    rows = audit._feature_action_rows(
        {
            "target_column": "target",
            "feature_profiles": {"target": {}, "kept": "legacy", "plain": {}},
            "profiling_recommendations": [
                "ignored",
                {"column": "kept", "strategy": None, "action": None},
                {"column": None, "strategy": "global_rule", "reason": "global"},
                {"column": "removed", "action": "drop", "reason": "leakage"},
            ],
        }
    )
    assert [row[0] for row in rows] == ["kept", "plain", "All eligible features", "removed"]
    assert rows[1][2] == "Task pipeline default"
    assert rows[2][1] == "Global"
    assert rows[3][1] == "Excluded"


def _html_report(*, missing: bool) -> dict:
    return {
        "document": {
            "generated_at": "2026-01-01T00:00:00Z",
            "evidence_sha256": "abc",
            "missing_evidence": ["SHAP"] if missing else [],
            "regulatory_note": "Evidence only <not approval>",
        },
        "model_identity": {
            "model_name": "Ridge <v2>",
            "task_type": "regression",
            "target_column": None,
            "candidate_status": "SUCCEEDED 1!",
            "rank": 1,
            "training_run_id": "run-1",
        },
        "dataset_and_target": {
            "target_visualization": {
                "semantic_type": "continuous",
                "distinct_count": 2,
                "missing_count": 0,
                "missing_ratio": 0.125,
                "statistics": {"mean": 1.5},
                "distribution_type": "histogram",
                "distribution": [{"label": "<low>", "count": 0}],
            }
        },
        "feature_processing": {
            "executable_training_contract": {"numeric_features": ["impute", "scale"]},
            "profiling_recommendations": [
                {"column": "a", "action": "impute", "strategy": "median", "reason": "missing"}
            ],
        },
        "training_pipeline": {
            "stages": [{"status": "SUCCEEDED 1!", "label": "fit", "summary": "done"}]
        },
        "model_training": {"parameters": {"alpha": 1}, "fixed": [1, 2]},
        "model_metrics": {
            "values": {"rmse": 1.2},
            "diagnostics": {"runtime": {"seconds": 2}, "ignored": 1},
        },
        "feature_contributions": {
            "global_normalized_contributions": [
                {"feature": "a", "contribution_percent": 120},
                {"feature": "b", "contribution_percent": -10},
            ],
            "waterfall": {
                "status": "available",
                "base_value": 1,
                "prediction_value": 2,
                "features": [
                    {"feature": "a", "shap_value": 2, "absolute_percent": 100},
                    {"feature": "b", "shap_value": -1, "absolute_percent": 50},
                ],
            },
        },
    }


def test_historical_html_renderer_handles_complete_and_partial_evidence() -> None:
    complete = audit._audit_html(_html_report(missing=False))
    partial = audit._audit_html(_html_report(missing=True))
    assert "Partial evidence package" not in complete
    assert "Partial evidence package" in partial
    assert "Ridge &lt;v2&gt;" in complete
    assert 'class="stage succeeded"' in complete
    assert "width:100.00%" in complete
    assert "width:0.00%" in complete
    assert 'class="fill negative"' in complete


def test_html_fragments_cover_absent_and_nested_values() -> None:
    assert "No completed" in audit._target_summary({})
    assert "No target distribution" in audit._distribution_chart({})
    assert "No profile preparation" in audit._preparation_table([])
    assert "No values" in audit._mapping_table({})
    assert "not been calculated" in audit._normalized_chart([])
    assert "unavailable" in audit._waterfall_chart({})
    assert "&lt;tag&gt;" in audit._escape("<tag>")
    assert "Not recorded" in audit._escape(None)
    assert "nested" in audit._mapping_table({"nested": {"b": 2, "a": 1}})


def test_metric_cards_cover_empty_primary_fallback_and_overflow() -> None:
    styles = audit._pdf_styles()
    assert len(audit._pdf_metric_cards({}, None, styles)) == 1
    values = {"accuracy": 0.9, **{f"metric_{index}": index for index in range(10)}}
    cards = audit._pdf_metric_cards(values, "not-present", styles)
    assert len(cards) == 3


def test_diagnostic_spec_variants_and_empty_path() -> None:
    diagnostics = {
        "prediction_samples": [{"order": 1, "actual": 1, "predicted": 2, "residual": -1}],
        "learning_curve": {"points": [{"training_rows": 1}], "scoring": None},
    }
    assert [kind for _, kind, _ in audit._diagnostic_chart_specs("time_series", diagnostics)][
        :3
    ] == [
        "actual_predicted",
        "histogram",
        "chronological",
    ]
    assert audit._diagnostic_chart_specs("regression", {}) == []
    clustering = audit._diagnostic_chart_specs(
        "clustering",
        {"cluster_sizes": {"0": 2}, "cross_validation": {"fold_metrics": [{"silhouette": 0.2}]}},
    )
    assert [kind for _, kind, _ in clustering] == ["cluster_sizes", "fold_metrics"]


class _ScalarRows:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def all(self) -> list[object]:
        return self.values


class _AuditDb:
    def __init__(
        self, *, scalar: object = None, rows: list[object] | None = None, got: object = None
    ) -> None:
        self.scalar_value = scalar
        self.rows = rows or []
        self.got = got

    def scalar(self, _query: object) -> object:
        return self.scalar_value

    def scalars(self, _query: object) -> _ScalarRows:
        return _ScalarRows(self.rows)

    def get(self, _model: object, _identity: object) -> object:
        return self.got


def _run(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "run_kind": RunKind.TRAINING,
        "status": RunStatus.SUCCEEDED,
        "task_type": TaskType.REGRESSION,
        "target_column": "target",
        "params": {"cv_folds": 3, "optimization_iterations": 5},
        "tags": {},
        "pipeline_name": "automl",
        "started_at": datetime(2026, 1, 1, tzinfo=UTC),
        "finished_at": datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        "dataset_version_id": uuid.uuid4(),
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_training_run_lookup_and_parent_resolution() -> None:
    run = _run()
    assert audit._training_run(_AuditDb(scalar=run), run.project_id, run.id) is run  # type: ignore[arg-type]
    with pytest.raises(HTTPException, match="Training run not found"):
        audit._training_run(_AuditDb(), run.project_id, run.id)  # type: ignore[arg-type]

    assert audit._leaderboard_parent(_AuditDb(), run) is run  # type: ignore[arg-type]
    run.tags = {"leaderboard_parent_run_id": "invalid"}
    assert audit._leaderboard_parent(_AuditDb(), run) is run  # type: ignore[arg-type]
    parent = _run(project_id=run.project_id)
    run.tags = {"leaderboard_parent_run_id": str(parent.id)}
    assert audit._leaderboard_parent(_AuditDb(got=parent), run) is parent  # type: ignore[arg-type]
    parent.project_id = uuid.uuid4()
    assert audit._leaderboard_parent(_AuditDb(got=parent), run) is run  # type: ignore[arg-type]


def test_leaderboard_entry_merges_related_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _run(
        params={
            "candidate_models": ["Ridge"],
            "candidate_limit": 1,
            "excluded_leakage_columns": ["leak"],
        },
        tags={"current_candidate": "Ridge"},
    )
    unrelated = _run(project_id=source.project_id, tags={})
    child = _run(
        project_id=source.project_id,
        tags={
            "leaderboard_parent_run_id": str(source.id),
            "leaderboard": [{"model": "Ridge", "status": "succeeded", "best_params": {"alpha": 2}}],
        },
    )
    candidate = SimpleNamespace(name="Ridge", cost_tier="low")
    monkeypatch.setattr(audit, "select_candidates", lambda *_args, **_kwargs: [candidate])
    entry = audit._leaderboard_entry(_AuditDb(rows=[unrelated, child]), source, child, "Ridge")  # type: ignore[arg-type]
    assert entry["status"] == "succeeded"
    assert entry["pipeline"]["parameters"] == {"alpha": 2}
    with pytest.raises(HTTPException, match="not present"):
        audit._leaderboard_entry(_AuditDb(rows=[]), source, source, "Missing")  # type: ignore[arg-type]


def test_explanation_lookup_and_payload_failure_modes(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _run()
    wrong = _run(
        run_kind=RunKind.EXPLAINABILITY,
        params={"model_name": "Other"},
        tags={"source_training_run_id": str(source.id)},
    )
    match = _run(
        run_kind=RunKind.EXPLAINABILITY,
        params={"model_name": "Ridge"},
        tags={"source_training_run_id": str(source.id), "artifact_uri": "s3://explanation"},
    )
    assert audit._latest_explanation(_AuditDb(rows=[wrong, match]), source, "Ridge") is match  # type: ignore[arg-type]
    assert audit._latest_explanation(_AuditDb(rows=[wrong]), source, "Ridge") is None  # type: ignore[arg-type]
    assert audit._explanation_payload(None) == {}
    assert audit._explanation_payload(_run(tags={})) == {}

    store = SimpleNamespace(read_bytes=lambda _uri: json.dumps({"shap_values": [[1]]}).encode())
    monkeypatch.setattr(audit, "get_object_store", lambda: store)
    assert audit._explanation_payload(match) == {"shap_values": [[1]]}
    store.read_bytes = lambda _uri: b"not-json"
    assert audit._explanation_payload(match) == {}
    store.read_bytes = lambda _uri: (_ for _ in ()).throw(OSError("offline"))
    assert audit._explanation_payload(match) == {}


def test_model_audit_report_builds_complete_canonical_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_id = uuid.uuid4()
    version_id = uuid.uuid4()
    version = SimpleNamespace(
        dataset_id=uuid.uuid4(),
        content_hash="sha256:data",
        row_count=20,
        column_count=3,
        schema_json={"target": "float"},
    )
    source = _run(
        project_id=project_id,
        dataset_version_id=version_id,
        dataset_version=version,
        params={"cv_folds": 3, "optimization_iterations": 5, "excluded_leakage_columns": ["leak"]},
        tags={"leaderboard_primary_metric": "rmse"},
    )
    entry = {
        "status": "succeeded",
        "rank": 1,
        "primary_score": 1.2,
        "metrics": {"mae": 0.8},
        "best_params": {"alpha": 1},
        "diagnostics": {"runtime": {"seconds": 2}},
        "pipeline": {},
        "mlflow_run_id": "mlflow",
        "model_artifact_uri": "s3://model",
        "duration_seconds": 2,
        "error": None,
    }
    explanation = _run(
        tags={"artifact_uri": "s3://explanation", "feature_importance": [], "diagnostics": {}}
    )
    profile = SimpleNamespace(
        id=uuid.uuid4(),
        feature_profiles_json={"target": {"distribution": [{"label": "1", "count": 2}]}},
        preparation_json=[],
        overview_json={"leakage_analysis": {"status": "passed"}},
        warnings_json=[],
    )
    user = SimpleNamespace(id=uuid.uuid4())
    db = _AuditDb(scalar=profile, got=SimpleNamespace(name="Project", description="Description"))
    monkeypatch.setattr(audit, "require_project_role", lambda *_args: None)
    monkeypatch.setattr(audit, "_training_run", lambda *_args: source)
    monkeypatch.setattr(audit, "_leaderboard_parent", lambda *_args: source)
    monkeypatch.setattr(audit, "_leaderboard_entry", lambda *_args: entry)
    monkeypatch.setattr(audit, "_latest_explanation", lambda *_args: explanation)
    monkeypatch.setattr(
        audit,
        "_explanation_payload",
        lambda _run: {
            "feature_names": ["a"],
            "shap_values": [[1]],
            "sample_feature_values": [{"a": 2}],
        },
    )
    report, digest = audit.model_audit_report(  # type: ignore[arg-type]
        db, user, project_id, source.id, "Ridge"
    )
    assert report["document"]["evidence_status"] == "complete"
    assert report["model_metrics"]["values"]["rmse"] == 1.2
    assert report["feature_processing"]["features_removed_before_training"] == ["leak"]
    assert report["document"]["evidence_sha256"] == digest
    assert len(digest) == 64


def test_model_audit_document_sanitizes_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    report = _html_report(missing=False)
    report["document"]["evidence_sha256"] = "digest"
    monkeypatch.setattr(audit, "model_audit_report", lambda *_args: (report, "digest"))
    monkeypatch.setattr(audit, "_audit_pdf", lambda _report: b"%PDF-test")
    content, media_type, filename, digest = audit.model_audit_document(
        object(),
        object(),
        uuid.uuid4(),
        uuid.uuid4(),
        " model / name ",
        "pdf",  # type: ignore[arg-type]
    )
    assert (content, media_type, filename, digest) == (
        b"%PDF-test",
        "application/pdf",
        "model-name-audit.pdf",
        "digest",
    )
