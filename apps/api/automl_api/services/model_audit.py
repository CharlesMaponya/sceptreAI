from __future__ import annotations

# The retired HTML renderer remains below as a compatibility helper for historical
# tests; the public audit route emits PDF only.
# ruff: noqa: E501
import hashlib
import html
import io
import json
import math
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import HTTPException, status
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Flowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import ProjectRole, RunKind, RunStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.services.model_evidence import (
    build_model_pipeline,
    feature_processing_contract,
)
from automl_api.services.projects import require_project_role
from automl_api.storage.object_store import get_object_store
from automl_api.training.model_catalog import select_candidates

AuditFormat = Literal["pdf"]


def model_audit_report(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    model_name: str,
) -> tuple[dict[str, Any], str]:
    """Build the canonical point-in-time evidence for any leaderboard candidate."""
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    requested = _training_run(db, project_id, run_id)
    source = _leaderboard_parent(db, requested)
    entry = _leaderboard_entry(db, source, requested, model_name)
    project = db.get(Project, project_id)
    profile = db.scalar(
        select(ProfilingJob)
        .where(
            ProfilingJob.project_id == project_id,
            ProfilingJob.dataset_version_id == source.dataset_version_id,
            ProfilingJob.status == "succeeded",
        )
        .order_by(ProfilingJob.created_at.desc())
    )
    explanation = _latest_explanation(db, source, model_name)
    explanation_payload = _explanation_payload(explanation)
    contributions = _contribution_evidence(explanation, explanation_payload)
    target_profile = (
        profile.feature_profiles_json.get(source.target_column)
        if profile and source.target_column
        else None
    )
    excluded_columns = list(source.params.get("excluded_leakage_columns") or [])
    pipeline = entry.get("pipeline") or build_model_pipeline(
        model_name,
        source.task_type,
        str(entry.get("status", "pending")),
        parameters=dict(entry.get("best_params") or {}),
        excluded_columns=excluded_columns,
    )
    generated_at = datetime.now(UTC)
    missing_evidence = _missing_evidence(profile, target_profile, entry, contributions)
    primary_metric = source.tags.get("leaderboard_primary_metric")
    all_metrics = dict(entry.get("metrics") or {})
    if primary_metric and entry.get("primary_score") is not None:
        all_metrics.setdefault(str(primary_metric), entry["primary_score"])
    report: dict[str, Any] = {
        "schema_version": "2.0",
        "document": {
            "title": "Sceptre model audit document",
            "generated_at": generated_at.isoformat(),
            "evidence_cutoff_at": generated_at.isoformat(),
            "generated_by_id": str(user.id),
            "evidence_status": "partial" if missing_evidence else "complete",
            "missing_evidence": missing_evidence,
            "regulatory_note": (
                "This document records available development evidence; it does not by "
                "itself confer regulatory approval or certification."
            ),
        },
        "model_identity": {
            "project_id": str(project_id),
            "project_name": project.name if project else None,
            "project_description": project.description if project else None,
            "training_run_id": str(source.id),
            "requested_run_id": str(requested.id),
            "model_name": model_name,
            "candidate_status": entry.get("status"),
            "rank": entry.get("rank"),
            "task_type": source.task_type.value,
            "target_column": source.target_column,
            "mlflow_run_id": entry.get("mlflow_run_id"),
            "model_artifact_uri": entry.get("model_artifact_uri"),
        },
        "dataset_and_target": {
            "dataset_id": str(source.dataset_version.dataset_id),
            "dataset_version_id": str(source.dataset_version_id),
            "content_hash": source.dataset_version.content_hash,
            "rows": source.dataset_version.row_count,
            "columns": source.dataset_version.column_count,
            "schema": source.dataset_version.schema_json,
            "target_visualization": target_profile,
            "profile_warnings": profile.warnings_json if profile else [],
        },
        "feature_processing": {
            "profiling_recommendations": profile.preparation_json if profile else [],
            "feature_profiles": profile.feature_profiles_json if profile else {},
            "target_column": source.target_column,
            "executable_training_contract": feature_processing_contract(model_name),
            "leakage_analysis": (
                profile.overview_json.get("leakage_analysis")
                if profile
                else {"status": "not_recorded"}
            ),
            "features_removed_before_training": excluded_columns,
        },
        "training_pipeline": pipeline,
        "model_training": {
            "status": entry.get("status"),
            "pipeline_name": source.pipeline_name,
            "started_at": source.started_at.isoformat() if source.started_at else None,
            "finished_at": source.finished_at.isoformat() if source.finished_at else None,
            "candidate_duration_seconds": entry.get("duration_seconds"),
            "primary_metric": primary_metric,
            "cross_validation_folds": source.params.get("cv_folds"),
            "optimization_iterations": source.params.get("optimization_iterations"),
            "best_parameters": entry.get("best_params") or {},
            "runtime": (entry.get("diagnostics") or {}).get("runtime", {}),
            "failure": entry.get("error"),
        },
        "model_metrics": {
            "values": all_metrics,
            "primary_score": entry.get("primary_score"),
            "diagnostics": entry.get("diagnostics") or {},
        },
        "feature_contributions": contributions,
        "evidence_provenance": {
            "profile_job_id": str(profile.id) if profile else None,
            "explainability_run_id": str(explanation.id) if explanation else None,
            "explainability_artifact_uri": (
                explanation.tags.get("artifact_uri") if explanation else None
            ),
            "source_dataset_hash": source.dataset_version.content_hash,
        },
    }
    evidence_bytes = json.dumps(
        report,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    evidence_hash = hashlib.sha256(evidence_bytes).hexdigest()
    report["document"]["evidence_sha256"] = evidence_hash
    return report, evidence_hash


def model_audit_document(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    model_name: str,
    output_format: AuditFormat,
) -> tuple[bytes, str, str, str]:
    """Render the canonical model evidence as the requested audit document."""
    report, evidence_hash = model_audit_report(
        db,
        user,
        project_id,
        run_id,
        model_name,
    )
    safe_model = re.sub(r"[^A-Za-z0-9_.-]+", "-", model_name).strip("-") or "model"
    content = _audit_pdf(report)
    return content, "application/pdf", f"{safe_model}-audit.pdf", evidence_hash


def _training_run(db: Session, project_id: uuid.UUID, run_id: uuid.UUID) -> ModelRun:
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == run_id,
            ModelRun.run_kind == RunKind.TRAINING,
        )
    )
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Training run not found.")
    return run


def _leaderboard_parent(db: Session, run: ModelRun) -> ModelRun:
    parent_id = run.tags.get("leaderboard_parent_run_id")
    if not parent_id:
        return run
    try:
        parent = db.get(ModelRun, uuid.UUID(str(parent_id)))
    except ValueError:
        parent = None
    return parent if parent and parent.project_id == run.project_id else run


def _leaderboard_entry(
    db: Session,
    source: ModelRun,
    requested: ModelRun,
    model_name: str,
) -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}
    related = list(
        db.scalars(
            select(ModelRun).where(
                ModelRun.project_id == source.project_id,
                ModelRun.run_kind == RunKind.TRAINING,
            )
        ).all()
    )
    for run in [source, requested, *related]:
        if run.id != source.id and run.tags.get("leaderboard_parent_run_id") != str(source.id):
            continue
        requested_models = run.params.get("candidate_models")
        candidates = select_candidates(
            run.task_type,
            (
                requested_models
                if isinstance(requested_models, list) and requested_models
                else None
            ),
            int(run.params.get("candidate_limit", 5)),
        )
        for candidate in candidates:
            entries.setdefault(
                candidate.name,
                {
                    "model": candidate.name,
                    "status": (
                        "running"
                        if run.tags.get("current_candidate") == candidate.name
                        else "pending"
                    ),
                    "rank": None,
                    "cost_tier": candidate.cost_tier,
                    "metrics": {},
                    "diagnostics": {},
                    "best_params": {},
                    "duration_seconds": None,
                    "error": None,
                },
            )
        for item in run.tags.get("leaderboard", []):
            if item.get("model"):
                entries[str(item["model"])] = dict(item)
    entry = entries.get(model_name)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The selected model is not present in this leaderboard.",
        )
    entry["pipeline"] = build_model_pipeline(
        model_name,
        source.task_type,
        str(entry.get("status", "pending")),
        parameters=dict(entry.get("best_params") or {}),
        excluded_columns=list(source.params.get("excluded_leakage_columns") or []),
    )
    return entry


def _latest_explanation(db: Session, source: ModelRun, model_name: str) -> ModelRun | None:
    runs = list(
        db.scalars(
            select(ModelRun)
            .where(
                ModelRun.project_id == source.project_id,
                ModelRun.run_kind == RunKind.EXPLAINABILITY,
                ModelRun.status == RunStatus.SUCCEEDED,
            )
            .order_by(ModelRun.created_at.desc())
        ).all()
    )
    return next(
        (
            run
            for run in runs
            if run.tags.get("source_training_run_id") == str(source.id)
            and run.params.get("model_name") == model_name
        ),
        None,
    )


def _explanation_payload(run: ModelRun | None) -> dict[str, Any]:
    if run is None or not run.tags.get("artifact_uri"):
        return {}
    try:
        return json.loads(get_object_store().read_bytes(str(run.tags["artifact_uri"])))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _contribution_evidence(
    run: ModelRun | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    importance = list((run.tags.get("feature_importance") if run else None) or [])
    total = sum(abs(float(item.get("mean_absolute_shap", 0) or 0)) for item in importance)
    normalized = [
        {
            **item,
            "contribution_percent": (
                float(item.get("contribution_percent"))
                if item.get("contribution_percent") is not None
                else abs(float(item.get("mean_absolute_shap", 0) or 0)) / total * 100
                if total
                else 0.0
            ),
        }
        for item in importance
    ]
    normalized.sort(key=lambda item: float(item.get("contribution_percent", 0)), reverse=True)
    waterfall = _waterfall(payload)
    return {
        "status": "calculated" if run else "not_calculated",
        "method": "SHAP permutation explainer" if run else None,
        "diagnostics": run.tags.get("diagnostics", {}) if run else {},
        "global_normalized_contributions": normalized,
        "raw_shap_values": payload.get("shap_values", []),
        "normalized_sample_contributions": payload.get("shap_contribution_percent", []),
        "waterfall": waterfall,
        "interpretation": (
            "Global values are normalized absolute SHAP magnitude. Waterfall values retain "
            "direction for one representative sample. Association is not causation."
        ),
    }


def _waterfall(payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("shap_values") or []
    if not values:
        return {"status": "not_available", "reason": "No persisted sample-level SHAP values."}
    row = values[0]
    if not isinstance(row, list):
        return {"status": "not_available", "reason": "Unexpected SHAP value shape."}
    multi_output = any(isinstance(value, list) for value in row)
    chosen: list[float] = []
    for value in row:
        if isinstance(value, list):
            chosen.append(float(value[-1]) if value else 0.0)
        else:
            chosen.append(float(value))
    features = list(payload.get("feature_names") or [])
    if not features:
        return {
            "status": "not_available",
            "reason": (
                "This historical SHAP artifact did not persist feature order. "
                "Recalculate SHAP for a trustworthy waterfall."
            ),
        }
    sample = (payload.get("sample_feature_values") or [{}])[0]
    total = sum(abs(value) for value in chosen)
    items = [
        {
            "feature": str(features[index]) if index < len(features) else f"feature_{index}",
            "feature_value": (
                sample.get(str(features[index]))
                if isinstance(sample, dict) and index < len(features)
                else None
            ),
            "shap_value": value,
            "absolute_percent": abs(value) / total * 100 if total else 0.0,
            "direction": "increases_output" if value >= 0 else "decreases_output",
        }
        for index, value in enumerate(chosen)
    ]
    if total and items:
        items[0]["absolute_percent"] += 100.0 - sum(
            float(item["absolute_percent"]) for item in items
        )
    items.sort(key=lambda item: abs(float(item["shap_value"])), reverse=True)
    return {
        "status": "available",
        "sample_index": 0,
        "output_index": -1 if multi_output else 0,
        "base_value": _sample_output_scalar(payload.get("base_values"), multi_output),
        "prediction_value": _sample_output_scalar(
            payload.get("prediction_values"),
            multi_output,
        ),
        "features": items,
    }


def _sample_output_scalar(value: Any, prefer_last_output: bool) -> float | None:
    if isinstance(value, list) and value:
        value = value[0]
    if isinstance(value, list) and value:
        value = value[-1] if prefer_last_output else value[0]
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _missing_evidence(
    profile: ProfilingJob | None,
    target_profile: dict[str, Any] | None,
    entry: dict[str, Any],
    contributions: dict[str, Any],
) -> list[str]:
    missing = []
    if profile is None:
        missing.append("Completed dataset profile")
    if target_profile is None:
        missing.append("Target distribution visualization")
    if not entry.get("metrics"):
        missing.append("Successful candidate metrics")
    if contributions["status"] != "calculated":
        missing.append("SHAP feature contributions")
    elif contributions["waterfall"].get("status") != "available":
        missing.append("Sample-level SHAP waterfall")
    return missing


NAVY = colors.HexColor("#172136")
BLUE = colors.HexColor("#2854C5")
BLUE_SOFT = colors.HexColor("#EAF0FB")
INK = colors.HexColor("#142033")
MUTED = colors.HexColor("#657187")
LINE = colors.HexColor("#DDE2EA")
WASH = colors.HexColor("#F4F5F2")
ORANGE = colors.HexColor("#B86B2B")
GREEN = colors.HexColor("#2F7D61")
SIDEBAR = colors.HexColor("#F4F6FA")
PLOT = colors.HexColor("#F8F9FC")
CONTENT_WIDTH = 149 * mm


def _audit_pdf(report: dict[str, Any]) -> bytes:
    """Render a branded, immutable governance package as a real PDF document."""
    output = io.BytesIO()
    document = report["document"]
    identity = report["model_identity"]
    dataset = report["dataset_and_target"]
    processing = report["feature_processing"]
    training = report["model_training"]
    metrics = report["model_metrics"]
    contributions = report["feature_contributions"]
    styles = _pdf_styles()
    pdf = SimpleDocTemplate(
        output,
        pagesize=A4,
        rightMargin=12 * mm,
        leftMargin=49 * mm,
        topMargin=19 * mm,
        bottomMargin=17 * mm,
        title=f"{identity['model_name']} · Sceptre model governance document",
        author="Sceptre AI",
        subject="Model development and governance evidence",
        pageCompression=0,
    )
    story: list[Any] = []

    story.append(Paragraph('<a name="overview"/>', styles["body"]))
    story.extend(_pdf_brand_header(identity, styles))
    story.append(_section_heading("1. Overview", "overview-section", styles))
    project_description = (
        identity.get("project_description") or "No project description was supplied."
    )
    story.append(Paragraph(_escape(project_description), styles["project_description"]))
    story.append(Spacer(1, 4 * mm))
    story.append(_pdf_overview_grid(report, styles))
    story.append(Spacer(1, 5 * mm))
    story.append(Paragraph("Document contents", styles["subheading"]))
    story.append(_pdf_contents_table(styles))
    if document.get("missing_evidence"):
        story.append(Spacer(1, 3 * mm))
        story.append(
            _pdf_notice(
                "Evidence still unavailable: " + ", ".join(document["missing_evidence"]),
                styles,
            )
        )

    story.append(PageBreak())
    story.append(
        _section_heading(
            "2. Task type and target visualization",
            "task-and-target",
            styles,
        )
    )
    story.extend(_pdf_task_target_intro(identity, dataset, styles))
    story.extend(_pdf_target_section(identity, dataset, styles))

    story.append(PageBreak())
    story.append(
        _section_heading(
            "3. Feature preparation and recorded actions",
            "feature-preparation",
            styles,
        )
    )
    story.extend(_pdf_feature_processing_section(processing, styles))

    story.append(PageBreak())
    story.append(
        _section_heading(
            "4. Training pipeline, record and tuning",
            "training-pipeline",
            styles,
        )
    )
    story.append(
        Paragraph(
            "The fitted preprocessing branches converge into the exact candidate estimator. "
            "The execution record and tuning contract below preserve how that fitted model was produced.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 2 * mm))
    pipeline_diagram = PipelinePdfFlowable(
        report["training_pipeline"].get("diagram") or {},
        width=112 * mm,
    )
    pipeline_diagram.hAlign = "CENTER"
    story.append(pipeline_diagram)
    story.append(Spacer(1, 3 * mm))
    story.extend(
        _pdf_training_record_section(
            report["training_pipeline"],
            training,
            styles,
        )
    )

    story.append(PageBreak())
    story.append(_section_heading("5. Model performance", "model-performance", styles))
    story.append(
        Paragraph(
            "Every score and visual below comes from the selected candidate's persisted "
            "holdout and cross-validation evidence.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 3 * mm))
    story.extend(
        _pdf_metric_cards(
            metrics.get("values") or {},
            training.get("primary_metric"),
            styles,
        )
    )
    story.append(Spacer(1, 5 * mm))
    story.extend(
        _pdf_diagnostic_visuals(
            str(identity.get("task_type") or ""),
            metrics.get("diagnostics") or {},
            styles,
        )
    )
    story.append(Spacer(1, 1 * mm))
    story.append(_pdf_provenance_strip(report, styles))

    story.append(PageBreak())
    story.append(
        _section_heading(
            "6. Explainability and feature contributions",
            "explainability",
            styles,
        )
    )
    story.append(Paragraph("Normalized feature contributions", styles["subheading"]))
    story.append(
        Paragraph(
            "Mean absolute SHAP magnitude normalized to 100% across the features recorded for this model.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 4 * mm))
    normalized = list(contributions.get("global_normalized_contributions") or [])
    if normalized:
        story.append(
            AuditBarsFlowable(
                [
                    (
                        str(item.get("feature")),
                        float(item.get("contribution_percent", 0) or 0),
                    )
                    for item in normalized[:30]
                ],
                value_suffix="%",
            )
        )
    else:
        story.append(
            _pdf_notice(
                "Global SHAP feature contributions were not available for this evidence snapshot.",
                styles,
                warning=False,
            )
        )
    story.append(Spacer(1, 7 * mm))
    story.append(Paragraph("Representative SHAP waterfall", styles["subheading"]))
    waterfall = contributions.get("waterfall") or {}
    if waterfall.get("status") == "available":
        story.append(
            Paragraph(
                "Directional contributions for the first persisted explanation sample. "
                f"Base value: <b>{_escape(waterfall.get('base_value'))}</b> · "
                f"prediction: <b>{_escape(waterfall.get('prediction_value'))}</b>.",
                styles["body"],
            )
        )
        story.append(Spacer(1, 4 * mm))
        story.append(
            AuditBarsFlowable(
                [
                    (str(item.get("feature")), float(item.get("shap_value", 0) or 0))
                    for item in waterfall.get("features", [])[:30]
                ],
                signed=True,
            )
        )
    else:
        reason = waterfall.get("reason") or (
            "No trustworthy sample-level SHAP waterfall was persisted for this model."
        )
        story.append(
            _pdf_notice(
                f"Waterfall omitted: {reason} The remaining audit evidence is still valid.",
                styles,
                warning=False,
            )
        )
    story.append(PageBreak())
    story.append(
        _section_heading(
            "7. Monitoring and thresholds",
            "monitoring-thresholds",
            styles,
        )
    )
    story.append(
        _pdf_notice(
            "This point-in-time training document does not invent live monitoring thresholds. "
            "Deployment drift, service quality, and alert thresholds must be recorded by the "
            "governance monitoring workflow after promotion.",
            styles,
            warning=False,
        )
    )
    story.append(Spacer(1, 5 * mm))
    story.append(Paragraph("Monitoring evidence and data lineage", styles["subheading"]))
    story.append(
        _pdf_table(
            _flatten_rows(report["evidence_provenance"]),
            styles,
            headers=["Evidence source", "Identifier"],
            column_widths=[47 * mm, 102 * mm],
        )
    )

    story.append(PageBreak())
    story.append(_section_heading("8. Risk assessment", "risk-assessment", styles))
    story.append(
        Paragraph(
            "This section records the leakage, profile-quality, and evidence-completeness "
            "signals available at the document cutoff. It does not infer risks that were not measured.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 4 * mm))
    story.append(
        _pdf_risk_summary(
            document,
            processing,
            dataset,
            styles,
        )
    )

    story.append(PageBreak())
    story.append(_section_heading("9. Approval and sign-off", "approvals", styles))
    story.append(
        Paragraph(
            "Training success does not constitute approval. The accountable team leader must "
            "complete the fields below before production promotion; Sceptre leaves every "
            "approval field intentionally blank.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 6 * mm))
    story.append(_pdf_signoff_form(styles))

    story.append(PageBreak())
    story.append(_section_heading("10. Change log", "change-log", styles))
    story.append(
        Paragraph(
            "Add a new entry whenever data, preprocessing, training configuration, validation, "
            "approval status, or deployment behavior materially changes.",
            styles["body"],
        )
    )
    story.append(Spacer(1, 4 * mm))
    story.append(_pdf_changelog(document, styles))
    story.append(Spacer(1, 6 * mm))
    story.append(_pdf_notice(document["regulatory_note"], styles, warning=False))

    decoration = _page_decoration(identity, document)
    pdf.build(story, onFirstPage=decoration, onLaterPages=decoration)
    return output.getvalue()


def _pdf_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "AuditTitle", parent=base["Title"], fontName="Helvetica-Bold", fontSize=24,
            leading=27, textColor=INK, alignment=TA_LEFT, spaceAfter=3 * mm,
        ),
        "brand": ParagraphStyle(
            "AuditBrand", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=15,
            leading=18, textColor=NAVY,
        ),
        "kicker": ParagraphStyle(
            "AuditKicker", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=7.5,
            leading=10, textColor=BLUE, spaceAfter=1.5 * mm,
        ),
        "project_description": ParagraphStyle(
            "ProjectDescription", parent=base["BodyText"], fontSize=10, leading=15,
            textColor=MUTED,
        ),
        "section_title": ParagraphStyle(
            "SectionTitle", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=15,
            leading=18, textColor=INK, spaceBefore=2 * mm, spaceAfter=2.5 * mm,
        ),
        "subheading": ParagraphStyle(
            "Subheading", parent=base["Heading3"], fontName="Helvetica-Bold", fontSize=10.5,
            leading=13, textColor=INK, spaceBefore=2 * mm, spaceAfter=2 * mm,
        ),
        "body": ParagraphStyle(
            "AuditBody", parent=base["BodyText"], fontSize=8.5, leading=12.5,
            textColor=MUTED,
        ),
        "cell": ParagraphStyle(
            "TableCell", parent=base["BodyText"], fontSize=7.4, leading=10,
            textColor=INK,
        ),
        "cell_key": ParagraphStyle(
            "TableKey", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=7.2,
            leading=9.5, textColor=MUTED,
        ),
        "notice": ParagraphStyle(
            "Notice", parent=base["BodyText"], fontSize=8, leading=11.5, textColor=INK,
        ),
        "toc": ParagraphStyle(
            "ContentsLink", parent=base["BodyText"], fontName="Helvetica-Bold",
            fontSize=9, leading=13, textColor=BLUE,
        ),
        "card_title": ParagraphStyle(
            "CardTitle", parent=base["BodyText"], fontName="Helvetica-Bold",
            fontSize=8.5, leading=11, textColor=INK,
        ),
        "card_value": ParagraphStyle(
            "CardValue", parent=base["BodyText"], fontName="Helvetica-Bold",
            fontSize=15, leading=17, textColor=NAVY,
        ),
    }


def _section_heading(
    title: str,
    anchor: str,
    styles: dict[str, ParagraphStyle],
) -> Paragraph:
    return Paragraph(f'<a name="{anchor}"/>{_escape(title)}', styles["section_title"])


def _pdf_overview_grid(
    report: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> Table:
    identity = report.get("model_identity") or {}
    dataset = report.get("dataset_and_target") or {}
    document = report.get("document") or {}
    cards = [
        (
            "Project",
            identity.get("project_name") or "Unnamed project",
            f"Project ID · {_fit_text(str(identity.get('project_id') or 'Not recorded'), 24)}",
        ),
        (
            "Model",
            identity.get("model_name") or "Not recorded",
            f"Candidate status · {_humanize(identity.get('candidate_status'))}",
        ),
        (
            "Dataset snapshot",
            f"{_display_value(dataset.get('rows'))} rows · {_display_value(dataset.get('columns'))} columns",
            f"Version · {_fit_text(str(dataset.get('dataset_version_id') or 'Not recorded'), 24)}",
        ),
        (
            "Evidence package",
            _humanize(document.get("evidence_status") or "complete"),
            f"Generated · {str(document.get('generated_at') or 'Not recorded')[:19]}",
        ),
    ]
    cells = [
        [
            Paragraph(_escape(label), styles["cell_key"]),
            Spacer(1, 1.5 * mm),
            Paragraph(_escape(value), styles["card_title"]),
            Spacer(1, 1 * mm),
            Paragraph(_escape(detail), styles["cell"]),
        ]
        for label, value, detail in cards
    ]
    table = Table(
        [[cells[0], cells[1]], [cells[2], cells[3]]],
        colWidths=[72.5 * mm, 72.5 * mm],
        rowHeights=[27 * mm, 27 * mm],
        hAlign="LEFT",
    )
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                ("GRID", (0, 0), (-1, -1), 0.45, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return table


def _pdf_contents_table(styles: dict[str, ParagraphStyle]) -> Table:
    sections = [
        ("overview", "Overview"),
        ("task-and-target", "Task type and target visualization"),
        ("feature-preparation", "Feature preparation and recorded actions"),
        ("training-pipeline", "Training pipeline, record and tuning"),
        ("model-performance", "Model performance"),
        ("explainability", "Explainability and feature contributions"),
        ("monitoring-thresholds", "Monitoring and thresholds"),
        ("risk-assessment", "Risk assessment"),
        ("approvals", "Approval and sign-off"),
        ("change-log", "Change log"),
    ]
    rows = [
        [
            Paragraph(f"{index}.", styles["cell_key"]),
            Paragraph(
                f'<link href="#{anchor}" color="#2854C5">{_escape(title)}</link>',
                styles["cell"],
            ),
            Paragraph(str(index), styles["cell_key"]),
        ]
        for index, (anchor, title) in enumerate(sections, 1)
    ]
    table = Table(rows, colWidths=[9 * mm, 122 * mm, 14 * mm], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LINEBELOW", (0, 0), (-1, -1), 0.35, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4.5),
            ]
        )
    )
    return table


def _pdf_task_target_intro(
    identity: dict[str, Any],
    dataset: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    target_name = str(identity.get("target_column") or "No target selected")
    task_key = str(identity.get("task_type") or "").lower()
    task = _humanize(task_key or "unsupervised")
    target_profile = dataset.get("target_visualization") or {}
    semantic_type = str(target_profile.get("semantic_type") or "not recorded").replace("_", " ")
    task_explanations = {
        "classification": "The model predicts one of the recorded target categories. Candidate ranking uses classification metrics and class-aware diagnostics.",
        "regression": "The model predicts a continuous numerical outcome. Candidate ranking uses regression errors and goodness-of-fit diagnostics.",
        "time_series": "The model predicts an ordered target through time. Validation preserves chronology so future observations cannot inform earlier predictions.",
        "clustering": "The model groups similar rows without a selected target. Cluster quality and stability replace supervised prediction metrics.",
    }
    task_explanation = task_explanations.get(
        task_key,
        "The recorded task type determines candidate compatibility, validation design, and model diagnostics.",
    )
    if identity.get("target_column"):
        named_explanation = (
            f"{target_name} is the selected outcome column for this project. Its recorded "
            f"semantic type is {semantic_type}; this run treats it as a {task.lower()} target and "
            "compares predictions against its observed values."
        )
    else:
        named_explanation = (
            "This is an unsupervised run, so no outcome column was selected and no target "
            "distribution is required."
        )
    cards = Table(
        [
            [
                [
                    Paragraph("Task type", styles["cell_key"]),
                    Spacer(1, 1.5 * mm),
                    Paragraph(_escape(task), styles["card_value"]),
                    Spacer(1, 1.5 * mm),
                    Paragraph(_escape(task_explanation), styles["body"]),
                ],
                [
                    Paragraph("Target", styles["cell_key"]),
                    Spacer(1, 1.5 * mm),
                    Paragraph(_escape(target_name), styles["card_title"]),
                    Spacer(1, 1.5 * mm),
                    Paragraph(_escape(named_explanation), styles["body"]),
                ],
            ]
        ],
        colWidths=[72.5 * mm, 72.5 * mm],
        hAlign="LEFT",
    )
    cards.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 0), (-1, -1), BLUE_SOFT),
                ("BOX", (0, 0), (-1, -1), 0.4, LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, colors.white),
                ("LEFTPADDING", (0, 0), (-1, -1), 9),
                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    return [cards, Spacer(1, 5 * mm)]


def _explanation_card(
    title: str,
    copy: str,
    styles: dict[str, ParagraphStyle],
) -> Table:
    card = Table(
        [[Paragraph(_escape(title), styles["card_title"])], [Paragraph(_escape(copy), styles["body"])]],
        colWidths=[71 * mm],
    )
    card.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                ("BOX", (0, 0), (-1, -1), 0.45, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, 0), 7),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 3),
                ("TOPPADDING", (0, 1), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
            ]
        )
    )
    return card


def _page_decoration(identity: dict[str, Any], document: dict[str, Any]):
    generated = str(document.get("generated_at") or "Not recorded")[:10]

    def draw(canvas: Any, doc: Any) -> None:
        canvas.saveState()
        page_width, page_height = A4
        canvas.setFillColor(SIDEBAR)
        canvas.rect(0, 0, 41 * mm, page_height, stroke=0, fill=1)
        logo = _logo_path()
        if logo:
            canvas.drawImage(
                str(logo),
                7 * mm,
                page_height - 23 * mm,
                width=11 * mm,
                height=11 * mm,
                mask="auto",
                preserveAspectRatio=True,
            )
        canvas.setFillColor(NAVY)
        canvas.setFont("Helvetica-Bold", 10)
        canvas.drawString(19 * mm, page_height - 18.7 * mm, "Sceptre")
        canvas.setFillColor(BLUE)
        canvas.drawString(32.8 * mm, page_height - 18.7 * mm, "AI")
        canvas.setStrokeColor(LINE)
        canvas.line(5 * mm, page_height - 29 * mm, 36 * mm, page_height - 29 * mm)

        active_by_page = {
            1: "Overview",
            2: "Task & target",
            3: "Feature prep",
            4: "Training",
            5: "Performance",
            6: "Explainability",
            7: "Monitoring",
            8: "Risk",
            9: "Approval",
            10: "Change log",
        }
        active = active_by_page.get(doc.page)
        nav_items = [
            "Overview",
            "Task & target",
            "Feature prep",
            "Training",
            "Performance",
            "Explainability",
            "Monitoring",
            "Risk",
            "Approval",
            "Change log",
        ]
        y = page_height - 40 * mm
        canvas.setFont("Helvetica", 6.6)
        for item in nav_items:
            if item == active:
                canvas.setFillColor(BLUE)
                canvas.roundRect(4 * mm, y - 2.7 * mm, 33 * mm, 7 * mm, 1.5 * mm, stroke=0, fill=1)
                canvas.setFillColor(colors.white)
                canvas.setFont("Helvetica-Bold", 6.6)
            else:
                canvas.setFillColor(MUTED)
                canvas.setFont("Helvetica", 6.6)
            canvas.circle(8 * mm, y + 0.5 * mm, 0.8 * mm, stroke=1, fill=0)
            canvas.drawString(11 * mm, y - 1 * mm, item)
            y -= 8.5 * mm

        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica-Bold", 5.7)
        canvas.drawString(6 * mm, 58 * mm, "DOCUMENT INFO")
        canvas.setFont("Helvetica", 6.2)
        info = [
            ("Version", str(document.get("schema_version") or "2.0")),
            ("Date", generated),
            ("Prepared by", "Sceptre AI"),
        ]
        info_y = 52 * mm
        for label, value in info:
            canvas.setFillColor(MUTED)
            canvas.drawString(6 * mm, info_y, label)
            canvas.setFillColor(INK)
            canvas.drawString(6 * mm, info_y - 4 * mm, _fit_text(value, 24))
            info_y -= 11 * mm
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica-Bold", 6.2)
        canvas.drawString(6 * mm, 12 * mm, "Confidential")

        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica-Bold", 6.4)
        canvas.drawRightString(
            page_width - 12 * mm,
            page_height - 10 * mm,
            "Sceptre AI · Model governance document",
        )
        canvas.setStrokeColor(LINE)
        canvas.line(49 * mm, 12 * mm, page_width - 12 * mm, 12 * mm)
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica", 6.6)
        canvas.drawString(49 * mm, 8 * mm, "Confidential · generated model evidence")
        canvas.drawRightString(page_width - 12 * mm, 8 * mm, f"Page {doc.page}")
        canvas.restoreState()

    return draw


def _pdf_brand_header(identity: dict[str, Any], styles: dict[str, ParagraphStyle]) -> list[Any]:
    return [
        Paragraph("Model governance document", styles["title"]),
        Paragraph("Transparent. Accountable. Trusted.", styles["project_description"]),
        Spacer(1, 2 * mm),
        Paragraph(
            f"Project · <b>{_escape(identity.get('project_name') or 'Unnamed project')}</b>"
            f" &nbsp;&nbsp; Model · <b>{_escape(identity['model_name'])}</b>"
            f" &nbsp;&nbsp; Task · <b>{_escape(_humanize(identity.get('task_type')))}</b>",
            styles["body"],
        ),
        Spacer(1, 1.5 * mm),
    ]


def _logo_path() -> Path | None:
    candidates = [
        Path(__file__).resolve().parents[4] / "apps/ui/react_app/public/sceptre-icon.png",
        Path.cwd() / "apps/ui/react_app/public/sceptre-icon.png",
    ]
    return next((path for path in candidates if path.exists()), None)


class SceptreMarkFlowable(Flowable):
    def __init__(self) -> None:
        super().__init__()
        self.width = 15 * mm
        self.height = 15 * mm

    def draw(self) -> None:
        self.canv.setFillColor(BLUE)
        self.canv.roundRect(0, 0, self.width, self.height, 4, stroke=0, fill=1)
        self.canv.setFillColor(colors.white)
        self.canv.setFont("Helvetica-Bold", 15)
        self.canv.drawCentredString(self.width / 2, self.height / 2 - 5, "S")


def _pdf_target_section(
    identity: dict[str, Any], dataset: dict[str, Any], styles: dict[str, ParagraphStyle]
) -> list[Any]:
    task = str(identity.get("task_type") or "")
    target = dataset.get("target_visualization") or {}
    if task == "classification":
        title = "Class balance"
        description = "Class counts reveal imbalance before model training."
    elif task == "regression":
        title = "Regression target distribution"
        description = "The histogram shows the range, concentration, and skew of the continuous target."
    elif task == "time_series":
        title = "Time-series target distribution"
        description = "The histogram shows how the temporal target is distributed across its observed range."
    else:
        title = "Unsupervised model"
        description = "Clustering does not use a target column, so no target distribution is required."
    result: list[Any] = [
        Paragraph(title, styles["subheading"]),
        Paragraph(description, styles["body"]),
        Spacer(1, 3 * mm),
    ]
    if task != "clustering":
        result.extend(
            [
                TargetDistributionFlowable(
                    target,
                    task=task,
                    target_name=str(identity.get("target_column") or "Target"),
                ),
                Spacer(1, 5 * mm),
            ]
        )
    return result


def _pdf_feature_processing_section(
    processing: dict[str, Any], styles: dict[str, ParagraphStyle]
) -> list[Any]:
    feature_rows = _feature_action_rows(processing)
    leakage = processing.get("leakage_analysis") or {}
    excluded = leakage.get("excluded_columns") or []
    leakage_message = (
        f"{len(excluded)} feature(s) were excluded before training: "
        + ", ".join(str(item) for item in excluded)
        if excluded
        else "The completed leakage check recorded no automatically excluded target proxies."
    )
    return [
        Paragraph(
            "Each row joins the profiled feature type, selected processing strategy, "
            "reason, and recorded action so reviewers can trace one feature without "
            "cross-referencing separate tables.",
            styles["body"],
        ),
        Spacer(1, 3 * mm),
        _pdf_table(
            feature_rows,
            styles,
            headers=[
                "Feature",
                "Type",
                "Processing strategy",
                "Reason",
                "Recorded action",
            ],
            column_widths=[27 * mm, 21 * mm, 34 * mm, 43 * mm, 24 * mm],
        ),
        Spacer(1, 4 * mm),
        Paragraph("Leakage controls", styles["subheading"]),
        _pdf_notice(leakage_message, styles, warning=bool(excluded)),
    ]


def _feature_action_rows(processing: dict[str, Any]) -> list[list[str]]:
    profiles = processing.get("feature_profiles") or {}
    target_column = processing.get("target_column")
    recommendations = processing.get("profiling_recommendations") or []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for step in recommendations:
        if not isinstance(step, dict):
            continue
        grouped.setdefault(str(step.get("column") or "__all_features__"), []).append(step)

    rows: list[list[str]] = []
    for feature, profile in profiles.items():
        if feature == target_column:
            continue
        details = profile if isinstance(profile, dict) else {}
        steps = grouped.pop(str(feature), [])
        if steps:
            strategies = "\n".join(_humanize(step.get("strategy")) for step in steps)
            reasons = "\n".join(str(step.get("reason") or "Not recorded") for step in steps)
            actions = "\n".join(_humanize(step.get("action")) for step in steps)
        else:
            strategies = "Task pipeline default"
            reasons = "No additional profile-driven transformation was recorded."
            actions = "Retained for preprocessing"
        rows.append(
            [
                str(feature),
                _humanize(details.get("semantic_type") or "unknown"),
                strategies,
                reasons,
                actions,
            ]
        )
    for feature, steps in grouped.items():
        rows.append(
            [
                "All eligible features" if feature == "__all_features__" else feature,
                "Global" if feature == "__all_features__" else "Excluded",
                "\n".join(_humanize(step.get("strategy")) for step in steps),
                "\n".join(str(step.get("reason") or "Not recorded") for step in steps),
                "\n".join(_humanize(step.get("action")) for step in steps),
            ]
        )
    return rows


def _pdf_training_record_section(
    pipeline: dict[str, Any],
    training: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    stage_rows = [
        [stage.get("label"), stage.get("status"), stage.get("summary")]
        for stage in pipeline.get("stages", [])
    ]
    runtime = training.get("runtime") or {}
    tuning_rows = [
        ("Status", training.get("status"), "Primary metric", training.get("primary_metric")),
        (
            "Pipeline",
            training.get("pipeline_name"),
            "CV folds",
            training.get("cross_validation_folds"),
        ),
        (
            "Started",
            training.get("started_at"),
            "Search iterations",
            training.get("optimization_iterations"),
        ),
        (
            "Finished",
            training.get("finished_at"),
            "Duration (seconds)",
            training.get("candidate_duration_seconds"),
        ),
        (
            "Accelerator",
            runtime.get("accelerator") or "CPU",
            "Failure",
            training.get("failure") or "None recorded",
        ),
    ]
    tuning_data = [
        [
            Paragraph(_escape(label_a), styles["cell_key"]),
            Paragraph(_escape(_display_value(value_a)), styles["cell"]),
            Paragraph(_escape(label_b), styles["cell_key"]),
            Paragraph(_escape(_display_value(value_b)), styles["cell"]),
        ]
        for label_a, value_a, label_b, value_b in tuning_rows
    ]
    tuning_table = Table(
        tuning_data,
        colWidths=[25 * mm, 49 * mm, 29 * mm, 46 * mm],
        hAlign="LEFT",
    )
    tuning_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.35, LINE),
                ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return [
        Paragraph("Training record", styles["subheading"]),
        _pdf_table(
            stage_rows,
            styles,
            headers=["Execution stage", "Status", "Recorded evidence"],
            column_widths=[32 * mm, 22 * mm, 95 * mm],
        ),
        Spacer(1, 3 * mm),
        Paragraph("Training and tuning", styles["subheading"]),
        tuning_table,
    ]


def _humanize(value: Any) -> str:
    text = str(value or "Not recorded").replace("_", " ").strip()
    return text[:1].upper() + text[1:]


def _pdf_provenance_strip(
    report: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> Table:
    identity = report.get("model_identity") or {}
    dataset = report.get("dataset_and_target") or {}
    document = report.get("document") or {}
    runtime = (report.get("model_training") or {}).get("runtime") or {}
    items = [
        ("Run ID", identity.get("training_run_id")),
        ("Dataset snapshot", dataset.get("content_hash")),
        ("Evidence hash", document.get("evidence_sha256")),
        ("Environment", runtime.get("accelerator") or "CPU"),
        ("Generated", document.get("generated_at")),
    ]
    cells = []
    for label, value in items:
        cells.append(
            [
                Paragraph(label, styles["cell_key"]),
                Spacer(1, 1 * mm),
                Paragraph(_escape(_fit_text(_display_value(value), 22)), styles["cell"]),
            ]
        )
    table = Table([cells], colWidths=[29 * mm] * 5, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                ("BOX", (0, 0), (-1, -1), 0.4, LINE),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return table


def _pdf_risk_summary(
    document: dict[str, Any],
    processing: dict[str, Any],
    dataset: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> Table:
    leakage = processing.get("leakage_analysis") or {}
    excluded = leakage.get("excluded_columns") or processing.get(
        "features_removed_before_training"
    ) or []
    warnings = dataset.get("profile_warnings") or []
    missing = document.get("missing_evidence") or []
    rows = [
        ("Leakage status", leakage.get("status") or "Not recorded"),
        ("Features excluded", len(excluded)),
        ("Profile warnings", len(warnings)),
        ("Missing evidence", len(missing)),
    ]
    return _pdf_table(
        rows,
        styles,
        headers=["Risk signal", "Recorded result"],
        column_widths=[49 * mm, 100 * mm],
    )


def _pdf_signoff_form(styles: dict[str, ParagraphStyle]) -> Table:
    data = [
        [Paragraph("Team leader name", styles["cell_key"]), ""],
        [Paragraph("Review date", styles["cell_key"]), ""],
        [
            Paragraph("Approval decision", styles["cell_key"]),
            Paragraph(
                "[   ] Approved&nbsp;&nbsp;&nbsp;&nbsp;[   ] Approved with conditions"
                "&nbsp;&nbsp;&nbsp;&nbsp;[   ] Not approved",
                styles["cell"],
            ),
        ],
        [Paragraph("Team leader signature", styles["cell_key"]), ""],
        [Paragraph("Conditions or comments", styles["cell_key"]), ""],
    ]
    form = Table(
        data,
        colWidths=[39 * mm, 110 * mm],
        rowHeights=[18 * mm, 18 * mm, 18 * mm, 42 * mm, 54 * mm],
        hAlign="LEFT",
    )
    form.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("GRID", (0, 0), (-1, -1), 0.7, colors.HexColor("#AEB8C8")),
                ("BACKGROUND", (0, 0), (0, -1), PLOT),
                ("BACKGROUND", (1, 0), (1, -1), colors.white),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return form


def _pdf_changelog(
    document: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> Table:
    return _pdf_table(
        [
            (
                document.get("generated_at"),
                document.get("schema_version") or "2.0",
                "Governance document generated from the immutable training evidence snapshot.",
            )
        ],
        styles,
        headers=["Recorded at", "Version", "Change"],
        column_widths=[45 * mm, 20 * mm, 84 * mm],
    )


def _pdf_notice(
    message: str, styles: dict[str, ParagraphStyle], *, warning: bool = True
) -> Table:
    table = Table(
        [[Paragraph(_escape(message), styles["notice"])]],
        colWidths=[CONTENT_WIDTH],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFF4E7") if warning else BLUE_SOFT),
                ("BOX", (0, 0), (-1, -1), 0.6, ORANGE if warning else BLUE),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return table


def _pdf_table(
    rows: list[Any],
    styles: dict[str, ParagraphStyle],
    *,
    headers: list[str] | None = None,
    key_value: bool = False,
    column_widths: list[float] | None = None,
) -> Table:
    normalized = rows or [("No evidence", "Not recorded")]
    data: list[list[Paragraph]] = []
    if headers:
        data.append([Paragraph(_escape(item), styles["cell_key"]) for item in headers])
    for row in normalized:
        values = list(row) if isinstance(row, (list, tuple)) else [row]
        data.append(
            [
                Paragraph(
                    _escape(_display_value(value)),
                    styles["cell_key"] if key_value and index == 0 else styles["cell"],
                )
                for index, value in enumerate(values)
            ]
        )
    table = Table(data, colWidths=column_widths, repeatRows=1 if headers else 0, splitByRow=1)
    commands = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.35, LINE),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#F8F9FB")]),
    ]
    if headers:
        commands.extend(
            [
                ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ]
        )
        for cell in data[0]:
            cell.style = ParagraphStyle("HeaderCell", parent=styles["cell_key"], textColor=colors.white)
    table.setStyle(TableStyle(commands))
    return table


def _flatten_rows(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            label = f"{prefix} · {str(key).replace('_', ' ').title()}" if prefix else str(key).replace("_", " ").title()
            rows.extend(_flatten_rows(item, label))
    elif isinstance(value, list):
        if not value:
            rows.append((prefix or "Value", "None recorded"))
        elif all(not isinstance(item, (dict, list)) for item in value):
            rows.append((prefix or "Value", ", ".join(_display_value(item) for item in value)))
        else:
            for index, item in enumerate(value, 1):
                rows.extend(_flatten_rows(item, f"{prefix} · Row {index}" if prefix else f"Row {index}"))
    else:
        rows.append((prefix or "Value", _display_value(value)))
    return rows


def _display_value(value: Any) -> str:
    if value is None:
        return "Not recorded"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _pdf_metric_cards(
    values: dict[str, Any],
    primary_metric: str | None,
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    if not values:
        return [_pdf_notice("No successful candidate metrics were recorded.", styles)]
    primary_name = primary_metric if primary_metric in values else next(iter(values))
    primary_value = values[primary_name]
    remaining = [(name, value) for name, value in values.items() if name != primary_name]
    summary_cells = [_metric_card(name, value, styles) for name, value in remaining[:4]]
    while len(summary_cells) < 4:
        summary_cells.append([Paragraph("", styles["cell"])])
    summary = Table([summary_cells], colWidths=[25.75 * mm] * 4)
    summary.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("INNERGRID", (0, 0), (-1, -1), 0.4, LINE),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    primary = [
        Paragraph(f"{_escape(_metric_label(primary_name))} · PRIMARY", styles["cell_key"]),
        Spacer(1, 1.5 * mm),
        Paragraph(
            f'<font size="19" color="#2854C5"><b>{_escape(_display_value(primary_value))}</b></font>',
            styles["body"],
        ),
        Paragraph(_metric_direction(primary_name), styles["cell_key"]),
    ]
    top = Table([[primary, summary]], colWidths=[42 * mm, 103 * mm], hAlign="LEFT")
    top.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                ("BOX", (0, 0), (-1, -1), 0.45, LINE),
                ("LINEBEFORE", (1, 0), (1, 0), 0.45, LINE),
                ("LEFTPADDING", (0, 0), (0, 0), 8),
                ("RIGHTPADDING", (0, 0), (0, 0), 8),
                ("TOPPADDING", (0, 0), (0, 0), 7),
                ("BOTTOMPADDING", (0, 0), (0, 0), 7),
                ("LEFTPADDING", (1, 0), (1, 0), 0),
                ("RIGHTPADDING", (1, 0), (1, 0), 0),
                ("TOPPADDING", (1, 0), (1, 0), 0),
                ("BOTTOMPADDING", (1, 0), (1, 0), 0),
            ]
        )
    )
    result: list[Any] = [top]
    overflow = remaining[4:]
    if overflow:
        rows = []
        for index in range(0, len(overflow), 5):
            cells = [_metric_card(name, value, styles) for name, value in overflow[index : index + 5]]
            while len(cells) < 5:
                cells.append([Paragraph("", styles["cell"])])
            rows.append(cells)
        more = Table(rows, colWidths=[29 * mm] * 5, hAlign="LEFT")
        more.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 0), (-1, -1), PLOT),
                    ("BOX", (0, 0), (-1, -1), 0.4, LINE),
                    ("INNERGRID", (0, 0), (-1, -1), 0.4, LINE),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        result.extend([Spacer(1, 2 * mm), more])
    return result


def _metric_card(
    name: str,
    value: Any,
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    return [
        Paragraph(_escape(_metric_label(name)), styles["cell_key"]),
        Spacer(1, 1 * mm),
        Paragraph(_escape(_display_value(value)), styles["card_title"]),
    ]


def _metric_label(name: str) -> str:
    labels = {
        "r2": "R²",
        "mae": "MAE",
        "mse": "MSE",
        "rmse": "RMSE",
        "rmsle": "RMSLE",
        "mape": "MAPE",
        "smape": "SMAPE",
        "roc_auc": "ROC AUC",
        "roc_auc_ovr_weighted": "ROC AUC OVR weighted",
        "mcc": "MCC",
    }
    return labels.get(str(name), _humanize(name))


def _metric_direction(name: str) -> str:
    lower_is_better = {
        "mae",
        "mse",
        "rmse",
        "rmsle",
        "mape",
        "smape",
        "log_loss",
        "brier_score",
        "median_absolute_error",
        "max_error",
        "davies_bouldin",
    }
    return "Lower is better" if name in lower_is_better else "Higher is better"


def _pdf_diagnostic_visuals(
    task: str,
    diagnostics: dict[str, Any],
    styles: dict[str, ParagraphStyle],
) -> list[Any]:
    specs = _diagnostic_chart_specs(task, diagnostics)
    if not specs:
        return [
            Paragraph("Model diagnostics", styles["subheading"]),
            _pdf_notice(
                "This historical candidate did not persist chart-ready diagnostics.",
                styles,
            ),
        ]
    result: list[Any] = [Paragraph("Model diagnostics", styles["subheading"])]
    for index in range(0, len(specs), 2):
        pair = specs[index : index + 2]
        charts: list[Any] = [
            EvidenceChartFlowable(title, kind, payload) for title, kind, payload in pair
        ]
        if len(charts) == 1:
            charts.append(Spacer(72.5 * mm, 62 * mm))
        row = Table([charts], colWidths=[72.5 * mm, 72.5 * mm], hAlign="LEFT")
        row.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]
            )
        )
        result.extend([row, Spacer(1, 4 * mm)])
    return result


def _diagnostic_chart_specs(
    task: str,
    diagnostics: dict[str, Any],
) -> list[tuple[str, str, Any]]:
    specs: list[tuple[str, str, Any]] = []
    samples = diagnostics.get("prediction_samples") or []
    if task in {"regression", "time_series"} and samples:
        specs.extend(
            [
                ("Actual vs predicted", "actual_predicted", samples),
                ("Residual distribution", "histogram", [row.get("residual") for row in samples]),
            ]
        )
        if task == "time_series":
            specs.append(("Chronological holdout", "chronological", samples))
    if task == "classification":
        matrix = diagnostics.get("confusion_matrix") or []
        labels = diagnostics.get("labels") or []
        if matrix:
            specs.append(
                ("Confusion matrix", "confusion_matrix", {"matrix": matrix, "labels": labels})
            )
        if diagnostics.get("roc_curves"):
            specs.append(("ROC curve", "roc", diagnostics["roc_curves"]))
        if diagnostics.get("precision_recall_curves"):
            specs.append(
                ("Precision–recall curve", "precision_recall", diagnostics["precision_recall_curves"])
            )
        if diagnostics.get("classification_report"):
            specs.append(
                ("Per-class quality", "per_class", diagnostics["classification_report"])
            )
    if task == "clustering" and diagnostics.get("cluster_sizes"):
        specs.append(("Cluster sizes", "cluster_sizes", diagnostics["cluster_sizes"]))
    learning = diagnostics.get("learning_curve") or {}
    if learning.get("points"):
        scoring = _humanize(learning.get("scoring") or "score")
        specs.append((f"Learning curve · {scoring}", "learning_curve", learning))
    cross_validation = diagnostics.get("cross_validation") or {}
    if isinstance(cross_validation.get("mean"), (int, float)):
        specs.append(("Cross-validation stability", "cross_validation", cross_validation))
    elif task == "clustering" and cross_validation.get("fold_metrics"):
        specs.append(("Cross-validation by fold", "fold_metrics", cross_validation))
    return specs


class TargetDistributionFlowable(Flowable):
    """Match the UI's upright target histogram and regression statistic strip."""

    def __init__(self, target: dict[str, Any], *, task: str, target_name: str) -> None:
        super().__init__()
        self.target = target
        self.task = task
        self.target_name = target_name
        self.orientation = "vertical"
        self.items = [
            (str(item.get("label")), _finite_number(item.get("count"), 0.0))
            for item in (target.get("distribution") or [])[:24]
        ]
        self.width = CONTENT_WIDTH
        self.height = 92 * mm if task == "regression" else 75 * mm

    def draw(self) -> None:
        canvas = self.canv
        statistics = self.target.get("statistics") or {}
        stats_height = 18 * mm if self.task == "regression" else 0
        plot_x = 14 * mm
        plot_y = stats_height + 20 * mm
        plot_width = self.width - 20 * mm
        plot_height = self.height - plot_y - 6 * mm
        canvas.setFillColor(PLOT)
        canvas.rect(plot_x, plot_y, plot_width, plot_height, stroke=0, fill=1)
        maximum = max((value for _, value in self.items), default=1.0) or 1.0
        for tick_index in range(3):
            value = maximum * tick_index / 2
            y = plot_y + plot_height * tick_index / 2
            canvas.setStrokeColor(LINE)
            canvas.setLineWidth(0.35)
            canvas.line(plot_x, y, plot_x + plot_width, y)
            canvas.setFillColor(MUTED)
            canvas.setFont("Helvetica", 6.2)
            canvas.drawRightString(plot_x - 1.5 * mm, y - 1.8, _axis_value(value))
        if self.items:
            slot = plot_width / len(self.items)
            gap = min(1.2 * mm, slot * (0.2 if self.task == "classification" else 0.08))
            for index, (label, value) in enumerate(self.items):
                x = plot_x + index * slot + gap / 2
                height = plot_height * max(0, value) / maximum
                canvas.setFillColor(BLUE)
                canvas.rect(x, plot_y, max(0.6, slot - gap), height, stroke=0, fill=1)
                canvas.saveState()
                canvas.translate(x + slot * 0.05, plot_y - 2.5 * mm)
                canvas.rotate(-32)
                canvas.setFillColor(MUTED)
                canvas.setFont("Helvetica", 5.5)
                canvas.drawString(0, 0, _fit_text(label, 16))
                canvas.restoreState()
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica", 7.2)
        canvas.drawCentredString(
            plot_x + plot_width / 2,
            stats_height + 3.5 * mm,
            self.target_name if self.task == "classification" else "Target range",
        )
        canvas.saveState()
        canvas.translate(3.5 * mm, plot_y + plot_height / 2)
        canvas.rotate(90)
        canvas.drawCentredString(0, 0, "Count")
        canvas.restoreState()
        if self.task == "regression":
            stat_items = [
                ("Minimum", statistics.get("min")),
                ("Median", statistics.get("median")),
                ("Mean", statistics.get("mean")),
                ("Maximum", statistics.get("max")),
            ]
            card_gap = 2 * mm
            card_width = (self.width - card_gap * 3) / 4
            for index, (label, value) in enumerate(stat_items):
                x = index * (card_width + card_gap)
                canvas.setFillColor(PLOT)
                canvas.roundRect(x, 0, card_width, 14 * mm, 2.5 * mm, stroke=0, fill=1)
                canvas.setFillColor(MUTED)
                canvas.setFont("Helvetica", 6.2)
                canvas.drawString(x + 3 * mm, 9 * mm, label)
                canvas.setFillColor(INK)
                canvas.setFont("Helvetica-Bold", 9)
                canvas.drawString(x + 3 * mm, 3.5 * mm, _display_value(value))


class EvidenceChartFlowable(Flowable):
    """Render persisted diagnostics with the same chart grammar as the React UI."""

    SERIES_COLORS = [BLUE, ORANGE, GREEN, colors.HexColor("#B54B5C"), MUTED]

    def __init__(self, title: str, kind: str, payload: Any) -> None:
        super().__init__()
        self.title = title
        self.kind = kind
        self.payload = payload
        self.border_visible = False
        self.width = 72.5 * mm
        self.height = 62 * mm

    def draw(self) -> None:
        canvas = self.canv
        canvas.setFillColor(colors.white)
        canvas.roundRect(
            0,
            0,
            self.width,
            self.height,
            2.2 * mm,
            stroke=int(self.border_visible),
            fill=1,
        )
        canvas.setFillColor(INK)
        canvas.setFont("Helvetica-Bold", 7.2)
        canvas.drawString(4 * mm, self.height - 7 * mm, _fit_text(self.title, 52))
        plot = (11 * mm, 11 * mm, self.width - 16 * mm, self.height - 25 * mm)
        if self.kind == "actual_predicted":
            self._actual_predicted(plot)
        elif self.kind == "histogram":
            self._histogram(plot)
        elif self.kind == "chronological":
            self._chronological(plot)
        elif self.kind == "confusion_matrix":
            self._confusion_matrix(plot)
        elif self.kind in {"roc", "precision_recall"}:
            self._probability_curves(plot)
        elif self.kind == "per_class":
            self._per_class(plot)
        elif self.kind == "learning_curve":
            self._learning_curve(plot)
        elif self.kind == "cross_validation":
            self._cross_validation(plot)
        elif self.kind == "cluster_sizes":
            self._category_bars(plot, list((self.payload or {}).items()), "Rows")
        elif self.kind == "fold_metrics":
            self._fold_metrics(plot)

    def _axes(
        self,
        plot: tuple[float, float, float, float],
        *,
        x_label: str = "",
        y_label: str = "",
    ) -> None:
        x, y, width, height = plot
        canvas = self.canv
        canvas.setFillColor(PLOT)
        canvas.rect(x, y, width, height, stroke=0, fill=1)
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.35)
        for index in range(3):
            line_y = y + height * index / 2
            canvas.line(x, line_y, x + width, line_y)
        canvas.setStrokeColor(MUTED)
        canvas.line(x, y, x + width, y)
        canvas.line(x, y, x, y + height)
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica", 5.4)
        if x_label:
            canvas.drawCentredString(x + width / 2, 3.5 * mm, x_label)
        if y_label:
            canvas.saveState()
            canvas.translate(3 * mm, y + height / 2)
            canvas.rotate(90)
            canvas.drawCentredString(0, 0, y_label)
            canvas.restoreState()

    def _actual_predicted(self, plot: tuple[float, float, float, float]) -> None:
        points = [
            (_finite_number(row.get("actual")), _finite_number(row.get("predicted")))
            for row in self.payload
            if isinstance(row, dict)
        ]
        points = [(x, y) for x, y in points if x is not None and y is not None]
        self._axes(plot, x_label="Actual", y_label="Predicted")
        if not points:
            return
        minimum = min(min(x, y) for x, y in points)
        maximum = max(max(x, y) for x, y in points)
        x, y, width, height = plot
        canvas = self.canv
        canvas.setStrokeColor(ORANGE)
        canvas.setDash(3, 2)
        canvas.line(x, y, x + width, y + height)
        canvas.setDash()
        canvas.setFillColor(BLUE)
        for actual, predicted in points[:300]:
            px = x + _scale(actual, minimum, maximum) * width
            py = y + _scale(predicted, minimum, maximum) * height
            canvas.circle(px, py, 1.05, stroke=0, fill=1)

    def _histogram(self, plot: tuple[float, float, float, float]) -> None:
        values = [
            number for item in self.payload if (number := _finite_number(item)) is not None
        ]
        bins = _histogram_counts(values, 12)
        self._axes(plot, x_label="Residual", y_label="Rows")
        self._draw_vertical_bars(plot, bins, BLUE)

    def _chronological(self, plot: tuple[float, float, float, float]) -> None:
        actual = []
        predicted = []
        for row in self.payload:
            if not isinstance(row, dict):
                continue
            order = _finite_number(row.get("order"))
            observed = _finite_number(row.get("actual"))
            estimate = _finite_number(row.get("predicted"))
            if order is not None and observed is not None:
                actual.append((order, observed))
            if order is not None and estimate is not None:
                predicted.append((order, estimate))
        self._axes(plot, x_label="Holdout order", y_label="Target")
        self._draw_line_series(plot, [(actual, BLUE), (predicted, ORANGE)])

    def _confusion_matrix(self, plot: tuple[float, float, float, float]) -> None:
        matrix = list((self.payload or {}).get("matrix") or [])
        labels = [str(item) for item in (self.payload or {}).get("labels") or []]
        if not matrix:
            return
        matrix = [list(row)[:8] for row in matrix[:8]]
        labels = labels[: len(matrix)]
        x, y, width, height = plot
        size = min(width, height)
        left = x + (width - size) / 2
        bottom = y + (height - size) / 2
        maximum = max((_finite_number(value, 0.0) for row in matrix for value in row), default=1.0) or 1.0
        cell_width = size / max(1, len(matrix[0]))
        cell_height = size / len(matrix)
        canvas = self.canv
        for row_index, row in enumerate(matrix):
            for column_index, raw in enumerate(row):
                value = _finite_number(raw, 0.0) or 0.0
                intensity = 0.16 + 0.84 * value / maximum
                canvas.setFillColor(colors.Color(0.94 - 0.76 * intensity, 0.96 - 0.68 * intensity, 1.0 - 0.08 * intensity))
                cell_x = left + column_index * cell_width
                cell_y = bottom + (len(matrix) - row_index - 1) * cell_height
                canvas.rect(cell_x, cell_y, cell_width, cell_height, stroke=0, fill=1)
                canvas.setFillColor(colors.white if intensity > 0.58 else INK)
                canvas.setFont("Helvetica-Bold", 5)
                canvas.drawCentredString(cell_x + cell_width / 2, cell_y + cell_height / 2 - 1.5, _axis_value(value))
        canvas.setFillColor(MUTED)
        canvas.setFont("Helvetica", 4.5)
        for index, label in enumerate(labels):
            canvas.drawCentredString(left + (index + 0.5) * cell_width, bottom - 5, _fit_text(label, 9))
            canvas.drawRightString(left - 2, bottom + (len(matrix) - index - 0.5) * cell_height - 1.5, _fit_text(label, 9))

    def _probability_curves(self, plot: tuple[float, float, float, float]) -> None:
        x_key = "false_positive_rate" if self.kind == "roc" else "recall"
        y_key = "true_positive_rate" if self.kind == "roc" else "precision"
        x_label = "False positive rate" if self.kind == "roc" else "Recall"
        y_label = "True positive rate" if self.kind == "roc" else "Precision"
        self._axes(plot, x_label=x_label, y_label=y_label)
        series = []
        for index, curve in enumerate(self.payload or []):
            points = [
                (_finite_number(point.get(x_key)), _finite_number(point.get(y_key)))
                for point in curve.get("points") or []
                if isinstance(point, dict)
            ]
            series.append(([(x, y) for x, y in points if x is not None and y is not None], self.SERIES_COLORS[index % len(self.SERIES_COLORS)]))
        self._draw_line_series(plot, series, fixed_bounds=(0.0, 1.0, 0.0, 1.0))
        if self.kind == "roc":
            x, y, width, height = plot
            self.canv.setStrokeColor(MUTED)
            self.canv.setDash(2, 2)
            self.canv.line(x, y, x + width, y + height)
            self.canv.setDash()

    def _per_class(self, plot: tuple[float, float, float, float]) -> None:
        report = self.payload or {}
        labels = [
            str(label)
            for label, values in report.items()
            if isinstance(values, dict) and "precision" in values
        ][:7]
        groups = []
        for label in labels:
            values = report[label]
            groups.append(
                (
                    label,
                    [
                        _finite_number(values.get("precision"), 0.0) or 0.0,
                        _finite_number(values.get("recall"), 0.0) or 0.0,
                        _finite_number(values.get("f1-score"), 0.0) or 0.0,
                    ],
                )
            )
        self._axes(plot, y_label="Score")
        self._draw_grouped_bars(plot, groups, maximum=1.0)

    def _learning_curve(self, plot: tuple[float, float, float, float]) -> None:
        points = self.payload.get("points") or []
        training = [
            (_finite_number(point.get("training_rows")), _finite_number(point.get("training_mean")))
            for point in points
        ]
        validation = [
            (_finite_number(point.get("training_rows")), _finite_number(point.get("validation_mean")))
            for point in points
        ]
        self._axes(plot, x_label="Training rows", y_label=_humanize(self.payload.get("scoring") or "Score"))
        self._draw_line_series(
            plot,
            [
                ([(x, y) for x, y in training if x is not None and y is not None], BLUE),
                ([(x, y) for x, y in validation if x is not None and y is not None], ORANGE),
            ],
        )

    def _cross_validation(self, plot: tuple[float, float, float, float]) -> None:
        values = [
            ("Mean", _finite_number(self.payload.get("mean"), 0.0) or 0.0),
            ("Std dev", _finite_number(self.payload.get("standard_deviation"), 0.0) or 0.0),
        ]
        self._axes(plot)
        self._draw_signed_category_bars(plot, values)

    def _fold_metrics(self, plot: tuple[float, float, float, float]) -> None:
        folds = self.payload.get("fold_metrics") or []
        names = sorted({name for fold in folds for name in fold})[:5]
        series = []
        for index, name in enumerate(names):
            points = [
                (float(fold_index + 1), _finite_number(fold.get(name)))
                for fold_index, fold in enumerate(folds)
            ]
            series.append(([(x, y) for x, y in points if y is not None], self.SERIES_COLORS[index]))
        self._axes(plot, x_label="Fold", y_label="Metric")
        self._draw_line_series(plot, series)

    def _category_bars(
        self,
        plot: tuple[float, float, float, float],
        values: list[tuple[Any, Any]],
        y_label: str,
    ) -> None:
        self._axes(plot, x_label="Cluster", y_label=y_label)
        normalized = [(str(label), _finite_number(value, 0.0) or 0.0) for label, value in values]
        self._draw_vertical_bars(plot, [value for _, value in normalized], BLUE, [label for label, _ in normalized])

    def _draw_vertical_bars(
        self,
        plot: tuple[float, float, float, float],
        values: list[float],
        color: colors.Color,
        labels: list[str] | None = None,
    ) -> None:
        if not values:
            return
        x, y, width, height = plot
        maximum = max(values) or 1.0
        slot = width / len(values)
        self.canv.setFillColor(color)
        for index, value in enumerate(values):
            bar_height = max(0, value) / maximum * height
            self.canv.rect(x + index * slot + 1, y, max(1, slot - 2), bar_height, stroke=0, fill=1)
            if labels:
                self.canv.setFillColor(MUTED)
                self.canv.setFont("Helvetica", 4.5)
                self.canv.drawCentredString(x + (index + 0.5) * slot, y - 5, _fit_text(labels[index], 8))
                self.canv.setFillColor(color)

    def _draw_grouped_bars(
        self,
        plot: tuple[float, float, float, float],
        groups: list[tuple[str, list[float]]],
        *,
        maximum: float,
    ) -> None:
        if not groups:
            return
        x, y, width, height = plot
        group_width = width / len(groups)
        bar_width = group_width * 0.72 / 3
        for group_index, (label, values) in enumerate(groups):
            start = x + group_index * group_width + group_width * 0.14
            for metric_index, value in enumerate(values):
                self.canv.setFillColor(self.SERIES_COLORS[metric_index])
                bar_height = max(0, value) / maximum * height
                self.canv.rect(start + metric_index * bar_width, y, bar_width, bar_height, stroke=0, fill=1)
            self.canv.setFillColor(MUTED)
            self.canv.setFont("Helvetica", 4.2)
            self.canv.drawCentredString(x + (group_index + 0.5) * group_width, y - 5, _fit_text(label, 8))

    def _draw_signed_category_bars(
        self,
        plot: tuple[float, float, float, float],
        values: list[tuple[str, float]],
    ) -> None:
        x, y, width, height = plot
        minimum = min(0.0, *(value for _, value in values))
        maximum = max(0.0, *(value for _, value in values))
        if minimum == maximum:
            maximum = minimum + 1.0
        zero_y = y + _scale(0.0, minimum, maximum) * height
        self.canv.setStrokeColor(MUTED)
        self.canv.line(x, zero_y, x + width, zero_y)
        slot = width / len(values)
        for index, (label, value) in enumerate(values):
            value_y = y + _scale(value, minimum, maximum) * height
            bottom = min(zero_y, value_y)
            self.canv.setFillColor(self.SERIES_COLORS[index])
            self.canv.rect(x + index * slot + slot * 0.18, bottom, slot * 0.64, abs(value_y - zero_y), stroke=0, fill=1)
            self.canv.setFillColor(MUTED)
            self.canv.setFont("Helvetica", 4.8)
            self.canv.drawCentredString(x + (index + 0.5) * slot, y - 5, label)

    def _draw_line_series(
        self,
        plot: tuple[float, float, float, float],
        series: list[tuple[list[tuple[float, float]], colors.Color]],
        fixed_bounds: tuple[float, float, float, float] | None = None,
    ) -> None:
        populated = [(points, color) for points, color in series if points]
        if not populated:
            return
        if fixed_bounds:
            x_min, x_max, y_min, y_max = fixed_bounds
        else:
            x_values = [point[0] for points, _ in populated for point in points]
            y_values = [point[1] for points, _ in populated for point in points]
            x_min, x_max = min(x_values), max(x_values)
            y_min, y_max = min(y_values), max(y_values)
        x, y, width, height = plot
        for points, color in populated:
            self.canv.setStrokeColor(color)
            self.canv.setFillColor(color)
            self.canv.setLineWidth(0.9)
            previous = None
            for point_x, point_y in points:
                px = x + _scale(point_x, x_min, x_max) * width
                py = y + _scale(point_y, y_min, y_max) * height
                if previous:
                    self.canv.line(previous[0], previous[1], px, py)
                self.canv.circle(px, py, 0.8, stroke=0, fill=1)
                previous = (px, py)


def _finite_number(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _scale(value: float, minimum: float, maximum: float) -> float:
    return 0.5 if maximum == minimum else (value - minimum) / (maximum - minimum)


def _axis_value(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.1f}m"
    if absolute >= 1_000:
        return f"{value / 1_000:.1f}k"
    return f"{value:.3g}"


def _histogram_counts(values: list[float], bins: int) -> list[float]:
    if not values:
        return []
    minimum, maximum = min(values), max(values)
    if minimum == maximum:
        return [float(len(values))]
    counts = [0.0] * bins
    for value in values:
        index = min(bins - 1, int((value - minimum) / (maximum - minimum) * bins))
        counts[index] += 1
    return counts


class AuditBarsFlowable(Flowable):
    def __init__(
        self, items: list[tuple[str, float]], *, signed: bool = False, value_suffix: str = ""
    ) -> None:
        super().__init__()
        self.items = items or [("No evidence recorded", 0.0)]
        self.signed = signed
        self.value_suffix = value_suffix
        self.width = CONTENT_WIDTH
        self.height = max(27 * mm, len(self.items) * 7.2 * mm + 3 * mm)

    def draw(self) -> None:
        canvas = self.canv
        maximum = max((abs(value) for _, value in self.items), default=1) or 1
        label_width = 49 * mm
        value_width = 22 * mm
        chart_x = label_width
        chart_width = self.width - label_width - value_width
        row_height = 7.2 * mm
        for index, (label, value) in enumerate(self.items):
            y = self.height - (index + 1) * row_height
            canvas.setFillColor(INK)
            canvas.setFont("Helvetica", 7)
            canvas.drawString(0, y + 2.2 * mm, _fit_text(label, 34))
            canvas.setFillColor(colors.HexColor("#E7EAF0"))
            canvas.roundRect(chart_x, y + 1.5 * mm, chart_width, 3 * mm, 1.5 * mm, stroke=0, fill=1)
            ratio = min(1.0, abs(value) / maximum)
            if self.signed:
                midpoint = chart_x + chart_width / 2
                canvas.setStrokeColor(colors.HexColor("#AAB2C0"))
                canvas.line(midpoint, y + 1 * mm, midpoint, y + 5 * mm)
                canvas.setFillColor(BLUE if value >= 0 else ORANGE)
                width = chart_width / 2 * ratio
                x = midpoint if value >= 0 else midpoint - width
            else:
                canvas.setFillColor(BLUE)
                width = chart_width * ratio
                x = chart_x
            canvas.roundRect(x, y + 1.5 * mm, width, 3 * mm, 1.5 * mm, stroke=0, fill=1)
            canvas.setFillColor(INK)
            canvas.setFont("Helvetica-Bold", 7)
            rendered = f"{value:+.4f}" if self.signed else f"{value:.4g}{self.value_suffix}"
            canvas.drawRightString(self.width, y + 2.2 * mm, rendered)


class PipelinePdfFlowable(Flowable):
    def __init__(self, diagram: dict[str, Any], *, width: float = 168 * mm) -> None:
        super().__init__()
        self.diagram = diagram
        self._base_width = 168 * mm
        branches = (diagram.get("transformer") or {}).get("branches") or []
        max_steps = max((len(branch.get("steps") or []) for branch in branches), default=1)
        self._base_height = (91 + max_steps * 12) * mm
        self._scale_factor = width / self._base_width
        self.width = width
        self.height = self._base_height * self._scale_factor

    def draw(self) -> None:
        canvas = self.canv
        layout_width, layout_height = self.width, self.height
        canvas.saveState()
        canvas.scale(self._scale_factor, self._scale_factor)
        self.width, self.height = self._base_width, self._base_height
        transformer = self.diagram.get("transformer") or {}
        branches = transformer.get("branches") or []
        top = self.height
        gate_y = top - 7 * mm
        gates = self.diagram.get("input_gates") or []
        gate_width = self.width / max(1, len(gates)) - 2 * mm
        for index, gate in enumerate(gates):
            x = index * (gate_width + 2 * mm)
            self._box(x, gate_y - 7 * mm, gate_width, 7 * mm, _fit_text(str(gate), 31), WASH, 6.5)
        root_y = gate_y - 21 * mm
        self._box(45 * mm, root_y, 78 * mm, 11 * mm, "Pipeline · Feature preprocessing", BLUE_SOFT, 8)
        canvas.setStrokeColor(MUTED)
        canvas.line(self.width / 2, root_y, self.width / 2, root_y - 6 * mm)
        transformer_top = root_y - 6 * mm
        max_steps = max((len(branch.get("steps") or []) for branch in branches), default=1)
        transformer_height = (26 + max_steps * 12) * mm
        transformer_y = transformer_top - transformer_height
        canvas.setStrokeColor(MUTED)
        canvas.setLineWidth(0.7)
        canvas.roundRect(5 * mm, transformer_y, self.width - 10 * mm, transformer_height, 3 * mm, stroke=1, fill=0)
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(INK)
        canvas.drawString(10 * mm, transformer_top - 7 * mm, f"{transformer.get('name', 'preprocessor')} · {transformer.get('type', 'ColumnTransformer')}")
        branch_gap = 8 * mm
        branch_width = (self.width - 26 * mm) / max(1, len(branches))
        branch_centres: list[float] = []
        for index, branch in enumerate(branches):
            x = 9 * mm + index * (branch_width + branch_gap)
            branch_centres.append(x + branch_width / 2)
            canvas.setFont("Helvetica-Bold", 7.5)
            canvas.drawCentredString(x + branch_width / 2, transformer_top - 15 * mm, str(branch.get("label")))
            for step_index, step in enumerate(branch.get("steps") or []):
                y = transformer_top - (27 + step_index * 12) * mm
                self._box(x, y, branch_width, 9 * mm, _fit_text(str(step), 32), colors.HexColor("#FFF9F1"), 7)
                if step_index:
                    canvas.setStrokeColor(MUTED)
                    canvas.line(x + branch_width / 2, y + 9 * mm, x + branch_width / 2, y + 12 * mm)
        convergence_y = transformer_y + 5 * mm
        if branch_centres:
            for centre in branch_centres:
                canvas.line(centre, transformer_y + 10 * mm, centre, convergence_y)
                canvas.line(centre, convergence_y, self.width / 2, convergence_y)
        selector = self.diagram.get("selector")
        next_y = transformer_y - 17 * mm
        canvas.line(self.width / 2, transformer_y, self.width / 2, next_y + 11 * mm)
        if selector:
            self._box(47 * mm, next_y, 74 * mm, 11 * mm, f"{selector.get('name')} · {selector.get('type')}", BLUE_SOFT, 7.5)
            next_y -= 17 * mm
            canvas.line(self.width / 2, next_y + 17 * mm, self.width / 2, next_y + 11 * mm)
        estimator = self.diagram.get("estimator") or {}
        self._box(43 * mm, next_y, 82 * mm, 11 * mm, f"{estimator.get('name', 'estimator')} · {estimator.get('type', 'model')}", NAVY, 8, white=True)
        self.width, self.height = layout_width, layout_height
        canvas.restoreState()

    def _box(
        self, x: float, y: float, width: float, height: float, label: str,
        fill: colors.Color, font_size: float, *, white: bool = False,
    ) -> None:
        canvas = self.canv
        canvas.setFillColor(fill)
        canvas.setStrokeColor(LINE)
        canvas.roundRect(x, y, width, height, 2 * mm, stroke=1, fill=1)
        canvas.setFillColor(colors.white if white else INK)
        canvas.setFont("Helvetica-Bold", font_size)
        canvas.drawCentredString(x + width / 2, y + height / 2 - font_size / 3, label)


def _fit_text(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: max(1, limit - 1)] + "…"


def _audit_html(report: dict[str, Any]) -> str:
    document = report["document"]
    identity = report["model_identity"]
    target = report["dataset_and_target"].get("target_visualization") or {}
    processing = report["feature_processing"]
    pipeline = report["training_pipeline"]
    training = report["model_training"]
    metrics = report["model_metrics"]
    contributions = report["feature_contributions"]
    missing = document.get("missing_evidence") or []
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_escape(identity['model_name'])} · model audit</title>
<style>
:root{{--ink:#142033;--muted:#657187;--line:#dfe4ec;--paper:#fffefb;--wash:#f4f6f9;--blue:#2854c5;--orange:#b85f2b;}}
*{{box-sizing:border-box}} body{{margin:0;background:#e9edf3;color:var(--ink);font:14px/1.55 Arial,sans-serif}}
main{{width:min(1100px,calc(100% - 32px));margin:28px auto;background:var(--paper);box-shadow:0 18px 60px #20314b22}}
header{{padding:38px 42px 30px;border-bottom:1px solid var(--line);background:radial-gradient(circle at 90% 0,#e6edff,transparent 34%)}}
.kicker{{color:var(--blue);font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase}}
h1{{margin:7px 0 4px;font-size:34px;line-height:1.08;letter-spacing:-.04em}} h2{{margin:0 0 17px;font-size:20px}} h3{{margin:0 0 8px;font-size:14px}}
p{{margin:4px 0;color:var(--muted)}} .identity{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;margin-top:25px;background:var(--line)}}
.identity div{{padding:12px;background:#fff}} dt{{color:var(--muted);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em}} dd{{margin:4px 0 0;font-weight:700;overflow-wrap:anywhere}}
section{{padding:29px 42px;border-bottom:1px solid var(--line)}} .status{{display:inline-block;padding:3px 8px;background:#eaf0ff;color:#21469e;font-size:11px;font-weight:800}}
.notice{{margin-top:18px;padding:12px 15px;border-left:3px solid #bd762f;background:#fff5e8;color:#69451f}} .hash{{font:11px/1.5 ui-monospace,monospace;overflow-wrap:anywhere}}
.pipeline{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}} .stage{{position:relative;padding:13px;border-top:3px solid #aeb8ca;background:var(--wash);min-height:104px}}
.stage.completed{{border-color:var(--blue)}} .stage.failed,.stage.cancelled{{border-color:var(--orange)}} .stage small{{display:block;margin-bottom:5px;color:var(--muted);font-size:10px;text-transform:uppercase}}
.stage p{{font-size:12px}} .two{{display:grid;grid-template-columns:1fr 1fr;gap:25px}} table{{width:100%;border-collapse:collapse}} th,td{{padding:8px 9px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}} th{{color:var(--muted);font-size:10px;text-transform:uppercase}}
.formula{{padding:20px;background:#17233a;color:#f5f7fd;font:20px/1.4 Georgia,serif;overflow-wrap:anywhere}} .chart{{display:grid;gap:8px}} .bar{{display:grid;grid-template-columns:minmax(90px,1fr) 3fr 70px;align-items:center;gap:9px;font-size:12px}}
.track{{height:11px;background:#e8ebf1}} .fill{{display:block;height:100%;background:var(--blue)}} .waterfall .fill.negative{{margin-left:auto;background:var(--orange)}}
.waterfall .track{{background:linear-gradient(90deg,transparent 49.6%,#aeb8c8 49.6% 50.4%,transparent 50.4%)}} .waterfall .fill{{margin-left:50%;max-width:50%}}
.waterfall .fill.negative{{margin-left:auto;margin-right:50%}} .raw-note{{font-size:12px}} .page-break{{break-before:page}}
@media(max-width:760px){{.identity,.pipeline{{grid-template-columns:1fr 1fr}}.two{{grid-template-columns:1fr}}header,section{{padding-left:22px;padding-right:22px}}}}
@media print{{body{{background:#fff}}main{{width:100%;margin:0;box-shadow:none}}section{{break-inside:avoid}}}}
</style></head><body><main>
<header><span class="kicker">Sceptre AI · model evidence</span><h1>{_escape(identity['model_name'])}</h1>
<p>{_escape(identity['task_type'])} model trained against target <b>{_escape(identity.get('target_column') or 'Not applicable')}</b></p>
<dl class="identity">{_identity_item('Status', identity.get('candidate_status'))}{_identity_item('Rank', identity.get('rank'))}{_identity_item('Training run', identity.get('training_run_id'))}{_identity_item('Generated', document.get('generated_at'))}</dl>
{f'<div class="notice"><b>Partial evidence package.</b> Missing: {_escape(", ".join(missing))}</div>' if missing else ''}
<p class="hash">Evidence SHA-256 · {_escape(document.get('evidence_sha256'))}</p></header>
<section><h2>Training pipeline</h2><div class="pipeline">{''.join(_pipeline_stage(stage) for stage in pipeline.get('stages', []))}</div></section>
<section><h2>Target evidence</h2><div class="two"><div><h3>{_escape(identity.get('target_column') or 'No target')}</h3><p>{_escape(target.get('semantic_type') or 'Target profile was not recorded.')}</p>{_target_summary(target)}</div><div>{_distribution_chart(target)}</div></div></section>
<section><h2>Feature processing</h2><div class="two"><div><h3>Executable training contract</h3>{_processing_table(processing.get('executable_training_contract') or {})}</div><div><h3>Profile-driven preparation evidence</h3>{_preparation_table(processing.get('profiling_recommendations') or [])}</div></div></section>
<section><h2>Training and tuning</h2>{_mapping_table(training)}</section>
<section><h2>Model metrics</h2>{_mapping_table(metrics.get('values') or {})}<h3 style="margin-top:18px">Recorded diagnostics</h3>{_diagnostic_summary(metrics.get('diagnostics') or {})}</section>
<section class="page-break"><h2>Normalized feature contributions</h2><p>Mean absolute SHAP magnitude, normalized to 100% across recorded features.</p>{_normalized_chart(contributions.get('global_normalized_contributions') or [])}</section>
<section><h2>Representative SHAP waterfall</h2><p>Directional contributions for the first persisted explanation sample.</p>{_waterfall_chart(contributions.get('waterfall') or {})}<p class="raw-note">Full raw and normalized sample arrays are included in the JSON version of this audit document.</p></section>
<section><h2>Audit boundary</h2><p>{_escape(document.get('regulatory_note'))}</p><p>Feature contributions describe model behaviour, not causal effects. Evidence reflects the stated cutoff and should be regenerated after material data, code, tuning, or model changes.</p></section>
</main></body></html>"""


def _escape(value: Any) -> str:
    return html.escape("Not recorded" if value is None else str(value))


def _identity_item(label: str, value: Any) -> str:
    return f"<div><dt>{_escape(label)}</dt><dd>{_escape(value)}</dd></div>"


def _pipeline_stage(stage: dict[str, Any]) -> str:
    state = re.sub(r"[^a-z_-]", "", str(stage.get("status", "planned")).lower())
    return f'<article class="stage {state}"><small>{_escape(stage.get("status"))}</small><h3>{_escape(stage.get("label"))}</h3><p>{_escape(stage.get("summary"))}</p></article>'


def _target_summary(target: dict[str, Any]) -> str:
    if not target:
        return "<p>No completed target profile was available.</p>"
    stats = target.get("statistics") or {}
    values = {
        "Distinct": target.get("distinct_count"),
        "Missing": target.get("missing_count"),
        "Missing rate": f"{float(target.get('missing_ratio', 0)) * 100:.2f}%",
        **stats,
    }
    return _mapping_table(values)


def _distribution_chart(target: dict[str, Any]) -> str:
    distribution = target.get("distribution") or []
    if not distribution:
        return "<p>No target distribution was recorded.</p>"
    maximum = max(float(item.get("count", 0) or 0) for item in distribution) or 1
    bars = "".join(
        f'<div class="bar"><span>{_escape(item.get("label"))}</span><i class="track"><i class="fill" style="width:{float(item.get("count", 0) or 0) / maximum * 100:.2f}%"></i></i><b>{_escape(item.get("count"))}</b></div>'
        for item in distribution
    )
    return f'<h3>{_escape(target.get("distribution_type") or "distribution")}</h3><div class="chart">{bars}</div>'


def _processing_table(contract: dict[str, Any]) -> str:
    rows = []
    for key, value in contract.items():
        rendered = "; ".join(map(str, value)) if isinstance(value, list) else value
        rows.append(f"<tr><th>{_escape(key.replace('_', ' '))}</th><td>{_escape(rendered)}</td></tr>")
    return f"<table>{''.join(rows)}</table>"


def _preparation_table(steps: list[dict[str, Any]]) -> str:
    if not steps:
        return "<p>No profile preparation steps were recorded.</p>"
    return "<table><thead><tr><th>Feature</th><th>Action</th><th>Strategy</th><th>Reason</th></tr></thead><tbody>" + "".join(
        f"<tr><td>{_escape(step.get('column'))}</td><td>{_escape(step.get('action'))}</td><td>{_escape(step.get('strategy'))}</td><td>{_escape(step.get('reason'))}</td></tr>"
        for step in steps
    ) + "</tbody></table>"


def _mapping_table(values: dict[str, Any]) -> str:
    if not values:
        return "<p>No values were recorded.</p>"
    rows = []
    for key, value in values.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value, sort_keys=True, default=str)
        rows.append(f"<tr><th>{_escape(str(key).replace('_', ' '))}</th><td>{_escape(value)}</td></tr>")
    return f"<table>{''.join(rows)}</table>"


def _diagnostic_summary(diagnostics: dict[str, Any]) -> str:
    selected = {
        key: diagnostics[key]
        for key in ("cross_validation", "runtime", "learning_curve", "cluster_count", "noise_rows")
        if key in diagnostics
    }
    return _mapping_table(selected)


def _normalized_chart(items: list[dict[str, Any]]) -> str:
    if not items:
        return "<div class=\"notice\">SHAP has not been calculated for this model.</div>"
    bars = "".join(
        f'<div class="bar"><span>{_escape(item.get("feature"))}</span><i class="track"><i class="fill" style="width:{min(100, max(0, float(item.get("contribution_percent", 0)))):.2f}%"></i></i><b>{float(item.get("contribution_percent", 0)):.2f}%</b></div>'
        for item in items
    )
    return f'<div class="chart">{bars}</div>'


def _waterfall_chart(waterfall: dict[str, Any]) -> str:
    if waterfall.get("status") != "available":
        return f'<div class="notice">{_escape(waterfall.get("reason") or "Waterfall evidence is unavailable.")}</div>'
    bars = "".join(
        f'<div class="bar"><span>{_escape(item.get("feature"))}</span><i class="track"><i class="fill {"negative" if float(item.get("shap_value", 0)) < 0 else ""}" style="width:{min(50, float(item.get("absolute_percent", 0)) / 2):.2f}%"></i></i><b>{float(item.get("shap_value", 0)):+.4f}</b></div>'
        for item in waterfall.get("features", [])
    )
    return f'<p>Base value: <b>{_escape(waterfall.get("base_value"))}</b> · prediction: <b>{_escape(waterfall.get("prediction_value"))}</b></p><div class="chart waterfall">{bars}</div>'
