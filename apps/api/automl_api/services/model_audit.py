from __future__ import annotations

# Generated audit HTML intentionally keeps CSS and markup inline so the downloaded
# document remains portable and printable without application assets.
# ruff: noqa: E501
import hashlib
import html
import json
import math
import re
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import HTTPException, status
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
    model_mathematics,
)
from automl_api.services.projects import require_project_role
from automl_api.storage.object_store import get_object_store
from automl_api.training.model_catalog import select_candidates

AuditFormat = Literal["html", "json"]


def model_audit_document(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    model_name: str,
    output_format: AuditFormat,
) -> tuple[bytes, str, str, str]:
    """Build a point-in-time audit document for any leaderboard candidate."""
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
    report: dict[str, Any] = {
        "schema_version": "1.0",
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
            "primary_metric": source.tags.get("leaderboard_primary_metric"),
            "cross_validation_folds": source.params.get("cv_folds"),
            "optimization_iterations": source.params.get("optimization_iterations"),
            "best_parameters": entry.get("best_params") or {},
            "runtime": (entry.get("diagnostics") or {}).get("runtime", {}),
            "failure": entry.get("error"),
        },
        "model_mathematics": model_mathematics(model_name, source.task_type),
        "model_metrics": {
            "values": entry.get("metrics") or {},
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
    safe_model = re.sub(r"[^A-Za-z0-9_.-]+", "-", model_name).strip("-") or "model"
    if output_format == "json":
        content = json.dumps(report, indent=2, sort_keys=True, default=str).encode("utf-8")
        return content, "application/json", f"{safe_model}-audit.json", evidence_hash
    content = _audit_html(report).encode("utf-8")
    return content, "text/html; charset=utf-8", f"{safe_model}-audit.html", evidence_hash


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


def _audit_html(report: dict[str, Any]) -> str:
    document = report["document"]
    identity = report["model_identity"]
    target = report["dataset_and_target"].get("target_visualization") or {}
    processing = report["feature_processing"]
    pipeline = report["training_pipeline"]
    training = report["model_training"]
    maths = report["model_mathematics"]
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
<section class="page-break"><h2>Model mathematics</h2><p>{_escape(maths.get('family'))}</p><div class="formula">{_escape(maths.get('equation'))}</div><div class="two"><div><h3>Training objective</h3><p>{_escape(maths.get('training_objective'))}</p></div><div><h3>Prediction rule</h3><p>{_escape(maths.get('prediction_rule'))}</p></div></div></section>
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
