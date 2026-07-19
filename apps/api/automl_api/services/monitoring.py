from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from automl_api.models.enums import (
    ArtifactKind,
    MetricKind,
    MetricSplit,
    ProjectRole,
    RunKind,
    RunStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import Metric, ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.schemas.monitoring import (
    DeploymentMonitoringRead,
    GovernanceReportRead,
    GovernanceReportSummaryRead,
    MonitoringConfigurationRead,
    MonitoringConfigurationUpdate,
    MonitoringDashboardRead,
    MonitoringMetricPointCreate,
    MonitoringMetricPointRead,
    MonitoringMetricSeriesRead,
    MonitoringTimelineEventRead,
)
from automl_api.services.model_audit import _audit_html, model_audit_report
from automl_api.services.projects import list_visible_projects, require_project_role
from automl_api.storage.object_store import get_object_store

DEFAULT_MONITORING = {
    "enabled": False,
    "schedule": "manual",
    "resource_class": "standard",
    "metrics": ["latency_p95_ms", "error_rate", "throughput", "drift_share"],
    "thresholds": {
        "latency_p95_ms": {"warning": 750.0, "critical": 1500.0, "direction": "above"},
        "error_rate": {"warning": 0.02, "critical": 0.05, "direction": "above"},
        "drift_share": {"warning": 0.2, "critical": 0.35, "direction": "above"},
    },
    "retraining_enabled": False,
    "approval_required": True,
    "revision": 0,
    "updated_at": None,
    "updated_by_id": None,
}


def _now() -> datetime:
    return datetime.now(UTC)


def deployment_run(
    db: Session,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> ModelRun:
    run = db.scalar(
        select(ModelRun).where(
            ModelRun.project_id == project_id,
            ModelRun.id == deployment_run_id,
            ModelRun.run_kind == RunKind.DEPLOYMENT,
        )
    )
    if run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Deployment not found.",
        )
    return run


def deployment_registry_entry(
    db: Session,
    run: ModelRun,
) -> ModelRegistryEntry | None:
    raw_id = run.tags.get("registry_entry_id") or run.params.get("registry_entry_id")
    if not raw_id:
        return None
    try:
        entry_id = uuid.UUID(str(raw_id))
    except ValueError:
        return None
    return db.scalar(
        select(ModelRegistryEntry).where(
            ModelRegistryEntry.project_id == run.project_id,
            ModelRegistryEntry.id == entry_id,
        )
    )


def _configuration(run: ModelRun) -> MonitoringConfigurationRead:
    stored = run.params.get("monitoring") if isinstance(run.params, dict) else None
    data = {**DEFAULT_MONITORING, **(stored if isinstance(stored, dict) else {})}
    return MonitoringConfigurationRead.model_validate(data)


def get_monitoring_configuration(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> MonitoringConfigurationRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    return _configuration(deployment_run(db, project_id, deployment_run_id))


def update_monitoring_configuration(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    payload: MonitoringConfigurationUpdate,
) -> MonitoringConfigurationRead:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    run = deployment_run(db, project_id, deployment_run_id)
    current = _configuration(run)
    updated = MonitoringConfigurationRead(
        **payload.model_dump(mode="json"),
        revision=current.revision + 1,
        updated_at=_now(),
        updated_by_id=user.id,
    )
    run.params = {
        **run.params,
        "monitoring": updated.model_dump(mode="json"),
    }
    db.flush()
    return updated


def _metric_point(metric: Metric) -> MonitoringMetricPointRead:
    metadata = dict(metric.value_json or {})
    return MonitoringMetricPointRead(
        id=metric.id,
        name=metric.name,
        kind=metric.kind,
        value=metric.value,
        recorded_at=metric.recorded_at,
        sample_count=metadata.pop("sample_count", None),
        higher_is_better=metric.higher_is_better,
        status=str(metadata.pop("status", "unknown")),
        metadata=metadata,
    )


def record_monitoring_metric(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    payload: MonitoringMetricPointCreate,
) -> MonitoringMetricPointRead:
    require_project_role(db, user, project_id, ProjectRole.ADMIN)
    run = deployment_run(db, project_id, deployment_run_id)
    existing = list(
        db.scalars(
            select(Metric).where(
                Metric.project_id == project_id,
                Metric.model_run_id == run.id,
                Metric.name == payload.name,
                Metric.split == MetricSplit.PRODUCTION,
            )
        ).all()
    )
    if payload.idempotency_key:
        duplicate = next(
            (
                metric
                for metric in existing
                if metric.value_json.get("idempotency_key") == payload.idempotency_key
            ),
            None,
        )
        if duplicate is not None:
            return _metric_point(duplicate)
    point = Metric(
        project_id=project_id,
        model_run_id=run.id,
        name=payload.name,
        kind=payload.kind,
        split=MetricSplit.PRODUCTION,
        value=payload.value,
        higher_is_better=payload.higher_is_better,
        step=max((metric.step for metric in existing), default=-1) + 1,
        recorded_at=payload.recorded_at or _now(),
        value_json={
            **payload.metadata,
            "sample_count": payload.sample_count,
            "status": payload.status,
            "idempotency_key": payload.idempotency_key,
            "deployment_run_id": str(run.id),
            "recorded_by_id": str(user.id),
        },
    )
    db.add(point)
    db.flush()
    return _metric_point(point)


def list_monitoring_metrics(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> list[MonitoringMetricSeriesRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    run = deployment_run(db, project_id, deployment_run_id)
    return _metric_series(
        list(
            db.scalars(
                select(Metric)
                .where(
                    Metric.project_id == project_id,
                    Metric.model_run_id == run.id,
                    Metric.split == MetricSplit.PRODUCTION,
                )
                .order_by(Metric.recorded_at.asc())
            ).all()
        )
    )


def _metric_series(metrics: list[Metric]) -> list[MonitoringMetricSeriesRead]:
    grouped: dict[tuple[str, MetricKind], list[Metric]] = defaultdict(list)
    for metric in metrics:
        grouped[(metric.name, metric.kind)].append(metric)
    return [
        MonitoringMetricSeriesRead(
            name=name,
            kind=kind,
            higher_is_better=points[-1].higher_is_better,
            points=[_metric_point(point) for point in points],
        )
        for (name, kind), points in sorted(grouped.items(), key=lambda item: item[0][0])
    ]


def _threshold_status(
    name: str,
    value: float | None,
    configuration: MonitoringConfigurationRead,
    reported_status: str = "unknown",
) -> str:
    if reported_status in {"warning", "critical"}:
        return reported_status
    if value is None:
        return "unknown"
    threshold = configuration.thresholds.get(name)
    if threshold is None:
        return "healthy" if reported_status == "healthy" else "unknown"
    critical = (
        value >= threshold.critical
        if threshold.direction == "above"
        else value <= threshold.critical
    )
    warning = (
        value >= threshold.warning
        if threshold.direction == "above"
        else value <= threshold.warning
    )
    return "critical" if critical else "warning" if warning else "healthy"


def _linked_runs(
    runs: list[ModelRun],
    deployment: ModelRun,
    registry_entry_id: str | None,
) -> tuple[list[ModelRun], list[ModelRun]]:
    drift = []
    retraining = []
    for run in runs:
        exact = (
            run.tags.get("deployment_run_id") == str(deployment.id)
            or run.params.get("deployment_run_id") == str(deployment.id)
        )
        same_model = registry_entry_id and (
            run.tags.get("registry_entry_id") == registry_entry_id
            or run.params.get("registry_entry_id") == registry_entry_id
        )
        if run.run_kind == RunKind.DRIFT and (exact or same_model):
            drift.append(run)
        if run.run_kind == RunKind.TRAINING and exact:
            retraining.append(run)
    return drift, retraining


def _drift_points(
    runs: list[ModelRun],
    configuration: MonitoringConfigurationRead,
) -> list[MonitoringMetricPointRead]:
    result = []
    for run in sorted(runs, key=lambda item: item.created_at):
        diagnostics = run.tags.get("diagnostics") or {}
        percent = diagnostics.get("drift_share_percent")
        value = float(percent) / 100.0 if percent is not None else None
        result.append(
            MonitoringMetricPointRead(
                id=run.id,
                name="drift_share",
                kind=MetricKind.DRIFT,
                value=value,
                recorded_at=run.finished_at or run.created_at,
                sample_count=run.params.get("max_rows"),
                status=(
                    _threshold_status("drift_share", value, configuration)
                    if run.status == RunStatus.SUCCEEDED
                    else run.status.value
                ),
                metadata={
                    "run_id": str(run.id),
                    "drifted_feature_count": diagnostics.get("drifted_feature_count"),
                    "drifted_features": diagnostics.get("drifted_features", []),
                },
            )
        )
    return result


def monitoring_dashboard(
    db: Session,
    user: User,
    project_id: uuid.UUID | None = None,
) -> MonitoringDashboardRead:
    if project_id is not None:
        require_project_role(db, user, project_id, ProjectRole.VIEWER)
        project = db.get(Project, project_id)
        if project is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found.")
        projects = [project]
        scope = "project"
    else:
        projects = list_visible_projects(db, user)
        scope = "portfolio"
    project_by_id = {project.id: project for project in projects}
    project_ids = list(project_by_id)
    if not project_ids:
        return MonitoringDashboardRead(
            scope=scope,
            generated_at=_now(),
            deployment_count=0,
            healthy_count=0,
            attention_count=0,
            unmonitored_count=0,
            open_alert_count=0,
            deployments=[],
        )
    runs = list(
        db.scalars(
            select(ModelRun)
            .where(ModelRun.project_id.in_(project_ids))
            .order_by(ModelRun.created_at.desc())
        ).all()
    )
    deployments = [
        run
        for run in runs
        if run.run_kind == RunKind.DEPLOYMENT and run.status != RunStatus.CANCELLED
    ]
    all_metrics = list(
        db.scalars(
            select(Metric)
            .where(
                Metric.project_id.in_(project_ids),
                Metric.model_run_id.in_([run.id for run in deployments]),
                Metric.split == MetricSplit.PRODUCTION,
            )
            .order_by(Metric.recorded_at.asc())
        ).all()
    ) if deployments else []
    metrics_by_run: dict[uuid.UUID, list[Metric]] = defaultdict(list)
    for metric in all_metrics:
        metrics_by_run[metric.model_run_id].append(metric)
    report_counts = dict(
        db.execute(
            select(RunArtifact.model_run_id, func.count(RunArtifact.id))
            .where(
                RunArtifact.project_id.in_(project_ids),
                RunArtifact.kind == ArtifactKind.GOVERNANCE_REPORT,
            )
            .group_by(RunArtifact.model_run_id)
        ).all()
    )

    items: list[DeploymentMonitoringRead] = []
    for deployment in deployments:
        configuration = _configuration(deployment)
        entry = deployment_registry_entry(db, deployment)
        registry_id = str(entry.id) if entry else deployment.tags.get("registry_entry_id")
        drift_runs, retraining_runs = _linked_runs(runs, deployment, registry_id)
        drift = _drift_points(drift_runs, configuration)
        series = _metric_series(metrics_by_run.get(deployment.id, []))
        latest_points = [
            metric_series.points[-1]
            for metric_series in series
            if metric_series.points
        ]
        statuses = [
            _threshold_status(point.name, point.value, configuration, point.status)
            for point in [*latest_points, *(drift[-1:] if drift else [])]
        ]
        if deployment.status in {RunStatus.FAILED, RunStatus.PREEMPTED}:
            health_status = "critical"
        elif "critical" in statuses:
            health_status = "critical"
        elif "warning" in statuses:
            health_status = "warning"
        elif configuration.enabled and statuses and all(value == "healthy" for value in statuses):
            health_status = "healthy"
        else:
            health_status = "unknown"
        open_alerts = sum(value in {"warning", "critical"} for value in statuses)
        observed = [point.recorded_at for point in latest_points]
        if drift:
            observed.append(drift[-1].recorded_at)
        timeline = [
            MonitoringTimelineEventRead(
                kind="deployment",
                label=deployment.run_name or "Model deployed",
                status=deployment.status.value,
                occurred_at=deployment.started_at or deployment.created_at,
                details={"environment": deployment.tags.get("environment", "kubernetes")},
            ),
            *[
                MonitoringTimelineEventRead(
                    kind="drift",
                    label=run.run_name or "Drift check",
                    status=run.status.value,
                    occurred_at=run.finished_at or run.created_at,
                    details=run.tags.get("diagnostics") or {},
                )
                for run in drift_runs
            ],
            *[
                MonitoringTimelineEventRead(
                    kind="retraining",
                    label=run.run_name or "Retraining run",
                    status=run.status.value,
                    occurred_at=run.finished_at or run.created_at,
                    details={"run_id": str(run.id)},
                )
                for run in retraining_runs
            ],
        ]
        items.append(
            DeploymentMonitoringRead(
                project_id=deployment.project_id,
                project_name=project_by_id[deployment.project_id].name,
                deployment_run_id=deployment.id,
                model_version_id=entry.id if entry else None,
                registry_entry_id=entry.id if entry else None,
                model_name=entry.model_name if entry else deployment.run_name or "Model",
                model_version=entry.version if entry else None,
                environment=str(deployment.tags.get("environment", "kubernetes")),
                task_type=deployment.task_type,
                deployment_status=deployment.status,
                health_status=health_status,
                deployed_at=deployment.started_at or deployment.created_at,
                last_observation_at=max(observed) if observed else None,
                monitoring=configuration,
                baseline_metric_name=entry.champion_metric_name if entry else None,
                baseline_metric_value=entry.champion_metric_value if entry else None,
                metric_series=series,
                drift_history=drift,
                retraining_events=len(retraining_runs),
                open_alerts=open_alerts,
                governance_reports=int(report_counts.get(deployment.id, 0)),
                timeline=sorted(timeline, key=lambda event: event.occurred_at, reverse=True),
            )
        )
    attention = sum(item.health_status in {"warning", "critical"} for item in items)
    return MonitoringDashboardRead(
        scope=scope,
        generated_at=_now(),
        deployment_count=len(items),
        healthy_count=sum(item.health_status == "healthy" for item in items),
        attention_count=attention,
        unmonitored_count=sum(not item.monitoring.enabled for item in items),
        open_alert_count=sum(item.open_alerts for item in items),
        deployments=items,
    )


def _governance_artifacts(
    db: Session,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> list[RunArtifact]:
    return list(
        db.scalars(
            select(RunArtifact)
            .where(
                RunArtifact.project_id == project_id,
                RunArtifact.model_run_id == deployment_run_id,
                RunArtifact.kind == ArtifactKind.GOVERNANCE_REPORT,
            )
            .order_by(RunArtifact.created_at.desc())
        ).all()
    )


def _report_summary(artifact: RunArtifact) -> GovernanceReportSummaryRead:
    metadata = artifact.artifact_metadata or {}
    base = (
        f"/api/v1/projects/{artifact.project_id}/operations/deployments/"
        f"{artifact.model_run_id}/governance/reports/{artifact.id}/download"
    )
    return GovernanceReportSummaryRead(
        id=artifact.id,
        project_id=artifact.project_id,
        deployment_run_id=artifact.model_run_id,
        model_version_id=metadata.get("model_version_id"),
        version=int(metadata.get("version", 1)),
        generated_at=artifact.created_at,
        evidence_cutoff_at=metadata.get("evidence_cutoff_at", artifact.created_at),
        generated_by_id=metadata.get("generated_by_id"),
        content_hash=artifact.content_hash or "",
        json_download_url=f"{base}?format=json",
        html_download_url=f"{base}?format=html" if metadata.get("html_uri") else None,
    )


def list_governance_reports(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> list[GovernanceReportSummaryRead]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    deployment_run(db, project_id, deployment_run_id)
    return [
        _report_summary(artifact)
        for artifact in _governance_artifacts(db, project_id, deployment_run_id)
    ]


def generate_governance_report(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
) -> GovernanceReportRead:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    deployment = deployment_run(db, project_id, deployment_run_id)
    entry = deployment_registry_entry(db, deployment)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The deployment is not linked to a registered model version.",
        )
    source = entry.model_run
    evidence_cutoff = _now()
    reports = _governance_artifacts(db, project_id, deployment.id)
    version = max(
        (int(item.artifact_metadata.get("version", 0)) for item in reports),
        default=0,
    ) + 1
    report, _ = model_audit_report(
        db,
        user,
        project_id,
        source.id,
        entry.model_name,
    )
    json_bytes = json.dumps(report, indent=2, sort_keys=True, default=str).encode("utf-8")
    content_hash = hashlib.sha256(json_bytes).hexdigest()
    json_object = get_object_store().put_bytes(
        f"projects/{project_id}/deployments/{deployment.id}/governance/report-v{version}.json",
        json_bytes,
    )
    html_bytes = _audit_html(report).encode("utf-8")
    html_hash = hashlib.sha256(html_bytes).hexdigest()
    html_object = get_object_store().put_bytes(
        f"projects/{project_id}/deployments/{deployment.id}/governance/report-v{version}.html",
        html_bytes,
    )
    artifact = RunArtifact(
        project_id=project_id,
        model_run_id=deployment.id,
        kind=ArtifactKind.GOVERNANCE_REPORT,
        name=f"governance-report-v{version}.json",
        object_uri=json_object.uri,
        content_hash=content_hash,
        byte_size=len(json_bytes),
        artifact_metadata={
            "version": version,
            "model_version_id": str(entry.id),
            "generated_by_id": str(user.id),
            "evidence_cutoff_at": evidence_cutoff.isoformat(),
            "html_uri": html_object.uri,
            "html_hash": html_hash,
        },
    )
    db.add(artifact)
    db.flush()
    summary = _report_summary(artifact)
    return GovernanceReportRead(**summary.model_dump(), report=report)


def get_governance_report(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    report_id: uuid.UUID,
) -> GovernanceReportRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    artifact = _governance_report_artifact(db, project_id, deployment_run_id, report_id)
    report = json.loads(get_object_store().read_bytes(artifact.object_uri))
    return GovernanceReportRead(**_report_summary(artifact).model_dump(), report=report)


def governance_report_download(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    report_id: uuid.UUID,
    output_format: str,
) -> tuple[bytes, str, str]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    artifact = _governance_report_artifact(db, project_id, deployment_run_id, report_id)
    version = int(artifact.artifact_metadata.get("version", 1))
    if output_format == "html":
        uri = artifact.artifact_metadata.get("html_uri")
        if not uri:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="HTML report not found.",
            )
        return (
            get_object_store().read_bytes(uri),
            "text/html; charset=utf-8",
            f"governance-report-v{version}.html",
        )
    return (
        get_object_store().read_bytes(artifact.object_uri),
        "application/json",
        f"governance-report-v{version}.json",
    )


def _governance_report_artifact(
    db: Session,
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    report_id: uuid.UUID,
) -> RunArtifact:
    artifact = db.scalar(
        select(RunArtifact).where(
            RunArtifact.project_id == project_id,
            RunArtifact.model_run_id == deployment_run_id,
            RunArtifact.id == report_id,
            RunArtifact.kind == ArtifactKind.GOVERNANCE_REPORT,
        )
    )
    if artifact is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Governance report not found.",
        )
    return artifact
