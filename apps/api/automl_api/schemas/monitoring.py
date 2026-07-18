from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from automl_api.models.enums import MetricKind, RunStatus, TaskType


class MonitoringThreshold(BaseModel):
    warning: float
    critical: float
    direction: Literal["above", "below"] = "below"


class MonitoringConfigurationUpdate(BaseModel):
    enabled: bool = True
    schedule: Literal["manual", "hourly", "daily", "weekly"] = "daily"
    resource_class: Literal["small", "standard", "large", "xlarge"] = "standard"
    metrics: list[str] = Field(default_factory=list, max_length=50)
    thresholds: dict[str, MonitoringThreshold] = Field(default_factory=dict)
    retraining_enabled: bool = False
    approval_required: bool = True


class MonitoringConfigurationRead(MonitoringConfigurationUpdate):
    revision: int = 0
    updated_at: datetime | None = None
    updated_by_id: uuid.UUID | None = None


class MonitoringMetricPointCreate(BaseModel):
    name: str = Field(min_length=1, max_length=160, pattern=r"^[A-Za-z0-9_.-]+$")
    kind: MetricKind = MetricKind.PERFORMANCE
    value: float
    recorded_at: datetime | None = None
    sample_count: int | None = Field(default=None, ge=0)
    higher_is_better: bool | None = None
    status: Literal["healthy", "warning", "critical", "unknown"] = "unknown"
    idempotency_key: str | None = Field(default=None, max_length=160)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MonitoringMetricPointRead(BaseModel):
    id: uuid.UUID
    name: str
    kind: MetricKind
    value: float | None
    recorded_at: datetime
    sample_count: int | None = None
    higher_is_better: bool | None = None
    status: str = "unknown"
    metadata: dict[str, Any] = Field(default_factory=dict)


class MonitoringMetricSeriesRead(BaseModel):
    name: str
    kind: MetricKind
    higher_is_better: bool | None = None
    points: list[MonitoringMetricPointRead] = Field(default_factory=list)


class MonitoringTimelineEventRead(BaseModel):
    kind: Literal[
        "deployment",
        "metric_alert",
        "drift",
        "retraining",
        "governance_report",
    ]
    label: str
    status: str
    occurred_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)


class DeploymentMonitoringRead(BaseModel):
    project_id: uuid.UUID
    project_name: str
    deployment_run_id: uuid.UUID
    model_version_id: uuid.UUID | None = None
    registry_entry_id: uuid.UUID | None = None
    model_name: str
    model_version: int | None = None
    environment: str
    task_type: TaskType
    deployment_status: RunStatus
    health_status: Literal["healthy", "warning", "critical", "unknown"]
    deployed_at: datetime
    last_observation_at: datetime | None = None
    monitoring: MonitoringConfigurationRead
    baseline_metric_name: str | None = None
    baseline_metric_value: float | None = None
    metric_series: list[MonitoringMetricSeriesRead] = Field(default_factory=list)
    drift_history: list[MonitoringMetricPointRead] = Field(default_factory=list)
    retraining_events: int = 0
    open_alerts: int = 0
    governance_reports: int = 0
    timeline: list[MonitoringTimelineEventRead] = Field(default_factory=list)


class MonitoringDashboardRead(BaseModel):
    scope: Literal["portfolio", "project"]
    generated_at: datetime
    deployment_count: int
    healthy_count: int
    attention_count: int
    unmonitored_count: int
    open_alert_count: int
    deployments: list[DeploymentMonitoringRead] = Field(default_factory=list)


class GovernanceReportSummaryRead(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    deployment_run_id: uuid.UUID
    model_version_id: uuid.UUID | None = None
    version: int
    generated_at: datetime
    evidence_cutoff_at: datetime
    generated_by_id: uuid.UUID | None = None
    content_hash: str
    json_download_url: str
    html_download_url: str | None = None


class GovernanceReportRead(GovernanceReportSummaryRead):
    report: dict[str, Any]
