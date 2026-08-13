from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import automl_api.services.monitoring as monitoring_service
import pytest
from automl_api.api.routes.monitoring import router
from automl_api.models.enums import (
    ArtifactKind,
    MetricKind,
    MetricSplit,
    RunKind,
    RunStatus,
    TaskType,
)
from automl_api.models.runs import Metric, ModelRun, RunArtifact
from automl_api.schemas.monitoring import (
    MonitoringConfigurationRead,
    MonitoringConfigurationUpdate,
    MonitoringMetricPointCreate,
)
from automl_api.services.monitoring import (
    _configuration,
    _threshold_status,
    update_monitoring_configuration,
)
from fastapi import HTTPException


def _deployment() -> ModelRun:
    now = datetime.now(UTC)
    return ModelRun(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        run_kind=RunKind.DEPLOYMENT,
        status=RunStatus.SUCCEEDED,
        task_type=TaskType.CLASSIFICATION,
        run_name="Deploy fraud model",
        gpu_requested=False,
        params={},
        tags={"environment": "production"},
        created_at=now,
        updated_at=now,
    )


def test_monitoring_routes_are_deployment_anchored() -> None:
    paths = {route.path for route in router.routes}

    assert "/monitoring/dashboard" in paths
    assert (
        "/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/metrics"
        in paths
    )
    assert (
        "/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports"
        in paths
    )


def test_new_deployment_monitoring_defaults_are_safe() -> None:
    configuration = _configuration(_deployment())

    assert configuration.enabled is False
    assert configuration.schedule == "manual"
    assert configuration.approval_required is True
    assert configuration.retraining_enabled is False
    assert configuration.thresholds["drift_share"].direction == "above"


def test_threshold_status_detects_degradation_in_both_directions() -> None:
    configuration = MonitoringConfigurationRead.model_validate(
        {
            **monitoring_service.DEFAULT_MONITORING,
            "thresholds": {
                "accuracy": {
                    "warning": 0.85,
                    "critical": 0.75,
                    "direction": "below",
                },
                "error_rate": {
                    "warning": 0.02,
                    "critical": 0.05,
                    "direction": "above",
                },
            },
        }
    )

    assert _threshold_status("accuracy", 0.70, configuration) == "critical"
    assert _threshold_status("accuracy", 0.80, configuration) == "warning"
    assert _threshold_status("error_rate", 0.01, configuration) == "healthy"
    assert _threshold_status("error_rate", 0.06, configuration) == "critical"


def test_monitoring_configuration_is_revisioned_and_deployment_scoped(
    monkeypatch,
) -> None:
    run = _deployment()
    user = SimpleNamespace(id=uuid.uuid4())
    db = SimpleNamespace(flush=lambda: None)
    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: run)

    result = update_monitoring_configuration(
        db,
        user,
        run.project_id,
        run.id,
        MonitoringConfigurationUpdate(
            enabled=True,
            schedule="hourly",
            resource_class="large",
            retraining_enabled=True,
        ),
    )

    assert result.revision == 1
    assert result.resource_class == "large"
    assert run.params["monitoring"]["updated_by_id"] == str(user.id)
    assert run.params["monitoring"]["approval_required"] is True


def test_monitoring_dashboard_excludes_cancelled_deployments(monkeypatch) -> None:
    project_id = uuid.uuid4()
    cancelled = _deployment()
    cancelled.project_id = project_id
    cancelled.status = RunStatus.CANCELLED
    project = SimpleNamespace(id=project_id, name="Governance")

    class DB:
        def get(self, *_):
            return project

        def scalars(self, _):
            return SimpleNamespace(all=lambda: [cancelled])

        def execute(self, _):
            return SimpleNamespace(all=lambda: [])

    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)

    result = monitoring_service.monitoring_dashboard(
        DB(),
        SimpleNamespace(id=uuid.uuid4()),
        project_id,
    )

    assert result.deployment_count == 0
    assert result.deployments == []


def test_governance_snapshot_reuses_leaderboard_audit_report(monkeypatch) -> None:
    now = datetime.now(UTC)
    project_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    entry_id = uuid.uuid4()
    source_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    deployment = SimpleNamespace(id=deployment_id)
    entry = SimpleNamespace(
        id=entry_id,
        model_name="Canonical model",
        model_run=SimpleNamespace(id=source_id),
    )
    canonical = {"schema_version": "2.0", "document": {"title": "Canonical audit"}}
    stored: dict[str, bytes] = {}
    db = SimpleNamespace(add=lambda _: None, flush=lambda: None)

    class Store:
        def put_bytes(self, path, content):
            stored[path] = content
            return SimpleNamespace(uri=f"s3://audit/{path}")

    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: deployment)
    monkeypatch.setattr(monitoring_service, "deployment_registry_entry", lambda *_: entry)
    monkeypatch.setattr(monitoring_service, "_governance_artifacts", lambda *_: [])
    monkeypatch.setattr(
        monitoring_service,
        "model_audit_report",
        lambda *_: (canonical, "evidence-hash"),
    )
    monkeypatch.setattr(monitoring_service, "_audit_html", lambda report: "canonical html")
    monkeypatch.setattr(monitoring_service, "get_object_store", Store)
    monkeypatch.setattr(
        monitoring_service,
        "_report_summary",
        lambda _: SimpleNamespace(
            model_dump=lambda: {
                "id": uuid.uuid4(),
                "project_id": project_id,
                "deployment_run_id": deployment_id,
                "model_version_id": entry_id,
                "version": 1,
                "generated_at": now,
                "evidence_cutoff_at": now,
                "generated_by_id": user.id,
                "content_hash": "content-hash",
                "json_download_url": "/audit.json",
                "html_download_url": "/audit.html",
            }
        ),
    )

    result = monitoring_service.generate_governance_report(
        db,
        user,
        project_id,
        deployment_id,
    )

    assert result.report == canonical
    assert any(content == b"canonical html" for content in stored.values())
    assert any(b'"schema_version": "2.0"' in content for content in stored.values())


def test_deployment_lookup_and_registry_identity_are_fail_closed() -> None:
    project_id = uuid.uuid4()
    deployment_id = uuid.uuid4()
    missing_db = SimpleNamespace(scalar=lambda _: None)

    with pytest.raises(HTTPException, match="Deployment not found"):
        monitoring_service.deployment_run(missing_db, project_id, deployment_id)

    run = SimpleNamespace(project_id=project_id, tags={}, params={})
    assert monitoring_service.deployment_registry_entry(missing_db, run) is None
    run.tags = {"registry_entry_id": "not-a-uuid"}
    assert monitoring_service.deployment_registry_entry(missing_db, run) is None

    entry = SimpleNamespace(id=uuid.uuid4())
    found_db = SimpleNamespace(scalar=lambda _: entry)
    run.tags = {"registry_entry_id": str(entry.id)}
    assert monitoring_service.deployment_registry_entry(found_db, run) is entry


def _metric(
    name: str,
    value: float,
    *,
    step: int = 0,
    status: str = "healthy",
) -> Metric:
    now = datetime.now(UTC)
    return Metric(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        model_run_id=uuid.uuid4(),
        name=name,
        kind=MetricKind.PERFORMANCE,
        split=MetricSplit.PRODUCTION,
        value=value,
        value_json={"sample_count": 10, "status": status, "source": "test"},
        higher_is_better=False,
        step=step,
        recorded_at=now,
        created_at=now,
        updated_at=now,
    )


def test_monitoring_metric_recording_is_idempotent_and_sequences_points(
    monkeypatch,
) -> None:
    deployment = _deployment()
    user = SimpleNamespace(id=uuid.uuid4())
    duplicate = _metric("error_rate", 0.01, step=2)
    duplicate.value_json["idempotency_key"] = "same"
    added = []

    class DB:
        def scalars(self, _):
            return SimpleNamespace(all=lambda: [duplicate])

        def add(self, value):
            value.id = uuid.uuid4()
            added.append(value)

        def flush(self):
            pass

    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: deployment)
    payload = MonitoringMetricPointCreate(
        name="error_rate",
        value=0.02,
        sample_count=20,
        status="warning",
        idempotency_key="same",
    )

    reused = monitoring_service.record_monitoring_metric(
        DB(), user, deployment.project_id, deployment.id, payload
    )
    assert reused.id == duplicate.id
    assert added == []

    payload.idempotency_key = "new"
    created = monitoring_service.record_monitoring_metric(
        DB(), user, deployment.project_id, deployment.id, payload
    )
    assert created.status == "warning"
    assert created.sample_count == 20
    assert created.metadata["idempotency_key"] == "new"
    assert added[0].step == 3


def test_monitoring_series_and_threshold_edge_cases() -> None:
    first = _metric("latency", 5.0, step=0)
    second = _metric("latency", 6.0, step=1)
    other = _metric("accuracy", 0.9)
    series = monitoring_service._metric_series([first, second, other])

    assert [item.name for item in series] == ["accuracy", "latency"]
    assert len(series[1].points) == 2
    configuration = _configuration(_deployment())
    assert _threshold_status("error_rate", None, configuration) == "unknown"
    assert _threshold_status("custom", 1.0, configuration) == "unknown"
    assert _threshold_status("custom", 1.0, configuration, "healthy") == "healthy"
    assert _threshold_status("custom", 1.0, configuration, "warning") == "warning"


def test_linked_drift_and_retraining_runs_produce_timeline_points() -> None:
    deployment = _deployment()
    now = datetime.now(UTC)
    drift = SimpleNamespace(
        id=uuid.uuid4(),
        run_kind=RunKind.DRIFT,
        status=RunStatus.SUCCEEDED,
        params={"max_rows": 50},
        tags={
            "deployment_run_id": str(deployment.id),
            "diagnostics": {
                "drift_share_percent": 40,
                "drifted_feature_count": 1,
                "drifted_features": ["age"],
            },
        },
        created_at=now,
        finished_at=now,
    )
    retraining = SimpleNamespace(
        id=uuid.uuid4(),
        run_kind=RunKind.TRAINING,
        params={"deployment_run_id": str(deployment.id)},
        tags={},
    )
    unrelated = SimpleNamespace(id=uuid.uuid4(), run_kind=RunKind.DRIFT, params={}, tags={})

    drift_runs, retraining_runs = monitoring_service._linked_runs(
        [drift, retraining, unrelated], deployment, None
    )
    assert drift_runs == [drift]
    assert retraining_runs == [retraining]
    points = monitoring_service._drift_points(drift_runs, _configuration(deployment))
    assert points[0].value == 0.4
    assert points[0].status == "critical"
    assert points[0].metadata["drifted_features"] == ["age"]


def test_portfolio_dashboard_returns_empty_without_visible_projects(monkeypatch) -> None:
    monkeypatch.setattr(monitoring_service, "list_visible_projects", lambda *_: [])

    dashboard = monitoring_service.monitoring_dashboard(
        SimpleNamespace(), SimpleNamespace(id=uuid.uuid4())
    )

    assert dashboard.scope == "portfolio"
    assert dashboard.deployment_count == 0
    assert dashboard.open_alert_count == 0


def test_monitoring_dashboard_combines_metrics_drift_and_retraining(monkeypatch) -> None:
    deployment = _deployment()
    project = SimpleNamespace(id=deployment.project_id, name="Risk")
    deployment.params = {
        "monitoring": {
            **monitoring_service.DEFAULT_MONITORING,
            "enabled": True,
        }
    }
    now = datetime.now(UTC)
    drift = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=deployment.project_id,
        run_kind=RunKind.DRIFT,
        status=RunStatus.SUCCEEDED,
        run_name="Drift",
        params={"deployment_run_id": str(deployment.id), "max_rows": 100},
        tags={"diagnostics": {"drift_share_percent": 10}},
        created_at=now,
        finished_at=now,
    )
    retraining = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=deployment.project_id,
        run_kind=RunKind.TRAINING,
        status=RunStatus.RUNNING,
        run_name="Retrain",
        params={"deployment_run_id": str(deployment.id)},
        tags={},
        created_at=now,
        finished_at=None,
    )
    metric = _metric("error_rate", 0.03, status="unknown")
    metric.project_id = deployment.project_id
    metric.model_run_id = deployment.id
    entry = SimpleNamespace(
        id=uuid.uuid4(),
        model_name="Fraud model",
        version=2,
        champion_metric_name="accuracy",
        champion_metric_value=0.92,
    )

    class DB:
        scalar_calls = 0

        def get(self, *_):
            return project

        def scalars(self, _):
            self.scalar_calls += 1
            values = [deployment, drift, retraining] if self.scalar_calls == 1 else [metric]
            return SimpleNamespace(all=lambda: values)

        def execute(self, _):
            return SimpleNamespace(all=lambda: [(deployment.id, 2)])

    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_registry_entry", lambda *_: entry)

    dashboard = monitoring_service.monitoring_dashboard(
        DB(), SimpleNamespace(id=uuid.uuid4()), deployment.project_id
    )

    item = dashboard.deployments[0]
    assert item.health_status == "warning"
    assert item.open_alerts == 1
    assert item.retraining_events == 1
    assert item.governance_reports == 2
    assert {event.kind for event in item.timeline} == {
        "deployment",
        "drift",
        "retraining",
    }


def _governance_artifact(*, html: bool = True) -> RunArtifact:
    now = datetime.now(UTC)
    return RunArtifact(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        model_run_id=uuid.uuid4(),
        kind=ArtifactKind.GOVERNANCE_REPORT,
        name="report.json",
        object_uri="s3://reports/report.json",
        content_hash="abc",
        byte_size=2,
        artifact_metadata={
            "version": 3,
            "evidence_cutoff_at": now.isoformat(),
            **({"html_uri": "s3://reports/report.html"} if html else {}),
        },
        created_at=now,
        updated_at=now,
    )


def test_governance_report_downloads_and_missing_html(monkeypatch) -> None:
    artifact = _governance_artifact()
    store = SimpleNamespace(read_bytes=lambda uri: uri.encode())
    db = SimpleNamespace(scalar=lambda _: artifact)
    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "get_object_store", lambda: store)

    content, content_type, filename = monitoring_service.governance_report_download(
        db,
        SimpleNamespace(),
        artifact.project_id,
        artifact.model_run_id,
        artifact.id,
        "html",
    )
    assert content == b"s3://reports/report.html"
    assert content_type.startswith("text/html")
    assert filename == "governance-report-v3.html"

    content, content_type, filename = monitoring_service.governance_report_download(
        db,
        SimpleNamespace(),
        artifact.project_id,
        artifact.model_run_id,
        artifact.id,
        "json",
    )
    assert content == b"s3://reports/report.json"
    assert content_type == "application/json"
    assert filename == "governance-report-v3.json"

    artifact.artifact_metadata.pop("html_uri")
    with pytest.raises(HTTPException, match="HTML report not found"):
        monitoring_service.governance_report_download(
            db,
            SimpleNamespace(),
            artifact.project_id,
            artifact.model_run_id,
            artifact.id,
            "html",
        )


def test_governance_artifact_lookup_and_unlinked_generation_fail_closed(
    monkeypatch,
) -> None:
    ids = [uuid.uuid4() for _ in range(3)]
    with pytest.raises(HTTPException, match="Governance report not found"):
        monitoring_service._governance_report_artifact(SimpleNamespace(scalar=lambda _: None), *ids)

    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: SimpleNamespace(id=ids[1]))
    monkeypatch.setattr(monitoring_service, "deployment_registry_entry", lambda *_: None)
    with pytest.raises(HTTPException, match="not linked"):
        monitoring_service.generate_governance_report(
            SimpleNamespace(), SimpleNamespace(), ids[0], ids[1]
        )


def test_monitoring_read_helpers_return_stored_configuration_and_metrics(
    monkeypatch,
) -> None:
    deployment = _deployment()
    deployment.params = {"monitoring": {**monitoring_service.DEFAULT_MONITORING, "enabled": True}}
    metric = _metric("throughput", 12.0)
    db = SimpleNamespace(
        scalars=lambda _: SimpleNamespace(all=lambda: [metric]),
    )
    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: deployment)

    configuration = monitoring_service.get_monitoring_configuration(
        db, SimpleNamespace(), deployment.project_id, deployment.id
    )
    series = monitoring_service.list_monitoring_metrics(
        db, SimpleNamespace(), deployment.project_id, deployment.id
    )

    assert configuration.enabled is True
    assert series[0].name == "throughput"


def test_governance_report_read_and_listing(monkeypatch) -> None:
    artifact = _governance_artifact()
    report = {"schema_version": "2.0"}
    store = SimpleNamespace(read_bytes=lambda _: b'{"schema_version": "2.0"}')
    db = SimpleNamespace(
        scalar=lambda _: artifact,
        scalars=lambda _: SimpleNamespace(all=lambda: [artifact]),
    )
    monkeypatch.setattr(monitoring_service, "require_project_role", lambda *_: None)
    monkeypatch.setattr(monitoring_service, "deployment_run", lambda *_: SimpleNamespace())
    monkeypatch.setattr(monitoring_service, "get_object_store", lambda: store)

    summaries = monitoring_service.list_governance_reports(
        db,
        SimpleNamespace(),
        artifact.project_id,
        artifact.model_run_id,
    )
    loaded = monitoring_service.get_governance_report(
        db,
        SimpleNamespace(),
        artifact.project_id,
        artifact.model_run_id,
        artifact.id,
    )

    assert summaries[0].version == 3
    assert summaries[0].html_download_url.endswith("?format=html")
    assert loaded.report == report
