from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import automl_api.services.monitoring as monitoring_service
from automl_api.api.routes.monitoring import router
from automl_api.models.enums import RunKind, RunStatus, TaskType
from automl_api.models.runs import ModelRun
from automl_api.schemas.monitoring import (
    MonitoringConfigurationRead,
    MonitoringConfigurationUpdate,
)
from automl_api.services.monitoring import (
    _configuration,
    _governance_html,
    _threshold_status,
    update_monitoring_configuration,
)


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


def test_governance_html_escapes_evidence_values_and_includes_hash() -> None:
    rendered = _governance_html(
        {"schema_version": "1.0", "model_development": {"name": "<unsafe>"}},
        "abc123",
    )

    assert "&lt;unsafe&gt;" in rendered
    assert "<unsafe>" not in rendered
    assert "abc123" in rendered
