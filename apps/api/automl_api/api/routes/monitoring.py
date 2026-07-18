from __future__ import annotations

import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.monitoring import (
    GovernanceReportRead,
    GovernanceReportSummaryRead,
    MonitoringConfigurationRead,
    MonitoringConfigurationUpdate,
    MonitoringDashboardRead,
    MonitoringMetricPointCreate,
    MonitoringMetricPointRead,
    MonitoringMetricSeriesRead,
)
from automl_api.services.monitoring import (
    generate_governance_report,
    get_governance_report,
    get_monitoring_configuration,
    governance_report_download,
    list_governance_reports,
    list_monitoring_metrics,
    monitoring_dashboard,
    record_monitoring_metric,
    update_monitoring_configuration,
)

router = APIRouter(tags=["model monitoring and governance"])


@router.get("/monitoring/dashboard", response_model=MonitoringDashboardRead)
def portfolio_dashboard(
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> MonitoringDashboardRead:
    return monitoring_dashboard(db, current_user)


@router.get(
    "/projects/{project_id}/operations/monitoring/dashboard",
    response_model=MonitoringDashboardRead,
)
def project_dashboard(
    project_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> MonitoringDashboardRead:
    return monitoring_dashboard(db, current_user, project_id)


@router.get(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/config",
    response_model=MonitoringConfigurationRead,
)
def monitoring_configuration(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> MonitoringConfigurationRead:
    return get_monitoring_configuration(
        db,
        current_user,
        project_id,
        deployment_run_id,
    )


@router.put(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/config",
    response_model=MonitoringConfigurationRead,
)
def replace_monitoring_configuration(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    payload: MonitoringConfigurationUpdate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> MonitoringConfigurationRead:
    result = update_monitoring_configuration(
        db,
        current_user,
        project_id,
        deployment_run_id,
        payload,
    )
    db.commit()
    return result


@router.get(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/metrics",
    response_model=list[MonitoringMetricSeriesRead],
)
def deployment_metrics(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[MonitoringMetricSeriesRead]:
    return list_monitoring_metrics(
        db,
        current_user,
        project_id,
        deployment_run_id,
    )


@router.post(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/metrics",
    response_model=MonitoringMetricPointRead,
    status_code=status.HTTP_201_CREATED,
)
def create_deployment_metric(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    payload: MonitoringMetricPointCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> MonitoringMetricPointRead:
    result = record_monitoring_metric(
        db,
        current_user,
        project_id,
        deployment_run_id,
        payload,
    )
    db.commit()
    return result


@router.get(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports",
    response_model=list[GovernanceReportSummaryRead],
)
def governance_reports(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[GovernanceReportSummaryRead]:
    return list_governance_reports(
        db,
        current_user,
        project_id,
        deployment_run_id,
    )


@router.post(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports",
    response_model=GovernanceReportRead,
    status_code=status.HTTP_201_CREATED,
)
def create_governance_report(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> GovernanceReportRead:
    result = generate_governance_report(
        db,
        current_user,
        project_id,
        deployment_run_id,
    )
    db.commit()
    return result


@router.get(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports/{report_id}",
    response_model=GovernanceReportRead,
)
def governance_report(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    report_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> GovernanceReportRead:
    return get_governance_report(
        db,
        current_user,
        project_id,
        deployment_run_id,
        report_id,
    )


@router.get(
    "/projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports/{report_id}/download"
)
def download_governance_report(
    project_id: uuid.UUID,
    deployment_run_id: uuid.UUID,
    report_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    output_format: Annotated[Literal["json", "html"], Query(alias="format")] = "json",
) -> Response:
    content, media_type, filename = governance_report_download(
        db,
        current_user,
        project_id,
        deployment_run_id,
        report_id,
        output_format,
    )
    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
