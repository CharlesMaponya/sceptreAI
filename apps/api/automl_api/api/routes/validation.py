from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.schemas.training import ModelRunRead
from automl_api.schemas.validation import (
    AnalysisLaunchRead,
    AnalysisResultRead,
    ExplainabilityLaunchRequest,
    ValidationLaunchRequest,
)
from automl_api.services.validation import (
    get_analysis_result,
    launch_explainability_run,
    launch_validation_run,
    list_analysis_runs,
)

router = APIRouter(
    prefix="/projects/{project_id}/training/runs/{training_run_id}",
    tags=["validation"],
)


@router.post(
    "/validations",
    response_model=AnalysisLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def validate_model(
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    payload: ValidationLaunchRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> AnalysisLaunchRead:
    result = launch_validation_run(
        db,
        current_user,
        project_id,
        training_run_id,
        payload,
    )
    db.commit()
    return result


@router.post(
    "/explanations",
    response_model=AnalysisLaunchRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def explain_model(
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    payload: ExplainabilityLaunchRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> AnalysisLaunchRead:
    result = launch_explainability_run(
        db,
        current_user,
        project_id,
        training_run_id,
        payload,
    )
    db.commit()
    return result


@router.get("/analyses", response_model=list[ModelRunRead])
def analyses(
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> list[ModelRunRead]:
    return [
        ModelRunRead.model_validate(run)
        for run in list_analysis_runs(
            db,
            current_user,
            project_id,
            training_run_id,
        )
    ]


@router.get(
    "/analyses/{analysis_run_id}",
    response_model=AnalysisResultRead,
)
def analysis_result(
    project_id: uuid.UUID,
    training_run_id: uuid.UUID,
    analysis_run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> AnalysisResultRead:
    return get_analysis_result(
        db,
        current_user,
        project_id,
        training_run_id,
        analysis_run_id,
    )
