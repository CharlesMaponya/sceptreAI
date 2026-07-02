from __future__ import annotations

import json
import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.db.session import get_db, get_session_factory
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import DatasetStatus
from automl_api.models.iam import User
from automl_api.schemas.profiling import DatasetProfileRead, ProfileRequest
from automl_api.schemas.profiling_jobs import (
    FeatureProfileJobRead,
    ProfilingJobCreate,
    ProfilingJobRead,
    ProfilingJobStatusRead,
)
from automl_api.services.profiling import build_dataset_profile
from automl_api.services.profiling_jobs import (
    TERMINAL_STATUSES,
    create_profiling_job,
    get_profiling_job,
    latest_profiling_job,
    schedule_profiling_job,
)

router = APIRouter(
    prefix="/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}",
    tags=["profiling"],
)


@router.post(
    "/profile-jobs",
    response_model=ProfilingJobStatusRead,
    status_code=status.HTTP_202_ACCEPTED,
)
def start_profile_job(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    payload: ProfilingJobCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> ProfilingJobStatusRead:
    job, created = create_profiling_job(
        db,
        current_user,
        project_id,
        dataset_id,
        dataset_version_id,
        payload,
    )
    db.commit()
    db.refresh(job)
    if created:
        schedule_profiling_job(job.id)
    else:
        response.status_code = status.HTTP_200_OK
    return ProfilingJobStatusRead.model_validate(job)


@router.get("/profile-jobs/latest", response_model=ProfilingJobStatusRead | None)
def latest_profile_job(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProfilingJobStatusRead | None:
    job = latest_profiling_job(
        db,
        current_user,
        project_id,
        dataset_version_id,
    )
    if job:
        _validate_job_scope(job, dataset_id, dataset_version_id)
    return ProfilingJobStatusRead.model_validate(job) if job else None


@router.get("/profile-jobs/{job_id}", response_model=ProfilingJobStatusRead)
def profile_job_status(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProfilingJobStatusRead:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    return ProfilingJobStatusRead.model_validate(job)


@router.get("/profile-jobs/{job_id}/result", response_model=ProfilingJobRead)
def profile_job_result(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> ProfilingJobRead:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    if job.status not in TERMINAL_STATUSES:
        response.status_code = status.HTTP_202_ACCEPTED
    return ProfilingJobRead.model_validate(job)


@router.get(
    "/profile-jobs/{job_id}/feature",
    response_model=FeatureProfileJobRead,
)
def profile_job_feature_query(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    column: str,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> FeatureProfileJobRead:
    return profile_job_feature(
        project_id,
        dataset_id,
        dataset_version_id,
        job_id,
        column,
        db,
        current_user,
        response,
    )


@router.get(
    "/profile-jobs/{job_id}/features/{column}",
    response_model=FeatureProfileJobRead,
)
def profile_job_feature(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    column: str,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> FeatureProfileJobRead:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    profile = job.feature_profiles_json.get(column)
    if profile is None and job.status not in TERMINAL_STATUSES:
        response.status_code = status.HTTP_202_ACCEPTED
        feature_status = "pending"
    elif profile is None:
        feature_status = "unavailable"
    else:
        feature_status = "completed"
    return FeatureProfileJobRead(
        job_id=job.id,
        column=column,
        status=feature_status,
        profile=profile,
    )


@router.get("/profile-jobs/{job_id}/relationships")
def profile_job_relationships(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> dict[str, object]:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    stage_status = job.overview_json.get("stages", {}).get("relationships", "queued")
    if stage_status not in {"completed", "failed"}:
        response.status_code = status.HTTP_202_ACCEPTED
    return {"status": stage_status, "relationships": job.relationships_json}


@router.get("/profile-jobs/{job_id}/preparation")
def profile_job_preparation(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    response: Response,
) -> dict[str, object]:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    stage_status = job.overview_json.get("stages", {}).get("preparation", "queued")
    if stage_status not in {"completed", "failed"}:
        response.status_code = status.HTTP_202_ACCEPTED
    return {"status": stage_status, "preparation": job.preparation_json}


@router.post("/profile-jobs/{job_id}/cancel", response_model=ProfilingJobStatusRead)
def cancel_profile_job(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> ProfilingJobStatusRead:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    if job.status not in TERMINAL_STATUSES:
        job.status = "cancelled"
        job.current_stage = "cancelled"
        version = db.get(DatasetVersion, job.dataset_version_id)
        if version is not None:
            version.status = DatasetStatus.UPLOADED
        db.commit()
        db.refresh(job)
    return ProfilingJobStatusRead.model_validate(job)


@router.get("/profile-jobs/{job_id}/events")
def profile_job_events(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    job_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> StreamingResponse:
    job = get_profiling_job(db, current_user, project_id, job_id)
    _validate_job_scope(job, dataset_id, dataset_version_id)
    session_factory = get_session_factory()

    def event_stream():
        while True:
            with session_factory() as event_db:
                current_job = event_db.get(type(job), job_id)
                if current_job is None:
                    yield 'event: error\ndata: {"detail":"Job not found"}\n\n'
                    return
                payload = ProfilingJobStatusRead.model_validate(current_job).model_dump(mode="json")
                yield f"event: progress\ndata: {json.dumps(payload)}\n\n"
                if current_job.status in TERMINAL_STATUSES:
                    return
            time.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/profile", response_model=DatasetProfileRead)
def profile_dataset_version(
    project_id: uuid.UUID,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
    payload: ProfileRequest,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> DatasetProfileRead:
    return build_dataset_profile(
        db,
        current_user,
        project_id,
        dataset_id,
        dataset_version_id,
        payload,
    )


def _validate_job_scope(
    job: ProfilingJob,
    dataset_id: uuid.UUID,
    dataset_version_id: uuid.UUID,
) -> None:
    if job.dataset_id != dataset_id or job.dataset_version_id != dataset_version_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Profiling job not found for this dataset version.",
        )
