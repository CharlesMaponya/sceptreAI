"""Refit workers receive capabilities, never application or provider credentials."""

from __future__ import annotations

import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.db.session import get_db
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowEvent
from automl_api.services.attempt_output_grants import require_output_completion
from automl_api.services.refit_access import locked_refit, refit_claims
from automl_api.services.workflow_state import StaleFence, transition_attempt
from automl_api.storage.object_store import get_object_store
from automl_api.training.champion_refit import (
    FrozenPipeline,
    RefitObject,
    _copy_verified,
    publish_frozen_pipeline,
    start_refit_attempt,
    validate_refit_paths,
)

router = APIRouter(prefix="/internal/refits", include_in_schema=False)


def principal(attempt_id: uuid.UUID, authorization: Annotated[str, Header()] = ""):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise ValueError("Bearer required")
        return refit_claims(token, attempt_id)
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(401, "Invalid refit capability") from exc


DB = Annotated[Session, Depends(get_db)]
Principal = Annotated[dict, Depends(principal)]


def active(db, attempt_id, claims, statuses=(AttemptStatus.RUNNING,)):
    try:
        return locked_refit(db, attempt_id, claims, statuses)
    except (StaleFence, ValueError) as exc:
        raise HTTPException(409, "Refit attempt is stale or its plan changed") from exc


@router.post("/{attempt_id}/start")
def start(attempt_id: uuid.UUID, claims: Principal, db: DB, response: Response):
    with db.begin():
        _, _, plan = active(db, attempt_id, claims, (AttemptStatus.SUBMITTED,))
        validate_refit_paths(plan, get_object_store())
        start_refit_attempt(
            db, project_id=plan.project_id, attempt_id=attempt_id, fencing_token=claims["fence"]
        )
    response.headers["Cache-Control"] = "no-store"
    return plan.model_dump(mode="json")


@router.get("/{attempt_id}/inputs/{index}")
def input_grant(attempt_id: uuid.UUID, index: int, claims: Principal, db: DB, response: Response):
    raise HTTPException(
        410,
        detail={
            "code": "pod_bound_grants_required",
            "message": "Use pod-bound attempt object grants.",
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/{attempt_id}/heartbeat")
def heartbeat(attempt_id: uuid.UUID, claims: Principal, db: DB):
    with db.begin():
        scope, attempt, _ = active(db, attempt_id, claims)
        attempt.heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = min(
            scope.scope_deadline_at,
            attempt.heartbeat_at + timedelta(seconds=90),
        )
    return {"renewed": True}


@router.put("/{attempt_id}/model/{digest}")
async def upload_model(
    attempt_id: uuid.UUID, digest: str, request: Request, claims: Principal, db: DB
):
    raise HTTPException(
        410,
        detail={
            "code": "pod_bound_grants_required",
            "message": "Use pod-bound attempt object uploads.",
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/{attempt_id}/publish")
def publish(attempt_id: uuid.UUID, result: FrozenPipeline, claims: Principal, db: DB):
    with db.begin():
        _, attempt, plan = active(
            db,
            attempt_id,
            claims,
            (AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED),
        )
        replay = attempt.status == AttemptStatus.SUCCEEDED
        if not replay:
            intent = db.scalar(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id == attempt_id,
                    WorkflowEvent.event_key == "refit_output",
                )
            )
            expected = {
                "uri": result.frozen_pipeline_uri,
                "sha256": result.frozen_pipeline_digest,
                "byte_size": result.byte_size,
            }
            if intent is None or intent.payload != expected:
                raise HTTPException(409, "Pipeline does not match the declared output")
    if not replay:
        try:
            with tempfile.TemporaryFile() as verified:
                _copy_verified(get_object_store(), RefitObject(**expected), verified, lambda: None)
        except Exception as exc:
            raise HTTPException(409, "Frozen pipeline bytes could not be verified") from exc
    with db.begin():
        active(db, attempt_id, claims, (AttemptStatus.RUNNING, AttemptStatus.SUCCEEDED))
        try:
            if not replay:
                require_output_completion(db, plan, claims["fence"], expected)
            checkpoint = publish_frozen_pipeline(db, plan, result, fencing_token=claims["fence"])
        except ValueError as exc:
            raise HTTPException(409, "Frozen pipeline publication rejected") from exc
        return {"checkpoint_id": str(checkpoint.id), "digest": checkpoint.content_digest}


@router.post("/{attempt_id}/fail")
def fail(attempt_id: uuid.UUID, claims: Principal, db: DB):
    with db.begin():
        _, attempt, _ = active(
            db,
            attempt_id,
            claims,
            (
                AttemptStatus.SUBMITTED,
                AttemptStatus.RUNNING,
                AttemptStatus.FAILED,
            ),
        )
        if attempt.status != AttemptStatus.FAILED:
            transition_attempt(
                attempt,
                AttemptStatus.FAILED,
                fencing_token=claims["fence"],
                terminal_reason="Refit worker reported execution failure",
            )
    return {"failed": True}
