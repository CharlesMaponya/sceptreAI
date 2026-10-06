"""Fenced evaluator control and bounded signed-result publication."""

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from automl_api.db.session import get_db, get_session_factory
from automl_api.models.enums import AttemptStatus
from automl_api.services.evaluation_access import evaluation_claims, locked_evaluation
from automl_api.services.evaluation_publication import (
    _append,
    _event,
    _read_result,
    persist_evaluation_result,
    recover_evaluation_result,
)
from automl_api.services.refit_access import mint_refit_read_url
from automl_api.services.workflow_state import transition_attempt
from automl_api.storage.object_store import get_object_store

router = APIRouter(prefix="/internal/evaluations", include_in_schema=False)


def authority_public_key():
    return Path(os.environ["EVALUATION_AUTHORITY_PUBLIC_KEY_FILE"]).read_text()


def principal(attempt_id: uuid.UUID, authorization: Annotated[str, Header()] = ""):
    try:
        scheme, token = authorization.split(" ", 1)
        if scheme.lower() != "bearer":
            raise ValueError("Bearer required")
        return evaluation_claims(token, attempt_id)
    except (ValueError, KeyError, TypeError) as error:
        raise HTTPException(401, "Invalid evaluator capability") from error


DB = Annotated[Session, Depends(get_db)]
Principal = Annotated[dict, Depends(principal)]


def active(db, attempt_id, claims, statuses=(AttemptStatus.RUNNING,)):
    try:
        return locked_evaluation(db, attempt_id, claims, statuses)
    except (ValueError, KeyError) as error:
        raise HTTPException(409, "Evaluator attempt is stale or its plan changed") from error


@router.post("/{attempt_id}/start")
def start(attempt_id: uuid.UUID, claims: Principal, db: DB, response: Response):
    with db.begin():
        scope, attempt, plan = active(db, attempt_id, claims, (AttemptStatus.SUBMITTED,))
        transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token=claims["fence"])
        attempt.heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = min(
            scope.scope_deadline_at, attempt.heartbeat_at + timedelta(seconds=90)
        )
    response.headers["Cache-Control"] = "no-store"
    return plan.model_dump(mode="json")


@router.post("/{attempt_id}/heartbeat")
def heartbeat(attempt_id: uuid.UUID, claims: Principal, db: DB):
    with db.begin():
        scope, attempt, _ = active(db, attempt_id, claims)
        attempt.heartbeat_at = datetime.now(UTC)
        attempt.lease_expires_at = min(
            scope.scope_deadline_at, attempt.heartbeat_at + timedelta(seconds=90)
        )
    return {"renewed": True}


@router.get("/{attempt_id}/pipeline")
def pipeline(attempt_id: uuid.UUID, claims: Principal, db: DB, response: Response):
    with db.begin():
        scope, attempt, plan = active(db, attempt_id, claims)
        try:
            grant = mint_refit_read_url(
                get_object_store(),
                plan.frozen_pipeline,
                min(scope.scope_deadline_at, attempt.lease_expires_at),
            )
        except Exception as error:
            raise HTTPException(503, "Frozen pipeline capability unavailable") from error
    response.headers["Cache-Control"] = "no-store"
    return grant


@router.put("/{attempt_id}/result")
async def result(attempt_id: uuid.UUID, claims: Principal, db: DB, request: Request):
    with db.begin():
        _, _, plan = active(db, attempt_id, claims)
    payload = bytearray()
    async for chunk in request.stream():
        if len(payload) + len(chunk) > 65536:
            raise HTTPException(413, "Evaluator result exceeds its byte budget")
        payload.extend(chunk)

    def persist():
        store = get_object_store()
        intent = persist_evaluation_result(
            get_session_factory(), plan, claims["fence"], bytes(payload), store
        )
        _read_result(store, intent, plan)
        return intent

    try:
        return await run_in_threadpool(persist)
    except ValueError as error:
        raise HTTPException(409, "Evaluator signed result was rejected") from error
    except Exception as error:
        raise HTTPException(503, "Evaluator result storage unavailable") from error


@router.post("/{attempt_id}/publish")
def publish(attempt_id: uuid.UUID, receipt: dict, claims: Principal, db: DB):
    # Terminal replay uses stored plan/fence verification in recovery, without new reads/grants.
    with db.begin():
        event = _event(db, attempt_id, "evaluation_plan")
        if event is None:
            raise HTTPException(409, "Evaluator plan is missing")
        from automl_api.training.champion_evaluation import EvaluationPlan

        plan = EvaluationPlan.model_validate(event.payload)
        if plan.project_id != claims["project_id"]:
            raise HTTPException(403, "Evaluator project differs")
    try:
        return recover_evaluation_result(
            get_session_factory(),
            plan,
            claims["fence"],
            get_object_store(),
            commit=lambda _digest: receipt,
            authority_public_key=authority_public_key(),
        )
    except ValueError as error:
        raise HTTPException(409, "Evaluator publication was rejected") from error
    except Exception as error:
        raise HTTPException(503, "Evaluator publication unavailable") from error


@router.post("/{attempt_id}/fail")
def failed(attempt_id: uuid.UUID, claims: Principal, db: DB):
    with db.begin():
        _, attempt, _ = active(db, attempt_id, claims)
        if _event(db, attempt.id, "evaluation_worker_failed") is None:
            _append(db, attempt, "evaluation_worker_failed", {"reason": "Evaluator worker failed"})
    # The reconciler must inspect stored output/authority state before terminalizing.
    return {"recorded": True}
