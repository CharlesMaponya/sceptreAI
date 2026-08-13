from __future__ import annotations

import base64
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select, tuple_
from sqlalchemy.orm import Session

from automl_api.models.enums import ProjectRole, ScopeStatus
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureSearchSpaceRevision,
    PromotionalScope,
    PromotionalScopeMember,
    SearchObjectiveRevision,
    TrainingCandidate,
    TrainingTrial,
    WorkflowAttempt,
    WorkflowEvent,
)
from automl_api.schemas.contracts import (
    EvaluationScopeCreate,
    ExperimentSpecCreate,
    FeatureContractCreate,
    RevisionCreate,
    SearchSpaceCreate,
)
from automl_api.services.projects import require_project_role
from automl_api.services.workflow_state import (
    IdempotencyConflict,
    InvalidTransition,
    begin_command,
    cancel_promotional_scope,
    canonical_request_hash,
    seal_promotional_scope,
)

REVISION_MODELS = {
    "feature-contract": FeatureContractRevision,
    "feature-search-space": FeatureSearchSpaceRevision,
    "search-objective": SearchObjectiveRevision,
}


def begin_contract_mutation(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    operation: str,
    idempotency_key: str,
    payload: dict[str, Any],
):
    try:
        command, replayed = begin_command(
            db,
            project_id=project_id,
            actor_id=user.id,
            operation=operation,
            idempotency_key=idempotency_key,
            payload=payload,
        )
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "idempotency_key_conflict", "message": str(exc)},
        ) from exc
    return command, replayed


def create_revision(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    kind: str,
    payload: RevisionCreate,
):
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    model = REVISION_MODELS[kind]
    current = int(
        db.scalar(
            select(func.max(model.revision)).where(
                model.project_id == project_id, model.name == payload.name
            )
        )
        or 0
    )
    if payload.expected_revision is not None and payload.expected_revision != current:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "code": f"{kind.replace('-', '_')}_revision_changed",
                "requested_revision": payload.expected_revision,
                "current_revision": current,
            },
        )
    values: dict[str, Any] = {
        "project_id": project_id,
        "name": payload.name,
        "revision": current + 1,
        "digest_algorithm": "sha256",
        "digest_scope": f"{kind}-v1",
        "content_digest": canonical_request_hash(payload.model_dump(mode="json")),
        "specification": payload.specification,
    }
    if isinstance(payload, FeatureContractCreate):
        values.update(task_type=payload.task_type.value, target_column=payload.target_column)
    if isinstance(payload, SearchSpaceCreate):
        values.update(
            metric_name=payload.metric_name,
            metric_direction=payload.metric_direction,
        )
    revision = model(**values)
    db.add(revision)
    db.flush()
    return revision


def get_revision(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    kind: str,
    revision_id: uuid.UUID,
):
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    model = REVISION_MODELS[kind]
    revision = db.scalar(
        select(model).where(model.project_id == project_id, model.id == revision_id)
    )
    if revision is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Revision not found.")
    return revision


def create_experiment_spec(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: ExperimentSpecCreate,
) -> ExperimentSpecRevision:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    _require_project_references(
        db,
        project_id,
        {
            DatasetSplitRevision: payload.split_revision_id,
            FeatureContractRevision: payload.feature_contract_revision_id,
            FeatureSearchSpaceRevision: payload.feature_search_space_revision_id,
            SearchObjectiveRevision: payload.search_objective_revision_id,
            EstimatorCatalogRevision: payload.catalog_revision_id,
        },
    )
    current = int(
        db.scalar(
            select(func.max(ExperimentSpecRevision.revision)).where(
                ExperimentSpecRevision.project_id == project_id,
                ExperimentSpecRevision.name == payload.name,
            )
        )
        or 0
    )
    if payload.expected_revision is not None and payload.expected_revision != current:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "experiment_spec_revision_changed", "current_revision": current},
        )
    spec = ExperimentSpecRevision(
        project_id=project_id,
        name=payload.name,
        revision=current + 1,
        digest_algorithm="sha256",
        digest_scope="experiment-spec-v1",
        content_digest=canonical_request_hash(payload.model_dump(mode="json")),
        specification=payload.specification,
        dataset_version_id=payload.dataset_version_id,
        split_revision_id=payload.split_revision_id,
        feature_contract_revision_id=payload.feature_contract_revision_id,
        feature_search_space_revision_id=payload.feature_search_space_revision_id,
        search_objective_revision_id=payload.search_objective_revision_id,
        catalog_revision_id=payload.catalog_revision_id,
        task_type=payload.task_type.value,
        target_column=payload.target_column,
        primary_metric=payload.primary_metric,
    )
    db.add(spec)
    db.flush()
    return spec


def _require_project_references(
    db: Session, project_id: uuid.UUID, bindings: dict[type, uuid.UUID]
) -> None:
    for model, identifier in bindings.items():
        if db.scalar(
            select(model.id).where(model.project_id == project_id, model.id == identifier)
        ):
            continue
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "experiment_spec_mismatch", "resource": model.__tablename__},
        )


def create_scope(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    payload: EvaluationScopeCreate,
) -> PromotionalScope:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    _require_project_references(
        db,
        project_id,
        {
            DatasetSplitRevision: payload.split_revision_id,
            ExperimentSpecRevision: payload.experiment_spec_revision_id,
        },
    )
    if payload.membership_deadline_at <= datetime.now(UTC):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Membership deadline must be in the future.",
        )
    if payload.mode == "promotional" and not payload.final_threshold_revision:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Promotional scopes require a final-threshold revision.",
        )
    scope = PromotionalScope(
        project_id=project_id,
        scope_key=payload.scope_key,
        split_revision_id=payload.split_revision_id,
        experiment_spec_revision_id=payload.experiment_spec_revision_id,
        canonical_provider=payload.canonical_provider,
        mode=payload.mode,
        expected_members=payload.expected_member_count,
        membership_deadline_at=payload.membership_deadline_at,
        comparison_policy=payload.comparison_policy,
        final_threshold_revision=payload.final_threshold_revision,
    )
    db.add(scope)
    db.flush()
    return scope


def add_scope_member(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    scope_id: uuid.UUID,
    run_id: uuid.UUID,
) -> PromotionalScopeMember:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    scope = db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.project_id == project_id, PromotionalScope.id == scope_id)
        .with_for_update()
    )
    if scope is None:
        raise HTTPException(status_code=404, detail="Evaluation scope not found.")
    existing = db.scalar(
        select(PromotionalScopeMember).where(
            PromotionalScopeMember.scope_id == scope_id,
            PromotionalScopeMember.model_run_id == run_id,
        )
    )
    if existing is not None:
        return existing
    if scope.status != ScopeStatus.OPEN or (
        scope.membership_deadline_at and scope.membership_deadline_at <= datetime.now(UTC)
    ):
        raise HTTPException(status_code=409, detail={"code": "scope_membership_closed"})
    run = db.scalar(
        select(ModelRun).where(ModelRun.project_id == project_id, ModelRun.id == run_id)
    )
    if run is None:
        raise HTTPException(status_code=404, detail="Training run not found.")
    count = int(
        db.scalar(
            select(func.count())
            .select_from(PromotionalScopeMember)
            .where(PromotionalScopeMember.scope_id == scope_id)
        )
        or 0
    )
    if count >= scope.expected_members:
        raise HTTPException(status_code=409, detail={"code": "scope_membership_full"})
    member = PromotionalScopeMember(
        project_id=project_id,
        scope_id=scope_id,
        model_run_id=run_id,
        ordinal=count,
    )
    db.add(member)
    db.flush()
    if count + 1 == scope.expected_members:
        seal_promotional_scope(db, scope.id, expected_cas_version=scope.cas_version)
    return member


def cancel_scope(
    db: Session, user: User, project_id: uuid.UUID, scope_id: uuid.UUID
) -> PromotionalScope:
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    scope = db.scalar(
        select(PromotionalScope)
        .where(PromotionalScope.project_id == project_id, PromotionalScope.id == scope_id)
        .with_for_update()
    )
    if scope is None:
        raise HTTPException(status_code=404, detail="Evaluation scope not found.")
    try:
        cancel_promotional_scope(db, scope)
    except InvalidTransition as exc:
        raise HTTPException(status_code=409, detail={"code": "scope_terminal"}) from exc
    db.flush()
    return scope


def decode_cursor(cursor: str | None) -> tuple[str, str] | None:
    if cursor is None:
        return None
    try:
        values = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
        if not isinstance(values, list) or len(values) != 2:
            raise ValueError
        return str(values[0]), str(values[1])
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=422, detail={"code": "invalid_cursor"}) from exc


def encode_cursor(first: str, second: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(json.dumps([first, str(second)]).encode()).decode()


def run_collection(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    kind: str,
    *,
    cursor: str | None,
    limit: int,
    status_filter: str | None = None,
    resource_class: str | None = None,
) -> tuple[list[Any], str | None]:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    _require_run(db, project_id, run_id)
    decoded = decode_cursor(cursor)
    if kind == "candidates":
        query = select(TrainingCandidate).where(TrainingCandidate.model_run_id == run_id)
        if status_filter:
            query = query.where(TrainingCandidate.status == status_filter)
        if resource_class:
            query = query.where(TrainingCandidate.params["resource_class"].astext == resource_class)
        if decoded:
            query = query.where(
                tuple_(TrainingCandidate.estimator_key, TrainingCandidate.id)
                > (decoded[0], uuid.UUID(decoded[1]))
            )
        query = query.order_by(TrainingCandidate.estimator_key, TrainingCandidate.id)
        rows = list(db.scalars(query.limit(limit + 1)))

        def key(row):
            return row.estimator_key
    elif kind == "trials":
        query = (
            select(TrainingTrial)
            .join(TrainingCandidate, TrainingTrial.candidate_id == TrainingCandidate.id)
            .where(TrainingCandidate.model_run_id == run_id)
        )
        if decoded:
            query = query.where(
                tuple_(TrainingTrial.suggestion_id, TrainingTrial.id)
                > (decoded[0], uuid.UUID(decoded[1]))
            )
        query = query.order_by(TrainingTrial.suggestion_id, TrainingTrial.id)
        rows = list(db.scalars(query.limit(limit + 1)))

        def key(row):
            return row.suggestion_id
    else:
        query = (
            select(WorkflowEvent)
            .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
            .where(WorkflowAttempt.model_run_id == run_id)
        )
        if decoded:
            query = query.where(
                tuple_(WorkflowEvent.event_key, WorkflowEvent.id)
                > (decoded[0], uuid.UUID(decoded[1]))
            )
        query = query.order_by(WorkflowEvent.event_key, WorkflowEvent.id)
        rows = list(db.scalars(query.limit(limit + 1)))

        def key(row):
            return row.event_key

    more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = encode_cursor(key(rows[-1]), rows[-1].id) if more and rows else None
    return rows, next_cursor


def candidate_for_run(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> TrainingCandidate:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    candidate = db.scalar(
        select(TrainingCandidate).where(
            TrainingCandidate.project_id == project_id,
            TrainingCandidate.model_run_id == run_id,
            TrainingCandidate.id == candidate_id,
        )
    )
    if candidate is None:
        raise HTTPException(status_code=404, detail="Candidate not found.")
    return candidate


def event_cursor_after_id(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    event_id: uuid.UUID,
) -> str:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    event = db.scalar(
        select(WorkflowEvent)
        .join(WorkflowAttempt, WorkflowEvent.attempt_id == WorkflowAttempt.id)
        .where(
            WorkflowAttempt.project_id == project_id,
            WorkflowAttempt.model_run_id == run_id,
            WorkflowEvent.id == event_id,
        )
    )
    if event is None:
        raise HTTPException(status_code=422, detail={"code": "invalid_last_event_id"})
    return encode_cursor(event.event_key, event.id)


def _require_run(db: Session, project_id: uuid.UUID, run_id: uuid.UUID) -> None:
    if db.scalar(
        select(ModelRun.id).where(ModelRun.project_id == project_id, ModelRun.id == run_id)
    ):
        return
    raise HTTPException(status_code=404, detail="Training run not found.")
