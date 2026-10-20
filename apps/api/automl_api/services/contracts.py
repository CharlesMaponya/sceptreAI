from __future__ import annotations

import base64
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import func, select, tuple_
from sqlalchemy.orm import Session

from automl_api.models.enums import ProjectRole, ScopeStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.projects import Project
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
    SearchObjectiveCreate,
    SearchSpaceCreate,
)
from automl_api.schemas.training import TrainingEstimateRequest
from automl_api.services.experiment_identity import (
    OBJECTIVE_PREFIX,
    SPEC_PREFIX,
    build_identity,
    specification_values,
)
from automl_api.services.feature_contracts import (
    bound_row,
    validate_contract,
    validate_objective,
    validate_search,
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
from automl_api.training.catalog_contract import validate_catalog_revision

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
    require_project_role(db, user, project_id, ProjectRole.EDITOR)
    # Lock before command FK checks to serialize revision allocation without lock upgrades.
    if db.scalar(select(Project.id).where(Project.id == project_id).with_for_update()) is None:
        raise HTTPException(status_code=404, detail="Project not found.")
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
    if kind == "search-objective" and payload.name.startswith(OBJECTIVE_PREFIX):
        raise HTTPException(status_code=422, detail={"code": "reserved_revision_name"})
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
    if isinstance(payload, FeatureContractCreate):
        validate_contract(
            db, project_id, payload.specification, payload.task_type, payload.target_column
        )
    elif isinstance(payload, SearchSpaceCreate):
        validate_search(
            db, project_id, payload.specification, payload.metric_name, payload.metric_direction
        )
    elif isinstance(payload, SearchObjectiveCreate):
        objective = validate_objective(
            payload.specification, payload.metric_name, payload.metric_direction
        )
        if objective.feature_search_space_revision_id is not None:
            search = bound_row(
                db,
                FeatureSearchSpaceRevision,
                project_id,
                objective.feature_search_space_revision_id,
            )
            if (search.metric_name, search.metric_direction) != (
                payload.metric_name,
                payload.metric_direction,
            ):
                raise HTTPException(status_code=409, detail={"code": "search_objective_changed"})
    else:
        raise HTTPException(status_code=422, detail={"code": "typed_revision_required"})
    values: dict[str, Any] = {
        "project_id": project_id,
        "name": payload.name,
        "revision": current + 1,
        "digest_algorithm": "sha256",
        "digest_scope": f"{kind}-v2",
        "content_digest": canonical_request_hash(
            payload.model_dump(mode="json", exclude={"name", "expected_revision"})
        ),
        "specification": payload.specification.model_dump(mode="json"),
    }
    if isinstance(payload, FeatureContractCreate):
        values.update(task_type=payload.task_type.value, target_column=payload.target_column)
    if isinstance(payload, (SearchSpaceCreate, SearchObjectiveCreate)):
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
    if payload.name.startswith(SPEC_PREFIX):
        raise HTTPException(status_code=422, detail={"code": "reserved_revision_name"})
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
            detail={
                "code": "experiment_spec_revision_changed",
                "requested_revision": payload.expected_revision,
                "current_revision": current,
            },
        )
    fields = set(TrainingEstimateRequest.model_fields) & set(type(payload).model_fields)
    request = TrainingEstimateRequest.model_validate(
        payload.model_dump(include=fields, exclude_unset=True)
    )
    identity = build_identity(db, project_id, request)
    spec = ExperimentSpecRevision(
        revision=current + 1,
        **specification_values(project_id, payload.name, identity, metadata=payload.specification),
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
    if payload.mode == "promotional":
        from automl_api.services.provider_source import reject_provider_promotion

        split = db.get(DatasetSplitRevision, payload.split_revision_id)
        reject_provider_promotion(db, split.dataset_version_id)
    scope = PromotionalScope(
        id=uuid.uuid4(),
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
    if scope.mode == "promotional":
        from automl_api.services.champion_planning import validate_refit_policy

        try:
            policy, _ = validate_refit_policy(db, scope)
        except ValueError as exc:
            raise HTTPException(
                status_code=422,
                detail={"code": "invalid_refit_policy"},
            ) from exc
        scope.comparison_policy = {
            **scope.comparison_policy,
            "refit_policy": policy.model_dump(mode="json"),
        }
        from automl_api.services.evaluation_planning import validate_evaluation_policy

        try:
            evaluation_policy, _ = validate_evaluation_policy(db, scope)
        except ValueError as exc:
            raise HTTPException(422, detail={"code": "invalid_evaluation_policy"}) from exc
        scope.comparison_policy = {
            **scope.comparison_policy,
            "evaluation_policy": evaluation_policy.model_dump(mode="json"),
        }
    db.add(scope)
    db.flush()
    if scope.mode == "promotional":
        from automl_api.services.scope_allocation import register_scope_allocation

        register_scope_allocation(db, scope, actor_id=user.id)
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
        .execution_options(populate_existing=True)
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
    if scope.mode == "promotional":
        from automl_api.services.provider_source import reject_provider_promotion

        reject_provider_promotion(db, run.dataset_version_id)
    count = int(
        db.scalar(
            select(func.count())
            .select_from(PromotionalScopeMember)
            .where(PromotionalScopeMember.scope_id == scope_id)
        )
        or 0
    )
    if scope.mode == "promotional":
        from automl_api.services.scope_allocation import require_scope_allocation

        try:
            require_scope_allocation(db, scope)
        except ValueError as exc:
            raise HTTPException(409, detail={"code": "scope_authority_not_ready"}) from exc
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
        if len(cursor) > 1024:
            raise ValueError
        values = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True).decode())
        if (
            not isinstance(values, list)
            or len(values) != 2
            or not all(isinstance(value, str) for value in values)
        ):
            raise ValueError
        return values[0], str(uuid.UUID(values[1]))
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


def resolve_catalog_revision(
    db: Session,
    project_id: uuid.UUID,
    task_type: TaskType,
    revision_id: uuid.UUID | None = None,
    *,
    require_current: bool = False,
) -> EstimatorCatalogRevision | None:
    query = select(EstimatorCatalogRevision).where(
        EstimatorCatalogRevision.project_id == project_id,
        EstimatorCatalogRevision.specification["task_type"].astext == task_type.value,
    )
    latest = query.order_by(
        EstimatorCatalogRevision.created_at.desc(),
        EstimatorCatalogRevision.id.desc(),
    )
    revision = db.scalar(
        query.where(EstimatorCatalogRevision.id == revision_id) if revision_id else latest
    )
    current = db.scalar(latest) if require_current and revision_id else revision
    error = None
    if revision is not None:
        try:
            if (
                require_current
                and current is not None
                and revision.content_digest != current.content_digest
            ):
                raise ValueError("Requested catalog is no longer the active revision for this task")
            validate_catalog_revision(revision, task_type)
        except ValueError as exc:
            error = exc
        else:
            return revision
    elif revision_id is None:
        return None
    if revision_id and not require_current:
        current = db.scalar(latest)
    raise HTTPException(
        status_code=409,
        detail={
            "code": "catalog_revision_changed",
            "requested_revision": str(revision_id) if revision_id else None,
            "current_revision": str(current.id) if current is not None else None,
            "message": str(error)
            if error is not None
            else "Requested catalog is unavailable for this task.",
        },
    )
