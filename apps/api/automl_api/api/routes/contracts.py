from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from automl_api.api.deps import get_current_user
from automl_api.core.config import get_settings
from automl_api.db.session import get_db
from automl_api.models.enums import CommandStatus, ProjectRole, TaskType
from automl_api.models.iam import User
from automl_api.models.workflows import (
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    PromotionalScope,
)
from automl_api.schemas.contracts import (
    CapabilitiesRead,
    CatalogEstimatorRead,
    CatalogRead,
    CursorPage,
    EvaluationScopeCreate,
    EvaluationScopeMemberCreate,
    EvaluationScopeRead,
    ExperimentSpecCreate,
    FeatureContractCreate,
    RevisionRead,
    SearchSpaceCreate,
)
from automl_api.services.contracts import (
    add_scope_member,
    begin_contract_mutation,
    cancel_scope,
    candidate_for_run,
    create_experiment_spec,
    create_revision,
    create_scope,
    event_cursor_after_id,
    get_revision,
    run_collection,
)
from automl_api.services.projects import require_project_role
from automl_api.services.upload_policy import configured_upload_data_region
from automl_api.services.workflow_state import transition_command
from automl_api.training.model_catalog import candidate_catalog

router = APIRouter(tags=["production contracts"])


def _mutation(
    db: Session,
    user: User,
    project_id: uuid.UUID,
    operation: str,
    idempotency_key: str,
    payload: dict,
    create,
    schema,
):
    command, replayed = begin_contract_mutation(
        db, user, project_id, operation, idempotency_key, payload
    )
    if replayed and command.response_payload:
        return schema.model_validate(command.response_payload)
    row = create()
    response = schema.model_validate(row)
    command.resource_type = operation
    command.resource_id = row.id
    command.response_status = 200
    command.response_payload = response.model_dump(mode="json")
    transition_command(command, CommandStatus.RUNNING)
    transition_command(command, CommandStatus.SUCCEEDED)
    db.commit()
    return response


@router.get("/capabilities", response_model=CapabilitiesRead)
def capabilities() -> CapabilitiesRead:
    settings = get_settings()
    return CapabilitiesRead(
        auth_modes=["simple"] if settings.simple_auth_enabled else ["sso"],
        upload_protocols=["multipart", "direct-object-store"],
        task_types=list(TaskType),
        active_catalog_revisions={},
        max_qualified_concurrency=settings.max_concurrent_jobs,
        environment_qualified=False,
        deployment_target=settings.environment,
        upload_data_region=configured_upload_data_region(settings),
        upload_storage_driver=getattr(settings, "object_store_type", "embedded"),
    )


@router.post("/projects/{project_id}/feature-contracts", response_model=RevisionRead)
def post_feature_contract(
    project_id: uuid.UUID,
    payload: FeatureContractCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> RevisionRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "feature-contract.create",
        idempotency_key,
        payload.model_dump(mode="json"),
        lambda: create_revision(db, current_user, project_id, "feature-contract", payload),
        RevisionRead,
    )


@router.get("/projects/{project_id}/feature-contracts/{revision}", response_model=RevisionRead)
def read_feature_contract(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    return RevisionRead.model_validate(
        get_revision(db, current_user, project_id, "feature-contract", revision)
    )


@router.post("/projects/{project_id}/feature-search-spaces", response_model=RevisionRead)
def post_search_space(
    project_id: uuid.UUID,
    payload: SearchSpaceCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> RevisionRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "feature-search-space.create",
        idempotency_key,
        payload.model_dump(mode="json"),
        lambda: create_revision(db, current_user, project_id, "feature-search-space", payload),
        RevisionRead,
    )


@router.get("/projects/{project_id}/feature-search-spaces/{revision}", response_model=RevisionRead)
def read_search_space(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    return RevisionRead.model_validate(
        get_revision(db, current_user, project_id, "feature-search-space", revision)
    )


@router.post("/projects/{project_id}/search-objectives", response_model=RevisionRead)
def post_search_objective(
    project_id: uuid.UUID,
    payload: SearchSpaceCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> RevisionRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "search-objective.create",
        idempotency_key,
        payload.model_dump(mode="json"),
        lambda: create_revision(db, current_user, project_id, "search-objective", payload),
        RevisionRead,
    )


@router.get("/projects/{project_id}/search-objectives/{revision}", response_model=RevisionRead)
def read_search_objective(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    return RevisionRead.model_validate(
        get_revision(db, current_user, project_id, "search-objective", revision)
    )


@router.post("/projects/{project_id}/experiment-specs", response_model=RevisionRead)
def post_experiment_spec(
    project_id: uuid.UUID,
    payload: ExperimentSpecCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> RevisionRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "experiment-spec.create",
        idempotency_key,
        payload.model_dump(mode="json"),
        lambda: create_experiment_spec(db, current_user, project_id, payload),
        RevisionRead,
    )


@router.get("/projects/{project_id}/experiment-specs/{revision}", response_model=RevisionRead)
def read_experiment_spec(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    require_project_role(db, current_user, project_id, ProjectRole.VIEWER)
    row = db.scalar(
        select(ExperimentSpecRevision).where(
            ExperimentSpecRevision.project_id == project_id,
            ExperimentSpecRevision.id == revision,
        )
    )
    if row is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Experiment spec not found.")
    return RevisionRead.model_validate(row)


@router.post(
    "/projects/{project_id}/training/evaluation-scopes", response_model=EvaluationScopeRead
)
def post_scope(
    project_id: uuid.UUID,
    payload: EvaluationScopeCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> EvaluationScopeRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "evaluation-scope.create",
        idempotency_key,
        payload.model_dump(mode="json"),
        lambda: create_scope(db, current_user, project_id, payload),
        EvaluationScopeRead,
    )


@router.get(
    "/projects/{project_id}/training/evaluation-scopes/{scope_id}",
    response_model=EvaluationScopeRead,
)
def read_scope(
    project_id: uuid.UUID,
    scope_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> EvaluationScopeRead:
    require_project_role(db, current_user, project_id, ProjectRole.VIEWER)
    scope = db.scalar(
        select(PromotionalScope).where(
            PromotionalScope.project_id == project_id, PromotionalScope.id == scope_id
        )
    )
    if scope is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Evaluation scope not found.")
    return EvaluationScopeRead.model_validate(scope)


@router.post("/projects/{project_id}/training/evaluation-scopes/{scope_id}/members")
def post_scope_member(
    project_id: uuid.UUID,
    scope_id: uuid.UUID,
    payload: EvaluationScopeMemberCreate,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> dict[str, str | int]:
    command, replayed = begin_contract_mutation(
        db,
        current_user,
        project_id,
        "evaluation-scope.member.add",
        idempotency_key,
        {"scope_id": str(scope_id), **payload.model_dump(mode="json")},
    )
    if replayed and command.response_payload:
        return command.response_payload
    member = add_scope_member(db, current_user, project_id, scope_id, payload.run_id)
    response = {"member_id": str(member.id), "ordinal": member.ordinal}
    command.resource_type = "promotional_scope_member"
    command.resource_id = member.id
    command.response_status = 200
    command.response_payload = response
    transition_command(command, CommandStatus.RUNNING)
    transition_command(command, CommandStatus.SUCCEEDED)
    db.commit()
    return response


@router.post(
    "/projects/{project_id}/training/evaluation-scopes/{scope_id}/cancel",
    response_model=EvaluationScopeRead,
)
def post_scope_cancel(
    project_id: uuid.UUID,
    scope_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1)],
) -> EvaluationScopeRead:
    return _mutation(
        db,
        current_user,
        project_id,
        "evaluation-scope.cancel",
        idempotency_key,
        {"scope_id": str(scope_id)},
        lambda: cancel_scope(db, current_user, project_id, scope_id),
        EvaluationScopeRead,
    )


def _revision_read(
    db: Session, user: User, project_id: uuid.UUID, model, revision: uuid.UUID
) -> RevisionRead:
    require_project_role(db, user, project_id, ProjectRole.VIEWER)
    row = db.scalar(select(model).where(model.project_id == project_id, model.id == revision))
    if row is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Revision not found.")
    return RevisionRead.model_validate(row)


@router.get("/projects/{project_id}/feature-registry/{revision}", response_model=RevisionRead)
def read_registry(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    return _revision_read(db, current_user, project_id, FeatureRegistryRevision, revision)


@router.get("/projects/{project_id}/feature-recipes/{revision}", response_model=RevisionRead)
def read_recipe(
    project_id: uuid.UUID,
    revision: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> RevisionRead:
    return _revision_read(db, current_user, project_id, FeatureRecipeRevision, revision)


@router.get("/projects/{project_id}/training/catalogs/{task_type}", response_model=CatalogRead)
def catalog(
    project_id: uuid.UUID,
    task_type: TaskType,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> CatalogRead:
    require_project_role(db, current_user, project_id, ProjectRole.VIEWER)
    revision = db.scalar(
        select(EstimatorCatalogRevision)
        .where(EstimatorCatalogRevision.project_id == project_id)
        .order_by(EstimatorCatalogRevision.revision.desc())
    )
    estimators = [
        CatalogEstimatorRead(
            name=item.name,
            source="sklearn"
            if not item.name.startswith(("XGB", "LGBM", "CatBoost"))
            else "external",
            status="supported",
            recipe_id=item.name,
            resource_class=item.cost_tier,
            sampling_policy="full_or_qualified_tier",
            incremental=False,
            serializable=True,
            inference=True,
            deprecated=False,
        )
        for item in candidate_catalog(task_type)
    ]
    return CatalogRead(
        catalog_revision=revision.content_digest if revision else "unpublished",
        task_type=task_type,
        generated_at=revision.created_at if revision else datetime.now(UTC),
        runtime_lock_digest="unqualified",
        signature_verified=False,
        estimators=estimators,
    )


def _page(rows: list, next_cursor: str | None) -> CursorPage:
    return CursorPage(
        items=[
            {
                column.name: getattr(row, column.name)
                for column in row.__table__.columns
                if column.name not in {"parameters", "payload"}
            }
            for row in rows
        ],
        next_cursor=next_cursor,
    )


@router.get("/projects/{project_id}/training/runs/{run_id}/candidates", response_model=CursorPage)
def candidates(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
    resource_class: str | None = None,
) -> CursorPage:
    rows, next_cursor = run_collection(
        db,
        current_user,
        project_id,
        run_id,
        "candidates",
        cursor=cursor,
        limit=limit,
        status_filter=status_filter,
        resource_class=resource_class,
    )
    return _page(rows, next_cursor)


@router.get("/projects/{project_id}/training/runs/{run_id}/candidates/{candidate_id}")
def candidate_detail(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    candidate_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> dict:
    row = candidate_for_run(db, current_user, project_id, run_id, candidate_id)
    return _page([row], None).items[0] | {"parameters": row.params}


@router.get("/projects/{project_id}/training/runs/{run_id}/trials", response_model=CursorPage)
def trials(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> CursorPage:
    rows, next_cursor = run_collection(
        db, current_user, project_id, run_id, "trials", cursor=cursor, limit=limit
    )
    return _page(rows, next_cursor)


@router.get("/projects/{project_id}/training/runs/{run_id}/events", response_model=None)
def events(
    project_id: uuid.UUID,
    run_id: uuid.UUID,
    db: Annotated[Session, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
    request: Request,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> CursorPage | StreamingResponse:
    if last_event_id:
        try:
            cursor = event_cursor_after_id(
                db, current_user, project_id, run_id, uuid.UUID(last_event_id)
            )
        except ValueError as exc:
            from fastapi import HTTPException

            raise HTTPException(status_code=422, detail={"code": "invalid_last_event_id"}) from exc
    rows, next_cursor = run_collection(
        db, current_user, project_id, run_id, "events", cursor=cursor, limit=limit
    )
    page = _page(rows, next_cursor)
    if "text/event-stream" not in request.headers.get("accept", ""):
        return page

    def stream():
        for item in page.items:
            data = json.dumps(item, default=str, sort_keys=True)
            yield f"id: {item['id']}\nevent: {item['event_type']}\ndata: {data}\n\n"

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Next-Cursor": next_cursor or ""},
    )
