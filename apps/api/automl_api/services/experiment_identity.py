"""Resolve legacy request criteria once, then validate their frozen prepared graph."""

from __future__ import annotations

import uuid

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import TaskType
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    ExperimentSpecRevision,
    FeatureContractRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    FeatureSearchSpaceRevision,
    SearchObjectiveRevision,
)
from automl_api.schemas.training import TrainingEstimateRequest
from automl_api.services.feature_contracts import (
    validate_compiled_view,
    validate_contract,
    validate_objective,
    validate_search,
    verify_revision_content,
)
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.training.catalog_contract import validate_catalog_revision
from automl_api.training.evaluation import metric_direction, resolve_primary_metric

SPEC_SCOPE = "selected-training-identity-v1"
SPEC_PREFIX = "legacy-selected-"
OBJECTIVE_PREFIX = "legacy-objective-"
BINDINGS = {
    "split_revision_id": DatasetSplitRevision,
    "feature_contract_revision_id": FeatureContractRevision,
    "feature_registry_revision_id": FeatureRegistryRevision,
    "feature_recipe_revision_id": FeatureRecipeRevision,
    "feature_search_space_revision_id": FeatureSearchSpaceRevision,
    "catalog_revision_id": EstimatorCatalogRevision,
}
ROLES = (
    "positive_label",
    "evaluation_column",
    "entity_column",
    "event_time_column",
    "prediction_time_column",
)
CRITERIA = (
    "dataset_version_id",
    "task_type",
    "target_column",
    "primary_metric",
    *BINDINGS,
    "search_objective_revision_id",
    *ROLES,
    "excluded_columns",
    "optimization_iterations",
    "cv_folds",
)


def _conflict(code, **detail):
    raise HTTPException(status_code=409, detail={"code": code, **detail})


def _mismatch(field):
    raise HTTPException(
        status_code=422, detail={"code": "experiment_spec_mismatch", "field": field}
    )


def _metric(task, requested):
    try:
        return resolve_primary_metric(task, requested)
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=422, detail={"code": "invalid_primary_metric"}) from exc


def _uuid(value):
    try:
        return uuid.UUID(str(value))
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=409, detail={"code": "preparation_required"}) from exc


def _row(db, model, project_id, identity):
    row = db.scalar(
        select(model).where(model.project_id == project_id, model.id == _uuid(identity))
    )
    if row is None:
        _conflict("prepared_revision_changed", resource=model.__tablename__)
    return row


def _normalize(payload):
    updates = {}
    alias = getattr(payload, "estimator_catalog_revision_id", None)
    if alias is not None:
        if payload.catalog_revision_id not in {None, alias}:
            _conflict("catalog_revision_changed")
        updates["catalog_revision_id"] = alias
    if payload.execution_mode is not None:
        if (
            "execution_mode_hint" in payload.model_fields_set
            and payload.execution_mode_hint != payload.execution_mode
        ):
            raise HTTPException(status_code=422, detail={"code": "invalid_catalog_selection"})
        updates["execution_mode_hint"] = payload.execution_mode
    hint = updates.get("execution_mode_hint", payload.execution_mode_hint)
    if hint == "incremental" or (payload.catalog_mode == "all" and hint != "auto"):
        raise HTTPException(status_code=422, detail={"code": "invalid_catalog_selection"})
    return payload.model_copy(update=updates) if updates else payload


def _profile_bindings(db, project_id, payload):
    if payload.dataset_version_id is None:
        raise HTTPException(status_code=422, detail={"code": "dataset_version_required"})
    query = select(ProfilingJob).where(
        ProfilingJob.project_id == project_id,
        ProfilingJob.dataset_version_id == payload.dataset_version_id,
        ProfilingJob.status == "succeeded",
    )
    if "target_column" in payload.model_fields_set or payload.task_type == TaskType.CLUSTERING:
        query = query.where(ProfilingJob.target_column == payload.target_column)
    for key in BINDINGS:
        provided = getattr(payload, key)
        if provided is not None and key != "feature_search_space_revision_id":
            stored = "estimator_catalog_revision_id" if key == "catalog_revision_id" else key
            query = query.where(
                ProfilingJob.overview_json["launch_bindings"][stored].astext == str(provided)
            )
    profile = db.scalar(
        query.order_by(ProfilingJob.finished_at.desc().nullslast(), ProfilingJob.id.desc()).limit(1)
    )
    if profile is None:
        _conflict("preparation_required")
    if not isinstance(profile.overview_json, dict):
        _conflict("preparation_required")
    bindings = profile.overview_json.get("launch_bindings")
    if not isinstance(bindings, dict):
        _conflict("preparation_required")
    try:
        task = payload.task_type or TaskType(profile.overview_json["task_inference"]["task_type"])
    except (ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=409, detail={"code": "preparation_required"}) from exc
    metric = _metric(task, payload.primary_metric)
    identities = {
        key: _uuid(
            bindings.get("estimator_catalog_revision_id" if key == "catalog_revision_id" else key)
        )
        for key in BINDINGS
        if key != "feature_search_space_revision_id"
    }
    identities["feature_search_space_revision_id"] = _uuid(
        (bindings.get("feature_search_space_revision_ids") or {}).get(metric)
    )
    for key, value in identities.items():
        if getattr(payload, key) is not None and getattr(payload, key) != value:
            _mismatch(key)
    return identities, profile


def _objective(db, project_id, search, identity=None):
    if identity is not None:
        objective = _row(db, SearchObjectiveRevision, project_id, identity)
        if (
            not isinstance(objective.specification, dict)
            or objective.metric_name != search.metric_name
            or objective.metric_direction != search.metric_direction
            or objective.specification.get("fold_aggregation", "mean") != "mean"
        ):
            _conflict("search_objective_changed")
        verify_revision_content(objective, "search-objective")
        parsed = validate_objective(
            objective.specification, search.metric_name, search.metric_direction
        )
        if parsed.feature_search_space_revision_id not in (None, search.id):
            _conflict("search_objective_changed")
        return objective
    specification = {
        "feature_search_space_revision_id": str(search.id),
        "metric_name": search.metric_name,
        "metric_direction": search.metric_direction,
        "fold_aggregation": "mean",
    }
    digest = canonical_request_hash(specification)
    name = OBJECTIVE_PREFIX + digest
    identity = uuid.uuid5(project_id, name)
    db.execute(
        insert(SearchObjectiveRevision)
        .values(
            id=identity,
            project_id=project_id,
            name=name,
            revision=1,
            digest_algorithm="sha256",
            digest_scope="selected-search-objective-v1",
            content_digest=digest,
            specification=specification,
            metric_name=search.metric_name,
            metric_direction=search.metric_direction,
        )
        .on_conflict_do_nothing(index_elements=["project_id", "name", "revision"])
    )
    objective = _row(db, SearchObjectiveRevision, project_id, identity)
    if objective.content_digest != digest or objective.specification != specification:
        _conflict("search_objective_changed")
    return objective


def build_identity(db, project_id, payload, *, creating=True):
    payload = _normalize(payload)
    profile = None
    if all(getattr(payload, key) is not None for key in BINDINGS):
        ids = {key: getattr(payload, key) for key in BINDINGS}
    else:
        ids, profile = _profile_bindings(db, project_id, payload)
    graph = {key: _row(db, model, project_id, ids[key]) for key, model in BINDINGS.items()}
    split, contract, registry, recipe, search, catalog = (graph[key] for key in BINDINGS)
    if any(not isinstance(row.specification, dict) for row in graph.values()):
        _conflict("prepared_revision_changed")
    version = _row(db, DatasetVersion, project_id, split.dataset_version_id)
    if split.sealed_at is None:
        _conflict("preparation_required")
    if (
        str(contract.specification.get("dataset_version_id")) != str(version.id)
        or str(contract.specification.get("split_revision_id")) != str(split.id)
        or recipe.registry_revision_id != registry.id
        or registry.specification.get("fitted_scope_digest") != split.train_digest
        or recipe.specification.get("fitted_scope_digest") != split.train_digest
        or search.specification.get("recipe_revision") != recipe.content_digest
    ):
        _conflict("prepared_revision_changed")
    try:
        task = TaskType(contract.task_type)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=409, detail={"code": "prepared_revision_changed"}) from exc
    verify_revision_content(contract, "feature-contract")
    verify_revision_content(registry, "feature-registry")
    verify_revision_content(search, "feature-search-space")
    validate_contract(db, project_id, contract.specification, task, contract.target_column)
    validate_search(
        db, project_id, search.specification, search.metric_name, search.metric_direction
    )
    compiled = validate_compiled_view(contract, registry, recipe, search)
    metric = _metric(task, payload.primary_metric or search.metric_name)
    if search.metric_name != metric or search.metric_direction != metric_direction(metric):
        _conflict("search_objective_changed")
    try:
        validate_catalog_revision(catalog, task)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"code": "catalog_revision_changed"}) from exc
    if catalog.specification.get("execution_policy") != "ray_owned_concurrency":
        _conflict("catalog_revision_changed")
    expected = {
        "dataset_version_id": version.id,
        "task_type": task,
        "target_column": contract.target_column,
        "primary_metric": metric,
        **ids,
    }
    for key, value in expected.items():
        if key in payload.model_fields_set and getattr(payload, key) != value:
            _mismatch(key)
    columns = {column["name"] for column in (version.schema_json or {}).get("columns", [])}
    if contract.target_column is not None and contract.target_column not in columns:
        _conflict("prepared_revision_changed")
    excluded = sorted(set(payload.excluded_columns))
    if set(excluded) - columns or contract.target_column in excluded:
        _mismatch("excluded_columns")
    roles = {key: contract.specification.get(key) for key in ROLES}
    roles["event_time_column"] = roles["event_time_column"] or split.specification.get(
        "time_column"
    )
    for key in ROLES:
        declared = roles[key]
        if key in ("positive_label", "evaluation_column") and declared is None:
            roles[key] = getattr(payload, key)
        elif key in payload.model_fields_set and getattr(payload, key) != declared:
            _mismatch(key)
    if any(
        roles[key] is not None and roles[key] not in columns
        for key in (
            "evaluation_column",
            "entity_column",
            "event_time_column",
            "prediction_time_column",
        )
    ):
        _conflict("prepared_revision_changed")
    if roles["evaluation_column"] is not None and (
        task != TaskType.CLUSTERING or roles["evaluation_column"] not in columns
    ):
        _mismatch("evaluation_column")
    if roles["positive_label"] is not None:
        if task != TaskType.CLASSIFICATION:
            _mismatch("positive_label")
        if creating:
            if profile is None:
                profile = db.scalar(
                    select(ProfilingJob)
                    .where(
                        ProfilingJob.project_id == project_id,
                        ProfilingJob.dataset_version_id == version.id,
                        ProfilingJob.status == "succeeded",
                        ProfilingJob.target_column == contract.target_column,
                        ProfilingJob.overview_json["launch_bindings"]["split_revision_id"].astext
                        == str(split.id),
                    )
                    .order_by(ProfilingJob.finished_at.desc())
                    .limit(1)
                )
            target_profile = (
                (profile.feature_profiles_json or {}).get(contract.target_column, {})
                if profile
                else {}
            )
            if target_profile.get("distinct_count") != 2 or roles[
                "positive_label"
            ] not in target_profile.get("sample_values", []):
                _mismatch("positive_label")
    if not set(compiled.input_columns) - set(excluded) - {roles["evaluation_column"]}:
        _mismatch("excluded_columns")
    objective = _objective(db, project_id, search, payload.search_objective_revision_id)
    identity = {
        key: str(value)
        if isinstance(value, uuid.UUID)
        else value.value
        if isinstance(value, TaskType)
        else value
        for key, value in expected.items()
    }
    identity.update(
        project_id=str(project_id),
        search_objective_revision_id=str(objective.id),
        **roles,
        excluded_columns=excluded,
        optimization_iterations=payload.optimization_iterations,
        cv_folds=payload.cv_folds,
        binding_digests={key: row.content_digest for key, row in graph.items()},
        objective_digest=objective.content_digest,
    )
    return identity


def specification_values(project_id, name, identity, *, metadata=None, request_digest=None):
    specification = {"identity": identity, "metadata": metadata or {}}
    if request_digest is not None:
        specification["compatibility_request_digest"] = request_digest
    values = {
        key: identity[key]
        for key in (
            "dataset_version_id",
            "split_revision_id",
            "feature_contract_revision_id",
            "feature_search_space_revision_id",
            "search_objective_revision_id",
            "catalog_revision_id",
            "task_type",
            "target_column",
            "primary_metric",
        )
    }
    return dict(
        project_id=project_id,
        name=name,
        digest_algorithm="sha256",
        digest_scope=SPEC_SCOPE,
        content_digest=canonical_request_hash(specification),
        specification=specification,
        **values,
    )


def _read_spec(db, project_id, spec):
    body = spec.specification
    if (
        spec.digest_algorithm != "sha256"
        or spec.digest_scope != SPEC_SCOPE
        or not isinstance(body, dict)
        or canonical_request_hash(body) != spec.content_digest
        or not isinstance(body.get("identity"), dict)
    ):
        _conflict("experiment_spec_changed")
    identity = body["identity"]
    for key in (
        "dataset_version_id",
        "split_revision_id",
        "feature_contract_revision_id",
        "feature_search_space_revision_id",
        "search_objective_revision_id",
        "catalog_revision_id",
        "task_type",
        "target_column",
        "primary_metric",
    ):
        value = getattr(spec, key)
        if (str(value) if isinstance(value, uuid.UUID) else value) != identity.get(key):
            _conflict("experiment_spec_changed")
    if identity.get("project_id") != str(project_id):
        _conflict("experiment_spec_changed")
    try:
        frozen = TrainingEstimateRequest.model_validate({key: identity[key] for key in CRITERIA})
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=409, detail={"code": "experiment_spec_changed"}) from exc
    try:
        current = build_identity(db, project_id, frozen, creating=False)
    except ValidationError as exc:
        raise HTTPException(status_code=409, detail={"code": "prepared_revision_changed"}) from exc
    if current != identity:
        _conflict("experiment_spec_changed")
    return identity


def resolve_training_identity(db, project_id, payload):
    payload = _normalize(payload)
    if payload.experiment_spec_revision_id is not None:
        spec = db.scalar(
            select(ExperimentSpecRevision).where(
                ExperimentSpecRevision.project_id == project_id,
                ExperimentSpecRevision.id == payload.experiment_spec_revision_id,
            )
        )
        if spec is None:
            _conflict("experiment_spec_changed")
    else:
        if payload.catalog_mode == "all":
            raise HTTPException(status_code=422, detail={"code": "experiment_spec_required"})
        request = payload.model_dump(mode="json")
        criteria = {key: request[key] for key in CRITERIA}
        criteria["excluded_columns"] = sorted(set(payload.excluded_columns))
        criteria["target_asserted"] = "target_column" in payload.model_fields_set
        digest = canonical_request_hash(criteria)
        name = SPEC_PREFIX + digest
        identity_id = uuid.uuid5(project_id, name)
        spec = db.scalar(
            select(ExperimentSpecRevision).where(
                ExperimentSpecRevision.project_id == project_id,
                ExperimentSpecRevision.id == identity_id,
            )
        )
        if spec is None:
            identity = build_identity(db, project_id, payload)
            values = specification_values(project_id, name, identity, request_digest=digest)
            db.execute(
                insert(ExperimentSpecRevision)
                .values(id=identity_id, revision=1, **values)
                .on_conflict_do_nothing(index_elements=["project_id", "name", "revision"])
            )
            spec = _row(db, ExperimentSpecRevision, project_id, identity_id)
        if spec.specification.get("compatibility_request_digest") != digest:
            _conflict("experiment_spec_changed")
    identity = _read_spec(db, project_id, spec)
    for key in CRITERIA:
        if key in payload.model_fields_set:
            provided = getattr(payload, key)
            expected = identity[key]
            if isinstance(provided, uuid.UUID):
                provided = str(provided)
            if isinstance(provided, TaskType):
                provided = provided.value
            if key == "excluded_columns":
                provided = sorted(set(provided))
            if provided != expected:
                _mismatch(key)
    updates = {key: identity[key] for key in CRITERIA}
    for key in ("dataset_version_id", *BINDINGS, "search_objective_revision_id"):
        updates[key] = uuid.UUID(updates[key])
    updates["task_type"] = TaskType(updates["task_type"])
    updates["experiment_spec_revision_id"] = spec.id
    if "estimator_catalog_revision_id" in type(payload).model_fields:
        updates["estimator_catalog_revision_id"] = updates["catalog_revision_id"]
    return payload.model_copy(update=updates)
