"""Validate typed policy and the compiled, server-owned prepared feature view."""

from __future__ import annotations

import hashlib

from automl_shared.feature_recipe import FeatureRecipe
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select

from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import TaskType
from automl_api.models.workflows import (
    DatasetSplitRevision,
    FeatureRecipeRevision,
    PreparedArtifact,
)
from automl_api.schemas.feature_contracts import (
    FeatureContractSpecification,
    FeatureSearchSpecification,
    SearchObjectiveSpecification,
)
from automl_api.services.provider_source import require_prepared_source
from automl_api.services.workflow_state import canonical_request_hash
from automl_api.training.evaluation import (
    PRIMARY_METRICS_BY_TASK,
    metric_direction,
    resolve_primary_metric,
)


def _invalid(code="feature_contract_changed"):
    raise HTTPException(status_code=409, detail={"code": code})


def bound_row(db, model, project_id, identity):
    row = db.scalar(select(model).where(model.project_id == project_id, model.id == identity))
    if row is None:
        _invalid("prepared_revision_changed")
    return row


def verify_revision_content(row, kind):
    if row.digest_algorithm != "sha256":
        _invalid("prepared_revision_changed")
    if (
        kind in {"feature-contract", "feature-search-space", "search-objective"}
        and row.digest_scope == f"{kind}-v2"
    ):
        fields = (
            ("task_type", "target_column")
            if kind == "feature-contract"
            else ("metric_name", "metric_direction")
        )
        content = {
            "specification": row.specification,
            **{field: getattr(row, field) for field in fields},
        }
    elif row.digest_scope == f"phase3-{kind}-v1" or (
        kind == "search-objective" and row.digest_scope == "selected-search-objective-v1"
    ):
        content = row.specification
    else:
        _invalid("unqualified_revision_scope")
    if canonical_request_hash(content) != row.content_digest:
        _invalid("prepared_revision_changed")


def validate_metric(name, direction, task=None):
    try:
        if (
            not any(name in metrics for metrics in PRIMARY_METRICS_BY_TASK.values())
            or metric_direction(name) != direction
        ):
            raise ValueError("Metric direction differs.")
        if task is not None and resolve_primary_metric(task, name) != name:
            raise ValueError("Metric is not supported for this task.")
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=422, detail={"code": "invalid_search_objective"}) from exc


def parse_policy(schema, specification):
    try:
        return schema.model_validate(specification)
    except ValidationError as exc:
        raise HTTPException(status_code=409, detail={"code": "prepared_revision_changed"}) from exc


def validate_contract(db, project_id, specification, task, target):
    spec = parse_policy(FeatureContractSpecification, specification)
    if task == TaskType.UNSPECIFIED or spec.target_column != target:
        _invalid()
    if (task == TaskType.CLUSTERING) != (target is None):
        _invalid()
    version = bound_row(db, DatasetVersion, project_id, spec.dataset_version_id)
    require_prepared_source(
        db,
        version,
        split_revision_id=spec.split_revision_id,
        prepared_artifact_id=spec.prepared_artifact_id,
    )
    split = bound_row(db, DatasetSplitRevision, project_id, spec.split_revision_id)
    artifact = bound_row(db, PreparedArtifact, project_id, spec.prepared_artifact_id)
    if not all(
        isinstance(value, dict)
        for value in (artifact.specification, split.specification, version.schema_json)
    ):
        _invalid()
    identity = artifact.specification.get("identity")
    columns = version.schema_json.get("columns")
    if (
        not isinstance(identity, dict)
        or not isinstance(identity.get("split_digests"), dict)
        or not isinstance(columns, list)
        or any(
            not isinstance(column, dict) or not isinstance(column.get("name"), str)
            for column in columns
        )
    ):
        _invalid()
    if (
        split.dataset_version_id != version.id
        or split.sealed_at is None
        or artifact.dataset_version_id != version.id
        or spec.source_snapshot_digest != version.content_hash
        or artifact.specification.get("source_digest") != spec.source_snapshot_digest
        or identity["split_digests"].get("train") != split.train_digest
        or spec.source_columns != [column["name"] for column in columns]
    ):
        _invalid()
    names = set(spec.source_columns)
    roles = (
        target,
        spec.evaluation_column,
        spec.entity_column,
        spec.event_time_column,
        spec.prediction_time_column,
    )
    if any(role is not None and role not in names for role in roles):
        _invalid()
    if spec.positive_label is not None and task != TaskType.CLASSIFICATION:
        _invalid()
    if spec.evaluation_column is not None and task != TaskType.CLUSTERING:
        _invalid()
    if target is not None and target in {
        spec.entity_column,
        spec.event_time_column,
        spec.prediction_time_column,
    }:
        _invalid()
    if spec.event_time_column is not None and spec.event_time_column != split.specification.get(
        "time_column"
    ):
        _invalid()
    if task == TaskType.TIME_SERIES and split.specification.get("split_strategy") != "temporal":
        _invalid()
    if "raw_passthrough" not in spec.allowed_feature_families:
        _invalid("unsupported_feature_execution")
    return spec


def validate_search(db, project_id, specification, metric, direction):
    spec = parse_policy(FeatureSearchSpecification, specification)
    validate_metric(metric, direction)
    recipe = bound_row(db, FeatureRecipeRevision, project_id, spec.feature_recipe_revision_id)
    if (
        spec.metric != metric
        or spec.direction != direction
        or spec.recipe_revision != recipe.content_digest
    ):
        _invalid("feature_search_space_changed")
    if "raw_passthrough" not in spec.allowed_feature_families:
        _invalid("unsupported_feature_execution")
    return spec


def validate_objective(specification, metric, direction):
    spec = parse_policy(SearchObjectiveSpecification, specification)
    validate_metric(metric, direction)
    if spec.metric_name not in (None, metric) or spec.metric_direction not in (None, direction):
        _invalid("search_objective_changed")
    return spec


def validate_compiled_view(contract, registry, recipe, search):
    try:
        policy = FeatureContractSpecification.model_validate(contract.specification)
        search_policy = FeatureSearchSpecification.model_validate(search.specification)
        fields = {
            "revision",
            "input_columns",
            "target_column",
            "excluded_leakage_columns",
            "ratio_features",
        }
        metadata = {
            "metadata_view",
            "registry_revision_id",
            "prepared_artifact_id",
            "materialization",
            "fitted_scope_digest",
        }
        body = recipe.specification
        if not isinstance(body, dict) or set(body) - fields - metadata:
            _invalid("feature_recipe_changed")
        if (
            not isinstance(body.get("input_columns"), list)
            or not isinstance(body.get("excluded_leakage_columns"), list)
            or not isinstance(body.get("target_column"), str)
            or any(
                not isinstance(name, str)
                for name in [*body["input_columns"], *body["excluded_leakage_columns"]]
            )
        ):
            _invalid("feature_recipe_changed")
        compiled = FeatureRecipe.from_dict({key: body[key] for key in fields if key in body})
        if (
            compiled.digest != recipe.content_digest
            or compiled.target_column != (contract.target_column or "")
            or not set(compiled.input_columns) <= set(policy.source_columns)
            or body.get("materialization") != "metadata_only"
            or body.get("prepared_artifact_id") != str(policy.prepared_artifact_id)
            or body.get("registry_revision_id") != str(registry.id)
            or search_policy.feature_recipe_revision_id != recipe.id
            or search_policy.recipe_revision != recipe.content_digest
            or not set(search_policy.allowed_feature_families)
            <= set(policy.allowed_feature_families)
            or "raw_passthrough" not in search_policy.allowed_feature_families
            or policy.entity_column in compiled.input_columns
            or len(compiled.input_columns) > policy.budgets.maximum_output_count
        ):
            _invalid("feature_recipe_changed")
        if compiled.ratio_features:
            _invalid("unsupported_feature_execution")
        accepted = registry.specification.get("accepted")
        if not isinstance(accepted, list) or [item["name"] for item in accepted] != list(
            compiled.input_columns
        ):
            _invalid("feature_registry_changed")
        for item in accepted:
            if (
                item.get("family") != "raw_passthrough"
                or item.get("execution_class") != "request_local"
                or item.get("value_accessed") is not False
                or item.get("formula_hash")
                != hashlib.sha256(f"column:{item['name']}".encode()).hexdigest()
            ):
                _invalid("feature_registry_changed")
    except (ValidationError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise HTTPException(status_code=409, detail={"code": "prepared_revision_changed"}) from exc
    return compiled
