"""Closed metadata contracts; feature definitions and executable objects are server owned."""

from __future__ import annotations

import uuid
from typing import Annotated, Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator

ColumnName = Annotated[StrictStr, Field(min_length=1, max_length=255)]
Sha256 = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
FeatureFamily = Literal[
    "raw_passthrough",
    "numeric_unary",
    "bounded_pairwise_arithmetic",
    "datetime_decomposition",
    "categorical_frequency",
    "text_length_pattern",
]
FEATURE_FAMILIES = get_args(FeatureFamily)


def unique_values(values):
    if len(values) != len(set(values)):
        raise ValueError("Values must be unique.")
    return values


class FeatureBudgets(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maximum_expression_depth: Annotated[StrictInt, Field(ge=1, le=2)]
    maximum_output_count: Annotated[StrictInt, Field(ge=1, le=500)]
    maximum_cardinality: Annotated[StrictInt, Field(ge=1, le=100_000)]
    maximum_batch_rows: Annotated[StrictInt, Field(ge=1, le=8192)]


class FeatureContractSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dataset_version_id: uuid.UUID
    prepared_artifact_id: uuid.UUID
    split_revision_id: uuid.UUID
    source_snapshot_digest: Sha256
    source_columns: Annotated[list[ColumnName], Field(min_length=1, max_length=10_000)]
    target_column: ColumnName | None
    positive_label: ColumnName | None = None
    evaluation_column: ColumnName | None = None
    entity_column: ColumnName | None = None
    event_time_column: ColumnName | None = None
    prediction_time_column: ColumnName | None = None
    allowed_feature_families: Annotated[list[FeatureFamily], Field(min_length=1, max_length=6)]
    disabled_feature_families: Annotated[
        list[Literal["lag", "rolling", "window", "arbitrary_python"]],
        Field(min_length=4, max_length=4),
    ]
    budgets: FeatureBudgets
    final_test_isolated: Literal[True]
    value_fit_scope: Literal["outer_train_only"]

    @field_validator("final_test_isolated", mode="before")
    @classmethod
    def isolated(cls, value):
        if value is not True:
            raise ValueError("Final-test isolation is mandatory.")
        return value

    @field_validator("source_columns", "allowed_feature_families", "disabled_feature_families")
    @classmethod
    def unique(cls, values):
        return unique_values(values)

    @field_validator("disabled_feature_families")
    @classmethod
    def unsafe_families_disabled(cls, values):
        if set(values) != {"lag", "rolling", "window", "arbitrary_python"}:
            raise ValueError("Stateful and arbitrary execution are not qualified.")
        return values


class FeatureSearchSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metric: Annotated[StrictStr, Field(min_length=1, max_length=64)]
    direction: Literal["maximize", "minimize"]
    feature_recipe_revision_id: uuid.UUID
    recipe_revision: Sha256
    allowed_feature_families: Annotated[list[FeatureFamily], Field(min_length=1, max_length=6)]

    @field_validator("allowed_feature_families")
    @classmethod
    def unique(cls, values):
        return unique_values(values)


class SearchObjectiveSpecification(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fold_aggregation: Literal["mean"] = "mean"
    feature_search_space_revision_id: uuid.UUID | None = None
    metric_name: Annotated[StrictStr, Field(min_length=1, max_length=64)] | None = None
    metric_direction: Literal["maximize", "minimize"] | None = None
