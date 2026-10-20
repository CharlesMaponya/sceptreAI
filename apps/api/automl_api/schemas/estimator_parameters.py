"""Closed estimator-discriminated parameter types from reviewed catalog dimensions."""

from __future__ import annotations

from functools import reduce
from operator import or_
from typing import Annotated, Literal

import numpy as np
from pydantic import (
    BeforeValidator,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    TypeAdapter,
    create_model,
)
from skopt.space import Categorical, Integer, Real

from automl_api.models.enums import TaskType
from automl_api.training.model_catalog import candidate_catalog

POLICY_REVISION = "catalog-parameter-overrides-v1"


def _categorical(values):
    categories = tuple(value.item() if isinstance(value, np.generic) else value for value in values)
    if any(type(value) not in (str, int, float, bool, type(None)) for value in categories):
        raise ValueError("Reviewed parameter categories must be JSON scalars.")

    def exact(value):
        if not any(type(value) is type(category) and value == category for category in categories):
            raise ValueError("Parameter is not a reviewed category of the expected type.")
        return value

    return Annotated[Literal[categories], BeforeValidator(exact)]


def _parameter_models():
    candidates = {}
    for task in (
        TaskType.CLASSIFICATION,
        TaskType.REGRESSION,
        TaskType.TIME_SERIES,
        TaskType.CLUSTERING,
    ):
        for candidate in candidate_catalog(task):
            candidates.setdefault(candidate.name, candidate)
    parameters, overrides = {}, {}
    config = ConfigDict(extra="forbid", allow_inf_nan=False)
    for name, candidate in sorted(candidates.items()):
        fields = {}
        for path, dimension in candidate.search_space.items():
            if not path.startswith("model__") or "__" in path.removeprefix("model__"):
                raise ValueError("Only reviewed estimator parameters may be overridden.")
            field = path.removeprefix("model__")
            if isinstance(dimension, Integer):
                annotation = Annotated[
                    StrictInt, Field(ge=int(dimension.low), le=int(dimension.high))
                ]
            elif isinstance(dimension, Real):
                annotation = Annotated[
                    StrictFloat,
                    Field(ge=float(dimension.low), le=float(dimension.high), allow_inf_nan=False),
                ]
            elif isinstance(dimension, Categorical):
                annotation = _categorical(dimension.categories)
            else:
                raise ValueError("Unsupported reviewed parameter dimension.")
            fields[field] = (annotation, Field(default_factory=lambda: None))
        if "n_clusters" in candidate.estimator.get_params(deep=False):
            # Use the existing clustering sweep's reviewed range.
            fields["n_clusters"] = (
                Annotated[StrictInt, Field(ge=2, le=8)],
                Field(default_factory=lambda: None),
            )
        parameters[name] = create_model(
            f"{name}Parameters", __config__=config, __module__=__name__, **fields
        )
        overrides[name] = create_model(
            f"{name}ParameterOverride",
            __config__=config,
            __module__=__name__,
            estimator=(Literal[name], ...),
            parameters=(parameters[name], Field(default_factory=parameters[name])),
        )
    return parameters, overrides


PARAMETER_MODELS, _OVERRIDE_MODELS = _parameter_models()
EstimatorParameterOverride = Annotated[
    reduce(or_, _OVERRIDE_MODELS.values()), Field(discriminator="estimator")
]
PARAMETER_OVERRIDES = TypeAdapter(
    Annotated[list[EstimatorParameterOverride], Field(max_length=100)]
)


def dump_overrides(rows):
    return sorted(
        [
            {
                "estimator": row.estimator,
                "parameters": row.parameters.model_dump(mode="json", exclude_unset=True),
            }
            for row in rows
        ],
        key=lambda row: row["estimator"],
    )


def parameter_schema(name):
    return PARAMETER_MODELS[name].model_json_schema()
