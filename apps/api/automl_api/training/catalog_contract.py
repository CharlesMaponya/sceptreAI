"""Bind runtime catalog definitions without claiming signed release qualification."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import platform
import sysconfig
from collections.abc import Mapping
from pathlib import Path

import numpy as np
from skopt.space import Categorical, Integer, Real

from automl_api.core.config import get_settings
from automl_api.models.enums import TaskType
from automl_api.schemas import estimator_parameters
from automl_api.training import model_catalog
from automl_api.training.runtime_profiles import runtime_identity

_DEFINITION_SHA256 = hashlib.sha256(Path(model_catalog.__file__).read_bytes()).hexdigest()
_OVERRIDE_POLICY_SHA256 = hashlib.sha256(
    Path(estimator_parameters.__file__).read_bytes()
).hexdigest()
# Ray adds vendored paths during import; inventory the interpreter installation instead.
_RUNTIME_DEPENDENCIES = {
    "python": platform.python_version(),
    "package_scope": "interpreter-site-packages-v1",
    "packages": sorted(
        {
            (distribution.metadata.get("Name", ""), distribution.version)
            for distribution in importlib.metadata.distributions(
                path=sorted({sysconfig.get_path("purelib"), sysconfig.get_path("platlib")})
            )
        }
    ),
}


def _json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return {"nonfinite_float": value.hex()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [_json_value(item) for item in value]
    if isinstance(value, np.generic):
        return _json_value(value.item())
    if isinstance(value, type) or callable(value):
        return {"symbol": f"{value.__module__}.{value.__qualname__}"}
    raise ValueError(f"Unsupported server catalog value: {type(value).__name__}")


def catalog_digest(specification: dict) -> str:
    encoded = json.dumps(specification, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def catalog_specification(task_type: TaskType) -> dict:
    definitions, rows = [], []
    for candidate in model_catalog.candidate_catalog(task_type):
        estimator = candidate.estimator
        dimensions = {}
        for name, dimension in sorted(candidate.search_space.items()):
            if isinstance(dimension, Categorical):
                details = {"categories": dimension.categories, "prior": dimension.prior}
            elif isinstance(dimension, (Integer, Real)):
                details = {
                    key: getattr(dimension, key)
                    for key in ("low", "high", "prior", "base", "dtype")
                }
            else:
                raise ValueError("Unsupported server catalog search dimension")
            dimensions[name] = _json_value(
                {
                    "type": type(dimension).__name__,
                    "name": dimension.name,
                    "transform": dimension.transform_,
                    **details,
                }
            )
        definitions.append(
            {
                "name": candidate.name,
                "estimator": f"{type(estimator).__module__}.{type(estimator).__qualname__}",
                "parameters": _json_value(estimator.get_params(deep=False)),
                "search_space": dimensions,
                "default_selected": candidate.default_selected,
            }
        )
        rows.append(
            {
                "name": candidate.name,
                "source": type(estimator).__module__.split(".")[0],
                "status": "supported",
                "reason": None,
                "recipe_id": candidate.name,
                "resource_class": candidate.cost_tier,
                "sampling_policy": "unqualified",
                "incremental": hasattr(estimator, "partial_fit"),
                "serializable": False,
                "inference": hasattr(estimator, "predict"),
                "deprecated": False,
                "parameter_schema": estimator_parameters.parameter_schema(candidate.name),
            }
        )
    return {
        "schema_revision": "sceptre-runtime-catalog-v1",
        "task_type": task_type.value,
        "runtime_lock_digest": catalog_digest(
            {
                "definitions": definitions,
                "dependencies": runtime_identity(
                    _RUNTIME_DEPENDENCIES,
                    profile=os.getenv("SCEPTRE_RUNTIME_PROFILE"),
                    environment=get_settings().environment,
                ),
                "definition_source_sha256": _DEFINITION_SHA256,
                "override_policy_revision": estimator_parameters.POLICY_REVISION,
                "override_policy_source_sha256": _OVERRIDE_POLICY_SHA256,
                "override_schemas": {row["name"]: row["parameter_schema"] for row in rows},
            }
        ),
        "estimators": rows,
    }


def validate_catalog_revision(revision, task_type: TaskType) -> dict:
    specification = revision.specification
    if not isinstance(specification, dict) or revision.content_digest != catalog_digest(
        specification
    ):
        raise ValueError("Catalog revision content digest differs")
    current = catalog_specification(task_type)
    if any(specification.get(key) != value for key, value in current.items()):
        raise ValueError(
            "Catalog revision task, definitions or runtime differs; reprofile before launch"
        )
    return specification
