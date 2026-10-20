"""Validate reviewed fixed parameters and preserve them through estimator search."""

from __future__ import annotations

import math
import os
import uuid
from copy import deepcopy
from dataclasses import replace

from pydantic import ValidationError
from sklearn.base import clone
from sqlalchemy import select

from automl_api.core.config import get_settings
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import CommandStatus, TaskType
from automl_api.models.workflows import TrainingCandidate, WorkflowCommand
from automl_api.schemas.estimator_parameters import (
    PARAMETER_OVERRIDES,
    POLICY_REVISION,
    dump_overrides,
)
from automl_api.services.workflow_state import canonical_request_hash


def parse_overrides(rows):
    parsed = dump_overrides(PARAMETER_OVERRIDES.validate_python(rows))
    if len({row["estimator"] for row in parsed}) != len(parsed):
        raise ValueError("Each estimator may have one parameter override.")
    return parsed


def overrides_digest(rows):
    return canonical_request_hash({"revision": POLICY_REVISION, "overrides": rows})


def validate_conditionals(name, parameters, *, target_classes=None, minimum_rows=None):
    if (
        name == "LogisticRegression"
        and parameters.get("solver") == "liblinear"
        and target_classes is not None
        and target_classes > 2
    ):
        raise ValueError("liblinear requires a binary classification target.")
    for key in ("n_neighbors", "n_clusters"):
        if key in parameters and minimum_rows is not None and parameters[key] > minimum_rows:
            raise ValueError(f"{key} exceeds the smallest fitting fold.")


def validate_overrides(
    rows, candidates, *, target_classes=None, minimum_rows=None, minimum_test_rows=None
):
    normalized = parse_overrides(rows)
    available = {candidate.name: candidate for candidate in candidates}
    if any(row["estimator"] not in available for row in normalized):
        raise ValueError("Parameter overrides require enabled selected estimators for this task.")
    for row in normalized:
        candidate = available[row["estimator"]]
        parameters = row["parameters"]
        validate_conditionals(
            candidate.name, parameters, target_classes=target_classes, minimum_rows=minimum_rows
        )
        if minimum_test_rows is not None and not hasattr(candidate.estimator, "predict"):
            validate_conditionals(candidate.name, parameters, minimum_rows=minimum_test_rows)
        estimator = clone(candidate.estimator).set_params(**parameters)
        if hasattr(estimator, "_parameter_constraints"):
            estimator._validate_params()
    return normalized


def validate_admission(db, project_id, payload, candidates, *, training_rows=None):
    if not payload.parameter_overrides:
        return []
    classes = None
    if payload.task_type == TaskType.CLASSIFICATION:
        profile = db.scalar(
            select(ProfilingJob)
            .where(
                ProfilingJob.project_id == project_id,
                ProfilingJob.dataset_version_id == payload.dataset_version_id,
                ProfilingJob.target_column == payload.target_column,
                ProfilingJob.status == "succeeded",
                ProfilingJob.overview_json["launch_bindings"]["split_revision_id"].astext
                == str(payload.split_revision_id),
            )
            .order_by(ProfilingJob.finished_at.desc())
            .limit(1)
        )
        if profile is not None:
            classes = (
                (profile.feature_profiles_json or {})
                .get(payload.target_column, {})
                .get("distinct_count")
            )
    minimum = None
    if training_rows:
        minimum = (
            training_rows // (payload.cv_folds + 1) + training_rows % (payload.cv_folds + 1)
            if payload.task_type == TaskType.TIME_SERIES
            else training_rows - math.ceil(training_rows / payload.cv_folds)
        )
    return validate_overrides(
        payload.parameter_overrides,
        candidates,
        target_classes=classes,
        minimum_rows=minimum,
        minimum_test_rows=(
            training_rows // payload.cv_folds
            if training_rows and payload.task_type == TaskType.CLUSTERING
            else None
        ),
    )


def fixed_candidate(candidate, rows, *, target_classes=None, minimum_rows=None):
    # BayesSearchCV changes dimension transformers while fitting.
    candidate = replace(candidate, search_space=deepcopy(candidate.search_space))
    if not rows:
        return candidate, {}
    normalized = parse_overrides(rows)
    parameters = next(
        (row["parameters"] for row in normalized if row["estimator"] == candidate.name), {}
    )
    if not parameters:
        return candidate, {}
    validate_conditionals(
        candidate.name, parameters, target_classes=target_classes, minimum_rows=minimum_rows
    )
    estimator = clone(candidate.estimator).set_params(**parameters)
    if hasattr(estimator, "_parameter_constraints"):
        estimator._validate_params()
    fixed = {f"model__{name}": value for name, value in parameters.items()}
    return replace(
        candidate,
        estimator=estimator,
        search_space={
            key: value for key, value in candidate.search_space.items() if key not in fixed
        },
    ), fixed


def merge_fixed(parameters, fixed):
    if any(key in parameters and parameters[key] != value for key, value in fixed.items()):
        raise ValueError("Search attempted to change a fixed estimator parameter.")
    return {**parameters, **fixed}


def validate_bound_overrides(db, run, candidates):
    rows = run.params.get("parameter_overrides", [])
    digest = run.params.get("parameter_overrides_digest")
    if not rows and digest is None:
        if (
            run.params.get("launch_command_id")
            or run.params.get("parameter_override_policy_revision")
            or os.getenv("AUTOML_ATTEMPT_ID")
            or get_settings().environment in {"production", "staging"}
        ):
            raise ValueError("Training parameter policy is missing from admitted intent")
        # shortcut: preserve unbound local calls until legacy migration is qualified.
        return
    try:
        normalized = validate_overrides(rows, candidates)
        if (
            run.params.get("parameter_override_policy_revision") != POLICY_REVISION
            or overrides_digest(normalized) != digest
        ):
            raise ValueError("Parameter policy digest differs.")
        command_id = uuid.UUID(str(run.params.get("launch_command_id")))
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == run.project_id, WorkflowCommand.id == command_id
            )
        )
        if (
            command is None
            or command.operation != "training.launch"
            or command.resource_type != "model_run"
            or command.resource_id != run.id
            or command.status != CommandStatus.SUCCEEDED
            or canonical_request_hash(command.request_payload) != command.request_hash
            or parse_overrides(command.request_payload.get("parameter_overrides", [])) != normalized
        ):
            raise ValueError("Parameters differ from immutable launch intent.")
        members = list(
            db.scalars(
                select(TrainingCandidate).where(
                    TrainingCandidate.project_id == run.project_id,
                    TrainingCandidate.model_run_id == run.id,
                )
            )
        )
        if len(members) != len(candidates) or {member.estimator_key for member in members} != {
            candidate.name for candidate in candidates
        }:
            raise ValueError("Candidate parameter ownership is ambiguous.")
        fixed = {row["estimator"]: row["parameters"] for row in normalized}
        for member in members:
            expected = fixed.get(member.estimator_key, {})
            if canonical_request_hash(
                member.params.get("parameter_overrides")
            ) != canonical_request_hash(expected) or member.params.get(
                "parameter_overrides_digest"
            ) != canonical_request_hash(expected):
                raise ValueError("Candidate parameters differ from admitted intent.")
    except (ValidationError, TypeError, KeyError, ValueError) as exc:
        raise ValueError("Training parameter overrides differ from admitted intent") from exc


def validate_trial_overrides(run, candidate, specification):
    expected = (candidate.params or {}).get("parameter_overrides")
    if not expected and not run.params.get("parameter_overrides_digest"):
        return
    rows = parse_overrides(run.params.get("parameter_overrides", []))
    fixed = next(
        (row["parameters"] for row in rows if row["estimator"] == candidate.estimator_key), {}
    )
    if (
        run.params.get("parameter_override_policy_revision") != POLICY_REVISION
        or canonical_request_hash(fixed) != canonical_request_hash(expected)
        or overrides_digest(rows) != run.params.get("parameter_overrides_digest")
        or canonical_request_hash(expected) != candidate.params.get("parameter_overrides_digest")
    ):
        raise ValueError("Trial parameter policy differs from admitted intent.")
    if not expected:
        return
    context = specification.get("search", specification)
    if not isinstance(context, dict):
        raise ValueError("Trial search context must be an object.")
    base = context.get("base_parameters", context.get("parameters"))
    if (
        not isinstance(base, dict)
        or any(name not in base for name in expected)
        or canonical_request_hash({name: base[name] for name in expected})
        != canonical_request_hash(expected)
    ):
        raise ValueError("Trial base parameters do not preserve fixed overrides.")
    suggestion = specification.get("suggestion", specification.get("parameters", {}))
    if not isinstance(suggestion, dict):
        raise ValueError("Trial suggestion must be an object.")
    if any(
        key in suggestion
        and canonical_request_hash({"value": suggestion[key]})
        != canonical_request_hash({"value": value})
        for name, value in expected.items()
        for key in (name, f"model__{name}")
    ):
        raise ValueError("Trial suggestion changes a fixed override.")
