"""Execute candidate CV trials on Ray workers with durable Tune search evidence."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
from automl_shared.tune_search import DeterministicSkoptSearch, SearchDimension
from ray import tune
from sklearn.base import clone
from sklearn.model_selection import cross_val_score
from skopt.space import Categorical, Integer, Real


def _dimensions(space: dict[str, Any]) -> dict[str, SearchDimension]:
    result = {}
    for name, dimension in space.items():
        if isinstance(dimension, Categorical):
            result[name] = SearchDimension.categorical(
                [
                    value.item() if isinstance(value, np.generic) else value
                    for value in dimension.categories
                ]
            )
        elif isinstance(dimension, Integer):
            result[name] = SearchDimension.integer(
                int(dimension.low), int(dimension.high), prior=dimension.prior
            )
        elif isinstance(dimension, Real):
            result[name] = SearchDimension.real(
                float(dimension.low), float(dimension.high), prior=dimension.prior
            )
        else:
            raise ValueError(f"Unsupported search dimension for {name}.")
    return result


def _cv_result(config, *, model, features, target, cv, scoring):
    parameters = {name: value for name, value in config.items() if not name.startswith("sceptre_")}
    scores = cross_val_score(
        clone(model).set_params(**parameters),
        features,
        target,
        cv=cv,
        scoring=scoring,
        n_jobs=1,
        error_score="raise",
    )
    return {
        "cv_score": float(np.mean(scores)),
        "cv_std": float(np.std(scores)),
        "folds": len(scores),
    }


def _tune_identity(config, execution):
    return f"tune:{config['sceptre_suggestion_id']}", {
        "search": execution["specification"],
        "suggestion": config,
    }


class _DurableTuneSearch(DeterministicSkoptSearch):
    def __init__(self, dimensions, *, execution=None, **kwargs):
        super().__init__(dimensions, **kwargs)
        self.execution = execution

    def suggest(self, trial_id):
        config = super().suggest(trial_id)
        if isinstance(config, dict) and self.execution is not None:
            from automl_api.training.trial_runtime import enroll_evaluation

            suggestion_id, parameters = _tune_identity(config, self.execution)
            enroll_evaluation(
                self.execution["run"],
                self.execution["candidate_name"],
                suggestion_id,
                parameters,
                self.execution["parent"],
                dispatch_id=trial_id,
            )
        return config

    def restore(self, checkpoint_path):
        # Restore algorithm state without replacing the current parent's execution context.
        restored = DeterministicSkoptSearch.from_state_dict(
            json.loads(Path(checkpoint_path).read_text())
        )
        self.__dict__.update(restored.__dict__)


def _cv_trial(config, *, model, features, target, cv, scoring, execution=None):
    from automl_api.training.trial_runtime import evaluate_trial, trial_parent_context

    def compute():
        return _cv_result(
            config, model=model, features=features, target=target, cv=cv, scoring=scoring
        )

    if execution is None:
        if trial_parent_context() is not None:
            raise RuntimeError("Fenced Tune workers require enrolled execution context.")
        result = compute()
    else:
        suggestion_id, parameters = _tune_identity(config, execution)
        result = evaluate_trial(
            execution["run"],
            execution["candidate_name"],
            suggestion_id,
            parameters,
            compute,
            parent_context=execution["parent"],
        )
    tune.report(result)


def search_candidate(
    model,
    search_space,
    features,
    target,
    cv,
    scoring,
    *,
    iterations: int,
    storage_path: str,
    storage_filesystem=None,
    name: str = "search",
    cpus_per_trial: float = 1,
    max_concurrent: int = 2,
    run=None,
    candidate_name=None,
    base_parameters=None,
) -> dict[str, Any]:
    from automl_api.services.workflow_state import StaleFence
    from automl_api.training.trial_runtime import trial_parent_context

    parent = trial_parent_context()
    if parent is not None and (run is None or not candidate_name or base_parameters is None):
        raise StaleFence("Fenced Tune dispatch requires a durable run and candidate contract.")
    searcher = _DurableTuneSearch(
        _dimensions(search_space),
        metric="cv_score",
        mode="max",
        seed=42,
        max_concurrent=max_concurrent,
        max_suggestions=iterations,
    )
    execution = None
    if parent is not None:
        execution = {
            "run": SimpleNamespace(id=run.id, project_id=run.project_id),
            "candidate_name": candidate_name,
            "parent": parent,
            "specification": {
                "search_revision": searcher.canonical_suggestion_log()["revision"],
                "seed": searcher.seed,
                "dimensions": {key: value.to_dict() for key, value in searcher.dimensions.items()},
                "iterations": iterations,
                "cv": repr(cv),
                "scoring": scoring,
                "base_parameters": base_parameters,
                "max_concurrent": max_concurrent,
                "cpus_per_trial": cpus_per_trial,
            },
        }
        searcher.execution = execution
    trainable = tune.with_resources(
        tune.with_parameters(
            _cv_trial,
            model=model,
            features=features,
            target=target,
            cv=cv,
            scoring=scoring,
            execution=execution,
        ),
        {"cpu": cpus_per_trial},
    )
    results = tune.Tuner(
        trainable,
        tune_config=tune.TuneConfig(num_samples=iterations, search_alg=searcher),
        run_config=tune.RunConfig(
            name=name,
            storage_path=storage_path,
            storage_filesystem=storage_filesystem,
            verbose=0,
            failure_config=tune.FailureConfig(max_failures=0),
        ),
    ).fit()
    for result in results:
        error = result.error
        from ray.exceptions import RayTaskError

        if isinstance(error, RayTaskError):
            error = error.as_instanceof_cause()
        if isinstance(error, StaleFence):
            raise error
    successful = [
        result
        for result in results
        if result.error is None and np.isfinite(result.metrics.get("cv_score", np.nan))
    ]
    if not successful:
        raise RuntimeError("Every Ray Tune trial failed; inspect the persisted trial logs.")
    best = max(successful, key=lambda result: result.metrics["cv_score"])
    parameters = {
        name: value for name, value in best.config.items() if not name.startswith("sceptre_")
    }
    return {
        "params": parameters,
        "mean": float(best.metrics["cv_score"]),
        "std": float(best.metrics["cv_std"]),
        "trials": len(results),
        "failed_trials": len(results) - len(successful),
        "suggestion_log": searcher.canonical_suggestion_log(),
    }


def search_candidate_local(
    model,
    search_space,
    features,
    target,
    cv,
    scoring,
    *,
    run,
    candidate_name,
    iterations,
    base_parameters,
    accelerator="cpu",
    fallback_from=None,
):
    from automl_api.training.trial_runtime import evaluate_trial

    dimensions = _dimensions(search_space)
    searcher = DeterministicSkoptSearch(
        dimensions,
        metric="cv_score",
        mode="max",
        seed=42,
        max_concurrent=1,
        max_suggestions=iterations,
    )
    specification = {
        "search_revision": searcher.canonical_suggestion_log()["revision"],
        "seed": searcher.seed,
        "dimensions": {key: value.to_dict() for key, value in sorted(dimensions.items())},
        "iterations": iterations,
        "cv": repr(cv),
        "scoring": scoring,
        "base_parameters": base_parameters,
    }
    if accelerator != "cpu" or fallback_from is not None:
        specification.update(accelerator=accelerator, fallback_from=fallback_from)
    prefix = (
        "cpu-fallback:"
        if fallback_from is not None
        else (f"{accelerator}:" if accelerator != "cpu" else "")
    )
    best = None
    for index in range(iterations):
        trial_id = f"{prefix}search:{index}"
        config = searcher.suggest(trial_id)
        if not isinstance(config, dict):
            raise RuntimeError("The local search did not produce its expected suggestion.")
        result = evaluate_trial(
            run,
            candidate_name,
            trial_id,
            {"search": specification, "suggestion": config},
            lambda trial_config=config: _cv_result(
                trial_config,
                model=model,
                features=features,
                target=target,
                cv=cv,
                scoring=scoring,
            ),
        )
        searcher.on_trial_complete(trial_id, result=result)
        if best is None or result["cv_score"] > best[1]["cv_score"]:
            best = (config, result)
    return {
        "engine": "local_durable",
        "params": {key: value for key, value in best[0].items() if not key.startswith("sceptre_")},
        "mean": best[1]["cv_score"],
        "std": best[1]["cv_std"],
        "trials": iterations,
        "failed_trials": 0,
        "suggestion_log": searcher.canonical_suggestion_log(),
    }
