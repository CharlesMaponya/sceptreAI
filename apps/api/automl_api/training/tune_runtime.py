"""Execute candidate CV trials on Ray workers with durable Tune search evidence."""

from __future__ import annotations

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
            result[name] = SearchDimension.categorical(dimension.categories)
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


def _cv_trial(config, *, model, features, target, cv, scoring):
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
    tune.report(
        {"cv_score": float(np.mean(scores)), "cv_std": float(np.std(scores)), "folds": len(scores)}
    )

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
) -> dict[str, Any]:
    searcher = DeterministicSkoptSearch(
        _dimensions(search_space),
        metric="cv_score",
        mode="max",
        seed=42,
        max_concurrent=max_concurrent,
        max_suggestions=iterations,
    )
    trainable = tune.with_resources(
        tune.with_parameters(
            _cv_trial, model=model, features=features, target=target, cv=cv, scoring=scoring
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
