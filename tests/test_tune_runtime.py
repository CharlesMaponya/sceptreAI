from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import ray
from automl_api.training.tune_runtime import _dimensions, search_candidate
from sklearn.linear_model import Ridge
from skopt.space import Categorical, Integer, Real


def test_catalog_dimensions_retain_their_types():
    dimensions = _dimensions(
        {
            "a": Real(0.01, 1, prior="log-uniform"),
            "b": Integer(1, 5),
            "c": Categorical([True, False]),
        }
    )
    assert dimensions["a"].prior == "log-uniform"
    assert dimensions["b"].kind == "integer"
    assert dimensions["c"].categories == (True, False)
    with pytest.raises(ValueError, match="Unsupported"):
        _dimensions({"bad": object()})


def test_real_tune_executes_requested_trials_and_persists_cv_evidence(tmp_path):
    # Other preparation tests may have initialized a local Ray runtime.
    ray.shutdown()
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024**2)
    try:
        x = pd.DataFrame({"distance": np.arange(80, dtype=float)})
        y = x["distance"] * 3 + 2
        result = search_candidate(
            Ridge(),
            {"alpha": Real(0.01, 10, prior="log-uniform")},
            x,
            y,
            cv=3,
            scoring="neg_root_mean_squared_error",
            iterations=2,
            storage_path=str(tmp_path),
            max_concurrent=2,
        )
        assert result["trials"] == 2
        assert result["failed_trials"] == 0
        assert result["mean"] <= 0
        assert set(result["params"]) == {"alpha"}
        assert len(result["suggestion_log"]["suggestions"]) == 2
        files = list(tmp_path.rglob("result.json"))
        assert len(files) == 2
        for file in files:
            assert json.loads(file.read_text().splitlines()[-1])["folds"] == 3
    finally:
        ray.shutdown()
