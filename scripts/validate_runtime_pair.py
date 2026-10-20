"""Check actual API/CPU images before publishing release tags."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

REPORT_PREFIX = "RUNTIME_PAIR_REPORT="
IMAGE_PROBE = r"""
import copy, hashlib, json, os, pickle, sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.base import clone
from skopt import BayesSearchCV
from automl_api.models.enums import TaskType
from automl_api.training import catalog_contract as catalog, model_catalog
from automl_api.training.attempt_admission import TRAINING_PROTOCOL
from automl_api.training.parameter_overrides import fixed_candidate, merge_fixed
from automl_api.training.runtime_profiles import runtime_identity

profile = sys.argv[1]
assert os.environ.get("SCEPTRE_RUNTIME_PROFILE") == profile
identity = runtime_identity(
    catalog._RUNTIME_DEPENDENCIES, profile=profile, environment="production"
)
tasks = (TaskType.CLASSIFICATION, TaskType.REGRESSION, TaskType.TIME_SERIES, TaskType.CLUSTERING)
catalogs = {task.value: catalog.catalog_specification(task) for task in tasks}
for task in tasks:
    spec = catalogs[task.value]
    catalog.validate_catalog_revision(
        SimpleNamespace(specification=spec, content_digest=catalog.catalog_digest(spec)), task
    )
if profile == "api":
    import automl_api.main
fits = []
controls = {}
if profile == "cpu":
    from automl_api.training import pipeline, worker
    assert callable(worker.main)
    emitted = json.loads(Path(sys.argv[2]).read_text() if len(sys.argv) > 2 else sys.stdin.read())
    for task in tasks:
        spec = emitted[task.value]
        catalog.validate_catalog_revision(
            SimpleNamespace(specification=spec, content_digest=catalog.catalog_digest(spec)), task
        )
    changed = copy.deepcopy(emitted["regression"])
    changed["runtime_lock_digest"] = "0" * 64
    try:
        catalog.validate_catalog_revision(
            SimpleNamespace(specification=changed, content_digest=catalog.catalog_digest(changed)),
            TaskType.REGRESSION,
        )
    except ValueError:
        controls["changed_catalog_denied"] = True
    else:
        raise AssertionError("incompatible catalog accepted")
    controls["api_catalog_consumed"] = True
    candidate = next(
        c for c in model_catalog.candidate_catalog(TaskType.REGRESSION) if c.name == "Ridge"
    )
    try:
        fixed_candidate(candidate, [{"estimator": "Ridge", "parameters": {"device": "cuda"}}])
    except ValueError:
        controls["unsafe_parameters_denied"] = True
    else:
        raise AssertionError("unsafe parameter accepted")
    overridden, fixed = fixed_candidate(
        candidate, [{"estimator": "Ridge", "parameters": {"alpha": 2.0}}]
    )
    estimator, accelerator = model_catalog.configure_estimator_for_training(
        overridden, cpu_threads=1, gpu_vendor=None
    )
    model = pipeline._supervised_model_pipeline("Ridge", estimator, TaskType.REGRESSION)
    frame = pd.DataFrame({"x": np.arange(36, dtype=float), "z": np.arange(36, dtype=float) % 5})
    target = frame.x * 2 + frame.z
    search = BayesSearchCV(
        model,
        overridden.search_space,
        n_iter=2,
        cv=3,
        scoring="neg_root_mean_squared_error",
        n_jobs=1,
        random_state=42,
        error_score="raise",
    )
    search.fit(frame.iloc[:30], target.iloc[:30])
    parameters = merge_fixed(search.best_params_, fixed)
    fitted = clone(model).set_params(**parameters).fit(frame.iloc[:30], target.iloc[:30])
    predictions = fitted.predict(frame.iloc[30:])
    assert fitted.named_steps["model"].alpha == 2.0 and parameters["model__alpha"] == 2.0
    assert np.isfinite(predictions).all() and accelerator == "cpu"
    restored = pickle.loads(pickle.dumps(fitted, protocol=5))
    np.testing.assert_array_equal(restored.predict(frame.iloc[30:]), predictions)
    assert catalog.catalog_specification(TaskType.REGRESSION) == catalogs[TaskType.REGRESSION.value]
    fits.append(
        {
            "estimator": "Ridge",
            "alpha": float(fitted.named_steps["model"].alpha),
            "best_parameters": parameters,
            "serialization_roundtrip": True,
            "finite_predictions": True,
            "catalog_unchanged": True,
        }
    )
sources = {
    str(p.relative_to("/app")): hashlib.sha256(p.read_bytes()).hexdigest()
    for directory in ("/app/apps/api", "/app/packages")
    for p in Path(directory).rglob("*.py")
}
for name in ("requirements-api.lock", "requirements-training-cpu.lock"):
    path = Path("/app/infra/locks") / name
    sources[str(path.relative_to("/app"))] = hashlib.sha256(path.read_bytes()).hexdigest()
print(
    "RUNTIME_PAIR_REPORT="
    + json.dumps(
        {
            "profile": profile,
            "identity": identity,
            "inventory": catalog._RUNTIME_DEPENDENCIES,
            "catalogs": catalogs,
            "training_protocol": TRAINING_PROTOCOL,
            "fits": fits,
            "controls": controls,
            "source_sha256": sources,
        }
    )
)
"""


def parse_report(output):
    rows = [
        line.removeprefix(REPORT_PREFIX)
        for line in output.splitlines()
        if line.startswith(REPORT_PREFIX)
    ]
    if len(rows) != 1:
        raise ValueError("Image probe must produce exactly one runtime report.")
    return json.loads(rows[0])


def verify_pair(api, cpu, expected_sources):
    assert api["profile"] == "api" and cpu["profile"] == "cpu"
    assert api["inventory"] != cpu["inventory"]
    assert api["identity"] == cpu["identity"]
    assert api["catalogs"] == cpu["catalogs"]
    assert set(api["catalogs"]) == {"classification", "regression", "time_series", "clustering"}
    assert api["source_sha256"] == cpu["source_sha256"] == expected_sources
    assert api["training_protocol"] == cpu["training_protocol"]
    assert all(
        cpu["controls"][key]
        for key in ("changed_catalog_denied", "api_catalog_consumed", "unsafe_parameters_denied")
    )
    assert cpu["fits"] and all(
        fit["estimator"] == "Ridge"
        and fit["alpha"] == 2.0
        and fit["best_parameters"]["model__alpha"] == 2.0
        and fit["catalog_unchanged"]
        and fit["finite_predictions"]
        and fit["serialization_roundtrip"]
        for fit in cpu["fits"]
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-image", required=True)
    parser.add_argument("--cpu-image", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    reports = {}
    for profile, image in (("api", args.api_image), ("cpu", args.cpu_image)):
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--interactive",
                "--network",
                "none",
                "--env",
                "LOCAL_OBJECT_STORE_PATH=/tmp/runtime-pair-objects",
                "--env",
                "OMP_NUM_THREADS=1",
                "--env",
                "OPENBLAS_NUM_THREADS=1",
                "--entrypoint",
                "python",
                image,
                "-c",
                IMAGE_PROBE,
                profile,
            ],
            capture_output=True,
            input=json.dumps(reports["api"]["catalogs"]) if profile == "cpu" else None,
            text=True,
            timeout=600,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                f"{profile} image probe failed: {result.stderr[-8000:]}\n{result.stdout[-2000:]}"
            )
        reports[profile] = parse_report(result.stdout)
    root = Path(__file__).resolve().parents[1]
    sources = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for directory in (root / "apps/api", root / "packages")
        for p in directory.rglob("*.py")
    }
    for name in ("requirements-api.lock", "requirements-training-cpu.lock"):
        path = root / "infra/locks" / name
        sources[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    verify_pair(reports["api"], reports["cpu"], sources)
    if args.output:
        args.output.write_text(json.dumps(reports, indent=2) + "\n")
    print("API_CPU_RUNTIME_PAIR_VERIFIED")


if __name__ == "__main__":
    main()
