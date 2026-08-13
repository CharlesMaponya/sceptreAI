from __future__ import annotations

import json
import tempfile
from pathlib import Path

import ray
from automl_shared.tune_search import (
    DeterministicSkoptSearch,
    SearchDimension,
    canonical_digest,
)
from ray import tune

TRIAL_COUNT = 4
CONCURRENCY = 2
SEARCH_SEED = 42


def objective(config: dict[str, object]) -> None:
    quality = 1.0 - abs(float(config["x"]) - 0.65)
    tune.report(
        {
            "quality": quality,
            "seed_echo": int(config["sceptre_trial_seed"]),
        }
    )


def build_searcher() -> DeterministicSkoptSearch:
    return DeterministicSkoptSearch(
        {
            "recipe": SearchDimension.categorical(["raw", "safe_engineered"]),
            "x": SearchDimension.real(0.0, 1.0),
        },
        metric="quality",
        mode="max",
        seed=SEARCH_SEED,
        max_concurrent=CONCURRENCY,
        max_suggestions=TRIAL_COUNT,
        objective_decimal_places=8,
    )


def main() -> None:
    ray.init(address="auto")
    with tempfile.TemporaryDirectory(prefix="sceptre-tune-") as temporary:
        searcher = build_searcher()
        results = tune.Tuner(
            objective,
            param_space={},
            tune_config=tune.TuneConfig(
                metric="quality",
                mode="max",
                search_alg=searcher,
                num_samples=TRIAL_COUNT,
                max_concurrent_trials=CONCURRENCY,
            ),
            run_config=tune.RunConfig(
                storage_path=str(Path(temporary).resolve()),
                verbose=0,
            ),
        ).fit()
        if results.errors:
            raise RuntimeError(f"Ray Tune returned {len(results.errors)} failed trials.")
        if len(results) != TRIAL_COUNT:
            raise RuntimeError(f"Expected {TRIAL_COUNT} trials, received {len(results)}.")

        suggestions = [event for event in searcher.events if event["type"] == "suggested"]
        observations = [event for event in searcher.events if event["type"] == "observed"]
        if len(suggestions) != TRIAL_COUNT or len(observations) != TRIAL_COUNT:
            raise RuntimeError("The canonical event log is incomplete.")
        if [event["suggestion_id"] for event in observations] != list(range(TRIAL_COUNT)):
            raise RuntimeError("Search observations were not committed in suggestion order.")

        checkpoint = Path(temporary) / "searcher-checkpoint.json"
        searcher.save(str(checkpoint))
        restored = build_searcher()
        restored.restore(str(checkpoint))
        if restored.state_dict() != searcher.state_dict():
            raise RuntimeError("Restored search state differs from the canonical event log.")

        suggestion_log = searcher.canonical_suggestion_log()
        print(
            json.dumps(
                {
                    "best_quality": results.get_best_result().metrics["quality"],
                    "canonical_suggestion_log_digest": canonical_digest(suggestion_log),
                    "concurrency": CONCURRENCY,
                    "event_count": len(searcher.events),
                    "observed_suggestion_ids": [event["suggestion_id"] for event in observations],
                    "python_version": ".".join(map(str, __import__("sys").version_info[:3])),
                    "ray_node_count": len([node for node in ray.nodes() if node.get("Alive")]),
                    "ray_version": ray.__version__,
                    "recipe_hashes": [
                        event["config"]["sceptre_recipe_hash"] for event in suggestions
                    ],
                    "restore_matches": True,
                    "search_seed": SEARCH_SEED,
                    "status": "passed",
                    "trial_count": len(results),
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
