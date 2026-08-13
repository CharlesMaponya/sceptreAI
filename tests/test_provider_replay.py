from __future__ import annotations

from automl_shared.tune_search import DeterministicSkoptSearch, SearchDimension


def run(provider: str) -> dict[str, object]:
    searcher = DeterministicSkoptSearch(
        {
            "recipe": SearchDimension.categorical(["raw", "safe_engineered"]),
            "x": SearchDimension.real(0.0, 1.0),
        },
        metric="quality",
        mode="max",
        seed=42,
        max_concurrent=2,
        max_suggestions=4,
        objective_decimal_places=6,
    )
    for batch in range(2):
        trials = [f"{provider}-{batch}-{slot}" for slot in range(2)]
        configs = [searcher.suggest(trial) for trial in trials]
        for trial, config, jitter in zip(trials, configs, [0.0000002, -0.0000002], strict=True):
            assert isinstance(config, dict)
            score = 1 - abs(float(config["x"]) - 0.65) + jitter
            searcher.on_trial_complete(trial, {"quality": score})
    return searcher.canonical_suggestion_log()


def test_canonical_suggestion_log_replays_across_all_providers() -> None:
    logs = [run(provider) for provider in ("aws-eks", "gcp-gke", "azure-aks")]
    assert logs[0] == logs[1] == logs[2]

