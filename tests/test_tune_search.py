from __future__ import annotations

import json

import pytest
from automl_shared.tune_search import (
    CHECKPOINT_REVISION,
    DeterministicSkoptSearch,
    SearchDimension,
    canonical_digest,
)
from ray.tune.search import Searcher


def _searcher(**overrides: object) -> DeterministicSkoptSearch:
    options = {
        "metric": "quality",
        "mode": "max",
        "seed": 90210,
        "max_concurrent": 2,
        "max_suggestions": 4,
        "objective_decimal_places": 4,
    }
    options.update(overrides)
    return DeterministicSkoptSearch(
        {
            "depth": SearchDimension.integer(2, 8),
            "learning_rate": SearchDimension.real(0.01, 0.2, prior="log-uniform"),
            "recipe": SearchDimension.categorical(["raw", "safe_engineered"]),
        },
        **options,
    )


def test_typed_dimensions_validate_and_round_trip() -> None:
    integer = SearchDimension.integer(1, 3)
    real = SearchDimension.real(0.1, 1.0)
    category = SearchDimension.categorical(["a", None, True])

    assert integer.to_skopt().rvs(random_state=1)[0] in {1, 2, 3}
    assert 0.1 <= real.to_skopt().rvs(random_state=1)[0] <= 1.0
    assert category.to_skopt().categories == ("a", None, True)
    assert SearchDimension.from_dict(category.to_dict()) == category

    with pytest.raises(ValueError, match="at least one"):
        SearchDimension.categorical([]).to_skopt()
    with pytest.raises(ValueError, match="ordered"):
        SearchDimension.real(2, 1).to_skopt()
    with pytest.raises(TypeError, match="integers"):
        SearchDimension(kind="integer", low=1.5, high=2).to_skopt()
    with pytest.raises(ValueError, match="Unsupported"):
        SearchDimension(kind="mystery").to_skopt()  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unsupported fields"):
        SearchDimension.from_dict({"kind": "real", "surprise": 1})


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"dimensions": {}}, "At least one"),
        ({"dimensions": {"sceptre_bad": SearchDimension.integer(1, 2)}}, "cannot start"),
        ({"mode": "sideways"}, "mode must"),
        ({"max_concurrent": 0}, "positive"),
        ({"max_suggestions": 0}, "positive"),
        ({"objective_decimal_places": 16}, "between 0 and 15"),
        ({"failure_score": float("inf")}, "finite"),
    ],
)
def test_searcher_rejects_invalid_contract(kwargs: dict[str, object], message: str) -> None:
    dimensions = kwargs.pop("dimensions", {"x": SearchDimension.integer(1, 2)})
    options = {"metric": "quality", "mode": "max", "seed": 1, **kwargs}
    with pytest.raises(ValueError, match=message):
        DeterministicSkoptSearch(dimensions, **options)  # type: ignore[arg-type]


def test_concurrent_results_are_observed_in_suggestion_order() -> None:
    searcher = _searcher()
    first = searcher.suggest("trial-a")
    second = searcher.suggest("trial-b")

    assert isinstance(first, dict) and isinstance(second, dict)
    assert {
        (first["depth"], first["learning_rate"], first["recipe"]),
        (second["depth"], second["learning_rate"], second["recipe"]),
    }.__len__() == 2
    assert searcher.suggest("trial-a") == first
    assert searcher.suggest("blocked") is None
    assert first["sceptre_suggestion_id"] == 0
    assert second["sceptre_suggestion_id"] == 1
    assert first["sceptre_recipe_hash"] == canonical_digest(
        {
            "parameters": {
                key: value for key, value in first.items() if not key.startswith("sceptre_")
            },
            "seed": first["sceptre_trial_seed"],
            "suggestion_id": 0,
        }
    )

    searcher.on_trial_complete("trial-b", {"quality": 0.812345})
    assert not [event for event in searcher.events if event["type"] == "observed"]
    assert searcher.suggest("still-blocked") is None
    searcher.on_trial_complete("trial-a", {"quality": 0.712345})

    observed = [event for event in searcher.events if event["type"] == "observed"]
    assert [event["trial_id"] for event in observed] == ["trial-a", "trial-b"]
    assert [event["objective"] for event in observed] == [-0.7123, -0.8123]
    assert isinstance(searcher.suggest("trial-c"), dict)


def test_result_waits_until_every_point_in_the_batch_is_assigned() -> None:
    searcher = _searcher()
    first = searcher.suggest("trial-a")
    searcher.on_trial_complete("trial-a", {"quality": 0.7})

    assert not [event for event in searcher.events if event["type"] == "observed"]
    second = searcher.suggest("trial-b")
    assert isinstance(first, dict) and isinstance(second, dict)
    searcher.on_trial_complete("trial-b", {"quality": 0.8})
    assert [event["suggestion_id"] for event in searcher.events if event["type"] == "observed"] == [
        0,
        1,
    ]


def test_save_restore_and_retry_preserve_the_search_sequence(tmp_path) -> None:
    original = _searcher()
    first = original.suggest("trial-a")
    second = original.suggest("trial-b")
    original.on_trial_complete("trial-b", {"quality": 0.8})
    checkpoint = tmp_path / "search.json"
    original.save(str(checkpoint))

    restored = _searcher(seed=7)
    restored.restore(str(checkpoint))
    assert restored.suggest("trial-a") == first
    assert restored.suggest("trial-b") == second
    restored.on_trial_complete("trial-a", {"quality": 0.7})
    original.on_trial_complete("trial-a", {"quality": 0.7})
    assert restored.suggest("trial-c") == original.suggest("trial-c")
    assert restored.state_dict() == original.state_dict()
    assert restored.canonical_suggestion_log() == original.canonical_suggestion_log()


def test_canonical_suggestion_log_excludes_physical_trial_ids() -> None:
    first = _searcher(max_concurrent=1, max_suggestions=2)
    second = _searcher(max_concurrent=1, max_suggestions=2)
    for index in range(2):
        first.suggest(f"provider-a-{index}")
        first.on_trial_complete(f"provider-a-{index}", {"quality": 0.5 + index / 10})
        second.suggest(f"provider-b-{index}")
        second.on_trial_complete(f"provider-b-{index}", {"quality": 0.5 + index / 10})

    assert first.canonical_suggestion_log() == second.canonical_suggestion_log()
    assert "trial_id" not in json.dumps(first.canonical_suggestion_log())


def test_failure_cancellation_idempotency_and_finish_contract() -> None:
    searcher = _searcher(max_concurrent=1, max_suggestions=3)
    searcher.suggest("failed")
    searcher.on_trial_complete("failed", error=True)
    searcher.on_trial_complete("failed", error=True)
    searcher.suggest("cancelled")
    searcher.cancel_trial("cancelled")
    searcher.suggest("missing-metric")
    searcher.on_trial_complete("missing-metric", {})

    assert searcher.suggest("finished") == Searcher.FINISHED
    statuses = [event["status"] for event in searcher.events if event["type"] == "result_received"]
    assert statuses == ["failed", "cancelled", "cancelled"]
    assert searcher.set_search_properties("x", "min", {}) is False

    with pytest.raises(ValueError, match="Conflicting"):
        searcher.on_trial_complete("failed", {"quality": 1})
    with pytest.raises(KeyError, match="Unknown"):
        searcher.cancel_trial("unknown")


@pytest.mark.parametrize("value", ["not-a-number", float("nan"), float("inf")])
def test_invalid_objectives_are_rejected(value: object) -> None:
    searcher = _searcher(max_concurrent=1)
    searcher.suggest("trial")
    with pytest.raises(ValueError, match="numeric|finite"):
        searcher.on_trial_complete("trial", {"quality": value})


def test_checkpoint_rejects_tampering_and_invalid_events(tmp_path) -> None:
    searcher = _searcher(max_concurrent=1)
    searcher.suggest("trial")
    searcher.on_trial_complete("trial", {"quality": 0.5})
    state = searcher.state_dict()

    with pytest.raises(ValueError, match="revision"):
        DeterministicSkoptSearch.from_state_dict({**state, "revision": "future"})
    with pytest.raises(ValueError, match="dimensions"):
        DeterministicSkoptSearch.from_state_dict({**state, "dimensions": None})
    with pytest.raises(ValueError, match="events"):
        DeterministicSkoptSearch.from_state_dict({**state, "events": None})

    tampered = json.loads(json.dumps(state))
    result = next(event for event in tampered["events"] if event["type"] == "result_received")
    result["score"] = 0.9
    with pytest.raises(ValueError, match="digest"):
        DeterministicSkoptSearch.from_state_dict(tampered)

    bad_result_lineage = json.loads(json.dumps(state))
    result = next(
        event for event in bad_result_lineage["events"] if event["type"] == "result_received"
    )
    result["suggestion_id"] = 99
    result["result_digest"] = canonical_digest(
        {key: value for key, value in result.items() if key != "result_digest"}
    )
    with pytest.raises(ValueError, match="suggestion lineage"):
        DeterministicSkoptSearch.from_state_dict(bad_result_lineage)

    bad_observation_lineage = json.loads(json.dumps(state))
    observation = next(
        event for event in bad_observation_lineage["events"] if event["type"] == "observed"
    )
    observation["suggestion_id"] = 99
    with pytest.raises(ValueError, match="wrong suggestion"):
        DeterministicSkoptSearch.from_state_dict(bad_observation_lineage)

    divergent = json.loads(json.dumps(state))
    divergent["events"][0]["point"][0] = 999
    with pytest.raises(ValueError, match="diverged"):
        DeterministicSkoptSearch.from_state_dict(divergent)

    bad_config = json.loads(json.dumps(state))
    bad_config["events"][0]["config"]["depth"] = 999
    with pytest.raises(ValueError, match="config"):
        DeterministicSkoptSearch.from_state_dict(bad_config)

    invalid = {**state, "events": [{"type": "unknown", "trial_id": "x"}]}
    with pytest.raises(ValueError, match="Unsupported search event"):
        DeterministicSkoptSearch.from_state_dict(invalid)

    checkpoint = tmp_path / "nested" / "search.json"
    searcher.save(str(checkpoint))
    assert json.loads(checkpoint.read_text())["revision"] == CHECKPOINT_REVISION
