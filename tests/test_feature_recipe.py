from __future__ import annotations

import json

import pyarrow as pa
import pytest
from automl_shared.feature_recipe import (
    FeatureRecipe,
    RatioFeature,
    authenticate_recipe,
    verify_recipe_authentication,
)


def _recipe() -> FeatureRecipe:
    return FeatureRecipe.build(
        input_columns=["income", "debt", "age"],
        target_column="approved",
        excluded_leakage_columns=["approved_copy", "decision_status"],
        ratio_features=[RatioFeature("debt_to_income", "debt", "income")],
    )


def test_recipe_is_deterministic_metadata_only_and_round_trips() -> None:
    recipe = _recipe()
    metadata = recipe.metadata_view(
        {"approved": "bool", "age": "int64", "debt": "float64", "income": "float64"}
    )

    assert metadata == {
        "recipe_digest": recipe.digest,
        "input_schema": [
            ["income", "float64"],
            ["debt", "float64"],
            ["age", "int64"],
        ],
        "output_columns": ["income", "debt", "age", "debt_to_income"],
        "excluded_column_count": 2,
        "value_accessed": False,
    }
    assert FeatureRecipe.from_dict(json.loads(json.dumps(recipe.to_dict()))) == recipe
    assert FeatureRecipe.from_dict(recipe.to_dict()).digest == recipe.digest


def test_bounded_polars_transform_has_training_inference_parity() -> None:
    recipe = _recipe()
    source = pa.table(
        {
            "income": [100.0, 0.0, None, float("inf")],
            "debt": [25.0, 10.0, 5.0, 1.0],
            "age": [30, 40, 50, 60],
            "approved": [True, False, True, False],
            "approved_copy": [True, False, True, False],
        }
    )

    training = recipe.transform_arrow(source, max_batch_rows=4)
    inference = FeatureRecipe.from_dict(recipe.to_dict()).transform_arrow(
        source,
        max_batch_rows=4,
    )

    assert training.equals(inference)
    assert training.column_names == ["income", "debt", "age", "debt_to_income"]
    assert training.column("debt_to_income").to_pylist() == [0.25, None, None, None]
    assert training.num_rows == source.num_rows


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"input_columns": []}, "nonempty"),
        ({"input_columns": ["age", "age"]}, "unique"),
        ({"input_columns": ["approved"]}, "target"),
        (
            {"input_columns": ["approved_copy"], "excluded_leakage_columns": ["approved_copy"]},
            "Leakage-excluded",
        ),
        ({"ratio_features": [RatioFeature("age", "age", "income")]}, "names"),
        ({"ratio_features": [RatioFeature("ratio", "age", "unknown")]}, "approved input"),
    ],
)
def test_recipe_rejects_invalid_or_leaking_contracts(
    kwargs: dict[str, object],
    message: str,
) -> None:
    options: dict[str, object] = {
        "input_columns": ["income", "age"],
        "target_column": "approved",
    }
    options.update(kwargs)
    with pytest.raises(ValueError, match=message):
        FeatureRecipe.build(**options)  # type: ignore[arg-type]


def test_recipe_rejects_unknown_revision_fields_and_missing_schema() -> None:
    recipe = _recipe()
    with pytest.raises(ValueError, match="revision"):
        FeatureRecipe.from_dict({**recipe.to_dict(), "revision": "future"})
    with pytest.raises(ValueError, match="unsupported fields"):
        FeatureRecipe.from_dict({**recipe.to_dict(), "extra": True})
    with pytest.raises(ValueError, match="absent from the schema"):
        recipe.metadata_view({"income": "float64"})


def test_transform_rejects_unbounded_or_incoherent_batches() -> None:
    recipe = _recipe()
    batch = pa.table({"income": [1.0], "debt": [0.5], "age": [30]})
    with pytest.raises(ValueError, match="positive"):
        recipe.transform_arrow(batch, max_batch_rows=0)
    oversized = pa.table({"income": [1.0, 2.0], "debt": [0.5, 1.0], "age": [30, 40]})
    with pytest.raises(ValueError, match="recipe limit"):
        recipe.transform_arrow(oversized, max_batch_rows=1)

    incomplete = pa.table({"income": [1.0], "age": [30]})
    with pytest.raises(ValueError, match="absent from the batch"):
        recipe.transform_arrow(incomplete, max_batch_rows=1)


def test_recipe_authentication_fails_closed() -> None:
    recipe = _recipe()
    envelope = authenticate_recipe(recipe, key_id="phase-0a", secret=b"test-key")

    assert verify_recipe_authentication(
        recipe,
        envelope,
        trusted_keys={"phase-0a": b"test-key"},
    )
    assert not verify_recipe_authentication(
        recipe,
        {**envelope, "signature": "0" * 64},
        trusted_keys={"phase-0a": b"test-key"},
    )
    assert not verify_recipe_authentication(
        recipe,
        {**envelope, "algorithm": "unknown"},
        trusted_keys={"phase-0a": b"test-key"},
    )
    assert not verify_recipe_authentication(
        recipe,
        envelope,
        trusted_keys={"other": b"test-key"},
    )
    assert not verify_recipe_authentication(
        recipe,
        {**envelope, "recipe_digest": "0" * 64},
        trusted_keys={"phase-0a": b"test-key"},
    )
    with pytest.raises(ValueError, match="key ID"):
        authenticate_recipe(recipe, key_id="", secret=b"")
