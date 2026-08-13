from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

import polars as pl
import pyarrow as pa

RECIPE_REVISION = "sceptre-polars-feature-recipe-v1"
SIGNATURE_ALGORITHM = "hmac-sha256"


@dataclass(frozen=True)
class RatioFeature:
    name: str
    numerator: str
    denominator: str


@dataclass(frozen=True)
class FeatureRecipe:
    input_columns: tuple[str, ...]
    target_column: str
    excluded_leakage_columns: tuple[str, ...] = ()
    ratio_features: tuple[RatioFeature, ...] = ()
    revision: str = RECIPE_REVISION

    def __post_init__(self) -> None:
        inputs = set(self.input_columns)
        excluded = set(self.excluded_leakage_columns)
        if self.revision != RECIPE_REVISION:
            raise ValueError("Unsupported feature recipe revision.")
        if not self.input_columns or len(inputs) != len(self.input_columns):
            raise ValueError("Feature recipe inputs must be nonempty and unique.")
        if self.target_column in inputs:
            raise ValueError("The target column cannot be a feature recipe input.")
        leaked_inputs = inputs & excluded
        if leaked_inputs:
            raise ValueError(
                "Leakage-excluded columns cannot be recipe inputs: "
                + ", ".join(sorted(leaked_inputs))
            )
        output_names = set(self.input_columns)
        for feature in self.ratio_features:
            if not feature.name or feature.name in output_names:
                raise ValueError("Derived feature names must be nonempty and unique.")
            if feature.numerator not in inputs or feature.denominator not in inputs:
                raise ValueError("Ratio features may reference only approved input columns.")
            output_names.add(feature.name)

    @classmethod
    def build(
        cls,
        *,
        input_columns: Sequence[str],
        target_column: str,
        excluded_leakage_columns: Sequence[str] = (),
        ratio_features: Sequence[RatioFeature] = (),
    ) -> FeatureRecipe:
        return cls(
            input_columns=tuple(input_columns),
            target_column=target_column,
            excluded_leakage_columns=tuple(sorted(set(excluded_leakage_columns))),
            ratio_features=tuple(ratio_features),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "input_columns": list(self.input_columns),
            "target_column": self.target_column,
            "excluded_leakage_columns": list(self.excluded_leakage_columns),
            "ratio_features": [asdict(feature) for feature in self.ratio_features],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> FeatureRecipe:
        allowed = {
            "revision",
            "input_columns",
            "target_column",
            "excluded_leakage_columns",
            "ratio_features",
        }
        if set(payload) - allowed:
            raise ValueError("Feature recipe contains unsupported fields.")
        return cls(
            revision=str(payload.get("revision", "")),
            input_columns=tuple(str(value) for value in payload.get("input_columns", ())),
            target_column=str(payload.get("target_column", "")),
            excluded_leakage_columns=tuple(
                str(value) for value in payload.get("excluded_leakage_columns", ())
            ),
            ratio_features=tuple(
                RatioFeature(
                    name=str(item["name"]),
                    numerator=str(item["numerator"]),
                    denominator=str(item["denominator"]),
                )
                for item in payload.get("ratio_features", ())
            ),
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    @property
    def output_columns(self) -> tuple[str, ...]:
        return (*self.input_columns, *(feature.name for feature in self.ratio_features))

    def metadata_view(self, schema: Mapping[str, str]) -> dict[str, Any]:
        missing = [column for column in self.input_columns if column not in schema]
        if missing:
            raise ValueError(
                "Recipe input columns are absent from the schema: " + ", ".join(missing)
            )
        return {
            "recipe_digest": self.digest,
            "input_schema": [[column, str(schema[column])] for column in self.input_columns],
            "output_columns": list(self.output_columns),
            "excluded_column_count": len(self.excluded_leakage_columns),
            "value_accessed": False,
        }

    def transform_arrow(self, batch: pa.Table, *, max_batch_rows: int) -> pa.Table:
        if max_batch_rows < 1:
            raise ValueError("max_batch_rows must be positive.")
        if batch.num_rows > max_batch_rows:
            raise ValueError(
                f"Arrow batch has {batch.num_rows} rows; recipe limit is {max_batch_rows}."
            )
        missing = [column for column in self.input_columns if column not in batch.column_names]
        if missing:
            raise ValueError(
                "Recipe input columns are absent from the batch: " + ", ".join(missing)
            )
        frame = pl.from_arrow(batch.select(self.input_columns))
        if not isinstance(frame, pl.DataFrame):
            frame = frame.to_frame()
        expressions: list[pl.Expr] = []
        for feature in self.ratio_features:
            numerator = pl.col(feature.numerator).cast(pl.Float64, strict=False)
            denominator = pl.col(feature.denominator).cast(pl.Float64, strict=False)
            expressions.append(
                pl.when(
                    numerator.is_finite()
                    & denominator.is_finite()
                    & denominator.is_not_null()
                    & (denominator != 0)
                )
                .then(numerator / denominator)
                .otherwise(None)
                .alias(feature.name)
            )
        if expressions:
            frame = frame.with_columns(expressions)
        return frame.select(self.output_columns).to_arrow()


def authenticate_recipe(recipe: FeatureRecipe, *, key_id: str, secret: bytes) -> dict[str, str]:
    if not key_id or not secret:
        raise ValueError("Recipe authentication requires a key ID and nonempty secret.")
    signature = hmac.new(secret, recipe.digest.encode("ascii"), hashlib.sha256).hexdigest()
    return {
        "algorithm": SIGNATURE_ALGORITHM,
        "key_id": key_id,
        "recipe_digest": recipe.digest,
        "signature": signature,
    }


def verify_recipe_authentication(
    recipe: FeatureRecipe,
    envelope: Mapping[str, str],
    *,
    trusted_keys: Mapping[str, bytes],
) -> bool:
    if envelope.get("algorithm") != SIGNATURE_ALGORITHM:
        return False
    key = trusted_keys.get(str(envelope.get("key_id", "")))
    if not key or envelope.get("recipe_digest") != recipe.digest:
        return False
    expected = hmac.new(key, recipe.digest.encode("ascii"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, str(envelope.get("signature", "")))


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
