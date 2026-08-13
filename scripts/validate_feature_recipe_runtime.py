from __future__ import annotations

import hashlib
import io
import json

import joblib
import numpy as np
import pyarrow as pa
import ray
from automl_shared.feature_recipe import (
    FeatureRecipe,
    RatioFeature,
    authenticate_recipe,
    verify_recipe_authentication,
)
from sklearn.linear_model import LogisticRegression

BATCH_ROWS = 64
ROW_COUNT = 256
SIGNING_KEY = b"phase-0a-disposable-local-signing-key"


def build_recipe() -> FeatureRecipe:
    return FeatureRecipe.build(
        input_columns=["income", "debt", "age"],
        target_column="approved",
        excluded_leakage_columns=["approved_copy", "decision_status"],
        ratio_features=[RatioFeature("debt_to_income", "debt", "income")],
    )


def source_table() -> pa.Table:
    income = np.asarray([40_000 + index * 250 for index in range(ROW_COUNT)], dtype=float)
    debt = np.asarray([500 + (index % 23) * 175 for index in range(ROW_COUNT)], dtype=float)
    age = np.asarray([21 + index % 47 for index in range(ROW_COUNT)], dtype=np.int64)
    approved = ((income / (debt + 1)) + age > 55).astype(np.int64)
    return pa.table(
        {
            "income": income,
            "debt": debt,
            "age": age,
            "approved": approved,
            "approved_copy": approved,
            "decision_status": np.where(approved == 1, "approved", "declined"),
        }
    )


def transform_batch(batch: pa.Table, *, recipe_payload: dict[str, object]) -> pa.Table:
    return FeatureRecipe.from_dict(recipe_payload).transform_arrow(
        batch,
        max_batch_rows=BATCH_ROWS,
    )


def digest_table(table: pa.Table) -> str:
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue()).hexdigest()


def main() -> None:
    ray.init(address="auto")
    recipe = build_recipe()
    authentication = authenticate_recipe(
        recipe,
        key_id="phase-0a-local",
        secret=SIGNING_KEY,
    )
    if not verify_recipe_authentication(
        recipe,
        authentication,
        trusted_keys={"phase-0a-local": SIGNING_KEY},
    ):
        raise RuntimeError("Feature recipe authentication failed.")

    source = source_table()
    dataset = ray.data.from_arrow(source).repartition(4)
    transformed_dataset = dataset.map_batches(
        transform_batch,
        batch_format="pyarrow",
        batch_size=BATCH_ROWS,
        zero_copy_batch=True,
        fn_kwargs={"recipe_payload": recipe.to_dict()},
    )
    transformed = pa.Table.from_pylist([dict(row) for row in transformed_dataset.take_all()])
    if transformed.num_rows != source.num_rows:
        raise RuntimeError("Feature transformation dropped or duplicated rows.")
    if transformed.column_names != list(recipe.output_columns):
        raise RuntimeError("Feature transformation did not enforce the recipe output schema.")
    if set(recipe.excluded_leakage_columns) & set(transformed.column_names):
        raise RuntimeError("Leakage-excluded columns reached the transformed feature table.")

    target = np.asarray(source.column("approved").to_pylist(), dtype=np.int64)
    training_features = transformed.to_pandas()
    model = LogisticRegression(random_state=42, max_iter=500).fit(training_features, target)
    batch_predictions = model.predict(training_features)

    payload = io.BytesIO()
    joblib.dump({"model": model, "recipe": recipe.to_dict()}, payload)
    payload.seek(0)
    bundle = joblib.load(payload)
    restored_recipe = FeatureRecipe.from_dict(bundle["recipe"])
    online_predictions: list[int] = []
    for index in range(source.num_rows):
        record = source.slice(index, 1)
        online_features = restored_recipe.transform_arrow(record, max_batch_rows=1).to_pandas()
        online_predictions.append(int(bundle["model"].predict(online_features)[0]))
    if online_predictions != batch_predictions.tolist():
        raise RuntimeError("Serialized online and batch predictions differ.")

    print(
        json.dumps(
            {
                "batch_rows": BATCH_ROWS,
                "bundle_round_trip": True,
                "excluded_leakage_columns": list(recipe.excluded_leakage_columns),
                "feature_table_digest": digest_table(transformed),
                "metadata_view": recipe.metadata_view(
                    {field.name: str(field.type) for field in source.schema}
                ),
                "output_columns": list(recipe.output_columns),
                "prediction_parity": True,
                "recipe_authenticated": True,
                "recipe_digest": recipe.digest,
                "row_count": source.num_rows,
                "status": "passed",
                "target_or_leakage_in_output": False,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
