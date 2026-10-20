"""Small typed request bodies; PostgreSQL tests use the actual preparation publisher."""

import uuid


def feature_specification(**updates):
    body = dict(
        dataset_version_id=uuid.uuid4(),
        prepared_artifact_id=uuid.uuid4(),
        split_revision_id=uuid.uuid4(),
        source_snapshot_digest="a" * 64,
        source_columns=["x", "target"],
        target_column="target",
        allowed_feature_families=["raw_passthrough"],
        disabled_feature_families=["lag", "rolling", "window", "arbitrary_python"],
        budgets=dict(
            maximum_expression_depth=2,
            maximum_output_count=100,
            maximum_cardinality=100_000,
            maximum_batch_rows=8192,
        ),
        final_test_isolated=True,
        value_fit_scope="outer_train_only",
    )
    return {**body, **updates}


def search_specification(**updates):
    return dict(
        metric="rmse",
        direction="minimize",
        feature_recipe_revision_id=uuid.uuid4(),
        recipe_revision="b" * 64,
        allowed_feature_families=["raw_passthrough"],
        **updates,
    )
