from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections import Counter
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.fs as pa_fs
import ray
from automl_shared.data_identity import content_fingerprint, split_role, stable_row_id
from automl_shared.feature_recipe import FeatureRecipe
from ray.data import Dataset
from ray.data.aggregate import Max, Min
from ray.data.datasource import SaveMode
from sqlalchemy import select

from automl_api.db.session import get_session_factory
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import AttemptStatus, DatasetFormat, DatasetStatus, WorkflowStage
from automl_api.models.workflows import (
    DatasetSplitRevision,
    EstimatorCatalogRevision,
    FeatureContractRevision,
    FeatureRecipeRevision,
    FeatureRegistryRevision,
    FeatureSearchSpaceRevision,
    PreparedArtifact,
    WorkflowAttempt,
    WorkflowCommand,
)
from automl_api.schemas.profiling import ColumnProfileRead
from automl_api.services.profiling import _build_preparation_plan, _infer_task
from automl_api.services.ray_polars_profiling import (
    RAY_BATCH_ROWS,
    _load_dataset,
    _ray_source,
    profile_dataset_with_ray,
)
from automl_api.services.workflow_state import (
    StaleFence,
    cas_register_terminal_artifact,
    enqueue_outbox,
    transition_attempt,
)
from automl_api.storage.object_store import get_object_store

SPLIT_SEED_REVISION = "dataset-content-sha256-v1"
IDENTITY_DIGEST_REVISION = "xor-sha256-row-id-v1"

TASK_METRICS = {
    "classification": {
        "balanced_accuracy": "maximize",
        "accuracy": "maximize",
        "f1_macro": "maximize",
        "f1_weighted": "maximize",
        "roc_auc": "maximize",
        "log_loss": "minimize",
    },
    "regression": {
        "rmse": "minimize",
        "mae": "minimize",
        "mse": "minimize",
        "r2": "maximize",
        "explained_variance": "maximize",
    },
    "time_series": {
        "rmse": "minimize",
        "mae": "minimize",
        "mse": "minimize",
        "r2": "maximize",
        "explained_variance": "maximize",
    },
    "clustering": {
        "silhouette": "maximize",
        "davies_bouldin": "minimize",
        "calinski_harabasz": "maximize",
    },
}


def _attempt_context() -> tuple[uuid.UUID, str]:
    raw_attempt_id = os.getenv("AUTOML_ATTEMPT_ID", "").strip()
    fencing_token = os.getenv("AUTOML_FENCING_TOKEN", "").strip()
    if not raw_attempt_id or not fencing_token:
        raise ValueError("AUTOML_ATTEMPT_ID and AUTOML_FENCING_TOKEN are required.")
    return uuid.UUID(raw_attempt_id), fencing_token


def _begin_attempt(
    profile_job_id: uuid.UUID,
    stage: WorkflowStage,
) -> tuple[uuid.UUID, str]:
    attempt_id, fencing_token = _attempt_context()
    with get_session_factory()() as db:
        attempt = (
            db.query(WorkflowAttempt)
            .filter(WorkflowAttempt.id == attempt_id)
            .with_for_update()
            .one_or_none()
        )
        profile = db.get(ProfilingJob, profile_job_id)
        if (
            attempt is None
            or profile is None
            or attempt.project_id != profile.project_id
            or attempt.dataset_version_id != profile.dataset_version_id
            or attempt.stage != stage
        ):
            raise ValueError("The fenced dataset attempt does not belong to this profile stage.")
        if attempt.fencing_token != fencing_token:
            raise StaleFence("The Ray worker received a stale dataset fence.")
        if attempt.status == AttemptStatus.SUBMITTED:
            transition_attempt(attempt, AttemptStatus.RUNNING, fencing_token=fencing_token)
        elif attempt.status != AttemptStatus.RUNNING:
            raise StaleFence(f"The Ray worker cannot start an attempt in {attempt.status} state.")
        now = datetime.now(UTC)
        attempt.heartbeat_at = now
        profile.status = "running"
        profile.current_stage = stage.value
        profile.started_at = profile.started_at or now
        profile.heartbeat_at = now
        db.commit()
    return attempt_id, fencing_token


def _verify_raw(version: DatasetVersion) -> str:
    if version.content_hash_algorithm.lower() != "sha256":
        raise ValueError("The splitter only accepts SHA-256 verified raw datasets.")
    store = get_object_store()
    metadata = store.stat(version.object_uri)
    if version.byte_size is not None and metadata.byte_size != version.byte_size:
        raise ValueError("Raw dataset byte size changed after upload verification.")
    digest = hashlib.sha256()
    with store.open_stream(version.object_uri) as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != version.content_hash.removeprefix("sha256:").lower():
        raise ValueError("Raw dataset SHA-256 does not match the verified upload digest.")
    return actual


def _attach_identity_batch(
    batch: pa.Table,
    *,
    columns: list[str],
    source_digest: str,
    split_seed: str,
    ordinal_column: str = "id",
) -> pa.Table:
    frame = pl.from_arrow(batch)
    if frame.height > RAY_BATCH_ROWS:
        raise RuntimeError("Ray exceeded the bounded splitter batch size.")
    frame = frame.rename({ordinal_column: "source_ordinal"})
    rows = frame.select(columns).to_dicts()
    fingerprints = [content_fingerprint(row, columns) for row in rows]
    ordinals = [int(value) for value in frame.get_column("source_ordinal")]
    return frame.with_columns(
        pl.Series(
            "row_id",
            [stable_row_id(source_digest, ordinal) for ordinal in ordinals],
        ),
        pl.Series("content_fingerprint", fingerprints),
        pl.Series(
            "split_role",
            [split_role(fingerprint, split_seed) for fingerprint in fingerprints],
        ),
    ).to_arrow()


def _identity_summary_batch(batch: pa.Table, *, split_seed: str = "") -> pa.Table:
    roles = batch.column("split_role").to_pylist()
    row_ids = batch.column("row_id").to_pylist()
    has_fingerprints = "content_fingerprint" in batch.column_names
    fingerprints = batch.column("content_fingerprint").to_pylist() if has_fingerprints else []
    counts: Counter[str] = Counter()
    xor_by_role = {"train": 0, "validation": 0, "final_test": 0}
    xor_all = 0
    role_mismatches = 0
    fingerprint_counts: Counter[str] = Counter()
    for index, (role_value, row_id_value) in enumerate(zip(roles, row_ids, strict=True)):
        role = str(role_value)
        row_id = str(row_id_value)
        value = int.from_bytes(hashlib.sha256(row_id.encode()).digest(), "big")
        counts[role] += 1
        xor_by_role[role] ^= value
        xor_all ^= value
        if has_fingerprints:
            fingerprint = str(fingerprints[index])
            fingerprint_counts[fingerprint] += 1
            # The approved split is a pure function of the content fingerprint, so
            # any row whose stored role differs from the recomputed role proves a
            # cross-role fingerprint overlap. Count violations opaquely instead of
            # letting duplicate records cross roles.
            if split_seed and split_role(fingerprint, split_seed) != role:
                role_mismatches += 1
    return pa.table(
        {
            "summary_json": [
                json.dumps(
                    {
                        "counts": dict(counts),
                        "xor": {role: f"{value:064x}" for role, value in xor_by_role.items()},
                        "xor_all": f"{xor_all:064x}",
                        "role_mismatches": role_mismatches,
                        "duplicate_rows_in_batch": sum(
                            count - 1 for count in fingerprint_counts.values() if count > 1
                        ),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            ]
        }
    )


def _merge_identity_summaries(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {"train": 0, "validation": 0, "final_test": 0}
    xor_by_role = {role: 0 for role in counts}
    xor_all = 0
    role_mismatches = 0
    duplicate_rows = 0
    for row in rows:
        summary = json.loads(str(row["summary_json"]))
        for role in counts:
            counts[role] += int(summary["counts"].get(role, 0))
            xor_by_role[role] ^= int(summary["xor"][role], 16)
        xor_all ^= int(summary["xor_all"], 16)
        role_mismatches += int(summary.get("role_mismatches", 0))
        duplicate_rows += int(summary.get("duplicate_rows_in_batch", 0))
    if role_mismatches:
        raise ValueError(
            "Split evidence rejected the dataset: content fingerprints crossed "
            f"split roles in {role_mismatches} rows."
        )
    return {
        "row_count": sum(counts.values()),
        "split_counts": counts,
        "split_digests": {role: f"{value:064x}" for role, value in xor_by_role.items()},
        "row_set_digest": f"{xor_all:064x}",
        "digest_revision": IDENTITY_DIGEST_REVISION,
        "split_integrity": {
            "cross_role_fingerprint_overlaps": role_mismatches,
            "duplicate_content_rows": duplicate_rows,
        },
    }


def _storage_path(uri: str) -> tuple[str, pa_fs.FileSystem | None]:
    descriptor = get_object_store().dataframe_source(uri)
    return _ray_source(descriptor.path, descriptor.filesystem_options)


def _write_parquet(dataset: Dataset, uri: str, *, partition_cols: list[str] | None = None) -> None:
    path, filesystem = _storage_path(uri)
    dataset.write_parquet(
        path,
        filesystem=filesystem,
        partition_cols=partition_cols,
        max_rows_per_file=250_000,
        mode=SaveMode.OVERWRITE,
    )


def _delete_prefix(uri: str) -> None:
    path, filesystem = _storage_path(uri)
    if filesystem is None:
        filesystem = pa_fs.LocalFileSystem()
    if filesystem.get_file_info(path).type != pa_fs.FileType.NotFound:
        filesystem.delete_dir(path)


def _prefix_size(uri: str) -> int:
    path, filesystem = _storage_path(uri)
    if filesystem is None:
        filesystem = pa_fs.LocalFileSystem()
    return sum(
        info.size
        for info in filesystem.get_file_info(pa_fs.FileSelector(path, recursive=True))
        if info.is_file
    )


def split_dataset(
    version: DatasetVersion,
    profile_job_id: uuid.UUID,
    generation: int,
    target_column: str | None,
    *,
    time_column: str | None = None,
) -> dict[str, Any]:
    source_digest = _verify_raw(version)
    # Ordinals must follow file order regardless of task completion order.
    ray.data.DataContext.get_current().execution_options.preserve_order = True
    block_count = min(
        128,
        max(1, math.ceil(int(version.byte_size or 0) / (128 * 1024 * 1024))),
    )
    source = _load_dataset(version, override_num_blocks=block_count)
    row_count = source.count()
    columns = [str(name) for name in source.schema().names]
    if not columns:
        raise ValueError("The raw dataset has no columns.")
    if target_column and target_column not in columns:
        raise ValueError(f"Target column '{target_column}' is absent from the raw dataset.")
    reserved = {
        "row_id",
        "source_ordinal",
        "content_fingerprint",
        "split_role",
        "__sceptre_ordinal__",
    }
    if reserved.intersection(columns):
        raise ValueError(
            "Rename reserved preparation columns: "
            + ", ".join(sorted(reserved.intersection(columns)))
        )
    ordinals = ray.data.range(row_count, override_num_blocks=block_count).rename_columns(
        {"id": "__sceptre_ordinal__"}
    )
    identified = source.zip(ordinals).map_batches(
        _attach_identity_batch,
        batch_format="pyarrow",
        batch_size=RAY_BATCH_ROWS,
        zero_copy_batch=True,
        fn_kwargs={
            "columns": columns,
            "source_digest": source_digest,
            "split_seed": f"{SPLIT_SEED_REVISION}:{source_digest}",
            "ordinal_column": "__sceptre_ordinal__",
        },
    )
    split_strategy = "content_hash"
    boundaries = {}
    if time_column:
        if time_column not in columns or time_column == target_column:
            raise ValueError("Choose a time column separate from the prediction target.")
        if row_count < 20:
            raise ValueError("Chronological preparation requires at least 20 rows.")
        parts = identified.sort(time_column).split_at_indices(
            [int(row_count * 0.7), int(row_count * 0.85)]
        )
        first_time = parts[0].min(time_column)
        validation_start = parts[1].min(time_column)
        final_start = parts[2].min(time_column)
        if first_time is None or validation_start is None or final_start is None:
            raise ValueError("Choose a time column with at least three distinct non-missing times.")
        identified = parts[0].union(*parts[1:])
        # Move a boundary to the start of the timestamp group, never through it.
        # Heavily repeated dates may also require moving to the next distinct time.
        if validation_start <= first_time:
            validation_start = identified.filter(
                lambda row: row[time_column] is not None and row[time_column] > first_time
            ).min(time_column)
        if validation_start is None:
            raise ValueError("Chronological preparation requires at least three distinct times.")
        if final_start <= validation_start:
            final_start = identified.filter(
                lambda row: row[time_column] is not None and row[time_column] > validation_start
            ).min(time_column)
        if final_start is None:
            raise ValueError("Chronological preparation requires at least three distinct times.")
        identified = identified.map_batches(
            _assign_temporal_split_batch,
            batch_format="pyarrow",
            batch_size=RAY_BATCH_ROWS,
            fn_kwargs={
                "time_column": time_column,
                "validation_start": validation_start,
                "final_start": final_start,
            },
        )
        boundary_rows = (
            identified.groupby("split_role")
            .aggregate(Min(time_column), Max(time_column))
            .take_all()
        )
        by_role = {
            row["split_role"]: (row[f"min({time_column})"], row[f"max({time_column})"])
            for row in boundary_rows
        }
        extrema = [
            by_role.get(role, (None, None)) for role in ("train", "validation", "final_test")
        ]
        if any(low is None or high is None for low, high in extrema):
            raise ValueError("Chronological preparation requires at least three distinct times.")
        if not (extrema[0][1] < extrema[1][0] and extrema[1][1] < extrema[2][0]):
            raise ValueError("Chronological split boundaries could not be separated.")
        split_strategy = "temporal"
        boundaries = {
            role: [str(low), str(high)]
            for role, (low, high) in zip(
                ("train", "validation", "final_test"), extrema, strict=True
            )
        }
    root_key = (
        f"automl/projects/{version.project_id}/prepared/{version.id}/"
        f"profiles/{profile_job_id}/generation-{generation}"
    )
    store = get_object_store()
    staging_uri = store.uri_for_key(f"{root_key}/roles")
    _write_parquet(identified, staging_uri, partition_cols=["split_role"])

    train_uri = f"{staging_uri}/split_role=train"
    validation_uri = f"{staging_uri}/split_role=validation"
    final_staging_uri = f"{staging_uri}/split_role=final_test"
    final_path, final_filesystem = _storage_path(final_staging_uri)
    final = ray.data.read_parquet(final_path, filesystem=final_filesystem)
    target = str(target_column or "")
    final_input_uri = store.uri_for_key(f"{root_key}/final-input")
    final_label_uri = store.uri_for_key(f"{root_key}/final-label")
    if target:
        _write_parquet(
            final.drop_columns([target, "content_fingerprint", "source_ordinal"]),
            final_input_uri,
        )
        _write_parquet(final.select_columns(["row_id", target]), final_label_uri)
    else:
        _write_parquet(final, final_input_uri)
        final_label_uri = ""
    staging_path, staging_filesystem = _storage_path(staging_uri)
    split_seed = "" if time_column else f"{SPLIT_SEED_REVISION}:{source_digest}"
    summaries = (
        ray.data.read_parquet(staging_path, filesystem=staging_filesystem)
        .map_batches(
            _identity_summary_batch,
            batch_format="pyarrow",
            batch_size=RAY_BATCH_ROWS,
            zero_copy_batch=True,
            fn_kwargs={"split_seed": split_seed},
        )
        .take_all()
    )
    identity = _merge_identity_summaries([dict(row) for row in summaries])
    _delete_prefix(final_staging_uri)
    uris = {
        "train": train_uri,
        "validation": validation_uri,
        "final_input": final_input_uri,
        "final_label": final_label_uri or None,
    }
    return {
        "source_digest": source_digest,
        "project_id": str(version.project_id),
        "columns": columns,
        "identity": identity,
        "uris": uris,
        "byte_size": sum(_prefix_size(uri) for uri in uris.values() if uri),
        "row_count": identity["row_count"],
        "root_uri": store.uri_for_key(root_key),
        "split_strategy": split_strategy,
        "time_column": time_column,
        "time_boundaries": boundaries,
    }


def _assign_temporal_split_batch(
    batch: pa.Table,
    *,
    time_column: str,
    validation_start: Any,
    final_start: Any,
) -> pa.Table:
    frame = pl.from_arrow(batch)
    if frame.get_column(time_column).null_count():
        raise ValueError(
            "The time column contains missing values. Choose a complete observation time column."
        )
    return frame.with_columns(
        pl.when(pl.col(time_column) < pl.lit(validation_start))
        .then(pl.lit("train"))
        .when(pl.col(time_column) < pl.lit(final_start))
        .then(pl.lit("validation"))
        .otherwise(pl.lit("final_test"))
        .alias("split_role")
    ).to_arrow()


def _assign_split_role(batch: pa.Table, *, role: str) -> pa.Table:
    return pl.from_arrow(batch).with_columns(pl.lit(role).alias("split_role")).to_arrow()


def _next_revision(db: Any, model: Any, version_id: uuid.UUID) -> int:
    rows = db.query(model.revision).filter(model.dataset_version_id == version_id).all()
    return max((int(row[0]) for row in rows), default=0) + 1


def _persist_split(
    profile_job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    fencing_token: str,
    result: dict[str, Any],
) -> None:
    store = get_object_store()
    manifest = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    # Terminal payloads are immutable and attempt-scoped. A superseded worker may
    # finish its upload before the database CAS rejects it; giving every attempt a
    # unique key prevents that stale upload from replacing the active attempt's
    # checkpoint.
    manifest_key = (
        f"automl/projects/{result['project_id']}/profiles/{profile_job_id}/"
        f"attempts/{attempt_id}/split-manifest.json"
    )
    manifest_uri = store.put_bytes(manifest_key, manifest).uri
    with get_session_factory()() as db:
        attempt = (
            db.query(WorkflowAttempt)
            .filter(WorkflowAttempt.id == attempt_id)
            .with_for_update()
            .one()
        )
        profile = db.get(ProfilingJob, profile_job_id)
        if profile is None or attempt.dataset_version_id != profile.dataset_version_id:
            raise ValueError("The splitter completion lineage is missing.")
        digest = hashlib.sha256(manifest).hexdigest()
        revision = _next_revision(db, PreparedArtifact, profile.dataset_version_id)
        prepared = PreparedArtifact(
            project_id=profile.project_id,
            dataset_version_id=profile.dataset_version_id,
            revision=revision,
            digest_scope="split_manifest",
            content_digest=digest,
            specification={**result, "profile_job_id": str(profile.id)},
            object_uri=result["root_uri"],
            byte_size=result["byte_size"],
            row_count=result["row_count"],
            status="ready",
        )
        split = DatasetSplitRevision(
            project_id=profile.project_id,
            dataset_version_id=profile.dataset_version_id,
            revision=_next_revision(db, DatasetSplitRevision, profile.dataset_version_id),
            digest_scope=IDENTITY_DIGEST_REVISION,
            content_digest=digest,
            specification={
                **result["identity"],
                "uris": result["uris"],
                "split_strategy": result.get("split_strategy", "content_hash"),
                "time_column": result.get("time_column"),
                "time_boundaries": result.get("time_boundaries", {}),
            },
            train_digest=result["identity"]["split_digests"]["train"],
            validation_digest=result["identity"]["split_digests"]["validation"],
            final_test_digest=result["identity"]["split_digests"]["final_test"],
            sealed_at=datetime.now(UTC),
        )
        db.add_all([prepared, split])
        db.flush()
        if not cas_register_terminal_artifact(
            db,
            attempt_id=attempt.id,
            fencing_token=fencing_token,
            expected_cas_version=attempt.terminal_cas_version,
            checkpoint_uri=manifest_uri,
        ):
            raise StaleFence("The splitter terminal artifact CAS was rejected.")
        command = (
            db.query(WorkflowCommand)
            .filter(
                WorkflowCommand.project_id == profile.project_id,
                WorkflowCommand.resource_type == "profiling_job",
                WorkflowCommand.resource_id == profile.id,
                WorkflowCommand.operation == "dataset.profile",
            )
            .order_by(WorkflowCommand.created_at.desc())
            .first()
        )
        if command is None:
            raise ValueError("The splitter has no durable profile command.")
        preparation = WorkflowAttempt(
            project_id=profile.project_id,
            stage=WorkflowStage.PREPARATION,
            logical_key=f"profile:{profile.id}:preparation",
            dataset_version_id=profile.dataset_version_id,
            workload_identity="sceptre-dataset-preparation",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            predecessor_attempt_id=attempt.id,
            predecessor_checkpoint_allowlist=[manifest_uri],
        )
        db.add(preparation)
        db.flush()
        enqueue_outbox(
            db,
            command,
            topic="ray.dataset.prepare.submit",
            aggregate_type="profiling_job",
            aggregate_id=profile.id,
            payload={
                "profiling_job_id": str(profile.id),
                "attempt_id": str(preparation.id),
                "fencing_token": preparation.fencing_token,
            },
            event_key=f"profile:{profile.id}:preparation:g1:submit",
        )
        stages = dict(profile.overview_json.get("stages", {}))
        stages.update({"splitter": "completed", "features": "queued"})
        profile.current_stage = "preparation"
        profile.progress = 0.2
        profile.overview_json = {
            **profile.overview_json,
            "stages": stages,
            "workflow_attempt_id": str(preparation.id),
            "workflow_generation": 1,
            "prepared_artifact_id": str(prepared.id),
            "split_revision_id": str(split.id),
        }
        db.commit()


def _prepared_for_profile(db: Any, profile: ProfilingJob) -> PreparedArtifact:
    artifacts = (
        db.query(PreparedArtifact)
        .filter(
            PreparedArtifact.project_id == profile.project_id,
            PreparedArtifact.dataset_version_id == profile.dataset_version_id,
            PreparedArtifact.status == "ready",
        )
        .order_by(PreparedArtifact.revision.desc())
        .all()
    )
    artifact = next(
        (item for item in artifacts if item.specification.get("profile_job_id") == str(profile.id)),
        None,
    )
    if artifact is None:
        raise ValueError("The preparation stage has no sealed splitter artifact.")
    return artifact


def _revision_digest(specification: dict[str, Any]) -> str:
    payload = json.dumps(
        specification,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _feature_launch_revisions(
    db: Any,
    profile: ProfilingJob,
    artifact: PreparedArtifact,
    result: dict[str, Any],
) -> dict[str, Any]:
    task_type = str(result["task_inference"]["task_type"])
    target = str(profile.target_column or "")
    excluded_leakage = set(result["leakage_analysis"].get("excluded_columns", []))
    identifiers = {
        item["name"] for item in result["columns"] if "identifier_like" in item["quality_flags"]
    }
    rejected = excluded_leakage | identifiers | ({target} if target else set())
    input_columns = [item["name"] for item in result["columns"] if item["name"] not in rejected]
    if not input_columns:
        raise ValueError("Feature safety gates rejected every available input column.")
    schema = {item["name"]: item["semantic_type"] for item in result["columns"]}
    recipe = FeatureRecipe.build(
        input_columns=input_columns,
        target_column=target,
        excluded_leakage_columns=sorted(excluded_leakage | identifiers),
    )
    split_revision_id = uuid.UUID(str(profile.overview_json["split_revision_id"]))
    base_name = f"dataset-{profile.dataset_version_id}-profile-{profile.id}"
    contract_spec = {
        "dataset_version_id": str(profile.dataset_version_id),
        "prepared_artifact_id": str(artifact.id),
        "split_revision_id": str(split_revision_id),
        "source_snapshot_digest": artifact.specification["source_digest"],
        "source_columns": list(schema),
        "target_column": profile.target_column,
        "allowed_feature_families": [
            "raw_passthrough",
            "numeric_unary",
            "bounded_pairwise_arithmetic",
            "datetime_decomposition",
            "categorical_frequency",
            "text_length_pattern",
        ],
        "disabled_feature_families": ["lag", "rolling", "window", "arbitrary_python"],
        "budgets": {
            "maximum_expression_depth": 2,
            "maximum_output_count": max(1, min(500, len(input_columns) * 3)),
            "maximum_cardinality": 100_000,
            "maximum_batch_rows": RAY_BATCH_ROWS,
        },
        "final_test_isolated": True,
        "value_fit_scope": "outer_train_only",
    }
    contract = FeatureContractRevision(
        project_id=profile.project_id,
        name=base_name,
        revision=1,
        digest_scope="phase3-feature-contract-v1",
        content_digest=_revision_digest(contract_spec),
        specification=contract_spec,
        task_type=task_type,
        target_column=profile.target_column,
    )
    registry_spec = {
        "fitted_scope_digest": artifact.specification["identity"]["split_digests"]["train"],
        "accepted": [
            {
                "name": column,
                "family": "raw_passthrough",
                "formula_hash": hashlib.sha256(f"column:{column}".encode()).hexdigest(),
                "execution_class": "request_local",
                "value_accessed": False,
            }
            for column in input_columns
        ],
        "rejected": [
            {
                "name": column,
                "reason": (
                    "target_or_leakage"
                    if column == target or column in excluded_leakage
                    else "identifier"
                ),
            }
            for column in sorted(rejected)
        ],
        "feature_budget": contract_spec["budgets"],
    }
    registry = FeatureRegistryRevision(
        project_id=profile.project_id,
        name=base_name,
        revision=1,
        digest_scope="phase3-feature-registry-v1",
        content_digest=_revision_digest(registry_spec),
        specification=registry_spec,
    )
    db.add_all([contract, registry])
    db.flush()
    recipe_spec = {
        **recipe.to_dict(),
        "metadata_view": recipe.metadata_view(schema),
        "registry_revision_id": str(registry.id),
        "prepared_artifact_id": str(artifact.id),
        "materialization": "metadata_only",
        "fitted_scope_digest": registry_spec["fitted_scope_digest"],
    }
    recipe_revision = FeatureRecipeRevision(
        project_id=profile.project_id,
        name=base_name,
        revision=1,
        digest_scope="sceptre-polars-feature-recipe-v1",
        content_digest=recipe.digest,
        specification=recipe_spec,
        registry_revision_id=registry.id,
    )
    catalog_spec = {
        "release": "phase3-polars-ray-v1",
        "task_type": task_type,
        "execution_policy": "ray_owned_concurrency",
    }
    catalog = EstimatorCatalogRevision(
        project_id=profile.project_id,
        name=base_name,
        revision=1,
        digest_scope="phase3-estimator-catalog-v1",
        content_digest=_revision_digest(catalog_spec),
        specification=catalog_spec,
        release_version="phase3-polars-ray-v1",
    )
    search_spaces = []
    for metric, direction in TASK_METRICS[task_type].items():
        specification = {
            "metric": metric,
            "direction": direction,
            "allowed_feature_families": contract_spec["allowed_feature_families"],
            "recipe_revision": recipe.digest,
        }
        search_spaces.append(
            FeatureSearchSpaceRevision(
                project_id=profile.project_id,
                name=f"{base_name}-{metric}",
                revision=1,
                digest_scope="phase3-feature-search-space-v1",
                content_digest=_revision_digest(specification),
                specification=specification,
                metric_name=metric,
                metric_direction=direction,
            )
        )
    db.add_all([recipe_revision, catalog, *search_spaces])
    db.flush()
    return {
        "split_revision_id": str(split_revision_id),
        "feature_contract_revision_id": str(contract.id),
        "feature_registry_revision_id": str(registry.id),
        "feature_recipe_revision_id": str(recipe_revision.id),
        "feature_search_space_revision_ids": {
            item.metric_name: str(item.id) for item in search_spaces
        },
        "estimator_catalog_revision_id": str(catalog.id),
    }


def _record_preparation_progress(
    profile_job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    fencing_token: str,
    *,
    progress: float,
    milestone: str,
    feature_profile: dict[str, Any] | None = None,
    completed_columns: int | None = None,
    total_columns: int | None = None,
    row_count: int | None = None,
) -> None:
    with get_session_factory()() as db:
        attempt = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
        )
        profile = db.scalar(
            select(ProfilingJob).where(ProfilingJob.id == profile_job_id).with_for_update()
        )
        active_attempt_id = (
            str(profile.overview_json.get("workflow_attempt_id", "")) if profile else ""
        )
        if (
            attempt is None
            or profile is None
            or attempt.fencing_token != fencing_token
            or attempt.status != AttemptStatus.RUNNING
            or active_attempt_id != str(attempt_id)
        ):
            raise StaleFence("A stale preparation worker cannot update profile progress.")
        now = datetime.now(UTC)
        attempt.heartbeat_at = now
        profile.heartbeat_at = now
        profile.progress = max(float(profile.progress), progress)
        if feature_profile is not None:
            profiles = dict(profile.feature_profiles_json)
            profiles[str(feature_profile["name"])] = feature_profile
            profile.feature_profiles_json = profiles
        if completed_columns is not None:
            profile.completed_columns = max(profile.completed_columns, completed_columns)
        if total_columns is not None:
            profile.total_columns = total_columns
        if row_count is not None:
            profile.row_count = row_count
        stages = dict(profile.overview_json.get("stages", {}))
        if milestone in {
            "loading_train",
            "row_counted",
            "summaries_complete",
            "feature_complete",
        }:
            profile.current_stage = "features"
            stages["features"] = "running"
        elif milestone == "histograms_complete":
            profile.current_stage = "relationships"
            stages["features"] = "completed"
            stages["relationships"] = "running"
        elif milestone in {"relationships_complete", "validation_counted"}:
            profile.current_stage = "preparation"
            stages["features"] = "completed"
            stages["relationships"] = "completed"
            stages["preparation"] = "running"
        profile.overview_json = {
            **profile.overview_json,
            "stages": stages,
            "preparation_milestone": milestone,
        }
        db.commit()


def prepare_profile(
    profile_job_id: uuid.UUID,
    *,
    attempt_id: uuid.UUID | None = None,
    fencing_token: str | None = None,
) -> tuple[dict[str, Any], PreparedArtifact]:
    with get_session_factory()() as db:
        profile = db.get(ProfilingJob, profile_job_id)
        if profile is None:
            raise ValueError("Profiling job was not found.")
        artifact = _prepared_for_profile(db, profile)
        version = db.get(DatasetVersion, profile.dataset_version_id)
        if version is None:
            raise ValueError("Dataset version was not found.")
        train_uri = str(artifact.specification["uris"]["train"])
        source = SimpleNamespace(
            object_uri=train_uri,
            format=DatasetFormat.PARQUET,
            original_filename="train.parquet",
            excluded_profile_columns={
                "row_id",
                "source_ordinal",
                "content_fingerprint",
                "split_role",
            },
        )
        target = profile.target_column
    progress_by_milestone = {
        "row_counted": 0.3,
        "summaries_complete": 0.55,
        "histograms_complete": 0.75,
        "relationships_complete": 0.9,
    }

    def record_progress(milestone: str) -> None:
        if attempt_id is None or fencing_token is None:
            return
        _record_preparation_progress(
            profile_job_id,
            attempt_id,
            fencing_token,
            progress=progress_by_milestone[milestone],
            milestone=milestone,
        )

    def record_feature(
        feature: ColumnProfileRead,
        completed_columns: int,
        total_columns: int,
        feature_row_count: int,
    ) -> None:
        if attempt_id is None or fencing_token is None:
            return
        _record_preparation_progress(
            profile_job_id,
            attempt_id,
            fencing_token,
            progress=0.55 + 0.2 * (completed_columns / max(1, total_columns)),
            milestone="feature_complete",
            feature_profile=feature.model_dump(mode="json"),
            completed_columns=completed_columns,
            total_columns=total_columns,
            row_count=feature_row_count,
        )

    if attempt_id is not None and fencing_token is not None:
        _record_preparation_progress(
            profile_job_id,
            attempt_id,
            fencing_token,
            progress=0.25,
            milestone="loading_train",
        )
    row_count, profiles, relationships, leakage, warnings = profile_dataset_with_ray(
        source,
        target,
        progress_callback=record_progress,
        feature_callback=record_feature,
    )
    validation_uri = str(artifact.specification["uris"]["validation"])
    validation_path, validation_filesystem = _storage_path(validation_uri)
    validation_count = ray.data.read_parquet(
        validation_path, filesystem=validation_filesystem
    ).count()
    if attempt_id is not None and fencing_token is not None:
        _record_preparation_progress(
            profile_job_id,
            attempt_id,
            fencing_token,
            progress=0.95,
            milestone="validation_counted",
        )
    task = _infer_task(target, {item.name: item for item in profiles})
    result = {
        "row_count": row_count,
        "validation_row_count": validation_count,
        "columns": [item.model_dump(mode="json") for item in profiles],
        "relationships": [item.model_dump(mode="json") for item in relationships],
        "leakage_analysis": leakage.model_dump(mode="json"),
        "task_inference": task.model_dump(mode="json"),
        "preparation_plan": [
            item.model_dump(mode="json")
            for item in _build_preparation_plan(
                profiles,
                target,
                task.task_type,
                leakage,
            )
        ],
        "warnings": [
            "Value-dependent statistics were computed from the train role only.",
            *warnings,
        ],
    }
    return result, artifact


def _persist_profile(
    profile_job_id: uuid.UUID,
    attempt_id: uuid.UUID,
    fencing_token: str,
    result: dict[str, Any],
    artifact: PreparedArtifact,
) -> None:
    store = get_object_store()
    payload = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    # See _persist_split: the CAS controls which URI becomes authoritative, while
    # the attempt-scoped key makes an upload by a stale worker harmless.
    key = (
        f"automl/projects/{artifact.project_id}/profiles/{profile_job_id}/"
        f"attempts/{attempt_id}/complete.json"
    )
    profile_uri = store.put_bytes(key, payload).uri
    with get_session_factory()() as db:
        attempt = (
            db.query(WorkflowAttempt)
            .filter(WorkflowAttempt.id == attempt_id)
            .with_for_update()
            .one()
        )
        profile = db.get(ProfilingJob, profile_job_id)
        version = db.get(DatasetVersion, attempt.dataset_version_id)
        if profile is None or version is None:
            raise ValueError("The preparation completion lineage is missing.")
        if not cas_register_terminal_artifact(
            db,
            attempt_id=attempt.id,
            fencing_token=fencing_token,
            expected_cas_version=attempt.terminal_cas_version,
            checkpoint_uri=profile_uri,
        ):
            raise StaleFence("The preparation terminal artifact CAS was rejected.")
        launch_bindings = _feature_launch_revisions(db, profile, artifact, result)
        now = datetime.now(UTC)
        profile.status = "succeeded"
        profile.current_stage = "complete"
        profile.progress = 1.0
        profile.completed_columns = len(result["columns"])
        profile.total_columns = len(result["columns"])
        profile.row_count = int(result["row_count"])
        profile.feature_profiles_json = {item["name"]: item for item in result["columns"]}
        profile.relationships_json = result["relationships"]
        profile.preparation_json = result["preparation_plan"]
        profile.warnings_json = result["warnings"]
        profile.artifact_uris_json = {
            **profile.artifact_uris_json,
            "complete": profile_uri,
            "prepared": artifact.object_uri,
        }
        stages = dict(profile.overview_json.get("stages", {}))
        stages.update(
            {
                "splitter": "completed",
                "features": "completed",
                "relationships": "completed",
                "preparation": "completed",
            }
        )
        profile.overview_json = {
            **profile.overview_json,
            "stages": stages,
            "task_inference": result["task_inference"],
            "leakage_analysis": result["leakage_analysis"],
            "validation_row_count": result["validation_row_count"],
            "launch_bindings": launch_bindings,
        }
        profile.finished_at = now
        profile.heartbeat_at = now
        version.status = DatasetStatus.READY
        version.row_count = artifact.row_count
        version.column_count = len(result["columns"])
        version.inferred_types_json = {
            item["name"]: item["semantic_type"] for item in result["columns"]
        }
        version.quality_report_json = {
            "warnings": result["warnings"],
            "leakage_analysis": result["leakage_analysis"],
        }
        version.profile_artifact_uri = profile_uri
        db.commit()


def _record_failure(profile_job_id: uuid.UUID, attempt_id: uuid.UUID, exc: Exception) -> None:
    with get_session_factory()() as db:
        # The observer locks attempts before profiles. Keep the same order here so
        # a terminal Ray status and the worker's root-cause write cannot deadlock.
        attempt = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == attempt_id).with_for_update()
        )
        profile = db.scalar(
            select(ProfilingJob).where(ProfilingJob.id == profile_job_id).with_for_update()
        )
        now = datetime.now(UTC)
        if attempt is not None:
            attempt.heartbeat_at = now
            attempt.terminal_reason = str(exc)[:2000]
        active_attempt_id = (
            str(profile.overview_json.get("workflow_attempt_id", "")) if profile else ""
        )
        if profile is not None and active_attempt_id == str(attempt_id):
            profile.heartbeat_at = now
            profile.failure_message = str(exc)[:2000]
            if (
                isinstance(exc, (ValueError, TypeError))
                and not isinstance(exc, StaleFence)
                and attempt is not None
                and attempt.status
                in {AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
            ):
                transition_attempt(
                    attempt,
                    AttemptStatus.FAILED,
                    fencing_token=attempt.fencing_token,
                    terminal_reason=str(exc)[:2000],
                    expected_cas_version=attempt.terminal_cas_version,
                )
                profile.status = "failed"
                profile.current_stage = "failed"
                profile.finished_at = now
                version = db.get(DatasetVersion, profile.dataset_version_id)
                if version is not None:
                    version.status = DatasetStatus.READY
        db.commit()


def run_stage(profile_job_id: uuid.UUID, stage: WorkflowStage) -> None:
    attempt_id, fencing_token = _begin_attempt(profile_job_id, stage)
    try:
        if stage == WorkflowStage.SPLITTER:
            with get_session_factory()() as db:
                profile = db.get(ProfilingJob, profile_job_id)
                version = db.get(DatasetVersion, profile.dataset_version_id) if profile else None
                attempt = db.get(WorkflowAttempt, attempt_id)
                if profile is None or version is None or attempt is None:
                    raise ValueError("The splitter source lineage is missing.")
                target_column = profile.target_column
                generation = attempt.generation
                time_column = profile.overview_json.get("time_column")
            result = split_dataset(
                version, profile_job_id, generation, target_column, time_column=time_column
            )
            _persist_split(profile_job_id, attempt_id, fencing_token, result)
        elif stage == WorkflowStage.PREPARATION:
            result, artifact = prepare_profile(
                profile_job_id,
                attempt_id=attempt_id,
                fencing_token=fencing_token,
            )
            _persist_profile(profile_job_id, attempt_id, fencing_token, result, artifact)
        else:
            raise ValueError(f"Unsupported dataset stage: {stage.value}")
    except Exception as exc:
        _record_failure(profile_job_id, attempt_id, exc)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-job-id", required=True)
    parser.add_argument("--stage", choices=["splitter", "preparation"], required=True)
    args = parser.parse_args()
    profile_job_id = uuid.UUID(args.profile_job_id)
    stage = WorkflowStage(args.stage)
    print(f"Starting {stage.value} for profile {profile_job_id}", flush=True)
    run_stage(profile_job_id, stage)
    print(f"Completed {stage.value} for profile {profile_job_id}", flush=True)


if __name__ == "__main__":
    main()
