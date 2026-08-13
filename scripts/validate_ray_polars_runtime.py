from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import pyarrow.fs as pa_fs
import ray
import ray.data
from automl_shared.data_identity import (
    content_fingerprint,
    identity_manifest,
    sequence_digest,
    split_role,
    stable_row_id,
)
from ray.data import DataContext

BATCH_ROWS = 128
EXPECTED_ROWS = 1_000
FIXTURE_COLUMNS = ("category", "value")
SPLIT_SEED = "sceptre-ray-polars-feasibility-v1"


def double_values(batch: pa.Table) -> pa.Table:
    frame = pl.from_arrow(batch)
    if not isinstance(frame, pl.DataFrame):
        frame = frame.to_frame()
    if frame.height > BATCH_ROWS:
        raise RuntimeError(f"Ray supplied {frame.height} rows; limit is {BATCH_ROWS}.")
    return frame.with_columns((pl.col("id") * 2).alias("doubled")).to_arrow()


def attach_identity(
    batch: pa.Table,
    *,
    source_digest: str,
    split_seed: str,
) -> pa.Table:
    frame = pl.from_arrow(batch)
    if not isinstance(frame, pl.DataFrame):
        frame = frame.to_frame()
    if frame.height > BATCH_ROWS:
        raise RuntimeError(f"Ray supplied {frame.height} rows; limit is {BATCH_ROWS}.")
    frame = frame.rename({"id": "source_ordinal"})
    source_rows = frame.select(FIXTURE_COLUMNS).to_dicts()
    fingerprints = [content_fingerprint(row, FIXTURE_COLUMNS) for row in source_rows]
    ordinals = [int(value) for value in frame.get_column("source_ordinal")]
    return frame.with_columns(
        pl.Series(
            "row_id",
            [stable_row_id(source_digest, ordinal) for ordinal in ordinals],
        ),
        pl.Series("content_fingerprint", fingerprints),
        pl.Series(
            "split_role",
            [split_role(value, split_seed) for value in fingerprints],
        ),
    ).to_arrow()


def prepared_digest(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: int(row["source_ordinal"]))
    return sequence_digest(
        json.dumps(
            [
                int(row["source_ordinal"]),
                str(row["row_id"]),
                str(row["content_fingerprint"]),
                str(row["split_role"]),
            ],
            separators=(",", ":"),
        )
        for row in ordered
    )


def fixture_content() -> bytes:
    destination = io.StringIO(newline="")
    writer = csv.writer(destination, lineterminator="\n")
    writer.writerow(["value", "category"])
    for index in range(EXPECTED_ROWS):
        writer.writerow([index % 113, f"group-{index % 7}"])
    return destination.getvalue().encode("utf-8")


def prepare_storage(
    local_root: Path,
    content: bytes,
) -> tuple[pa_fs.FileSystem, str, str, str | None]:
    source_digest = hashlib.sha256(content).hexdigest()
    endpoint = os.getenv("SCEPTRE_SMOKE_S3_ENDPOINT")
    if not endpoint:
        filesystem: pa_fs.FileSystem = pa_fs.LocalFileSystem()
        fixture_path = str(local_root / "fixture.csv")
        with filesystem.open_output_stream(fixture_path) as destination:
            destination.write(content)
        return filesystem, fixture_path, str(local_root), None

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("S3 smoke storage requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
    bucket = os.getenv("SCEPTRE_SMOKE_S3_BUCKET", "automl").strip("/")
    parsed = urlparse(endpoint)
    if not parsed.netloc or parsed.scheme not in {"http", "https"}:
        raise RuntimeError("SCEPTRE_SMOKE_S3_ENDPOINT must be an HTTP(S) URL.")
    filesystem = pa_fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=parsed.netloc,
        scheme=parsed.scheme,
    )
    prefix = f"{bucket}/ray-polars-smoke/{source_digest}"
    if filesystem.get_file_info(prefix).type != pa_fs.FileType.NotFound:
        filesystem.delete_dir(prefix)
    fixture_path = f"{prefix}/fixture.csv"
    with filesystem.open_output_stream(fixture_path) as destination:
        destination.write(content)
    return filesystem, fixture_path, prefix, prefix


def validate_identity_pass(
    fixture_path: str,
    output_path: str,
    *,
    filesystem: pa_fs.FileSystem,
    source_digest: str,
    block_count: int,
) -> dict[str, Any]:
    source = ray.data.read_csv(
        fixture_path,
        filesystem=filesystem,
        override_num_blocks=block_count,
    )
    indexed = source.zip(
        ray.data.range(EXPECTED_ROWS, override_num_blocks=block_count)
    )
    identified = indexed.map_batches(
        attach_identity,
        batch_format="pyarrow",
        batch_size=BATCH_ROWS,
        zero_copy_batch=True,
        fn_kwargs={"source_digest": source_digest, "split_seed": SPLIT_SEED},
    )
    rows = [dict(row) for row in identified.take_all()]
    manifest = identity_manifest(rows)
    fingerprint_roles: defaultdict[str, set[str]] = defaultdict(set)
    fingerprint_counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        fingerprint = str(row["content_fingerprint"])
        fingerprint_roles[fingerprint].add(str(row["split_role"]))
        fingerprint_counts[fingerprint] += 1
    duplicate_groups = sum(count > 1 for count in fingerprint_counts.values())
    cross_role_duplicate_groups = sum(
        len(fingerprint_roles[fingerprint]) > 1
        for fingerprint, count in fingerprint_counts.items()
        if count > 1
    )
    if duplicate_groups == 0 or cross_role_duplicate_groups:
        raise RuntimeError("Duplicate rows were not kept inside one split role.")

    identified.sort("source_ordinal").repartition(1).write_parquet(
        output_path,
        filesystem=filesystem,
    )
    parquet_files = sorted(
        info.path
        for info in filesystem.get_file_info(
            pa_fs.FileSelector(output_path, recursive=True)
        )
        if info.is_file and info.path.endswith(".parquet")
    )
    if len(parquet_files) != 1:
        raise RuntimeError(f"Expected one canonical Parquet file, found {len(parquet_files)}.")
    restored_rows = [
        dict(row)
        for row in ray.data.read_parquet(
            output_path,
            filesystem=filesystem,
        ).take_all()
    ]
    restored_manifest = identity_manifest(restored_rows)
    if restored_manifest != manifest:
        raise RuntimeError("Parquet round-trip changed the identity manifest.")
    logical_digest = prepared_digest(rows)
    if prepared_digest(restored_rows) != logical_digest:
        raise RuntimeError("Parquet round-trip changed prepared row content.")
    return {
        "block_count": block_count,
        "duplicate_groups": duplicate_groups,
        "manifest": manifest.to_dict(),
        "parquet_file_count": len(parquet_files),
        "prepared_digest": logical_digest,
    }


def main() -> None:
    ray.init(address="auto")
    DataContext.get_current().enable_progress_bars = False
    live_nodes = [node for node in ray.nodes() if node.get("Alive")]
    worker_nodes = [
        node
        for node in live_nodes
        if "node:__internal_head__" not in (node.get("Resources") or {})
    ]
    output = ray.data.range(EXPECTED_ROWS, override_num_blocks=8).map_batches(
        double_values,
        batch_format="pyarrow",
        batch_size=BATCH_ROWS,
        zero_copy_batch=True,
    )
    result = output.take_all()
    rows = len(result)
    maximum = max(int(row["doubled"]) for row in result)
    if rows != EXPECTED_ROWS:
        raise RuntimeError(f"Expected {EXPECTED_ROWS} rows, received {rows}.")
    if maximum != (EXPECTED_ROWS - 1) * 2:
        raise RuntimeError(f"Unexpected maximum transformed value: {maximum}.")
    with tempfile.TemporaryDirectory(prefix="sceptre-ray-polars-") as temporary:
        root = Path(temporary)
        content = fixture_content()
        source_digest = hashlib.sha256(content).hexdigest()
        filesystem, fixture_path, output_root, cleanup_path = prepare_storage(root, content)
        try:
            identity_passes = [
                validate_identity_pass(
                    fixture_path,
                    f"{output_root}/prepared-{block_count}",
                    filesystem=filesystem,
                    source_digest=source_digest,
                    block_count=block_count,
                )
                for block_count in (1, 4)
            ]
        finally:
            if cleanup_path is not None:
                filesystem.delete_dir(cleanup_path)
    reference = identity_passes[0]
    for result in identity_passes[1:]:
        if result["manifest"] != reference["manifest"]:
            raise RuntimeError("Identity manifest changed across Ray block counts.")
        if result["prepared_digest"] != reference["prepared_digest"]:
            raise RuntimeError("Prepared digest changed across Ray block counts.")
    print(
        json.dumps(
            {
                "batch_rows": BATCH_ROWS,
                "maximum": maximum,
                "polars_version": pl.__version__,
                "pyarrow_version": pa.__version__,
                "ray_version": ray.__version__,
                "ray_node_count": len(live_nodes),
                "rows": rows,
                "identity": {
                    "block_counts": [result["block_count"] for result in identity_passes],
                    "duplicate_groups": reference["duplicate_groups"],
                    "manifest": reference["manifest"],
                    "parquet_file_count": reference["parquet_file_count"],
                    "prepared_digest": reference["prepared_digest"],
                    "source_digest": source_digest,
                    "storage": "s3" if cleanup_path is not None else "local",
                },
                "worker_node_count": len(worker_nodes),
                "status": "passed",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
