"""Uniform analysis samples with memory bounded by a batch and the sample budget."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow.dataset as pa_dataset
import pyarrow.fs as pa_fs

from automl_api.services.ray_polars_profiling import _ray_source
from automl_api.storage.object_store import get_object_store

BATCH_ROWS = 16_384


def sample_batches(batches, *, max_rows: int, seed: int) -> pd.DataFrame:
    if max_rows < 1:
        raise ValueError("Analysis sample size must be positive.")
    rng = np.random.default_rng(seed)
    selected = pd.DataFrame()
    priorities = np.empty(0)
    rows_seen = 0
    for batch in batches:
        rows_seen += len(batch)
        combined = pd.concat([selected, batch], ignore_index=True)
        keys = np.concatenate([priorities, rng.random(len(batch))])
        indices = np.arange(len(keys))
        if len(keys) > max_rows:
            indices = np.argpartition(keys, max_rows - 1)[:max_rows]
        selected = combined.iloc[indices].reset_index(drop=True)
        priorities = keys[indices]
    selected = selected.iloc[np.argsort(priorities)].reset_index(drop=True)
    selected.attrs["source_rows"] = rows_seen
    selected.attrs["sampling_policy"] = "seeded_uniform_streaming_sample"
    return selected


def sample_source(uri: str, filename: str, *, max_rows: int, seed: int) -> pd.DataFrame:
    descriptor = get_object_store().dataframe_source(uri)
    path, filesystem = _ray_source(descriptor.path, descriptor.filesystem_options)
    filename = filename.lower()
    if filename.endswith(".parquet"):
        scanner = pa_dataset.dataset(path, filesystem=filesystem, format="parquet").scanner(
            batch_size=BATCH_ROWS,
            batch_readahead=1,
            fragment_readahead=1,
            use_threads=False,
        )
        return sample_batches(
            (batch.to_pandas() for batch in scanner.to_batches()), max_rows=max_rows, seed=seed
        )
    if filesystem is None:
        filesystem, path = pa_fs.FileSystem.from_uri(path)
    with filesystem.open_input_file(path) as stream:
        if filename.endswith(".csv"):
            batches = pd.read_csv(stream, chunksize=BATCH_ROWS)
        elif filename.endswith((".jsonl", ".ndjson")):
            batches = pd.read_json(stream, lines=True, chunksize=BATCH_ROWS)
        elif filename.endswith((".json", ".xlsx", ".xls")):
            if stream.size() > 16 * 1024 * 1024:
                raise ValueError(
                    "Export JSON arrays or Excel files above 16 MiB "
                    "to CSV or Parquet for drift analysis."
                )
            frame = pd.read_json(stream) if filename.endswith(".json") else pd.read_excel(stream)
            batches = [frame]
        else:
            raise ValueError(
                "Drift sampling supports CSV, Parquet, and JSONL. Export this file to CSV."
            )
        return sample_batches(batches, max_rows=max_rows, seed=seed)
