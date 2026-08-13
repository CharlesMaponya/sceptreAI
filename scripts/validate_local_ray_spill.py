from __future__ import annotations

import json
import os
import resource
import shutil
import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import ray

OBJECT_COUNT = 12
OBJECT_BYTES = 32 * 1024 * 1024
OBJECT_STORE_BYTES = 96 * 1024 * 1024


@ray.remote
def allocate_object(index: int) -> np.ndarray:
    return np.full(OBJECT_BYTES, index % 251, dtype=np.uint8)


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="sceptre-ray-spill-") as directory:
        spill = Path(directory)
        ray.init(
            num_cpus=8,
            object_store_memory=OBJECT_STORE_BYTES,
            object_spilling_directory=str(spill),
            include_dashboard=False,
        )
        references = [allocate_object.remote(index) for index in range(OBJECT_COUNT)]
        checksums = [int(value[0]) for value in ray.get(references)]
        if checksums != [index % 251 for index in range(OBJECT_COUNT)]:
            raise RuntimeError("Spilled object contents changed.")

        frame = pl.DataFrame({"value": np.arange(1_000_000, dtype=np.int64)})
        total = 0
        max_batch_rows = 0
        for batch in frame.lazy().collect(engine="streaming").iter_slices(n_rows=65_536):
            arrow: pa.Table = batch.to_arrow()
            max_batch_rows = max(max_batch_rows, arrow.num_rows)
            total += int(batch.get_column("value").sum())
        expected_total = 999_999 * 1_000_000 // 2
        if total != expected_total or max_batch_rows > 65_536:
            raise RuntimeError("Polars streaming result or batch bound is wrong.")

        spill_files = [path for path in spill.rglob("*") if path.is_file()]
        spill_bytes = sum(path.stat().st_size for path in spill_files)
        if spill_bytes <= 0:
            raise RuntimeError("The bounded Ray object store did not spill to disk.")
        peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print(
            json.dumps(
                {
                    "cpu_count": os.cpu_count(),
                    "object_bytes": OBJECT_BYTES,
                    "object_count": OBJECT_COUNT,
                    "object_store_bytes": OBJECT_STORE_BYTES,
                    "peak_driver_rss_kib": peak_rss_kib,
                    "polars_max_batch_rows": max_batch_rows,
                    "polars_rows": frame.height,
                    "spill_bytes": spill_bytes,
                    "spill_file_count": len(spill_files),
                    "spill_free_bytes": shutil.disk_usage(spill).free,
                    "status": "passed",
                },
                sort_keys=True,
            )
        )
        ray.shutdown()


if __name__ == "__main__":
    main()
