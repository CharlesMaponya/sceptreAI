"""Read approved safe Parquet objects by exact immutable version in Ray workers."""

import hashlib
import shutil
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from automl_api.services.provider_copy_verification import (
    CopyVerificationBudget,
    logical_schema_digest,
)
from automl_api.services.provider_distribution import ProviderDistribution, SafeObject
from automl_api.training.object_capabilities import provider_grant_reader


def read_provider_batches(batch, *, budget, manifest):
    import ray

    limits = CopyVerificationBudget.model_validate(budget)
    deadline = time.monotonic() + limits.timeout_seconds
    if isinstance(manifest, ray.ObjectRef):
        manifest = ray.get(manifest, timeout=limits.timeout_seconds)
    manifest = ProviderDistribution.model_validate(manifest)
    if manifest.mode != "validation_only" or manifest.final_data is not None:
        raise ValueError("Provider source permits only its approved safe roles")
    approved = {(item.uri, item.version): item for item in manifest.safe_data.objects}
    store = provider_grant_reader(manifest)
    schema_digest = manifest.safe_data.schema_digest
    for value in batch.to_pylist():
        item = SafeObject.model_validate(value)
        if approved.get((item.uri, item.version)) != item:
            raise ValueError("Provider read differs from its approved object declaration")
        if item.byte_size > limits.max_temporary_bytes:
            raise ValueError("Provider read exceeds its temporary byte budget")
        with tempfile.TemporaryDirectory(prefix="sceptre-provider-read-") as directory:

            def check(extra=0):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Provider read deadline elapsed")
                if shutil.disk_usage(directory).free < limits.min_free_bytes + extra:
                    raise ValueError("Provider read would consume its free-space reserve")

            check(item.byte_size)
            with (Path(directory) / "object.parquet").open("w+b") as target:
                actual = hashlib.sha256()
                remaining = item.byte_size
                with store.open_stream(item.uri, version=item.version) as source:
                    while chunk := source.read(min(65536, remaining + 1)):
                        check(len(chunk))
                        if len(chunk) > remaining:
                            raise ValueError("Provider object exceeds its declared bytes")
                        target.write(chunk)
                        actual.update(chunk)
                        remaining -= len(chunk)
                if remaining or actual.hexdigest() != item.sha256:
                    raise ValueError("Provider object size or SHA-256 changed")
                target.seek(0)
                check()
                with pq.ParquetFile(
                    target,
                    pre_buffer=False,
                    page_checksum_verification=True,
                    arrow_extensions_enabled=False,
                    thrift_string_size_limit=limits.max_decoded_group_bytes,
                    thrift_container_size_limit=max(1, limits.max_decoded_group_bytes // 128),
                ) as parquet:
                    if (
                        parquet.metadata.num_rows != item.rows
                        or logical_schema_digest(parquet.schema_arrow) != schema_digest
                        or not {"row_id", "source_ordinal", "content_fingerprint"}.issubset(
                            parquet.schema_arrow.names
                        )
                        or any(
                            parquet.metadata.row_group(i).total_byte_size
                            > limits.max_decoded_group_bytes
                            for i in range(parquet.num_row_groups)
                        )
                    ):
                        raise ValueError("Provider read schema/rows/decoded group budget changed")
                    count = 0
                    for block in parquet.iter_batches(batch_size=4096, use_threads=False):
                        check()
                        if block.nbytes > limits.max_decoded_group_bytes:
                            raise ValueError("Provider decoded batch exceeds its budget")
                        table = pa.Table.from_batches([block])
                        if "split_role" not in table.column_names:
                            table = table.append_column(
                                "split_role", pa.array([item.role] * len(table))
                            )
                        elif any(role != item.role for role in table["split_role"].to_pylist()):
                            raise ValueError("Provider safe role changed")
                        count += len(table)
                        yield table.select(sorted(table.column_names))
                    if count != item.rows:
                        raise ValueError("Provider decoded row count changed")


def read_provider_role(plan, role):
    import ray

    from automl_api.services.provider_source import ProviderReadPlan

    plan = ProviderReadPlan.model_validate(plan)
    if (
        role not in ("train", "validation")
        or plan.manifest.mode != "validation_only"
        or plan.manifest.final_data is not None
    ):
        raise ValueError("Provider source permits only its approved safe roles")
    objects = [
        item.model_dump(mode="json")
        for item in plan.manifest.safe_data.objects
        if item.role == role
    ]
    # Share a large declaration once rather than serialize it into every object task.
    manifest = ray.put(plan.manifest.model_dump(mode="json"))
    return ray.data.from_items(objects).map_batches(
        read_provider_batches,
        batch_size=1,
        batch_format="pyarrow",
        fn_kwargs={
            "budget": plan.budget.model_dump(mode="json"),
            "manifest": manifest,
        },
    )
