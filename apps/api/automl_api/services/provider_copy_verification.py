"""Verify declared safe Parquet copies without trusting their current object versions."""

from __future__ import annotations

import hashlib
import re
import shutil
import sqlite3
import tempfile
import time
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import pyarrow.parquet as pq
from automl_shared.data_identity import content_fingerprint
from pydantic import Field

from automl_api.services.provider_distribution import Contract, Digest, ProviderDistribution
from automl_api.services.workflow_state import canonical_request_hash

IDENTITY_COLUMNS = frozenset({"row_id", "source_ordinal", "content_fingerprint", "split_role"})


class CopyVerificationBudget(Contract):
    max_temporary_bytes: Annotated[int, Field(ge=65536, strict=True)]
    max_decoded_group_bytes: Annotated[int, Field(gt=0, strict=True)]
    min_free_bytes: Annotated[int, Field(ge=0, strict=True)]
    timeout_seconds: Annotated[int, Field(gt=0, strict=True)]


class LogicalColumn(Contract):
    name: str
    type: str
    nullable: bool


def logical_columns(schema):
    return tuple(
        LogicalColumn(name=f.name, type=str(f.type), nullable=f.nullable)
        for f in sorted(schema, key=lambda field: field.name)
        if f.name not in IDENTITY_COLUMNS
    )


class CopyVerification(Contract):
    manifest_digest: Digest
    schema_digest: Digest
    columns: Annotated[tuple[LogicalColumn, ...], Field(min_length=1)]
    objects: Annotated[int, Field(gt=0)]
    byte_size: Annotated[int, Field(gt=0)]
    rows: dict[str, int]
    role_digests: dict[str, Digest]
    verified_at: datetime


def logical_schema_digest(schema) -> str:
    """arrow-logical-schema-v1 ignores writer metadata and identity columns."""
    if len(set(schema.names)) != len(schema.names):
        raise ValueError("Provider Parquet schema contains repeated column names")
    fields = sorted((f for f in schema if f.name not in IDENTITY_COLUMNS), key=lambda f: f.name)
    if not fields:
        raise ValueError("Provider Parquet has no logical data columns")
    return canonical_request_hash(
        {
            "revision": "arrow-logical-schema-v1",
            "columns": [
                {"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in fields
            ],
        }
    )


def verify_provider_objects(
    manifest: ProviderDistribution,
    store,
    budget: CopyVerificationBudget,
    *,
    progress: Callable[[], None] = lambda: None,
    temporary_root: Path | None = None,
) -> CopyVerification:
    """Consume an already signature/reference-validated manifest; never read final objects.

    The deadline is checked between SDK calls. Worker/process and provider RPC
    limits must independently bound blocked I/O and decoder RSS.
    """
    providers = {"aws": {"aws_s3", "s3_compatible"}, "gcp": {"gcs"}, "azure": {"azure_blob"}}
    if store.driver_name not in providers[manifest.provider]:
        raise ValueError("Provider copy verifier is configured for another provider")
    deadline = time.monotonic() + budget.timeout_seconds
    largest = max(o.byte_size for o in manifest.safe_data.objects)
    index_pages = (budget.max_temporary_bytes - largest) // 4096
    if index_pages < 4:
        raise ValueError("Provider copy exceeds its temporary byte budget")
    with tempfile.TemporaryDirectory(
        prefix="sceptre-provider-copy-", dir=temporary_root
    ) as directory:

        def check(extra_bytes=0):
            if time.monotonic() >= deadline:
                raise TimeoutError("Provider copy verification deadline elapsed")
            if shutil.disk_usage(directory).free < budget.min_free_bytes + extra_bytes:
                raise ValueError("Provider copy verification would consume its free-space reserve")
            progress()
            if time.monotonic() >= deadline:
                raise TimeoutError("Provider copy verification deadline elapsed")

        check(budget.max_temporary_bytes)
        with closing(sqlite3.connect(Path(directory) / "identities.sqlite")) as index:
            index.execute("PRAGMA page_size=4096")
            index.execute(f"PRAGMA max_page_count={index_pages}")
            index.execute("PRAGMA cache_size=-1024")
            # This disposable index is never published or recovered after a crash.
            index.execute("PRAGMA journal_mode=OFF")
            index.execute("CREATE TABLE ids (id BLOB PRIMARY KEY) WITHOUT ROWID")
            index.execute(
                "CREATE TABLE fingerprints (hash BLOB PRIMARY KEY, role TEXT) WITHOUT ROWID"
            )
            counts = {"train": 0, "validation": 0}
            digests = {"train": 0, "validation": 0}
            for item in manifest.safe_data.objects:
                check()
                with tempfile.TemporaryFile(dir=directory) as target:
                    actual = hashlib.sha256()
                    remaining = item.byte_size
                    with store.open_stream(item.uri, version=item.version) as source:
                        while True:
                            request_size = min(65536, remaining + 1)
                            check(request_size)
                            chunk = source.read(request_size)
                            if not chunk:
                                break
                            if len(chunk) > request_size or len(chunk) > remaining:
                                raise ValueError(
                                    "Provider object exceeds its bounded declared bytes"
                                )
                            actual.update(chunk)
                            target.write(chunk)
                            remaining -= len(chunk)
                    if remaining or actual.hexdigest() != item.sha256:
                        raise ValueError(
                            "Provider object size or SHA-256 differs from its declaration"
                        )
                    target.seek(0)
                    check()
                    with pq.ParquetFile(
                        target,
                        pre_buffer=False,
                        page_checksum_verification=True,
                        arrow_extensions_enabled=False,
                        thrift_string_size_limit=budget.max_decoded_group_bytes,
                        thrift_container_size_limit=max(1, budget.max_decoded_group_bytes // 128),
                    ) as parquet:
                        if (
                            parquet.metadata.num_rows != item.rows
                            or logical_schema_digest(parquet.schema_arrow)
                            != manifest.safe_data.schema_digest
                            or not {"row_id", "source_ordinal", "content_fingerprint"}.issubset(
                                parquet.schema_arrow.names
                            )
                        ):
                            raise ValueError(
                                "Provider object schema, row count or identity columns changed"
                            )
                        if any(
                            parquet.metadata.row_group(i).total_byte_size
                            > budget.max_decoded_group_bytes
                            for i in range(parquet.num_row_groups)
                        ):
                            raise ValueError(
                                "Provider Parquet row group exceeds its decoded byte budget"
                            )
                        columns = [
                            c for c in parquet.schema_arrow.names if c not in IDENTITY_COLUMNS
                        ]
                        rows, row_xor = 0, 0
                        for batch in parquet.iter_batches(batch_size=4096, use_threads=False):
                            check()
                            if batch.nbytes > budget.max_decoded_group_bytes:
                                raise ValueError(
                                    "Provider Parquet batch exceeds its decoded byte budget"
                                )
                            identities, fingerprints = [], []
                            for row in batch.to_pylist():
                                row_id, fingerprint = row["row_id"], row["content_fingerprint"]
                                ordinal = row["source_ordinal"]
                                if (
                                    not isinstance(row_id, str)
                                    or re.fullmatch(r"[0-9a-f]{64}", row_id) is None
                                    or not isinstance(fingerprint, str)
                                    or re.fullmatch(r"[0-9a-f]{64}", fingerprint) is None
                                    or not isinstance(ordinal, int)
                                    or isinstance(ordinal, bool)
                                    or ordinal < 0
                                    or ("split_role" in row and row["split_role"] != item.role)
                                    or content_fingerprint(row, columns) != fingerprint
                                ):
                                    raise ValueError(
                                        "Provider row identity, content or role is invalid"
                                    )
                                identities.append((bytes.fromhex(row_id),))
                                fingerprints.append((bytes.fromhex(fingerprint), item.role))
                                row_xor ^= int.from_bytes(
                                    hashlib.sha256(row_id.encode()).digest(), "big"
                                )
                                rows += 1
                            try:
                                index.executemany("INSERT INTO ids VALUES (?)", identities)
                                written = index.executemany(
                                    "INSERT INTO fingerprints VALUES (?, ?) ON CONFLICT(hash) "
                                    "DO UPDATE SET role=excluded.role "
                                    "WHERE fingerprints.role=excluded.role",
                                    fingerprints,
                                ).rowcount
                            except sqlite3.IntegrityError as exc:
                                raise ValueError("Provider copies repeat a row identity") from exc
                            except sqlite3.DatabaseError as exc:
                                raise ValueError(
                                    "Provider identity index exceeded its working budget"
                                ) from exc
                            if written != len(fingerprints):
                                raise ValueError("Provider content fingerprints cross safe roles")
                        if rows != item.rows or f"{row_xor:064x}" != item.row_digest:
                            raise ValueError(
                                "Provider row identities differ from their signed digest"
                            )
                        columns = logical_columns(parquet.schema_arrow)
                        counts[item.role] += rows
                        digests[item.role] ^= row_xor
            check()
            result = CopyVerification(
                manifest_digest=canonical_request_hash(manifest.model_dump(mode="json")),
                schema_digest=manifest.safe_data.schema_digest,
                columns=columns,
                objects=len(manifest.safe_data.objects),
                byte_size=sum(o.byte_size for o in manifest.safe_data.objects),
                rows=counts,
                role_digests={r: f"{value:064x}" for r, value in digests.items()},
                verified_at=datetime.now(UTC),
            )
        return result
