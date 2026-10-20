"""Stream a metadata-only, read-only PostgreSQL digest provenance inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime
from enum import Enum
from pathlib import Path

from automl_api.db.session import get_engine
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from sqlalchemy import or_, select, text


def _sha256(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def review_reasons(row: dict) -> list[str]:
    reasons = []
    if row["algorithm"] != "sha256":
        reasons.append("unsupported_algorithm")
    if row["scope"] != "byte_stream":
        reasons.append(
            "legacy_manifest" if row["scope"] == "multipart_manifest" else "unsupported_scope"
        )
    if not _sha256(row["expected_digest"]):
        reasons.append("missing_or_invalid_digest")
    size = row.get("byte_size")
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        reasons.append("missing_or_invalid_byte_size")
    if row.get("legacy_upload_link"):
        reasons.append("legacy_upload_provenance")
    verified = row["state"] == ("ready" if row["resource_type"] == "upload" else "verified")
    verified_at = row["verified_at"]
    if (
        not verified
        or not isinstance(verified_at, datetime)
        or verified_at.tzinfo is None
        or verified_at.utcoffset() is None
    ):
        reasons.append("verification_not_proven")
    if row["resource_type"] == "upload" and verified:
        if row.get("scanner_status") != "allowed":
            reasons.append("scanner_not_allowed")
        if not _sha256(row["observed_digest"]) or row["observed_digest"] != row["expected_digest"]:
            reasons.append("observed_digest_mismatch")
    return reasons


def _queries():
    upload, version = DatasetUploadSession.__table__, DatasetVersion.__table__
    legacy_link = (
        select(upload.c.id)
        .where(
            upload.c.project_id == version.c.project_id,
            upload.c.dataset_version_id == version.c.id,
            or_(upload.c.digest_algorithm != "sha256", upload.c.digest_scope != "byte_stream"),
        )
        .exists()
    )
    return (
        (
            "upload",
            select(
                upload.c.id,
                upload.c.project_id,
                upload.c.dataset_version_id,
                upload.c.byte_size,
                upload.c.scanner_status,
                upload.c.digest_algorithm.label("algorithm"),
                upload.c.digest_scope.label("scope"),
                upload.c.expected_object_digest.label("expected_digest"),
                upload.c.observed_object_digest.label("observed_digest"),
                upload.c.checksum_verified_at.label("verified_at"),
                upload.c.status.label("state"),
            ).order_by(upload.c.id),
        ),
        (
            "dataset_version",
            select(
                version.c.id,
                version.c.project_id,
                version.c.byte_size,
                version.c.content_hash_algorithm.label("algorithm"),
                version.c.content_hash_scope.label("scope"),
                version.c.content_hash.label("expected_digest"),
                version.c.content_hash_verified_at.label("verified_at"),
                version.c.content_hash_verification_status.label("state"),
                legacy_link.label("legacy_upload_link"),
            ).order_by(version.c.id),
        ),
    )


def _line(value) -> str:
    return (
        json.dumps(
            value, sort_keys=True, default=lambda v: v.value if isinstance(v, Enum) else str(v)
        )
        + "\n"
    )


def inventory(engine, output) -> dict:
    if engine.dialect.name != "postgresql":
        raise ValueError("Digest inventories require PostgreSQL read-only snapshot semantics.")
    counts = Counter(
        upload=0, upload_requires_review=0, dataset_version=0, dataset_version_requires_review=0
    )
    reasons = Counter()
    digest = hashlib.sha256()
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
        with connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            snapshot, database, read_only, isolation = connection.execute(
                text(
                    "SELECT txid_current_snapshot()::text, current_database(), "
                    "current_setting('transaction_read_only'), "
                    "current_setting('transaction_isolation')"
                )
            ).one()
            if (read_only, isolation) != ("on", "repeatable read"):
                raise RuntimeError("The database did not enforce the read-only snapshot.")
            output.write(
                _line(
                    {
                        "type": "header",
                        "schema": "dataset-digest-inventory-v2",
                        "snapshot": snapshot,
                        "database": database,
                        "read_only": read_only,
                        "isolation": isolation,
                    }
                )
            )
            for resource_type, query in _queries():
                with connection.execution_options(yield_per=500).execute(query) as result:
                    for row in result.mappings():
                        record = {"type": "resource", "resource_type": resource_type, **row}
                        record["review_reasons"] = review_reasons(record)
                        counts[resource_type] += 1
                        counts[resource_type + "_requires_review"] += bool(record["review_reasons"])
                        reasons.update(record["review_reasons"])
                        line = _line(record)
                        digest.update(line.encode("utf-8"))
                        output.write(line)
    summary = {
        "type": "summary",
        "complete": True,
        "counts": dict(counts),
        "review_reasons": dict(reasons),
        "resource_records_sha256": digest.hexdigest(),
    }
    output.write(_line(summary))
    return summary


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    # Never overwrite a prior inventory or expose its tenant identifiers by default.
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        inventory(get_engine(), output)


if __name__ == "__main__":
    main()
