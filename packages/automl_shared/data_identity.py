from __future__ import annotations

import base64
import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

ROW_ID_REVISION = "sha256-source-ordinal-v1"
CONTENT_FINGERPRINT_REVISION = "sha256-typed-row-v1"
SPLIT_REVISION = "sha256-content-fingerprint-bps-v1"
ROLE_ORDER = ("train", "validation", "final_test")


@dataclass(frozen=True)
class IdentityManifest:
    row_count: int
    row_set_digest: str
    split_counts: dict[str, int]
    split_digests: dict[str, str]
    row_id_revision: str = ROW_ID_REVISION
    content_fingerprint_revision: str = CONTENT_FINGERPRINT_REVISION
    split_revision: str = SPLIT_REVISION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def canonical_value(value: Any) -> Any:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        if math.isnan(value):
            encoded = "nan"
        elif math.isinf(value):
            encoded = "+inf" if value > 0 else "-inf"
        else:
            encoded = value.hex()
        return {"type": "float64", "value": encoded}
    if isinstance(value, Decimal):
        encoded = "0" if value.is_zero() else str(value.normalize())
        return {"type": "decimal", "value": encoded}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, bytes):
        return {
            "type": "bytes",
            "value": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, datetime):
        if value.tzinfo is None:
            encoded = f"naive:{value.isoformat(timespec='microseconds')}"
        else:
            encoded = value.astimezone(UTC).isoformat(timespec="microseconds")
        return {"type": "datetime", "value": encoded}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, Mapping):
        return {
            "type": "mapping",
            "value": [
                [str(key), canonical_value(item)]
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            ],
        }
    if isinstance(value, (list, tuple)):
        return {"type": "sequence", "value": [canonical_value(item) for item in value]}
    raise TypeError(f"Unsupported canonical value type: {type(value).__name__}")


def content_fingerprint(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    ordered_columns = sorted(dict.fromkeys(columns))
    payload = [
        [column, canonical_value(row.get(column))]
        for column in ordered_columns
    ]
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stable_row_id(source_digest: str, source_ordinal: int) -> str:
    normalized_digest = source_digest.removeprefix("sha256:").lower()
    if len(normalized_digest) != 64 or any(
        character not in "0123456789abcdef" for character in normalized_digest
    ):
        raise ValueError("source_digest must be a SHA-256 hex digest.")
    if source_ordinal < 0:
        raise ValueError("source_ordinal must be non-negative.")
    payload = f"{ROW_ID_REVISION}\0{normalized_digest}\0{source_ordinal}".encode()
    return hashlib.sha256(payload).hexdigest()


def split_role(
    fingerprint: str,
    split_seed: str,
    *,
    train_basis_points: int = 7_000,
    validation_basis_points: int = 1_500,
) -> str:
    if train_basis_points <= 0 or validation_basis_points <= 0:
        raise ValueError("Train and validation allocations must both be positive.")
    if train_basis_points + validation_basis_points >= 10_000:
        raise ValueError("The split must reserve positive basis points for final_test.")
    payload = f"{SPLIT_REVISION}\0{split_seed}\0{fingerprint}".encode()
    bucket = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % 10_000
    if bucket < train_basis_points:
        return "train"
    if bucket < train_basis_points + validation_basis_points:
        return "validation"
    return "final_test"


def sequence_digest(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def identity_manifest(rows: Iterable[Mapping[str, Any]]) -> IdentityManifest:
    row_ids: list[str] = []
    split_row_ids = {role: [] for role in ROLE_ORDER}
    for row in rows:
        row_id = str(row["row_id"])
        role = str(row["split_role"])
        if role not in split_row_ids:
            raise ValueError(f"Unsupported split role: {role}")
        row_ids.append(row_id)
        split_row_ids[role].append(row_id)
    if len(row_ids) != len(set(row_ids)):
        raise ValueError("row_id values must be unique within one dataset version.")
    return IdentityManifest(
        row_count=len(row_ids),
        row_set_digest=sequence_digest(sorted(row_ids)),
        split_counts={role: len(split_row_ids[role]) for role in ROLE_ORDER},
        split_digests={
            role: sequence_digest(sorted(split_row_ids[role]))
            for role in ROLE_ORDER
        },
    )
