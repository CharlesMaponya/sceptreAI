from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from automl_shared.data_identity import (
    canonical_value,
    content_fingerprint,
    identity_manifest,
    sequence_digest,
    split_role,
    stable_row_id,
)

SOURCE_DIGEST = "a" * 64


def test_canonical_values_preserve_type_and_edge_semantics() -> None:
    assert canonical_value(1) != canonical_value("1")
    assert canonical_value(True) != canonical_value(1)
    assert canonical_value(float("nan"))["value"] == "nan"
    assert canonical_value(float("inf"))["value"] == "+inf"
    assert canonical_value(Decimal("1.00")) == canonical_value(Decimal("1.0"))
    assert canonical_value(datetime(2026, 1, 1, tzinfo=UTC))["value"].endswith("+00:00")
    assert math.isnan(float("nan"))


def test_row_ids_are_positional_but_duplicates_share_a_split() -> None:
    columns = ("category", "value")
    first = {"category": "duplicate", "value": 7}
    second = {"value": 7, "category": "duplicate"}

    fingerprint_a = content_fingerprint(first, columns)
    fingerprint_b = content_fingerprint(second, tuple(reversed(columns)))

    assert fingerprint_a == fingerprint_b
    assert stable_row_id(SOURCE_DIGEST, 10) != stable_row_id(SOURCE_DIGEST, 11)
    assert split_role(fingerprint_a, "split-seed") == split_role(fingerprint_b, "split-seed")


def test_identity_manifest_is_independent_of_worker_completion_order() -> None:
    rows = [
        {"row_id": stable_row_id(SOURCE_DIGEST, index), "split_role": role}
        for index, role in enumerate(("train", "validation", "final_test", "train"))
    ]

    forward = identity_manifest(rows)
    reverse = identity_manifest(reversed(rows))

    assert forward == reverse
    assert forward.row_count == 4
    assert forward.split_counts == {"train": 2, "validation": 1, "final_test": 1}


def test_identity_manifest_rejects_duplicate_row_ids() -> None:
    row_id = stable_row_id(SOURCE_DIGEST, 0)

    with pytest.raises(ValueError, match="unique"):
        identity_manifest(
            [
                {"row_id": row_id, "split_role": "train"},
                {"row_id": row_id, "split_role": "validation"},
            ]
        )


def test_split_requires_a_nonempty_final_role() -> None:
    with pytest.raises(ValueError, match="final_test"):
        split_role("b" * 64, "seed", train_basis_points=8_500)


def test_canonical_value_covers_every_supported_type_and_rejects_objects() -> None:
    assert canonical_value(None) == {"type": "null"}
    assert canonical_value(-float("inf"))["value"] == "-inf"
    assert canonical_value(1.25)["value"] == (1.25).hex()
    assert canonical_value(Decimal("0.000"))["value"] == "0"
    assert canonical_value(b"abc")["value"] == "YWJj"
    assert canonical_value(date(2026, 1, 2))["value"] == "2026-01-02"
    assert canonical_value(datetime(2026, 1, 1))["value"].startswith("naive:")
    offset = datetime(2026, 1, 1, 2, tzinfo=timezone(timedelta(hours=2)))
    assert canonical_value(offset)["value"].endswith("+00:00")
    mapping = canonical_value({2: "b", "1": [True, (None,)]})
    assert mapping["type"] == "mapping" and mapping["value"][0][0] == "1"
    with pytest.raises(TypeError, match="Unsupported canonical value"):
        canonical_value(object())


def test_row_id_split_and_manifest_validation_edges() -> None:
    assert stable_row_id(f"sha256:{SOURCE_DIGEST}", 0) == stable_row_id(SOURCE_DIGEST.upper(), 0)
    with pytest.raises(ValueError, match="SHA-256"):
        stable_row_id("bad", 0)
    with pytest.raises(ValueError, match="non-negative"):
        stable_row_id(SOURCE_DIGEST, -1)
    with pytest.raises(ValueError, match="positive"):
        split_role("f", "seed", train_basis_points=0)
    with pytest.raises(ValueError, match="positive"):
        split_role("f", "seed", validation_basis_points=0)

    roles = {
        split_role(f"fingerprint-{index}", "seed", train_basis_points=1, validation_basis_points=1)
        for index in range(20_000)
    }
    assert roles == {"train", "validation", "final_test"}
    with pytest.raises(ValueError, match="Unsupported split role"):
        identity_manifest([{"row_id": "one", "split_role": "holdout"}])
    assert identity_manifest([]).to_dict()["split_counts"] == {
        "train": 0,
        "validation": 0,
        "final_test": 0,
    }
    assert sequence_digest(["ab", "c"]) != sequence_digest(["a", "bc"])
