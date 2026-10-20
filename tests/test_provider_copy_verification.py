"""Actual Parquet bytes, bounded identity indexing and exact-version copy proofs."""

import hashlib
import io
import uuid
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from automl_api.services import provider_copy_verification as verification
from automl_api.services.provider_copy_verification import (
    CopyVerificationBudget,
    logical_schema_digest,
    verify_provider_objects,
)
from automl_api.services.provider_distribution import ProviderDistribution
from automl_shared.data_identity import content_fingerprint, stable_row_id


class VersionStore:
    driver_name = "s3_compatible"

    def __init__(self):
        self.objects = {}
        self.calls = []
        self.streams = []

    def open_stream(self, uri, *, version):
        assert version is not None
        self.calls.append((uri, version))
        source = io.BytesIO(self.objects[(uri, version)])
        self.streams.append(source)
        return source


def rows(start, count, *, role=None):
    values = []
    for ordinal in range(start, start + count):
        row = {"x": ordinal + 10, "target": ordinal % 2}
        row.update(
            row_id=stable_row_id("a" * 64, ordinal),
            source_ordinal=ordinal,
            content_fingerprint=content_fingerprint(row, ["x", "target"]),
        )
        if role is not None:
            row["split_role"] = role
        values.append(row)
    return values


def install(store, role, values, *, part="part", metadata=None, fields=None):
    table = pa.Table.from_pylist(values)
    if fields:
        table = table.select(fields)
    if metadata:
        table = table.replace_schema_metadata(metadata)
    output = io.BytesIO()
    pq.write_table(table, output, write_page_checksum=True)
    content = output.getvalue()
    uri = f"s3c://safe/roles/split_role={role}/{part}.parquet"
    store.objects[(uri, "v1")] = content
    row_xor = 0
    for row in values:
        row_xor ^= int.from_bytes(hashlib.sha256(row["row_id"].encode()).digest(), "big")
    return {
        "role": role,
        "uri": uri,
        "version": "v1",
        "sha256": hashlib.sha256(content).hexdigest(),
        "byte_size": len(content),
        "rows": len(values),
        "row_digest": f"{row_xor:064x}",
    }, table.schema


def manifest(store, train=None, validation=None):
    first, schema = install(store, "train", rows(0, 2) if train is None else train)
    second, _ = install(store, "validation", rows(2, 2) if validation is None else validation)
    return ProviderDistribution(
        allocation_id=uuid.uuid4(),
        project_reference="fixture",
        scope_id=uuid.uuid4(),
        fixture_revision="fixture-v1",
        split_revision="split-v1",
        split_digest="b" * 64,
        canonical_provider="gcp",
        provider="aws",
        mode="validation_only",
        safe_data={
            "provider": "aws",
            "schema_digest": logical_schema_digest(schema),
            "objects": [first, second],
        },
        final_role_digests={"inputs": "c" * 64, "labels": "d" * 64},
    )


def budget(**changes):
    return CopyVerificationBudget(
        max_temporary_bytes=changes.get("max_temporary_bytes", 4 * 1024**2),
        max_decoded_group_bytes=changes.get("max_decoded_group_bytes", 4 * 1024**2),
        min_free_bytes=changes.get("min_free_bytes", 0),
        timeout_seconds=changes.get("timeout_seconds", 30),
    )


def change_item(value, **changes):
    raw = value.model_dump(mode="json")
    raw["safe_data"]["objects"][0].update(changes)
    return ProviderDistribution.model_validate(raw)


def test_actual_parquet_proof_uses_exact_versions_and_removes_temporary_work(tmp_path):
    store = VersionStore()
    value = manifest(store)
    before = list(tmp_path.iterdir())
    result = verify_provider_objects(value, store, budget(), temporary_root=tmp_path)
    assert result.rows == {"train": 2, "validation": 2} and result.objects == 2
    assert result.byte_size == sum(o.byte_size for o in value.safe_data.objects)
    assert result.role_digests == {r: d[1] for r, d in value.safe_data.logical_roles()[1].items()}
    assert store.calls == [(o.uri, o.version) for o in value.safe_data.objects]
    assert all(source.closed for source in store.streams)
    assert list(tmp_path.iterdir()) == before


@pytest.mark.parametrize(
    "fault", ["hash", "short", "long", "schema", "rows", "row_digest", "not_parquet"]
)
def test_declared_bytes_and_metadata_are_independently_verified(tmp_path, fault):
    store = VersionStore()
    value = manifest(store)
    item = value.safe_data.objects[0]
    if fault == "hash":
        value = change_item(value, sha256="0" * 64)
    elif fault == "short":
        value = change_item(value, byte_size=item.byte_size - 1)
    elif fault == "long":
        value = change_item(value, byte_size=item.byte_size + 1)
    elif fault == "rows":
        value = change_item(value, rows=item.rows + 1)
    elif fault == "row_digest":
        value = change_item(value, row_digest="0" * 64)
    elif fault == "schema":
        raw = value.model_dump(mode="json")
        raw["safe_data"]["schema_digest"] = "0" * 64
        value = ProviderDistribution.model_validate(raw)
    else:
        content = b"not a parquet file"
        store.objects[(item.uri, item.version)] = content
        value = change_item(
            value, byte_size=len(content), sha256=hashlib.sha256(content).hexdigest()
        )
    with pytest.raises((ValueError, OSError)):
        verify_provider_objects(value, store, budget(), temporary_root=tmp_path)
    assert all(source.closed for source in store.streams) and not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "fault",
    [
        "row_id",
        "fingerprint",
        "ordinal",
        "role",
        "duplicate",
        "cross_role_id",
        "cross_role_content",
    ],
)
def test_signed_byte_hash_cannot_bless_invalid_logical_rows(tmp_path, fault):
    store = VersionStore()
    train = rows(0, 2)
    validation = rows(2, 2)
    if fault == "row_id":
        train[0]["row_id"] = "not-a-canonical-id"
    elif fault == "fingerprint":
        train[0]["content_fingerprint"] = "0" * 64
    elif fault == "ordinal":
        train[0]["source_ordinal"] = -1
    elif fault == "role":
        train[0]["split_role"] = "final_test"
        train[1]["split_role"] = "train"
    elif fault == "duplicate":
        train[1]["row_id"] = train[0]["row_id"]
    elif fault == "cross_role_id":
        validation[0]["row_id"] = train[0]["row_id"]
    else:
        validation[0].update({k: train[0][k] for k in ["x", "target", "content_fingerprint"]})
    value = manifest(store, train, validation)
    with pytest.raises(ValueError):
        verify_provider_objects(value, store, budget(), temporary_root=tmp_path)
    assert all(s.closed for s in store.streams) and not list(tmp_path.iterdir())


def test_identical_content_is_allowed_with_distinct_ids_inside_one_role(tmp_path):
    store = VersionStore()
    train = rows(0, 2)
    train[1].update({k: train[0][k] for k in ["x", "target", "content_fingerprint"]})
    result = verify_provider_objects(
        manifest(store, train), store, budget(), temporary_root=tmp_path
    )
    assert result.rows == {"train": 2, "validation": 2}


def test_physical_column_order_writer_metadata_and_explicit_role_do_not_change_logical_schema(
    tmp_path,
):
    store = VersionStore()
    value = manifest(store, rows(0, 2, role="train"), rows(2, 2, role="validation"))
    objects = []
    for role, start in [("train", 0), ("validation", 2)]:
        data = rows(start, 2, role=role)
        item, _ = install(
            store, role, data, metadata={b"writer": role.encode()}, fields=list(reversed(data[0]))
        )
        objects.append(item)
    raw = value.model_dump(mode="json")
    raw["safe_data"]["objects"] = objects
    result = verify_provider_objects(
        ProviderDistribution.model_validate(raw), store, budget(), temporary_root=tmp_path
    )
    assert result.rows == {"train": 2, "validation": 2}


def test_decode_budget_rejects_large_row_group_before_row_conversion(tmp_path):
    store = VersionStore()
    train = rows(0, 2)
    for row in train:
        row["x"] = "x" * 20000
        row["content_fingerprint"] = content_fingerprint(row, ["x", "target"])
    value = manifest(store, train, rows(2, 2))
    with pytest.raises((ValueError, OSError)):
        verify_provider_objects(
            value, store, budget(max_decoded_group_bytes=4096), temporary_root=tmp_path
        )
    assert not list(tmp_path.iterdir())


def test_temporary_byte_budget_fails_before_provider_read(tmp_path):
    store = VersionStore()
    value = manifest(store, rows(0, 4096), rows(5000, 2))
    with pytest.raises(ValueError, match="temporary byte budget"):
        verify_provider_objects(
            value, store, budget(max_temporary_bytes=65536), temporary_root=tmp_path
        )
    assert not store.calls and not list(tmp_path.iterdir())


def test_identity_index_disk_budget_fails_without_an_unbounded_python_set(tmp_path):
    store = VersionStore()
    value = manifest(store, rows(0, 4096), rows(5000, 2))
    maximum = max(o.byte_size for o in value.safe_data.objects) + 4 * 4096
    with pytest.raises(ValueError, match="index exceeded"):
        verify_provider_objects(
            value, store, budget(max_temporary_bytes=maximum), temporary_root=tmp_path
        )
    assert all(s.closed for s in store.streams) and not list(tmp_path.iterdir())


def test_free_space_and_expired_progress_stop_before_object_open(tmp_path, monkeypatch):
    store = VersionStore()
    value = manifest(store)
    with pytest.raises(ValueError, match="free-space reserve"):
        verify_provider_objects(value, store, budget(min_free_bytes=2**63), temporary_root=tmp_path)
    clock = [0]
    monkeypatch.setattr(verification, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    def progress():
        clock[0] += 1

    with pytest.raises(TimeoutError):
        verify_provider_objects(
            value, store, budget(timeout_seconds=1), progress=progress, temporary_root=tmp_path
        )
    assert not store.calls and not list(tmp_path.iterdir())


def test_lost_lease_callback_closes_all_streams_and_work_files(tmp_path):
    store = VersionStore()
    value = manifest(store)
    calls = []

    def progress():
        calls.append(True)
        if len(calls) == 4:
            raise RuntimeError("attempt no longer owns verification")

    with pytest.raises(RuntimeError, match="no longer owns"):
        verify_provider_objects(value, store, budget(), progress=progress, temporary_root=tmp_path)
    assert all(s.closed for s in store.streams) and not list(tmp_path.iterdir())


def test_wrong_provider_and_missing_versions_do_not_read_current(tmp_path):
    store = VersionStore()
    value = manifest(store)
    store.driver_name = "gcs"
    with pytest.raises(ValueError, match="another provider"):
        verify_provider_objects(value, store, budget(), temporary_root=tmp_path)
    assert not store.calls
    store.driver_name = "s3_compatible"
    store.objects.clear()
    with pytest.raises(KeyError):
        verify_provider_objects(value, store, budget(), temporary_root=tmp_path)
    assert len(store.calls) == 1 and store.calls[0][1] == "v1" and not list(tmp_path.iterdir())


def test_schema_digest_rejects_duplicate_names_and_ignores_arrow_writer_metadata():
    schema = pa.schema([pa.field("x", pa.int64()), pa.field("x", pa.int64())])
    with pytest.raises(ValueError, match="repeated"):
        logical_schema_digest(schema)
    good = pa.schema([pa.field("x", pa.int64())])
    assert logical_schema_digest(good) == logical_schema_digest(
        good.with_metadata({b"writer": b"other"})
    )
