from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest


def _module():
    path = Path(__file__).parents[1] / "scripts" / "benchmark_phase2_ingestion.py"
    spec = importlib.util.spec_from_file_location("phase2_load", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


phase2 = _module()


def test_capacity_preflight_requires_payload_plus_headroom() -> None:
    failed = phase2.capacity_preflight(
        object_count=15,
        object_bytes=10 * phase2.GIB,
        available_capacity_bytes=150 * phase2.GIB,
    )
    assert failed.payload_bytes == 150 * phase2.GIB
    assert failed.required_capacity_bytes == 180 * phase2.GIB
    assert failed.passed is False
    passed = phase2.capacity_preflight(
        object_count=15,
        object_bytes=10 * phase2.GIB,
        available_capacity_bytes=180 * phase2.GIB,
    )
    assert passed.passed is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"object_count": 0, "object_bytes": 1, "available_capacity_bytes": 1},
        {"object_count": 1, "object_bytes": 0, "available_capacity_bytes": 1},
        {"object_count": 1, "object_bytes": 1, "available_capacity_bytes": -1},
        {
            "object_count": 1,
            "object_bytes": 1,
            "available_capacity_bytes": 1,
            "headroom_fraction": -0.1,
        },
    ],
)
def test_capacity_preflight_rejects_invalid_values(kwargs) -> None:
    with pytest.raises(ValueError):
        phase2.capacity_preflight(**kwargs)


@pytest.mark.parametrize("total_size", [18, 19, 20, 21, 1024 * 1024 + 3])
def test_csv_range_is_valid_exact_and_random_access(total_size: int) -> None:
    whole = phase2.CsvRange(total_size, 0, total_size).read()
    assert len(whole) == total_size
    assert whole.startswith(b"feature,target\n")
    assert whole.endswith(b",0\n")
    assert all(len(row.split(b",")) == 2 for row in whole.splitlines())
    midpoint = total_size // 2
    assert phase2.CsvRange(total_size, midpoint, total_size - midpoint).read() == whole[midpoint:]
    assert phase2.fixture_sha256(total_size, block_size=7) == hashlib.sha256(whole).hexdigest()


def test_csv_range_rejects_invalid_ranges() -> None:
    with pytest.raises(ValueError):
        phase2.CsvRange(17, 0, 17)
    with pytest.raises(ValueError):
        phase2.CsvRange(18, -1, 1)
    with pytest.raises(ValueError):
        phase2.CsvRange(18, 0, 0)
    with pytest.raises(ValueError):
        phase2.CsvRange(18, 17, 2)


def test_rss_bound_rejects_missing_zero_and_excess_samples() -> None:
    assert phase2.rss_within_bound({"api": 100, "ui": 50}, 100)
    assert not phase2.rss_within_bound({}, 100)
    assert not phase2.rss_within_bound({"api": 0, "ui": 50}, 100)
    assert not phase2.rss_within_bound({"api": 101, "ui": 50}, 100)


def test_upload_retry_budget_is_scoped_per_transfer_unit(monkeypatch) -> None:
    class Client:
        origin = "https://app.example"

        def __init__(self) -> None:
            self.confirmed = 0
            self.receipts = []
            self.keys: list[str] = []
            self.begin_payload = None

        def request(self, path, **kwargs):
            if path.endswith("/uploads"):
                self.begin_payload = kwargs["payload"]
                return {"id": "session-1"}
            if path.endswith("/progress"):
                complete = self.confirmed == 20
                unit = (self.confirmed // 10) + 1
                return {
                    "confirmed_bytes": self.confirmed,
                    "receipts": self.receipts,
                    "next_cursor": None
                    if complete
                    else {"unit_number": unit, "offset": self.confirmed, "length": 10},
                    "complete": complete,
                }
            if path.endswith("/instructions"):
                self.keys.append(kwargs["idempotency_key"])
                return {
                    "url": "https://storage.example/object",
                    "headers": {},
                    "method": "PUT",
                    "cursor": kwargs["payload"]["cursor"],
                    "expose_response_headers": [],
                }
            if path.endswith("/complete"):
                return {"session": {"status": "object_completed"}}
            return {"status": "ready"}

    client = Client()
    attempts = {1: 0, 2: 0}

    def transfer(instruction, _stream, _origin):
        unit = instruction["cursor"]["unit_number"]
        attempts[unit] += 1
        if attempts[unit] == 1:
            raise OSError("interrupted")
        cursor = instruction["cursor"]
        client.confirmed += cursor["length"]
        client.receipts.append({**cursor, "etag": f"etag-{unit}", "provider_headers": {}})
        return {"etag": f"etag-{unit}", "provider_headers": {}, "payload_host": "storage.example"}

    monkeypatch.setattr(phase2, "put_range", transfer)
    monkeypatch.setattr(phase2.time, "sleep", lambda _seconds: None)
    result = phase2.upload_fixture(
        client=client,
        project_id="project-1",
        index=1,
        object_bytes=20,
        digest="a" * 64,
        data_region="eu-west-1",
        retry_budget=1,
    )

    assert result.status == "ready" and result.confirmed_bytes == 20
    assert attempts == {1: 2, 2: 2}
    assert len(client.keys) == len(set(client.keys)) == 4
    assert client.begin_payload["data_region"] == "eu-west-1"


def test_aggregate_goodput_is_measured_in_bits_per_second() -> None:
    assert phase2.aggregate_goodput_bits_per_second(62_500_000, 1.0) == 500_000_000
    with pytest.raises(ValueError):
        phase2.aggregate_goodput_bits_per_second(-1, 1.0)
    with pytest.raises(ValueError):
        phase2.aggregate_goodput_bits_per_second(1, 0)
