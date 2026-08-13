from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]


def _load_generator() -> ModuleType:
    path = ROOT / "scripts" / "generate_phase0_snapshots.py"
    spec = importlib.util.spec_from_file_location("generate_phase0_snapshots", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_phase0_snapshot_manifest_matches_generated_files(tmp_path: Path) -> None:
    manifest = _load_generator().generate(tmp_path)

    assert manifest["status"] in {"frozen", "provisional"}
    assert manifest["source"]["worktree_clean"] is (manifest["status"] == "frozen")
    assert set(manifest["snapshots"]) == {
        "database-metadata.json",
        "estimator-catalog.json",
        "openapi.json",
        "python-dependencies.json",
    }
    for filename, identity in manifest["snapshots"].items():
        path = tmp_path / filename
        assert path.stat().st_size == identity["bytes"]
        assert _sha256(path) == identity["sha256"]

    database = json.loads((tmp_path / "database-metadata.json").read_text())
    assert database["table_count"] == len(database["tables"])
    assert {table["name"] for table in database["tables"]}.issuperset(
        {"audit_events", "dataset_upload_sessions", "rate_limit_buckets"}
    )


def test_phase0_snapshots_are_byte_stable_for_the_same_runtime(tmp_path: Path) -> None:
    generate = _load_generator().generate
    first = generate(tmp_path)
    first_digests = {
        filename: identity["sha256"]
        for filename, identity in first["snapshots"].items()
    }

    second = generate(tmp_path)
    second_digests = {
        filename: identity["sha256"]
        for filename, identity in second["snapshots"].items()
    }

    assert second_digests == first_digests
