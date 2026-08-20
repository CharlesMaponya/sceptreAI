from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import yaml

ROOT = Path(__file__).resolve().parents[1]
GUIDE = ROOT / "docs" / "production-readiness" / "implementation-guide.md"
INDEX = ROOT / "docs" / "production-readiness" / "task-index.yaml"


def _load_generator() -> ModuleType:
    path = ROOT / "scripts" / "generate_production_task_index.py"
    spec = importlib.util.spec_from_file_location("generate_production_task_index", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_task_index_covers_every_phase_work_and_gate_list() -> None:
    generator = _load_generator()
    bullets = generator.parse_guide(GUIDE)
    coverage = {(bullet.phase, bullet.kind) for bullet in bullets}

    for phase in ["0", "0A", *(str(number) for number in range(1, 14))]:
        assert (phase, "work") in coverage
        assert (phase, "gate") in coverage


def test_checked_in_task_index_matches_guide_without_renumbering() -> None:
    generator = _load_generator()
    existing = yaml.safe_load(INDEX.read_text())
    regenerated = generator.build_index(GUIDE, existing)

    generator._validate(regenerated)
    assert regenerated == existing


def test_unchanged_guide_preserves_the_commit_bound_to_its_digest() -> None:
    generator = _load_generator()
    introducing_commit = "a" * 40
    existing = {
        "guide": {
            "sha256": hashlib.sha256(GUIDE.read_bytes()).hexdigest(),
            "commit": introducing_commit,
        },
        "entries": [],
    }

    regenerated = generator.build_index(GUIDE, existing)

    assert regenerated["guide"]["commit"] == introducing_commit
    assert {entry["guide_commit"] for entry in regenerated["entries"]} == {
        introducing_commit
    }
