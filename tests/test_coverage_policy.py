from __future__ import annotations

import importlib.util
import json
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_backend_coverage_policy_is_shared_by_local_and_ci() -> None:
    configuration = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    coverage = configuration["tool"]["coverage"]
    runner = (ROOT / "scripts/test_backend.sh").read_text(encoding="utf-8")
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    test_steps = workflow["jobs"]["test"]["steps"]

    assert coverage["run"]["branch"] is True
    assert set(coverage["run"]["source"]) == {"automl_api", "automl_shared"}
    assert coverage["report"]["fail_under"] > 90
    assert '"$coverage_python" -m pytest tests/' in runner
    assert "--cov-branch" in runner
    assert "check_coverage_thresholds.py coverage.json --minimum 90.01" in runner
    assert any(step.get("run") == "scripts/test_backend.sh" for step in test_steps)


def test_backend_coverage_validator_enforces_each_dimension(tmp_path) -> None:
    spec = importlib.util.spec_from_file_location(
        "check_coverage_thresholds", ROOT / "scripts" / "check_coverage_thresholds.py"
    )
    assert spec is not None and spec.loader is not None
    check_coverage_thresholds = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check_coverage_thresholds)

    assert check_coverage_thresholds._percent(9, 10) == 90
    assert check_coverage_thresholds._percent(0, 0) == 100


def test_frontend_coverage_policy_is_shared_by_local_and_ci() -> None:
    package = json.loads(
        (ROOT / "apps/ui/react_app/package.json").read_text(encoding="utf-8")
    )
    vitest = (ROOT / "apps/ui/react_app/vitest.config.ts").read_text(encoding="utf-8")
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    frontend_steps = workflow["jobs"]["frontend"]["steps"]

    assert package["scripts"]["test:coverage"] == "vitest run --coverage"
    assert package["devDependencies"]["@vitest/coverage-v8"] == "4.1.9"
    for metric in ("statements", "branches", "functions", "lines"):
        assert f"{metric}: 90.01" in vitest
    assert any(step.get("run") == "npm run test:coverage" for step in frontend_steps)
