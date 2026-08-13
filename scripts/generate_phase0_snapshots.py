from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from automl_api.db.base import Base
from automl_api.main import app
from automl_api.models.enums import TaskType
from automl_api.training.model_catalog import candidate_catalog
from skopt.space import Categorical, Integer, Real
from sqlalchemy import CheckConstraint, ForeignKeyConstraint

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "docs" / "production-readiness" / "snapshots"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n").encode()


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_canonical_json(value))


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in sorted(value.items())}
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return [_json_value(item) for item in value]
    if isinstance(value, type):
        return f"{value.__module__}.{value.__qualname__}"
    return repr(value)


def _dimension(dimension: Any) -> dict[str, Any]:
    common = {
        "type": type(dimension).__name__,
        "name": dimension.name,
        "transform": dimension.transform_,
    }
    if isinstance(dimension, Categorical):
        return {
            **common,
            "categories": [_json_value(value) for value in dimension.categories],
            "prior": _json_value(dimension.prior),
        }
    if isinstance(dimension, (Integer, Real)):
        return {
            **common,
            "low": _json_value(dimension.low),
            "high": _json_value(dimension.high),
            "prior": dimension.prior,
            "base": dimension.base,
            "dtype": _json_value(dimension.dtype),
        }
    raise TypeError(f"Unsupported search dimension: {type(dimension)!r}")


def _catalog_snapshot() -> dict[str, Any]:
    tasks: dict[str, Any] = {}
    supported_tasks = (
        TaskType.CLASSIFICATION,
        TaskType.REGRESSION,
        TaskType.TIME_SERIES,
        TaskType.CLUSTERING,
    )
    for task_type in supported_tasks:
        candidates = []
        for candidate in candidate_catalog(task_type):
            estimator_type = type(candidate.estimator)
            candidates.append(
                {
                    "name": candidate.name,
                    "estimator": (
                        f"{estimator_type.__module__}.{estimator_type.__qualname__}"
                    ),
                    "default_parameters": _json_value(
                        candidate.estimator.get_params(deep=False)
                    ),
                    "search_space": {
                        name: _dimension(dimension)
                        for name, dimension in sorted(candidate.search_space.items())
                    },
                    "cost_tier": candidate.cost_tier,
                    "default_selected": candidate.default_selected,
                    "tunable": candidate.tunable,
                }
            )
        tasks[task_type.value] = {
            "candidate_count": len(candidates),
            "candidates": candidates,
        }
    return {
        "schema_revision": "sceptre-estimator-catalog-snapshot-v1",
        "tasks": tasks,
    }


def _constraint(constraint: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "type": type(constraint).__name__,
        "name": constraint.name,
        "columns": sorted(column.name for column in constraint.columns),
    }
    if isinstance(constraint, ForeignKeyConstraint):
        result["references"] = sorted(
            (
                {
                    "column": element.parent.name,
                    "target": element.target_fullname,
                    "ondelete": element.ondelete,
                    "onupdate": element.onupdate,
                }
                for element in constraint.elements
            ),
            key=lambda item: (item["column"], item["target"]),
        )
    if isinstance(constraint, CheckConstraint):
        result["sqltext"] = str(constraint.sqltext)
    return result


def _database_snapshot() -> dict[str, Any]:
    tables = []
    for table_name in sorted(Base.metadata.tables):
        table = Base.metadata.tables[table_name]
        tables.append(
            {
                "name": table.name,
                "columns": [
                    {
                        "name": column.name,
                        "type": str(column.type),
                        "nullable": column.nullable,
                        "primary_key": column.primary_key,
                        "unique": column.unique,
                        "server_default": (
                            str(column.server_default.arg)
                            if column.server_default is not None
                            else None
                        ),
                    }
                    for column in table.columns
                ],
                "indexes": sorted(
                    (
                        {
                            "name": index.name,
                            "unique": index.unique,
                            "columns": [column.name for column in index.columns],
                        }
                        for index in table.indexes
                    ),
                    key=lambda item: item["name"] or "",
                ),
                "constraints": sorted(
                    (_constraint(constraint) for constraint in table.constraints),
                    key=lambda item: (item["type"], item["name"] or ""),
                ),
            }
        )
    return {
        "schema_revision": "sceptre-database-metadata-snapshot-v1",
        "naming_convention": dict(Base.metadata.naming_convention or {}),
        "table_count": len(tables),
        "tables": tables,
    }


def _dependency_snapshot() -> dict[str, Any]:
    distributions = sorted(
        (
            {
                "name": distribution.metadata["Name"],
                "version": distribution.version,
            }
            for distribution in importlib.metadata.distributions()
            if distribution.metadata["Name"]
        ),
        key=lambda item: item["name"].lower(),
    )
    return {
        "schema_revision": "sceptre-python-dependency-snapshot-v1",
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "distributions": distributions,
    }


def generate(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots = {
        "openapi.json": app.openapi(),
        "database-metadata.json": _database_snapshot(),
        "estimator-catalog.json": _catalog_snapshot(),
        "python-dependencies.json": _dependency_snapshot(),
    }
    for filename, contents in snapshots.items():
        _write_json(output_dir / filename, contents)

    git_sha = _git("rev-parse", "HEAD")
    worktree_clean = not bool(_git("status", "--porcelain"))
    inputs = [
        "pyproject.toml",
        "requirements-training.txt",
        "apps/ui/react_app/package-lock.json",
        "infra/helm/sceptre/Chart.yaml",
        "infra/helm/sceptre/values.schema.json",
        "docs/production-readiness/implementation-guide.md",
    ]
    manifest = {
        "schema_revision": "sceptre-phase-0-baseline-manifest-v1",
        "status": "frozen" if worktree_clean else "provisional",
        "source": {
            "git_sha": git_sha,
            "worktree_clean": worktree_clean,
        },
        "generator": {
            "path": "scripts/generate_phase0_snapshots.py",
            "sha256": _sha256(ROOT / "scripts" / "generate_phase0_snapshots.py"),
        },
        "inputs": {
            path: {
                "sha256": _sha256(ROOT / path),
                "bytes": (ROOT / path).stat().st_size,
            }
            for path in inputs
        },
        "snapshots": {
            filename: {
                "sha256": _sha256(output_dir / filename),
                "bytes": (output_dir / filename).stat().st_size,
            }
            for filename in sorted(snapshots)
        },
    }
    _write_json(output_dir / "baseline-manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    output_dir = arguments.output_dir.resolve()
    if arguments.check:
        with tempfile.TemporaryDirectory(prefix="sceptre-phase0-") as temporary:
            generated_dir = Path(temporary)
            manifest = generate(generated_dir)
            filenames = [*manifest["snapshots"], "baseline-manifest.json"]
            stale = [
                filename
                for filename in filenames
                if not (output_dir / filename).exists()
                or (output_dir / filename).read_bytes()
                != (generated_dir / filename).read_bytes()
            ]
        if stale:
            raise SystemExit(f"Phase 0 snapshots are stale: {', '.join(sorted(stale))}")
        print(f"validated {len(filenames)} Phase 0 snapshot files")
        return

    manifest = generate(output_dir)
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
