from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOTS = (ROOT / "apps" / "api", ROOT / "packages", ROOT / "infra" / "helm")
RUNTIME_FILES = (
    ROOT / "pyproject.toml",
    ROOT / "requirements-training.txt",
    ROOT / "requirements-inference.txt",
    ROOT / "requirements-inference-upload.txt",
    *ROOT.glob("Dockerfile*"),
)


def main() -> None:
    candidates = list(RUNTIME_FILES)
    for root in RUNTIME_ROOTS:
        candidates.extend(path for path in root.rglob("*") if path.is_file())
    violations = []
    for path in candidates:
        if any(part in {"__pycache__", "node_modules"} for part in path.parts):
            continue
        try:
            content = path.read_text(encoding="utf-8").lower()
        except UnicodeDecodeError:
            continue
        if "dask" in content:
            violations.append(str(path.relative_to(ROOT)))
    if violations:
        raise SystemExit("Dask is forbidden in runtime paths: " + ", ".join(sorted(violations)))
    print("Runtime dependency guard passed: Polars + Ray only.")


if __name__ == "__main__":
    main()
