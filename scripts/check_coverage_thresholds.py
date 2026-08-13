#!/usr/bin/env python3
"""Enforce coverage dimensions independently from Coverage.py's combined score."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def _percent(covered: int, total: int) -> float:
    return 100.0 if total == 0 else covered * 100.0 / total


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--minimum", type=float, default=90.01)
    args = parser.parse_args()
    totals = json.loads(args.report.read_text(encoding="utf-8"))["totals"]
    dimensions = {
        "statements": _percent(totals["covered_lines"], totals["num_statements"]),
        "branches": _percent(totals["covered_branches"], totals["num_branches"]),
    }
    for name, value in dimensions.items():
        print(f"backend {name}: {value:.3f}% (minimum {args.minimum:.2f}%)")
    failed = {
        name: value
        for name, value in dimensions.items()
        if not math.isfinite(value) or value < args.minimum
    }
    if failed:
        print("coverage threshold failed: " + ", ".join(sorted(failed)))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
