#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${PYTHON_BIN:-}" ]]; then
  coverage_python="$PYTHON_BIN"
elif [[ -x .venv/bin/python ]]; then
  coverage_python=.venv/bin/python
elif command -v python >/dev/null 2>&1; then
  coverage_python=python
else
  coverage_python=python3
fi

"$coverage_python" scripts/generate_production_task_index.py --check
"$coverage_python" scripts/validate_phase2_evidence.py

"$coverage_python" -m pytest tests/ -v --tb=short \
  --cov \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml \
  --cov-report=html \
  --cov-report=json:coverage.json

"$coverage_python" scripts/check_coverage_thresholds.py coverage.json --minimum 90.01
