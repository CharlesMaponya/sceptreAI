from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _module():
    path = Path(__file__).parents[1] / "scripts" / "validate_phase0a_schedule.py"
    spec = importlib.util.spec_from_file_location("phase0a_schedule", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_schedule_has_capacity_headroom_and_deadline() -> None:
    module = _module()
    for profile in module.PROFILES:
        result = module.schedule(profile)
        assert result["unschedulable_placements"] == 0
        assert result["quota_headroom_fraction"] >= 0.20
        assert result["wall_seconds"]["maximum"] <= module.DEADLINE_SECONDS


def test_cost_envelope_is_monotonic() -> None:
    module = _module()
    for profile in module.PROFILES:
        expected = module.cost(profile, multiplier=1.0)
        p90 = module.cost(profile, multiplier=1.35)
        worst = module.cost(profile, multiplier=2.0)
        assert 0 < expected < p90 < worst
