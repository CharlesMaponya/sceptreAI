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


def test_capacity_respects_quota_and_counts_each_head() -> None:
    from dataclasses import replace

    module = _module()
    profile = module.PROFILES[0]
    # 11 CPUs after the reserve fit two workers, but only one head/worker pair.
    constrained = replace(profile, node=module.ResourceVector(11, 64, 150))
    assert module.schedule(constrained)["slots"] == 10
    result = module.schedule(replace(profile, quota_nodes=1))
    assert result["slots"] == 2
    assert result["unschedulable_placements"] == 13
    result = module.schedule(replace(profile, node=module.ResourceVector(1, 1, 1)))
    assert result["slots"] == 0
    assert result["required_nodes"] is None
    assert result["quota_headroom_fraction"] is None
    assert result["unschedulable_placements"] == module.RUNS


def test_cli_never_qualifies_synthetic_results(capsys, monkeypatch) -> None:
    import json
    from dataclasses import replace

    module = _module()
    for profiles, constraints_pass in (
        (module.PROFILES, True),
        ((replace(module.PROFILES[0], quota_nodes=0),), False),
    ):
        monkeypatch.setattr(module, "PROFILES", profiles)
        module.main()
        result = json.loads(capsys.readouterr().out)
        assert result["status"] == "synthetic_estimate"
        assert result["qualified"] is False
        assert result["synthetic_constraints_pass"] is constraints_pass
        assert result["limitations"]
        for envelope in result["cost_envelope"].values():
            assert "p90_usd" not in envelope
            assert "worst_case_usd" not in envelope
