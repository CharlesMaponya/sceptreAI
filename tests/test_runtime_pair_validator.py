import copy
from pathlib import Path

import pytest

from scripts.validate_runtime_pair import parse_report, verify_pair


def reports():
    api = {
        "profile": "api",
        "inventory": {"packages": ["api"]},
        "identity": {"lock": "known"},
        "catalogs": {
            name: {} for name in ("classification", "regression", "time_series", "clustering")
        },
        "source_sha256": {"file": "current"},
        "training_protocol": "current",
        "fits": [],
        "controls": {},
    }
    cpu = copy.deepcopy(api)
    cpu.update(
        profile="cpu",
        inventory={"packages": ["cpu"]},
        fits=[
            {
                "estimator": "Ridge",
                "alpha": 2.0,
                "best_parameters": {"model__alpha": 2.0},
                "catalog_unchanged": True,
                "finite_predictions": True,
                "serialization_roundtrip": True,
            }
        ],
    )
    cpu["controls"] = dict.fromkeys(
        ("changed_catalog_denied", "api_catalog_consumed", "unsafe_parameters_denied"), True
    )
    return api, cpu


@pytest.mark.parametrize(
    "problem", [None, "sources", "catalog", "lock", "fits", "tasks", "inventory", "controls"]
)
def test_image_pair_oracle_requires_matching_current_artifacts_and_real_fit_evidence(problem):
    api, cpu = reports()
    if problem == "sources":
        api["source_sha256"] = cpu["source_sha256"] = {"file": "old"}
    elif problem == "catalog":
        cpu["catalogs"]["regression"] = {"changed": True}
    elif problem == "lock":
        cpu["identity"]["lock"] = "changed"
    elif problem == "fits":
        cpu["fits"] = []
    elif problem == "tasks":
        api["catalogs"].pop("clustering")
        cpu["catalogs"].pop("clustering")
    elif problem == "inventory":
        cpu["inventory"] = api["inventory"]
    elif problem == "controls":
        cpu["controls"]["changed_catalog_denied"] = False
    if problem:
        with pytest.raises(AssertionError):
            verify_pair(api, cpu, {"file": "current"})
    else:
        verify_pair(api, cpu, {"file": "current"})


def test_report_parsing_does_not_accept_missing_or_duplicate_success_output():
    assert parse_report('log\nRUNTIME_PAIR_REPORT={"ok": true}\n') == {"ok": True}
    for output in ("log", "RUNTIME_PAIR_REPORT={}\nRUNTIME_PAIR_REPORT={}"):
        with pytest.raises(ValueError):
            parse_report(output)


def test_release_pair_check_precedes_every_version_tag_publication():
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
    release = text.split("  publish-release:", 1)[1]
    assert release.index("scripts/validate_runtime_pair.py") < release.index(
        "Publish clean version tags"
    )
    assert '--api-image "${IMAGE}@$(cat "$RUNNER_TEMP/digests/api")"' in release
    assert '--cpu-image "${IMAGE}@$(cat "$RUNNER_TEMP/digests/training-cpu")"' in release
