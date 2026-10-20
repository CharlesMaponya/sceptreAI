"""Reviewed image differences must not disable complete inventory validation."""

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest
from automl_api.models.enums import TaskType
from automl_api.training import catalog_contract as catalog
from automl_api.training.runtime_profiles import (
    locked_packages,
    reviewed_profiles,
    runtime_identity,
)


def inventory(profile):
    reviewed = reviewed_profiles()
    return {
        "python": reviewed["python"],
        "package_scope": "interpreter-site-packages-v1",
        "packages": list(reviewed["profiles"][profile].items()),
    }


def test_each_reviewed_complete_inventory_produces_the_same_identity():
    assert inventory("api") != inventory("cpu")
    assert runtime_identity(inventory("api"), profile="api", environment="production") == (
        runtime_identity(inventory("cpu"), profile="cpu", environment="production")
    )
    cpu = reviewed_profiles()["profiles"]["cpu"]
    assert {"kubernetes", "sqlglot", "argon2-cffi", "reportlab", "torch"} <= cpu.keys()


@pytest.mark.parametrize("profile", ["api", "cpu"])
@pytest.mark.parametrize("problem", ["extra", "missing", "version", "python", "duplicate", "scope"])
def test_inventory_drift_is_not_ignored(profile, problem):
    actual = inventory(profile)
    if problem == "extra":
        actual["packages"].append(("unreviewed-package", "1"))
    elif problem == "missing":
        actual["packages"].pop()
    elif problem == "version":
        actual["packages"][0] = (actual["packages"][0][0], "changed")
    elif problem == "python":
        actual["python"] = "3.12.12"
    elif problem == "scope":
        actual["package_scope"] = "unchecked"
    else:
        actual["packages"].append(actual["packages"][0])
    with pytest.raises(ValueError, match="differs"):
        runtime_identity(actual, profile=profile, environment="production")


@pytest.mark.parametrize("environment", ["production", "staging", "PRODUCTION", " Staging "])
def test_production_cannot_use_an_unqualified_local_inventory(environment):
    with pytest.raises(ValueError, match="required"):
        runtime_identity(inventory("api"), profile=None, environment=environment)


def test_wrong_or_unknown_profile_fails_closed():
    with pytest.raises(ValueError, match="Unknown"):
        runtime_identity(inventory("api"), profile="nvidia", environment="local")
    with pytest.raises(ValueError, match="differs"):
        runtime_identity(inventory("api"), profile="cpu", environment="local")


def test_development_keeps_its_entire_inventory_and_does_not_claim_a_reviewed_profile():
    actual = inventory("api")
    identity = runtime_identity(actual, profile=None, environment="local")
    changed = copy.deepcopy(actual)
    changed["packages"].append(("local-tool", "1"))
    assert identity == actual and "profiles" not in identity
    assert runtime_identity(changed, profile=None, environment="local") != identity


@pytest.mark.parametrize(
    "configured,expected",
    [(" PRODUCTION ", "production"), ("STAGING", "staging"), (" Local ", "local")],
)
def test_configuration_normalizes_environment_before_legacy_worker_guards(
    monkeypatch, configured, expected
):
    from automl_api.core import config
    from automl_api.training import parameter_overrides, trial_runtime

    monkeypatch.setattr(config, "_read_dotenv", lambda: {})
    monkeypatch.setenv("ENVIRONMENT", configured)
    for name in (
        "AUTOML_ATTEMPT_ID",
        "AUTOML_FENCING_TOKEN",
        "AUTOML_MODEL_PODS",
        "TRAINING_EXECUTION_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
    config.get_settings.cache_clear()
    try:
        observed = config.get_settings().environment
        assert observed == expected
        if expected in {"production", "staging"}:
            with pytest.raises(ValueError, match="missing from admitted intent"):
                parameter_overrides.validate_bound_overrides(None, SimpleNamespace(params={}), [])
        else:
            parameter_overrides.validate_bound_overrides(None, SimpleNamespace(params={}), [])
        assert trial_runtime.requires_durable_trials() == (expected == "production")
    finally:
        config.get_settings.cache_clear()


@pytest.mark.parametrize(
    "body",
    ["name>=1", "name==1", "-r other.lock", "name @ https://invalid/wheel", "", "--hash=bad"],
)
def test_runtime_lock_must_contain_exact_hashed_requirements(body):
    with pytest.raises(ValueError):
        locked_packages(body)


def test_duplicate_and_unhashed_prior_lock_entries_are_rejected():
    hashed = "name==1 \\\n    --hash=sha256:" + "a" * 64
    assert locked_packages(hashed) == {"name": "1"}
    with pytest.raises(ValueError, match="Duplicate"):
        locked_packages(hashed + "\n" + hashed)
    with pytest.raises(ValueError, match="no wheel hashes"):
        locked_packages("missing==1\n" + hashed)


def test_catalogs_match_between_the_reviewed_api_and_worker_profiles(monkeypatch):
    monkeypatch.setattr(catalog, "get_settings", lambda: SimpleNamespace(environment="production"))
    results = []
    for profile in ("api", "cpu"):
        monkeypatch.setenv("SCEPTRE_RUNTIME_PROFILE", profile)
        monkeypatch.setattr(catalog, "_RUNTIME_DEPENDENCIES", inventory(profile))
        results.append(
            {
                task.value: catalog.catalog_specification(task)
                for task in (
                    TaskType.CLASSIFICATION,
                    TaskType.REGRESSION,
                    TaskType.TIME_SERIES,
                    TaskType.CLUSTERING,
                )
            }
        )
    assert results[0] == results[1]


@pytest.mark.parametrize("change", ["lock", "application"])
def test_changed_reviewed_fingerprint_invalidates_a_stored_catalog(monkeypatch, change):
    from automl_api.training import runtime_profiles

    monkeypatch.setenv("SCEPTRE_RUNTIME_PROFILE", "api")
    monkeypatch.setattr(catalog, "_RUNTIME_DEPENDENCIES", inventory("api"))
    original = catalog.catalog_specification(TaskType.REGRESSION)
    row = SimpleNamespace(specification=original, content_digest=catalog.catalog_digest(original))
    assert catalog.validate_catalog_revision(row, TaskType.REGRESSION) == original
    changed = copy.deepcopy(reviewed_profiles())
    if change == "lock":
        changed["lock_sha256"]["api"] = "0" * 64
    else:
        changed["application_source_digest"] = "0" * 64
    monkeypatch.setattr(runtime_profiles, "reviewed_profiles", lambda: changed)
    with pytest.raises(ValueError, match="runtime differs"):
        catalog.validate_catalog_revision(row, TaskType.REGRESSION)


def test_normal_images_declare_their_profile_and_include_both_reviewed_locks():
    root = Path(__file__).resolve().parents[1]
    for filename, profile in (("Dockerfile.api", "api"), ("Dockerfile.training.cpu", "cpu")):
        text = (root / filename).read_text()
        assert f"SCEPTRE_RUNTIME_PROFILE={profile}" in text
        assert (
            "infra/locks/requirements-api.lock infra/locks/requirements-training-cpu.lock" in text
        )
        assert "pip==26.1.2" in text and "python:3.12.13-" in text
    worker = (root / "Dockerfile.training.cpu").read_text()
    assert (
        "-r infra/locks/requirements-api.lock -r infra/locks/requirements-training-cpu.lock"
        in worker
    )


@pytest.mark.parametrize("changed_path", ["apps/api/model.py", "packages/automl_shared/recipe.py"])
def test_source_fingerprint_changes_without_dependency_or_policy_changes(
    tmp_path, monkeypatch, changed_path
):
    from automl_api.training import runtime_profiles

    root = Path(__file__).resolve().parents[1]
    policy = tmp_path / "apps/api/automl_api/training/runtime_profiles.py"
    policy.parent.mkdir(parents=True)
    policy.write_text("# fixed policy fixture\n")
    for name in ("api", "training-cpu"):
        source = root / "infra/locks" / f"requirements-{name}.lock"
        target = tmp_path / "infra/locks" / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    changed = tmp_path / changed_path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text("value = 1\n")
    monkeypatch.setattr(runtime_profiles, "__file__", str(policy))
    reviewed_profiles.cache_clear()
    try:
        original = reviewed_profiles()
        reviewed_profiles.cache_clear()
        assert reviewed_profiles() == original
        changed.write_text("value = 2\n")
        reviewed_profiles.cache_clear()
        updated = reviewed_profiles()
        assert updated["application_source_digest"] != original["application_source_digest"]
        for key in ("lock_sha256", "profiles", "python", "policy_source_sha256"):
            assert updated[key] == original[key]
    finally:
        reviewed_profiles.cache_clear()
