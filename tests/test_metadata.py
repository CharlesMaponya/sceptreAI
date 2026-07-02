from __future__ import annotations

from automl_api.db.base import Base


def test_core_tables_are_registered() -> None:
    expected_tables = {
        "users",
        "projects",
        "project_memberships",
        "project_share_links",
        "datasets",
        "dataset_versions",
        "profiling_jobs",
        "model_runs",
        "metrics",
        "run_artifacts",
        "model_registry_entries",
    }

    assert expected_tables.issubset(Base.metadata.tables.keys())


def test_project_id_is_present_on_isolated_tables() -> None:
    isolated_tables = [
        "datasets",
        "dataset_versions",
        "profiling_jobs",
        "model_runs",
        "metrics",
        "run_artifacts",
        "model_registry_entries",
    ]

    for table_name in isolated_tables:
        assert "project_id" in Base.metadata.tables[table_name].columns
