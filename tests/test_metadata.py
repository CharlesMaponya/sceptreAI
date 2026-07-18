from __future__ import annotations

import ast
from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from automl_api.db.base import Base

ROOT = Path(__file__).resolve().parents[1]


def _migration_table_calls(function_name: str, operation_name: str) -> set[str]:
    migration_path = ROOT / "alembic" / "versions" / "0001_initial_schema.py"
    module = ast.parse(migration_path.read_text())
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == function_name
    )
    return {
        call.args[0].value
        for call in ast.walk(function)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "op"
        and call.func.attr == operation_name
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    }


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
        "refresh_tokens",
        "password_reset_tokens",
    }

    assert expected_tables.issubset(Base.metadata.tables.keys())


def test_initial_migration_covers_all_registered_tables() -> None:
    expected_tables = set(Base.metadata.tables)

    assert _migration_table_calls("upgrade", "create_table") == expected_tables
    assert _migration_table_calls("downgrade", "drop_table") == expected_tables


def test_database_migrations_have_exactly_one_head() -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))

    assert ScriptDirectory.from_config(config).get_heads() == [
        "0002_expand_artifact_kind"
    ]


def test_artifact_kind_growth_has_a_forward_migration() -> None:
    migration = (
        ROOT / "alembic" / "versions" / "0002_expand_artifact_kind.py"
    ).read_text()

    assert 'down_revision = "0001_initial"' in migration
    assert '"GOVERNANCE_REPORT"' in migration
    assert 'sa.String(length=16)' in migration


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
