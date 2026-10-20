"""The operator entrypoint cannot accept client-selected trust or expose locations."""

import importlib.util
import json
import sys
from unittest.mock import MagicMock

import pytest
from automl_api.db import session
from automl_api.models.datasets import DatasetVersion
from automl_api.storage import object_store
from test_provider_import_consumers import source_case as source_case  # noqa: F401
from test_provider_import_registration import registration_case as registration_case  # noqa: F401

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def command(source_case, tmp_path, monkeypatch):
    c = source_case
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(c.receipt))
    limits = tmp_path / "budget.json"
    limits.write_text(c.limits.model_dump_json())
    argv = [
        "import_provider_dataset.py",
        str(receipt),
        "--project-id",
        str(c.project_id),
        "--allocation-id",
        str(c.value.allocation_id),
        "--provider",
        "aws",
        "--actor-id",
        str(c.actor_id),
        "--budget",
        str(limits),
        "--name",
        "Signed safe fixture",
    ]
    spec = importlib.util.spec_from_file_location(
        "provider_import_cli", "scripts/import_provider_dataset.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setattr(session, "get_session_factory", lambda: c.factory)
    store = MagicMock(side_effect=AssertionError("Completed replay must not construct storage"))
    monkeypatch.setattr(object_store, "get_object_store", store)
    return c, cli, argv, store


def test_operator_cli_resolves_server_trust_and_replays_without_storage(command, capsys):
    c, cli, _, store = command
    cli.main()
    output = capsys.readouterr()
    value = json.loads(output.out)
    assert value["verified"] and value["mode"] == "validation_only"
    assert value["dataset_version_id"] == str(c.version_id)
    assert "s3" not in output.out and output.err == ""
    store.assert_not_called()
    with c.factory() as db:
        assert db.get(DatasetVersion, c.version_id).source_kind == "provider_distribution"


def test_operator_cli_rejects_unapproved_binding_before_database_or_storage(
    command, monkeypatch, capsys
):
    _, cli, _, store = command
    monkeypatch.delenv("PROVIDER_IMPORT_POLICIES_FILE")
    database = MagicMock(side_effect=AssertionError("Unapproved policy reached database"))
    monkeypatch.setattr(session, "get_session_factory", database)
    with pytest.raises(SystemExit):
        cli.main()
    output = capsys.readouterr()
    assert output.err == "Provider dataset import failed\n" and output.out == ""
    database.assert_not_called()
    store.assert_not_called()
