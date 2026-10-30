from types import SimpleNamespace

import pytest
from fault_worker_environment import database_url, environment
from sqlalchemy.engine import make_url


def test_fault_child_uses_the_parent_test_database_and_import_paths(monkeypatch):
    engine = SimpleNamespace(
        url=make_url("postgresql+psycopg://worker:owned@ci-db:5432/automl_tests")
    )
    monkeypatch.setenv("PYTHONPATH", "existing-runtime")
    values = environment(engine)
    assert make_url(values["SCEPTRE_TEST_DATABASE_URL"]) == engine.url
    assert values["PYTHONPATH"].startswith("existing-runtime")
    assert "apps/api" in values["PYTHONPATH"] and "tests" in values["PYTHONPATH"]
    monkeypatch.setenv("SCEPTRE_TEST_DATABASE_URL", values["SCEPTRE_TEST_DATABASE_URL"])
    assert database_url() == values["SCEPTRE_TEST_DATABASE_URL"]


def test_fault_controls_refuse_an_application_database(monkeypatch):
    value = "postgresql+psycopg://worker:owned@ci-db:5432/application"
    monkeypatch.setenv("SCEPTRE_TEST_DATABASE_URL", value)
    with pytest.raises(ValueError, match="dedicated _tests"):
        database_url()
    with pytest.raises(ValueError, match="dedicated _tests"):
        environment(SimpleNamespace(url=make_url(value)))
