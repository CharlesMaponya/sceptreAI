"""Reject unbounded database settings before either process constructs an engine."""

from dataclasses import replace
from unittest.mock import Mock

import pytest
from automl_api.core.config import Settings
from automl_api.db import qualification_session, session
from sqlalchemy import create_engine, text
from sqlalchemy.exc import TimeoutError

pytest_plugins = ["test_phase1_postgres_contracts"]

LIMITS = {
    "database_pool_size": 1,
    "database_max_overflow": 0,
    "database_pool_timeout_seconds": 1,
    "database_connect_timeout_seconds": 1,
    "database_statement_timeout_ms": 1,
    "database_lock_timeout_ms": 1,
    "database_idle_transaction_timeout_ms": 1,
}
INVALID = [
    (name, value) for name, minimum in LIMITS.items() for value in (minimum - 1, True, "1", 1.5)
]


@pytest.mark.parametrize("pooler", [False, True])
@pytest.mark.parametrize("name,value", INVALID)
def test_database_limits_cannot_disable_resource_bounds(pooler, name, value):
    settings = replace(Settings(pgbouncer_transaction_mode=pooler), **{name: value})
    with pytest.raises(RuntimeError, match=name):
        session.engine_options(settings)


@pytest.mark.parametrize("pooler", [False, True])
@pytest.mark.parametrize("authority", [False, True])
@pytest.mark.parametrize("name", list(LIMITS))
def test_invalid_limits_never_construct_an_application_or_authority_engine(
    monkeypatch, pooler, authority, name
):
    module = qualification_session if authority else session
    settings = replace(Settings(pgbouncer_transaction_mode=pooler), **{name: LIMITS[name] - 1})
    monkeypatch.setattr(module, "_engine", None)
    monkeypatch.setattr(module, "get_settings", lambda: settings)
    create = Mock(side_effect=AssertionError("Invalid limits reached engine construction"))
    monkeypatch.setattr(module, "create_engine", create)
    factory = module.get_qualification_engine if authority else module.get_engine
    with pytest.raises(RuntimeError, match=name):
        factory()
    create.assert_not_called()


@pytest.mark.parametrize("pooler", [False, True])
def test_finite_minimum_limits_and_zero_overflow_are_supported(pooler):
    options = session.engine_options(replace(Settings(pgbouncer_transaction_mode=pooler), **LIMITS))
    assert options["pool_size"] == 1 and options["max_overflow"] == 0
    assert options["pool_timeout"] == options["connect_args"]["connect_timeout"] == 1
    if pooler:
        assert "options" not in options["connect_args"]
        assert options["connect_args"]["prepare_threshold"] is None
    else:
        assert "statement_timeout=1" in options["connect_args"]["options"]
        assert "lock_timeout=1" in options["connect_args"]["options"]
        assert "idle_in_transaction_session_timeout=1" in options["connect_args"]["options"]


def test_real_postgresql_pool_rejects_excess_checkout_and_recovers(phase1_engine, monkeypatch):
    settings = Settings(
        database_url=phase1_engine.url.render_as_string(hide_password=False),
        database_pool_size=1,
        database_max_overflow=0,
        database_pool_timeout_seconds=1,
        database_application_name="phase1-finite-pool-test",
    )
    engine = create_engine(settings.sqlalchemy_database_url, **session.engine_options(settings))
    monkeypatch.setattr(session, "_engine", engine)
    try:
        with engine.connect() as first:
            backend = first.scalar(text("select pg_backend_pid()"))
            assert first.scalar(text("show application_name")) == settings.database_application_name
            with pytest.raises(TimeoutError):
                with engine.connect():
                    pytest.fail("A bounded pool admitted an excess connection")
            assert session.pool_metrics()["checkedout"] == 1
            assert first.scalar(text("select 1")) == 1
        assert session.pool_metrics()["checkedout"] == 0
        with engine.connect() as recovered:
            assert recovered.scalar(text("select pg_backend_pid()")) == backend
        assert session.pool_metrics()["checkedin"] == 1
        assert session.pool_metrics()["overflow"] == 0
    finally:
        engine.dispose()
