from __future__ import annotations

import asyncio
import sys
import uuid
from contextlib import AbstractContextManager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api import deps
from automl_api.core.config import Settings
from automl_api.db import qualification_session
from automl_api.db import session as db_session
from automl_api.main import create_app, lifespan
from automl_api.models.enums import RunKind
from automl_api.security.tokens import create_signed_token
from automl_api.training import analysis, pipeline, worker
from fastapi import HTTPException, Response
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials


class _RunSession(AbstractContextManager):
    def __init__(self, run: object | None) -> None:
        self.run = run

    def __enter__(self) -> _RunSession:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def get(self, _model: object, _identifier: object) -> object | None:
        return self.run


def _endpoint(app: object, path: str):
    return next(
        route.endpoint
        for route in app.routes  # type: ignore[attr-defined]
        if isinstance(route, APIRoute) and route.path == path
    )


def test_current_user_dependency_validates_credentials_and_token_version(monkeypatch) -> None:
    settings = Settings(jwt_secret_key="test-secret")
    monkeypatch.setattr(deps, "get_settings", lambda: settings)
    user_id = uuid.uuid4()
    user = SimpleNamespace(id=user_id, is_active=True, token_version=2)
    token = create_signed_token(
        subject=str(user_id),
        email="user@example.com",
        token_version=2,
        secret=settings.jwt_secret_key,
        token_type="access",
        expires_delta=timedelta(minutes=5),
    )
    credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
    db = SimpleNamespace(scalar=lambda _statement: user)
    assert deps.get_current_user(credentials, db) is user

    with pytest.raises(HTTPException, match="Authentication required"):
        deps.get_current_user(None, db)
    invalid = HTTPAuthorizationCredentials(scheme="Bearer", credentials="invalid")
    with pytest.raises(HTTPException, match="Invalid or expired"):
        deps.get_current_user(invalid, db)
    with pytest.raises(HTTPException, match="inactive"):
        deps.get_current_user(credentials, SimpleNamespace(scalar=lambda _statement: None))
    user.token_version = 3
    with pytest.raises(HTTPException, match="rotated or revoked"):
        deps.get_current_user(credentials, db)


def test_database_session_singletons_and_generator_cleanup(monkeypatch) -> None:
    engine = MagicMock()
    factory = MagicMock()
    db_session._engine = None
    db_session._session_factory = None
    monkeypatch.setattr(
        db_session,
        "get_settings",
        lambda: Settings(database_url="postgresql://db/test"),
    )
    monkeypatch.setattr(db_session, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(db_session, "sessionmaker", lambda **kwargs: factory)
    assert db_session.get_engine() is engine
    assert db_session.get_engine() is engine
    assert db_session.get_session_factory() is factory
    assert db_session.get_session_factory() is factory

    session = MagicMock()
    monkeypatch.setattr(db_session, "get_session_factory", lambda: lambda: session)
    generator = db_session.get_db()
    assert next(generator) is session
    with pytest.raises(StopIteration):
        next(generator)
    session.close.assert_called_once()


def test_database_engine_options_bound_postgres_and_validate_tls(tmp_path: Path) -> None:
    settings = Settings(
        database_pool_size=7,
        database_max_overflow=2,
        database_application_name="phase1-test",
    )
    options = db_session.engine_options(settings)
    assert options["pool_size"] == 7
    assert options["max_overflow"] == 2
    assert options["connect_args"]["application_name"] == "phase1-test"
    assert "statement_timeout=30000" in options["connect_args"]["options"]
    assert db_session.engine_options(SimpleNamespace(sqlalchemy_database_url="sqlite://")) == {
        "pool_pre_ping": True
    }

    with pytest.raises(RuntimeError, match="sslmode"):
        db_session.engine_options(Settings(environment="production", database_ssl_mode="prefer"))
    with pytest.raises(RuntimeError, match="certificate"):
        db_session.engine_options(
            Settings(environment="production", database_ssl_mode="verify-full")
        )
    certificate = tmp_path / "postgres-ca.pem"
    certificate.write_text("test CA")
    secure = db_session.engine_options(
        Settings(
            environment="production",
            database_ssl_mode="verify-full",
            database_ssl_root_cert=certificate,
        )
    )
    assert secure["connect_args"]["sslrootcert"] == str(certificate)


def test_final_authority_database_is_separate_in_production(monkeypatch, tmp_path: Path) -> None:
    certificate = tmp_path / "postgres-ca.pem"
    certificate.write_text("test CA")
    shared = Settings(
        environment="production",
        database_ssl_mode="verify-full",
        database_ssl_root_cert=certificate,
    )
    qualification_session._engine = None
    monkeypatch.setattr(qualification_session, "get_settings", lambda: shared)
    with pytest.raises(RuntimeError, match="separately protected"):
        qualification_session.get_qualification_engine()

    engine = MagicMock()
    separate = Settings(
        environment="production",
        database_ssl_mode="verify-full",
        database_ssl_root_cert=certificate,
        qualification_database_url="postgresql+psycopg://authority/db",
    )
    monkeypatch.setattr(qualification_session, "get_settings", lambda: separate)
    monkeypatch.setattr(qualification_session, "create_engine", lambda *_args, **_kwargs: engine)
    assert qualification_session.get_qualification_engine() is engine
    assert qualification_session.get_qualification_engine() is engine

    factory = MagicMock()
    qualification_session._session_factory = None
    monkeypatch.setattr(qualification_session, "sessionmaker", lambda **_kwargs: factory)
    assert qualification_session.get_qualification_session_factory() is factory
    assert qualification_session.get_qualification_session_factory() is factory
    session = MagicMock()
    monkeypatch.setattr(
        qualification_session,
        "get_qualification_session_factory",
        lambda: lambda: session,
    )
    generator = qualification_session.get_qualification_db()
    assert next(generator) is session
    with pytest.raises(StopIteration):
        next(generator)
    session.close.assert_called_once()


def test_pool_metrics_export_only_supported_counters(monkeypatch) -> None:
    pool = SimpleNamespace(
        size=lambda: 10,
        checkedin=lambda: 8,
        checkedout=lambda: 2,
        overflow=lambda: 0,
    )
    monkeypatch.setattr(db_session, "get_engine", lambda: SimpleNamespace(pool=pool))
    assert db_session.pool_metrics() == {
        "size": 10,
        "checkedin": 8,
        "checkedout": 2,
        "overflow": 0,
    }


def test_api_health_checks_report_dependency_boundaries(monkeypatch) -> None:
    monkeypatch.setattr("automl_api.main.get_settings", lambda: SimpleNamespace(environment="test"))
    app = create_app()
    assert _endpoint(app, "/health/live")() == {"status": "ok"}
    ready = _endpoint(app, "/health/ready")

    connection = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    store = MagicMock()
    monkeypatch.setattr("automl_api.main.get_engine", lambda: engine)
    monkeypatch.setattr("automl_api.main.get_object_store", lambda: store)
    response = Response()
    assert ready(response) == {"status": "ok", "database": "ok", "object_store": "ok"}

    engine.connect.side_effect = RuntimeError("database down")
    response = Response()
    assert ready(response)["database"] == "unavailable"
    assert response.status_code == 503

    engine.connect.side_effect = None
    store.healthcheck.side_effect = RuntimeError("object store down")
    response = Response()
    result = ready(response)
    assert result["object_store"] == "unavailable"
    assert response.status_code == 503


def test_lifespan_resumes_profiling_jobs(monkeypatch) -> None:
    resumed = MagicMock()
    monkeypatch.setattr("automl_api.main.resume_incomplete_profiling_jobs", resumed)

    async def enter() -> None:
        async with lifespan(MagicMock()):
            resumed.assert_called_once()

    asyncio.run(enter())


def test_worker_accelerator_selection_is_explicit(monkeypatch) -> None:
    monkeypatch.setenv("AUTOML_GPU_VENDOR", "intel")
    assert worker._enable_rapids_accelerator() is False
    assert worker.os.environ["AUTOML_RAPIDS_ACTIVE"] == "0"

    installer = MagicMock()
    monkeypatch.setenv("AUTOML_GPU_VENDOR", "nvidia")
    monkeypatch.setitem(
        sys.modules,
        "cuml",
        SimpleNamespace(accel=SimpleNamespace(install=installer)),
    )
    assert worker._enable_rapids_accelerator() is True
    installer.assert_called_once()
    assert worker.os.environ["AUTOML_RAPIDS_ACTIVE"] == "1"


@pytest.mark.parametrize(
    ("run_kind", "mode", "expected"),
    [
        (RunKind.VALIDATION, "direct", "analysis"),
        (RunKind.TRAINING, "zenml", "zenml"),
        (RunKind.TRAINING, "direct", "training"),
    ],
)
def test_worker_dispatches_by_durable_run_kind(
    monkeypatch,
    run_kind: RunKind,
    mode: str,
    expected: str,
) -> None:
    run_id = uuid.uuid4()
    calls: list[str] = []
    monkeypatch.setattr(
        worker,
        "get_session_factory",
        lambda: lambda: _RunSession(SimpleNamespace(run_kind=run_kind)),
    )
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(analysis, "execute_analysis_run", lambda _run_id: calls.append("analysis"))
    monkeypatch.setattr(pipeline, "execute_training_run", lambda _run_id: calls.append("training"))
    monkeypatch.setattr(
        pipeline,
        "tabular_automl_pipeline",
        lambda **_kwargs: calls.append("zenml"),
    )
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", mode)
    monkeypatch.setattr(sys, "argv", ["worker", "--run-id", str(run_id)])

    worker.main()

    assert calls == [expected]


def test_worker_rejects_unknown_run(monkeypatch) -> None:
    run_id = uuid.uuid4()
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: _RunSession(None))
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(sys, "argv", ["worker", "--run-id", str(run_id)])

    with pytest.raises(ValueError, match="was not found"):
        worker.main()
