from __future__ import annotations

import asyncio
import importlib.util
import sys
import tomllib
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
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.security.tokens import create_signed_token
from automl_api.training import analysis, pipeline, worker
from fastapi import HTTPException, Response
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials


def test_packaging_is_bounded_to_the_qualified_python_312_runtime() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text())
    assert project["project"]["requires-python"] == ">=3.12,<3.13"
    requirements = Path("requirements-training.txt").read_text()
    assert "boto3==1.43.56" in project["project"]["dependencies"]
    assert "botocore==1.43.56" in project["project"]["dependencies"]
    assert "boto3==1.43.56\nbotocore==1.43.56\n" in requirements
    for dependency in (
        "azure-identity",
        "azure-storage-blob",
        "boto3",
        "botocore",
        "google-cloud-storage",
        "httpx",
        "s3fs",
    ):
        assert dependency in requirements
    assert "minio" not in requirements.lower()
    for module in (
        "azure.identity",
        "azure.storage.blob",
        "boto3",
        "botocore",
        "google.cloud.storage",
        "httpx",
        "s3fs",
    ):
        assert importlib.util.find_spec(module) is not None
    assert 'python-version: "3.12"' in Path(".github/workflows/ci.yml").read_text()
    for dockerfile in ("Dockerfile.api", "Dockerfile.training.cpu"):
        assert Path(dockerfile).read_text().startswith("FROM python:3.12.13-slim-trixie@sha256:")


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
    monkeypatch.setattr(
        "automl_api.main.get_settings",
        lambda: Settings(environment="test", object_store_type="embedded"),
    )
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


def _session_context(session):
    context = MagicMock()
    context.__enter__.return_value = session
    return context


def test_worker_fenced_context_is_atomic_and_required_as_a_pair(monkeypatch) -> None:
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    monkeypatch.delenv("AUTOML_FENCING_TOKEN", raising=False)
    assert worker._fenced_attempt_context() is None
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="must be set together"):
        worker._fenced_attempt_context()
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "fence")
    assert worker._fenced_attempt_context()[1] == "fence"


def test_worker_begins_only_the_matching_submitted_attempt(monkeypatch) -> None:
    run_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "fence")
    attempt = SimpleNamespace(
        id=attempt_id,
        model_run_id=run_id,
        fencing_token="fence",
        status=AttemptStatus.SUBMITTED,
        heartbeat_at=None,
        terminal_cas_version=0,
    )
    durable_run = SimpleNamespace(
        id=run_id,
        status=RunStatus.QUEUED,
        started_at=None,
    )
    db = MagicMock()
    db.scalar.return_value = attempt
    db.get.return_value = durable_run
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: _session_context(db))

    assert worker._begin_fenced_attempt(run_id) == (attempt_id, "fence")
    assert attempt.status == AttemptStatus.RUNNING
    assert attempt.heartbeat_at is not None
    assert durable_run.status == RunStatus.RUNNING
    db.commit.assert_called_once()

    attempt.status = AttemptStatus.SUPERSEDED
    with pytest.raises(worker.StaleFence, match="cannot start"):
        worker._begin_fenced_attempt(run_id)
    attempt.status = AttemptStatus.RUNNING
    attempt.fencing_token = "new-fence"
    with pytest.raises(worker.StaleFence, match="stale run fence"):
        worker._begin_fenced_attempt(run_id)
    db.scalar.return_value = None
    with pytest.raises(ValueError, match="does not belong"):
        worker._begin_fenced_attempt(run_id)


def test_worker_terminal_cas_publishes_once_and_rejects_missing_artifact(monkeypatch) -> None:
    run_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    context = (attempt_id, "fence")
    attempt = SimpleNamespace(
        id=attempt_id,
        model_run_id=run_id,
        terminal_cas_version=0,
    )
    durable_run = SimpleNamespace(
        id=run_id,
        tags={"winner_model_artifact_uri": "s3://models/winner"},
        status=RunStatus.RUNNING,
        finished_at=None,
    )
    db = MagicMock()
    db.scalar.side_effect = [attempt, durable_run]
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: _session_context(db))
    terminal_cas = MagicMock(return_value=True)
    monkeypatch.setattr(worker, "cas_register_terminal_artifact", terminal_cas)

    worker._complete_fenced_attempt(run_id, context)

    terminal_cas.assert_called_once_with(
        db,
        attempt_id=attempt_id,
        fencing_token="fence",
        expected_cas_version=0,
        checkpoint_uri="s3://models/winner",
    )
    assert durable_run.status == RunStatus.SUCCEEDED
    assert durable_run.finished_at is not None

    durable_run.tags = {}
    db.scalar.side_effect = [attempt, durable_run]
    with pytest.raises(ValueError, match="was not published"):
        worker._complete_fenced_attempt(run_id, context)
    db.scalar.side_effect = [None, durable_run]
    with pytest.raises(ValueError, match="lineage is missing"):
        worker._complete_fenced_attempt(run_id, context)
    db.scalar.side_effect = [attempt, durable_run]
    durable_run.tags = {"winner_model_artifact_uri": "s3://models/winner"}
    terminal_cas.return_value = False
    with pytest.raises(worker.StaleFence, match="CAS was rejected"):
        worker._complete_fenced_attempt(run_id, context)


def test_worker_failure_is_fenced_and_cannot_overwrite_a_successor(monkeypatch) -> None:
    run_id = uuid.uuid4()
    attempt_id = uuid.uuid4()
    context = (attempt_id, "fence")
    attempt = SimpleNamespace(
        id=attempt_id,
        model_run_id=run_id,
        fencing_token="fence",
        status=AttemptStatus.RUNNING,
        terminal_cas_version=0,
        terminal_reason=None,
    )
    durable_run = SimpleNamespace(
        id=run_id,
        status=RunStatus.RUNNING,
        failure_code=None,
        failure_message=None,
        finished_at=None,
    )
    db = MagicMock()
    db.scalar.side_effect = [attempt, durable_run]
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: _session_context(db))

    assert worker._fail_fenced_attempt(run_id, context, RuntimeError("fit failed"))
    assert attempt.status == AttemptStatus.FAILED
    assert durable_run.status == RunStatus.FAILED
    assert durable_run.failure_code == "ray_attempt_failed"

    attempt.status = AttemptStatus.SUPERSEDED
    db.scalar.side_effect = [attempt]
    assert not worker._fail_fenced_attempt(run_id, context, RuntimeError("late"))
    attempt.fencing_token = "replacement"
    db.scalar.side_effect = [attempt]
    with pytest.raises(worker.StaleFence, match="stale run fence"):
        worker._fail_fenced_attempt(run_id, context, RuntimeError("late"))
    db.scalar.side_effect = [None]
    with pytest.raises(ValueError, match="lineage is missing"):
        worker._fail_fenced_attempt(run_id, context, RuntimeError("missing"))
