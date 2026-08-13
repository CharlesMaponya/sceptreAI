from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from automl_api.core.config import get_settings

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(settings.sqlalchemy_database_url, **engine_options(settings))
    return _engine


def engine_options(settings) -> dict:
    options: dict = {"pool_pre_ping": True}
    if not settings.sqlalchemy_database_url.startswith("postgresql"):
        return options
    if settings.environment in {"staging", "production"}:
        if settings.database_ssl_mode not in {"verify-ca", "verify-full"}:
            raise RuntimeError("Production PostgreSQL requires sslmode verify-ca or verify-full.")
        if settings.database_ssl_root_cert is None or not settings.database_ssl_root_cert.is_file():
            raise RuntimeError("Production PostgreSQL requires a readable TLS CA certificate.")
    connect_args = {
        "connect_timeout": settings.database_connect_timeout_seconds,
        "application_name": settings.database_application_name,
        "sslmode": settings.database_ssl_mode,
        "options": (
            f"-c statement_timeout={settings.database_statement_timeout_ms} "
            f"-c lock_timeout={settings.database_lock_timeout_ms} "
            "-c idle_in_transaction_session_timeout="
            f"{settings.database_idle_transaction_timeout_ms}"
        ),
    }
    if settings.database_ssl_root_cert is not None:
        connect_args["sslrootcert"] = str(settings.database_ssl_root_cert)
    options.update(
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout_seconds,
        pool_recycle=1_800,
        connect_args=connect_args,
    )
    return options


def pool_metrics() -> dict[str, int]:
    pool = get_engine().pool
    metrics: dict[str, int] = {}
    for name in ("size", "checkedin", "checkedout", "overflow"):
        value = getattr(pool, name, None)
        if callable(value):
            metrics[name] = int(value())
    return metrics


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _session_factory


def get_db() -> Generator[Session, None, None]:
    db = get_session_factory()()
    try:
        yield db
    finally:
        db.close()
