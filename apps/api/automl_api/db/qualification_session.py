from __future__ import annotations

from collections.abc import Generator
from dataclasses import replace

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from automl_api.core.config import get_settings
from automl_api.db.session import configure_transaction_pooling, engine_options

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_qualification_engine() -> Engine:
    global _engine
    if _engine is None:
        settings = get_settings()
        authority_url = make_url(settings.sqlalchemy_qualification_database_url)
        application_url = make_url(settings.sqlalchemy_database_url)
        if settings.environment in {"staging", "production"}:
            if authority_url.get_backend_name() != "postgresql":
                raise RuntimeError("Production authority requires PostgreSQL.")
            if not settings.qualification_database_url or (
                authority_url.host == application_url.host
                and (authority_url.port or 5432) == (application_url.port or 5432)
                and authority_url.database == application_url.database
            ):
                raise RuntimeError(
                    "Production final-test authority requires a separately protected database."
                )
        authority_settings = replace(
            settings,
            database_url=settings.sqlalchemy_qualification_database_url,
            database_application_name=f"{settings.database_application_name}-qualification",
        )
        _engine = create_engine(
            settings.sqlalchemy_qualification_database_url,
            **engine_options(authority_settings),
        )
        configure_transaction_pooling(_engine, authority_settings)
    return _engine


def get_qualification_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=get_qualification_engine(),
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )
    return _session_factory


def get_qualification_db() -> Generator[Session, None, None]:
    db = get_qualification_session_factory()()
    try:
        yield db
    finally:
        db.close()
