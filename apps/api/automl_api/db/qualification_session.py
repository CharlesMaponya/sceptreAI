from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from automl_api.core.config import get_settings
from automl_api.db.session import engine_options

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_qualification_engine() -> Engine:
    global _engine
    if _engine is None:
        settings = get_settings()
        if (
            settings.environment in {"staging", "production"}
            and settings.sqlalchemy_qualification_database_url
            == settings.sqlalchemy_database_url
        ):
            raise RuntimeError(
                "Production final-test authority requires a separately protected database."
            )
        authority_settings = type(
            "AuthoritySettings",
            (),
            {
                **settings.__dict__,
                "sqlalchemy_database_url": settings.sqlalchemy_qualification_database_url,
                "database_application_name": f"{settings.database_application_name}-qualification",
            },
        )()
        _engine = create_engine(
            settings.sqlalchemy_qualification_database_url,
            **engine_options(authority_settings),
        )
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
