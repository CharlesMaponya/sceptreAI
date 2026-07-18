from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from sqlalchemy import text

from automl_api.api.routes import (
    auth,
    datasets,
    monitoring,
    operations,
    profiling,
    projects,
    training,
    validation,
)
from automl_api.core.config import get_settings
from automl_api.db.session import get_engine
from automl_api.services.profiling_jobs import resume_incomplete_profiling_jobs
from automl_api.storage.object_store import get_object_store


@asynccontextmanager
async def lifespan(_: FastAPI):
    resume_incomplete_profiling_jobs()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="SMME Tabular AutoML API",
        version="0.1.0",
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
        lifespan=lifespan,
    )

    @app.get("/health/live", tags=["health"])
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    def ready(response: Response) -> dict[str, str]:
        try:
            with get_engine().connect() as connection:
                connection.execute(text("select 1"))
        except Exception as exc:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "degraded", "database": "unavailable", "detail": str(exc)}

        try:
            get_object_store().healthcheck()
        except Exception as exc:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {
                "status": "degraded",
                "database": "ok",
                "object_store": "unavailable",
                "detail": str(exc),
            }

        return {"status": "ok", "database": "ok", "object_store": "ok"}

    app.include_router(auth.router, prefix="/api/v1")
    app.include_router(projects.router, prefix="/api/v1")
    app.include_router(datasets.router, prefix="/api/v1")
    app.include_router(profiling.router, prefix="/api/v1")
    app.include_router(training.router, prefix="/api/v1")
    app.include_router(validation.router, prefix="/api/v1")
    app.include_router(operations.router, prefix="/api/v1")
    app.include_router(monitoring.router, prefix="/api/v1")

    return app


app = create_app()
