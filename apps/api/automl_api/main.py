from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from automl_api import __version__
from automl_api.api.routes import (
    auth,
    contracts,
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
from automl_api.services.upload_policy import (
    configured_upload_data_region,
    validate_scanner_configuration,
)
from automl_api.storage.contracts import UploadContractError, UploadExpired, UploadThrottled
from automl_api.storage.object_store import get_object_store


@asynccontextmanager
async def lifespan(_: FastAPI):
    resume_incomplete_profiling_jobs()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    validate_scanner_configuration(settings)
    configured_upload_data_region(settings)
    if hasattr(settings, "object_store_type"):
        get_object_store(settings)
    app = FastAPI(
        title="SMME Tabular AutoML API",
        version=__version__,
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(
            getattr(settings, "upload_allowed_origins", ("http://localhost:8080",))
        ),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "Idempotency-Key",
            "Origin",
            "x-amz-checksum-sha256",
            "x-ms-version",
        ],
        expose_headers=[
            "ETag",
            "Range",
            "Retry-After",
            "x-amz-checksum-sha256",
            "x-amz-request-id",
            "x-goog-hash",
            "x-ms-request-id",
            "x-ms-version",
        ],
        max_age=900,
    )

    @app.exception_handler(UploadExpired)
    async def upload_expired(_: Request, exc: UploadExpired) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(UploadThrottled)
    async def upload_throttled(_: Request, exc: UploadThrottled) -> JSONResponse:
        headers = (
            {"Retry-After": str(exc.retry_after_seconds)}
            if exc.retry_after_seconds is not None
            else None
        )
        return JSONResponse(status_code=429, content={"detail": str(exc)}, headers=headers)

    @app.exception_handler(UploadContractError)
    async def upload_contract_error(_: Request, exc: UploadContractError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

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
            health = get_object_store().healthcheck()
            if health is not None and not health.healthy:
                raise OSError(health.detail or f"{health.driver} healthcheck failed")
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
    app.include_router(contracts.router, prefix="/api/v1")
    app.include_router(projects.router, prefix="/api/v1")
    app.include_router(datasets.router, prefix="/api/v1")
    app.include_router(profiling.router, prefix="/api/v1")
    app.include_router(training.router, prefix="/api/v1")
    app.include_router(validation.router, prefix="/api/v1")
    app.include_router(operations.router, prefix="/api/v1")
    app.include_router(monitoring.router, prefix="/api/v1")

    return app


app = create_app()
