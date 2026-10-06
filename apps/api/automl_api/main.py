from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest
from sqlalchemy import text

from automl_api import __version__
from automl_api.api.routes import (
    auth,
    contracts,
    datasets,
    evaluation_control,
    monitoring,
    oidc,
    operations,
    profiling,
    projects,
    refit_control,
    serving_artifacts,
    training,
    validation,
)
from automl_api.core.config import get_settings
from automl_api.core.structured_logging import bind_logging_context, configure_structured_logging
from automl_api.db.session import get_engine, pool_metrics
from automl_api.security.authentication_policy import validate_authentication_configuration
from automl_api.services.profiling_jobs import resume_incomplete_profiling_jobs
from automl_api.services.upload_policy import (
    configured_upload_data_region,
    validate_scanner_configuration,
)
from automl_api.storage.contracts import UploadContractError, UploadExpired, UploadThrottled
from automl_api.storage.object_store import get_object_store


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_structured_logging()
    resume_incomplete_profiling_jobs()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    validate_scanner_configuration(settings)
    validate_authentication_configuration(settings)
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

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        registry = CollectorRegistry()
        descriptions = {
            "size": "Configured persistent connection capacity of this process pool.",
            "checkedin": "Idle connections in this process pool.",
            "checkedout": "Connections checked out from this process pool.",
            "overflow": "Connections above persistent capacity in this process pool.",
        }
        for name, value in pool_metrics().items():
            Gauge(f"sceptre_database_pool_{name}", descriptions[name], registry=registry).set(
                max(0, value) if name == "overflow" else value
            )
        return Response(
            generate_latest(registry),
            headers={"Content-Type": CONTENT_TYPE_LATEST, "Cache-Control": "no-store"},
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
    app.include_router(oidc.router, prefix="/api/v1")
    app.include_router(serving_artifacts.router, prefix="/api/v1")
    app.include_router(refit_control.router, prefix="/api/v1")
    app.include_router(evaluation_control.router, prefix="/api/v1")
    app.include_router(contracts.router, prefix="/api/v1")
    app.include_router(projects.router, prefix="/api/v1")
    app.include_router(datasets.router, prefix="/api/v1")
    app.include_router(profiling.router, prefix="/api/v1")
    app.include_router(training.router, prefix="/api/v1")
    app.include_router(validation.router, prefix="/api/v1")
    app.include_router(operations.router, prefix="/api/v1")
    app.include_router(monitoring.router, prefix="/api/v1")

    @app.middleware("http")
    async def correlate_requests(request: Request, call_next):
        header_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        bind_logging_context(request_id=header_id)
        try:
            response = await call_next(request)
        finally:
            bind_logging_context(request_id=None)
        response.headers["X-Request-ID"] = header_id
        return response

    return app


app = create_app()
