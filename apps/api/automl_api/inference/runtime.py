"""Serve prediction traffic only over mTLS; keep unauthenticated probes separate."""

import asyncio
import logging
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime

import joblib
import uvicorn
from fastapi import FastAPI, HTTPException

from automl_api.inference.app import create_app
from automl_api.inference.credentials import ServingCredentials
from automl_api.services.inference_tls import DATA_PORT, HEALTH_PORT


def health_app(credentials):
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready():
        if not credentials.active:
            raise HTTPException(503, "Serving unavailable.")
        return {"status": "ok"}

    return app


async def renew_identity(credentials):
    while True:
        await asyncio.sleep(5)
        if (credentials.expires - datetime.now(UTC)).total_seconds() <= 120:
            try:
                await asyncio.to_thread(credentials.renew)
            except RuntimeError:
                logging.getLogger(__name__).warning(
                    "Serving identity renewal unavailable; prediction readiness withdrawn"
                )


async def serve():
    credentials = ServingCredentials()
    await asyncio.to_thread(credentials.renew)

    def load():
        with credentials.download_model() as source:
            return joblib.load(source)

    model = await asyncio.to_thread(load)
    config = uvicorn.Config(
        create_app(credentials=credentials, model=model),
        host="0.0.0.0",
        port=DATA_PORT,
        access_log=False,
    )
    config.load()
    config.ssl = credentials.context
    if config.ssl is None or not credentials.active:
        raise RuntimeError("Serving startup requires active mutual TLS")
    data = uvicorn.Server(config)
    health = uvicorn.Server(
        uvicorn.Config(
            health_app(credentials),
            host="0.0.0.0",
            port=HEALTH_PORT,
            access_log=False,
        )
    )
    health_thread = threading.Thread(target=health.run, name="inference-health", daemon=True)
    health_thread.start()
    renewal = asyncio.create_task(renew_identity(credentials))
    try:
        deadline = time.monotonic() + 5
        while not health.started:
            if not health_thread.is_alive() or time.monotonic() >= deadline:
                raise RuntimeError("Serving health listener failed to start")
            await asyncio.sleep(0.01)
        await data.serve()
    finally:
        data.should_exit = health.should_exit = True
        renewal.cancel()
        with suppress(asyncio.CancelledError):
            await renewal
        await asyncio.to_thread(health_thread.join, 5)
        if health_thread.is_alive():
            raise RuntimeError("Serving health listener did not retire")


if __name__ == "__main__":
    asyncio.run(serve())
