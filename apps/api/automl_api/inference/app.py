from __future__ import annotations

import io
import os
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from automl_api.services.temporal import normalize_temporal_features
from automl_api.storage.object_store import get_object_store


class PredictionRequest(BaseModel):
    records: list[dict[str, Any]] = Field(min_length=1, max_length=10_000)
    include_probabilities: bool = False


class PredictionResponse(BaseModel):
    model_name: str
    predictions: list[Any]
    probabilities: list[list[float]] | None = None


class PointPredictionRequest(BaseModel):
    record: dict[str, Any]
    include_probabilities: bool = False


class PointPredictionResponse(BaseModel):
    model_name: str
    prediction: Any
    probabilities: list[float] | None = None


class ModelMetadata(BaseModel):
    project_name: str
    environment: str
    model_name: str


@lru_cache
def _load_model() -> Any:
    model_uri = os.getenv("MODEL_URI")
    if not model_uri:
        raise RuntimeError("MODEL_URI is required.")
    payload = get_object_store().read_bytes(model_uri)
    return joblib.load(io.BytesIO(payload))


def _json_values(values: Any) -> list[Any]:
    array = np.asarray(values)
    return array.tolist()


def predict_records(
    model: Any,
    records: list[dict[str, Any]],
    *,
    include_probabilities: bool,
) -> tuple[list[Any], list[list[float]] | None]:
    frame = normalize_temporal_features(pd.DataFrame.from_records(records))
    predictions = _json_values(model.predict(frame))
    probabilities = None
    if include_probabilities and hasattr(model, "predict_proba"):
        probabilities = _json_values(model.predict_proba(frame))
    return predictions, probabilities


def _uploaded_frames(upload: UploadFile, chunk_size: int) -> Any:
    suffix = Path(upload.filename or "").suffix.lower()
    upload.file.seek(0)
    if suffix == ".csv":
        yield from pd.read_csv(upload.file, chunksize=chunk_size)
        return
    if suffix in {".jsonl", ".ndjson"}:
        yield from pd.read_json(upload.file, lines=True, chunksize=chunk_size)
        return
    if suffix == ".json":
        yield pd.read_json(upload.file)
        return
    if suffix in {".parquet", ".pq"}:
        try:
            import pyarrow.parquet as parquet
        except ImportError as exc:
            raise ValueError("Parquet uploads require pyarrow.") from exc
        parquet_file = parquet.ParquetFile(upload.file)
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            yield batch.to_pandas()
        return
    raise ValueError(
        "Unsupported file type. Upload CSV, JSONL, JSON, or Parquet."
    )


def _prediction_output_frame(
    model: Any,
    frame: pd.DataFrame,
    *,
    include_probabilities: bool,
) -> pd.DataFrame:
    predictions, probabilities = predict_records(
        model,
        frame.to_dict(orient="records"),
        include_probabilities=include_probabilities,
    )
    output = frame.copy()
    output["prediction"] = predictions
    if probabilities is not None:
        probability_array = np.asarray(probabilities)
        for index in range(probability_array.shape[1]):
            output[f"probability_{index}"] = probability_array[:, index]
    return output


def create_offline_prediction_file(
    model: Any,
    upload: UploadFile,
    *,
    include_probabilities: bool,
    chunk_size: int = 10_000,
    max_rows: int = 5_000_000,
) -> tuple[Path, int]:
    output_handle = tempfile.NamedTemporaryFile(
        prefix="model-predictions-",
        suffix=".csv",
        delete=False,
    )
    output_path = Path(output_handle.name)
    output_handle.close()
    row_count = 0
    wrote_header = False
    try:
        for frame in _uploaded_frames(upload, chunk_size):
            if frame.empty:
                continue
            row_count += len(frame)
            if row_count > max_rows:
                raise ValueError(
                    f"Upload exceeds the {max_rows:,}-row offline prediction limit."
                )
            output = _prediction_output_frame(
                model,
                frame,
                include_probabilities=include_probabilities,
            )
            output.to_csv(
                output_path,
                mode="a",
                header=not wrote_header,
                index=False,
            )
            wrote_header = True
        if not wrote_header:
            raise ValueError("The uploaded dataset contains no rows.")
    except Exception:
        output_path.unlink(missing_ok=True)
        raise
    return output_path, row_count


def create_app() -> FastAPI:
    model_name = os.getenv("MODEL_NAME", "model")
    project_name = os.getenv("PROJECT_NAME", "Model project")
    environment = os.getenv("DEPLOYMENT_ENVIRONMENT", "local")
    app = FastAPI(
        title=f"{project_name} model API ({environment})",
        description=(
            f"Online prediction API for {model_name}. "
            f"Project: {project_name}. Environment: {environment}."
        ),
        version="1.0.0",
    )

    @app.get("/", include_in_schema=False)
    def index() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        try:
            _load_model()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Model is unavailable: {exc}",
            ) from exc
        return {"status": "ok"}

    @app.get("/v1/metadata", response_model=ModelMetadata)
    def metadata() -> ModelMetadata:
        return ModelMetadata(
            project_name=project_name,
            environment=environment,
            model_name=model_name,
        )

    @app.post(
        "/v1/predict/online",
        response_model=PointPredictionResponse,
        tags=["online prediction"],
        summary="Predict one record",
    )
    def predict_online(
        payload: PointPredictionRequest,
    ) -> PointPredictionResponse:
        try:
            predictions, probabilities = predict_records(
                _load_model(),
                [payload.record],
                include_probabilities=payload.include_probabilities,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Prediction failed: {exc}",
            ) from exc
        return PointPredictionResponse(
            model_name=model_name,
            prediction=predictions[0],
            probabilities=probabilities[0] if probabilities else None,
        )

    @app.post(
        "/v1/predict",
        response_model=PredictionResponse,
        tags=["online prediction"],
        summary="Predict a JSON record batch",
    )
    def predict(payload: PredictionRequest) -> PredictionResponse:
        try:
            predictions, probabilities = predict_records(
                _load_model(),
                payload.records,
                include_probabilities=payload.include_probabilities,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Prediction failed: {exc}",
            ) from exc
        return PredictionResponse(
            model_name=model_name,
            predictions=predictions,
            probabilities=probabilities,
        )

    @app.post(
        "/v1/predict/offline",
        response_class=FileResponse,
        tags=["offline prediction"],
        summary="Upload a dataset and download predictions",
        description=(
            "Accepts CSV, JSONL, JSON, or Parquet. The downloaded CSV contains "
            "the input columns plus prediction and optional probability columns."
        ),
    )
    def predict_offline(
        file: Annotated[
            UploadFile,
            File(description="Tabular dataset to score"),
        ],
        include_probabilities: Annotated[bool, Form()] = False,
    ) -> FileResponse:
        try:
            output_path, row_count = create_offline_prediction_file(
                _load_model(),
                file,
                include_probabilities=include_probabilities,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Offline prediction failed: {exc}",
            ) from exc
        output_name = (
            f"{Path(file.filename or 'dataset').stem}-predictions.csv"
        )
        return FileResponse(
            path=output_path,
            media_type="text/csv",
            filename=output_name,
            headers={"X-Prediction-Row-Count": str(row_count)},
            background=BackgroundTask(output_path.unlink, missing_ok=True),
        )

    return app


app = create_app()
