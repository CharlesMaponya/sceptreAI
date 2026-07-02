from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _read_dotenv(path: Path = Path(".env")) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _get_env(name: str, default: str | None, dotenv: dict[str, str]) -> str | None:
    return os.getenv(name, dotenv.get(name, default))


def _get_bool(name: str, default: bool, dotenv: dict[str, str]) -> bool:
    raw_value = _get_env(name, str(default), dotenv)
    return str(raw_value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int, dotenv: dict[str, str]) -> int:
    raw_value = _get_env(name, str(default), dotenv)
    return int(str(raw_value))


def _get_float(name: str, default: float, dotenv: dict[str, str]) -> float:
    raw_value = _get_env(name, str(default), dotenv)
    return float(str(raw_value))


def resolve_database_url(database_url: str) -> str:
    if (
        database_url.startswith("postgresql+psycopg://")
        and importlib.util.find_spec("psycopg") is None
        and importlib.util.find_spec("psycopg2") is not None
    ):
        return database_url.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
    return database_url


@dataclass(frozen=True)
class Settings:
    environment: str = "local"
    database_url: str = "postgresql+psycopg://automl:automl@localhost:55432/automl"

    jwt_secret_key: str = "change-me"
    jwt_access_token_minutes: int = 24 * 60
    jwt_refresh_rotation_hours: int = 7 * 24
    simple_auth_enabled: bool = True

    object_store_type: str = "minio"
    object_store_endpoint: str | None = None
    object_store_bucket: str = "automl"
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    local_object_store_path: Path = Path(".automl_object_store")

    dataset_cache_size_gb: int = 5
    gpu_enabled: bool = False
    max_cluster_cpu_percent: int = 70
    max_node_available_fraction_per_job: float = 0.60
    max_concurrent_jobs: int = 2
    mlflow_tracking_uri: str = "http://mlflow:5000"
    training_namespace: str = "automl"
    training_image: str = "automl-training:local"
    training_service_account: str = "default"
    training_active_deadline_seconds: int = 6 * 60 * 60
    training_max_active_deadline_seconds: int = 24 * 60 * 60
    training_deadline_multiplier: int = 6

    @property
    def sqlalchemy_database_url(self) -> str:
        return resolve_database_url(self.database_url)


@lru_cache
def get_settings() -> Settings:
    dotenv = _read_dotenv()
    return Settings(
        environment=str(_get_env("ENVIRONMENT", Settings.environment, dotenv)),
        database_url=str(_get_env("DATABASE_URL", Settings.database_url, dotenv)),
        jwt_secret_key=str(_get_env("JWT_SECRET_KEY", Settings.jwt_secret_key, dotenv)),
        jwt_access_token_minutes=_get_int(
            "JWT_ACCESS_TOKEN_MINUTES",
            Settings.jwt_access_token_minutes,
            dotenv,
        ),
        jwt_refresh_rotation_hours=_get_int(
            "JWT_REFRESH_ROTATION_HOURS",
            Settings.jwt_refresh_rotation_hours,
            dotenv,
        ),
        simple_auth_enabled=_get_bool("SIMPLE_AUTH_ENABLED", Settings.simple_auth_enabled, dotenv),
        object_store_type=str(_get_env("OBJECT_STORE_TYPE", Settings.object_store_type, dotenv)),
        object_store_endpoint=_get_env(
            "OBJECT_STORE_ENDPOINT",
            Settings.object_store_endpoint,
            dotenv,
        ),
        object_store_bucket=str(
            _get_env("OBJECT_STORE_BUCKET", Settings.object_store_bucket, dotenv)
        ),
        object_store_access_key=_get_env(
            "OBJECT_STORE_ACCESS_KEY",
            Settings.object_store_access_key,
            dotenv,
        ),
        object_store_secret_key=_get_env(
            "OBJECT_STORE_SECRET_KEY",
            Settings.object_store_secret_key,
            dotenv,
        ),
        local_object_store_path=Path(
            str(_get_env("LOCAL_OBJECT_STORE_PATH", str(Settings.local_object_store_path), dotenv))
        ),
        dataset_cache_size_gb=_get_int(
            "DATASET_CACHE_SIZE_GB",
            Settings.dataset_cache_size_gb,
            dotenv,
        ),
        gpu_enabled=_get_bool("GPU_ENABLED", Settings.gpu_enabled, dotenv),
        max_cluster_cpu_percent=_get_int(
            "MAX_CLUSTER_CPU_PERCENT",
            Settings.max_cluster_cpu_percent,
            dotenv,
        ),
        max_node_available_fraction_per_job=_get_float(
            "MAX_NODE_AVAILABLE_FRACTION_PER_JOB",
            Settings.max_node_available_fraction_per_job,
            dotenv,
        ),
        max_concurrent_jobs=_get_int("MAX_CONCURRENT_JOBS", Settings.max_concurrent_jobs, dotenv),
        mlflow_tracking_uri=str(
            _get_env("MLFLOW_TRACKING_URI", Settings.mlflow_tracking_uri, dotenv)
        ),
        training_namespace=str(_get_env("TRAINING_NAMESPACE", Settings.training_namespace, dotenv)),
        training_image=str(_get_env("TRAINING_IMAGE", Settings.training_image, dotenv)),
        training_service_account=str(
            _get_env(
                "TRAINING_SERVICE_ACCOUNT",
                Settings.training_service_account,
                dotenv,
            )
        ),
        training_active_deadline_seconds=_get_int(
            "TRAINING_ACTIVE_DEADLINE_SECONDS",
            Settings.training_active_deadline_seconds,
            dotenv,
        ),
        training_max_active_deadline_seconds=_get_int(
            "TRAINING_MAX_ACTIVE_DEADLINE_SECONDS",
            Settings.training_max_active_deadline_seconds,
            dotenv,
        ),
        training_deadline_multiplier=_get_int(
            "TRAINING_DEADLINE_MULTIPLIER",
            Settings.training_deadline_multiplier,
            dotenv,
        ),
    )
