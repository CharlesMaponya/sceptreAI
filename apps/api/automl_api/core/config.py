from __future__ import annotations

import importlib.util
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from automl_api import __version__


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


def _get_csv(name: str, default: tuple[str, ...], dotenv: dict[str, str]) -> tuple[str, ...]:
    raw_value = _get_env(name, ",".join(default), dotenv)
    return tuple(item.strip() for item in str(raw_value or "").split(",") if item.strip())


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
    public_app_url: str = "http://localhost:8080"
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from_email: str | None = None
    smtp_starttls: bool = True
    smtp_use_ssl: bool = False

    object_store_type: str = "minio"
    object_store_endpoint: str | None = None
    object_store_bucket: str = "automl"
    object_store_access_key: str | None = None
    object_store_secret_key: str | None = None
    local_object_store_path: Path = Path(".automl_object_store")

    dataset_cache_size_gb: int = 5
    dataset_cache_pvc_name: str | None = None
    gpu_enabled: bool = False
    cluster_observer_enabled: bool = False
    nvidia_gpu_resource: str = "nvidia.com/gpu"
    intel_gpu_resource: str = "gpu.intel.com/xe"
    max_concurrent_jobs: int = 2
    mlflow_tracking_uri: str = "http://mlflow:5000"
    training_namespace: str = "automl"
    training_image: str = f"docker.io/maponyacharles/sceptreai:training-cpu-{__version__}"
    training_image_nvidia: str = f"docker.io/maponyacharles/sceptreai:training-nvidia-{__version__}"
    training_image_intel: str = f"docker.io/maponyacharles/sceptreai:training-intel-{__version__}"
    training_image_pull_policy: str = "IfNotPresent"
    workload_image_pull_secrets: tuple[str, ...] = ()
    training_service_account: str = "default"
    training_cpu_request_cores: float = 1.0
    training_cpu_limit_cores: float = 2.0
    training_memory_request_mb: int = 1024
    training_memory_limit_mb: int = 4096
    training_priority_class_name: str | None = None
    training_job_ttl_seconds: int = 300
    database_secret_name: str = "automl-platform-secrets"
    database_secret_key: str = "DATABASE_URL"
    object_store_secret_name: str = "automl-seaweedfs-credentials"
    object_store_access_key_secret_key: str = "AWS_ACCESS_KEY_ID"
    object_store_secret_key_secret_key: str = "AWS_SECRET_ACCESS_KEY"
    inference_image: str = f"docker.io/maponyacharles/sceptreai:inference-{__version__}"
    inference_image_pull_policy: str = "IfNotPresent"
    inference_service_account: str = "default"
    inference_service_type: str = "ClusterIP"
    inference_external_host: str | None = None
    inference_external_scheme: str = "http"
    inference_ingress_enabled: bool = False
    inference_ingress_class_name: str | None = None
    inference_ingress_host_template: str | None = None
    inference_ingress_tls_secret_name: str | None = None
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
        public_app_url=str(_get_env("PUBLIC_APP_URL", Settings.public_app_url, dotenv)),
        smtp_host=_get_env("SMTP_HOST", Settings.smtp_host, dotenv),
        smtp_port=_get_int("SMTP_PORT", Settings.smtp_port, dotenv),
        smtp_username=_get_env("SMTP_USERNAME", Settings.smtp_username, dotenv),
        smtp_password=_get_env("SMTP_PASSWORD", Settings.smtp_password, dotenv),
        smtp_from_email=_get_env("SMTP_FROM_EMAIL", Settings.smtp_from_email, dotenv),
        smtp_starttls=_get_bool("SMTP_STARTTLS", Settings.smtp_starttls, dotenv),
        smtp_use_ssl=_get_bool("SMTP_USE_SSL", Settings.smtp_use_ssl, dotenv),
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
        dataset_cache_pvc_name=_get_env(
            "DATASET_CACHE_PVC_NAME",
            Settings.dataset_cache_pvc_name,
            dotenv,
        ),
        gpu_enabled=_get_bool("GPU_ENABLED", Settings.gpu_enabled, dotenv),
        cluster_observer_enabled=_get_bool(
            "CLUSTER_OBSERVER_ENABLED",
            Settings.cluster_observer_enabled,
            dotenv,
        ),
        nvidia_gpu_resource=str(
            _get_env("NVIDIA_GPU_RESOURCE", Settings.nvidia_gpu_resource, dotenv)
        ),
        intel_gpu_resource=str(
            _get_env("INTEL_GPU_RESOURCE", Settings.intel_gpu_resource, dotenv)
        ),
        max_concurrent_jobs=_get_int("MAX_CONCURRENT_JOBS", Settings.max_concurrent_jobs, dotenv),
        mlflow_tracking_uri=str(
            _get_env("MLFLOW_TRACKING_URI", Settings.mlflow_tracking_uri, dotenv)
        ),
        training_namespace=str(_get_env("TRAINING_NAMESPACE", Settings.training_namespace, dotenv)),
        training_image=str(_get_env("TRAINING_IMAGE", Settings.training_image, dotenv)),
        training_image_nvidia=str(
            _get_env("TRAINING_IMAGE_NVIDIA", Settings.training_image_nvidia, dotenv)
        ),
        training_image_intel=str(
            _get_env("TRAINING_IMAGE_INTEL", Settings.training_image_intel, dotenv)
        ),
        training_image_pull_policy=str(
            _get_env(
                "TRAINING_IMAGE_PULL_POLICY",
                Settings.training_image_pull_policy,
                dotenv,
            )
        ),
        workload_image_pull_secrets=_get_csv(
            "WORKLOAD_IMAGE_PULL_SECRETS",
            Settings.workload_image_pull_secrets,
            dotenv,
        ),
        training_service_account=str(
            _get_env(
                "TRAINING_SERVICE_ACCOUNT",
                Settings.training_service_account,
                dotenv,
            )
        ),
        training_cpu_request_cores=_get_float(
            "TRAINING_CPU_REQUEST_CORES",
            Settings.training_cpu_request_cores,
            dotenv,
        ),
        training_cpu_limit_cores=_get_float(
            "TRAINING_CPU_LIMIT_CORES",
            Settings.training_cpu_limit_cores,
            dotenv,
        ),
        training_memory_request_mb=_get_int(
            "TRAINING_MEMORY_REQUEST_MB",
            Settings.training_memory_request_mb,
            dotenv,
        ),
        training_memory_limit_mb=_get_int(
            "TRAINING_MEMORY_LIMIT_MB",
            Settings.training_memory_limit_mb,
            dotenv,
        ),
        training_priority_class_name=_get_env(
            "TRAINING_PRIORITY_CLASS_NAME",
            Settings.training_priority_class_name,
            dotenv,
        ),
        training_job_ttl_seconds=_get_int(
            "TRAINING_JOB_TTL_SECONDS",
            Settings.training_job_ttl_seconds,
            dotenv,
        ),
        database_secret_name=str(
            _get_env("DATABASE_SECRET_NAME", Settings.database_secret_name, dotenv)
        ),
        database_secret_key=str(
            _get_env("DATABASE_SECRET_KEY", Settings.database_secret_key, dotenv)
        ),
        object_store_secret_name=str(
            _get_env("OBJECT_STORE_SECRET_NAME", Settings.object_store_secret_name, dotenv)
        ),
        object_store_access_key_secret_key=str(
            _get_env(
                "OBJECT_STORE_ACCESS_KEY_SECRET_KEY",
                Settings.object_store_access_key_secret_key,
                dotenv,
            )
        ),
        object_store_secret_key_secret_key=str(
            _get_env(
                "OBJECT_STORE_SECRET_KEY_SECRET_KEY",
                Settings.object_store_secret_key_secret_key,
                dotenv,
            )
        ),
        inference_image=str(
            _get_env("INFERENCE_IMAGE", Settings.inference_image, dotenv)
        ),
        inference_image_pull_policy=str(
            _get_env(
                "INFERENCE_IMAGE_PULL_POLICY",
                Settings.inference_image_pull_policy,
                dotenv,
            )
        ),
        inference_service_account=str(
            _get_env(
                "INFERENCE_SERVICE_ACCOUNT",
                Settings.inference_service_account,
                dotenv,
            )
        ),
        inference_service_type=str(
            _get_env(
                "INFERENCE_SERVICE_TYPE",
                Settings.inference_service_type,
                dotenv,
            )
        ),
        inference_external_host=_get_env(
            "INFERENCE_EXTERNAL_HOST",
            Settings.inference_external_host,
            dotenv,
        ),
        inference_external_scheme=str(
            _get_env(
                "INFERENCE_EXTERNAL_SCHEME",
                Settings.inference_external_scheme,
                dotenv,
            )
        ),
        inference_ingress_enabled=_get_bool(
            "INFERENCE_INGRESS_ENABLED",
            Settings.inference_ingress_enabled,
            dotenv,
        ),
        inference_ingress_class_name=_get_env(
            "INFERENCE_INGRESS_CLASS_NAME",
            Settings.inference_ingress_class_name,
            dotenv,
        ),
        inference_ingress_host_template=_get_env(
            "INFERENCE_INGRESS_HOST_TEMPLATE",
            Settings.inference_ingress_host_template,
            dotenv,
        ),
        inference_ingress_tls_secret_name=_get_env(
            "INFERENCE_INGRESS_TLS_SECRET_NAME",
            Settings.inference_ingress_tls_secret_name,
            dotenv,
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
