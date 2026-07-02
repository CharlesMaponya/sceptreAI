from __future__ import annotations

from enum import StrEnum


class AuthProvider(StrEnum):
    SIMPLE = "simple"
    SSO = "sso"


class GlobalRole(StrEnum):
    MEMBER = "member"
    ADMIN = "admin"


class ProjectRole(StrEnum):
    OWNER = "owner"
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class ProjectStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class DatasetFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"
    EXCEL = "excel"
    JSON = "json"


class DatasetStatus(StrEnum):
    UPLOADED = "uploaded"
    PROFILING = "profiling"
    READY = "ready"
    FAILED = "failed"
    ARCHIVED = "archived"


class ObjectStoreType(StrEnum):
    MINIO = "minio"
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"


class TaskType(StrEnum):
    UNSPECIFIED = "unspecified"
    REGRESSION = "regression"
    CLASSIFICATION = "classification"
    TIME_SERIES = "time_series"
    CLUSTERING = "clustering"


class RunKind(StrEnum):
    TRAINING = "training"
    VALIDATION = "validation"
    EXPLAINABILITY = "explainability"
    DRIFT = "drift"
    DEPLOYMENT = "deployment"


class RunStatus(StrEnum):
    QUEUED = "queued"
    PRECHECK_RUNNING = "precheck_running"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"


class MetricSplit(StrEnum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"
    EXTERNAL = "external"
    PRODUCTION = "production"


class MetricKind(StrEnum):
    PERFORMANCE = "performance"
    DATA_QUALITY = "data_quality"
    DRIFT = "drift"
    RESOURCE = "resource"
    DIAGNOSTIC = "diagnostic"


class ArtifactKind(StrEnum):
    DATASET_PROFILE = "dataset_profile"
    DIAGNOSTIC_PLOT = "diagnostic_plot"
    MODEL_OBJECT = "model_object"
    SHAP_VALUES = "shap_values"
    LOG_BUNDLE = "log_bundle"
    DEPLOYMENT_IMAGE = "deployment_image"


class ModelStage(StrEnum):
    CANDIDATE = "candidate"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"
    REJECTED = "rejected"
