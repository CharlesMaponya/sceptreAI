from __future__ import annotations

from enum import Enum


class AuthProvider(str, Enum):
    SIMPLE = "simple"
    SSO = "sso"


class GlobalRole(str, Enum):
    MEMBER = "member"
    ADMIN = "admin"


class ProjectRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class ProjectStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class DatasetFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    EXCEL = "excel"
    JSON = "json"


class DatasetStatus(str, Enum):
    UPLOADED = "uploaded"
    PROFILING = "profiling"
    READY = "ready"
    FAILED = "failed"
    ARCHIVED = "archived"


class ObjectStoreType(str, Enum):
    MINIO = "minio"
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"


class TaskType(str, Enum):
    UNSPECIFIED = "unspecified"
    REGRESSION = "regression"
    CLASSIFICATION = "classification"
    TIME_SERIES = "time_series"
    CLUSTERING = "clustering"


class RunKind(str, Enum):
    TRAINING = "training"
    VALIDATION = "validation"
    EXPLAINABILITY = "explainability"
    DRIFT = "drift"
    DEPLOYMENT = "deployment"


class RunStatus(str, Enum):
    QUEUED = "queued"
    PRECHECK_RUNNING = "precheck_running"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PREEMPTED = "preempted"


class MetricSplit(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"
    EXTERNAL = "external"
    PRODUCTION = "production"


class MetricKind(str, Enum):
    PERFORMANCE = "performance"
    DATA_QUALITY = "data_quality"
    DRIFT = "drift"
    RESOURCE = "resource"
    DIAGNOSTIC = "diagnostic"


class ArtifactKind(str, Enum):
    DATASET_PROFILE = "dataset_profile"
    DIAGNOSTIC_PLOT = "diagnostic_plot"
    MODEL_OBJECT = "model_object"
    SHAP_VALUES = "shap_values"
    LOG_BUNDLE = "log_bundle"
    DEPLOYMENT_IMAGE = "deployment_image"


class ModelStage(str, Enum):
    CANDIDATE = "candidate"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"
    REJECTED = "rejected"
