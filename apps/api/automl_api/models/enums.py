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
    EMBEDDED = "embedded"
    S3_COMPATIBLE = "s3_compatible"
    AWS_S3 = "aws_s3"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    # Legacy persisted identities remain readable until the Phase 2 backfill is complete.
    MINIO = "minio"
    S3 = "s3"
    AZURE = "azure"


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
    DRIFT_REPORT = "drift_report"
    LOG_BUNDLE = "log_bundle"
    DEPLOYMENT_IMAGE = "deployment_image"
    GOVERNANCE_REPORT = "governance_report"


class ModelStage(StrEnum):
    CANDIDATE = "candidate"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"
    REJECTED = "rejected"


class CommandStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DEAD = "dead"


class WorkflowStage(StrEnum):
    SPLITTER = "splitter"
    PREPARATION = "preparation"
    TRAINING_RUN = "training_run"
    TRAINING_TRIAL = "training_trial"
    CHAMPION_REFIT = "champion_refit"
    CHAMPION_EVALUATION = "champion_evaluation"
    PROVIDER_CONFORMANCE = "provider_conformance"


class AttemptStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"


class ScopeStatus(StrEnum):
    OPEN = "open"
    SEALED = "sealed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FinalTestStatus(StrEnum):
    ALLOCATED = "allocated"
    OPENED = "opened"
    COMMITTED = "committed"
    FAILED = "failed"
