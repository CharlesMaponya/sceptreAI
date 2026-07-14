# Implementation Plan

## Phase 1: Foundation

- Project structure
- SQLAlchemy metadata schema
- Alembic wiring
- Minimal API/UI entry points
- Base Kubernetes guardrail manifests

## Phase 2: IAM and Project Workspaces

- Registration, login, password reset: implemented
- JWT-style access tokens and refresh-token rotation: implemented
- Simple-auth/SSO toggle foundation: implemented
- Project RBAC and share links: implemented
- React session handling and workspace shell: implemented

## Phase 3: Dataset Ingestion and Storage

- CSV, Parquet, Excel, JSON, and JSONL upload contracts: implemented
- Dataset versions and content hashing: implemented
- Remote MinIO object storage with legacy local-read fallback: implemented
- CSV and JSON metadata extraction: implemented
- Parquet and Excel storage/versioning with deferred rich parsing: implemented
- Embedded MinIO manifests: implemented
- Ephemeral per-Job dataset cache with optional shared PVC: implemented
- Default or explicitly configured StorageClass through Helm: implemented

## Phase 4: Profiling and Preparation

- Target selection and task inference: implemented
- Type inference for categorical, text, numeric, and temporal columns: implemented
- Completeness, missingness, outlier flags, descriptive statistics: implemented
- Full-dataset profiling with automatic memory-aware Dask partitioning: implemented
- Durable staged profiling jobs with restart resume and cancellation: implemented
- Progressive feature batches, polling, SSE progress, and lazy feature retrieval: implemented
- MinIO-cached feature, relationship, preparation, and complete profile artifacts: implemented
- Automatic background profiling after dataset upload: implemented
- Target replacement with feature reuse and task-dependent reprofiling: implemented
- Pearson and Cramer relationship summaries against the selected target: implemented     
- Type-aware preprocessing and feature engineering plans: implemented
- Information Value and pairplot artifact generation: pending

## Phase 5: Training Orchestration

- Namespace-scoped Kubernetes quota, optional capability, and runtime dependency checks: implemented
- Helm-configured CPU and memory requests/limits with scheduler-owned admission: implemented
- Portable CPU-first scheduling without node pinning: implemented
- Cluster-wide concurrency guardrails and transaction-locked admission: implemented
- One-active-run-per-project fairness across concurrent users: implemented
- Optional PriorityClass and standard CPU/GPU resources with workload deadlines: implemented
- ZenML pipeline assembly for classification, regression, time series, and clustering: implemented
- Direct Kubernetes worker execution without a nested ephemeral ZenML local stack: implemented
- Resource-bounded Bayesian model tournaments and task-specific leaderboards: implemented
- Dynamic sklearn `all_estimators()` discovery using task-specific mixins: implemented
- UI estimator catalog, cost tiers, and per-run model selection: implemented
- Full classification, regression, time-series, and clustering review metrics: implemented
- Optional external clustering validation with fold means and deviations: implemented
- Chronological holdout for time series and sampled silhouette ranking for clustering: implemented
- Candidate failure isolation, winner persistence, and leaderboard API/UI: implemented
- MLflow parent/candidate runs, project tags, leaderboard artifact, and winning model: implemented
- In-cluster PostgreSQL for application and MLflow metadata with persistent storage: implemented
- MLflow candidate models mirrored to MinIO for durable validation and recovery: implemented
- HTTP and authenticated WebSocket log streaming: implemented
- Optional Kubernetes Metrics API workload telemetry with graceful fallback: implemented
- Kubernetes/metadata reconciliation before multi-user admission checks: implemented
- Workload-aware 6-to-24-hour Job deadlines with explicit expiry diagnostics: implemented

## Phase 6: Validation and Explainability

- Holdout and K-fold diagnostics during model tournaments: implemented
- External validation against compatible dataset versions: implemented
- Task-specific metrics and stored MinIO diagnostic artifacts: implemented
- On-demand isolated SHAP jobs with mixed-type feature support: implemented
- Progressive React validation and explainability results: implemented
- Failure parsing and user-facing remediation messages: implemented

## Phase 7: Monitoring, Registry, and Deployment

Status: baseline implemented.

- Evidently drift checks: implemented as bounded Kubernetes analysis Jobs with
  stable summary metrics and durable report artifacts.
- Model registry promotion and fallback selection: implemented with
  project-scoped candidate, staging, production, rejected, and archived stages.
- One-click Kubernetes deployment: implemented for staging and production
  registry entries using a generic inference runtime, generated Dockerfile
  artifact, Deployment, and ClusterIP Service.
- Cluster usage and dependency health dashboards: implemented in the Operations
  workspace using Kubernetes capacity and runtime dependency checks.
- Artifact cleanup and resource reclaim workflows: implemented with
  administrator-only preview and execution modes, active-deployment protection,
  object deletion, and completed Job cleanup.
