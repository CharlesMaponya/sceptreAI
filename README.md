# Sceptre

<p align="center">
  <img src="docs/architecture/images/sceptre-logo.png" alt="Sceptre logo" width="420">
</p>

<p align="center">
  Governed, Kubernetes-native AutoML for tabular data.
</p>

<p align="center">
  <a href="https://github.com/CharlesMaponya/sceptreAI/actions/workflows/ci.yml">
    <img src="https://github.com/CharlesMaponya/sceptreAI/actions/workflows/ci.yml/badge.svg" alt="CI status">
  </a>
  <a href="https://www.python.org/downloads/">
    <img src="https://img.shields.io/badge/Python-3.11%2B-3776AB.svg" alt="Python 3.11 or newer">
  </a>
  <a href="#quality-engineering">
    <img src="https://img.shields.io/badge/Coverage%20gate-%E2%89%A540%25-brightgreen.svg" alt="Coverage gate 40 percent">
  </a>
  <a href="https://kubernetes.io/">
    <img src="https://img.shields.io/badge/Orchestration-Kubernetes-326CE5.svg" alt="Kubernetes">
  </a>
</p>

## Overview

Sceptre is an AI operations platform for teams that need to turn tabular data
into governed, reviewable machine-learning outcomes without assembling a large
specialist platform team.

It combines dataset management, full-dataset profiling, resource-aware model
training, experiment tracking, validation, and explainability in one
project-isolated workspace. Compute-intensive work runs as disposable Kubernetes
Jobs, while PostgreSQL, MinIO, and MLflow provide durable operational state.

Sceptre is designed for controlled shared environments where auditability,
resource fairness, and reproducibility matter as much as model performance.

> **Maturity:** The platform foundation and Minikube deployment are implemented
> and continuously tested. Production adoption should include organization-specific
> identity integration, TLS, secret management, backup policy, monitoring, and
> multi-node capacity planning.

## Business Value

- **Reduce delivery time:** move from uploaded data to ranked candidate models in
  a single governed workflow.
- **Improve decision quality:** compare task-specific metrics, diagnostics, and
  external validation results rather than selecting models by one score.
- **Control infrastructure risk:** estimate CPU and memory before launch and
  enforce Kubernetes resource limits, concurrency rules, and runtime deadlines.
- **Preserve traceability:** retain immutable dataset versions, training metadata,
  model artifacts, MLflow runs, and analysis outputs.
- **Support responsible review:** provide feature-level profiling, preparation
  recommendations, SHAP importance, and clear failure remediation.

## Core Capabilities

| Area | Capabilities |
| --- | --- |
| Identity and workspaces | Registration, 24-hour access sessions, refresh-token rotation, project RBAC, and share links |
| Data management | CSV, Parquet, Excel, JSON, and JSONL ingestion; immutable versions; hashes; MinIO persistence |
| Profiling | Full-dataset statistics, five-number summaries, distributions, missingness, quality flags, temporal inference, and Dask fallback |
| Target management | Classification, regression, clustering, and time-series inference with target reprofiling and reusable feature statistics |
| Training | Up to 20 models per run, dynamic scikit-learn discovery, Bayesian tuning, adaptive resource requests, and isolated Kubernetes Jobs |
| Leaderboards | Progressive results, task-specific metrics, diagnostics, model ranking, and additional model runs without retraining completed candidates |
| Experiment tracking | MLflow parent and candidate runs backed by PostgreSQL, with candidate models mirrored to MinIO |
| Validation | External dataset validation with persisted metrics and diagnostic artifacts |
| Explainability | On-demand SHAP, cached historical explanations, legacy model reconstruction, and support for non-predictive clustering estimators |

## Supported Machine-Learning Tasks

| Task | Ranking and review metrics |
| --- | --- |
| Classification | Balanced accuracy, accuracy, precision, recall, F1, ROC-AUC, average precision, log loss, Brier score, MCC, Cohen's kappa, specificity, and Gini |
| Regression | RMSE, MAE, MAPE, median absolute error, explained variance, and R-squared |
| Time series | Chronological holdout plus regression metrics and time-aware error diagnostics |
| Clustering | Silhouette, Davies-Bouldin, and Calinski-Harabasz; optional ARI, NMI, AMI, Fowlkes-Mallows, and homogeneity |

The estimator catalog is discovered from scikit-learn using the task-appropriate
`ClassifierMixin`, `RegressorMixin`, or `ClusterMixin`. XGBoost, LightGBM, and
CatBoost candidates are included when their optional dependencies are installed.

## Platform Workflow

1. Create a project and assign access.
2. Upload a dataset; Sceptre creates an immutable version in MinIO.
3. Profile the complete dataset and select or revise the target.
4. Review inferred types, distributions, quality findings, and preparation steps.
5. Select up to 20 compatible models and estimate cluster resources.
6. Launch an isolated Kubernetes training job.
7. Review the progressive leaderboard and MLflow experiment.
8. Add individual models without rerunning completed candidates.
9. Run external validation and SHAP analysis for current or historical models.

## Architecture

```mermaid
flowchart LR
    User[Analyst or Data Scientist] --> UI[Streamlit]
    UI --> API[FastAPI]
    API --> PG[(PostgreSQL)]
    API --> MINIO[(MinIO)]
    API --> K8S[Kubernetes API]
    K8S --> JOBS[Training and Analysis Jobs]
    JOBS --> MINIO
    JOBS --> MLFLOW[MLflow]
    MLFLOW --> PG
    MLFLOW --> PVC[(Artifact PVC)]
```

| Component | Responsibility |
| --- | --- |
| Streamlit | Authenticated analytical workflows and progressive result rendering |
| FastAPI | Business rules, authorization, metadata APIs, and Kubernetes admission |
| PostgreSQL | Users, RBAC, projects, datasets, runs, metrics, and MLflow metadata |
| MinIO | Dataset versions, profiles, diagnostics, SHAP output, and durable model mirrors |
| MLflow | Experiment, candidate, metric, parameter, and model tracking |
| Kubernetes Jobs | Isolated training, validation, and explainability execution |

Project UUIDs are the tenant isolation boundary. Every dataset version, run,
metric, artifact, and registry record carries a `project_id`, and backend queries
enforce project access before returning data.

## Resource Governance

Sceptre is built for shared clusters:

- PostgreSQL advisory locks serialize admission decisions.
- The default global limit is two active compute Jobs.
- Each project may hold one active training slot.
- Live CPU and memory headroom determines whether a Job can launch.
- Per-Job requests are capped at 60% of available capacity on the best node.
- Low-priority Jobs can be preempted when the cluster needs resources.
- Stale database state is reconciled against Kubernetes before admission.
- Planned duration drives cost estimates; the safety deadline ranges from six to
  24 hours and is displayed separately.
- Completed Kubernetes Jobs are removed automatically.

Increasing `MAX_CONCURRENT_JOBS` permits more parallelism only when the cluster
has sufficient aggregate capacity. It does not overcommit a single node.

## Quick Start with Minikube

### Prerequisites

- Python 3.11 or newer
- Docker
- Minikube
- `kubectl`
- A local machine with sufficient CPU, memory, and disk for the selected datasets
  and model budget

### 1. Install the application

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev,ml,k8s]"
```

### 2. Build the runtime images

```bash
minikube addons enable metrics-server
minikube image build -t automl-mlflow:local -f Dockerfile.mlflow .
minikube image build -t automl-training:local -f Dockerfile.training .
```

### 3. Deploy platform infrastructure

```bash
kubectl apply -k infra/k8s/base
kubectl -n automl rollout status statefulset/automl-postgres
kubectl -n automl rollout status deployment/automl-mlflow
kubectl -n automl get pods,pvc
```

### 4. Forward platform services

Run each command in a separate terminal:

```bash
kubectl -n automl port-forward svc/automl-postgres 55432:5432
kubectl -n automl port-forward svc/automl-minio 9000:9000 9001:9001
kubectl -n automl port-forward svc/automl-mlflow 5000:5000
```

### 5. Start the API and UI

```bash
DATABASE_URL=postgresql+psycopg://automl:automl@127.0.0.1:55432/automl \
MLFLOW_TRACKING_URI=http://127.0.0.1:5000 \
uvicorn automl_api.main:app --app-dir apps/api --host 0.0.0.0 --port 8000
```

```bash
streamlit run apps/ui/streamlit_app/app.py \
  --server.address 0.0.0.0 \
  --server.port 8501
```

### Service URLs

| Service | URL |
| --- | --- |
| Sceptre UI | [http://localhost:8501](http://localhost:8501) |
| API documentation | [http://localhost:8000/docs](http://localhost:8000/docs) |
| MinIO console | [http://localhost:9001](http://localhost:9001) |
| MLflow | [http://localhost:5000](http://localhost:5000) |

## Configuration

Configuration is supplied through environment variables and Kubernetes Secrets.
The most important operational settings are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DATABASE_URL` | Local PostgreSQL forward | Application metadata connection |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | MLflow tracking endpoint |
| `MAX_CONCURRENT_JOBS` | `2` | Global compute Job limit |
| `MAX_NODE_AVAILABLE_FRACTION_PER_JOB` | `0.60` | Maximum node headroom allocated to one Job |
| `TRAINING_ACTIVE_DEADLINE_SECONDS` | `21600` | Minimum Job safety deadline |
| `TRAINING_MAX_ACTIVE_DEADLINE_SECONDS` | `86400` | Maximum Job safety deadline |
| `TRAINING_DEADLINE_MULTIPLIER` | `6` | Planned-duration safety multiplier |
| `JWT_ACCESS_TOKEN_MINUTES` | `1440` | Access-token lifetime |
| `OBJECT_STORE_ENDPOINT` | Environment-specific | MinIO or compatible object-store endpoint |

Use `.env.example` as the local configuration reference. Do not commit production
credentials or reuse the development secrets in `infra/k8s/base`.

## Quality Engineering

Pull requests and pushes to `main` or `develop` must pass all CI gates:

| Gate | Command | Purpose |
| --- | --- | --- |
| Ruff | `ruff check apps packages alembic tests` | Correctness, imports, modernization, and style |
| Tests and coverage | `pytest tests/ -v --tb=short --cov --cov-fail-under=40` | Behavioral, API, UI, training, and analysis verification |
| Syntax | `python -m compileall apps packages alembic tests` | Python 3.11 syntax and import compilation |

Current quality baseline:

- **63 automated tests**
- **43.59% branch coverage**
- **40% enforced coverage floor**
- XML and HTML coverage reports retained by CI for 14 days

The suite covers ingestion, temporal inference, exact and Dask profiling,
authentication, route contracts, Streamlit navigation, Kubernetes resource
estimation, adaptive deadlines, task metrics, estimator discovery, leaderboards,
external validation, MinIO model recovery, historical reconstruction, and
mixed-type SHAP preparation.

Run the complete local quality suite:

```bash
ruff check apps packages alembic tests
pytest tests/ -v --tb=short \
  --cov \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-fail-under=40
python -m compileall apps packages alembic tests
```

## Repository Structure

```text
apps/
  api/                 FastAPI service and training runtime
  ui/                  Streamlit multipage application
packages/              Shared Python packages
alembic/               Database migrations
infra/k8s/base/        Kubernetes and Minikube manifests
scripts/               Validation and operational utilities
tests/                 Automated test suite
docs/                  Architecture, schema, and decision records
```

## Operational Considerations

- The current scikit-learn tournaments are single-pod, in-memory workloads.
- Multi-gigabyte datasets may exceed a node's safety ceiling even when the raw
  file fits on disk.
- Horizontal model training requires additional Kubernetes nodes and a
  distributed backend such as Dask or Ray; an HPA cannot divide one in-memory
  scikit-learn fit across nodes.
- Historical models created before MinIO mirroring are reconstructed from the
  immutable source dataset and saved parameters before explainability runs.
- PostgreSQL, MinIO, and MLflow PVCs require environment-specific backup,
  retention, and disaster-recovery policies.
- Base manifests contain development defaults and must be hardened before
  internet-facing or regulated deployment.

## Documentation

- [Implementation plan](docs/architecture/implementation-plan.md)
- [Directory structure](docs/architecture/directory-structure.md)
- [Database schema](docs/architecture/database-schema.md)
- [Architecture decision 0001](docs/decisions/0001-decoupled-smme-automl.md)

## Contributing

Create a feature branch, keep changes scoped, add tests for behavioral changes,
and open a pull request against `develop` or `main`. CI must pass before merge.
