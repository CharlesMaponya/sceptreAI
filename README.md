# Sceptre

<p align="center">
  <img src="docs/architecture/images/sceptre-logo.png" alt="Sceptre logo" width="420">
</p>

<p align="center">
  <strong>From tabular data to a governed model endpoint—without building an MLOps department.</strong>
</p>

<p align="center">
  Train, compare, validate, explain, register, and deploy models from one
  Kubernetes-native workspace.
</p>

<p align="center">
  <a href="#quick-start-with-minikube"><strong>Run Sceptre locally</strong></a>
  ·
  <a href="#platform-workflow">See the workflow</a>
  ·
  <a href="docs/production-readiness/README.md">Plan for production</a>
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

Most AutoML products finish at a leaderboard. That is where the difficult part
usually begins: proving the model on new data, explaining its decisions,
controlling compute, preserving lineage, promoting the right version, and
serving it safely.

Sceptre closes that gap. It gives small and growing teams one governed path from
an uploaded table to a reviewable, deployable model. Dataset management,
full-dataset profiling, resource-aware training, experiment tracking, external
validation, SHAP explainability, model promotion, drift analysis, and Kubernetes
serving all live in one project-isolated workspace.

The result is less platform assembly, fewer hand-offs, and a much clearer answer
to the question every serious ML project eventually faces: **why should we trust
this model, and can we operate it?**

Sceptre runs on infrastructure you control. Compute-heavy work is isolated in
disposable Kubernetes Jobs, while PostgreSQL, MinIO, and MLflow retain the
operational record. It is designed for shared environments where auditability,
resource fairness, and reproducibility matter as much as raw model performance.

> **Current maturity:** The working platform and Minikube deployment are
> implemented and tested. Before an internet-facing or regulated production
> rollout, add organization-specific identity, TLS, managed secrets, backups,
> monitoring, and multi-node capacity planning.

The phased target architecture, provider-neutral packaging contract, and
production acceptance gates are documented in the
[Production Readiness Implementation Guide](docs/production-readiness/README.md).

## Why Teams Choose Sceptre

| What teams usually piece together | What Sceptre provides |
| --- | --- |
| Upload scripts, notebooks, and shared folders | Project-scoped datasets, immutable versions, hashes, access roles, and durable object storage |
| Manual profiling and preparation guesses | Full-dataset statistics, quality flags, temporal inference, relationships, and preparation recommendations |
| One opaque “best model” score | Progressive leaderboards with task-specific metrics, diagnostics, parameters, and experiment history |
| Cluster requests based on intuition | Preflight CPU, memory, and duration estimates with admission limits and adaptive deadlines |
| Validation and explainability as follow-up work | External validation and on-demand SHAP for current or historical candidates |
| Model files passed between people | A project registry with staged promotion, explicit fallback, drift checks, and artifact protection |
| A bespoke serving service for every model | Generated model packaging and Kubernetes deployments with online and offline prediction APIs |

### The business case

- **Ship sooner:** move from raw data to ranked, validated candidates in one
  workflow instead of integrating separate tools first.
- **Make defensible model choices:** evaluate more than a headline score with
  diagnostics, holdout results, external validation, and feature contributions.
- **Protect shared infrastructure:** estimate demand before launch, cap each Job,
  limit concurrency, and let higher-priority business workloads win.
- **Keep evidence attached:** preserve dataset versions, parameters, metrics,
  artifacts, model lineage, and operational status by project.
- **Turn experiments into an operating process:** promote, deploy, monitor, stop,
  and clean up models through explicit governed actions.

### Built for

- Small ML and data teams that need production discipline without a dedicated
  platform group.
- Organizations running Kubernetes that want model workloads to coexist fairly
  with business services.
- Consultancies and internal analytics teams that need isolated, reviewable
  project workspaces.
- Regulated or approval-driven environments that value traceability and human
  review over one-click automation.

## Product Capabilities

| Stage | What Sceptre delivers |
| --- | --- |
| Secure the workspace | Registration, 24-hour access sessions, refresh-token rotation, project RBAC, and share links |
| Bring the data | CSV, Parquet, Excel, JSON, and JSONL ingestion; immutable versions; content hashes; MinIO persistence |
| Understand it | Full-dataset statistics, five-number summaries, distributions, missingness, quality flags, temporal inference, relationships, and Dask fallback |
| Frame the problem | Classification, regression, clustering, and time-series inference with target reprofiling and reusable feature statistics |
| Train efficiently | Up to 20 models per run, dynamic scikit-learn discovery, Bayesian tuning, adaptive resource requests, and isolated Kubernetes Jobs |
| Choose with evidence | Progressive results, task-specific metrics, diagnostics, ranking, and additional candidates without retraining completed models |
| Reproduce the work | MLflow parent and candidate runs backed by PostgreSQL, with candidate models mirrored to MinIO |
| Challenge the model | External dataset validation with persisted metrics and diagnostic artifacts |
| Explain the outcome | On-demand SHAP, cached historical explanations, legacy model reconstruction, and support for non-predictive clustering estimators |
| Operate the winner | Project registry, staged promotion, explicit fallback, Evidently drift Jobs, generated model Dockerfiles, Kubernetes inference deployments, health reporting, and guarded cleanup |

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
10. Register approved candidates, select a fallback, run drift checks, and
    deploy or stop models from the Operations workspace.

## Architecture

```mermaid
flowchart LR
    User[Analyst or Data Scientist] --> UI[React + TypeScript]
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
| React + TypeScript | Authenticated, responsive workflows and progressive result rendering |
| FastAPI | Business rules, authorization, metadata APIs, and Kubernetes admission |
| PostgreSQL | Users, RBAC, projects, datasets, runs, metrics, and MLflow metadata |
| MinIO | Dataset versions, profiles, diagnostics, SHAP output, and durable model mirrors |
| MLflow | Experiment, candidate, metric, parameter, and model tracking |
| Kubernetes Jobs | Isolated training, validation, and explainability execution |
| Inference runtime | Generic FastAPI prediction service deployed from registered model artifacts |

Project UUIDs are the tenant isolation boundary. Every dataset version, run,
metric, artifact, and registry record carries a `project_id`, and backend queries
enforce project access before returning data.

## Resource Governance

Sceptre is built for shared clusters:

- PostgreSQL advisory locks serialize admission decisions.
- The default global limit is two active compute Jobs.
- Each project may hold one active training slot.
- Live CPU and memory headroom determines whether a Job can launch.
- A training Job reserves the selected node's available CPU and memory with equal
  requests and limits, and is pinned to that node for predictable throughput.
- NVIDIA and Intel device-plugin resources are detected explicitly; supported
  estimators use the matching accelerator and retry on CPU when GPU execution fails.
- NVIDIA Jobs run on the RAPIDS 26.06 CUDA 12 image and enable `cuml.accel`
  before sklearn is imported. RAPIDS-supported sklearn estimators use cuML;
  unsupported operations retain the CPU fallback.
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
minikube image build -t automl-training:metrics-v2 -f Dockerfile.training .
minikube image build -t automl-inference:local -f Dockerfile.inference .
```

The training image is based on the NVIDIA RAPIDS CUDA 12 image. NVIDIA nodes
must expose `nvidia.com/gpu` through the NVIDIA device plugin and use a driver
compatible with CUDA 12. Intel GPUs continue to use the Intel device-plugin
resources and the LightGBM OpenCL path rather than RAPIDS, which is NVIDIA-only.

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
OBJECT_STORE_TYPE=minio \
OBJECT_STORE_ENDPOINT=http://127.0.0.1:9000 \
OBJECT_STORE_BUCKET=automl \
OBJECT_STORE_ACCESS_KEY=automl \
OBJECT_STORE_SECRET_KEY=change-me-in-production \
MLFLOW_TRACKING_URI=http://127.0.0.1:5000 \
uvicorn automl_api.main:app --app-dir apps/api --host 0.0.0.0 --port 8000
```

In another terminal, start the React development server. It proxies `/api` to
the FastAPI process on port 8000:

```bash
cd apps/ui/react_app
npm install
npm run dev
```

### Service URLs

| Service | URL |
| --- | --- |
| Sceptre UI | [http://localhost:5173](http://localhost:5173) |
| API documentation | [http://localhost:8000/docs](http://localhost:8000/docs) |
| MinIO console | [http://localhost:9001](http://localhost:9001) |
| MLflow | [http://localhost:5000](http://localhost:5000) |

### Deployed model APIs

Each ready model deployment exposes project- and environment-specific Swagger
documentation from the **Operations** tab:

| Endpoint | Workload |
| --- | --- |
| `POST /v1/predict/online` | One-record online prediction |
| `POST /v1/predict` | Online JSON record batch |
| `POST /v1/predict/offline` | CSV, JSONL, JSON, or Parquet upload with downloadable CSV predictions |
| `GET /v1/metadata` | Project, environment, and model metadata |
| `GET /docs` | Interactive Swagger documentation |

## Configuration

Configuration is supplied through environment variables and Kubernetes Secrets.
The most important operational settings are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DATABASE_URL` | Local PostgreSQL forward | Application metadata connection |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | MLflow tracking endpoint |
| `MAX_CONCURRENT_JOBS` | `2` | Global compute Job limit |
| `MAX_NODE_AVAILABLE_FRACTION_PER_JOB` | `1.0` | Available node CPU and memory allocated to one training Job |
| `TRAINING_ACTIVE_DEADLINE_SECONDS` | `21600` | Minimum Job safety deadline |
| `TRAINING_MAX_ACTIVE_DEADLINE_SECONDS` | `86400` | Maximum Job safety deadline |
| `TRAINING_DEADLINE_MULTIPLIER` | `6` | Planned-duration safety multiplier |
| `JWT_ACCESS_TOKEN_MINUTES` | `1440` | Access-token lifetime |
| `OBJECT_STORE_ENDPOINT` | Environment-specific | MinIO or compatible object-store endpoint |
| `OBJECT_STORE_BUCKET` | `automl` | Shared bucket used by the API and Kubernetes Jobs |
| `OBJECT_STORE_ACCESS_KEY` | Environment-specific | MinIO access key |
| `OBJECT_STORE_SECRET_KEY` | Environment-specific | MinIO secret key |
| `INFERENCE_IMAGE` | `automl-inference:local` | Kubernetes model-serving runtime |
| `INFERENCE_SERVICE_ACCOUNT` | `default` | Service account assigned to model deployments |
| `INFERENCE_SERVICE_TYPE` | `NodePort` | Kubernetes Service type used to expose model APIs |

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

- **88 passing automated tests**
- **4 explicitly disabled compatibility tests**
- **42.35% branch coverage**
- **40% enforced coverage floor**
- XML and HTML coverage reports retained by CI for 14 days

The suite covers ingestion, temporal inference, exact and Dask profiling,
authentication, route contracts, React workflows, Kubernetes resource
estimation, adaptive deadlines, task metrics, estimator discovery, leaderboards,
external validation, MinIO model recovery, historical reconstruction, SHAP
percentage contributions, registry fallback, generated Dockerfiles, inference
contracts, drift summaries, deployment manifests, and guarded cleanup.

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
  ui/react_app/        React and TypeScript product application
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
