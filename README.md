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
  <a href="#quick-start-on-local-kubernetes"><strong>Run Sceptre locally</strong></a>
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

> **Current maturity:** The working platform and provider-neutral Helm deployment
> are implemented and tested. Before an internet-facing or regulated production
> rollout, add organization-specific identity, TLS, managed secrets, backups,
> monitoring, and multi-node capacity planning.

The phased target architecture, provider-neutral packaging contract, and
production acceptance gates are documented in the
[Production Readiness Implementation Guide](docs/production-readiness/README.md).
The implemented local-cluster boundary is documented in the
[Kubernetes Portability Contract](docs/architecture/kubernetes-portability.md).

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
- Helm-configured CPU and memory requests/limits describe each training Job;
  Kubernetes scheduling and namespace quotas make the final admission decision.
- Jobs are not pinned to node names. Optional selectors or scheduling policy can
  be added by cluster owners without changing application behavior.
- NVIDIA and Intel device-plugin resources are detected explicitly; supported
  estimators use the matching accelerator and retry on CPU when GPU execution fails.
- NVIDIA Jobs run on the RAPIDS 26.06 CUDA 12 image and enable `cuml.accel`
  before sklearn is imported. RAPIDS-supported sklearn estimators use cuML;
  unsupported operations retain the CPU fallback.
- PriorityClass support is optional and omitted by default.
- Stale database state is reconciled against Kubernetes before admission.
- Planned duration drives cost estimates; the safety deadline ranges from six to
  24 hours and is displayed separately.
- Completed Kubernetes Jobs are removed automatically.

Increasing `MAX_CONCURRENT_JOBS` permits more application-level parallelism;
Kubernetes ResourceQuota and the scheduler remain the final resource guardrails.

## Quick Start on Local Kubernetes

### Prerequisites

- Docker
- Kubernetes 1.27 or newer (Minikube, kind, k3d, MicroK8s, Docker Desktop,
  Rancher Desktop, k3s, or another conformant distribution)
- Helm 3 or 4 and `kubectl`
- A local machine with sufficient CPU, memory, and disk for the selected datasets
  and model budget

### 1. Build the versioned application images

```bash
docker build -t sceptre-api:0.1.0 -f Dockerfile.api .
docker build -t sceptre-ui:0.1.0 apps/ui/react_app
docker build -t sceptre-mlflow:0.1.0 -f Dockerfile.mlflow .
docker build -t sceptre-training-cpu:0.1.0 -f Dockerfile.training.cpu .
docker build -t sceptre-inference:0.1.0 -f Dockerfile.inference .
```

### 2. Make the images available to the cluster

```bash
for image in sceptre-api:0.1.0 sceptre-ui:0.1.0 sceptre-mlflow:0.1.0 \
  sceptre-training-cpu:0.1.0 sceptre-inference:0.1.0; do
  minikube image load "$image"
done
```

Use the equivalent `kind load docker-image` or `k3d image import` command for
those clusters. Docker Desktop can use its shared image store. A registry is the
preferred path for repeatable installs; set `global.imageRegistry` and the image
tags/digests in Helm values. Cluster-specific commands never run inside Sceptre.

### 3. Install the whole application with Helm

```bash
helm upgrade --install sceptre infra/helm/sceptre \
  --namespace sceptre \
  --create-namespace \
  --values infra/helm/sceptre/values-local.yaml \
  --wait --wait-for-jobs --timeout 15m
```

This installs PostgreSQL, MinIO, MLflow, database migrations, the API, the UI,
namespace-scoped RBAC, and the configuration used by training and inference
workloads. No Metrics Server, ingress controller, GPU plugin, or Minikube binary
is required.

### 4. Open the UI

```bash
kubectl -n sceptre port-forward service/sceptre-ui 8080:80
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080). The UI proxies API and
health requests over Kubernetes service DNS. Optional ingress, external
dependencies, persistence, GPU, registry, and uninstall instructions are in the
[Helm chart guide](infra/helm/sceptre/README.md).

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
| `TRAINING_CPU_REQUEST_CORES` | `1` | CPU requested from the Kubernetes scheduler per training Job |
| `TRAINING_CPU_LIMIT_CORES` | `2` | CPU limit per training Job |
| `TRAINING_MEMORY_REQUEST_MB` | `1024` | Minimum requested memory per training Job |
| `TRAINING_MEMORY_LIMIT_MB` | `4096` | Maximum memory and preflight working-set ceiling |
| `TRAINING_ACTIVE_DEADLINE_SECONDS` | `21600` | Minimum Job safety deadline |
| `TRAINING_MAX_ACTIVE_DEADLINE_SECONDS` | `86400` | Maximum Job safety deadline |
| `TRAINING_DEADLINE_MULTIPLIER` | `6` | Planned-duration safety multiplier |
| `JWT_ACCESS_TOKEN_MINUTES` | `1440` | Access-token lifetime |
| `OBJECT_STORE_ENDPOINT` | Environment-specific | MinIO or compatible object-store endpoint |
| `OBJECT_STORE_BUCKET` | `automl` | Shared bucket used by the API and Kubernetes Jobs |
| `OBJECT_STORE_ACCESS_KEY` | Environment-specific | MinIO access key |
| `OBJECT_STORE_SECRET_KEY` | Environment-specific | MinIO secret key |
| `INFERENCE_IMAGE` | `sceptre-inference:0.1.0` | Kubernetes model-serving runtime |
| `INFERENCE_SERVICE_ACCOUNT` | Chart-generated | Service account assigned to model deployments |
| `INFERENCE_SERVICE_TYPE` | `ClusterIP` | Internal Service type used for model APIs |

Use `.env.example` as the local configuration reference. Do not commit production
credentials or reuse the development secrets in `infra/k8s/base`.

## Quality Engineering

Pull requests and pushes to `main` or `develop` must pass all CI gates:

| Gate | Command | Purpose |
| --- | --- | --- |
| Ruff | `ruff check apps packages alembic scripts tests` | Correctness, imports, modernization, and style |
| Tests and coverage | `pytest tests/ -v --tb=short --cov --cov-fail-under=40` | Behavioral, API, UI, training, and analysis verification |
| Syntax | `python -m compileall apps packages alembic scripts tests` | Python 3.11 syntax and import compilation |
| React | `npm test -- --run && npm run lint && npm run build` | UI workflows, types, lint, and production bundle |
| Helm | `helm lint` plus all profile renders | Portable packaging and manifest regressions |

Current quality baseline:

- **101 passing backend tests and 16 passing React tests**
- **4 explicitly disabled compatibility tests**
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
ruff check apps packages alembic scripts tests
pytest tests/ -v --tb=short \
  --cov \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-fail-under=40
python -m compileall apps packages alembic scripts tests
```

## Repository Structure

```text
apps/
  api/                 FastAPI service and training runtime
  ui/react_app/        React and TypeScript product application
packages/              Shared Python packages
alembic/               Database migrations
infra/helm/sceptre/    Primary provider-neutral Helm distribution
infra/k8s/base/        Legacy/development Kustomize resources
scripts/               Validation and operational utilities
tests/                 Automated test suite
docs/                  Architecture, schema, and decision records
```

## Operational Considerations

- The current scikit-learn tournaments are single-pod, in-memory workloads.
- Multi-gigabyte datasets may exceed the configured Job memory limit even when
  the raw file fits on disk.
- Horizontal model training requires additional Kubernetes nodes and a
  distributed backend such as Dask or Ray; an HPA cannot divide one in-memory
  scikit-learn fit across nodes.
- Historical models created before MinIO mirroring are reconstructed from the
  immutable source dataset and saved parameters before explainability runs.
- PostgreSQL, MinIO, and MLflow PVCs require environment-specific backup,
  retention, and disaster-recovery policies.
- The default Helm values contain local-development credentials; override them
  or use existing Secrets before sharing a cluster.

## Documentation

- [Implementation plan](docs/architecture/implementation-plan.md)
- [Directory structure](docs/architecture/directory-structure.md)
- [Database schema](docs/architecture/database-schema.md)
- [Architecture decision 0001](docs/decisions/0001-decoupled-smme-automl.md)

## Contributing

Create a feature branch, keep changes scoped, add tests for behavioral changes,
and open a pull request against `develop` or `main`. CI must pass before merge.
