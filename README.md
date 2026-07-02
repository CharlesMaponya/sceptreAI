# SMME-Safe Tabular AutoML Platform

This repository contains a multi-tenant, Kubernetes-native AutoML platform focused on tabular data and small shared clusters.

## Architecture

- **Frontend:** Streamlit analytical UI
- **Backend:** FastAPI service for business logic, metadata, and Kubernetes orchestration
- **Database:** PostgreSQL metadata store for users, RBAC, datasets, runs, metrics, and registry state
- **Storage:** Embedded MinIO by default, with S3/Azure/GCS switching via configuration
- **Compute:** Native Kubernetes Jobs with low-priority scheduling and strict resource guardrails

## Current Scope

- Project IAM, RBAC, share links, and Streamlit multipage workspaces
- Versioned dataset ingestion backed by MinIO
- Full-dataset staged profiling with target reprofiling and artifact reuse
- Kubernetes capacity-aware training admission and low-priority Jobs
- Classification, regression, time-series, and clustering model leaderboards
- Dynamic sklearn estimator selection with task-specific metrics and diagnostics
- Bayesian tuning, MLflow tracking, winning-model persistence, and live logs
- External validation and isolated SHAP analysis with durable MinIO artifacts

## Project Isolation Rule

Project UUIDs are the isolation boundary. Project names are descriptive only and are intentionally not globally unique.

Every dataset version, run, metric, artifact, and registry entry carries `project_id` so backend queries can always enforce tenant/project isolation before returning data.

## Local Development

Create an environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Run syntax checks:

```bash
python -m compileall apps packages alembic tests
```

Start the Minikube PostgreSQL service for local development:

```bash
kubectl apply -k infra/k8s/base
kubectl -n automl rollout status statefulset/automl-postgres
kubectl -n automl port-forward svc/automl-postgres 55432:5432
```

Start the API after dependencies are installed:

```bash
uvicorn automl_api.main:app --app-dir apps/api --reload
```

Initial API routes:

- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `POST /api/v1/auth/refresh`
- `GET /api/v1/auth/me`
- `GET /api/v1/projects`
- `POST /api/v1/projects`
- `POST /api/v1/projects/{project_id}/share-links`
- `POST /api/v1/projects/share-links/accept`
- `GET /api/v1/projects/{project_id}/datasets`
- `POST /api/v1/projects/{project_id}/datasets/upload`
- `GET /api/v1/projects/{project_id}/datasets/{dataset_id}/versions`
- `POST /api/v1/projects/{project_id}/datasets/{dataset_id}/versions/{dataset_version_id}/profile`
- `POST /api/v1/projects/{project_id}/training/estimate`
- `POST /api/v1/projects/{project_id}/training/runs`
- `GET /api/v1/projects/{project_id}/training/runs/{run_id}/leaderboard`
- `POST /api/v1/projects/{project_id}/training/runs/{run_id}/validations`
- `POST /api/v1/projects/{project_id}/training/runs/{run_id}/explanations`
- `GET /api/v1/projects/{project_id}/training/runs/{run_id}/analyses`

Start the Streamlit UI after dependencies are installed:

```bash
streamlit run apps/ui/streamlit_app/app.py
```

## Minikube Training

Build the worker image directly into Minikube and apply the platform manifests:

```bash
minikube addons enable metrics-server
minikube image build -t automl-mlflow:local -f Dockerfile.mlflow .
minikube image build -t automl-training:local -f Dockerfile.training .
kubectl apply -k infra/k8s/base
kubectl -n automl rollout status statefulset/automl-postgres
kubectl -n automl rollout status deployment/automl-mlflow
```

Forward PostgreSQL for the local API, then start the application processes:

```bash
kubectl -n automl port-forward svc/automl-postgres 55432:5432
uvicorn automl_api.main:app --app-dir apps/api --host 0.0.0.0 --port 8000
streamlit run apps/ui/streamlit_app/app.py --server.address 0.0.0.0 --server.port 8501
```

Optional service UIs:

```bash
kubectl -n automl port-forward svc/automl-minio 9000:9000 9001:9001
kubectl -n automl port-forward svc/automl-mlflow 5000:5000
```

Open Streamlit at `http://localhost:8501`, MinIO at `http://localhost:9001`,
and MLflow at `http://localhost:5000`.

Training admission is serialized in PostgreSQL, capped globally at two Jobs, and
limited to one active run per project so concurrent users cannot oversubscribe the
cluster or monopolize both training slots. Admission reconciles stale metadata
against Kubernetes before reserving capacity.

The Training tab discovers compatible models from sklearn's `all_estimators()`
registry using `ClassifierMixin`, `RegressorMixin`, or `ClusterMixin`. Users can
select up to 12 models per run. Clustering always reports silhouette,
Davies-Bouldin, and Calinski-Harabasz; selecting an evaluation label additionally
reports adjusted Rand, normalized and adjusted mutual information,
Fowlkes-Mallows, and homogeneity.

PostgreSQL application and MLflow metadata now live on the
`automl-postgres-data` PVC. MLflow artifacts remain on their own PVC, while every
new successful candidate model is also mirrored to MinIO so validation and SHAP
jobs remain recoverable if an MLflow artifact volume is replaced.

## Key Docs

- [Directory structure](docs/architecture/directory-structure.md)
- [Database schema](docs/architecture/database-schema.md)
- [Implementation plan](docs/architecture/implementation-plan.md)
- [Architecture decision 0001](docs/decisions/0001-decoupled-smme-automl.md)
