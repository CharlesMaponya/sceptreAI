# Recommended Directory Structure

```text
.
├── apps/
│   ├── api/
│   │   └── automl_api/
│   │       ├── core/              # configuration, security settings, feature flags
│   │       ├── db/                # SQLAlchemy metadata, sessions, migration hooks
│   │       ├── models/            # relational metadata schema
│   │       └── main.py            # FastAPI app factory and health endpoints
│   └── ui/
│       └── streamlit_app/         # Streamlit analytical UI
├── packages/
│   └── automl_shared/             # shared constants and typed contracts
├── alembic/
│   └── versions/                  # database migrations
├── docs/
│   ├── architecture/              # ecosystem design and implementation sequencing
│   └── decisions/                 # architecture decision records
├── infra/
│   ├── k8s/
│   │   ├── base/                  # namespace, ConfigMap, PriorityClass, base manifests
│   │   └── overlays/              # environment-specific Kustomize overlays
│   └── helm/                      # future packaged deployment chart
└── tests/                         # fast tests around contracts and metadata
```

## Module Boundaries

`apps/api` owns metadata, RBAC checks, object-store routing, Kubernetes orchestration, job logs, and lifecycle APIs.

`apps/ui` owns only Streamlit presentation and user workflow state. It should call the FastAPI backend rather than reading PostgreSQL or Kubernetes directly.

`packages/automl_shared` is reserved for stable contracts shared by API, UI, and training containers. Keep it small to avoid tight coupling.

`infra/k8s/base` stores low-overhead Kubernetes primitives suitable for 1-3 node clusters. Heavy dependencies should stay optional.

## Future Modules

Recommended next backend modules:

1. `auth`: registration, login, refresh rotation, password resets, simple-auth/SSO toggle.
2. `projects`: RBAC enforcement, project sharing, workspace isolation.
3. `datasets`: uploads, object storage, dataset versioning, cache PVC coordination.
4. `profiling`: task inference, EDA summaries, quality reports.
5. `training`: resource estimation, Kubernetes job manifests, ZenML/MLflow integration.
6. `explainability`: on-demand SHAP jobs and stored artifacts.
7. `monitoring`: drift checks, cluster usage endpoint, dependency health.
8. `registry`: staged model artifacts, fallback selection, deployment records.
