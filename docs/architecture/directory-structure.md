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
│       └── react_app/             # Production React + TypeScript product UI
├── packages/
│   └── automl_shared/             # shared constants and typed contracts
├── alembic/
│   └── versions/                  # database migrations
├── docs/
│   ├── architecture/              # ecosystem design and implementation sequencing
│   └── decisions/                 # architecture decision records
├── infra/
│   ├── helm/
│   │   └── sceptre/               # primary portable full-stack distribution
│   ├── k8s/
│   │   ├── base/                  # legacy low-level development manifests
│   │   └── overlays/              # legacy Kustomize development overlays
└── tests/                         # fast tests around contracts and metadata
```

## Module Boundaries

`apps/api` owns metadata, RBAC checks, object-store routing, Kubernetes orchestration, job logs, and lifecycle APIs.

`apps/ui/react_app` owns product presentation and client workflow state. It calls
the FastAPI backend rather than reading PostgreSQL or Kubernetes directly.

`packages/automl_shared` is reserved for stable contracts shared by API, UI, and training containers. Keep it small to avoid tight coupling.

`infra/helm/sceptre` is the supported installation boundary. It packages the UI,
API, database migration, PostgreSQL, SeaweedFS, MLflow, namespaced RBAC, durable
storage, and optional capability profiles without depending on a cluster vendor.
`infra/k8s/base` remains a low-level development reference and should not fork
application behavior.

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
