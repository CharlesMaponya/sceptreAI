# Kubernetes portability contract

Sceptre targets conformant Kubernetes APIs. A local distribution is an
installation profile, not an application runtime dependency.

## Layer boundaries

| Layer | Ownership |
| --- | --- |
| Application | FastAPI, React, training/analysis, inference, S3-compatible storage, and MLflow clients |
| Workload | Namespace-scoped Jobs, pod status/logs, Deployments, Services, and optional Ingresses |
| Capability | Optional quota, metrics, GPU, ingress, PriorityClass, and shared-cache discovery |
| Packaging | One generic Helm chart plus thin image-distribution and accelerator profiles |

The API never creates clusters, imports images, enables add-ons, runs
port-forwards, or invokes Minikube/kind/k3d/MicroK8s commands.

## Scheduling contract

- Helm supplies Job CPU/memory requests and limits.
- Kubernetes is the final admission and node-selection authority.
- The API does not calculate node headroom or set hostname node selectors.
- Namespace ResourceQuota is used for early feedback when visible, not as a
  replacement scheduler.
- Unschedulable pod conditions are returned to the UI as status details.
- `MAX_CONCURRENT_JOBS` is an application fairness limit, not cluster capacity.

CPU is the universal runtime. NVIDIA and Intel profiles use configurable
extended-resource keys and distinct images. Read-only node discovery is isolated
behind the optional cluster-observer ClusterRole. If discovery is disabled or
forbidden, GPU execution is disabled and CPU operation continues.

## Namespace permissions

The default API Role is limited to its release namespace:

- create/read/delete training Jobs;
- read workload pods, logs, events, metrics, quotas, and PVC state;
- create/read/delete inference Deployments and Services;
- read only the configured database and object-store Secrets; and
- manage per-model Ingresses only when that capability is enabled.

No ClusterRole is installed by default. The optional observer can read nodes and
PriorityClasses but cannot mutate them.

## Capability degradation

| Capability missing | Behavior |
| --- | --- |
| Metrics Server | Jobs, logs, and status work; live CPU/RAM is unavailable |
| GPU device plugin | GPU is not selected; CPU remains available |
| Ingress controller | ClusterIP plus `kubectl port-forward` remains available |
| PriorityClass | Omitted by default; configured absence is a warning |
| Job TTL controller | Project cleanup can delete completed Jobs |
| Shared RWX storage | Per-pod `emptyDir` cache is used; object storage is authoritative |

## Storage and lifecycle

Bundled PostgreSQL, SeaweedFS, and MLflow use independently configurable durable
claims. A cluster default StorageClass is used unless one is supplied. The chart
can instead consume external services and existing Secrets.

Each Helm revision creates an idempotent database bootstrap and an Alembic
migration Job. A fresh bundled PostgreSQL installation creates the application
and MLflow databases; Alembic then creates the 13 application tables, constraints,
and indexes. API pods do not start until both the current migration revision and
all registered application tables are present. External PostgreSQL remains
responsible for database creation and credentials, while the same migration Job
owns application table lifecycle. PostgreSQL, SeaweedFS, and MLflow claims are
retained on uninstall by default; explicit PVC deletion is the delete-data
operation.

## Exposure contract

The UI, API, dependencies, and deployed models start with ClusterIP Services.
The UI proxies API traffic over generated Kubernetes service DNS. UI ingress is
optional. Model endpoint URLs are returned only after a configured Ingress or
LoadBalancer is admitted, or after a NodePort external host is explicitly set.

## Acceptance matrix

Every change must at minimum lint and render the default, local, Minikube, kind,
k3d, MicroK8s, NVIDIA, Intel, and external-service values. Cluster smoke tests
should additionally cover upload persistence, CPU training, logs/status, absent
Metrics Server, absent GPUs, model exposure, and retained/deleted data on at
least Minikube, kind, k3d, and one other local distribution.
