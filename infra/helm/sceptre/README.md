# Sceptre Helm chart

This chart is the primary Kubernetes distribution for the complete Sceptre
application. It uses standard Kubernetes APIs and has no runtime dependency on
Minikube, kind, k3d, MicroK8s, Docker Desktop, or any cluster-specific CLI.

## What one Helm release installs

- PostgreSQL and durable metadata storage, or an external database connection
- SeaweedFS and durable dataset/model storage, or an external S3-compatible endpoint
- MLflow and durable artifacts, or an external tracking server
- KubeRay 1.6.2 CRDs and a namespace-scoped KubeRay operator
- Idempotent database bootstrap and Alembic migration Jobs
- FastAPI and React/Nginx Deployments and ClusterIP Services
- Namespace-scoped RBAC for Jobs, pod status/logs, inference workloads, quotas,
  metrics, and optional ingresses
- CPU-first training configuration and a generic inference runtime

Metrics Server, storage provisioners, ingress controllers, GPU device plugins,
and cluster creation remain cluster-owner responsibilities. Their absence does
not block CPU training or normal UI/API operation.

The centralized model-metrics API can scale independently when Metrics Server is
available:

```yaml
api:
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 8
```

Deployment monitoring policies also select `small`, `standard`, `large`, or
`xlarge` resource floors for drift Jobs. The admission check rejects a selected
class when the cluster cannot satisfy its CPU or memory request. HPA applies to
the long-running API; bounded drift computations remain Kubernetes Jobs.

## Prerequisites

- Kubernetes 1.27+ API compatibility; use a currently supported upstream minor
- Helm 3 or 4
- A default dynamic StorageClass, unless storage classes are set explicitly
- Internet access to the public `maponyacharles/sceptreai` Docker Hub repository
- Permission to install the cluster-scoped KubeRay CRDs on the first release

The default release needs roughly 2 CPU cores, 3 GiB RAM, and 25 GiB of
provisionable storage before training workloads are considered.

## Published application images

The chart pulls eight pinned `linux/amd64` images from one public repository:

- `maponyacharles/sceptreai:api-<version>`
- `maponyacharles/sceptreai:ui-<version>`
- `maponyacharles/sceptreai:mlflow-<version>`
- `maponyacharles/sceptreai:postgresql-<version>`
- `maponyacharles/sceptreai:seaweedfs-<version>`
- `maponyacharles/sceptreai:training-cpu-<version>`
- `maponyacharles/sceptreai:training-intel-<version>`
- `maponyacharles/sceptreai:inference-<version>`

NVIDIA training remains an optional bring-your-own image profile; override
`training.nvidia.image.repository` and `training.nvidia.image.tag` before enabling it.

No local image build, import, or registry is needed. Override
`global.imageRegistry`, component repositories, tags, digests, or
`global.imagePullSecrets` only when installing from another registry.
Complete Windows and Linux beginner walkthroughs are in the repository's
[main README](../../../README.md#quick-start-on-local-kubernetes).

## Install

Install the published chart without cloning the source repository:

```bash
helm upgrade --install sceptre oci://registry-1.docker.io/maponyacharles/sceptre \
  --version <version> \
  --namespace sceptre \
  --create-namespace \
  --set environment=local \
  --wait --wait-for-jobs --timeout 15m
```

That single release installs the application data plane and KubeRay operator.
Do not install KubeRay separately. If a cluster administrator already manages a
compatible KubeRay 1.6.2 operator that watches the Sceptre namespace, opt out
explicitly with `--set kuberay-operator.enabled=false`.

Provide secure credentials through a private values file or existing Secrets;
the defaults are only suitable for a local evaluation. Contributors can replace
the OCI URL with `infra/helm/sceptre` and use the included cluster profiles when
testing chart changes from a source checkout.

Check the installation:

```bash
kubectl -n sceptre get pods,jobs,pvc
helm test sceptre -n sceptre
kubectl -n sceptre port-forward service/sceptre-ui 8080:80
```

Open `http://127.0.0.1:8080`. API requests are proxied through the UI service.
If `ingress.enabled=true`, use the configured host instead.

## GPU profiles

The chart does not install device plugins. After the cluster owner installs one:

```bash
# NVIDIA device plugin exposing nvidia.com/gpu
helm upgrade --install sceptre infra/helm/sceptre -n sceptre \
  -f infra/helm/sceptre/values-local.yaml \
  -f infra/helm/sceptre/values-nvidia.yaml

# Intel device plugin; override training.intel.resourceKey if necessary
helm upgrade --install sceptre infra/helm/sceptre -n sceptre \
  -f infra/helm/sceptre/values-local.yaml \
  -f infra/helm/sceptre/values-intel.yaml
```

GPU profiles enable a separate, read-only ClusterRole used only to discover node
extended resources. Training still uses standard resource requests and lets the
Kubernetes scheduler select a node. If observation is forbidden or the resource
is absent, the API reports a warning and uses the CPU image.

## Storage and external services

PostgreSQL, SeaweedFS, and MLflow PVCs use the cluster's default StorageClass unless
`storageClass` is set. Their default `retainOnDelete=true` annotations preserve
data during `helm uninstall`.

The training cache defaults to per-pod `emptyDir`; object storage remains the source of
truth. Enable `training.cache.mode=shared-pvc` only when the selected StorageClass
and access mode work across the cluster (usually RWX).

Set `postgresql.enabled=false`, `seaweedfs.enabled=false`, or `mlflow.enabled=false`
to use external services. Prefer `platform.existingSecret`,
`externalObjectStore.existingSecret`, and `auth.existingSecret` rather than
putting credentials in values files. Required secret key names are documented in
`values.yaml`.
`examples/values-external.yaml` shows the full external-service shape without
embedding credentials.
Set `externalObjectStore.region` to the bucket or container's physical region.
The chart rejects staging and production renders without it so the browser and
API cannot issue an upload with an ambiguous residency classification.
Staging and production also require `uploads.scanner.command`,
`uploads.scanner.version`, and `uploads.scanner.signatureVersion`. The last two
values must be governed release identifiers rather than `unconfigured`; they
are persisted with each upload verification result.
`examples/values-capabilities.yaml` exercises ingress, per-model ingress, an RWX
cache, PriorityClass, ResourceQuota, and LimitRange on clusters that provide
those capabilities.

### Upgrading from the bundled MinIO service

SeaweedFS uses a different on-disk format, so the chart deliberately creates a
new `sceptre-seaweedfs` PVC and leaves any retained `sceptre-minio` PVC
untouched. Before upgrading an installation that contains data, copy every
object from MinIO to an external S3-compatible bucket or to the new SeaweedFS
endpoint, verify object counts and checksums, and only then retire the old PVC.
The application continues to accept existing `minio://` artifact URIs; that
scheme is retained as a compatibility identifier and does not require a MinIO
server.

## Exposure

The UI and API use ClusterIP Services by default. Port-forward is the universal
local fallback. Enabling `ingress` exposes the UI, which also proxies `/api`.

One-click model deployment creates a ClusterIP Service first. The Sceptre API
provides an authenticated gateway from the existing application host to every
ready model, so users do not need to expose or port-forward each model Service.
The Operations UI reports project-scoped routes shaped as:

```text
/api/v1/projects/<project-id>/operations/deployments/<deployment-run-id>/inference/<model-route>
```

Supported model routes are `v1/predict`, `v1/predict/online`,
`v1/predict/offline`, `v1/metadata`, `openapi.json`, `docs`, `health/live`, and
`health/ready`. Every gateway request requires a Sceptre Bearer token and viewer
access to the project.

A configured LoadBalancer, NodePort host, or per-model Ingress can still expose
a model directly. Per-model ingress hosts support `{name}` and
`{deployment_id}` templates. Direct exposure bypasses the project-authenticated
Sceptre gateway; the cluster operator is therefore responsible for TLS,
authentication, and network policy at that edge.

An operator may still run `kubectl port-forward` against a model Service for
troubleshooting. That temporary direct connection bypasses project membership
and is not the normal user-access path.

## Optional controls

Champion refit Jobs require `championRefit.controlBaseUrl` pointing to the API's
HTTPS `/api/v1/internal/refits` endpoint, a digest-pinned `championRefit.image`
(or digest-pinned training image), and explicit `championRefit.egressRules`.
Use Kubernetes NetworkPolicy peers for the API and object-store endpoints with
TCP ports 443, 8443 or 8334. DNS egress is added separately. The cluster must
enforce NetworkPolicies. An optional `championRefit.caSecret` contains only the
public `ca.crt` trust bundle; no private key belongs in that Secret.

`championRefit.cpuCores` and `championRefit.memoryMiB` set equal requests and
limits. Input-budget validation is not a measured peak-memory guarantee. Each
worker receives an attempt-scoped control token and registered input read URLs,
with no database or bucket credentials. The reconciler commits the Job manifest
before creation, owns retry generations, and retains the token Secret and deny
policy until foreground Job deletion finishes. Kubernetes Job retries are off.
These settings implement bounded sklearn refit; evaluator integration and
production runtime, cloud identity, memory and capacity qualification remain open.

- `resourceQuota` and `limitRange` can create namespace guardrails.
- `training.priorityClass.enabled` creates an optional non-preempting class.
- `capabilities.clusterObserver.enabled` grants read-only node/PriorityClass
  observation. It is off by default.
- Metrics Server is optional. Without it, live CPU/RAM telemetry is marked
  unavailable while training status and logs continue to work.

## Upgrade and uninstall

Each install/upgrade creates a revision-specific migration Job. API pods wait for
the database to reach the Alembic head and for every application table to exist
before serving traffic. On a new bundled PostgreSQL volume, the chart creates the
`automl` and `mlflow` databases, applies the initial 13-table application schema,
and lets MLflow initialize or upgrade its own tables. External PostgreSQL must
provide the databases and a user allowed to create and alter tables; the chart
still applies the application migrations.

```bash
helm upgrade sceptre infra/helm/sceptre -n sceptre -f <your-values.yaml> \
  --wait --wait-for-jobs
helm uninstall sceptre -n sceptre
```

With default retention, explicitly delete PVCs only when data loss is intended:

```bash
kubectl -n sceptre delete pvc \
  sceptre-postgresql sceptre-seaweedfs sceptre-mlflow
```

## Chart regression checks

```bash
version="$(sed -n 's/^version = "\([^"]*\)"/\1/p' pyproject.toml)"
helm package infra/helm/sceptre --destination /tmp \
  --version "$version" --app-version "$version"
chart="/tmp/sceptre-${version}.tgz"
helm lint "$chart"
for profile in infra/helm/sceptre/values*.yaml; do
  helm template sceptre "$chart" -n sceptre -f "$profile" >/dev/null
done
```

## Production OIDC and HTTPS

Set `auth.simpleAuthEnabled: false`, `auth.publicAppUrl` to the public HTTPS
origin, and `auth.oidc.issuer` / `auth.oidc.clientId` to the organization's OIDC
configuration. Production requires `auth.oidc.requireMfa: true`. Register
`<auth.publicAppUrl>/api/v1/auth/oidc/callback` with the identity provider and
include the exact application origin in `uploads.allowedOrigins`.

For a confidential client, set `auth.oidc.existingSecret` and optionally
`auth.oidc.clientSecretKey` (default `OIDC_CLIENT_SECRET`). The chart references
the existing Secret; it does not accept client secret material in values. Leave
the Secret name empty for an identity-provider-approved public PKCE client.

With `gateway.tls.enabled: true`, the chart creates only an HTTPS listener and
binds the application route to it. Plain HTTP does not serve the application;
there is no automatic HTTP redirect. Evaluation without TLS retains HTTP.

These values wire application authentication and edge TLS only. Production
database CA mounts, workload identities, NetworkPolicies, live IdP lifecycle
tests and transport/rotation qualification remain separate readiness gates.

### Champion evaluation

Production requires `championEvaluation.enabled: true`. Set HTTPS
`controlBaseUrl` (ending in `/api/v1/internal/evaluations`) and `authorityUrl`,
plus nonempty `egressRules` selecting only the API, authority and object-store
TLS endpoints. Ports are limited to 443, 8443 and 8334. The worker image defaults
to `training.cpu.image`; the resulting image must use a SHA-256 digest, including
in local environments when evaluation is enabled. `cpuCores` and `memoryMiB`
bound the worker resources.

Provision these existing Secrets in the release namespace:

- `allocatorSecret`: `tokens.json`, a JSON object mapping each manifest's
  `project_reference` to its project-scoped allocator token. Only the reconciler
  mounts this Secret; no token material belongs in Helm values.
- `authorityPublicKeySecret`: `public-key.pem`, the authority's public Ed25519
  key. The API and reconciler mount it; evaluator Jobs reference the same Secret.
- Optional `caSecret`: `ca.crt`, the trust bundle for controller and evaluator
  HTTPS connections. Omitting it uses system trust roots.

The reconciler shares the API's existing JWT Secret to sign local refit and
evaluator capabilities. The chart supplies Secret references and configuration;
it does not provision the separate authority or turn an HTTP API into a TLS
endpoint. Live certificate rotation, network isolation and failure-recovery
qualification remain required.

### API database pool metrics

Scrape each API Pod's `/metrics` endpoint on its application port through the private service/network. It exports `sceptre_database_pool_size` (configured persistent capacity), `sceptre_database_pool_checkedin`, `sceptre_database_pool_checkedout`, and `sceptre_database_pool_overflow` as Prometheus gauges. Counters are process-local; scraping a load-balanced Service alone does not measure every replica. Overflow is normalized to zero until connections exceed persistent capacity. Unsupported pool counters are omitted. Scraping reads pool state without acquiring a database connection.

The UI reverse proxy does not forward `/metrics`; keep this endpoint internal and preserve the deployment's TLS verification when configuring a scraper. No ServiceMonitor, scraper, monitoring ingress exception or dashboard is installed by this change. Reconciler, worker and qualification-control pool export and PgBouncer transaction-mode qualification remain open. The implementation uses the already-locked [Prometheus Python client](https://prometheus.github.io/client_python/).

### PgBouncer transaction mode

Set `platform.database.pgbouncerTransactionMode: true` only when the configured database endpoint uses transaction pooling. Both application and qualification-control engine factories omit timeout startup options in that mode and apply statement, lock and idle-transaction limits with transaction-local `set_config` calls at each SQLAlchemy transaction begin. Direct PostgreSQL mode retains its startup timeout options. Psycopg automatic prepared statements remain disabled in pooler mode. Autocommit is rejected because it would bypass the transaction-local limits; migrations and administrative autocommit operations need a separate direct connection.

Configure client TLS and pooler-to-PostgreSQL certificate verification independently. Do not work around startup rejection with `ignore_startup_parameters=options`: discarding those options would discard timeout protection. Local PgBouncer 1.25.1 testing verified one-backend reuse between differently configured clients, all three timeouts, rollback/reconnect, prepared-statement behavior, application names and TLS rejection controls. Production deployment, concurrent workflow claims and failover remain unqualified. See the [PgBouncer startup-parameter documentation](https://www.pgbouncer.org/config.html#ignore_startup_parameters) and [local evidence](../../../docs/production-readiness/evidence/phase-1/pgbouncer-2026-10-05.yaml).
