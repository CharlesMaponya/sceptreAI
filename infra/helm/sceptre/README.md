# Sceptre Helm chart

This chart is the primary Kubernetes distribution for the complete Sceptre
application. It uses standard Kubernetes APIs and has no runtime dependency on
Minikube, kind, k3d, MicroK8s, Docker Desktop, or any cluster-specific CLI.

## What one Helm release installs

- PostgreSQL and durable metadata storage, or an external database connection
- MinIO and durable dataset/model storage, or an external S3-compatible endpoint
- MLflow and durable artifacts, or an external tracking server
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
- Versioned Sceptre images in a registry reachable by the cluster, or imported
  into every local-cluster node

The default release needs roughly 2 CPU cores, 3 GiB RAM, and 25 GiB of
provisionable storage before training workloads are considered.

## Build the application images

```bash
docker build -t sceptre-api:0.1.0 -f Dockerfile.api .
docker build -t sceptre-ui:0.1.0 apps/ui/react_app
docker build -t sceptre-mlflow:0.1.0 -f Dockerfile.mlflow .
docker build -t sceptre-training-cpu:0.1.0 -f Dockerfile.training.cpu .
docker build -t sceptre-inference:0.1.0 -f Dockerfile.inference .
```

Optional accelerators:

```bash
# NVIDIA CUDA/RAPIDS
docker build -t sceptre-training-nvidia:0.1.0 -f Dockerfile.training .

# Intel OpenCL; build the CPU base first
docker build -t sceptre-training-intel:0.1.0 \
  --build-arg CPU_BASE_IMAGE=sceptre-training-cpu:0.1.0 \
  -f Dockerfile.training.intel .
```

For shared or repeatable installations, push these tags to a registry and set
`global.imageRegistry`. Set `global.imagePullSecrets` to Secret names for a
private registry; the chart propagates them to API/UI/dependencies and to model
training/inference workloads. Digests can be set independently under each image.

## Publish images for local clusters

```bash
IMAGES="sceptre-api:0.1.0 sceptre-ui:0.1.0 sceptre-mlflow:0.1.0 sceptre-training-cpu:0.1.0 sceptre-inference:0.1.0"

# Minikube
for image in $IMAGES; do minikube image load "$image"; done

# kind
kind load docker-image $IMAGES --name <cluster>

# k3d: create the registry together with a new cluster
set -euo pipefail
k3d cluster create sceptre-local \
  --registry-create sceptre-registry.localhost:127.0.0.1:5111 \
  --enforce-registry-port-match

for image in $IMAGES; do
  docker tag "$image" "localhost:5111/$image"
  docker push "localhost:5111/$image"
done

for repository in sceptre-api sceptre-ui sceptre-mlflow sceptre-training-cpu sceptre-inference; do
  curl --fail --silent "http://127.0.0.1:5111/v2/${repository}/tags/list" \
    | grep -Fq '"0.1.0"'
done
```

Install that cluster with `values-k3d.yaml`. It points application images at
`sceptre-registry.localhost:5111` and uses `IfNotPresent`, allowing a node to
re-pull a dynamic training or inference image after image garbage collection.
The `--registry-create` form uses the exact registry name supplied above; it
does not require the extra `k3d-` prefix used in some standalone-registry
examples.
An existing cluster created without this registry must be recreated to adopt
the profile. Back up required data first: deleting a k3d cluster deletes its
cluster-local persistent volumes. As a temporary recovery for an older cluster,
`k3d image import sceptre-training-cpu:0.1.0 --cluster <cluster>` makes the CPU
training image available again, but it is not durable against later garbage
collection. Use `values-local.yaml` for Helm upgrades while keeping that legacy
import-based cluster; `values-k3d.yaml` intentionally expects the managed
registry.

The baseline list contains only the CPU training image. Build, tag, and push
`sceptre-training-nvidia:0.1.0` or `sceptre-training-intel:0.1.0` as well before
enabling the corresponding GPU profile.

Docker Desktop's single-node kubeadm provisioner can use locally built images
with `values-local.yaml`. Its kind provisioner requires Docker Desktop's
containerd image store and a kind-compatible image workflow. For MicroK8s, tag
and push the images to its local registry (commonly `localhost:32000`) and
install with `values-microk8s.yaml`. Loading or publishing images is an
installation action; the Sceptre API never invokes these commands. Complete
Windows and Linux beginner walkthroughs are in the repository's
[main README](../../../README.md#quick-start-on-local-kubernetes).

## Install

```bash
helm upgrade --install sceptre infra/helm/sceptre \
  --namespace sceptre \
  --create-namespace \
  --values infra/helm/sceptre/values-local.yaml \
  --wait --wait-for-jobs --timeout 15m
```

Use `values-kind.yaml`, `values-k3d.yaml`, `values-minikube.yaml`, or
`values-microk8s.yaml` in place of the generic local profile when appropriate.
These profiles alter image distribution only; they do not fork application
behavior.

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

PostgreSQL, MinIO, and MLflow PVCs use the cluster's default StorageClass unless
`storageClass` is set. Their default `retainOnDelete=true` annotations preserve
data during `helm uninstall`.

The training cache defaults to per-pod `emptyDir`; MinIO remains the source of
truth. Enable `training.cache.mode=shared-pvc` only when the selected StorageClass
and access mode work across the cluster (usually RWX).

Set `postgresql.enabled=false`, `minio.enabled=false`, or `mlflow.enabled=false`
to use external services. Prefer `platform.existingSecret`,
`externalObjectStore.existingSecret`, and `auth.existingSecret` rather than
putting credentials in values files. Required secret key names are documented in
`values.yaml`.
`examples/values-external.yaml` shows the full external-service shape without
embedding credentials.
`examples/values-capabilities.yaml` exercises ingress, per-model ingress, an RWX
cache, PriorityClass, ResourceQuota, and LimitRange on clusters that provide
those capabilities.

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
  sceptre-postgresql sceptre-minio sceptre-mlflow
```

## Chart regression checks

```bash
helm lint infra/helm/sceptre
for profile in infra/helm/sceptre/values*.yaml; do
  helm template sceptre infra/helm/sceptre -n sceptre -f "$profile" >/dev/null
done
```
