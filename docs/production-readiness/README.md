# Sceptre Local Development and Production Readiness

> **Current status:** the provider-neutral Helm chart is the
> supported local Kubernetes and compatibility-test distribution. It is not yet
> a production-certified deployment. Shared non-production use is possible when
> the controls in this guide are supplied by the cluster owner. Internet-facing,
> regulated, or availability-critical production is blocked until the launch
> gates below are satisfied.

A successful `helm install` proves that the application can be packaged and run
on Kubernetes. It does not by itself prove high availability, security,
recoverability, large-dataset safety, or production capacity.

## 1. Purpose and Sources of Truth

This document separates three environments that were previously mixed together:

1. A local product installation for an analyst, developer, or evaluator.
2. A shared non-production installation for integration and acceptance testing.
3. A production-qualified installation with explicit operational ownership and
   evidence.

Use the following documents together:

- [Main README: local Kubernetes quick start](../../README.md#quick-start-on-local-kubernetes)
  is the Windows and Linux installation guide for a complete local Sceptre
  release.
- [Helm chart guide](../../infra/helm/sceptre/README.md) documents chart values,
  image import, external services, GPU profiles, exposure, and upgrades.
- [Kubernetes portability contract](../architecture/kubernetes-portability.md)
  describes the implemented provider-neutral scheduling, RBAC, storage, and
  capability boundary.
- This document defines the promotion contract and the evidence required before
  calling an environment production ready.

### Terminology

- **Local Kubernetes installation** means Sceptre runs inside a Kubernetes
  cluster on one workstation. `http://127.0.0.1:8080` is only the browser address
  created by `kubectl port-forward`.
- **Host-process development** means Vite and/or FastAPI run directly on the
  developer machine for a short edit-test loop. It is not a complete deployment
  model.
- **Production ready** describes a specific application version in a specific
  environment after its required controls and tests pass. It is not an
  application-wide label conferred by Helm.
- **Operator-owned** means the Kubernetes or platform operator must provide and
  validate the capability; the Sceptre chart does not install it.

## 2. Environment Model

| Concern | Local analyst/development | Shared non-production | Production |
| --- | --- | --- | --- |
| Purpose | Learn, develop, and evaluate the complete workflow | Integration, user acceptance, upgrade rehearsal, and realistic workload tests | Approved business workloads with an agreed availability and recovery policy |
| Cluster | Single workstation; one or more local nodes | Shared conformant cluster with namespace isolation | Supported, resilient multi-node cluster with a documented upgrade policy |
| Access | Loopback port-forward over HTTP | Private ingress with TLS and controlled users | Managed TLS ingress or gateway, DNS, authentication, authorization, and traffic policy |
| Values | One local image-distribution profile plus a Git-ignored local secret override | Environment-specific non-secret values and centrally delivered Secrets | Reviewed, version-controlled non-secret production values and externally managed Secrets |
| Images | Locally built and imported images; `pullPolicy: Never` is acceptable | Registry-hosted versioned images | Registry-hosted images pinned by immutable digest, scanned, and signed according to organization policy |
| Data services | Bundled single-replica PostgreSQL, SeaweedFS, and MLflow | Bundled services only for disposable testing; external services for persistent shared data | HA or managed PostgreSQL, object storage, and MLflow with tested backup and recovery |
| Persistence | Local PVCs; retention is convenience, not backup | Explicit retention and backup policy for any important test data | Encryption, retention, backup, point-in-time or equivalent recovery, and restore evidence |
| Scale | One user or a small trusted group; CPU-first | Quotas, metrics, bounded concurrency, and production-like tests | Measured capacity, multi-node failure tolerance, alerts, and documented scaling limits |
| Data | Synthetic, public, or safely de-identified | Synthetic or approved non-production copies | Data classified and governed for the environment |
| Readiness result | Functional local installation | Qualified shared test environment | All launch gates pass with an owner and dated evidence |

### Promotion rules

- Promote the same tested image digests; do not rebuild images per environment.
- Select actively supported LTS or stable runtime lines no more than two major
  releases behind current, and pin the exact patch release or immutable digest.
- Keep local cluster profiles local. `values-local.yaml`, `values-k3d.yaml`,
  `values-kind.yaml`, `values-minikube.yaml`, and `values-microk8s.yaml` change
  image distribution and must never be used as a production base.
- Keep credentials out of values files and source control. Values may name
  existing Secrets, but they must not contain secret material.
- Use a separate release namespace, database, object-store prefix or bucket, and
  MLflow tracking boundary for each environment.
- Do not copy local PVCs into production. Promote immutable datasets and model
  artifacts through an approved, traceable process.
- Do not interpret `ENVIRONMENT=production` as automatic hardening. In the
  current application it only changes a small number of API behaviors; the chart
  does not presently expose this setting correctly.

## 3. What the Current Release Actually Provides

### Implemented baseline

The current compatibility baseline includes:

- A React/Vite UI served by Nginx and a separately containerized FastAPI API.
- One provider-neutral chart at [`infra/helm/sceptre`](../../infra/helm/sceptre)
  with thin local-cluster and accelerator profiles.
- API, UI, MLflow, CPU training, NVIDIA/RAPIDS training, Intel training, and
  generic inference image definitions.
- Bundled single-replica PostgreSQL, SeaweedFS, and MLflow for local use, with
  external PostgreSQL, S3-compatible object storage, and MLflow configuration.
- Revision-specific Alembic migration Jobs, fresh-database bootstrap, a
  13-table schema verifier, and API startup gating on a complete schema.
- Namespace-scoped RBAC, Kubernetes training and analysis Jobs, resource
  requests and limits, adaptive deadlines, job status/log reporting, and
  optional CPU/RAM telemetry.
- CPU-first execution with optional NVIDIA or Intel extended resources and
  cluster-supplied device plugins.
- Optional UI and per-model ingress, optional TLS Secret references, optional
  ResourceQuota and LimitRange, and ClusterIP Services by default.
- Functional React workflows for upload progress, target selection, on-demand
  profiling, training, progressive leaderboards, external validation, SHAP,
  registry promotion, fallback selection, drift analysis, inference deployment,
  deployment status, stopping, and cleanup.
- Model endpoint URLs that remain hidden until the configured exposure mechanism
  reports a usable endpoint.
- An evaluation-stage centralized model-metrics view that aggregates authorized
  deployments, deployment-linked production metrics, drift history, retraining
  lineage, revisioned monitoring policy, and versioned JSON/HTML governance
  snapshots. Monitoring Job resource floors and optional API HPA configuration
  provide bounded scale controls.

The current drift workflow remains analyst initiated. When its registered model
has been deployed, new drift runs attach to that deployment and appear in the
centralized history. Sceptre does not yet supply a scheduled durable monitoring
worker, automatic privacy-controlled inference/ground-truth collection, a
dedicated time-series store, alert delivery, or automatic retraining execution.

### Runtime images and responsibilities

| Current image | Current responsibility |
| --- | --- |
| `sceptre-ui` | React static application and same-origin API proxy |
| `sceptre-api` | HTTP API, authentication, profiling threads, admission, Kubernetes workload creation, and reconciliation |
| `sceptre-training-cpu` | CPU training and analysis Jobs |
| `sceptre-training-nvidia` | NVIDIA CUDA/RAPIDS training Jobs |
| `sceptre-training-intel` | Intel-enabled training Jobs |
| `sceptre-inference` | Generic model-serving Deployment |
| `sceptre-mlflow` | Bundled local MLflow server |
| `sceptre-seaweedfs` | Bundled local S3-compatible object storage, rebuilt from the pinned upstream release with security-fixed Go dependencies |

There is no separate orchestrator, durable queue service, model-builder image, or
worker control plane in the current release. Those are production target
boundaries, not current components.

### Helm and operator ownership

| Helm release owns | Cluster or platform operator owns |
| --- | --- |
| Sceptre UI, API, training configuration, inference configuration, and namespace RBAC | Kubernetes lifecycle, nodes, CNI, DNS, time synchronization, and control-plane availability |
| Bundled development PostgreSQL, SeaweedFS, and MLflow, when enabled | Production-grade database, object storage, MLflow, encryption, backup, and disaster recovery |
| Application schema migration and verification | Database creation and privileges for external services, plus pre-upgrade backups |
| ClusterIP Services and optional Ingress objects | Ingress/Gateway controller, certificates, public DNS, WAF, load balancer, and traffic policy |
| Resource requests/limits and optional namespace quota objects | Metrics Server, monitoring stack, node capacity, autoscaling, and cost controls |
| Optional GPU resource selection | Host drivers, device plugins, compatible nodes, GPU telemetry, and scheduling policy |
| Existing Secret references and imagePullSecret names | Secret manager/controller, rotation, registry authentication, image policy, scanning, and signing |

PVC retention annotations prevent an ordinary Helm uninstall from deleting some
claims. They do not provide replication, backup, encryption, or recovery.

## 4. Current Production Blockers

The following are code or operational gaps, not configuration suggestions.

| Area | Current baseline | Production implication |
| --- | --- | --- |
| Identity | Local email/password authentication includes registration, login, rotating refresh tokens, profile updates, password change, and SMTP-backed reset; there is no OIDC/SSO, SAML adapter, MFA, SCIM lifecycle, or passkey support; self-registration creates verified users, browser tokens use `localStorage`, access tokens default to 24 hours, the log WebSocket accepts a bearer token in its URL, and authentication/reset endpoints have no abuse throttling | Production requires a managed identity boundary, secure browser sessions with no bearer tokens in URLs or script-readable storage, phishing-resistant MFA for privileged access, shorter token exposure, rate limits, and reviewed CSRF/XSS and credential-stuffing controls |
| Password reset — **critical** | For any `ENVIRONMENT` value other than the exact string `production`, the unauthenticated reset-request endpoint returns a valid reset token for the supplied existing email address; the UI offers that requester a direct continuation into password confirmation | Any user who can reach a local, staging, or misconfigured deployment can reset another user’s password and revoke their sessions; environment-name gating is not an acceptable control and the token must never be returned by the API |
| Authorization semantics | Project role checks are centralized, but an administrator can create an `OWNER` share link and stored membership `permissions` are not evaluated by authorization decisions | Define and enforce delegation ceilings, ownership-transfer rules, and any claimed fine-grained permissions; test concurrent share-link use limits |
| Organization and tenant lifecycle | The application has users and project membership but no organization/tenant boundary, enterprise group mapping, automated provisioning/deprovisioning, or tenant-level quota and audit policy | Do not claim enterprise multi-tenancy until tenant identity is explicit in data, authorization, storage, queueing, billing/quota, audit, deletion, and negative isolation tests |
| Dataset ingestion | The browser reports multipart progress, but Nginx permits 5 GB on upload routes, FastAPI calls `file.file.read()`, and CSV inspection retains unbounded distinct values and row fingerprints | The former 5 GB or 10 GB goal is not a supported current limit; enforce a small bounded limit until memory-safe resumable or direct-to-object-store upload and bounded inspection are implemented |
| Training memory | Training reads complete dataset objects before creating in-memory pandas structures | Raw file size is not a memory requirement; large-data claims require a bounded or distributed implementation and load evidence |
| Profiling durability | Profiling runs in a FastAPI-owned thread pool and incomplete jobs are resumed at API startup | API restarts and multiple API replicas do not provide safe exactly-once or leased execution |
| Scheduling | FastAPI performs admission and creates Kubernetes resources directly; capacity exhaustion is rejected instead of queued | There is no durable fair queue or separately scalable orchestrator |
| Failure domain | All selected candidates run in one training Job and process | One candidate or process failure can affect the complete tournament |
| Availability | API and UI default to one replica; bundled PostgreSQL, SeaweedFS, and MLflow are single replica; there are no PDBs or topology rules | A node or voluntary disruption can interrupt the control plane or data services |
| Network and workload security | No NetworkPolicy is installed; training Jobs do not set a restricted container security context, the GPU image remains root, and training receives platform database and shared object-store credentials | Namespace RBAC alone does not isolate traffic; a parser, dependency, or image compromise can cross tenant and data-service boundaries |
| Secrets | Defaults contain known JWT, PostgreSQL, and object-store credentials, and production mode does not reject them at startup or chart render | Default values are unsafe anywhere shared; production must fail closed on weak/default secrets and use workload-specific, least-privilege credentials |
| Serving security | Generic inference endpoints have no built-in authentication, authorization, rate limit, or request quota; enabling per-model ingress exposes those endpoints directly and bypasses the authenticated platform gateway | Keep model Services internal until every direct path has authentication, authorization, quotas, TLS, logging, abuse controls, and tenant isolation |
| Model artifact integrity | Inference downloads a model object and passes it directly to `joblib.load()` without checking the registered digest | Object-store tampering can become code execution; verify an expected immutable digest before deserialization and use read-only, prefix-scoped serving credentials |
| Application hardening | Readiness and inference handlers return raw exception text, while the UI proxy does not set a reviewed CSP, HSTS, clickjacking, MIME-sniffing, or referrer policy | Public responses can disclose internal details, and browser compromise has greater impact while tokens remain script-readable |
| Model delivery | A Dockerfile is generated as evidence, but no model builder scans, signs, pushes, resolves, or deploys a model-specific immutable image | Current one-click deployment is a functional baseline, not a governed supply-chain boundary |
| Observability | Health probes, run status, logs, optional resource telemetry, deployment-linked metric/drift history, a governance dashboard, and versioned audit evidence exist; complete user-activity coverage and seven-day centralized log persistence are not proven | Operator alert delivery, durable user/security/application log storage, retention enforcement, platform SLOs, and on-call runbooks still require deployment-specific integration and validation |
| Recovery | Retained PVCs and migrations exist; backup/restore automation does not | Restore time, restore point, credential continuity, and rollback are unproven |
| Release safety | Unit/frontend/migration/render CI, image SBOM/provenance generation, all-severity actionable container and embedded-secret gates, and SecObserve-compatible dependency, IaC, and credential reports exist; most third-party Actions are still version-pinned rather than commit-pinned | Live cluster upgrade, rollback, disaster recovery, security, performance, supply-chain, and multi-cluster qualification are incomplete |

Relevant implementation evidence:

- [Complete API upload buffering](../../apps/api/automl_api/api/routes/datasets.py)
- [Unbounded upload inspection](../../apps/api/automl_api/services/dataset_inspection.py)
- [Current authentication lifecycle](../../apps/api/automl_api/api/routes/auth.py)
- [Password-reset response schema](../../apps/api/automl_api/schemas/auth.py)
- [Password-reset browser flow](../../apps/ui/react_app/src/Auth.tsx)
- [Training log WebSocket authentication](../../apps/api/automl_api/api/routes/training.py)
- [Project role and share-link authorization](../../apps/api/automl_api/services/projects.py)
- [Browser session storage](../../apps/ui/react_app/src/api.ts)
- [Generic inference application](../../apps/api/automl_api/inference/app.py)
- [API-owned profiling lifecycle](../../apps/api/automl_api/services/profiling_jobs.py)
- [API startup profiling resumption](../../apps/api/automl_api/main.py)
- [Direct Kubernetes training control](../../apps/api/automl_api/services/kubernetes_training.py)
- [GPU training image](../../Dockerfile.training)
- [Current Helm defaults](../../infra/helm/sceptre/values.yaml)
- [Current RBAC boundary](../../infra/helm/sceptre/templates/rbac.yaml)
- [Current chart CI](../../.github/workflows/ci.yml)

## 5. Local Development and Evaluation

### Supported complete local path

Use the [main README quick start](../../README.md#quick-start-on-local-kubernetes).
It performs the complete local product installation:

1. Create or select a conformant local cluster.
2. Pull the versioned Sceptre images from Docker Hub.
3. Verify the cluster can reach the public image repository.
4. Generate a Git-ignored values file with random local credentials.
5. Install one Helm release and wait for bootstrap and migration Jobs.
6. Verify pods, Jobs, PVCs, and the Helm smoke test.
7. Port-forward the UI and open `http://127.0.0.1:8080`.

The chart creates the bundled data services and schema for a fresh local
installation. A developer should not manually create the 13 application tables.

### What localhost does and does not mean

| Item | Meaning |
| --- | --- |
| `kubectl port-forward service/sceptre-ui 8080:80` | Temporary browser access to a UI running in Kubernetes |
| `npm run dev` | UI-only Vite edit loop; it expects an API on `127.0.0.1:8000` |
| `compose.yaml` | PostgreSQL and database bootstrap convenience only; it is not the complete Sceptre stack |
| `.env.example` | Unsafe host-development defaults and variable reference; not a deployable production configuration |
| Local embedded object storage | Useful only for isolated API development; Kubernetes training Jobs cannot read files that exist only on the host |
| A local Helm profile | Image-import behavior for one local distribution; not a security or availability profile |

For end-to-end behavior, prefer the local Helm installation. Host-process
development is appropriate for focused code changes when the developer also
provides every dependency and a usable Kubernetes context.

### Local acceptance criteria

A local installation is successful when:

- the selected Kubernetes context and default StorageClass are correct;
- PostgreSQL, SeaweedFS, MLflow, API, and UI are ready;
- bootstrap and migration Jobs completed;
- `helm test sceptre -n sceptre` passes;
- registration and login work through the UI;
- upload progress is visible and upload returns to project overview;
- target selection immediately shows task type and target visualization;
- profiling starts only when the user requests it;
- a small CPU training run completes and logs metrics/artifacts to MLflow; and
- uninstall/reinstall behavior matches the intended PVC retention decision.

This proves local functionality only. It does not satisfy a production gate.

### Local data lifecycle

Retained PVCs survive a normal Helm uninstall, but deleting the cluster, resetting
Docker Desktop Kubernetes, or deleting PVCs destroys local data. Preserve the
same generated secret override when reconnecting to retained PostgreSQL and
SeaweedFS claims. Keep local `.env` and secret override files readable only by their
owner, for example mode `0600` on Linux. Use only disposable or separately
backed-up data locally.

## 6. Shared Non-Production

A shared development, integration, or acceptance environment is the bridge
between a workstation and production. At minimum:

- use registry-hosted versioned images, never local `pullPolicy: Never` profiles;
- replace all default credentials with centrally delivered existing Secrets;
- disable open self-registration unless the test explicitly covers it;
- keep the environment on private networking with TLS ingress and controlled
  identity;
- set ResourceQuota, LimitRange, concurrency, and training resource bounds;
- provide Metrics Server and central application/workload logs;
- use a separate database, object-store bucket/prefix, MLflow boundary, and
  namespace;
- establish data classification, expiry, and cleanup rules;
- back up any data that cannot be recreated;
- exercise schema migration, application upgrade, rollback decision-making, and
  restore before a production release;
- test the same ingress, storage, registry, GPU, and secret-delivery classes
  intended for production; and
- record known deviations so a non-production success is not mistaken for
  production evidence.

Bundled PostgreSQL, SeaweedFS, and MLflow are acceptable for disposable shared tests.
They are not a high-availability architecture.

## 7. Production Qualification Target

The following is an acceptance target, not a current performance claim:

- at least 15 concurrently active users;
- datasets around 10 GB without buffering complete uploads in browser or API
  memory;
- durable admission and fair queueing when compute is unavailable;
- four concurrent large compute workloads as an initial measured baseline, with
  additional work queued;
- reproducible models from immutable data, code, configuration, dependencies,
  and images;
- recovery after API, worker, node, database, and object-store disruptions; and
- one versioned Helm release composed of independently scalable runtime
  responsibilities.

Fifteen simultaneous 10 GB in-memory training Jobs are not implied. Feature
expansion and estimator choice can make working memory many times larger than
the raw file. Capacity must be measured with representative datasets and model
selections.

### Capacity and two-hour benchmark contract

“15 users,” “10 GB,” and “one to two hours” are not sufficient benchmark
definitions on their own. Qualification must publish the exact dataset and
pipeline envelope: file format and compressed/uncompressed size, rows, columns,
sparsity, categorical cardinality, text width, missingness, target balance,
split strategy, feature-selection method, candidate algorithms, search budget,
cross-validation folds, explanations, hardware, storage class, and warm/cold
cache state.

Two separate tests are required:

1. **Admission-safety profile:** 15 authenticated users each start a 10 GB
   resumable or direct-to-object-store upload and submit a qualifying workflow.
   Every accepted request is durable, idempotent, cancellable, visible in queue
   position, and survives API/orchestrator restart. Four running workloads with
   eleven queued may be used as the initial control-plane baseline, but this
   does not satisfy a two-hour completion claim.
2. **Two-hour service profile:** all 15 qualifying workflows are submitted
   concurrently and each reaches its declared terminal success state within
   120 minutes measured from accepted submission, including queue wait,
   profiling, feature selection, tuning, validation, explanation, registration,
   and required artifact persistence. If only training compute is covered, label
   the metric `training_compute_duration` and do not market it as end-to-end
   completion.

Run at least three production-like repetitions, including one cold-cache run and
one controlled worker or node disruption. Report per-stage and end-to-end p50,
p95, maximum, confidence intervals where meaningful, queue wait, throughput,
API latency/error rate, retries, OOM/evictions, CPU/GPU/memory/disk/network,
database/object-store saturation, autoscaling time, and cost. Passing requires
zero lost or corrupt uploads, zero silently dropped or duplicated jobs, zero
cross-project access, zero unexplained OOMs, all 15 service-profile workflows
within 120 minutes, and control-plane/error-budget objectives remaining within
their approved SLOs.

Admission must use measured peak working-set and temporary-storage estimates,
not raw file size alone. Unsupported shapes or algorithms must be rejected
before work starts with a stable reason and recommended resource class. Meeting
the service profile therefore requires enough simultaneously available
capacity—or an implementation proven to reduce the work—not merely a deeper
queue.

### Service-level, overload, and operations contract

Define measurable service-level indicators and objectives before load testing.
At minimum cover UI/API availability and p95/p99 latency, authentication success,
upload-part acceptance and completion, durable queue admission and age, time to
first worker, workflow success and end-to-end duration, inference availability/
latency/error rate, audit-write success, telemetry freshness, and restore
objectives. Publish the measurement point, exclusions, window, target, owner,
alert, runbook, and error-budget policy for each SLO.

“Does not stall” means the control plane remains responsive and gives an honest
state under saturation. Enforce bounded request and dependency timeouts,
connection/worker pools, backpressure, per-user/project/global quotas, priority
and fair scheduling, idempotency keys, exponential backoff with jitter, maximum
attempts, lease expiry, cancellation, poison-work quarantine or dead-letter
handling, and `Retry-After`/stable error responses. Readiness must fail when an
instance cannot safely accept traffic; liveness must not create restart loops
during a recoverable dependency outage.

Capacity and autoscaling policies must include minimum warm capacity, maximum
scale, scale-up latency, node/GPU availability, disruption/headroom reserve,
regional/zone failure assumptions, database/object-store connection and request
limits, queue-age alarms, and a cost ceiling. Error-budget exhaustion freezes
risky releases and triggers the documented reliability work or an explicitly
approved exception; it must not be hidden by excluding failed or queued runs.

The on-call owner needs dashboards, paging and ticket thresholds, dependency and
provider status visibility, incident severity/command/communications, a user
status channel, rollback and feature-disable controls, and exercised runbooks
for saturation, identity outage, stuck/poisoned work, data-service failure,
corrupt artifacts, credential compromise, restore, regional loss, and
multi-cloud partial failure. Every exercise records detection, acknowledgement,
mitigation, recovery, data loss, customer impact, follow-up owner, and due date.

### Model-quality and reproducibility contract

Fast completion is not a valid result if model selection leaks data or produces
an irreproducible model. For every task and representative dataset:

- compare against a simple dummy baseline and the deployed incumbent, if one
  exists, using pre-declared primary and guardrail metrics;
- use stratified, grouped, or time-aware splits where the data-generating
  process requires them, and fit preprocessing and feature selection inside
  each training fold;
- report cross-validation distribution and uncertainty rather than only the best
  point estimate; use precision-recall metrics for materially imbalanced
  classification and calibration evidence when probabilities drive decisions;
- run duplicate, temporal, target, split, and feature leakage checks before
  promotion, plus external holdout or production-like validation;
- record seed, source revision, image/dependency digests, immutable dataset and
  schema fingerprints, split indices, feature set, parameters, resource class,
  and every model/artifact digest needed to reproduce the result; and
- evaluate subgroup performance, fairness, explainability, privacy, and human
  approval where the intended use or policy requires them. A statistically
  worse, uncalibrated, non-reproducible, or policy-failing candidate cannot be
  promoted merely because it finished inside the time budget.

### Target production control boundary

The present API owns HTTP handling, profiling, admission, and Kubernetes
reconciliation. The production target separates those responsibilities:

```mermaid
flowchart LR
    USER[Browser] --> EDGE[TLS ingress or gateway]
    EDGE --> UI[React UI]
    UI --> API[Stateless API]
    API --> PG[(PostgreSQL desired state)]
    API --> OBJ[(Object storage)]

    ORCH[Durable orchestrator] --> PG
    ORCH --> K8S[Kubernetes API]
    K8S --> JOBS[Profiling, training, validation, and explanation Jobs]

    JOBS --> OBJ
    JOBS --> MLFLOW[MLflow]
    MLFLOW --> ARTIFACTS[(Artifact storage)]
```

Required properties of the target boundary:

- PostgreSQL is authoritative for workflow state.
- Object storage is authoritative for datasets and large artifacts.
- API and UI processes do not own durable background work.
- Work is leased, idempotent, recoverable, and queued instead of rejected solely
  because a worker is temporarily unavailable.
- API and orchestrator use separate service accounts and least-privilege
  permissions.
- Training and inference receive workload-specific, narrowly scoped data access.
- Inference traffic is authenticated, authorized, rate-limited, monitored, and
  isolated from the control plane.
- Model promotion and deployment remain explicit governed actions; leaderboard
  rank does not automatically approve production use.

## 8. Production Configuration Contract

There is intentionally no `values-production.yaml` in the repository today.
Shipping one would imply that unresolved organization-specific decisions and
current blockers can be solved by defaults. A production values file must be
created and reviewed for the destination environment after the blockers are
closed.

### Values that production must define

| Concern | Required production decision |
| --- | --- |
| Images | Reachable authenticated registry and immutable digest for every Sceptre runtime image |
| API/UI scale | Replica, disruption, and placement policy proven safe by multi-replica tests |
| PostgreSQL | `postgresql.enabled=false`, external connection Secret, TLS, HA, backup, restore, and migration privileges |
| Object storage | `seaweedfs.enabled=false`, pre-provisioned bucket, TLS endpoint, existing Secret or future workload identity, retention, versioning, and recovery |
| MLflow | `mlflow.enabled=false` with an external protected tracking service and durable artifact store |
| Authentication | Open registration and verification policy, approved account provisioning, secure token/session policy, identity integration, and login/registration/reset throttling |
| Exposure | UI ingress/gateway class, trusted certificate, DNS, proxy limits/timeouts, security headers, error redaction, and request protection |
| Model serving | Internal-only ClusterIP unless an authenticated gateway and endpoint policy are ready |
| Resources | API/UI requests, training resource classes, namespace quotas, node pools, and bounded concurrency |
| Storage | Explicit StorageClass only for any remaining PVC-backed component; encryption and recovery validated |
| Logging and audit | Central sink, structured user-activity event schema, redaction, access/integrity controls, capacity alerts, and at least seven days of searchable user/security/application log persistence |
| Capabilities | Metrics, ingress, GPU, PriorityClass, and read-only cluster observation enabled only when installed and approved |

### Secret contract

Prefer existing Secrets populated by an external secret-management process:

| Reference | Required keys |
| --- | --- |
| `auth.existingSecret` | `JWT_SECRET_KEY` |
| `platform.existingSecret` | `DATABASE_URL` and, only when bundled MLflow is used, `MLFLOW_DATABASE_URL` |
| `externalObjectStore.existingSecret` | Configured access-key and secret-key fields |
| `global.imagePullSecrets` | Registry credentials in Kubernetes pull-secret format |

Current external object storage uses an S3-compatible static-key adapter. It
does not yet provide native Azure Blob/GCS adapters, cloud workload identity,
session-token handling, or chart-managed custom CA mounts. Do not claim those
capabilities until their implementation and tests exist.

Production startup and Helm validation must reject missing, known-default, or
insufficiently strong JWT and configured database, object-store, and SMTP
credentials. Merely placing a default value in a Kubernetes Secret does not make
it secret.

Pre-create the production bucket and grant only the required object operations.
The current health check attempts to create the bucket when it is absent, so
bucket existence and least-privilege behavior must be verified with the exact
credentials used by Sceptre.

### Identity, SSO, OAuth, and account lifecycle

`auth.simpleAuthEnabled=false` only disables self-registration. It does not
configure enterprise identity or harden browser sessions. Use one standards
path first:

- OpenID Connect (OIDC) is the production authentication and SSO contract;
  OAuth 2.0 supplies delegated authorization, not user authentication by itself.
- Use Authorization Code flow with transaction-bound PKCE `S256`, `state`, and
  OIDC `nonce`. Register exact HTTPS redirect URIs and reject the implicit and
  resource-owner-password grants.
- Use a maintained OIDC relying-party library and an external identity provider;
  Sceptre must not become a general-purpose authorization server or store
  enterprise passwords merely to provide SSO.
- Add SAML 2.0 only as an adapter when a named customer identity provider cannot
  use OIDC. Normalize SAML and OIDC identities into the same internal subject,
  organization, group, role, session, and audit model.
- Treat `(issuer, subject)` as the stable federated identity key. Email,
  display name, hosted domain, and other mutable claims are profile data, not
  authorization decisions.

The preferred browser boundary is a backend-for-frontend session: the server
redeems the code and keeps provider/access/refresh tokens out of JavaScript,
while the browser receives an opaque session identifier in a `Secure`,
`HttpOnly`, appropriately scoped `SameSite` cookie. Apply CSRF protection to
state-changing requests, rotate the session identifier after authentication and
privilege change, impose idle and absolute expiry, and revoke all applicable
sessions on logout, deprovisioning, recovery, or security response. Do not put
tokens in query strings, WebSocket URLs, browser history, logs, or
`localStorage`. If a browser token is temporarily unavoidable, document the
threat-model exception, minimize lifetime and scope, and provide replay
detection and revocation.

OIDC validation must allowlist issuer, audience/client ID, authorized party where
applicable, signature algorithms, and redirect targets; validate signature,
`iss`, `aud`, `azp`, `exp`, `nbf`, `iat`, `nonce`, and transaction binding;
cache and rotate provider metadata/JWKS safely; and fail closed on an unknown key,
algorithm, issuer, or audience. Refresh tokens, when issued, require rotation,
reuse detection, revocation, secure server-side storage, and tested provider
logout/session-expiry behavior. Provider outage, JWKS rotation, clock skew,
account disablement, group removal, and replay are required failure tests.

Account lifecycle must define:

- invite-only, verified-domain, or approved just-in-time provisioning policy;
- group-to-role mappings based on immutable provider group IDs, with
  deny-by-default authorization and no automatic mapping to `OWNER` or platform
  administrator;
- SCIM 2.0 provisioning/deprovisioning for enterprise deployments, or a
  documented reconciliation process with an equivalent disablement SLO;
- tenant/org creation, domain claim, ownership transfer, merge, suspension,
  export, retention, deletion, and legal-hold behavior;
- phishing-resistant MFA such as WebAuthn/passkeys for privileged users and
  step-up authentication for identity, ownership, deployment, secret, export,
  billing/quota, and break-glass actions;
- at least two separately controlled break-glass administrator accounts,
  excluded from ordinary federation failure, strongly authenticated, vaulted,
  alerted on every use, rotated, and exercised; and
- non-human identities using OAuth client credentials, cloud workload identity,
  or another short-lived machine credential with an explicit audience and
  narrow scope. A user token must never be shared with a workload.

SSO establishes identity; Sceptre remains responsible for authorization. Enforce
global, organization, project, dataset, run, model, deployment, monitoring,
audit, and export permissions server-side on every API and object lookup.
Positive and negative tests must cover horizontal/vertical privilege escalation,
group-removal delay, stale sessions, share-link delegation ceilings, cross-tenant
IDs, bulk/export paths, and background Jobs acting outside the initiating
user’s current permissions.

The chart now defaults `ENVIRONMENT` to `production`, which suppresses
development API documentation and reset-token responses. That string comparison
is too fragile to protect an account-recovery credential: the current
`reset_token_for_dev` response permits cross-user account takeover in every
non-production environment and in production if the environment is misspelled.

Remove `reset_token_for_dev` from the response schema and browser flow in every
runtime mode. A reset token may leave the server only through the account
owner’s verified recovery channel. If that channel is unavailable, return the
same generic response without exposing a token or reset continuation. Disable
local-password reset for federated-only identities. Reset requests must remain
indistinguishable for existing and unknown accounts, be single-use and
rate-limited, and avoid credentials in proxy logs, referrers, or browser
history.

Production mode does not currently validate secrets, configure federation, or
harden sessions. Identity therefore remains an implementation, migration,
threat-model, and penetration-test gate; documenting these requirements does not
close it.

### Exposure

- Keep UI, API, dependencies, and model Services as ClusterIP by default.
- Port-forwarding is a local diagnostic method, not production ingress.
- Terminate trusted TLS at an approved ingress or gateway and encrypt upstream
  database, object-store, and MLflow connections.
- Apply reviewed CSP, HSTS, clickjacking, MIME-sniffing, and referrer policies,
  and return stable public errors without raw dependency or infrastructure text.
- Rate-limit login, registration, password reset, uploads, and prediction paths
  at the edge, with application-level quotas where tenant identity matters.
- Configure upload size, streaming, timeout, and body-buffering behavior at every
  proxy only after the API ingestion path is memory safe.
- Do not expose a generated inference Service until endpoint authentication,
  authorization, request limits, tenant isolation, logging, and abuse controls
  pass.
- Verify the registered SHA-256 digest before deserializing every model artifact;
  serving identities must have read-only access only to their approved prefix.
- Continue to hide endpoint links until Kubernetes and the configured exposure
  mechanism report a usable endpoint.

### Cloud-provider and simultaneous multi-cluster qualification

A provider-neutral Helm render is not evidence that Sceptre is operationally
qualified on a provider. Qualify one cloud end to end first, then reuse the same
contract for the next provider. Because the current object-store integration is
S3-compatible, AWS is the smallest first qualification unless a required GCS or
Azure Blob adapter is implemented first.

| Target | Required provider integration |
| --- | --- |
| AWS EKS | EKS Pod Identity or IRSA with short-lived, service-account-scoped access; private S3 and approved PostgreSQL/MLflow services; KMS, private endpoints, registry, ingress/gateway, autoscaling/GPU, audit, backup, and restore evidence |
| Google GKE | Workload Identity Federation for GKE; a native GCS adapter or a separately qualified S3-compatible service; approved PostgreSQL/MLflow services; KMS, private networking, registry, ingress/gateway, autoscaling/GPU, audit, backup, and restore evidence |
| Azure AKS | Microsoft Entra Workload ID with the cluster OIDC issuer; a native Blob/ADLS adapter or a separately qualified S3-compatible service; approved PostgreSQL/MLflow services; Key Vault, private networking, registry, ingress/gateway, autoscaling/GPU, audit, backup, and restore evidence |

Each provider profile must pin supported Kubernetes, CNI, CSI/StorageClass,
ingress/Gateway, autoscaler, accelerator/device-plugin, registry, external-secret,
database, object-store, and observability versions. IaC must create the cluster
and external dependencies reproducibly with encrypted, locked remote state,
policy/security scanning, least-privilege deploy roles, drift detection, budget
alerts, and a documented destroy/recovery procedure. Static cloud keys in Helm
Secrets do not satisfy the production identity gate where provider workload
identity is available.

Running on EKS, GKE, and AKS **simultaneously** is a separate product capability,
not the sum of three install tests. Before claiming it:

- identify every cluster and namespace in durable workflow state and route each
  job to an explicit eligible cluster based on data locality, capability,
  quota, cost, and policy;
- use one reconciler ownership/lease model so two clusters cannot execute the
  same logical attempt, and make retries, cancellation, failover, and result
  reconciliation idempotent;
- define whether the control plane and data services are per-cloud or shared,
  then document latency, egress cost, residency, consistency, replication,
  backup ordering, and the failure of the shared dependency. Do not stretch
  PostgreSQL or a filesystem across clouds without a separately qualified
  design;
- make dataset and artifact locations explicit and content-addressed; do not
  assume an S3 URI is readable from GKE or AKS or silently copy regulated data
  across a residency boundary;
- use separate cloud accounts/projects/subscriptions, clusters, service
  accounts, encryption keys, buckets/containers, databases, registries, quotas,
  and audit streams so one compromise or quota exhaustion does not become a
  three-cloud failure; and
- run the 15-user admission-safety and two-hour service profiles with work
  distributed across all three providers, then remove a cluster, identity
  provider, network path, registry, and object store in controlled tests.

Record per-provider and combined results. A failure or unqualified native
storage/identity adapter keeps that provider—and therefore the simultaneous
multi-cloud claim—blocked without preventing a separately qualified provider
from operating.

## 9. Database, Migrations, Upgrade, and Rollback

### Fresh local database

For bundled PostgreSQL, the chart:

1. creates the `automl` database and bootstraps the `mlflow` database;
2. runs `alembic upgrade head` in a revision-specific Job;
3. verifies the Alembic head and all registered application tables; and
4. prevents the API from serving until verification passes.

### External database

The platform operator must create the database, network policy, TLS trust,
credentials, and backup policy before installation. The migration identity must
be able to create and alter application schema objects. Runtime and migration
credentials should be separated in the production target even though the
current chart uses one database Secret.

### Upgrade procedure

For every shared persistent or production-like environment:

1. Record the current chart version, image digests, Alembic revision, values
   commit, and dependency versions.
2. Complete and verify a database backup and object/artifact recovery point.
3. Render the proposed release and inspect image, RBAC, Secret reference,
   Service, Ingress, resource, and migration changes.
4. Rehearse the upgrade against a restored non-production copy.
5. Apply the Helm upgrade and wait for migration Jobs.
6. Verify the schema, API readiness, UI proxy, MLflow connectivity, object-store
   access, and a representative workflow.
7. Hold or roll forward if a schema change is not backward compatible. A Helm
   rollback does not reverse a database migration.

Example inspection commands:

```bash
helm lint infra/helm/sceptre
helm template sceptre infra/helm/sceptre \
  --namespace sceptre \
  --values path/to/environment-values.yaml \
  > /tmp/sceptre-rendered.yaml

helm upgrade --install sceptre infra/helm/sceptre \
  --namespace sceptre \
  --create-namespace \
  --values path/to/environment-values.yaml \
  --wait --wait-for-jobs --timeout 20m

kubectl -n sceptre get deployments,statefulsets,pods,jobs,pvc
kubectl -n sceptre logs job/sceptre-migrate-RELEASE_REVISION
kubectl -n sceptre rollout status deployment/sceptre-api
helm test sceptre -n sceptre
```

The exact generated resource prefix can change with release/name overrides.
Discover the migration Job with `kubectl -n sceptre get jobs` rather than
copying a guessed name into automation.

### Backup and recovery evidence

PVC retention is not a backup. Before production, prove:

- automated PostgreSQL backups and point-in-time or approved equivalent recovery;
- object and MLflow artifact versioning/retention appropriate to the business;
- encrypted backups with independent credentials and lifecycle policy;
- restoration into a clean environment;
- consistency between database metadata, dataset objects, MLflow artifacts, and
  registered models;
- credential and encryption-key recovery;
- documented RPO and RTO achieved during a timed exercise; and
- restore and disaster-recovery runbooks executable by the on-call team.

## 10. Production Launch Gates

Each gate needs a named owner, evidence location, test date, application/chart
version, environment, result, and accepted exception expiry. “Configured” is not
evidence; a test result is.

### Security verification contract

Maintain a versioned data-flow diagram and STRIDE threat model for the browser,
identity provider, edge, API, database, upload/object path, MLflow, orchestrator,
Kubernetes API, build/registry path, training Jobs, model artifacts, inference,
monitoring, and every cross-cloud link. Each threat records asset, trust
boundary, precondition, abuse case, DREAD score, control, owner, due date,
validation evidence, residual risk, and approval. Threats scoring 7 or higher
require an explicit security owner and cannot be silently accepted in a release
note.

Use OWASP ASVS 5.0 Level 2 as the minimum application verification baseline,
plus risk-selected Level 3 controls for privileged administration, sensitive
datasets, model deployment, audit, and key management. Add the OWASP API
Security Top 10 to cover object/function/property authorization, authentication,
resource consumption, sensitive workflows, SSRF, inventory, and third-party API
trust. The security test plan must include:

- SAST, dependency and license analysis, full-history secret scanning, IaC and
  Helm policy scanning, container/OS scanning, SBOMs, signatures, and verified
  provenance for application and model images;
- API authorization tests for every role and object type, including BOLA/IDOR,
  function/property-level authorization, pagination/bulk/export, guessed opaque
  IDs, share links, stale membership, and background worker permissions;
- OIDC redirect, issuer/audience/algorithm/JWKS, PKCE/state/nonce, login CSRF,
  session fixation/replay/revocation, logout, account linking, MFA/step-up,
  SCIM deprovisioning, break-glass, brute-force, and recovery tests;
- upload and data tests for size/count/decompression limits, interrupted
  resumable parts, content/type mismatch, path traversal and object-key
  canonicalization, malicious formulas where exported, parser bombs, malware
  quarantine, unsupported schemas, and storage-quota exhaustion;
- command/template/query injection, XSS/CSP, CSRF, SSRF including cloud metadata
  and DNS rebinding, unsafe deserialization, error disclosure, CORS, WebSocket,
  request smuggling/normalization, and denial-of-wallet/resource-exhaustion
  tests;
- cloud/IaC tests for wildcard or escalation-capable IAM, public storage,
  unencrypted or unversioned data, open management/data ports, missing audit
  logs, default service accounts, unrestricted pod service-account tokens,
  privileged/root workloads, host access, and absent default-deny network
  policy; and
- model-supply-chain tests that reject a tampered digest, unsigned/unapproved
  model or image, poisoned/unapproved dataset version, cross-project artifact,
  unsafe serialized object, and rollback to an unapproved version.

Run DAST and manual penetration testing only against an authorized isolated
environment with production-equivalent controls and synthetic or approved test
data. Complete an independent penetration test before the first internet-facing
launch and after a material identity, tenant, upload, execution, serving, or
cross-cloud boundary change. Retest fixes. Unresolved critical or high findings,
confirmed secrets, exploitable privilege paths, public data stores, or bypasses
of authentication/tenant isolation block release.

| Gate | Required evidence | Current status |
| --- | --- | --- |
| Packaging | Reproducible images, commit-pinned CI Actions, Helm render/install, digest manifest, SBOM, dependency/secret/SAST/container scans, and signature verification | **Blocked** |
| Identity and lifecycle | OIDC Authorization Code + PKCE conformance; issuer/claim/JWKS validation; server-side secure sessions; MFA/step-up; SCIM or equivalent deprovisioning; break-glass; service identity; approved account/tenant lifecycle; no reset token in any response/log; verified recovery delivery; rejection of default secrets; abuse throttling; delegation constraints; and positive/negative authorization tests | **Blocked** |
| Security verification | Current DFD/STRIDE register, ASVS 5.0 and API Top 10 verification, SAST/SCA/secret/DAST/IaC/container evidence, independent penetration test and retest, and no open critical/high finding or confirmed secret | **Blocked** |
| Ingestion | Resumable or direct memory-bounded upload through all proxies with interruption/retry tests | **Blocked** |
| Durable work | Leased persistent queue/orchestrator, idempotency, restart recovery, and multi-replica correctness | **Blocked** |
| Availability | Multi-node placement, safe replicas, PDBs, dependency HA, and node/disruption tests | **Blocked** |
| Data protection and privacy | Classification, purpose/consent where applicable, minimization, malware/quarantine policy, residency, retention/deletion/export/legal hold, encryption/key rotation, least privilege, automated backups, successful clean restore, and RPO/RTO evidence | **Blocked** |
| Kubernetes security | Restricted non-root workload posture, service-account and data-credential isolation, NetworkPolicies, admission policy, and RBAC review | **Blocked** |
| Cloud and IaC | Reproducible IaC, locked state, drift/policy scans, private networking, workload identity, per-provider support matrix, install/upgrade/restore/failure evidence, and combined multi-cluster tests for every simultaneous-cloud claim | **Blocked** |
| Serving security | Authenticated and authorized inference, rate/request limits, tenant isolation, model-digest verification before deserialization, and safe endpoint lifecycle | **Blocked** |
| Platform reliability and observability | Central logs/metrics/traces, correlation IDs, complete user-activity auditing, at least seven days of searchable user/security/application logs, platform/dependency dashboards, paging and ticket alerts, tested SLOs/error budgets, queue/overload behavior, tamper-evident audit export, incident/status process, and exercised runbooks | **Blocked** |
| Model observability and governance | Deployment-anchored performance/drift timelines, governed retraining, versioned governance reports, monitoring-scale tests, scoped roles, and audit evidence defined in Section 15 | **Blocked** |
| Capacity | Three representative executions of both Section 7 profiles with all 15 service-profile workflows succeeding within 120 minutes, durable admission, no data loss/duplicate work/cross-tenant access/unexplained OOM, SLO conformance, saturation and cost measurements, and a published capacity limit | Unqualified |
| Upgrade/recovery | Backward-compatible migration rehearsal, rollback decision test, dependency failure test, and DR exercise | Partial |
| Model validation and governance | Dummy/incumbent comparison, leakage-safe validation and feature selection, uncertainty/calibration and task metrics, reproducibility, external validation, fairness where applicable, immutable lineage, approval, explainability, scan/sign/deploy evidence, monitoring, and rollback policy | Partial |

No internet-facing or regulated production launch may proceed while a **Blocked**
gate remains. A lower-risk internal deployment still needs an explicit security
and data-owner decision; renaming it “production” does not remove the gaps.

### Functional acceptance journey

At minimum, a release candidate must prove:

1. OIDC SSO or the explicitly approved local-auth fallback, provisioning,
   group/role mapping, MFA/step-up, session refresh/revocation, logout,
   deprovisioning, authorization, tenant/project isolation, break-glass, and
   recovery-channel-only reset behave as configured; requesting another user’s
   reset never reveals a token or permits takeover.
2. Upload shows real transfer progress, persists the exact object, and returns to
   project overview without starting profiling.
3. Selecting a target immediately shows the inferred task and appropriate target
   visualization; profiling starts only on explicit user action.
4. Feature profiling renders numeric, categorical, and text diagnostics without
   blocking the API control plane.
5. Every selected model appears in the leaderboard as pending, running,
   succeeded, or failed with stable unique ranks and phase/resource progress.
6. Successful candidates, metrics, parameters, artifacts, and registered models
   are visible and consistent in MLflow.
7. SHAP results are bound to the selected run and model and refresh without a
   manual browser reload.
8. External validation and drift accept uploaded comparison data only after
   schema compatibility checks.
9. Deployment requires the intended approval state, reports its real phase, and
   reveals no endpoint before readiness and exposure succeed.
10. Backup, restore, upgrade, workload retry, cleanup, and failure diagnostics
    preserve or remove state exactly as policy specifies.
11. A deployed model records operational and model-monitoring evidence against
    its deployment and immutable model version, raises a test drift alert,
    creates a governed retraining proposal, and produces a reproducible
    governance report without crossing project authorization boundaries.

### Reliability and failure tests

Test at least:

- API restart during upload, profiling, training, validation, and deployment;
- duplicate user requests and controller retries;
- worker eviction, node loss, unschedulable resources, deadline, and OOM;
- PostgreSQL failover/unavailability and recovery;
- object-store and MLflow latency, denial, and outage;
- ingress/gateway and certificate failure;
- login/reset/upload/prediction abuse at configured rate and size limits;
- OIDC provider outage and recovery, signing-key rotation, invalid issuer,
  audience, algorithm, redirect, PKCE verifier, state, nonce, and clock claims;
- session fixation, replay after logout/deprovisioning, CSRF, bearer-token
  leakage, stale group/role mapping, MFA/step-up bypass, and audited break-glass;
- user activity from authentication through data, training, model, deployment,
  export, role, and administrative actions reaches central audit storage with
  actor, tenant/project, action, object, outcome, time, and correlation ID;
- user/security/application logs survive pod, node, and logging-backend restart,
  remain searchable for at least seven consecutive 24-hour periods, expire only
  according to policy, and do not contain tokens, secrets, raw dataset values,
  passwords, reset links, or unnecessary personal data;
- reset requests for existing and unknown accounts produce indistinguishable
  public responses, and a requester cannot reset another user’s password;
- reset tokens never appear in API responses, application/proxy logs, referrers,
  or browser history in local, shared, or production modes;
- Metrics Server and GPU telemetry absence;
- GPU resource unavailable with expected CPU fallback or explicit rejection;
- stale Kubernetes resources versus database state;
- migration failure before API rollout;
- restore into an empty namespace/cluster;
- model endpoint failure with rollback or safe traffic removal;
- tampered model artifact rejection before deserialization;
- compromised training or inference workload containment without cross-project
  database or object-store access;
- BOLA/IDOR and function/property authorization across tenant, project, dataset,
  run, model, deployment, monitoring, bulk, and export endpoints;
- interrupted/multipart upload retry, malformed and disguised files, traversal
  and object-key normalization, parser/decompression bombs, malware quarantine,
  and storage/compute denial-of-wallet;
- EKS, GKE, and AKS workload-identity failure or revocation, plus loss of one
  cluster during combined multi-cluster admission and execution;
- monitoring-store latency or outage, stale/missing inference telemetry, delayed
  labels, duplicate monitoring windows, failed alert delivery, and safe backfill;
- drift-job retry and scheduler overlap without duplicate metrics, alerts, or
  retraining proposals;
- governance-report regeneration, evidence cutoff, integrity verification, and
  denial of cross-project or unauthorized export.

## 11. Validation Commands and Evidence

The repository CI currently supplies useful compatibility evidence, not
production certification:

```bash
ruff check apps packages alembic scripts tests
pytest tests/ -v --tb=short --cov --cov-fail-under=40
python -m compileall apps packages alembic scripts tests

npm --prefix apps/ui/react_app ci
npm --prefix apps/ui/react_app test -- --run
npm --prefix apps/ui/react_app run lint
npm --prefix apps/ui/react_app run build

helm lint infra/helm/sceptre
for profile in infra/helm/sceptre/values*.yaml; do
  helm template sceptre infra/helm/sceptre \
    --namespace sceptre \
    --values "$profile" \
    > /dev/null
done
helm template sceptre infra/helm/sceptre \
  --namespace sceptre \
  --values infra/helm/sceptre/examples/values-external.yaml \
  > /dev/null
helm template sceptre infra/helm/sceptre \
  --namespace sceptre \
  --values infra/helm/sceptre/examples/values-capabilities.yaml \
  > /dev/null
```

Production qualification must add live-cluster tests for the selected Kubernetes
minor, CNI, CSI, ingress/gateway, registry, secret delivery, external services,
and accelerator profiles. Run install, upgrade, failure, rollback-decision,
backup, restore, load, and security tests on the same classes of infrastructure
used in production.

Retain at least:

- chart package and rendered manifest;
- immutable image digest manifest, SBOM, dependency/secret/SAST/container scan,
  and signature results;
- values commit with Secret names but no secret values;
- database migration and schema verification results;
- OIDC/SSO conformance, MFA/step-up, session, SCIM/deprovisioning,
  break-glass, functional, tenant-isolation, and authorization reports;
- versioned DFD/threat register, ASVS/API verification matrix, SAST/SCA/secret/
  DAST/IaC/container results, penetration-test report, remediation, and retest;
- benchmark dataset/pipeline manifest and raw results for both Section 7
  profiles, including per-run timing, resource/saturation, failure, SLO, cost,
  and capacity reports;
- EKS/GKE/AKS provider support matrices, IaC plans/policy results, workload-
  identity tests, and per-provider plus simultaneous multi-cluster evidence for
  every claimed target;
- backup and restore timestamps;
- dashboards, alert tests, SLO report, and runbook exercise;
- user-activity event coverage and denied-action tests, seven-day log-retention
  configuration plus oldest/newest query evidence, access-control and
  tamper-detection results, redaction tests, and retention-expiry evidence;
- model lineage, approval, validation, explainability, and deployment evidence;
  and
- release approval with owners and exception expiries.

## 12. Prioritized Readiness Roadmap

### P0: production launch blockers

- Make runtime environment explicit in Helm and suppress every development-only
  response in production; reject default or weak secrets before startup.
- Remove `reset_token_for_dev` from the API and UI in every environment; deliver
  single-use reset tokens only through the verified recovery channel and leave
  reset unavailable when that channel is not configured.
- Implement OIDC Authorization Code + PKCE through an external provider and a
  server-side browser session; add phishing-resistant privileged MFA/step-up,
  SCIM or bounded deprovisioning reconciliation, deny-by-default role/group
  mapping, audited break-glass, workload identity, and safe migration/linking of
  existing local accounts. Add SAML only for a named incompatible customer IdP.
- Rate-limit identity, upload, expensive analysis/training, report/export, and
  prediction paths by IP plus authenticated tenant/project identity; enforce
  quotas and cost budgets in the application, not only at ingress.
- Replace API-buffered uploads with bounded inspection and resumable or direct
  object-store ingestion, content-addressed integrity, malware/quarantine
  policy, and bounded parser/cardinality/decompression behavior.
- Move profiling and workload reconciliation into durable leased execution and
  prove multiple API replicas safe.
- Separate API, orchestrator, training, and inference identities and credentials.
- Add production Pod security, NetworkPolicies, PDBs, topology placement, and
  safe replica controls.
- Keep model Services internal, verify model digests before deserialization, and
  expose inference only through an authenticated, authorized gateway.
- Qualify external HA data services, backup/restore, encryption, and least
  privilege.
- Complete the Section 10 threat model and ASVS/API verification plan; add
  dependency, license, full-history secret, SAST, DAST, IaC, Helm, container,
  and model-artifact scanning; pin third-party CI Actions; produce/verify SBOM,
  signatures, and provenance; close and retest all critical/high findings.
- Add explicit tenant/org and data lifecycle semantics: residency,
  classification, minimization, export, retention, deletion, legal hold,
  immutable audit, and negative cross-tenant tests.
- Add central logs/metrics/traces, correlation IDs, complete user-activity
  events, at least seven days of searchable user/security/application log
  retention, redaction, tamper-evident audit export, service-level
  indicators/objectives, error budgets, on-call ownership, status/incident
  communications, and exercised runbooks.
- Establish the deployment-anchored monitoring event schema, privacy and
  retention controls, versioned thresholds, audit events, and governance-report
  integrity contract in Section 15.

### P1: scale and governed delivery

- Isolate candidates or add recoverable candidate checkpoints.
- Build, scan, sign, push, and deploy immutable model-specific artifacts.
- Add canary/traffic policy, automated health rollback, endpoint quotas, and
  prediction monitoring.
- Add the centralized model dashboard, scheduled drift Jobs, governed retraining
  proposals, auditor views, and versioned JSON/HTML/PDF governance reports.
- Implement leakage-safe preprocessing/feature selection and the Section 7 model
  quality, calibration, uncertainty, fairness, reproducibility, and promotion
  contract.
- Pass the Section 7 admission-safety and two-hour service profiles; publish the
  tested dataset/algorithm envelope, resource classes, queue fairness, capacity,
  saturation, cost, and scaling limits.
- Add live install/upgrade/restore/security/performance qualification to release
  CI.

### P2: extended portability

- Qualify one managed cloud end to end, then GKE, EKS, AKS, and required
  on-premises profiles with TLS external PostgreSQL and the support matrix in
  Section 8.
- Add provider workload identity and native GCS and Azure Blob/ADLS adapters;
  retain S3 compatibility without forcing static keys into other providers.
- Add durable multi-cluster placement/reconciliation, explicit data locality,
  per-cloud failure isolation, and combined chaos/load/DR/cost evidence before
  claiming simultaneous EKS/GKE/AKS execution.
- Add custom CA, private registry, offline bundle, and air-gapped procedures.
- Qualify at least one ingress and one Gateway API implementation if both are
  declared supported.
- Publish a support matrix for Kubernetes minors, CNIs, CSIs, GPUs, and external
  service versions.

If simultaneous EKS, GKE, and AKS operation is part of the first production
launch promise, every applicable P2 item above becomes a P0 launch blocker.
Multi-cloud complexity is not a substitute for first making one provider
secure, recoverable, observable, and fast.

## 13. Definition of Production Ready

Sceptre is production ready for a named environment only when:

- all P0 blockers are closed or replaced by formally approved equivalent
  controls;
- every launch gate passes with current evidence;
- the exact release has completed install, upgrade, failure, recovery, security,
  and representative load qualification;
- the restore exercise meets the approved RPO and RTO;
- the named identity/tenant lifecycle, independent security verification, and
  penetration-test gates pass without an unresolved critical/high issue;
- the claimed workload envelope passes the Section 7 profiles; a four-worker
  queue test cannot be used to claim 15 workflows complete within two hours;
- operations, security, data, and model-risk owners approve the release;
- monitoring, alerts, escalation, rollback, and disaster-recovery runbooks are
  active; and
- the model observability and governance gate in Section 15 passes for every
  environment in which that capability is claimed; and
- every claimed cloud provider is independently qualified, and any simultaneous
  multi-cloud claim also passes the combined placement, identity, data,
  failure-isolation, performance, recovery, and cost tests; and
- no local-only image, credential, port-forward, bundled single-replica data
  service, or unprotected model endpoint remains in the production path.

None of the current local Helm profiles satisfies this definition. Local Sceptre
is ready for development and evaluation; production readiness remains a measured
environment-specific promotion decision.

## 14. Production Capability Modes

A production capability mode is an environment-scoped contract, not a UI toggle
or an application-wide marketing label. Each mode must publish one of these
states for the exact release and environment:

| State | Meaning |
| --- | --- |
| `unavailable` | Required implementation or controls are absent; the UI must not imply that the capability is production ready |
| `evaluation` | The workflow can be tested with approved non-production data, but its production gate has not passed |
| `qualified` | The capability gate passed with owners, dated evidence, limits, runbooks, and an exception record |
| `suspended` | A previously qualified mode was withdrawn because evidence expired, a control failed, or an incident requires review |

Capability state must be visible to operators and must fail closed. Enabling a
feature flag, installing a metrics backend, or naming an environment
`production` cannot move a mode to `qualified`. Evidence has an expiry date and
must be renewed after material architecture, dependency, data-contract, or risk
changes.

The centralized model observability and governance mode defined below is in
`evaluation` in the current release. The dashboard, deployment metric ingestion,
revisioned configuration, drift linkage, and governance snapshots form a usable
vertical slice, but the production gate and durable scheduled execution remain
open.

## 15. Centralized Model Observability and Governance Mode

### 15.1 Objective, scope, and personas

This mode provides a production-capable, centralized view of deployed-model
health, performance, drift, retraining, and lifecycle evidence across projects.
It is subject to the same availability, security, privacy, recovery, and audit
standards as the rest of the platform.

The intended personas are:

- **Analysts and project viewers:** see authorized project/deployment metrics,
  explanations, alerts, validation, and governance reports. Analysts may propose
  retraining but cannot approve their own production promotion.
- **Project owners:** configure deployment monitoring within operator-defined
  bounds, acknowledge alerts, and approve project-scoped workflow decisions.
- **Platform administrators:** see authorized cross-project summaries, manage
  resource envelopes and global policies, and approve sensitive production
  actions according to separation-of-duty policy.
- **Audit/compliance readers:** have read-only access to approved reports,
  lineage, audit events, and exports without access to raw inference payloads by
  default.

The dashboard must support an administrator-scoped global view and
project-scoped views. A global view is never implemented by bypassing project
authorization; it uses an explicit privileged role and records access/export
events.

### 15.2 Deployment-anchored identity and lineage

For every deployed model, Sceptre must treat the deployment attempt as the
operational anchor and the immutable model version as the lineage anchor. Every
monitoring or governance record carries, at minimum:

- `project_id`;
- `deployment_run_id`, unique for one deployment attempt;
- `model_version_id`, resolving to an immutable registry/model artifact;
- `environment` and endpoint or traffic-policy identity; and
- creation time plus the creating service/user identity.

The following must attach to both `deployment_run_id` and `model_version_id`:

- performance and prediction metrics over time;
- operational metrics correlated to the serving workload;
- drift Jobs, windows, baselines, outputs, and alerts;
- monitoring configuration and every configuration revision;
- retraining triggers, work items, runs, comparisons, approvals, promotions,
  rollbacks, and rejections;
- explainability, external-validation, fairness, and leakage evidence included
  in governance decisions; and
- governance report records, exports, and evidence manifests.

IDs must never be silently reused. A redeployment, promotion to another
environment, canary, or rollback creates a distinct deployment record and a
timeline relationship to its predecessor. Stopping a deployment freezes its
history; it does not delete or re-parent evidence. Aggregation by model version
is allowed only after deployment-specific records remain recoverable.

Required attachment invariants:

1. Every monitoring write is authorized against the record's `project_id` and
   verifies that the deployment and model version agree.
2. A metric, artifact, alert, or report without a resolvable deployment is
   rejected or quarantined; there are no floating monitoring records.
3. Idempotency keys prevent scheduler overlap, retries, or at-least-once message
   delivery from duplicating a monitoring window, alert, retraining proposal, or
   report generation.
4. Database records point to immutable object/MLflow artifacts by digest or
   version, not only by a mutable path or display name.
5. Deletion and retention preserve referential and audit integrity while meeting
   approved privacy and legal-erasure policy.

### 15.3 Monitoring configuration contract

Monitoring configuration is versioned per deployment and contains:

- enabled operational, data-quality, performance, prediction, and drift metrics;
- schedule, event or label source, baseline dataset/model version, analysis
  window, minimum sample size, and segment definitions;
- warning/critical thresholds, directionality, consecutive-window rule,
  hysteresis, cooldown, and missing-data behavior;
- alert destination, severity, owner, acknowledgement deadline, and escalation
  policy;
- retraining proposal conditions and approval policy;
- data capture, sampling, redaction, retention, residency, and access policy;
- monitoring Job resource class, deadline, retry policy, and priority; and
- configuration author, approval, effective time, expiry, and reason for change.

Threshold changes are prospective and auditable. Historical charts and reports
must resolve the threshold revision effective for each window rather than
rewriting history with the newest value. Configuration must be validated against
operator-defined bounds so one project cannot request unsafe frequency,
retention, cardinality, or compute.

### 15.4 Metrics and monitoring-data contract

The platform must distinguish three evidence classes:

| Evidence | Examples | Preferred source |
| --- | --- | --- |
| Operational | request count, latency distribution, errors, saturation, CPU/RAM/GPU, pod restarts | OpenTelemetry/Prometheus-compatible service telemetry |
| Model behavior | prediction distribution, abstention, confidence, class balance, feature/schema quality | privacy-controlled inference event pipeline and monitoring store |
| Measured performance | accuracy, F1, ROC-AUC, calibration, RMSE, MAE, business outcome | predictions joined to trustworthy delayed ground truth |

Training metrics in MLflow are not production performance metrics. MLflow
remains the source for experiment parameters, training/evaluation metrics, and
artifacts; it must not be the only store for high-volume deployment telemetry.
A production design normally uses a metrics system for operational series and a
time-series/analytical monitoring store for model windows and evidence, with
PostgreSQL retaining authoritative configuration and lifecycle state.

Every aggregated model metric records:

- project, deployment, model version, metric name and metric schema version;
- window start/end, event time and computation time, timezone, sample count,
  missing/late count, and optional approved segment;
- value, unit, aggregation/statistical method, threshold revision, and status;
- baseline/reference identity where applicable; and
- computation Job, code/image version, input snapshot, and artifact lineage.

Metric labels must have bounded cardinality. User IDs, raw feature values,
request IDs, and unrestricted model/project names must not become Prometheus
labels. Detailed evidence belongs in access-controlled storage with retention
limits.

Performance is `unknown`, not healthy, until sufficient trusted ground truth is
available. The UI must show label coverage, label delay, window completeness,
last successful computation, and telemetry freshness so missing data cannot look
like good model health.

### 15.5 Privacy, security, and inference evidence

Monitoring does not grant permission to retain every inference request. Before
collection, the data owner must approve:

- whether raw features, transformed features, predictions, labels, or only
  aggregates may be stored;
- purpose, lawful/approved use, data classification, residency, sampling,
  minimization, redaction/tokenization, retention, and deletion behavior;
- field-level access and whether auditors can see only aggregates;
- encryption and key ownership in transit, at rest, and in backups; and
- controls preventing secrets, free-text PII, or protected attributes from
  appearing in logs, traces, metrics labels, alerts, or exported reports.

Inference events need an opaque join key and event time so delayed outcomes can
be attached without exposing identity in monitoring metrics. The collection path
must validate schema fingerprints, tolerate late/out-of-order events, quarantine
malformed data, deduplicate retries, and measure its own loss and lag. Reported
monitoring results must state when sampling or redaction changes their meaning.

### 15.6 Centralized dashboard behavior

The React UI, backed by RBAC-enforcing FastAPI aggregation endpoints, must offer:

- a privileged cross-project summary and authorized project/deployment views;
- filters for environment, project, deployment status, model/version, task,
  owner, alert severity, and time range;
- time series for appropriate task performance metrics, operational latency,
  throughput/errors/resources, prediction behavior, and feature/global drift;
- visible thresholds, configuration revisions, data completeness, stale-data
  warnings, degradation events, and uncertainty/sample size;
- a single event timeline for retraining, model/version deployment, canary or
  champion-challenger decisions, approval, rollback, threshold changes, drift
  alerts, acknowledgement, and incident response; and
- drill-down into the exact run, model version, external validation, SHAP or
  other explanation, drift artifact, alert, and governance report.

Charts must not compare incompatible metric definitions, task types, segments,
baselines, or time windows. Expensive cross-project queries require bounded time
ranges, pagination/downsampling, query limits, caching where safe, and tests that
authorization is applied before aggregation. Exported dashboard data carries the
same classification, authorization, and audit requirements as the UI.

### 15.7 Drift Jobs and alert lifecycle

Scheduled drift work is a first-class durable workflow. A drift execution record
contains `drift_job_id`, project/deployment/model IDs, monitoring-config revision,
baseline ID, current window, schedule and actual start/end, resource class,
status, retry/attempt, code/image version, metrics, artifacts, and failure
diagnostics.

Each drift definition must document:

- reference dataset/window and why it is appropriate;
- feature/schema and prediction drift methods, bins/categories, distance or
  statistical tests, and task-specific interpretation;
- minimum sample and missing/new-category behavior;
- warning/critical thresholds, consecutive windows, multiple-comparison control,
  seasonal/segment expectations, and false-positive review; and
- limitations: drift does not by itself prove performance degradation or
  causality.

External comparison uploads must pass column, semantic-type, target-presence,
schema fingerprint, privacy, and supported-size validation before computation.
Scheduled production checks must use an approved data source rather than depend
on an analyst uploading a file.

Alert state follows a recorded lifecycle such as `open`, `acknowledged`,
`investigating`, `resolved`, or `suppressed`. Deduplication, suppression windows,
ownership, escalation, delivery attempts, acknowledgement, resolution evidence,
and threshold changes are auditable. Alert delivery failure is itself monitored.

### 15.8 Governed retraining

A threshold breach creates a retraining proposal or work item; it must not
silently promote a replacement model. The proposal references the triggering
deployment, windows, alerts, configuration revision, intended dataset cutoff,
and approval policy.

The retraining workflow must enforce:

- cooldown, deduplication, budget, per-project/global concurrency, and loop
  prevention so repeated windows cannot create a retraining storm;
- immutable data/code/configuration/dependency lineage and the same leakage,
  validation, fairness, explainability, and security checks required for a new
  model;
- champion-challenger comparison against the deployed model using approved
  metrics, segments, holdouts, and business constraints;
- explicit authorized approval before production promotion, with
  separation-of-duty where policy requires it; and
- canary/traffic decision, rollback criteria, rejection reason, and outcome
  attached to both the source deployment and replacement deployment.

An organization may approve narrowly defined automatic rollback or traffic
removal for safety, but automatic training is never equivalent to automatic
production approval.

### 15.9 Scalable and fair execution

Scaling must preserve deployment attachment, authorization, idempotency, quota,
and audit guarantees:

- Kubernetes Jobs/CronJobs or durable orchestrator work items execute bounded
  drift/report computations. Jobs scale through queue depth, controlled
  parallelism, resource classes, and, if adopted, a Job-aware controller such as
  KEDA—not through an HPA attached directly to a completed Job.
- HPA may scale long-running API, event-collector, scheduler, or monitoring-worker
  Deployments using suitable CPU/memory or custom queue/latency metrics.
- Larger CPU/memory/GPU resource classes are operator-defined and exposed through
  Helm values. GPU is used only when the selected algorithm/runtime supports it;
  resource choice must not change metric semantics or reproducibility silently.
- Namespace and project quotas, priority classes, maximum concurrency, deadlines,
  retries, backoff, and preemption policy prevent monitoring or retraining from
  starving inference and control-plane workloads.
- Backpressure must degrade safely: queued or stale monitoring is clearly shown,
  ingestion is bounded, and unavailable capacity never causes evidence to be
  attached to the wrong deployment or dropped without a signal.

Capacity evidence includes sustained and burst inference telemetry, scheduler
overlap, queue backlog, Job fan-out, cross-project fairness, data-store query
load, report generation, HPA/worker scale-up and scale-down, cost/resource
envelopes, and recovery after worker/node/store failure.

### 15.10 Governance report contract

Governance reports are generated per deployment while retaining the immutable
model-version lineage. A report is a versioned evidence snapshot with an
`evidence_cutoff_at`; new monitoring evidence creates a new report version and
never mutates an already approved/exported report.

Each report includes, where applicable:

- report/project/deployment/model IDs, environment, endpoint, owners, approvers,
  dates, intended use, prohibited uses, limitations, risk classification, and
  current lifecycle state;
- source/code revision, dependency and image digests, model artifact digest,
  registry history, approval, deployment, canary, promotion, rollback, and
  retirement history;
- immutable training/validation dataset versions, provenance, collection/time
  windows, classification, consent/approved purpose, quality and sampling;
- preprocessing, imputation, encoding, scaling, text handling, feature
  engineering/selection, feature catalog, and excluded columns with reasons;
- algorithms, search/tuning strategy, parameters, resource/runtime details,
  split/holdout method, task-appropriate metrics, uncertainty, and external
  validation;
- global and approved representative local explanations, explanation method and
  limitations, plus bias/fairness evaluations and protected-group policy where
  lawful and applicable;
- data, feature, target, temporal, split, and duplicate leakage checks, results,
  mitigations, residual risks, and approval of accepted exceptions;
- monitoring configuration revisions, data/label coverage, metric and drift
  methods, thresholds, alert/response history, retraining decisions, incidents,
  and evidence through the report cutoff; and
- open risks, exceptions with owner/expiry, monitoring/SLO status, rollback and
  retirement plan, and final approvals.

Delivery formats are machine-readable versioned JSON (and optionally YAML) plus
accessible human-readable HTML and PDF. The canonical JSON schema is versioned.
Every format records generator version, generation time, evidence cutoff,
content/evidence-manifest digests, and signature/attestation where policy
requires it. PDF/HTML is rendered from the same canonical snapshot and verified
against its digest; it is not an independently edited source of truth.

Generating a report does not certify legal or regulatory compliance. The report
states missing/not-applicable evidence explicitly and requires the named model
risk, security, data, operations, and compliance owners appropriate to the
environment.

The Operations UI provides **Governance & audit** actions to generate a report,
view current and historical versions, verify integrity, and download authorized
formats. Generation and export are audit events.

The Results UI also provides a candidate-scoped evaluation artifact for every
leaderboard model. Its printable HTML and canonical JSON include target-profile
evidence, the executable feature-processing branch, leakage exclusions,
training-pipeline state, algorithm mathematics, metrics/diagnostics, normalized
global SHAP magnitude, and a directional sample waterfall when model-specific
SHAP evidence exists. Missing evidence is explicit and never borrowed from a
different model. This point-in-time download is useful for review, but it is not
yet a signed, retained, versioned governance snapshot and therefore does not
satisfy the production gate by itself.

### 15.11 Access control and auditability

Add an explicit read-only audit/compliance role rather than overloading platform
admin. Effective permissions must cover global summary, project/deployment
details, raw monitoring evidence, configuration changes, alert actions,
retraining proposal/approval, governance generation, integrity verification, and
export. Sensitive actions require recent authentication or equivalent controls
where organizational policy demands it.

At minimum, immutable or tamper-evident audit events capture:

- actor/service identity, effective role, project/deployment/model IDs, action,
  object ID/version, event and receipt time, request/correlation ID, source, and
  outcome;
- login, logout, failed authentication, MFA/step-up, recovery, session
  revocation, provisioning/deprovisioning, break-glass, role/membership/share
  changes, and denied authorization attempts;
- dataset upload, download, export, access, classification, and deletion;
  profiling/training/validation start, cancellation, retry, and completion; and
  model registration, approval, promotion, deployment, inference-endpoint
  exposure, rollback, and retirement;
- before/after digest or safe structured change for monitoring configuration,
  threshold, suppression, approval, promotion, rollback, and retention changes;
- alert acknowledgement/resolution, retraining trigger/decision, report
  generation/integrity verification/export, and privileged dashboard/export
  access; and
- denied attempts and policy failures without logging tokens, secrets, or raw
  protected data.

User-activity audit events and security-relevant authentication/authorization
logs must be centralized and searchable for at least seven consecutive 24-hour
periods from receipt. Application operational logs must have the same minimum
seven-day persistence. A data, legal, incident-response, or regulatory policy
may require longer retention; it may not silently shorten the production
minimum. Expiry after the approved period must be automatic, testable, and
compatible with documented incident/legal holds.

Container stdout and node-local files are transport buffers, not the retained
record. Audit storage has independent access, backup, clock synchronization,
integrity, export, capacity, and retention controls. It is append-only or
tamper-evident, and alerts before ingestion failure, backlog, or storage
exhaustion causes loss. Application administrators must not be able to silently
alter audit history. Audit access, queries, exports, retention changes, and
deletions are themselves audited.

Log only the metadata needed for security and accountability. Never record
passwords, reset links, session/OAuth tokens, secret values, raw dataset rows,
model inputs, unrestricted free text, or unnecessary personal data. Apply
structured redaction before events leave the application and test redaction
against application, ingress, identity, worker, Kubernetes, and cloud audit
streams.

### 15.12 API and persistence boundary

The target API keeps every deployment-specific route under the existing project
and deployment URL boundary:

```text
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/metrics
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/drift
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/config
PUT   /projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/config
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/monitoring/alerts
POST  /projects/{project_id}/operations/deployments/{deployment_run_id}/retraining/proposals
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports
POST  /projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports
GET   /projects/{project_id}/operations/deployments/{deployment_run_id}/governance/reports/{report_id}
```

The public contract uses opaque IDs, pagination, bounded time ranges, explicit
timezone/window semantics, consistent problem responses, and idempotency keys on
write/generation operations. An administrator-only aggregate endpoint may power
the global dashboard, but returned records retain their deployment identity and
are filtered before aggregation.

The conceptual persistence model separates:

- deployment and immutable model-version lineage;
- versioned monitoring configuration;
- inference/label evidence or privacy-preserving aggregates;
- metric windows and drift executions;
- alerts and alert events;
- retraining proposals, runs, approvals, and deployment outcomes;
- governance report snapshots and evidence manifests; and
- append-only audit events.

PostgreSQL remains authoritative for configuration and workflow state. A
time-series/analytical backend stores monitoring windows at the required scale;
object storage holds large immutable evidence/report artifacts; MLflow retains
training/model artifacts. Cross-store references require reconciliation,
backup/restore ordering, orphan detection, and consistency tests.

### 15.13 Availability and operational runbooks

Define SLOs and alerts for the monitoring system itself: collection acceptance,
event loss/duplication, ground-truth join coverage, processing lag, schedule
lateness, Job success, query latency, dashboard freshness, alert delivery, report
generation, and audit-write success. If audit persistence required for a
critical action is unavailable, that action fails closed unless an approved
break-glass procedure records equivalent evidence.

Required runbooks cover:

- triaging performance, drift, stale-data, collection, and alert-delivery alarms;
- acknowledging/suppressing alerts and changing thresholds with approval;
- investigating data quality, schema, late-label, seasonal, or segment changes;
- backfilling idempotently after store/worker outage;
- proposing, approving, rejecting, promoting, canarying, and rolling back
  retraining outcomes;
- restoring and reconciling database, monitoring series, MLflow, object, report,
  and audit evidence;
- rotating monitoring credentials/keys and responding to monitoring-data
  exposure; and
- generating, verifying, exporting, revoking/superseding, and delivering a
  governance report to an authorized auditor or regulator.

Every runbook has an owner, prerequisite access, last exercise date, expected
time, escalation path, and evidence output.

### 15.14 Production readiness gate: observability and governance

This mode is `qualified` only when all of the following evidence exists for the
named environment and exact release:

- a centralized dashboard displays operational health, task-appropriate model
  performance, data/label coverage, drift, alerts, retraining, deployment, and
  rollback timelines for representative production-like deployments;
- every dashboard metric, drift execution, alert, retraining record, and report
  resolves to the correct project, deployment run, model version, configuration
  revision, and evidence window;
- delayed/missing labels, stale/lost telemetry, incompatible metric definitions,
  and insufficient samples are shown as unknown/incomplete rather than healthy;
- drift thresholds, baseline choice, statistical method, alert lifecycle,
  acknowledgement, response, suppression, and retraining proposal are tested;
- monitoring Jobs, queue/backpressure, quotas, and long-running worker/API HPA
  behavior are load-tested without starvation, cross-project leakage, duplicate
  work, or lost attachment;
- at least one production-like deployment has a versioned JSON and HTML/PDF
  governance report covering data, preprocessing, training/tuning, validation,
  explainability, fairness where applicable, drift, alerts, leakage, approvals,
  incidents, limitations, and monitoring evidence through a declared cutoff;
- report schema, evidence/artifact digests, signature/attestation if required,
  regeneration, history, authorization, retention, and integrity verification
  are tested;
- analyst, owner, administrator, and audit/compliance permissions are verified,
  including negative cross-project, raw-evidence, configuration, approval, and
  export tests;
- critical actions and denied attempts appear in tamper-evident, queryable,
  exportable audit records with tested retention and recovery;
- privacy, minimization, sampling, redaction, label-join, retention, deletion,
  residency, encryption, and high-cardinality controls pass data/security review;
- monitoring-store failure, worker/node failure, scheduler overlap, duplicate
  requests, delayed/out-of-order events, alert-delivery failure, backup, clean
  restore, and cross-store reconciliation exercises pass approved objectives;
  and
- the alert, threshold, retraining, monitoring outage/backfill, governance export,
  incident, rollback, and audit-delivery runbooks have owners and dated exercise
  evidence.

Record the gate owner, application/chart/image versions, Kubernetes and
dependency versions, environment, test date, evidence links, result, capacity
limits, residual risks, approved exceptions and expiries, and next review date.
Only then may Sceptre claim centralized model observability and governance
readiness for that environment.

### 15.15 Incremental delivery without premature claims

Implement the mode in evidence-producing increments:

1. **Foundation:** deployment/model lineage invariants, versioned monitoring
   configuration, privacy-controlled event/label contract, monitoring store,
   operational metrics, RBAC, and audit events.
2. **Model monitoring:** project/deployment dashboard, scheduled idempotent drift
   Jobs, data/label coverage, alerts, resource classes, quotas, backpressure, and
   load/recovery tests.
3. **Governed response:** retraining proposals and approvals,
   champion-challenger/canary/rollback timeline, centralized administrator view,
   auditor role, signed/versioned governance reports, exports, and full gate
   exercise.

Earlier increments may be labelled `evaluation`; they do not reduce the gate or
permit a `qualified` claim.

## 16. External References

- [OAuth 2.0 Security Best Current Practice (RFC 9700)](https://www.rfc-editor.org/rfc/rfc9700.html)
- [OpenID Connect Core 1.0](https://openid.net/specs/openid-connect-core-1_0-18.html)
- [SCIM protocol (RFC 7644)](https://www.rfc-editor.org/rfc/rfc7644.html)
- [NIST SP 800-63-4 digital identity guidelines](https://pages.nist.gov/800-63-4/)
- [OWASP Application Security Verification Standard 5.0](https://owasp.org/www-project-application-security-verification-standard/)
- [OWASP API Security Top 10](https://owasp.org/API-Security/editions/2023/en/0x11-t10/)
- [SLSA specification 1.2](https://slsa.dev/spec/v1.2/)
- [Kubernetes production environment guidance](https://kubernetes.io/docs/setup/production-environment/)
- [Kubernetes application security checklist](https://kubernetes.io/docs/concepts/security/application-security-checklist/)
- [Kubernetes security checklist](https://kubernetes.io/docs/concepts/security/security-checklist/)
- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [Kubernetes Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/)
- [Kubernetes supported releases](https://kubernetes.io/releases/)
- [Kubernetes encryption at rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
- [Amazon EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)
- [Workload Identity Federation for GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/workload-identity)
- [Microsoft Entra Workload ID for AKS](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview)
- [Helm chart tests](https://helm.sh/docs/topics/chart_tests/)
