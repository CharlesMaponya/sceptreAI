---
meta:
  contentType: Reference
---

# Implement Sceptre for production

> **Document status:** implementation plan only. This guide does not claim that
> any control, cloud platform, performance target, or qualification gate has
> been implemented or passed.
>
> **Content type:** production implementation reference.
>
> **Goal:** implement and qualify Sceptre's automated feature-engineering and
> model-search platform on Polars, Ray, and Kubernetes without data leakage,
> hidden sampling, or competing schedulers.
>
> **Audience:** software engineers, ML engineers, platform engineers, security
> engineers, SREs, release engineers, and implementation agents delivering the
> `0.2.0` production release.
>
> **Authority:** this guide defines the required Phase 0, Phase 0A, and Phase
> 1–13 delivery sequence and the production qualification
> contract. The current-state audit in
> [README.md](README.md) remains useful evidence, but any weaker workload target,
> backpressure allowance, or completion criterion in that audit is superseded by
> this guide.
>
> **Accepted architecture decision, 2026-08-09:** Polars and Ray are the only
> production data and distributed-execution path for this release. Dask is
> prohibited as a dependency, implementation, fallback, deployment, test path,
> or documented option. KubeRay manages Ray workloads. Ray owns Ray-worker
> scaling, and the Kubernetes node autoscaler owns node provisioning.

## Document map

- [Required outcome](#1-required-outcome)
- [Decisions and production contract](#2-accepted-decisions-and-production-contract)
- [Baseline](#3-baseline-that-phase-0-must-regenerate)
- [Target architecture and interfaces](#4-target-architecture-and-required-interfaces)
- [Phase 0: Baseline](#phase-0-freeze-and-reconcile-the-moving-baseline)
- [Phase 0A: Feasibility](#phase-0a-prove-feasibility-cost-and-recovery)
- [Phase 1: Durable state](#phase-1-durable-state-idempotency-and-public-contracts)
- [Phase 2: Ingestion](#phase-2-cloud-portable-10-gib-ingestion)
- [Phase 3: Data and features](#phase-3-ray-data-preparation-and-automated-feature-engineering)
- [Phase 4: Ray execution](#phase-4-ray-tune-train-and-catalog-orchestration)
- [Phase 5: Helm](#phase-5-portable-helm-runtime)
- [Phase 6: Security](#phase-6-identity-security-and-serving-hardening)
- [Phase 7: Reliability](#phase-7-observability-reliability-and-recovery)
- [Phase 8: Candidate build](#phase-8-hermetic-cicd-and-candidate-build)
- [Phase 9: Local and Railway](#phase-9-local-runtimes-and-railway)
- [Phase 10: EKS](#phase-10-aws-eks-implementation-and-prequalification)
- [Phase 11: GKE](#phase-11-google-gke-implementation-and-prequalification)
- [Phase 12: AKS](#phase-12-azure-aks-implementation-and-prequalification)
- [Phase 13: Qualification](#phase-13-final-production-qualification)
- [Cross-phase test matrix](#cross-phase-test-matrix)
- [Gate record](#gate-record-template)
- [Evidence layout](#evidence-layout-and-naming)
- [Assumptions and defaults](#explicit-assumptions-and-defaults)
- [Definition of done](#definition-of-done)

## 1. Required outcome

Convert Sceptre from an evaluation-grade Kubernetes application into a
separately qualified production release that:

- generates, searches, rejects, versions, and serves leakage-safe feature
  recipes;
- uses Ray Data with bounded Arrow batches and Polars transformations;
- uses Ray Tune for model-plus-feature trials and Ray Train only for
  distributed-native training recipes;
- keeps PostgreSQL and object storage as the durable source of truth while
  treating Ray execution state as recoverable and disposable; and
- qualifies the same application release for:

  - Amazon Elastic Kubernetes Service (Amazon EKS);
  - Google Kubernetes Engine (GKE);
  - Azure Kubernetes Service (AKS);
  - local functional-conformance environments on k3d, kind, Minikube, and
    MicroK8s; and
  - ancillary Railway PostgreSQL and S3-compatible bucket contract testing.

Railway is not a Kubernetes deployment target for this release. Local clusters
prove functional portability, not production capacity. EKS, GKE, and AKS must
each pass independent platform, validation, recovery, security, and workload-
stage qualification. That independence does not permit repeated use of a
locked release test: each task split has one cross-provider final allocation,
executed once on its preregistered canonical provider.

The supported application boundary is a provider-neutral Helm chart. OpenTofu
and GitHub Actions provision cloud infrastructure outside the application. The
FastAPI service must never expose a cluster-creation API or receive cloud-admin
credentials.

### 1.1 How an implementation agent must use this guide

An implementation agent must:

1. Start at Phase 0 and complete phases in order unless a phase explicitly says
   work may proceed in parallel.
2. Create a tracked work item for every bullet under **Work** and every item
   under **Gate**.
3. Link each code change, migration, infrastructure plan, test, and evidence
   artifact to its work item.
4. Treat a phase gate as a release boundary. A phase is not complete because the
   code exists; the listed evidence must pass in an appropriate environment.
5. Preserve backward compatibility during expand/backfill/contract migrations
   and during the selected-model to all-catalog API transition.
6. Record exceptions with an owner, business reason, compensating control,
   expiry date, and explicit approver. No exception may weaken the hard
   performance contract after its qualification target is approved.
7. Stop production promotion when evidence is missing, stale, provider-mocked,
   or generated from artifacts other than the immutable release candidate.
8. Never mark an estimator successful if it was skipped, silently replaced, or
   sampled without the required disclosure.

Before starting a phase, materialize its checklist in the issue tracker and the
release evidence index. Assign work bullets `P<phase>-W01`, `P<phase>-W02`, and
so on; gate bullets use `P<phase>-G01`, `P<phase>-G02`, and so on. Record the
guide commit with the mapping. Never renumber an issued ID: insert later work
with a suffix such as `P2-W04A`. Each record must name repository paths, tests,
evidence output, owner, reviewer, prerequisites, rollback, and requalification
triggers. This convention makes every checklist item independently assignable
without forcing volatile issue IDs into this guide.

Phase 0 creates `docs/production-readiness/task-index.yaml`, the authoritative
mapping after initial assignment. Each entry stores `id`, `phase`, `kind`,
heading path, normalized-bullet SHA-256 fingerprint, guide commit,
prerequisites, evidence-schema revision, issue URL, and optional `supersedes`.
IDs are assigned once, never regenerated from current ordinal position. CI fails
on a missing/duplicate ID, changed fingerprint without an explicit reviewed
carry-forward/supersession, or prose bullet absent from the index.

### 1.2 Phase dependency map

```text
Phase 0 → Phase 0A
Phase 0A ─┬→ Phase 1 → Phase 2 ────────────────┐
          └→ Phase 5 platform bootstrap ───────┼→ Phase 3 → Phase 4
Phase 1 + Phase 4 → Phase 5 final conformance ─┬→ Phase 6
                                               └→ Phase 7
Phases 1–7 → Phase 8
Phase 8 → Phase 9
        ├→ Phase 10 ─┐
        ├→ Phase 11 ─┼→ Phase 13
        └→ Phase 12 ─┘
```

Phase 0A is a kill gate. Do not start substantial schema, cloud, or execution
work until it passes. Phase 1 and the Phase 5 platform bootstrap may begin in
parallel. Phase 3 requires both Phase 2 and a pinned, tested KubeRay operator,
CRDs, workload templates, and identity pattern from that bootstrap. Phase 5's
final conformance gate waits for the Phase 1–4 runtime contracts. Phases 6, 7,
and Phase 8 pipeline scaffolding may then proceed in parallel; the Phase 8
release gate waits for Phases 1–7. Cloud module construction may begin after
Phase 0A, but provider prequalification cannot begin until Phases 0, 0A, and
1–8 pass. EKS, GKE, and AKS all join into Phase 13.

## 2. Accepted decisions and production contract

This section separates accepted product and architecture decisions from
qualification targets that still require a named business owner and budget
approver. An implementation agent must not silently convert an assumption into
an accepted requirement.

### 2.0 Requirements and decision register

Maintain a machine-readable register with decision ID, source, owner, approver,
decision date, rationale, status, and superseded alternatives. Phase 0 freezes
the first revision.

| ID | Decision or target | Status | Source and implementation effect |
| --- | --- | --- | --- |
| ADR-001 | Use Polars and Ray as the first and only production pathway; exclude Dask completely | Accepted | User decision, 2026-08-09. Remove Dask from code, dependencies, CI, Helm, tests, scripts, and docs |
| ADR-002 | Run Ray through KubeRay; Ray scales workers and the Kubernetes node autoscaler scales nodes | Accepted | Prevents KEDA, HPA, or another controller from competing for Ray worker counts |
| ADR-003 | Use one immutable benchmark dataset version, target, row set, and split revision for comparable model-plus-feature experiments | Accepted | Trials vary recipes and parameters, not physical dataset copies or split rows |
| ADR-004 | Keep PostgreSQL and object storage durable; treat Ray clusters, jobs, ObjectRefs, and local spill as disposable execution state | Accepted | Every logical attempt maps to a fenced Ray job/trial and remote checkpoint lineage |
| ADR-005 | Use one ephemeral project-and-stage-bound Ray cluster per splitter, preparation, training-run, champion-refit, champion-evaluation, or provider-stage-conformance attempt | Pending Phase 0A architecture, security, and budget approval | Enforces raw/test isolation, workload identity, object/spill isolation, and Kubernetes quotas; Phase 0A must validate 15-head startup, operator load, cache loss, recovery, and cost |
| QLT-001 | Admit 15 concurrent runs | Pending named business and budget approval | Phase 0A must prove head/worker capacity, startup, fairness, and cost |
| QLT-002 | Use exactly 10 GiB rather than decimal 10 GB for the load fixture | Pending product approval | The load corpus may use exact 10 GiB; the ML benchmark records its actual immutable byte size |
| QLT-003 | Qualify classification, regression, time-series, and clustering | Pending product approval | Each task uses its own immutable conformance fixture; cross-task metrics are never compared |
| QLT-004 | Preserve an 8-core, 24 GB local evaluation profile | Pending product approval | Phase 9 includes a bounded single-node Ray and Polars profile without the production deadline |
| QLT-005 | Execute every supported task-applicable estimator | Pending product and budget approval | Catalog exclusions remain visible and reviewed |
| QLT-006 | Run five search suggestions per estimator | Pending product and budget approval | Tunable entries search joint feature/model choices; fixed entries search five feature-only choices with model parameters fixed, including raw and safe engineered choices when available. This is workload strength, not proof that Bayesian search converged |
| QLT-007 | Evaluate each configuration with three task-correct inner folds | Pending product approval | The feature contract owns group/time/gap-aware splitter semantics |
| QLT-008 | Finish each approved qualification group within 7,200 seconds | Pending business and budget approval | Phase 0A and later provider calibration must prove the schedule and cost |
| QLT-009 | Qualify platform, validation, recovery, security, and every RayJob stage independently on EKS, GKE, and AKS | Pending product and budget approval | One provider pass never substitutes for another; synthetic conformance qualifies refit/evaluation everywhere while each locked task test is consumed once on its canonical provider |

Accepted architecture decisions are binding. Phase 0A cannot pass while an ADR
or QLT row remains pending: mark each accepted, rejected, or superseded with an
owner, approver, date, rationale, and cost ceiling where applicable. Generate
the later gate index from that frozen register. A numeric clause below is a
provisional feasibility target until its QLT is accepted; rejected or
superseded rows require a reviewed guide and gate-index update rather than a
silent change during execution.

### 2.1 Concurrency and input

1. The provisional qualification target admits 15 authenticated training runs
   concurrently. None may enter a
   `waiting_capacity` or rejected state because a platform-wide concurrency
   limit was reached.
2. The systems-load gate uploads 15 independent 10 GiB objects, each exactly
   10,737,418,240 bytes. This corpus tests transport, parsing, preparation,
   storage, and autoscaling. It is not the ML fairness benchmark.
3. Comparable feature and model experiments reference one immutable benchmark
   dataset version, target, split revision, and train/validation/test row
   digests. They must not create one physical dataset per trial.
4. The systems-load gate uses uncompressed CSV as the parsing worst case.
   Canonical ML benchmarks use immutable, partitioned Parquet after verified
   preparation. Parquet,
   JSONL/NDJSON, and every other supported smaller format receive separate
   conformance tests.
5. Production object data flows directly between the browser and provider
   object storage. API and UI pods must not proxy the 150 GiB data plane.

### 2.2 Estimator coverage

The all-catalog and experiment-strength clauses become hard qualification gates
only after QLT-005, QLT-006, and QLT-007 approval. Until then, they define the Phase 0A
feasibility workload.

1. Every run executes the complete task-applicable estimator catalog revision.
2. Remove the current 20-candidate limit for `catalog_mode=all`.
3. Classification, regression, time-series, and clustering use separate
   immutable task-conformance fixtures. Within a task, every comparable run
   uses the same rows, target where applicable, and split revision.
4. If QLT-003 is approved, qualification on each managed provider includes:

   - 15 classification runs;
   - 15 regression runs;
   - 15 time-series runs;
   - 15 clustering runs; and
   - 15 mixed runs with a deterministic 4/4/4/3 task distribution. Rotate the
     task assigned to three users across mixed-run repetitions.

   Run four mixed repetitions. Designate the principal benchmark as the
   task-conformance fixture for one homogeneous group, so its 15 comparable
   runs replace that group rather than adding a fifth homogeneous group. The
   resulting base is 60 homogeneous plus 60 mixed runs, or 120 runs per
   provider.

   Mixed repetitions are validation-only scheduling and compatibility gates;
   they never consume a locked final test or participate in release-champion
   selection.

5. The qualification catalog is the signed catalog generated from the pinned
   runtime. Abstract, removed, test-only, or logically non-applicable classes
   remain visible in the catalog with reviewed reasons.
6. A candidate failure, timeout, unexplained exclusion, missing artifact, or
   unrecorded sampling fallback fails qualification.

### 2.3 Deadline and experiment strength

The following deadline and search-strength targets are provisional until
QLT-006, QLT-007, and QLT-008 have named approvers and an approved cost ceiling:

1. An unscoped run's training clock starts when
   `POST /api/v1/projects/{project_id}/training/runs` returns `202 Accepted`
   with `admission_state=submitted`, a durable run, a consumed capacity
   reservation, and a published outbox command. A scoped launch may first
   return `202 admission_state=barrier_pending`; that response creates no Ray
   work and starts no training clock. The group clock starts at the single
   database `scope_started_at` timestamp that atomically seals the complete
   membership, consumes every reservation, changes every member to `submitted`,
   and emits their outbox commands.
2. All 15 runs, including catalog planning, candidate fitting, scoring,
   artifact recording, and leaderboard aggregation, plus scope-level champion
   selection, refit, final evaluation where promotional, CAS, and evidence
   finalization, finish within 7,200 seconds of the applicable start timestamp.
3. Each tunable catalog entry uses five joint feature-plus-model suggestions,
   each evaluated with three task-correct inner folds. A fixed entry uses one
   fixed model configuration with a five-suggestion feature-only budget: raw is
   mandatory and distinct safe engineered choices fill the remaining slots.
   If safety filtering leaves fewer than five unique choices, execute every
   unique safe choice and record `safe_search_space_exhausted`; never duplicate
   a no-op or weaken a gate merely to reach five. Evaluate each across the same
   task-correct folds; "fixed" never means a single holdout-only fit. Champion
   refit is additional work.
4. The deadline must not be met by lowering iterations, lowering folds,
   shrinking the catalog, silently changing validation semantics, or hiding
   failures.
5. Search uses task-correct inner folds inside the training role. Candidate and
   cross-run selection use one common outer validation holdout. Create one
   sealed `evaluation_scope` for the comparable campaign, select one global
   champion before test access is granted, refit it on train plus validation,
   and evaluate it exactly once on the locked final test set. Final-test labels
   are unavailable to candidate workers and the leaderboard reconciler. A
   failed final result is a release no-go; it cannot trigger more tuning,
   runner-up selection, or a second evaluation.

### 2.4 Sampling disclosure

Every estimator must execute. When full-data execution is mathematically or
operationally infeasible, use a deterministic bounded sample and record:

- full dataset row count and byte size;
- sampled row count;
- selection method and seed;
- estimator-specific reason;
- applicable memory and runtime estimates;
- prepared sample artifact digest; and
- whether validation uses a full or bounded holdout.

The UI, API, leaderboard, model card, MLflow lineage, and audit evidence must all
identify sampled results. A sampled result must never be described as trained on
the full 10 GiB dataset.

Sampling never changes final-test rows, and candidates may not select a sample
using final-test metrics. Results from different training tiers appear in
separate leaderboard leagues unless an approved comparison policy states how
sampling uncertainty is handled.

### 2.5 Autoscaling timeline

- All 15 runs become `running` within 120 seconds.
- Event-driven workers and cloud nodes reach required capacity within ten
  minutes.
- The provisional profile gives candidate execution up to 105 minutes and
  reserves five minutes for scope-level champion selection, refit, final
  evaluation, CAS commit, and evidence finalization. Phase 0A must measure that
  finalization bound; if it exceeds five minutes, shorten the candidate window
  so scale-up + candidate work + finalization remains ≤7,200 seconds. Never hide
  final evaluation outside the qualification clock.
- Ray trials may wait internally for resource-class placement, but there is no
  run-level admission queue for the approved 15-run qualification group.
- Qualification begins with 25% warm candidate capacity and proves scale-up to
  the full calculated requirement.

`barrier_pending` means a scoped run and membership exist durably but no
capacity token is consumed and no execution command exists. `submitted` means
the API or barrier transaction consumed the admission token and published the
outbox command. `running` means the fenced Ray job is accepted and at least one
planned trial has entered Ray's running state. Planning alone does not satisfy
the 120-second gate. Measure both timelines from the unscoped submission event
or the scoped `scope_started_at`, never from a `barrier_pending` response.

### 2.6 Upload gate

- Use controlled, in-region clients.
- Sustain at least 500 Mbit/s aggregate direct-to-object-storage throughput.
- Complete all 150 GiB within 45 minutes.
- Restart one UI pod and one API pod during transfer.
- Interrupt the network for at least five clients.
- Completed parts must not be retransmitted after client, UI, API, or network
  recovery.
- WAN variability is outside the two-hour training clock but remains visible in
  upload evidence.

### 2.7 Availability, recovery, retention, and cost

| Objective or store | Required default | Qualification method |
| --- | --- | --- |
| Control-plane API availability | 99.9% monthly | Monthly SLI and error-budget calculation; the soak is supporting evidence only |
| Control API latency | p95 below 500 ms, excluding provider object-transfer time | Authenticated load test and production SLI |
| Regional availability | Three-zone placement where supported | Zone-loss drill against the approved serving error-rate SLO |
| Regional application service recovery | RTO ≤4 hours | Recreate the Kubernetes platform and restore authenticated API/inference service from signed infrastructure and durable stores |
| PostgreSQL workflow and MLflow metadata | RPO ≤15 minutes; RTO ≤4 hours | Timestamped PITR/failover drill plus workflow reconciliation |
| Acknowledged immutable raw, prepared, split, feature, checkpoint, model, and evidence objects | No acknowledged-object loss; access restored within 4 hours | Inventory-digest comparison after provider restore or replica failover |
| Audit records and deletion tombstones | No acknowledged-record loss; access restored within 4 hours | Immutable-sink receipt comparison and tombstone replay before restored data is exposed |
| Release final-allocation authority and signed receipts | No acknowledged state/event loss; RTO ≤4 hours; qualification fails closed while unavailable | Restore plus unique-allocation/open/commit replay and a three-provider race test before reopening access |
| Ephemeral Ray jobs, clusters, ObjectRefs, and local spill | No durability objective; recreate the fenced workflow within its approved deadline | Delete the Ray workload, restore from PostgreSQL and remote objects, and reject stale writers |
| Searchable application and cluster logs | Ingestion loss ≤5 minutes before destination acknowledgement; query service restored within 4 hours; retain 30 days | Collector outage/replay drill plus retention queries at boundary timestamps |
| Security and governance audit records | 365 days | Retention and legal-hold tests |

`RayCluster` loss and Kubernetes-cluster loss are different failures. The
application reconciler replaces an ephemeral `RayJob`/`RayCluster` from durable
state. Platform automation and the on-call owner recreate a regional Kubernetes
cluster from signed OpenTofu, platform BOM, identity, and backup state; only
after platform recovery may the application reconciler resubmit unfinished
work. Phase 7 measures and gates each path separately.

No qualification environment may be provisioned until the budget owner approves
a cost ceiling. Phase 0A records expected and worst-case compute, storage,
object-request, network, logging, and retained-evidence cost. Every qualification
run records estimated and actual cost. Cost may guide engineering choices but
may not silently weaken an approved performance contract.

## 3. Baseline that Phase 0 must regenerate

The worktree changed while the initial audit was performed. Treat all existing
figures as provisional and regenerate them from the frozen Phase 0 commit.

| Area | Provisional observed state | Production gap |
| --- | --- | --- |
| Verification | 259 backend tests passed, 2 skipped; 40 UI tests passed; lint, build, and Helm render passed | Counts are provisional until Phase 0 freezes the dirty worktree; no real authenticated browser flow, cloud integration, load, chaos, upgrade, or restore qualification exists |
| Uploads | A resumable multipart first pass exists | Production proxy still targets bundled SeaweedFS; recorded hash is not a verified byte SHA-256; completion is not transactionally reconciled; cleanup is not durable |
| Object storage | Configuration advertises MinIO, S3, GCS, and Azure | Only MinIO is implemented; unsupported choices may fall back to local storage; training manifests hardcode MinIO-style credentials |
| Training | One run Job processes at most 20 candidates, mostly sequentially; candidate selection currently uses test-set metrics | The implementation needs Ray trial scheduling, a locked final-test evaluator, and explicit per-estimator execution strategies |
| Catalog snapshot | Approximately 39 classification, 52 regression, 52 time-series, and 11 clustering entries | Snapshot must be generated from the pinned runtime, reviewed, signed, and tested |
| Scaling | API HPA only; existing Ray support is a Joblib backend, one serial actor, and static head/worker Deployments | No Ray Data, Ray Tune, Ray Train, KubeRay custom resources, qualified Ray autoscaling, or node autoscaling exists |
| Kubernetes | Provider-neutral Helm foundation | Bundled stateful services, permissive schema, incomplete pod security/network policy, legacy ingress, telemetry, and recovery gaps |
| Cloud delivery | No executable EKS/GKE/AKS IaC | Identity, networks, registries, databases, storage, gateways, observability, CI identities, and qualification environments are missing |
| Release | Useful CI and digest-first build foundations | Version drift, mutable Actions/base tags, unlocked Python dependencies, a root NVIDIA image, missing signature enforcement, and no staged immutable promotion |

## 4. Target architecture and required interfaces

### 4.1 End-to-end flow

```text
Browser ─ upload-control API
       └─ short-lived provider upload instructions
            └─ S3 / GCS / Azure Blob / local S3-compatible storage
                 └─ upload reconciler verifies SHA-256 and registers dataset
                      └─ PostgreSQL desired state + outbox
                           ├─ [1] privileged splitter submission
                           │    └─ fenced splitter RayJob / ephemeral RayCluster
                           │         └─ role-isolated objects + split digests
                           ├─ [2] preparation submission after [1]
                           │    └─ fenced preparation RayJob / ephemeral RayCluster
                           │         └─ Ray Data + Polars artifacts/profile
                           ├─ [3] training submission after [2]
                           │    └─ fenced run RayJob / ephemeral RayCluster
                           │         └─ Ray Tune joint feature/model trials
                           │              ├─ ordinary sklearn worker
                           │              ├─ incremental state owner
                           │              └─ Ray Train distributed recipe
                           │                   └─ validation evidence + candidate artifact
                           ├─ [4] refit submission after all scope runs
                           │    └─ global champion selected from validation
                           │         └─ fenced refit RayJob / ephemeral RayCluster
                           │              └─ CAS-frozen pipeline artifact
                           └─ [5] evaluation submission after [4]
                                └─ fenced evaluator RayJob / ephemeral RayCluster
                                     └─ one CAS-committed final result
```

PostgreSQL stores logical runs, attempts, recipes, fences, events, and terminal
state. Object storage holds immutable datasets, splits, feature artifacts,
checkpoints, models, and evidence. Ray executes disposable work and must not be
the only copy of state required for recovery. Splitter, preparation, training,
refit, and evaluator submissions use distinct project-and-stage base identities
plus attempt-scoped delegated credentials. Only the splitter can read the raw
object, and only the final evaluator can read the two locked-test prefixes.

### 4.2 Object-store driver boundary

Implement a capability-aware `ObjectStoreDriver` protocol. Unknown drivers fail
application startup. Cloud configurations must never fall back to local disk.
Do not force offset-based or block-based providers into an S3 part-list model.

```python
@dataclass(frozen=True)
class UploadCapabilities:
    protocol: Literal["multipart", "resumable_offset", "block_list", "single_put"]
    parallel_chunks_per_object: int
    can_list_committed_units: bool
    supports_unit_checksum: bool
    supports_final_checksum: bool
    supports_provider_lifecycle: bool


class UploadDriver(Protocol):
    def capabilities(self) -> UploadCapabilities: ...
    def begin_upload(self, request: BeginUpload) -> ProviderUpload: ...
    def create_transfer_instruction(
        self, upload: ProviderUpload, cursor: TransferCursor
    ) -> UploadInstruction: ...
    def query_progress(self, upload: ProviderUpload) -> UploadProgress: ...
    def complete_upload(
        self, upload: ProviderUpload, receipts: list[TransferReceipt]
    ) -> CompletedObject: ...
    def abort_upload(self, upload: ProviderUpload) -> None: ...
```

Keep object access and Ray source resolution in the same provider driver:

```python
class ObjectStoreDriver(UploadDriver, Protocol):
    def stat(self, uri: str) -> ObjectMetadata: ...
    def open_stream(self, uri: str, byte_range: ByteRange | None = None): ...
    def put_stream(self, uri: str, source: BinaryIO) -> ObjectMetadata: ...
    def put_bytes(self, uri: str, value: bytes) -> ObjectMetadata: ...
    def read_bytes(self, uri: str) -> bytes: ...
    def read_head(self, uri: str, byte_count: int) -> bytes: ...
    def exists(self, uri: str) -> bool: ...
    def size(self, uri: str) -> int: ...
    def dataframe_source(self, uri: str) -> RayDataSourceDescriptor: ...
    def healthcheck(self) -> HealthcheckResult: ...
    def delete(self, uri: str) -> None: ...
```

Preserve a compatibility facade for existing consumers while migrating them to
the capability-aware protocol. `RayDataSourceDescriptor` contains provider
filesystem options and object paths but never static credentials. Workers obtain
short-lived access through workload identity.

Migrate persisted storage identities before renaming drivers. Map legacy enum
values and `minio://` URIs explicitly, backfill a versioned read path, and fail
startup on an unknown driver. Never route an unknown production driver to
embedded storage.

Store digest algorithm, digest scope, verification status, and verification time
with every content hash. A multipart-manifest digest is not a byte digest. Keep
legacy values labeled accurately, then rescan, backfill, or quarantine them
under a reviewed migration.

Required drivers:

- `embedded`: development-only, single-process storage. Reject it at startup
  when distributed workers are enabled.
- `s3_compatible`: SeaweedFS, MinIO, and Railway Buckets.
- `aws_s3`: AWS SDK default credential chain and EKS Pod Identity.
- `gcs`: native Google Cloud Storage resumable sessions and Workload Identity
  Federation.
- `azure_blob`: native block upload, user-delegation SAS, and Azure Workload
  Identity.

Use native protocols rather than pretending GCS or Azure Blob is S3. Use
supported provider SDK methods rather than private MinIO methods. Relevant
provider contracts are [AWS multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html),
[GCS resumable upload](https://cloud.google.com/storage/docs/resumable-uploads),
and [Azure Put Block](https://learn.microsoft.com/rest/api/storageservices/put-block).

Protocol defaults:

| Driver | Transfer/resume contract | Per-object concurrency | Credential/session rule |
| --- | --- | ---: | --- |
| `aws_s3` and qualified `s3_compatible` | 128 MiB numbered multipart units; persist provider tags and checksums; list/reconcile parts; complete ordered manifest | 4 | Part instruction expires in 15 minutes; application upload expires in 24 hours |
| `gcs` | Native resumable session; upload sequential 128 MiB chunks and persist/query the provider-confirmed byte offset | 1 | Treat the session URI as a bearer secret; application expires/cancels it at 24 hours even though GCS may retain the URI for up to one week |
| `azure_blob` | 128 MiB blocks with deterministic block IDs; persist/query committed and uncommitted block state; commit block list | 4 | User-delegation SAS expires in 15 minutes; application upload expires in 24 hours |
| `embedded` | Single bounded stream for development only | 1 | No signed browser instruction; reject in distributed mode |

Capability details:

| Driver | Intermediate integrity | Final integrity | Provider lifecycle/versioning |
| --- | --- | --- | --- |
| AWS S3 | Provider-supported per-part checksum and recorded receipt | Provider full-object checksum plus Sceptre byte SHA-256 | Required in production |
| GCS resumable | No independent intermediate-chunk integrity result; rely on TLS and confirmed offset | Provider final checksum plus Sceptre byte SHA-256 | Required in production |
| Azure Blob | Per-block receipt where exposed; do not treat block ID as a checksum | Provider object properties plus Sceptre byte SHA-256 | Required in production |
| S3-compatible | Discover and contract-test; do not assume AWS checksum extensions | Sceptre byte SHA-256 is mandatory | Capability-specific |
| Railway Bucket | Contract-test basic S3-compatible behavior | Sceptre byte SHA-256 is mandatory | Unsupported for the production lifecycle/versioning contract |

Common defaults:

- Five retries with exponential backoff and full jitter, honoring
  provider `Retry-After` guidance.
- Reconcile every 15 minutes. Configure provider lifecycle cleanup only when the
  capability matrix says it is supported.
- Compute incremental client SHA-256 in a Web Worker and recompute byte SHA-256
  during preparation.
- Preserve provider-specific progress: part receipts for S3, confirmed offset
  for GCS, and block IDs/status for Azure.
- Keep the qualification target at 500 Mbit/s aggregate payload goodput. Do not
  assume four parallel transfers per object on GCS; concurrency comes from the
  15 independent objects.

Production bytes go directly from the browser to cloud storage.
`/object-storage/` is a local-only SeaweedFS compatibility proxy. Redact query
strings and signed headers at browser telemetry, UI Nginx, Gateway, WAF, load
balancer, tracing, and central logging layers.

Upload state machine:

```text
initiated → uploading → object_completed → verifying → preparing → ready
                 ├─→ aborting → aborted
                 ├─→ expired
                 └─→ failed → retryable|terminal
```

External completion and SQL state changes use desired-state and outbox records.
Repeating create, transfer-instruction, progress query, complete, or abort
requests returns the original logical result. The normalized API exposes
protocol, next required cursor, receipts, and expiry; it does not invent a part
list for an offset-based session.

### 4.3 Data, split, and sampling contract

Each dataset version receives an immutable prepared-data manifest containing:

- raw object URI and verified SHA-256;
- parsed schema, logical types, null statistics, cardinalities, and row count;
- stable row identities derived independently of Ray block order;
- partitioned compressed Parquet snapshot;
- task-specific deterministic train, validation, and locked-test splits with a
  separate row-set digest for each role;
- sample artifacts and row-selection evidence; and
- dataset, code, image, dependency-lock, catalog, and random-seed lineage.

Phase 3 materializes provider-neutral, immutable sample tiers before selecting a
provider capacity profile. At minimum, create task-correct tiers at 1,000,
10,000, 250,000, and 1,000,000 rows where the dataset is large enough, plus a
full-data manifest. Phase 4 selects the largest existing tier allowed by the
signed capacity-profile revision; it never regenerates an estimator-specific or
provider-specific sample.

Build tiers as deterministic nested row sets where the task policy permits. Two
candidates using the same tier receive identical bytes and row digests. Results
from different tiers remain in separate leaderboard leagues unless an approved
uncertainty-aware policy permits comparison.

Default sample policies:

| Estimator class | Default data policy |
| --- | --- |
| Incremental/streaming | All partitions in deterministic order with checkpoints |
| Ordinary sparse/general | Select from full, 1,000,000, 250,000, or 10,000-row deterministic tiers |
| Dense high-memory | Select from full, 250,000, or 10,000-row deterministic tiers |
| Pairwise/quadratic | Select the 10,000-row or 1,000-row immutable tier according to the measured memory bound |
| GPU-supported | Select full data if its qualified bound fits 105 minutes; otherwise select the largest immutable tier that fits |
| Time-series | Select from prebuilt contiguous ordered windows preserving order and seasonality; never random row sampling |

The planner selects the largest sample satisfying both:

```text
qualified_peak_memory_bound <= 0.60 × worker_memory_limit
qualified_complete_candidate_runtime_bound <= 105 minutes
```

Use target-stratified selection for classification, target-quantile strata for
regression, deterministic reservoir selection with rare-category preservation
for clustering, and ordered windows for time-series. Derive the seed from:

```text
SHA256(dataset_hash + split_revision + sample_policy_revision + sample_tier)
```

Estimator name, catalog revision, provider, worker type, and trial identifier
must not change sample rows. Every comparable candidate uses the same validation
digest. Create one `evaluation_scope` for all comparable runs sharing dataset,
target, split, and experiment-group or qualification-campaign identity. Select
one global champion from validation evidence before opening the final test. If
validation must be bounded, disclose it once at scope level and never choose it
per estimator. Include the sample-tier digest and signed capacity-profile
revision in candidate lineage.

The objective revision freezes a champion-refit data policy. Persist the exact
training and validation tier plus row digests used by refit, including bounded
validation. A sampled winner refits on the same approved tier by default; moving
to a larger or full-data tier is allowed only when that transition, comparison
rule, resource bound, and resulting row digests were preregistered and
capacity-qualified before search.

A short-lived privileged splitter attempt reads the raw object, creates the
approved split, then writes train and validation artifacts, final-test inputs,
and final-test labels to separate prefixes. Feature, candidate, Tune, Train, and
leaderboard identities lose raw-object access and cannot read either final-test
prefix.

After every training run in the evaluation scope is terminal, submit a separate
fenced, retryable `champion_refit_attempt`. Its refit-only identity reads the
approved train/validation tiers and candidate manifest, cannot read raw or final
prefixes, executes the catalog backend's sklearn/incremental/Ray Train refit,
and CAS-registers one immutable frozen-pipeline digest. Only then may a separate
fenced `champion_evaluation_attempt` run under a dedicated evaluator identity.

The evaluator receives time-bounded access to the frozen pipeline and final-test
prefixes, commits one result with a compare-and-set on the sealed scope, and
then loses access. Before issuing credentials, atomically consume the scope's
final-test allocation and record `test_opened_at`. Submission failure before
that CAS may create a new fenced evaluator attempt. After the CAS, never issue a
second test credential: if the signed immutable result object exists,
reconciliation may register that same digest without rereading data; otherwise
any evaluator or metric failure seals the scope as failed. It never permits
runner-up selection, more tuning, or another evaluation. A later experiment
requires a new preregistered split and evaluation scope.

Cross-provider release qualification adds one authoritative
`ReleaseFinalTestAuthority` outside the three provider-local application
databases. Its strongly consistent store has a unique row per
`(fixture_revision, split_revision)` and an append-only signed state sequence:
`unallocated → assigned(provider, scope) → opened(evaluator_attempt) →
committed(result_digest)|failed`. Before any campaign result exists, it binds
the canonical provider/scope and creates signed provider-distribution manifests.
Non-canonical manifests contain train/validation roles and opaque final-role
digests only; final input/label objects and their decrypt permission exist only
in the canonical provider's locked prefixes. A provider-local final allocation
references the central allocation ID but cannot mint evaluator access. The
central authority first CAS-opens the allocation, then authorizes the canonical
`AttemptCredentialBroker` to mint exactly one evaluator grant and finally
CAS-commits the signed result digest. Every transition and failed mint is
evidence. Loss of the authority fails qualification closed.

Use a signed `validation_only` dataset-import manifest to register the
train/validation-only copies on non-canonical providers. Such a dataset cannot
join a promotional scope or resolve a final prefix. Each provider still tests
its splitter and evaluator templates on separate synthetic, non-promotional
conformance fixtures; those fixtures never contain the locked release-test
roles.

Ray Data block ordering and worker count must not define dataset identity. Use
hash-based row assignment, canonical partition naming, and exact or
tolerance-defined aggregation rules. Record logical row/schema digests
separately from physical object digests because distributed writers may produce
different byte layouts for the same logical dataset.

Derive each row identity from the immutable source-object digest plus its
canonical logical record position: CSV/JSONL byte offset or record ordinal, or
Parquet file, row-group, and row ordinal. Include a duplicate-occurrence index
and fail on identity collision. Never use a row-value content hash alone because
duplicate rows are valid.

The privileged splitter also derives a canonical content fingerprint that does
not replace positional identity. For tasks without an explicit repeatable-event
key, assign every exact-duplicate fingerprint group to one split role or reject
the split; never scatter it across train, validation, and final roles. For
entity/time tasks, the declared group/time boundary remains primary and the
contract must explicitly justify repeated identical events. Record duplicate
group counts and prove zero unauthorized cross-role fingerprint overlap without
exposing final-role values downstream.

The splitter must also enforce the feature contract's outer-role isolation
before registering any role artifact. For a group-disjoint contract, require
`groups(train) ∩ groups(validation ∪ final) = ∅` and
`groups(validation) ∩ groups(final) = ∅`. For a temporal or panel
contract, require monotonically ordered role intervals and prove that the
configured label horizon, lookback window, purge duration, and embargo duration
do not cross the train→validation or validation→final boundary. A panel may
reuse an entity across time only when that temporal rule is explicit; otherwise
the group-disjoint predicate applies. Store counts, boundary timestamps, and
opaque group-set digests as evidence and fail the split before downstream
access when any predicate is false.

Transform bounded Arrow batches with Polars. Prohibit full 10 GiB conversion to
pandas, full collection on the Ray driver, and any equivalent materialization.
Group, lag, and rolling features require explicit entity partitioning, ordering,
boundary halos, and deterministic merge rules; arbitrary batch-local windows do
not satisfy the feature contract.

Freeze Arrow-to-Polars type mappings and null, NaN, infinity, divide-by-zero,
overflow, categorical, timezone, and daylight-saving behavior. Distributed
group/window tests must prove `output_row_ids == input_row_ids`, no duplicate or
dropped rows, deterministic overlap trimming, and fold-scoped reduce/shuffle
state.

### 4.4 Estimator catalog

Generate a signed, versioned manifest from the pinned runtime:

```json
{
  "catalog_revision": "sha256:...",
  "runtime_lock_digest": "sha256:...",
  "task_type": "classification",
  "estimators": [
    {
      "name": "RandomForestClassifier",
      "source": "sklearn",
      "status": "supported",
      "constructor_recipe": "default-v1",
      "resource_class": "cpu-general",
      "execution_backend": "single_process_sklearn",
      "search_policy": "ray_tune_skopt",
      "distributed_capable": false,
      "full_data_capable": false,
      "incremental_capable": false,
      "sampling_policy": "general",
      "serialization_supported": true,
      "inference_supported": true
    }
  ]
}
```

Catalog rules:

- Discover all concrete task-compatible scikit-learn estimators from the pinned
  version.
- Add explicitly supported XGBoost, LightGBM, CatBoost, Keras/SciKeras, and
  other registered integrations only when their complete dependency, import,
  and runtime path is Dask-free.
- Mark TPOT `not_applicable` for `0.2.0`: the supported TPOT line introduces a
  Dask dependency/runtime and therefore violates ADR-001. Do not install,
  import, or execute it. Reconsidering TPOT requires a future reviewed ADR and
  cannot change this release's no-Dask contract.
- Add reviewed constructor recipes for meta/composition estimators requiring
  base estimators. Never accept arbitrary Python objects from an API client.
- Assign one execution backend to every supported entry:
  `single_process_sklearn`, `incremental_sklearn`, `ray_train_xgboost`,
  `ray_train_lightgbm`, `ray_train_torch`, or another reviewed
  distributed-native recipe.
- Record recipe preconditions for dense/sparse representation, input/target
  dtype and domain, missing-value support, minimum rows/classes, and measured
  materialization bound. Reject an inapplicable recipe with a catalog reason
  before allocating a trial.
- Recursively set every nested estimator's `n_jobs=1` when Ray owns concurrency;
  cap BLAS/OpenMP threads with the pinned runtime/threadpool policy, prohibit
  child process pools unless a recipe explicitly owns them, and recycle the
  isolated process after every ordinary-sklearn trial to reclaim native memory.
- Treat ordinary sklearn as candidate-level parallelism. Ray and Polars do not
  make an arbitrary estimator sharded or incremental.
- Define true forecasting contracts before marking a time-series entry
  supported: horizon, cadence, entity/panel key, gap, rolling backtest, known
  future covariates, and supported missing-period behavior.
- Keep reviewed `not_applicable` records for abstract, removed, test-only, or
  logically non-applicable classes.
- Fail CI when a dependency update introduces, removes, or reclassifies an
  estimator without a reviewed manifest diff.
- Persist the catalog revision on every run.
- Expand `catalog_mode=all` on the server; clients do not submit a mutable list.
- Set `catalog_complete=false` when any applicable candidate fails.

### 4.5 Candidate execution model

Replace the monolithic candidate loop with one durable control plane and one Ray
execution plane:

- Create one durable `model_run` row per user run. Use sibling branches so
  immutable logical trials survive execution replacement:
  `model_run → run_attempts` and
  `model_run → candidates → trials → trial_attempts → checkpoints`. Each
  `trial_attempt` references the `run_attempt` that executed it.
- Commit desired state and an outbox event before submitting work to Ray.
- Let a restricted reconciler create, observe, cancel, and replace one KubeRay
  `RayJob` custom resource per `run_attempt`. Each `RayJob` embeds its ephemeral
  `rayClusterSpec`; forbid `clusterSelector`. Pin `K8sJobMode`, derive the Ray
  submission ID from the fenced attempt, set `backoffLimit: 0`, and set
  `shutdownAfterJobFinishes: false` so KubeRay cannot create an untracked retry
  or delete evidence before durable reconciliation. After terminal CAS and the
  evidence/cleanup grace period, the reconciler deletes the owning `RayJob` and
  its cluster. Sceptre creates only the Kubernetes CR; KubeRay's internal
  submitter owns the Ray job submission. API clients may not access Ray Jobs,
  Dashboard, Client, or GCS endpoints.
- Map each `run_attempt` and run-level fence to exactly one `RayJob`, ephemeral
  `RayCluster`, and Tune experiment. Map its many Tune trials to durable `trial`
  rows. Map each
  `trial_attempt` to at most one Ray Train run when its catalog backend requires
  distributed training.
- Maintain separate monotonic run and trial generations. Run-level writes check
  `run_generation`; every trial-side heartbeat, checkpoint, metric, artifact,
  event, and terminal write checks the composite
  `(run_generation, trial_generation)`. Apply every terminal transition with a
  database compare-and-set so an expired writer cannot overwrite its successor.
- Use an independently scheduled heartbeat. Set the lease to more than three
  heartbeat intervals and qualify it under long blocking fits and database
  failover before freezing the values.
- Apply one absolute candidate deadline across queueing, all retries, restore,
  evaluation, serialization, and artifact upload.
- Give retry ownership to the durable reconciler. Configure `backoffLimit: 0`,
  Tune/Train trial failure retries to zero, and side-effecting Ray tasks/actors
  with `max_retries=0`/`max_restarts=0`. A failed logical trial, not its parent
  candidate, creates a new fenced `trial_attempt` and consumes that trial's
  aggregate execution-attempt budget.
- On driver, head, RayJob, or cluster loss, use one database transaction to
  supersede the old `run_attempt`, advance/supersede every active
  `trial_attempt`, create the replacement `run_attempt`, and create one new
  counted `trial_attempt` under it for every unfinished logical trial. Restore
  only from each trial's last CAS-registered checkpoint. Pure tasks with no
  external writes may use an explicitly enumerated Ray-Core retry policy, but
  every restart is recorded and cannot reset either budget.
- Treat append-only PostgreSQL suggestion and result events as canonical search
  state. Persist each suggestion ID, typed parameters, seed, pending-set
  membership, concurrency batch, and sequence before launch. A retry reuses the
  same suggestion and seed. Persist each terminal result with an idempotency key
  and result digest. Freeze either sequential suggestions or a deterministic
  batch/buffered completion order; worker completion timing may not reorder
  `tell`. The objective revision defines treatment of failed, cancelled, and
  partial trials. A Searcher snapshot records the last applied event sequence;
  restore the snapshot, replay later events in order, and make `tell`
  exactly-once by suggestion/result digest.
- Publish checkpoints and artifacts to immutable attempt-scoped object keys,
  then compare-and-swap the database manifest under the active fence. A stale
  worker may upload an unreferenced object but cannot register it; reconciliation
  deletes such objects after the grace period.
- Run fixed and ordinary sklearn recipes in one bounded trial worker. Keep
  incremental estimator state in one checkpointed owner consuming ordered Ray
  Data batches.
- Use Ray Train only for reviewed distributed-native recipes and explicit
  CPU/GPU worker topology.
- Use three reconciler replicas for deadlines, cancellation, aggregation,
  orphan detection, and resubmission after Ray loss.
- Keep `evaluation_scope` and `champion_evaluation_attempt` outside the training
  attempt hierarchy. Add `champion_refit_attempts` as a retryable scope-level
  branch. The evaluator starts only after every scope member is terminal and the
  refit artifact is frozen; dedicated fences and one-shot compare-and-set
  prevent a run/refit retry from reopening test access.
- Persist `scope_deadline_at`, a final-evidence reserve, and separate monotonic
  refit/evaluator generations. The provisional profile permits at most two
  `champion_refit_attempts` and two evaluator submissions before
  `test_opened_at`; there is no post-open evaluator retry. Before creating a
  scope-stage `RayJob`, set its kill boundary to the lesser of the qualified
  stage bound and the time remaining before `scope_deadline_at` minus the
  evidence reserve. Never start an attempt that cannot finish inside that
  bound. Phase 0A may lower these ceilings, but may not leave them undefined.
- Define typed `provider_stage_conformance_attempts` for `refit` and
  `evaluation`. They use the same fenced KubeRay templates, backends,
  serialization, delegated-credential, retry, cleanup, and autoscaling paths as
  promotional scope stages, but consume a signed synthetic/non-promotional
  holdout and can never reference `ReleaseFinalTestAuthority`. Their bundles and
  results are quarantined conformance evidence, not promotable model artifacts.

The provisional production topology under ADR-005 uses one ephemeral Ray cluster per
`run_attempt`. The cluster uses the owning project's training-stage Kubernetes
service account, cloud workload identity, object prefix, network policy, spill
prefix, resource quota, and encryption context. The
reconciler deletes it only after durable
terminal reconciliation and the configured evidence/cleanup grace period.

Per-run clusters make Kubernetes quota, pod priority, object-store limits, and
ephemeral-storage limits enforceable across runs. The durable admission
controller uses weighted fair queuing and per-project/resource-class caps before
creating each `RayJob`. A shared-cluster mode requires a separate threat,
credential-isolation, object-memory, fairness, and cost ADR and cannot claim the
qualification in this guide.

Ray owns Ray-worker scaling. The Kubernetes node autoscaler provisions nodes for
pending Ray worker pods. Do not use KEDA, HPA, a custom controller, or static
Deployments to set Ray worker counts. This prevents two control loops from
scaling the same capacity.

Treat the Ray head, Global Control Service (GCS), jobs, ObjectRefs, and local
spill as disposable. Persist manifests and Tune/Train checkpoints to remote
object storage, then create a new fenced attempt and restore it after head or
cluster loss. Follow the documented [KubeRay architecture](https://docs.ray.io/en/latest/cluster/kubernetes/index.html),
[Ray autoscaling](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html),
and [Tune fault-tolerance](https://docs.ray.io/en/latest/tune/tutorials/tune-fault-tolerance.html)
contracts. Freeze and test the official [KubeRay RayJob](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html)
and [Ray TLS](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/tls.html)
fields against the pinned operator/runtime versions.

### 4.6 Automated feature-engineering contract

Automated feature engineering is a first-class product stage, not estimator
preprocessing hidden inside a candidate. Each dataset and task configuration
creates a versioned feature contract before generating a feature.

The feature contract records:

- source columns, semantic roles, entities, event/effective time,
  transaction/recorded time, source-availability time, immutable source-snapshot
  digest and `as_of` time, decision/cutoff time, prediction time, target,
  protected fields, identifiers, and known-future fields;
- event-window start/end and boundary-tie rules, label horizon and availability
  time, known-future semantics, and late-arrival/correction/backfill policy;
- separate availability invariants for observed and known-future values. An
  observed feature requires `source_available_at <= decision_cutoff_at`,
  `event_time <= window_end <= decision_cutoff_at`, and
  `feature_available_time <= decision_cutoff_at`. A declared known-future
  covariate requires publication/source availability by the cutoff but may have
  an effective/event time after cutoff and no later than the forecast horizon;
  target-derived future values are always forbidden. If prediction time equals
  cutoff, record that equality rather than substituting one timestamp silently;
- split revision, grouping and temporal-gap rules, and final-test isolation;
- allowed feature families, maximum expression depth, output count, cardinality,
  memory, runtime, and materialization budgets; and
- missing/unknown input behavior, serving-time availability, and one execution
  class per feature: `request_local`, `online_stateful`, or `batch_only`.

The initial reviewed feature-family allowlist may include numeric unary and
bounded pairwise arithmetic, datetime decomposition, categorical frequency,
text length/pattern statistics, and explicitly keyed group aggregates. Lag,
rolling, and window features remain disabled unless entity ordering, cutoff,
partition-boundary, and serving semantics are complete. Arbitrary Python,
client-supplied expressions, SQL, or imports are prohibited.

Every historical lookup uses an immutable source snapshot and an as-of join on
entity plus decision cutoff. A row recorded after the cutoff remains unavailable
even when its event time is earlier. Backfills and corrections create a new
source-snapshot revision; they never rewrite evidence for an earlier experiment.
Labels whose horizon is incomplete at the cutoff are excluded from training.

Apply safety and quality stages in this order:

1. Validate declared roles, source availability, cutoff/as-of rules, formula
   safety, output type, and identifier policy without inspecting validation or
   final-test values.
2. Fit every value-dependent rule on the outer training role and refit it inside
   each inner training fold. This includes constant/quasi-constant,
   missingness, cardinality, duplicate, frequency, group aggregate, correlation,
   redundancy, mutual-information, encoding, imputation, scaling,
   decomposition, and supervised-selection state.
3. Build reusable safe-feature artifacts only for stateless,
   target-independent, cutoff-safe expressions. Store fitted or aggregate
   outputs per fold/trial; never build a reusable superset from validation or
   final-test distributions.
4. Search feature-family switches, budgets, and model parameters with a custom
   scikit-optimize adapter implementing Ray Tune's `Searcher` lifecycle.
5. Penalize excessive feature count, compute cost, and fold instability in the
   objective. Do not weaken safety gates to meet a minimum feature count.
6. Confirm the selected recipe with family ablation and an engineered-versus-raw
   baseline on validation data.
7. Select the global champion for the evaluation scope, submit the durable
   scope-level refit on the preregistered train/validation tier, freeze its
   complete pipeline by CAS, then invoke the isolated final-test evaluator once.
   A failed final test is a no-go and cannot trigger runner-up selection or
   additional tuning.

Store one immutable base dataset and, where justified, one reusable safe-feature
superset. Trials store metadata-only feature views and formula hashes instead of
full dataset copies. Persist selected and rejected definitions, reasons,
dependency lineage, fold evidence, compute cost, and stability in a versioned
feature registry.

The released model bundle contains the signed feature contract, recipe revision,
compiled Polars logic, formula hashes, fitted vocabularies/statistics/matrices,
aggregate or online-state snapshot revision, final train-plus-validation refit
state, refit tier and train/validation row digests, parity-fixture/policy
revisions, runtime lock, input schema, and model artifact. Offline and serving
transformations must pass parity tests before promotion. An
`online_stateful` feature requires a qualified point-in-time lookup source,
freshness/TTL rule, fallback, and outage policy. A model containing `batch_only`
features may serve batch predictions only and cannot receive an online endpoint.

Freeze a versioned parity corpus and per-output comparison policy. It asserts
ordered feature names/types, exact categorical and row-identity behavior,
exact-or-tolerant numeric features and predictions, and golden cases for null,
NaN, infinity, unknown categories, timezone/DST, as-of boundaries,
online-state freshness/fallback, and rejection of `batch_only` online serving.

### 4.7 Capacity calculation

Use the aggregate formula below as a lower-bound screening calculation, not as
proof that the workload meets the deadline. Benchmark every catalog candidate
on the reference fixture and persist all
observations plus a reviewed qualified upper bound for runtime, peak memory,
CPU, GPU, ephemeral storage, object reads, and output size by normalized
resource class. The complete-candidate runtime includes its allocated reusable
feature-preparation cost, joint model-plus-feature search, approved suggestions
and inner folds, validation, serialization, artifact upload, and retry/recovery
allowance. Capacity and deadline models separately add one global-champion refit
and isolated final evaluation per sealed evaluation scope.
P50 and p95
remain planning telemetry; they are not the bound used to promise completion.

Phases 10–12 calibrate the normalized profile against each provider's node
types, networking, storage, and autoscaler, then freeze a signed provider
capacity-profile revision before Phase 13.

For resource class `c`:

```text
candidate_window_seconds = 6,300

required_slots[c] =
  ceil(
    1.20
    × 15
    × max_over_task_types(
        sum(qualified_runtime_bound_seconds[e, c] for e in catalog[task][c])
      )
    / candidate_window_seconds
  )

required_nodes[c] = max(
  nodes_for_cpu,
  nodes_for_memory,
  nodes_for_ephemeral_storage,
  nodes_for_pod_slots,
  nodes_for_network_addresses,
  nodes_for_gpu_if_worker_gpu_is_nonzero,
  minimum_nodes_for_required_zone_spread
)
```

For each scalar resource, compute nodes as
`ceil(required_slots × worker_request / (node_allocatable × 0.75))`. Do not
evaluate a GPU division for a CPU-only class. Apply Kubernetes and provider pod
density, IP/ENI, local ephemeral storage, volume attachment, topology, taint,
architecture, and scarce-instance constraints after the scalar calculation.

Capacity rules:

- Phase 0A must run a discrete scheduling simulation and a representative
  KubeRay benchmark covering long-tail durations, precedence, retries, Ray head
  and driver capacity, placement groups, multi-resource bin-packing, object
  bandwidth, autoscaler delay, and distributed-training gang placement.
- Provider node-pool maxima equal or exceed `required_nodes[c]`.
- Cloud quota exceeds the computed maximum by 20%.
- The qualified production profile keeps 25% warm candidate capacity.
- Isolate system capacity from training capacity so candidate scaling cannot
  evict API, Gateway, DNS, telemetry, reconcilers, or database connectors.
- Record requested, allocatable, used, throttled, pending, and interrupted
  capacity in qualification evidence.
- A cheaper profile may be labeled `evaluation`; it must not claim production
  certification.
- A candidate's 105-minute budget is absolute across all attempts, queue waits
  after planning, checkpoint restore, and artifact finalization. The provisional
  profile sets `max_run_attempts=5` and
  `max_trial_execution_attempts_per_trial=5` across all run generations. A trial
  execution attempt evaluates its unchanged suggestion across the approved
  folds, so actual estimator-fit calls are at most trial execution attempts ×
  fold count. Add up to `max_champion_refit_attempts=2` logical refit fits and
  separately budget up to `max_preopen_evaluator_attempts=2` evaluator jobs per
  promotional scope. Run replacement never resets or multiplies any ceiling.
  The scope deadline and remaining-time rule may make the executable ceiling
  smaller. Phase 0A may lower these values before approval.
- A PostgreSQL capacity reservation is an admission token, not a reservation of
  real Kubernetes or cloud resources. Launch admission also checks fresh Ray
  demand, Kubernetes schedulability, node-pool/GPU availability, provider quota,
  scale-up limits, and partial-placement cleanup.

### 4.8 Public API additions

Preserve selected-model requests while adding explicit all-catalog semantics:

```python
class TrainingEstimateRequest(BaseModel):
    experiment_spec_revision: str | None = None
    evaluation_scope_id: UUID | None = None
    dataset_version_id: UUID | None = None
    task_type: TaskType | None = None
    target_column: str | None = None
    feature_contract_revision: str | None = None
    split_revision: str | None = None
    feature_search_space_revision: str | None = None
    search_objective_revision: str | None = None
    prefer_gpu: bool = True
    expected_minutes: int = Field(default=10, ge=1)
    candidate_limit: int | None = Field(default=5, ge=1)
    catalog_mode: Literal["selected", "all"] = "selected"
    catalog_revision: str | None = None
    candidate_models: list[str] = Field(default_factory=list)
    optimization_iterations: int = Field(default=5, ge=1, le=100)
    cv_folds: int = Field(default=3, ge=2, le=20)
    execution_mode_hint: Literal["auto", "in_memory", "incremental"] = "auto"
    deadline_seconds: int = Field(default=7200, ge=60, le=604800)
    reserve_capacity: bool = False
```

During the compatibility window, the schema also accepts optional
`positive_label`, `evaluation_column`, `entity_column`, `event_time_column`,
`prediction_time_column`, and `primary_metric` assertions. They never override
the immutable experiment spec.

Launch extends the estimate contract with typed, server-validated overrides:

```python
class TrainingLaunchRequest(TrainingEstimateRequest):
    run_name: str | None = Field(default=None, max_length=255)
    parameter_overrides: list[EstimatorParameterOverride] = Field(
        default_factory=list
    )
    estimate_digest: str | None = None
    capacity_reservation_id: UUID | None = None
    capacity_profile_revision: str | None = None
```

Behavior:

- `catalog_mode=selected` stays backward compatible and accepts reviewed
  catalog names. Retain `prefer_gpu`, `expected_minutes`, and the legacy
  `candidate_limit` field during the compatibility window. A legacy selected
  request may supply the old task/dataset fields; the server validates them and
  creates or resolves an immutable experiment spec before returning an estimate.
- Treat legacy `evaluation_column` as validation grouping metadata only. It may
  never identify or unlock the final test. New clients use the versioned split
  and feature contract fields, including entity and time semantics.
- Treat `execution_mode_hint` and the legacy `execution_mode` field as
  selected-mode compatibility hints only. The signed catalog owns the execution
  backend. Reject incompatible hints and reject any non-`auto` hint in all-catalog
  mode; clients cannot turn an ordinary estimator into incremental or
  distributed training.
- `catalog_mode=all` rejects a non-empty `candidate_models` list and resolves
  every supported estimator from the requested revision. Production all mode
  requires `experiment_spec_revision`; it never constructs one implicitly from
  mutable client fields.
- Resolve one immutable `experiment_spec_revision` server-side. It atomically
  binds dataset version, task, target/positive label, entity/time roles, split,
  feature contract, feature search space, search objective, primary metric, and
  catalog compatibility. Legacy duplicate request fields are compatibility
  assertions only: reject any mismatch rather than allowing them to override
  the spec. Persist the resolved lineage on the run.
- Validate estimator overrides against a server-owned discriminated schema with
  type, range, conditional, and compatibility checks. Reject unknown fields,
  Python objects, callables, code URIs, runtime environments, package lists, and
  arbitrary expressions.
- `candidate_limit` retains its selected-mode default for backward
  compatibility. It never caps all mode. If a client explicitly supplies it
  with all mode, return `422 invalid_catalog_selection`; an omitted inherited
  default is not applied to the expanded catalog.
- Omitting `catalog_revision` resolves and persists the active revision.
- A stale revision returns `409 catalog_revision_changed` and the current
  revision.
- Remove the arbitrary 20-model cap from schemas, backend logic, and UI.
- The estimate response includes candidate count, capacity by resource class,
  sampling summary, required node quotas, expected object reads, projected cost
  range, and environment qualification status.
- Production rejects an all-catalog launch when preflight cannot prove the
  deadline.
- The `0.2.0` production-qualification profile requires
  `deadline_seconds=7200`; the bounded field remains additive for evaluation and
  backward-compatible selected runs.
- After QLT-006 and QLT-007 are approved, production all-catalog estimates and
  launches resolve `optimization_iterations` and `cv_folds` from the signed
  qualification profile. Reject a client value that differs with
  `422 qualification_strength_required`; fixed recipes still execute one model
  configuration across the task-correct folds.
- In production, an all-catalog estimate with `reserve_capacity=true` creates a
  short-lived transactional reservation. Launch must present the reservation
  ID, catalog revision, capacity-profile revision, and estimate digest. Consuming
  or expiring the reservation is atomic, preventing estimate/launch TOCTOU.
- An evaluation scope binds one experiment spec, split, comparison/league
  policy, expected member count, and preregistered final-test threshold. Scope
  membership is reserved transactionally and sealed at the start barrier; no
  later run may join. A unique final-test allocation permits a split's locked
  test role to belong to at most one promotional scope. A launch without an
  explicit scope is validation-only and can never create a final-evaluation
  attempt; a one-run promotional experiment must explicitly create a scope with
  `expected_member_count=1`. A failed or incomplete barrier fails the scope
  before any final-test access.
- Model scope state with mode-specific branches. Promotional scopes use
  `draft → reserving → sealed → running → refitting → evaluating → passed|failed`;
  validation-only scopes use
  `draft → reserving → sealed → running → passed|failed` and cannot enter refit
  or evaluation. Both allow `expired|cancelled` before test access. Scope create
  requires `Idempotency-Key`, expected member count, mode
  (`promotional|validation_only`), membership deadline, comparison policy, and
  final-threshold revision where promotional. Each `(scope_id, run_id)`
  membership is unique.
- Use a two-phase launch protocol. Each scoped launch first validates and stores
  a `barrier_pending` run, its membership, request digest, and unconsumed
  capacity reservation; it emits no execution outbox event. In the transaction
  that adds the nth valid member, lock the scope and all reservations, verify
  the exact expected count and request/profile revisions, set one
  `scope_started_at`, derive `scope_deadline_at = scope_started_at + 7,200s`
  for the qualification profile, consume every reservation, change every member
  to `submitted`, and emit one outbox command per run. Earlier callers observe the
  transition through scope/run GET or event APIs. Timeout/cancellation before
  sealing atomically releases reservations and fails/cancels the pending runs.
  Only project compute may create validation-only scopes; promotional scope
  creation and cancellation additionally require the release-evaluation
  permission.
- The qualification harness requests 15 reservations behind one start barrier.
  Any rejected or expired reservation fails the qualification attempt; each
  member uses its own authenticated user and idempotency key.
- An unscoped production all-catalog launch with a valid reservation returns
  `202 admission_state=submitted`. A scoped launch returns
  `202 admission_state=barrier_pending` until the exact membership seals; it is
  not a capacity queue and starts no work or deadline. Neither path returns
  `waiting_capacity`. Reject an invalid request before creating a run rather
  than creating a queued run that violates the contract.

Add these endpoints under the existing `/api/v1` router:

```text
GET /api/v1/capabilities
POST /api/v1/projects/{project_id}/feature-contracts
GET /api/v1/projects/{project_id}/feature-contracts/{revision}
POST /api/v1/projects/{project_id}/feature-search-spaces
GET /api/v1/projects/{project_id}/feature-search-spaces/{revision}
POST /api/v1/projects/{project_id}/search-objectives
GET /api/v1/projects/{project_id}/search-objectives/{revision}
POST /api/v1/projects/{project_id}/experiment-specs
GET /api/v1/projects/{project_id}/experiment-specs/{revision}
POST /api/v1/projects/{project_id}/training/evaluation-scopes
GET /api/v1/projects/{project_id}/training/evaluation-scopes/{scope_id}
POST /api/v1/projects/{project_id}/training/evaluation-scopes/{scope_id}/members
POST /api/v1/projects/{project_id}/training/evaluation-scopes/{scope_id}/cancel
GET /api/v1/projects/{project_id}/feature-registry/{revision}
GET /api/v1/projects/{project_id}/feature-recipes/{revision}
GET /api/v1/projects/{project_id}/training/catalogs/{task_type}
GET /api/v1/projects/{project_id}/training/runs/{run_id}/candidates
GET /api/v1/projects/{project_id}/training/runs/{run_id}/candidates/{candidate_id}
GET /api/v1/projects/{project_id}/training/runs/{run_id}/trials
GET /api/v1/projects/{project_id}/training/runs/{run_id}/events
```

`GET /api/v1/capabilities` is unauthenticated because the UI needs auth mode
before login. It reports only runtime auth modes, upload protocols/capabilities,
task types, active public catalog revisions, maximum qualified concurrency,
evaluation/local status, and deployment target. It contains no hostnames,
identity IDs, storage names, quotas, or secrets and returns `200` with a stable
`CapabilitiesRead` schema.

Catalog and candidate endpoints require an authenticated project viewer;
capacity estimates and launches require the existing compute permission.
Catalog responses contain `catalog_revision`, `task_type`, `generated_at`,
`runtime_lock_digest`, signature-verification status, and public estimator rows
with name, source, status/reason, recipe ID, resource class, sampling policy,
incremental/serialization/inference flags, and deprecation state. The signed
internal CI artifact additionally contains build provenance and signature
material and is never accepted from a client.

The estimate response contains `estimate_digest`, `catalog_revision`,
`capacity_profile_revision`, candidate count, resource-class slot demand,
selected sample-tier summary, node/quota requirements, expected object reads,
cost range, deadline, blockers, `environment_qualified`, and optional
`capacity_reservation{id,expires_at}`. Return:

- `409 catalog_revision_changed` with requested/current revisions;
- `409 capacity_profile_changed` with the current revision;
- `409 capacity_reservation_expired` when launch misses its reservation;
- `422 invalid_catalog_selection` for all mode plus selected candidates;
- `422 experiment_spec_required` for production all mode without an immutable
  spec revision;
- `422 experiment_spec_mismatch` when a compatibility field disagrees with its
  immutable experiment spec;
- `422 qualification_strength_required` when a production all-catalog request
  weakens or changes the signed iteration/fold profile;
- `422 qualification_deadline_required` for a non-7,200-second production
  all-catalog request; and
- `503 capacity_not_qualified` with structured resource/quota blockers when the
  environment cannot prove the deadline.

Candidate collections use opaque cursor pagination with a default and maximum
page size of 100, deterministic `(estimator_name, candidate_id)` ordering, and
filtering by status/resource class. Candidate detail returns lineage, attempts,
Ray job/trial identity, feature contract and recipe revisions, resources, sample
tier, split digests, metrics, artifact digests, and terminal reason.
Run events support `text/event-stream` with `Last-Event-ID` resume and a
cursor-paginated JSON fallback. Events are append-only, project scoped, and may
be replayed; clients deduplicate by event ID.

Every state-changing evaluation-scope create/member/cancel, estimate
reservation, and launch requires
`Idempotency-Key`; retries with the same key and request digest return the same
response, while a changed digest returns `409 idempotency_key_reused`.

Extend upload contracts:

```text
ResumableUploadCreate:
  dataset_name, filename, byte_size, content_type,
  client_sha256, resume_key, purpose, tags

UploadInstruction:
  protocol, method, url, required_headers, byte_range,
  part_number|confirmed_offset|block_id, expires_at

ResumableUploadComplete:
  client_sha256,
  receipts[
    {protocol, part_number|confirmed_offset|block_id,
     provider_tag, unit_checksum, byte_size}
  ]
```

Extend and migrate the existing `dataset_upload_sessions` table with protocol,
provider upload/session identifier, confirmed offset, instruction expiry,
client/final checksum, reconciliation lease, terminal reason, and capability
snapshot columns. Do not create a duplicate upload-session table.

Add normalized tables:

- `workflow_commands`;
- `workflow_outbox`;
- `dataset_splitter_attempts`;
- `dataset_preparation_attempts`;
- `training_capacity_reservations`;
- `training_run_attempts`;
- `training_run_candidates`;
- `training_search_trials`;
- `training_search_events`;
- `training_trial_attempts`;
- `training_checkpoints`;
- `training_run_events`;
- `evaluation_scopes`;
- `evaluation_scope_memberships`;
- `final_test_allocations`;
- `champion_refit_attempts`;
- `champion_evaluation_attempts`;
- `provider_stage_conformance_attempts`;
- `estimator_catalog_revisions`;
- `prepared_dataset_artifacts`;
- `dataset_split_revisions`;
- `feature_contract_revisions`;
- `feature_search_space_revisions`;
- `search_objective_revisions`;
- `experiment_spec_revisions`;
- `feature_registry_revisions`;
- `feature_definitions`;
- `feature_recipe_revisions`;
- `feature_parity_fixture_revisions`;
- `workflow_attempt_credential_grants`; and
- `artifact_digest_verifications`.

Add project-scoped foreign keys, status constraints, candidate identity
constraints, active resume-key uniqueness, dataset identity uniqueness,
unique parent/generation fences with at most one active attempt,
one-promotional-scope-per-final-test-allocation uniqueness, exactly-once search
result digests, and monitoring idempotency constraints using
expand/backfill/contract migrations.

Persist stage/attempt-scoped workload identity and credential-grant digest, Ray
job/cluster IDs, Tune experiment/trial IDs, attempt generations and fences,
split-role digests,
selected/rejected feature evidence, checkpoint lineage, and terminal
compare-and-set version. Search-space/objective revisions contain typed ranges,
conditional constraints, metric direction, fold aggregation, penalty
normalization/weights, joint-suggestion count for tunable entries,
feature-suggestion count for fixed entries, and failed/cancelled/partial-trial
policy.

Evaluation scopes additionally store mode, `scope_started_at`,
`scope_deadline_at`, evidence reserve, maximum/actual refit attempts, maximum/
actual pre-open evaluator attempts, central allocation ID where promotional,
and the terminal CAS version. Provider-stage conformance rows store stage type,
signed fixture/config/tier/parity revisions, quarantine prefix, and an explicit
`promotable=false` constraint.

### 4.9 Infrastructure module contract

Provision clusters through OpenTofu and GitHub Actions OIDC, not FastAPI.

| Target | Control-plane API wrapped by OpenTofu | Required cluster shape |
| --- | --- | --- |
| AWS EKS | EKS `POST /clusters` (`CreateCluster`) through `aws_eks_cluster` | Private endpoint, VPC/subnets, control-plane logging, deletion protection |
| Google GKE | `POST https://container.googleapis.com/v1/{parent}/clusters` through `google_container_cluster` | Private regional GKE Standard, explicit node pools |
| Azure AKS | `PUT https://management.azure.com/{resourceId}?api-version={frozenVersion}` through `azurerm_kubernetes_cluster` | Private, zone-resilient AKS Standard mode and Standard pricing tier |
| Railway | `POST https://backboard.railway.com/graphql/v2` with an environment-scoped project token after protected bootstrap | Ancillary non-production PostgreSQL/bucket contract environment only |

Useful primary API references:

- [EKS CreateCluster](https://docs.aws.amazon.com/eks/latest/APIReference/API_CreateCluster.html)
- [GKE clusters.create](https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.locations.clusters/create)
- [AKS managedClusters create/update](https://learn.microsoft.com/rest/api/aks/managed-clusters/create-or-update)
- [Railway public API](https://docs.railway.com/integrations/api)

Shared non-secret module inputs:

```hcl
environment
region
cluster_name
kubernetes_minor
dns_zone
application_hostname
oidc_issuer
oidc_client_id
capacity_profile
database_profile
object_retention_days
audit_retention_days
enable_gpu
budget_monthly
owner_tags
```

Shared non-secret outputs:

```text
cluster_name
cluster_region
registry_host
gateway_class
application_hostname
database_secret_reference
object_store_driver
object_store_bucket_or_container
object_store_identity
mlflow_uri
storage_classes
observability_destinations
workload_identity_bindings
qualification_capacity_profile
```

Do not intentionally render or publish secrets in OpenTofu outputs, plan text,
Helm values, or GitHub logs. Treat saved OpenTofu plans and state as
secret-bearing even when terminal output marks values sensitive. Bootstrap a
separate encrypted, versioned, locked remote-state backend per account/project/
subscription and environment; restrict readers and writers, audit access, test
recovery, and document break-glass ownership. Saved plans are short-lived,
encrypted, access-restricted deployment artifacts and are destroyed after apply
and evidence extraction. Archive a redacted plan summary, never the raw plan or
state. Follow OpenTofu's guidance for
[sensitive state](https://opentofu.org/docs/language/state/sensitive-data/) and
[saved plans](https://opentofu.org/docs/cli/commands/plan/).

### 4.10 Identity and private-access matrix

CI federation and pod workload identity are separate trust relationships. Record
issuer, audience, subject/claim conditions, principal, role bindings, resource
scope, cluster-auth mechanism, token lifetime, and negative tests for each row.

| Target | CI/bootstrap identity | Private cluster access | Pod identity |
| --- | --- | --- | --- |
| AWS | GitHub OIDC role restricted by repository, protected environment, workflow/ref, and audience | EKS access entry for an ephemeral runner in/peered to the VPC | EKS Pod Identity association per Kubernetes service account; Pod Identity Agent and EKS Auth endpoint required |
| GCP | GitHub Workload Identity Federation provider restricted by repository/ref/environment attributes | Ephemeral runner in/connected to the VPC and authorized through the cluster IAM/RBAC path | Workload Identity Federation for GKE; choose and document direct KSA principal IAM or one-to-one GSA impersonation |
| Azure | GitHub federated credential restricted by repository, protected environment, subject, and audience | Ephemeral runner in/peered to the VNet using Azure and Kubernetes RBAC | Azure Workload Identity federated credential per Kubernetes service account |
| Railway | One-time protected workspace bootstrap if needed, then an environment-scoped project token | Public API with IP/egress controls where available; no Kubernetes access | Not applicable; the token never enters Sceptre workloads |

Dynamic Ray identities are scoped by project and stage on every provider:

| Stage identity | May read | May write | Explicit denials |
| --- | --- | --- | --- |
| Splitter/verifier | One registered raw object | Immutable train, validation, final-input, final-label, and split-manifest prefixes | Other projects; model/checkpoint/registry prefixes |
| Preparation | Train and validation role prefixes | Prepared, profile, safe-feature, and preparation-checkpoint prefixes | Raw; both final-test prefixes; registry promotion |
| Training | Train/validation, approved prepared/features, own remote checkpoints, and explicitly authorized predecessor checkpoint manifests | Attempt-scoped metrics, checkpoints, models, and validation evidence | Raw; both final-test prefixes; any unlisted attempt/project |
| Champion refit | Frozen winning candidate plus preregistered train/validation tiers | One immutable refit pipeline and refit evidence | Raw; both final-test prefixes; search mutation; another scope/project |
| Champion evaluator | Frozen champion pipeline plus final inputs and labels | One attempt-scoped result object and fenced result callback | Raw; training evidence mutation; tuning/search submission; another scope/project |
| Provider stage conformance | Signed synthetic config/tier, train/validation or synthetic holdout, and parity corpus | Quarantined refit/evaluator bundle, predictions, and conformance evidence | Raw; locked release-test prefixes; registry promotion; `ReleaseFinalTestAuthority`; another project |

Project-and-stage workload identity is only the base issuer. An
`AttemptCredentialBroker` mints short-lived delegated object credentials after
the durable attempt/fence commits, restricted to exact input/output prefixes,
attempt or scope ID, methods, byte limits, and expiry. A replacement attempt gets
read-only grants to the CAS-registered predecessor checkpoint allowlist, never
the predecessor's write prefix. The final-evaluator grant is minted only after
the `test_opened_at` CAS and cannot be minted twice. Persist grant digests and
revocation/expiry evidence, never token material; negative tests cover another
attempt and scope within the same project.

For Azure user-delegation SAS, grant `Storage Blob Delegator` (including
`Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action`)
at storage-account scope and the minimum required container data role; do not
use storage account keys. Negative tests must prove that each principal cannot
read another environment/project prefix, administer the cloud account, or
authenticate through the other identity path.

### 4.11 Planned repository touchpoints

Phase 0 must confirm these locations before implementation. If the repository
layout changes, record the replacement in the baseline and keep one owner per
boundary.

| Boundary | Planned location |
| --- | --- |
| Object-store protocol and drivers | `apps/api/automl_api/storage/` and `apps/api/automl_api/storage/drivers/` |
| Upload API, state, and reconciliation | `apps/api/automl_api/api/routes/datasets.py`, dataset schemas/services, and a dedicated upload reconciler |
| Workflow/outbox infrastructure | `apps/api/automl_api/workflows/` and worker entry points under `apps/api/automl_api/workers/` |
| Ray Data preparation and Polars transforms | dataset inspection/profiling services plus Ray preparation jobs; remove the existing Dask service and benchmark paths |
| Feature contracts, registry, and recipes | `apps/api/automl_api/features/` with versioned schemas, generators, filters, compiler, and parity tests |
| Catalog generation and execution recipes | `apps/api/automl_api/training/` plus a generated, reviewed manifest directory |
| Ray coordination | training routes/services, durable reconciler, Tune searcher adapter, Train recipes, and KubeRay resource templates |
| Cross-provider final-test authority | `services/qualification-control/` plus its isolated migrations, signed receipt schemas, provider-distribution tooling, and protected deployment workflow |
| Browser upload and training UX | `apps/ui/react_app/src/` with Web Worker code kept outside React render paths |
| Database changes | `alembic/versions/`, ORM models, and database integration tests |
| Portable deployment | `infra/helm/sceptre/` |
| KubeRay operator and CRDs | pinned dependency of `infra/helm/sceptre/`, enabled by default with namespace-scoped RBAC plus install, upgrade, and conversion tests; an explicit external-operator opt-out is supported |
| Cloud infrastructure | `infra/tofu/modules/{aws-eks,gcp-gke,azure-aks}` and environment compositions |
| Local runtime automation | `infra/{k3d,kind,minikube,microk8s}/` and `sceptrectl` commands |
| Release and qualification | `.github/workflows/`, `scripts/`, `tests/`, and non-secret evidence indexes |

Do not place provider SDK selection, cloud-admin APIs, OpenTofu execution, or
cluster credentials in FastAPI routes. Do not duplicate provider business logic
between the browser and API: the API issues a normalized instruction and the
browser executes it.

Phase 0 inventories every Dask reference; Phase 3 removes every one before its
gate. The scope includes source, configuration, dependencies, CI, Helm values
and schema, manifests, network policies, tests, scripts, benchmarks, README
files, and architecture documents. A repository-wide case-insensitive search
for `dask` must return no production or fallback implementation reference
before the Phase 3 gate may pass.

## Phase 0: Freeze and reconcile the moving baseline

**Depends on:** no implementation phase.
**Unblocks:** every later phase.
**Primary owners:** release engineering, backend, UI, ML, platform, and docs.

### Purpose

Create one reproducible starting point and eliminate version, schema, runtime,
and documentation ambiguity before production work branches.

### Work

- Inventory and classify all existing user changes before creating a reviewed
  baseline. The observed worktree contains 72 tracked modifications and 31
  untracked paths, including this guide and migrations. Preserve every user
  change, discard nothing, and never create a blanket baseline commit without
  review.
- Record Git SHA, dirty-state resolution, generated OpenAPI snapshot, Alembic
  heads, database table/index/constraint inventory, chart version, image
  versions, dependency graph, estimator manifests, and exact test counts.
- Define artifact version separately from qualification status. Phase 8 builds
  artifacts once with final embedded version `0.2.0` and writes an immutable
  artifact manifest. A separate signed qualification attestation carries
  `qualification_label=0.2.0-rc.1` and the decision status. A mutable release
  channel pointer references an approved attestation; neither attestation nor
  channel state is embedded in the artifact manifest.
- Define the SemVer display, Python PEP 440 representation, chart version, and
  image labels in the artifact-manifest schema. Update release CI before producing the
  first candidate.
- Inventory the existing mixed Python images and dependency locks. Define the
  candidate Python 3.12/runtime compatibility matrix and Phase 0A proof plan,
  but do not rebuild images, change locks, migrate the runtime, or claim wheel/
  fit compatibility before the kill gate passes.
- Declare Helm the supported deployment boundary. Label legacy Kustomize and
  partial Compose stacks evaluation-only.
- Freeze the requirements and decision register from Section 2, including named
  owners, approvers, sources, cost ceiling, and pending decisions.
- Inventory all Dask references and assign a removal or Ray/Polars replacement
  for every source, dependency, CI, Helm, test, script, and documentation path.

The initial repository scan found the following Dask surfaces. Treat this as a
lower bound and regenerate it with a hidden-file-aware search in Phase 0:

```text
.github/workflows/ci.yml
apps/api/automl_api/core/config.py
apps/api/automl_api/services/{dask_profiling,kubernetes_training,profiling,profiling_jobs,training}.py
apps/api/automl_api/training/{distributed_search,pipeline}.py
infra/helm/sceptre/{values.yaml,values.schema.json}
infra/helm/sceptre/examples/values-production.yaml
infra/helm/sceptre/templates/{configmap,distributed-compute,network-policies,production-gates}.yaml
pyproject.toml
requirements-training.txt
scripts/benchmark_dask_ingestion.py
tests/{test_distributed_search,test_incremental_training,test_profiling}.py
README.md
docs/architecture/implementation-plan.md
docs/production-readiness/README.md
```

- Reconcile the object-store protocol with current consumers, storage enum/URI
  migrations, and legacy content-hash semantics before changing schemas.
- Decide and document whether upload completion registers only the dataset or
  automatically starts profiling. Update the API, UI, scripts, and tests as one
  contract.
- Update stale documentation covering upload status, schema inventory,
  profiling behavior, React architecture, outputs, routes, and local clusters.
- Reconcile `.env.example`, typed settings, Helm values, and production policy.
  Remove undocumented settings and resolve conflicting access-token lifetimes.
- Supersede the accepted Streamlit ADR with the current React/FastAPI
  architecture.
- Record the observed baseline of 259 passing backend tests, 2 skipped backend
  tests, and 40 passing UI tests. Resolve both skips or record a dated owner and
  removal gate; do not carry unexplained skips.
- Record the current approximately 4.85 MB minified Plotly build chunk and set a
  reviewed route-level JavaScript budget. Lazy-load visualization code outside
  upload, authentication, and run-control critical paths.
- Replace the test that hardcodes migration head
  `0004_resumable_dataset_uploads` with a dynamic single-head assertion or a
  deliberately versioned migration manifest.
- Snapshot classification, regression, time-series, and clustering catalogs from
  the current baseline runtime. Phase 0A generates the candidate-runtime diff.
- Add a machine-readable baseline record with hashes for every generated
  snapshot.

### Deliverables

- reviewed baseline commit plus the artifact-manifest, qualification-attestation,
  and release-channel identity schemas;
- OpenAPI and database inventory snapshots;
- current Python/model dependency inventory and candidate migration plan;
- baseline catalog snapshot and reviewed provisional exclusions;
- test/evidence report tied to the baseline SHA;
- updated architecture decisions and documentation map; and
- checked-in `docs/production-readiness/task-index.yaml` with immutable IDs and
  bullet fingerprints for every Phase 0–13 work/gate item.

### Gate

- Checkout is clean and reproducible.
- The requirements register records the accepted Ray/Polars path, Dask
  prohibition, benchmark identity, pending numeric targets, approvers, and cost
  ceiling.
- The identity contract maps package, chart, image, and manifest versions without
  claiming that Phase 0 built the release artifacts.
- Alembic reports one head.
- Backend, UI, lint, build, and Helm checks pass with zero unexplained skips.
- OpenAPI, table inventory, and catalog manifest match runtime behavior.
- The readiness record is dated and names the frozen SHA.
- Release CI validates the qualification-attestation label schema separately from the final
  embedded artifact version and immutable artifact-manifest digest.

### Baseline verification commands

Run the repository's current checks, then store command versions and complete
output in the baseline evidence bundle:

```bash
ruff check apps packages alembic scripts tests
python -m compileall apps packages alembic scripts tests
pytest tests/ -v --tb=short

cd apps/ui/react_app
npm ci
npm run test
npm run lint
npm run build
cd ../../..

alembic heads
alembic upgrade head
alembic check

helm lint infra/helm/sceptre
helm template sceptre infra/helm/sceptre --namespace sceptre
```

Run schema verification and every production/evaluation Helm fixture defined by
the frozen CI workflow. Do not substitute a shorter local command for the
archived CI result.

### Rollback and requalification

Phase 0 is observational and does not perform the runtime migration. If the
historical starting snapshot is factually wrong, create a corrected reviewed
baseline revision and invalidate evidence that references the error. Later
runtime, dependency, migration, catalog, chart, or version changes create new
candidate/evidence revisions and requalification triggers; they never rewrite
or replace the historical baseline identity.

## Phase 0A: Prove feasibility, cost, and recovery

**Depends on:** Phase 0.
**Unblocks:** every substantial implementation and cloud-provisioning phase.
**Primary owners:** ML platform, data platform, SRE, security, and finance or
the named budget owner.

### Purpose

Prove that the chosen Ray and Polars design can satisfy correctness, memory,
recovery, scheduling, and cost constraints before building the full control
plane or three cloud environments.

### Work

- Pin one Python 3.12 patch, KubeRay operator/CRD revision, and candidate Ray,
  PyArrow, Polars, scikit-optimize, sklearn, and deep-learning dependency lock.
  Prove imports, CR compatibility, and supported CPU/GPU wheels in the target
  images.
- Build a vertical spike that reads representative and pathological fixtures
  with Ray Data, transforms bounded Arrow batches with Polars, writes canonical
  Parquet, and produces stable row and split digests across worker counts.
- Implement one controlled feature family, one leakage rejection, one
  metadata-only feature view, and one signed recipe through training and
  inference parity.
- Implement a minimal scikit-optimize adapter for Ray Tune with typed search
  spaces, deterministic ask/tell behavior, concurrency, save, restore,
  cancellation, and trial retry.
- Exercise one ordinary sklearn estimator, one incremental estimator, one
  distributed-native CPU estimator, and one distributed deep-learning recipe.
- Exercise the scope-level champion refit and one-shot evaluator, including a
  refit retry, failure before/after `test_opened_at`, same-project cross-attempt
  denial, predecessor-checkpoint allowlisting, and delegated-credential expiry
  on each provider's credential mechanism.
- Exercise the non-promotional provider refit/evaluator conformance path and
  prove its identity cannot request or resolve a release final allocation.
- Prove remote Tune/Train checkpoint recovery after worker, driver, Ray head,
  operator-leader, and complete ephemeral `RayCluster` loss. Separately rehearse
  loss and OpenTofu recreation of the containing Kubernetes cluster. Treat each
  recreated application workload as a new fenced attempt.
- Measure peak resident memory, Ray object-store memory, shuffle, spill,
  ephemeral storage, object reads, network throughput, driver/head pressure,
  scale-up delay, and end-to-end recovery time.
- Run a discrete scheduling simulation for the provisional 15-run catalog using
  measured long-tail runtimes, resource vectors, dependencies, retries, gang
  placement, provider quota, and node startup distributions.
- Validate the qualified per-attempt ephemeral-cluster topology, including 15 head
  pods, operator load, startup latency, quotas, workload identities, and cleanup.
  A shared-cluster experiment may inform a later ADR but cannot replace this
  topology in the current qualification.
- Freeze a protocol transport-security matrix for the Ray job submission path,
  dashboard, GCS/control traffic, Ray inter-node data, PostgreSQL, object store,
  MLflow, registry, and telemetry. Prove TLS/mTLS, trust-root validation, and
  certificate rotation for every supported endpoint. If the pinned Ray release
  cannot encrypt a required inter-node path, record the exact trusted-boundary
  compensating control and require explicit security acceptance before ADR-005
  can pass; network isolation alone must not be mislabeled as encryption.
- Test the pending 8-core, 24 GB local profile with a bounded single-node Ray
  runtime, Polars streaming, and configured spill. Record the highest safe
  workload rather than claiming the production deadline.
- Produce expected, p90, and worst-case qualification cost for each provider,
  including compute, GPUs, storage, requests, network, telemetry, retained
  evidence, chaos repetitions, and reruns. This is a conservative feasibility
  envelope, not the exact campaign ledger.

### Gate

- Ray Data, Tune, Train, KubeRay, Polars, PyArrow, sklearn, and the selected deep
  framework import and run from the same pinned runtime family.
- No 10 GiB path performs full pandas conversion or driver collection.
- Row, split, sample, schema, and recipe digests remain exact across worker count
  and provider. Floating aggregate outputs either use a deterministic algorithm
  or pass a separately recorded numeric-tolerance comparison.
- Canonical suggestion-log replay yields identical trial parameters, seeds, and
  recipe hashes across providers; objective quantization and stable tie-breaking
  make hardware-level score variation unable to alter the search sequence.
- The Tune searcher and Train recipe restore from remote checkpoints without
  duplicate terminal writes or lost lineage.
- Worker, head, driver, operator, node, and cluster-loss drills have measured
  recovery behavior and an accepted resubmission design.
- The discrete schedule and representative benchmark pass every threshold
  generated from the signed decision register, produce zero unexplained
  unschedulable placements, retain at least 20% provider-quota headroom, and
  record measured p50/p95/p99 startup, fit, recovery, and wall-clock values.
- Every ADR and QLT row is accepted, rejected, or superseded. The resulting
  signed register generates the remaining phase and gate index.
- The budget owner approves the conservative cost envelope, preflight remaining-
  budget rule, overrun stop authority, and maximum Phase 1–4 implementation
  spend. Phase 4 and provider calibration later freeze the exact campaign ledger.
- A signed go/no-go record approves the implementation, narrows the pending
  target through a reviewed decision, or stops the program before material
  build-out.

### Rollback and requalification

Phase 0A creates disposable spike infrastructure only. Destroy it after
archiving non-secret evidence. A Python, Ray, PyArrow, Polars, searcher,
estimator, feature-execution, cluster-topology, node-family, or qualification
target change invalidates the applicable feasibility evidence.

## Phase 1: Durable state, idempotency, and public contracts

**Depends on:** Phases 0 and 0A.
**Unblocks:** Phases 2–4 and reliable cloud reconciliation.
**Primary owners:** backend and database engineering.

### Purpose

Make workflows recoverable across API, database, worker, and external system
failures before adding cloud-scale side effects.

### Work

- Extend the existing upload-session table and add workflow-command, outbox,
  capacity-reservation, prepared-artifact, split, feature-contract,
  feature-registry, feature-recipe, catalog, candidate, attempt, trial,
  checkpoint, event, and digest-verification tables with project-scoped
  integrity constraints.
- Represent splitter, preparation, training run, training trial, champion
  refit, champion evaluation, and typed provider-stage conformance attempts
  explicitly. Each row stores its parent scope or conformance campaign, stage,
  project-and-stage workload identity, generation/fence, RayJob/RayCluster IDs
  where applicable, heartbeat/lease, checkpoint, terminal CAS version, and
  cleanup state. A generic table is acceptable only if typed constraints and
  foreign keys make invalid parent/stage combinations impossible.
- Replace session-level worker locks with leased queue rows claimed using
  `FOR UPDATE SKIP LOCKED`.
- Add `Idempotency-Key` handling to upload completion, training launch,
  validation, deployment, governance generation, monitoring ingestion, and
  cleanup execution.
- Store request hashes with idempotency records. Reusing a key with a different
  request returns a deterministic conflict.
- Record desired state before object-store, Kubernetes, registry, or deployment
  side effects.
- Build reconcilers for unfinished uploads, Kubernetes resources, registry
  artifacts, Ray jobs/trials, deployments, governance objects, and deletions.
- Define one retry owner and map database attempt generations to KubeRay jobs,
  Tune trials, and Train runs. No controller may create an untracked nested
  retry.
- Define the `ReleaseFinalTestAuthority` protocol and signed receipt schemas,
  including unique allocation, canonical provider/scope assignment, one-shot
  open, result commit/failure, provider-distribution manifests, and provider-
  local reference validation. Implement it in a separately protected,
  strongly consistent qualification-control store; provider databases are
  consumers, never competing authorities.
- Bound PostgreSQL pools; configure connect, statement, idle-transaction, and
  lock timeouts; validate TLS CAs; set application names; export pool metrics;
  and qualify PgBouncer transaction mode.
- Implement expand/backfill/contract migrations. The previous release must
  continue to read during expansion and backfill.
- Upgrade large artifact byte-size columns to `BigInteger` where 32-bit limits
  can be exceeded. Verify every column, type, constraint, index, default, and
  foreign key after migration rather than checking only table names and head.
- Introduce catalog and candidate APIs while retaining selected-model
  compatibility.
- Add typed feature contracts, feature-search requests, estimator overrides, and
  version-conflict responses. Remove the generic client-controlled parameter
  dictionary before exposing Ray execution.
- Migrate legacy storage enum values and URIs without reinterpreting persisted
  objects. Add a no-fallback startup gate for unknown production drivers.
- Version content digests by algorithm and scope. Backfill or quarantine legacy
  resumable uploads whose stored hash describes a part manifest rather than
  object bytes.
- Persist deletion tombstones and durable deletion stages for raw objects,
  multipart units, prepared data, samples, feature artifacts, Ray spill and
  checkpoints, Tune state, MLflow artifacts, models, logs, replicas, and
  backups.
- Define state transition functions centrally and reject impossible transitions
  at service and database boundaries.
- Add lease expiry, heartbeat, retry budget, terminal reason, and operator replay
  fields to every durable worker workflow.

### Required failure tests

- Kill the API before and after each external side effect.
- Replay each command and outbox entry at least twice.
- Run multiple worker replicas against the same queue.
- Force PostgreSQL connection loss after claim and before commit.
- Reuse idempotency keys concurrently and with conflicting payloads.
- Rehearse migrations from every supported prior schema with production-scale
  row counts.
- Kill the reconciler between database attempt creation, Ray submission, Ray ID
  persistence, checkpoint publication, and terminal writes.
- Kill a run with multiple active trials and prove one transaction supersedes
  every old trial attempt before replacements can write. Exercise attempt-grant
  expiry and predecessor-checkpoint allowlists.
- Race final-allocation/open/commit requests from all three provider identities;
  exactly one canonical request may advance state and every non-canonical or
  duplicate request must fail without receiving an object credential.

### Gate

- Repeating creation and finalization requests returns the same resource.
- API death around every side effect self-heals through reconciliation.
- Concurrent dataset/session creation cannot duplicate resources or exceed
  quotas.
- Worker replicas process a row once or perform an idempotent replay.
- Migration rehearsal succeeds from every supported schema and on a
  production-sized database.
- Ray resubmission preserves one logical run/candidate/trial history with
  append-only attempts, rejects stale fences, and never duplicates a terminal
  artifact or final-test evaluation.
- Scope refit is independently retryable and CAS-publishes one pipeline; scope
  creation/membership/sealing/cancellation is idempotent and releases no member
  before the exact barrier commits.
- Scoped launches remain `barrier_pending` with no outbox or clock until the
  exact membership transaction seals them, and the central final authority
  permits one canonical provider/scope to open and commit each locked split.

### Rollback and requalification

Deploy only expand-compatible schema changes before new readers/writers. Disable
new workers, roll application code back, and keep expanded columns/tables until
the old release is retired. Requalify after a state-machine, lease, retry,
idempotency, database engine, pooler, or migration-range change.

## Phase 2: Cloud-portable 10 GiB ingestion

**Depends on:** Phase 1 state, idempotency, and outbox contracts.
**Unblocks:** Phase 3 preparation and cloud data planes.
**Primary owners:** backend, UI, cloud integration, and security.

### Purpose

Move large object bytes off the control plane and provide resumable,
integrity-checked uploads on every storage target.

### Work

- Implement all five object-store drivers and one shared contract suite.
- Replace private MinIO SDK calls with supported SDK operations.
- Remove fixed SeaweedFS proxying from production UI configuration.
- Implement provider-native upload instructions, exact-origin CORS, checksum
  headers, and log/query redaction.
- Move primary dataset, validation, drift, and offline-scoring uploads to the
  same resumable browser client.
- Add browser pause, cancel, resume after reload, progress, transfer rate, retry,
  and session-expiry UX.
- Compute incremental SHA-256 in a Web Worker, persist resumable hash/session
  state in IndexedDB, capture provider receipts and response headers, and expose
  required CORS headers. A filename/size/mtime fingerprint is not an integrity
  identity.
- Reconcile abandoned uploads on a schedule and configure provider lifecycle
  rules only when the driver capability reports support. The application
  reconciler remains mandatory even when provider lifecycle exists.
- Verify client SHA-256 while preparing the object. Quarantine mismatches and
  delete them after the configured retention period.
- Classify dataset sensitivity and region before issuing upload instructions.
  Apply tenant retention, legal hold, deletion, and residency policy to raw and
  prepared object prefixes.
- Validate extension/content type, decompression ratios, row width, encoding,
  project storage quota, and maximum object size. Provide a malware/content
  policy with a sandboxed scanner interface, allow/deny outcomes, quarantine,
  scanner timeout/failure behavior, signature/version evidence, and release
  gate. A no-op hook is not a production control.
- Disable API-buffered multipart upload above 100 MiB in staging and production.
- Make upload instruction, complete, abort, and cleanup operations idempotent.
- Make signed URLs/session URIs grant or enforce the minimum object, method,
  transfer unit/offset, size, checksum, and expiry scope supported by each
  provider; document controls the provider protocol cannot encode.

### Provider contract cases

Run the capability-aware suite for SeaweedFS/MinIO, S3, GCS, Azure Blob, and
Railway Buckets. Require a case only when the driver claims the capability;
unsupported features return an explicit capability result. Test:

- start, issue transfer instruction, retry a transfer unit, query progress,
  resume, complete, stat, stream, abort, and delete;
- expired instruction/session, throttling, wrong byte range or unit size,
  missing receipt, duplicate completion, checksum mismatch, and unauthorized
  object/key;
- interrupted browser, API restart, UI restart, database failover, and cleanup;
  and
- redaction of query strings, signatures, credentials, and provider tags.

### Gate

- The contract/failure suite passes on the local S3-compatible reference
  environment. Isolated developer accounts prove native S3, GCS, and Azure API
  semantics without claiming scale qualification.
- The load harness drives 15 concurrent exactly-10-GiB objects on the designated
  reference environment. Peak API and UI RSS stays at or below the signed
  Phase 0A `control_plane_rss_bound_bytes`, and measured payload bytes proxied
  through either service equal zero.
- The load harness labels these objects as systems fixtures. Comparable ML
  experiments reference the approved immutable benchmark dataset rather than
  treating 15 transport copies as 15 independent statistical datasets.
- Browser/API restart, pod eviction, network loss, expired instruction,
  throttling, and database failover do not restart already confirmed bytes.
- Corrupted transfer units or final objects are rejected.
- Cleanup finds zero abandoned uploads and zero unreferenced completed objects
  after its grace period.
- Phases 10–12 repeat the full gate on their real provider and freeze provider
  evidence; Phase 13 repeats it with the immutable release candidate.

### Rollback and requalification

Keep the previous upload protocol readable until all live sessions expire or are
migrated. A driver, SDK, CORS policy, part size, checksum algorithm, browser
client, storage class, provider identity, or lifecycle-rule change invalidates
that provider's ingestion evidence.

## Phase 3: Ray Data preparation and automated feature engineering

**Depends on:** Phases 1 and 2 plus the Phase 5 platform bootstrap gate.
**Unblocks:** Phase 4 candidate planning.
**Primary owners:** data platform and ML engineering.

### Purpose

Create deterministic, restartable datasets, splits, samples, and safe feature
recipes with Ray Data and Polars. Keep 10 GiB parsing and feature work outside
API processes and avoid per-trial dataset copies.

### Work

- Make durable Ray-job profiling and preparation mandatory in staging and
  production after the explicit product action selected in Phase 0.
- Add retry count, heartbeat expiry, checkpoint, dead-letter state, operator
  replay, and continuous reconciliation.
- Submit a short-lived, privileged `dataset_splitter_attempt` as its own fenced
  KubeRay `RayJob` with an embedded ephemeral `RayCluster`. Its stage-scoped
  identity verifies the raw-object digest, computes stable row identities and
  the approved split, writes role-isolated train, validation, final-input, and
  final-label objects, then permanently loses raw access. It performs no learned
  feature selection and emits no final-role distribution, label statistic, row
  preview, or value-derived metadata beyond preregistered counts and opaque
  digests.
- The only no-raw splitter exception is a release-qualification
  `validation_only` import signed by `ReleaseFinalTestAuthority`. Verify its
  distribution-manifest signature, logical/train/validation digests, and absent
  raw/final capabilities before registration. It can feed preparation and
  validation-only training but cannot create a promotional scope. No ordinary
  tenant dataset or unsigned provider import may use this path.
- In that privileged stage, keep positional row IDs but group exact content
  fingerprints into one split role unless a declared repeatable-event key and
  group/time rule justifies them. Reject an impossible split and record opaque
  overlap counts rather than letting duplicate records cross roles.
- Submit each later preparation/profile attempt as a separate fenced KubeRay
  `RayJob` with an embedded ephemeral `RayCluster` and a preparation-only
  identity that can read train/validation artifacts but cannot read the raw or
  either final-test prefix. Apply the same retry, autoscaling,
  remote-checkpoint, terminal-reconciliation, and deletion contract as
  training. Persist both attempt types and their `RayJob`/`RayCluster`
  identities.
- Delete or replace every current Dask service, configuration field, dependency,
  CI check, Helm value/schema/resource, network policy, test, script, benchmark,
  and documentation path. Production and evaluation profiles must import and
  exercise the Ray/Polars implementation only.
- Read CSV, JSONL/NDJSON, and Parquet with Ray Data. Transform bounded Arrow
  batches with Polars. Reject 10 GiB Excel files with deterministic
  `422 large_excel_requires_conversion`, the accepted formats, and a remediation
  reference.
- Derive stable row IDs independently of Ray block order. Materialize canonical
  partitioned Parquet plus deterministic task-specific train, validation,
  locked-test, and sample artifacts.
- Compute SHA-256, row count, schema, stateless structural checks, and resource
  estimates in one distributed pass where practical. Only schema/role/formula
  safety and row-identity integrity may inspect all role manifests. Fit every
  value-dependent threshold, count, statistic, transform, or leakage heuristic
  on the outer training role and refit it within each inner training fold.
- Generate only feature families allowed by the versioned feature contract.
  Enforce expression-depth, output-count, cardinality, memory, runtime, and
  materialization budgets before expensive computation.
- Apply hard formula, availability, post-outcome, and identifier rules before
  model search. Learn constant/quasi-constant, duplicate, missingness,
  cardinality, frequency, aggregate, correlation, redundancy, encoding,
  imputation, scaling, decomposition, and supervised-selection state only from
  outer-train/inner-fold-train rows. Record the fitted-scope digest and reason
  for every accepted or rejected definition. Never inspect validation values or
  final-test inputs/labels while generating or filtering candidate features.
- Persist one immutable base dataset, justified reusable safe-feature artifacts,
  and metadata-only feature views. Prohibit one full physical dataset per trial.
- Store source SHA, logical dataset digest, split digests, image digest,
  dependency-lock digest, Ray/Polars/PyArrow versions, catalog revision, feature
  contract and registry revisions, random seeds, and physical artifact hashes.
- Implement the profiling behavior ratified in Phase 0. The provisional default
  is that upload completion registers the dataset and the UI explicitly asks the
  user to profile; changing that default requires a reviewed product decision.
- Add preparation and feature-generation progress, retry status, rejection
  summary, dead-letter explanation, sampling preview, and feature-budget usage
  to UI and API.
- Enforce project-specific object prefixes and database queries at every stage.
- Design Parquet partition and row-group sizes from measured object throughput
  and worker memory, then pin the qualified defaults.
- Partition group, lag, and rolling computations by entity and time. Specify
  ordering, boundary halos, shuffle, and deterministic merge behavior before
  enabling each family.
- Qualify source-availability, immutable as-of snapshot, decision cutoff,
  label-horizon, window-boundary/tie, known-future, and late-arrival/backfill
  behavior with positive and negative point-in-time fixtures.
- Enforce outer-role group and temporal isolation in the privileged splitter.
  Reject group overlap, role-order inversion, and any label horizon, lookback,
  purge, or embargo crossing before registering a split artifact.
- Add automated tests that fail if a production 10 GiB path calls full pandas
  conversion, driver collection, or an equivalent full materialization.

### Statistical invariants

- Classification samples preserve target proportions within a reviewed bound
  and retain every supported class.
- Regression quantile strata cover the target range and preserve tail examples.
- Clustering samples are deterministic and preserve rare categorical values.
- Time-series windows are ordered, leakage-safe, and preserve configured
  seasonal periods.
- Within one sample tier and leaderboard league, every comparable estimator and
  feature trial receives identical training, validation, final-test, and sample
  row digests. The signed comparison policy gives the full/highest approved tier
  champion precedence and forbids cross-league promotion unless uncertainty and
  eligibility rules were preregistered.
- Observed features are published, available, and effective by the decision
  cutoff. Declared known-future covariates are published/available by cutoff and
  effective no later than the forecast horizon; no target-derived future value
  qualifies. Label-horizon and as-of rules hold for every row, and a late
  backfill creates a new source snapshot rather than changing old output.
- Supervised transforms fit within each training fold and cannot access
  validation or final-test labels.
- Outer train/validation/final group sets are disjoint whenever the contract
  requires group isolation. Temporal and panel roles have ordered cutoff ranges
  and satisfy the exact signed purge/embargo and horizon/window inequalities;
  the splitter records opaque set/boundary evidence before downstream access.
- Dataset, row, split, and sample identities follow their frozen seed policy
  across worker counts and providers. Recipe hashes are identical only for an
  identical complete experiment spec, search events, and seed. Floating
  aggregate outputs either remain exact or pass the separately recorded
  tolerance policy; do not describe a tolerance comparison as hash equality.

### Gate

- The systems-load corpus supports 15 concurrent 10 GiB preparations without
  API pressure or duplicate ownership. This result is not used as model-quality
  evidence.
- Comparable ML experiments reuse one immutable benchmark dataset version and
  create no per-trial full copies.
- Worker, driver, Ray head, node, and complete ephemeral-RayCluster loss resume
  through a new fenced attempt from the last valid remote stage.
- Prepared manifests are deterministic for the same input and release.
- All statistical invariants pass.
- Distributed global/group/window features prove fold-scoped reduce/shuffle
  state, `output_row_ids == input_row_ids`, zero duplicates/drops, deterministic
  halo trimming, and the frozen Arrow/Polars dtype/null/NaN/infinity/overflow/
  timezone semantics.
- Split evidence reports zero unauthorized exact-content fingerprint overlap
  across roles while preserving unique positional row identities.
- Split evidence reports zero forbidden group overlap, zero temporal ordering
  inversion, and zero purge/embargo/horizon/window boundary violation under the
  task's outer-split contract.
- Feature safety gates reject known leakage, post-outcome, duplicate,
  identifier, and cardinality-bomb fixtures. They continue with fewer safe
  features instead of lowering the gates.
- Peak driver memory remains within the Phase 0A bound and no full-data pandas
  or driver materialization occurs.
- Runtime locks, imports, CI, rendered Helm output, tests, and benchmarks contain
  no Dask execution or fallback path.
- Metadata, temporary objects, samples, and artifacts remain project scoped.
- A signed `validation_only` provider import exposes train/validation roles
  only, cannot resolve raw/final URIs, and is rejected from every promotional
  scope; a forged/altered manifest fails registration.
- Negative IAM tests prove the splitter alone can read raw data, has write-only
  final-prefix capability, and loses its grant at terminal state; preparation
  and training can read only their role prefixes. After role materialization,
  only the centrally authorized dedicated evaluator can read final inputs or
  labels.

### Rollback and requalification

Prepared manifests are immutable. A rollback may continue reading an older
manifest version but must never reinterpret a newer one. Parser, partition,
sampling, split, schema, dependency, Ray, PyArrow, Polars, feature contract,
generator, filter, recipe, or lineage changes require new versioned artifacts
and invalidate downstream training evidence.

## Phase 4: Ray Tune, Train, and catalog orchestration

**Depends on:** Phases 1–3.
**Unblocks:** the approved training qualification and provider capacity plans.
**Primary owners:** ML platform, backend, and SRE.

### Purpose

Replace per-run sequential tournaments with durable model-plus-feature trials
scheduled by Ray Tune on KubeRay. Use Ray Train only for supported distributed
recipes and preserve a locked final-test boundary.

### Work

- Generate and sign catalog revisions in CI.
- Add reviewed recipes for concrete meta-estimators currently omitted.
- Remove backend/UI 20-model constraints and add `catalog_mode=all`.
- Replace the current one-active-run-per-project guard with explicit user,
  project, environment, and resource-class admission policy. The approved
  qualification profile must support 15 concurrent runs referencing one
  benchmark in one project without creating one project per user to hide the
  limit.
- Expand each run into durable candidate and model-plus-feature trial rows grouped
  by execution backend and resource class.
- Implement the PostgreSQL/outbox to fenced Ray reconciler, KubeRay submission,
  independent heartbeat, deadlines, remote checkpoints, dead-letter evidence,
  and orphan reconciliation.
- Implement a custom scikit-optimize adapter for Ray Tune's Searcher contract.
  Persist typed search spaces, suggestion-to-parameter/seed mappings, pending
  sets, concurrency batches, deterministic or buffered `tell` order,
  exactly-once result digests, failure/cancellation/partial-result treatment,
  save/restore state, and deterministic search seeds. A retry uses the same
  suggestion and seed.
- Freeze deterministic estimator/library kernels where supported, objective
  quantization precision, metric reduction order, and stable tie-breaking. For
  cross-provider qualification, generate and sign one canonical suggestion
  event log during the Phase 4 reference campaign and replay those exact
  feature/model suggestions on every provider; provider-specific floating-point
  completion must not alter later suggestions.
- Treat five iterations as the provisional qualification workload, not evidence
  that Bayesian search converged. Phase 0A must compare search quality against a
  larger offline reference and document the expected regret or limitation.
- Define whether early termination is allowed per recipe. A stopped trial cannot
  count toward the five-iteration, three-fold qualification contract unless the
  approved scenario explicitly permits and reports it.
- Apply deterministic sampling policy before candidate workers start.
- Isolate artifacts by project, run, candidate, and attempt.
- Define one Tune trial as one joint feature-plus-model suggestion. A fixed
  estimator has one model configuration and, under QLT-006, five feature-only
  suggestion slots including raw plus distinct safe engineered choices; a
  safety-exhausted space runs all unique choices and records that terminal
  search condition;
  a tunable estimator has the approved number of joint suggestions. Evaluate
  every suggestion across the task-correct inner folds, then perform an
  additional champion refit when eligible.
- Run ordinary sklearn in one bounded trial worker. Run incremental estimators
  through one checkpointed state owner. Run only distributed-native catalog
  recipes through Ray Train with an explicit worker count, CPU/GPU topology,
  dataset-sharding policy, rendezvous behavior, and remote checkpoint contract.
- Derive every inner splitter from the immutable feature/experiment contract:
  group-aware or stratified-group folds when entities must not cross roles,
  purged/embargoed rolling folds for temporal tasks, task-correct ordinary or
  stratified folds only when no group/time restriction applies, and reviewed
  stability/internal metrics for clustering. Keep these inner folds distinct
  from the common outer validation holdout used for cross-candidate selection.
- Include the raw approved feature set as a trial. Promote an engineered recipe
  only when it meets the fixture's preregistered predictive non-inferiority or
  lift threshold, stability bound, and compute budget. Otherwise select the raw
  or smaller safe recipe and record why.
- Aggregate each run's validation evidence only after every candidate is
  terminal. After every run in the sealed `evaluation_scope` is terminal,
  select one global champion under the preregistered league/metric rule. Submit
  one separately fenced `champion_refit_attempt` RayJob using the catalog
  backend and refit-only identity; CAS-freeze its pipeline on the exact approved
  train/validation tier. Only after refit succeeds, submit the fenced
  champion-evaluation RayJob with evaluator-only credentials. Commit its result
  once by scope compare-and-set and immediately revoke access.
- Implement cancellation: stop new submissions, terminate active Ray jobs,
  preserve completed candidates, and consistently mark interrupted candidates.
- Implement capacity preflight and expose quota/node-pool blockers before
  launch.
- Reconcile the database admission token with current Ray demand, Kubernetes
  schedulability, placement groups, node-pool and GPU availability, provider
  quota, and scale-up limits immediately before submission.
- Set qualified production concurrency to at least 15.
- Enforce weighted fair admission and project/resource-class quotas across
  per-run `RayJob` resources. Prove that one run cannot consume every worker
  node, GPU, ephemeral-storage allocation, or provider quota and cannot access
  another run's object store or spill.
- Benchmark every candidate on normalized reference resource classes, then
  version the measurements and capacity-model schema used by provider
  calibration.
- Freeze a signed catalog workload ledger with exact candidates, feature/model
  configurations, folds, refits, fit-attempt ceiling, artifact retention, and
  logical object-read estimates. Provider phases calibrate this ledger and
  reapprove their exact campaign cost before Phase 13.
- Freeze a signed provider-stage conformance matrix with at least one ordinary
  sklearn, incremental, and each distributed-native backend recipe. It binds a
  non-promotional config, train/validation tier, synthetic holdout, bundle
  parity corpus, exact-or-tolerant prediction/metric policy, and RayJob template
  for refit and evaluation. Every managed provider must execute it without
  release-final access.
- Prevent stale profiles from authorizing production launches after a runtime,
  catalog, node type, or provider change.

The evidence must distinguish planned, budgeted, and observed fit workload. It must not
multiply a joint feature-plus-model suggestion by a second feature-grid factor:

```text
planned_logical_fits = Σtrial(inner_folds[trial]) + planned_refit_fits
maximum_budgeted_fits =
  Σtrial(max_attempts[trial] × inner_folds[trial])
  + max_champion_refit_attempts
actual_started_fits = count(fold_fit_started events) + started_refits
actual_completed_fits = count(fold_fit_completed events) + completed_refits
```

Report partial failed-fit runtime, retries, refits, pre-open evaluator attempts,
the one post-open evaluation, and evidence-finalization time as separate
planned/budgeted/observed fields; never label a retry ceiling as physical work
done.

It must distinguish HTTP timeout, run-planning SLO, node scale-up SLO,
candidate deadline, and whole-run wall-clock SLO. A Kubernetes
`activeDeadlineSeconds` value is a kill boundary, not performance evidence.

### Candidate terminal evidence

Every candidate record shows estimator, catalog, experiment spec, feature
contract, feature recipe, split, search-space, and objective revisions,
constructor recipe, input/sample
lineage, selected/rejected feature evidence, database and Ray attempt history,
Ray job/trial/Train identities, worker class, requested/actual resources,
start/end/deadline, validation metrics, artifacts, serialization result,
inference compatibility, planned/budgeted/started/completed fold-fit counts,
partial failed-fit runtime, and final reason. The evaluation-scope record, not an
individual run, separately shows the global champion, frozen pipeline digest,
refit attempt/backend/tier/row digests, one-shot final-test result,
preregistered acceptance threshold, and sealed state.

### Gate

- Every applicable catalog estimator succeeds on the frozen principal or
  task-conformance ML fixture of recorded actual size using full data or
  disclosed sampling. The 10 GiB systems corpus is not ML-quality evidence.
- Automated feature engineering rejects unsafe fixtures, produces a versioned
  recipe, and meets the preregistered raw-baseline, stability, and compute
  threshold without lowering safety gates. If no generated feature qualifies,
  it selects the raw or smaller safe recipe and reports zero accepted additions.
- Catalog diff tests detect dependency-driven changes.
- Catalog contract tests cover representation/dtype/target/missingness/minimum-
  sample preconditions; ordinary-sklearn trials enforce recursive `n_jobs=1`,
  frozen BLAS/OpenMP caps, and process recycling without residual RSS growth
  beyond the signed bound.
- Fifteen all-catalog planners become active within two minutes.
- Workers and nodes reach calculated capacity within ten minutes.
- Retry, eviction, OOM, deadline, unschedulable, image-pull, and GPU fallback
  failures populate a stable terminal-reason code, failing resource, last
  checkpoint, retry owner/count, remediation reference, and correlated event
  IDs; no required terminal field is empty.
- No candidate is omitted or reported as full-data when sampled.
- No candidate worker or leaderboard process can read raw data or final-test
  inputs/labels. Exactly one evaluator attempt may CAS-commit a result for a
  sealed `evaluation_scope`; the task-specific preregistered final-test metric
  must pass its go/no-go bound, and failure cannot reopen selection.
- Evaluator failure before `test_opened_at` can be fenced and resubmitted;
  failure afterward either registers the already signed result digest or seals
  the scope failed without issuing another credential or rereading the test.
- Tune search and distributed Train runs restore after worker, driver, head, and
  cluster loss without duplicate terminal writes.
- Search restore replays canonical PostgreSQL events to the same suggestion
  sequence and produces exactly one `tell` observation per result digest.
- The scope-level refit restores independently, preserves its frozen data-tier
  and row digests, and publishes at most one pipeline manifest before evaluator
  credentials can exist.
- The signed provider conformance matrix refits, serializes, reloads, and
  evaluates every execution-backend class; all outputs pass the frozen bundle
  parity corpus and prediction/metric tolerance, remain quarantined, and cannot
  request a release final allocation.

### Rollback and requalification

Stop new claims, allow or cancel active candidates by policy, preserve completed
artifacts, and roll coordinators/workers back only across compatible candidate
schemas. Catalog, recipe, runtime lock, sample policy, benchmark profile,
resource class, worker image, feature generator/filter, searcher, validation,
Ray/KubeRay version, cluster topology, or autoscaling changes invalidate
applicable training evidence.

## Phase 5: Portable Helm runtime

**Depends on:** Phases 0 and 0A for scaffolding; its final gate depends on completed
Phase 1–4 static and dynamic workload contracts.
**Unblocks:** Phase 3 after the platform bootstrap gate; Phases 6–13 only after
the final gate.
**Primary owners:** platform engineering and security.

### Purpose

Make one strict, secure Helm release render the same application contract across
managed providers and local conformance targets.

### Platform bootstrap milestone

Complete this milestone after Phase 0A and before Phase 3. Install the pinned
KubeRay operator and CRDs through the same Sceptre Helm chart, enabled by
default and scoped to the release namespace. Freeze the operator namespace/RBAC,
conversion and rollback procedure, Ray image digest, embedded-cluster templates,
network baseline, autoscaler ownership, and project-and-stage workload-identity
pattern. A cluster-owned compatible operator is supported only through an
explicit chart opt-out. Phase 5 remains open until the final conformance gate
validates the completed Phase 1–4 workload contract.

**Bootstrap gate:**

- The signed bootstrap profile matches the installed operator/CRD/Ray image
  digests and records successful CRD conversion plus operator rollback.
- A non-production `K8sJobMode` RayJob with embedded `rayClusterSpec` and
  `backoffLimit: 0` can be created, observed, fenced, cancelled, autoscaled, and
  deleted with zero finalizer residue.
- Splitter and preparation delegated credentials pass their allowed-prefix
  tests and are denied another attempt within the same project plus an
  unauthorized project/stage prefix.
- Ray-worker demand scales Ray pods, pending pods trigger only the Kubernetes
  node autoscaler, and no second controller changes Ray worker counts.

### Work

- Set `additionalProperties: false` throughout the Helm JSON schema and add
  conditional validation.
- Add explicit `production` and `evaluation` modes plus negative production
  fixtures.
- Add Gateway API `Gateway` and `HTTPRoute`. Remove `className: nginx` from every
  production fixture and forbid the
  [retired ingress-nginx controller](https://kubernetes.io/blog/2025/11/11/ingress-nginx-retirement/)
  in production. If Ingress remains for evaluation compatibility, name a
  supported controller, owner, and removal date.
- Pin Gateway API CRDs, controller, `GatewayClass`, provider policy CRDs, and
  conformance profile; test upgrades and API conversion.
- Create separate service accounts for API, migrations, inference,
  reconciliation, backup, and telemetry. Dynamically provision
  project-and-stage-scoped base service accounts and broker/issuer bindings for
  splitter/verifier, preparation, training, champion refit, and champion
  evaluation, plus isolated provider-stage conformance identities. Head and
  workers receive only an attempt-scoped delegated grant:
  splitter may read its one raw object; preparation/training/refit cannot read
  raw or final prefixes; only the evaluator may read its scope's final inputs
  and labels after the allocation CAS. Delete attempt pods/tokens during
  terminal cleanup and disable project issuers on project deletion or security
  revocation.
  Provision and verify these bindings during project setup, before run
  admission, so cloud-IAM propagation is not hidden inside the 120-second start
  SLO. Attempts receive short-lived pod tokens plus brokered object credentials
  for the exact attempt; Phase 0A measures principal/service-account quotas,
  delegation latency, expiry, and revocation.
- Pin the KubeRay operator and custom resource definitions as an enabled-by-
  default dependency of the Sceptre Helm chart. Define install and upgrade
  ordering, conversion tests, namespace scope, and least-privilege permissions
  for `RayCluster`, `RayJob`, and their status/finalizer paths. Permit an
  external compatible operator only through an explicit chart value. This
  release does not use `RayService`.
- Replace the hand-built static Ray head and worker Deployments with one
  reconciler-created `RayJob` and embedded ephemeral `RayCluster` per splitter,
  preparation, training-run, champion-refit, or champion-evaluation attempt. Ray
  Use the same template for typed provider-stage conformance attempts. Ray
  autoscaling is the only controller allowed to set any such cluster's worker
  counts.
- Apply Restricted Pod Security: non-root, seccomp, read-only root filesystem,
  bounded writable `emptyDir`, dropped capabilities, and no unnecessary service
  account token.
- Make dynamically generated training and inference resources inherit the same
  security, labels, topology, affinity, toleration, pull, runtime class, and
  NetworkPolicy contracts as static resources.
- Keep Ray Jobs, Dashboard, Client, GCS, metrics, and management ports private.
  Permit `RayJob` CR create/delete only from the reconciler, operator mutation
  only from the pinned KubeRay controller, and internal dashboard submission
  only from KubeRay's stage-scoped `K8sJobMode` submitter. No application or
  tenant calls the Ray Jobs API directly. Disable or restrict the dashboard in
  production and test every NetworkPolicy denial.
- Add default-deny ingress and egress with explicit DNS, IdP, database, object
  store, MLflow, registry, metrics, telemetry, and provider identity paths.
- Add probes, graceful termination, progress deadlines, PDBs, topology spread,
  and rollout strategy to every long-running workload.
- Run migrations as an explicitly ordered deployment stage that must succeed
  before new application pods roll out. Do not rely on an unordered ordinary
  Job that can race the Deployment.
- Use external PostgreSQL, object storage, and MLflow in production. Run at least
  three stateless MLflow replicas backed by external database and object store.
- Repair smoke tests for disabled bundled services and external dependencies.
- Add optional `ServiceMonitor`, `PrometheusRule`, and OpenTelemetry Collector.
  Render KubeRay application resources when the chart-managed pinned operator
  is enabled or an explicitly declared compatible external operator is present.
- Keep all inference traffic behind the authenticated project gateway; forbid
  public per-model Services or Ingress in production.
- Include Kubernetes version and API-deprecation checks for the selected common
  supported minors.
- Generate the signed platform BOM schema and common/staging profile covering
  Kubernetes, the KubeRay operator and CRDs, Ray images, Gateway/controller,
  CNI, node autoscaler, CSI, policy engine, and OpenTofu module revisions.
  Provider phases instantiate and sign environment-specific BOMs. Assign
  install, upgrade, conversion, rollback, and emergency-removal ownership to
  each component.

### Gate

- `helm lint`, strict values validation, kubeconform, conftest, RBAC `can-i`, and
  negative production fixtures pass.
- Restricted Pod Security admission accepts every generated workload.
- Install, upgrade, rollback, and uninstall leave no unmanaged application
  resources.
- Production rendering contains no plaintext credential, bundled stateful
  service, mutable image tag, direct public inference route, or local object
  proxy.
- Production rendering contains no Dask resource, static Ray head/worker
  Deployment, public Ray endpoint, or competing Ray-worker autoscaler.
- KubeRay CRD conversion and operator upgrade/rollback tests preserve active
  status/finalizers, and the common/staging BOM digest matches its deployed
  components.

### Rollback and requalification

Use atomic Helm upgrades and keep previous compatible artifact manifests.
Cluster-version, Gateway implementation, CNI, Helm schema, workload security,
service-account, dynamic-resource template, or operator version changes require
re-rendering and the relevant security/upgrade tests.

## Phase 6: Identity, security, and serving hardening

**Depends on:** Phases 1 and 5.
**Unblocks:** public access and Phase 13 security qualification.
**Primary owners:** security, identity, backend, and serving.

### Purpose

Make tenant, identity, artifact, and serving boundaries fail closed under normal
operation and hostile input.

### Work

- Use OIDC-only interactive authentication in production. Keep local accounts
  only in evaluation profiles.
- Expose runtime auth capabilities so registration/password UI disappears in
  OIDC-only environments.
- Validate JWT issuer, audience, key ID, expiry, and asymmetric signatures;
  support key rotation, refresh-family reuse revocation, and short lifetimes.
- Use Argon2id with rehash-on-login for retained local users.
- Exchange password-reset secrets once and remove them from browser URL/history
  before rendering.
- Add single-flight browser refresh to prevent token-rotation races.
- Add scoped OAuth/M2M clients for automation and model APIs. Never expose
  HttpOnly browser tokens to JavaScript.
- Complete member removal, role update, invitation revocation, ownership
  transfer, and project lifecycle APIs.
- Fix owner-invite, share-link-use, viewer-compute, and member-email
  authorization gaps.
- Emit semantic, tamper-evident audit events for membership, upload, training,
  promotion, deployment, prediction administration, cleanup, and governance.
- Assign distinct workload identities and least-privilege object prefixes to
  migrations, API, reconciliation, splitter/verifier, Ray preparation, Ray
  training, champion refit, champion evaluation, credential brokerage, MLflow,
  provider-stage conformance, `ReleaseFinalTestAuthority`, inference, backup,
  and audit delivery. Never grant a generic project Ray
  object identity. The training/refit clusters cannot read raw or final-test
  objects, and the evaluator cannot mutate training evidence.
- Remove full application database credentials from training Jobs. Use a
  restricted worker role or authenticated callback API.
- Treat code inside each per-attempt Ray cluster as trusted server-generated code,
  not a sandbox for hostile payloads. Block client-controlled `runtime_env`,
  entrypoints, code URIs, Python callables, pip/conda packages, imports, and
  arbitrary expressions. The per-attempt cluster, project-and-stage identity,
  network policy, and object prefix form the cross-project and raw/test
  execution boundary.
- Resolve every object URI through a project-scoped storage descriptor. Reject
  arbitrary schemes, hosts, local paths, and redirects to prevent server-side
  request forgery. Bound parser CPU, memory, decompression, row width, nesting,
  and malformed-input retries.
- Add poisoning and integrity checks for benchmark data, feature definitions,
  checkpoints, metrics, model-selection evidence, and registry promotion.
- Sign model artifacts with release/workload identity and verify producer,
  digest, runtime lock, and project before deserialization.
- Prefer non-executable model formats where supported. Treat signed Joblib and
  pickle artifacts as executable code, isolate deserialization, and never load
  an artifact from an untrusted producer or project.
- Bundle the signed feature contract and recipe with the model. Reject missing,
  unknown, mistyped, unavailable, or oversized inference inputs and prove
  offline/serving feature parity against the versioned parity corpus and
  per-output exact/tolerance policy.
- Complete a formal threat model for upload/object storage, PostgreSQL/outbox,
  Ray control plane, object spill, checkpoints, MLflow, inference, OIDC, CI/CD,
  and qualification evidence. Assign an owner and mitigation to every high
  risk before Phase 13.
- Freeze a versioned threat-model methodology before scoring: enumerate every
  data-flow-diagram process, store, external actor, data flow, and trust boundary
  and assess all six STRIDE categories as applicable. Score likelihood and
  impact from 1–5 using signed rubrics; `risk_score = likelihood × impact`, and
  treat score ≥15 or impact 5 as high. A methodology/reclassification change
  creates a new reviewed revision. Accepting a high risk requires the security
  owner, affected data/service owner, and release approver, with compensating
  controls and expiry no later than 90 days.
- Enforce the Phase 0A transport-security matrix: validate TLS/mTLS peers and
  trust roots for database, object, MLflow, registry, telemetry, and supported
  Ray control/data paths; rotate certificates without disabling verification.
  Keep every Ray endpoint private even when encrypted. Any pinned-Ray plaintext
  exception requires an explicit, expiring high-risk acceptance and a
  node/network trusted boundary that cannot contain workloads from another
  project.
- Define data classification, lawful purpose, region pinning, subprocessor
  inventory, data-subject export/deletion, deletion SLA, legal hold, backup
  deletion, and cryptographic key-lifecycle requirements.
- Implement deletion with durable tombstones plus cryptographic erasure for
  project/retention-cohort data-encryption keys. Delete active copies and access
  bindings immediately under the approved SLA; retain immutable backups only
  until their declared expiry. A restore stays quarantined while tombstones are
  replayed and erased keys are denied, then a second inventory proves deleted
  data cannot reappear. Record legal-hold scope/expiry, DSAR identity, residency,
  key rotation, backup expiry, and restore-then-redelete evidence.
- Encrypt and isolate Ray object spill, temporary volumes, checkpoints, and
  logs. Clean them when a run, tenant, node, or cluster terminates.

Governed serving classes:

| Class | Replicas | HPA | Resources per pod |
| --- | ---: | --- | --- |
| Small | 2–10 | CPU 60%, memory 70% | 500m CPU / 1 GiB |
| Medium | 2–20 | CPU 60%, memory 70% | 1 CPU / 4 GiB |
| Large | 3–30 | CPU 60%, memory 70% | 2 CPU / 8 GiB |

### Gate

- Every role/action pair has positive and negative tests.
- Cross-project database, object, artifact, log, and inference access fails.
- Same-project cross-attempt and cross-scope object access fails; a replacement
  attempt can read only explicitly allowlisted predecessor checkpoints, and a
  second evaluator credential cannot be minted after `test_opened_at`.
- Non-canonical provider and provider-conformance identities cannot resolve,
  decrypt, or open a release final allocation. Concurrent central-authority
  requests produce one canonical open/commit sequence and signed denials for
  every other provider.
- OIDC MFA, revocation, JWKS rotation, logout, reset leakage, CSRF, refresh race,
  and account-linking tests pass.
- Direct unauthenticated inference is impossible.
- Tampered or untrusted-producer artifacts fail before deserialization.
- Offline and serving outputs preserve ordered feature names/types and pass
  every exact/tolerant feature and prediction oracle, including all frozen
  boundary cases; `batch_only` online requests fail with the expected code.
- Ray management endpoints reject public and tenant access; malicious runtime
  environment, code, expression, parser, and parameter payloads fail closed.
- Every endpoint marked TLS/mTLS in the transport-security matrix rejects an
  unknown CA, wrong identity/SAN, expired certificate, and plaintext downgrade;
  rotation preserves verified connectivity within its frozen error-rate
  threshold. Any explicitly accepted Ray-path exception passes its dedicated
  isolation/negative tests and remains visible in the signed risk register.
- Tenant deletion removes active raw uploads, prepared data, samples, feature
  artifacts, Ray spill/checkpoints, Tune state, MLflow, models, replicas, and
  governed logs within the approved SLA. Retained backups cannot decrypt the
  erased tenant/cohort data and expire on schedule; PITR cannot expose data
  before tombstone replay.
- Restore-then-redelete, legal-hold release, residency, DSAR, and key-rotation
  tests satisfy the signed deletion policy before restored services leave
  quarantine.
- Every DFD element and applicable STRIDE category has a disposition, and the
  count of unaccepted high-risk items under the signed scoring revision is zero.
  Accepted high risks carry all three required approvals, compensating control,
  and an expiry within 90 days.
- During node and zone loss, authenticated serving remains within the frozen
  availability/error-rate SLI threshold and records zero integrity or
  cross-project failures.

### Rollback and requalification

Keep prior trusted signing roots and compatible JWT keys through the rollback
window. Never roll back to a release that reopens a known authorization or
deserialization vulnerability. Identity provider, claim mapping, token/session,
role matrix, signing policy, model runtime, feature contract, or Gateway route
changes invalidate the relevant security evidence.

## Phase 7: Observability, reliability, and recovery

**Depends on:** Phases 1 and 5; instrument Phases 2–4 before load testing.
**Unblocks:** SLO, chaos, recovery, and provider qualification.
**Primary owners:** SRE, platform, data services, and application teams.

### Purpose

Make every stage diagnosable, alertable, reconcilable, restorable, and operable
within the production SLO/RPO/RTO contract.

### Work

- Emit structured JSON logs with request, user, project, dataset, run,
  candidate, database attempt, Ray cluster/job/trial/Train run, and trace
  identifiers.
- Propagate OpenTelemetry context through API, outbox, workers, Kubernetes Jobs,
  KubeRay, Ray Data/Tune/Train, object operations, MLflow, and inference.
- Export metrics for HTTP, authentication, database pools, upload bytes/retries,
  session age, object failures, queue depth/age, leases, candidate duration and
  status, feature generation/rejection, sampling, Ray object-store memory,
  reusable-feature cache hits/misses, object reads, shuffle/spill, pending
  workers/nodes, autoscaling, inference, and reconciliation.
- Create dashboards for control plane, ingestion, preparation, training,
  catalog completion, serving, capacity, and provider dependencies.
- Monitor ML health by deployed model/feature-contract revision: offline versus
  serving transform skew, feature missingness/range/category drift, prediction
  and confidence drift, delayed-ground-truth performance and calibration, and
  online-state freshness. Freeze alert thresholds, minimum sample sizes,
  immutable reference-distribution revision/window, statistical test and effect
  size, multiple-feature correction, task-specific ground-truth cohort,
  label-maturity/censoring rule, investigation owner, rollback rule, and
  retraining trigger without using the locked final test as a monitoring
  dataset.
- Alert on SLO burn, upload stalls, checksum mismatch, old queue entries, lost
  heartbeat, catalog failure, unschedulable work, slow node scale-up, dependency
  failure, certificate expiry, audit delivery loss, and backup failure.
- Implement deployment and monitoring reconcilers; execute database-stored
  monitoring schedules.
- Configure managed PostgreSQL HA, PITR, encrypted backup, pooling, failover
  alarms, and automated restore.
- Configure object versioning/soft deletion, lifecycle cleanup, retention, and
  backup replication where required.
- Move audit retention and purge to a durable scheduled workflow. Do not perform
  synchronous purge on every API startup. Enforce 30-day searchable logs and
  365-day audit retention unless the approved data policy is stricter.
- Implement and version the datastore-specific RPO/RTO matrix in Section 2.7
  for PostgreSQL, raw/prepared object data, feature registry, Tune/Train
  checkpoints, MLflow metadata/artifacts, model registry, audit, and
  qualification evidence. A database-only restore is insufficient.
- Back up and restore the isolated release final-allocation authority separately;
  while it is unavailable or its signed event chain is incomplete, deny fixture
  distribution, evaluator grants, final reads, and promotion.
- Back up MLflow metadata and verify database/artifact consistency.
- Persist signed evidence manifests with file digests to an immutable or
  write-once external audit destination. Database paths alone are not evidence
  integrity.
- Publish runbooks for dependency outage, node exhaustion, stuck upload, failed
  candidate, catalog regression, certificate expiry, database failover, object
  corruption, release rollback, and tenant security incident.
- Run restore exercises at least quarterly.
- Drill Ray worker, driver, head, operator leader, node, ephemeral `RayCluster`,
  Kubernetes cluster, object-store, and database loss independently. The
  application reconciler owns Ray-workload replacement; platform/OpenTofu and
  on-call own Kubernetes-cluster reconstruction before application replay.
  Verify fenced resubmission, remote restore, identity recreation, and cleanup
  of partial placements and spill.
- Define alert ownership, escalation, acknowledgement, and evidence retention.
  The provisional qualification profile requires critical-rule detection ≤2
  minutes, notification ≤1 minute later, acknowledgement ≤15 minutes, and clear
  ≤5 minutes after recovery; warning rules use ≤5 minutes, ≤5 minutes, ≤4 hours,
  and ≤15 minutes respectively. Deterministic drills allow zero missed or
  duplicate notifications, and the 72-hour soak allows at most one unexplained
  false page per rule. Phase 0A may tighten these values before approval.
- Define monthly service level indicators (SLIs), error-budget calculations, and
  measurement windows for the 99.9% objective. Treat the 72-hour soak as one
  reliability input, not proof of a monthly availability target.
- Keep metric labels bounded. Put user, dataset, run, candidate, and trial IDs in
  traces or logs rather than high-cardinality metric labels.

### Gate

- One distributed trace follows upload → prepare → train → register → deploy →
  predict.
- Every alert meets its signed detection, notification, acknowledgement, clear,
  duplicate, and false-positive thresholds during drills and soak.
- Database failover loses or duplicates no queued work.
- Every Section 2.7 durability row passes its named RPO/RTO or acknowledged-write
  loss threshold and test method; retention-only predicates pass their boundary
  queries. PostgreSQL/MLflow metadata specifically achieve RPO ≤15 minutes and
  RTO ≤4 hours.
- Ray head and ephemeral-RayCluster loss recreate work from durable state under
  the workflow deadline without lost lineage, duplicate final evaluation, or
  stale-fence writes. A separate Kubernetes-cluster-loss drill restores the
  signed platform profile, identities, services, and workflow replay within the
  regional application service RTO.
- Release-authority loss restores with zero acknowledged state/event loss within
  four hours, then replays assignment/open/commit receipts and the three-
  provider race test before any final credential can be issued.
- Reconciliation reports zero orphan Kubernetes and object-store resources.
- ML-health fixtures trigger and clear every frozen skew/drift/performance alert,
  reproduce corrected statistical decisions under mature/censored label
  cohorts, and record the rollback/retraining action required by policy.

### Rollback and requalification

Telemetry failure must not corrupt workflows, but audit-delivery loss must fail
closed where policy requires. Keep dashboard/alert/runbook versions with the
release. Schema, collector, backend, retention, alert threshold, database HA,
backup, object lifecycle, or reconciliation changes require targeted drills and
new evidence.

## Phase 8: Hermetic CI/CD and candidate build

**Depends on:** Phases 0 and 0A for pipeline scaffolding; its final gate depends on the
immutable artifact definitions and tests from Phases 1–7.
**Unblocks:** deployable immutable candidates for Phases 9–13.
**Primary owners:** release engineering, platform, and security.

### Purpose

Build once, prove provenance, and produce one immutable candidate digest set for
local and provider qualification. Phase 13 alone signs qualification status and
changes the stable production channel.

### Work

- Pin GitHub Actions by commit and base images by digest.
- Generate hashed Python locks per runtime and enforce `npm ci` with the
  committed lock.
- Standardize preparation/trainer/inference Python, Ray, PyArrow, Polars, and
  model-library ABI.
- Split lightweight control/API images from ML training images.
- Build the isolated qualification-control service/tooling hermetically, sign
  its image and receipt-schema revision, and include its digest in the artifact
  manifest. It is release-board infrastructure, not a tenant or FastAPI route,
  and its database/credentials remain outside provider application charts.
- Build UI, API, inference, and standard CPU training for AMD64 and ARM64.
  Keep Intel/NVIDIA images AMD64 with scheduling guards.
- Correct NVIDIA version injection and make every image non-root.
- Build helper images hermetically or consume verified upstream digests.
- Generate SPDX and CycloneDX SBOMs, SLSA provenance, vulnerability/license
  reports, and keyless cosign signatures.
- Freeze supply-chain policy: zero known-exploited vulnerabilities and zero
  unwaived exploitable critical/high findings in released artifacts; an explicit
  license allow/deny list; remediation SLAs by severity; and a waiver schema
  with owner, rationale, compensating control, approver, and expiry. Admission
  rejects a missing/invalid signature, provenance, SBOM, platform BOM, prohibited
  license, expired waiver, or disallowed vulnerability.
- Run repository, image, rendered-manifest, and history secret scanning. Block
  confirmed findings and rehearse credential revocation and rotation before
  release.
- Enforce signatures and approved provenance at cluster admission.
- Replace automatic patch guessing with one immutable artifact manifest
  containing embedded artifact version, Git SHA, chart digest, image digests,
  Python ABI, dependency lock, Ray/Polars versions, catalog,
  feature-contract/registry schema revisions, platform-compatibility contract
  revision, canonical qualification suggestion-log/workload revisions, and
  migration range. It contains no mutable qualification status or provider BOM
  that does not yet exist.
- Define and test the qualification-attestation and release-channel-pointer
  schemas, but do not create a final attestation or publish the stable pointer.
- Promote the same candidate digest set through development, staging, local
  conformance, and provider qualification environments.
- Use protected GitHub environments and provider OIDC. Store no static cloud key
  in GitHub. OIDC supplies identity, not private network reachability: run
  deploy/qualification jobs on ephemeral, outbound-only self-hosted runners in
  or privately peered to each cloud network, with a separately bootstrapped
  runner identity and automatic teardown.

Required workflows:

```text
pull-request.yml       lint, tests, contracts, chart policy, kind/k3d E2E
build-release.yml      build once, scan, sign, publish candidate manifest
infra-bootstrap.yml    protected state backend, CI federation, runner network
infra-plan.yml         OpenTofu fmt/validate/security/plan
infra-apply.yml        protected-environment approval and apply
deploy.yml             atomic Helm deployment of immutable manifest
qualify.yml            provider conformance/performance/chaos suite
rollback.yml           application rollback when schema compatibility permits
```

### Gate

- Two hermetic builds from the same source, lockfiles, toolchain, and build
  inputs produce identical deployable artifact digests; the comparison report
  lists every digest and contains zero unexplained difference.
- Every running image is digest-referenced and signature-verified.
- The qualification-control service runs the artifact-manifest digest and its
  allocation/receipt schema passes concurrent cross-provider CAS and recovery
  tests before any locked fixture is distributed.
- Vulnerability and license policy passes with zero prohibited finding, and the
  platform-compatibility contract/BOM schemas are signed and admission-tested.
- The candidate manifest and digest set deployed to staging are immutable and
  ready for provider qualification; this phase creates no qualification
  attestation and changes no stable production pointer.
- Failed rollout returns automatically to the previous compatible release.
- Migration rollback limitations are explicit and rehearsed.

### Rollback and requalification

Rollback selects a prior signed artifact manifest and approved qualification
attestation; it never rebuilds an old tag.
Do not roll application code behind an incompatible contract migration. Source,
lock, Action, base image, build configuration, SBOM policy, signing identity,
admission policy, chart, artifact manifest, or attestation changes invalidate supply-chain
evidence.

## Phase 9: Local runtimes and Railway

**Depends on:** Phases 0, 0A, and 1–8 for the full workflow; individual preflight work may
begin after Phase 5.
**Unblocks:** portable functional confidence before managed-cloud qualification.
**Primary owners:** developer experience and platform engineering.

### Purpose

Prove provider-neutral functional behavior on common local clusters and test
Railway data-service adapters without misrepresenting Railway as Kubernetes.

### Local work

- Provide one `sceptrectl local` interface with `preflight`, `create`,
  `load-images`, `install`, `verify`, `upgrade`, and `destroy`.
- Pin local runtime and Kubernetes versions in the repository.
- k3d: three-node configuration, local registry or explicit image import,
  metrics-server, and Envoy Gateway.
- Minikube: Docker driver, three nodes, metrics-server, storage add-on, explicit
  image loading, and CPU-only default.
- MicroK8s: pinned snap channel, `dns`, `hostpath-storage`, and `metrics-server`
  add-ons; install a pinned conformant Gateway controller/CRDs separately;
  import images into containerd and document node-local storage warnings.
- kind: three-node CI target with explicit image loading, storage, metrics, and a
  Gateway implementation.
- Use port-forwarding as the universal exposure fallback.
- Bundle SeaweedFS/MinIO, PostgreSQL, and MLflow only in evaluation mode.
- Install the chart-pinned KubeRay operator through the same Sceptre release and
  use the same Ray custom-resource contract as managed clusters, with smaller
  CPU-only worker limits.
- Add a single-node evaluation profile for an 8-core, 24 GB workstation. Limit
  Ray plus application memory to a measured safe bound, enable spill to an
  explicit workspace path, stream Polars batches, and publish the largest
  qualified local workload. Do not claim the production concurrency or deadline.
- State prominently that local environments do not meet the 15-user production
  capacity certification.

### Railway work

- Do not call the target “Railway Kubernetes.” Railway maps services rather than
  exposing a Helm-managed Kubernetes cluster. See the
  [Railway Compose model](https://docs.railway.com/guides/docker-compose).
- Use Railway GraphQL only to create optional non-production PostgreSQL/bucket
  contract environments.
- Validate `s3_compatible` behavior against Railway Buckets and database
  TLS/pooling against Railway PostgreSQL.
- Use the environment-scoped Railway project token for routine contract tests,
  honor API rate limits, redact it everywhere, and never place it in the
  application. Pre-create the environment or use a one-time workspace-token
  bootstrap from a protected infrastructure workflow, then revoke that broader
  token.
- Railway Buckets do not provide the production lifecycle/versioning/object-lock
  controls required here. Mark those capabilities false, rely on application
  abort/reconciliation for contract cleanup, and never qualify Railway storage
  as a production data plane. Track the current
  [Railway Bucket limitations](https://docs.railway.com/storage-buckets) in the
  contract fixture.
- Do not run Sceptre API, training workers, or inference controller on Railway
  in this release; they require Kubernetes workload APIs.
- Require a separate ADR and qualification before adding a Railway execution
  backend.

### Gate

- Fresh create → install → login → resume upload → profile → CPU train → deploy
  internally → predict → upgrade → uninstall passes on k3d, kind, Minikube, and
  MicroK8s.
- The 8-core, 24 GB profile processes the approved 10 GB/10 GiB local fixture
  without full-data pandas conversion, unbounded collection, or host OOM.
- Railway's declared basic storage and database contracts pass; unsupported
  lifecycle/versioning/object-lock capabilities are reported false and are not
  treated as successful production controls.
- Documentation clearly marks core Railway deployment unsupported.

### Rollback and requalification

Destroy/recreate is the recovery path for disposable local clusters; retained
data requires an explicit backup first. Runtime, Kubernetes minor, Gateway,
storage provisioner, image-loading method, Helm contract, Railway database, or
Railway bucket behavior changes require that target's conformance suite again.

## Phase 10: AWS EKS implementation and prequalification

**Depends on:** Phases 0, 0A, and 1–8; Phase 4 capacity model and reference profile must be frozen.
**Unblocks:** EKS portion of Phase 13.
**Primary owners:** AWS platform, security, SRE, and release engineering.

### Provision

- Separate AWS accounts for development, staging, and production.
- Three-AZ VPC with private worker subnets, controlled egress, required VPC
  endpoints, and private EKS API access from ephemeral self-hosted runners
  inside or privately peered to the VPC.
- EKS Standard cluster with deletion protection, control-plane audit/API/
  authenticator/controller/scheduler logs, access entries, and encrypted
  Kubernetes secrets.
- A three-node managed system pool.
- Karpenter-managed CPU, high-memory, pairwise, and optional GPU pools with
  taints, disruption budgets, consolidation rules, and qualification maxima.
- Enable the chart-pinned KubeRay operator and map Ray worker groups to the
  approved Karpenter pools. Configure object-store memory, `/dev/shm`, encrypted spill,
  ephemeral-storage limits, placement constraints, and Ray autoscaler bounds.
- ECR repositories with immutable release policy and scanning.
- EKS Pod Identity association per service account; use the SDK default
  credential chain. See [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html).
- Install and monitor the Pod Identity Agent, use compatible SDKs, and provide
  the EKS Auth VPC endpoint in restricted private networks. CI uses GitHub OIDC;
  pods use EKS Pod Identity. Do not reuse either trust policy for the other.
- S3 with SSE-KMS, versioning, checksums, lifecycle, incomplete multipart
  cleanup, and per-prefix policies.
- RDS PostgreSQL Multi-AZ with TLS, PITR, alarms, pooling, and restore automation.
- EBS CSI for remaining PVCs.
- AWS Load Balancer Controller Gateway API, ACM, Route 53, and WAF.
- CloudWatch/ADOT and managed Prometheus/Grafana destinations.

### Autoscaling

Use Karpenter for candidate capacity. It must respond to pending pod compute,
memory, storage, topology, and acceleration requirements. Set pool limits from
the capacity calculation and quota evidence. See
[EKS autoscaling](https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html).

### Provider evidence

Archive redacted OpenTofu plan summaries, state-backend security/recovery evidence, access-entry and
Pod Identity mappings, S3 checksum/CORS/lifecycle policy, RDS failover/restore,
ECR/admission results, Gateway/TLS/WAF probes, node provisioning timelines,
quota records, and cost estimates.

Repeat the Phase 2 storage gate, Phase 3 preparation gate, Phase 4
catalog/resource calibration, identity/network negative tests, and a rehearsal
of the Phase 13 load shape. Freeze the signed AWS capacity profile and secure
20% quota headroom. This is provider prequalification, not final release
qualification.

Run the signed provider-stage conformance matrix through fenced refit and
evaluator RayJobs using only its synthetic holdout. Archive the quarantined
bundle/prediction evidence and prove the identities cannot reach the release-
final authority or prefixes.

### Gate

- No static AWS credential exists in pods, Kubernetes Secrets, Helm state,
  OpenTofu output, or GitHub.
- Private control plane, Pod Identity, S3, RDS TLS, ECR pull, Gateway/TLS/WAF,
  telemetry, backup, failover, and restore tests pass.
- Provider prequalification passes, the AWS capacity profile is frozen, quota is
  confirmed, and the environment is declared ready for Phase 13.
- Every refit/evaluator backend in the provider-stage conformance matrix passes
  serialization, parity, recovery, cleanup, and its frozen prediction/metric
  tolerance without release-final access.
- The signed AWS platform BOM/profile digest equals the deployed Kubernetes,
  KubeRay/CRD, Ray image, Gateway/CNI/autoscaler, CSI/policy, and OpenTofu module
  revisions recorded in provider evidence.

### Rollback and requalification

Application rollback uses the prior signed artifact manifest and attestation. Infrastructure
rollback uses reviewed OpenTofu plans and must not destroy stateful data. EKS,
Kubernetes minor, VPC, CNI, Karpenter, node type, Pod Identity, S3, RDS, ECR,
Gateway, WAF, or quota changes invalidate the affected EKS evidence.

## Phase 11: Google GKE implementation and prequalification

**Depends on:** Phases 0, 0A, and 1–8; Phase 4 capacity model and reference profile must be frozen.
**Unblocks:** GKE portion of Phase 13.
**Primary owners:** GCP platform, security, SRE, and release engineering.

### Provision

- Separate GCP projects for development, staging, and production.
- Private regional GKE Standard on a supported regular release channel.
- Dataplane V2, private nodes/control-plane access, private DNS, and ephemeral
  self-hosted deploy/qualification runners inside or connected to the VPC.
- A system pool with one node in each of three zones, three nodes total, plus
  autoscaled general, high-memory, pairwise, and optional GPU pools. Use total
  node-count settings; do not mistake a per-zone count of three for three total.
- Workload Identity Federation for GKE with distinct Google service accounts.
- Enable the chart-pinned KubeRay operator and map Ray worker groups to the
  approved GKE node pools. Configure object-store memory, `/dev/shm`, encrypted spill,
  ephemeral-storage limits, placement constraints, and Ray autoscaler bounds.
- Artifact Registry with immutable digest promotion.
- Native GCS driver, bucket versioning/retention/lifecycle, CMEK where required,
  and no HMAC keys.
- Regional HA Cloud SQL PostgreSQL with private IP, TLS, backup, PITR, and
  connector/pooling qualification.
- GKE Gateway API, Certificate Manager, Cloud DNS, and Cloud Armor.
- Secret Manager CSI or External Secrets.
- Cloud Logging, Cloud Monitoring, a Google-built or self-managed OpenTelemetry
  Collector, and Managed Service for Prometheus. Managed OpenTelemetry for GKE
  is currently [Preview](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/managed-otel-gke)
  and may replace the collector only after GA qualification or an approved
  pre-GA exception with a tested fallback.

### Autoscaling

Set per-pool total minimum and maximum nodes from the capacity formula. Keep 25%
warm capacity and prove scale-up from it. See
[GKE cluster autoscaler](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-autoscaler).

### Provider evidence

Archive redacted OpenTofu plan summaries, state-backend security/recovery evidence,
Workload Identity bindings, GCS
resume/checksum/CORS/lifecycle policy, Cloud SQL failover/restore, Artifact
Registry/admission results, Gateway/TLS/Armor probes, autoscaler timelines,
quota records, and cost estimates.

Repeat the Phase 2 storage gate, Phase 3 preparation gate, Phase 4
catalog/resource calibration, identity/network negative tests, and a rehearsal
of the Phase 13 load shape. Freeze the signed GCP capacity profile and secure
20% quota headroom. This is provider prequalification, not final release
qualification.

Run the signed provider-stage conformance matrix through fenced refit and
evaluator RayJobs using only its synthetic holdout. Archive the quarantined
bundle/prediction evidence and prove the identities cannot reach the release-
final authority or prefixes.

### Gate

- No service-account JSON key exists.
- Private networking, Workload Identity, native GCS resume, Cloud SQL failover,
  Gateway/TLS/Armor, telemetry, backup, and restore tests pass.
- Provider prequalification passes, the GCP capacity profile is frozen, quota is
  confirmed, and the environment is declared ready for Phase 13.
- Every refit/evaluator backend in the provider-stage conformance matrix passes
  serialization, parity, recovery, cleanup, and its frozen prediction/metric
  tolerance without release-final access.
- The signed GCP platform BOM/profile digest equals the deployed Kubernetes,
  KubeRay/CRD, Ray image, Gateway/CNI/autoscaler, CSI/policy, and OpenTofu module
  revisions recorded in provider evidence.

### Rollback and requalification

Application rollback uses the prior signed artifact manifest and attestation. Infrastructure
rollback must preserve buckets, Cloud SQL, logs, and evidence. GKE/Kubernetes
version, release channel, Dataplane V2, node pool/type, Workload Identity, GCS,
Cloud SQL, Artifact Registry, Gateway, Armor, or quota changes invalidate the
affected GKE evidence.

## Phase 12: Azure AKS implementation and prequalification

**Depends on:** Phases 0, 0A, and 1–8; Phase 4 capacity model and reference profile must be frozen.
**Unblocks:** AKS portion of Phase 13.
**Primary owners:** Azure platform, security, SRE, and release engineering.

### Provision

- Separate Azure subscriptions, or strongly isolated resource groups, for
  development, staging, and production.
- Private, zone-resilient AKS Standard cluster mode on the Standard pricing tier,
  with a pinned upgrade channel, maintenance window, OIDC issuer, and Azure
  Workload Identity.
- Use private DNS and ephemeral self-hosted deploy/qualification runners inside
  or privately peered to the VNet; GitHub OIDC is the CI identity and Azure
  Workload Identity is the pod identity.
- Azure CNI Overlay with Cilium network policy.
- A three-node system pool plus autoscaled general, high-memory, pairwise, and
  optional tainted GPU pools.
- Enable the chart-pinned KubeRay operator and map Ray worker groups to the
  approved AKS node pools. Configure object-store memory, `/dev/shm`, encrypted spill,
  ephemeral-storage limits, placement constraints, and Ray autoscaler bounds.
- ACR with immutable release policy and digest deployment.
- Native Azure Blob block-upload driver using user-delegation SAS. Enable
  versioning, soft delete, lifecycle, and customer-managed keys where required.
- Azure Database for PostgreSQL Flexible Server with zone-redundant HA, TLS,
  backup, PITR, pooling, and restore automation.
- Azure Disk CSI for remaining PVCs.
- Application Gateway for Containers with Gateway API, DNS, and WAF. Listener
  certificates are Kubernetes TLS Secrets managed by cert-manager or another
  [supported mechanism](https://learn.microsoft.com/en-us/azure/application-gateway/for-containers/how-to-ssl-offloading-gateway-api);
  do not reference a Key Vault CSI object directly from the listener.
- Key Vault CSI or External Secrets with rotation for application secrets.
- Azure Monitor, Container Insights, Managed Prometheus, Grafana, and Log
  Analytics.

### Autoscaling

Enable the AKS cluster autoscaler per Ray worker node pool, with maxima from the
capacity calculation. Ray autoscaling controls Ray worker counts; the AKS
cluster autoscaler controls nodes. No KEDA or HPA target may control Ray workers.

### Provider evidence

Archive redacted OpenTofu plan summaries, state-backend security/recovery evidence,
federated identity credentials and role
assignments, Blob resume/checksum/CORS/lifecycle settings, PostgreSQL
failover/restore, ACR/admission results, Gateway/TLS/WAF probes, autoscaler
timelines, quota records, and cost estimates.

Repeat the Phase 2 storage gate, Phase 3 preparation gate, Phase 4
catalog/resource calibration, identity/network negative tests, and a rehearsal
of the Phase 13 load shape. Freeze the signed Azure capacity profile and secure
20% quota headroom. This is provider prequalification, not final release
qualification.

Run the signed provider-stage conformance matrix through fenced refit and
evaluator RayJobs using only its synthetic holdout. Archive the quarantined
bundle/prediction evidence and prove the identities cannot reach the release-
final authority or prefixes.

### Gate

- No client secret or storage account key exists in a workload.
- Private access, Workload Identity, native Blob resume, PostgreSQL failover,
  Gateway/TLS/WAF, telemetry, backup, and restore tests pass.
- Provider prequalification passes, the Azure capacity profile is frozen, quota
  is confirmed, and the environment is declared ready for Phase 13.
- Every refit/evaluator backend in the provider-stage conformance matrix passes
  serialization, parity, recovery, cleanup, and its frozen prediction/metric
  tolerance without release-final access.
- The signed Azure platform BOM/profile digest equals the deployed Kubernetes,
  KubeRay/CRD, Ray image, Gateway/CNI/autoscaler, CSI/policy, and OpenTofu module
  revisions recorded in provider evidence.

### Rollback and requalification

Application rollback uses the prior signed artifact manifest and attestation. Infrastructure
rollback preserves Blob, PostgreSQL, Log Analytics, and evidence. AKS/Kubernetes
version, CNI/Cilium, node pool/type, Workload Identity, Blob, PostgreSQL, ACR,
Gateway, WAF, or quota changes invalidate the affected AKS evidence.

## Phase 13: Final production qualification

**Depends on:** Phases 0, 0A, and 1–9 complete and EKS, GKE, and AKS independently marked
ready by Phases 10–12.
**Unblocks:** signing the final qualification attestation and publishing a
stable-channel pointer to that attestation; the attestation binds the immutable
`0.2.0` artifact manifest.
**Primary owners:** release approval board; tests are witnessed by engineers
other than the primary implementers.

### Purpose

Qualify the immutable `0.2.0` artifact manifest independently on EKS, GKE, and
AKS, then sign the candidate/final qualification attestation and make the
stable-publication decision. Provider independence covers platform and every
RayJob stage through synthetic conformance; it does not multiply access to a
locked release test.
The harness is generated from the Phase 0A signed decision/profile snapshot;
none of the provisional 15-run, 10-GiB, all-catalog, five-suggestion,
three-fold, four-task, local-profile, provider, or 7,200-second targets may
remain pending. If a target was rejected or superseded, update this guide and
the stable gate index before building the release candidate.

### Qualification fixture matrix

Freeze a signed fixture manifest and qualification ledger before the first
provider run. Each approved task entry contains fixture ID/version, object and
logical dataset digests, actual byte size, format/encoding/compression, schema,
row count, target, positive label, entity/event/cutoff/prediction-time columns,
feature contract, split revision, train/validation/final-test digests, allowed
sample tiers, preregistered baseline/non-inferiority/lift/stability/compute
thresholds, task-specific final-test go/no-go bound, sealed evaluation-scope ID,
canonical final-evaluation provider, central final-allocation ID, and generator
commit. It also binds the `ReleaseFinalTestAuthority` revision and signed
provider-distribution-manifest digest. Clustering records that no target exists.

Maintain three separate fixture purposes:

- The systems-load corpus has one base ingestion scenario per provider with 15
  fresh, exactly 10 GiB browser uploads and distinct object URIs. Only an
  explicitly listed ingestion-chaos repetition creates another fresh set. ML
  scenarios reference their frozen fixtures and do not repeat these uploads.
  The corpus proves transport, storage, preparation, and autoscaling only.
- The principal ML fairness benchmark is one immutable dataset version, target,
  row set, and split revision. All comparable model-plus-feature experiments
  reference it and vary only recipe or parameters.
- Additional task-conformance fixtures are immutable within each task. They
  prove classification, regression, time-series, or clustering behavior but are
  never compared across tasks or presented as the principal benchmark.

When QLT-003 is approved, designate the principal benchmark as one of those four
task fixtures. Its 15-run principal scenario is the homogeneous scenario for
that task, not an additional group. Together, four homogeneous groups and four
15-run mixed repetitions total exactly 120 logical training runs per provider.
Without QLT-003, the base is the 15 principal runs only.

Do not server-side copy or reuse prior objects for the systems upload gate. Do
not create a full physical ML dataset per experiment or trial.

Name users `q00` through `q14`. Homogeneous scenarios assign all users the named
task. A mixed repetition assigns contiguous groups of four to three tasks and a
group of three to the remaining task; rotate the three-user task through
classification, regression, time-series, and clustering in repetitions 0–3.
Run comparable principal-benchmark experiments in one qualification project per
provider so all 15 users there reference the same dataset version. Before any
provider result exists, designate one canonical final-evaluation provider and
bind each task's only promotional scope/final allocation in
`ReleaseFinalTestAuthority`. A trusted release-fixture splitter creates the
roles once. Distribute signed `validation_only` manifests containing only
train/validation objects to non-canonical providers; distribute locked final
objects only to the canonical provider and keep their decrypt/access policy
disabled except for the one central-authority-approved evaluator. Never send a
raw object containing locked roles to a non-canonical provider. All
non-canonical campaign runs are validation-only and create zero
`champion_refit_attempts` or `champion_evaluation_attempts`; their separately
typed synthetic provider-stage conformance attempts remain mandatory. Mixed
repetitions are also validation-only and cannot influence the preregistered
homogeneous-scope selection rule. Run separate negative tests across projects
to prove tenant isolation.

Execute both non-canonical provider campaigns first. On the canonical provider,
complete systems, mixed, chaos, recovery, and other prerequisite gates before
launching each 15-member homogeneous promotional scope. That scope selects one
global champion, refits, evaluates, CAS-commits, and finalizes evidence within
its own 7,200-second clock. Separate provider databases cannot authorize another
final read: the central authority's unique CAS state, canonical-only object
placement/KMS policy, and attempt grant together form the cross-provider
enforcement point. Archive all central signed assignment/open/commit/denial
receipts.
Freeze the exact user-to-task and user-to-fixture table in the manifest. Derive
logical experiment seeds from
`SHA256(artifact_manifest_digest + scenario + repetition + user_id)`. Provider
must not change rows, splits, or the signed canonical suggestion event log.
Keep provider only in execution evidence; quantized metrics and stable
tie-breaking govern parity comparisons.

The signed ledger states every scenario ID, repetition, upload, run, candidate,
expected fit, chaos action, soak cadence, retry rule, partial-rerun rule,
retention period, cleanup action, and approved cost ceiling. Never infer
campaign multiplication from prose during execution.

Before every scenario, compare committed plus forecast cost with the approved
remaining budget. The named qualification lead must stop before launch when the
forecast exceeds the ceiling. Only the budget owner may approve a larger ceiling
through a new signed ledger; partial results never authorize silent overrun.

Write evidence as
`<provider>/<scenario>/r<repetition>/<user_id>/<stage>.json`, with candidate
evidence nested by stable candidate ID. A rerun gets a new repetition/attempt
directory; never overwrite a failed attempt.

### Performance and failure scenarios

Replace the current qualification scripts rather than extending their obsolete
request shapes. The harness must use real authenticated browser upload behavior,
provider receipts, idempotency keys, a 15-user start barrier,
`catalog_mode=all`, one shared immutable benchmark reference for comparable ML
runs, and provider evidence receipts. The corpus generator trims its final chunk
and asserts exact byte size and digest before any upload.

For each managed provider:

1. Upload 15 independent, exactly 10 GiB uncompressed CSV files concurrently.
2. Interrupt browser/network transfer for five users and restart one UI and one
   API pod.
3. Verify all systems-load objects and prepare them without using them as
   model-quality evidence.
4. Execute 15 concurrent comparable runs against the immutable principal ML
   benchmark; these runs are its task's homogeneous group when QLT-003 applies.
5. If QLT-003 is approved, execute the other three 15-run homogeneous groups and
   four 15-run mixed repetitions against their frozen fixtures. Together with
   step 4, this is exactly 120 logical runs per provider.
6. Use the signed catalog and search-strength profile. If QLT-005/006/007 were
   accepted, this means every applicable estimator, five joint suggestions for
   each tunable entry, and three task-correct inner folds.
7. Start from 25% warm candidate capacity and record worker/node scale-up.
8. In separate repetitions, inject Ray worker, driver, head, operator-leader,
   and node loss, ephemeral-RayCluster recreation, full Kubernetes-cluster
   reconstruction, object throttling, and database failover.
9. On all three providers, repeat the signed synthetic provider-stage
   conformance matrix with the exact candidate: fenced refit and evaluator
   RayJobs must restore, serialize/reload, execute the bundle parity corpus, and
   match the canonical conformance outputs under the preregistered prediction/
   metric tolerance. Quarantine every artifact and prove zero release-final
   authority/prefix access.
10. Confirm each non-promotional 15-run group reaches terminal validation within
   7,200 seconds and every applicable candidate succeeds.
11. On the preregistered canonical provider only, close each homogeneous
    promotional scope within 7,200 seconds after all candidates, champion
    selection, scope-level refit, one final-test read/result, CAS, and evidence
    finalization. Require the task-specific metric bound to pass; failure is a
    release no-go and cannot trigger reselection or a rerun. Assert that every
    other provider and every mixed scenario created zero promotional champion-
    refit/evaluation attempts and had no final-object access.
12. Run a 72-hour soak with upload, preparation, training, prediction,
    monitoring, and cleanup traffic.
13. Exercise upgrade, compatible application rollback, database restore, object
    reconciliation, certificate rotation, and workload-identity rotation.

### Required evidence bundle

```text
artifact manifest and signatures
git, IaC, Helm, schema, and catalog revisions
requirements/decision register and approved cost ceiling
redacted provider configuration
capacity calculation and cloud quota evidence
upload throughput, pause/resume, retry, and checksum results
dataset, split, feature-contract, registry, and recipe digests
per-run, candidate, trial, and Ray attempt duration/resource/sample evidence
feature-cache, object-read, shuffle, spill, and materialization evidence
Prometheus metrics, distributed traces, and redacted log extracts
autoscaler and node-provisioning timelines
chaos, outage, and failover outcomes
security and tenant-isolation results
central final-allocation/distribution/open/commit/denial receipts
provider-stage refit/evaluator conformance and parity results
backup/restore RPO and RTO results
cost estimate and actual cost report
known exceptions with owner and expiry
unsigned go/no-go decision template with proposed disposition
```

This is the pre-decision evidence set. After approval, append the signed go/no-
go record, newly signed qualification attestation, stable-channel-pointer
identity, and publication receipt; they are outputs of Phase 13, never
prerequisites used to justify their own creation.

### Approval conditions

Production is approved only when:

- EKS, GKE, and AKS pass independently;
- every approved 15-run training group meets its applicable validation-only or
  promotional 7,200-second terminal predicate;
- no estimator is skipped, timed out, silently replaced, or mislabeled;
- automated feature engineering produces auditable selected/rejected evidence,
  meets the preregistered quality/stability/compute rule, preserves split
  isolation, and never weakens safety gates to reach a feature count;
- every comparable experiment uses the frozen benchmark and split digests, and
  only the dedicated champion evaluator reads final-test inputs/labels;
- each task has exactly one cross-provider final allocation and one
  CAS-committed canonical-provider result for its frozen global champion; every
  non-canonical and mixed campaign records zero final access;
- all three providers pass the signed synthetic refit/evaluator RayJob matrix,
  bundle parity corpus, and cross-provider validation prediction/metric
  tolerance with quarantined outputs;
- no upload corruption, tenant leak, lost work, orphan resource, unsigned
  artifact, or static cloud credential is found;
- upload, chaos/recovery, 72-hour soak, restore, and cleanup scenarios pass
  their own throughput, loss, SLI/RTO, retention, and orphan-count predicates;
- alerting, restore, upgrade, rollback, and operational runbooks are exercised by
  someone other than the implementer; and
- release approvers sign the evidence record for the exact immutable digest set.

After approval, create and sign the qualification attestation binding the
artifact manifest, all three provider BOM/profile and evidence digests, final
allocation/results, decision register, and approvers; then publish it and update
the stable channel pointer to the qualification-attestation digest. The
attestation references the existing `0.2.0` artifact-manifest digest. Change no
embedded artifact, package, chart, image, or model version during promotion.

### Rollback and requalification

Any failed approval condition is a no-go; keep the release candidate out of
production. A catalog, runtime, dependency lock, image, chart, migration,
Kubernetes/provider component, node type, capacity profile, security policy, or
test-fixture change requires requalification of every affected gate. A provider
pass never substitutes for another provider.

## Cross-phase test matrix

| Layer | Required scenarios |
| --- | --- |
| Unit | Driver behavior, deterministic splits/samples, feature contracts/generators/filters, Tune Searcher persistence, catalog construction, capacity math, retry/backoff, checksums, states, authorization |
| Database | Concurrent creation, scope barriers, uniqueness, leases, fences, Ray attempt/trial identity, checkpoints, dead letters, outbox replay, idempotency, central final-allocation CAS/receipt replay, upgrades, populated migrations, PgBouncer mode |
| Storage contracts | SeaweedFS/MinIO, AWS S3, GCS, Azure Blob, Railway Bucket: capability-aware progress/resume/complete/abort/tamper/expiry; lifecycle only where supported |
| API integration | Upload state machine, feature contract/registry/recipe revisions, typed overrides, all-catalog two-phase scope launch, stale revision, preflight blockers, cancellation, isolation, capabilities |
| UI | Pause/resume/reload/cancel, complete catalog, feature rejection/recipe evidence, sampling disclosure, task configuration, OIDC-only mode, accessible progress/errors |
| Kubernetes | Restricted security, KubeRay CRDs/operator upgrades, Ray worker autoscaling, node autoscaling, RBAC and NetworkPolicy negative tests, PDB, topology, taints, GPU fallback |
| ML compatibility | Every catalog recipe fits, scores, serializes, reloads, predicts where applicable, records lineage, preserves final-test isolation, and passes offline/serving plus cross-provider synthetic-refit/evaluator parity |
| Load | 15 × 10 GiB upload, 15 all-catalog runs, homogeneous and mixed tasks, background API/inference traffic |
| Chaos | API/UI/Ray worker/driver/head/operator/node/cluster/zone loss, checkpoint restore, object throttling, database failover, MLflow outage, expired identity, certificate rotation |
| Security | OIDC lifecycle, CSRF, rotation, M2M scopes, signed URL isolation, Ray endpoint/runtime-env denial, cross-project/provider denial, central final-authority races, artifact tamper, malicious inputs, deletion completeness |
| Release | Reproducible build, SBOM, vulnerability/license gates, signatures, provenance, digest promotion, unsigned-image rejection |
| Operations | Install, upgrade, rollback, PITR, object restore/reconcile, certificate renewal, credential rotation, alerts/runbooks |

Mocks and emulators are acceptable pull-request gates but are not provider
qualification evidence. Release evidence must come from the real managed
service named in the qualification record.

### Required corpus separation

A repeated source file expanded to 10 GiB is valid for I/O, storage, memory,
partitioning, autoscaling, and scheduling evidence. It is not evidence of model
quality, statistical independence, fairness, or generalization. Maintain three
fixture families:

- a deterministic exactly-10-GiB load corpus for systems qualification;
- one immutable principal benchmark dataset, target, row set, and split revision
  for comparable automated feature-engineering and model-search experiments;
  and
- immutable representative task-conformance datasets for model-quality and
  metric invariants that the principal benchmark cannot exercise.

Store generator version, source digest, output digest, exact byte size, row
count, schema, target distribution, and permitted evidence uses in each fixture
manifest.

The 15 systems objects may contain identical bytes because they test the data
plane. They are never called statistically independent datasets. Comparable ML
trials reference the principal benchmark and metadata-only feature views rather
than physical copies.

### Capacity and traffic facts that evidence must expose

- Logical raw input alone is 150 GiB. A self-managed store configured for
  three-way replication consumes 450 GiB before multipart staging, versions,
  prepared Parquet, samples, MLflow artifacts, models, backups, and free-space
  headroom. Managed-provider physical replication and billing follow the chosen
  storage class and must be recorded separately.
- If QLT-003 remains approved, the base is exactly 120 logical training runs per
  provider: four 15-run homogeneous groups, one of which is the principal
  benchmark, plus four 15-run mixed repetitions. The base systems ingestion is
  one fresh 150 GiB upload set per provider, or 450 GiB across three providers;
  ML scenarios do not re-upload it. Explicit ingestion-chaos repetitions,
  prepared data, checkpoints, artifacts, backups, and retries are additional
  signed-ledger rows, never implicit multipliers.
- Incomplete multipart and completed objects may coexist transiently.
- Completing 150 GiB in 45 minutes requires roughly 477 Mbit/s of payload
  throughput before protocol overhead, so the 500 Mbit/s threshold leaves little
  margin and must be measured at both clients and object storage.
- At 128 MiB per part, the campaign contains 1,200 object parts.
- Training reads can multiply by candidate, fold, trial, preparation, and retry.
- A 10 GiB CSV can expand several times in memory. Resource models must be based
  on measured format/schema amplification, not raw byte size.
- The total number of fits can be approximated by candidate-specific search
  behavior; evidence must report actual folds, trials, final refits, retries,
  and early terminations instead of one global estimate.
- Do not freeze a global fit count from an approximate catalog size. Generate
  the exact per-provider count from the signed catalog and objective revisions:
  sum joint suggestions × inner folds for tunable entries, approved feature
  suggestions × inner folds for fixed entries, then add global-champion refits,
  one final evaluation per scope, and separately enumerated physical retries.
  The signed ledger owns each term and its cost.

## Gate record template

Every gate must have a durable record with these fields:

```yaml
gate_id: P<phase>-G<sequence>
title: <measurable outcome>
owner: <team or named role>
reviewer: <independent reviewer>
status: not_started | running | passed | failed | expired | waived
prerequisites: [<gate IDs>]
environment:
  profile: evaluation | staging | production-qualification
  provider: local | aws-eks | gcp-gke | azure-aks | railway-contract
  region: <region or local runtime>
```

Add immutable artifact, procedure, result, and evidence identity to the same
record:

```yaml
artifacts:
  git_sha: sha256:<value>
  baseline_revision: sha256:<value> | null
  artifact_manifest: sha256:<value> | null
  qualification_attestation: sha256:<value> | null
  release_channel_pointer: sha256:<value> | null
  channel_publication_receipt: <reference> | null
  chart: sha256:<value>
  images: {component: sha256:<value>}
  migration_range: <from>..<to>
  dependency_lock: sha256:<value>
  catalog_revision: sha256:<value>
  feature_revisions: {contract: sha256:<value>, registry: sha256:<value>, recipe: sha256:<value>}
  split_revision: sha256:<value>
  ray_runtime_revision: sha256:<value>
not_applicable_reasons: {field: <phase-specific reason>}
procedure:
  command_or_workflow: <exact reproducible invocation>
  fixture_manifest: sha256:<value>
  workload_parameters: {}
  capacity_profile: sha256:<value>
thresholds: {}
expected_failures: []
observed_results: {}
```

Artifact fields are phase-applicable. Early gates use `baseline_revision` and
set unavailable artifact/attestation/channel fields to `null` with a reason;
Phase 8 candidate gates require the artifact manifest; only Phase 13 approval
and promotion gates require a qualification attestation, channel-pointer
identity, and publication receipt.

Attach evidence identity to the same record:

```yaml
evidence:
  manifest: sha256:<value>
  files: [{path: <path>, digest: sha256:<value>}]
  immutable_sink_receipt: <reference>
```

Finish the same record with rollback, lifecycle, approval, and waiver fields:

```yaml
rollback_procedure: <runbook reference>
requalification_triggers: []
started_at: <RFC 3339 UTC>
completed_at: <RFC 3339 UTC>
expires_at: <RFC 3339 UTC or null>
approvals: []
waiver:
  allowed: false
  reason: null
  compensating_controls: []
  owner: null
  approver: null
  expires_at: null
```

The concurrency, 10 GiB input, all-catalog coverage, five-iteration/three-fold
strength, 7,200-second completion, upload integrity/throughput, tenant isolation,
feature-availability/leakage safety, locked final-test isolation,
static-credential, artifact-signature, and RPO/RTO gates are non-waivable once
the corresponding qualification targets are approved.
`status: waived` is valid only for a gate explicitly marked waivable and only
when every waiver field is populated. An expired waiver is a failed gate.

Replace subjective terms such as “healthy,” “scalable,” or “works” with a
number or state invariant: p95/p99 latency, error rate, throughput, peak RSS,
queue age, object request rate, retry count, recovery time, orphan count,
checksum equality, terminal-state correctness, or explicit expected error code.

## Traceability from current readiness gates

| Current readiness concern | Implementation phases | Final evidence |
| --- | --- | --- |
| Version drift and unreproducible baseline | 0, 8 | Signed artifact manifest, qualification attestation, and matching runtime snapshots |
| Durable workflow state and database safety | 1 | Idempotency, replay, migration, and failover records |
| Large resumable upload and object integrity | 2 | Real-provider 15 × 10 GiB ingestion gates |
| Ray Data/Polars preparation and lineage | 0A, 3 | Bounded-batch, determinism, restart, statistical invariant, spill, and isolation records |
| Automated feature engineering | 0A, 3, 4, 6 | Feature contract/registry/recipe, selected/rejected evidence, leakage gates, ablation, raw baseline, and serving parity |
| sklearn/catalog completeness and Ray execution | 0A, 4 | Signed manifest, execution-backend compatibility matrix, Tune/Train recovery, candidate evidence, locked final test, and deadline result |
| Helm strictness, least privilege, network isolation, availability | 5 | Policy, RBAC, Pod Security, NetworkPolicy, install/upgrade/rollback records |
| OIDC, authorization, tenant isolation, artifact and serving security | 6 | Identity lifecycle, negative access, tamper, and serving HA records |
| Logs, metrics, traces, alerts, backup, recovery, runbooks | 7 | SLO drill, distributed trace, failover, RPO/RTO, reconciliation records |
| Supply chain and immutable promotion | 8 | SBOM, provenance, signatures, admission, digest promotion records |
| Local portability and Railway contracts | 9 | Four local E2E records and Railway adapter contracts |
| AWS environment | 10, 13 | Independent EKS qualification bundle |
| Google environment | 11, 13 | Independent GKE qualification bundle |
| Azure environment | 12, 13 | Independent AKS qualification bundle |
| Production go/no-go | 13 | Signed cross-provider decision for exact release digests |

## Evidence layout and naming

Store non-secret evidence under a release-specific structure or an approved
external evidence system with equivalent indexing:

```text
evidence/0.2.0-rc.1/
  manifest/
  baseline/
  migrations/
  security/
  features/{contracts,registry,recipes,parity}/
  ray/{data,tune,train,recovery,spill}/
  local/{k3d,kind,minikube,microk8s}/
  railway/{storage,database}/
  aws-eks/{ingestion,preparation,training,chaos,recovery,cost}/
  gcp-gke/{ingestion,preparation,training,chaos,recovery,cost}/
  azure-aks/{ingestion,preparation,training,chaos,recovery,cost}/
  approvals/
```

Never commit credentials, signed URLs, private keys, private addresses, raw
production data, OpenTofu state, or unredacted logs. Each evidence index names
the source commit, artifact-manifest digest when built, qualification-
attestation and channel-pointer digests when approved, provider, region, cluster
version, test start/end time, owner, and redaction method. Pre-candidate and
pre-decision indexes mark later identities not applicable rather than inventing
them.

## Explicit assumptions and defaults

- The systems-load fixture uses 10 GiB, exactly 10,737,418,240 bytes, while
  QLT-002 remains pending. Every ML benchmark records its actual byte size and
  does not change identity to satisfy the load-fixture size.
- The two-hour deadline begins at successful training submission, not the first
  upload byte.
- “Every estimator” means every supported, task-applicable estimator in the
  pinned catalog revision, including reviewed safe recipes for concrete
  meta-estimators.
- Abstract/test-only entries remain visible as reviewed `not_applicable` rows.
- If QLT-003 is approved, every user may select classification, regression,
  time-series, or clustering. Homogeneous worst cases and mixed scenarios then
  bound arbitrary combinations.
- Sampling is allowed only under the deterministic, disclosed policy and never
  permits skipping an estimator.
- Comparable ML experiments reference one immutable benchmark dataset version,
  target, row set, and train/validation/final-test split revision. Feature views
  are metadata and do not create physical trial datasets.
- Task-conformance fixtures are separate from the principal benchmark and from
  the systems-load corpus. Cross-task metrics are not compared.
- Qualification uses five optimization configurations and three folds only
  after QLT-006 and QLT-007 approval.
- Candidate selection uses validation data. Only the dedicated champion
  evaluator may access final-test inputs and labels, exactly once per sealed
  promotional evaluation scope after one global champion is frozen. Across
  providers, `ReleaseFinalTestAuthority` assigns each task split to one
  canonical provider; validation-only and synthetic conformance scopes never
  access that release test.
- Polars, Ray Data, Ray Tune, Ray Train, and KubeRay are the production data and
  execution stack. Dask is prohibited and is not retained as a fallback.
- PostgreSQL and object storage are durable. Ray clusters, jobs, ObjectRefs, and
  local spill are disposable and recover through fenced resubmission.
- Ray autoscaling owns Ray worker counts. The Kubernetes node autoscaler owns
  nodes. No KEDA, HPA, static Deployment, or other controller competes for Ray
  workers.
- OpenTofu is the IaC CLI. Official AWS, Google, Azure, Kubernetes, and Helm
  providers wrap the cloud APIs.
- GitHub Actions with protected environments and OIDC performs infrastructure
  and application delivery. GitOps is optional, not required for this release.
- The application receives no cloud-admin credentials and exposes no
  cluster-creation endpoint.
- Production uses one regional HA Kubernetes cluster per environment with
  external managed PostgreSQL and object storage. Ephemeral Ray clusters are
  created per fenced workflow attempt. Bundled stateful services are local only.
- UI and API remain same-origin because the cookie/CSRF contract depends on it.
- Gateway API is the production traffic boundary.
- EKS, GKE, and AKS require separate evidence and approval.
- Railway is ancillary storage/database integration only.
- Minikube, MicroK8s, k3d, and kind prove functional portability, not the
  15-user capacity contract.
- Select the supported Kubernetes version at release time as the newest common
  GA minor minus one. CI tests that minor and the immediately preceding common
  supported minor.
- Secure capacity quota before qualification. If a provider cannot supply it in
  the chosen region, change region or obtain quota; do not reduce the contract.

### Feasibility rule

This guide records accepted architecture decisions and provisional
qualification targets, not a claim that the current implementation can meet
them. Phase 0A determines technical feasibility, schedule shape, and cost before
substantial build-out. If an approved catalog candidate cannot satisfy the
contract under the allowed deterministic sampling policy, qualification fails.
Change the implementation, recipe, resource class, provider region, or capacity
and rerun the gate. Change a target only through the requirements register with
a named owner and approver.

## Definition of done

The production-readiness program is complete only when:

1. Phases 0, 0A, and 1–12 meet their gates with traceable evidence.
2. Phase 13 independently passes platform, validation, recovery, security, and
   synthetic refit/evaluator conformance on EKS, GKE, and AKS using the same
   immutable release artifacts, while each locked task test is evaluated once
   on its canonical provider.
3. All approved hard performance, security, recovery, and retention objectives
   pass.
4. No unresolved exception weakens the contract.
5. A reviewer other than the primary implementer exercises operational runbooks
   and signs the go/no-go record.
6. The stable `0.2.0` channel points to the approved qualification-attestation
   digest, which binds the exact immutable artifact and provider-evidence
   digests.

Until all six conditions are true, Sceptre remains evaluation or release-
candidate software and must not claim the production certification defined by
this guide.
