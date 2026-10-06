# Production readiness remediation

Updated 2026-10-04. Started 2026-09-24 from branch `codex/production-readiness`, HEAD `7a47ed8`. See the [initial assessment](phase-0-9-branch-assessment-2026-09-24.md) and [item inventory](phase-0-9-item-assessment-2026-09-24.csv). Those documents remain the historical audit; this ledger records subsequent fixes.

## Verification policy

Use bounded local correctness, concurrency and failure tests while implementing. The workstation is not expected to execute 15 simultaneous 10 GiB uploads. That production capacity gate remains **deferred to a suitably provisioned qualification environment**, not passed, waived or reduced. Native provider tests, operational drills and independent approvals retain their original requirements.

Development CI verifies the integrity of historical Phase 2 records using `--historical`; it does not qualify today's code with yesterday's results. Default evidence validation remains strict about candidate ancestry/code changes and release preflight invokes that strict mode. No historical gate result, signature, candidate SHA or evidence digest was rewritten. Successful record validation only validates records; failed/not-started statuses remain failed/not-started.

## Phase status

| Phase | Implementation status | Qualification remaining |
| --- | --- | --- |
| 0 | In progress: lint repair, historical evidence validation, CI database/history setup, architecture reconciliation | Exact pinned runtime checks, current candidate inventories, full quality/coverage, reviewed clean candidate |
| 0A | Synthetic capacity/accounting corrected; measured requalification pending | Changed runtime, signed RSS/benchmark/cost bounds and current evidence |
| 1 | Synthetic k3d refit/evaluator flow, lost Job creation reply and completed-scope replay verified | Broader failure recovery, real search journey, pooler, migration, deletion and production qualification remain open |
| 2 | Upload foundation present | Native accounts, scanner/provider evidence, 15×10 GiB qualification and review |
| 3 | Preparation foundation present | Role IAM, group/time/fold safety, feature execution, scale/recovery |
| 4 | Candidate execution present | Durable joint trial search/replay, refit/evaluation, fairness/capacity |
| 5 | OIDC settings/Secret wiring and TLS-only application routing implemented and chart-tested | CA mounts, NetworkPolicy, workload credentials, ordered rollout and live IdP/Gateway qualification |
| 6 | Auth/serving foundation present | M2M, producer attestation, audit, deletion and live security qualification |
| 7 | Logging/dashboard foundation present | Tracing, metrics/alerts, scheduler, cross-store restore and runbooks |
| 8 | CI/image foundation present | Hermetic signed multiarch immutable candidates and deployment qualification |
| 9 | Local tooling foundation present | Four-runtime full journeys, workstation bound and Railway contracts |

## Phase 0 changes

- Fixed the 11 Python lint findings without changing authentication behavior.
- Added explicit historical evidence-integrity validation. Candidate validation remains the default and still rejects code changes after the recorded candidate. Regression tests cover tampering and unrelated history.
- Changed development checks to archival verification and added strict candidate verification before release builds. Full Git history is fetched where archived candidate hashes are needed.
- Configured the dedicated `_tests` PostgreSQL database and `SCEPTRE_TEST_DATABASE_URL` in CI so the 14 project-management integration cases run instead of skipping.
- Superseded the Streamlit ADR with the implemented React/FastAPI architecture.
- Kept unlazy execution state ignored and preserved the user's existing coverage edit.

## Phase 5/6 authentication transport changes

- Added typed Helm values for OIDC issuer/client ID/MFA and optional existing confidential-client Secret. Secret material is never accepted in values.
- Production rendering now rejects missing/insecure OIDC configuration and disabled MFA. The rendered configuration passes the application's authentication startup policy in regression tests.
- TLS-enabled Gateways expose only HTTPS and bind the application HTTPRoute to that listener. Non-TLS evaluation retains HTTP. No redirect is implied.
- Added positive rendering, negative production and secret-reference tests, and operator documentation.

The Ray database credential fix remains open. Inspection showed the existing analysis-worker role cannot write the preparation/training workflow tables. Merely substituting that credential would break those workflows; expanding that role indiscriminately would weaken analysis isolation. Separate verified stage grants and credential wiring are required. No claim of Ray least privilege is made.

## Verification

- Initial Phase 0 run: 1,272 backend tests passed, no skips. Combined coverage was 92.79%, but the independent checks found statement coverage 94.298% and branch coverage 87.172%. **The branch threshold of 90.01% failed and remains unchanged.**
- Targeted Helm/OIDC/startup-policy run: 49 passed; Helm and Python lint pass.
- Fresh database migration/schema verification: 42 tables at `0010_phase3_contract_target`.
- Historical evidence validation, task-index consistency and Dask guard pass. Strict candidate evidence remains rejected until a new reviewed candidate has appropriate records.
- Current snapshots generated locally under `.unlazy/phase0-fixes/snapshots` are explicitly provisional; historical signed snapshots were preserved.
- Tests used Python 3.12.13 with the existing Ray 2.56.1 / MLflow 3.14.0 environment, not the required newer release runtime. Exact-runtime requalification remains open.

Final regression after Helm changes: 1,268 passed initially, with nine database-dependent cases failing because the disposable database was unavailable. All nine passed after recreating/migrating the database: **1,277 effective passes, no remaining functional failures or skips**, across the full run and targeted rerun. This is not described as a single green full-suite invocation. Combined coverage is 93.044%, statement coverage 94.439%, and branch coverage **87.828%**; the independent branch gate still fails. All nine Helm value profiles render successfully. Source lint and diff whitespace checks pass.

See [local verification record](evidence/phase-0/remediation-2026-09-25.yaml). CI is not yet fully green: it now reaches and exposes the genuine coverage gap rather than failing on archived candidate age. No whole phase is marked complete. The next Phase 0 work is meaningful coverage of uncovered failure paths and verification on the pinned runtime; remaining Helm work includes CA distribution and properly scoped Ray credentials.


## Phase 0A local feasibility correction

The scheduling helper previously labeled generated timings and assumed prices as `passed`. It now always emits `status: synthetic_estimate` and `qualified: false`, with a separate `synthetic_constraints_pass` boolean. A successful process exit means an estimate was generated, not that readiness passed. Cost multipliers are named scenarios, not an unsupported p90 or worst-case guarantee. Prices remain unverified historical assumptions.

Capacity now accounts for one head per worker placement, reserves 25% of node resources, caps usable nodes at the provider quota, and reports zero capacity without inventing a required-node count when a pair cannot fit. The old misleading `warm_slots` output is removed: this is static colocated packing, not a model of warm starts, autoscaling, stage dependencies or recovery scheduling.

Verification: four targeted scheduling tests passed, including quota exhaustion, per-head packing, impossible placement and both successful/failed synthetic constraints at the CLI boundary. Repository Python lint and diff whitespace checks pass. This increment has three unlazy gates met, zero unmet and zero abandoned; these are implementation checks, not phase qualification gates.

Phase 0A remains open: exact pinned CPU/GPU runtime evidence, measured memory/determinism/search replay, worker/head/operator/cluster recovery, credential fences, measured stage/dependency/gang scheduling, TLS and workstation bounds, and current signed performance/cost approvals. The 15×10 GiB campaign remains deferred to suitable infrastructure. Historical signed records were preserved; this correction does not retroactively validate their synthetic schedule or cost claims.

## Phase 1 authority replay and receipt integrity

Allocation replay now compares the project reference as well as scope, provider and manifest, including the unique-conflict recovery path. Reusing a split allocation with another project returns an idempotency conflict. Failure replay also rejects a changed terminal reason even when the caller repeats the same request digest. Receipt verification now requires the supported HMAC-SHA256 algorithm and verifies the stored receipt digest alongside its signature.

Local verification covers unchanged replays, changed project/reason rejection, a simulated allocation uniqueness race and tampered receipt metadata. The 51-test workflow/idempotency/public-contract/evaluation suite passes; repository Python lint and whitespace checks pass. Three unlazy increment gates are met, none unmet or abandoned. No schema migration or receipt serialization change was required; correctly generated existing receipts remain compatible.

Phase 1 is still incomplete. These service checks do not establish live PostgreSQL concurrency or provider isolation. Separate authority deployment, credential issuance and refit/evaluator integration, full trial/checkpoint replay, production-sized migration and rollback rehearsals, PgBouncer qualification, current multi-replica/API/database failure evidence, and cross-store durable deletion remain open. No new live database or infrastructure qualification was run for this increment.

## Phase 1 authority service and local recovery

Implemented a separately launched qualification-control FastAPI factory with configured, expiring bearer identities bound to project/provider/scope. Allocation is reserved for allocator identities; provider transitions cannot override identity fields. Requests have closed typed schemas and server-computed replay hashes. Responses are acknowledged only after the database transaction commits; database failures return 503 without an authority receipt. Readiness checks both authority tables.

Added a standalone two-table database initialization migration and a separate Helm chart with digest-required images, HTTPS, existing Secret references, database CA mounting, bounded pools, no service-account token, read-only containers and ingress/egress NetworkPolicy. These are deployable definitions, not evidence of a live deployment. Operator setup and remaining integration are in [qualification-control README](../../services/qualification-control/README.md).

Fixed a stale ORM identity-map race: acquiring the allocation row lock now refreshes previously loaded state, preventing an old session from overwriting a committed result. Strengthened the production database guard against reuse under different credentials/query options and against non-PostgreSQL stores. The psycopg transaction-pooling option now disables automatic prepared statements; this configuration fix does not qualify a live PgBouncer deployment.

Local verification on disposable PostgreSQL 16:

- 62 targeted authority, database-policy, workflow and PostgreSQL contract tests passed with zero skips/failures/errors; repository Python lint and whitespace checks pass.
- The HTTP tests cover three-provider open races, conflicting result commits, replay through a fresh service instance, project/scope/expiry rejection, and terminating a real PostgreSQL backend before commit. The terminated transaction leaves the allocation unopened.
- Applied the standalone migration to an empty authority-only database. Ran all six authority tests successfully there.
- Seeded an opened/committed allocation, dumped that isolated database, restored into another empty database, verified the exact allocation/version/result and both signed receipt digests, and replayed the committed result. All six authority tests also passed against the restored database.
- Three implementation gates verified. The bounded restore is not a production-sized backup, HA/PITR, zero-loss or RTO qualification. The existing local Python environment remains different from the required release runtime.

Still required: provider broker integration with a durable one-grant ledger, evaluator-attempt/frozen-pipeline binding and failed-mint evidence; separate least-privilege refit/evaluator jobs; signed provider-distribution manifests and provider-local reference validation; public-key receipt/key-rotation design and immutable evidence export; a hermetic authority image/protected deployment workflow; live database/IAM/TLS/network enforcement and managed backup/restore qualification. Full trial/checkpoint replay, production-sized migrations, PgBouncer, and cross-store deletion remain open. No object credentials are minted by this service, and replaying an open receipt must never mint another credential. Deployment target/resource names have been requested for the live work. Phase 1 is not complete.

## Phase 1 local k3d deployment and public receipts

The user authorized the existing local `k3d-sceptre` cluster as a test environment. Deployed the authority in `qa-phase1` with a separate TLS PostgreSQL instance and a restricted runtime database role. Created allowed and denied probe namespaces. The original `sceptre` application namespace was not modified. Two authority replicas and the dedicated 1 GiB database PVC remain available for subsequent integration work.

Fixed chart egress to support an in-namespace database selected by pod labels as an alternative to an external CIDR; exactly one is required. Live probes prove that the allowed namespace reaches the authority/database, while the denied namespace cannot. The runtime role rejects receipt modification, allocation deletion and schema creation. Both application HTTPS and database connections verify the local test CA.

The HTTP authority now requires an Ed25519 private key. New receipts carry a signed public-key identifier and can be verified using only a pinned public key. Tests reject wrong keys, invalid signatures and HMAC/public-key algorithm confusion. Historical HMAC verification remains isolated for old records; no existing receipts were rewritten. Public-key distribution and coordinated rotation still need operational qualification.

Verification: 76 targeted contracts passed, no errors/failures/skips; Python lint passes. The deployed probe verified three-provider open races, duplicate/concurrent commits, changed-result rejection, and public-key signatures. Exact receipt fingerprints survived replacement of both authority pods and replacement of the PostgreSQL pod on its existing PVC. Five increment gates verified, none unmet or abandoned.

During test setup, host contract checks lost a kubectl port-forward connection; the database NetworkPolicy also rejected temporary NodePort access. That temporary Service was removed. The final green contract suite ran against disposable local PostgreSQL, while live HTTP/TLS/network/restart checks ran inside k3d. An early public-verification probe also ran before the client container had picked up its updated image; it passed after the correct image was running. These setup failures are not reported as application qualification passes.

See [k3d evidence](evidence/phase-1/k3d-authority-2026-09-25.yaml) and [operator instructions](../../services/qualification-control/README.md). The image is a digest-pinned local source overlay on an existing API image, not the production runtime artifact. Database pod recovery on a retained PVC does not qualify HA/PITR or node-loss recovery. The 15×10 GiB campaign remains deferred. Broker one-grant persistence, attempt/frozen-pipeline binding, refit/evaluator jobs and their scoped credentials, signed provider-distribution manifests, production migrations and cross-store deletion remain incomplete. Local deployment access is now available; it is no longer the blocker.

## Phase 1 evaluator binding (2026-09-26)

Provider identities now require a preregistered evaluator attempt UUID and frozen-pipeline SHA-256 digest. The authority injects those values from trusted configuration into the replay hash and signed receipt; callers cannot override them in the request body. The first open durably binds them. Another attempt or pipeline cannot reopen, commit, fail or replay that allocation. This uses the existing append-only receipt table and requires no migration. Legacy internal unbound receipts remain verifiable/replayable through their historical interface; the bound HTTP service cannot turn them into fresh evaluator grants.

The local chart now uses `Recreate` so old and new identity/signing policies are not served together. Upgrades briefly interrupt authority availability, which fails closed. Kubernetes retained the prior rolling-update defaults during a server-side Helm upgrade; removing that old strategy field with an explicit merge patch allowed the upgrade. The operator README documents this required transition. Final verification confirmed the deployed strategy and two ready replicas.

Verification: 79 targeted contracts passed with no failures/errors/skips, plus two existing PostgreSQL authority compatibility/race tests. The local k3d probe rejects alternate attempt and pipeline identities for open/commit/fail, verifies signed binding fields, and recovers an identical receipt after replacing the service. Repository Python lint and whitespace checks pass. Four increment gates verified, none unmet or abandoned. Early probes during configuration/rollout changes failed and were not counted; final checks ran after the deployment settled.

See [binding evidence](evidence/phase-1/evaluator-binding-2026-09-26.yaml). This verifies identity-to-receipt binding against a synthetic preregistered pipeline digest. It does not execute champion refit, establish the immutable winning-pipeline CAS, mint an object credential or prove broker one-grant behavior. Those integrations, failed-mint recovery and the remaining production qualification requirements are still open. The separate local test environment remains running and the existing application namespace is unchanged.

## Phase 1 completion objective and credential broker

The active objective is now completion of the entire Phase 1. The
[completion ledger](phase-1-completion-ledger.md) retains all 36 Phase 1 entries
from the task index. An implementation increment does not close a phase gate.

Implemented a durable one-grant broker using the authority's existing signed,
append-only receipt table. Claim consumption commits before minting; issuance
commits before disclosure. Replays return no credentials. Failed minting seals
the allocation; expired unacknowledged claims are reconciled after process death.
Registered manifests pin separate final-input and label objects to immutable
provider versions and SHA-256 digests. SDK grants permit reads only, expire within
15 minutes, and never use caller-selected object paths. Result commit cannot
bypass an unacknowledged grant by removing its deployment manifest.

Current local verification: 93 targeted contracts passed with zero failures,
errors or skips, including 14 broker tests. Those tests cover concurrent requests,
restart, process death, real PostgreSQL connection termination on each side of
credential issuance, expiry reconciliation, secret redaction, and the native SDK
request restrictions. GCP/Azure adapter checks are local SDK tests, not live
cloud-account evidence. An expiry-boundary test initially failed and led to a fix
that checks the full remaining duration before rounding provider TTL seconds.

A separate TLS SeaweedFS instance in `qa-phase1` supplies versioned synthetic
objects with a read-only broker identity. It reuses the cluster's existing cached
image; it does not use or modify the application namespace's object store. Two
MinIO image pulls were rejected by their registries before this local alternative
was selected. The live HTTPS probe proves one grant across five racing requests,
exact object versions despite later overwrites, signed binding/receipt integrity,
and denial of unsigned reads, changed paths and writes. A service restart refuses
another grant and returns the same committed receipt. This is bounded local
S3-compatible protocol evidence, not AWS/GCP/Azure production qualification.

The integrated champion ledger has one verified gate and five still unmet:
refit execution/publication, evaluator execution/recovery, application scheduling,
the full local journey, and final integration verification. Operator-installed
identities/manifests still substitute for the missing refit-to-evaluator handoff.
Those remaining gates are retained, not waived. Phase 1 remains active and
incomplete; production-sized migration, PgBouncer, broader workflow recovery and
deletion requirements also remain in the completion ledger.

The [broker evidence record](evidence/phase-1/credential-broker-2026-09-26.yaml)
binds the final source hashes, deployed local image, fresh grant probe and
identical post-restart receipt. It explicitly records `phase_complete: false`.

## Phase 1 durable evaluator registration

Added allocator-only refit publication and evaluator registration to the central
authority. One signed publication binds the frozen pipeline and refit-policy
digests. Evaluator JWTs derive their provider/scope/pipeline binding from durable
authority records rather than operator-installed per-attempt configuration.
One pre-open replacement is allowed; a replaced identity is checked again under
the allocation lock, including requests authenticated before replacement.
Opened allocations cannot acquire a replacement evaluator.

The 96-test targeted regression passes with no failures/errors/skips. Tests cover
publication conflicts, pre-publication denial, registration races, stale
preauthenticated callers, expired/forged/changed tokens, service restart, and the
registered identity's broker/commit flow. The first run exposed JSONB key ordering
changing replayed JWT bytes; deterministic claim ordering fixes that defect.

This remains an attestation protocol: the provider-side refit worker, frozen
artifact CAS, evaluator worker, and reconciler calls are still required. The
integrated champion ledger now has seven gates: two verified, five unmet, none
abandoned. No Phase 1 requirement is marked complete on the strength of this
protocol alone.

The live k3d probe also passes the publication → registration → one-shot grant →
commit path. After replacement of both authority replicas, the replayed evaluator
token digest and committed receipt digest are unchanged, and another object grant
is denied. See the [registration evidence](evidence/phase-1/evaluator-registration-2026-09-26.yaml)
for source/image bindings and the explicit synthetic-attestation limitation.

## Phase 1 refit execution and frozen-pipeline CAS (2026-09-27)

Added a callable refit worker entrypoint and a PostgreSQL publication transaction.
The executor clones the selected pipeline, preserves its recipe/hyperparameters,
and fits exactly the preregistered train/validation rows. It does not make a new
train/test split or run another search. Every input object's byte size and SHA-256,
row count, versioned row-identity digest, role path and project/run lineage are
checked. Duplicate row identities, missing targets and exceeded input/model
budgets fail before publication. The output URI includes the attempt and pipeline
SHA-256. The preregistered data-policy digest stays stable across replacement
attempts; the execution-plan digest includes the attempt.

The worker starts only a submitted, fenced refit attempt in a running scope,
maintains a lease, and records a failed attempt on execution failure. Publication
locks scope before attempt, validates the durable plan and original champion
choice, checks lease/deadline, and commits one checkpoint, completion event and
terminal CAS together. Identical publication replays return the same checkpoint;
a stale or conflicting publisher cannot replace it. Scope cancellation and
heartbeat failure prevent publication. Provider URI checks preserve Azure's
container component, and authority publication now accepts the actual S3-compatible
`s3c` URI emitted by the storage driver.

Verification: 118 targeted tests passed with zero failures/errors/skips against a
separate disposable PostgreSQL database migrated to the current head. This includes
22 refit execution/publication tests with real sklearn fitting and PostgreSQL CAS.
They cover complete registered-row use, candidate state preservation, changed data,
replacement attempts, duplicate worker starts, heartbeat/cancellation, expired
leases/deadlines, and S3-compatible/Azure/GCS URI compatibility. The heartbeat SQL
is exercised with a controlled timer; this does not claim a live pod-kill drill.
Repository Python lint and whitespace checks pass.

The full refit integration gate remains open. The scope planner still needs to
select the global validation winner, persist the approved plan and create/retry
these attempts; the reconciler must submit separate jobs with verified stage
credentials. Evaluator execution and its recovery remain missing. The current
executor is the bounded sklearn path, not distributed/incremental refit
qualification; input budgets are not measured process-RSS guarantees. No new refit
job was deployed to k3d in this increment. The champion integration ledger still
has two gates verified and five unmet, with none abandoned.

## Phase 1 durable champion planning (2026-09-27)

The reconciliation daemon now plans refits after the exact sealed membership is
terminal and no training-run or trial attempt remains active. It selects across
member leaderboards using the experiment's validation metric, ignores cached ranks
and winners, and resolves equal scores by run UUID then model name. Classification
`log_loss` now correctly uses the shared minimize direction.

Promotional scope creation validates an explicit refit policy against its sealed
split and experiment. Sealing registers its digest before releasing training.
Role prefixes, aggregate row counts and identity digests must match; members that
already executed or whose sampled search tier would expand during refit are
rejected. This currently supports the full train/validation tier, not an approved
cross-tier transition or automatic partition-manifest generation.

Planning commits a pending attempt and immutable execution-plan event before any
workload submission. A failed refit gets at most one replacement, using the
original durable selection and data policy without reranking. Exhaustion or an
expired scope fails closed. Invalid plans become terminal failures; transient
planning failures rotate through the bounded queue. This adds desired work to the
reconciler; it does not yet submit refit Kubernetes jobs or observe their failure.

Verification: 157 targeted tests passed with no failures/errors/skips against the
fully migrated disposable PostgreSQL database. These include global selection,
unfinished-training rejection, policy and tier mismatches, pre-search sealing,
durable retry followed by real sklearn execution and frozen publication, and the
reconciler's normal/error paths. Python lint and whitespace checks pass. The
[planning evidence](evidence/phase-1/champion-planning-2026-09-27.yaml) records
source hashes and the test artifact. No new refit workload was deployed to k3d.

The champion integration ledger remains **2 met, 5 unmet, 0 abandoned** after
reverification. Separate stage DB/storage credentials, workload submission and
failure observation, evaluator execution/result recovery, distributed/incremental
refit, and full k3d recovery evidence remain required. All 36 Phase 1 requirements
remain open pending complete evidence; this increment does not qualify the phase.

## Phase 1 refit worker capabilities (2026-09-27)

Added an attempt-scoped control API under `/api/v1/internal/refits/{attempt_id}`
and a worker entrypoint, `automl_api.training.refit_worker`. The worker uses a
short-lived, domain-separated token bound to its project, attempt, fence and scope
deadline. The token cannot authenticate as an application user. It needs no
database password, bucket key, cloud identity, or application signing key.
The trusted API owns database transitions and publication checks.

Start, input grants, heartbeat, output upload, publication and failure reporting
recheck durable attempt ownership. Inputs are addressed by their index in the
registered plan, never by a worker-supplied URI. Read-only signed URLs expire no
later than the current lease; provider versions are pinned when available and
the executor still verifies every byte digest. Provider signing paths cover S3,
GCS and Azure using existing drivers. Native cloud operation remains unqualified.

The worker requires HTTPS for control and object access, rejects input redirects,
and keeps its control token off object-store requests. An optional `REFIT_CA_FILE`
adds a private CA while retaining system trust. Model output goes through a
bounded streaming upload: the API commits its intended URI/size/SHA before writing,
rejects changed or oversized output, then independently verifies stored bytes
before frozen-pipeline CAS. Dataset inputs go directly to object storage.
Publication acknowledgement loss can replay the same result without repeating
fitting. Output and publication events append under the attempt lock.

Verification: **168 tests passed**, no failures/errors/skips, including 11 new
control/worker tests. The worker performed real sklearn fitting using a local
HTTPS input server and the FastAPI control endpoints backed by PostgreSQL. Checks
cover cross-attempt/signature denial, cancellation, stale fences, expired leases
and deadlines, upload bounds, output corruption, idempotent publication, and
provider-error redaction. S3 signing was checked with a mocked SDK; the HTTPS
input server is synthetic, not a native-provider or Kubernetes qualification.
Python lint and whitespace checks pass. See the
[worker capability evidence](evidence/phase-1/refit-control-2026-09-27.yaml).

The intended credential boundary now has an executable worker/API path. Separate
Kubernetes submission, Secret/NetworkPolicy lifecycle, job/lease failure
observation, and deployed isolation still need implementation and k3d evidence.
No new refit pod was deployed in this increment. The evaluator, automatic shard
manifest production, distributed/incremental backends and remaining Phase 1
requirements are still open. Reverified champion gates remain **2 met, 5 unmet,
0 abandoned**; all 36 Phase 1 requirements remain retained and open.


## Phase 1 isolated refit Jobs (2026-09-29)

The reconciler now commits a refit Job manifest before creating its immutable
attempt-token Secret, NetworkPolicy and Job. Replays reuse the same names and
manifest; a missing or failed running Job, expired lease, deadline or terminating
Job invalidates the attempt. The planner owns the single replacement. Workers
run without database/bucket credentials or a Kubernetes API token, with a
read-only root filesystem, non-root identity and explicit endpoint egress.
Foreground deletion retains the Secret and policy until the Job disappears;
a bounded periodic scan also recovers late resource creation after cleanup.

A real k3d kill probe found that the Python PID 1 worker could finish and publish
while its Job was terminating. The worker now handles SIGTERM, the Job has a
five-second termination grace, and observation treats termination as failure.
The corrected probe published through exactly one replacement after fitting
10,000 synthetic rows. Normal completion, lost creation acknowledgement and two
concurrent reconcilers each published through one attempt. These fixtures begin
with synthetic terminal training rows; they do not prove the full training flow.

A diagnostic Pod using the stored worker security specification and network
policy reached the API over verified HTTPS and received 403 for an unsigned GET
of a known object. PostgreSQL was reachable from the controller but refused from
the isolated Pod. The metadata address was also unreachable, without a positive
cloud metadata control; that is not native-cloud metadata qualification. All
refit Jobs, attempt-token Secrets and policies were absent after cleanup.

Verification: **287 targeted tests passed**, with no failures, errors or skips.
Six deployed application source hashes match the workspace. The local image is
a source overlay on an existing image, not the pinned production runtime.
A kubelet certificate/IP mismatch prevented kubectl exec; probes entered the
same test controller through the local Docker runtime without disabling TLS.
The original application namespace was unchanged. See the
[refit Job evidence](evidence/phase-1/refit-jobs-2026-09-29.yaml).

Champion integration gates remain **2 met, 5 unmet, 0 abandoned**. The evaluator,
signed result recovery and authority handoff, automatic shard manifests, other
refit backends and remaining migration, pooler, deletion and replay requirements
are still open. All 36 Phase 1 requirements remain retained; this is partial
local evidence, not Phase 1 completion or production qualification.


## Phase 1 frozen-pipeline evaluator core (2026-09-29)

Added a bounded evaluator for classification and regression. It loads the exact
published pipeline bytes without cloning or fitting, validates registered final
object hashes, input/label schemas and row identities, and joins shuffled label
partitions by row ID. Rows and decoded data have preregistered limits. Binary
classification requires the positive label from the frozen policy; it cannot
choose the positive class from final-test prevalence. Temporal and clustering
execution remain unsupported by this new path.

The final-data adapter verifies the authority's Ed25519 grant receipt, allocation,
attempt, frozen pipeline, manifest, expiry and exact URL-list digest. It sends no
control bearer header to object storage, rejects redirects and non-HTTPS URLs,
and consumes each local read before network I/O without retrying it. It uses only
the authority-issued versioned object URLs, with no bucket or database credentials.

The executor returns canonical aggregate result bytes signed by the per-attempt
Ed25519 key declared in its plan. The recovery verifier checks the expected byte
digest, signature and complete plan binding without reading final data. These
bytes are ready for durable publication; an object-store writer, durable key/plan
registration, scope CAS and scheduled evaluator lifecycle still need wiring.
The existing authority commit endpoint remains digest-based: the integration
caller must verify the stored signed object before calling it. This increment
does not claim that every caller is already wired to that verification.

Verification: **302 targeted tests passed**, no failures/errors/skips, including
15 evaluator checks. An integration test uses the real PostgreSQL authority API
to register an evaluator, issue one grant, evaluate a fitted sklearn pipeline,
verify its signed result after simulated process loss, restart the authority,
and commit/replay one digest while rejecting a second grant or changed digest.
Object URL minting and transport are mocked in that test; no new evaluator pod
was deployed. Python lint and whitespace checks pass. See the
[evaluator core evidence](evidence/phase-1/evaluation-core-2026-09-29.yaml).

The full Phase 1 objective remains active. Evaluator control and Job scheduling,
immutable result persistence and scope publication, post-open failure sealing,
full k3d recovery and the other retained requirements remain open. The 36-item
completion ledger is unchanged in scope; no phase is declared complete.


## Phase 1 evaluation publication service (2026-09-29)

Added a service that commits an evaluation output intent before writing the
signed result to its attempt-specific, content-addressed object key. The intent
fixes URI, byte count and SHA; changed declarations are rejected. Independent
recovery reads bounded stored bytes, verifies their signature and complete plan
binding, then submits the digest to the authority and validates its signed commit
receipt. Only then does one database transaction register the checkpoint, finish
the attempt and complete the scope. Identical acknowledgements can be replayed.

Scope-before-attempt locks enforce the project, scope, stage, fence and durable
plan, and require the same successful refit checkpoint. New writes require a live
lease and scope deadline. Recovery of a previously stored result can proceed
after worker lease expiry, but cancellation, supersession and failed scopes
reject publication. Recovery never requests final-data credentials. Stored bytes
are verified on every recovery; storage-provider object-lock/version retention
is not established by this content-addressed writer alone.

Verification: **310 targeted tests passed**, no failures/errors/skips. Eight new
PostgreSQL service tests cover output intent before I/O, lost object write replies,
lost authority commit replies, idempotent terminal publication, corrupt/missing
objects, cancelled scopes, stale fences, altered plans and forged receipts.
These tests use local object storage and transaction/savepoint fixtures, with
injected transport failures; they are not live process-kill qualification.
Lint and whitespace checks pass. See the
[publication evidence](evidence/phase-1/evaluation-publication-2026-09-29.yaml).

This service is not yet invoked by a scheduled evaluator: durable plan and key
registration, control API/worker, reconciler and Job lifecycle remain required.
The full integrated k3d journey and all other Phase 1 requirements remain open.


## Phase 1 authority recovery and allocation conflicts (2026-09-30)

Fixed allocation replay to check both unique bindings: split digest and scope ID.
Previously, another split requested for an existing scope could surface as a 503
rather than a deterministic conflict. The shared allocation function now returns
409 through the API for either conflicting binding, including a race after the
initial lookup. Identical requests still return the same allocation.

Added allocator-only `GET /allocations/{allocation_id}` for recovery inspection.
It returns project-scoped allocation state and signed receipts under a shared
allocation lock, with no object URLs or evaluator access tokens. The reconciler
can recover a committed receipt even after the worker identity has been removed.
It must still verify the receipt and stored result before completing local state;
this endpoint itself neither issues a grant nor changes allocation state.

Verification: **53 affected tests passed**, no failures/errors/skips. A forced
six-request insert race produced one allocation and five 409 conflicts; replay
preserved the winner. Recovery returned the same valid signed commit after an
authority restart with only the allocator identity, and rejected unauthorized
and cross-project reads. The tests use real disposable PostgreSQL and FastAPI
TestClient, not a newly deployed authority. Lint and whitespace checks pass.
See the [authority recovery evidence](evidence/phase-1/authority-recovery-2026-09-30.yaml).
Evaluator planning, key registration, scheduling and complete k3d integration
remain open; no Phase 1 requirement is declared complete by this increment.


## Phase 1 evaluator policy and durable planning (2026-09-30)

Promotional scope creation now requires an evaluator policy declaring the exact
final object template, metric, target, row count/digest and resource budgets.
Validation binds it to the sealed split and experiment, canonical provider and
final-role prefixes. Scope sealing revalidates and records its digest in the
same transaction as membership release. The template excludes the server-assigned
scope ID; constructing the execution plan adds that ID without changing the
registered objects. Authority manifest hashes remain canonically computed over
the complete scoped manifest.

Added initial evaluator registration under the scope lock. It requires the
preregistered policy digest and a successful refit checkpoint with matching
publication evidence, then writes one pending attempt and execution-plan event.
Identical registration replays return the same attempt; a changed allocation or
result signing key is rejected. The function records work before credentials or
Job side effects. It does not authorize an allocation, persist a private key or
replace failed evaluators; the control-plane integration remains required.

Verification: **321 targeted tests passed**, no failures/errors/skips, including
nine new policy/registration checks. Tests exercise real sklearn refit and
PostgreSQL plan publication, pre-refit rejection, exact registration replay,
changed allocation/key/policy rejection, invalid final versions/prefixes/digests,
and scope creation without a client-selected scope ID. Existing barrier tests
now verify that both refit and evaluator policy digests are frozen on release.
Lint and whitespace checks pass. See the
[evaluator planning evidence](evidence/phase-1/evaluation-planning-2026-09-30.yaml).

No evaluator was deployed in this increment. Credential handoff, private-key
custody, control API/worker and Kubernetes lifecycle remain open, as do automatic
manifest generation, other backends and the rest of the retained Phase 1 ledger.


## Phase 1 evaluator authority handoff (2026-09-30)

Added `prepare_evaluator` to connect the preregistered policy and successful
frozen-pipeline publication to the central authority. It records desired
allocation/refit requests and the result public key before remote side effects,
checks the allocation through allocator-only inspection, persists the local
plan, verifies the signed refit receipt, and validates evaluator JWT lineage and
expiry. Workflow events contain public claims and a token hash, never the raw
access token. The token is returned for later Secret delivery.

Authority registration now accepts a timezone-aware scope deadline and bounds
identity expiry by it. Replay returns the original token without extending it.
The handoff rejects opened or terminal allocations and never requests final data.
It requires a caller-owned persistent private key matching the registered public
key; custody and delivery remain integration work.

Verification: **325 targeted tests passed**, no failures/errors/skips. Four new
handoff tests use real PostgreSQL, refit execution and the authority API. Injected
lost replies after allocation, refit publication and evaluator registration replay
the same plan/token and leave the allocation unopened. A wrong-project response
halts before refit or identity publication. The initial fixture's embedded URI was
correctly rejected; the corrected fixture uses compatible-storage URIs over local
bytes. This is not native S3 or live workload evidence. Lint and whitespace checks
pass. See the [handoff evidence](evidence/phase-1/evaluator-handoff-2026-09-30.yaml).

The controller does not yet invoke this helper or deliver evaluator Secrets/Jobs.
Private-key custody, control/worker execution, pre-open replacement, post-open
recovery/failure sealing and complete k3d verification remain required, along with
all other retained Phase 1 work. No production qualification is claimed.


## Phase 1 evaluator signing-key custody (2026-10-03)

Added initial-generation evaluator key custody using an immutable Kubernetes
Secret. The service validates the live scope, preregistered policy and successful
refit, commits key-creation intent before Kubernetes I/O, then records the public
key before the authority handoff. Creation replay handles an existing Secret or
lost acknowledgement. A recorded key that disappears is not regenerated; changed
ownership, namespace, immutability or key material fails closed. No private key or
raw evaluator token is written to workflow events.

`prepare_evaluator_credentials` connects this custody path to the existing
allocation/refit/identity handoff and returns the plan, token and key Secret name.
The helper covers initial generation only. Controller invocation, worker mounts,
pre-open replacement and terminal Secret cleanup remain integration work.

Verification: **333 targeted tests passed**, no failures/errors/skips, plus **one
live k3d check**. Eight new automated checks cover normal/409/lost-reply creation,
exact plan/token replay, absent secret material in events and invalid Secret
rejection. The live check created only a temporary Secret in `qa-refit`, verified
native immutable-Secret enforcement (mutation rejected with 422), replayed its
public key, then deleted it and verified refusal to regenerate. The temporary
Secret was confirmed absent afterward. Local PostgreSQL and synthetic refit
fixtures supplied the scope; no evaluator workload was deployed. Lint and
whitespace checks pass. See the
[key-custody evidence](evidence/phase-1/evaluation-keys-2026-10-03.yaml).

Champion gates remain 2 met, 5 unmet, 0 abandoned. The entire Phase 1 objective
and all 36 requirements remain retained; this is not production qualification.


## Phase 1 evaluator control API (2026-10-03)

Mounted internal evaluator routes in the application API. A domain-derived
control token binds project, attempt and fence independently of user tokens and
central authority identities. Start claims a submitted attempt once; heartbeat
and the single frozen-pipeline capability require a live scope, lease, durable
execution plan and authority registration. No worker-selected object path is
accepted by this read endpoint.

Signed result upload is bounded to 64 KiB. It uses the durable output-intent
service, then independently rereads and verifies stored bytes before acknowledging.
Publication verifies a central signed commit receipt and uses the existing
recovery service to complete checkpoint, attempt and scope atomically. A worker
failure report records only a fixed event; the future reconciler integration
must inspect output and authority state before deciding terminal failure.

Verification: **338 targeted tests passed**, no failures/errors/skips, including
five new API checks. Checks cover claim replay, capability isolation, lease/fence/
cancellation rejection, upload bounds and signature corruption, duplicate result
upload, authority-receipt publication replay and nonterminal failure recording.
The API fixture signs synthetic aggregate results and uses the authority's
explicit open/commit contract; it does not read final objects or run an evaluator
worker. Existing executor tests remain separate. Lint and whitespace checks pass.
See the [control API evidence](evidence/phase-1/evaluation-control-2026-10-03.yaml).

No new evaluator workload was deployed. Worker entrypoint, Job/controller wiring,
post-open recovery/failure sealing, lifecycle cleanup and all other retained
Phase 1 requirements remain open. Champion gates remain 2 met, 5 unmet,
0 abandoned; this increment does not qualify the phase.


## Phase 1 connected evaluator worker (2026-10-03)

Added `automl_api.training.evaluation_worker`, connecting attempt claim,
heartbeat, bounded pipeline download, one-shot authority grant, frozen-pipeline
prediction, signed-result upload, central digest commit and local publication.
The worker verifies pipeline bytes before asking for final data. It uses separate
control and authority identities, verified HTTPS and redirect-free object reads,
and needs no database or bucket credentials. SIGTERM and errors report a fixed
failure event; the recovery loop must inspect durable output and authority state.

The worker never retries a final-data grant or evaluation. It may replay only
identical result upload, commit or publication after a transport acknowledgement
is lost. Upload acknowledgement follows the application's independent stored-byte
verification; the worker checks the expected URI, digest and byte count before
central commit. Temporary pipeline and decoded-object files are cleaned on normal or handled-error exits.

Verification: **344 targeted tests passed**, no failures/errors/skips. Six new
connected tests execute real refit/evaluation and both APIs with PostgreSQL and a
real local HTTPS object server. Normal execution and lost upload/commit/publication
replies finish one scope with one grant and exactly three object reads (pipeline,
inputs, labels), without object-request Authorization headers. A lost grant reply
is never retried and performs no final reads. Corrupt pipeline bytes fail before
grant issuance. APIs use FastAPI TestClient and object URLs/version parameters
are synthetic; this is not native-cloud or deployed evaluator evidence. Lint and
whitespace checks pass. See the
[worker evidence](evidence/phase-1/evaluation-worker-2026-10-03.yaml).

Kubernetes manifest/submission, controller invocation, pre-open replacement,
post-open recovery/failure sealing, lifecycle cleanup and the full live k3d
journey remain required. The broader Phase 1 ledger remains open; the worker
integration does not establish production runtime or capacity qualification.

## Phase 1 authority recovery transitions (2026-10-03)

Added project-scoped allocator recovery commit/failure endpoints. They validate
attempt, frozen pipeline and evaluator generation under the allocation lock,
without requiring or renewing a worker token. Recovery preserves the original
transition request digest, so lost acknowledgements return the same signed
receipt. Stale generations, other projects, conflicting terminal requests and
commits without an issued grant are rejected.

Verification: **349 targeted tests passed**, no failures/errors/skips. New
checks expire the original evaluator JWT through the authentication clock,
verify allocator recovery and receipt replay, and reject attempts to fail a
replacement evaluator. Two connected worker cases lose contact before or after
central commit, expire the local lease and recover the independently verified
stored result through the new endpoint without additional final-data reads or
grants. Tests use local PostgreSQL, TestClient and synthetic object URLs; no
Kubernetes evaluator deployment or native-cloud qualification is claimed.

The controller must still invoke recovery, inspect stored output before failure,
and implement scheduling, pre-open replacement and foreground resource cleanup.
The Phase 1 requirements and champion integration gates remain open. See
[recovery evidence](evidence/phase-1/evaluator-recovery-2026-10-03.yaml).

## Phase 1 evaluator Job submission (2026-10-03)

Added durable evaluator workload submission. The registered plan, token fingerprint,
result-key Secret name and local fence are checked before saving the Job and
NetworkPolicy manifests and startup lease. External creation follows that commit.
Lost creation acknowledgements reuse the same native resources; changed tokens,
ownership or manifest digests are rejected. Started and expired attempts cannot
be resubmitted through this entrypoint. Submission never opens final data.

Refit and evaluation share the existing native isolation profile, with separate
worker entrypoints and identities. Evaluator Jobs mount only the recorded result
key, public authority key and optional TLS CA, and receive separate local-control
and authority tokens. They use pinned images, bounded resources, no service-account
API token, non-root/read-only execution, explicit egress and zero Kubernetes retries.

Verification: **361 targeted tests passed**, no failures/errors/skips. Twelve
new submission cases and the eleven existing refit Job cases pass using actual
PostgreSQL/authority APIs and a Kubernetes API fake. Lint and whitespace checks
pass. This is not deployed evaluator evidence. Controller invocation, pre-open
replacement, recovery/cleanup observation, Helm configuration and the live k3d
journey remain required; Phase 1 stays open. See
[submission evidence](evidence/phase-1/evaluation-jobs-2026-10-03.yaml).

## Phase 1 pre-open evaluator replacement (2026-10-03)

Extended the existing planning, key custody and authority handoff functions with
an explicit evaluator generation. Generation two requires a failed first attempt,
remaining retry budget and no recorded output. It preserves the original final
manifest, frozen pipeline, allocation and metric/resource policy while using a
new private key, attempt ID and fence. It must win the central authority's
pre-open generation CAS before workload submission. Generation three and stale
initial-generation replay are rejected.

Verification: **367 targeted tests passed**, no failures/errors/skips. Six new
cases cover normal replacement, a lost registration reply, ineligible initial
attempts (live, output-bearing or exhausted), and the old worker winning the open
race between allocation inspection and replacement registration. That race leaves
no replacement authority identity or Job. Successful replacement revokes the old
identity and submits with the new generation's key without opening final data.
Tests use PostgreSQL, TestClient and a Kubernetes fake. The first targeted run
failed three cases because an inherited test callback expected initial submission
during replacement key creation; the corrected callback verifies the committed
generation-two key intent. The failed run is retained locally.

Controller invocation, observation, recovery/failure sealing, foreground cleanup,
Helm wiring and live k3d probes remain required. Phase 1 is not complete. See
[replacement evidence](evidence/phase-1/evaluation-replacement-2026-10-03.yaml).

## Phase 1 evaluator controller integration (2026-10-04)

Added the evaluator lifecycle loop and connected it to the workflow reconciler.
When configured with project-scoped allocator tokens, it submits registered
attempts, observes Job/lease state, records a recovery fence, verifies stored output
before central commit, and seals opened attempts without valid output. Worker
control rejects late requests after recovery starts. Unopened first failures may
be replaced after foreground deletion. Cleanup removes delivery credentials,
policies and result keys only after Job absence; a periodic sweep covers late
creates and scope-deletion orphans. Cancellation after a central commit preserves
that signed receipt while cleaning the workload.

Verification: **376 targeted tests passed**, no failures/errors/skips. Nine new
controller tests cover submission/replacement, foreground cleanup, post-open
failure, heartbeat races, connected real-worker result recovery before/after
central commit, corrupt output, cancellation, late start rejection and orphan
cleanup. They use PostgreSQL and real application/authority code with a Kubernetes
fake. The stopped disposable test database was restarted after inspecting Docker
state. No evaluator was deployed to k3d in this increment. Lint and whitespace
checks pass. See [controller evidence](evidence/phase-1/evaluation-controller-2026-10-04.yaml).

Expiry during an unfinished authority registration still needs a terminal recovery
path. Helm configuration, live k3d failure probes and all other retained Phase 1
requirements remain open. The controller is not production-qualified; champion
integration gates remain 2 met, 5 unmet and 0 abandoned.

## Phase 1 incomplete-registration recovery (2026-10-04)

Closed the expiry/cancellation gap before evaluator registration. A project-scoped
allocator can atomically abort an unregistered, unopened allocation and receive
an immutable signed receipt. Registration and abort use the same allocation lock;
registered workers must use the existing evaluator recovery path. The controller
replays durable allocation intent even when no local evaluator row exists, verifies
the abort receipt, records handoff closure and makes keys eligible for cleanup.
Identical retries cannot allocate fresh access after an earlier lost response.

Verification: **385 targeted tests passed**, no failures/errors/skips. Nine new
cases cover authorization, identical receipt replay, conflicts, registered-worker
rejection, expiry/cancellation at three handoff boundaries, and a concurrent
registration/abort race with one winner. The first targeted run exposed an abort
receipt verifier that rejected the authority's additional signed key-ID field;
that verifier was corrected without weakening signature or lineage checks. The
failed run remains in local evidence. Tests use local PostgreSQL, real APIs and
a Kubernetes fake; no new live k3d qualification is claimed. See
[abort evidence](evidence/phase-1/registration-abort-2026-10-04.yaml).

Helm wiring, live k3d execution/failure probes and all other retained Phase 1
requirements remain open. Champion gates remain 2 met, 5 unmet and 0 abandoned.

## Phase 1 evaluator Helm configuration (2026-10-04)

Added `championEvaluation` values, schema and rendered configuration. Production
requires evaluator configuration, HTTPS endpoints, explicit TLS egress and a
digest-pinned worker image. The allocator token map is mounted only in the
reconciler; the API and reconciler receive the authority public key, and the
controller can mount a custom CA. The reconciler now receives the same JWT Secret
as the API, fixing a missing capability-signing key for both refit and evaluation.

Verification: **394 targeted tests passed**, no failures/errors/skips, including
30 Helm contract tests. Nine new chart cases inspect rendered credential mounts
and matching JWT Secret references, reject unsafe evaluator settings, and check
disabled local defaults. The first chart run caught an unclosed egress-port schema;
it was fixed and reverified. Lint/whitespace checks pass. See
[Helm evidence](evidence/phase-1/evaluation-helm-2026-10-04.yaml).

Live-test preparation found the existing local test CA expired. Six TLS leaf
certificates and the two test CA mounts were renewed without rotating private
keys or credential fields, and unhealthy workloads in `qa-refit` and `qa-phase1`
were restarted. These actions are environment recovery, not a successful evaluator
probe. Live k3d qualification and all other Phase 1 requirements remain open.

## Live evaluator integration — 2026-10-04

Two fresh synthetic scopes completed through the real refit and evaluator Jobs in local k3d. Each used one final-data grant and two successful attempts. The second run deliberately lost the evaluator Job creation acknowledgement; reconciliation completed without a duplicate attempt or grant. Replaying that completed scope retained the exact signed result digest. The probe independently checked removal of both Jobs, token Secrets, NetworkPolicies and the evaluator signing-key Secret. All 133 Python source files checked in the test image matched the workspace. See [local evidence](evidence/phase-1/evaluation-live-2026-10-04.yaml).

Test ingress now admits the specific refit API, evaluator and controller workloads to the separate final store/authority. Historical authority identities and manifests were retained. Fresh versioned final-data fixtures were used after ephemeral storage recovery.

Phase 1 remains open: the broad champion gates reverified at **2 met, 5 unmet, 0 abandoned**. Live worker-death, lost result/commit acknowledgement and network-denial probes remain. Older refit-only fixtures repeatedly log `KeyError` because they lack evaluation policy; that recovery behavior still needs correction. The source-overlay image, synthetic pre-fitted candidate and local compatible object store do not establish hermetic runtime, real search, native-cloud or capacity qualification. The prior 394-test regression belongs to the preceding Helm increment; this increment added live probe evidence, not a new full regression claim.

## Missing evaluator policy recovery — 2026-10-04

Fixed the repeated `KeyError` discovered in the live test namespace. Reconciliation now locks the scope and records a failed handoff when evaluation policy is absent and neither an evaluator attempt nor an authority intent exists. Cancellation is preserved. Existing external handoffs remain recoverable errors rather than being incorrectly closed.

The **398-test regression passed with no failures, errors or skips**. Four added cases cover ordinary/cancelled closure and protection of registered or partially allocated handoffs. The updated local test controller closed seven legacy scopes once; the second sweep selected none and created no evaluator attempts. See [evidence](evidence/phase-1/missing-evaluation-policy-2026-10-04.yaml). Broad champion gates remain **2 met, 5 unmet, 0 abandoned**; Phase 1 is not complete.

## Live evaluator death boundaries — 2026-10-04

Added a bounded synthetic [failure probe](../../scripts/validate_evaluator_failure.py) and ran both boundaries against real k3d worker Jobs. Before final-data open, the stopped worker had zero grants; one replacement completed with one grant and one signed result/commit. After open, the stopped worker had one signed issued-grant receipt; recovery sealed failure without a replacement or committed result. Both runs verified cleanup of worker Jobs, token/signing-key Secrets and NetworkPolicies, then replayed reconciliation twice without changing terminal authority state. [Evidence](evidence/phase-1/evaluator-death-2026-10-04.yaml) records exact executed versions and results.

A k3d restart between probes removed the controller and invalidated its temporary allocator identity. A fresh identity and fixture were added while retaining historical receipts. Kubelet certificate/address mismatch required local Docker/CRI exec; application TLS verification stayed enabled. The previous 398-test regression remains the application-code evidence; this increment changes the probe only. Broad champion gates reverified at **2 met, 5 unmet, 0 abandoned**. Lost result/commit acknowledgements, full network qualification and the remaining Phase 1 requirements are still open.

## API database pool metrics — 2026-10-04

The API now serves process-local Prometheus pool gauges at `/metrics`; the UI proxy does not forward this path. Scraping does not check out a database connection, including when the pool is saturated. The direct Prometheus-client dependency matches the existing API hash lock.

**38 targeted tests passed**, including real QueuePool checkout, overflow, saturation, release and unsupported counters. An isolated k3d PostgreSQL-backed API returned four gauges over verified TLS. [Evidence](evidence/phase-1/pool-metrics-2026-10-04.yaml) records the test image and results. This verifies API export only: other process pools, scraper/network configuration and live PgBouncer transaction mode remain open. Broad champion gates remain **2 met, 5 unmet, 0 abandoned**; Phase 1 is not complete.

## PgBouncer transaction-mode compatibility — 2026-10-05

A real local PgBouncer 1.25.1 test exposed a connection failure: application timeout startup options were rejected. The application and qualification-control engine factories now install the same transaction-begin hook in pooler mode, applying statement, lock and idle-transaction limits locally to each transaction. Direct PostgreSQL startup behavior stays intact. Automatic prepared statements remain disabled, and autocommit cannot bypass the limits.

**399 regression tests passed without failures, errors or skips.** The live probe verified differently configured clients reusing one backend without timeout leakage, enforcement of all three limits, rollback/reconnect, application names, and verified TLS on both connections; plaintext and wrong-CA connections failed. [Evidence](evidence/phase-1/pgbouncer-2026-10-05.yaml) preserves the original failure and corrected run. Concurrent claims, connection-loss/failover, production deployment and other process pool exports remain open. Broad champion gates remain **2 met, 5 unmet, 0 abandoned**.

## Durable queue ownership and pooler recovery — 2026-10-05

Real PostgreSQL tests exposed two stale-session defects: a former owner could complete or defer an entry after another worker reclaimed it, and a later reclaim could lose a delivery-attempt increment. Completion/deferral now lock and refresh persisted ownership without autoflushing stale state; claim selection refreshes the counter under its existing row lock. Three regression cases preserve these failures.

The live PgBouncer probe held four claim transactions concurrently on four backends and obtained 32 distinct rows. Terminating another backend after claim flush but before commit rolled back the claim; a later worker recovered it. All 33 completions replayed without changing terminal state. These are worker-thread/database tests, not deployment-replica qualification.

The expanded regression initially found three outdated capacity mocks and an unbounded pagination mock in the main-loop test. The corrected final run passed **504 tests, no failures/errors/skips**; the interrupted log is retained. [Evidence](evidence/phase-1/pooler-queue-2026-10-05.yaml) records both failures and final checks. Broad champion gates remain **2 met, 5 unmet, 0 abandoned**. Full external-side-effect recovery and the other Phase 1 requirements remain open.

## Structural schema verification — 2026-10-05

The schema verifier previously accepted extra columns, a removed primary key and changes to foreign-key delete/update actions, deferral and match mode. It now compares those properties, retaining referenced schema names. Seven real PostgreSQL mutation cases first demonstrated the omissions and now pass; every case includes an unchanged-schema positive control. The fully migrated application database also passes with 42 tables at `0010_phase3_contract_target`. [Evidence](evidence/phase-1/schema-structure-2026-10-05.yaml) records both checks.

This is partial verifier hardening, not full schema parity: check/default normalization still risks hiding grouping or literal-case changes; index predicates/expressions and constraint validation state require additional coverage. Expand/backfill compatibility and production-sized migration rehearsals remain open. Broad champion gates reverified at **2 met, 5 unmet, 0 abandoned**.

### 2026-10-05 — constraint state and default literals (Phase 1 remains open)

The startup schema verifier now rejects unvalidated foreign-key/CHECK constraints and compares primary-key/unique deferral settings. It reads PostgreSQL catalog state for the visible relation, without DDL, and ignores constraints on same-named hidden relations. Default comparison now uses SQLAlchemy's DDL rendering and preserves literal case, whitespace, double quotes and cast-looking text. It simplifies only recognized literals with compatible column casts; unknown expression spellings compare exactly.

Validation: **20 PostgreSQL tests passed, zero failures/errors/skips**, including all seven earlier structural mutations, six constraint-state mutations, five default-literal mutations, hidden-schema isolation and a read-only full-schema check. The CLI also accepts the current **42-table** schema at `0010_phase3_contract_target`. Six state tests and two literal tests reproduced the old false positives before the fixes. One command referenced a nonexistent migration-test filename, exited 4 and ran no tests; the failure is preserved and the corrected suite passed. Ruff and `git diff --check` passed.

Reverified champion integration gates remain **2 met / 5 unmet / 0 abandoned**. No Phase 1 requirement is closed by this increment. CHECK expression normalization, index semantics/validity, identity defaults, migration compatibility/rehearsals and the remaining phase requirements are still open. Evidence: [schema-state-defaults-2026-10-05.yaml](evidence/phase-1/schema-state-defaults-2026-10-05.yaml). This code has not been rebuilt into the k3d images or production-qualified.

### 2026-10-05 — CHECK structure and native index verification (Phase 1 remains open)

Replaced the CHECK normalizer that removed parentheses, literal case and casts with SQLGlot PostgreSQL expression trees. The comparison preserves Boolean/arithmetic structure, literal contents, quoted identifiers and meaningful casts. Narrow handling accepts PostgreSQL's reflected varchar/text literal comparisons and IN/ANY forms. Other casts remain structural; the verifier does not evaluate expressions or assume arbitrary function-overload equivalence. SQLGlot is pinned at `30.21.0`; API lock regeneration added only its two distribution hashes, and hash-enforced installation passed.

PostgreSQL index verification now compares native catalog definitions with ORM-compiled DDL. It covers predicates, expressions, uniqueness, ordering/null placement, included columns, access methods and operator classes, and rejects extra duplicate definitions. Invalid/not-ready/not-live indexes are rejected, including the real invalid index left by a failed concurrent unique build. Index naming, table qualification and creation-time concurrency do not change the signature.

Validation: **40 focused PostgreSQL tests passed** and **544 broader regression tests passed**, both with zero failures/errors/skips. The startup CLI accepts the unchanged **42-table** schema at `0010_phase3_contract_target`; read-only inspection is covered. Ten expression mutations and eight index defects reproduced misses in the old verifier. An interim type-node serialization failure was fixed and retained as evidence. The synthetic `length()` control explicitly casts to text so the verifier need not infer arbitrary function-overload equivalence. Ruff and `git diff --check` passed.

The reverified integration ledger remains **2 met / 5 unmet / 0 abandoned**. Phase 1 is not complete. Identity/sequence/computed columns, remaining constraint/type/collation options, large-byte inventory and migration qualification still require work. No new runtime image has been deployed to k3d. Evidence: [schema-expressions-indexes-2026-10-05.yaml](evidence/phase-1/schema-expressions-indexes-2026-10-05.yaml).

### 2026-10-05 — generated columns, constraint options and migration rehearsal

The verifier now compares identity mode/start/increment/limits/cache/cycle and computed expressions, while excluding the moving sequence value from schema state. It detects unique `NULLS NOT DISTINCT`, CHECK `NO INHERIT`, duplicate CHECK/unique/FK constraints and unmodeled constraint types, including exclusion constraints. Twelve generated/option mutations and four extra-constraint cases first reproduced false positives. An invalid MINVALUE/START fixture was corrected and its failed run retained.

**63 focused PostgreSQL tests passed** and **567 broader regression tests passed**, with zero failures/errors/skips. Ascending and descending identity defaults pass for smallint/integer/bigint, including after sequence advancement. The seven current byte-count columns are BIGINT in both metadata and PostgreSQL; a test checks 10 GiB and signed-64-bit values through each reflected type. No large object upload is implied. Ruff and `git diff --check` passed.

The existing migration rehearsal passed all five declared starts (`0001_initial`, `0002_expand_artifact_kind`, `0003_security_controls`, `0004_resumable_dataset_uploads`, `0007_phase1_attempt_lineage`) against the final verifier and head `0010_phase3_contract_target`. Each passed the 42-table schema check and Alembic drift check. The two upload-capable starts seeded 10,000 metadata rows each; other starts seeded a user/project. This is bounded local evidence, not production-sized qualification. Review found that the seed oracle mainly checks counts/minima, so stronger per-row preservation evidence remains required, along with previous-release concurrent-reader and expand/backfill qualification.

The integration ledger remains **2 met / 5 unmet / 0 abandoned**. Phase 1 remains open and these source changes have not been deployed to k3d. Evidence: [schema-generation-migrations-2026-10-05.yaml](evidence/phase-1/schema-generation-migrations-2026-10-05.yaml).

### 2026-10-06 — migration preservation verified

The rehearsal now fingerprints every original column and row in seeded users/projects/uploads, streams reads and batches inserts, and validates new backfill columns against explicit migration policy. It rejects outlier and NULL corruption that count/minimum checks missed, validates early user/project migrations, and cleans up its own database after verification failure. **20 focused PostgreSQL tests and 587 regression tests passed**, with no failures/errors/skips. All five declared starts passed the final preservation, schema and Alembic checks; two seeded 10,000 upload metadata rows each. [Evidence](evidence/phase-1/migration-preservation-2026-10-06.yaml) retains exact hashes and per-table manifests.

The user supplied **45 GB** as the maximum database size they can qualify. The local disk has about **23 GiB free**; a 45 GB fixture plus migration overhead cannot fit. No scale qualification is claimed. Representative workflow data, omitted schema starts, previous-release readers and all other Phase 1 requirements remain open. After a host restart, the gate recheck found a missing Python target and stopped test database; recovery is in progress and prior passing output is not treated as a fresh gate pass.

### 2026-10-06 — all checked-in prior schema paths exercised

Added omitted revisions 0005, 0006, 0008 and 0009. A regression check compares the declared inventory with Alembic ancestry. Upload seeding now follows actual table availability. **21 focused tests passed**, and the final live campaign passed all **nine prior schemas**, each with schema/drift/preservation checks; six paths seeded **10,000 upload metadata rows** each. Explicit output flushing keeps fingerprint records intact alongside subprocess logs. The interrupted evidence parser from the earlier buffered run was not counted as valid manifest evidence.

Restored the local test environment after the host restart. Gate re-verification again reports **2 met / 5 unmet / 0 abandoned**. [Evidence](evidence/phase-1/migration-history-2026-10-06.yaml) records final source/log hashes and all nine manifests. No full Phase 1 completion or 45 GB qualification is claimed. Next compatibility issue found: migration 0008 emits lowercase storage values, but the current SQLAlchemy enum reader accepts enum names and rejects those lowercase values. Dataset/version rows must be represented in migration tests before this requirement can close.

### 2026-10-06 — storage enum compatibility

Eight real PostgreSQL cases reproduced failure to read lowercase storage identities emitted by migrations/defaults; all eight corresponding enum names already worked. The shared dataset-version column mapping now reads both known spellings, retains existing name spelling on ORM writes, and rejects unknown values. No stored URI or provider identity is rewritten by this fix. An actual 0007-to-head migration of eight legacy dataset versions verifies provider identity, immutable URI, content hash and 10 GiB numeric size metadata.

The schema verifier now unwraps dialect-specific decorated storage types when comparing default casts; its first failure and final fix are retained. **133 focused tests and 625 broader regression tests passed**, without failures/errors/skips. Alembic reports no new operations; Ruff and diff checks pass. Champion gates remain **2 met / 5 unmet / 0 abandoned**. [Evidence](evidence/phase-1/storage-enum-2026-10-06.yaml) records exact artifacts. Runtime changes have not been deployed to k3d.

Review identified twelve other enum-backed defaults whose lowercase values the current ORM rejects. That shared compatibility work, previous-release readers, production-sized qualification and the rest of Phase 1 remain open.

### 2026-10-06 — shared legacy enum defaults and filters

Reproduced all twelve remaining default-read failures against PostgreSQL, then generalized the storage mapping into `StoredEnum`. The affected columns now accept known enum names and values while retaining original write spelling and column widths. Literal equality/inequality and IN/NOT IN filters include both spellings, so reconcilers and status queries do not silently miss legacy rows. Existing value-based workflow/authority enum mappings remain unchanged.

The 637-test regression run passed **all 625 preexisting tests** and eight new cases. Four new cases hit VARCHAR(6) before reaching the intended invalid-enum check; shortening only that fixture value resolved them. The final **12-case default/filter suite passed**, including all enum members, unknown read/write/filter rejection, NULL and empty IN behavior. No clean 637-test single run is claimed. Schema verification and Alembic checks pass; Ruff/diff checks pass. Gates remain **2 met / 5 unmet / 0 abandoned**. [Evidence](evidence/phase-1/enum-defaults-2026-10-06.yaml) preserves the failures and final checks. Previous-release binaries, deployment and the other Phase 1 requirements remain unqualified.
