# Production readiness remediation

Updated 2026-10-30. Started 2026-09-24 from branch `codex/production-readiness`, HEAD `7a47ed8`. See the [initial assessment](phase-0-9-branch-assessment-2026-09-24.md) and [item inventory](phase-0-9-item-assessment-2026-09-24.csv). Those documents remain the historical audit; this ledger records subsequent fixes.

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

### 2026-10-07 — recover an operator-aborted evaluator handoff

An operator could seal an unregistered allocation with a reason different from the controller’s fixed reason. The controller then retried forever on an idempotency conflict. Six real-authority/controller cases reproduced this across allocation, refit and evaluator-registration crash boundaries, with both expiry and cancellation. The controller now recovers the original signed abort after a conflict, validating its allocation/scope/manifest/CAS, request digest and signature. It preserves the original reason and does not rewrite the receipt.

**35 focused tests and 650 regression tests passed**, with no failures/errors/skips. The final regression includes two terminal replays, unchanged receipt checks, no evaluator registration or workloads, and seven tampered/missing/duplicate evidence cases. An interim test expected the wrong local event for handoffs with an existing evaluator plan; it was corrected to check the actual event and original receipt. [Evidence](evidence/phase-1/prior-abort-recovery-2026-10-07.yaml) preserves all results. Native k3d qualification and the full Phase 1 gate remain open.

### 2026-10-07 — champion contract gates audited and reconnected

Three gate commands still named aggregate test files that did not exist. Reviewed the actual source and assertion bodies before mapping them to explicit suites. Refit checks verify preregistered rows, sealed policy, unchanged selection on retry, stale publication rejection and one frozen checkpoint. Evaluation checks verify frozen inference, signed immutable results, lost acknowledgements, stored-output recovery and no second grant/read. Scheduling checks cover the application loop, exact membership barrier, separate durable Job submissions, replacement fences and stale-worker rejection.

Fresh gate runs passed **61 refit**, **87 evaluation** and **146 reconciliation tests**, all without failures/errors/skips; these suites overlap. The champion ledger is now **5 met / 2 unmet / 0 abandoned**. [Audit evidence](evidence/phase-1/champion-contract-audit-2026-10-07.yaml) maps each gate to concrete assertions and preserves source/test hashes. English gate requirements were retained.

These contract suites use real PostgreSQL, actual model execution and local HTTPS/authority flows where applicable, plus Kubernetes API doubles. They do not substitute for native Kubernetes/network qualification. G5’s remaining live matrix and G6’s final integration audit stay open, as do the separate Phase 1 ledger and 45 GB database qualification. Current host free space is about 22 GiB.

## Phase 1 current image and native commit-fault preparation

The normal API Dockerfile now builds the current source and locked dependencies; `.unlazy` is excluded from its build context. Image imports and selected source hashes were verified before deploying its immutable amd64 digest to the QA API and authority. The QA-only commit-fault proxy has seven passing local checks. Native stored-result recovery and lost-commit-reply recovery both passed on separate ten-row synthetic scopes on 2026-10-07/08. Each worker exited after the intended dropped connection, and the controller recovered the verified stored result with exactly one grant and one commit, complete cleanup and two terminal replays. The first probe needed QA pod-log read permission; it resumed the original scope without reseeding.

The rollout caused shared-host disk pressure, interrupting existing MLflow and QA databases. All existing deployments and QA services recovered after removing selected unused API build cache and allowing Kubernetes to clear disk pressure. Verified TLS readiness checks pass for the QA API and authority. Application volumes and images were retained; eviction safeguards remain enabled. The [current image and fault record](evidence/phase-1/current-image-commit-fault-2026-10-07.yaml) records this limitation. This qualifies those two bounded failure cases; the full training journey, other native boundaries and whole-phase completion remain open. The user’s 45 GB migration qualification ceiling remains an unexecuted gate.

## Phase 1 deletion evidence retention

Project deletion previously erased its own tombstone and stage history through both explicit table cleanup and cascading project foreign keys. The cleanup now retains those two tables and records completion. Migration `0011_preserve_deletion_evidence` preserves their historical project IDs and retains the composite stage-to-tombstone tenant constraint. Run-deletion replay also preserves the original completion timestamp. Downgrade fails without changing data when retained evidence references a removed project.

The PostgreSQL baseline reproduced the lost tombstone. The fix passes all 14 project-management integration tests, 23 migration tests and 267 related regression tests, with no skipped cases. These counts overlap. This change has not been deployed to k3d; its current API image remains on the prior schema. Full resource-class deletion stages, external-store and backup cleanup, the 45 GB rehearsal, and Phase 1 completion remain open.

All ten prior schema revisions also passed rehearsal to head 0011, including 10,000 upload metadata rows at each of seven upload-capable starts, preserved row fingerprints, the 42-table schema verifier and Alembic drift checks. See [deletion retention evidence](evidence/phase-1/deletion-retention-2026-10-08.yaml). This bounded rehearsal does not qualify the 45 GB target.

## Phase 1 durable deletion stages

Project/run deletion now records its existing runtime, object-store and metadata work as durable stages; projects also record prepared-store cleanup. Each outbox delivery completes one stage and defers through the existing worker transaction without consuming the failure budget. Old queued requests first persist missing stages. A lost prepared-store deletion acknowledgement retries that stage without repeating committed object-store cleanup. Active attempts and promotional scopes block deletion both at admission and before each stage, even when their model run is terminal.

The final 24 PostgreSQL integration cases pass; 210 related regression cases passed before four additional late-writer checks. Counts overlap. See [stage recovery evidence](evidence/phase-1/deletion-stages-2026-10-08.yaml). Kubernetes calls remain doubled in these checks, and this change is not deployed. Cloud object versions, multipart remnants, external MLflow/spill/replica/backup cleanup and the broader Phase 1 gates remain open.

## Phase 1 S3 prefix deletion

S3-compatible and AWS S3 prefix cleanup now removes explicit object versions and delete markers, aborts multipart uploads, verifies part removal and checks that the targeted listings become empty. It drains bounded first pages so deletion cannot invalidate a saved pagination marker, and fails if the provider denies deletion or reports no progress. It does not bypass retention controls.

All 20 storage checks passed. A verified-TLS local SeaweedFS test removed two versions, a marker and an unfinished multipart upload, preserved an adjacent prefix and passed two replays. The current driver source was injected into the existing QA process for this probe; no rebuilt application image was deployed. See [S3 prefix evidence](evidence/phase-1/s3-prefix-deletion-2026-10-08.yaml). Exact-object artifact cleanup, Azure/GCS history, actual AWS IAM/retention, external replicas/backups and whole-phase qualification remain open.

Final combined deletion/reconciler/operations/storage regression: **234 passed, no failures, errors or skips**. An earlier invocation had 24 setup errors because the disposable PostgreSQL container was stopped; all 234 passed in one run after restarting that same test database and verifying its schema revision.

## Phase 1 exact-object deletion

S3 artifact and expired-upload cleanup now reuse the prefix cleanup implementation with exact-key filtering across SDK pages. Upload cleanup always requests deletion for a recorded URI, including when a delete marker hides the current object. Missing-object deletion is also idempotent for Azure and GCS; authorization and other errors still propagate.

All 40 storage/upload-service checks pass. A verified-TLS local SeaweedFS probe removed the target’s two versions, marker and multipart upload while preserving suffix, child and adjacent-project keys, then passed two replays and cleaned its test data. See [exact-object deletion evidence](evidence/phase-1/exact-object-deletion-2026-10-08.yaml). This does not qualify Azure/GCS historical versions, cloud retention/replication/backups, the 45 GB migration or a newly deployed application image.

Final combined deletion, reconciliation, operations, storage and upload-service regression: **254 passed, no failures, errors or skips**.


## Phase 1 GCS generations and retention waits

GCS exact-key and prefix deletion now drain bounded batches of generation-pinned objects and check for soft-deleted history. Retained history keeps metadata and tombstones pending. The existing outbox retries after an hour without consuming its failure budget; expired and quarantined uploads also defer while allowing other cleanup rows to advance. Authorization errors, missing-policy listing errors and lack of deletion progress remain failures, never evidence of complete deletion.

All **63 focused checks** and **270 combined regression checks** passed, with no failures, errors or skips; counts overlap. PostgreSQL cases exercise 32 retention deliveries for both project and run deletion and limit-one upload cleanup across three statuses. See [GCS retention evidence](evidence/phase-1/gcs-retention-deletion-2026-10-09.yaml). Native GCS configuration behavior, Azure history, external replicas/backups and a deployed-image rehearsal remain open. The 45 GB migration target is recorded; current free disk cannot safely accommodate the database plus migration overhead and the cluster’s reserve. Phase 1 remains incomplete.


## Phase 1 Azure deletion history

Azure exact-key and prefix cleanup now enumerate versions, snapshots, soft-deleted entries and uncommitted blobs. Deleting a current blob is followed by another pass to remove its demoted version. Snapshot and prior-version deletions use their individual identities; retained entries keep the request pending through the existing retention retry path. No policy changes or permanent-delete bypass are used.

All **63 focused checks** and **278 combined regression checks** passed with no failures, errors or skips; counts overlap. The SDK contract fixtures cover more than 1,000 versions, replay, neighboring objects, retention expiry and provider failure cases. See [Azure history evidence](evidence/phase-1/azure-history-deletion-2026-10-09.yaml). Native Azure/GCS qualification, a rebuilt/deployed image, external replicas/backups, the 45 GB migration and broader Phase 1 requirements remain open.


## Phase 1 native deletion rehearsal and QA storage incident

The normal API image `sha256:c0ae5a3ba688eda3c6be07ae2fad5e3740cc8dd5b059358e850964fdc30891cc` is deployed only in `qa-refit`. The QA database was backed up, migrated to `0011_preserve_deletion_evidence`, and passed the 42-table schema verifier. Running source hashes match the worktree. A new native PostgreSQL/S3-compatible fixture passes four durable stages, recovery after a lost object-deletion acknowledgement, two terminal replays, retained tombstone/stage evidence, and neighboring-object preservation. This is not process-death or multiworker qualification.

**QA incident:** while correcting the application identity’s missing bucket-scoped listing permission, the agent restarted `qa-refit/refit-store` without first checking its volume type. The existing deployment used `emptyDir`, so the restart discarded that ephemeral store. A subsequent inventory found 13 distinct historical dataset/artifact URIs in the QA database, all missing from the configured store. This is a post-incident inventory; no before/after byte inventory exists. No artifact backup was found in the inspected QA evidence directories. The main namespace was not modified, and the QA database remains intact.

The QA store now uses a 1 GiB PVC with a Recreate rollout strategy. Its recreated bucket has versioning enabled, the application has bucket-scoped listing permission, and a control object survived a further store restart. This repair does not recover the historical objects. The original preservation gate G7 is abandoned and requires handoff; G8 records only the new rehearsal on the repaired store. Historical fixture recovery/requalification and Phase 1 completion remain open. See [native evidence and incident record](evidence/phase-1/native-deletion-and-qa-storage-incident-2026-10-09.yaml).


## QA fixture recovery after the storage incident

Expanded inspection found **57 distinct object references across 13 synthetic projects**, including partitions, candidate models, checkpoints and output intents. **50 objects were reconstructed and restored only after matching their original database SHA-256 records.** Downloaded-byte verification and a fingerprint of seven workflow tables passed again after a later host/cluster restart. All 13 dataset-version objects are among the restored objects.

**Seven objects remain missing:** two fitted pipeline objects and five signed evaluation-result files. Offline execution of the original slow refit plan produced different model bytes, which were not substituted. No evaluator-scoped signing Secrets remain, and no final-data evaluation or receipt was repeated. G7 remains abandoned; G9 verifies only the bounded partial recovery and its remaining-reference inventory. This does not establish complete historical recovery or Phase 1 readiness. See [partial recovery evidence](evidence/phase-1/qa-storage-partial-recovery-2026-10-09.yaml).


## Phase 1 training action replay

Restart and add-model actions now persist the complete response, including parent/child links, through the existing durable mutation helper. Replays return before checking mutable dataset storage, estimator catalogs or completed leaderboards. Earlier launch-only records remain usable when their stored lineage and request fields match. Parent tags are refreshed under a row lock before merging links. Overlong generated model-selection keys are hashed; existing short keys retain replay compatibility.

All **14 PostgreSQL-backed action checks** and **87 existing training/schema/route checks** passed without failures, errors or skips. Two-connection tests cover identical requests, conflicting tuning and different model additions. The new tests stub runtime launch; they do not qualify Kubernetes submission, process death or a full training journey. See [training replay evidence](evidence/phase-1/training-action-replay-2026-10-09.yaml). These changes are not yet deployed to k3d.

Phase 1 remains incomplete. The 45 GB migration still exceeds available host storage before overhead and cluster reserve. External MLflow deletion, other provider/native failure tests and seven missing historical QA objects remain open.


## Phase 1 replay authorization

Cached durable-command and contract-command responses could bypass project permissions because the check ran only inside the new-command callback. Both shared entry points now verify current project permissions before command lookup or creation. Deployment, project cleanup and monitoring ingestion retain their admin requirement; other durable mutation callers require editor access.

All **24 focused authorization checks** and **250 regression checks** passed without failures, errors or skips. PostgreSQL tests cover outsiders, revoked and expired membership, insufficient roles, and authorized replay. HTTP cleanup tests verify that viewers/editors receive 403 without private response data while admins replay without executing cleanup again. The audit covers all 14 durable mutation callers and both contract-helper call sites. See [replay authorization evidence](evidence/phase-1/replay-authorization-2026-10-09.yaml).

These fixes remain local to the branch and are not deployed to k3d. Phase 1 remains incomplete; malformed idempotency headers, the broader runtime failure matrix, the 45 GB migration, external deletion coverage and historical QA recovery remain outstanding.


## Phase 1 idempotency header boundary

All 21 documented `Idempotency-Key` header parameters now enforce the database's 255-character limit and reject empty or whitespace-only values. Valid values are preserved exactly, and the estimate endpoint still accepts an omitted key when a reservation is not requested.

All **41 boundary and route checks** pass without failures, errors or skips; 19 overlap the replay-authorization regression suite. Tests exercise each real endpoint annotation through FastAPI and reconcile the tested set with OpenAPI. See [header validation evidence](evidence/phase-1/idempotency-headers-2026-10-09.yaml). The fix is not yet deployed to k3d and does not complete Phase 1.


## Phase 1 outbox delivery ownership

The reconciler previously checked the outbox owner only after invoking an external handler. It now locks and refreshes the claim before dispatch, rejects expired or superseded ownership, and skips deleted or completed entries. Upload verification rechecks ownership before heartbeat renewal and terminal publication after releasing its transaction. A stale worker cannot extend the replacement lease or turn an ownership error into a scanner quarantine.

All **14 focused PostgreSQL cases** and **190 regression checks** passed without failures, errors or skips. Tests cover actual claim takeover through another connection, stale ORM state, lock-held competition, two terminal replays and upload ownership changes at both commit boundaries. See [delivery ownership evidence](evidence/phase-1/outbox-delivery-fence-2026-10-10.yaml). These changes are not deployed to k3d and do not prove recovery from every external request already in flight at database loss.

That audit also identified process-local profiling scheduling and unconditional startup resets. The following increment replaces that path. The unused generic attempt claimer and native worker recovery still require qualification; Phase 1 remains incomplete.

## Phase 1 durable local profiling

Local profiling now enqueues its command and outbox delivery in the same transaction as the job. The reconciler owns execution, renews the lease during computation and checks the exact delivery before every checkpoint and dispatcher outcome. Legacy recovery backfills missing commands without resetting running jobs. Artifact paths are unique to each execution, and retries reuse committed feature checkpoints.

All **13 focused PostgreSQL cases and 171 regression checks** passed without failures, errors or skips. Coverage includes real CSV processing, existing Excel metadata-only behavior, actual claim takeover, same-worker replacement, operator replay after counter reset, cancellation, heartbeat renewal, exhausted retries and two completed replays. See [local profiling evidence](evidence/phase-1/local-profiling-delivery-2026-10-11.yaml).

This change has not been deployed to k3d. Local computation occupies a reconciler process; replica fairness, process death and provider requests already in flight remain unqualified. It does not add typed workflow attempts for local profiling. The 45 GB migration target remains unrun with only about 41.1 GB free, and seven historical QA objects remain missing. The original QA preservation gate remains abandoned; Phase 1 remains incomplete.

## Phase 1 generic attempt claim refresh

A PostgreSQL session holding a cached attempt could reclaim it using an outdated retry count, overwriting another worker's increment and allowing additional claims beyond the configured budget. The generic attempt claimer now refreshes the locked row before changing ownership and incrementing the counter.

The new database test failed before the fix and passed afterward; all **58 PostgreSQL and workflow-state checks** passed twice without skips. See [attempt claim evidence](evidence/phase-1/attempt-claim-refresh-2026-10-11.yaml). This helper currently has no production callers, so these results do not qualify the native workers. QA remains healthy on the older image; available disk is below the existing 43 GB build guard. No build or cluster mutation was performed.

## Phase 1 training result commit recovery

A lost acknowledgement after the training-result transaction could cause repeated metric insertion or allow the same attempt to replace an already committed result. Result publication now stores a canonical receipt with the metrics and metadata. Identical replay is a no-op; conflicting replay is rejected. Late progress and failure writes preserve that result. Worker startup and the observer can finish its terminal CAS without retraining or creating a replacement attempt.

All **18 PostgreSQL cases and 156 regression checks** pass without skips. Two new tests reproduced duplicate metrics and conflicting overwrite before the fix. Tests also cover concurrent writers, superseded/cancelled fences, a successful successor, late progress, worker recovery and observer recovery. See [training result recovery evidence](evidence/phase-1/training-result-recovery-2026-10-11.yaml).

Model artifacts and MLflow identifiers in these checks are synthetic. This does not prove live artifact preservation, native Ray process-loss recovery or full Phase 1 completion. Legacy results without receipts remain unqualified. The current worktree is not deployed; the 45 GB migration and seven missing historical QA objects remain unresolved.

## Phase 1 candidate artifact isolation

Different model results from the same attempt previously shared a model object key. Candidate keys now include the serialized content's SHA-256 digest, so a late different result cannot overwrite a committed model. Identical retries retain the same reference. Existing checkpoint URIs remain readable.

Embedded storage previously truncated the destination before copying replacement bytes. It now writes and fsyncs a temporary sibling, then atomically replaces the destination. Input failures preserve the old complete file and remove temporary output. Byte writes use this same path.

All **65 artifact, storage, PostgreSQL publication, profiling and upload-driver checks** pass without skips. Negative controls reproduced all three failures before the fix. A fitted Ridge model retains its original bytes and predictions after another model is stored. See [artifact isolation evidence](evidence/phase-1/model-artifact-isolation-2026-10-11.yaml).

These are local embedded-store checks, not remote-provider or k3d qualification. Host power loss is untested; a process killed before replacement can leave an unreferenced temporary file. This change does not repair historical missing objects or complete Phase 1.

## Phase 1 stop candidate work after lost ownership

Supervised and clustering candidate exception handlers previously treated a stale fence as a model failure. GPU candidates could retry on CPU, and both paths could create a new MLflow failure run after losing ownership. Both handlers now propagate `StaleFence` immediately.

All **61 training, candidate recovery and PostgreSQL publication checks** passed twice without skips. Three new negative controls reproduced the swallowed exception before the fix. Ordinary model-error and GPU fallback tests still pass. See [candidate fence evidence](evidence/phase-1/candidate-fence-stop-2026-10-11.yaml).

External effects in the focused cases are mocked. This does not qualify a live GPU or MLflow deployment. MLflow run creation before durable identifier persistence, with some lineage tags added in a later request, remains an open recovery/deletion gap. The 45 GB rehearsal still requires additional storage; the mounted data filesystem has about 41.1 GB free, and an alternative disk/environment has been requested. Phase 1 remains incomplete.

## Phase 1 durable MLflow tracking identity

Parent, supervised candidate, clustering candidate and failure tracking now persist a workflow command before creating an MLflow run. Creation includes the command and project/run/attempt lineage tags in the same request. A visible run can therefore be found after a lost response, and its ID is persisted before model work. Replay resumes that ID instead of creating another record.

A creation-start marker is committed before sending the request. If its outcome is uncertain and no run is visible, the worker refuses another creation request. Concurrent callers can encounter this state until the first result appears. Unresolved tracking bypasses candidate fallback and failure logging. Deleted, missing, duplicate or incorrectly tagged tracking records fail closed, and committed training results reject new tracking activity.

All **13 focused PostgreSQL/MLflow cases and 101 regressions** pass without skips. MLflow uses isolated local SQLite stores; acknowledgement loss is injected after real store/database commits. See [tracking intent evidence](evidence/phase-1/tracking-intent-2026-10-11.yaml).

The following increment adds bounded lookup reconciliation for visible outcomes; safe operator resolution of a permanently uncertain creation remains open. Live HTTP/k3d recovery, legacy untagged records, externally in-flight logging/model registration and full MLflow deletion remain open. This is progress toward the side-effect requirements, not Phase 1 completion.

## Phase 1 tracking outcome reconciliation

The worker loop now reconciles due MLflow creation intents independently of the original training worker. It only looks up remote records: one matching backend/lineage record is bound to the durable command without creating or resuming a remote run. Live writer locks are skipped, and competing reconcilers cannot bind the same intent twice.

Unresolved lookups are delayed by 60 seconds and stop at the command's retry budget. The creation-start marker and a redacted reason remain, so exhaustion cannot trigger blind creation. A later explicit worker replay can adopt a record that has since become visible.

All **78 tracking, PostgreSQL publication, training and worker-loop checks** pass without skips, including two concurrent reconcilers and a live writer lock. See [tracking reconciliation evidence](evidence/phase-1/tracking-reconciliation-2026-10-11.yaml).

This recovers tracking metadata, not failed training jobs. Permanently unknown outcomes still need safe operator resolution; legacy untagged or deleted application-run histories, full external deletion and live HTTP/k3d qualification remain open. Phase 1 remains incomplete.

## Phase 1 MLflow HTTP retry ownership

MLflow's HTTP SDK retried run-creation POST requests internally. A real HTTP fault test reproduced two committed runs after a proxy substituted a 503 for the first successful response. The worker and shared tracking helper now force zero SDK HTTP retries, leaving uncertain creation with the durable command. A replay that still cannot find the remote result also retains `TrackingCreationUnresolved`, preventing candidate fallback from treating it as an ordinary model error.

All **72 HTTP, tracking, worker and training checks** pass without skips. Both a substituted 503 and a dropped connection after committed creation leave one run; two replays recover the same ID without another creation request. The isolated server processes were cleaned up. See [HTTP retry evidence](evidence/phase-1/tracking-http-retries-2026-10-12.yaml).

This is local HTTP qualification with SQLite tracking metadata and PostgreSQL workflow state. The current worktree remains undeployed; k3d, TLS and remote artifact providers are unqualified. Disabling SDK retries also surfaces transient read/logging failures immediately; the complete retry-owner inventory remains open. The 45 GB migration needs additional storage, seven historical QA objects remain missing, and Phase 1 is not complete.

## Phase 1 tracking queue contention

The tracking reconciler previously selected a limited list of oldest command IDs before checking ownership. A busy run or an orphaned command could repeatedly consume that list, starving eligible work behind it. Command-row contention could also block until PostgreSQL's lock timeout.

The reconciler now selects and locks one eligible run/command pair with `FOR UPDATE SKIP LOCKED` before applying the limit, committing each lookup independently. The batch limit bounds actual claimed work. It preserves the read-only remote lookup, retry budget and fail-closed lineage checks.

All **34 tracking and worker-loop checks** pass without skips. Three negative controls reproduced run-lock starvation, command-lock timeout and orphan-row starvation before the fix. With a one-item batch, later eligible work now completes while the blocked command remains unchanged; released locks allow subsequent recovery. See [tracking claim evidence](evidence/phase-1/tracking-claims-2026-10-12.yaml).

These checks use real PostgreSQL contention and isolated SQLite MLflow stores. They do not qualify deployed replicas or recover orphaned tracking history; database locks remain held during remote lookup. The current worktree, 45 GB migration and historical QA object recovery remain unqualified. Phase 1 is not complete.

## Phase 1 Ray candidate tracking errors

Ray candidate collection previously converted `TrackingCreationUnresolved` into an ordinary failed-model result. A tournament could then finish while a candidate's remote tracking outcome was unknown. Collection now propagates that error, as it already did for stale ownership, and cancels pending candidate tasks through the existing cleanup path.

All **70 candidate, partition, recovery, training and Tune checks** pass without skips. Six cases use actual Ray error wrappers and cloudpickle serialization for supervised/clustering modes: stale ownership and uncertain tracking abort, while ordinary model errors remain failed-candidate results. Both uncertain-tracking cases failed before the fix. See [Ray tracking error evidence](evidence/phase-1/ray-tracking-errors-2026-10-12.yaml).

The retry inventory confirms explicit zero-retry settings for KubeRay job/submission, candidate Ray tasks and Tune trial failures. That does not prove durable per-trial/Train identity mapping or native process-loss recovery. The focused error-propagation cases mock task scheduling. The same suite also starts a local two-CPU Ray runtime for a separate two-trial Tune smoke test; that is not native failure/cancellation qualification. Current-image deployment, the 45 GB migration and seven missing historical QA objects remain unresolved; Phase 1 is incomplete.

## Phase 1 actual trial execution paths

The default production RayJob launches whole-candidate workers, which change execution mode and use local `BayesSearchCV`; they bypass the standalone Tune helper. Fixed-parameter supervised CV and clustering sweeps also need durable enrollment. No direct production `TrainingTrial` construction site was found. Wiring only Tune would therefore leave the default execution path untracked.

The [trial ownership implementation contract](phase-1-trial-ownership.md) now covers all these paths, pre-execution enrollment, parent/child fencing, leases, immutable result replay, checkpoint allowlists and durable replacements. **This is an audited gap and implementation contract, not an implemented feature.** Fourteen manifest/candidate/Tune checks passed twice without skips; [source-bound evidence](evidence/phase-1/trial-execution-paths-2026-10-12.yaml) records the audit limits.

Prior Ray tracking evidence was corrected: its focused error-propagation tests mocked scheduling, while the combined suite also ran a real local two-trial Tune smoke test. Neither qualifies deployed worker-loss/cancellation behavior. All 36 broad Phase 1 requirements remain open.

## Phase 1 transactional trial enrollment primitive

Added enrollment using the existing trial and typed attempt tables. A suggestion binds immutable parameters to one logical trial, with a parent-owned pending execution. Parent/run locks serialize concurrent enrollment and replacement. Stale ownership, conflicting identity, expired parent leases, unapproved predecessor results and exhausted replacement budgets fail before dispatch. Callers must commit enrollment before running work.

All **50 PostgreSQL checks** pass without skips, including **20 enrollment cases**. These cover concurrent same/conflicting requests, rollback, a lost acknowledgement after a real database commit, cached stale parents, wrong candidate lineage and append-only superseded-parent replacement. See [enrollment evidence](evidence/phase-1/trial-enrollment-2026-10-13.yaml).

There is no production execution caller yet. Worker start, heartbeat, child grants, result publication, accelerator fallback and search-path integration remain required by the [trial ownership contract](phase-1-trial-ownership.md). The primitive's two gates pass; the broader trial-ownership work retains four unmet gates. Phase 1 remains incomplete.

## Phase 1 trial worker lifecycle primitives

Added leased worker start, heartbeat and terminal publication around enrolled trials. Parent and child fences are checked with fresh database state. Only one worker can start; expired owners cannot renew or publish. Startup replay consumes one claim-budget use. Terminal status, logical result digest, event evidence and CAS are committed together; identical replay returns the existing result and conflicting publication fails.

All **120 checks** pass without skips, including **38 lifecycle cases**, 20 enrollment cases and PostgreSQL/upload-policy regressions. Cases cover competing workers, expired ownership, superseded parents, concurrent same/conflicting results, lost start/terminal commit acknowledgements, rollback and stale/corrupted evidence. See [trial lifecycle evidence](evidence/phase-1/trial-lifecycle-2026-10-13.yaml).

The failure-record test also exposed URL passwords surviving shared redaction. Backend URL userinfo is now removed, and malformed URLs no longer abort redaction. Eight new scheme/malformed-URL cases accompany the existing policy checks.

Production workers still do not call these lifecycle primitives. Actual search-path integration, signed grants, predecessor adoption, owned fallback and recovery remain required. The primitive's two gates pass; all four broader trial-ownership gates and all 36 Phase 1 requirements remain open.

## Phase 1 fixed CPU CV execution integration

Fixed-parameter CPU cross-validation now calls the durable trial lifecycle. The adapter commits identity and the worker lease before scoring, maintains a heartbeat and commits terminal evidence afterward. Repeated calls return committed results or failures without rescoring. Missing production fences, conflicting identities and uncertain database commits stop execution instead of becoming ordinary candidate fallback.

All **122 checks** pass without skips, including **15 adapter/integration cases**. Actual Ridge CV verifies the lease is visible from another PostgreSQL session before computation. The real preprocessing/selection/Ridge pipeline replays its CV result; external tracking and artifact effects are mocked. Tests cover lost enrollment/start/terminal acknowledgements, parent supersession, heartbeat, invalid numerical results and Ray exception propagation. See [fixed CPU trial evidence](evidence/phase-1/fixed-cpu-trials-2026-10-13.yaml).

The real pipeline test exposed a feature-selector bug: positive percentile rounding could remove a sole input feature. The selector now retains one feature in that case while preserving explicit zero-percentile selection. Existing sampling, serialization and prediction checks still pass.

GPU CV, parameter search, clustering, Tune and production Train are not yet enrolled. Final full-data model fitting and candidate side effects remain outside the CV trial. An uncertain start fails closed pending durable recovery; signed grants and predecessor adoption remain open. All four broader trial-ownership gates and all 36 Phase 1 requirements remain unmet.

## Phase 1 default local CPU parameter search

The default whole-candidate CPU worker now runs an explicit deterministic suggestion loop. Each suggestion is enrolled and leased before CV. Its durable identity includes the search revision, seed, dimensions, iteration count, scoring, CV specification, base estimator parameters and proposed configuration. Replay rebuilds the optimizer history from committed trial results without rescoring. This uses the shared versioned search policy; it does not claim the same proposal sequence as legacy `BayesSearchCV`.

All **70 checks** pass without skips, including **seven local-search cases** with real PostgreSQL and Ridge CV. Actual candidate pipeline replay produces the same selected parameters while avoiding both untracked Bayes search and nested Tune scheduling. Changed search contracts and superseded parents stop further computation. Ordinary failed configurations remain failed on replay. The suite also includes the existing real local Tune smoke test. See [local CPU search evidence](evidence/phase-1/local-cpu-search-2026-10-13.yaml).

Unfenced local legacy execution retains its existing behavior. GPU CV/search, clustering, standalone Tune enrollment, production Train, signed grants and predecessor adoption remain open. Final model fitting and external candidate side effects are outside the trial receipts. All four broader trial-ownership gates and all 36 Phase 1 requirements remain unmet.

## Phase 1 clustering trial integration

Each clustering parameter set now commits durable ownership before fold preparation or fitting. Lazy preparation retains fold reuse within the invocation. Replay reads committed fold metrics and selects the same winning parameters without repeating CV preprocessing or scoring. Changed CV contracts and superseded parent ownership stop execution; ordinary model failure is recorded and replayed.

All **72 checks** pass with no failures, errors or skips, including **five clustering cases** with real PostgreSQL, KMeans, AgglomerativeClustering, preprocessing and metrics. External tracking and artifacts are mocked. The baseline ownership-before-preparation test failed against the previous implementation. See [clustering trial evidence](evidence/phase-1/clustering-trials-2026-10-14.yaml).

Final model fitting remains outside the trial receipt. GPU hardware, current-image k3d execution, signed grants, predecessor adoption, durable child recovery and remaining supervised GPU/Tune/Train integration are unqualified. All four broader trial-ownership gates and all 36 Phase 1 requirements remain open. The 45 GB rehearsal still needs sufficient storage; seven historical QA objects remain missing.

## Phase 1 supervised accelerator trial ownership

GPU-selected fixed CV and fenced parameter search now use durable trial enrollment. Accelerator-prefixed identities preserve the GPU measurement; CPU fallback uses distinct cpu-fallback identities with the source accelerator recorded. The CPU fallback search remains local even when the parent mode is ray, avoiding an untracked switch to standalone Tune. Cached GPU failures and fallback results replay without CV recomputation; stale ownership propagates without fallback.

All **86 checks** pass without skips, including **12 accelerator cases** covering candidate/ray modes, fixed/search models, success, ordinary failure and stale ownership. PostgreSQL, Ridge CV and preprocessing are real; GPU selection and kernel failures are simulated. See [accelerator trial evidence](evidence/phase-1/accelerator-trials-2026-10-14.yaml).

These are separate logical accelerator measurements under one candidate and parent. They do not implement replacement generations or predecessor adoption. Final model fitting and external effects remain outside CV receipts and may repeat. Actual GPU hardware, current-image native execution, Tune/Train integration, grants and durable recovery remain open. All four broad trial gates and all 36 Phase 1 requirements remain unmet; storage for the 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 standalone Tune trial ownership

Tune's searcher now commits each logical suggestion and a digest-bound physical Ray trial-ID event before returning the configuration for dispatch. Workers receive explicit parent context through Ray's parameter handoff and use the existing leased evaluation lifecycle. Fence tokens stay out of suggestion configurations and the searcher's JSON checkpoint. Restoring algorithm state retains the caller's execution context. Fenced dispatch requires a durable run/candidate contract before scheduling.

All **103 checks** pass without failures, errors or skips, including **10 Tune ownership cases**. Real PostgreSQL and a local two-CPU Ray runtime execute two Ridge CV trials and redispatch them in a fresh Tune invocation; the database retains two terminal CAS operations and four physical dispatch events. A direct worker replay forbids scoring. Lost dispatch acknowledgements, corrupted events, changed contracts, stale parents and serialized Ray ownership errors are covered. The legacy real Tune smoke test also passes with explicit local mode. Shared CPU, simulated-GPU and clustering regressions pass. See [Tune ownership evidence](evidence/phase-1/tune-trials-2026-10-14.yaml).

This proves same-parent CV receipt replay. Signed grants, predecessor adoption, durable replacement recovery, production Train and current-image native failure campaigns remain incomplete. Final fitting and external effects remain outside CV receipts. All four broad trial gates and all 36 Phase 1 requirements remain open. The 45 GB migration rehearsal still needs sufficient storage; seven historical QA objects remain missing.

## Phase 1 replacement trial execution handoff

The reconciler creates pending child replacements before resubmitting a lost run. Enrollment previously rejected these rows because their submission identity was unset. It now binds the first identity only to untouched pending replacements with matching predecessor, parent, project/run/trial, logical key, workload and generation budgets. Identical concurrent bindings converge; conflicting identities and altered rows fail closed.

Exhausted child budgets now stop parent replacement. Testing also exposed a forbidden pending-to-failed transition. Terminal recovery now cancels pending children and fails started children, keeping logical trial status consistent.

All **228 checks** pass without failures, errors or skips, including **21 replacement cases** with real PostgreSQL. Cases cover two active children superseded together, rejected old-worker publication, valid replacement completion/replay, actual Ridge CV after committed lease acquisition, transaction rollback, invalid replacement rows, concurrent binding, and child budget exhaustion in all four active statuses. See [replacement handoff evidence](evidence/phase-1/trial-replacement-handoff-2026-10-14.yaml).

Signed grants, predecessor checkpoint/result adoption, full durable recovery, production Train and native failure qualification remain open. All four broad trial gates and all 36 Phase 1 requirements remain unmet. The 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 verified predecessor CV result adoption

Replacement runs can now reuse explicitly authorized predecessor CV results and ordinary failures. The reconciler verifies terminal receipts and records exact source attempt/trial/digest references in the existing allowlist, alongside object checkpoint URIs. These structured database references grant no object access. Enrollment verifies terminal event type, payload digest, terminal CAS, parameters and parent lineage, then commits a new adoption event and terminal receipt without rewriting source evidence or running CV. Generation budgets remain enforced.

A replacement may die before adopting every result. Verified references now carry forward, with exact authorization required at each skipped superseded-parent generation. Missing or changed authorization fails closed.

All **259 checks** pass without failures, errors or skips, including **31 adoption cases** with real PostgreSQL, Ridge CV and local parameter search. A partial search recovers two completed scores and evaluates only its unfinished third suggestion; another replay performs no scoring. Cases also cover mixed active/completed trials, failures, concurrent adoption, lost commit acknowledgement, rollback, repeated replacements, corrupt event payload/type/digest, altered scope/workload/parameters, unauthorized references and exhausted budgets. See [trial result adoption evidence](evidence/phase-1/trial-result-adoption-2026-10-14.yaml).

Signed grants, predecessor object-checkpoint authorization, full native recovery, production Train, final fitting and external effects remain open. All four broader trial gates and all 36 Phase 1 requirements remain unmet. The 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 owned training-parent leases

The training entrypoint now claims one worker owner and a renewable 90-second parent lease. It renews every 15 seconds and stops its heartbeat when execution exits. Concurrent competing starts are rejected. Expired parents cannot restart, renew or publish progress/results/failure; another owner cannot complete or fail the parent. Already-committed result recovery remains valid after expiry through its existing receipt/fence/CAS.

Heartbeat uncertainty and stale ownership now propagate without being recorded as ordinary model failure. Cancelled runs cannot be resurrected during worker start.

All **318 checks** pass without failures, errors or skips, including **20 parent-lease cases** with real PostgreSQL. The actual worker entrypoint is exercised with synthetic training callbacks, including heartbeat renewal, real result/CAS publication, ordinary failure, ownership uncertainty, heartbeat connection failure and thread shutdown. Concurrency, expiry, wrong ownership and committed-result recovery are covered. See [parent lease evidence](evidence/phase-1/training-parent-lease-2026-10-14.yaml).

This is a database lease prerequisite for grants; it is not a signed credential broker. Signed grants, delegated object credentials, predecessor object-checkpoint access, automatic native expiry cleanup/replacement, production Train and current-image qualification remain open. Legacy no-lease attempts remain compatible outside the updated entrypoint. All four broad trial gates and all 36 Phase 1 requirements remain unmet. The 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 expired-parent cancellation and replacement

The observer now handles expired parent leases before querying Ray status, while preserving committed-result recovery priority. It queues durable cancellation of the old workload and bounded replacement of the parent/active trials in one transaction. Exhausted budgets still queue cancellation and stop replacement. Concurrent observation, repeat observation and rollback preserve one durable intent per generation.

Cancellation now verifies stage, project, run aggregate and fence before deleting, including already-terminal attempts. It uses the run's recorded namespace rather than the Kubernetes client's default.

All **369 regression checks** pass without skips, including **14 expiry cases** with real PostgreSQL. A separate **native k3d control-plane check** also passes: current local Python code rejects a stale fence, deletes a fresh suspended RayJob in its recorded namespace, replays deletion after absence and preserves a same-name neighbor in another namespace. No pods were observed; both isolated test namespaces were removed. See [parent expiry evidence](evidence/phase-1/training-parent-expiry-2026-10-14.yaml).

The native check uses current local code and suspended objects. It does not qualify a newly deployed image or real worker death/partition/restart. Signed grants, delegated object credentials, predecessor object-checkpoint reads, production Train and full native recovery remain open. All four broad trial gates and all 36 Phase 1 requirements remain unmet. The 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 pod-bound admission API

Added the server-side admission boundary for attempt credentials. Kubernetes TokenReview must attest the expected attempt/fence audience and a physical pod identity. Admission checks current attempt/run scope, pod UID, namespace, deletion state, labels and service-account subject. A KubeRay head's cluster-specific identity additionally requires the matching Pod → RayCluster → recorded RayJob controller UID chain. Accepted proofs expire within fifteen minutes; evidence stores token digests and scoped metadata. Replay checks evidence type and digest. The hidden API endpoint fails closed and uses no-store responses.

Helm grants TokenReview create and controller metadata read only to the API identity. No signing key or TokenRequest permission is given to workers.

All **364 regression checks** pass without failures, errors or skips, including **29 admission cases** and real PostgreSQL/Helm checks. A separate **native k3d admission check** passes with a real 600-second token bound to an unscheduled pod. Wrong audience, changed project/parent/fence and deleted pod are rejected; the pod never scheduled and its isolated namespace was removed. See [admission evidence](evidence/phase-1/attempt-admission-2026-10-14.yaml).

Token projection, worker consumption and expiry enforcement remain unimplemented; existing workers do not call the new endpoint yet. The complete issuer/consumer/delegated-provider/native requirements are retained in the [credential contract](phase-1-attempt-credentials.md). This API is not a complete credential broker and does not restrict existing worker database/object credentials. All four full credential gates, four broader trial gates and all 36 Phase 1 requirements remain open. The 45 GB rehearsal and seven missing historical QA objects remain unresolved.

## Phase 1 projected identity and training admission consumption

Ray head, worker and submitter templates now project dedicated 600-second attempt-audience tokens into a read-only directory without `subPath`, while preserving the head's autoscaler identity. Token bytes stay out of environment values and Tune configurations. Configure `attemptIdentity.controlBaseUrl` with the trusted HTTPS admission base. Production Helm and Ray manifest construction require it; production/staging workers cannot disable admission.

Training startup, trial dispatch/execution and parent/child renewal now request current admission before acquiring database locks. The client rereads the token file, rejects redirects and invalid identity/expiry, and caps leases to proof expiry. Child leases also respect the parent deadline. Cached results require current admission; late publication is rejected. Preparation and non-Ray analysis consumers still need integration.

All **415 regression checks** pass without failures, errors or skips, including **20 runtime cases** and actual worker entrypoint/Helm checks. A separate **local-worker/k3d check** uses a real pod-bound token through the actual admission/TokenReview boundary: a wrong token cannot renew, and a manually replaced valid token can renew. The file and isolated namespace were removed; the pod never scheduled. See [runtime admission evidence](evidence/phase-1/admission-runtime-2026-10-14.yaml).

The native HTTP path is an in-process relay. This does not qualify deployed HTTPS/egress, kubelet rotation, a current immutable image or running Ray head/worker identity. Separate child grants, delegated provider credentials, predecessor checkpoint access and full native recovery remain open. Existing database/object credentials remain broader than the final contract. All full credential/trial gates and all 36 Phase 1 requirements remain unmet; the 45 GB rehearsal and seven missing historical QA objects remain unresolved.


## Phase 1 splitter and preparation admission — 2026-10-15

Splitter/preparation startup now verifies admission, claims one worker owner and renews a 90-second lease every 15 seconds, capped to proof expiry. Progress and publication require current admission and live lineage/leases. Late failures cannot overwrite replacement or inactive-profile evidence. Server admission rejects inactive or moved profiles before TokenReview. Heartbeat uncertainty prevents publication; shutdown is requested on every exit.

Final verification passed **509 regression tests**, including **34 focused admission cases** and the existing real local Ray splitting tests, plus **two k3d identity probes**, with zero failures, errors or skips. The probes use real pod-bound tokens and actual TokenReview through an in-process HTTP relay. Wrong tokens, inactive profiles and expired uploads are denied. Both pods remained unscheduled; token files and isolated namespaces were removed. See [preparation admission evidence](evidence/phase-1/preparation-admission-2026-10-15.yaml).

Dataset lease-expiry cleanup/replacement, non-Ray analysis, separate child grants, provider object restrictions and full deployed TLS/rotation/process-loss qualification remain open. Existing database/object credentials remain broad; an upload racing expiry may leave an own-attempt orphan before metadata publication is rejected. All 36 Phase 1 requirements remain Open. The 45 GB migration ceiling is recorded; current free space is 42.8 GB before overhead. Seven historical QA objects remain missing and the original preservation gate remains abandoned.


## Phase 1 dataset lease-expiry recovery — 2026-10-15

The dataset observer now refreshes locked attempts/profile state, queues old-generation cancellation and bounded replacement atomically on lease expiry, and still queues cleanup when retries are exhausted. Cancellation uses a dedicated outbox handler with typed project/profile/stage/fence checks before external reads, physical RayJob scope-label checks and UID-preconditioned deletion. A missing RayJob is a successful cleanup replay. The old terminal CAS stays unchanged.

Review also found that replaying a retired submission could recreate a RayJob before its invalid status transition failed. Submission now validates complete command scope, refreshes current state and returns retired generations without Kubernetes effects. New submissions record their namespace; cancellation retains that namespace across replacement. Legacy submissions use the configured namespace explicitly.

Final verification passed **589 regression tests**, including **55 focused dataset cases**, and **two k3d control-plane probes**, without failures, errors or skips. Tests cover rollback, retry exhaustion, concurrent observers, stale identity maps, scope denial, retired submission replay and lost-delete acknowledgement. Each native probe verifies stale-fence denial, wrong-UID HTTP 409, exact deletion, 404 replay, neighbor preservation and isolated namespace cleanup. No pods started. See [dataset expiry evidence](evidence/phase-1/dataset-expiry-2026-10-15.yaml).

This does not qualify deployed worker death/restart, current immutable images, TLS/egress or token rotation. Inconsistent legacy profile attempts remain an inventory gap. Non-Ray analysis, provider credential restrictions, separate child grants and complete recovery remain open. All 36 Phase 1 requirements remain Open. The 45 GB migration rehearsal lacks disk space; seven historical QA objects remain missing and the original preservation gate remains abandoned.


## Phase 1 non-Ray analysis admission and atomic publication — 2026-10-15

Validation, explainability and drift launches now persist a ModelRun-bound attempt, analysis logical key and project service account. Batch manifests project a short-lived attempt token and the HTTPS control configuration; production/staging cannot deliver legacy no-identity submissions. Submission locks the exact attempt before the run, validates current scope/fence/pointer/namespace, ensures its account and records submission. Retired delivery replay cannot create another Job.

The existing worker now applies admission and owned proof-capped leases to analysis. Analysis guards status/report/failure writes with current fencing, uses content- and attempt-specific report keys, rechecks expiry after upload and commits report metadata, metrics, receipt and terminal CAS atomically. Failure commits attempt/run together with redaction. Shared completion recognizes an already committed analysis report without incrementing CAS again; a late worker error preserves user cancellation.

Final verification passed **747 regression tests**, including **36 focused analysis cases**, plus **three local-worker/k3d identity probes**, without failures, errors or skips. Cases include actual launch/manifest/submission/main paths, stale/expired/cancelled writers, upload-time expiry, CAS rollback, lost commit acknowledgement and failure redaction. The native probes use actual pod-bound tokens and TokenReview, current local worker.main and PostgreSQL/embedded report publication; computation is mocked and HTTP uses an in-process relay. Each pod remained unscheduled and its token file and UID-owned namespace were removed. See [analysis admission evidence](evidence/phase-1/analysis-admission-2026-10-15.yaml).

Batch observer attempt reconciliation, lease-expiry cancellation/replacement and legacy manifest migration remain open. Native probes do not qualify deployed images, full analysis computations, worker death, TLS/egress, rotation or deployed API RBAC. Provider credentials remain broad; separate child/object grants remain required. All 36 Phase 1 requirements remain Open; 45 GB migration storage and seven missing historical QA objects/original abandoned preservation gate remain unresolved.


## Phase 1 managed analysis Job recovery — 2026-10-15

Analysis submission now records physical namespace/name/UID and the original outbox/manifest digest in an attempt event. Lost creation acknowledgement can adopt only a Job whose scope labels match. Replacement binds a deep copy of that verified manifest to a fresh attempt, fence, Job name and projected-token audience. The original source remains unchanged.

The managed observer locks attempt before run and refreshes state. Lease expiry, missing/terminated Jobs without report CAS and an unclaimed startup lease now queue exact old-Job cleanup plus bounded replacement in one transaction. Exhausted budgets still queue cleanup. Committed report CAS, cancellation and changed active pointers are preserved. Cleanup checks scope/fence/evidence and physical labels/UID before UID-preconditioned deletion; missing-Job replay leaves terminal CAS unchanged. Legacy observation excludes managed rows.

Final verification passed **904 regression tests**, including **68 focused recovery cases**, and **three k3d control-plane probes**, without failures, errors or skips. Cases include concurrent observers, stale cached leases, rollback, retry exhaustion, changed source evidence, lost-delete acknowledgement and an actual replacement submit/claim/report commit. Native suspended Jobs verify creation replay, foreign-adoption denial, wrong-UID 409, exact old deletion, 404 replay, replacement submission and denial after same-name UID substitution. Neighboring Job UIDs stayed unchanged; no pods started; all owned namespaces were removed. See [analysis recovery evidence](evidence/phase-1/analysis-recovery-2026-10-15.yaml).

The first broad run exposed a race in the earlier heartbeat test: live-thread enumeration could miss an already exited heartbeat and raise StopIteration. The test now captures its thread and synchronizes via an event. The full rerun includes that fix, with no test exclusions or product-code change to hide the failure.

Legacy manifests and submitted rows without the new submission evidence still need migration/replay. Native checks do not qualify deployed worker death, full computation, immutable images, TLS/egress, rotation or deployed RBAC. Provider/database credentials remain broad; separate child/object grants remain required. All 36 Phase 1 requirements remain Open. The 45 GB rehearsal lacks disk space; seven historical QA objects remain missing and the original preservation gate remains abandoned.


## Phase 1 first-delivery legacy analysis handoff — 2026-10-15

Production/staging can now upgrade a first-claimed, never-operator-replayed CPU analysis delivery that is still pending with no active attempt or report evidence. The legacy manifest must match the recorded project/run/namespace/name, and the old Job and all run-labeled Pods must be absent. Retried, running, submitted, moved, GPU or ambiguous scopes stay blocked for controlled replay.

The new attempt, rebound manifest and migration digests commit through deferred delivery before Job creation. The manifest selects the configured current CPU image/pull configuration and declares `--attempt-protocol analysis-v1`. Current workers reject missing/unknown protocol context before admission or status writes; a read-only inspection of the known deployed predecessor parser confirms it rejects the new argument. This is a compatibility guard, not image attestation or full binary qualification.

Read-only preflight outages defer without spending the first delivery and preserve a redacted error. The first broad run caught diagnostics being lost during a row refresh; both paths now flush before deferral. Final verification passed **961 regression tests**, including **57 focused migration cases**, plus **three k3d handoff probes**, with zero failures, errors or skips. Native probes prove old Job/Pod denial, durable deferral and one tracked suspended Job; all owned namespaces were removed. See [legacy handoff evidence](evidence/phase-1/analysis-migration-2026-10-15.yaml).

This does not migrate already running/retried/ambiguous legacy work or qualify claim loss before handoff commit, old-controller drain, missing historical submission evidence, previous-release readers, deployed process death, full model computation, TLS/egress, rotation or current images. Provider credentials and separate child/object grants remain open. All 36 requirements remain Open; the 45 GB storage gap and seven missing historical QA objects/original abandoned preservation gate remain unresolved.


### 2026-10-15 — Phase 1 structural schema requirement verified

Closed P1-W12 against its complete structural scope. The current PostgreSQL 16.15 schema has 41 application tables plus Alembic metadata, 608 columns, 191 modeled constraints (83 foreign keys), 41 explicit indexes and seven BIGINT byte-count columns. A negative control exposed case-folding of quoted collation names hiding a change in text equality. Shared normalization now preserves quoted case, whitespace and escaped quotes for both types and default casts; valid quoted enum defaults are accepted. All 93 schema/migration regressions pass with zero failures/errors/skips.

All ten checked-in prior schemas migrate in isolated databases to head 0011, preserving every seeded legacy field and passing full schema and Alembic model comparisons. Upload-capable starts each use 10,000 metadata rows, including 10 GiB byte-size values; all owned rehearsal databases were removed. This does not qualify previous-release readers, a 45 GB database or a deployed image. W11, W25 and G05 remain Open. The ledger now records one verified requirement and 35 open; Phase 1 remains in progress. Evidence: `evidence/phase-1/schema-closure-2026-10-15.yaml`. No k3d rollout or QA storage mutation occurred; seven historical QA objects remain missing.


### 2026-10-15 — Public contract validation and revision races

Fixed malformed cursor handling, concurrent revision allocation and ignored estimator overrides. Cursor decoding now bounds and validates base64/JSON/string/UUID structure; actual candidate/trial/event HTTP routes return `422 invalid_cursor` before collection queries. Contract mutations lock their project before inserting command foreign keys, preventing revision races and lock upgrades. Eight controlled two-connection PostgreSQL races cover feature contracts, search spaces, search objectives and experiment specs: explicit expected versions yield one success/one structured conflict; implicit versions remain consecutive. Replays retain the original revision and failed requests leave no committed command.

Training launch now rejects unknown/duplicate selected estimator names and unsupported resource policies before execution intent. Valid disable overrides retain the selected candidates, whose resource class comes from the server catalog. The negative run reproduced 18 failures in 20 cases; final checks pass 284 cases with zero failures/errors/skips, including 28 overlapping focused cases. The first broad invocation skipped 21 authorization cases due to a missing dedicated database setting; the gate rejected it and the corrected invocation ran those cases. Evidence: `evidence/phase-1/contract-boundaries-2026-10-15.yaml`.

W13/W14 remain Open: immutable catalog ownership, legacy selected-mode/spec compatibility, complete candidate details, typed feature/search specifications and discriminated estimator parameter validation are still required. Phase 1 remains one verified requirement/35 open. No deployment, Ray execution or 45 GB qualification is implied; no k3d/QA storage changes occurred.


### 2026-10-15 — Catalog identity from publication to training startup

Preparation now publishes task-scoped estimator metadata and a runtime fingerprint covering estimator classes/defaults/search dimensions, catalog source, Python and installed package versions. Catalog APIs return persisted rows and revision UUIDs; they no longer attach another task's revision to fresh runtime discovery. Reads/admission detect content/runtime drift, and admission rejects stale content relative to the active task catalog. Equal-content republished IDs retain equivalent catalog semantics. Signatures remain unverified; sampling and serialization qualifications are not fabricated.

Launch persists content/runtime digests. Training startup verifies the stored project/catalog/task/selection before data reads or MLflow activity. Selected-mode estimates count actual default candidates and reject duplicates. Ray training commands carry `--attempt-protocol training-catalog-v1`; workers reject mismatched/missing fenced protocols before admission/status writes, preserving `analysis-v1` for analysis. The known predecessor parser rejects the new command in a read-only QA Pod probe. Legacy catalogs are not rewritten; refreshing them requires a new published revision and compatible worker image.

Final verification: 650 regressions and 37 overlapping focused cases, zero failures/errors/skips; real PostgreSQL project/task/stale-revision tests, independent-process identities, actual startup boundary checks and current local Ray/preparation/recovery checks. Evidence: `evidence/phase-1/catalog-binding-2026-10-15.yaml`. Full selected-mode immutable spec compatibility, candidate-detail histories, signed CI/native image/platform qualification and other Phase 1 requirements remain open. Ledger remains one verified/35 open. No image deployment, k3d training, real GPU or 45 GB rehearsal is implied; QA data and storage were not changed.


### 2026-10-15 — Candidate detail and durable status

Candidate detail now returns an explicit response with recorded lineage, parent/trial attempts, Ray identities, run resources, sample tiers, scoped sealed split digests, checkpoint digests, finite metrics, model SHA256 and terminal reasons. Trial receipts require consistent typed parent/trial lineage, parameter/result digests and terminal CAS; missing or inconsistent proof is labelled and its result withheld. Another candidate/project/run or extension-run summary cannot supply its metrics. Ambiguous legacy duplicates do not acquire a result. The read path does not repair or backfill data.

Current fenced phase/partial/final training transactions now synchronize durable candidate rows with run evidence, preserve terminal outcomes on old nonterminal hints and reject contradictory outcomes. Stale writers leave candidate/run/metric evidence unchanged. Shared redaction now covers nested arrays, quoted/JSON/Bearer/Basic credentials and provider/private keys; candidate collections also redact metadata.

Verification: 508 regressions and 37 overlapping focused cases, zero failures/errors/skips, using actual PostgreSQL producers/writers and read-only HTTP routes with real project-role checks. Query counts remain constant from one to 21 trials; 100 unrelated progress events are filtered before returning two relevant event rows. Provider-key negative controls caught a text/map mismatch that is corrected and reverified. Evidence: `evidence/phase-1/candidate-detail-2026-10-15.yaml`.

W13/W18 and the phase remain open: complete legacy selected-mode immutable specs, historical status/cancellation/deletion migration, full state-machine inventory, signed/runtime/native qualification and other readiness requirements remain. Ledger stays one Verified/35 Open. No current-image, GPU, production-scale, k3d/QA storage or object-byte qualification is implied.


### 2026-10-15 — Selected-model immutable identity and callers

Current prepared legacy selected estimates and launches now resolve one durable experiment and objective from a coherent project-scoped graph. Explicit spec fields remain assertions; changed roles, graph bindings and incompatible hints fail before capacity or execution intent. Named specs separate validated identity from opaque metadata. Estimates return usable bindings; launches persist experiment/objective/roles, the UI sends the estimated spec, and restart/add-model preserve recorded lineage.

Reservation retries return their original response before mutable preflight. The normalized digest now permits estimate reservations to be consumed by launch. Internal analysis capacity checks are separate, allowing validation/explanation/drift inputs without inventing a training experiment or requiring preparation.

The broader run caught catalog fingerprints changing with Python search paths and Ray vendored imports. The fingerprint now uses unique distribution/version pairs in the interpreter installation, with an explicit scope. Independent-process checks cover duplicate paths and Ray import/reload. Existing catalogs require fresh publication with a compatible runtime; no historical rows are rewritten.

859 Python regressions, 40 overlapping focused cases and 37 UI workflow tests pass without failures or skips; TypeScript build and scoped Ruff checks pass. Real PostgreSQL races and HTTP estimate/launch/reservation/replay checks run against stubbed Kubernetes capacity/object existence; no current-image/Ray/provider qualification is implied. Evidence: `evidence/phase-1/selected-identity-2026-10-15.yaml`.

W13/W14 and Phase 1 remain open: historical specs, request hashes and reservation protocols, old binaries, typed feature/search specifications, signed/native runtime variants, object grants and the full failure inventory still require qualification. Ledger stays one Verified/35 Open. The confirmed 45 GB migration target was not run: available disk cannot hold it while preserving the cluster reserve and migration overhead. No QA/storage rollout occurred; seven historical objects remain missing.


### 2026-10-15 — Typed feature/search policies and worker feature view

Feature, feature-search and search-objective creation now accept strict nested specifications instead of generic execution dictionaries. Source snapshots, exact columns, sealed splits, prepared artifacts, project-scoped recipe references, roles, metric direction and safe aggregation are validated. Authoritative content uses explicit v2 digest scopes; known preparation scopes, registry and compiled recipe content are verified before admission. Preparation emits the same typed metadata and search-to-recipe UUID, with a 500-input raw-view bound. Named revisions remain usable across all four tasks.

The worker now checks immutable experiment/data/policy/revision bindings before data and MLflow activity, and projects the approved raw feature view into train/validation readers before Pandas materialization. Invalid formula metadata, entity reinterpretation, over-budget output and nonraw compiled recipes fail closed. Training now declares `training-features-v1`; old `training-catalog-v1` is rejected before admission, while analysis keeps its own protocol.

930 related Python regressions and 71 overlapping focused cases pass without failures/errors/skips; scoped Ruff checks pass. Real PostgreSQL/HTTP mutation, conflict, replay, rollback, source isolation and fenced startup checks pass. A standard-library source/parser probe in the known QA predecessor rejects the new protocol while its run-ID control remains valid and its Pod UID is unchanged. This is not a current-image workload qualification. Evidence: `evidence/phase-1/typed-contracts-2026-10-15.yaml`.

W14 and Phase 1 remain Open: discriminated estimator parameter overrides, historical generic specs and request protocols, compatible deployed images and broader qualification still need work. Eligibility metadata does not qualify engineered feature generation, stateful availability rules, joint search, value-dependent engineering budgets or serving parity. No old records were rewritten, no QA/storage rollout occurred and no 45 GB rehearsal was run. Ledger remains one Verified/35 Open.


### 2026-10-15 — Estimator parameter policies through actual execution

Closed estimator-discriminated schemas now accept only reviewed parameters and reject unsafe execution/resource settings. Admission validates task, selection, types, ranges and known class/fold conditions. Versioned normalized policies bind reservations, immutable launch commands, runs, candidates and trial specifications. Modified or erased policy is rejected; restart/add-model and existing empty-policy replay remain compatible.

Fixed values are applied to cloned estimators before search and server resource configuration, removed from automatic search dimensions, preserved through final fitting and saved best parameters, and checked before durable trial enrollment. Every fit copies mutable search dimensions so BayesSearchCV cannot change cached catalog fingerprints. Transductive clustering counts must also fit the smallest held-out CV fold before capacity admission. Actual small Ridge/KMeans fits and real local Tune workers pass. GPU selection/fallback tests use CPU computation with simulated accelerator selection. Raw nonfinite JSON now returns 422. Parent and child workers require `training-parameters-v1`; analysis remains unchanged.

1092 Python regressions and 63 overlapping focused cases pass without failures/errors/skips; scoped Ruff and formatting checks pass. The known predecessor parser rejects the new protocol with its run-ID control passing and unchanged Pod UID. Evidence: `evidence/phase-1/estimator-overrides-2026-10-15.yaml`.

W14 and Phase 1 remain Open. Historical specification/protocol migration, compatible deployed images, real GPU and production Train, restricted object grants and complete failure qualification remain required. No QA/storage rollout or 45 GB rehearsal occurred. Ledger remains one Verified/35 Open; seven historical QA objects and the original abandoned preservation gate remain unresolved.


### 2026-10-15 — API/CPU runtime pairing (in progress)

The worker dependency lock omitted shared API-module imports, and the previous full-inventory identity incorrectly required the API and CPU images to contain identical package sets. The CPU image now installs both existing hashed locks, without upgrading their shared versions. Reviewed API/CPU profiles retain complete inventory validation and bind both full profiles and lock hashes to common catalog identity. Unknown/missing/extra/changed packages and missing production profiles fail closed, including normalized governed environment names.

A reusable actual-image validator and pre-publication CI check are implemented. Initial broad verification passed 1,129 checks; 39 profile/release checks pass after the environment normalization fix. The final broad recheck and guarded Docker build remain active. The first build captured earlier source; inspect it to terminal and intentionally rebuild affected images before native qualification. No current-image execution or whole-requirement completion is claimed. Evidence is explicitly in progress: `evidence/phase-1/runtime-pair-2026-10-15.yaml`.

Phase 1 remains one Verified/35 Open. Historical compatibility, signed/native variants, production Train, delegated grants, real GPU and full failure/scale qualification remain required. No QA data/storage rollout occurred. The 45 GB storage gap and seven historical missing QA objects/original abandoned preservation gate remain unresolved.


### 2026-10-16 — API/CPU runtime pair verified on immutable k3d images

The normal CPU image now includes its required shared API dependencies. Reviewed profiles validate every installed package/version, Python/pip, both lock hashes and complete application Python source; API206 and CPU212 package inventories remain distinct. Canonical environment settings protect existing production fence/lease checks. Different code or dependencies cannot silently consume the same catalog. The actual-image validator runs before release version tags in CI.

1154 related regressions, 35 overlapping profile cases and 10 overlapping release-validator cases pass without failures/errors/skips. Both normal Dockerfiles built current immutable images, then passed actual offline image validation and isolated k3d API/CPU Jobs. All four task catalogs match; one small Ridge pipeline preserves fixed alpha through real preprocessing/BayesSearch/fit, catalog stability and pickle roundtrip. Incompatible catalog and unsafe parameter controls fail before fitting. Actual Pod image IDs match requested digests, exact namespace cleanup reaches404 and existing QA Pod/PVC identities are unchanged. Disk reserve remained intact.

The earlier deadline/source-guard failures, harness log-decoding correction and zero-byte cleanup attempts are retained. Only exact test-owned cache entries were reclaimed, and images were reverified afterward. Evidence: `evidence/phase-1/runtime-pair-2026-10-15.yaml`. This does not qualify full fenced HTTP/DB/MLflow/provider workflows, production Train, real GPUs, signed production variants or complete recovery. Historical compatibility, the45GB migration/storage gap and seven original missing QA objects remain unresolved. Phase1 remains one Verified/35 Open.


### 2026-10-16 — Upload digest interpretation fails closed

Upload completion and reconciliation now require explicit SHA-256 byte-stream metadata and a canonical digest before provider access or verified publication. The legacy size-only fallback is removed. Unsupported or missing metadata blocks work through the existing contract error without rewriting historical hashes, changing object references or scheduling quarantine deletion.

217 related checks pass without failures/errors/skips, including real PostgreSQL legacy rejection and valid heartbeat/publication fencing. Scoped Ruff lint/format and diff checks pass; coverage.json is unchanged. Evidence: `evidence/phase-1/upload-digest-boundary-2026-10-16.yaml`.

This increment has local Python/PostgreSQL verification; the earlier immutable API/CPU runtime proof covers its earlier source snapshot. No new deployment or native upload qualification is claimed. The full legacy inventory and reviewed migration remain required, and blocked legacy work may exhaust the existing retry budget. P1-W16 and Phase 1 remain Open: one Verified/35 Open. The 45 GB migration requires suitable disk capacity; seven original QA objects and the original abandoned preservation gate remain unresolved.


### 2026-10-16 — Digest provenance inventory and direct byte writer

The direct byte upload writer now explicitly records its SHA-256, byte-stream scope, verification status and UTC time. A new metadata-only inventory detects legacy upload provenance even when historical ingestion defaults labeled a linked version as a verified byte digest. It streams every upload/version once in a PostgreSQL read-only repeatable-read snapshot with bounded batches, private non-overwriting output and a complete counted/hash-bound summary. See `phase-1-digest-migration.md`.

182 related checks pass without failures/errors/skips, including real PostgreSQL legacy links, snapshot isolation, rejected writes, bounded result settings and unchanged stored hashes. Scoped Ruff and diff checks pass; coverage.json is unchanged. The actual existing k3d QA database inventory completed:13 versions require verification review, with0 upload sessions. QA Pod/PVC identities and phases match before/after. No object contents were read and no data was migrated. Evidence: `evidence/phase-1/digest-provenance-2026-10-16.yaml`.

These13 versions are not proven corrupt or repaired. Byte rescans, controlled migration, reader/previous-release qualification and the45GB rehearsal remain required. This latest source increment has local Python/PostgreSQL checks; the native action qualified only read-only metadata inventory. P1-W16 and Phase1 remain Open with1Verified/35Open. Seven historical QA objects and the original abandoned preservation gate remain unresolved.


### 2026-10-16 — QA dataset byte verification and guarded metadata repair

The read-only rescan matched all13 actual dataset hashes over280,788 bytes. All13 persisted byte sizes were missing. The first attempt stopped at its pre-read size guard; the resumed probe admitted provider HEAD sizes under the same16MiB object/64MiB total caps,64KiB reads and 40s deadline. No object changed. Observation evidence: `evidence/phase-1/qa-digest-rescan-2026-10-16.yaml`.

A frozen plan then passed five real PostgreSQL controls: changed digest, wrong project, stale row, conflicting evidence and full valid-repair rollback. The original rows and proof set survived every rollback unchanged. One guarded transaction committed13 digest-verification records and observed sizes/verification times. Replaying repaired0/replayed13. The follow-up read-only inventory measured13 versions/0 requiring verification review and0 upload sessions. Content hashes/scopes and all unrelated row fields are preserved; QA Pod/PVC identities are unchanged. No object writes/deletes, shared rollout or migration occurred. Repair evidence: `evidence/phase-1/qa-digest-metadata-repair-2026-10-16.yaml`.

P1-W16 remains Open: this exactly13-row QA repair does not qualify production legacy-manifest migration, consuming readers, previous-release binaries or45GB scale. The entire Phase1 ledger stays1Verified/35Open. Seven original historical QA objects and the original abandoned preservation gate remain unresolved. coverage.json is unchanged.


### 2026-10-16 — Verified raw digest consumption across readers

Training/analysis/profiling/feature admission and relevant workers now require explicit verified byte-digest provenance, aware verification times and known sizes. Full-memory raw readers compare actual bytes before parsing; integrity errors propagate instead of returning empty-profile warnings. The splitter counts actual stream bytes. Drift checks both source versions; offline inference rechecks ready-upload hash/scanner evidence before storage/upstream access. API digest failures return409. Inventory v2 reports the same metadata prerequisites.

820 related regressions and101 overlapping focused cases pass without failures/errors/skips, including real PostgreSQL admission, replay, query/snapshot controls and local Ray preparation. Explicit valid fixtures preserve original test intentions; the expanded gateway test now correctly asserts the existing legacy/canonical deployment enum spellings. Scoped Ruff lint/format and diff checks pass; coverage.json is unchanged. The exact current helper accepts13 actual QA records/rejects13 unverified copies via read-only execution; v2 inventory reports13 versions/0 requiring review/0 uploads. QA Pod/PVC identities are unchanged. Evidence: `evidence/phase-1/digest-readers-2026-10-16.yaml`.

This is current-source regression/helper qualification, not newly built/deployed HTTP workflows. Production legacy migration, old releases and distributed provider/version/grant bindings remain required. Prior immutable image qualification predates these source changes. The45GB rehearsal/storage gap, seven original missing QA objects and original abandoned preservation gate remain unresolved. P1-W16 and the whole Phase1 ledger stay Open with1Verified/35Open.


### 2026-10-16 — Committed leases for read-only MLflow recovery

Background tracking recovery now commits a unique owner,90second lease,heartbeat and retry budget before MLflow lookups, releasing its caller connection and row locks. Independent short heartbeats renew ownership; publication requires the same live claim and unchanged typed payload/lineage. Recovery stays read-only and parent publication clears superseded leases. Malformed intents are bounded without starving healthy work, creation flags use exact JSON booleans and unexpected parent tags are rejected. Heartbeats stop before publication so the worker cannot skip its own lock.

297 related regressions and52 overlapping focused cases pass without failures/errors/skips. Actual PostgreSQL verifies released lookup connections/locks, exclusive leases, stale publication rejection and heartbeat concurrency. A real Python claimant is killed after committing; zero/exhausted/remaining-budget cases recover correctly without duplicate creation. Local real HTTP fault and existing replay/lineage cases pass. Scoped Ruff and diff checks pass; coverage.json is unchanged. Evidence: `evidence/phase-1/tracking-leases-2026-10-16.yaml`.

Inline tracking creation/replay still holds database locks across I/O and is the next concrete pool/lease gap. This local recovery proof does not qualify a current image, Kubernetes process loss, production MLflow/PgBouncer failover, all worker workflows or production Train/grants. The entire Phase1 ledger stays1Verified/35Open; the45GB storage gap, seven historical missing objects and original abandoned preservation gate remain unresolved.


### 2026-10-16 — Inline MLflow writer releases database connections across I/O

The inline writer now commits a short, exclusive renewable command lease before experiment lookup/creation, run lookup/creation and cached-run validation. It commits the irreversible creation marker before POST, closes each transaction before remote I/O, and reacquires the current parent/command to check live ownership and unchanged typed scope before publication. Parent writers can supersede read-only recovery, while competing live writers are rejected. Heartbeats stop before publication and errors release only the still-owned claim. Invalid stored creation states and backend changes fail before remote calls; an uncertain run never recreates a missing experiment.

321 related regressions and 76 overlapping focused cases pass with zero failures/errors/skips. PostgreSQL checks show no caller connection or row lock at each remote operation, including fluent resume. Real writer processes die at lookup, immediately before POST and immediately after remote creation; recovery preserves intent, waits for lease expiry, and adopts visible results without duplicate creation. Tests also cover cancellation, changed/stolen/expired ownership, lease renewal and lost initial/marker/result commit acknowledgements. Scoped Ruff lint/format and diff checks pass; coverage.json is preserved. Evidence: `evidence/phase-1/tracking-writer-2026-10-16.yaml`.

A crash or lost database acknowledgement after committing the marker but before POST can remain unresolved without a remote object. This remains a required recovery gap; a blind retry is deliberately blocked. Current-image/Kubernetes process loss, remote service-side authorization, all worker pools/side effects, production Train and full failure qualification remain open. No QA data/object mutation, schema migration or rollout occurred. Phase 1 stays one Verified/35 Open. The user-confirmed 45 GB migration target still requires suitable storage; seven historical QA objects and the original abandoned preservation gate remain unresolved.


### 2026-10-16 — Fence refit and evaluator read-capability disclosure

Both internal read-grant endpoints now snapshot their authenticated attempt/plan and deadline in a short transaction, close it before object-store inspection/signing, then reload current state before returning the URL. Cancellation, supersession, changed fence/plan, expired lease or original URL deadline, and shortened deadlines reject disclosure. Heartbeat extensions and valid replay remain permitted. Revalidation expires ORM state first so a retained old event cannot hide a concurrent plan replacement. This uses the existing attempt and plan contracts; final-data one-shot issuance is unchanged.

117 related regressions/22 overlapping focused cases pass with zero failures/errors/skips. Real independent PostgreSQL sessions verify zero caller pool checkouts and acquire scope/attempt locks during minting, mutate ownership/plan/deadlines, and prove stale URLs are not returned. Existing HTTP controls, actual credential-free sklearn refit and evaluator workers, publication, authority handoff and one-shot credential tests pass. Scoped lint/format and diff checks pass; unrelated older refit formatting and coverage.json are preserved. Evidence: `evidence/phase-1/read-grants-2026-10-16.yaml`.

The first regression harness referenced a nonexistent test filename; after correcting it, an imported test module registered an S3 fixture globally and broke two unrelated Azure/GCS URI checks. Fixture imports now remain local to the new test module; all provider URI checks pass without product changes. An additional final review case rejects a URL whose original deadline elapsed despite heartbeat renewal.

This is local Python/PostgreSQL and local HTTPS-worker evidence, not a current-image/Kubernetes or real cloud signing qualification. A previously disclosed URL remains usable until its signed expiry; final SQL checks cannot revoke a capability already in flight. Full worker/provider/predecessor grants, production Train, central-authority deployment and failure inventory remain required. No QA data/object change, rollout, schema migration, commit or publication occurred. Phase 1 stays one Verified/35 Open. The 45 GB storage target and seven historical missing objects/original abandoned preservation gate remain unresolved.


### 2026-10-16 — Sign provider distributions and verify local references

The qualification authority now exposes allocator-only `POST /allocations/{id}/distributions`, using closed versioned contracts and its existing Ed25519 receipt store. It validates one provider-matching, versioned train/validation copy per provider; schema and aggregate logical row counts/XOR digests match across copies while physical layouts may differ. Registered final data comes only from the trusted authority manifest registry. Known final URIs/byte digests cannot be declared safe. One transaction locks the allocation and signs all three manifests. Canonical payloads contain final locations; non-canonical payloads contain safe roles and opaque final-role digests only. First issuance is rejected after authority refit/evaluation evidence or state changes; exact replay retains the original signatures. Partial histories and changed requests fail closed.

The library verifier and `scripts/validate_provider_distribution.py` require a trusted public key and explicit independent allocation, project, scope, provider/canonical-provider, fixture/split revision and split-digest references. They reject altered signatures, algorithm confusion, signed inconsistent envelopes, unknown fields, coercions and removed defaults. CLI input is bounded to32MiB; success/failure output excludes object locations/credentials. The authority operator README documents issuance, delivery and verification, and corrects its obsolete early description of one-shot credentials as unimplemented.

121 related/54 overlapping focused tests pass without failures/errors/skips. Actual PostgreSQL covers six concurrent identical/conflicting requests and backend termination between receipt inserts: all receipts roll back and exact recovery signs one set. A complete metadata lifecycle proceeds through distribution, refit/evaluator registration, one credential mint, one committed result and unchanged distribution replay. Signature and reference negative controls, no-final-location checks, known-final overlap, missing/late/partial history and actual CLI subprocess output checks pass. Scoped Ruff lint/format/diff checks pass; coverage.json is preserved. Evidence: `evidence/phase-1/provider-distribution-2026-10-16.yaml`.

This verifies signed declarations and local reference validation; it does not copy/read provider data or establish IAM isolation. Automatic dataset-import/admission enforcement, unique fixture/split revision-pair allocation, trusted cross-provider distribution/deployment, production Train/grants and full failure/restore qualification remain required. Existing allocation/evaluator protocols remain compatible and do not become mandatory distribution consumers in this increment. No QA mutation, schema migration, rollout, commit or public publication occurred. Phase1 stays one Verified/35 Open;45GB storage and the original seven missing QA objects/abandoned preservation gate remain unresolved.


### 2026-10-16 — Bind central allocations to unique fixture/split revisions

Application Alembic head0012 and dedicated authority SQL0002 add nullable fixture/split revision columns, a globally unique pair and a paired/canonical-identifier check. Historical rows remain NULL without relabelling. Allocation lookup/race recovery includes the pair and refreshes cached ORM state; exact replay binds all prior scope/project/provider/digest fields plus both references. Requests with incomplete/invalid references fail at API/service/database boundaries. Distribution must match an already bound pair; replay cannot upgrade an old unbound allocation. Allocator inspection exposes both fields. Readiness now selects all columns of both authority tables so an unexpanded store returns503. Alembic downgrade refuses to discard bound evidence. The operator README documents expansion before binary rollout and retention during rollback.

185 related/23 overlapping focused checks pass with zero failures/errors/skips. Actual PostgreSQL tests cover six-caller competing/identical pair allocation, cross-project/digest/scope/provider conflicts, direct SQL constraint failures, stale cached revisions, non-relabelled legacy replay and guarded downgrade. Dedicated SQL0001→0002 preserves nonempty legacy rows/receipts, accepts old-column inserts/reads, rejects readiness before expansion and passes full two-table metadata comparison afterward. These are old SQL interfaces, not an actual previous-release binary qualification.

Full W12 requalification passes 93 schema/migration regressions and all11 prior-schema rehearsals to0012. The independent inventory records41 application tables,610 columns,193 constraints/83 foreign keys,41 indexes and7 BIGINT byte columns. Eight upload-capable starts preserve10000 rows each; seven authority-capable starts preserve nonempty synthetic allocations/receipts and leave new references NULL. Every owned database is removed and Alembic drift is absent. The dedicated test database expansion initially had zero authority rows; nonempty preservation proof comes from these fixtures and isolated SQL expansion. No QA database was migrated.

Initial checks caught PostgreSQL regex cast spelling differences in the schema comparator, an outdated seed-count expectation and a missing test import. Model/migration declarations now use the exact native PostgreSQL cast form; the verifier was not weakened. Rehearsal seeding now covers authority rows and its expected counts were updated. Final frozen source passes scoped lint/format/diff checks and coverage.json remains unchanged. Evidence: `evidence/phase-1/allocation-revisions-2026-10-16.yaml`.

W12 remains Verified; the whole ledger remains one Verified/35 Open. Actual previous binaries, controlled legacy reference binding, automatic signed dataset import/admission and early-campaign registration, current-image/provider IAM/bytes and production Train/grants/failure/restore qualification remain required. Existing unbound allocation protocols remain compatible and are not promoted into release qualification. The45GB storage target, seven historical missing QA objects and original abandoned preservation gate remain unresolved. No shared rollout, object mutation, commit or public publication occurred.


### 2026-10-16 — Bind allocator credentials to persisted local projects

Consumer-path tracing found that a policy's authority project reference selected credentials directly from a controller-wide token map. The controller now takes the local project UUID from the persisted scope row and selects only that UUID's protected record. Its reference must match the policy before TLS or HTTP transport creation. The record is exactly `project_reference` plus `token`; malformed/missing records, legacy reference-to-token maps and invalid header tokens fail closed with a generic error. No fallback can select another project's credential. Explicitly configured local projects may share an authority reference while retaining their own credentials.

All controller factories and maintained native evaluator probe callers pass both project UUID and reference. The authority/Helm operator docs and values comment describe the new Secret format and suspend/update/deploy/resume sequence. Bindings must be approved independently of user-controlled policies; existing token material need not change. No live Secret was converted or exposed, and no QA rollout occurred.

118 related/18 overlapping focused tests pass without failures/errors/skips. Negative controls prove foreign references/projects, legacy/invalid records and malformed tokens cannot construct a TLS context or client. Actual PostgreSQL selection sends each of two persisted project IDs through the real credential selector even when references are identical, with no checked-out caller connection during dispatch. Downstream reconciliation is stubbed in that selector test; existing worker/authority recovery, abort, key custody, one-shot credentials and Helm isolation tests cover their existing full local paths. The caller audit parses repository Python and checks all7 direct selector call sites. Scoped lint/format/diff and coverage preservation pass. Evidence: `evidence/phase-1/authority-project-binding-2026-10-16.yaml`.

This fixes the tenant credential boundary before automatic dataset-import and early-campaign registration integration, which remain unfinished. Current images/Secrets still need coordinated qualification; old token-file formats intentionally stop reconciliation under the new source. No schema change occurred: head0012 and the prior full W12 structural proof remain unchanged. Phase1 remains one Verified/35 Open, with production Train/grants/full failure qualification,45GB capacity and seven historical missing QA objects/original abandoned preservation gate unresolved.


### 2026-10-16 — Read immutable provider versions without eager Azure buffering

The existing object-store `open_stream` protocol now accepts a keyword-only immutable version. S3 sends VersionId, GCS binds a canonical positive generation and Azure constructs a version-specific BlobClient. Full and ranged reads use the same pinned identity. Invalid IDs, unsupported embedded versions, missing versions and denied reads do not fall back to current bytes. S3/Azure reject a mismatched or missing returned version. Existing callers that omit the keyword retain current-object behavior.

Azure now exposes the native downloader's incremental read through a small RawIOBase reader, supporting readinto, buffering, close and error cleanup. It no longer calls readall before returning the stream. A caller asking for all remaining bytes still receives that behavior, and SDK initial/chunk buffers remain. GCS ranges retain their existing in-memory range behavior. These semantics and remaining reader/grant integration are documented in phase-1-digest-migration.md.

273 related/46 overlapping focused cases pass with zero failures/errors/skips. Actual installed SDK clients/signatures and S3 botocore response contracts cover full/ranged pins, malformed versions, missing/denied versions, response identity mismatch, stream lifetime and Azure bounded reads/readinto/BufferedReader/error cleanup. The initial test harness incorrectly treated boto3 clients as context managers; contextlib.closing fixed that harness. The Azure upload fixture now implements the actual SDK read method as well as readall; product compatibility checks pass.

A bounded native probe ran the exact current contracts/S3 helper in a separate Python process inside the unchanged QA API Pod, using existing verified TLS to the local S3-compatible service. It wrote51bytes across3owned versions, read historical full/range bytes after replacement, rejected a missing version while current reads remained healthy, preserved its owned neighbor and removed only fixture versions. Metadata fingerprints for all16pre-existing version records match before/after; qa-refit and qa-phase1 Pod/PVC identity/phase snapshots match. This is helper-source qualification, not a rebuilt/deployed image or real AWS/GCS/Azure qualification. Evidence: `evidence/phase-1/versioned-streams-2026-10-16.yaml`.

Scoped Ruff lint/format/diff and coverage preservation pass. No schema change, existing-object mutation, stateful restart, rollout, commit or public publication occurred. W16 and Phase1 stay Open: one Verified/35 Open. Automatic signed imports, explicit versions through all raw/prepared/distributed readers and attempt grants, production Train/full native failures and old-reader/production legacy migration remain required. The45GB storage gap and seven historical QA objects/original abandoned preservation gate remain unresolved.


### 2026-10-16 — Verify actual signed provider-copy data

The new provider-copy verifier reads only declared safe objects at exact immutable versions, checks whole-object bytes/SHA, then parses actual Parquet metadata and batches. It checks logical Arrow schema, row counts, canonical row IDs/nonnegative ordinals, recomputed content fingerprints, safe role labels and declared XOR digests. A capped SQLite index rejects repeated IDs across files and content crossing train/validation roles while allowing distinct-ID duplicates within one role. Hive partition role columns may be absent; when present they must match. Final inputs/labels are not read.

Explicit budgets bound one object plus index pages, decoded row groups, a working-filesystem free-space reserve and a cooperative deadline. Temporary/index work is removed on normal or exception exit. Provider RPC limits and outer process/worker memory/deadline enforcement remain independently required. The existing operator CLI preserves offline signature-only behavior and adds `--verify-objects --verification-budget`; it validates signatures/references/budgets before constructing storage and emits only safe aggregate proof fields. The operator README defines arrow-logical-schema-v1 and all limits, including no old-digest relabelling.

167 related/32 overlapping focused cases pass with zero failures/errors/skips. Actual Parquet controls reject byte/hash/size/schema/count/digest/identity/fingerprint/role errors, duplicates, malformed bytes and resource/deadline failures. Cleanup/progress abort and CLI pre-storage rejection/private-error checks pass. A bounded native TLS S3-compatible probe runs the exact current helper stack in a separate unchanged-QA-Pod Python process: signed copies verify by old versions after current replacement; a validly signed incorrect row digest is rejected. It verified4854bytes/4safe rows, wrote4892fixture bytes across4versions including replacement/neighbor controls, removed them all and preserved16pre-existing version records plus both namespace Pod/PVC identities. Host free disk stayed above43GB. This is helper/CLI qualification, not a current image or production workload. Evidence: `evidence/phase-1/provider-bytes-2026-10-16.yaml`.

Scoped lint/format/diff and coverage preservation pass. No schema, live configuration/Secret, existing object, stateful restart, rollout, commit or public publication changed. Actual dataset import registration, protected source/mode provenance, admission enforcement, early central registration and distributed grants/worker qualification remain unfinished; the copy proof alone does not admit data or authorize final-test access. The wholePhase1 remains one Verified/35 Open, with45GB migration/capacity, seven historical QA objects and original abandoned preservation gate unresolved.


### 2026-10-16 — Register authority allocation before promotional training

Promotional scope creation now records a closed, server-owned `scope.authority.allocate` command in the same transaction. It binds the local project, scope, split and experiment revisions, evaluation policy digest and central allocation request. The controller uses the protected project credential binding, commits a bounded lease/retry claim, performs authority HTTP outside SQL, and rechecks current scope/policy/ownership before publishing success. Lost acknowledgements replay the same central allocation. Invalid controller configuration backs off instead of occupying every batch.

Training launch and member admission report a normal 409 while registration is unavailable. Sealing checks completed registration again before releasing members. Evaluator handoff uses the recorded allocation ID and no longer creates an allocation after refit. Stopped pre-training scopes cancel never-sent commands locally, or recover the original allocation and verify/store its signed abort receipt; lost abort acknowledgements replay without opening final data. Delivery/cleanup have a combined bounded attempt budget. Scope queries now refresh cached state under the row lock, preventing a previously loaded OPEN scope from bypassing another session's committed cancellation.

The synthetic evaluator seed leaves an open scope with pending registration and no refit attempt; its runner waits for registration before planning refit. Synthetic training remains fixture setup. New API/controller code must deploy together with independently approved project bindings. Existing scopes without early evidence fail closed; post-refit receipts must not be rewritten into proof of early registration.

Verification: 39 focused and 393 related tests passed with zero failures, errors or skips (counts overlap), including real committed PostgreSQL connection/lock release during HTTP, expired predecessor reclaim, stale cancellation, lost creation/abort acknowledgements, bounded retry/backoff, current-policy/owner checks, API conflicts, actual authority and synthetic seed setup. All 16 changed Python files pass lint; 14 pass whole-file format checks, with two inherited formatting differences left untouched. Eighteen scope inputs and 24 unchanged schema inputs were independently hashed; schema head remains 0012, and coverage.json is unchanged. See `evidence/phase-1/early-allocation-2026-10-16.yaml`.

This completes early allocation for the existing canonical scope flow, not the signed dataset import/distribution workflow. Trusted import provenance, revision-bound distributions, validation_only admission and safe versioned prepared-reader integration remain required. No current image or local k3d rollout was qualified. Phase 1 remains one Verified requirement and 35 Open. The 45 GB migration still lacks space for its fixture, reserve and overhead; largest-table row targets remain unspecified. Seven historical QA objects and the original abandoned preservation gate remain unresolved.


### 2026-10-16 — Signed provider import integration in progress

This remains one unfinished integration under `.unlazy/phase1-provider-import/GATES.md`: two gates are met and four are unmet. The new source-kind column distinguishes raw datasets from actual JSON manifest descriptors. A protected provenance table binds the local dataset version, durable command, signed authority allocation/project/provider/revision references, verification proof, prepared artifact and split. Database checks permit only non-canonical validation_only imports and require complete lineage before the record becomes verified. Raw byte readers reject manifest sources.

The operator library now records a closed durable intent, validates an independently approved policy and authority key, claims bounded retries/leases before constructing storage, verifies actual version-pinned Parquet copies and hashes the actual stored descriptor. It publishes descriptor/split/artifact/proof metadata in one fresh ownership transaction. Real committed PostgreSQL tests cover no held connections/available row locks during storage IO, lost claim/publication commit acknowledgements, lost descriptor acknowledgements, bad source/descriptor bytes, changed owners/leases, archived datasets/projects, deletion and tampered completion metadata. Failure cleanup preserves an externally archived dataset. Final commitments remain explicitly opaque; no final row identity or raw data is fabricated.

Verified checkpoint:107 schema tests and161 registration-related tests passed with zero failures/errors/skips (counts overlap;27 dedicated registration cases). All12 prior schema paths reach0013 with full structural parity and no Alembic drift. The inventory is42 application tables,632 columns,206 constraints/88 foreign keys,41 declared indexes and7 BIGINT byte fields. Nine applicable starts preserve10000 upload metadata rows each; eight preserve synthetic authority allocation/receipt fields. Eight changed Python files pass lint/format;43 source inputs and XML/artifact results were independently checked. coverage.json is unchanged. Only the dedicated test database was expanded; no QA/native rollout occurred.

Still required before this import path works: exact-version preparation/training readers and approved configuration lookup, source-aware feature contracts, validation_only restrictions across service/API/worker/replay, operator CLI/read schema exposure, profiling and deletion/cancellation integration, native access-denial/cleanup tests and final audit. The current raw-only readers intentionally reject imported descriptors; prepared role URIs remain unresolved provider-import references. No real imports should be qualified from this checkpoint. See `evidence/phase-1/provider-import-2026-10-16.yaml`; do not mark the integration complete or narrow its remaining gates.

Schema head is now0013 in the test environment; QA remains0011/old images. Phase1 remains one Verified requirement and35 Open.45GB qualification, actual previous binaries/current images/production grants and the original seven missing QA objects/abandoned preservation gate remain unresolved.


### 2026-10-17 — Signed import consumers and workload configuration verified locally

The signed provider-import integration now has **four met / two unmet / zero abandoned gates**. This is an unfinished integration, and the full Phase 1 ledger remains **one Verified / 35 Open**.

Preparation and training read the approved train/validation objects by exact immutable version, with byte, schema, row and decode-budget checks. Protected source provenance binds the approved local split and prepared artifact; configuration failures and forged raw/mode flags cannot supply a fallback. Actual persisted promotional membership is checked at admission, queue/replay, launch and worker boundaries. Imported profiling uses preparation directly and publishes usable feature contracts. Sampling uses the signed split/schema identity, so local provider IDs cannot change the selected rows.

The protected operator CLI loads independently approved public policy/key files. Standard Jobs and all Ray submitter/head/worker pods receive those files read-only. Review caught and fixed a missing Ray mount path and a clustering producer bug that serialized a null recipe target. The real Ray tests now validate the published compiled recipe for both supervised and clustering profiles. Pending imports block deletion until settled or cancelled; deletion removes owned descriptors while preserving external safe copies and rejects conflicting prefixes.

Fresh verification passes 107 schema tests, all 12 prior-schema migration paths to0013, 161 registration checks, 25 consumer tests and 663 connected regressions, without failures/errors/skips. There are 956 Python test executions and942 distinct test IDs because schema cases overlap. Lint, UI typechecking and all eight training-page tests pass separately. The checkpoint audit verifies367 frozen source inputs, unchanged coverage.json and cleanup of test import rows. The earlier broad run found one clustering failure; the producer fix passes71 focused feature-contract tests and the full affected gates were rerun. [Current evidence](evidence/phase-1/provider-import-2026-10-16.yaml) retains the exact scope and fingerprints.

Native G5 and final G6 remain open. Worker readers still use ambient object-store credentials; approved-object selection does not prove denial of raw/final access. Raw preview, synchronous raw profiling and raw validation reject imported descriptors; distributed profiling is the implemented import path. Lost descriptor-write acknowledgements can leave multiple physical versions or an owned-prefix orphan. No hard decoder RSS, blocked-SDK deadline, current-image, cloud IAM or complete retirement qualification is claimed.

The user reconfirmed a45 GB qualification ceiling, interpreted as45,000,000,000 bytes. Approximately47.8 GB host free space cannot cover that fixture plus the existing approximately37.47 GB cluster reserve and migration overhead; largest-table row targets remain unspecified. Only the dedicated test database is at0013; QA stays0011/old images. No QA Secret, Pod, PVC, object or schema mutation, deployment, commit or push occurred. Seven original QA objects remain missing and the original preservation gate remains abandoned; no replacement outputs or signatures were manufactured.


### 2026-10-18 — Admission releases SQL resources during Kubernetes verification

The admission IO fix passes all three scoped gates, including an independent gate rerun and source/result audit. This is a prerequisite for delegated attempt credentials; it does not close the full broker requirement or the provider import's native isolation gates. The whole Phase1 ledger remains **one Verified /35 Open**.

Admission now owns a short read transaction, freezes the workload/source binding, releases its SQL connection and locks, and performs TokenReview/Pod/Ray-controller calls. A fresh transaction locks the project before the attempt and checks active parent/source state and the unchanged binding before committing identity evidence. Changed fences, workload identities, namespaces, controller names and source identities fail closed. Project/run deletion intent and archived sources/projects deny admission; expiry is checked again after source validation so waiting for SQL locks cannot authorize an expired attempt. The HTTP route and maintained native harnesses use the transaction-owning factory interface.

Current verification includes45 committed database race/replay cases and358 related regressions, with no failures/errors/skips. Positive controls prove zero checked-out SQL connections and independently available Project/attempt/run/version row locks during each remote lookup. Concurrent same-token and rotated-token requests preserve unique evidence, and actual commit-then-lost-acknowledgement cases recover without duplicate rows. The analysis fixtures now store real small source bytes and matching verified SHA-256 metadata; the earlier three fixture failures were not resolved by weakening the digest guard.

Seven bounded native cases use real local K3d TokenReview and Pod-bound tokens: identity denial/replay, training startup/heartbeat/manual token replacement, validation/explainability/drift publication, and splitter/preparation admission/expiry barriers. All test Pods remained unscheduled; all seven owned namespaces are absent. Existing QA Pod and PVC identities match the recorded baseline. The native harness import-path collection failure was corrected before any resources were created. Across gate and native runs, the audit measures412 test executions and 410 distinct IDs; lint/diff checks pass and coverage.json is unchanged. [Evidence](evidence/phase-1/admission-io-2026-10-18.yaml) records exact source and result hashes.

These native tests execute current application code locally through ASGI, with manual token files and synthetic conformance inputs. They do not qualify deployed worker images, TLS/egress, automatic kubelet rotation, cloud IAM, absent raw/final storage capabilities or a complete AttemptCredentialBroker. The older import checkpoint remains bound to its recorded source snapshot. Model/migration inputs did not change; the dedicated test database stays0013 and QA stays0011/old images. No existing QA Pod/PVC/Secret/object or schema was modified, and no rollout, commit or push occurred.

The45 GB database qualification still lacks fixture-plus-reserve/overhead capacity, largest-table row targets remain unspecified, and previous-release binary qualification remains open. The original preservation gate remains abandoned and seven historical QA objects remain missing. No historical outputs or signatures were replaced.


### 2026-10-18 — Attempt object grants: structural gate verified; integration unfinished

The application expansion now reaches `0014_attempt_object_grants`. It records typed
project/attempt/stage/fence grants with exact object/version/method/byte bounds and
issuance, expiry and revocation evidence. No raw tokens or signed URLs are stored.
All seven stages have database constraints. Nonempty rollback and referenced
attempt deletion/fence mutation are deliberately blocked to preserve grant evidence.

Fresh structural verification passes 165 tests and all 13 prior-schema upgrades.
The inventory is 43 application tables, 657 columns, 219 constraints/89 foreign
keys, 42 indexes and eight BIGINT byte columns. Ten paths preserve 10000 upload
metadata rows each; nine preserve seeded authority records. The0013 path also
preserves pending import provenance and its dataset/version/command. Strict
read-only parity, Alembic no-drift and owned-database cleanup pass. W12 is restored:
Phase1 has one Verified requirement and 35 Open.

The partial read issuer commits intent/ownership before remote signing and
rechecks current identity/source/authority before disclosure. Replay returns only
a receipt, without a URL or deadline extension. Concurrency and commit acknowledgement
loss are tested against PostgreSQL. Refit and final-data minters now check native
immutable-version and encoded expiry controls. Actual botocore refresh tests prove
slow signing cannot extend the original deadline. A final-data overrun is withheld,
records a signed failure and consumes one-shot permission; retry cannot mint again.
There are 339 distinct passing Python cases across 353 executions, with no failures,
errors or skips in the final recorded runs. Eight touched Python files pass lint
and formatting. Source/artifact audit and unchanged coverage.json are recorded in
[the checkpoint evidence](evidence/phase-1/attempt-object-grants-2026-10-18.yaml).

The broker has one of seven gates met. Its read service is not wired into HTTP or
worker data paths and currently supports only preparation/training safe inputs.
All-stage/child/predecessor authorization, outputs/filesystems, physical retirement,
worker credential removal, current-image native denials and complete audit remain
open. No QA workload, image, Secret, PVC, object or schema changed. The45GB rehearsal
and actual old binaries remain unqualified. Seven original QA objects remain
missing; the original abandoned preservation obligation is unchanged.


### 2026-10-18 — Grant-aware deletion retirement

Project and run cleanup now revoke affected attempt grants and await the latest
original expiry of every started GET/PUT capability before destructive side
effects. Unknown, revoked or nominally expired metadata cannot shorten that
wait. All affected generations, dependent runs and associated champion scopes
are covered. Pending intents can be retired immediately because signing never
started. Legal holds still prevent cleanup and grant mutation.

The existing DeletionPending/outbox path commits revocation and schedules the
remaining wait without consuming failure retries. Cleanup then resumes through
normal stages; selected-run grant records are removed before restrictive attempt
foreign keys cascade. Unrelated runs, scopes, projects, grants and objects survive.
An actual committed signing/deletion race rejects URL disclosure and retains
revocation. All three focused checkpoint gates pass:22 focused cases,16 integration
cases and346 connected regressions,384 distinct tests/executions in total, with no
failures/errors/skips. Lint/format, strict read-only schema parity,57 frozen source
inputs, unchanged schema-source fingerprints and coverage.json also pass.
[Evidence](evidence/phase-1/grant-retirement-2026-10-18.yaml) states the exact scope.

These tests use a controlled clock, committed PostgreSQL, real embedded file
cleanup and doubled Kubernetes/provider signing. No native provider/k3d case,
QA resource mutation, image rollout, commit or push occurred. Broker integration
still has one of seven gates met, and Phase1 remains one Verified/35 Open.
All-stage/child/predecessor authorization, outputs/filesystems, replacement retirement,
worker credential removal and native/current-image qualification remain required.
The45GB rehearsal and actual previous binaries remain open. Seven original QA
objects remain missing and the original preservation gate remains abandoned.


### 2026-10-18 — Pending provider import attempt and safe-read authority

Real import execution now commits a typed preparation attempt with generation,
fence and lease before storage construction. Progress and publication recheck
command and attempt ownership. Retries append generations, preserve prior fences
and keep one retry owner. Terminal outcomes revoke grant rows while retaining
original deadlines. Expired matching process-loss executions can be cancelled or
exhausted without an active orphan; running work with missing or unmatched lease
evidence remains protected. The original cancellation regression caught the
missing-lease case and the implementation was tightened before qualification.

Pending-source authorization validates independently configured policy/key, signed
noncanonical references, request/receipt digests, source/project/operator state,
current command/attempt owner and lease and the latest generation. The existing
broker can issue an exact signed-safe train/validation GET before READY. Its normal
READY/raw paths remain intact. Commit acknowledgement fault injection now inspects
transaction state instead of relying on ORM dirty flags that explicit flushes clear.

The first pending-import gate passes61 checks. All three deletion-retirement gates
were independently reverified on the updated source:384 more cases,445 distinct
passing tests in total. Lint/format, frozen60 authority-source inputs, unchanged
schema32-source fingerprints, strict schema parity and coverage.json pass.
[Evidence](evidence/phase-1/pending-import-authority-2026-10-18.yaml) remains in progress:
HTTP/worker handoff, isolated managed execution, ambient credential removal and
actual native denials before READY are still required. The operator importer still
constructs a privileged store and reads through it. This is not a storage isolation
or current-image qualification. No QA/native/schema/image mutation, deployment,
commit or push occurred. Phase1 remains one Verified/35 Open;45GB and prior-binary
qualification and seven missing original QA objects/abandoned preservation remain open.


### 2026-10-18 — HTTP broker and capability-only provider reader

The maintained attempt router now provides exact safe-object read grants through
pod-bound identity and factory-owned SQL phases. Responses are not cached; replay
returns a receipt without another URL. Database/provider/identity outages fail
closed. The provider Ray reader now uses the broker rather than constructing an
object-store SDK. Missing broker configuration has no ambient-authority fallback.

The worker client rereads projected identity, sends it only to the broker, retains
TLS verification and disables redirects/proxy-environment routing. It verifies
native version/expiry and expected byte/hash binding, bounds broker replies and
stream chunks and suppresses signed-URL errors. Existing byte/hash/schema/row/role,
decoded-memory and scratch-space checks remain. Ray tasks share one manifest
reference instead of repeatedly serializing the full declaration. An integration
case verifies actual signed fixture Parquet bytes through the real HTTP router and
reader while the import is still pending. Provider and Kubernetes transport is
doubled; two real local Ray cases verify data processing, not cloud authority.

G1/G2 of the pending-import integration pass. The authority and deletion dependencies
were reverified along with transport and Ray checks:479 distinct/executed Python
tests, zero failures/errors/skips, current68 source inputs and lint/format. Schema
and coverage.json remain unchanged. [Evidence](evidence/phase-1/brokered-provider-reads-2026-10-18.yaml)
retains managed-workload/native/final-audit gates as open. No QA/k3d resource, image,
Secret, PVC, object or schema mutation, deployment, commit or push occurred.

Managed verifier scheduling and bounded publication, ambient credential removal
from all pods, actual native access denials and current-image qualification remain
required. The operator importer still verifies through a privileged store. Full
broker gates remain one met/six open; Phase1 remains one Verified/35 Open.45GB,
previous binaries and seven missing original QA objects/abandoned preservation
remain unresolved.

Final transport review enlarged the bounded broker reply to64KiB and added a valid
12000-character temporary session query case. Reverification now passes479 distinct
tests (61 authority,384 deletion/connected,32 HTTP/client and two local Ray), with
no failures/errors/skips. Managed workload, native and final integration gates stay open.

### Pending verifier control checkpoint: integration remains incomplete

The importer now shares its durable claim, execution binding, identity-capped
renewal, proof validation and publication logic with the pending verifier control
path. The maintained router accepts bounded start/heartbeat/result/failure
metadata. Start binds the publishing pod to the protected execution; replacement
pods, rewritten intent, revocation and stale identity cannot take over. Pod
identity is rechecked around descriptor I/O, before READY can commit.

The verifier worker reads actual approved Parquet through the HTTP capability
broker and reports its bounded proof through the control endpoint. Tests cover
heartbeats, invalid copy bytes and a lost result acknowledgement without undoing
committed READY. Shared publication replay returns the same IDs without new
storage calls. The control client rejects oversized projected-token files before
trimming, encoded replies and responses over the caller's byte budget.

Independent unlazy re-verification passes 530 distinct tests/executions: 61
pending-authority, 384 deletion/connected, 83 transport/control and two actual
local Ray cases. The source snapshot contains 76 inputs; deletion dependency
verification covers 70 inputs. Schema and coverage.json are unchanged. Kubernetes
and provider transports are doubled; no native permission or current-image
qualification was performed.

This does not complete managed verifier integration: CLI/controller scheduling,
workload isolation, terminal HTTP receipt replay, uncertain or late descriptor
writes and cleanup remain open. The pending-import ledger remains 2/5 met, the
full attempt broker 1/7 met, and Phase 1 remains 1 Verified/35 Open. The 45 GB
migration target still lacks fixture plus reserve and migration overhead space.
Seven original historical QA objects and the abandoned preservation gate remain
unresolved. No shared QA resource, schema, image or deployment changed.

### Pending verifier receipt recovery: integration remains incomplete

READY and an exact publication receipt now commit in one transaction. The original
publishing pod can recover only the committed proof/response under fresh
TokenReview, unchanged signed source and policy, active project/operator and the
latest typed generation. Changed proofs, self-rehashed evidence or command
changes, other pods, missing receipts, expiry and concurrent authority loss fail
closed. Completed attempts still cannot obtain object grants or use generic
admission, start, heartbeat or failure control.

An unknown result acknowledgement now triggers one metadata-only receipt lookup
with the original proof and a freshly read projected token. The worker does not
repeat the uncertain publication. Lost receipt replies stop this bounded recovery
without undoing committed READY. An unfinished publication cannot be restarted
through receipt lookup; managed controller recovery and uncertain descriptor
writes remain open.

Unlazy re-verification passes 552 distinct tests/executions, zero failures/errors/
skips: 61 authority, 384 deletion/connected, 105 transport/control and two actual
local Ray cases. This includes 20 dedicated receipt and six worker cases.
There are 77 frozen source inputs and 71 in the refreshed deletion dependency;
32 schema hashes and coverage.json are unchanged. Kubernetes/provider transport
is doubled, including projected-token replacement; this does not qualify native
permissions, kubelet rotation, isolated scheduling or a deployed image pair.

The pending-import ledger remains 2 met/3 unmet, the full broker 1 met/6 unmet, and
Phase 1 remains 1 Verified/35 Open. The 45 GB migration storage/overhead gap, previous
binaries, seven original missing historical QA objects and the abandoned original
preservation gate remain unresolved. No shared QA resource, schema, object or
image was mutated, and no deployment, commit or push occurred.

### 2026-10-19 managed verifier checkpoint: incomplete

The maintained import CLI now queues work without reading copy bytes or
constructing storage. The real workflow daemon commits the exact isolated
Job/service-account/network plan with the execution claim before Kubernetes I/O.
One resource-create request marker owns each call; a committed lost reply can be
adopted only after body/UID checks. An absent unknown outcome is never blindly
created again. Untracked or replaced objects cannot be adopted or deleted.

The copy verifier uses a per-attempt account and custom-audience projected token,
a pinned image, literal environment allowlist, bounded memory/scratch/deadline and
explicit TLS egress. It receives no database/provider credentials or default
Kubernetes API token. Admission checks actual observed resource UIDs, Job owner,
security-critical pod/container fields and selected network policy; injected
authority, extra replicas and broader policies fail closed. Required protocol
provider-import-verifier-v1 rejects older control before copy reads. Native signer
handoff also rejects safe-copy locations inside application-owned prefixes.

Managed live cancellation fences work. UID cleanup preserves isolation while
Pods or original issued read deadlines remain and precedes replacement or project
deletion. Descriptor publication has an irreversible pre-SDK marker; uncertain
publication cannot write again. resource_unknown/publication_unknown preserve
intent and cannot become completed cleanup. Permanent-outcome resolution and
legacy/untracked migration remain required before this integration can pass.

Final independent unlazy re-verification passes 586 distinct tests/executions:
61 authority, 384 deletion/connected, 139 transport/control and two real local Ray
cases; zero failures/errors/skips. The snapshot covers 88 connected source inputs,
82 in the deletion dependency, and unchanged 32 schema fingerprints/coverage.json.
An initial broad run exposed cached-settings coupling in the new test fixture;
explicit fixture settings fixed the order dependence before final re-verification.
A synthetic-expiry fixture was corrected to retain its database expiry invariant;
no production constraint was weakened.

Ten Helm profiles render after fixing the external PostgreSQL example's required
scoped worker Secret reference. Three local k3d server-side dry runs accepted the
service-account, network-policy and Job declarations. Those dry runs created no
resources and prove no pod execution, native permission, CNI, TLS or rotation.
Kubernetes/provider transports in worker tests are doubled; the actual fixture
Parquet and committed PostgreSQL handoff are exercised. No image build, rollout,
shared QA mutation, schema change, commit or push occurred.

Pending-import gates remain 2 met/3 unmet, the full broker 1 met/6 unmet and Phase 1
1 Verified/35 Open. Current-image native/process-loss and permanent unknown-outcome
qualification remain open. The 45 GB migration still lacks fixture plus protected
reserve/overhead capacity (47,778,734,080 bytes free when rechecked). Previous
binaries and seven original missing QA objects remain unresolved; original
preservation G7 remains abandoned. No historical output or signature was replaced.


## 2026-10-19 — atomic provider descriptor metadata; Phase 1 remains open

New provider imports no longer PUT and read back a duplicate native JSON descriptor.
The signed distribution receipt already reconstructs its canonical bytes. Publication
validates the full copy proof, checks pod authority outside SQL, then commits the
protected descriptor reference, prepared artifact, sealed split, READY state and
exact response receipt in one fenced PostgreSQL transaction. The logical URI is
bound to the dataset version UUID. Existing verified native descriptor URIs remain
compatible without rewriting or deleting their objects. Safe-copy verification
still reads and validates the actual signed immutable train/validation versions.

Training resource estimates now use signed train-object bytes and protected safe
role counts; opaque final-test commitments do not require invented row counts.
Provider restart resolves protected provenance without a physical descriptor check.
Raw dataset digest, split-count and object-availability behavior remains covered.
The provider restart test proves the validated launch handoff, not full training.

Actual PostgreSQL tests retain claim/publication commit-then-lost-ack recovery,
rollback, exact replay, one artifact/split under concurrency and authority failures.
Six new metadata/consumer cases and a controller legacy-write cleanup barrier pass.
The removed descriptor-PUT failure case no longer has a corresponding operation;
its canonical-hash and stale-owner checks now exercise the metadata boundary.
A seeded existing descriptor provides the positive deletion-preservation control.
Legacy descriptor_requested without a committed receipt still blocks publication,
cleanup completion and replacement. No old uncertainty event is cleared.

Independent unlazy re-verification passes 678 distinct tests/executions: 60 import
authority, 232 transport/worker/consumer/raw-training, 384 grant-retirement/deletion
and connected regressions, and two actual local Ray cases. There are no failures,
errors or skips. Ruff lint/format and diff checks pass; 91 connected source inputs
and 32 unchanged schema inputs are fingerprinted. The schema remains0014 and the
existing coverage.json hash is unchanged. Updated public evidence is in
managed-provider-verifier-2026-10-19.yaml and the affected authority/read/retirement
records. Earlier 586-case results are historical and superseded for these inputs.

The importer still has two gates met and three open; the original full broker
has one met and six open. The whole Phase 1 ledger remains one Verified/35 Open.
Permanent unknown Kubernetes creates, legacy unknown descriptor writers, current
native images/permissions/process-loss and all-stage broker integration remain
required. No current image build, shared QA mutation, rollout, commit or push
occurred. The host recheck found47,774,375,936 bytes free: the user-confirmed45GB
migration fixture cannot fit with the protected cluster reserve and overhead.
Largest-table row targets and previous binaries remain unqualified. Seven original
QA objects remain missing and the original preservationG7 remains abandoned;
no historical outputs or signatures were fabricated.


## 2026-10-19 — current-source image preflight on isolated k3d

Two actual k3d image probes now pass on source-current API/CPU test images.
Registry layer/configuration checks and native file hashes bind 191 current
inputs. The actual API emits all four current estimator catalogs; the CPU
consumes them, rejects incompatible catalogs and unsafe overrides, fits Ridge,
checks finite predictions and verifies serialization. Both images import the
current provider control/reconciler/worker modules and reject missing or unknown
verifier protocols. Both Jobs exit zero without restarts as UID10001 with a
read-only root and no ambient database/provider credential environment or default
Kubernetes API token. The isolated namespace has a quota and deny-all policy;
this is not proof of the full native network or credential boundary.

The first full Dockerfile build missed its dependency cache and was deliberately
stopped before compromising the43GB free-space reserve. The Docker source-overlay
attempt also needed to unpack old base layers and stopped at its higher guard.
Both terminal attempts and their logs are retained; no blind retry or broad cache
prune occurred. Small OCI source layers (528,238 API bytes and 409,976 CPU bytes)
reuse the unchanged, previously qualified dependency layers in the local registry.
They run on agent-0, which already caches those bases. These are test images; the
fresh release-build requirement stays open. Source directories use opaque
replacement markers, and independent tar/native-source checks exclude surviving
old modules. The registry image composition audit caught and corrected an initial
assumption that Docker's image ID must equal an OCI configuration digest; the
actual immutable parent manifest and filesystem layers are now checked directly.

The independent audit matches all164 recorded shared resource UID/specification/
image/restart baselines. No shared QA Pod, image, Secret, PVC, object or schema was
changed, and no shared workload was restarted. Existing678 Python/PostgreSQL/Ray
checks remain current; the two native image cases are reported separately.
Evidence: source-current-image-preflight-2026-10-19.yaml.

All four native qualification gates remain open. Dedicated database/TLS store
fixtures, the actual maintained managed verification path, native permission and
identity denials, rotation/process-loss and physical cleanup remain required.
The parent importer stays2/5 met, the original broker1/7, and Phase1 remains one
Verified/35 Open.45GB/prior-binary qualification and the original seven missing QA
objects/abandoned preservation gate remain unresolved. No commit or push to a
remote source repository or shared rollout occurred; only isolated local test
images were pushed to the existing local registry.


## 2026-10-19 — dedicated native fixtures; submission failed placement guard

Dedicated bounded TLS PostgreSQL16.15 and S3-compatible fixtures now run in the
new native namespace. Plaintext database TCP is rejected; actual API connections
use verify-full and the independent CA. Current maintained migrations and schema
verification pass at0014 (44 tables including Alembic). Five native immutable
versions, including real safe Parquet and a changed latest version, retain their
original sizes/hashes. These are new synthetic fixtures, not historical repairs
or the45GB rehearsal.

An upstream PostgreSQL CDN pull failed; the exact cached platform was mirrored
to the local registry and its filesystem verified. A host database forwarding
tunnel terminated after plaintext rejection/client disconnect. The original
migration had committed0014; resumption checked that head and zero metadata
before seeding, without reinitializing PostgreSQL. The unchanged maintained
operator CLI was then executed inside the actual trusted controller pod to
remove the fragile host forwarding link from registration.

That CLI queued the signed import, and the maintained workflow daemon submitted
one actual managed verifier. The scheduler selected uncached agent-1. Protected
cancellation succeeded, but the image pull continued and the disk reserve was
breached: the lowest observed free space was40,334,802,944 bytes, below43GB.
The test is failed; recovering space does not qualify it. The late verifier start
hit the real API and received401. No grants, READY versions or prepared artifacts
were issued. Controller UID cleanup completed and all per-attempt Job/Pod/SA/
network resources disappeared. No operator container stop was needed: the
container had already gone when inspected. Only the new image was targeted in a
removal attempt; it returned nonzero and the image is now absent. No broad or
shared cache prune occurred. Free space recovered to approximately44.7GB.

The native controller is scaled to zero pending explicit approved verifier
placement and an effective submission guard. The next fix must use the existing
Helm/environment/workload contract and admission digest; no test-only command or
hidden scheduler dependency. Preserve the cancelled attempt and allocate a new
signed source for a later run. All four native gates remain open. Evidence:
native-verifier-fixtures-2026-10-19.yaml, with193 source inputs and retained failed
attempt/transport records. The five original native fixture versions, prior678
tests and164 shared resource baselines remain verified. No shared QA Pod, image,
Secret, PVC, object or schema was changed or restarted. Phase1 remains one
Verified/35 Open; fresh release builds,45GB/prior-binary qualification and seven
historical missing objects/original abandoned preservationG7 remain unresolved.


## 2026-10-19 — verifier placement and production-session publication

Approved `providerImports.nodeSelector` now reaches the trusted control service,
committed Job digest and actual Pod admission. Invalid selectors fail before
claiming work; altered selectors, preassigned Job nodes and custom schedulers are
rejected. The isolated test uses cached agent-0 and a calibrated 512 MiB limit for
two tiny objects; the production default remains 2048 MiB.

The actual native run exposed a publication bug hidden by test sessions with
autoflush enabled: credential revocation and the receipt received the same event
sequence, rolling back READY with HTTP 503. Flushing pending events before the
shared sequence query fixes it. A regression reproduces the failure with the
production autoflush setting. Current checks cover 765 distinct tests across
894 executions, including 718 connected authority/transport/retirement cases;
all passed without failures, errors or skips. Schema inputs and coverage.json
are unchanged. Two current-source native image probes also passed; these reuse
immutable dependency layers and do not qualify a fresh release build.

A distinct signed synthetic source then completed through the maintained
CLI/controller/Job/API path: two Parquet objects, four rows, one READY version,
one artifact, one split revision and one receipt. Both grants were revoked and
their original deadlines elapsed; controller cleanup completed with no remaining
attempt resources. Eleven prior failed/cancelled/superseded attempts and all nine
synthetic object versions remain preserved. No retry budget or historical receipt
was reset. The earlier source passed 14 access probes. The extra probe on the
current image failed with a read-grant401; the original worker then completed.
This is not a complete current permissions qualification.

The shared continuation audit matches163 of164 records: the refit API Pod's
restart count increased. Earlier21 shared baseline changes and seven missing
historical QA objects remain unresolved; preservation is not qualified. The
original uncached-node disk reserve failure remains failed. This bounded run
observed more than45.2GB free. The native controller currently runs one replica
in its isolated namespace; prior statements that it is held at zero describe the
earlier failed run.

Phase1 remains **one Verified /35 Open**. All original broad native qualification
gates remain open, including complete isolation, cross-project/attempt, expiry,
rotation/process-loss and unknown-outcome recovery.45GB is the migration
qualification ceiling, not a passed rehearsal; representative rows, prior-binary
compatibility and suitable capacity remain required. Historical preservation G7
remains abandoned with handoff required. Current evidence:
[verifier-placement-publication-2026-10-19.yaml](evidence/phase-1/verifier-placement-publication-2026-10-19.yaml).


## 2026-10-19 — enforce the returned version on brokered reads

The HTTP reader now checks the returned version before exposing storage bytes:
exactly one S3 version ID, GCS generation or Azure version ID must match the
approved object. Missing, empty, null, incorrect, duplicate and wrong-provider
headers fail even when the object bytes are identical. Existing SDK readers
already checked returned versions; this closes the corresponding HTTP gap.

The21 new negative cases reproduced the gap before the fix. All316 connected
checks now pass, including real HTTP broker/worker publication, rotation and
receipt recovery;66 focused checks overlap that set. Lint passes and schema/
coverage inputs are unchanged. A read-only trusted-process probe using the new
reader source verified two objects/four rows in the local TLS S3-compatible
store. It does not qualify a deployed image or GCS/Azure permissions.

The earlier765-test/native-READY checkpoint is historical after this reader
change. Source-current images and the full native matrix require qualification;
no shared deployment was changed for this fix. Phase1 remains one Verified/35
Open.45GB/prior-binary migration, shared preservation and the original seven
missing QA objects/abandoned preservationG7 remain unresolved.
[Current evidence](evidence/phase-1/response-version-2026-10-19.yaml).


## 2026-10-19 — reject unbounded database environment settings

The shared PostgreSQL engine-options boundary now enforces Helm's existing
minimums for direct environment/programmatic configuration too. A zero pool size,
negative overflow and zero pool/connect/statement/lock/idle-transaction timeouts
are rejected before engine construction. Boolean and noninteger values are
rejected; zero overflow remains supported. Both application and independent
qualification-control factories use this boundary in direct and transaction-pool
modes. This fixes a bypass that could silently remove resource limits.

All242 connected checks pass, including87 database-bound cases, existing pool/
TLS/transaction-limit/metrics checks and typed feature/estimator admission.
A real dedicated PostgreSQL test holds the only pool connection, observes an
excess checkout timeout, then verifies backend reuse and pool recovery. The84
invalid configuration cases failed before the fix and pass afterward. No schema
or coverage change, dependency addition or shared k3d mutation was needed.

The feature/estimator caller review found no additional generic execution
parameter path; it does not close W14. W10 remains open for the complete process/
metrics inventory, PgBouncer deployment/failover and current-image qualification.
This local direct PostgreSQL test does not requalify PgBouncer or deployed
images. Phase1 remains one Verified/35 Open.45GB/prior-binary migration and shared/
historical preservation, including the seven missing QA objects and abandoned
preservationG7, remain unresolved.
[Evidence](evidence/phase-1/database-bounds-2026-10-19.yaml).


## 2026-10-19 — project-scoped attempt predecessors; schema head0015

A global predecessor ID foreign key allowed cross-project attempt lineage.
Migration0015 adds `fk_attempt_predecessor_project` over project/predecessor IDs
and a partial predecessor/project lookup index. The original ID foreign key and
all columns remain in place. Existing invalid links abort the migration; no
lineage, receipt, grant or historical output is rewritten.

All250 connected regressions pass, including14 PostgreSQL insert/update denials
across all seven stages and two disposable migration cases. The latter verify
that a failed legacy upgrade leaves its original head, rows and schema intact,
and that valid expansion/downgrade/re-upgrade preserves every seeded field.
All fourteen checked-in prior schemas reach0015 with full structural parity and
no Alembic drift. Each upload-capable path preserves10000 metadata rows; these
separate paths are not a production-sized database rehearsal. The new prior0014
path also preserves pending provider-import provenance. Owned rehearsal
databases are removed after verification.

W12 remains structurally Verified against the new current head. Earlier0014
schema and deployed-image proofs are historical. No shared schema or image was
rolled out. W01/W02 and the whole phase remain open: project-scoped predecessor
integrity does not establish all same-project stage/run/generation invariants.
Phase1 remains one Verified/35 Open;45GB, actual prior-binary execution, the full
native/credential/failover matrix, shared preservation and the seven missing QA
objects/abandoned preservationG7 remain unresolved.
[Evidence](evidence/phase-1/predecessor-project-2026-10-19.yaml).


## 2026-10-19 — stable same-run trial ownership; head0016

Migration0016 closes six same-project ownership gaps: inconsistent trial insertion,
trial-ID reassignment, trial/candidate movement to another run and changes to the
run-attempt parent's run or stage. The child trigger now checks and locks the run
parent, trial and candidate; reciprocal parent checks preserve existing children.
Two partial indexes support parent lookups. Existing inconsistent ownership
aborts expansion before installation; the migration never rewrites old attempts.

The guard supports read-committed and serializable writes. Repeatable-read
ownership writes fail closed because their snapshots can hide a concurrent
child. Serializable conflicts require the existing caller's bounded retry path;
no additional retry owner is introduced. Legal same-run trial/candidate changes
and ordinary metadata updates remain supported.

There are286 distinct passing checks across285 connected regressions and36 final
focused checks. Twelve actual PostgreSQL races exercise both write orders across
run, trial and candidate parents under read committed and serializable, observing
real lock waits and checking that no inconsistent attempt survives. Six
repeatable-read cases fail closed. Positive controls accompany the negative
checks. Migration tests preserve all seeded fields, reject corrupt legacy data
without mutation and restore the original function/trigger on downgrade.

The schema verifier now checks critical trigger presence, enabled state, watched
columns/events, function binding, body hash, volatility and execution properties.
It rejects no-op functions, disabled/missing triggers, omitted trial-ID watching,
security-definer changes and stable-snapshot functions. All fifteen prior schema
paths pass with10000 upload metadata rows on each upload-capable path; these are
separate bounded rehearsals, not a45GB database. Complete current structural
parity, Alembic no-drift and all eight BigInteger fields are reverified.

W12 remains structurally Verified at0016. The full phase remains one Verified/35
Open. No shared schema or image rollout was performed. Full native journeys,
complete attempt-generation/state qualification,45GB/actual old-binary migration,
pooler/failover and shared/historical preservation remain required. The original
seven missing QA objects and abandoned preservationG7 are not repaired by these
fixtures. Earlier0014/0015 images and structural proofs remain historical.
[Evidence](evidence/phase-1/trial-parent-2026-10-19.yaml).


## 2026-10-19 — branch regression qualification in progress

The full backend CI selection collects3677 tests. The first run exposed two
fixture defects: catalog startup lacked split/version identity after source
validation was introduced, and the direct candidate-worker test restored
TRAINING_EXECUTION_MODE incorrectly, contaminating later local clustering tests.
The failure sequences were reproduced. Fixtures now provide consistent source
identity and let monkeypatch restore the execution mode correctly; the five
combined targeted cases pass. Production admission and worker behavior were not
relaxed. The contaminated run and its failures are retained; a fresh full run is
in progress in a separately owned test database. No backend-wide green result or
coverage qualification is claimed yet.

Frontend qualification passes under checksum-verified Node24:214 unit tests,
lint, TypeScript/Vite build and all four coverage thresholds. Measured coverage
is94.77% statements,90.43% branches,91.74% functions and96.75% lines. Four desktop/
mobile landing/navigation browser cases pass with all API requests blocked.
These browser cases do not qualify a live backend journey or release image.
[Frontend evidence](evidence/phase-1/branch-frontend-2026-10-19.yaml).

The original coverage.json remains unchanged; fresh reports use ignored private
paths. No shared QA mutation or rollout was performed. Phase1 remains one
Verified/35 Open.45GB/prior-binary/native/failover qualification and shared/
historical preservation, including the seven missing QA objects and abandoned
preservationG7, remain unresolved.


## 2026-10-19 — full backend regression exposes an open coverage gate

The fresh full selection finished: 3676 passed, one failed, zero errors and zero
skips across 3677 tests. The failure was an outdated cancellation-test fixture:
its settings lacked environment, its task type was a namespace, and its source
objects/queries lacked the identities now required by admission. The failure was
reproduced, a private repaired copy passed all37 module tests, and the applied
repair also passes all37. Production guards and cancellation assertions remain
unchanged. Repository Ruff and diff checks pass. The full suite has not yet been
rerun after this third fixture repair.

Fresh backend statement coverage is94.464%, but branch coverage is85.788%
(5837/6804), below the independently enforced90.01% threshold. The backend gate
remains unmet; the threshold was not lowered. The current denominator requires
288 additional covered branches. Missing paths include reconciliation, workload
identity validation and native credential issuance; follow-up checks must verify
meaningful failures and denials. Passing individual fixes cannot substitute for
the final complete run and both coverage dimensions.

Strict read-only schema verification still passes at head0016 in the owned test
database. The original coverage.json is unchanged. Frontend214 unit tests, lint,
build and four API-blocked browser cases remain qualified only in their stated
scope. No shared QA mutation or rollout was performed. Phase1 remains one
Verified/35 Open. The45GB migration, actual prior binaries, full native workflows,
pooler/failover and shared/historical preservation remain open, including the
seven missing historical QA objects and abandoned preservationG7.
[Regression evidence](evidence/phase-1/branch-regression-2026-10-19.yaml).


## 2026-10-19 — verifier policy checks use the live pod labels

A coverage-guided review reproduced two admission defects: another NetworkPolicy
could select an extra live pod label while being missed by the approved-label
check, and a verifier pod could lose policy-only labels without being rejected.
Six pre-fix cases reached valid admission first and then failed to reject the
changed policy or pod. One exercised actual input-grant issuance through the
broker with a doubled signer.

The shared network check now requires the approved policy to select the actual
pod and evaluates other policies against that pod's full label set. The
pre-creation caller still checks the approved Job template labels. Both callers
are updated; no dependency, configuration or schema change is required.

All154 connected tests pass, including all53 managed-admission cases. Matching
matchLabels and In/NotIn/Exists/DoesNotExist selectors block ingress/egress
broadening before the signer and leave no grant row. Nonmatching selectors,
compound AND requirements, absent keys, an additional default-deny policy and
paginated listings have positive controls. Unsupported selectors fail closed.
All30 recorded branches across the selector and network-check functions are
covered. Ruff and format checks pass; the original coverage.json is unchanged.
[Evidence](evidence/phase-1/effective-network-2026-10-19.yaml).

This is a scoped isolation repair. The prior full backend result is historical
after these source changes; full-suite and global90.01% branch qualification
remain unmet. No fresh release image, native workflow or shared QA rollout was
performed. Phase1 remains one Verified/35 Open.45GB/prior-binary migrations,
full native workflows, pooler/failover and shared/historical preservation remain
required. The original seven missing QA objects and abandoned preservationG7
remain unresolved.


## 2026-10-19 — upload-session ownership gap confirmed

The live0016 schema matches metadata, but its two upload-session output links
(dataset_id and dataset_version_id) lack project binding. All other42 declared
project-to-project references are bound. Four actual PostgreSQL probes accept
cross-project insert/update associations after valid same-project controls. All
probe transactions rolled back and retained no fixture rows; no object-store
operation occurred. P1-W01 stays Open. A project-bound foreign-key migration must
preserve current nullable SET NULL deletion behavior, reject invalid legacy rows
without rewriting them, and pass the supported-schema rehearsals. No migration
or repair of these two relationships has been performed yet.
[Gap evidence](evidence/phase-1/upload-session-project-2026-10-19.yaml).


## 2026-10-20 — upload ownership fixed; W01 structurally verified

Migration0017 adds project-bound dataset and dataset-version references to the
existing upload-session table, plus partial lookup indexes. The original global
SET NULL references remain. Four maintained pre-fix denial tests reproduced the
ownership gap; the fixed schema rejects both references on insert/update.
Completed ready-session metadata survives parent deletion, with only the result
IDs cleared. Null references and valid same-project output links still work.

All13 focused cases and341 connected cases pass, with zero failures, errors or
skips. The tests include both invalid legacy references: upgrade failure leaves
all row fingerprints, old constraints/indexes and head0016 unchanged. Valid
upgrade/downgrade/re-upgrade preserves every seeded field and restores prior
foreign-key/index definitions exactly. All16 prior-schema rehearsals reach0017
with strict schema parity and no Alembic drift;13 upload-capable paths preserve
10000 metadata rows each. Every owned rehearsal database is removed. These
fixtures do not qualify45GB physical data or actual previous application binaries.

A separate literal P1-W01 audit maps all16 required table concepts to live tables
and proves non-null ownership paths to projects. All92 foreign keys are validated
and their368 native triggers enabled; all46 declared project-to-project references
have binding and no invalid existing references. The anti-join detector has
wrong-scope, missing-parent and valid positive/negative controls. No duplicate
upload-session table was introduced. W01 and W12 are structurally Verified;
the current ledger is two Verified/34 Open. This does not qualify the whole phase
or its prerequisite phases.

The final audit restarted only the stopped, owned signing-test PostgreSQL
container after the host restart. Its persistent database still held0017; no
schema or fixture data was recreated. All checks use this owned test database.
No shared schema/image rollout was performed. Current structure is44 tables,
657 columns,222 constraints,92 foreign keys,47 indexes and eight BigInteger byte
fields. Earlier0014/0015/0016 schema/image proofs remain historical.

Global backend coverage and a fresh complete suite, native workflows/images,
pooler/failover,45GB/prior-binary migration and shared/historical preservation
remain open. The original seven missing QA objects and abandoned preservationG7
remain unresolved. The original coverage.json is unchanged.
[Upload ownership evidence](evidence/phase-1/upload-session-project-2026-10-19.yaml),
[W01 table evidence](evidence/phase-1/table-ownership-2026-10-20.yaml).


## 2026-10-20 — champion cleanup ownership and replacement safety

Refit and evaluator cleanup now verifies ownership labels, namespace, name and
Kubernetes UID before deleting Jobs, Secrets or NetworkPolicies. Native UID
preconditions protect replacements created between observation and deletion.
Orphan sweeps retain the UIDs they listed. Cleanup waits for confirmed absence;
foreground Job deletion and finalizers retain dependent credentials and policies.
Foreign resources and ambiguously claimed scope keys remain untouched.

Evaluator result-key cleanup uses the recorded key-custody namespace, including
when a submission event is missing after a namespace configuration change.
Missing or invalid custody leaves cleanup incomplete rather than guessing.
Eight retained pre-fix assertion failures reproduce the ownership, mixed-scope
and namespace defects. The fixed source passes38 focused and167 connected tests
with no failures, errors or skips; the focused cases overlap the connected set.
Both shared cleanup functions have no missing measured branches (24 covered).
All four scoped acceptance gates pass.

Nine real local k3d API checks verify owned deletion, foreign preservation and
stale-UID HTTP409 rejection for Secrets, NetworkPolicies and suspended Jobs.
No Pods were created. The isolated test namespace was deleted and API404
confirmed. These checks qualify API deletion semantics only, not worker images,
complete native workflows, previously disclosed URL retirement or capacity.
[Cleanup evidence](evidence/phase-1/champion-cleanup-2026-10-20.yaml).

W07 and W19 remain Open; Phase1 remains two Verified/34 Open. The current schema
still matches head0017 and its schema inputs are unchanged from the W01 audit.
A fresh full backend suite/global coverage, complete native workflows,45GB and
actual prior-binary migration, pooler/failover and shared preservation remain
required.45GB means45,000,000,000 bytes; largest-table row counts remain unknown.
The original seven missing QA objects and abandoned preservationG7 remain
unresolved. The original coverage.json is unchanged.


## 2026-10-20 — project-and-stage dataset workload identities

New distributed splitter and preparation attempts now record service-account
names containing the exact project UUID and stage. The committed splitter-to-
preparation handoff uses the same naming rule. The existing reconciler provisions
and passes these recorded names to workloads; no new provisioning layer or
schema migration was introduced. Replay preserves identities already recorded
on historical attempts, including the earlier shared service-account names.

Three retained PostgreSQL assertion failures reproduce the shared identities in
raw splitter creation, direct preparation creation and successful splitter
handoff. The fixed source passes229 connected tests with no failures, errors or
skips, including three overlapping focused PostgreSQL cases. Both stages' Ray
manifest checks verify recorded service-account propagation; connected admission,
submission, replay and preparation regressions pass. The direct prepared-source
creation fixture stubs approved-source lookup, and the handoff identity test
stubs admission; these are creation checks, not native security qualification.
All three scoped acceptance gates pass.485 frozen test/source inputs and retained
artifacts are fingerprinted; lint, formatting and diff checks pass. The original
coverage.json remains unchanged.
[Identity evidence](evidence/phase-1/dataset-identity-2026-10-20.yaml).

W02 remains Open. This fixes new dataset-attempt identity creation only; existing
identities were not rewritten, and no native workload rollout or complete
project isolation was qualified. Other stage identity boundaries, complete typed
attempt/runtime qualification, a fresh full suite/global coverage,45GB and actual
prior-binary migrations and shared preservation remain required. Phase1 remains
two Verified/34 Open. The original seven missing QA objects and abandoned
preservationG7 remain unresolved.


## 2026-10-20 — complete current backend run; branch coverage still below gate

The entire CI Python selection collected and executed3781 tests against the
existing owned local PostgreSQL database:3781 passed, zero failures, errors or
skips, in1223.31 seconds. This replaces the older3677-case execution as the
current complete-suite baseline. The earlier cancellation fixture failure does
not recur. All three readiness preflights and repository-wide Ruff checks pass.
A post-suite read-only check confirms strict schema parity at head0017, including
the critical trial trigger contracts.486 frozen inputs and all retained test and
coverage artifacts are fingerprinted. Coverage/storage outputs are private;
the original coverage.json is unchanged. No live cloud/Kubernetes tests or
shared QA rollout were enabled. Minimum free space was45,116,678,144 bytes,
above the43,500,000,000-byte reserve.

Statement coverage is21835/23080 (94.605719%); branch coverage is5895/6838
(86.209418%). The independent90.01% branch gate fails. With the current branch
denominator, at least260 additional branch outcomes need coverage; no threshold
or exclusion was changed. The current scope has two met gates and one unmet
coverage gate. The full backend regression requirement therefore remains open.
The largest measured gaps include reconciler lifecycle decisions, training
pipeline outcomes and evaluator cleanup/recovery. Add meaningful failure and
boundary checks, repair any defects they reveal, and requalify the full suite.
[Current backend evidence](evidence/phase-1/current-backend-2026-10-20.yaml).

Phase1 remains two Verified/34 Open. The full suite does not qualify native
workflows/images,45GB physical/actual prior-binary migrations, pooler/failover,
all-stage identity/credential boundaries or shared preservation. The original
seven missing QA objects and abandoned preservationG7 remain unresolved.


## 2026-10-20 — refit uncertain cleanup and observation races

Two PostgreSQL-backed regression probes reproduced actual refit defects after
valid committed submissions: missing submission custody acknowledged completed
cleanup while Job/Secret/NetworkPolicy resources still existed, and a deadline
removed during native observation caused a TypeError. Terminal refit cleanup now
stays pending when its submission evidence is missing. It does not guess an old
namespace or treat unknown resources as absent. Restoring the controlled synthetic
fixture's exact custody allows ordinary confirmed cleanup; no production history
was reconstructed. Concurrently missing/expired scope deadlines now fail the
scope and attempt before further worker creation. Cancellation and lease-only
expiry retain their appropriate scope state.

All152 connected tests pass, including29 overlapping focused cases for custody,
observation races, rejected configuration, explicit CA mounting, transient native
creation failures, changed credentials/manifests and paginated orphan sweeps.
There are zero failures, errors or skips. All three changed recovery decisions
have both outcomes covered; the complete _reconcile function measures30 covered
branches and two missing branches in this scoped selection. All three scoped
acceptance gates pass.487 frozen inputs and retained artifacts are fingerprinted;
lint, formatting and diff checks pass. The original coverage.json is unchanged.
[Refit recovery evidence](evidence/phase-1/refit-recovery-2026-10-20.yaml).

The3781-pass full-backend checkpoint predates this production change and is now
historical. It measured94.605719% statements and86.209418% branches; no current
global coverage improvement or full-suite requalification is claimed. Its source
oracles have been rerun and demoted because the snapshot changed, preserving the
original results. Add the remaining meaningful failure/boundary coverage and
rerun the complete suite. W07/W19 and Phase1 remain Open:two Verified/34 Open.
Legacy custody recovery, all-stage grant retirement, native workflows/images,
45GB/actual prior binaries, pooler/failover and shared preservation remain
required. The original seven missing QA objects and abandoned preservationG7
remain unresolved.


## 2026-10-20 — evaluator authority recovery boundary coverage

Added42 focused cases using the real PostgreSQL-backed qualification-control
service and Ed25519-signed refit, abort and failure receipts. Positive controls
precede response mutations. Checks reject foreign allocation/scope/project/split/
manifest/provider bindings, altered receipts, wrong signing algorithms or keys,
missing/duplicate abort receipts, invalid reasons and HTTP failures. Exact
immutable abort replay succeeds. Validly signed responses with wrong allocation,
evaluator-attempt or frozen-pipeline bindings are also rejected. These signatures
are generated only for synthetic response fixtures; protected authority history
is not rewritten. Failure finalization rejects completed or deleted local scopes.

All105 connected tests pass, including the42 overlapping focused cases, with no
failures, errors or skips. The authority-state, abort-recovery, failure-sealing
and local failure-finalization helpers have30 covered branches and zero missing
branches in this selection. All three scoped gates pass.488 frozen inputs and
retained artifacts are fingerprinted; lint, formatting and diff checks pass.
Production code is unchanged: the existing guards pass these new checks. The
initial fixture setup error (an allocation has no signed receipt until refit
registration) is retained separately and is not a production defect or a passing
qualification result. The original coverage.json is unchanged.
[Evaluator recovery evidence](evidence/phase-1/evaluator-recovery-boundaries-2026-10-20.yaml).

The focused tests request no final-data object grant. Connected worker regressions
use their existing mocked grant fixtures. The canonical provider fixture is AWS;
this is not all-provider/native/TLS/IAM or full lifecycle qualification. The3781-
pass full-backend checkpoint remains historical after the preceding refit code
change; no new global coverage percentage or complete-suite result is claimed.
W09 and Phase1 remain Open:two Verified/34 Open. Native workflows/images, global
90.01% branch coverage,45GB/actual prior binaries, pooler/failover and shared
preservation remain required. The original seven missing QA objects and abandoned
preservationG7 remain unresolved.


## 2026-10-20 — refit predecessor and recorded capability retirement barriers

Three PostgreSQL probes reproduced premature refit replacement: before confirmed
predecessor cleanup, and while recorded started GET or PUT capabilities retained
a future expiry. The planner now revokes recorded predecessor grants and defers
replacement until cleanup is complete and all started capabilities have passed
their original recorded expiry. Existing candidate selection, fences, generation
limits and replay remain intact. Planner-only fixtures explicitly acknowledge
that no workload was submitted; workload tests now wait for actual fake-native
cleanup before requesting their replacement.

Terminal refit cleanup and orphan sweeps revoke recorded grants, stop owned Jobs
first, and retain token Secrets and network policies until recorded GET and PUT
deadlines pass. Logical issuing/issued/unknown/failed/revoked/expired states cannot
shorten that wait. Unstarted issuance and timestamps already past expiry permit
safe progress. The shared deadline helper now covers both methods; provider-import
cleanup uses that helper too. Missing refit submission custody still revokes
recorded grants but cannot acknowledge cleanup or guess resource ownership.
Another attempt's grants remain unchanged.

All194 connected tests pass, including34 overlapping focused cases, with zero
failures, errors or skips. Both outcomes of the planner and dependency-retirement
gates are covered. All three scoped acceptance gates pass.489 frozen inputs and
retained artifacts are fingerprinted. Ruff checks cover all nine changed files;
format checks cover the seven previously formatted/new files, preserving existing
planner/test-planner style. Diff checks pass; original coverage.json is unchanged.
The preliminary logical-expired-status assertion and custody-restoration fixture
sequence collision were repaired and retained separately. The latter now appends
synthetic recovered custody after the new revocation event rather than reusing
its sequence; no production history was reconstructed.
[Refit retirement evidence](evidence/phase-1/refit-retirement-2026-10-20.yaml).

This qualifies application behavior for recorded capability deadlines and fake
native resources. It does not qualify provider-enforced expiry, current native
worker images, unjournaled older URLs, every worker's replacement barrier or
unknown native creation outcomes. Full-suite/global coverage remains required;
the3781-pass/86.209418%-branch checkpoint predates these changes. W07/W08/W19
and Phase1 remain Open:two Verified/34 Open.45GB/actual prior binaries, pooler/
failover and shared preservation remain required. The original seven missing QA
objects and abandoned preservationG7 remain unresolved.


## 2026-10-20 — evaluator predecessor and recorded capability retirement

Five PostgreSQL-backed probes reproduced missing retirement barriers: direct
replacement-key preparation before cleanup or recorded GET/PUT expiry, and
submission/control on an already registered replacement whose predecessor
cleanup was unconfirmed. Shared replacement validation now checks completed
predecessor cleanup and original recorded grant expiry. The publication/control
lock applies the same guard to nonterminal replacement attempts; terminal
identity lookup replay retains its existing behavior.

Terminal cleanup commits recorded grant revocation before native cleanup, stops
owned Jobs first, and retains token Secrets, network policies and result-signing
keys while recorded GET/PUT capabilities remain live. Key-only orphan sweeps
check the exact evaluator project/scope/generation, retain active attempts' keys
even under stopped scopes, and wait for recorded expiry. Other stages' grants
are neither revoked nor used to block that key. Missing custody leaves cleanup
pending even after expiry. The controller also retires a known predecessor
before resuming an already registered replacement, including when only its old
signing key remains; no operator intervention is needed for this known-history
case. Unknown custody remains unresolved.

All184 connected tests pass, including44 overlapping focused cases, with zero
failures, errors or skips. Both outcomes of five changed critical decisions are
covered. All three scoped acceptance gates pass.490 frozen inputs and retained
artifacts are fingerprinted; lint/format and diff checks pass. Existing replacement
fixtures now confirm real fake-native predecessor key cleanup rather than
skipping the barrier. The original coverage.json is unchanged.
[Evaluator retirement evidence](evidence/phase-1/evaluator-retirement-2026-10-20.yaml).

This verifies application behavior against recorded deadlines and fake native
resources. Provider-enforced expiry, unjournaled older URLs, actual current
worker images, unknown native creates and all-stage retirement remain unqualified.
Full-suite/global coverage qualification remains required after these source
changes; no new global percentage is claimed. W07/W08/W19 and Phase1 remain Open:
two Verified/34 Open.45GB/actual prior binaries, pooler/failover and shared
preservation remain required. The original seven missing QA objects and abandoned
preservationG7 remain unresolved.


## 2026-10-20 — durable command replay cannot blindly execute again

Three actual PostgreSQL probes reproduced replay defects: a cached command
ignored another connection's committed response and executed a second callback;
a persisted command without a response was executed again; and a confirmed empty
response executed again before its illegal terminal transition raised. Command
lookups now refresh existing ORM identities. Shared response validation returns
409/idempotency_response_pending when no durable response exists, accepts a
confirmed empty response, and preserves legacy nonempty responses without
rewriting their missing HTTP status. A later committed response can be replayed
without manual history reconstruction. Changed payloads still conflict first.

The shared check reaches generic mutations, contract mutation/membership and
training estimate/launch/derived replay paths. It runs before mutable planning;
actual workflow-specific reconciliation owners remain separate. Two PostgreSQL
insert races force both requests to miss the initial lookup: identical requests
return one stored result, while conflicting requests produce one success and one
409. Exactly one callback executes in either race. Existing current-permission
checks and command/outbox behavior remain tested.

All388 connected tests pass, including23 overlapping focused cases, with no
failures, errors or skips. All three scoped gates pass. Both outcomes of the
shared response guard are covered. A source audit inventories required bounded
Idempotency-Key headers and key forwarding for all seven canonical families:
upload completion, training launch, validation, deployment, governance generation,
monitoring ingestion and cleanup. All22 existing framework/header inventory cases
pass, covering21 annotated command routes. This inventories dispatch boundaries;
it is not full business-callback/native qualification for every family.

All491 frozen inputs and retained artifacts are fingerprinted; lint/format and
diff checks pass. The original coverage.json is unchanged.
[Command replay evidence](evidence/phase-1/command-replay-2026-10-20.yaml).

W04/W05 and Phase1 remain Open:two Verified/34 Open. Missing responses with no
recoverable original owner still require reconciliation or explicit operator
recovery; they cannot authorize an unsafe new mutation. A separate complete-suite
run and independent global coverage checks remain required after these source
changes. Native workflows,45GB/actual prior binaries, pooler/failover and shared
preservation remain unqualified. The original seven missing QA objects and
abandoned preservationG7 remain unresolved.


## 2026-10-20 — evaluator fixture isolation

The complete backend run collected3953 tests and finished with3952 passing,
one failure, no errors and no skips. The failed embedded-storage assertion
received an s3c URI because two evaluator test modules registered a shared
autouse compatibility fixture as a global pytest plugin. The failure and full
coverage accounting are retained in
[the backend checkpoint](evidence/phase-1/replay-backend-2026-10-20.yaml).
Statement coverage was94.685%; branch coverage was86.549%, below the independent
90.01% requirement.

Both modules now import their shared fixtures locally, following the existing
evaluator tests. Production storage and its embedded URI assertion are unchanged.
All105 combined cases pass:42 recovery-boundary,44 retirement and19 object-store
tests. Repository Ruff and changed-file formatting pass; all three scoped gates
pass. [Fixture isolation evidence](evidence/phase-1/fixture-isolation-2026-10-20.yaml).
The subsequent full backend checkpoint is recorded below.

No whole requirement changes status:two Verified/34 Open. Global branch coverage,
native workflows,45GB/actual prior binaries, pooler/failover and shared preservation
remain unqualified. The original seven missing QA objects and abandoned
preservationG7 remain unresolved.


## 2026-10-20 — complete backend after fixture isolation

The corrected current source collects and passes all3953 backend tests against
the existing owned PostgreSQL database, with zero failures, errors or skips.
The retained prior full run's single embedded URI failure is fixed; its assertion
and production storage code are unchanged. The105 focused evaluator/storage cases
overlap this full selection. Full execution took1264.36 seconds.

All491 frozen inputs match. Post-suite strict schema parity passes at
0017_upload_session_project, repository Ruff passes, and all three existing
preflights pass: task index, historical Phase2 evidence and no-Dask runtime.
The original coverage.json remains unchanged. The disk guard was retained;
minimum observed free space was45017645056 bytes, above the
43,500,000,000-byte reserve. No live provider or Kubernetes tests were enabled.

Independent coverage is21907/23123 statements
(94.741167%) and5946/6862 branches
(86.651122%). Statements meet90.01%; branches do not.
At this denominator,231 additional branch outcomes are required. No exclusion,
deselection or threshold reduction was used. Backend gates:two met/one unmet;
fixture isolation gates:three met/zero unmet.
[Current full backend evidence](evidence/phase-1/fixture-backend-2026-10-20.yaml).

Phase1 remains in progress:two Verified/34 Open. Native workflows and release
images,45GB/actual prior-binary migration, pooler/failover and shared preservation
remain unqualified. The original seven missing QA objects and abandoned
preservationG7 remain unresolved. This full Python pass does not close those gates.


## 2026-10-20 — revocation honors pending transaction history

Two PostgreSQL probes using committed attempt/grant fixtures reproduced bugs with
production-style autoflush disabled. Repeated revocation in one transaction ignored the first call's pending
marker and grant updates; a preceding pending workflow event made revocation reuse
its sequence and fail the unique constraint. Both autoflush-enabled positive
controls passed before the fix.

The shared revocation helper now explicitly flushes pending writes while holding
the attempt lock, before querying the marker and next sequence. Repeated calls
preserve one event and return zero newly revoked grants after the first call.
Previously pending events retain their payload and sequence. Original native
capability deadlines remain unchanged; revocation blocks new disclosure and does
not pretend existing URLs have expired. No schema or dependency change is needed.

All329 connected tests pass without failures, errors or skips, including four
overlapping transaction cases in both session modes. The selection includes broker
HTTP/schema, deletion, provider import, refit and evaluator retirement checks.
A source inventory finds10 call sites across six modules, using the same shared
helper. All491 frozen inputs match, repository Ruff and formatting pass, and
post-suite strict schema parity remains at0017. All three scoped gates pass.
The original coverage.json is unchanged.
[Revocation transaction evidence](evidence/phase-1/revocation-transaction-2026-10-20.yaml).

The3953-pass full-backend checkpoint predates this production change. Current-source
full-suite/global coverage qualification remains required; its earlier86.651%
branch result did not meet90.01%. No whole requirement changes status:two Verified/
34 Open. Complete stage/child broker wiring, native workflows/release images,
45GB/actual prior binaries, pooler/failover and shared preservation remain required.
The original seven missing QA objects and abandoned preservationG7 remain unresolved.


## 2026-10-20 — champion transports ignore ambient proxies

Real loopback TLS origins and a separate rejecting CONNECT proxy reproduced six
unexpected proxy routes: refit input reads, evaluator pipeline reads, final-data
reads, refit control, evaluator control and evaluator authority. The six direct
TLS positive controls passed before the fix. This demonstrated inherited proxy
routing; it did not demonstrate interception of TLS-encrypted credentials.

All three urllib storage transports now explicitly disable proxy discovery.
The refit HTTPX client and both evaluator HTTPX clients set trust_env=False.
Existing TLS contexts, explicit CA files, redirect rejection, separate control/
storage authentication, hashes, bounded reads and one-shot final-data consumption
remain in place. No dependency, schema or grant protocol changes were introduced.

All173 connected cases pass with no failures, errors or skips. Twelve overlapping
cases exercise the six actual transport paths with proxies absent and present;
no request reaches the ambient proxy after the fix. The final-data test reads both
registered objects once, rejects a second read and observes no bearer headers at
storage. Existing refit/evaluation/credential/recovery cases pass. All492 frozen
inputs match, repository Ruff and new-test formatting pass, and post-suite strict
schema parity remains at0017. All three scoped gates pass. Original coverage.json
is unchanged.
[Champion proxy isolation evidence](evidence/phase-1/champion-proxy-isolation-2026-10-20.yaml).

This closes the ambient proxy-routing increment only. Refit/evaluator read minters
still do not fully use the durable pod-bound grant broker. Complete stage/child/
output wiring and native permissions/current-image qualification remain required.
The3953-pass full-backend checkpoint predates this and the revocation transaction
change; current-source full-suite/global coverage checks remain required. Its
86.651% branch result was below 90.01%. Phase1 remains two Verified/34 Open.
45GB/actual prior-binary migration, pooler/failover, shared preservation and the
original seven missing QA objects/abandoned preservationG7 remain unresolved.


## 2026-10-20 — enforce declared attempt predecessor lineage

Migration `0018_attempt_predecessor_lineage` now checks every declared predecessor
edge across all seven typed stages. Retries retain stage, logical identity and
logical parents, with exactly the next generation. The matching-profile/dataset
splitter-to-preparation handoff remains supported, including a retried splitter.
Referenced-parent changes must preserve the same rules. Missing legacy links are
not inferred or backfilled.

The initial PostgreSQL baseline accepted 88 invalid links while nine valid retry/
handoff controls passed. Review then caught fourteen self-reference or multi-row
cycle cases in the candidate guard. Final validation runs after statement changes
are visible, before commit, closing that visibility gap. Child writes lock their
predecessor; twelve actual lock races preserve lineage under read committed and
serializable isolation. Fourteen repeatable-read cases assert SQLSTATE23514 and
the exact rejecting ownership constraint. Non-lineage metadata updates remain
supported. Three existing corruption tests now assert earlier database denial
and unchanged persisted data; the other application rejection checks remain.

All 755 connected tests pass without failures, errors or skips, including 155
overlapping focused cases. Five tamper controls verify the new critical trigger.
Six dedicated migration fixtures prove atomic rejection of invalid history and
exact field/old-trigger preservation through downgrade and re-upgrade. Every one
of the 17 supported prior schemas upgrades with seeded-data verification, strict
schema parity and no Alembic drift. Fourteen paths preserve 10,000 upload metadata
rows each; these are bounded fixtures, not physical 45 GB qualification.

Current structural verification covers 43 application tables plus Alembic metadata,
657 application columns, 222 application constraints, 92 validated foreign keys,
368 enabled FK enforcement triggers, 47 nonconstraint indexes and four critical
application trigger contracts. All 494 frozen inputs match. Repository Ruff and
changed Python formatting pass. All four scoped gates pass; owned rehearsal
databases are removed and original coverage.json is unchanged.
[Evidence](evidence/phase-1/predecessor-lineage-2026-10-20.yaml) and
[upgrade/operation notes](phase-1-predecessor-lineage.md).

W01/W12 remain structurally Verified; Phase1 remains two Verified /34 Open.
The 3953-pass full-backend checkpoint predates the subsequent changes; current-
source full-suite and independent coverage checks remain required. Its86.651%
branch result did not meet90.01%. Full stage/child broker wiring, native workflows/
release images, pooler/failover,45GB/actual prior binaries and shared preservation
remain unqualified. The seven missing historical QA objects and original abandoned
preservationG7 remain unresolved.


## 2026-10-20 — monitoring ingestion identity and concurrent writes

Monitoring ingestion now stores the complete typed request hash and rejects reuse
of an observation key with changed input. A deployment row lock serializes metric
sequence allocation and configuration revisions; refreshed ORM rows prevent stale
session state from defeating either check. Lookup uses a bounded database query
and sequence aggregation rather than loading the complete metric history.

Canonical cached responses and body-key replays must match the submitted point,
including user metadata and deployment identity. Ambiguous legacy history without
hash evidence returns409 `monitoring_idempotency_evidence_missing`; retrying the
original canonical Idempotency-Key header preserves valid historical replay.
Changed requests return409 `idempotency_key_reused`; contradictory saved responses
return409 `monitoring_response_mismatch`. Existing history and caches are never
rewritten or assigned inferred hashes. Internal hash evidence stays out of public
metric metadata. Nonfinite point values, nested metadata and threshold values
return422 through the production validation handler.

The corrected initial reproduction had15 failing cases and six passing controls;
six additional nonfinite configuration cases also reproduced failures. Final
verification passes167 connected tests, including41 overlapping focused cases,
with no failures, errors or skips. Six real concurrent HTTP/PostgreSQL scenarios
cover metric sequencing, duplicate/conflicting keys and configuration revisions.
Two cached ORM controls, three supplied timestamp variants and three current-role
controls pass. These tests use the real router, PostgreSQL, services and project
permissions; the authenticated-user dependency is overridden. They do not qualify
native providers, Kubernetes or release images.

All495 frozen source inputs match, repository Ruff and five changed-file format
checks pass, and strict schema parity remains at0018. No schema or dependency
change was needed. All three scoped gates pass; original coverage.json is
unchanged. [Monitoring evidence](evidence/phase-1/monitoring-ingestion-2026-10-20.yaml).

W04/W05 remain Open pending the other canonical mutation families and deployed
qualification. Phase1 remains two Verified/34 Open. The3953-pass full-backend
checkpoint predates the revocation, proxy, lineage and monitoring changes;
current-source full-suite/global coverage checks remain required. Its86.651%
branch coverage was below 90.01%. The user confirmed45GB as the database
qualification ceiling; a physical rehearsal and largest-table row counts remain
unqualified. Full broker/native workflows, pooler/failover, actual prior binaries
and shared preservation remain open. The seven missing historical QA objects and
original abandoned preservation gate `phase1-deletion:G7` remain unresolved.


## 2026-10-20 — serialize governance report generation after audit collection

Governance generation now allocates report versions under a refreshed deployment
row lock after audit collection and HTML rendering. It locks and refreshes the
linked registry entry, rejecting changed deployment/model identity with409
`governance_binding_changed`. The failed command and report intent roll back.
External audit reads occur before these locks. JSON/HTML output always persists
as pending intent for the existing reconciler; the unused inline-write path and
its option were removed. The only production caller already used deferred output.
Existing report history is not rewritten.

The original HTTP/PostgreSQL race returned version1 for both distinct requests;
sequential and same-header replay controls passed. Three additional controls
reproduced acceptance after deployment detachment, source replacement or a model
name change during an audit read. Final verification passes304 connected tests,
including13 overlapping focused cases, with no failures, errors or skips. Two
actual concurrent requests now serialize versions1/2 or replay the same command.
Four audit-read cases allow a separate writer to finish before version locking,
then either generate valid evidence or reject the changed binding. Three current-
role controls, replay after binding removal and cross-deployment key conflicts
pass. Real PostgreSQL reconciliation writes four distinct JSON/HTML keys with the
expected bytes; repeated reconciliation does not write them again.

The HTTP tests use real services, the canonical audit builder/HTML renderer and
project permissions, with the authenticated-user dependency overridden. Binding
controls inject an audit object reader; the byte-integrity reconciler control
uses an in-memory store. These do not qualify native providers or release images.
All496 frozen inputs match, repository Ruff and four changed-file formatting
checks pass, strict schema parity remains at0018 and all three scoped gates pass.
No schema/dependency change was needed; original coverage.json is unchanged.
[Governance generation evidence](evidence/phase-1/governance-generation-2026-10-20.yaml).

W04/W05 remain Open for the other mutation families and deployed qualification.
Phase1 remains two Verified/34 Open. Earlier full-backend and monitoring evidence
is historical for files changed by this increment; current-source full-suite and
global coverage remain required. The3953-pass checkpoint had86.651% branch
coverage, below 90.01%. Full broker/native workflows, pooler/failover, physical45GB
and actual prior-binary migrations remain unqualified. Shared preservation remains
open with seven missing historical QA objects and original abandoned
`phase1-deletion:G7`.


## 2026-10-20 — current full backend checkpoint at schema0018

The complete backend selection now collects and passes all4178 tests against the
owned PostgreSQL fixture, with zero failures, errors or skips, in1368.11 seconds.
This checkpoint includes the revocation transaction, proxy isolation, declared
predecessor lineage, monitoring and governance generation fixes. All496 frozen
source/configuration inputs match after the run. Post-suite strict schema parity
passes at0018 and repository Ruff passes. All three preflights pass: generated
production task index, historical Phase2 evidence validation and no-Dask runtime
checks. Original coverage.json remains unchanged. The lowest free-space reading
was44,929,232,896 bytes, above the43,500,000,000-byte test reserve.

Whole-backend statements cover21967/23159=94.852973%; branches cover5963/6872=
86.772410%. Statements exceed90.01%, but branches do not. At this denominator,
223 additional branch outcomes are needed. The coverage checker exits1 as
required. Two full-backend gates pass and one coverage gate remains unmet after
reverification; no threshold or exclusion was changed.
[Current backend evidence](evidence/phase-1/current-backend-2026-10-20.yaml).
Earlier backend evidence remains preserved as historical.

This is a local fixture-toolchain checkpoint. Its recorded Ray version is2.56.1,
while the manifest pins2.58.0; it does not qualify the release dependency runtime
or current release images. No live provider/Kubernetes test path was enabled.
Native workflows and complete stage/child/output broker wiring, pooler/failover,
actual prior-release binaries, physical45GB migration and prerequisite phase
qualification remain required. The storage recheck finds only the root data disk,
with about44.95GB free, and a separate1GB EFI partition; no second data disk is
available for the45GB fixture plus reserve and migration overhead.

Phase1 remains two Verified/34 Open. Shared preservation remains unqualified:
seven historical QA objects are missing and original abandoned preservation
`phase1-deletion:G7` remains a required handoff. A passing full backend selection
has not closed these broader requirements.


## 2026-10-20 — align champion workload identity with saved Job delivery

New champion refit attempts now record `project-{project_id}-refit`; evaluator
attempts record `project-{project_id}-evaluation`. Their generated Jobs use those
same project-and-stage service-account names. Delivery creates the account named
in the durable Job, in that Job's namespace, instead of using a global hard-coded
name and the current default namespace. The existing account-creation helper and
controller RBAC already support this. Existing planning replays and previously
recorded submission manifests retain their original identity/account; no history
or old grant/receipt is rewritten. The static legacy chart accounts remain for
such replays.

The baseline reproduced five mismatches while two legacy delivery controls
passed. Final verification passes233 connected tests, including nine overlapping
focused cases, with no failures, errors or skips. Real PostgreSQL planning checks
cover both stages, project/stage separation and refit replacement. Four delivery/
replay controls use the declared account even after the current default namespace
changes. Two legacy planning controls preserve original identities. Kubernetes
transport is doubled; this is not native permission or pod-admission evidence.

All497 frozen source/configuration inputs match. Repository Ruff and six Python
format checks pass; the older champion planner keeps its existing formatting for
the single-line identity change. Strict schema parity passes at0018, all three
scoped gates pass and original coverage.json remains unchanged. No schema or
dependency change was needed.
[Champion workload identity evidence](evidence/phase-1/champion-workload-identity-2026-10-20.yaml).

This is the identity prerequisite for champion safe-read brokering. Refit and
evaluator legacy control endpoints still mint unjournaled read URLs. Projected
pod tokens, actual Job/Pod/SA/policy identity validation, immutable native object
versions, durable read/output grant wiring, uncertain-outcome retirement and
native/release-image qualification remain required. A pod UID or object version
must not be invented to fill the existing grant schema. Old plan digests and
historical grant evidence must remain intact when those paths are connected.

W02 and the broader Phase1 remain Open: two Verified/34 Open. The4178-pass full
backend checkpoint predates this increment; its86.772% branch coverage was below
90.01%, and current-source full-suite/global coverage remain required. Release-
dependency runtime, pooler/failover, physical45GB/prior-binary migration and the
prerequisite phases remain unqualified. Shared preservation remains open with
seven missing historical QA objects and original abandoned `phase1-deletion:G7`.


## 2026-10-20 — enforce revocation in live champion access checks

Refit and evaluator shared access checks now reject nonterminal attempts with an
existing credential-revocation event, even if their status remainsRUNNING. The
existing read broker uses the same shared check. It flushes pending events before
querying, so revocation in the caller's autoflush-disabled transaction also blocks
access. A revocation committed during external URL minting prevents disclosure
when the route rechecks current authorization. Valid active reads and completed-
attempt acknowledgement lookup retain their existing behavior.

The PostgreSQL baseline reproduced six bypasses: committed revocation, pending
revocation and revocation during minting, for both champion stages. Four valid
active/completed controls passed. Final verification passes287 connected tests,
including11 overlapping focused cases, with no failures, errors or skips. The
additional common-broker case rejects same-transaction pending revocation before
any signer call or grant creation. The new champion checks call the real route
handlers directly against committed PostgreSQL fixtures; URL minting is doubled.
During-mint revocation uses a genuinely independent connection. Existing checks
continue to verify that external minting holds no SQL connection/lock and stale
capabilities are not disclosed. These checks do not qualify native providers or
Kubernetes permissions.

All498 frozen source/configuration inputs match, repository Ruff and six changed-
file formatting checks pass, and strict schema parity remains at0018. All three
scoped gates pass after reverification. Original coverage.json is unchanged; no
schema, dependency, historical plan/grant/receipt or new grant ledger was added.
[Champion revocation evidence](evidence/phase-1/champion-revocation-2026-10-20.yaml).

Logical revocation does not physically invalidate a previously issued native URL.
Original expiry, credential retirement and cleanup/replacement barriers still
apply. The legacy champion minters are now guarded against this bypass, but their
full pod-bound read/output broker integration remains required, including actual
Job/Pod/SA/policy validation and real immutable object versions. No such evidence
is inferred from a revocation event or fabricated for historical attempts.

Phase1 remains two Verified/34 Open. The4178-pass full-backend checkpoint predates
this and the champion workload-identity increment; its86.772% branch coverage was
below 90.01%. Current-source full-suite/global coverage, release dependency runtime,
complete broker/native workflows, pooler/failover, physical45GB/prior-binary
migration and prerequisite phase qualification remain required. Shared preservation
remains unqualified with seven missing historical QA objects and original abandoned
`phase1-deletion:G7`.


## 2026-10-21 — integrate champion safe reads with the durable broker

New refit and evaluator plans declare `pod-bound-read-v1`; saved Jobs project
attempt/fence-bound tokens. Deployments must configure the existing
`attemptIdentity.controlBaseUrl` with the HTTPS broker root and permit that
endpoint in both stage egress policies. Their existing CA bundles also cover
broker TLS. Worker images must implement the same protocol. Both workers use
the existing broker for approved candidate, train/validation and frozen-pipeline
reads. Admission validates the Job, Pod, service account and effective network
policies against the saved declaration and pins their native UIDs. Immutable object versions and issuance intent commit before signing;
revalidation prevents stale disclosure, nonce replay cannot mint again, and
renewal, revocation or an unknown result cannot extend the original deadline.

Legacy HMAC download routes now return410 without minting. Old plan serialization
and hashes remain unchanged. Unknown legacy read history blocks cleanup,
replacement and deletion until verified recovery; no expiry or grant history is
inferred. New plan fields are rejected by a structural copy of the old closed
schema. This does not qualify actual previous-release binaries or legacy recovery.

All 623 connected tests across 31 modules pass, without failures, errors or skips.
They include 137 broker/identity cases, 80 overlapping champion broker cases,
47 capability-reader cases and 36 worker/proxy cases. Joined worker tests preserve
real TLS storage transport and rejecting ambient-proxy controls. Four real worker
recovery scenarios accept or reject stored results as before, keep cleanup pending
until recorded read expiry, then complete without another final-data grant.
Deletion checks preserve unrelated data and quarantine missing champion history.
Kubernetes/TokenReview and native stat/signing are doubled; two additional joined
HTTP broker tests also double the object transport. These are not native provider,
deployed Kubernetes or release image qualification.

All 501 frozen source/configuration inputs match. Repository Ruff, 31 owned file
format checks and strict 0018 schema parity pass; all four scoped gates pass after
reverification. Original coverage.json is unchanged. No schema, dependency or
production historical plan/grant/receipt was rewritten.
[Champion read-broker evidence](evidence/phase-1/champion-read-broker-2026-10-21.yaml).

Phase 1 remains **two Verified/34 Open**. The 4178-test full-backend checkpoint is
historical; current-source full-suite/global coverage, complete all-stage reads,
outputs/child attempts/final authority, native workflows, release runtime,
pooler/failover and prerequisite qualifications remain open. 45 GB remains the
migration qualification ceiling; largest-table row counts and enough space for
that physical database, reserve and migration overhead are still required.
Shared preservation remains unqualified: seven historical QA objects are missing,
and original `phase1-deletion:G7` remains abandoned with handoff required.


## 2026-10-21 — enforce champion output write authority

Evaluator persistence now rejects committed or pending credential revocation and
controller recovery claims. Both champion output paths revalidate current authority
and their exact durable intent after storage I/O before acknowledging the write.
Refit reloads output history to avoid stale ORM values. Evaluator readback and
signature verification now live in shared persistence, before its final SQL check;
the route no longer repeats that read. Unexpected storage URI or byte size fails.

Revocation during request streaming prevents storage. Refit retains the intent it
registered before reading the body. Changes during a write prevent acknowledgement
while preserving already written bytes and intent for reconciliation. Trusted
controller recovery can still publish a valid stored signed result after worker
revocation or lease expiry; it does not reopen final data.

The baseline reproduced two evaluator revocation bypasses with four passing
controls. All 659 connected tests across 32 modules now pass, including 36 focused
cases, without failures, errors or skips. Tests use committed PostgreSQL,
independent mutation connections, actual HTTP/ASGI body interleaving and local
embedded object bytes. They cover revoked, expired, superseded or recovery-fenced
workers, changed plans/intents, incorrect storage metadata and valid recovery.
Storage I/O holds no broker SQL connection. Native provider behavior and deployed
Kubernetes are separate open qualifications.

All 502 frozen inputs match. Repository Ruff, four Python formatting checks
and strict schema parity at 0018 pass. All three scoped gates pass after
reverification; original coverage.json is unchanged. No schema or dependency
changed. The earlier 623-test read-broker checkpoint and 4178-test full backend
checkpoint are historical after this change.
[Output authority evidence](evidence/phase-1/champion-output-authority-2026-10-21.yaml).

Phase 1 remains **two Verified/34 Open**. Full delegated PUT grants, native in-flight
write retirement, all-stage broker/retry qualification, current release images,
full-suite and global coverage and pooler/failover remain required. Rejecting a stale
acknowledgement does not undo an already started write or prove physical
retirement. The 45 GB physical migration and prior-binary qualification remain open.
Seven historical QA objects remain missing; original `phase1-deletion:G7` remains
abandoned with handoff required.


## 2026-10-21 — remeasure the entire backend after champion integration

The complete CI `tests/` selection now passes **4,308 / 4,308 tests**, with no
failures, errors or skips. Collection and execution match all unique cases;
four UUID4-valued parameters are normalized only for identity comparison across
the two pytest processes. All three CI preflights pass, all 502 frozen inputs
match, repository Ruff passes and post-suite strict schema parity is 0018.

Whole-backend statement coverage is **22,136 / 23,328  = 94.890%**. Branch coverage
is **6,030 / 6,940  = 86.888%**, below the independently required 90.01%. At this
fixed denominator, 217 additional branch outcomes would be needed. No thresholds,
exclusions or test selections were changed, and original coverage.json is intact.
The full-backend verification scope has **two gates met / one unmet**: coverage
G3 remains open. The earlier 4178-test checkpoint stays historical.

The owned test PostgreSQL container was stopped after execution. Its original
container and persistent volume were resumed without reset or recreation, then
the read-only schema audit passed. The failed connection attempt is retained.
Observed free space stayed above 43.5 GB throughout the test run. This is fixture
runtime qualification: local Ray is 2.56.1 while the CPU lock pins 2.58.0. Current
release images, native providers/Kubernetes and release dependency runtime remain
unqualified.
[Full backend evidence](evidence/phase-1/current-backend-2026-10-21.yaml).

Phase 1 remains **two Verified / 34 Open**. Full delegated PUT grants, physical
in-flight write retirement, complete native workflows, pooler/failover and
prerequisite qualification remain required. The 45 GB physical migration and actual
prior-binary qualification remain open. Seven historical QA objects remain
missing; original `phase1-deletion:G7` stays abandoned with handoff required.


## 2026-10-21 — stop hidden S3 mutation retries

Configured AWS/S3-compatible data clients and separate public signing clients now
allow one SDK attempt per operation. Previously the driver configured five SDK retries: an
accepted multipart-upload creation or object write followed by 503 or a connection
drop could be silently repeated. The workflow did not observe the first uncertain
outcome. Explicit caller recovery now queries the original multipart upload ID
without creating another upload. Caller error classification and externally
injected clients retain their existing behavior.

Eight actual boto/botocore HTTP fault cases reproduced duplicate accepted mutations;
four normal controls passed. All 315 connected checks now pass, including 14 focused
checks, without failures, errors or skips. Both client modes, creates and writes,
503/disconnect faults and an environment retry override are exercised. The SDK
connects only to owned loopback fixtures using synthetic credentials. Cloud
permissions, actual cloud assets, TLS and current release images remain unqualified.

All 503 frozen inputs match. Repository Ruff, both changed-file formatting checks
and strict 0018 schema parity pass; all three scoped gates pass after reverification.
No schema or dependency changed, and original coverage.json remains intact.
[S3 retry ownership evidence](evidence/phase-1/s3-sdk-retry-owner-2026-10-21.yaml).

Phase 1 remains **two Verified / 34 Open**. The 4308-pass full-backend checkpoint is
historical after this SDK change; current-source full-suite/global coverage remains
required. Its 86.888% branch coverage was below 90.01%. Full delegated PUT grants,
physical in-flight write retirement, all-stage/native workflows, pooler/failover,
release runtime and 45 GB/prior-binary migration remain open. Seven historical QA
objects remain missing, with original abandoned `phase1-deletion:G7` requiring
handoff. No prior unknown native outcome or historical record was inferred away.


## 2026-10-21 — unknown started PUTs cannot retire from expiry alone

Fixed the shared retirement barrier so recorded started PUTs in issuing, issued,
unknown, failed, expired or revoked state remain pending after their original
deadline unless an exact native-completion event binds the grant, owner, object,
size/hash and immutable version. Known completion still waits for the original
deadline and does not bypass legacy champion-history quarantine. Project/run
deletion, refit/evaluator cleanup and replacement, and provider-import identity/
network cleanup use the barrier. No unsafe rewrite or inferred outcome is added.

Six baseline regression cases reproduced premature retirement. All 486 connected
checks now pass without failures, errors or skips across 15 modules, including
30 retirement cases and 80 conditional-storage cases. Twelve of the storage cases
exercise actual installed SDKs over owned loopback HTTP with synthetic credentials.
One initial test failure was an ordering assumption: GCS queues bucket metadata
reads in a background thread. The corrected check permits only that exact metadata
read while retaining single-mutation, no object-readback/delete on error and byte
preservation controls. The corrected wire and full connected runs pass.

All 506 frozen inputs match. Repository Ruff, formatting for 11 changed Python
files and strict schema parity at 0018 pass; root coverage.json remains intact.
The three narrow safeguards gates pass after reverification. The original full
PUT broker contract remains **0 met /5 unmet**: the storage helper is not connected
to worker uploads, no production writer emits the new completion evidence yet,
and issuer/router/protocol/worker integration and unknown-outcome reconciliation
remain unfinished. [Scoped evidence](evidence/phase-1/started-put-retirement-2026-10-21.yaml).

Phase1 remains **two Verified /34 Open**. Source-current full backend coverage,
all-stage/native release qualification and actual prior-binary migration remain
required. The 45GB physical migration was not run: 45.8GB free cannot accommodate
the 45GB database plus the 43.5 GB reserve and WAL/migration overhead. Largest-table
row counts remain unspecified. Seven historical QA objects remain missing;
original abandoned `phase1-deletion:G7` requires handoff.


## 2026-10-21 — champion outputs use the durable pod-bound broker

Completed both worker output paths through the existing grant table and projected
Pod identity. New plans/manifests use `pod-bound-object-v2`; None/read-v1 shapes
and hashes remain stable. The API binds one role/UUID/own URI/Pod/fence/size/hash
and original deadline before streaming, validates bytes and evaluator signature,
then commits one issuance owner before conditional native I/O outside SQL. Exact
version readback precedes a bound completion event. Lost upload replies trigger
one receipt lookup and no new write. Legacy output routes return 410.

Publication cannot bypass native completion or live worker authority. Regressions
reproduced four premature-publication cases and a revoked evaluator publication;
the existing refit revocation control passed. The fixed worker/controller paths
preserve signed-result recovery without reopening final data. Unknown or unjournaled
write history remains quarantined after expiry. Native completion survives later
revocation, but cannot authorize stale disclosure or new native operations.

All 928 connected checks pass across 39 modules, with no failures, errors or skips.
Focused overlapping checks include 79 authority/protocol cases, 8 publication cases
and 12 actual SDK wire cases. Two cases terminate only the active test-owned SQL
connection after a native write and verify unknown custody/no reupload. The run
also covers both actual workers, lost replies, ASGI stream revocation, concurrent
SQL preparation, original deadlines, old hashes and old-schema rejection.

One broad run exposed GET-only fixture assumptions and selection of an arbitrary
grant expiry; checks now include PUTs and the latest original deadline. Another
run had an allocation 409 while a separate probe overlapped the shared synthetic
split identity. The guard was retained; the final serial 928-case run passed.
All failed reports remain available. Native wire checks use owned small loopback
payloads and synthetic credentials; joined worker calls use explicit native and
Kubernetes doubles. No cloud/release/large-volume claim is inferred.

All 509 frozen inputs match. Repository Ruff, formatting for 28 changed Python files
and strict schema parity at 0018 pass. Existing champion-planning style is retained
with only the protocol-string change, verified against its prior hash. Root
coverage.json is unchanged. All five integration gates pass after reverification.
[Broker evidence](evidence/phase-1/champion-put-broker-2026-10-21.yaml).

Phase1 remains **two Verified /34 Open**. The whole-backend checkpoint predates
these changes; current global coverage remains required. All-stage/child/native
permissions, unknown-outcome reconciliation, large/chunked fault matrices, release
images/runtime, TLS/rotation/process loss and pooler/HA remain open. The 45 GB physical
migration was not run: 45.8 GB free cannot accommodate 45 GB plus the 43.5 GB reserve and
WAL/migration overhead. Largest-table rows and actual prior-binary qualification
remain outstanding. Seven historical QA objects remain missing; original abandoned
`phase1-deletion:G7` requires handoff. No commit, push or deployment was performed.


## 2026-10-21 — current whole-backend validation started

The full CI `tests/` selection now collects **4515 tests** against all 509 frozen
inputs after the PUT broker/publication changes. All three CI preflights pass.
The serial test process is still running; no full-suite or coverage pass is
claimed. Private coverage outputs will be measured independently against 90.01%
for statements and branches, preserving root coverage.json. The dedicated PostgreSQL
fixture and 43.5 GB free-space guard remain in effect. Local Ray remains 2.56.1
versus the CPU lock 2.58.0, so release/runtime qualification remains open.
[Run status](evidence/phase-1/current-backend-put-2026-10-21.yaml).

Phase1 remains **two Verified /34 Open**, including the unrun 45 GB physical
migration and original abandoned preservation `phase1-deletion:G7` handoff.


## 2026-10-21 — full-backend result requires fixture isolation and more branch coverage

All 4515 cases completed: 4511 passed and 4 failed, with no errors or skips. Statement
coverage is 94.880%; branch coverage is 86.793%, below 90.01%. The current branch
denominator needs 227 more covered outcomes. No threshold or exclusion changed.
Full-backend gates at that frozen snapshot are 1 met /2 unmet; Phase1 is incomplete.

The failures are the broker's premature-publication check, two provider-URI
publication checks and the embedded upload/streaming contract. Registering the
HTTP broker test module as a global pytest plugin also registered its synthetic
cloud-URI autouse fixture globally. The repair keeps explicit fixtures local and
retains every assertion. All 97 affected tests pass with broker-first collection;
reverse-order and connected checks are underway. A source-current full suite
remains required after the repair.

The original 509-input full snapshot and failure report remain retained. All three
CI preflights pass; root coverage.json stays unchanged. Native/release/runtime,
actual prior-binary and 45GB qualification remain open, with original abandoned
`phase1-deletion:G7` still requiring handoff for seven missing historical objects.


## 2026-10-21 — broker fixture isolation passes connected requalification

The global helper-plugin registration is replaced by explicit module-local
fixture imports. Every original broker assertion is preserved, with added upload
failure diagnostics. All97 affected cases pass in both collection orders;
all947 connected cases pass across40 modules without failures, errors or skips.
Only the broker test module differs among509 frozen inputs; production, coverage
configuration and exclusions are unchanged. Repository Ruff, changed-file format,
strict0018 schema parity and root coverage preservation pass.

The fixture-isolation scope has3 met /0 unmet /0 abandoned gates. This is a
scoped repair: the full4515-case requalification and independent90.01% coverage
remain required. The earlier4511-pass/4-failure full snapshot is retained.
Phase1 stays two Verified /34 Open. Native/release/runtime, actual prior-binary
and45GB physical migration remain open; original abandoned phase1-deletion:G7
still requires handoff for seven missing historical QA objects.
[Evidence](evidence/phase-1/broker-fixture-isolation-2026-10-21.yaml).


## 2026-10-21 — full backend passes after fixture isolation; branch coverage stays open

All 4515 collected cases pass without failures, errors or skips. Collection and
execution identities match, all 509 frozen inputs remain unchanged, and all three
CI preflights plus repository Ruff and post-suite strict 0018 schema parity pass.
The four previously failing cases pass without weaker assertions or production
changes. Root coverage.json is preserved; private outputs remain separate.

Coverage measures 22397/23593 statements (94.931%) and 6111/7034 branches (86.878%).
The independent 90.01% branch threshold still fails and requires 221 additional
covered outcomes at the current denominator. Full-backend gates are 2 met /1
unmet /0 abandoned; no whole-phase completion is claimed.

The retained frontend checkpoint matches all 76 source hashes and 10 artifact
hashes: 214 unit tests, lint/build, four coverage dimensions and four offline
browser cases. This is an integrity recheck of retained evidence, not a new
execution or live API/release qualification. Local Ray remains 2.56.1 versus
the CPU lock 2.58.0.

Phase1 remains two Verified /34 Open. Generic revocation guards and all-stage
broker/native credential removal remain implementation work. Native/release,
actual prior-binary and 45 GB physical migration qualification remain open.
Original phase1-deletion:G7 remains abandoned, requiring handoff for seven
missing historical QA objects.
[Full result](evidence/phase-1/current-backend-fixture-isolation-2026-10-21.yaml).


## 2026-10-21 — generic revocation guards pass focused checks

The real PostgreSQL baseline records 37 failures /18 passes across 55 cases:
36 cases lack typed revocation denial, while a pending child revocation collides
with a terminal event sequence. Existing active controls and committed receipt
recovery pass. The shared revocation helper now protects generic admission and
ordinary training/analysis/trial mutation guards, including pending markers.
Trial enrollment also denies a revoked nonterminal child before assigning an
execution identity. Verified terminal replay and explicit predecessor adoption
remain available without new compute or changed historical receipts.

All 56 focused cases pass. A connected attempt was stopped after capturing an
existing test's changed denial-stage message; its 467-case partial report and
runner interruption are retained, not counted as a completed suite. Only that
test's expected diagnostic text changed; its no-signer/no-grant assertions stay
intact. The new 1647-case connected run is underway across 63 modules on 510
frozen inputs. No scoped or whole-phase completion is claimed yet.

The earlier 4515-pass full snapshot is historical; it needs source-current
requalification after these guards. Its independent branch coverage was86.878%,
requiring 221 further covered outcomes at that denominator. Native credentials,
direct native writes and SQL-spanning I/O remain broader implementation gaps.
Native/release, actual prior-binary,45GB qualification and original abandoned
phase1-deletion:G7 remain open.
[Evidence](evidence/phase-1/generic-attempt-revocation-2026-10-21.yaml).


## 2026-10-21 — generic revocation guards pass connected qualification

All 1647 connected cases pass across 63 modules with no failures, errors or skips.
The scope has 4 met /0 unmet /0 abandoned gates, reverified against 510 frozen
inputs, repository Ruff, changed-file formatting and strict 0018 schema parity.
All 58 focused cases pass, including nine new terminal-recovery/adoption cases
and the existing pending-grant and runtime controls. The prior full connected
attempt recorded 1645 passes/2 failures because its session double lacked the
attempt ID and revocation query/flush interface. That double now represents an
active, unrevoked attempt; every original test assertion remains unchanged.
The failed/interrupted attempts and source manifests remain retained.

Committed and pending revocations deny generic admission and ordinary training,
analysis and trial mutations. Verified completed receipts and explicit
predecessor adoption remain available without new execution or lease extension.
No native credential, output journal or direct-native-I/O completion claim is
made: those broader implementation gaps remain open.

The earlier 4515-pass full backend is a historical 509-input snapshot; a new full
run is required after these changes. Its branch coverage was 86.878%, below the
independent 90.01% threshold. Phase1 remains two Verified /34 Open. Native/release,
actual prior-binary and 45 GB physical migration qualification remain open, as
does original abandoned phase1-deletion:G7 for seven missing historical objects.
[Evidence](evidence/phase-1/generic-attempt-revocation-2026-10-21.yaml).


The source-current full-backend run after the generic guards now collects
4561 cases on the same 510 frozen inputs. All three CI preflights pass. Its
terminal test result and independent coverage remain pending; root coverage.json
is preserved.
[Run status](evidence/phase-1/current-backend-generic-revocation-2026-10-21.yaml).


## 2026-10-22 — explicit queue-selector inventory during the frozen backend run

Static source review accounts for 17 explicit SKIP LOCKED selections and one
transaction-scoped advisory acquisition across 166 production Python files and
15 functions. Nine positive/negative scanner controls distinguish the selection
and lock kinds. The inventory is source review only: dynamic SQL, dependency
internals, blocking/no-lock polling loops and runtime worker behavior are not
qualified by it. W03 and the whole Phase1 remain Open.

The concrete next implementation gaps include native upload deletion/abort under
SQL locks without a committed cleanup owner, Kubernetes observers holding their
selection transaction across remote reads, candidate stats during planning, and
transaction-bound native outbox handlers. Existing durable claims and several
split-transaction paths are distinguished rather than counted as blanket
coverage. No running full-backend inputs, schema or dependencies changed.
[Inventory](phase-1-queue-claims.md) and
[evidence](evidence/phase-1/queue-claims-2026-10-21.yaml).


## 2026-10-22 — full-backend result and runtime-foundation fixture correction

The frozen 4561-case full run completed with 4558 passes /3 failures, without
errors or skips. All three CI preflights pass. Statement coverage is94.894%;
branch coverage is86.779%, below the independent90.01% minimum and requiring
228 further covered outcomes at the current denominator. The full checkpoint
has1 met /2 unmet /0 abandoned gates.

All three failures come from older session doubles that return an attempt for
the new event lookup or exhaust a fixed list of scalar results. The doubles now
explicitly represent no revocation event. No production code, test assertion or
expected exception changed. All105 cases in the complete affected module and
PostgreSQL revocation/parent-lease/publication controls pass. All three fixture
gates, repository Ruff and strict0018 schema parity pass; new hunks need no
formatting and the six unrelated legacy formatter hunks remain untouched.

The failed full snapshot is retained, and a fresh full-backend/coverage run is
required. Phase1 remains two Verified /34 Open. Native credentials/brokers,
transaction-bound I/O, release/runtime and45GB qualification remain unfinished;
original abandoned phase1-deletion:G7 still requires seven historical QA objects.
[Fixture evidence](evidence/phase-1/runtime-foundation-fixtures-2026-10-22.yaml).


The fresh full backend after the session-double correction collected 4561 cases
on 510 frozen inputs and is running. All three CI preflights pass. No complete
backend or coverage pass is claimed until its terminal result is verified.
[Run status](evidence/phase-1/current-backend-runtime-foundations-2026-10-22.yaml).


## 2026-10-22 — all 4561 backend cases pass; branch coverage stays open

The source-current full run passes every collected case with no failures, errors
or skips. Collection/execution identities match, all510 frozen inputs match,
all three CI preflights and repository Ruff pass, and post-suite strict0018
schema parity remains intact. Root coverage.json is unchanged.

Coverage measures22403/23601 statements (94.924%) and6118/7042 branches (86.879%).
The independent90.01% branch minimum remains unmet, needing221 further covered
outcomes at this denominator. Full-backend gates are2 met /1 unmet /0 abandoned;
Phase1 remains two Verified /34 Open. This does not qualify native/release,
actual prior-binary,45GB or original abandoned phase1-deletion:G7.
[Evidence](evidence/phase-1/current-backend-runtime-foundations-2026-10-22.yaml).


## 2026-10-22 — upload retention guards

Legal hold and future retention now block explicit abort and every automatic cleanup status. Filters apply before LIMIT and locked rows refresh cached ORM state. 153 connected checks pass, including 55 focused PostgreSQL cases; baseline reproduced 20 failures. Eligible, equal-deadline, idempotent no-op and provider-retention deferral behavior pass. [Evidence](evidence/phase-1/upload-retention-guards-2026-10-22.yaml).

This policy increment does not close native cleanup ownership or SQL/I-O separation. Phase1 remains 2 Verified/34 Open; current-source full/coverage, native/release, prior-binary and45GB qualification remain required. Original phase1-deletion:G7 remains abandoned for seven missing historical QA objects.


## 2026-10-22 — project deletion preserves retained uploads

Project deletion now also respects stored upload hold/retention at admission and before every queued stage. 242 connected checks (70 focused PostgreSQL cases) pass after reproducing 48 failures. Eligible/equal-deadline deletion, run-only deletion and policy-release replay pass; 32 repeated waits preserve the retry budget. [Evidence](evidence/phase-1/project-upload-retention-2026-10-22.yaml). Native cleanup custody and SQL/I-O separation remain required; Phase 1 stays 2 Verified / 34 Open.

This closes a project-prefix deletion bypass for retained uploads. It does not qualify native cleanup ownership, interrupted operations or concurrent out-of-band SQL policy changes. Current-source full/coverage, release/native, prior-binary and 45GB migration checks remain required. Original phase1-deletion:G7 remains abandoned for seven missing historical QA objects.


## 2026-10-22 — champion planner releases SQL before inspection

Champion planning now releases its SQL connection and scope/run/training locks before candidate stat, then revalidates current selection under fresh locks before storing a plan. 138 connected checks (15 focused) pass, including competing planners, stale selection, oversized/wrong metadata, inspection errors and actual owned-process death during a doubled read. [Evidence](evidence/phase-1/planner-inspection-2026-10-22.yaml). W03/W10 remain Open; native upload cleanup ownership and observer/outbox boundaries still require implementation.

This is read-only inspection separation, not native cleanup custody or whole-phase completion. Phase 1 remains 2 Verified / 34 Open. Current-source full/coverage, native/release, actual prior-binary, 45GB qualification and original abandoned phase1-deletion:G7 remain required.


## 2026-10-22 — Ray observer reads release SQL locks

Training and dataset RayJob observations now release SQL before native reads and revalidate the current locked attempt/run/profile snapshot before publication. 441 connected checks (32 focused) pass, including canceled/renewed state, competing missing observations and committed results. Retry constructors also preserve dataset/run/trial/scope/campaign identifiers; the PostgreSQL lineage check covers a parent and both active children. [Evidence](evidence/phase-1/ray-observer-inspection-2026-10-22.yaml). The two analysis observers and native mutation custody remain required; W03/W10 and Phase 1 remain Open.

The first run retained two failures from lost training retry dataset lineage; the final frozen snapshot passes all connected cases. This does not close native mutation custody, both analysis observers, all queue leases/fairness or whole-phase qualification. Local Ray 2.56.1 differs from locked 2.58.0. Current-source full/coverage, native/release, actual prior-binary and 45GB checks remain required. Original phase1-deletion:G7 remains abandoned for seven missing historical QA objects.


## 2026-10-22 — managed analysis observation releases SQL locks

Managed analysis observations now release SQL before Job reads and revalidate current attempt/run plus submission/source evidence before publication. 365 connected checks (45 focused across validation, explainability and drift) pass, including a real report commit during an in-flight read, foreign Job identity, competing observers and non-null dataset retry lineage. All 32 focused Ray controls also pass. [Evidence](evidence/phase-1/analysis-observer-inspection-2026-10-22.yaml). The legacy analysis observer, native cleanup custody and full W03/W10 qualification remain required; Phase 1 remains 2 Verified / 34 Open.

This closes the managed analysis read boundary, not native delete/cleanup custody or whole-phase readiness. The legacy analysis observer can still delete a Job under SQL and remains required. Current-source full/coverage, native/release, actual prior-binary and 45GB checks remain open. Original phase1-deletion:G7 remains abandoned for seven missing historical QA objects.


## 2026-10-22 — legacy analysis inspection and durable UID cleanup

Legacy analysis observation and fatal-waiting cleanup now use committed, namespace/UID-bound state. 459 connected checks (84 focused) pass, including atomic intent/projection rollback, competing observers/owners, lost native/SQL acknowledgements, real SDK process death, owned-Pod/finalizer waits and deletion guards. An actual local k3d suspended-Job probe rejected the old UID against a replacement and removed its owned namespace normally, with no Pods created. [Evidence](evidence/phase-1/legacy-analysis-custody-2026-10-22.yaml). Other native cleanup/producer custody and W03/W10 remain Open; Phase 1 stays 2 Verified /34 Open.

The corrected negative baseline runs in a private pre-change source overlay. Initial control/isolation failures are retained; only the final 459-case frozen run is qualified. Failed fixture cleanup used 12 exact user/project/tombstone sets from their own teardown evidence, and the native test used one owned namespace and exact UIDs. No existing QA preservation or maintained-image qualification is claimed. Source now scans 167 production Python files with 17 explicit SKIP LOCKED selections and one transaction advisory acquisition. Current-source whole-backend/coverage, remaining native mutations and producer credentials, actual prior-binary and 45GB qualification stay required. Original phase1-deletion:G7 remains abandoned for seven missing historical QA objects.


## 2026-10-22 — current full-backend qualification after legacy cleanup

The new full CI selection collected all4862 tests on521 frozen inputs; all three preflights pass. The driver is still running, and source changes remain frozen until it is terminal. No full-test or coverage pass is claimed. [Run status](evidence/phase-1/current-backend-legacy-custody-2026-10-22.yaml). The read-only triage recorded unconditioned training RayJob cancellation and unvalidated same-name submission conflict replay as required follow-up fixes. All36 Phase1 requirements and original abandoned phase1-deletion:G7 remain required.


### 2026-10-22 — full backend checkpoint after legacy cleanup

All **4,862 tests passed**, with zero failures, errors or skips. All three CI preflights, strict schema0018 parity and repository lint passed. Independent coverage: **22,703/23,917 statements (94.92411%)**, **6,218/7,158 branches (86.86784%)**. Branch coverage still fails90.01%;225 further outcomes at this denominator are needed. The original coverage.json is unchanged. Three gates re-executed:2 met/1 unmet. This checkpoint precedes subsequent Ray cancellation changes. [Evidence](evidence/phase-1/current-backend-legacy-custody-2026-10-22.yaml).

The user confirmed45 GB as the maximum migration qualification target; row-count distribution is unspecified. The43.5 GB reserve and roughly45.3 GB free space prevent a genuine45 GB database rehearsal locally. No reserve reduction or scale qualification is claimed. All36 requirements remain:2 Verified/34 Open. Original abandoned phase1-deletion:G7 still requires the seven missing historical QA objects.


### 2026-10-22 — training Ray cancellation verifies native identity

Training cancellation now reads the RayJob in the run’s stored namespace, verifies its exact name, project/run/attempt/generation labels and nonempty UID, then passes that UID to the existing foreground-delete SDK. Malformed or foreign responses fail closed;404 replay preserves terminal CAS and native read/delete failures remain retryable. Existing job names are preserved.

**355 connected checks pass**, including35 focused cases; the pre-fix source failed34 controls without collection/setup/teardown errors. All three CI preflights, repository lint, new-hunk formatting and strict schema0018 parity pass. Four unlazy gates were re-executed:4 met. On local k3d, suspended RayJobs proved foreign-job preservation, a real UID-precondition409 with replacement survival, correct owned deletion and404 replay. No Pods/RayClusters launched; the owned namespace is gone. Native SQL was a test double in this probe; physical retirement, native transaction custody and release qualification remain open. [Evidence](evidence/phase-1/training-ray-cancel-identity-2026-10-22.yaml). All36 requirements remain2 Verified/34 Open, with the45 GB gate and original abandoned phase1-deletion:G7 unchanged.


### 2026-10-23 — Ray submission binds command scope and exact native specification

Training submission now verifies topic/project/run/type/stage/logical scope before terminal replay or native effects, and refreshes the locked attempt. Shared RayJob creation accepts fresh responses or409 replay only when the native name, namespace, ownership labels, UID and exact submitted specification match. Changed images, entrypoints, environments, resources, worker groups or unrequested execution fields fail closed. Existing job names are unchanged. Worker groups explicitly pin the existing KubeRay numOfHosts=1 default, preserving native replay compatibility without accepting arbitrary extra specification fields.

**410 connected checks pass**, including55 focused cases and every355-case cancellation regression. Captured pre-fix modules fail49 controls without collection/setup/teardown errors. Repository Ruff, new-hunk formatting, strict0018 parity and all three CI preflights pass. Four gates were re-executed:4 met. On local k3d, all three suspended workload types passed actual create/409 replay and conflict rejection; no Pods/RayClusters launched and the owned namespace is gone. The initial strict native failure on implicit numOfHosts defaulting remains recorded, with its namespace cleaned and the final explicit-default probe passing. [Evidence](evidence/phase-1/ray-submission-identity-2026-10-23.yaml).

This fixes submission/cancellation identity gaps; it does not establish one physical execution or full native lifecycle custody. Persistent native UID receipts, observer identity, credential/producer retirement, all other side effects and maintained-image qualification remain required. The full4862-pass checkpoint is historical after these two increments; independent branch coverage remains below90.01%. Phase1 stays2 Verified/34 Open. The45 GB migration rehearsal and original abandoned phase1-deletion:G7 remain unresolved.


### 2026-10-23 — Ray observers verify native ownership and the full fence

Training, splitter and preparation observers now verify the returned RayJob’s exact name/namespace, nonempty UID and project/resource/attempt/generation labels before publishing status or native IDs. Dataset observations also bind version and stage. The submitter must carry exactly one full fence, project ID and attempt ID and the saved service account, alongside the original Ray jobId. A token sharing the same16-character jobId prefix cannot pass as the current full fence. Training parent scope is checked without changing the missing-parent LookupError contract. Existing terminal receipt recovery, lease expiry and SQL snapshot fencing remain active.

**696 connected cases pass**, including89 focused identity cases, every410-case submission/cancellation regression and197 analysis/recovery cases. Captured pre-fix source fails all86 negative snapshot controls; three positive controls pass. All original assertions in changed existing test modules are preserved. The initial665-pass/13-failure mock run is retained; selected-row/native-identity fixture setup was corrected. Repository Ruff, changed-hunk formatting, strict0018 parity and all three CI preflights pass. Four unlazy gates were re-executed:4 met. [Evidence](evidence/phase-1/ray-observer-identity-2026-10-23.yaml).

Local k3d suspended RayJobs for all three stages pass actual native identity verification and reject mismatched project/full-fence identities, including an equal jobId prefix. The initial short-token probe failed its fixture assertion and cleaned its namespace; the corrected full-length-token probe passes. All three owned probe namespaces are404 on re-verification; the successful probe launched no Pods/RayClusters. This is control-plane verification, not running-image or physical producer retirement qualification. Persisted native UID receipts, all-stage broker conversion, native operation custody,45 GB migration rehearsal and all other Phase1 gates remain required. Status stays2 Verified/34 Open; original abandoned phase1-deletion:G7 remains visible.


### 2026-10-23 — remove unused credentials from current-controller Ray pods

New Ray submit outbox rows now save the server-selected ray-pod-env-v1 policy. The current controller omits database, object-store and tracking environment/configuration credentials from submitters in all three stages, and omits DATABASE_URL from splitter/preparation compute workers. Heads and training compute workers retain the database access their current code still uses. The existing restricted analysis login does not have the required Ray lifecycle/trial grants, so it was not substituted. Full all-stage broker conversion remains required.

Old queued rows retain their exact original payload and legacy manifest; an unknown or explicit null policy fails before native work. All three legacy bodies match the captured pre-change source on deterministic inputs. New generation/retry/handoff producers share the existing enqueue_outbox path; no new config option, dependency or schema migration is added. Older consumers ignore the new payload declaration, so mixed-version rollout and existing credentialed Pods remain unqualified.

**799 connected cases pass**, including65 focused controls, all696 prior connected cases and38 PostgreSQL contracts. The pre-fix factories fail34/65 controls using an adapter that accepts only the new keyword while preserving original bodies. Four unlazy gates were re-executed:4 met. Repository Ruff, new-hunk formatting, strict0018 parity and three CI preflights pass. Local k3d accepted three current and three captured legacy suspended RayJobs; saved templates match their credential policy and same-name replay retains each UID. No Pods/RayClusters launched; the owned namespace is404. [Evidence](evidence/phase-1/ray-pod-credentials-2026-10-23.yaml).

This is a reduction in explicit template secret exposure, not full worker isolation. Ordinary driver/trial SQL, storage-capable filesystems, native custody/retirement, maintained-image rollout, independent branch coverage and45 GB migration qualification remain open. All36 Phase1 requirements remain2 Verified/34 Open; original abandoned phase1-deletion:G7 remains a required historical-data handoff.


### 2026-10-23 — Pod-bound parent lifecycle control

Declared workers now use bounded TLS RPC for parent metadata, start, heartbeat, stored-result recovery, completion and failure. The API binds ownership to the verified Pod UID and worker instance, rechecks SQL authority after native identity I/O, and rejects a run-kind change between context lookup and start. Recovery validates stored result facts and preserves one terminal CAS without minting capabilities or extending execution. Legacy workers share the original SQL lifecycle implementation.

New Ray outbox rows select ray-parent-control-v2; configured training heads and managed analysis select parent-control-v1. Six saved legacy/v1 Ray manifests match the captured builders exactly. Pipeline, trial and data/result access still require their existing credentials; full worker isolation remains open.

**903 connected cases pass**, including 41 worker/manifest and 28 API authority cases. The six captured pre-change controls fail in their bodies. Existing expiry/CAS test patch targets were updated after moving the shared helper; all original assertions remain. All five scoped unlazy gates pass re-verification. Repository lint, changed-hunk formatting, strict 0018 parity and three CI preflights pass on 531 frozen inputs. The local k3d probe exercises actual TokenReview, Pod-bound token rotation and verified TLS with worker SQL disabled, then preserves one result CAS/metric through recovery and repeated completion. Both created probe namespaces are404; the successful Pod never ran a container. [Evidence](evidence/phase-1/parent-control-2026-10-23.yaml).

Maintained-image execution, remaining worker broker conversion, native operation custody/retirement, current-source full backend and independent branch coverage remain required. The confirmed 45 GB migration target still needs sufficient capacity while preserving the 43.5 GB reserve. All 36 Phase1 requirements remain 2 Verified / 34 Open. Original abandoned phase1-deletion:G7 still requires the seven missing historical QA objects.


### 2026-10-23 — Pod-bound durable trial control

Declared fixed-CV, clustering and Tune workers now enroll, record dispatch evidence, start/replay, heartbeat and publish trial results/failures through bounded TLS RPC. The API reuses the existing SQL domain, binds ownership to Pod UID and worker nonce, revalidates source/run/candidate/child authority after native identity I/O, and returns no child SQL fence. Closed finite metadata has a 256 KiB budget. Unknown protocols, missing parents, mismatched replies, withdrawn declarations and uncertain publication stop execution without SQL fallback. Shared terminal replay now also requires CAS version 1.

New Ray outbox rows select ray-trial-control-v3. Configured training heads and compute workers select trial-control-v1; heads retain parent control. Nine captured legacy/v1/v2 Ray manifests remain exactly equal. Existing driver/data/model/tracking credentials remain until their dependent operations are converted.

**1,133 connected cases pass**, including 44 worker/manifest and 27 API authority cases. Three captured pre-change body failures and one positive local control establish the transport gap. Real foreign-project and foreign-parent records, competing workers/Pods, native I/O without SQL locks, cancellation/revocation/source changes, dispatch/result replay, real PostgreSQL loss before claim commit and lost terminal commit acknowledgement are covered. All five unlazy gates pass re-verification on 536 frozen inputs; repository Ruff, new-hunk formatting, strict 0018 and three CI preflights pass. [Evidence](evidence/phase-1/trial-control-2026-10-23.yaml).

The real k3d/TLS probe runs three-fold Ridge CV once, then replays twice without recomputing, with trial worker SQL and legacy admission disabled. It verifies two distinct Pod-bound tokens, one child terminal CAS/event and one parent result CAS/metric. The identity Pod stays pending; its owned namespace and project/run/parent/child rows are read back absent. This qualifies the local control transport, not a maintained image or full worker/network isolation.

Pipeline/analysis/dataset broker conversion, native custody/retirement, maintained-image rollout, current-source full tests and independent branch coverage remain required. The confirmed 45 GB migration target needs suitable capacity while preserving the 43.5 GB reserve. All 36 requirements remain 2 Verified / 34 Open; original abandoned phase1-deletion:G7 still needs the seven missing historical QA objects.


### 2026-10-23 — validated training input context

Declared startup now validates and loads its complete initial run/catalog/feature/override/prepared-source context through Pod-authenticated TLS. It reuses the legacy SQL preparation path with an explicit trusted fence for API calls. The current parent publisher, lease and requesting Pod must agree. Source/run/graph authority is checked again after native identity I/O; TokenReview holds no SQL connection or locks. Missing bindings cannot use a local shortcut. Initialization and replay preserve the original start timestamp, including real connection loss before commit and lost commit acknowledgement.

The closed plan contains approved execution metadata and train/validation locations only. It excludes unknown configuration fields and final locations, rejects canonical-provider final manifests and missing provider plans, and preserves candidate worker identity metadata during resume. The shared legacy/API role guard also rejects location aliases, directory containment, traversal and safe/final overlap. A terminal context cannot authorize a new prepared read.

New Ray rows select ray-training-context-v4; only configured training heads select training-context-v1. Parent/trial control remains enabled. All twelve captured legacy/v1/v2/v3 manifests are exactly equal. Tracking/progress/result SQL and data/model/output access still need their existing credentials and further broker conversion.

**1,390 distinct connected checks pass across the preserved 1,292-case run and the current 401-case affected-path rerun**, including 52 context worker/manifest and 30 API authority cases. The exact union matches current collection; source/AST guards verify that unchanged-source evidence is retained and every startup consumer affected by the review fixes was rerun. One original body failure proves the SQL transport gap. Five scoped unlazy gates pass re-verification on 541 frozen inputs; repository Ruff, new-hunk formatting, strict0018 and three preflights pass. [Evidence](evidence/phase-1/training-context-2026-10-23.yaml).

Real local TLS/k3d delivers the sealed plan into the prepared-reader boundary twice with worker SQL disabled, two distinct Pod-bound tokens, only approved columns/safe roles and no new object grant. The identity Pod never runs a container. All three owned probe namespaces and the current project/run/attempt rows are read back absent. The probe stops before data loading; it does not qualify a whole pipeline or maintained image. A temporary owned-PostgreSQL outage was repaired by starting its exact preserved container, without resetting data; the failed run remains retained.

All 36 requirements remain 2 Verified / 34 Open. Pipeline/analysis/dataset capabilities, native custody/retirement, release/coverage and the 45 GB migration rehearsal remain required. The 43.5 GB reserve is unchanged. Original abandoned phase1-deletion:G7 still needs the seven missing historical QA objects.


### 2026-10-23 — attempt-owned training model outputs

New declared candidate writers serialize and publish models through the Pod-authenticated output broker. The server derives each location from its current project/run/attempt/candidate and content digest, requires the saved writer declaration and sealed input/candidate authority, and commits a fixed candidate slot plus issuance owner before native I/O. Different candidates can write concurrently. An uncertain write cannot be repeated or replaced by another nonce. Native completion is retained across cancellation, while acknowledgement requires fresh authority. Worker response loss uses receipt lookup only. A reusable publication guard requires the exact verified completion; remaining progress/result publication still needs conversion.

**1,302 connected checks pass**, including 14 model worker/manifest and 24 API authority cases. The connected run is retained with a current 272-case manifest rerun after alphabetically sorting two existing constant imports; exact source-diff and case accounting are preserved. Three original body failures prove the worker-storage gap. Five scoped unlazy gates pass re-verification over 545 frozen inputs. The original fifteen legacy/v1/v2/v3/v4 manifests remain exactly equal. New Ray records select ray-model-output-v5 and configured training head/compute writers select training-model-output-v1.

The local probe uses real verified TLS and k3d Pod TokenReview, two distinct bound tokens, actual model serialization and deliberately lost PUT acknowledgement; recovery returns its original receipt after one stored write. Storage is a conditional in-memory fixture, not live provider SDK/permission evidence. Its pending identity Pod never executes a container. The exact owned namespace and SQL fixtures are read back absent, and owned object bytes/secrets are retired. [Evidence](evidence/phase-1/training-model-output-2026-10-23.yaml).

All 36 requirements remain **2 Verified / 34 Open**. Broader credential withdrawal, remaining native custody/retirement, maintained images and full/release/branch coverage remain required. Single model uploads are capped at 5 GB; larger models require tracked multipart custody. The user qualification ceiling is 45 GB physical database size, with largest-table row counts still unspecified. No 45 GB rehearsal was run, and the 43.5 GB free reserve is preserved. Original abandoned phase1-deletion:G7 still requires seven missing historical QA objects.


### 2026-10-23 — training publication custody

Declared broker runs now require the exact verified native model completion at the shared candidate-status boundary used by both partial leaderboard and final result publication. A readable URI is insufficient. The server resolves the current saved parent declaration and revalidates source/candidate authority. Missing identity variables, closed parents, changed fences, duplicate declarations and unknown policies cannot downgrade to legacy publication. Project locks precede attempt/run locks, matching project deletion. Result replay checks the committed digest before candidate mutation; duplicate publication retains one metric commit.

New model slots record their immutable input/candidate digest without changing the closed worker receipt. The recovery reconciler carries explicit verified-model references. Predecessor publication requires the recorded reference at every generation and the same current input binding. Unlisted, legacy-without-binding, unknown or changed-source artifacts are refused. Old slots remain usable in their original attempt; predecessor adoption requires controlled qualification. This protects publication and does not qualify the still-ambient model reader.

**1,509 connected checks pass**, including 27 publication authority cases. Evidence combines the retained connected run with a current 156-case rerun of all changed legacy fixture consumers. Production bytes and old assertions are unchanged; 32 initial fake-session failures are retained and superseded by corrected fixture results. Two original body failures establish the missing-receipt gap. Four scoped unlazy gates pass re-verification over 546 frozen inputs. All eighteen captured legacy/v1/v2/v3/v4/v5 manifests remain unchanged. The local real TLS/k3d probe rejects forged metadata, publishes the verified model through actual SQL partial/final paths, replays the result and completes the parent once with one metric commit. Its conditional storage backend is a fixture, and publication deliberately still uses worker SQL. Owned namespace/SQL resources and secrets are retired and read back absent. [Evidence](evidence/phase-1/training-publication-custody-2026-10-23.yaml).

All 36 requirements remain **2 Verified / 34 Open**. Closed result/progress contracts, complete tournament validation, tracking/SQL withdrawal, model/data readers, remaining native custody/retirement and full/release/coverage remain required. The 45 GB migration rehearsal is still pending and the 43.5 GB reserve is preserved. Original abandoned phase1-deletion:G7 still requires seven missing historical QA objects.


### 2026-10-23 — final training result control RPC

New declared training heads now publish final results through Pod-authenticated control without opening worker-local SQL. The finite closed contract requires the complete selected terminal tournament, the server-ranked winner and matching winner metrics, plus verified model custody for every success. It validates public leaderboard types, preserves candidate row counts and rejects malformed/nonfinite/oversized metadata, foreign scopes and contradictory derived counts. Shared persistence takes an explicit trusted fence and atomically commits metrics, metadata and a request-bound acknowledgement.

A lost acknowledgement permits receipt lookup only. Exact result replay can recover after parent lease expiry without new work; conflicting or changed stored metrics, directions, steps, leaderboard, tracking pointer or typed derived parameters are refused. Parent API, legacy-worker completion and observer adoption all enforce the saved RPC receipt policy. New Ray rows select ray-training-result-v6; configured training heads select training-result-v1, and v6 training submission fails before service-account creation without a valid HTTPS control URL. All eighteen captured legacy/v1/v2/v3/v4/v5 templates remain unchanged.

**1,629 connected checks pass**, including 21 result worker/manifest/deadline and 52 API authority cases. Three original test-body failures establish the worker SQL gap. Five scoped gates pass re-verification over 551 frozen inputs; repository lint, new-hunk formatting, strict schema 0018 and three CI preflights pass. [Evidence](evidence/phase-1/training-result-rpc-2026-10-23.yaml).

The real local TLS/k3d probe uses separate pending writer and publisher Pod identities. The writer cannot publish the parent result; forged artifact metadata is rejected. With worker session factories disabled, valid final publication recovers a deliberately lost acknowledgement and parent completion replays with one metric commit and one terminal CAS. Native storage is explicitly a conditional fixture. Both identity Pods never execute containers; owned namespace/SQL resources, object bytes and secrets are retired and read back absent.

All 36 requirements remain **2 Verified / 34 Open**. Progress/failure/tracking SQL, data/model readers and remaining native custody/retirement still need conversion before ambient credentials can be withdrawn. Shared candidate exception logging also still needs credential redaction. Full/release/coverage, maintained-image/process-loss/mixed-version and45 GB migration qualification remain pending, preserving43.5GB free reserve. Original abandoned phase1-deletion:G7 still requires seven missing historical QA objects.


### 2026-10-23 — training failure credential redaction

Shared supervised/clustering failures now redact exception text before existing truncation and before console, MLflow tags and returned candidate metadata. Wrapped Ray task failures and legacy run failure persistence use the same existing redactor. URL credentials, signed query values, bearer values and quoted credentials at clipping boundaries are covered; ordinary error diagnostics and existing status/control flow remain unchanged. No protocol, dependency, schema or config changes were needed.

Seven original test-body failures prove disclosure across the candidate, Ray and legacy SQL paths. **224 connected checks pass**, including eight focused privacy cases; two scoped unlazy gates pass re-verification over 552 frozen inputs. Source/AST guards restrict the production diff to three redactor calls plus imports. Repository lint, three CI preflights, root coverage and all36 status rows are preserved. The tests use synthetic secrets and isolate network/tracking sinks. [Evidence](evidence/phase-1/training-failure-redaction-2026-10-23.yaml).

This closes the exception-message finding recorded after result RPC. All 36 requirements remain **2 Verified / 34 Open**. Remaining progress/tracking and data/model credential conversion, native custody/retirement, full/release/coverage and45 GB qualification remain required. The43.5GB reserve and original abandoned phase1-deletion:G7 handoff for seven missing historical QA objects are unchanged.


### 2026-10-23 — ordered training progress control RPC

New saved training submissions select `ray-training-progress-v7`. Their heads and compute workers publish candidate phases and partial leaderboards through Pod-authenticated control without opening worker-local SQL. All 21 captured legacy/v1–v6 templates remain unchanged, including v6 final-result RPC. Shared persistence receives an explicit trusted fence; sealed source, candidate identities and parent routing are checked again after native identity verification. Only the admitted parent publisher can publish the complete aggregate.

Durable request receipts bind each stream to one Pod/client and monotonic sequence. Exact replay is harmless, nonce/sequence reuse conflicts, and older updates cannot replace newer phases. Terminal candidate metadata cannot change through partial or final publication. Pending snapshots preserve terminal outcomes, and updates after trusted final publication are no-ops, including recovery after lease expiry. Resumed candidates retain original training and validation row counts.

**1,747 connected checks pass**, including 15 progress worker/manifest/count and 50 API authority cases, with no failures/errors/skips. Four original test-body failures establish the worker SQL gap. Five scoped gates pass re-verification over 557 frozen inputs; repository lint, new-hunk formatting, strict0018 and all three CI preflights pass. [Evidence](evidence/phase-1/training-progress-rpc-2026-10-23.yaml). Failed test-oracle drafts remain private evidence: initial launch already has pending entries, phase updates mark candidates running, hashes cover normalized closed requests, and receipt nonces cannot be reused between phase and aggregate actions.

A real local TLS/k3d probe used separate pending candidate and parent Pods, fresh bound TokenReview and three distinct tokens. With worker SQL disabled, phase, partial and final publication succeeded; three lost progress acknowledgements recovered through GET only, a candidate Pod could not publish parent progress, and late progress left the final result unchanged. Model storage is explicitly an in-memory conditional fixture. One model write and one terminal CAS were observed. Identity Pods never executed containers. Exact namespace/SQL/object resources and secret files were retired and read back absent.

All 36 requirements remain **2 Verified / 34 Open**. Worker tracking SQL and native tracking/data/model access still prevent broad credential withdrawal. Other native custody/retirement, current full/release/coverage, maintained-image/mixed-version/process-loss, 45 GB migration and 15×10 GiB qualification remain required. The user’s database qualification ceiling is 45,000,000,000 physical bytes; largest-table row targets remain unspecified. No scale test was attempted, preserving the43.5 GB reserve. Original abandoned phase1-deletion:G7 still requires seven missing original historical QA objects; no replacement artifacts were manufactured.


### 2026-10-24 — server-owned training tracking creation

New saved training submissions select `ray-training-tracking-v8`. Heads and compute workers use `training-tracking-v1` for parent/candidate/failure tracking creation and recovery, without opening worker SQL or the inline tracking creation client. The native writer is shared once with legacy workers; its original lease, creation-marker, error and uncertainty branches are preserved by an AST guard and current regression coverage. All24 captured legacy/v1–v7 templates remain unchanged.

The server derives tracking names, tags, backend and parent ID from the current sealed run. Only the admitted parent publisher can create parent tracking; selected candidate/failure roles bind their first Pod owner before native effects. Child creation checks the current native parent lifecycle and lineage. SQL connections and locks are released for native I/O, then source/role/lease/token authority is checked again before publication.

A durable acknowledgement binds the native tracking ID to the command request, parent, backend and role owner atomically with writer publication. Changed command IDs or acknowledgement fields are refused. Existing commands without their original Pod-ownership record require controlled recovery; the API does not invent a new owner. Lost HTTP/SQL acknowledgements use receipt GET only. Native uncertainty never permits a repeated creation. If read-only reconciliation knew the ID before an acknowledgement was recorded, receipt repair validates the existing native record and cannot create a new run.

**1,872 connected checks pass**, including 21 tracking worker/manifest cases, 52 API authority cases and all 78 legacy tracking writer/lease/recovery/HTTP/process-loss cases, with no failures/errors/skips. Four original test-body failures establish the worker SQL gap. Five scoped gates pass re-verification over562 frozen inputs; repository lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. [Evidence](evidence/phase-1/training-tracking-rpc-2026-10-24.yaml).

The real local TLS/k3d probe uses separate pending parent/candidate Pod identities and three distinct bound tokens. With worker SQL and the inline creation SDK disabled, it creates one parent, one candidate and one failure tracking run, recovers four lost HTTP acknowledgements by GET, rejects a nonpublisher parent and replays the original parent without another creation. MLflow uses a real owned SQLite store through its SQLAlchemy SDK; tracking HTTP/provider and maintained-image qualification are not claimed by this probe. Both Pods never execute containers. Exact namespace/SQL/store resources and secret files are retired and read back absent. The first probe passed tracking operations but failed harness cleanup on a version-specific cache attribute; its exact resources were retired separately, cleanup was corrected, and only the later full probe counts as passing evidence.

All36 requirements remain **2 Verified / 34 Open**. Direct worker MLflow logging/resume and data/model readers still require conversion before broad credentials can be withdrawn. Final/candidate tracking references also still need binding to the exact server tracking receipts. All-stage native permissions, remaining custody/retirement, current full/release/coverage, maintained-image/mixed-version and full-pipeline process-loss qualification remain required. No45GB or15×10GiB scale claim is made; the43.5GB free reserve remains enforced. Original abandoned phase1-deletion:G7 remains a required handoff for seven missing historical QA objects, without reconstructed or re-signed replacements.


### 2026-10-24 — tracking references bound to model custody

Model reservations under saved v8 policy now bind the original candidate tracking receipt and producer Pod. Partial/final API and SQL publication, stored-result replay and explicitly allowlisted predecessor recovery verify that original custody and the exact parent tracking ID. Missing historical receipts require controlled recovery. Validation precedes pending/terminal snapshot restoration, so merging cannot hide an invalid incoming tracking ID. No migration, dependency, configuration or protocol version changed; all 27 captured manifests remain unchanged.

**1,897 connected checks pass**, including 25 tracking-reference cases, with no failures/errors/skips. The three genuine original negative controls remain preserved. Five scoped gates were reverified over 564 frozen inputs; lint, changed-hunk formatting, strict0018 and the three CI preflights pass. [Evidence](evidence/phase-1/training-tracking-references-2026-10-24.yaml).

The local verified-TLS/k3d test used separate pending parent/writer Pods and three bound tokens, two real MLflow SDK tracking records and one conditional model-storage fixture write. Worker SQL was disabled. Missing/forged tracking references failed, lost model/progress/result acknowledgements recovered through GET, and completion retained one metric and terminal CAS. Exact owned namespace/SQL/store resources and secret files were retired. Native model providers, tracking HTTP, maintained images and complete worker isolation are not qualified by this probe.

Phase 1 remains **2 Verified / 34 Open**. Direct worker MLflow logging/resume/registry artifacts and data/model readers still prevent broad credential withdrawal. Current full/branch coverage, remaining native custody/retirement and runtime qualification remain required. No45GB or15×10GiB scale qualification was attempted; the43.5GB reserve remains enforced. Original abandoned phase1-deletion:G7 still requires seven original historical QA objects; none were reconstructed or re-signed.


### 2026-10-24 — Pod-owned training metadata and retirement

New saved v9 training routes ordinary parent/candidate/failure tags, parameters, metrics and parent mirrors through authenticated control. Immutable request slots bind original tracking custody and source before native effects. Each synchronous native batch has its own completion marker; uncertainty permits GET only. Pre-existing tag values cannot acknowledge a pending writer, and a missing marker retains the retirement barrier. Read-only background inspection can settle marked effects after cancellation or actual API process death, without another write or a worker acknowledgement. Malformed rows defer fairly.

V9 brokered model writes and final publication now require original metadata readiness. Model slots, explicit predecessor references and final-result links preserve the exact proof. Missing or altered history fails closed. V8 model custody retains its original policy, and all27 captured old manifests remain unchanged. No migration, dependency or threshold changed.

**2,159 distinct connected cases pass in composed evidence**, including 37 metadata worker, 51 API and 15 retirement cases, with no remaining failures/errors/skips. The full run passed2,158 cases and exposed one SQL fault-injection harness error; changing only that test hook from before_commit to before_flush made its unchanged assertions pass in a focused rerun against a terminated PostgreSQL backend. Production inputs and every other test body remain identical. Seven scoped gates were reverified over570 frozen inputs; lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. Seven genuine negative controls are retained across worker metadata, numeric overflow and missing readiness. The first two retirement failure artifacts were accidentally overwritten; they are explicitly excluded from retained automatic negative evidence. Current retirement controls pass. [Evidence](evidence/phase-1/training-tracking-metadata-2026-10-24.yaml).

The real local TLS/k3d probe used separate pending parent/writer Pods and three bound tokens. With worker SQL and ordinary metadata SDK functions disabled, three real MLflow SDK tracking records received nine metadata batches and nine completion markers; HTTP acknowledgement loss recovered by GET. Model storage remains a conditional in-memory fixture. One model write, SQL metric and terminal CAS were retained. Exact owned namespace/SQL/store resources and secret files were retired. The probe does not qualify native model providers, tracking HTTP, maintained images or full worker isolation.

Phase1 remains **2 Verified / 34 Open**. Native run context, diagnostic/model artifacts, registry operations, data/model readers, remaining custody/retirement and current full/runtime/coverage qualification remain required. No45GB or15×10GiB scale claim is made; the43.5GB reserve is enforced. Original abandoned phase1-deletion:G7 still requires seven original historical QA objects, without reconstructed or re-signed replacements.


### 2026-10-24 — authenticated model restoration

New saved v10 training heads restore published candidate models through the existing short-lived GET broker. The server selects the original verified model version; only the current parent Pod can read it. Replacement attempts require explicit predecessor custody and unchanged inputs. Native signing holds no caller SQL connection, uncertain or replayed requests cannot remint a URL, and the original read deadline remains a retirement barrier. Workers verify complete bounded bytes and SHA-256 before deserialization, with storage access disabled. No migration, dependency or broad credential withdrawal was introduced; all 30 captured legacy/v1–v9 manifests are unchanged.

**868 connected checks pass**, including 41 model reader worker/manifest cases and 26 PostgreSQL authority cases, with no failures/errors/skips, over 575 frozen inputs. Four scoped unlazy gates passed re-verification. Lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. The original storage-access negative control remains preserved. Earlier role-selection and fixture-cleanup failures are retained; the exact failed fixture resources were retired. [Evidence](evidence/phase-1/training-model-read-2026-10-24.yaml).

The local verified-TLS/k3d probe used separate pending parent/writer Pods and five distinct bound tokens. One exact-version model was streamed and restored with worker storage/SQL disabled; the writer Pod was denied. Owned namespace/SQL/store/secrets were retired and read back absent. Model storage and HTTPS serving are conditional fixtures; native providers, maintained images and full worker isolation are not qualified.

Phase 1 remains **2 Verified / 34 Open**. Training dataset readers, Ray Tune checkpoint storage, native MLflow context/artifacts/registry, remaining custody/retirement and current full/runtime/branch-coverage qualification remain required. The 45 GB database ceiling is recorded; no 45 GB or 15×10 GiB scale claim is made and the 43.5 GB reserve is preserved. Original abandoned phase1-deletion:G7 still requires seven original historical QA objects; none were reconstructed or re-signed.


### 2026-10-24 — registry inspection outside SQL

Background registry verification now uses the factory-owned dispatcher, releasing its connection and locks before storage existence/size reads. Fresh project, command, registry, run, artifact and delivery snapshots fence publication afterward. Only the exact live delivery can record a result or retry; an old reader cannot change a successor even when both use the same worker name. Verification, delivery and command completion commit atomically, and verified replay performs no storage reads. The old transaction-owned route rejects this topic.

**269 connected checks pass**, including 27 focused PostgreSQL cases over 577 frozen inputs, with no failures/errors/skips. Three scoped unlazy gates passed re-verification. Real process termination, backend connection loss, lost commit acknowledgement, concurrent readers and committed source changes are covered. The original negative control recorded a checked-out connection; its unchanged body now records zero for both native reads. Lint, changed-hunk format, strict schema 0018 and three CI preflights pass. [Evidence](evidence/phase-1/registry-inspection-2026-10-24.yaml).

This preserves the existing existence/size check. It does not qualify native content hashes, immutable versions or producer custody. The HTTP registration path still has its initial size read under SQL and remains required work. No dependency, migration, credential protocol or coverage threshold changed; no current native-provider or scale claim is made.

Phase 1 remains **2 Verified / 34 Open**. Remaining worker credential conversion, native custody/retirement, current full/runtime/coverage and 45 GB/load qualification remain required. The 43.5 GB reserve is preserved. Original abandoned phase1-deletion:G7 still requires seven original historical QA objects; none were reconstructed or re-signed.


### 2026-10-24 — registry HTTP registration outside SQL

HTTP registration now commits a read-only selected-model snapshot before the initial storage-size read, releasing both its connection and admission mutex. A short final transaction rechecks active user/token, current project/membership, deletion barriers and exact candidate/data/schema metadata before publication. Existing registrations replay without storage access. Concurrent requests share one canonical registry entry, artifact and version; unrelated sibling progress or parent completion does not reject an unchanged successful selected model.

**288 connected checks pass**, including 26 focused HTTP/PostgreSQL registration cases and 27 background-inspection cases, over 578 frozen inputs, with no failures/errors/skips. Three scoped unlazy gates passed re-verification. Real backend termination, lost commit acknowledgement, concurrent registration and authority/source changes during reads are covered. The unchanged negative-control body now observes zero caller connections; all original selected-model persistence assertions remain. Lint, changed-hunk format, strict schema 0018 and three CI preflights pass. [Evidence](evidence/phase-1/registry-registration-2026-10-24.yaml).

This closes the initial-read transaction gap recorded in the previous registry checkpoint. Visibility and size still do not attest native content hash, immutable version or producer custody. No migration, dependency, credential protocol or public request schema changed, and no native-provider/image/scale qualification is claimed.

Phase 1 remains **2 Verified / 34 Open**. Remaining worker credential conversion, native custody/retirement, current full/runtime/coverage and 45 GB/load qualification remain required. The 43.5 GB reserve is preserved. Original abandoned phase1-deletion:G7 still requires seven original historical QA objects; none were reconstructed or re-signed.

### 2026-10-24 — original model custody and pinned registry integrity

New registrations and background verification now require the original model producer reference and receipts, verify the original immutable version's full SHA-256 and exact size outside SQL, and revalidate custody before publication. Replay rejects changed history, numeric receipt rewrites and legacy verified flags without proof. Existing models without original receipts require controlled qualification (HTTP 409); no history is manufactured or rewritten.

**443 connected checks pass**, including 19 custody, 26 HTTP registration and 32 background cases on 581 frozen inputs, with no failures/errors/skips. Five scoped unlazy gates passed re-verification. The two original failing control bodies now reject missing custody and corrupt bytes. Actual backend/process loss, lost acknowledgements, concurrent publication and original business assertions remain covered. Lint, changed-hunk format, strict schema 0018 and three CI preflights pass. An actual TLS S3-compatible test on the owned local k3d fixture conditionally wrote one model, rejected an overwrite, changed latest and registered the original version; both new versions were retired and all five preserved versions stayed unchanged. [Evidence](evidence/phase-1/registry-custody-2026-10-24.yaml).

This does not qualify cloud IAM, real Pod TokenReview in this storage probe, maintained images, all downstream model readers or the full historical input graph. Legacy qualification, remaining worker credentials/native mutations and retirement, current full/branch >=90.01% checks, complete native journeys, the 45 GB migration and deferred 15-by-10-GiB load remain required. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified / 34 Open.**

### 2026-10-24 — pinned model delivery to deployments

New deployments bind the original verified registry model digest. Restart and authenticated download reject missing/changed bindings and original receipts. Downloads release SQL before reading the pinned native version, verify the complete hash and size in bounded chunks through a temporary spool, then revalidate current deployment/project/proof and deletion barriers before returning bytes. Failures and stale reads disclose no model bytes; temporary files close on failure and normal delivery. Legacy deployments require controlled qualification rather than reconstructed bindings.

**272 connected checks pass**, including 34 focused serving-custody, 19 registry-custody, 26 HTTP-registration and 32 background cases on 582 frozen inputs, without failures/errors/skips. Four scoped unlazy gates passed re-verification. All seven retained negative controls now pass unchanged, including stale cached authority and malformed credentials; original serving-security and deployment lifecycle assertions are preserved. Lint, changed-hunk format, strict schema 0018 and three CI preflights pass. A real TLS S3-compatible local k3d probe serves the original registered version after latest changes and rejects a missing original without reading latest. Every owned native read holds zero SQL connections; both new versions and the port forward were retired and all five preserved versions remained unchanged. [Evidence](evidence/phase-1/serving-model-custody-2026-10-24.yaml).

The native probe uses real deployment HMAC authentication and fixture workload identity, not a deployed inference image or real Pod TokenReview. Existing running processes, legacy bindings, static deployment credentials, standalone inference fallback/materialization, remaining worker credentials, deployment/restart mutation custody and SQL/I-O separation, current full/branch >=90.01% checks, maintained images/cloud/native journeys, the 45 GB migration and deferred 15-by-10-GiB load remain required. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified / 34 Open.**

### 2026-10-25 — deployment Dockerfile native custody

New deployment admissions record the Dockerfile protocol and use a versioned outbox topic that the captured predecessor dispatcher refuses. The factory-owned storage stage commits its original actor/resource/source-bound intent, releases SQL, creates conditionally and verifies complete bytes at the immutable native version. Uncertain replies use matching native metadata and pinned reads without another PUT. Original completion facts survive cancellation or lease loss, but changed source or delivery cannot authorize Kubernetes effects or acknowledge a successor. Unresolved writes block project/run and direct/queued artifact cleanup.

**426 connected checks pass**, including 32 focused cases on 584 frozen inputs, with no failures/errors/skips. Five scoped unlazy gates passed re-verification. The two retained negative-control bodies now pass unchanged. Real PostgreSQL backend termination after native acceptance, lost acknowledgements, later Kubernetes failure, source/command drift, successor claims, cancellation and malformed history are covered. Lint, changed-hunk format, strict schema 0018 and three CI preflights pass. A real local TLS S3-compatible probe conditionally writes the Dockerfile and recovers an intentionally withheld acknowledgement by metadata and pinned full-byte reads, with zero SQL connections and no second PUT. Both new versions and the port forward were retired; all five preserved fixture versions stayed unchanged. [Evidence](evidence/phase-1/deployment-object-custody-2026-10-25.yaml).

This completes the storage stage only. Kubernetes creation still runs under SQL and needs UID/spec custody, partial-resource recovery and native qualification; lifecycle mutations remain required. Unknown/legacy history, current full/branch >=90.01% checks, broad worker credential withdrawal, maintained images, previous binaries/mixed versions/cloud providers, complete native journeys, the 45 GB migration and deferred 15-by-10-GiB load remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified / 34 Open.**

### 2026-10-25 — deployment creation conflict and failure safety

The shared Kubernetes creator now validates all requested targets before any POST. A conflict requires original ownership labels, namespace/name/kind/API version, a nonempty UID, no deletion timestamp and matching requested configuration. Later creation errors preserve partial resources instead of deleting by name; matching retries preserve their UIDs.

**333 connected checks pass**, including 39 focused cases on 585 frozen inputs, without failures/errors/skips. Four scoped unlazy gates passed re-verification. Both original failing control bodies pass unchanged. Lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. A real owned local k3d probe verifies matching/foreign conflicts and later failure using zero-replica resources, starts no Pods, then retires both resource UIDs and the owned namespace with UID preconditions. The shared fixture namespace is preserved. [Evidence](evidence/phase-1/deployment-create-safety-2026-10-25.yaml).

This is requested-field collision safety: extra dictionary fields used by server defaults remain permitted. Complete defaulted-spec/security-policy attestation, durable expected-UID and namespace-lifetime receipts, native creation outside SQL, partial acknowledgement recovery and restart/stop/cleanup remain required. Broad worker credential withdrawal, current full/branch >=90.01% qualification, maintained images, prior binaries/mixed versions/cloud providers, native journeys, the 45 GB migration and deferred 15-by-10-GiB campaign remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified / 34 Open.**

### 2026-10-25 — durable Kubernetes deployment submission

New admissions use the shared v3 topic. After Dockerfile custody, the reconciler commits original namespace/source/manifest identity and a separate one-shot start for each Deployment/Service/Ingress. Native calls release SQL. Original intent annotations, request digests, UID and full observed spec receipts support read-only acknowledgement recovery without another POST. Completion facts survive cancellation or lease takeover, while fresh authority gates further creation and exact observed receipts gate final publication. Pending listings stay progressing until launch commits; changed Dockerfile receipts, status or desired state cannot be overwritten. The unused SQL-bound helper was removed and SDK transport retries are explicitly disabled.

**328 connected checks pass**, including 31 focused cases on 587 frozen inputs, without failures/errors/skips. Four scoped unlazy gates passed re-verification. Both original controls and the additional publication-race and four caller-race controls pass unchanged. PostgreSQL backend death, missing/replaced resources, namespace drift, Ingress receipts, cancellation, successor delivery, replay and final receipt tampering are covered. Lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. An owned local k3d zero-replica probe recovers a withheld real creation acknowledgement, replays matching receipts without POSTs and rejects a replacement UID. Native calls hold zero SQL connections; all three explicitly created resource UIDs and the owned namespace were retired, no Pods started and the shared fixture namespace stayed unchanged. [Evidence](evidence/phase-1/deployment-submission-2026-10-25.yaml).

Namespace reads cannot condition a Kubernetes POST; admission enforcement remains required for the read/POST namespace-replacement window. Initial requested-field comparison permits server-default dictionary fields, while saved full observed specs are pinned on recovery. Uncertain missing outcomes stay blocked; controlled resolution, UID-bound restart/stop/cleanup and physical retirement remain required. Active native history deliberately blocks deletion until retirement is proven. Broad credentials/IAM, current full/branch>=90.01%, maintained images, actual prior binaries/mixed versions/cloud providers, complete native process-loss journeys, the 45 GB migration and deferred 15-by-10-GiB campaign remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified /34 Open.**

### 2026-10-25 — original deployment identity and retirement

Stop, restart and cleanup now require original native submission custody. Scale patches test UID and current resourceVersion; cleanup preflights every original target and uses UID-bound foreground deletion. Acknowledgement alone cannot mark cleanup complete: original resources and labeled ReplicaSets/Pods must be absent in the original namespace, then a canonical receipt binds retirement to the original journal and resource UIDs before the deletion barrier lifts. Missing/coerced/misbound evidence keeps the barrier. Known unstarted modern launches remain cancellable without native calls; unknown/legacy histories require controlled qualification.

**275 connected checks pass**, including 50 focused cases on 588 frozen inputs, without failures/errors/skips. Four scoped unlazy gates passed re-verification. All three original failing control bodies pass unchanged. Actual PostgreSQL backend termination after native deletion recovers without another DELETE. Lint, changed-hunk formatting, strict schema 0018 and three CI preflights pass. An owned zero-replica k3d probe verifies a UID/resourceVersion patch, rejects a stale version with HTTP 422, proves original retirement and rejects same-name replacements for all three actions. All four explicitly created resource UIDs and the owned namespace were retired, no Pods started and the shared fixture namespace stayed unchanged. [Evidence](evidence/phase-1/deployment-lifecycle-identity-2026-10-25.yaml).

Native control calls still hold the caller SQL transaction. Factory-owned control admission/dispatch, durable control generations, complete ordering/process-loss and observer qualification remain required; this increment does not qualify the whole lifecycle. The native probe never scales above zero or executes a maintained inference image. Namespace admission, legacy/partial-history qualification, broad credentials/IAM, current full/branch>=90.01%, maintained images, actual prior binaries/mixed versions/cloud providers, complete native journeys, the 45 GB migration and deferred 15-by-10-GiB campaign remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified /34 Open.**

### 2026-10-25 — durable deployment controls

Stop/start/cleanup now admit SQL-only commands and dispatch native work outside SQL through `deployment.control.v1`. Original custody, exact delivery and control generations fence publication. UID/resourceVersion JSON patches always advance native generation, including no-op scales; accepted-write acknowledgement loss is observed without another patch. Cleanup blocks restart, uses UID/resourceVersion deletes, waits for physical retirement and then records original proof. Pending controls stay pending in the deployment list and UI, with retry actions for observed failures.

**273 connected checks pass**, including 22 focused cases on 590 frozen inputs, without failures/errors/skips. The three original failing admission bodies pass unchanged. Tests include concurrent admission, newer/no-op generations, same/different owner takeover, actual PostgreSQL backend death, revoked restart actors and terminating cleanup. UI tests/types/lint, repository lint, changed-hunk formatting, strict0018, captured predecessor refusal and three preflights pass. An owned zero-replica k3d dispatcher probe recovers a withheld native reply without another patch, rejects stale patches/deletes, retires both original resource UIDs and its namespace, and observes no Pods or SQL held during native calls. All four scoped unlazy gates were freshly reverified. [Evidence](evidence/phase-1/deployment-control-workflows-2026-10-25.yaml).

Qualified controls require delivered original v3 creation receipts; legacy/partial/unknown producer histories remain unqualified. Active observers still need SQL separation. Namespace admission, broad credentials/IAM, current full/branch>=90.01%, maintained images, actual prior binaries/mixed versions/cloud providers, complete native journeys, the 45 GB migration and deferred 15-by-10-GiB campaign remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified /34 Open.**

### 2026-10-25 — original deployment observations outside SQL

Deployment status listing now uses the existing two-pass request pattern: commit original SQL selection, inspect native resources outside SQL, then revalidate current authentication/permission and unchanged source/control before publishing. Original namespace/resource UIDs and specs, completed control generation and ReplicaSet/Pod lineage bind each read. Observation revisions prevent an older ready result from overwriting a newer control or observation. Pending creation/control histories stay protected, and unverified URLs are withheld. Name-only deployment readers have been removed from production.

**211 connected checks pass**, including 37 focused cases on 626 frozen code/chart/dependency inputs, without failures/errors/skips. Two unchanged baseline bodies now pass; current coverage includes source/reader revocation, stale/concurrent publication, native ownership/readiness and the real paginated route. All35 Helm contracts,22 native unit cases, repository lint, changed-hunk formatting, strict0018 and three preflights pass. Helm withdraws API model writes, grants the reconciler its required Deployment patch and restricts namespace metadata reads to the release namespace. Original Ingress reads/retirement survive feature disablement. An owned zero-replica k3d probe uses real temporary service-account tokens:16 API model-write denials, another namespace denied, an original-UID/resourceVersion reconciler patch allowed, original native observation and same-name replacement refusal. Three model resource UIDs, eight RBAC object UIDs and the owned namespace were retired; no Pods started and native calls held no SQL connections. All four scoped unlazy gates were freshly reverified. [Evidence](evidence/phase-1/deployment-observers-2026-10-25.yaml).

Creation namespace admission and legacy/partial/unknown histories, inference proxy target identity/admission, other workload/native I/O paths, complete credentials/project/stage/cloud IAM controls, current full/branch>=90.01%, maintained images, prior binaries/mixed versions, positive inference and full native journeys remain Open. The 45 GB migration and deferred 15-by-10-GiB campaign were not run locally. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified /34 Open.**

### 2026-10-25 — inference pod identity (in progress)

Pod certificate admission, permanent key binding, verified pod-authorized model downloads and a renewing mTLS worker runtime have been added. Current frozen inputs pass **69 issuer/model-admission checks, 86 connected runtime/serving checks and 45 worker-version checks**; these suites overlap. Real local listeners exercise client-certificate enforcement, a health-only plain port and expiry withdrawal. Native issuer fixtures use real PostgreSQL with controlled Kubernetes/object responses. Repository lint and read-only strict schema 0018 checks pass; original coverage and the 43.5 GB reserve are preserved. [Progress evidence](evidence/phase-1/inference-peer-identity-progress-2026-10-25.yaml).

**This is not an integrated production fix yet.** Normal deployment production still emits legacy v3/HMAC manifests, and the gateway still uses its old name-only plaintext target. Both unchanged proxy security tests still fail. Producer/predecessor/retirement integration, ordinary/docs/offline proxy conversion, Dockerfile/CA/Helm/network wiring and the owned k3d campaign remain unfinished. All eight scope gates remain unmet; no maintained-image, full release, 45 GB/load or full native/provider qualification is claimed. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains incomplete: 2 Verified / 34 Open.**

### 2026-10-25 — TLS deployment producer and authority wiring (in progress)

New admissions select `deployment.reconcile.v4` and generate the pod-token/TLS runtime, separate data/health ports and writable private-key volume. A shared declaration check protects both native submission and certificate selection. Helm references an existing serving CA Secret, mounts it only into the API container and requires the correct HTTPS control path. [Operator setup](inference-pod-identity.md). TLS deployments withhold unusable Service DNS URLs; viewers use the platform Gateway.

On **638 frozen inputs**, **197 producer, 90 connected admission/lifecycle, 45 Helm/authority-file and 45 worker checks pass**, plus two captured-source compatibility cases. Counts overlap. Certificate/model admission now uses the real producer and SQL selection. The compatibility replay rejects v4 with captured v3 source and retires a fresh v3 fixture journal without rewriting it; this is controlled source/SDK replay, not a prior binary or native rollout. CA provisioning commands, repository lint and read-only strict schema0018 checks pass. A read-only k3d preflight confirms the preserved namespace/service UIDs. [Progress evidence](evidence/phase-1/inference-serving-producer-progress-2026-10-25.yaml).

**The inference path remains unfinished.** Both original proxy security tests still fail. Ordinary/docs/offline proxy admission must be converted, and upload completion must preserve and verify the original native object version instead of reading latest. The owned pending-pod campaign and maintained images are not qualified. All eight scope gates remain unmet; full release/branch coverage, native/provider/IAM/admission journeys, the 45 GB migration, deferred load qualification and original deletion:G7 missing-object handoff remain Open. **Phase 1 remains incomplete: 2 Verified / 34 Open.**


### 2026-10-26 — authenticated inference gateway and pending-Pod qualification

The ordinary/docs/browser-multipart gateway now selects and revalidates original Pod custody, uses a pinned numeric Pod IP with UID-bound TLS SNI and mTLS, and releases SQL before native inspection and request/response streaming. Current tests cover foreign-peer refusal before headers or body consumption, reader/source revocation, lock ordering and cleanup after a browser disconnect. Native qualification exposed a producer default mismatch: Kubernetes omits the writable mount's redundant `readOnly: false`; the producer now omits it while retaining an explicitly read-only token mount and strict ownership checks.

On **638 frozen inputs**, **130 gateway/identity checks, 197 producer checks and 45 worker checks pass**, plus the corrected new-model credential-denial case and one native campaign; counts overlap. The native campaign uses real chart API service-account permissions and a single unschedulable Pending Pod with `imagePullPolicy: Never`. It admits/renews the original key, refuses a replacement key and refuses issuance after scale-to-zero plus original Pod removal. Ten owned resource UIDs, the Pod/ReplicaSet and namespace are confirmed retired; native calls hold zero SQL connections and shared fixture UIDs remain unchanged. Failed pilots and their cleanup evidence are retained. Repository lint, read-only strict schema0018 and original coverage preservation pass. [Current progress evidence](evidence/phase-1/inference-gateway-progress-2026-10-26.yaml).

**The inference path and Phase 1 remain incomplete.** JSON object-backed offline inference still needs original provider completion receipts, pinned object verification and SQL-free authenticated streaming. The native result does not isolate TokenReview revocation latency or qualify a running maintained image, deployed HTTP issuer, cloud IAM or network policy. All eight scope gates remain unmet pending complete integration/audit. Full release/branch coverage, prior binaries/mixed versions and complete native/provider/admission journeys remain Open. The user's qualification ceiling is recorded as **45,000,000,000 physical bytes**; that migration and deferred15-by-10-GiB load were not run, and largest-table row counts remain unspecified. The43.5GB reserve is preserved. Original deletion:G7 still requires seven missing historical objects. **Phase 1 remains at 2 Verified / 34 Open.**


### 2026-10-26 — original provider completion receipts; API handoff remains open

S3/Azure completion now keeps the immutable version from the original create-only completion response instead of choosing a version from latest-object metadata. S3 requires its single-attempt SDK configuration; Azure disables per-request retries. GCS creates a conditional session without hidden retries and validates a bounded original-session completion resource. Azure progress also now handles the actual SDK's tuple return and decoded block IDs; completion encodes those IDs exactly once.

On **639 frozen inputs**, **128 provider/storage checks and 254 connected upload/storage checks pass**, including **12 real-SDK local HTTP cases** covering conditional requests, original response versions and response loss. Four pre-fix replacement cases are retained. Local SDK fixtures are protocol tests, not cloud-provider/IAM qualification. Repository lint, read-only strict schema0018, original coverage and the43.5GB reserve are preserved. [Provider progress evidence](evidence/phase-1/upload-completion-provider-progress-2026-10-26.yaml).

**The completion-to-inference chain is unfinished.** The API completion helper still adopts latest-object recovery, drops version receipts and holds SQL across provider calls. Passing its existing regression tests does not qualify that behavior. Factory-owned original intent/receipt custody, response-loss/concurrency/source fences, pinned verification ranges and SQL-free authenticated offline streaming must reach their callers together. Completion acknowledgements are not exact-version byte/SHA/scanner proof. All six upload scope gates and eight parent inference gates remain unmet; no Phase1 requirement is promoted. Full release/branch coverage, maintained images, native/cloud/IAM/admission journeys, the45GB migration, deferred15-by-10-GiB campaign and original deletion:G7 seven-object handoff remain Open. **Phase1 remains at 2 Verified /34 Open.**


### 2026-10-26 — durable completion custody; pinned verifier remains unfinished

The API completion route now releases its authentication SQL session and uses a factory-owned service. Separate short transactions retain original admission, native start, receipt and publication. The unsafe direct-native completion helper and latest-object recovery branch are removed. Known native versions survive caller revocation or publication rollback; fresh caller/project/source checks still precede acknowledgement publication. Unknown S3/Azure outcomes never reissue a commit or adopt latest. GCS recovery reads only its original session. Independent start/command anchors hold missing journals against replay, abort and expiry cleanup.

On **641 frozen inputs**, **74 workflow/replay/route checks, 216 connected checks and 128 provider checks pass**. The workflow suite includes61 real PostgreSQL cases, the actual completion route, concurrent same/different-key requests, native response loss, publication rollback, source/receipt tampering and original-result replay. Forty malformed-manifest cases moved into real SQL coverage; eight forbidden null cases assert the unchanged NOT NULL constraints. The new service and shared idempotency helper also now preserve Pydantic response aliases, with known old field-name responses still readable. Lint, strict read-only schema0018, original coverage and the43.5GB reserve remain preserved. [Workflow progress evidence](evidence/phase-1/upload-completion-workflow-progress-2026-10-26.yaml).

**Readiness is still blocked by unfinished verifier integration.** The new `upload.reconcile.v2` topic is not yet dispatched to the old unpinned reader, so completion acknowledgements queue verification rather than proving readiness. Exact-version size metadata, pinned ranges/scans, source/lease revalidation, authenticated offline streaming and qualified version retirement remain required. Truncating reads at an expected size is insufficient. Old upload histories without original admission/receipts need controlled recovery or a new upload; held completion custody still needs a complete cancellation/retirement path. All six upload gates and eight parent inference gates remain unmet. Full release/branch coverage, maintained images, native/cloud/IAM/admission and prior-binary/mixed-version journeys,45GB/load qualification and original deletion:G7 seven-object handoff remain Open. **Phase1 remains at 2 Verified /34 Open.**


### 2026-10-26 — original-version upload verifier integrated

The new `upload.reconcile.v2` topic now reaches a factory-owned verifier. It checks metadata for the original receipt version before scanning, pins every range, releases SQL during all native/scanner work and revalidates original source, policy, project, dataset core and delivery attempt before readiness. Verification proof is anchored to the original command/outbox; completion replay remains valid. An older reader failure cannot fail a newer claim by the same worker. Legacy histories cannot infer versions or read latest. Cleanup holds unknown/completed custody instead of deleting by URI; Azure staged abort explicitly reports unsupported. Qualified version retirement is still required.

On **644 frozen inputs**, **105 verifier/reconciler, 94 metadata/storage, 128 provider, 74 completion/replay/route and 216 connected checks pass**, without failures/errors/skips; suites overlap. Three pre-fix metadata failures are retained. Real owned PostgreSQL verifies transaction/source fences and zero checked-out connections during controlled provider/scanner I/O. Actual SDK metadata boundary and local HTTP completion tests are separate qualification. Repository lint, read-only strict schema0018, original coverage, shared native fixture UIDs and the43.5GB reserve are preserved. [Verifier progress evidence](evidence/phase-1/upload-verification-progress-2026-10-26.yaml).

**The completion-to-inference chain remains unfinished.** JSON offline inference still needs fresh reader/source checks after body parsing and lazy exact-version reads after authenticating the original TLS peer. Qualified retirement, broad dataset/training readers, complete integration audits, full release/branch coverage, maintained images, cloud/IAM/admission and prior-binary/mixed-version journeys remain Open. All six upload gates and eight parent gates remain unmet; no Phase1 requirement is promoted. The45GB migration and deferred15-by-10-GiB load are unqualified. Original deletion:G7 remains a seven-missing-object handoff. **Phase1 remains 2 Verified /34 Open.**


### 2026-10-26 — pinned offline inference with post-body authority checks

The actual JSON offline route now rechecks current reader/token/project/model/control and original Pod-key binding after bounded body parsing. It shares the verifier's original completion/source/proof checks, requires original delivered verification and commits a frozen descriptor before provider construction. The exact version is opened lazily only after HTTPS authenticates the original peer. Body/provider/read/close work holds no SQL connection, and early stop/read/send failures close resources.

The positive consumer exposed a real persistence bug in the preceding verifier increment: outbox delivery refreshed the row before its pending verification digest was flushed. The verifier now flushes readiness/proof/anchor in the same transaction before delivery refresh. The earlier report's durable-anchor claim was not proved by its component tests; this increment corrects that claim and retains the failing consumer evidence. No historical measurement or missing object was rewritten.

On **644 frozen inputs**, **74 offline/gateway/TLS, 116 verifier/reconciler, 74 completion/replay, 94 metadata, 128 provider and 216 connected checks pass**, without failures/errors/skips; suites overlap. A newly completed and verified input traverses the real route and local mTLS server using `original-v1`; a foreign Pod certificate is refused before object open/read. Nine authority/source/proof/policy changes during JSON parsing all refuse before provider construction. Ten additional SQL descriptor-tamper cases, a positive descriptor read and early-stop/read-error cleanup pass. Lint, strict read-only schema0018, original coverage, shared fixture UIDs and the43.5GB reserve are preserved. [Offline progress evidence](evidence/phase-1/upload-offline-inference-progress-2026-10-26.yaml).

**Qualified original-version retirement/cancellation and full integration audits remain unfinished.** Unknown/completed custody stays held, Azure staged abort is unsupported and broad dataset/training readers are not all qualified. Real PostgreSQL/local mTLS/controlled provider streams and separate SDK tests do not qualify a deployed maintained image or a full cloud/IAM/admission journey. The whole identity/release suite was not rerun in this increment. All six upload and eight parent scope gates remain unmet. Full release/branch coverage, images, prior binaries/mixed versions,45GB migration and deferred15-by-10-GiB load remain Open. Original deletion:G7 still requires seven missing historical objects. **Phase1 remains2 Verified/34 Open.**


### 2026-10-26 — exact-version native retirement; cancellation callers remain open

Storage deletion now accepts an optional original immutable version across the contract and native drivers. S3/S3-compatible, Azure and GCS bind that identity, independently confirm its absence, preserve replacement/neighbor objects and recover accepted-write response loss without deleting again when the original is absent. Azure/GCS soft-deleted originals remain pending until physically absent. Embedded refuses versioned retirement. Existing broad project-deletion behavior is preserved. Malformed, nonadvancing or bounded-out inventories cannot claim absence.

On **645 frozen inputs**, **208 provider/storage, 116 verifier/reconciler, 74 completion/replay and 216 connected checks pass**, without failures/errors/skips; suites overlap. The provider profile includes **63 versioned-deletion cases and 14 actual-SDK local HTTP cases** across S3, S3-compatible, Azure and GCS. Three pre-fix unsupported-version cases and failed pilots are retained. The GCS fixture now distinguishes the installed SDK's background bucket-metadata requests from object reads; an intentional unversioned replacement read proves the pin detector still fails. Lint/format, strict read-only schema0018, original coverage, shared native fixture UIDs and the43.5GB reserve pass. [Native retirement progress evidence](evidence/phase-1/upload-version-retirement-provider-progress-2026-10-26.yaml).

**The new native operation is not yet wired to cancellation/cleanup.** Staged abort and expiry cleanup still need a factory-owned service to release SQL before provider work and persist original intent/outcome with fresh authority/source/retention/hold fences. Known completion custody and unknown histories stay held. No retirement workflow, cloud IAM deployment, maintained image, whole release/branch coverage, prior-binary/mixed-version or45GB/load qualification is claimed. All three retirement gates, six upload gates and eight parent gates remain unmet. Original deletion:G7 still requires seven missing historical objects. **Phase1 remains2 Verified/34 Open.**


### 2026-10-26 — native staged cancellation proof tightened

Tracing the cancellation service exposed two native prerequisites. S3 abort now checks the original multipart parts before and after the single-attempt abort; residual/truncated parts remain pending, and an already absent multipart ID does not trigger another abort. Only explicit `NoSuchUpload` counts as multipart absence; missing buckets, permissions and malformed/foreign inventories do not. GCS JSON cancellation now requires its documented499 acknowledgement;200/204/404/410 no longer qualify cancellation. A browser may already have completed a native GCS object, so missing session status cannot justify a false aborted/expired state.

On **646 frozen inputs**, **171 staged/provider, 210 storage/provider, 116 verifier/reconciler, 74 completion/replay and 216 connected checks pass**, without failures/errors/skips; suites overlap. The staged profile includes24 focused cases and4 real-S3-SDK local HTTP cases covering success, accepted disconnect,503 response loss and remaining parts. Six pre-fix failures and the original499 positive control are retained. The wire test also exposed a real `response=None` error-guard bug; its fix preserves transport exceptions in both staged and project/prefix cleanup. Lint/format, strict read-only schema0018, original coverage, shared native fixture UIDs and the43.5GB reserve pass. [Staged cancellation evidence](evidence/phase-1/upload-staged-cancellation-progress-2026-10-26.yaml).

**The factory-owned cancellation/cleanup service is still required.** Actual route/worker callers still hold SQL around provider work and do not have the required original retirement intent/outcome transactions. S3 multipart absence is not completed-object retirement; known completed objects need their original version receipt, and unknown/legacy histories must remain held. GCS needs original-session receipt recovery before exact-version retirement or an explicit unknown-outcome hold. Azure staged abort remains unsupported. All3 retirement,6 upload and8 parent gates remain unmet; no Phase1 requirement is promoted. Full release/branch coverage, images/cloud IAM/admission/prior binaries and45GB/load qualification remain Open. Original deletion:G7 remains a seven-missing-object handoff. **Phase1 remains2 Verified/34 Open.**


### 2026-10-26 — factory-owned upload cancellation and cleanup integrated

The actual abort route and cleanup worker now delegate to one factory-owned retirement service; the old native-in-transaction helpers are removed. Separate intent/start/known-outcome/publication transactions keep native work outside SQL and preserve original source/target/command/attempt custody. Fresh authority, project, retention/hold and dataset-reference checks precede native start and publication. Known outcomes survive revocation or publication rollback without a false success. GCS uses only its original session receipt to retire browser-completed data; unknown outcomes never adopt latest. New uploads retain original store URI, retiring uploads reserve quota, and pending verification deliveries stop after confirmed retirement.

A completion race found during review is fixed: cancellation can interrupt an admitted but unissued completion, and completion rechecks retirement before its native start. The browser now retries cancellation with a stable per-session key, preserves resume state on failure and cancels a session admitted after the user stopped.

On **648 frozen inputs**, **33 retirement workflow, 212 connected, 74 completion/replay, 116 verifier/reconciler, 28 worker, 210 provider and 171 staged/provider checks pass**, without failures/errors/skips; suites overlap. **60 browser/API/hash tests, Node24 type checking and targeted lint pass.** A real subprocess is killed after a controlled native-acceptance witness and committed intent; recovery observes the issued lease and original absence without another controlled write. Actual abort-route/cleanup-worker callbacks observe zero checked-out SQL connections. Corrupt custody, stale attempts, concurrent keys, revocation, native retention, GCS completion recovery and publication rollback are covered. Mock cleanup cases moved into real-SQL source/retention/batch tests, including all55 existing retention cases. Lint/format, strict read-only schema0018, original coverage, shared native fixture UIDs and the43.5GB reserve pass. [Workflow evidence](evidence/phase-1/upload-retirement-workflow-progress-2026-10-26.yaml).

**Acceptance audits and the wider Phase1 work remain unfinished.** Direct dataset-version retirement guard coverage, original-request/operator recovery, remaining upload begin/instruction/progress/native I/O and complete parent integration still need qualification. All3 retirement,6 upload and8 parent gates remain unmet; no requirement is promoted. Full release/branch coverage, maintained images, cloud/IAM/admission and prior-binary/mixed-version journeys,45GB migration and15-by-10-GiB load remain unqualified. Original deletion:G7 still requires seven missing historical objects. **Phase1 remains2 Verified/34 Open.**


### 2026-10-26 — upload retirement acceptance and Azure admission fix

The retirement acceptance review added twelve real-PostgreSQL dataset-version cases: eligible unready/unreferenced versions retire after none/past/equal retention; READY, legal hold, future retention and model references prevent provider construction; changes to those fences or the original URI during native work prevent publication while preserving the known outcome. A thirteenth regression exposed unsupported Azure staged cancellation admitting irreversible retirement before rejection. The shared service now rejects before command admission and provider construction, preserving the uploading record and its original history.

On the same **648 frozen inputs**, **46 retirement, 212 connected, 74 completion/workflow, 116 verifier/reconciler, 28 worker, 210 provider and 171 staged/provider checks pass**, with no failures/errors/skips. Counts overlap. **60 browser/API/hash tests, Node24 types and targeted lint pass.** The acceptance oracle checks current source bindings, terminal results, focused test-function coverage and known failing baselines; its self-check rejects stale/failed/running/under-reserve evidence. Repository lint, changed-file format, strict read-only schema0018/44 tables, original coverage, the43.5GB reserve and shared k3d fixture identities pass. [Acceptance evidence](evidence/phase-1/upload-retirement-acceptance-2026-10-26.yaml).

The three retirement gates passed; upload-completion remains **0/6**, parent inference **0/8**, and Phase1 **2 Verified /34 Open**. Broader upload begin/instruction/progress native-in-SQL paths and complete parent integration remain required. The user supplied a45GB qualification ceiling, recorded as45,000,000,000 physical bytes; largest-table row counts are unspecified. Production-sized migrations, the deferred15-by-10-GiB load campaign, maintained images, full release/branch coverage, mixed-version recovery and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — bounded S3 upload-admission inventory

The provider review found that begin retries read only the first multipart-inventory page and silently chose an identity when several matched. S3 now follows both markers through at most 100 pages of 1,000 entries, returns an exact-key identity only after complete inventory, and rejects ambiguous, malformed, foreign, nonadvancing or exhausted histories. Page permission/transport failures remain errors. New native IDs must be bounded strings with matching returned bucket/key; hidden SDK retries are rejected before listing or creation. This fixes provider lookup and receipt handling; it does not independently prove the original request payload.

The old provider failed **58 of 60** new checks. On **649 frozen inputs**, **78 focused, 63 SDK/provider, 178 service, 212 connected, 74 workflow, 116 verifier/reconciler, 28 worker, 210 provider and 171 staged checks pass**, with no failures/errors/skips. Suites overlap. Eight focused cases use the installed SDK against local HTTP for pagination, lost creation responses and ambiguous later pages. The neighbor positive checks retained provider state and no abort. An existing completion retry-refusal test now enables bad configuration after creating its positive session; its guard remains unchanged. The first pilot failure is retained. Strict read-only schema0018/44 tables, repository lint, five-file format, original coverage, the 43.5 GB reserve and shared k3d fixture identities pass. [Evidence](evidence/phase-1/upload-admission-provider-2026-10-26.yaml).

The three provider acceptance gates passed. Factory-owned begin admission, quota reservation before native creation, persisted original request/receipt recovery and SQL-free instruction/progress remain unfinished. Upload-completion stays **0/6**, parent inference **0/8**, Phase1 **2 Verified /34 Open**. UI source is unchanged; browser tests were not repeated for this Python-only increment. The 45 GB migration, deferred load campaign, maintained images, full release/branch coverage, mixed-version recovery and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — durable upload admission and policy-release integration (in progress)

The begin route now releases its authentication transaction and delegates to a factory-owned workflow. Project-first locking reserves quota and commits the original request/target/issued attempt before provider session creation outside SQL. Validated native replies are recorded independently before fresh actor/token/membership/project/source publication. Revocation, publication rollback, conflicting requests and stale attempts cannot replace the original result or silently create a second logical session. A committed intent nonce beneath the deterministic API session UUID prevents adoption of unrelated legacy or missing-history multipart state.

Store identity includes the configured driver and endpoint/bucket/region/account context without credentials; the supported minio alias and credential rotation remain compatible. Transfer/completion/retirement/verifier/offline consumers reject changed target context for new admissions. Published hold/retention changes are checked as live policy rather than immutable byte identity: releasing a hold, or waiting for an extended retention deadline, can retire the original object without rewriting its receipts. Original verification proof scopes remain stable after a legitimate policy release.

On **651 frozen inputs**, **50 admission workflow, 181 service, 212 connected, 74 completion/workflow, 117 verifier/reconciler, 28 worker, 63 SDK/provider, 210 provider, 171 staged/provider and 74 offline/TLS checks pass**, without failures/errors/skips. Counts overlap. Actual route and native callbacks observe zero checked-out SQL connections. A real SIGKILL after a controlled native-creation witness leaves committed quota/intent and recovers the original identity without another controlled write; separate installed-SDK/local-HTTP tests cover the wire contracts. No deployed cloud process-loss claim is made. Repository lint, twelve-file format, strict read-only schema0018/44 tables, original coverage, shared k3d fixture identities and the 43.5 GB reserve pass. [Admission progress evidence](evidence/phase-1/upload-admission-workflow-progress-2026-10-26.yaml).

**Admission remains 0/4 gates**, upload-completion **0/6**, parent inference **0/8**, Phase1 **2 Verified /34 Open**. Pending expiry, exhausted/unknown creation cleanup, background admission reconciliation and full acceptance remain required. Superseded-attempt receipts may alias the winner and must not be blindly aborted. Instruction/progress native I/O still spans SQL; their factory conversion must freshly validate original source/command/current actor. UI source is unchanged and its prior browser checkpoint is historical. The 45 GB migration, deferred load campaign, maintained images, full release/branch coverage, prior-binary/mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — unpublished admission expiry (in progress)

Unpublished admissions now retire logically after TTL or native retry exhaustion, with original command/source/receipt checks and proof that transfer authority was never published. Quota becomes reusable, old requests stay terminal, and late provider replies cannot reopen them. Bounded corrupt-row backoff allows neighboring cleanup work; legal hold/retention metadata and all native uncertainty remain preserved. Project deletion stays held on these records until native cleanup is qualified.

On 653 frozen inputs, 77 focused, 181 service, 212 connected, 74 workflow, 117 verification, 28 worker, 63 SDK, 210 provider, 171 staged and 74 offline/TLS checks pass without failures/errors/skips; counts overlap. Repository lint, seven-file format, diff checks, read-only schema0018/44-table parity, original coverage, shared k3d fixture UIDs and the 43.5GB reserve pass. [Expiry progress evidence](evidence/phase-1/upload-admission-expiry-progress-2026-10-26.yaml).

Admission remains **0/4**, upload-completion **0/6**, inference **0/8**; Phase1 remains **2 Verified /34 Open**. Native cleanup, background admission recovery and SQL-free instruction/progress remain required. The 45GB migration, deferred load, maintained images, full release/branch coverage, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — original multipart session cancellation (in progress)

S3 cancellation previously accepted a surviving empty session after abort. It now reports DeletionPending until the exact original session is absent. Two added controls reproduced the defect before the fix, including the installed SDK against local HTTP. A surviving session is then deliberately removed on a later call to verify recovery; exact native key/session arguments remain asserted.

On653 frozen inputs, all current profiles pass without failures/errors/skips: cancellation 26, focused 77, services 181, connected 212, workflow 74, verification 117, worker 28, sdk 63, provider 210, staged 173, offline 74. Counts overlap. Lint, changed-file format, diff, read-only schema0018/44 tables, original coverage, shared k3d fixture UIDs and43.5GB reserve pass. [Cancellation evidence](evidence/phase-1/upload-cancellation-session-2026-10-26.yaml).

All three cancellation gates pass. This is a cancellation prerequisite only. Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Native admission cleanup and SQL-free instruction/progress remain required.45GB migration, deferred load, maintained images, full release/branch coverage, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — qualified multipart absence status (in progress)

The shared S3 absence check and expired-session mapping now require both NoSuchUpload and an integerHTTP404. Contradictory permission/throttle/status or missing metadata cannot become cancellation success. Prefix cleanup preserves the original error and stops before deleting object versions. The old missing-status fixture now models a real404; every existing neighbor/replay assertion remains.43 baseline failures, including six installed-SDK/local HTTP fault controls, are corrected.

On653 frozen inputs the selected profiles pass without failures/errors/skips: cancellation 69, connected 560. Counts overlap. Lint, three-file format, diff, read-only schema0018/44 tables, original coverage, shared k3d fixture UIDs and43.5GB reserve pass. [Status-boundary evidence](evidence/phase-1/upload-cancellation-status-2026-10-26.yaml).

All three status-boundary gates pass. Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Native admission cleanup, background recovery and SQL-free instruction/progress remain required. UI/offline/TLS were not repeated for this S3 mutation/error increment; no full release qualification is claimed.45GB migration, deferred load, maintained images, full release/branch coverage, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — durable unpublished admission cleanup (in progress)

The actual worker previously left a known expired native session uncancelled. It now commits a separate cleanup command, original target/retirement/receipt binding and bounded lease before SDK construction/cancellation outside SQL. Native replies are retained before fresh source/policy publication, so changed policy/source or failed publication cannot force another cancellation of a known result. Corrupt history stays held; bounded backoff permits neighboring work, and native errors exhaust after five attempts with explicit controlled-recovery state.

On655 frozen inputs the current profiles pass without failures/errors/skips: focused 112, connected 560, worker 28, cancellation 69. Counts overlap. Two real SQL workers count one cleanup. Actual S3 SDK/local HTTP creation and cleanup preserve one native identity through disconnected responses and surviving empty sessions; actual GCS cancellation HTTP accepts499 and holds404 uncertain. Lint, changed-file format, diff, read-only schema0018/44tables, original coverage, shared k3d fixture UIDs and43.5GB reserve pass. [Cleanup progress evidence](evidence/phase-1/upload-admission-cleanup-progress-2026-10-26.yaml).

Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Original logical retirement still records native_absence_confirmed=false and project deletion remains held. Complete unknown/superseded/provider cleanup, authenticated deletion release, process-loss/operator replay, admission recovery and SQL-free instruction/progress remain required. UI/offline/TLS and full release/branch coverage were not requalified.45GB migration, deferred load, maintained images, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — authenticated admission cleanup releases project deletion (in progress)

Project deletion no longer stays blocked solely by a retired admission after original native cleanup is authenticated. The guard validates the original source/retirement/target and separately anchored succeeded cleanup command/outcome, without provider I/O. It runs at request and before runtime, object-store, prepared-store and metadata stages. Independent commands retain the barrier when upload JSON or rows are lost or relabeled. Current hold/retention/store context remains enforced; a flag or incomplete known reply cannot release deletion.

On656 frozen inputs the current profiles pass without failures/errors/skips: focused 155, deletion 200, connected 560, worker 28, cancellation 69. Counts overlap. The positive SQL journey replays intent and commits all four stages; fault controls preserve pending tombstones before effects. The first functional run exposed only an owned-fixture tombstone teardown error; scoped fixture cleanup and an exact owned repair fix it without weakening production foreign keys. Lint, changed-file format, diff, read-only schema0018/44tables, original coverage, shared native UIDs and43.5GB reserve pass. [Deletion-release progress evidence](evidence/phase-1/upload-admission-deletion-progress-2026-10-26.yaml).

Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Unknown/superseded/provider cleanup, process-loss/operator replay, original admission recovery and SQL-free instruction/progress remain required. Existing project deletion provider I/O still spans SQL. UI/offline/TLS/full release and independent branch coverage were not requalified.45GB migration, deferred load, maintained images, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-26 — native cleanup process loss and attempt reporting (in progress)

Admission and cleanup commands now report their actual claimed native attempt and existing five-attempt budget. Two baseline failures reproduced the missing count. Independently anchored attempt history remains authority: mutable reporting fields cannot expand retries, and legacy known outcomes remain read-only. Four actual SIGKILL windows and two actual owned PostgreSQL backend terminations around intent/native/receipt/publication commits recover the original S3-compatible session with one creation/abort; original admission remains unpublished. Six inspectable witnesses are retained.

On657 frozen inputs the current profiles pass without failures/errors/skips: focused 165, deletion 200, connected 560, worker 28, cancellation 69, process 6. Counts overlap; the six process cases repeat the focused cases only to retain their private witnesses. Native SQL/SDK/local HTTP assertions, source/XML/witness hashes, lint, changed-file format, diff, read-only schema0018/44tables, original coverage, native UIDs and43.5GB reserve pass. [Process-loss progress evidence](evidence/phase-1/upload-cleanup-process-loss-progress-2026-10-26.yaml).

Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Operator replay is still unimplemented. Full unknown/superseded/provider recovery, original admission reconciliation, instruction/progress and deletion SQL-I/O separation remain required. UI/offline/TLS/full release/branch coverage,45GB migration, deferred load, maintained images, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-27 — bounded owner replay of exhausted native cleanup (in progress)

The missing HTTP replay operation is implemented with fresh owner-level auth, current source/store/hold/project checks and Idempotency-Key/reason validation. Each separately anchored approval preserves the prior attempt/error and grants five more attempts for the same original native target. The background cleanup remains sole owner; API approval performs no provider I/O. Duplicate requests and concurrent keys cannot double-authorize, corrupted/orphan approval history blocks native work/deletion, and real approval-backend loss/lost HTTP acknowledgement preserve atomic recovery.

On658 frozen inputs all current profiles pass without failures/errors/skips: focused 199, deletion 200, connected 560, worker 28, cancellation 69, process 6. Counts overlap. Existing six actual process/backend failure windows pass again and witnesses are retained. Lint, changed-file format, diff, strict read-only0018/44tables, original coverage and shared native UIDs pass. A later disk-reserve shortfall was resolved by removing only identified public PyPI HTTP download cache, preserving installed packages/source/evidence; reserve43.5GB was rechecked. The operator procedure is documented. [Replay progress evidence](evidence/phase-1/upload-cleanup-replay-progress-2026-10-27.yaml).

Admission remains0/4, upload-completion0/6, inference0/8; Phase1 remains2Verified/34Open. Replay cannot recover unknown native identities or override targets/policy. Full unknown/superseded/provider recovery, original admission reconciliation, instruction/progress and deletion SQL-I/O separation remain required. UI/offline/TLS/full release/branch coverage,45GB migration, deferred load, maintained images, mixed-version and deployed cloud/native journeys remain unqualified. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-27 — deletion custody schema prerequisite

Migration 0019 gives deletion stages an internal JSONB object for durable native custody. Historical rows receive an empty object; existing completion flags acquire no fabricated receipts. Rollback locks the table before checking and refuses to erase any nonempty custody, including a concurrent writer's committed history. The historical 0010 fixture now reflects its original table for insertion and legacy-field comparisons. The rehearsal catalog includes all 18 prior revisions; a full 18-path campaign was not run in this increment.

The missing-field baseline and the historical fixture failure were reproduced before their fixes. Current frozen checks pass:14 schema,93 migration/verifier,200 deletion and28 worker cases. The schema suite proves committed project/command/outbox removal preserves stage custody and tombstones. The owned local PostgreSQL database expanded from 0018 to 0019 with unchanged row counts and historical deletion fingerprints, 44 tables and strict schema parity. Repository lint, changed-file formatting, original coverage hash, 43.5 GB reserve and read-only checks of preserved local k3d namespace/service identities pass. [Evidence](evidence/phase-1/deletion-custody-schema-2026-10-27.yaml).

This completes a schema prerequisite only. The deletion factory ledger remains 0/5 accepted gates, and Phase 1 remains 2 Verified / 34 Open. Native runtime/storage work still spans SQL; immutable original runtime UID custody, separately committed replies, fresh publication, metadata proof enforcement, factory dispatch/callers and process-loss qualification remain required. The 45 GB ceiling is recorded as 45,000,000,000 bytes; largest-table row counts remain unspecified, and that scale does not fit locally alongside the reserve. Deferred 15 users × 10 GiB load, maintained images, cloud/native journeys, mixed-version and full release/branch coverage remain unqualified. Original deletion:G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


### 2026-10-27 — known Ray submission identity prerequisite

Training, splitter and preparation now append the successful native create UID, name and namespace to the existing workflow-event history, with the original outbox ID and request/manifest fingerprints. Receipt and logical submission commit together. Existing evidence on a claimable attempt prevents native reissuance; submitted replay keeps the original event. The reader refreshes cached evidence and rejects changed source scope, request, fence or receipt. No token bytes or manifest bodies are stored in this event.

The missing-receipt baseline failed for all three stages. A second baseline demonstrated that the old native client adopted a replacement UID after an HTTP 409 despite an identical manifest; conflicts now propagate without GET/adoption. The stale-cache defect was also reproduced for all three stages before the reader refresh fix. Current evidence covers 60 focused checks, including 30 committed PostgreSQL cases, and 460 connected checks on 669 frozen inputs. Commit/rollback, lost commit acknowledgements, replay, existing-event sequence preservation and source corruption are covered. [Evidence](evidence/phase-1/ray-submission-identity-2026-10-27.yaml).

An actual local k3d probe used a unique owned namespace and suspended RayJobs with image pulling disabled. A real native create produced the committed UID receipt; a conflict was held; a same-name replacement survived deletion attempted with the old UID; and replay kept the original receipt. Three Pod inventory snapshots and RayCluster inventories were empty. These snapshots do not qualify full workloads or continuous observation. The owned namespace was removed normally with its UID precondition, and the owned SQL tenant was removed. Strict schema parity at head 0019, preserved provider namespace/service UIDs, the original coverage hash, repository lint and the 43.5 GB reserve pass. Prior schema evidence hashes remain intact.

This is a prerequisite, not completed cleanup. Both native creation and deletion still need factory ownership and SQL separation; actual cleanup must consume the original receipt and retain stage history after source erasure. Unknown-create/conflict recovery and legacy attempts remain unqualified. Receipt gates remain 0/3 accepted and the parent deletion factory gates remain 0/5 accepted. Phase 1 remains 2 Verified / 34 Open. The 45 GB migration ceiling, deferred load, maintained images, cloud/runtime/process-loss journeys, mixed-version and full release/branch coverage remain open. Original deletion:G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


### 2026-10-27 — factory-owned project/run deletion

The worker now dispatches project/run deletion through a session factory. Native runtime/store construction and effects release SQL. The shared current policy checks remain in use; original command/source/stage/store bindings commit before effects, known native facts commit separately, and a fresh delivery/source/policy check controls publication. Runtime cleanup consumes original creation or qualified retirement receipts, checks namespace/UID and ownership labels, uses UID delete preconditions and checks descendant inventories. Native prefixes have read-only absence inspection across embedded, S3, GCS and Azure adapters. Unexpired native leases cannot be stolen; uncertain deletes inspect first after expiry and may repair only the original immutable target under writer-retirement barriers. Known absence avoids another delete. Late facts cannot acknowledge a successor delivery.

Metadata removal requires published prior-stage proof and leaves stage/tombstone history intact after project/command/outbox removal. Legacy completion flags and deployment cleanup timestamps cannot substitute for receipts. Deployment retirement summaries omit manifest/secret bodies. Existing callers and fixtures now use committed factory calls with precise owned-row teardown. The original SQL-overlap baseline was reproduced before conversion.

Current evidence covers 30 focused, 225 deletion, 217 connected and 96 legacy/provider checks on 673 frozen inputs. The focused controls include two actual SIGKILL windows, two terminated owned PostgreSQL backend windows, known-fact recovery after policy changes, live native lease contention, late-fact rejection, historical custody preservation, ownership-label rejection and provider inventory/retention controls. An actual suspended k3d RayJob factory probe rejected a same-name replacement, then completed all four stages once the owned replacement was removed. Every instrumented native call had zero checked-out SQL connections; stage proofs survived source erasure, three Pod inventory snapshots were empty, and the owned namespace was removed normally. This does not qualify running training/inference workloads or continuous observation. [Evidence](evidence/phase-1/deletion-factory-2026-10-27.yaml).

Free space fell below the reserve between functional runs. Task-venv bytecode was removed and diagnostics for two confirmed terminal owned Ray test PIDs were archived; source, retained evidence and shared fixtures were preserved. Subsequent drivers disable bytecode writes and retain the 43.5 GB reserve guard. Current strict schema parity at head 0019, original coverage hash, protected provider namespace/service UIDs, repository lint and whitespace checks pass.

Full independent deletion acceptance remains open (0/5 certified gates); Phase 1 remains 2 Verified / 34 Open. Further mutation controls, broader provider/runtime qualification, other native producers and credential/caller requirements remain required. The 45 GB migration ceiling, deferred load, maintained images, cloud/IAM/native full journeys, mixed-version and full release/branch coverage remain unqualified. Original deletion:G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


### 2026-10-27 — current provider policy on deletion replay

A failing committed-SQL regression showed that a known native outcome could skip current provider-copy protection, advance later stages and erase project metadata. The shared deletion authority now checks original storage bindings against current provider policy on known-result replay and when qualifying every prior stage. It rejects malformed bindings, preserves the original native fact and resumes without repeating deletion after authority is restored. Existing pre-effect protection and SQL-free native execution remain in place.

On 674 frozen inputs, all 577 checks pass without failures, errors or skips: 9 publication/mutation, 30 factory (including actual process/backend loss and stale-delivery controls), 225 deletion callers, 217 storage/reconciler and 96 legacy/provider cases. Lint, formatting, strict read-only schema0019/44tables, original coverage hash, preserved k3d namespace/service identities and the43.5GB reserve pass. Two test-setup pilots and the pre-fix failure are retained. [Evidence](evidence/phase-1/deletion-publication-policy-2026-10-27.yaml).

This records a tested fix, without promoting full acceptance: publication gates remain0/3 automatically certified, parent factory gates0/5, and Phase1 remains2 Verified/34 Open. The 45GB migration ceiling is recorded; that run, largest-table row counts, deferred load, maintained images, deployed cloud/full runtime journeys, mixed-version and full release/branch coverage remain unqualified. No new native workload probe was run in this increment. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


### 2026-10-27 — atomic cancellation of active training trials

A committed PostgreSQL baseline showed that cancelling a Ray training parent left its trial attempt pending. Cancellation now calls the existing trial-supersession helper in the same transaction, covering all four active child states and clearing their leases before commit. Locked children refresh from current database state and use a consistent order. Completed successful/failed trial attempts, result rows/events and another run remain unchanged. Repeating cancellation queues no duplicate cleanup and does not increment terminal versions again. The original native grant deadlines are unchanged.

All 675 frozen inputs match. Nine focused tests and 329 affected training/workflow/trial-control regressions pass without failures, errors or skips. Four actual PostgreSQL blocking races cover both orders of cancellation against enrollment and result publication. An injected rollback preserves the parent, run, all children and queue. Old child start/heartbeat/finish operations are rejected. Two runtime test-double failures were retained and fixed by adding the missing iterable result interface; production assertions were preserved. Lint, formatting, strict read-only schema0019/44tables, original coverage hash, preserved k3d namespace/service identities and the43.5GB reserve pass. [Evidence](evidence/phase-1/trial-cancellation-2026-10-27.yaml).

This is cancellation progress for P1-W27, with all three scoped gates still0/3 automatically certified. Full native Ray kill/replacement, predecessor-checkpoint and native grant-expiry qualification remain required. Phase1 remains2 Verified/34 Open. Factory-owned Ray submission and other native producer/caller boundaries, current maintained images,45GB migration, deferred load, deployed cloud/IAM journeys, mixed-version and full release/branch coverage remain open. Original deletion:G7 still requires seven missing historical objects; none were reconstructed or re-signed.


## Phase 1 Ray submission factory — 2026-10-28

Training, splitter and preparation submissions now commit their original intent and native attempt before SDK construction, ServiceAccount ensure and suspended RayJob creation outside SQL transactions. Known replies are retained independently and recover without another creation. Unknown creation outcomes hold deletion and replacement. Original UID/submitted state commits before activation, whose JSON patch tests UID, resourceVersion and the complete original spec. Current actor, project, source, delivery and verified native context fence effects, publication and acknowledgement. Direct Session-native submission paths were removed; component fixtures preserve their assertions through a Mock-only adapter.

All 681 frozen inputs match. The five current test groups pass 101 focused, 27 process, 167 connected, 39 deletion and 353 component cases: 687 distinct cases. Three supplemental replacement-generation cases also pass. Process controls include 21 real SIGKILLs and six owned PostgreSQL backend terminations across all three stages. The replacement probe initially failed during setup because the dedicated database container was stopped; its diagnostics are retained and the unchanged-source rerun passed after restarting that owned container.

The current owned k3d probe confirms suspended creation, committed identity before activation, original-UID replacement rejection, five native calls with zero SQL checkouts, three zero-Pod observations under a hard quota, and normal namespace removal. Strict read-only schema verification remains at revision 0019 with 44 tables. Original coverage and protected namespace/service identities are preserved. [Evidence](evidence/phase-1/ray-submission-factory-2026-10-28.yaml).

These are scoped test results; automatic gate certification remains 0/6 and Phase 1 remains 2 Verified / 34 Open. Ray cancellation must still consume the original published or known unpublished create UID through factory-owned cleanup; Session-native cancellation and analysis producers remain open. Unknown native histories remain held. All-stage worker credentials, maintained images, full running workloads, cloud/IAM, mixed-version and full release/branch coverage remain unqualified. The user’s 45 GB database target is recorded as 45,000,000,000 bytes; representative largest-table row counts and enough capacity for that physical database plus reserve/WAL are still needed. The 15 × 10 GiB load campaign remains deferred. Original deletion G7 still requires seven missing historical objects; none were reconstructed or re-signed.


## Phase 1 Ray cancellation factory — 2026-10-28

Training, splitter and preparation cancellation now use original published or known unpublished create facts rather than deleting the UID returned by a name lookup. Unknown creation/service-account histories and live native units hold. Cancellation intent commits before SDK construction; per-unit custody commits before UID-conditional deletion and complete descendant inventory, all outside SQL transactions. Only qualified physical absence completes cleanup. Fresh actor, source, delivery and native context govern publication; late observations cannot acknowledge another owner. Changed or invalid context observations are retained without becoming original-cluster absence proofs.

Qualified retirement also lets an old unpublished submission delivery finish and releases its replacement/deletion hold. Both Session-native cancellation entry points now fail closed. The legacy component fixtures use owned UID inputs and shared scope/native readers; actual factory custody is checked separately. A supplemental upstream activation-death check exposed the generic one-hour pending-deletion delay being applied to a one-minute native lease. Live native waits now requeue at the actual lease expiry, verified without manually making the delivery due.

Current-source qualification passes **597 cases**: 89 focused, 15 cancellation process/backend faults, 190 affected component regressions, 299 connected regressions, three upstream activation-death cases and one real user cancellation-command journey with API/outbox replays. The cancellation qualification includes 12 actual SIGKILLs and six owned PostgreSQL backend terminations; the connected suite also reruns prior submission fault controls. All 684 frozen base inputs and the two preserved subprocess helpers match their hashes. Initial failed component and activation-probe diagnostics are retained.

Both owned local k3d zero-Pod scenarios pass: original-UID cleanup plus completed-delivery replay, and rejection of a replacement present before the first cancellation read. Native calls use zero checked-out SQL connections, and both test namespaces are removed normally. Read-only schema verification remains at revision 0019 with 44 tables and no differences; original coverage and protected namespace/service identities are preserved. [Evidence](evidence/phase-1/ray-cancellation-factory-2026-10-28.yaml).

This is scoped progress: automatic gates remain 0/5, and Phase 1 remains **2 Verified / 34 Open**. A read-only follow-up reproduces the next gap in all three stages: Ray observer identity validation accepts a distinct replacement UID when the other scope/spec fields are copied. Observer collection/publication must bind the original submitted UID. Session-native analysis producers, all-stage worker credentials, unknown native qualification, maintained images, full running workloads, cloud/IAM, mixed-version and full release/branch coverage remain required. The 45 GB physical migration target and largest-table row counts remain unqualified, and 15 × 10 GiB load remains deferred. Original deletion G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


## Phase 1 original-UID Ray observation — 2026-10-28

Training, splitter and preparation observation requests now carry the original submitted UID. Native inspection and fresh SQL publication both reject a replacement, even when its name, labels and fence/spec fields are copied. The request snapshot binds the original receipt, source outbox, command and factory intent. Changes during the read discard the stale result. For new factory submissions, the original native creation fact and immutable source digest also prevent a coherently rehashed logical receipt from rebinding identity before collection. Missing or corrupt evidence holds the affected row before native reads.

The original scope, full-token and spec checks remain in place. Native reads still release SQL connections and parent locks. Existing expiry and result-CAS behavior passes, and an invalid receipt does not abort another valid row in the same collection batch. Disposable SQL fixtures now seed original submission receipts; legacy component adapters use explicit owned fixture identities and are separate from durable evidence qualification.

The baseline reproduced 12 failures across all three stages. Current qualification passes **567 cases**: 160 focused, 158 affected component, 246 connected and three supplemental mixed-batch controls. All 686 frozen inputs match. Both owned local k3d zero-Pod probes accept the original RayJob UID and reject a real same-name replacement, with zero checked-out SQL connections during native reads and normal namespace removal. Strict read-only schema verification remains at revision 0019 with 44 tables and no differences; original coverage and protected namespace/service identities are preserved. [Evidence](evidence/phase-1/ray-observer-receipts-2026-10-28.yaml).

This is scoped progress, with automatic gates still 0/3 and Phase 1 still **2 Verified / 34 Open**. Session-native analysis producers and all-stage worker credentials remain required. Fair scheduling for observer backlogs larger than one collection batch and legacy/operator qualification remain open. Maintained images, full running workloads, cloud/IAM, mixed-version and full release/branch coverage remain unqualified. The 45 GB physical migration target, representative largest-table row counts and deferred 15 × 10 GiB load remain unqualified. Original deletion G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


## Phase 1 durable observer fairness — 2026-10-28

Training, splitter and preparation Ray collection now uses an independent nullable `last_observed_at` clock. Selected rows stamp database time while their existing SKIP LOCKED locks are held, and later scans rotate past unchanged held rows. The SQL update explicitly preserves `updated_at`; it does not renew worker heartbeat, lease or startup grace. Captured-attempt publication does not stamp again, so original UID/source snapshot fencing remains effective.

Migration 0020 adds the nullable clock without a default or backfill. Disposable expansion/rollback checks preserve legacy SQL projections and every original attempt domain field, including populated scan-clock rollback. The existing custody-preservation downgrade guard remains effective. Current strict schema verification reports 44 tables with no differences. Previous-release application startup during expansion is still unqualified; SQL projection compatibility is narrower evidence.

Current qualification passes **720 tests**: nine focused fairness/clock/rollback/locking controls, 88 schema checks, 205 observer regressions, 158 affected component checks and 260 connected API/publication checks. All **19 supported prior-schema paths** pass bounded rehearsals to head, with four upload rows requested where applicable, preserved seeded fingerprints, strict schema and clean Alembic checks. These small seeds do not substitute for a 45 GB physical rehearsal. Both owned local zero-Pod k3d probes preserve original-UID acceptance and replacement rejection with native SQL checkouts zero and normal namespace removal. Original coverage and protected namespace/service identities are preserved. [Evidence](evidence/phase-1/observer-fairness-2026-10-28.yaml).

This is scoped progress: automatic gates remain 0/3 and Phase 1 remains **2 Verified / 34 Open**. Session-native analysis producers, analysis/legacy observer recovery, all-stage credentials and unknown native qualification remain required. Maintained images, full running workloads, cloud/IAM, mixed-version application startup and full release/branch coverage remain unqualified. The 45 GB physical migration target, representative largest-table row counts and deferred 15 × 10 GiB load remain open. Original deletion G7 still requires seven missing historical objects (two models and five signed results); none were reconstructed or re-signed.


## Phase 1 analysis cleanup factory — 2026-10-28

Validation, explainability and drift cleanup now runs through the factory-owned dispatcher. The worker commits intent and native units before Kubernetes construction/read/delete/Pod inventory, releases SQL for native work, stores independent observations, and checks current source, actor, claim and verified context before acknowledgement. Only the original submitted Job UID can authorize deletion; same-name replacements and remaining descendants cannot produce completed cleanup. Lost replies and crashed units recover by inspecting the original identity before another delete. Legacy component fixtures retain their assertions through a Mock-only adapter.

Current qualification passes **599 tests**: 54 focused, 15 process/backend, 292 affected component and 238 connected. Nine SIGKILL and six owned PostgreSQL backend controls pass; both local k3d suspended-Job scenarios pass with zero SQL checkouts, zero Pods and normal namespace removal. All 694 frozen inputs match; schema parity remains revision 0020 with 44 tables. Root coverage and 193 artifacts from the preceding four scope records retain their hashes. [Evidence](evidence/phase-1/analysis-cleanup-factory-2026-10-28.yaml).

Legacy analysis receipts do not record creator context; the first cleanup unit binds the verified current context. Submission/capacity and first-delivery migration factories must still establish original creation-context custody. Maintained images, full workload runtime, cloud IAM, mixed-version startup and full release/branch coverage remain unqualified. The user confirmed 45 GB as the migration qualification target; 45,000,000,000 physical bytes plus the 43,500,000,000-byte reserve and WAL/migration overhead do not fit the approximately 45.1 GB currently free. No smaller fixture substitutes for this gate. The 15 × 10 GiB upload campaign remains deferred, and the seven missing historical deletion objects remain a required handoff. Phase 1 remains **2 Verified / 34 Open**; scoped raw oracles pass 3, with formal gates **0 met / 3 unmet / 0 abandoned**. No whole phase is complete.


## Phase 1 native analysis create identity — 2026-10-28

`KubernetesTrainingClient.create_job` now propagates every create conflict without looking up or adopting a same-name Job. Matching labels do not attest an original UID after an uncertain create. Bound requests require complete name, namespace and five scope labels before POST; successful replies must retain the submitted name, namespace, labels and a nonempty UID before returning to the SQL publisher. Successful legacy unbound delegation keeps its existing response shape, while conflicts now hold. The existing conflict test deliberately asserts this stricter contract for both matching and changed labels.

Current qualification passes **681 tests**: 52 focused, 344 affected component and 285 connected. The retained old-code baseline has 40 failures and 12 passes. The real local k3d probe covers an existing original Job, a same-label replacement and a lost original reply after server creation: five POSTs, zero SDK lookup calls, three zero-Pod observations and normal namespace removal. All 695 frozen inputs match; strict schema remains revision 0020 with 44 tables and root coverage remains unchanged. [Evidence](evidence/phase-1/analysis-create-identity-2026-10-28.yaml).

This corrects the shared native identity boundary. Analysis submission still holds SQL during native work and lacks independent durable create facts, guarded suspended activation, durable capacity reservations and factory-owned legacy migration. An unknown create outcome now stays uncertain rather than adopting a resource; automatic recovery is still required. No whole requirement or phase is promoted: Phase 1 remains **2 Verified / 34 Open**, with scoped automatic gates **0 met / 3 unmet / 0 abandoned**. Maintained images/runtime, cloud IAM, mixed-version startup, full release/branch coverage, the 45 GB migration and deferred 15 × 10 GiB upload campaign remain unqualified. Original deletion G7 still needs seven missing historical objects.


## Phase 1 analysis submission, admission and legacy migration — 2026-10-29

Validation, explainability and drift submission now uses factory-owned recovery. Intent, capacity reservations and native units commit before native work, and SQL connections are released for construction, capacity sampling, service-account creation, Job creation and activation. The original suspended Job UID and server-normalized specification are retained independently; publication commits before UID/resourceVersion/spec-guarded activation. Current command kind, actor, project, source, context and lease fence publication. A retired old generation can settle its own delivery without changing the newer generation. Unknown native creates are never reissued or adopted by name.

Short SQL admission locks serialize durable reservations without enclosing native calls. An uncertain outcome retains its slot even after cancellation; qualified original Job/Pod retirement releases it. Legacy first delivery records immutable source and independent absence observations before creating tracking, then defers creation until that tracking is committed. Late observations preserve the selected immutable proof. API-default normalization permits equivalent quantities and omitted empty environment values while rejecting injected workload/credential changes. Verified raw-dataset provenance is checked before native construction.

Five process-fault workers moved from ignored workstation files into repository test support. They use the parent dedicated test database and import paths; backend termination remains scoped to the owned child PID, application name and database. This makes the tests repository-contained without claiming a remote CI or release-image pass.

Current qualification passes **818 distinct tests**: 149 focused, 247 affected component, 329 connected, 36 submission failure controls and 57 additional fault controls. The new submission controls include 24 SIGKILL cases and 12 owned PostgreSQL backend terminations. Three real local k3d scenarios cover normal creation/activation/cleanup, same-name replacement rejection and legacy first-delivery migration, all with native SQL checkouts zero and a hard zero-Pod quota. All 709 frozen inputs match, schema parity remains revision 0020 with 44 tables, root coverage is unchanged, all current probe namespaces retired normally and no owned fault backend remains. Six preceding evidence sets retain all 262 artifact hashes. [Evidence](evidence/phase-1/analysis-submission-factory-2026-10-29.yaml).

The initial fault matrix retained 35 passes and one foreign-row queue claim under an advanced test clock. Owned fixture queue priority corrected isolation; final qualification is sequential. Captured prior Session dispatch fails all three native SQL-custody controls, and the old-generation regression fails on the pre-fix implementation with its six controls passing. These retained failures are not recast as green runs.

No whole requirement or phase is promoted: Phase 1 remains **2 Verified / 34 Open**; scoped raw oracles pass four, while formal automatic gates remain **0 met / 4 unmet / 0 abandoned**. Unknown creates still need independent original native qualification. Analysis/legacy observation, all-stage worker credentials/capacity and remaining workflow families remain required. Maintained images/full running workloads, cloud IAM, previous-release startup/mixed-version deployment, full release/branch coverage, the physical 45 GB migration and deferred 15 × 10 GiB campaign remain unqualified. Original deletion G7 still requires seven missing historical objects; none were rebuilt or re-signed.


## Phase 1 managed analysis observer fairness — 2026-10-29

Validation, explainability and drift observation now uses the existing separate scan clock, so held evidence rotates out of a bounded batch without renewing worker leases or restarting startup grace. Invalid submission evidence stays held per attempt while valid rows continue. Receipt/source container checks reject malformed data before field access. Targeted publication preserves the scan clock. The shared test delivery helper now gives each attempt a unique event key, allowing two submitted runs in one project.

Current qualification passes **620 distinct tests**: 30 focused, 386 connected and 204 submission/cleanup regressions on 710 frozen inputs. The captured prior implementation fails 27 controls while its three lock controls pass. Fresh-session batches of one and two, all three analysis kinds, zero SQL checkouts during native reads, worker-clock preservation, rollback and actual concurrent SKIP LOCKED controls pass. The auditor accepts the current result and rejects an injected XML failure. The initial fixture run had 24 setup errors from duplicate event keys; those records are retained separately. All 454 preceding artifact hashes remain intact. Strict schema remains 0020 with 44 tables; root coverage and protected local cluster identities are preserved. [Evidence](evidence/phase-1/analysis-observer-fairness-2026-10-29.yaml).

This is partial progress: Phase 1 remains **2 Verified / 34 Open**; raw oracles pass both, while formal gates remain **0 met / 2 unmet / 0 abandoned**. No new native workload probe was run for this SQL selection change. Legacy observer fairness, complete source/actor/context recovery, all-stage credentials/capacity and remaining workflow families remain open. The accepted physical migration target is **45,000,000,000 bytes**; present disk cannot hold that dataset alongside the preserved 43.5 GB reserve and migration overhead, and largest-table cardinalities are unknown. The 15 × 10 GiB campaign, full running images/workloads, cloud IAM, prior-release startup, mixed-version deployment and full branch/release coverage remain unqualified. Original deletion G7 still requires seven missing historical objects; none were rebuilt or re-signed.


## Phase 1 legacy analysis observer fairness — 2026-10-29

Legacy validation, explainability and drift observation now records a separate nullable run scan timestamp through the existing bulk observer helper. Invalid stored namespace/name holds only its own row; native read failures and foreign Job identities rotate to later work in bounded fresh-session scans. Domain/startup clocks remain unchanged and targeted publication does not rescan. Revision 0021 adds the nullable timestamp without a default or backfill; rollback drops only that operational field.

Current qualification passes **883 distinct tests**: 36 focused, 113 schema, 500 connected and 234 submission/deletion cases on 713 frozen inputs. The prior implementation reproduces 33 failures with three lock controls passing. Tests cover all three kinds, batch sizes one and two, actual legacy dispatch after native failures, zero SQL checkouts during native reads, preserved domain clocks, rollback and concurrent SKIP LOCKED. Expansion/rollback preserves domain rows and prior SQL projections. All **20 supported prior-schema paths** reach 0021 with strict parity across 44 tables, preserved seeded metadata and no Alembic drift. Each applicable path uses four upload metadata rows; this is a bounded metadata rehearsal. The auditor rejects an injected XML failure. All 482 preceding artifact hashes, root coverage and protected local cluster identities remain intact; final read-only checks find no rehearsal databases or owned fault backends. [Evidence](evidence/phase-1/legacy-observer-fairness-2026-10-29.yaml).

Phase 1 remains **2 Verified / 34 Open**; raw oracles pass three, while formal gates remain **0 met / 3 unmet / 0 abandoned**. Full legacy/managed analysis source, actor and context recovery, all-stage credentials/capacity and remaining workflow families remain open. No new running workload or release-image qualification is claimed. Physical **45,000,000,000-byte** migration, representative complete workflow histories, prior application binaries, mixed-version startup, remote CI/full branch coverage, cloud IAM and the deferred 15 × 10 GiB campaign remain unqualified. Present disk cannot hold the physical target alongside the preserved 43.5 GB reserve and migration overhead; largest-table cardinalities are unknown. Original deletion G7 still requires seven missing historical objects; none were rebuilt or re-signed.


## Phase 1 analysis observation source and command custody — 2026-10-29

Managed analysis observation now verifies the original immutable factory command, run, dataset and request source before accepting its receipt. Job UID, name and namespace bind to the original native creation fact. Factory create history without its original intent stays held. The original-source validator is shared with cleanup, preserving existing physical retirement and old-generation behavior. Observation validates the current scoped command/resource/request hash and includes the complete command snapshot when collecting and publishing; command changes during a native read discard the captured result. Valid unchanged missing-Job observations retain one cleanup and one successor.

Current qualification passes **1,015 distinct tests**: 66 focused, 536 connected, 320 factory and 93 process/backend controls on 714 frozen inputs. The captured old branch has 54 failures and six unchanged-original controls passing. A separate replay of its original identity verifier on current callers reproduces all six metadata failures; the initial 60-test positive checkpoint is retained separately. The current auditor accepts the 66-case result and rejects an injected XML failure. The owned zero-Pod k3d validation probe accepts an original observation, holds a changed command without another native read, retires the original UID and removes its namespace normally. Capacity sampling uses a test double; no running workload qualification is claimed. Final read-only checks confirm strict schema revision 0021/44 tables, unchanged root coverage/protected local identities, no remaining probe namespace, no rehearsal databases and no owned fault backends. All 518 preceding artifact hashes remain intact. [Evidence](evidence/phase-1/analysis-observer-source-2026-10-29.yaml).

Phase 1 remains **2 Verified / 34 Open**; raw oracles pass three, while formal gates remain **0 met / 3 unmet / 0 abandoned**. Complete actor/project policy, native-context and legacy original-outcome qualification, all-stage credentials/capacity and remaining workflow families remain open. The prior scope retains all 20 bounded prior-schema rehearsals. Physical 45 GB migration, representative full workflow histories, maintained prior binaries/mixed-version startup, running images/workloads, cloud IAM and full remote CI/branch/release coverage remain unqualified. Present storage cannot hold the accepted 45,000,000,000-byte target alongside the preserved 43.5 GB reserve and WAL; largest-table cardinalities are unknown. The 15 × 10 GiB campaign remains deferred. Original deletion G7 still requires seven missing historical objects; none were rebuilt or re-signed.


## Phase 1 analysis run/report scope isolation — 2026-10-29

Current run-kind/project, tag-container and report/attempt consistency validation now shares the existing per-row source hold path. Invalid run kinds, malformed tags and inconsistent report states hold only their own row, allowing same-batch and later bounded fresh-session valid work to continue without rolling back the scan clock. Held workflows and original evidence stay unchanged. Cancellation keeps its cleanup precedence over a late inconsistent report; native operations and terminal-state decisions retain their existing behavior.

Current qualification passes **641 distinct tests**: 39 focused and 602 connected cases on 715 frozen inputs. The prior implementation reproduces 36 failures with its three cancellation controls passing. Tests cover all three analysis kinds, scan limits one and two, fresh sessions, actual dispatch, preserved workflow state and zero SQL checkouts during native reads. The auditor accepts the current result and rejects an injected XML failure. The initial 39-case checkpoint before formatting is retained separately; final checks use the formatted source. Strict schema remains revision 0021 with 44 tables. Root coverage, protected local cluster identities and all 787 preceding artifact hashes remain intact. [Evidence](evidence/phase-1/analysis-observer-scope-2026-10-29.yaml).

Phase 1 remains **2 Verified / 34 Open**; raw oracles pass both, while formal gates remain **0 met / 2 unmet / 0 abandoned**. No new native or process-failure campaign was run for this SQL validation change; the preceding source scope retains its 93 process/backend controls and owned zero-Pod k3d probe. Complete actor/project policy, native-context and legacy original-outcome qualification, all-stage credentials/capacity and remaining workflow families remain open. Physical 45 GB migration, representative full workflow histories, prior binaries/mixed-version startup, full workloads/images, cloud IAM and full remote CI/branch/release coverage remain unqualified. Present disk cannot hold the accepted physical target alongside the preserved 43.5 GB reserve and WAL; largest-table cardinalities are unknown. The 15 × 10 GiB campaign remains deferred. Original deletion G7 still needs seven missing historical objects; none were rebuilt or re-signed.


## Phase 1 current idempotency coverage and analysis command contract — 2026-10-29

The current application exposes 22 idempotency-header handlers. The old inventory asserted 21 even though all 22 dynamic header probes passed. Coverage now names each handler explicitly and checks its registration against global OpenAPI. The production Python scan covers repository sources under apps/packages, finds one explicit command constructor and 11 named key readers, and detects injected extra constructors/raw SQL. The coverage map records how each reader validates or refuses replay. Two initial collector API assumptions failed and were corrected; those records remain separate from product regressions.

The audit also exposed a live producer/consumer mismatch: generic analysis API mutation commands use `validation.launch`, `explainability.launch` or `drift.launch` as their resource type, while worker guards accepted only `model_run`. All six API/legacy contract controls failed before the fix. Four consumer checks now accept the public operation label or the older physical label. Exact operation/run-kind, project/resource/actor, hash, original identity and fence checks remain enforced; producers and persisted requests are unchanged. Real HTTP/SQL admission and outbox tests then drive the actual factory/native SDK boundary for all three kinds, repeat cached API responses before and after cleanup, and preserve qualified legacy delivery. Plans and native SDK responses in those tests are doubles.

Current qualification passes **1,127 distinct tests**: 23 header, 144 contract, 6 producer/consumer, 2 concurrency, 49 legacy, 198 upload, 274 worker and 431 analysis cases on 717 frozen inputs. New HTTP/PostgreSQL hash controls inspect actual inserted requests for ten public variants, then verify stable pending replay and changed-body/file/target conflicts before runtime callbacks. The owned zero-Pod k3d validation probe uses the public operation label, accepts original observation, holds a changed command without another native read, retires the original UID and removes its namespace. Capacity sampling is a test double. The auditor rejects an injected XML failure. Strict schema remains revision 0021/44 tables, root coverage and protected local cluster identities are unchanged, no probe namespace or known owned fault backend remains, and all 813 preceding artifact hashes stay intact. [Coverage map](phase-1-idempotency-coverage.md) · [Evidence](evidence/phase-1/idempotency-coverage-2026-10-29.yaml).

P1-W04/P1-W05 and Phase 1 remain open: **2 Verified / 34 Open**, with three raw oracles passing and formal gates **0 met / 3 unmet / 0 abandoned**. The buffered upload route still executes its object-store write before the surrounding SQL mutation commits; its crash/recovery boundary needs separate remediation. Current contract tests do not qualify every historical/deployed protocol or ingress/provider path. Actor/project policy, native-context and all-stage credentials/capacity, full workloads/images, cloud IAM, prior binaries/mixed-version startup and full remote CI/branch/release coverage remain unqualified. The physical 45 GB target cannot fit with the preserved 43.5 GB reserve and WAL; largest-table cardinalities are unknown and the 15 × 10 GiB campaign is deferred. Original deletion G7 still requires seven missing historical objects; none were rebuilt or re-signed.

## Phase 1 buffered upload allocation (2026-10-29)

Public buffered uploads now acquire the existing project-serialization lock before command insertion. This prevents concurrent requests from creating duplicate named datasets or competing for the same version, and shares the resumable publication lock without changing response contracts. Real HTTP/PostgreSQL tests cover new and existing names, identical content, same-key replay, changed-body conflicts, a failed first request and independent projects. The mixed-protocol control calls the actual resumable SQL publication function; it is not a complete resumable HTTP journey. **241 tests** pass (8 focused and 233 connected), with four failing old-code controls and four passing controls.

An independent diagnostic injects failure after filesystem PUT but before SQL commit. Two identical retries leave two objects and no SQL metadata or command; **buffered crash recovery remains open**. Same-project uploads now wait through the prior buffered storage call and commit. Durable input/intent, SQL-free native calls, original-outcome custody and deletion recovery are still required. No new k3d workload or cloud-provider qualification is claimed.

The user's 45 GB database limit remains the physical **45,000,000,000-byte** migration target; largest-table cardinalities are unspecified. No smaller/sparse substitute or reduced disk reserve was used. Current schema parity is 0021/44 tables; root coverage, protected cluster identities and 893 prior artifact hashes remain intact. Phase 1 remains **2 Verified / 34 Open**; scoped formal gates are **0 met / 3 unmet / 0 abandoned**. [Evidence](evidence/phase-1/buffered-upload-serialization-2026-10-29.yaml).

## Phase 1 original byte-create custody (2026-10-29)

The first dependency of the full buffered recovery plan adds `create_bytes` to the existing storage contract and all four concrete drivers. Existing overwrite operations and public endpoints retain their contracts. Exclusive writes return the original native version rather than a later metadata lookup. S3 requires the existing single-attempt client; Azure disables call retries, including chunked block commits. GCS sends an expected CRC32C for server validation and disables automatic SDK corrupt-outcome deletion, then checks original object ID, generation, size and checksum. Embedded creation atomically links a fully written temporary file and preserves the concurrent winner. It remains local-only, without an immutable provider version or power-loss qualification.

**433 cases pass**: 69 component cases, 32 real SDK request cases and 332 connected regressions. Request fixtures cover ordinary/resumable GCS and ordinary/chunked Azure paths, collisions, missing identities, lost replies, unavailable responses and corrupt GCS replies without latest-object reads or automatic deletes. They are not live AWS/GCS/Azure qualification. A separate native SeaweedFS probe checks original immutable reads, collision rejection, concurrent creation and lost-reply propagation over verified TLS. It removes its owned prefix and namespace normally.

The protected fixture's CA expired on 2026-10-26. Its store uses ephemeral data, so it was not restarted or reconfigured. A first isolated certificate fixture also failed chain validation because issuer and leaf shared a subject DN; that failed namespace was removed, and the corrected fixture uses distinct subjects. The failures are retained. Protected namespace/service/Pod identities remain unchanged, but protected object bytes were not requalified through expired TLS.

The public buffered endpoint still needs private durable input/schema, committed intent, SQL-free factory dispatch, fresh actor/source fences, interruption recovery and policy-aware deletion that accounts for unknown writes, partial blocks, sessions and late replies. The complete recovery plan and six root gates remain required; this primitive does not establish recovery completion. Strict schema remains 0021/44 tables; root coverage and 924 prior artifact hashes remain intact. Phase 1 stays **2 Verified / 34 Open**. Formal storage gates: **0 met / 3 unmet / 0 abandoned**; recovery root gates: **0 met / 6 unmet / 0 abandoned**. [Evidence](evidence/phase-1/buffered-upload-create-custody-2026-10-29.yaml).

## Phase 1 private buffered source admission (2026-10-29)

Revision **0022** adds a private deferred BYTEA source, exact PostgreSQL SHA256/size checks and a restrictive project/command foreign key. A checked INSERT/UPDATE/DELETE trigger requires initial admission, preserves original source and native outcomes, prevents phase skipping/rollback, and blocks deletion until retirement. Bytes clear only after publication or retirement. Populated downgrade fails before custody can be dropped; empty expansion/rollback passes. Strict parity now covers **45 tables** and five critical trigger contracts.

Session-factory admission commits source bytes, canonical command and minimal outbox delivery together. Fresh actor/token/project/membership checks surround inspection; expensive inspection and shared pure backend configuration validation do not construct a storage SDK or retain a SQL checkout. Replay preserves the original source, rejects changed or ambiguous history and keeps qualified cached legacy responses separate from pending commands whose original input is missing. Full raw input does not enter command/outbox/response JSON. Existing authorized response previews remain part of the compatibility contract.

The shared upload validator now rejects empty values after normalization. A first before-validator implementation broke supported byte-to-text normalization; that four-case regression is retained, and the final implementation preserves the existing conversion/length-check order with an explicit normalized-empty check. Pure storage configuration validation is shared with the current factory and rejects invalid backends before private admission. Whitespace in persisted driver spelling is normalized consistently.

**507 tests pass**: 39 schema, 74 admission/validation and 394 connected cases on 604 hashed Python inputs. Two actual SIGKILL cases prove precommit rollback and postcommit source/delivery durability through a fresh process/session; they exercise the admission service rather than a full HTTP/native journey. All **21 supported prior schemas** reach head with preserved bounded metadata, four upload rows where applicable, strict parity and no Alembic drift. An injected auditor XML failure is rejected. Preliminary runs and negative controls are retained. The dedicated test database has no remaining private inputs, fault backends or rehearsal databases; root coverage, protected cluster identities and 969 earlier artifact hashes remain intact.

The public buffered endpoint still runs the old write-before-commit path. This dependency remains unexposed until factory dispatch, HTTP handoff, original native fact recovery and policy-aware retirement/deletion are implemented together. Unknown writes, partial blocks/sessions and late replies must retain custody. Private BYTEA/WAL capacity and large local buffered bodies require qualification before activation. Physical45GB migration, table cardinalities, maintained previous-binary/mixed-version startup, full images/native workloads, cloud IAM, the15-user by10GiB campaign and the seven missing original historical objects remain open. No objects were rebuilt or re-signed; no Kubernetes resources were created for this dependency, and the expired protected-store certificate was not changed.

Phase 1 remains **2 Verified / 34 Open**. Formal input gates: **0 met / 3 unmet / 0 abandoned**. The six full recovery gates and three storage gates remain formally unmet. [Evidence](evidence/phase-1/buffered-input-admission-2026-10-29.yaml).


## Phase 1 original buffered execution and publication (2026-10-29)

The factory now commits an issued write before provider work, preserves the qualified original reply in a separate transaction and verifies the original immutable version and SHA256 before publication. Fresh actor/token/membership/project/backend/source/delivery checks guard each stage. Original facts survive changed authority and leases; unknown writes are held without reissue or latest-object adoption. Publication atomically allocates metadata, caches201, clears private bytes and acknowledges delivery. Project deletion now holds before prefix erasure for every unretired buffered input or missing private history. This hold remains until complete retirement and custody transfer are integrated.

**386 tests pass**:35 execution,74 admission/validation and277 connected cases on607 hashed Python inputs. Three actual SIGKILL and two owned PostgreSQL backend-termination cases distinguish committed facts from unknown outcomes, retain bytes on rollback and recover known outcomes without another create. Fault-case provider operations use a versioned file-backed SDK double. A separate actual SeaweedFS k3d factory probe passes one create, pinned reads, zero SQL checkouts, cached replay and a real deletion-request hold over fresh verified TLS. Its namespace, port-forward and SQL fixtures are removed; protected namespace/service/Pod identities are unchanged. Only the two fault-test sources changed after that native probe; product sources stayed identical. Strict schema0022/45 tables, original coverage and1,092 preceding artifact hashes remain intact. The auditor rejects an injected failure. [Evidence](evidence/phase-1/buffered-execution-2026-10-29.yaml).

The public endpoint is still on its previous native-in-SQL producer. HTTP handoff and dispatcher activation must include complete retention/cleanup and custody transfer; published private records currently hold project deletion. Controlled unknown-outcome resolution, physical producer quiescence, partial provider writes, all-stage credentials/context, large local BYTEA/WAL compatibility and full native/cloud runtime qualification remain open. The accepted physical45GB migration cannot fit alongside the43.5GB reserve before WAL; table cardinalities are unknown and15x10GiB load remains deferred. The protected store's expired CA was not changed, its ephemeral Pod was not restarted and its historical object bytes were not requalified. Original deletionG7 still needs seven missing historical objects; none were reconstructed or re-signed. Phase1 remains **2 Verified /34 Open**. Direct execution oracles pass, while formal execution gates remain **0 met/3 unmet/0 abandoned** and all15 buffered pipeline gates remain formally unmet. This is progress, not recovery or phase completion.


## Phase 1 buffered custody through project deletion (2026-10-29)

Owner-authorized project deletion now captures the complete original private-input inventory and commits cancellation of unissued/known recorded deliveries with its deletion request. Unknown issued writes, missing command/delivery/private history, changed source and foreign object bindings remain held. The existing deletion stages preserve the input/content/request digests, original object-version receipts and publication bindings. Each stage revalidates current source/backend/policy; private bytes and receipts remain until all native stages qualify cleanup. Final SQL publication retires input and removes project metadata atomically. Tombstone holds and published version rows lock through that transaction. No table or migration was added.

**540 distinct tests pass**:43 focused,109 execution/admission/validation and388 connected cases on609 hashed Python inputs. Controls cover both project-deletion/native-issue orderings, request and metadata rollback, legal holds on private bytes, current-policy changes after native absence, complete mixed inventory, independent missing-history anchors and10 URI controls across five backends. Five SIGKILL and two actual owned PostgreSQL backend-loss regressions pass; their native operations are doubles. Two initial tests incorrectly expected a cancelled delivery to be a no-op; they now require its existing stale-lease exception, and those failed records are retained.

A separate actual SeaweedFS k3d factory probe transfers unissued, known recorded and published inputs through all four deletion stages with two original factory creates. A legal hold prevents native work, private bytes survive until metadata, a neighboring project object remains intact, and all surviving stage records retain the original versions. Provider calls hold zero SQL checkouts. The test namespace, forwarding process, neighboring fixture and owned SQL rows are removed; protected namespace/service/Pod identities remain unchanged. Strict schema0022/45 tables, original coverage and1,277 prior hashes remain intact. The auditor rejects an injected XML failure. [Evidence](evidence/phase-1/buffered-deletion-2026-10-29.yaml).

This implements project-deletion custody transfer, not the full buffered recovery protocol. The public endpoint still uses its prior native-in-SQL producer. HTTP handoff/dispatcher/removal of that producer, standalone cancellation/expiry/quarantine/quota, controlled unknown outcomes and physical producer quiescence remain required. Large local BYTEA compatibility and capacity/WAL/backup lifecycle are unqualified; logical SQL retirement does not establish secure physical erasure. Full runtime/images/cloud IAM/all-stage credentials, previous binaries/mixed versions, full remote release/branch CI and physical45GB migration remain open; current disk cannot fit45GB plus43.5GB reserve before WAL. Table cardinalities remain unknown and15x10GiB load is deferred. Protected original store TLS remains expired; its ephemeral Pod and historical objects were not recreated, restarted or requalified. Original deletionG7 still requires seven missing objects. Phase1 remains **2 Verified/34 Open**. Direct oracles pass, while formal deletion gates remain **0 met/3 unmet/0 abandoned** and all18 buffered recovery gates remain formally unmet.


## Phase 1 shared upload quota (2026-10-29)

The live buffered route, durable buffered admission and resumable admission now use one SQL-only quota check under the project lock. Private input reserves space before its source/command/delivery commits; expensive inspection runs after an initial check and final admission rechecks current reservations. Unknown and unpublished inputs, plus unqualified retired history, retain their full declared size. Published metadata offsets the matching reservation, avoiding double charges, while missing/null/smaller/inconsistent metadata cannot reduce the known original reservation. Active resumable sessions also offset their already-published version. Metadata aggregation uses a grouped join and never materializes private bodies; pending metadata counts with autoflush disabled. The legacy public route now enforces the check before inspection/native work and keeps original cached201 replay after quota changes. Its native-in-SQL crash gap remains unfinished.

**441 distinct tests pass**:31 quota/API,152 buffered,246 resumable/retirement and12 legacy service/concurrency cases on610 hashed Python inputs. Controls include actual public HTTP/SQL rejection, exact-bound success, unchanged cached replay after quota reduction, mixed-family final-slot contention, project isolation and conservative missing-metadata accounting. A runtime override recreating the old quota behavior fails28 cases, with3 positive controls and no final errors; this is not a prior branch checkout. Its first run exposed a missing teardown for an unissued test-only private input. That exact owned fixture was validated and removed, and the race now cleans its source regardless of which request wins. A grouped-query refactor initially used a subquery where a scalar column was required; that failure is retained and the final campaign passes after correction. An auditor fixture-copy omission was also corrected before its positive and injected-failure controls passed.

Strict schema0022/45 tables, original coverage, protected cluster identities and1,538 prior artifact hashes remain intact. No private input, owned fault backend or rehearsal database remains. Five SIGKILL and two actual owned PostgreSQL backend-loss regressions pass in the connected profiles; their native operations are doubles. No new native workload probe or migration was needed for this SQL-only change. [Evidence](evidence/phase-1/buffered-quota-2026-10-29.yaml).

Quota tracks logical declared inputs, not duplicate copies, WAL/backups or physical disk usage. Provider-import admission and complete historical/native-custody quota-release qualification remain open. Recovery-route HTTP/dispatcher activation and removal of the old native-in-SQL producer, standalone cancellation/expiry/quarantine, controlled unknown outcomes and physical producer quiescence remain required. Large local BYTEA compatibility/capacity, maintained images/cloud IAM/all-stage credentials, previous binaries/mixed versions, full remote release/branch CI and physical45GB qualification remain open. Current disk cannot fit45GB plus43.5GB reserve before WAL; table cardinalities are unknown and15x10GiB load remains deferred. Protected original store TLS stays expired, and its ephemeral Pod and seven missing historical objects were not rebuilt or re-signed. Phase1 remains **2 Verified/34 Open**. Direct oracles pass; formal quota gates remain **0 met/3 unmet/0 abandoned**, with all21 buffered pipeline gates formally unmet and original deletionG7 still a separate required handoff.


## Phase 1 standalone buffered cancellation and expiry (2026-10-30)

Standalone buffered cancellation and expiry now use a durable retirement owner, original-source/version binding, bounded attempts and independently committed absence. The actual abort route and periodic worker use this service. Native calls release SQL; unissued input retires without an SDK. Fresh authority/policy/source checks guard byte clearing and cached response publication. Valid new credentials for the original actor can complete a known result without another deletion, while a token revoked during native work blocks publication. Quota releases only with complete authenticated history, and project deletion preserves that proof through all four stages. Unknown creates, published references and unversioned embedded recorded data remain held.

**382 distinct tests pass** (40 focused,183 buffered,159 caller cases) on **613 hashed Python inputs**. Fault controls include eight process kills and four actual owned PostgreSQL backend terminations across admission/execution/retirement; their native effects use doubles and owned files. A separate fresh-TLS SeaweedFS k3d probe exercises the actual cancellation route, deletes exactly the original version, preserves a replacement at the same key, retires unissued inputs without native calls, rejects published input and validates quota release and all four project-deletion stages. All owned SQL/native fixtures are removed, protected original namespace/service/Pod identities remain unchanged, and strict schema0022/45 tables, root coverage and2,180 prior artifact hashes are preserved. The live inventory records23 public idempotency headers, one central constructor and12 named SQL key readers; the earlier idempotency campaign/document remains immutable historical evidence. [Evidence](evidence/phase-1/buffered-retirement-2026-10-30.yaml).

This qualifies standalone known-input retirement, not full buffered recovery. Complete quarantine/retention classification, immutable embedded retirement, compatible public upload HTTP/factory activation and removal of its native-in-SQL producer, controlled unknown outcomes and physical producer quiescence remain required. Large-local BYTEA/WAL/backup/physical-erasure behavior and complete historical/provider quota release remain unqualified. The user confirmed a45GB decimal qualification ceiling: **45,000,000,000 bytes**. A physical rehearsal cannot fit the current disk while preserving43.5GB reserve, before WAL; largest-table row counts remain unknown and15x10GiB load is deferred. Full runtime, maintained images/cloud IAM/all-stage credentials, previous binaries/mixed versions and full remote release/branch CI remain open. Protected original TLS is expired and its ephemeral Pod was preserved; original deletionG7 still requires seven missing objects, with no recreation or re-signing. **Phase1 remains2 Verified/34 Open**. Formal retirement gates remain **0 met/3 unmet/0 abandoned**, and all24 buffered pipeline gates remain formally unmet; raw passing checks do not replace inspected formal oracle approval.


## Phase 1 original embedded write incarnation (2026-10-30)

Managed local buffered writes now return a durable original file incarnation. Verification pins that incarnation; managed overwrite, conditional unlink and prefix deletion share a process-safe store lock. Replacement data survives an original-version deletion even when its bytes are identical. The original inode and affected directory chain are synced before success. Missing/malformed identity evidence cannot become an absence proof, new replies without an incarnation cannot publish, and legacy unversioned input stays held for controlled verification/retirement recovery. Unsupported identity filesystems explicitly refuse qualified create while existing overwrite behavior remains available. Embedded storage remains forbidden in staging/production.

**583 distinct tests pass** (122 storage,215 buffered lifecycle,246 connected SDK/retirement/worker/expiry cases) on **616 hashed Python inputs**. Actual local files exercise public abort with replacement preservation, policy expiry, publication rollback, authenticated quota release and project-deletion proof transfer, with zero SQL checkouts at filesystem/factory boundaries. A real lock-holder process is killed and another process safely resumes managed writes; existing admission/execution/retirement controls include eight more process kills and four actual owned PostgreSQL backend terminations, with controlled native effects. Durability failures return no false original receipt. Initial failures used a nonexistent deletion API and fake cloud-version fields in local fixtures; the fixtures were corrected without weakening identity validation. A final review also closed unpinned legacy verification, and final checks were rerun on matching fingerprints. The auditor rejects an injected test failure. Strict schema0022/45 tables, root coverage, protected cluster identities and **3,189 prior artifact hashes** remain intact. No owned private input, fault backend or rehearsal database remains. [Evidence](evidence/phase-1/buffered-embedded-2026-10-30.yaml).

This is an owned Linux filesystem qualification. Historical local versions are not retained, out-of-band filesystem administrators are outside the managed mutation protocol, and cross-host filesystems, Windows/macOS runtime behavior and physical data erasure remain unqualified. Complete quarantine/retention classification, controlled unknown creation/deletion outcomes, late replies, compatible public HTTP/factory activation/removal of its native-in-SQL producer and physical producer quiescence remain required. Large-local BYTEA/WAL/backup behavior, full historical/provider quota release, maintained images/cloud IAM/all-stage credentials, complete native runtime journeys, previous binaries/mixed versions and full remote release/branch CI remain open. Physical45GB qualification cannot fit the current disk plus43.5GB reserve before WAL; largest-table counts remain unknown and15x10GiB load is deferred. Protected original TLS stays expired; its ephemeral Pod and seven missing historical objects were not rebuilt or re-signed. Original deletionG7 remains a required handoff. **Phase1 remains2 Verified/34 Open**. Formal embedded gates remain **0 met/3 unmet/0 abandoned**, with all27 buffered pipeline gates formally unmet; passing direct checks do not replace inspected automatic oracle approval.


## Phase 1 bounded buffered worker and quarantine replay (2026-10-30)

The actual worker now dispatches buffered uploads through the factory after releasing SQL, while direct session dispatch refuses the topic. Failed deliveries keep their attempt count; zero/exhausted budgets stop before SDK construction. Unknown writes and exhausted known-state work enter durable quarantine bound to their original source, delivery, phase and counters. Private bytes and quota remain reserved. An administrator replay endpoint records a separate source-bound approval with the original quarantine, prior grant, credential epoch and bounded additional attempts. It caches same-key responses and rejects changed/missing histories, conflicts and unqualified unknown writes. Fresh grant credentials and role guard native issue and publication without changing the original manifest or native target.

Late original receipts still commit independently. They can requeue only their own known delivery within its existing budget; exhausted known outcomes require a new approval. Generic dead-letter replay cannot reset buffered counters or bypass that grant. Known quarantined inputs remain cancellable/expirable, and project deletion archives their original quarantine and replay history through existing stages.

**483 distinct tests pass** (38 quarantine/replay,215 buffered lifecycle,230 caller/storage/worker cases) on **618 hashed Python inputs**. Controls include actual HTTP replay, credential rotation/revocation, another administrator continuing the original source, concurrent same/different-key approvals, rollback, corrupt/missing grant/quarantine history, late replies inside/exhausting the original budget, expiry and cancellation/deletion transfer. Eight buffered process kills and four actual owned PostgreSQL backend terminations pass with controlled native effects; the local lock-holder process control also passes. Two older workflow mocks omitted their required topic; fixtures were corrected without weakening the product guard and all final profiles use matching fingerprints.

A fresh-TLS SeaweedFS k3d probe exercises the actual zero-budget worker quarantine, HTTP replay/cache and actual worker publication with one original create. Original-version retirement preserves a replacement, unissued cancellation makes no native call, published retirement refuses, quota release remains authenticated and all four deletion stages retain the replay chain. Provider calls/streams and application store factories hold zero SQL checkouts. Its namespace, forwarding process and owned SQL fixtures are removed; protected original namespace/service/Pod identities remain unchanged. Strict schema0022/45 tables, root coverage and **3,952 prior artifact hashes** are preserved. The auditor rejects an injected failure; no private input, owned fault backend or rehearsal database remains. The live inventory now has24 public idempotency headers, one command constructor and12 named key readers. [Evidence](evidence/phase-1/buffered-quarantine-2026-10-30.yaml).

Quarantine preserves custody; it does not resolve uncertain native work. Controlled unknown create/delete recovery, physical producer quiescence and late replies, complete buffered retention/quarantine integration, compatible public upload HTTP/factory activation and removal of its native-in-SQL producer remain required. Large-local BYTEA/WAL/backup/physical-erasure behavior, full historical/provider quota release, maintained images/cloud IAM/all-stage credentials, full runtime journeys, prior binaries/mixed versions and remote release/branch CI stay open. The physical45GB target cannot fit current disk plus43.5GB reserve before WAL; table cardinalities remain unknown and15x10GiB load is deferred. Protected original TLS stays expired, its ephemeral Pod was preserved, and original deletionG7 still requires seven missing historical objects; none were recreated or re-signed. **Phase1 remains2 Verified/34 Open**. Formal quarantine gates remain **0 met/3 unmet/0 abandoned**, with all30 buffered pipeline gates formally unmet; direct passing checks do not replace inspected automatic oracle approval.
