# Phase 1 completion ledger

Status: **in progress**. The active objective is the entire Phase 1 in the production-readiness guide, not an individual increment. All 36 Phase 1 task-index requirements are retained below: two verified, 34 open. Existing implementation and historical passing checks are leads for verification, not proof that the whole requirement is complete.

The latest full backend checkpoint passed3953 tests with no failures, errors or skips, at94.741% statement and86.651% branch coverage (required90.01% independently). That checkpoint includes the refit/evaluator retirement, command replay and fixture isolation changes, but predates the revocation transaction fix below. Post-suite schema parity at head0017 and repository Ruff passed. Current-source full-suite and global branch qualification remain open. See `evidence/phase-1/fixture-backend-2026-10-20.yaml`.

Local k3d is authorized for bounded tests. The user supplied a 45 GB maximum database qualification size (interpreted as 45,000,000,000 bytes); largest-table row counts remain unspecified. The host recheck on 2026-10-19 records 47,774,375,936 bytes free (approximately 47.8 GB), below the 45 GB fixture plus the existing approximately 37.47 GB cluster reserve even before migration overhead. Production-sized migration qualification and the guide’s prerequisite phases remain required; no local scale substitution or waiver is implied. The 15 × 10 GiB upload campaign belongs to later capacity qualification and remains deferred to suitable infrastructure.

The current QA image predates the recent training replay, authorization, header-validation, delivery-fence and durable local profiling fixes. Isolated current-source API/CPU images passed catalog/model runtime qualification on 2026-10-16; the subsequent upload/digest and tracking writer/recovery changes have local Python/PostgreSQL verification only. Earlier native evidence is historical; it does not qualify the entire current worktree.

The earlier 2026-10-19 isolated k3d image preflight verified source-current API/CPU test images on 191 frozen inputs: exact image contents, current verifier protocol/imports, API-to-CPU catalog compatibility and a real fit/serialization round trip. At that earlier checkpoint the 164 recorded shared resource baselines matched; subsequent drift is recorded below. These source-overlay test images are not a fresh release build and do not qualify the maintained verifier path or native storage/identity denials. All four native qualification gates remain open. See `evidence/phase-1/source-current-image-preflight-2026-10-19.yaml`.

The first maintained native verifier submission on 2026-10-19 failed qualification: an uncached node was selected and image unpacking crossed the43GB disk reserve before cancellation cleanup completed (lowest observation40,334,802,944 bytes). Free space recovered above43GB, the late cancelled start was denied401, zero grants/READY/artifacts were issued and all per-attempt resources retired. At that checkpoint the dedicated controller was scaled to zero pending approved verifier placement, now implemented and tested as recorded below. Dedicated PostgreSQL16.15/TLS and five preserved versioned storage fixtures are ready; none of this closes the four native gates. See `evidence/phase-1/native-verifier-fixtures-2026-10-19.yaml`.

Current execution order: (1) integrated refit/evaluator/one-shot object grants; (2) complete retry, checkpoint and side-effect recovery; (3) database pooler, migration and schema qualification; (4) deletion and public contract gaps; (5) requirement-by-requirement final verification. Reuse the existing workflow tables and reconcilers wherever they satisfy the actual contract.

The 2026-10-17 import work adds exact-version safe readers, approved public configuration on standard and Ray pods, source-aware profiling and feature contracts, promotion guards, operator cancellation and protected descriptor deletion. Four of six import integration gates now pass: schema/migrations, registration, consumers and connected regressions. Native denial of raw/final capabilities and a current deployment remain unqualified; ambient worker object-store credentials are still a gap. No additional whole-Phase1 requirement is marked verified.

The in-progress attempt credential-grant expansion now passes its structural schema gate at head0014, restoring W12. All six remaining integration gates stay open; the expansion and read issuer do not qualify the complete broker or worker credential boundary.

The 2026-10-19 metadata-publication fix removes the duplicate native descriptor write for new provider imports. Canonical descriptor hashes remain bound to the protected signed receipt, and READY/artifact/split/response metadata commit atomically. Provider training estimates use signed train-object sizes and protected train/validation counts; restart resolves protected provenance without requiring a physical descriptor. Existing native descriptors remain compatible. Legacy uncertain descriptor writes, unknown Kubernetes creates and native workload qualification remain open; no whole-phase requirement changes status.

| Requirement | Required outcome | Current evidence / next verification | Status |
| --- | --- | --- | --- |
| P1-W01 | Extend the existing upload-session table and add workflow-command, outbox, capacity-reservation, prepared-artifact, split, feature-contract, feature-registry, feature-recipe, catalog, candidate, attempt, trial, checkpoint, event, and digest-verification tables with project-scoped integrity constraints. | All16 named table concepts are present, extending the original upload-session table. Current0018 schema parity, non-null ownership paths to projects, all92 validated/enabled foreign keys and all46 declared project-to-project references are verified; anti-joins find no invalid existing references and have positive/negative controls. The upload-session gap is fixed with preserved SET NULL semantics, four denial tests, atomic bad-history rejection and exact downgrade/re-upgrade preservation. All341 connected tests and16 prior-schema rehearsals pass. This verifies the schema work item; full Phase1 quality, runtime and prerequisite gates remain separate. Evidence: table-ownership-2026-10-20.yaml and upload-session-project-2026-10-19.yaml.  The new head preserves all92 validated foreign keys and368 enabled enforcement triggers; strict parity additionally verifies four critical application triggers (predecessor-lineage-2026-10-20.yaml). | Verified |
| P1-W02 | Represent splitter, preparation, training run, training trial, champion refit, champion evaluation, and typed provider-stage conformance attempts explicitly. Each row stores its parent scope or conformance campaign, stage, project-and-stage workload identity, generation/fence, RayJob/RayCluster IDs where applicable, heartbeat/lease, checkpoint, terminal CAS version, and cleanup state. A generic table is acceptable only if typed constraints and foreign keys make invalid parent/stage combinations impossible. | Refit execution, fenced PostgreSQL publication, sealed-policy checks and reconciler-owned planning/replacement are tested. Attempt-scoped Kubernetes Jobs now have durable submission, failure observation and cleanup. Local k3d verified normal/lost-response/concurrent execution, one replacement after Job deletion, and a matching-policy network probe. Synthetic native evaluator integration, lost Job creation acknowledgement and completed-scope replay now pass on k3d. Live pre-open/post-open evaluator death tests are recorded; Native stored-output and lost-commit-response recovery passed on the current normal Dockerfile image on 2026-10-07/08: each consumed one grant, retained one commit, completed cleanup and passed two replays. Other backends and the full training-to-evaluation journey remain open. Champion contract gates G2/G3/G4 were audited against assertions and freshly passed 61/87/146 overlapping tests on 2026-10-07; overall champion gates are 5 met / 2 unmet. The predecessor foreign key now also binds project ownership: insertion/update denials cover all seven stages; invalid legacy lineage blocks upgrade without row rewriting, and valid downgrade/re-upgrade preserves rows. Evidence: predecessor-project-2026-10-19.yaml. Same-project parent/run and full current native qualification remain Open. Trial attempts now bind both run and candidate ownership, including trial-ID updates and reciprocal parent changes. Actual row-lock races pass under read committed and serializable; repeatable-read ownership writes fail closed. Full typed-generation/native journey qualification remains Open. Evidence: trial-parent-2026-10-19.yaml. New distributed dataset attempts and the splitter handoff now record project-and-stage service-account names; historical replay retains its original identity.229 connected regressions include both-stage manifest propagation and three overlapping PostgreSQL creation/replay/handoff cases. Other stage identity and native/legacy qualification remain Open (dataset-identity-2026-10-20.yaml).  Declared predecessor edges now enforce same-stage/same-parent monotonic retries or the matching-profile/dataset splitter-to-preparation handoff. Self-links, multi-row cycles and reciprocal parent changes are rejected.755 connected tests include155 focused cases,12 actual lock races and14 repeatable-read denials. Missing legacy links, complete stage identity/broker wiring and native qualification remain Open (predecessor-lineage-2026-10-20.yaml). | Open |
| P1-W03 | Replace session-level worker locks with leased queue rows claimed using `FOR UPDATE SKIP LOCKED`. | Real SKIP LOCKED claims produced 32 disjoint rows across four concurrent PgBouncer backends; stale cached ownership/counter regressions fixed. Outbox delivery now checks ownership before dispatch and after upload verification commits (14 PostgreSQL cases, 190 regressions; outbox-delivery-fence-2026-10-10.yaml). Local profiling now has durable claims, heartbeat and fenced checkpoints (13 PostgreSQL cases, 171 regressions). The unused generic attempt claimer now refreshes locked rows and preserves retry budgets (58 checks). Native current-code worker recovery and the complete lock inventory remain unqualified. MLflow recovery now commits SKIP LOCKED claims with owner/heartbeat/expiry and retry budgets before remote reads, renews through independent short transactions and fences publication on unchanged live ownership/lineage. 297 related regressions/52 overlapping focused cases pass, including actual process kill, zero/exhausted budgets, malformed JSON/intent fairness, typed scope changes and parent-writer races; no lookup caller DB connection/row lock is held across I/O. Evidence: tracking-leases-2026-10-16.yaml. The inline writer now also commits an exclusive renewable lease and irreversible creation marker before database-free MLflow I/O, then checks current parent/command ownership before publication. 321 related regressions/76 overlapping focused cases pass, including actual writer SIGKILL before/after remote creation, competing owners and lost commit acknowledgements (tracking-writer-2026-10-16.yaml). Full queue/pool/native inventory remains Open. | Open |
| P1-W04 | Add `Idempotency-Key` handling to upload completion, training launch, validation, deployment, governance generation, monitoring ingestion, and cleanup execution. | Training restart/add-model actions now persist their complete replay response before mutable prerequisites. PostgreSQL replay, legacy response, lineage, rollback and two-connection races pass (14 new cases; 87 training/schema/route regressions). Shared durable/contract replay now rechecks current project permission; admin-only actions retain ADMIN (24 authorization and 250 regression checks; replay-authorization-2026-10-09.yaml). Other mutation families and deployed end-to-end qualification remain open. Cached command rows now refresh before replay; missing durable responses return a pending conflict and confirmed empty/legacy nonempty responses replay safely.388 connected/23 overlapping focused tests include actual two-connection insert races; all seven canonical header/dispatch boundaries are inventoried. Full family/native qualification remains Open (command-replay-2026-10-20.yaml). | Open |
| P1-W05 | Store request hashes with idempotency records. Reusing a key with a different request returns a deterministic conflict. | Restart/add-model action hashes reject changed tuning before mutable catalog checks; legacy launch replay also checks tuning and run lineage. PostgreSQL conflict and compatibility cases pass. All 21 public idempotency headers now reject empty, whitespace-only and over-255-character keys (41 boundary/route checks, including overlapping regressions). Full command-family coverage remains open. Cached command rows now refresh before replay; missing durable responses return a pending conflict and confirmed empty/legacy nonempty responses replay safely.388 connected/23 overlapping focused tests include actual two-connection insert races; all seven canonical header/dispatch boundaries are inventoried. Full family/native qualification remains Open (command-replay-2026-10-20.yaml). | Open |
| P1-W06 | Record desired state before object-store, Kubernetes, registry, or deployment side effects. | Training MLflow creation records durable intent and atomic lineage; read-only reconciliation binds visible uncertain outcomes. SDK HTTP retries are now disabled so a committed create followed by a 503 or connection drop cannot silently create another run. Both real HTTP faults and unresolved-replay handling pass in 72 checks (tracking-http-retries-2026-10-12.yaml). The inline writer now commits its lease before all remote calls and its irreversible creation marker before POST; failed/stale/cancelled owners cannot publish. Actual process deaths and lost initial/marker/publication commit acknowledgements pass in 321 related/76 overlapping focused cases (tracking-writer-2026-10-16.yaml). A crash or lost marker acknowledgement before POST can remain permanently unresolved: no unsafe blind retry is enabled. Permanent unknown-outcome resolution, legacy records and the full side-effect inventory remain open. | Open |
| P1-W07 | Build reconcilers for unfinished uploads, Kubernetes resources, registry artifacts, Ray jobs/trials, deployments, governance objects, and deletions. | Read-only MLflow reconciliation validates and binds visible uncertain creation outcomes without restarting training. Queue claims now skip locked run/command pairs before applying the limit; missing application-run rows cannot starve later work (34 PostgreSQL/tracking/worker checks; tracking-claims-2026-10-12.yaml). The full reconciler inventory, permanent unknown-outcome resolution, orphan-history recovery and native qualification remain open. MLflow recovery now commits SKIP LOCKED claims with owner/heartbeat/expiry and retry budgets before remote reads, renews through independent short transactions and fences publication on unchanged live ownership/lineage. 297 related regressions/52 overlapping focused cases pass, including actual process kill, zero/exhausted budgets, malformed JSON/intent fairness, typed scope changes and parent-writer races; no lookup caller DB connection/row lock is held across I/O. Evidence: tracking-leases-2026-10-16.yaml. The inline writer now also commits an exclusive renewable lease and irreversible creation marker before database-free MLflow I/O, then checks current parent/command ownership before publication. 321 related regressions/76 overlapping focused cases pass, including actual writer SIGKILL before/after remote creation, competing owners and lost commit acknowledgements (tracking-writer-2026-10-16.yaml). Full queue/pool/native inventory remains Open.  Grant-aware project/run cleanup now revokes affected credentials and waits for the latest original started GET/PUT deadline, including uncertain outcomes and associated scopes, before side effects. Outbox waiting preserves failure budgets; post-expiry metadata cleanup respects grant foreign keys and unrelated data. All384 focused/integration/connected cases pass (grant-retirement-2026-10-18.yaml). This closes only the deletion-retirement increment; replacement and native qualification remain open. Champion cleanup now checks ownership and observed UIDs, uses native UID delete preconditions and waits for confirmed absence; evaluator key cleanup honors recorded custody namespaces and preserves ambiguous scope keys. All167 connected/38 overlapping focused tests and nine local k3d API-only checks pass (champion-cleanup-2026-10-20.yaml). Complete native workflows and the full inventory remain Open. Missing refit submission custody now retains incomplete cleanup; scope deadline loss during observation fails closed.152 connected/29 overlapping focused tests pass (refit-recovery-2026-10-20.yaml); legacy custody recovery and full qualification remain Open. | Open |
| P1-W08 | Define one retry owner and map database attempt generations to KubeRay jobs, Tune trials, and Train runs. No controller may create an untracked nested retry. | KubeRay submission, candidate Ray tasks and Tune failure retries are explicitly disabled. Fixed CPU CV and default local CPU parameter search now use durable child trials with committed result replay; search revision, seed, dimensions and suggestions are identity-bound (70 current search/runtime checks; local-cpu-search-2026-10-13.yaml). Clustering sweeps now also commit ownership before fold preparation and replay stored metrics (clustering-trials-2026-10-14.yaml). GPU-selected CV/search and distinct CPU-fallback evaluations now enroll under accelerator-bound identities (86 checks using simulated GPU selection; accelerator-trials-2026-10-14.yaml). Standalone Tune now also commits suggestion enrollment and Ray dispatch events before scheduling, with explicit-context leased workers and same-parent result replay (tune-trials-2026-10-14.yaml). Reconciler-created pending trial replacements now bind valid execution identities; exhausted child budgets stop parent replacement (trial-replacement-handoff-2026-10-14.yaml). Completed CV results/failures now support explicit digest-verified predecessor adoption, including skipped replacement generations (trial-result-adoption-2026-10-14.yaml). Real GPU qualification, production Train integration, signed grants, predecessor object-checkpoint authorization, complete retry ownership and native failure qualification remain open. Refit replacement now requires confirmed predecessor cleanup and retirement of all recorded started GET/PUT grants. Terminal cleanup and sweeps retain credentials/policies until original deadlines pass;194 connected/34 overlapping focused cases pass. Unjournaled URLs and all-stage/native replacement qualification remain Open (refit-retirement-2026-10-20.yaml). Evaluator replacement keys and nonterminal publication/control now enforce predecessor cleanup/recorded expiry too. Terminal/key-only cleanup preserves dependencies, and the controller can resume registered replacements after known predecessor retirement.184 connected/44 overlapping focused tests pass; legacy/native/all-stage qualification remains Open (evaluator-retirement-2026-10-20.yaml). | Open |
| P1-W09 | Define the `ReleaseFinalTestAuthority` protocol and signed receipt schemas, including unique allocation, canonical provider/scope assignment, one-shot open, result commit/failure, provider-distribution manifests, and provider- local reference validation. Implement it in a separately protected, strongly consistent qualification-control store; provider databases are consumers, never competing authorities. | Authority/broker, evaluator planning, credential handoff, scheduling and recovery have local evidence (398-test regression); synthetic k3d success/lost Job acknowledgement/replay passed. Signed provider-distribution contracts and allocator-only atomic issuance now use existing central receipts. Canonical manifests alone contain final objects; other providers receive versioned safe-role declarations and opaque final-role digests. A trusted-key library/CLI verifies independent allocation/project/scope/provider/fixture/split references. 121 related/54 overlapping focused tests pass, including conflicting concurrent issuance, PostgreSQL backend death before commit, tampering/coercion/references and distribution-to-one-shot-result integration (provider-distribution-2026-10-16.yaml). Full live failure recovery, automatic dataset import/admission enforcement, production integration and deployed provider qualification remain open. The authority now binds a globally unique fixture/split revision pair in PostgreSQL (head0012/dedicated SQL0002), keeps legacy rows unbound and rejects distribution relabelling. 185 related/23 overlapping focused checks pass, including concurrent cross-scope/digest conflicts, direct database constraints, cached-state refresh, legacy SQL compatibility and guarded downgrade (allocation-revisions-2026-10-16.yaml). Controller allocator selection now binds protected credentials to the persisted local project UUID and permitted authority reference; policy text cannot select another project's token. Reference-keyed legacy token files fail closed; all controller/probe callers and operator instructions are updated. 118 related/18 overlapping focused checks pass, including forged references, separate projects sharing references, real PostgreSQL selection and Helm isolation (authority-project-binding-2026-10-16.yaml). Protected Secret conversion is required before deploying this source. The provider-local CLI can now verify actual version-pinned safe Parquet copies under explicit disk/decode/deadline budgets: byte hashes/sizes, logical schema, row IDs/counts/digests and train/validation content separation. 167 related/32 overlapping focused tests and a local TLS S3 signed-copy probe pass (provider-bytes-2026-10-16.yaml). This produces a copy proof; it does not register a dataset or enforce import mode in admission. Promotional scope creation now records a closed project/scope/split/experiment/policy-bound authority command; the controller registers it before training, sealing requires its unchanged completion, and evaluator handoff reuses the pinned allocation. Stopped pre-training scopes recover lost replies and retain signed abort evidence without a final grant. 393 related/39 overlapping focused tests pass, including committed PostgreSQL lock/connection release, stale caches, retries/leases, pending API conflicts and synthetic seed ordering (early-allocation-2026-10-16.yaml). Legacy scopes without early registration fail closed; signed import mode/revision-bound distribution integration, protected rollout and native qualification remain required. Typed provider-import provenance, explicit manifest source kind and durable operator registration now have local proof (161 related/27 registration cases; provider-import-2026-10-16.yaml). Actual descriptor bytes are hashed; prepared/split evidence commits only under current ownership. Import integration remains unfinished:2/6 gates met, with exact-version consumers, validation_only admission/replay, CLI/configuration/profiling/deletion and native denials still required. Evaluator recovery now has42 focused/105 connected passing checks for signed state and abort/failure receipt boundaries, including valid signatures with wrong bindings; four helpers have no missing measured branches. This is a scoped AWS fixture qualification; production code is unchanged and all-provider/native requirements remain Open (evaluator-recovery-boundaries-2026-10-20.yaml). | Open |
| P1-W10 | Bound PostgreSQL pools; configure connect, statement, idle-transaction, and lock timeouts; validate TLS CAs; set application names; export pool metrics; and qualify PgBouncer transaction mode. | Pool/TLS configuration exists; API Prometheus pool gauges pass saturation tests and a live TLS k3d PostgreSQL scrape. Local real PgBouncer transaction-mode tests now verify transaction-local limits, backend reuse, TLS and rollback/reconnect. Other process pools, scrape configuration, concurrent claims, pooler deployment and failover qualification remain. MLflow recovery now commits SKIP LOCKED claims with owner/heartbeat/expiry and retry budgets before remote reads, renews through independent short transactions and fences publication on unchanged live ownership/lineage. 297 related regressions/52 overlapping focused cases pass, including actual process kill, zero/exhausted budgets, malformed JSON/intent fairness, typed scope changes and parent-writer races; no lookup caller DB connection/row lock is held across I/O. Evidence: tracking-leases-2026-10-16.yaml. The inline writer now also commits an exclusive renewable lease and irreversible creation marker before database-free MLflow I/O, then checks current parent/command ownership before publication. 321 related regressions/76 overlapping focused cases pass, including actual writer SIGKILL before/after remote creation, competing owners and lost commit acknowledgements (tracking-writer-2026-10-16.yaml). Full queue/pool/native inventory remains Open. Refit and evaluator read-capability endpoints now release caller SQL connections/locks before provider inspection/signing, then reload and revalidate current parent/attempt, plan and original deadline before URL disclosure. 117 related/22 overlapping focused PostgreSQL/control/worker/authority cases pass, including concurrent cancellation/supersession, plan replacement, deadline changes and heartbeat renewal (read-grants-2026-10-16.yaml). This does not revoke previously disclosed URLs or qualify all worker/provider grants. | Open |
| P1-W11 | Implement expand/backfill/contract migrations. The previous release must continue to read during expansion and backfill. | Current readers and literal filters now handle legacy enum names and migration/default values across thirteen columns, preserving existing writes and widths (12 final default/filter checks; all 625 preexisting regression checks pass). Previous-release binaries and concurrent expand/backfill behavior remain unqualified. | Open |
| P1-W12 | Upgrade large artifact byte-size columns to `BigInteger` where 32-bit limits can be exceeded. Verify every column, type, constraint, index, default, and foreign key after migration rather than checking only table names and head. | Current head0018 preserves all eight large byte-size fields as BigInteger. Strict parity covers43 application tables plus Alembic metadata,657 application columns,222 application constraints,92 foreign keys,47 nonconstraint indexes and four critical trigger contracts. All17 prior schemas upgrade with preserved seeded data and strict verification;14 upload-capable paths preserve10000 metadata rows each.755 connected tests and155 overlapping focused cases pass.45GB and actual prior-binary qualification remain required under W11/W25/G05. Evidence: predecessor-lineage-2026-10-20.yaml. | Verified |
| P1-W13 | Introduce catalog and candidate APIs while retaining selected-model compatibility. | Catalog/runtime and candidate-history implementation remains recorded in catalog-binding-2026-10-15.yaml and candidate-detail-2026-10-15.yaml. Current prepared legacy selected requests now resolve immutable experiment/objective identities; estimate, launch, reservation replay, UI and follow-up actions retain validated lineage. 859 Python regressions, 40 overlapping focused cases and 37 UI workflow tests pass without failures or skips; TypeScript build and scoped Ruff checks pass. Evidence: selected-identity-2026-10-15.yaml. Historical data/spec/command-protocol migration, signed catalog/runtime variants and deployed-image qualification remain required. Actual normal API/CPU images now enforce reviewed complete package/source profiles and consume matching four-task catalogs; isolated immutable k3d Jobs pass one small Ridge search/fit/serialization path, exact cleanup and QA identity preservation. 1154 related regressions, 35 overlapping profile cases and 10 overlapping release-validator cases pass without failures/errors/skips. Evidence: runtime-pair-2026-10-15.yaml. Full native workflows, historical compatibility, signed/runtime variants and the remaining qualification stay Open. | Open |
| P1-W14 | Add typed feature contracts, feature-search requests, estimator overrides, and version-conflict responses. Remove the generic client-controlled parameter dictionary before exposing Ray execution. | Closed feature/search/objective specifications and immutable raw-view bindings remain implemented. Estimator-specific closed parameter schemas now validate reviewed types/ranges/categories/conditions and bind estimate/reservation, original command, run, candidate and trial identity. Actual fixed/local-search/Tune/clustering fits preserve requested values; replay/restart/add-model and tamper checks pass. 1092 Python regressions and 63 overlapping focused cases pass without failures/errors/skips; scoped Ruff and formatting checks pass. training-parameters-v1 prevents older consumers silently ignoring overrides; known QA predecessor parser rejects it read-only (estimator-overrides-2026-10-15.yaml). Historical generic data/request protocols and deployed-image qualification remain required. Advanced engineering/joint search and serving parity remain unqualified. Actual normal API/CPU images now enforce reviewed complete package/source profiles and consume matching four-task catalogs; isolated immutable k3d Jobs pass one small Ridge search/fit/serialization path, exact cleanup and QA identity preservation. 1154 related regressions, 35 overlapping profile cases and 10 overlapping release-validator cases pass without failures/errors/skips. Evidence: runtime-pair-2026-10-15.yaml. Full native workflows, historical compatibility, signed/runtime variants and the remaining qualification stay Open. | Open |
| P1-W15 | Migrate legacy storage enum values and URIs without reinterpreting persisted objects. Add a no-fallback startup gate for unknown production drivers. | ORM now reads all eight storage identities as persisted names or lowercase migration values and rejects unknown values. Actual 0007-to-head dataset-version migration preserves URI/hash/size and expected identities (133 focused tests; 625 regression tests). Previous-release/deployed-image compatibility and full driver qualification remain open. | Open |
| P1-W16 | Version content digests by algorithm and scope. Backfill or quarantine legacy resumable uploads whose stored hash describes a part manifest rather than object bytes. | Direct/resumable writers and consuming readers require explicit verified SHA-256 byte-stream provenance; earlier bounded QA scan/CAS repair preserved original hashes for13dataset versions (qa-digest-metadata-repair/digest-readers evidence dated2026-10-16). The storage protocol now supports exact S3 VersionId/GCS generation/Azure version_id reads and rejects missing/denied/unsupported versions without current-object fallback. Azure chunk reads avoid eager whole-object buffering. 273 related/46 overlapping focused checks pass; actual local TLS S3 old-version/range reads and cleanup preserved16existing version records and Pod/PVC identities (versioned-streams-2026-10-16.yaml). This primitive is not yet wired through all readers/import/distributed grants. Production legacy-upload inventory/migration, controlled old-reader compatibility and current-image/provider qualification remain required. | Open |
| P1-W17 | Persist deletion tombstones and durable deletion stages for raw objects, multipart units, prepared data, samples, feature artifacts, Ray spill and checkpoints, Tune state, MLflow artifacts, models, logs, replicas, and backups. | Fixed evidence loss: project deletion retains tombstones and existing stage history; migration 0011 removes cascading project foreign keys while preserving the composite stage/tombstone tenant constraint. PostgreSQL retention/replay and guarded downgrade checks pass. Project/run requests now persist runtime, object-store, prepared-store (project only) and metadata stages, advancing one per outbox delivery. Real PostgreSQL lost-response recovery and active-writer guards pass (24 final integration cases). S3 prefix cleanup now drains explicit versions, delete markers and multipart uploads; 20 storage checks and a verified-TLS local SeaweedFS probe pass. S3 exact-object cleanup now uses the same version/multipart purge, with SDK-page filtering and local neighbor-preservation evidence. Upload cleanup no longer skips hidden history when the current object is absent; missing Azure/GCS objects are idempotent. GCS generation cleanup and retained-history waits now pass local SDK-contract and PostgreSQL checks (63 focused, 270 combined; overlapping counts). Retention defers through the existing outbox without exhausting retries; uploads waiting on retention release their cleanup batch slot. Azure exact/prefix cleanup now enumerates versions, snapshots and uncommitted blobs and waits for retained history; 63 focused and 278 combined checks pass (overlapping counts). Normal-image qa-refit deployment, schema 0011/42-table verification and a new native lost-ack deletion fixture now pass. A rollout mistake discarded the pre-existing emptyDir-backed QA store; the initial inventory found 13 missing dataset/artifact URIs. Expanded inventory found 57 referenced objects; 50 were restored with original hash verification, leaving two fitted models and five signed results missing. PVC/restart protection is repaired, but historical recovery remains open (native-deletion-and-qa-storage-incident-2026-10-09.yaml). Native Azure/GCS qualification and external MLflow/spill/replica/backup coverage remain required.  Grant-aware project/run cleanup now revokes affected credentials and waits for the latest original started GET/PUT deadline, including uncertain outcomes and associated scopes, before side effects. Outbox waiting preserves failure budgets; post-expiry metadata cleanup respects grant foreign keys and unrelated data. All384 focused/integration/connected cases pass (grant-retirement-2026-10-18.yaml). This closes only the deletion-retirement increment; replacement and native qualification remain open. | Open |
| P1-W18 | Define state transition functions centrally and reject impossible transitions at service and database boundaries. | Current training candidate phase/partial/final status updates now share the existing parent fence and transaction, retain terminal evidence and reject contradictory outcomes. Real PostgreSQL stale-write/atomic rollback and receipt checks pass (508 related tests; candidate-detail-2026-10-15.yaml). The complete service/database transition inventory, historical data and cancellation/deletion qualification remain required. Scope sealing/member admission and training scope resolution now refresh current scope state under the row lock. Committed PostgreSQL tests retain an old OPEN object across another session’s cancellation and reject sealing/member admission (early-allocation-2026-10-16.yaml). | Open |
| P1-W19 | Add lease expiry, heartbeat, retry budget, terminal reason, and operator replay fields to every durable worker workflow. | Local profiling, training parents/trials, splitter/preparation and new analysis enforce durable ownership, heartbeats, expiry and fenced publication. Dataset and analysis observers queue exact old-generation cleanup plus bounded replacement. First-claimed pending CPU legacy analysis now upgrades through an absence-checked deferred intent with a declared worker protocol (961 regressions, 57 focused cases, three k3d probes and a read-only predecessor-parser check; analysis-migration-2026-10-15.yaml). Earlier scoped evidence remains in remediation-progress and the evidence directory. Running/retried/ambiguous legacy and missing-evidence migration, inconsistent profile cleanup, operator replay coverage, provider delegation, deployed process-loss/current-image/TLS/rotation, replica fairness and the full workflow inventory remain required. Refit and evaluator read-capability endpoints now release caller SQL connections/locks before provider inspection/signing, then reload and revalidate current parent/attempt, plan and original deadline before URL disclosure. 117 related/22 overlapping focused PostgreSQL/control/worker/authority cases pass, including concurrent cancellation/supersession, plan replacement, deadline changes and heartbeat renewal (read-grants-2026-10-16.yaml). This does not revoke previously disclosed URLs or qualify all worker/provider grants. | Open |
| P1-W20 | Kill the API before and after each external side effect. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-W21 | Replay each command and outbox entry at least twice. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-W22 | Run multiple worker replicas against the same queue. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-W23 | Force PostgreSQL connection loss after claim and before commit. | Real PgBouncer backend termination after outbox claim flush and before commit rolled back ownership/counter; another worker recovered delivery. Other claim paths and deployment/failover qualification remain. | Open |
| P1-W24 | Reuse idempotency keys concurrently and with conflicting payloads. | Two-connection PostgreSQL add-model races pass: identical requests produce one child, changed tuning produces one deterministic 409, and different models retain both parent links. Runtime launch is stubbed; other mutation families and native failure coverage remain open. | Open |
| P1-W25 | Rehearse migrations from every supported prior schema with production-scale row counts. | All17 supported prior schemas reach head0018 with strict structural verification and no Alembic drift. Fourteen upload-capable paths preserve10000 metadata rows each; seeded authority/import fields are checked where present. Six dedicated fixtures preserve all existing fields and prior trigger definitions through downgrade/re-upgrade, or block bad declared lineage atomically without rewriting history.755 connected tests pass. These are bounded metadata fixtures; representative full workflow data, actual prior binaries and45GB qualification remain required. Evidence: predecessor-lineage-2026-10-20.yaml. | Open |
| P1-W26 | Kill the reconciler between database attempt creation, Ray submission, Ray ID persistence, checkpoint publication, and terminal writes. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-W27 | Kill a run with multiple active trials and prove one transaction supersedes every old trial attempt before replacements can write. Exercise attempt-grant expiry and predecessor-checkpoint allowlists. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-W28 | Race final-allocation/open/commit requests from all three provider identities; exactly one canonical request may advance state and every non-canonical or duplicate request must fail without receiving an object credential. | Authority/broker, evaluator planning, credential handoff, scheduling and recovery have local evidence (398-test regression); synthetic k3d success/lost Job acknowledgement/replay passed. Signed provider-distribution contracts and allocator-only atomic issuance now use existing central receipts. Canonical manifests alone contain final objects; other providers receive versioned safe-role declarations and opaque final-role digests. A trusted-key library/CLI verifies independent allocation/project/scope/provider/fixture/split references. 121 related/54 overlapping focused tests pass, including conflicting concurrent issuance, PostgreSQL backend death before commit, tampering/coercion/references and distribution-to-one-shot-result integration (provider-distribution-2026-10-16.yaml). Full live failure recovery, automatic dataset import/admission enforcement, fixture/split revision-pair allocation uniqueness, production integration and deployed provider qualification remain open. | Open |
| P1-G01 | Repeating creation and finalization requests returns the same resource. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-G02 | API death around every side effect self-heals through reconciliation. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-G03 | Concurrent dataset/session creation cannot duplicate resources or exceed quotas. | Reverify current implementation and failure coverage against the complete requirement. | Open |
| P1-G04 | Worker replicas process a row once or perform an idempotent replay. | Outbox dispatch now locks and validates the current owner before effects; completed/deleted deliveries are no-ops, and upload heartbeats/publication reject takeover (14 focused PostgreSQL cases, 190 regressions). Local profiling now uses transactional outbox scheduling, heartbeats and fenced checkpoint publication; native worker/process-loss coverage remains open. | Open |
| P1-G05 | Migration rehearsal succeeds from every supported schema and on a production-sized database. | All17 supported prior schemas reach head0018 with strict verification and no Alembic drift;14 paths preserve10000 upload metadata rows each. Invalid declared predecessor links abort expansion atomically, and valid seven-stage history survives downgrade/re-upgrade with previous trigger definitions restored. Owned rehearsal databases are removed. Representative full workflow histories, production-sized45GB data and actual prior binaries remain unqualified. Evidence: predecessor-lineage-2026-10-20.yaml. | Open |
| P1-G06 | Ray resubmission preserves one logical run/candidate/trial history with append-only attempts, rejects stale fences, and never duplicates a terminal artifact or final-test evaluation. | Training result receipt now prevents duplicate/conflicting commits and late progress writes; worker and observer recover committed results through terminal CAS. Eighteen PostgreSQL cases plus 156 regressions pass, including concurrent writers and a real successor attempt (training-result-recovery-2026-10-11.yaml). Local embedded artifact checks now preserve a fitted model and its digest using content-specific keys and atomic replacement (65 combined checks; model-artifact-isolation-2026-10-11.yaml). Native Ray resubmission, remote artifact stores, trial history and complete final-test qualification remain open. | Open |
| P1-G07 | Scope refit is independently retryable and CAS-publishes one pipeline; scope creation/membership/sealing/cancellation is idempotent and releases no member before the exact barrier commits. | Refit execution, fenced PostgreSQL publication, sealed-policy checks and reconciler-owned planning/replacement are tested. Attempt-scoped Kubernetes Jobs now have durable submission, failure observation and cleanup. Local k3d verified normal/lost-response/concurrent execution, one replacement after Job deletion, and a matching-policy network probe. Synthetic native evaluator integration, lost Job creation acknowledgement and completed-scope replay now pass on k3d. Live pre-open/post-open evaluator death tests are recorded; Native stored-output and lost-commit-response recovery passed on the current normal Dockerfile image on 2026-10-07/08: each consumed one grant, retained one commit, completed cleanup and passed two replays. Other backends and the full training-to-evaluation journey remain open. Champion contract gates G2/G3/G4 were audited against assertions and freshly passed 61/87/146 overlapping tests on 2026-10-07; overall champion gates are 5 met / 2 unmet. | Open |
| P1-G08 | Scoped launches remain `barrier_pending` with no outbox or clock until the exact membership transaction seals them, and the central final authority permits one canonical provider/scope to open and commit each locked split. | The early authority command now gates promotional admission and sealing; evaluator handoff cannot create a late allocation. 393 related/39 overlapping focused checks cover registration/replay, barrier rejection, stale cancellation and signed pre-training abort cleanup (early-allocation-2026-10-16.yaml). Full signed import/distribution, production training and native lifecycle qualification remain open. | Open |

Managed provider verification now has a 586-test checkpoint on 88 frozen
connected source inputs, including two local Ray cases. The maintained CLI queues
work; the reconciler creates bounded isolated Jobs, verifies actual UID/spec/policy
authority and fences cancellation, cleanup and replacement. Unknown create or
publication outcomes retain history and block completed cleanup. Ten Helm profiles
render; three k3d server-side dry runs validate declarations only. No current image
or native permission/process-loss qualification was performed. No whole requirement
changes status. See [managed verifier evidence](evidence/phase-1/managed-provider-verifier-2026-10-19.yaml).


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
86.651% branch result was below90.01%. Phase1 remains two Verified/34 Open.
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
