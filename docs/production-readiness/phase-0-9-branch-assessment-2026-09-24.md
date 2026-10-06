# Production readiness assessment: phases 0–9

Assessed branch: `codex/production-readiness`, commit `7a47ed8fe92703b392c6c7a2df41392da4fd653f`. Assessment date: 2026-09-24. The controlling requirements are [implementation-guide.md](implementation-guide.md), [task-index.yaml](task-index.yaml), and the promotion contract in [README.md](README.md). Phase 0A is included. Phases 10–13 are outside this assessment, except where an earlier phase explicitly defers qualification to them.

**The branch is not production ready. None of the eleven phase groups can be certified in full for this HEAD from the available evidence.** This does not mean no work is complete: durable state, native upload adapters, Ray preparation, candidate execution, OIDC, browser sessions, inference integrity checks, and Helm hardening have substantial implementation. The major remaining work is integration of those controls, bounded and statistically qualified ML execution, operational recovery, and immutable release qualification.

This is an assessment, not a deployment, penetration test, cloud campaign, or implementation change. The companion [item matrix](phase-0-9-item-assessment-2026-09-24.csv) accounts for every in-scope work/gate ID. Its open dispositions are conservative: an implementation or unit test is not credited as an environment-qualified gate. Evidence absence means no adequate evidence was found in the assessed repository, not proof that an external system cannot exist.

## Immediate blockers confirmed in code

1. **CI evidence binding is broken.** `scripts/validate_phase2_evidence.py` rejects HEAD because non-evidence files changed after candidate `7eb134c8de0786e6e134262ccf1c25492cc2913a`. Both `.github/workflows/ci.yml` and `scripts/test_backend.sh` invoke this validator before the main test suite. Preserve the historical record; qualify and bind a new candidate rather than merely editing its SHA to make validation pass.
2. **Production Helm cannot configure its required OIDC flow through its current values interface.** `security/authentication_policy.py` requires issuer and client ID when simple authentication is disabled. Production policy requires it disabled, but chart templates expose no OIDC issuer/client/secret wiring. Database CA configuration similarly needs an actual mounted trust bundle, not only a path string. A successful render does not prove API startup.
3. **The rendered production Gateway also routes plain HTTP.** `templates/gateway.yaml` always creates an HTTP listener. Its HTTPRoute has no HTTPS-only `sectionName` or redirect filter and therefore attaches to both permitted listeners. Enabling TLS does not enforce HTTPS.
4. **Ray attempt identities are not effective data isolation.** `services/reconciler.py:_build_ephemeral_ray_job_manifest` injects `settings.database_secret_name/key`, the application DB secret, and `object_store_workload_environment`, which supplies shared S3-compatible credentials. The ordinary Job builder uses the restricted worker secret, but the Ray builder does not. Labels, fences, and service-account names do not restrict what those credentials can read. No NetworkPolicy is rendered. The Ray head also replaces the stage service account with the operator-created autoscaler account.
5. **The active candidate path is not the required durable joint Ray Tune/Train workflow.** The reconciler sets `AUTOML_MODEL_PODS=1`; `candidate_runtime._execute` sets `TRAINING_EXECUTION_MODE=candidate`; `pipeline._fit_candidate` consequently uses `BayesSearchCV`, bypassing the `ray`-mode Tune branch. The separate `tune_runtime.search_candidate` creates a fresh Tuner and lacks a restore path. Recovery of a candidate or run is not proof of durable trial/suggestion recovery.
6. **Large-data training can still collect whole roles into pandas.** `pipeline._load_prepared_role` ends in `dataset.to_pandas()` and permits `max_rows=0`. Positive sampling limits help individual runs, but no universal byte/memory bound establishes the 10 GiB contract. Candidate arguments then enter Ray's object store from those materialized frames. Do not advertise full-data distributed training on the strength of distributed preparation.
7. **Release construction still violates the immutable identity contract.** CI's “Select the next available release version” increments patch numbers according to registry occupancy; builds are AMD64-only. No completed immutable candidate manifest, cosign verification/admission path, or duplicate-build digest comparison was found.

## Phase 0 — baseline and reconciliation

Present: identity schemas, snapshots, immutable task IDs, dynamic migration-head checking, explicit upload/profile flow, Python 3.12 migration, and a Dask runtime guard. The task-index check passes for 420 IDs across the full guide; 339 belong to this assessment.

Missing: a clean reviewed candidate and current evidence bundle; green quality checks; reconciliation of generated API/catalog/schema documentation with runtime; accepted current requirement/guide bindings; and release-CI identity enforcement. The accepted Streamlit ADR still says Streamlit, while the implementation is React. The readiness README still describes missing controls now present and a 13-table schema; fresh migration verification finds 42 tables. Historical snapshots should remain historical, with a separately versioned current candidate inventory.

The initial worktree had a modified `coverage.json`. The branch was two commits ahead of its local upstream-tracking ref; no fetch, push, merge or remote protection review was performed. The source package remains `0.1.5`, rather than a built Phase 8 `0.2.0` candidate. Plotly is lazy-loaded, but the reviewed/enforced route budget still needs evidence.

## Phase 0A — feasibility and authorization

Present: signed local go/no-go and decision register; Ray/Polars, feature recipe, search, estimator, topology, scheduling, TLS and recovery spike artifacts.

Missing: requalification against the current runtime and guide. Historical evidence names Ray 2.56.1 and MLflow 3.14.0; current dependencies use Ray 2.58.0 and MLflow 3.15.1. The signed go/no-go binds an older guide/task-index and authorizes Phase 1 with no external spend. It is not blanket authorization or feasibility proof for today's provider campaigns. Freeze the control-plane RSS bound, approved benchmark identity and numerical criteria, measured runtime/capacity distributions, provider quota headroom, current transport/isolation controls, and revised implementation/campaign cost approvals. Repeat affected recovery and deterministic replay experiments after runtime changes.

The accepted plaintext dashboard exception assumes an isolated per-attempt namespace plus NetworkPolicy. Current Ray manifests use the release namespace without those policies; the exception's conditions cannot simply be assumed satisfied.

## Phase 1 — durable state and contracts

Present: command/outbox leases, request-hash idempotency, CAS/fencing, typed workflow attempts, reconciliation, scope models, deletion tombstones, pool/timeouts/TLS configuration, and migrations. Fresh migration and schema checks succeed.

Missing: live integration of the separately protected release-final authority with credential issuance and dedicated refit/evaluator jobs; full trial/checkpoint replay; production-sized upgrade/backfill and rollback rehearsals from every supported schema; PgBouncer transaction-mode qualification; current multi-replica/API/DB failure evidence; and durable deletion across every store and backup class. `final_test_authority.py`, its models, and `db/qualification_session.py` provide a useful foundation, but not a separately deployed, hermetically built qualification-control service. No application caller of the allocation service was found outside its definitions/tests.

The August Phase 1 “passed” summary describes a dirty historical checkout and migration 0007. It cannot certify HEAD's migration 0010 or newer runtime paths.

## Phase 2 — portable ingestion

Present: S3-compatible, native AWS/GCS/Azure and embedded drivers, resumable browser client across upload workflows, Web Worker hashing, IndexedDB state, quarantine/scanner policy, cleanup reconciliation, and historical local interruption evidence.

Missing: native AWS/GCS/Azure semantics campaigns; exactly 15 concurrent 10 GiB uploads with zero payload proxying and a signed RSS bound; current real scanner/CORS/least-privilege evidence; independent review and immutable evidence receipts. Historical records report required storage of 193,273,528,320 bytes versus 44,253,073,408 available at that test date. These are historical capacity observations, not measurements of today's free space.

Formal historical dispositions are five failed records and two not-started records. P2-G04–G06 had local technical success but lacked independent approval; P2-G01 lacked native accounts; P2-G02 failed preflight; P2-G03 awaits the exact load; P2-G07 belongs to later provider/release repetitions. Refresh evidence for the new candidate without conflating small smoke tests with the full load requirement.

## Phase 3 — preparation and feature engineering

Present: separately fenced splitter/preparation RayJobs, bounded Arrow/Polars transforms, raw digest verification, positional identities, role Parquet, content-hash and chronological splitting, feature revisions, progress, and retry reconciliation. Dask execution has been removed from the guarded runtime surfaces.

Missing: effective raw/train/validation/final IAM separation and grant revocation; signed validation-only import integration; group isolation, purge/embargo/label-horizon/as-of contracts; statistically qualified task-specific sampling; distributed fold-fitted feature generation and boundary semantics; signed complete lineage; and 15×10 GiB preparation plus remote-stage failure recovery evidence.

Concrete limitations: `split_dataset` accepts target/time columns but no group/purge/embargo contract. `_feature_launch_revisions` registers raw passthrough features and disables lag/rolling/window families; listing allowed families does not implement their qualified execution. The training sample ranks row IDs and salts with a split revision UUID, requiring cross-provider logical-identity review. It does not itself prove class retention, regression-tail coverage or rare-category preservation. Bounded summary collection is permissible; it must not be confused with the full-role pandas collection in training.

## Phase 4 — catalog orchestration

Present: catalog/all-model API and UI support, candidate pods, concurrency policy, cancellation, per-candidate result persistence, deterministic search helper, and run/candidate recovery.

Missing: durable joint feature/model trial expansion; canonical signed suggestion replay; exactly-once trial result/tell/checkpoint recovery; production Ray Train recipes for each approved backend; independently fenced scope champion refit and one-shot evaluation; signed catalog workload/conformance ledgers; quota-aware weighted fairness; and measured 15-run planning, node scaling, quality, resource and deadline gates.

`QUALIFIED_PRODUCTION_CONCURRENCY=15` is a configured constant, not qualification evidence. Admission currently checks counts and bypasses its limits for scope-bound requests; it is not a weighted fair queue. Candidate-local BayesSearchCV and fresh-Tuner execution need reconciliation with the required execution contract. Preserve the existing functionality while completing the durable control plane.

## Phase 5 — portable Helm

Present: strict schema, production rejection rules, Gateway resources, static pod hardening, PDBs, external-service options, KubeRay dependency, ephemeral RayJob templates, and chart tests.

Missing: OIDC/CA wiring, HTTPS-only routing, default-deny policies, real stage/attempt credentials, restricted dynamic workloads including read-only root filesystems, telemetry resources, ordered migration-before-rollout stage, signed platform BOM, and live PSA/RBAC/CRD conversion/upgrade/rollback/cleanup qualification. Migration Jobs are ordinary Jobs; API schema init checks help but do not establish the required ordered deployment stage.

There is also a direct guide/code conflict: the guide requires chart-managed KubeRay by default with explicit external opt-out; `production-policy.yaml` rejects chart-managed KubeRay in production. Resolve through implementation or a reviewed contract change. Production fixtures use placeholder digests and secrets and are render tests, not deployable qualification profiles. External MLflow HA and infrastructure ownership still need evidence.

## Phase 6 — identity, security and serving

Present: OIDC discovery/PKCE/nonce/MFA checks, HttpOnly same-origin browser sessions, short access-token default, Argon2 rehashing, browser refresh coordination, expanded project administration, internal inference authentication and SHA-256 verification before Joblib deserialization. The old README's reset-token/digest/session blockers must be reassessed against these changes.

Missing: deployable OIDC settings and real IdP revocation/rotation/account lifecycle tests; scoped M2M clients; full role/action and cross-attempt/provider negative tests; tamper-evident externally retained audit; effective Ray DB/object least privilege; producer signatures and runtime/project attestation before executable model loading; signed feature bundle validation and strict serving input/parity policy; complete current threat model and approved residuals; transport negative/rotation tests; governed serving-class HA and node/zone loss tests; and DSAR/residency/cryptographic erasure/backup redelete controls.

The serving digest check is valuable but authenticates bytes against a stored digest, not the producer's signing identity. Application-issued sessions still use the platform HMAC token implementation, even though IdP token verification supports asymmetric signatures. Deletion tombstones and legal-hold checks exist; no cohort-key destruction and quarantined restore/redelete implementation was found. Existing scanner/security reports are historical inputs, not current vulnerability clearance.

## Phase 7 — observability, recovery and operations

Present: API structured logging, workflow observation, deployment-linked metric/drift dashboards and governance snapshots, plus audit-retention sweeps outside API startup.

Missing: end-to-end OpenTelemetry propagation; exported bounded-label operational metrics, production dashboards and alert delivery; execution of persisted monitoring schedules; calibrated ML-health thresholds/cohorts and governed retraining responses; searchable log retention backend; managed HA/PITR, cross-store backup and tested RPO/RTO; independent authority recovery; immutable signed external evidence; complete owned runbooks and quarterly drills; and SLO/error-budget and soak evidence. The reconciler schedules upload cleanup and audit purge, but not the requested model-monitoring scheduler. Retention constants alone do not create searchable log storage or backup retention.

## Phase 8 — release engineering

Present: commit-pinned Actions in the main CI workflow, several digest-pinned Python base images, hashed locks for core runtimes, npm lock/ci, split images, non-root app containers, image scans and BuildKit SBOM/provenance generation, and identity-schema tests.

Missing: digest pinning and reproducible dependency installation for every image/helper, ARM64 builds, a hermetic isolated authority artifact, keyless signatures and verified admission, license/waiver/platform-BOM policy, full current secret-scan and rotation evidence, immutable `0.2.0` artifact manifest, same-digest promotion, duplicate-build comparison, private federated deployment runners, protected infrastructure/deploy/qualify/rollback workflows, and rehearsed rollout rollback. Only `ci.yml` and `fastforward-merge.yml` are present; equivalent required infrastructure and qualification stages are absent. UI, RAPIDS, SeaweedFS and PostgreSQL Dockerfiles still have tag-based upstream inputs; MLflow/Ray helper dependency installation is not uniformly hash-locked.

Do not create the final qualification attestation or stable pointer merely to close this phase: the guide correctly reserves those decisions for Phase 13.

## Phase 9 — local runtimes and Railway

Present: `scripts/sceptrectl`, local version file, four Helm profiles, three-node k3d creation, image import, port-forward instructions, and historical k3d tests.

Missing: full fresh install/upload/profile/train/deploy/predict/upgrade/uninstall evidence on all four runtimes; pinned Gateway/metrics/storage bootstrap; qualified 8-core/24 GB 10 GiB profile; and real Railway bucket/database TLS/pooling/capability tests with restricted token handling and explicit unsupported execution documentation. The CLI uses `sceptrectl <command>`, not the documented `sceptrectl local <command>` form. kind creation does not pass the pinned Kubernetes image, Minikube creation does not pass a Kubernetes version or enable the required add-ons, and Gateway installation is absent. `verify` runs Helm smoke tests, not the whole product journey. `upgrade` is not atomic.

## Recommended completion order

1. Restore a reviewable green branch: lint/tests, fresh candidate evidence bindings, documented runtime/toolchain, and current architecture/baseline records.
2. Close deployability and isolation blockers: OIDC/CA wiring, HTTPS-only Gateway, NetworkPolicy, restricted Ray DB access and real attempt-scoped object grants; reconcile the KubeRay ownership contract.
3. Complete and qualify preparation/trial/refit/evaluation integration, bounded memory and statistical safety. Freeze RSS/cost/benchmark thresholds and execute the exact ingestion/preparation/reference campaigns.
4. Build operational monitoring, alerting, cross-store restore and deletion guarantees; exercise failure/runbook gates.
5. Build/sign one immutable candidate; run all local/Railway conformance gates. Managed-provider and final production qualification still follow in phases 10–13.

## Current verification

| Check | Current result | Qualification limit |
| --- | --- | --- |
| Task index | Pass: 420 IDs; 339 in scope | Index consistency is not completion evidence |
| Phase 2 evidence validator | Fail: non-evidence changes after recorded candidate | Also fails its backend regression test and blocks CI |
| Ruff | Fail: 11 findings | Imports, line length and one unused loop variable; source was not modified |
| Python compile | Pass | Syntax only |
| Alembic head / fresh upgrade / schema / autogenerate check | Pass: one head, `0010_phase3_contract_target`, 42 tables, no new upgrade operations | Disposable local PostgreSQL; not production-sized HA/restore qualification |
| Migration rehearsal | Pass: all five configured start revisions → head; 10,000 legacy upload rows in applicable paths | Local rehearsal; not a measured production-sized migration campaign |
| Backend regression | 1,256 passed, 1 failed, 14 fixture setup errors in full run; all 14 affected tests passed in corrected dedicated DB rerun | Effective combined result: 1,270 passed, 1 failed; not one entirely green invocation |
| UI unit suite | 213 passed in 21 files | Existing installed dependencies; no new coverage run |
| UI lint/build | Pass | Node 22.22.1 is below package requirement Node ≥24 |
| Build size | Entry JS 572.35 kB; lazy Plotly chunk 4,846.59 kB | Build warns about chunks over 500 kB; no reviewed route budget proved |
| Default Playwright | 4 passed, 40 skipped | Live-cluster/account/data scenarios require explicit fixtures; not full product conformance |
| Helm lint / production fixture render | Pass | No live admission, RBAC denial, network, upgrade or rollback certification |

The first backend attempt used `.venv` and stopped with 32 collection errors because dependencies including Argon2 and Joblib were absent. Its console entry points also retained an obsolete workspace interpreter path; module invocation worked. The complete run used the existing `.venv-ci312`: Python 3.12.13, Ray 2.56.1, MLflow 3.14.0, sklearn 1.9.0, Polars 1.43.2 and PyArrow 24.0.0. Ray/MLflow differ from the current lock requirements, so these are useful regression observations, not exact-runtime release qualification. The 14 fixture errors were an audit setup issue: the project-management fixture requires a database name ending `_tests`; its corrected rerun passed all 14. They are not reported as application defects.

CI does not currently set `SCEPTRE_TEST_DATABASE_URL`, so those project-management tests would skip under the shown workflow even after the earlier evidence failure is repaired. Add the dedicated database setup and an explicit skip policy. `kubeconform` and `conftest` were unavailable locally. No current coverage percentage, cloud security clearance, complete live-cluster suite, 150 GiB transfer campaign, restore drill, or release reproducibility claim is made.

Raw local check logs and the unlazy acceptance ledger are under `.unlazy/readiness-audit/`. The pre-existing `coverage.json` edit was preserved. Only assessment artifacts were added to the repository; no application fixes, commits, pushes or deployment changes were made.

The item matrix uses `open` for a requirement with a related partial baseline whose full conjunctive outcome remains unproved; `partial` and `missing` identify more specific reviewed gaps; `unqualified` means required environment evidence is absent/stale; `observed_pass` has only the narrow scope stated in its evidence. These are audit dispositions, not a percentage-complete estimate.
