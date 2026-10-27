# Phase 1 attempt identity and credential contract

Status: **in progress**. The pod-bound admission API, Ray token projection and training parent/trial, splitter/preparation and new non-Ray analysis admission enforcement are implemented. Separate child grants, delegated object credentials and native qualification remain required. This contract preserves the complete attempt-grant requirement; admission alone does not close it.

The existing Ray manifest deliberately gives a KubeRay head its cluster-specific autoscaler service account. Workers and submitters use the recorded project/stage identity. Authentication must accommodate that controller-owned head without accepting arbitrary service accounts or trusting labels alone.

## Admission boundary

`POST /api/v1/internal/attempts/{attempt_id}/admission` uses Kubernetes TokenReview with one expected audience derived from the attempt ID and a hash of its current fence. The attempt must be active and its recorded lease unexpired when present. Training admission also checks the run's active state and recorded namespace. Dataset admission checks the exact stage/profile logical key, project/version, active profile status and current attempt pointer before TokenReview.

The review must attest a pod name and UID. Admission rereads that pod and checks its UID, deletion state, namespace, project/attempt/generation labels and service-account subject. A project/stage worker must use the recorded identity. A different service account is allowed only for a head with a matching Pod → RayCluster → recorded RayJob controller UID chain, cluster-specific service-account name, head label and matching job scope labels. Deleting controller objects are rejected.

Admission owns two short database transactions. It reads a frozen workload/source binding, closes the transaction and connection, then performs TokenReview, Pod and Ray controller lookups. A fresh transaction takes the project lock before the attempt, checks current parent/source state, compares the binding and appends identity evidence atomically. Project/run deletion tombstones, archived projects/sources, changed fences, identities, namespaces, jobs and source revisions deny admission. The lease is checked again after source validation so a SQL wait cannot admit an expired attempt. Same-token replay preserves one evidence row; a lost commit acknowledgement can be retried safely. No raw token bytes are persisted.

After TokenReview authenticates the exact token, admission reads its expiry and rejects missing, expired or longer-than-fifteen-minute validity. It records only a token digest, expiry and scoped identity metadata in append-only workflow events. Replay verifies that evidence. Responses use `Cache-Control: no-store`. Invalid identity fails closed; unavailable Kubernetes configuration or API authorization returns unavailable rather than admitting work.

Helm gives only the API service account permission to create TokenReviews and read Ray controller metadata. It does not grant TokenRequest or TokenReview permissions to training workers. Rendered RBAC is tested; deployment of that RBAC remains part of runtime qualification.

## Grant implementation checkpoint: incomplete

Application head `0014_attempt_object_grants` persists project, attempt, stage,
fence, pod, request, exact object/version/method, bytes, digest and lifecycle
metadata. It stores no token bytes or signed URLs. Constraints cover all seven
stages; rollback and parent deletion/fence mutation cannot discard grant evidence.
Structural qualification passes 165 tests and all 13 prior-schema upgrades.

The read issuer currently supports registered safe train/validation inputs for
preparation and training parents only. It commits intent and issuance ownership
before provider calls, then rechecks current identity/source/authority before
committing publication and disclosing a URL. Same-request replay returns a receipt
without a URL or deadline extension. Unknown issuance cannot mint again under the
same request. The internal HTTP router and provider Ray reader now use this service;
managed pending-verifier execution and other worker paths remain unfinished.

Refit and final-data minters validate the native URL's immutable version and actual
encoded expiry before disclosure. Real botocore credential refresh is tested:
slow refresh cannot extend the original deadline. Final-data mint failure still
consumes its original one-shot permission. Logical revocation prevents new grants;
it does not invalidate already signed native URLs. Terminal cleanup/replacement
must await their original expiry. Project/run deletion now implements that barrier;
replacement and other terminal cleanup barriers remain unfinished.

The broker integration has one of seven gates met. Stage/child/predecessor
selection, outputs and filesystem integration, producer/consumer wiring and
credential removal, native access-denial qualification and final audit remain
open. See [the checkpoint evidence](evidence/phase-1/attempt-object-grants-2026-10-18.yaml).

## Deletion retirement checkpoint

Project and run deletion revoke affected grants before destructive side effects
and defer through the existing outbox until the latest original started capability
expires. Started PUTs additionally require verified native completion, even when
expired or revoked. This covers every affected attempt
generation, dependent runs and associated champion scopes. Waiting preserves the
failure budget. Pending intents never reached the provider and can be retired
immediately. Legal holds prevent grant mutation and cleanup.

After expiry, reads and verified completed PUTs can retire. Unknown started PUTs
keep deletion pending; expiry alone does not prove a native write has stopped.
Selected-run grant records are removed
before restrictive attempt foreign keys cascade; unrelated runs and projects keep
their records and objects. An in-flight signing/deletion race rejects disclosure
and retains revocation. The historical deletion checkpoint passed 384 Python tests with committed
PostgreSQL and real embedded file deletion. Controlled-clock and doubled Kubernetes/
provider checks do not qualify native URL expiry or current images. See
[the deletion evidence](evidence/phase-1/grant-retirement-2026-10-18.yaml).
Full broker gates remain open, including replacement retirement.

## Pending import authority checkpoint: incomplete

Provider import execution now commits a typed preparation attempt before storage
construction. Generations, fences and leases survive retry and commit acknowledgement
loss. Progress and publication recheck both command and attempt ownership; terminal
outcomes revoke grants while preserving their original expiry and history.

The pending-source path independently validates configured policy/key, signed safe
references, request and receipt digests, current ownership/lease and the latest
attempt. It can issue exact safe train/validation reads before READY without treating
a pending descriptor as raw rows or admitting final objects. Cancellation accepts
tracked managed execution, including live cancellation. Missing-lease or unmatched
legacy running work still requires controlled stop/recovery.

There are 61 authority checks and 384 reverified deletion/connected cases,445 distinct
Python tests. Kubernetes and signing are doubled; native cases are zero. This meets
the authority gate; the subsequent HTTP/reader checkpoint also passes,
while full managed-workload, native and final audit gates remain open. The
maintained operator CLI now queues verification through the reconciler. Its
bounded isolated Job uses brokered reads; remaining workload paths, permanent
unknown outcomes and native denials are not qualified by this checkpoint.
[Evidence](evidence/phase-1/pending-import-authority-2026-10-18.yaml) preserves that scope.

## Brokered provider reads checkpoint: incomplete

The maintained router now exposes `POST /{attempt_id}/objects/read` with only a
request UUID, safe role and object index. Successful replies use `no-store`.
Replay returns a receipt without another URL; authorization and storage/provider
outages fail closed. The provider Ray reader now uses this broker instead of
constructing an object-store SDK. Missing broker configuration cannot fall back
to ambient credentials.

Workers require `ATTEMPT_CONTROL_BASE_URL`, `AUTOML_ATTEMPT_ID` and the projected
`AUTOML_IDENTITY_TOKEN_FILE`. `ATTEMPT_CONTROL_CA_FILE` may add trusted CA roots.
The client rereads the token, sends it only to the broker, verifies native version
and expiry controls, bounds replies and stream chunks and preserves byte/hash/
schema/row/role/budget checks. HTTPS verification stays enabled, redirects and
proxy-environment routing are disabled, and signed URLs are omitted from errors.
Large declarations are shared once through a Ray object reference.

All 678 distinct connected tests pass, including two actual local Ray cases.
Kubernetes/provider transports are doubled, so this does not qualify native
permissions, TLS or current images. The maintained CLI/reconciler now schedules
verification, but full unknown-outcome recovery and removal of ambient authority
from other workloads remain open. Two of five pending-import gates pass, while the
original full broker remains one of seven gates met.
[Evidence](evidence/phase-1/brokered-provider-reads-2026-10-18.yaml) records the exact scope.

The pending verifier now has bounded `provider-import/start`, `heartbeat`,
`result`, `receipt` and `failure` control endpoints under the attempt route. Start binds the
first publishing pod to the protected execution; another pod, rewritten request,
revocation or stale identity cannot renew or publish it. Heartbeats cap the lease
at the current identity deadline. Publication validates the complete proof, checks pod authority outside SQL, then
rechecks the signed source and current fenced execution in the READY transaction.
New descriptors use `provider-import://<dataset-version-id>` backed by the protected
signed receipt; publication constructs no storage driver and performs no external
write. Exact committed replay returns the original IDs. Existing verified native
descriptor URIs remain compatible without rewriting their objects.

The verifier worker uses these endpoints and brokered reads. Its HTTP integration
tests verify actual signed fixture Parquet, reject changed bytes, and preserve
committed READY after a lost result acknowledgement. Kubernetes and native storage
transports are doubled. The CLI/controller now launches this worker from durable
submission intent. Permanent unknown Kubernetes creates, legacy uncertain descriptor writes and
deployed cleanup qualification remain open. A legacy `descriptor_requested` event
without a committed receipt still blocks publication and cleanup; this change
does not establish that an old writer has stopped. The same two integration gates are met and
three remain unmet; this does not complete the full broker or Phase 1.

Publication now records an exact proof/response receipt in the READY transaction.
The original publishing pod can recover it through the metadata-only `receipt`
endpoint or repeat an already committed `result`. Recovery rechecks the signed
source, current policy/operator/project, latest typed generation and fresh
TokenReview. Missing or tampered receipts, changed proofs, other pods and changed
authority fail closed. Generic admission, object reads and other control actions
still reject completed attempts.

After an unknown result acknowledgement, the worker makes one receipt lookup
with the original proof and a freshly read projected token. It never repeats the
publication request. An unfinished publication cannot be retried through this
endpoint; controller recovery remains required. Tests cover token replacement and
lost receipt replies without undoing committed READY or issuing more storage I/O.

## Producer, consumer and remaining integration

1. Ray manifests now project a dedicated 600-second pod-bound token into every head, worker and submitter, with the expected attempt audience, a short requested lifetime and no default Kubernetes API audience. Retain the head's existing autoscaler identity. Do not copy tokens into RayJob environment values, Tune configurations, logs or durable database payloads.
2. Configured training startup, trial dispatch/execution and renewal now require verified HTTPS admission. Production/staging cannot disable it. The client rereads the projected token file, rejects redirects or changed identity/expiry, and caps parent/trial leases at the verified expiry. Parent and child lease guards reject late publication. Production Helm rendering and Ray manifest construction require a control URL. Splitter/preparation startup, renewal, progress and publication also consume admission. Each stage claims one worker owner, renews a 90-second lease every 15 seconds and caps renewal at proof expiry. Expired or superseded workers cannot publish metadata or overwrite current failure evidence. The dataset observer now queues expired-generation cancellation and bounded replacement atomically; exhausted budgets still queue cleanup. Its typed cancellation handler checks scope/fence and physical RayJob labels, then uses a UID precondition. Retired submission messages cannot recreate old jobs. New validation/explainability/drift launches now persist a ModelRun attempt, project its identity and consume shared worker admission/leases. Report metadata, receipt and terminal CAS commit atomically; fenced analysis failures commit together. Managed analysis submission now records physical Job identity and source-manifest evidence. A typed observer queues exact old-Job cleanup plus bounded fresh-generation replacement on expiry or loss, while preserving committed report CAS and cancellation. First-claimed, never-replayed CPU legacy deliveries can now upgrade only while pending, without report evidence, and after the old Job and Pods are absent. Attempt/manifest/audit intent commits through deferral before Job creation. The configured current CPU image is selected. Bound analysis commands declare `--attempt-protocol analysis-v1`; current workers reject missing/unknown context and the known predecessor parser rejects this flag. Retried/running/ambiguous legacy work and submitted rows without evidence still require controlled migration/replay; inconsistent profile cleanup and deployed recovery remain unqualified. Local legacy execution without a configured URL remains explicit.
3. Bind child trial grants to the current parent and child fences and expiry. Preserve reconciler ownership of recovery and existing retry budgets. Do not let a replacement reuse an old grant or acquire a predecessor write prefix.
4. Implement the `AttemptCredentialBroker` provider handoff with exact registered read objects/versions, approved input roles, own output prefixes, methods, byte limits and expiry. Replacements may read only explicitly authorized predecessor checkpoint manifests. Admission does not itself restrict the workers' existing database or object-store credentials.
5. Persist grant digests and expiry/revocation evidence, never credential material. Test acknowledged and unknown issuance outcomes, replay, rotation, supersession, another project/attempt/scope and predecessor access denial.
6. Qualify the configured API identity, TLS/egress, projected-token rotation, KubeRay head ownership, expiry, revocation and process loss on the current immutable image. A manually requested token for a deliberately unscheduled test pod proves only the admission boundary.

Configure `attemptIdentity.controlBaseUrl` to the trusted HTTPS admission base path, ending in `/api/v1/internal/attempts`. Workers must trust its certificate and have explicit network access. The projected volume is mounted as a read-only directory without `subPath`, so file rereads can observe Kubernetes rotation. Parent/trial runtime tests exercise expiry and file replacement; splitter/preparation tests exercise owned leases and stale writes; a current local worker/native-admission probe stages a real pod-bound token manually. This does not qualify actual kubelet rotation, a deployed worker image or TLS/egress.

Current implementation requirements remain open until full issuance and enforcement are verified. The full Phase 1 ledger, 45 GB migration rehearsal and historical QA recovery retain their existing scope.

Kubernetes recommends [TokenReview for service-account verification](https://kubernetes.io/docs/concepts/security/service-accounts/) and documents [projected token lifetimes and audiences](https://kubernetes.io/docs/concepts/storage/projected-volumes/). The installed KubeRay v1.6.2 [service-account selection code](https://github.com/ray-project/kuberay/blob/v1.6.2/ray-operator/controllers/ray/utils/util.go) defaults the head identity to the cluster name when none is specified. Native head/rotation qualification is still required.


## Managed pending verifier: incomplete

The CLI now reports pending imports as queued. The maintained workflow reconciler
commits desired resources with one execution claim before Kubernetes I/O. It
creates one bounded isolated Job, a clean per-attempt service account and explicit
deny/allow network policy. Later preparation and profiling retain their separate
RayJobs. Verification receives only approved control metadata and brokered safe
reads, without database/provider credentials or the default API token.

Admission checks actual observed UIDs, the Job owner, the pod/container spec,
service-account authority and selected network policies. Mutable image tags,
broader policies, injected environment/sidecars/hooks and nested Job replicas are
rejected. The required worker protocol is `provider-import-verifier-v1`; older
control replies cannot authorize copy reads. Enabled Helm imports require a pinned
image, HTTPS control and explicit egress, with scoped API/controller permissions.

Each resource create has one durable request marker. Lost committed replies can
be adopted after body/UID validation; an absent unknown outcome is never blindly
created again. Cleanup revokes grants, deletes only observed resources by UID,
retains isolation while Pods or original read deadlines remain, and precedes any
replacement generation or project deletion. A tracked live verifier can be
cancelled; untracked legacy live execution still needs a controlled stop.

Descriptor publication also has an irreversible request marker before SDK I/O.
An uncertain publication cannot be reissued in that attempt. `resource_unknown`
and `publication_unknown` retain history and block completed cleanup/replacement.
A qualified permanent-outcome resolver, legacy migration and current-image native
permission/process-loss qualification remain required. Three local server-side
dry runs checked declaration schemas only; no worker or permission test was run
in k3d at this checkpoint.


## Isolated native qualification status (2026-10-19)

Source-current API and CPU test images now pass actual k3d image probes. The
preflight verifies exact source, verifier protocol/imports and API-to-CPU catalog
compatibility, including a real model fit and serialization round trip. These
images reuse unchanged immutable dependency layers; they are isolated test
artifacts, not qualified release images. The fresh production-Dockerfile build
remains open after reserve-protected build attempts. Exact test-image digests,
191 frozen source inputs and retained build/probe evidence are recorded in
[source-current image evidence](evidence/phase-1/source-current-image-preflight-2026-10-19.yaml).

The `phase1-provider-native` namespace holds the bounded image probes. The
maintained CLI/controller/API/verifier path still needs dedicated TLS storage and
database fixtures plus actual identity, immutable safe-read, negative-permission,
rotation, process-loss and cleanup qualification. The image probes cannot qualify
those behaviors. Shared QA images, credentials, Pods, PVCs, schema and historical
objects must remain intact while this isolated work continues.


The first maintained native submission failed the resource guard. Kubernetes
selected uncached agent-1; cancellation preserved authority but did not stop the
image pull synchronously, and free space crossed the43GB reserve. The late start
was denied401, no data grants/READY/artifacts were issued, and controller cleanup
completed. The dedicated controller is scaled to zero. Before another submission,
add explicit approved verifier placement through the existing Helm/workload
contract and bind it to desired-resource/admission validation. Do not patch worker
commands or rely on image-locality scheduling for the test. The failed attempt
and its original events remain retained. Dedicated TLS PostgreSQL16.15/head0014
and five original native versions are intact; full native gates remain open.
[Fixture and failure evidence](evidence/phase-1/native-verifier-fixtures-2026-10-19.yaml).


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


## 2026-10-21 — started PUTs require native completion proof

The shared retirement barrier now retains any started PUT without an exact native
completion record, including issued, issuing, unknown, failed, revoked and expired
rows. The record must match the immutable grant digest, issuance owner, own object
URI, size/hash and native version. Logical revocation does not erase issuance
ownership or shorten its original deadline. A valid completion record does not
bypass that original deadline or legacy champion-history quarantine.

Project/run deletion defers without native cleanup or retry-budget consumption.
Refit and evaluator cleanup/sweeps retain dependencies and reject replacement.
Provider-import cleanup also uses this shared check before deleting its service
account or network policy. Unstarted intents and elapsed read grants keep their
existing behavior. Unknown rows remain quarantined until trusted reconciliation
can establish completion; this change does not create that reconciler.

The conditional native storage helper has SDK fault checks but is not yet wired
to worker uploads. The full pod-bound PUT issuer, router, protocol/worker migration,
legacy unjournaled writers and native process-loss recovery remain unfinished.
See [the scoped evidence](evidence/phase-1/started-put-retirement-2026-10-21.yaml).


## 2026-10-21 — champion output broker integration

New refit and evaluator plans declare `pod-bound-object-v2`. Their workers upload
through `PUT /api/v1/internal/attempts/{id}/objects/{role}/{sha256}` using fresh
projected Pod tokens, an exact Content-Length and one request UUID. The API computes
the own output URI and commits a role/nonce/Pod/fence/size/hash/deadline declaration
before streaming, then validates the body and evaluator signature before claiming
one native operation. All provider I/O runs outside SQL.

Conditional native creation, disabled write retries, immutable versions and exact
version size/hash readback precede durable completion. A completion event binds
the original grant and issuance owner without changing its intent digest or
expiry. An already claimed operation can finish after logical expiry; cleanup
waits for actual completion evidence. Cancellation or revocation cannot erase
known completion, and fresh authority still governs disclosure. A lost upload
acknowledgement permits one receipt GET, never a second upload.

Worker publication also requires matching native completion; a readable object
alone cannot bypass missing version evidence. Evaluator publication rechecks
lease/revocation/recovery authority after stored-result verification. Trusted
evaluator recovery retains valid signed-output adoption without reopening final
data, and requires completion for v2 results. Old plan hashes remain unchanged.
Legacy model/result upload routes return 410; read-v1 plans cannot request new
output writes. Actual prior-binary/rolling qualification remains independent.

The S3 model-output path uses a single PUT capped at 5,000,000,000 bytes; larger
S3 model outputs need tracked multipart custody. Dataset-upload limits and the
45 GB migration target are independent. GCS/Azure remain bounded by their registered
artifact budgets; large resumable/block fault paths remain unqualified.

All 928 connected checks pass across 39 modules, without failures, errors or skips.
Two checks terminate only their active owned PostgreSQL completion connection
after native acceptance and retain unknown custody. All 509 frozen inputs match;
Ruff, formatting and strict 0018 schema parity pass. All five integration gates
pass after reverification. Native SDK wire controls use small loopback payloads;
joined workers use explicit native/Kubernetes/control-socket doubles. These
checks do not qualify cloud IAM, current release images, whole-process loss or
large-volume operation. Full Phase1 remains in progress, with 34 requirements Open.
[Integration evidence](evidence/phase-1/champion-put-broker-2026-10-21.yaml).


## Generic revocation guards — 2026-10-21

Generic splitter, preparation and training admission now checks the durable
revocation marker both before and after remote identity verification. Ordinary
training/analysis mutation guards and parent/child trial enrollment/execution
reuse that marker check, including pending revocations in sessions with
autoflush disabled. The worker lease helper receives its current SQL session
from all six production callers.

Verified committed training/analysis receipts remain recoverable; terminal trial
replay and explicitly authorized predecessor adoption preserve their original
hashes without new execution or lease extension. A 55-case PostgreSQL baseline
records 37 failures before these guards; all 58 focused cases and 1647 connected
cases across 63 modules now pass. All four scoped gates, repository Ruff,
changed-file formatting and strict 0018 schema parity pass on 510 frozen inputs.

This verifies the existing logical guard boundaries. Ordinary workers still
have database/provider configuration, and direct native writes plus SQL-spanning
I/O need their full broker and recovery integration. Source-current full backend,
independent coverage, native/release and 45 GB migration qualification remain
required. Original abandoned phase1-deletion:G7 still requires handoff for seven
missing historical QA objects.
[Revocation evidence](evidence/phase-1/generic-attempt-revocation-2026-10-21.yaml).


## Parent lifecycle RPC — 2026-10-23

Configured current Ray training heads and managed analysis workers select
parent-control-v1. Parent metadata/start/heartbeat/result recovery/completion/failure
use projected Pod identity over bounded verified TLS; a declared worker never falls
back to SQL for these operations. Shared SQL mutators preserve legacy workers.
New Ray outbox rows carry ray-parent-control-v2; saved legacy/v1 policies remain intact.

903 connected cases pass, including 41 worker/manifest and 28 API controls. A real
k3d Pod-bound TokenReview and token-rotation probe passes over verified local TLS
with worker-local SQL disabled; its pending Pod never runs a container.
Remaining pipeline/trial/data/result SQL and storage access still require conversion
before credentials can be withdrawn. Maintained images, full coverage, native
custody/retirement, 45 GB qualification and original abandoned deletion:G7 remain open.
[Parent control evidence](evidence/phase-1/parent-control-2026-10-23.yaml).


## Durable trial RPC — 2026-10-23

New configured Ray training heads and compute workers select trial-control-v1
through the saved ray-trial-control-v3 policy. Fixed-CV, clustering and Tune reuse
the existing SQL trial domain through Pod-bound TLS RPC for enrollment, dispatch,
start/replay, heartbeat and immutable result/failure publication. No child SQL fence
is returned. Legacy workers and saved legacy/v1/v2 manifests retain their behavior.

1,133 connected cases pass, including 44 worker/manifest and 27 API
controls. Real local k3d identity and verified TLS execute three-fold CV once and
replay twice with worker SQL and legacy admission disabled. All five scoped gates
reverify; complete worker credential withdrawal remains open because pipeline,
analysis, dataset, model and tracking operations still need conversion. Native
custody/retirement, maintained images, full coverage, 45 GB rehearsal and original
abandoned deletion:G7 also remain required.
[Trial control evidence](evidence/phase-1/trial-control-2026-10-23.yaml).


## Training input context — 2026-10-23

Configured current training heads select training-context-v1 through the saved
ray-training-context-v4 policy. Initial run/catalog/feature/override/source validation
and RUNNING initialization use Pod-authenticated TLS with an explicit API-side fence,
active publisher/lease checks and fresh graph authority after native identity I/O.
The closed context contains safe train/validation roles and approved execution fields;
final manifests, missing source plans, terminal contexts and overlapping role locations
are rejected. Legacy startup shares the same role-separation guard.

The evidence combines the preserved connected run with the current affected-consumer
rerun using exact collection and source/AST accounting. The real TLS/k3d probe reaches
the prepared-reader boundary with worker SQL disabled and deliberately stops before
loading data. Tracking/progress/result SQL and data/model/output operations still need
conversion before credentials can be withdrawn. Maintained images, full coverage,
native custody/retirement,45 GB qualification and original abandoned G7 remain open.
[Training context evidence](evidence/phase-1/training-context-2026-10-23.yaml).


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
