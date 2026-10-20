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
expires. This includes uncertain or revoked GET/PUT grants, every affected attempt
generation, dependent runs and associated champion scopes. Waiting preserves the
failure budget. Pending intents never reached the provider and can be retired
immediately. Legal holds prevent grant mutation and cleanup.

After expiry, normal deletion resumes. Selected-run grant records are removed
before restrictive attempt foreign keys cascade; unrelated runs and projects keep
their records and objects. An in-flight signing/deletion race rejects disclosure
and retains revocation. The checkpoint passes 384 Python tests with committed
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
