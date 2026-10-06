# Qualification control

This entrypoint runs the existing final-allocation state machine as a separate
HTTPS service. It is **not mounted in the application API**. It commits each
transition and its receipt in the dedicated PostgreSQL transaction before
returning an acknowledgement. Provider callers cannot allocate, choose their
own project/provider/scope, or bypass the canonical provider/manifest checks.

## Database initialization

Provision a dedicated PostgreSQL database and migration owner outside the
provider application databases. Apply `migrations/0001_authority.sql` once,
using `psql -1 -v ON_ERROR_STOP=1 -f migrations/0001_authority.sql`. This initial
migration creates only the two authority tables and their indexes. Do not run
the application Alembic migrations against this database. Subsequent upgrades
must add ordered migrations; do not edit an applied migration.

Use a separate runtime login. Grant CONNECT on the database, USAGE on its
schema, SELECT/INSERT/UPDATE on `final_test_allocations`, and SELECT/INSERT on
`final_test_authority_receipts`. Do not give that login table ownership, DELETE,
receipt UPDATE, DDL or application-database privileges. Revoke public database
access. Provider application and worker roles must have no authority database
access. The URL startup guard catches obvious shared-database configuration,
including changes of username/password/query options; DNS aliases and actual
privilege isolation still require infrastructure verification.

## Identity and signing material

Mount an `identities.json` file containing a JSON array of records:

```json
[
  {
    "token_sha256": "<SHA-256 hex digest of a random bearer token>",
    "role": "allocator",
    "project_reference": "campaign-project",
    "expires_at": "2026-10-01T00:00:00Z"
  },
  {
    "token_sha256": "<SHA-256 hex digest of a different random bearer token>",
    "role": "provider",
    "project_reference": "campaign-project",
    "provider": "aws",
    "scope_id": "<preregistered scope UUID>",
    "evaluator_attempt_id": "<preregistered evaluator attempt UUID>",
    "frozen_pipeline_digest": "<SHA-256 hex digest of the frozen pipeline>",
    "expires_at": "2026-10-01T00:00:00Z"
  }
]
```

Generate independent tokens with at least 32 random bytes; distribute raw tokens
only to their assigned control-plane/broker callers, never training workers.
The file holds token hashes. Supported providers are `aws`, `gcp`, and `azure`.
Expiry is checked for every request. Duplicate token hashes, empty identity
lists, naive expiry timestamps and incomplete provider identities prevent
startup. Provider identities must include both the evaluator attempt UUID and
frozen pipeline SHA-256 digest. These fields are taken from this trusted
configuration, never from a request-body override. Identity rotation/revocation requires restarting every replica.

Mount an Ed25519 private key in unencrypted PKCS8 PEM format as `signing-key`.
The HTTP service rejects shared HMAC secrets and public-only keys at startup.
Generate a test key with `openssl genpkey -algorithm ED25519 -out signing-key`
and export its public key with `openssl pkey -in signing-key -pubout -out public-key.pem`.
Protect the private file and distribute only the public key to receipt consumers
through a trusted channel. Pin that key rather than trusting a key supplied with
a receipt. The receipt payload includes `signing_key_id`, the SHA-256 digest of
the raw public key; this identifier is covered by the signature.

`verify_receipt` accepts a trusted Ed25519 public key or PEM public key. It
checks the payload digest, key identifier and signature and rejects algorithm
confusion. Historical HMAC receipts remain verifiable inside their original
trust domain with their old shared secret; the HTTP service no longer issues
HMAC receipts. Retain old verification keys with their immutable evidence when
rotating the private key. Automatic key-ring distribution, coordinated rotation
and external append-only evidence export remain qualification work. Signing
uses the existing [cryptography Ed25519 implementation](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/ed25519/).

## Launch and deploy

Run `uvicorn automl_api.qualification_control:create_app --factory` as its own
process, setting `QUALIFICATION_DATABASE_URL`, `QUALIFICATION_IDENTITIES_FILE`
and `QUALIFICATION_SIGNING_SECRET_FILE`. Staging/production also require
`DATABASE_SSL_MODE=verify-full` and `DATABASE_SSL_ROOT_CERT`; set `DATABASE_URL`
to a credential-free reference to the provider application database solely for
the isolation guard. The authority never connects to that reference URL.

The separate Helm chart requires an immutable image digest containing the new
entrypoint, existing database/identity/TLS/CA Secret names, and either the narrow
PostgreSQL destination CIDR or `databasePodSelector` labels for a database in
the authority namespace. Configure exactly one destination form. Secret keys are documented in `chart/values.yaml`.
It launches two replicas with bounded pools, no service-account token, a
read-only filesystem, HTTPS on port 8443, and ingress/egress NetworkPolicy.
The Deployment uses `Recreate` to avoid simultaneously serving old and new
identity/signing policies. Upgrades briefly make the authority unavailable;
callers must retry exact requests after readiness returns. When upgrading an
older Deployment, remove its persisted `spec.strategy.rollingUpdate` field
before applying this strategy. For example, apply a merge patch setting
`spec.strategy` to `{"type":"Recreate","rollingUpdate":null}` to this authority
Deployment, then run the Helm upgrade. The local upgrade verified this path;
a plain server-side Helm apply retained the old defaulted field and was rejected.
Choose an isolated namespace and label only approved client namespaces
`qualification-client=true`. The default Service is internal. Cross-cluster
routing, trusted TLS certificates, CNI policy enforcement and managed database
availability must be configured and tested by the platform owner.

`/healthz` checks the process; `/readyz` checks both authority tables. Readiness
fails on missing schema or database loss. `/allocations` accepts an allocator
identity and binds the project from that identity. Provider identities call
`/allocations/{id}/open`, `/commit`, and `/fail`; every operation requires the
registered `provider_manifest_digest`. Commit additionally takes
`result_digest`; failure takes `reason`. Requests reject unknown fields and
invalid SHA-256 digests. Versions default to 0 for open/fail and 1 for commit;
a failure after opening must specify `expected_cas_version: 1`.

The server hashes the validated request body together with the configured
evaluator-attempt and frozen-pipeline binding. The signed open receipt stores
that binding durably. Commit, failure and replay reject a different attempt or
pipeline, including after a service restart. No schema migration is required.
Deployments with old provider identity files must add these fields before
upgrading. Historical unbound receipts are preserved; the bound HTTP interface
cannot resume them as new evaluator grants.

The server computes replay hashes itself. Exact replay returns the
same receipt. Changed requests/stale versions return 409, wrong identity scope
or provider returns 403, other projects return 404, and store failures return
503. Never log bearer headers. Never infer that a timeout means a transition
failed: retry the exact request to recover its committed receipt.

## Credential issuance and remaining integration

An open receipt **is not an object credential**. This service never mints one.
Replaying an open returns the original receipt even after a terminal result;
the broker must not interpret replay as permission to issue another grant.
The dedicated broker's durable one-grant ledger, actual refit-pipeline CAS
publication, failed-mint evidence and provider-local reference validation remain
unimplemented. Do not connect this endpoint directly to generic object signing.

Separate refit/evaluator jobs and their least-privilege roles are also still
required. No new path gives candidate workers final-test access. The chart
accepts an existing application image containing this module; it is not yet the
minimal hermetic authority image or protected deployment pipeline required by
Phase 8. Deployment of this component alone does not qualify Phase 1.

## Recovery checks

`tests/test_qualification_control.py` exercises real PostgreSQL transactions:
provider races, conflicting commits, a fresh service instance replaying a
receipt, stale ORM state, and a backend terminated before commit. It also checks
HTTP identity boundaries, expired tokens, database failure and chart rendering.
Run it against a disposable migrated database with `DATABASE_URL` set. Tests
create and clean their own allocation rows and require permission to terminate
their own PostgreSQL backend for the failure case.

Restore the authority database and protected signing/identity material as one
recovery exercise. Before reopening callers, verify the allocation/receipt
history, replay existing open/commit requests and run the three-provider race.
A service-instance restart test is not evidence of database restore, zero
acknowledged data loss or the four-hour RTO; those drills remain outstanding.

For psycopg 3 transaction-pooling connections, `PGBOUNCER_TRANSACTION_MODE=true`
disables automatic prepared statements so the client does not depend on a
particular pooler prepared-statement configuration. See the
[psycopg prepared-statement documentation](https://www.psycopg.org/psycopg3/docs/advanced/prepare.html).
Live pooler compatibility and timeout enforcement still need qualification.

## Local k3d evidence (2026-09-25)

The `k3d-sceptre` test deployment uses namespaces `qa-phase1`,
`qa-phase1-clients` and `qa-phase1-denied`. Its database and credentials are
separate from the existing application. The authority runs two replicas;
PostgreSQL uses one 1 GiB local-path PVC. The local image overlays this branch's
source onto an existing API image and is pinned by digest. This is a test image,
not the hermetic production artifact.

The reusable `scripts/validate_qualification_authority.py` probe requires a
configuration containing `test_environment: true`, the HTTPS `base_url`,
preregistered synthetic `scope`, `evaluator_attempt_id`, `frozen_pipeline_digest`,
and a trusted PEM `public_key`. Its bearer `tokens` map contains allocator/aws/gcp/azure
identities plus `alternate_attempt` and `alternate_pipeline` test identities
for the same project/scope/provider but a different attempt or pipeline. Run with `--config <private-config.json> --ca <ca.crt>`.
It verifies signatures, identity boundaries, concurrent requests and replay.
Never run it against a real locked final-test allocation. Local private files
remain under the ignored `.unlazy/phase1-k3d/` directory; they are not evidence
artifacts for publication.

Live local checks verified authority and database TLS, allowed/denied namespace
network boundaries, restricted runtime database permissions, and exact receipt
replay after replacing both authority pods and the PostgreSQL pod. Database
pod recovery uses the same PVC; it is not a node-loss, HA or PITR recovery proof.
Test certificates expire after seven days and caller identities after two days.
Refresh them before another campaign. The temporary database NodePort used while
troubleshooting host-based contract tests was removed. No application namespace
resources were changed.

The test environment is left running for subsequent integration. To retire only
this synthetic environment, uninstall Helm release `authority` in `qa-phase1`,
then delete these three `qa-phase1*` namespaces; this also removes the synthetic
PVC. Preserve any needed evidence first.

## Evaluator binding qualification (2026-09-26)

The local deployment now persists evaluator-attempt and pipeline bindings in
signed receipts. The probe checks that alternate identities cannot reopen,
commit or fail an allocation opened by the selected identity, and that the
same receipt survives service replacement. The binding uses a synthetic
preregistered pipeline digest; this is not a proof of actual champion refit or
provider object access. The future refit/controller path must populate the
identity from the immutable CAS-published winning pipeline. The credential
broker must still enforce durable, one-time grant issuance and failed-mint
handling before any final-test object access.

## One-shot final-data credentials

Promotional scope creation now requires `comparison_policy.evaluation_policy`.
It declares `target_column`, `task_type`, `primary_metric`, `positive_label`,
`final_rows`, `final_row_digest`, `max_decoded_bytes`, `max_input_bytes` and
`max_model_bytes`, plus a `final_data` template. The template has the same fields
as `FinalDataManifest` except `scope_id`: project reference, split digest,
provider, bucket, optional Azure account, and exact input/label objects with key,
immutable provider version, byte count and SHA. The application validates these
against the sealed split/experiment and freezes their digest before releasing
training. The scope ID is added when constructing the evaluator execution plan.
Missing evaluator policies in old unsealed promotional scopes must be supplied
before release; they are not inferred from a winning model.

`register_evaluation_plan` records a pending evaluator and immutable plan only
after the successful refit checkpoint exists. The control-plane caller must
authenticate the central allocation and durably manage the matching private key;
that controller integration is still in progress. The registration helper does
not issue credentials, submit Jobs, or infer that a failed evaluator is safe to
replace. Replacement requires authoritative proof that final data has not opened.

`prepare_evaluator` connects the provider's durable plan to the authority. It
records allocation/refit intent before requests, verifies the inspected
allocation, persists the local attempt, and verifies the signed refit receipt
and evaluator JWT. It records public claims and a token hash, then returns the
token for Secret delivery; the raw token is not stored in workflow events.
Lost responses replay the same requests. `scope_deadline_at` on evaluator
registration bounds token expiry by both the scope and allocator deadlines.
Replay cannot extend the token. The handoff never opens final data and rejects
already-opened allocations. Its caller must preserve the matching result private
key across retries; use the custody helper below before workload delivery.

`prepare_evaluator_credentials` supplies generation-scoped key custody:
it records intent before creating an immutable namespace-scoped Kubernetes Secret,
then records its public key before invoking the authority handoff. Replays read
that Secret; a missing recorded key, changed owner, namespace, key or immutability
flag is rejected instead of silently rotating the signing identity. Workflow
events contain only Secret metadata and the public key. The return value contains
the plan, evaluator token and Secret name for the controller to deliver.
The reconciler invokes custody before separate workload submission;
this helper does not submit a Job. Passing `generation=2` prepares the single
permitted replacement only after the first local attempt has failed, has retry
budget, and has no recorded output. It preserves the allocation and complete
evaluation policy, uses a new key and fence, and asks the authority to replace
generation one under its allocation lock. An old worker winning the open race
prevents replacement registration and submission. Lost registration replies
replay the same plan, key and identity; a third generation is rejected.

The application exposes internal evaluator control under
`/api/v1/internal/evaluations/{attempt_id}`: `start`, `heartbeat`, `pipeline`,
`result` (PUT), `publish` and `fail`. These require a separate domain-derived
application capability bound to project, attempt and fence; authority JWTs and
user access tokens are not interchangeable with it. The application needs
`EVALUATION_AUTHORITY_PUBLIC_KEY_FILE` to verify publication receipts.
The result upload limit is 64 KiB. Upload acknowledgement follows independent
stored-byte and signature verification. Publication accepts a central signed
commit receipt and performs local checkpoint/attempt/scope completion through
the existing recovery service. A failure report records a fixed event; the
reconciler must inspect authority state and stored output before failing the
scope. The controller lifecycle and its remaining qualifications are described below.

The allocator can inspect `GET /allocations/{allocation_id}` to recover an
allocation's current state and signed receipts after losing an acknowledgement.
The endpoint is project-scoped, allocator-only and sends `Cache-Control: no-store`.
It never mints or returns object URLs or evaluator access tokens. A shared
allocation lock keeps the returned state and receipts consistent with transitions.
Recovery must verify receipt signatures and their allocation/attempt/pipeline
bindings before using a committed result. This read path remains usable when
the old worker identity is gone; it does not permit another evaluation.

Allocator-only `POST /allocations/{id}/recovery/commit` and `/recovery/fail`
accept the corresponding worker transition payload plus `evaluator_attempt_id`,
`frozen_pipeline_digest`, and `generation`. The authority checks the project and
current registered evaluator under the allocation lock. These operations need no
worker token and never issue credentials. Generation is an authorization fence;
the transition request digest remains identical to the worker request so a lost
acknowledgement replays the same signed receipt. Conflicting terminal requests
remain rejected. A recovery commit still requires the issued-grant receipt.
The controller must independently verify stored result bytes before committing,
and inspect storage before choosing failure. The reconciler uses these operations.

Allocation creation treats both the split and scope as unique bindings. A
conflicting binding returns 409, including concurrent requests; an identical
request returns the existing allocation.

The optional broker adds `POST /allocations/{allocation_id}/credentials` with a
`provider_manifest_digest` body. Only a configured evaluator identity may call it.
The authority operator installs `QUALIFICATION_FINAL_MANIFESTS_FILE`, a JSON array
of `FinalDataManifest` objects. Each manifest names the project, scope, split
SHA-256, canonical provider, bucket/container, and exact input/label object keys,
immutable provider versions, SHA-256 hashes and byte sizes. Azure also requires
`azure_account`. Both roles must be present and use distinct keys. The manifest's
canonical JSON SHA-256 is the allocation's provider-manifest digest. Arbitrary
object URIs and caller-selected expiry are not accepted by this endpoint.

The broker atomically opens the allocation and appends a signed `grant_claim`
receipt **before** invoking a provider SDK. It then appends `grant_issued` before
returning the URLs with `Cache-Control: no-store`. Receipts contain expiry,
manifest/evaluator/pipeline binding and a capability digest, never the URLs.
Credentials expire within 15 minutes and no later than the evaluator identity.
AWS uses a version-pinned GET signature; GCS uses a generation-pinned V4 GET;
Azure uses a version-pinned read-only user-delegation SAS. Each URL can be used
for its permitted reads until expiry; the one-shot invariant is one grant, not
one HTTP request.

A replay returns 409 without credentials. A lost response does not justify
another mint. A mint or acknowledgement failure records a terminal failure when
the database is available; a committed claim continues blocking another mint
when the database is unavailable. Every replica reconciles expired claims that
lack an issued receipt, using row locks with `SKIP LOCKED`. Result commit rejects
an unacknowledged grant even if its manifest has since been removed from the
service configuration.

Configure the separate chart's `broker.secret` with a `manifests.json` key,
`broker.credentialsSecret` with dedicated provider SDK environment, and
`broker.egress` with explicit Kubernetes NetworkPolicy egress rules. Do not reuse
application object-store credentials. Provision the broker's cloud identity for
only the registered final objects and required signing/decryption operations.
For AWS-compatible local tests, `QUALIFICATION_S3_ENDPOINT` must be HTTPS and
`AWS_CA_BUNDLE` can reference the trusted CA mount. For GCP workload credentials
that cannot sign locally, set `QUALIFICATION_GCP_SIGNING_ACCOUNT` and authorize
IAM signing for that account; missing configuration fails closed. Azure uses
`DefaultAzureCredential` and user-delegation signing.

The broker is not yet connected to the application's champion refit/evaluator
jobs. Manifests and bound evaluator identities are currently operator-installed.
Do not treat an arbitrary preregistered pipeline digest or a broker probe's
synthetic result as proof that refit ran or that model evaluation succeeded.
Native AWS/GCP/Azure account and IAM qualification remain required.

The bounded local probe is `scripts/validate_champion_credentials.py`, with a
private synthetic configuration, pinned public key and CA. Its initial run
consumes a fresh synthetic allocation, races five credential requests, checks
version-pinned reads and denied unsigned/changed-key/write requests, and commits
a synthetic broker-test digest. `--replay` verifies that service replacement
cannot issue another grant and recovers the same immutable result receipt.
Never run this consuming probe against release final-test data.

## Refit publication and evaluator registration

The allocator can now publish a frozen-pipeline **attestation** through
`POST /allocations/{id}/refit`. Its closed request contains `refit_attempt_id`,
`frozen_pipeline_digest`, `frozen_pipeline_uri`, and `refit_policy_digest`.
The authority stores one signed, append-only receipt. Identical publication
replays return that receipt; changed publications conflict. A first publication
after final data opens is rejected. The allocator is responsible for verifying
the provider-side refit terminal CAS and artifact bytes before attesting: this
endpoint does not run refit or read the model object.

After publication, `POST /allocations/{id}/evaluators` accepts
`evaluator_attempt_id` and `expected_generation` (initially zero). The authority
returns an EdDSA JWT bound to the allocation's project, scope, canonical provider
and published pipeline digest. Expiry is at most two hours and cannot exceed the
allocator identity's expiry. Token generation is deterministic across JSONB
round-trips and service restarts. The bearer response is marked `no-store`.

One pre-open replacement is allowed with `expected_generation: 1`. The prior
JWT is revoked by the new durable registration. The allocation lock rechecks
registration when opening or minting, so a request authenticated just before
replacement cannot use its old identity. Replacing an evaluator after opening
is forbidden. Tokens with changed claims, wrong audience/issuer/algorithm,
missing registrations or expired lifetimes are rejected. Existing operator-bound
identities remain compatible for historical allocations; once a refit is
published, all transitions must match its current registered evaluator.

The application calls these endpoints after its actual refit CAS
and delivers the returned identity only to the separate evaluator job. The
`--register-evaluator` option on the local broker probe exercises this HTTP
handoff using a synthetic attestation, not an executed model refit. Production
refit/evaluator integration is not qualified by that probe.


## Evaluator worker

`python -m automl_api.training.evaluation_worker` runs the bounded evaluator.
It needs `EVALUATION_CONTROL_URL`, `EVALUATION_CONTROL_TOKEN`,
`EVALUATION_AUTHORITY_URL`, `EVALUATION_AUTHORITY_TOKEN`,
`EVALUATION_RESULT_KEY_FILE` and `EVALUATION_AUTHORITY_PUBLIC_KEY_FILE`.
Optional `EVALUATION_CA_FILE` adds a private CA to system trust. Control and
authority URLs require HTTPS; redirects are disabled and neither bearer token
is sent to object storage. No database password or bucket credential is required.

The worker claims its attempt and maintains the control lease, downloads and
verifies the frozen pipeline, then requests one final-data grant. Grant requests
and evaluation are never retried. It uploads signed aggregate bytes, verifies the
storage acknowledgement, commits their digest to the authority, and publishes the
signed authority receipt through the control API. Only identical result uploads,
commits and publications can replay once after a lost transport acknowledgement.
SIGTERM and errors report a fixed failure event for reconciliation. The worker
entrypoint is implemented; live controller/Job and recovery-loop qualification
still need completion.

### Evaluator workload submission

`services.evaluation_jobs.submit_evaluator` accepts the registered plan, authority
token, recorded result-key Secret and local attempt fence. It commits the Job and
NetworkPolicy manifests with a five-minute startup lease before creating the
immutable two-token Secret, policy or Job. Creation replay validates token
ownership and manifest digests. It cannot resubmit a started or expired attempt.
The refit and evaluator now share the native isolation profile in
`services.champion_workloads`: digest image, no Kubernetes API token, non-root
execution, read-only root filesystem, dropped capabilities, bounded temporary
storage and resources, explicit egress, and no Kubernetes retries.

Submission configuration is `EVALUATION_CONTROL_BASE_URL`,
`EVALUATION_AUTHORITY_URL`, `EVALUATION_AUTHORITY_PUBLIC_KEY_SECRET` (key
`public-key.pem`), `EVALUATION_EGRESS_RULES`, optional `EVALUATION_IMAGE`,
`EVALUATION_MEMORY_MIB` (default 2048), `EVALUATION_CPU_CORES` (default 1), and
`EVALUATION_CA_SECRET` (key `ca.crt`). The result-key Secret is supplied by key
custody. Worker containers receive no database or bucket credentials. The
controller invokes submission and owns observation, replacement, recovery and
resource cleanup; this helper alone does not schedule evaluations.

### Evaluator reconciliation

The workflow reconciler now invokes `reconcile_evaluation_jobs_once` when
`EVALUATION_ALLOCATOR_TOKENS_FILE` is configured. That controller-only JSON file
maps each manifest `project_reference` to its allocator token. Connections use
`EVALUATION_AUTHORITY_URL`, verified TLS (optional `EVALUATION_CA_FILE`), bounded
timeouts and no redirects; `EVALUATION_AUTHORITY_PUBLIC_KEY_FILE` verifies receipts.
Allocator credentials never enter evaluator Jobs or workflow events.

The loop submits prepared attempts, observes native Job state and leases, records
a recovery fence that rejects late worker requests, and independently verifies
stored output before committing its digest. It replaces an unopened failed first
attempt only after foreground Job deletion. An opened attempt without valid output
is sealed failed centrally and locally. Cleanup retains credentials and policy
until Job absence is confirmed, then removes them and the generation's result key.
A periodic labeled-resource sweep handles late creates and scope-deletion orphans.
Cancellation after a central commit preserves that receipt and cleans the workload
without publishing a successful local scope.

This lifecycle has local database/API tests with a Kubernetes fake and rendered
Helm contract tests. Full live Kubernetes failure probes remain unfinished. The controller is not
production-qualified by these tests.

Allocator-only `POST /allocations/{id}/recovery/abort` closes a handoff that never
registered an evaluator. It requires the scope, manifest digest and reason, checks
project ownership, and holds the allocation lock while rejecting any registered
or opened allocation. It records a signed `abort` receipt and seals the allocation
failed. Identical requests replay the receipt; changed requests conflict. A racing
registration and abort cannot both succeed.

On expiry or cancellation, the reconciler can replay the saved allocation intent
even if no local evaluator row was created, verify the abort receipt, record local
handoff closure, and clean result-key Secrets. Replaying allocation before abort
also prevents an earlier delayed allocation request from creating fresh access
after cleanup. Registered evaluators continue through the existing fenced recovery
operations.
