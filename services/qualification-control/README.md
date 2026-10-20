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

Before starting this authority binary, expand its dedicated database. For an
empty store apply `migrations/0001_authority.sql`, then
`migrations/0002_allocation_revisions.sql`, each with `psql -1 -v ON_ERROR_STOP=1`.
For an existing store at the first schema, apply only the second script. It adds
nullable paired references and their unique/check constraints in one transaction;
existing allocation and receipt values are preserved. Readiness selects all
columns of both tables and returns 503 if expansion is missing. This is not a
substitute for constraint/schema qualification.

Application-schema rehearsal uses Alembic head `0012_final_test_revision_pair`;
do not apply the dedicated-store SQL scripts again to a database already upgraded
with Alembic. Deploy expansion before application/authority code, retain the
expanded schema during application rollback and avoid schema downgrade. The new
Alembic downgrade refuses to discard any bound revision evidence. Qualification
of actual previous-release binaries and production-sized migration remains open.

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

An open receipt **is not an object credential**. The separate `/credentials`
path commits one durable grant claim before native signing, then records its
issued receipt before disclosure. Replaying an open cannot authorize a second
mint. Separate refit/evaluator jobs, frozen-pipeline CAS publication and signed
result recovery are implemented in the current source; the sections below
record their contracts and qualification limits.

The chart accepts an existing application image containing this module; it is
not yet the minimal hermetic authority image or protected deployment pipeline
required by Phase 8. Deployment of this component alone does not qualify Phase 1.

## Register a promotional scope before training

Creating a promotional scope now writes a `scope.authority.allocate` command in
that same application transaction. Its closed payload binds the local project,
scope, split revision, experiment revision, evaluation policy digest and central
allocation request. The evaluator controller uses the independently approved
project UUID credential binding to register the allocation while the scope is
open. HTTP runs between short SQL transactions, under a 90-second command lease;
publication rechecks current scope, policy, owner and expiry. A lost response
replays the same central allocation request. Delivery uses the command's retry
budget (five attempts by default) and a 60-second retry delay. Invalid controller
configuration also backs off so another scope can make progress.

Training launch and member admission return HTTP 409 with
`scope_authority_not_ready` until registration succeeds. The sealing transaction
checks the completed server command again before releasing any member. Later
evaluator handoff reads that allocation ID; it cannot allocate after refit.
A stopped scope with no refit gets bounded cleanup through the same controller:
replay creation when its reply was lost, verify the signed central abort receipt,
and retain it on the cancelled command. No final-data grant is requested. The
combined delivery/cleanup budget is at most `max_retries + 5` claimed attempts;
exhausted cleanup requires operator investigation. A never-sent command is
cancelled locally. HTTP/RPC timeouts and lease expiry still require process-level
bounds for a complete workload qualification.

Deploy the matching API and controller together after configuring approved
project bindings. Existing scopes without this command do not have evidence of
pre-training registration and fail closed at new sealing/handoff. Do not
manufacture a successful command from a post-refit allocation or rewrite old
receipts. Drain or separately review legacy campaigns before upgrading; previous
binary compatibility and this current image remain unqualified. The synthetic
QA evaluator seed now leaves an open scope with pending registration and no refit
attempt; its runner waits for registration before planning the synthetic refit.
Synthetic training remains fixture setup, not proof of production training order.

This registration closes the allocation timing gap in the existing canonical
scope flow. It does not supply trusted fixture/split revision references for an
imported campaign, issue provider distributions, verify imported dataset bytes,
or enforce signed `validation_only` source admission. Those integrations remain
required before claiming release campaign or Phase 1 readiness. Legacy unbound
allocation requests cannot be used as evidence of revision-bound distribution.

## Attempt object grants: integration unfinished

Application head `0014_attempt_object_grants` adds typed grant intent and lifecycle
records. Apply the application expansion before current models; the dedicated
authority store still uses SQL `0002_allocation_revisions.sql`. Structural tests
cover all 13 prior application schemas and preserve seeded data. This is not a
45 GB rehearsal or previous-binary qualification. QA remains on its older schema
and images; no production or QA rollout is qualified by this checkpoint.

Do not enable the partial read issuer for production. Its current scope is
registered safe inputs for preparation and training parents. Other stages,
child/predecessor access, bounded outputs, worker credential removal and native
access denials remain unfinished. Native read URLs keep their original expiry
after logical revocation. Cleanup and replacement must await physical retirement.
Grant foreign keys deliberately prevent deleting a referenced attempt or rewriting
its fence; recovery must append a new generation rather than mutate that evidence.
Downgrade refuses to discard any grant rows. Retain the expanded schema during
application rollback; do not delete grant evidence to force a downgrade.

## Provider dataset import: integration unfinished

The application expansion `0013_provider_dataset_import` adds an explicit raw
versus manifest source kind and protected project-bound import provenance.
Apply it through application Alembic before running current application models;
it does not add tables to the separately protected authority database, whose
SQL expansion remains `0002_allocation_revisions.sql`. Only the dedicated local
test database has been upgraded; existing QA workloads have not been rolled out.

The operator library verifies a trusted authority key and independently approved
project/provider/allocation/scope/revision references, verifies safe copy bytes,
and publishes an actual JSON descriptor with its own verified byte hash. It
never treats that descriptor as raw rows or invents final row identities. The
library accepts only signed non-canonical `validation_only` manifests. Its
trusted policy/key must come from protected operator/server configuration;
public API clients must not be allowed to supply a key or bless a policy.

This path remains unqualified for production. The implementation includes exact
version preparation/training readers, source-aware profiling/feature contracts,
promotion guards and operator cancellation/deletion. Native stage capability
denials and the final integration audit remain required. The reader still uses
the configured object-store identity: its approved-object checks do not remove
that identity's other permissions. Do not provision a broad raw/final credential
to an imported-source worker and call the import isolated.
See `docs/production-readiness/evidence/phase-1/provider-import-2026-10-16.yaml`
for the measured gate status and limits.

Configure `PROVIDER_IMPORT_POLICIES_FILE` as a protected, read-only JSON file with
exactly one `policies` array. Each entry has `project_id`,
`authority_key_digest` (SHA-256 of the raw 32-byte Ed25519 public key), and
`references`. The references are exactly `allocation_id`, `project_reference`,
`scope_id`, `provider`, `canonical_provider`, `fixture_revision`,
`split_revision` and `split_digest`. Approve the local-project association and
all central references independently of the receipt or public client. Configure
`EVALUATION_AUTHORITY_PUBLIC_KEY_FILE` with the authority's public PEM key. A
missing, duplicate, changed or invalid binding fails closed; there is no raw or
broad-prefix fallback. Changing an approved policy can make existing imports
unreadable, so coordinate configuration and application upgrades.

Using the application Python environment and database configuration, invoke:

```sh
python scripts/import_provider_dataset.py receipt.json \
  --project-id LOCAL_PROJECT_UUID --allocation-id ALLOCATION_UUID \
  --provider gcp --actor-id ACTIVE_ADMIN_UUID --name 'Approved safe copy' \
  --budget verification-budget.json
```

The budget is a closed JSON object containing positive integer
`max_temporary_bytes`, `max_decoded_group_bytes`, `timeout_seconds`, and a
nonnegative `min_free_bytes` reserve. Temporary capacity must cover the largest
object plus the verifier's identity index. Choose real host, process and provider
RPC limits too; the between-call deadline does not terminate a blocked SDK call
or impose a hard decoder RSS limit. The command prints verified IDs only after
publication. Pending registration now exits successfully with `verified: false`,
`status: queued` and the dataset-version ID; the workflow reconciler owns native
verification. The CLI does not read copy bytes or construct storage. Repeating
the command reports the same registration or verified result and never resets an
exhausted/cancelled command. `--cancel` fences a tracked managed verifier and
requests cleanup, including a live Job. Legacy untracked live execution still
requires a controlled stop; completed imports cannot be cancelled as pending work.

Keep safe copies outside every application-owned `projects/` prefix (including
an embedded store's `automl/projects/` prefix). Deleting a project removes its
owned descriptor, never the external safe-copy objects. Pending/running imports
block deletion until settled; conflicting storage configuration blocks cleanup.
The signed source is validation-only even if a caller forges a raw source flag
or promotional scope. Final object commitments are opaque commitments, not
fabricated final row counts. Use distributed profiling jobs for imported sources;
the raw preview, synchronous raw profiling and raw validation endpoints reject
manifest descriptors before reading storage. Existing raw sources retain their
verified byte provenance requirements.

## Signed provider distribution

Before training a release campaign, the allocator calls
`POST /allocations/{allocation_id}/distributions`. The closed request contains
`fixture_revision`, `split_revision` and exactly three `copies`, one for each of
`aws`, `gcp` and `azure`. Each copy declares `schema_digest` and versioned object
metadata (`role`, `uri`, `version`, `sha256`, `byte_size`, `rows`, `row_digest`,
`row_digest_revision`). Only train/validation roles are accepted, under matching
`/roles/split_role=train/` or `/roles/split_role=validation/` paths. URIs must use
the provider's object-store scheme and contain no credentials, query, fragment
or traversal. GCS versions must be numeric generations. Logical schema and
per-role row count/XOR digest must match across copies; physical partition
boundaries and byte digests may differ.

The final manifest is read from the authority's trusted
`QUALIFICATION_FINAL_MANIFESTS_FILE` registry using the allocation's bound
digest. The caller cannot override it in this request. Known final object URIs
and byte digests cannot appear as safe copies. Under the allocation lock, the
authority atomically signs and stores one receipt per provider in its existing
receipt table. The canonical manifest includes the registered final objects;
other manifests are `validation_only` and contain opaque final-role digests
without those locations. Only the allocator receives the three-receipt response;
it must deliver each receipt solely to its named provider. Provider/evaluator
identities cannot call this endpoint.

For a release allocation, `/allocations` must receive both `fixture_revision`
and `split_revision` before distribution. The central database enforces global
uniqueness of that pair, in addition to existing split-digest and scope uniqueness.
Changing a bound revision, project, provider, scope or digest conflicts rather
than allocating another authority. `/allocations/{id}` reports both references.
Canonical revision identifiers use 1–128 ASCII letters/digits plus `.`, `_`, `:`
or `-`, beginning with a letter/digit.

First distribution issuance is rejected after any authority refit/evaluation
receipt or an allocation state change. Exact replay returns the same three
signatures, including after evaluation terminates; it never mints access or
alters the allocation. Changed requests, partial histories and invalid stored
signatures fail closed. Distribution must match the allocation's stored revision
pair. Historical allocations remain unbound (both fields NULL): old requests
can replay them, but adding references by replay or issuing a new distribution
for them is rejected. A controlled legacy-reference migration remains required;
never infer these references from old digests or rewrite signed history.

Providers validate their received receipt against independently trusted local
references and an authority public key before using the declaration:

```sh
PYTHONPATH=apps/api python3 scripts/validate_provider_distribution.py distribution.json \
  --public-key trusted-authority.pem \
  --allocation-id <central-allocation-uuid> --scope-id <canonical-scope-uuid> \
  --project-reference <campaign-project> --provider gcp --canonical-provider aws \
  --fixture-revision <fixture-revision> --split-revision <split-revision> \
  --split-digest <registered-split-sha256>
```

The command requires every reference explicitly and emits only verification
status, protocol revision, provider, mode and safe-object count. It reads at
most 32 MiB, rejects unknown fields, coercions, altered defaults, signatures or
references, and prints a generic failure without object locations. The library
entrypoint is `services.provider_distribution.verify_distribution`.

The default command performs offline signature/reference validation. To also
check the local provider's declared objects, configure its approved object-store
identity and add `--verify-objects --verification-budget budget.json`. The budget
file is a closed declaration containing four integer fields. This budget was
used by the tiny native fixture:

```json
{
  "max_temporary_bytes": 8388608,
  "max_decoded_group_bytes": 8388608,
  "min_free_bytes": 33554432,
  "timeout_seconds": 30
}
```

Production budgets must come from an approved capacity profile. Temporary work
contains one downloaded object at a time plus a capped SQLite identity index;
the declared object bytes plus index-page budget are bounded by
`max_temporary_bytes`. Allow filesystem overhead in the outer worker allocation.
Verification rejects
objects that cannot fit alongside the index and retains `min_free_bytes` on the
working filesystem. It checks Parquet footer/row-group/batch decode sizes before
converting batches to Python rows. The decoded-group budget does not cap process
RSS: worker memory limits must also allow parser/Python overhead and SDK buffers.
The deadline is cooperative between calls; provider RPC timeouts and the outer
worker/process deadline must bound blocked I/O. Interrupted work cannot return a
successful proof; SIGKILL cleanup and durable import recovery remain integration
work.

Object checking reads only signed train/validation locations at their exact
provider versions. It verifies whole-object SHA/size, Parquet row counts,
canonical row IDs/ordinals, recomputed content fingerprints, declared safe roles
and per-object `xor-sha256-row-id-v1` digests. A capped disk index rejects repeated
row IDs across files and identical content crossing safe roles; duplicates with
distinct IDs within one role remain valid. A Parquet file may omit its Hive
partition `split_role` column; when present it must match the signed object role.
Final objects are never read. Original raw-row derivation and final-role overlap
cannot be independently checked from this safe-copy manifest alone.

`schema_digest` uses `arrow-logical-schema-v1`: SHA-256 over canonical JSON with
that revision and name-sorted data fields (`name`, Arrow type string, `nullable`),
excluding `row_id`, `source_ordinal`, `content_fingerprint` and `split_role`.
Column order and writer metadata do not affect the digest. Field types and
nullability must match. Existing declarations that fail this definition are
rejected; do not rewrite their digests or signatures to force acceptance.

Success adds `objects_verified`, `verified_bytes`, `verified_rows` and
`manifest_digest` to the existing safe CLI summary. Failed object/budget checks
print only the generic verification failure. No object locations or credentials
are emitted, and temporary work is removed on normal/error exit.

The copy verifier alone does not prove IAM isolation, check live allocation
state, register a dataset or issue credentials. Use the protected import path
above for durable registration and source-aware admission. Native capability
denials, controlled legacy-reference migration, provider copying/conformance,
coordinated key rotation and deployed qualification remain required. Existing allocation/evaluator interfaces are retained; the verifier's
output alone grants no permission to open final data or promote a
`validation_only` import.

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
is keyed by the canonical local application project UUID. Each value is a closed
record containing `project_reference` and `token`, for example:

```json
{
  "<local-project-uuid>": {
    "project_reference": "campaign-project",
    "token": "<project-scoped-allocator-token>"
  }
}
```

The controller takes the project UUID from its persisted scope row and requires
the policy's authority reference to match that protected record before creating
a transport. A policy reference cannot select another project's token. Different
local projects may share a reference only through explicit records in this file.
Missing/malformed records and the older reference-to-token format fail closed;
there is no legacy fallback. Tokens must be nonempty printable ASCII without
whitespace. Never generate these bindings from user-controlled scope policies.
The operator must independently approve the local-project/reference association.

Before deploying this source, suspend evaluator reconciliation, replace the
controller-only Secret with approved UUID bindings, deploy the updated controller
and then resume reconciliation. Do not change any existing raw token or authority
allocation merely to change the file structure. Old and new parsers reject each
other's formats, so a mixed rollout can temporarily defer reconciliation. Existing
shared test Secrets have not been converted by the source change.

Connections use
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


### Brokered provider reads: managed verifier integration unfinished

Current preparation/training provider readers require the HTTPS internal attempt
broker and projected pod identity. They no longer use an object-store SDK or
recover ambient credentials when broker configuration is missing. Configure
ATTEMPT_CONTROL_BASE_URL, AUTOML_ATTEMPT_ID and AUTOML_IDENTITY_TOKEN_FILE on
managed pods; ATTEMPT_CONTROL_CA_FILE may add trusted roots for local/private TLS.
Keep the API and CPU sources together. Old images and new endpoints have not been
qualified as a deployed pair by this checkpoint.

The maintained operator CLI now queues verification through the workflow
reconciler. Its isolated verifier uses the broker and pod-bound control endpoints.
Native raw/final/cross-scope denials and current-image/process-loss qualification
remain unfinished. Other workload paths still retain ambient credentials; this
checkpoint does not establish isolation across the application.

The pending verifier entrypoint now consumes bounded start/heartbeat/result/receipt/failure
control endpoints and brokered reads. Start durably binds the publishing pod to
the exact execution. Shared publication validates the proof, caps heartbeat
leases at identity expiry, and rechecks pod authority around descriptor I/O.
HTTP integration tests cover real fixture Parquet, bad bytes and a lost committed
result acknowledgement. The maintained CLI/controller now launches this
entrypoint. Permanent unknown outcomes and deployed cleanup qualification
remain open. The checkpoint passes 586 distinct tests on 88 frozen source inputs,
including two local Ray cases; Kubernetes/provider transports are doubled.

Publication records the exact verification proof and response with READY in one
database transaction. If the worker loses the result acknowledgement, it makes
one metadata-only receipt lookup using its original proof and current projected
token. It does not repost an uncertain publication. Only the original pod can
recover a complete receipt, under current policy/project/operator and latest
attempt checks. Receipt recovery grants no data access and performs no storage
I/O. Incomplete publication still needs controller recovery; a lost receipt reply
ends the bounded worker handoff without undoing READY.


### Managed verifier deployment and recovery

Configure `PROVIDER_IMPORT_IMAGE` with the current CPU image digest,
`ATTEMPT_CONTROL_BASE_URL` with the trusted HTTPS attempt base path, and
`PROVIDER_IMPORT_EGRESS_RULES` with explicit control and storage endpoint peers.
Helm exposes these as `providerImports.image`, `attemptIdentity.controlBaseUrl`
and `providerImports.egressRules`. Enabled policies require a pinned image, HTTPS
and nonempty egress configuration. `providerImports.cpuCores` and `memoryMiB`
(default 1 and 2048) bound the process; a decoded group may consume at most one
quarter of that memory allowance. Scratch reserves the approved temporary budget
plus 64 MiB. Startup has a 600-second lease; work renews a 90-second lease capped
at current identity expiry. The Job has no Kubernetes retry and a deadline of
startup allowance plus the verification budget.

Egress rules require explicit namespace/pod label peers or single public endpoint
IP addresses, with TCP ports 443, 8443 or 8334. Broad CIDRs and implicit peers fail
closed. DNS is separately restricted to cluster DNS. Another selected policy may
not broaden access. Configure `providerImports.caSecret` (environment
`ATTEMPT_CONTROL_CA_SECRET`) for a public `ca.crt` when necessary; only that key
is mounted. The worker receives a dedicated per-attempt service account and a
projected 600-second custom-audience token, without database/provider credentials,
allocator tokens or the default Kubernetes API token. Source policy/key files
remain in the API/reconciler; the verifier obtains its approved work from control.

Upgrade API and CPU sources together: `provider-import-verifier-v1` is required
before any copy read. The API checks actual observed resource UIDs, Job ownership,
pod/container security fields and effective network policies before admission.
A changed UID, injected authority or a broader selected network policy fails
closed. Source-only and server-dry-run checks do not qualify a deployed image pair
or native IAM/TLS/token rotation.

Resource create intent commits before each Kubernetes request. A lost committed
acknowledgement is adopted only from the original desired body and observed UID;
a missing uncertain outcome is not created again. Cleanup uses UID preconditions,
keeps network isolation while Pods remain, and waits for original issued read
deadlines before retiring the service account and network policy. Replacement
cannot consume a new generation before this cleanup completes. Project deletion
also waits for managed cleanup.

`resource_unknown` means an issued create still lacks authoritative outcome;
`publication_unknown` means descriptor publication started without a committed
result receipt. Neither state means cleanup is complete. Preserve its intent,
original resource identities and logs; do not delete history, reset the retry
budget or reissue the write to make it disappear. A qualified permanent-outcome
resolver/operator workflow and actual process-loss/native tests remain required.
Legacy attempts without durable submission identity are not adopted or silently
migrated. Stop/retire those producers and qualify their controlled migration before
production rollout. The older privileged library verifier remains unqualified as
a workload path; the maintained CLI does not call it.
