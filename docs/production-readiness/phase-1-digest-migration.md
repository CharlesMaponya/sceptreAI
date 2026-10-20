# Dataset digest inventory and migration

P1-W16 remains open. Digest interpretation must distinguish object bytes from a
multipart manifest. The ingestion migration added `byte_stream` and
`verified_legacy` defaults to existing dataset versions; those labels alone do
not prove that an old hash describes object bytes.

With `DATABASE_URL` configured for the intended PostgreSQL database, run:

```sh
PYTHONPATH=apps/api .venv-ci312/bin/python scripts/inventory_dataset_digests.py \
  --output /secure/path/dataset-digests.ndjson
```

Use a fresh output path. The command refuses to overwrite existing evidence and
creates the report with owner-only permissions. It reads metadata under one
read-only, repeatable-read transaction and streams results in batches of 500.
It does not read object contents, change database rows, or schedule deletion.
Database statement and transaction timeouts still apply; a partial report is
incomplete and must not be used as a migration baseline.

The header identifies the database, snapshot and enforced transaction settings.
The current schema is `dataset-digest-inventory-v2`. It includes byte sizes and
upload scanner results, and flags missing/invalid sizes, incomplete verification
times and disallowed scanner results. Older v1 inventories remain historical
evidence; they do not establish these additional prerequisites.
Each resource record contains its project/resource identifiers, digest metadata
and reasons for review. Versions linked to legacy upload scopes are flagged even
if the version's own labels claim a byte digest. Multiple upload links do not
duplicate version records. The final `complete: true` summary counts all uploads
and versions, lists review reasons and hashes the exact UTF-8 resource lines.
A missing final summary means the inventory did not complete. Keep reports
private: they contain tenant identifiers and content digests.

Current raw-data consumers require canonical `sha256`/`byte_stream` provenance,
`verified` status, an aware verification timestamp and a known nonnegative byte
size. Unqualified versions are rejected at training, analysis, profiling and
feature-contract admission and relevant worker startup. Local readers compare
the downloaded bytes with the stored size and digest before parsing; the splitter
also counts streamed bytes. Drift checks both current and reference provenance.
Offline inference checks upload digest/scanner evidence before opening storage.
Digest failures return a conflict rather than a successful empty profile.

These checks do not pin mutable provider objects. Distributed readers and bounded
prefix previews still need their provider/version bindings; a prior verification
timestamp cannot prove that an object has not subsequently been replaced.

`verification_not_proven` includes ordinary unfinished uploads and versions with
missing verification times. It is a review category, not proof of corruption.
Current direct byte uploads explicitly record their SHA-256, byte scope,
verification status and time. This records the hash of the bytes supplied to
storage; it does not attest scanner clearance or a later provider readback.

Before migrating an affected version, inspect its upload provenance, immutable
consumers, retention/legal-hold rules and object availability. Recompute the
object-byte digest with a bounded reader and preserve the original digest and
scope as historical evidence. A different digest requires a new version and
reviewed handling of dependent immutable contracts; never replace their hashes
in place. Missing objects require recovery or a retained blocked state.

Do not route legacy records through normal upload quarantine without review:
that path schedules object deletion. Completion and reconciliation now reject
unqualified digest metadata without scheduling that deletion. A controlled
legacy migration and qualification of all consuming readers and previous
release binaries are still required.

On 2026-10-16, the metadata-only local k3d QA inventory completed with 13 dataset
versions and no upload-session rows. All 13 versions require verification review;
none was declared corrupt or repaired. Existing QA Pod/PVC identities were
unchanged. This fixture is not a production inventory or the required 45 GB
migration qualification.

A subsequent bounded byte rescan matched all 13 QA hashes over 280,788 bytes.
All 13 versions lacked persisted byte sizes. A frozen, project-bound plan was
then tested with altered observations, stale rows, conflicting evidence and a
complete deliberately rolled-back repair before commitment. One transaction
added 13 digest-verification records and filled observed size/time/status
metadata, preserving every content hash and other row content. Replaying the
plan changed nothing; the follow-up read-only inventory reports zero versions
requiring verification review. See
`evidence/phase-1/qa-digest-metadata-repair-2026-10-16.yaml`.

This repaired only the qualified local fixture. It neither migrates production
legacy manifests nor qualifies every consuming reader. Verification attests the
bytes observed at the recorded time; consumers still need their own content
bindings. No historical model/result was replaced or re-signed.


## Version-pinned reads

The object-store protocol accepts `open_stream(uri, byte_range=None, *, version=None)`.
When an independently registered immutable version is available, pass it explicitly:
S3 uses `VersionId`, GCS uses its canonical positive generation and Azure uses
`version_id`. Invalid or unsupported versions fail closed; missing/denied versions
never fall back to current bytes. Embedded storage rejects explicit versions.
S3/Azure also reject a missing or different returned version identifier.

Omitting `version` retains current-object reads for compatibility. This does not
pin existing raw/prepared readers automatically or make an earlier metadata-only
proof immutable. Approved manifest versions still need to be threaded through
import/admission/worker grants and distributed readers.

Azure `open_stream` now exposes the installed SDK's incremental `read(size)`
instead of calling `readall` before returning. Callers that read chunks avoid
whole-object buffering; `read()`/`read(None)` still request all remaining bytes.
The SDK may retain its normal bounded initial/chunk buffer. GCS byte-range reads
retain their existing range-buffering behavior. Neither change qualifies a full
production-sized memory or provider conformance campaign.

Local evidence: `evidence/phase-1/versioned-streams-2026-10-16.yaml`. Actual TLS
S3-compatible helper checks created three fixture versions totalling51bytes,
verified historical full/range reads and missing-version denial, then removed
only owned fixture versions. All16 pre-existing version records and checked
Pod/PVC identities remained unchanged. This ran exact current helper source in
a separate Python process inside an existing test Pod; its deployed image and
application process were not updated. Azure/GCS evidence is installed-SDK
contract verification, not live provider qualification.


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

Before upgrading a database at0014, inspect cross-project predecessor links with
a bounded read-only query. A nonzero count requires investigation using actual
lineage evidence; do not clear links to force an upgrade:

```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT count(*) AS cross_project_predecessors
FROM workflow_attempts AS child
JOIN workflow_attempts AS parent ON parent.id = child.predecessor_attempt_id
WHERE child.project_id <> parent.project_id;
ROLLBACK;
```

The migration builds an index and validates existing attempts transactionally.
Lock duration and resource use on the45GB qualification target remain untested.
The disposable corrupt test fixture is repaired only inside its own temporary
database to exercise resumption; this is not a production repair procedure.


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

The0016 expansion takes write-conflicting locks on attempts, trials and candidates
while validating existing ownership and installing the guards/indexes. Duration
and lock impact at45GB remain unqualified. Default application transactions use
read committed; do not switch ownership writers to repeatable read. Historical
inconsistencies require investigation from actual lineage evidence. The test
fixture's deliberate corruption is repaired only in its disposable database to
exercise resumption; it is not an automatic production repair procedure.


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
