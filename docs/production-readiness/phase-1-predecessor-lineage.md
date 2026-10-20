# Declared attempt predecessor lineage

Migration `0018_attempt_predecessor_lineage` validates declared predecessor links
and installs the `trg_validate_attempt_predecessor` database trigger. The existing
project foreign key remains authoritative for absent or cross-project references.

A retry must retain its project, stage, logical key, dataset version, model run,
trial, promotional scope and conformance campaign. Its generation must be exactly
one greater than its predecessor's. A training trial can reference the successor
training-run attempt while retaining the same run and trial; existing same-run
trial constraints and application authorization still apply.

The supported cross-stage edge is a splitter-to-preparation handoff. Both attempts
must reference the same dataset version and matching `profile:<id>` logical keys,
with the appropriate stage suffixes. Preparation starts at generation one even
when its splitter needed retries.

Both sides of a declared edge are protected. Changing an already referenced
predecessor cannot invalidate its children. Child writes lock the predecessor;
validation runs after the statement's row changes are visible, before commit, so
self-references and cycles inserted together cannot bypass the checks.
Parent writes and child insertion serialize under read committed. Serializable
transactions may require retry after SQLSTATE `40001`. Repeatable-read transactions
cannot insert a declared predecessor edge or update lineage columns: they fail
closed with SQLSTATE `23514` rather than trusting an old child snapshot. Status,
heartbeat and other updates outside the trigger's column list retain their
existing rules.

Before deployment, preserve a database backup and rehearse the upgrade against a
copy containing representative workflow history. The migration takes a
`SHARE ROW EXCLUSIVE` lock on `workflow_attempts`, validates existing edges and
creates the trigger in one transaction. Invalid history aborts with constraint
`attempt_predecessor_lineage`; the migration does not rewrite or detach that
history. Investigate the reported lineage against authoritative records before
any separately reviewed repair. Missing predecessor links are not inferred or
backfilled by this migration.

Downgrading to `0017_upload_session_project` removes the new trigger and function.
It preserves rows and the previous critical trial-parent triggers. The schema
verifier checks the new trigger's event and column coverage, enablement, function
body and execution properties, as well as the existing trigger contracts.

This schema boundary does not qualify complete worker grants, checkpoint
allowlists, all legacy recovery, current release images or native workflow
execution. Production-sized 45 GB data, actual prior-release binaries and the
remaining Phase 1 requirements still need their own evidence. Current results are
recorded in the [completion ledger](phase-1-completion-ledger.md).
