-- Expand once, with psql -1 -v ON_ERROR_STOP=1, before starting revised authority binaries.
-- NULL pairs preserve historical evidence; never infer revisions from old digests.
ALTER TABLE final_test_allocations
    ADD COLUMN fixture_revision VARCHAR(128),
    ADD COLUMN split_revision VARCHAR(128),
    ADD CONSTRAINT uq_final_test_revision_pair UNIQUE (fixture_revision, split_revision),
    ADD CONSTRAINT ck_final_test_allocations_allocation_revision_pair CHECK (
        (fixture_revision IS NULL AND split_revision IS NULL) OR
        (fixture_revision IS NOT NULL AND split_revision IS NOT NULL
         AND fixture_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text
         AND split_revision::text ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'::text)
    );
