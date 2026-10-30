"""Expand private custody to bounded immutable parts without a single BYTEA limit."""

import sqlalchemy as sa

from alembic import op

revision = "0023_private_buffered_chunks"
down_revision = "0022_private_buffered_input"
branch_labels = None
depends_on = None

INLINE_BODY = """
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.phase <> 'admitted' OR NEW.native_receipt <> '{}'::jsonb OR NEW.content IS NULL THEN
      RAISE EXCEPTION 'Buffered input must begin with private admitted source'
        USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_initial_admission';
    END IF;
    RETURN NEW;
  END IF;
  IF TG_OP = 'DELETE' THEN
    IF OLD.phase <> 'retired' THEN
      RAISE EXCEPTION 'Buffered input must retire before deletion'
        USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_retirement_required';
    END IF;
    RETURN OLD;
  END IF;
  IF ROW(NEW.command_id, NEW.project_id, NEW.byte_size, NEW.content_sha256,
         NEW.manifest, NEW.manifest_digest)
     IS DISTINCT FROM
     ROW(OLD.command_id, OLD.project_id, OLD.byte_size, OLD.content_sha256,
         OLD.manifest, OLD.manifest_digest) THEN
    RAISE EXCEPTION 'Original buffered source is immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_original_source';
  END IF;
  IF NEW.content IS DISTINCT FROM OLD.content
     AND NOT (NEW.content IS NULL AND NEW.phase IN ('published', 'retired')) THEN
    RAISE EXCEPTION 'Buffered bytes can only be cleared after publication or retirement'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_original_bytes';
  END IF;
  IF OLD.native_receipt <> '{}'::jsonb
     AND NEW.native_receipt IS DISTINCT FROM OLD.native_receipt THEN
    RAISE EXCEPTION 'Original native buffered outcome is immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_original_receipt';
  END IF;
  IF OLD.native_receipt = '{}'::jsonb AND NEW.native_receipt <> '{}'::jsonb
     AND NOT (OLD.phase = 'issued' AND NEW.phase = 'recorded') THEN
    RAISE EXCEPTION 'Native buffered outcome requires its issued operation'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_receipt_admission';
  END IF;
  IF NEW.phase <> OLD.phase AND NOT (
       (OLD.phase = 'admitted' AND NEW.phase IN ('issued', 'retired'))
    OR (OLD.phase = 'issued' AND NEW.phase = 'recorded')
    OR (OLD.phase = 'recorded' AND NEW.phase IN ('published', 'retired'))
    OR (OLD.phase = 'published' AND NEW.phase = 'retired')
  ) THEN
    RAISE EXCEPTION 'Buffered custody cannot skip or repeat native admission'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_phase_transition';
  END IF;
  RETURN NEW;
END
"""

INPUT_BODY = (
    INLINE_BODY.replace(
        "OR NEW.content IS NULL THEN",
        "OR (NEW.content IS NULL AND NEW.chunk_count = 0) THEN",
    )
    .replace(
        "NEW.manifest, NEW.manifest_digest)",
        "NEW.manifest, NEW.manifest_digest, NEW.chunk_count, NEW.chunk_manifest_sha256)",
    )
    .replace(
        "OLD.manifest, OLD.manifest_digest)",
        "OLD.manifest, OLD.manifest_digest, OLD.chunk_count, OLD.chunk_manifest_sha256)",
    )
)

CHUNK_BODY = """
DECLARE source_phase text;
BEGIN
  SELECT phase INTO source_phase FROM buffered_upload_inputs
    WHERE command_id = COALESCE(NEW.command_id, OLD.command_id);
  IF TG_OP = 'INSERT' THEN
    IF source_phase IS NOT NULL THEN
      RAISE EXCEPTION 'Buffered parts must precede immutable input admission'
        USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_initial_admission';
    END IF;
    RETURN NEW;
  END IF;
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'Original buffered parts are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_original_bytes';
  END IF;
  IF source_phase IS NOT NULL AND source_phase NOT IN ('published', 'retired') THEN
    RAISE EXCEPTION 'Private buffered parts must retain custody'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_retirement_required';
  END IF;
  RETURN OLD;
END
"""

LAYOUT_BODY = """
DECLARE
  source buffered_upload_inputs%ROWTYPE;
  actual_count bigint;
  actual_bytes bigint;
  first_ordinal integer;
  last_ordinal integer;
  actual_manifest text;
BEGIN
  SELECT * INTO source FROM buffered_upload_inputs WHERE command_id = NEW.command_id;
  IF NOT FOUND THEN RETURN NULL; END IF;
  SELECT count(*), COALESCE(sum(byte_size),0), min(ordinal), max(ordinal),
    encode(sha256(convert_to(COALESCE(string_agg(
      ordinal::text || ':' || byte_size::text || ':' || content_sha256 || E'\n',
      '' ORDER BY ordinal), ''), 'UTF8')), 'hex')
    INTO actual_count, actual_bytes, first_ordinal, last_ordinal, actual_manifest
    FROM buffered_upload_chunks WHERE command_id = source.command_id;
  IF source.chunk_count = 0 OR source.phase IN ('published', 'retired') THEN
    IF actual_count <> 0 THEN
      RAISE EXCEPTION 'Closed or inline buffered source cannot retain private parts'
        USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_closed_custody';
    END IF;
  ELSIF actual_count <> source.chunk_count OR actual_bytes <> source.byte_size
     OR first_ordinal <> 0 OR last_ordinal <> source.chunk_count - 1
     OR actual_manifest IS DISTINCT FROM source.chunk_manifest_sha256 THEN
    RAISE EXCEPTION 'Original buffered part layout is incomplete or changed'
      USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_complete_layout';
  END IF;
  RETURN NULL;
END
"""


def upgrade():
    op.add_column(
        "buffered_upload_inputs",
        sa.Column("chunk_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "buffered_upload_inputs", sa.Column("chunk_manifest_sha256", sa.String(64), nullable=True)
    )
    op.drop_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_custody"),
        "buffered_upload_inputs",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_custody"),
        "buffered_upload_inputs",
        "content IS NOT NULL OR phase IN ('published', 'retired') OR chunk_count > 0",
    )
    op.create_check_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_chunk_layout"),
        "buffered_upload_inputs",
        "chunk_count >= 0 AND ((chunk_count = 0 AND chunk_manifest_sha256 IS NULL) OR "
        "(chunk_count > 0 AND content IS NULL AND chunk_manifest_sha256 IS NOT NULL AND "
        "chunk_manifest_sha256::text ~ '^[0-9a-f]{64}$'::text))",
    )
    op.create_table(
        "buffered_upload_chunks",
        sa.Column("command_id", sa.UUID(), primary_key=True),
        sa.Column("ordinal", sa.Integer(), primary_key=True),
        sa.Column("project_id", sa.UUID(), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.ForeignKeyConstraint(
            ["command_id"],
            ["buffered_upload_inputs.command_id"],
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
            name="fk_buffered_chunk_input",
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="RESTRICT",
            name="fk_buffered_chunk_command_project",
        ),
        sa.CheckConstraint(
            "ordinal >= 0", name=op.f("ck_buffered_upload_chunks_buffered_chunk_ordinal")
        ),
        sa.CheckConstraint(
            "byte_size > 0 AND byte_size <= 8388608",
            name=op.f("ck_buffered_upload_chunks_buffered_chunk_size"),
        ),
        sa.CheckConstraint(
            "content_sha256::text ~ '^[0-9a-f]{64}$'::text",
            name=op.f("ck_buffered_upload_chunks_buffered_chunk_sha256"),
        ),
        sa.CheckConstraint(
            "octet_length(content) = byte_size AND encode(sha256(content), "
            "'hex'::text) = content_sha256::text",
            name=op.f("ck_buffered_upload_chunks_buffered_chunk_bytes"),
        ),
    )
    op.execute(
        "CREATE OR REPLACE FUNCTION preserve_buffered_input_custody() RETURNS "
        "trigger LANGUAGE plpgsql AS $$" + INPUT_BODY + "$$"
    )
    op.execute(
        "CREATE FUNCTION preserve_buffered_chunk_custody() RETURNS trigger LANGUAGE plpgsql AS $$"
        + CHUNK_BODY
        + "$$"
    )
    op.execute(
        "CREATE FUNCTION validate_buffered_chunk_layout() RETURNS trigger LANGUAGE plpgsql AS $$"
        + LAYOUT_BODY
        + "$$"
    )
    op.execute(
        "CREATE TRIGGER trg_preserve_buffered_chunk_custody BEFORE INSERT OR "
        "UPDATE OR DELETE ON buffered_upload_chunks FOR EACH ROW EXECUTE "
        "FUNCTION preserve_buffered_chunk_custody()"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_validate_buffered_chunk_layout AFTER "
        "INSERT OR UPDATE ON buffered_upload_inputs DEFERRABLE INITIALLY "
        "DEFERRED FOR EACH ROW EXECUTE FUNCTION validate_buffered_chunk_layout()"
    )


def downgrade():
    op.execute("LOCK TABLE buffered_upload_inputs, buffered_upload_chunks IN ACCESS EXCLUSIVE MODE")
    op.execute("""DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM buffered_upload_chunks) OR EXISTS (
        SELECT 1 FROM buffered_upload_inputs WHERE chunk_count <> 0
      ) THEN
        RAISE EXCEPTION 'Chunked private source and history must survive schema rollback'
          USING ERRCODE = '23514', CONSTRAINT = 'buffered_chunk_downgrade_preservation';
      END IF;
    END $$""")
    op.execute("DROP TRIGGER trg_validate_buffered_chunk_layout ON buffered_upload_inputs")
    op.execute("DROP FUNCTION validate_buffered_chunk_layout()")
    op.drop_table("buffered_upload_chunks")
    op.execute("DROP FUNCTION preserve_buffered_chunk_custody()")
    op.drop_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_chunk_layout"),
        "buffered_upload_inputs",
        type_="check",
    )
    op.drop_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_custody"),
        "buffered_upload_inputs",
        type_="check",
    )
    op.create_check_constraint(
        op.f("ck_buffered_upload_inputs_buffered_input_custody"),
        "buffered_upload_inputs",
        "content IS NOT NULL OR phase IN ('published', 'retired')",
    )
    op.execute(
        "CREATE OR REPLACE FUNCTION preserve_buffered_input_custody() RETURNS trigger "
        "LANGUAGE plpgsql AS $$" + INLINE_BODY + "$$"
    )
    op.drop_column("buffered_upload_inputs", "chunk_manifest_sha256")
    op.drop_column("buffered_upload_inputs", "chunk_count")
