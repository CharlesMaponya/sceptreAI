"""Retain private buffered source and native custody before external writes."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0022_private_buffered_input"
down_revision = "0021_legacy_observer_clock"
branch_labels = None
depends_on = None

FUNCTION_BODY = """
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


def upgrade() -> None:
    op.create_table(
        "buffered_upload_inputs",
        sa.Column("command_id", sa.UUID(), primary_key=True),
        sa.Column("project_id", sa.UUID(), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=True),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("manifest", postgresql.JSONB(), nullable=False),
        sa.Column("manifest_digest", sa.String(64), nullable=False),
        sa.Column("phase", sa.String(16), nullable=False, server_default="admitted"),
        sa.Column(
            "native_receipt",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["project_id", "command_id"],
            ["workflow_commands.project_id", "workflow_commands.id"],
            ondelete="RESTRICT",
            name="fk_buffered_input_command_project",
        ),
        sa.CheckConstraint(
            "byte_size > 0", name=op.f("ck_buffered_upload_inputs_buffered_input_size")
        ),
        sa.CheckConstraint(
            "content_sha256::text ~ '^[0-9a-f]{64}$'::text",
            name=op.f("ck_buffered_upload_inputs_buffered_input_sha256"),
        ),
        sa.CheckConstraint(
            "manifest_digest::text ~ '^[0-9a-f]{64}$'::text",
            name=op.f("ck_buffered_upload_inputs_buffered_input_manifest_digest"),
        ),
        sa.CheckConstraint(
            "content IS NULL OR (octet_length(content) = byte_size "
            "AND encode(sha256(content), 'hex'::text) = content_sha256::text)",
            name=op.f("ck_buffered_upload_inputs_buffered_input_bytes"),
        ),
        sa.CheckConstraint(
            "phase IN ('admitted', 'issued', 'recorded', 'published', 'retired')",
            name=op.f("ck_buffered_upload_inputs_buffered_input_phase"),
        ),
        sa.CheckConstraint(
            "content IS NOT NULL OR phase IN ('published', 'retired')",
            name=op.f("ck_buffered_upload_inputs_buffered_input_custody"),
        ),
        sa.CheckConstraint(
            "phase IN ('admitted', 'issued', 'recorded', 'published') OR content IS NULL",
            name=op.f("ck_buffered_upload_inputs_buffered_input_retired"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(manifest) = 'object'::text AND manifest <> '{}'::jsonb "
            "AND jsonb_typeof(native_receipt) = 'object'::text",
            name=op.f("ck_buffered_upload_inputs_buffered_input_objects"),
        ),
        sa.CheckConstraint(
            "phase IN ('admitted', 'issued', 'retired') OR native_receipt <> '{}'::jsonb",
            name=op.f("ck_buffered_upload_inputs_buffered_input_receipt_required"),
        ),
        sa.CheckConstraint(
            "phase IN ('recorded', 'published', 'retired') OR native_receipt = '{}'::jsonb",
            name=op.f("ck_buffered_upload_inputs_buffered_input_receipt_phase"),
        ),
    )
    op.execute(
        "CREATE FUNCTION preserve_buffered_input_custody() RETURNS trigger LANGUAGE plpgsql AS $$"
        + FUNCTION_BODY
        + "$$"
    )
    op.execute(
        "CREATE TRIGGER trg_preserve_buffered_input_custody BEFORE INSERT OR UPDATE OR DELETE "
        "ON buffered_upload_inputs FOR EACH ROW "
        "EXECUTE FUNCTION preserve_buffered_input_custody()"
    )


def downgrade() -> None:
    op.execute("LOCK TABLE buffered_upload_inputs IN ACCESS EXCLUSIVE MODE")
    op.execute("""
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM buffered_upload_inputs) THEN
        RAISE EXCEPTION 'Buffered input custody must be preserved before schema rollback'
          USING ERRCODE = '23514', CONSTRAINT = 'buffered_input_downgrade_preservation';
      END IF;
    END $$
    """)
    op.drop_table("buffered_upload_inputs")
    op.execute("DROP FUNCTION preserve_buffered_input_custody()")
