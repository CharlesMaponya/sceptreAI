"""Protect typed retry lineage and the splitter-to-preparation handoff."""

import sqlalchemy as sa

from alembic import op

revision = "0018_attempt_predecessor_lineage"
down_revision = "0017_upload_session_project"
branch_labels = None
depends_on = None

PAIR = """
  {child}.project_id = {parent}.project_id
  AND {child}.id <> {parent}.id
  AND (
    ({child}.stage = {parent}.stage
      AND {child}.logical_key = {parent}.logical_key
      AND {child}.generation = {parent}.generation + 1
      AND {child}.dataset_version_id IS NOT DISTINCT FROM {parent}.dataset_version_id
      AND {child}.model_run_id IS NOT DISTINCT FROM {parent}.model_run_id
      AND {child}.trial_id IS NOT DISTINCT FROM {parent}.trial_id
      AND {child}.scope_id IS NOT DISTINCT FROM {parent}.scope_id
      AND {child}.conformance_campaign_id IS NOT DISTINCT FROM {parent}.conformance_campaign_id)
    OR ({child}.stage = 'preparation' AND {parent}.stage = 'splitter'
      AND {child}.generation = 1
      AND {child}.dataset_version_id = {parent}.dataset_version_id
      AND {parent}.logical_key ~ '^profile:[0-9a-f-]{{36}}:splitter$'
      AND {child}.logical_key = regexp_replace(
        {parent}.logical_key, ':splitter$', ':preparation'))
  )
"""

BODY = (
    """
DECLARE
  previous workflow_attempts;
  child workflow_attempts;
BEGIN
  IF (TG_OP = 'UPDATE' OR NEW.predecessor_attempt_id IS NOT NULL)
     AND current_setting('transaction_isolation') = 'repeatable read' THEN
    RAISE EXCEPTION 'predecessor lineage writes do not support repeatable read'
      USING ERRCODE = '23514', CONSTRAINT = 'attempt_predecessor_lineage';
  END IF;
  IF NEW.predecessor_attempt_id IS NOT NULL THEN
    SELECT * INTO previous FROM workflow_attempts AS p
      WHERE p.id = NEW.predecessor_attempt_id AND p.project_id = NEW.project_id
      FOR SHARE;
    -- The existing project foreign key rejects absent or cross-project references.
    IF FOUND AND NOT ("""
    + PAIR.format(child="NEW", parent="previous")
    + """) THEN
      RAISE EXCEPTION 'Invalid declared predecessor lineage'
        USING ERRCODE = '23514', CONSTRAINT = 'attempt_predecessor_lineage';
    END IF;
  END IF;
  IF TG_OP = 'UPDATE' THEN
    FOR child IN SELECT a.* FROM workflow_attempts AS a
      WHERE a.predecessor_attempt_id = OLD.id ORDER BY a.id
    LOOP
      IF NOT ("""
    + PAIR.format(child="child", parent="NEW")
    + """) THEN
        RAISE EXCEPTION 'Parent change violates declared predecessor lineage'
          USING ERRCODE = '23514', CONSTRAINT = 'attempt_predecessor_lineage';
      END IF;
    END LOOP;
  END IF;
  RETURN NEW;
END
"""
)


def upgrade() -> None:
    op.execute("LOCK TABLE workflow_attempts IN SHARE ROW EXCLUSIVE MODE")
    op.execute(
        sa.DDL(
            """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM workflow_attempts AS a
            LEFT JOIN workflow_attempts AS p ON p.id = a.predecessor_attempt_id
            WHERE a.predecessor_attempt_id IS NOT NULL
              AND (p.id IS NULL OR NOT ("""
            + PAIR.format(child="a", parent="p")
            + """))
          ) THEN
            RAISE EXCEPTION 'Existing declared predecessor lineage is inconsistent'
              USING ERRCODE = '23514', CONSTRAINT = 'attempt_predecessor_lineage';
          END IF;
        END $$;
    """
        )
    )
    op.execute(
        sa.DDL(
            "CREATE FUNCTION validate_attempt_predecessor() RETURNS trigger "
            "LANGUAGE plpgsql AS $$" + BODY + "$$"
        )
    )
    op.execute("""
        CREATE TRIGGER trg_validate_attempt_predecessor
        AFTER INSERT OR UPDATE OF id, project_id, stage, logical_key, generation,
          dataset_version_id, model_run_id, trial_id, scope_id, conformance_campaign_id,
          predecessor_attempt_id
        ON workflow_attempts FOR EACH ROW
        EXECUTE FUNCTION validate_attempt_predecessor();
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE workflow_attempts IN SHARE ROW EXCLUSIVE MODE")
    op.execute("DROP TRIGGER trg_validate_attempt_predecessor ON workflow_attempts")
    op.execute("DROP FUNCTION validate_attempt_predecessor()")
