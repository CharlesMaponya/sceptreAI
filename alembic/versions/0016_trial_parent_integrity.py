"""Keep training trial attempts bound to stable run, trial and candidate parents."""

import sqlalchemy as sa

from alembic import op

revision = "0016_trial_parent_integrity"
down_revision = "0015_attempt_predecessor_project"
branch_labels = None
depends_on = None

CHILD_BODY = """
BEGIN
  IF (TG_OP = 'UPDATE' OR NEW.stage = 'training_trial')
     AND current_setting('transaction_isolation') = 'repeatable read' THEN
    RAISE EXCEPTION 'same-run trial ownership writes do not support repeatable read'
      USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
  END IF;
  IF TG_OP = 'UPDATE' AND EXISTS (
    SELECT 1 FROM workflow_attempts AS child
    WHERE child.run_attempt_id = OLD.id AND child.stage = 'training_trial'
      AND (NEW.stage <> 'training_run'
        OR child.project_id IS DISTINCT FROM NEW.project_id
        OR child.model_run_id IS DISTINCT FROM NEW.model_run_id)
  ) THEN
    RAISE EXCEPTION 'Parent change violates same-run trial attempt ownership'
      USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
  END IF;
  IF NEW.stage = 'training_trial' THEN
    PERFORM 1
    FROM workflow_attempts AS parent
    JOIN training_trials AS trial
      ON trial.id = NEW.trial_id AND trial.project_id = NEW.project_id
    JOIN training_candidates AS candidate
      ON candidate.id = trial.candidate_id AND candidate.project_id = NEW.project_id
    WHERE parent.id = NEW.run_attempt_id AND parent.project_id = NEW.project_id
      AND parent.stage = 'training_run' AND parent.model_run_id = NEW.model_run_id
      AND candidate.model_run_id = NEW.model_run_id
    FOR SHARE OF parent, trial, candidate;
    IF NOT FOUND THEN
      RAISE EXCEPTION
        'training_trial attempt requires a same-run training_run parent and same-run trial'
        USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
    END IF;
  END IF;
  RETURN NEW;
END
"""

PARENT_BODY = """
BEGIN
  IF current_setting('transaction_isolation') = 'repeatable read' THEN
    RAISE EXCEPTION 'same-run trial ownership writes do not support repeatable read'
      USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
  END IF;
  IF TG_TABLE_NAME = 'training_trials' THEN
    PERFORM 1 FROM training_candidates AS candidate
      WHERE candidate.id = NEW.candidate_id AND candidate.project_id = NEW.project_id
      FOR SHARE;
    IF EXISTS (
      SELECT 1 FROM workflow_attempts AS child
      WHERE child.trial_id = OLD.id AND child.stage = 'training_trial'
        AND (child.project_id IS DISTINCT FROM NEW.project_id
          OR child.model_run_id IS DISTINCT FROM (
            SELECT candidate.model_run_id FROM training_candidates AS candidate
            WHERE candidate.id = NEW.candidate_id AND candidate.project_id = NEW.project_id
          ))
    ) THEN
      RAISE EXCEPTION 'Trial change violates same-run trial attempt ownership'
        USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
    END IF;
  ELSIF TG_TABLE_NAME = 'training_candidates' THEN
    IF EXISTS (
      SELECT 1 FROM workflow_attempts AS child
      JOIN training_trials AS trial ON trial.id = child.trial_id
      WHERE trial.candidate_id = OLD.id AND child.stage = 'training_trial'
        AND (child.project_id IS DISTINCT FROM NEW.project_id
          OR child.model_run_id IS DISTINCT FROM NEW.model_run_id)
    ) THEN
      RAISE EXCEPTION 'Candidate change violates same-run trial attempt ownership'
        USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
    END IF;
  END IF;
  RETURN NEW;
END
"""

OLD_BODY = """
        BEGIN
          IF NEW.stage = 'training_trial' AND NOT EXISTS (
            SELECT 1
            FROM workflow_attempts AS parent
            WHERE parent.project_id = NEW.project_id
              AND parent.id = NEW.run_attempt_id
              AND parent.stage = 'training_run'
              AND parent.model_run_id = NEW.model_run_id
          ) THEN
            RAISE EXCEPTION
              'training_trial attempt requires a same-run training_run parent'
              USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
          END IF;
          RETURN NEW;
        END
        """


def _lock():
    op.execute(
        "LOCK TABLE workflow_attempts, training_trials, training_candidates "
        "IN SHARE ROW EXCLUSIVE MODE"
    )


def upgrade() -> None:
    _lock()
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM workflow_attempts AS child
            LEFT JOIN workflow_attempts AS parent ON parent.id = child.run_attempt_id
              AND parent.project_id = child.project_id
            LEFT JOIN training_trials AS trial ON trial.id = child.trial_id
              AND trial.project_id = child.project_id
            LEFT JOIN training_candidates AS candidate ON candidate.id = trial.candidate_id
              AND candidate.project_id = child.project_id
            WHERE child.stage = 'training_trial'
              AND (parent.id IS NULL OR parent.stage <> 'training_run'
                OR parent.model_run_id IS DISTINCT FROM child.model_run_id
                OR candidate.id IS NULL
                OR candidate.model_run_id IS DISTINCT FROM child.model_run_id)
          ) THEN
            RAISE EXCEPTION 'Existing same-run trial ownership is inconsistent'
              USING ERRCODE = '23514', CONSTRAINT = 'trial_attempt_run_parent';
          END IF;
        END $$;
    """)
    for name, column in (
        ("ix_attempt_run_parent", "run_attempt_id"),
        ("ix_attempt_trial_parent", "trial_id"),
    ):
        op.create_index(
            name,
            "workflow_attempts",
            [column, "project_id"],
            postgresql_where=sa.text(f"{column} IS NOT NULL"),
        )
    op.execute(
        "CREATE OR REPLACE FUNCTION validate_trial_attempt_run_parent() "
        "RETURNS trigger LANGUAGE plpgsql AS $$" + CHILD_BODY + "$$"
    )
    op.execute(
        "CREATE FUNCTION preserve_trial_attempt_ownership() "
        "RETURNS trigger LANGUAGE plpgsql AS $$" + PARENT_BODY + "$$"
    )
    op.execute("DROP TRIGGER trg_validate_trial_attempt_run_parent ON workflow_attempts")
    op.execute("""
        CREATE TRIGGER trg_validate_trial_attempt_run_parent
        BEFORE INSERT OR UPDATE OF stage, project_id, model_run_id, run_attempt_id, trial_id
        ON workflow_attempts FOR EACH ROW
        EXECUTE FUNCTION validate_trial_attempt_run_parent();
        CREATE TRIGGER trg_preserve_trial_attempt_ownership
        BEFORE UPDATE OF project_id, candidate_id ON training_trials FOR EACH ROW
        EXECUTE FUNCTION preserve_trial_attempt_ownership();
        CREATE TRIGGER trg_preserve_candidate_attempt_ownership
        BEFORE UPDATE OF project_id, model_run_id ON training_candidates FOR EACH ROW
        EXECUTE FUNCTION preserve_trial_attempt_ownership();
    """)


def downgrade() -> None:
    _lock()
    op.execute("DROP TRIGGER trg_preserve_candidate_attempt_ownership ON training_candidates")
    op.execute("DROP TRIGGER trg_preserve_trial_attempt_ownership ON training_trials")
    op.execute("DROP FUNCTION preserve_trial_attempt_ownership()")
    op.execute("DROP TRIGGER trg_validate_trial_attempt_run_parent ON workflow_attempts")
    op.execute(
        "CREATE OR REPLACE FUNCTION validate_trial_attempt_run_parent() "
        "RETURNS trigger LANGUAGE plpgsql AS $$" + OLD_BODY + "$$"
    )
    op.execute("""
        CREATE TRIGGER trg_validate_trial_attempt_run_parent
        BEFORE INSERT OR UPDATE OF stage, project_id, model_run_id, run_attempt_id
        ON workflow_attempts FOR EACH ROW
        EXECUTE FUNCTION validate_trial_attempt_run_parent();
    """)
    op.drop_index("ix_attempt_trial_parent", table_name="workflow_attempts")
    op.drop_index("ix_attempt_run_parent", table_name="workflow_attempts")
