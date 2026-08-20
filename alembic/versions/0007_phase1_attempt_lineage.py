"""enforce Phase 1 run and trial attempt lineage

Revision ID: 0007_phase1_attempt_lineage
Revises: 0006_phase1_public_contracts
Create Date: 2026-08-20 10:05:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0007_phase1_attempt_lineage"
down_revision = "0006_phase1_public_contracts"
branch_labels = None
depends_on = None


_ACTIVE = "status IN ('pending', 'claimed', 'submitted', 'running')"


def upgrade() -> None:
    op.add_column("workflow_attempts", sa.Column("run_attempt_id", sa.Uuid(), nullable=True))
    op.execute(
        """
        UPDATE workflow_attempts AS child
        SET run_attempt_id = (
            SELECT candidate.id
            FROM workflow_attempts AS candidate
            WHERE candidate.project_id = child.project_id
              AND candidate.model_run_id = child.model_run_id
              AND candidate.stage = 'training_run'
            ORDER BY candidate.generation DESC, candidate.created_at DESC
            LIMIT 1
        )
        WHERE child.stage = 'training_trial'
          AND child.run_attempt_id IS NULL
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM workflow_attempts
            WHERE stage = 'training_trial' AND run_attempt_id IS NULL
          ) THEN
            RAISE EXCEPTION
              'cannot migrate training_trial attempts without a training_run parent';
          END IF;
          IF EXISTS (
            SELECT project_id, logical_key
            FROM workflow_attempts
            WHERE status IN ('pending', 'claimed', 'submitted', 'running')
            GROUP BY project_id, logical_key
            HAVING count(*) > 1
          ) THEN
            RAISE EXCEPTION
              'cannot migrate multiple active generations for one logical attempt';
          END IF;
        END
        $$
        """
    )
    op.drop_constraint(
        op.f("ck_workflow_attempts_attempt_typed_parent"),
        "workflow_attempts",
        type_="check",
    )
    op.create_foreign_key(
        "fk_trial_attempt_run_attempt_project",
        "workflow_attempts",
        "workflow_attempts",
        ["project_id", "run_attempt_id"],
        ["project_id", "id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        op.f("ck_workflow_attempts_attempt_typed_parent"),
        "workflow_attempts",
        "(stage = 'training_run' AND model_run_id IS NOT NULL AND trial_id IS NULL "
        "AND run_attempt_id IS NULL AND scope_id IS NULL "
        "AND conformance_campaign_id IS NULL) OR "
        "(stage = 'training_trial' AND model_run_id IS NOT NULL AND trial_id IS NOT NULL "
        "AND run_attempt_id IS NOT NULL AND scope_id IS NULL "
        "AND conformance_campaign_id IS NULL) OR "
        "(stage IN ('splitter', 'preparation') AND dataset_version_id IS NOT NULL "
        "AND model_run_id IS NULL AND trial_id IS NULL AND run_attempt_id IS NULL "
        "AND scope_id IS NULL AND conformance_campaign_id IS NULL) OR "
        "(stage IN ('champion_refit', 'champion_evaluation') AND scope_id IS NOT NULL "
        "AND trial_id IS NULL AND run_attempt_id IS NULL "
        "AND conformance_campaign_id IS NULL) OR "
        "(stage = 'provider_conformance' AND conformance_campaign_id IS NOT NULL "
        "AND model_run_id IS NULL AND trial_id IS NULL AND run_attempt_id IS NULL "
        "AND scope_id IS NULL)",
    )
    op.create_index(
        "uq_attempt_active_logical",
        "workflow_attempts",
        ["project_id", "logical_key"],
        unique=True,
        postgresql_where=sa.text(_ACTIVE),
    )
    op.execute(
        """
        CREATE FUNCTION validate_trial_attempt_run_parent()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
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
        $$;

        CREATE TRIGGER trg_validate_trial_attempt_run_parent
        BEFORE INSERT OR UPDATE OF stage, project_id, model_run_id, run_attempt_id
        ON workflow_attempts
        FOR EACH ROW
        EXECUTE FUNCTION validate_trial_attempt_run_parent();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_validate_trial_attempt_run_parent ON workflow_attempts")
    op.execute("DROP FUNCTION validate_trial_attempt_run_parent()")
    op.drop_index("uq_attempt_active_logical", table_name="workflow_attempts")
    op.drop_constraint(
        op.f("ck_workflow_attempts_attempt_typed_parent"),
        "workflow_attempts",
        type_="check",
    )
    op.drop_constraint(
        "fk_trial_attempt_run_attempt_project",
        "workflow_attempts",
        type_="foreignkey",
    )
    op.drop_column("workflow_attempts", "run_attempt_id")
    op.create_check_constraint(
        op.f("ck_workflow_attempts_attempt_typed_parent"),
        "workflow_attempts",
        "(stage = 'training_run' AND model_run_id IS NOT NULL AND trial_id IS NULL "
        "AND scope_id IS NULL AND conformance_campaign_id IS NULL) OR "
        "(stage = 'training_trial' AND model_run_id IS NOT NULL AND trial_id IS NOT NULL "
        "AND scope_id IS NULL AND conformance_campaign_id IS NULL) OR "
        "(stage IN ('splitter', 'preparation') AND dataset_version_id IS NOT NULL "
        "AND model_run_id IS NULL AND trial_id IS NULL AND scope_id IS NULL "
        "AND conformance_campaign_id IS NULL) OR "
        "(stage IN ('champion_refit', 'champion_evaluation') AND scope_id IS NOT NULL "
        "AND trial_id IS NULL AND conformance_campaign_id IS NULL) OR "
        "(stage = 'provider_conformance' AND conformance_campaign_id IS NOT NULL "
        "AND model_run_id IS NULL AND trial_id IS NULL AND scope_id IS NULL)",
    )
