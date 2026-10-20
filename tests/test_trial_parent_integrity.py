"""Same-project trial identity must remain bound to its actual run."""

import uuid

import pytest
from automl_api.models.enums import WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingCandidate, TrainingTrial, WorkflowAttempt
from sqlalchemy.exc import IntegrityError
from test_attempt_object_grant_schema import make_attempt

pytest_plugins = ["test_phase1_postgres_contracts"]


def change_parent(db, child, other, operation):
    if operation == "trial":
        db.get(TrainingTrial, child.trial_id).candidate_id = db.get(
            TrainingTrial, other.trial_id
        ).candidate_id
    elif operation == "candidate":
        trial = db.get(TrainingTrial, child.trial_id)
        db.get(TrainingCandidate, trial.candidate_id).model_run_id = other.model_run_id
    elif operation == "parent_run":
        db.get(WorkflowAttempt, child.run_attempt_id).model_run_id = other.model_run_id
    elif operation == "parent_stage":
        parent = db.get(WorkflowAttempt, child.run_attempt_id)
        parent.stage = WorkflowStage.PREPARATION
        parent.dataset_version_id = db.get(ModelRun, child.model_run_id).dataset_version_id
        parent.model_run_id = None
    else:
        raise AssertionError(operation)


@pytest.mark.parametrize(
    "operation", ["insert", "trial_id", "trial", "candidate", "parent_run", "parent_stage"]
)
def test_all_trial_ownership_writes_preserve_same_run(phase1_db, tenant, operation):
    db = phase1_db
    user, project = tenant
    child = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
    other = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
    original_trial = child.trial_id
    with pytest.raises(IntegrityError, match="same-run"):
        with db.begin_nested():
            if operation == "insert":
                db.add(
                    WorkflowAttempt(
                        project_id=child.project_id,
                        stage=child.stage,
                        model_run_id=child.model_run_id,
                        trial_id=other.trial_id,
                        run_attempt_id=child.run_attempt_id,
                        logical_key=uuid.uuid4().hex,
                        workload_identity="trial-integrity-test",
                        generation=1,
                        fencing_token=uuid.uuid4().hex,
                    )
                )
            elif operation == "trial_id":
                child.trial_id = other.trial_id
            else:
                change_parent(db, child, other, operation)
            db.flush()
    db.refresh(child)
    assert child.trial_id == original_trial
    trial = db.get(TrainingTrial, child.trial_id)
    assert db.get(TrainingCandidate, trial.candidate_id).model_run_id == child.model_run_id
    assert db.get(WorkflowAttempt, child.run_attempt_id).model_run_id == child.model_run_id
    child.terminal_reason = "harmless metadata update"
    db.flush()


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
@pytest.mark.parametrize("operation", ["candidate", "trial", "parent_run"])
@pytest.mark.parametrize("first_action", ["insert", "mutate"])
def test_concurrent_trial_and_parent_writes_serialize(
    phase1_engine, committed_tenant, operation, first_action, isolation
):
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue
    from threading import Event
    from time import monotonic, sleep
    from types import SimpleNamespace

    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.orm import Session

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        user, project = db.get(User, user_id), db.get(Project, project_id)
        child = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
        other = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
        names = (
            "project_id",
            "stage",
            "model_run_id",
            "trial_id",
            "run_attempt_id",
            "logical_key",
            "workload_identity",
            "generation",
            "fencing_token",
        )
        template = {name: getattr(child, name) for name in names}
        child_ref = SimpleNamespace(**template)
        other_ref = SimpleNamespace(model_run_id=other.model_run_id, trial_id=other.trial_id)
        db.delete(child)  # This synthetic setup has no child before either racing write.
    ready, release = Event(), Event()
    waiting = Queue()

    def write(action, first):
        try:
            with Session(phase1_engine) as db, db.begin():
                db.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
                db.execute(text("SET LOCAL statement_timeout='15s'"))
                db.execute(text("SET LOCAL lock_timeout='10s'"))
                if not first:
                    waiting.put(db.scalar(text("SELECT pg_backend_pid()")))
                if action == "insert":
                    db.add(WorkflowAttempt(**template))
                else:
                    change_parent(db, child_ref, other_ref, operation)
                db.flush()
                if first:
                    ready.set()
                    assert release.wait(10), "Known first transaction was not released"
            return True
        except DBAPIError as error:
            assert (isinstance(error, IntegrityError) and "same-run" in str(error)) or (
                isolation == "SERIALIZABLE" and error.orig.sqlstate == "40001"
            )
            return False

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(write, first_action, True)
        second = None
        try:
            assert ready.wait(10), "Inspect first transaction; never restart on observation timeout"
            second = workers.submit(
                write, "mutate" if first_action == "insert" else "insert", False
            )
            pid = waiting.get(timeout=10)
            deadline = monotonic() + 5
            with phase1_engine.connect() as observer:
                while True:
                    state = observer.scalar(
                        text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                        {"pid": pid},
                    )
                    observer.rollback()
                    if state == "Lock":
                        break
                    assert monotonic() < deadline and not second.done(), (
                        "Expected actual row lock wait"
                    )
                    sleep(0.02)
        finally:
            release.set()
        assert second is not None
        results = [first.result(timeout=20), second.result(timeout=20)]
        assert sum(results) == 1
        if isolation == "READ COMMITTED":
            assert results == [True, False]
    with phase1_engine.connect() as db:
        assert (
            db.scalar(
                text("""
            SELECT count(*) FROM workflow_attempts a
            JOIN training_trials t ON t.id=a.trial_id
            JOIN training_candidates c ON c.id=t.candidate_id
            JOIN workflow_attempts p ON p.id=a.run_attempt_id
            WHERE a.project_id=:project AND a.stage='training_trial'
              AND (c.model_run_id<>a.model_run_id OR p.model_run_id<>a.model_run_id
                   OR p.stage<>'training_run')
        """),
                {"project": project_id},
            )
            == 0
        )


@pytest.mark.parametrize(
    "operation", ["insert", "trial_id", "trial", "candidate", "parent_run", "parent_stage"]
)
def test_repeatable_read_cannot_hide_trial_ownership_writes(
    phase1_engine, committed_tenant, operation
):
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        child = make_attempt(
            db, db.get(User, user_id), db.get(Project, project_id), WorkflowStage.TRAINING_TRIAL
        )
        other = make_attempt(
            db, db.get(User, user_id), db.get(Project, project_id), WorkflowStage.TRAINING_TRIAL
        )
        ids = child.id, other.id
    with Session(phase1_engine) as db, db.begin():
        db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        child, other = (db.get(WorkflowAttempt, identity) for identity in ids)
        with pytest.raises(IntegrityError, match="do not support repeatable read"):
            with db.begin_nested():
                if operation == "insert":
                    db.add(
                        WorkflowAttempt(
                            project_id=child.project_id,
                            stage=child.stage,
                            model_run_id=child.model_run_id,
                            trial_id=child.trial_id,
                            run_attempt_id=child.run_attempt_id,
                            logical_key=uuid.uuid4().hex,
                            workload_identity="isolation-test",
                            generation=1,
                            fencing_token=uuid.uuid4().hex,
                        )
                    )
                elif operation == "trial_id":
                    child.trial_id = other.trial_id
                else:
                    change_parent(db, child, other, operation)
                db.flush()


def test_same_run_trial_reassignment_remains_supported(phase1_db, tenant):
    db = phase1_db
    user, project = tenant
    child = make_attempt(db, user, project, WorkflowStage.TRAINING_TRIAL)
    trial = db.get(TrainingTrial, child.trial_id)
    candidate = db.get(TrainingCandidate, trial.candidate_id)
    replacement = TrainingCandidate(
        project_id=project.id,
        model_run_id=child.model_run_id,
        candidate_key=uuid.uuid4().hex,
        estimator_key=candidate.estimator_key,
        catalog_revision_id=candidate.catalog_revision_id,
        feature_recipe_revision_id=candidate.feature_recipe_revision_id,
    )
    db.add(replacement)
    db.flush()
    second = TrainingTrial(
        project_id=project.id,
        candidate_id=candidate.id,
        suggestion_id=uuid.uuid4().hex,
        params_digest="a" * 64,
    )
    db.add(second)
    db.flush()
    child.trial_id = second.id
    db.flush()
    second.candidate_id = replacement.id
    db.flush()
    assert db.get(TrainingCandidate, second.candidate_id).model_run_id == child.model_run_id


@pytest.mark.parametrize(
    "mutation",
    [
        "ALTER TABLE workflow_attempts DISABLE TRIGGER trg_validate_trial_attempt_run_parent",
        "ALTER TABLE training_trials DISABLE TRIGGER trg_preserve_trial_attempt_ownership",
        "ALTER TABLE training_candidates DISABLE TRIGGER trg_preserve_candidate_attempt_ownership",
        "DROP TRIGGER trg_validate_trial_attempt_run_parent ON workflow_attempts",
        "CREATE OR REPLACE FUNCTION validate_trial_attempt_run_parent() "
        "RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
        "CREATE OR REPLACE FUNCTION preserve_trial_attempt_ownership() "
        "RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
        "ALTER FUNCTION preserve_trial_attempt_ownership() SECURITY DEFINER",
        "ALTER FUNCTION preserve_trial_attempt_ownership() STABLE",
        "DROP TRIGGER trg_validate_trial_attempt_run_parent ON workflow_attempts; "
        "CREATE TRIGGER trg_validate_trial_attempt_run_parent BEFORE INSERT OR UPDATE OF "
        "stage, project_id, model_run_id, run_attempt_id ON workflow_attempts FOR EACH ROW "
        "EXECUTE FUNCTION validate_trial_attempt_run_parent()",
    ],
)
def test_schema_verifier_checks_critical_trial_trigger_contract(phase1_db, mutation):
    import importlib.util
    from pathlib import Path

    from sqlalchemy import text

    spec = importlib.util.spec_from_file_location(
        "trial_schema_verifier", Path("scripts/verify_database_schema.py")
    )
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    db = phase1_db
    connection = db.connection()
    assert verifier.schema_differences(connection) == []
    savepoint = db.begin_nested()
    try:
        db.execute(text(mutation))
        assert any(
            "critical trigger" in message for message in verifier.schema_differences(connection)
        )
    finally:
        savepoint.rollback()
    assert verifier.schema_differences(connection) == []
