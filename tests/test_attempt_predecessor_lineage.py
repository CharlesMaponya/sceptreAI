"""Declared predecessors must be typed retries or the dataset stage handoff."""

import uuid

import pytest
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from sqlalchemy.exc import IntegrityError
from test_attempt_object_grant_schema import make_attempt

pytest_plugins = ["test_phase1_postgres_contracts"]
PARENTS = (
    "dataset_version_id",
    "model_run_id",
    "trial_id",
    "run_attempt_id",
    "scope_id",
    "conformance_campaign_id",
)


def retry(previous, **updates):
    return WorkflowAttempt(
        **{
            **{name: getattr(previous, name) for name in PARENTS},
            "project_id": previous.project_id,
            "stage": previous.stage,
            "logical_key": previous.logical_key,
            "workload_identity": previous.workload_identity,
            "generation": previous.generation + 1,
            "fencing_token": uuid.uuid4().hex,
            "predecessor_attempt_id": previous.id,
            **updates,
        }
    )


@pytest.mark.parametrize("stage", list(WorkflowStage))
@pytest.mark.parametrize("action", ["insert", "child_update", "predecessor_update"])
@pytest.mark.parametrize("defect", ["stage", "logical_key", "generation", "parent"])
def test_declared_predecessor_rejects_invalid_lineage(phase1_db, tenant, stage, action, defect):
    db = phase1_db
    user, project = tenant
    previous = make_attempt(db, user, project, stage)
    stages = list(WorkflowStage)
    other_stage = stages[(stages.index(stage) + 1) % len(stages)] if defect == "stage" else stage
    other = make_attempt(db, user, project, other_stage)
    previous.status = other.status = AttemptStatus.FAILED
    db.flush()
    child = retry(previous)
    if action != "insert":
        db.add(child)
        db.flush()
    with pytest.raises(IntegrityError, match="predecessor lineage"):
        with db.begin_nested():
            target = previous if action == "predecessor_update" else child
            if defect == "stage":
                if action == "predecessor_update":
                    target.stage = other.stage
                    for name in PARENTS:
                        setattr(target, name, getattr(other, name))
                else:
                    target.predecessor_attempt_id = other.id
            elif defect == "logical_key":
                target.logical_key = uuid.uuid4().hex
            elif defect == "generation":
                target.generation += 3
            else:
                for name in PARENTS:
                    setattr(target, name, getattr(other, name))
            if action == "insert":
                db.add(child)
            db.flush()


@pytest.mark.parametrize("stage", list(WorkflowStage))
def test_declared_predecessor_accepts_same_parent_monotonic_retry(phase1_db, tenant, stage):
    db = phase1_db
    previous = make_attempt(db, *tenant, stage)
    previous.status = AttemptStatus.FAILED
    db.flush()
    child = retry(previous)
    db.add(child)
    db.flush()
    assert child.predecessor_attempt_id == previous.id and child.generation == 2
    previous.terminal_reason = "retained historical failure"
    child.cleanup_state = "pending"
    db.flush()


def handoff(db, tenant, *, generation=1, reverse=False):
    previous = make_attempt(
        db, *tenant, WorkflowStage.PREPARATION if reverse else WorkflowStage.SPLITTER
    )
    profile = uuid.uuid4()
    previous.logical_key = f"profile:{profile}:{previous.stage.value}"
    previous.status = AttemptStatus.SUCCEEDED
    previous.generation = generation
    db.flush()
    stage = WorkflowStage.SPLITTER if reverse else WorkflowStage.PREPARATION
    return previous, retry(
        previous, stage=stage, logical_key=f"profile:{profile}:{stage.value}", generation=1
    )


@pytest.mark.parametrize("generation", [1, 3])
def test_declared_predecessor_preserves_splitter_preparation_handoff(phase1_db, tenant, generation):
    db = phase1_db
    previous, child = handoff(db, tenant, generation=generation)
    db.add(child)
    db.flush()
    assert child.generation == 1 and child.predecessor_attempt_id == previous.id


@pytest.mark.parametrize("defect", ["profile", "dataset", "reverse", "generation"])
def test_declared_predecessor_rejects_invalid_stage_handoff(phase1_db, tenant, defect):
    db = phase1_db
    _, child = handoff(db, tenant, reverse=defect == "reverse")
    if defect == "profile":
        child.logical_key = f"profile:{uuid.uuid4()}:preparation"
    elif defect == "dataset":
        child.dataset_version_id = make_attempt(
            db, *tenant, WorkflowStage.SPLITTER
        ).dataset_version_id
    elif defect == "generation":
        child.generation = 2
    with pytest.raises(IntegrityError, match="predecessor lineage"):
        with db.begin_nested():
            db.add(child)
            db.flush()


@pytest.mark.parametrize("stage", list(WorkflowStage))
@pytest.mark.parametrize("action", ["insert", "update"])
def test_declared_predecessor_self_reference_is_rejected(phase1_db, tenant, stage, action):
    db = phase1_db
    previous = make_attempt(db, *tenant, stage)
    previous.status = AttemptStatus.FAILED
    db.flush()
    row_id = uuid.uuid4()
    child = retry(previous, id=row_id)
    if action == "update":
        db.add(child)
        db.flush()
    with pytest.raises(IntegrityError, match="predecessor lineage"):
        with db.begin_nested():
            child.predecessor_attempt_id = row_id
            if action == "insert":
                db.add(child)
            db.flush()


@pytest.mark.parametrize("stage", list(WorkflowStage))
def test_declared_predecessor_two_row_cycle_is_rejected(phase1_db, tenant, stage):
    db = phase1_db
    previous = make_attempt(db, *tenant, stage)
    previous.status = AttemptStatus.FAILED
    db.flush()
    first_id, second_id, key = uuid.uuid4(), uuid.uuid4(), uuid.uuid4().hex
    with pytest.raises(IntegrityError, match="predecessor lineage"):
        with db.begin_nested():
            db.add_all(
                [
                    retry(
                        previous,
                        id=first_id,
                        logical_key=key,
                        generation=1,
                        predecessor_attempt_id=second_id,
                        status=AttemptStatus.FAILED,
                    ),
                    retry(
                        previous,
                        id=second_id,
                        logical_key=key,
                        generation=2,
                        predecessor_attempt_id=first_id,
                        status=AttemptStatus.FAILED,
                    ),
                ]
            )
            db.flush()


@pytest.mark.parametrize("isolation", ["READ COMMITTED", "SERIALIZABLE"])
@pytest.mark.parametrize("operation", ["logical_key", "generation", "parent"])
@pytest.mark.parametrize("first_action", ["insert", "mutate"])
def test_declared_predecessor_concurrent_writes_serialize(
    phase1_engine, committed_tenant, isolation, operation, first_action
):
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue
    from threading import Event
    from time import monotonic, sleep

    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy import select, text
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.orm import Session

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        user, project = db.get(User, user_id), db.get(Project, project_id)
        previous = make_attempt(db, user, project)
        other = make_attempt(db, user, project)
        previous.status = other.status = AttemptStatus.FAILED
        db.flush()
        previous_id, other_run = previous.id, other.model_run_id
        child = retry(previous)
        values = {
            name: getattr(child, name)
            for name in (
                *PARENTS,
                "project_id",
                "stage",
                "logical_key",
                "workload_identity",
                "generation",
                "fencing_token",
                "predecessor_attempt_id",
            )
        }
    ready, release, waiting = Event(), Event(), Queue()

    def write(action, first):
        try:
            with Session(phase1_engine) as db, db.begin():
                db.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
                db.execute(text("SET LOCAL statement_timeout='15s'"))
                db.execute(text("SET LOCAL lock_timeout='10s'"))
                if not first:
                    waiting.put(db.scalar(text("SELECT pg_backend_pid()")))
                if action == "insert":
                    db.add(WorkflowAttempt(**values))
                else:
                    row = db.get(WorkflowAttempt, previous_id)
                    if operation == "parent":
                        row.model_run_id = other_run
                    elif operation == "generation":
                        row.generation = 4
                    else:
                        row.logical_key = uuid.uuid4().hex
                db.flush()
                if first:
                    ready.set()
                    assert release.wait(10)
            return True
        except DBAPIError as error:
            assert (isinstance(error, IntegrityError) and "predecessor lineage" in str(error)) or (
                isolation == "SERIALIZABLE" and error.orig.sqlstate == "40001"
            )
            return False

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(write, first_action, True)
        second = None
        try:
            assert ready.wait(10)
            second = workers.submit(
                write, "mutate" if first_action == "insert" else "insert", False
            )
            pid, deadline = waiting.get(timeout=10), monotonic() + 5
            with phase1_engine.connect() as observer:
                while True:
                    state = observer.scalar(
                        text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                        {"pid": pid},
                    )
                    observer.rollback()
                    if state == "Lock":
                        break
                    assert monotonic() < deadline and not second.done()
                    sleep(0.02)
        finally:
            release.set()
        assert second is not None
        outcomes = [first.result(timeout=20), second.result(timeout=20)]
        assert sum(outcomes) == 1
        if isolation == "READ COMMITTED":
            assert outcomes == [True, False]
    with Session(phase1_engine) as db:
        previous = db.get(WorkflowAttempt, previous_id)
        children = list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.predecessor_attempt_id == previous_id)
            )
        )
        assert all(
            child.logical_key == previous.logical_key
            and child.model_run_id == previous.model_run_id
            and child.generation == previous.generation + 1
            for child in children
        )


@pytest.mark.parametrize("stage", list(WorkflowStage))
@pytest.mark.parametrize("action", ["insert", "predecessor_update"])
def test_declared_predecessor_repeatable_read_fails_closed(
    phase1_engine, committed_tenant, stage, action
):
    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        previous = make_attempt(db, db.get(User, user_id), db.get(Project, project_id), stage)
        previous.status = AttemptStatus.FAILED
        db.flush()
        previous_id = previous.id
    with pytest.raises(IntegrityError) as rejected:
        with Session(phase1_engine) as db, db.begin():
            db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            previous = db.get(WorkflowAttempt, previous_id)
            if action == "insert":
                db.add(retry(previous))
            else:
                previous.logical_key = uuid.uuid4().hex
            db.flush()
    assert rejected.value.orig.sqlstate == "23514"
    expected = (
        "trial_attempt_run_parent"
        if stage == WorkflowStage.TRAINING_TRIAL and action == "insert"
        else "attempt_predecessor_lineage"
    )
    assert rejected.value.orig.diag.constraint_name == expected


def schema_verifier():
    import importlib.util

    spec = importlib.util.spec_from_file_location("schema", "scripts/verify_database_schema.py")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    return verifier


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE workflow_attempts DISABLE TRIGGER trg_validate_attempt_predecessor",
        "ALTER TABLE workflow_attempts ENABLE ALWAYS TRIGGER trg_validate_attempt_predecessor",
        "ALTER FUNCTION validate_attempt_predecessor() STABLE",
        "CREATE OR REPLACE FUNCTION validate_attempt_predecessor() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
        "DROP TRIGGER trg_validate_attempt_predecessor ON workflow_attempts; "
        "CREATE TRIGGER trg_validate_attempt_predecessor BEFORE INSERT ON workflow_attempts "
        "FOR EACH ROW EXECUTE FUNCTION validate_attempt_predecessor()",
    ],
)
def test_declared_predecessor_trigger_contract_is_verified(phase1_db, statement):
    from sqlalchemy import text

    db, verifier = phase1_db, schema_verifier()
    assert verifier.schema_differences(db.connection()) == []
    savepoint = db.begin_nested()
    try:
        db.execute(text(statement))
        assert any(
            "trg_validate_attempt_predecessor" in difference
            for difference in verifier.schema_differences(db.connection())
        )
    finally:
        savepoint.rollback()
    assert verifier.schema_differences(db.connection()) == []


@pytest.mark.parametrize(
    "defect", [None, "stage", "logical_key", "generation", "parent", "handoff"]
)
def test_declared_predecessor_migration_preserves_or_rejects_history(defect):
    import os
    import subprocess

    from automl_api.models.iam import User
    from automl_api.models.projects import Project
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import make_url
    from sqlalchemy.orm import Session
    from test_phase1_migration_rehearsal import drop_database, rehearsal

    source = make_url(os.environ["DATABASE_URL"])
    assert source.database.endswith("_tests")
    admin = create_engine(source.set(database="postgres"), isolation_level="AUTOCOMMIT")
    name = "sceptre_phase1_rehearsal_lineage_" + uuid.uuid4().hex
    with admin.connect() as db:
        db.execute(text(f'CREATE DATABASE "{name}"'))
    target = source.set(database=name).render_as_string(hide_password=False)
    engine = create_engine(target)

    def migrate(action, revision):
        return subprocess.run(
            [rehearsal.sys.executable, "-m", "alembic", action, revision],
            env={**os.environ, "DATABASE_URL": target},
            capture_output=True,
            text=True,
        )

    def triggers(db):
        return list(
            db.execute(
                text(
                    "SELECT t.tgname, pg_get_triggerdef(t.oid), pg_get_functiondef(t.tgfoid) "
                    "FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid "
                    "WHERE NOT t.tgisinternal AND pg_table_is_visible(r.oid) ORDER BY t.tgname"
                )
            )
        )

    try:
        result = migrate("upgrade", "0017_upload_session_project")
        assert result.returncode == 0, result.stderr
        repair = None
        with Session(engine) as db, db.begin():
            user = User(
                email=uuid.uuid4().hex + "@example.test",
                full_name="lineage fixture",
                auth_provider="simple",
                global_role="member",
            )
            db.add(user)
            db.flush()
            project = Project(owner_id=user.id, created_by_id=user.id, name="lineage fixture")
            db.add(project)
            db.flush()
            for stage in WorkflowStage:
                previous = make_attempt(db, user, project, stage)
                previous.status = AttemptStatus.FAILED
                db.flush()
                child = retry(previous)
                db.add(child)
                db.flush()
                if stage == WorkflowStage.TRAINING_RUN and defect not in {None, "handoff"}:
                    field = {
                        "stage": "predecessor_attempt_id",
                        "logical_key": "logical_key",
                        "generation": "generation",
                        "parent": "model_run_id",
                    }[defect]
                    repair = (child.id, field, getattr(child, field))
                    if defect == "stage":
                        child.predecessor_attempt_id = make_attempt(
                            db, user, project, WorkflowStage.SPLITTER
                        ).id
                    elif defect == "logical_key":
                        child.logical_key = uuid.uuid4().hex
                    elif defect == "generation":
                        child.generation = 4
                    else:
                        child.model_run_id = make_attempt(db, user, project).model_run_id
            _, child = handoff(db, (user, project), generation=3)
            db.add(child)
            db.flush()
            if defect == "handoff":
                repair = (child.id, "logical_key", child.logical_key)
                child.logical_key = f"profile:{uuid.uuid4()}:preparation"
        with engine.connect() as db:
            tables = [
                table for table in inspect(db).get_table_names() if table != "alembic_version"
            ]
            before = rehearsal._snapshot_seeded_rows(db, tables)
            previous_triggers = triggers(db)
        if defect:
            rejected = migrate("upgrade", "head")
            assert rejected.returncode != 0 and "predecessor lineage" in rejected.stderr
            with engine.connect() as db:
                assert (
                    db.scalar(text("SELECT version_num FROM alembic_version"))
                    == "0017_upload_session_project"
                )
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                assert triggers(db) == previous_triggers
            # Restore only the deliberate fault in this disposable fixture before resuming.
            with Session(engine) as db, db.begin():
                row_id, field, value = repair
                setattr(db.get(WorkflowAttempt, row_id), field, value)
            with engine.connect() as db:
                before = rehearsal._snapshot_seeded_rows(db, tables)
        for action, revision in (
            ("upgrade", "head"),
            ("downgrade", "0017_upload_session_project"),
            ("upgrade", "head"),
        ):
            result = migrate(action, revision)
            assert result.returncode == 0, result.stderr
            with engine.connect() as db:
                assert rehearsal._snapshot_seeded_rows(db, tables) == before
                if action == "downgrade":
                    assert triggers(db) == previous_triggers
                else:
                    assert schema_verifier().schema_differences(db) == []
    finally:
        engine.dispose()
        drop_database(admin, name)
        admin.dispose()
