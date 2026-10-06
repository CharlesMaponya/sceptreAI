"""Exercise real outbox claims and backend loss on an isolated local PgBouncer fixture."""

import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import psycopg
from automl_api.core.config import Settings
from automl_api.db import base as _base  # noqa: F401
from automl_api.db import session
from automl_api.models.enums import AuthProvider, OutboxStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import OutboxEntry
from automl_api.services.workflow_state import (
    begin_command,
    claim_outbox,
    complete_outbox,
    enqueue_outbox,
)
from sqlalchemy import delete, func, make_url, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session


def main():
    url = make_url(os.environ["PGBOUNCER_TEST_DATABASE_URL"])
    direct = make_url(os.environ["PGBOUNCER_TEST_DIRECT_URL"])
    for target in (url, direct):
        assert target.host in {"localhost", "127.0.0.1"} and target.database.endswith("_tests")
    ca = Path(os.environ["PGBOUNCER_TEST_CA"]).resolve()
    config = Settings(
        database_url=url.render_as_string(hide_password=False),
        pgbouncer_transaction_mode=True,
        database_ssl_mode="verify-full",
        database_ssl_root_cert=ca,
        database_pool_size=4,
        database_max_overflow=0,
        database_statement_timeout_ms=5000,
        database_lock_timeout_ms=1000,
        database_idle_transaction_timeout_ms=15000,
    )
    session._engine = None
    session.get_settings = lambda: config
    engine = session.get_engine()
    raw = dict(
        host=url.host,
        port=url.port,
        user=url.username,
        password=url.password,
        sslmode="verify-full",
        sslrootcert=str(ca),
    )
    project_id, user_id = None, None
    with psycopg.connect(**raw, dbname="pgbouncer", autocommit=True) as admin:
        original = {r[0]: r[1] for r in admin.execute("show config")}["default_pool_size"]
        assert original == "1", "Expected dedicated single-backend fixture"
        admin.execute("set default_pool_size = 4")
        try:
            with Session(engine) as db, db.begin():
                assert (
                    db.scalar(
                        select(func.count())
                        .select_from(OutboxEntry)
                        .where(OutboxEntry.status.in_([OutboxStatus.PENDING, OutboxStatus.CLAIMED]))
                    )
                    == 0
                ), "Do not consume another unfinished test run"
                user = User(
                    email=f"pooler-{uuid.uuid4().hex}@example.test",
                    full_name="Pooler fixture",
                    auth_provider=AuthProvider.SIMPLE,
                )
                db.add(user)
                db.flush()
                user_id = user.id
                project = Project(owner_id=user.id, created_by_id=user.id, name="Pooler fixture")
                db.add(project)
                db.flush()
                project_id = project.id

            def seed(count):
                with Session(engine) as db, db.begin():
                    ids = []
                    for _ in range(count):
                        command, _ = begin_command(
                            db,
                            project_id=project_id,
                            actor_id=user_id,
                            operation="training.launch",
                            idempotency_key=uuid.uuid4().hex,
                            payload={},
                        )
                        ids.append(
                            enqueue_outbox(
                                db,
                                command,
                                topic="ray.training.submit",
                                aggregate_type="model_run",
                                aggregate_id=uuid.uuid4(),
                                payload={},
                            ).id
                        )
                    return ids

            victim_id = seed(1)[0]
            with Session(engine) as db:
                assert [r.id for r in claim_outbox(db, worker_id="lost-connection")] == [victim_id]
                pid = db.execute(text("select pg_backend_pid()")).scalar_one()
                with psycopg.connect(
                    **{**raw, "host": direct.host, "port": direct.port},
                    dbname=direct.database,
                    autocommit=True,
                ) as killer:
                    assert killer.execute("select pg_terminate_backend(%s)", (pid,)).fetchone()[0]
                try:
                    db.commit()
                except DBAPIError:
                    db.rollback()
                else:
                    raise AssertionError("Killed backend unexpectedly committed")
            with Session(engine) as db, db.begin():
                row = db.get(OutboxEntry, victim_id)
                assert row.status == OutboxStatus.PENDING and row.delivery_attempts == 0
                assert row.lease_owner is None
                assert [r.id for r in claim_outbox(db, worker_id="recovered")] == [victim_id]
                complete_outbox(db, victim_id, worker_id="recovered", delivered=True)
                complete_outbox(db, victim_id, worker_id="recovered", delivered=True)
            expected = set(seed(32))
            barrier = Barrier(4, timeout=10)

            def claim(index):
                worker = f"parallel-{index}"
                with Session(engine) as db, db.begin():
                    barrier.wait()
                    rows = claim_outbox(db, worker_id=worker, limit=8)
                    assert len(rows) == 8
                    ids = [r.id for r in rows]
                    backend = db.execute(text("select pg_backend_pid()")).scalar_one()
                    barrier.wait()  # All four claims hold their row locks concurrently.
                with Session(engine) as db, db.begin():
                    for row_id in ids:
                        complete_outbox(db, row_id, worker_id=worker, delivered=True)
                        complete_outbox(db, row_id, worker_id=worker, delivered=True)
                return ids, backend

            with ThreadPoolExecutor(max_workers=4) as workers:
                results = list(workers.map(claim, range(4)))
            ids = [row_id for batch, _ in results for row_id in batch]
            assert len(ids) == len(set(ids)) == 32 and set(ids) == expected
            assert len({pid for _, pid in results}) == 4
            with Session(engine) as db:
                rows = list(
                    db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == project_id))
                )
                assert len(rows) == 33
                assert all(
                    r.status == OutboxStatus.DELIVERED and r.delivery_attempts == 1 for r in rows
                )
            print(
                json.dumps(
                    dict(
                        status="passed",
                        parallel_workers=4,
                        distinct_backends=4,
                        disjoint_claims=32,
                        rolled_back_claims=1,
                        recovered_deliveries=1,
                        terminal_replays=33,
                    )
                )
            )
        finally:
            if project_id:
                with Session(engine) as db, db.begin():
                    db.execute(delete(Project).where(Project.id == project_id))
                    db.execute(delete(User).where(User.id == user_id))
            engine.dispose()
            admin.execute("set default_pool_size = 1")


if __name__ == "__main__":
    main()
