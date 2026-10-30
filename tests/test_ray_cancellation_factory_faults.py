"""Actual process/backend loss around original-UID cancellation and publication."""

import json
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from automl_api.models.enums import OutboxStatus
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import ray_cancellation, reconciler, workflow_state
from fault_worker_environment import environment as fault_environment
from sqlalchemy import text
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_cancellation_factory import cancellation_case as cancellation_case
from test_ray_cancellation_factory import history, retry
from test_ray_submission_factory import factory_case as factory_case
from test_ray_submission_receipts import committed_submission as committed_submission


@pytest.mark.parametrize(
    "mode", ["before_delete", "after_delete", "after_fact", "database_fact", "database_publication"]
)
def test_real_process_and_backend_loss_preserve_ray_retirement(
    cancellation_case, tmp_path, monkeypatch, mode
):
    c = cancellation_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.cancel_id)
        entry.status, entry.lease_owner = OutboxStatus.CLAIMED, "owned-ray-cancel-process"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts = 1
    native_path = tmp_path / "native.json"
    native_path.write_text(json.dumps(dict(body=c.native, deletes=0)))
    marker, log_path = tmp_path / "checkpoint.json", tmp_path / "child.log"
    with log_path.open("w") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                str(
                    Path(__file__).resolve().parent / "support" / "ray_cancellation_fault_worker.py"
                ),
                str(c.cancel_id),
                str(c.project_id),
                str(tmp_path),
                mode,
            ],
            env=fault_environment(c.engine),
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 35
            while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            assert marker.exists(), log_path.read_text()[-2500:]
            witness = json.loads(marker.read_text())
            assert witness["pid"] == child.pid and child.poll() is None
            assert witness["entry_id"] == str(c.cancel_id) and witness["project_id"] == str(
                c.project_id
            )
            database = mode.startswith("database_")
            assert witness["sql_checked_out"] == int(database)
            if database:
                with c.engine.connect() as db, db.begin():
                    assert (
                        db.scalar(
                            text(
                                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                "WHERE pid=:pid AND datname=:database "
                                "AND application_name=:application"
                            ),
                            dict(
                                pid=witness["backend_pid"],
                                database=c.engine.url.database,
                                application=f"phase1_ray_cancel_fault_{child.pid}",
                            ),
                        )
                        is True
                    )
                child.stdin.write(b"c")
                child.stdin.flush()
                assert child.wait(timeout=20) == 1, log_path.read_text()[-2500:]
                assert (
                    "OperationalError" in log_path.read_text()
                    or "DBAPIError" in log_path.read_text()
                )
            else:
                child.kill()
                assert child.wait(timeout=10) == -signal.SIGKILL
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
            child.stdin.close()
    native = json.loads(native_path.read_text())
    c.native = native["body"]
    known = mode in {"after_fact", "database_publication"}
    with c.factory() as db:
        facts = history(c, db)
        assert (
            any(
                k.startswith(ray_cancellation.RESULT) and v.payload["absent"]
                for k, v in facts.items()
            )
            == known
        )
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state != "completed"
        assert db.get(OutboxEntry, c.cancel_id).status != OutboxStatus.DELIVERED
    future = datetime.now(UTC) + timedelta(minutes=5)
    clock = type(
        "Clock",
        (),
        {"now": staticmethod(lambda _tz: future), "fromisoformat": datetime.fromisoformat},
    )
    for module in (ray_cancellation, reconciler, workflow_state):
        monkeypatch.setattr(module, "datetime", clock)
    retry(c)
    if mode == "before_delete":
        assert not c.deletions
        retry(c)
    assert native["deletes"] + len(c.deletions) == 1
    with c.factory() as db:
        assert db.get(OutboxEntry, c.cancel_id).status == OutboxStatus.DELIVERED
        assert db.get(WorkflowAttempt, c.attempt_id).cleanup_state == "completed"
    witness.update(
        child_returncode=child.returncode,
        known_retirement_recovered=known,
        native_deletes=native["deletes"],
        recovery_deletes=len(c.deletions),
    )
    marker.write_text(json.dumps(witness, indent=2) + "\n")
