"""Actual owned process/backend loss across original native write boundaries."""

import json
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from automl_api.models.enums import OutboxStatus
from automl_api.models.workflows import OutboxEntry
from automl_api.services import ray_submission
from fault_worker_environment import environment as fault_environment
from sqlalchemy import text
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_ray_submission_factory import events, retry
from test_ray_submission_factory import factory_case as factory_case
from test_ray_submission_receipts import committed_submission as committed_submission


@pytest.mark.parametrize(
    "mode",
    [
        "before_sa",
        "after_sa",
        "before_create",
        "after_create",
        "after_create_fact",
        "after_activate",
        "after_activation_fact",
        "database_fact",
        "database_publication",
    ],
)
def test_real_process_and_backend_loss_preserve_original_ray_history(
    factory_case, tmp_path, monkeypatch, mode
):
    c = factory_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = "owned-ray-process"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts += 1
    marker, log_path = tmp_path / "checkpoint.json", tmp_path / "child.log"
    with log_path.open("w") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve().parent / "support" / "ray_submission_fault_worker.py"),
                str(c.entry_id),
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
            assert witness["pid"] == child.pid and witness["entry_id"] == str(c.entry_id)
            assert witness["project_id"] == str(c.project_id) and child.poll() is None
            assert witness["sql_checked_out"] == int(mode.startswith("database_"))
            if mode.startswith("database_"):
                with c.engine.connect() as db, db.begin():
                    terminated = db.scalar(
                        text(
                            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE pid=:pid AND datname=:database AND application_name=:application"
                        ),
                        dict(
                            pid=witness["backend_pid"],
                            database=c.engine.url.database,
                            application=f"phase1_ray_fault_{child.pid}",
                        ),
                    )
                    assert terminated is True
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
    native_path = tmp_path / "native.json"
    native = json.loads(native_path.read_text()) if native_path.exists() else None
    assert bool(native) is (mode not in {"before_sa", "after_sa", "before_create"})
    if native:
        assert native["creates"] == 1
        c.native = native["body"]
    known = mode in {
        "after_create_fact",
        "after_activate",
        "after_activation_fact",
        "database_publication",
    }
    with c.factory() as db:
        history = events(c, db)
        assert (ray_submission.CREATE_RESULT in history) == known
        if mode == "after_activate":
            from test_ray_submission_factory import advance_native_clock

            advance_native_clock(c, monkeypatch)
    retry(c)
    assert not c.creations
    if known:
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert len(c.activations) == int(mode not in {"after_activate", "after_activation_fact"})
    else:
        assert not c.activations
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
            assert "ray.submitted" not in events(c, db)
    witness.update(
        child_returncode=child.returncode,
        known_create_recovered=known,
        native_creates=(native or {}).get("creates", 0),
        recovery_creates=len(c.creations),
        recovery_activations=len(c.activations),
    )
    marker.write_text(json.dumps(witness, indent=2) + "\n")
