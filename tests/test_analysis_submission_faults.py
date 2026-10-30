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
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import analysis_submission as service
from automl_api.services import reconciler, workflow_state
from fault_worker_environment import environment as fault_environment
from sqlalchemy import text
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_submission_factory import factory_case as factory_case
from test_analysis_submission_factory import retry
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication


@pytest.mark.parametrize(
    "mode",
    [
        "before_sa",
        "after_sa",
        "before_create",
        "after_create",
        "after_create_fact",
        "before_activate",
        "after_activate",
        "after_activation_fact",
        "database_fact",
        "database_publication",
        "database_activation_fact",
        "database_ack",
    ],
)
def test_real_process_and_backend_loss_preserve_original_analysis_history(
    factory_case, tmp_path, monkeypatch, mode
):
    c = factory_case
    with c.factory() as db, db.begin():
        entry = db.get(OutboxEntry, c.entry_id)
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = "owned-analysis-process"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts += 1
    marker, log_path = tmp_path / "checkpoint.json", tmp_path / "child.log"
    with log_path.open("w") as log:
        child = subprocess.Popen(
            [
                sys.executable,
                str(
                    Path(__file__).resolve().parent
                    / "support"
                    / "analysis_submission_fault_worker.py"
                ),
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
                            application=f"phase1_analysis_submit_fault_{child.pid}",
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
        "before_activate",
        "after_activate",
        "after_activation_fact",
        "database_publication",
        "database_activation_fact",
        "database_ack",
    }
    with c.factory() as db:
        attempt = db.get(WorkflowAttempt, c.attempt_id)
        assert (service.journal._event(db, attempt, service.CREATE_RESULT) is not None) == known
    if mode in {"before_activate", "after_activate", "database_activation_fact"}:

        class ExpiredNativeClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(minutes=5)

        for module in (service, workflow_state, reconciler):
            monkeypatch.setattr(module, "datetime", ExpiredNativeClock)
    retry(c)
    assert not c.creations
    if known:
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status == OutboxStatus.DELIVERED
        assert len(c.activations) == int(
            mode in {"after_create_fact", "before_activate", "database_publication"}
        )
    else:
        assert not c.activations
        with c.factory() as db:
            assert db.get(OutboxEntry, c.entry_id).status != OutboxStatus.DELIVERED
            assert (
                service.journal._event(
                    db, db.get(WorkflowAttempt, c.attempt_id), "analysis.submitted"
                )
                is None
            )
    witness.update(
        child_returncode=child.returncode,
        known_create_recovered=known,
        native_creates=(native or {}).get("creates", 0),
        native_activations=(native or {}).get("activations", 0),
        recovery_creates=len(c.creations),
        recovery_activations=len(c.activations),
    )
    marker.write_text(json.dumps(witness, indent=2) + "\n")
