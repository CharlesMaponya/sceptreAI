"""Held analysis evidence must not starve another original submitted Job."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services import reconciler
from automl_api.services.workflow_state import canonical_request_hash
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_recovery import create_delivery
from test_analysis_recovery import recovery as recovery
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication

CLOCKS = ("updated_at", "heartbeat_at", "lease_expires_at", "lease_owner", "terminal_cas_version")


@pytest.fixture
def pair(recovery):
    engine, run_id, bad_id, _, k8s, _, _, _ = recovery
    with Session(engine) as db, db.begin():
        original = db.get(ModelRun, run_id)
        bad = db.get(WorkflowAttempt, bad_id)
        good_run = ModelRun(
            project_id=original.project_id,
            created_by_id=original.created_by_id,
            dataset_version_id=original.dataset_version_id,
            run_kind=original.run_kind,
            task_type=original.task_type,
            status=RunStatus.QUEUED,
            k8s_namespace=original.k8s_namespace,
            tags={},
        )
        db.add(good_run)
        db.flush()
        good = WorkflowAttempt(
            project_id=bad.project_id,
            model_run_id=good_run.id,
            dataset_version_id=bad.dataset_version_id,
            stage=bad.stage,
            logical_key=f"analysis-run:{good_run.id}",
            workload_identity=bad.workload_identity,
            generation=1,
            fencing_token=uuid.uuid4().hex,
        )
        db.add(good)
        db.flush()
        good_run.tags = {"workflow_attempt_id": str(good.id)}
        good_run_id, good_id = good_run.id, good.id
    create_delivery(engine, good_run_id, good_id, k8s)
    with Session(engine) as db, db.begin():
        for identity, year in ((bad_id, 2000), (good_id, 2001)):
            row = db.get(WorkflowAttempt, identity)
            row.updated_at = datetime(year, 1, 1, tzinfo=UTC)
            row.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
    k8s.reset_mock()
    return engine, bad_id, good_id, k8s


@pytest.mark.parametrize("corruption", ["digest", "namespace", "outbox_uuid", "payload_list"])
@pytest.mark.parametrize("limit", [1, 2])
def test_invalid_evidence_rotates_to_valid_original_job(pair, corruption, limit):
    engine, bad_id, good_id, k8s = pair
    with Session(engine) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == bad_id,
                WorkflowEvent.event_key == "analysis.submitted",
            )
        )
        if corruption == "digest":
            event.result_digest = "0" * 64
        elif corruption == "payload_list":
            event.payload = []
            event.result_digest = canonical_request_hash(event.payload)
        else:
            event.payload = {
                **event.payload,
                **(
                    {"namespace": ""}
                    if corruption == "namespace"
                    else {"outbox_id": "invalid-uuid"}
                ),
            }
            event.result_digest = canonical_request_hash(event.payload)
        before = {
            identity: tuple(getattr(db.get(WorkflowAttempt, identity), field) for field in CLOCKS)
            for identity in (bad_id, good_id)
        }
    requests = []
    for _ in range(2 if limit == 1 else 1):
        with Session(engine) as db, db.begin():
            assert (
                reconciler.observe_analysis_attempts(db, k8s, read=requests.append, limit=limit)
                == limit
            )
    assert len(requests) == 1 and requests[0].attempt_id == good_id
    with Session(engine) as db:
        scanned = {
            identity: db.get(WorkflowAttempt, identity).last_observed_at
            for identity in (bad_id, good_id)
        }
    counts = []
    original_read = k8s.batch.read_namespaced_job.side_effect

    def read(**kwargs):
        counts.append(engine.pool.checkedout())
        return original_read(**kwargs)

    k8s.batch.read_namespaced_job.side_effect = read
    result = reconciler.inspect_analysis_observation(k8s, requests[0])
    with Session(engine) as db, db.begin():
        assert (
            reconciler.observe_analysis_attempts(
                db, k8s, attempt_id=good_id, read=lambda _request: result
            )
            == 1
        )
    assert counts == [0]
    with Session(engine) as db:
        for identity in (bad_id, good_id):
            row = db.get(WorkflowAttempt, identity)
            assert row.last_observed_at is not None and row.last_observed_at == scanned[identity]
            assert tuple(getattr(row, field) for field in CLOCKS) == before[identity]
            assert row.status in {AttemptStatus.SUBMITTED, AttemptStatus.RUNNING}
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.predecessor_attempt_id.in_([bad_id, good_id])
                )
            )
        )
    k8s.delete_job.assert_not_called()
    k8s.create_job.assert_not_called()


def test_analysis_scan_clock_rolls_back_without_worker_clock_changes(pair):
    engine, bad_id, good_id, k8s = pair
    with Session(engine) as db:
        before = {
            identity: tuple(getattr(db.get(WorkflowAttempt, identity), field) for field in CLOCKS)
            for identity in (bad_id, good_id)
        }
    with pytest.raises(RuntimeError, match="owned rollback"):
        with Session(engine) as db, db.begin():
            assert reconciler.observe_analysis_attempts(db, k8s, read=lambda _request: None) == 2
            assert db.get(WorkflowAttempt, bad_id).last_observed_at is not None
            raise RuntimeError("owned rollback")
    with Session(engine) as db:
        for identity in (bad_id, good_id):
            row = db.get(WorkflowAttempt, identity)
            assert row.last_observed_at is None
            assert tuple(getattr(row, field) for field in CLOCKS) == before[identity]


def test_analysis_scanner_skips_locked_attempt_and_later_rotates(pair):
    engine, bad_id, good_id, k8s = pair

    def scan():
        requests = []
        with Session(engine) as db, db.begin():
            assert reconciler.observe_analysis_attempts(db, k8s, read=requests.append, limit=1) == 1
        return requests

    with Session(engine) as db, db.begin():
        db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.id == bad_id).with_for_update())
        with ThreadPoolExecutor(max_workers=1) as pool:
            requests = pool.submit(scan).result(timeout=5)
        assert len(requests) == 1 and requests[0].attempt_id == good_id
    requests = scan()
    assert len(requests) == 1 and requests[0].attempt_id == bad_id
