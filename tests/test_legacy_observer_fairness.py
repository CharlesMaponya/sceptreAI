"""Legacy native failures and incomplete identities cannot starve later runs."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime
from functools import partial

import pytest
from automl_api.models.enums import RunStatus
from automl_api.models.runs import ModelRun
from automl_api.services import legacy_analysis, reconciler
from kubernetes.client import ApiException
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_legacy_analysis_custody import legacy_scope as legacy_scope
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication

CLOCKS = ("updated_at", "queued_at", "started_at", "finished_at")


@pytest.fixture
def pair(legacy_scope):
    engine, bad_id, _, k8s, backend, _ = legacy_scope
    with Session(engine) as db, db.begin():
        original = db.get(ModelRun, bad_id)
        good = ModelRun(
            project_id=original.project_id,
            created_by_id=original.created_by_id,
            dataset_version_id=original.dataset_version_id,
            run_kind=original.run_kind,
            status=RunStatus.QUEUED,
            k8s_namespace=original.k8s_namespace,
            k8s_job_name="job-" + uuid.uuid4().hex,
            tags={"desired_state": "kubernetes_submitted"},
        )
        db.add(good)
        db.flush()
        original.updated_at = datetime(2000, 1, 1, tzinfo=UTC)
        good.updated_at = datetime(2001, 1, 1, tzinfo=UTC)
        good_id, name, namespace = good.id, good.k8s_job_name, good.k8s_namespace
    job = deepcopy(next(iter(backend.jobs.values())))
    job.metadata.name, job.metadata.uid = name, uuid.uuid4().hex
    job.metadata.labels["automl.platform/run-id"] = str(good_id)
    backend.jobs[(namespace, name)] = job
    pod = deepcopy(backend.pods[0])
    pod.metadata.name, pod.metadata.uid = name + "-pod", uuid.uuid4().hex
    pod.metadata.labels = {**job.metadata.labels, "job-name": name}
    pod.metadata.owner_references[0].name = name
    pod.metadata.owner_references[0].uid = job.metadata.uid
    backend.pods.append(pod)
    return engine, bad_id, good_id, k8s, backend


@pytest.mark.parametrize("field", ["k8s_namespace", "k8s_job_name"])
@pytest.mark.parametrize("value", [None, ""])
@pytest.mark.parametrize("limit", [1, 2])
def test_incomplete_legacy_scope_rotates_without_changing_domain_clocks(pair, field, value, limit):
    engine, bad_id, good_id, k8s, backend = pair
    with Session(engine) as db, db.begin():
        setattr(db.get(ModelRun, bad_id), field, value)
    with Session(engine) as db:
        before = {
            identity: tuple(getattr(db.get(ModelRun, identity), key) for key in CLOCKS)
            for identity in (bad_id, good_id)
        }
    requests = []
    for _ in range(2 if limit == 1 else 1):
        with Session(engine) as db, db.begin():
            assert legacy_analysis.observe_jobs(db, read=requests.append, limit=limit) == limit
    assert len(requests) == 1 and requests[0].run_id == good_id
    with Session(engine) as db:
        scans = {
            identity: getattr(db.get(ModelRun, identity), "last_observed_at", None)
            for identity in (bad_id, good_id)
        }
        assert all(scans.values())
        for identity in (bad_id, good_id):
            assert (
                tuple(getattr(db.get(ModelRun, identity), key) for key in CLOCKS)
                == before[identity]
            )
    counts = []
    backend.on_read = lambda: counts.append(engine.pool.checkedout())
    result = legacy_analysis.inspect_legacy_observation(k8s, requests[0])
    assert counts == [0]
    with Session(engine) as db, db.begin():
        assert legacy_analysis.observe_jobs(db, run_id=good_id, read=lambda _request: result) == 1
    with Session(engine) as db:
        assert db.get(ModelRun, bad_id).status == RunStatus.QUEUED
        assert db.get(ModelRun, good_id).status == RunStatus.RUNNING
        assert db.get(ModelRun, good_id).last_observed_at == scans[good_id]
    assert backend.deletes == []


@pytest.mark.parametrize("failure", ["transient", "foreign_identity"])
def test_failed_native_legacy_read_rotates_through_actual_dispatch(pair, monkeypatch, failure):
    engine, bad_id, good_id, k8s, backend = pair
    with Session(engine) as db:
        bad_name = db.get(ModelRun, bad_id).k8s_job_name
    original = backend.read

    def read(name, namespace, **kwargs):
        assert engine.pool.checkedout() == 0
        if name == bad_name and failure == "transient":
            raise ApiException(status=503)
        job = original(name, namespace, **kwargs)
        if name == bad_name:
            job.metadata.labels["automl.platform/project-id"] = str(uuid.uuid4())
        return job

    k8s.batch.read_namespaced_job = read
    k8s.batch.read_namespaced_job_status = read
    monkeypatch.setattr(
        reconciler, "observe_analysis_jobs", partial(reconciler.observe_analysis_jobs, limit=1)
    )
    for _ in range(2):
        assert legacy_analysis.observe_legacy_once(lambda: Session(engine), k8s) == 1
    with Session(engine) as db:
        assert db.get(ModelRun, bad_id).status == RunStatus.QUEUED
        assert db.get(ModelRun, good_id).status == RunStatus.RUNNING
    assert backend.deletes == []


def test_legacy_scan_clock_rolls_back_without_changing_domain_clocks(pair):
    engine, bad_id, good_id, _, _ = pair
    with Session(engine) as db:
        before = {
            identity: tuple(getattr(db.get(ModelRun, identity), key) for key in CLOCKS)
            for identity in (bad_id, good_id)
        }
    with pytest.raises(RuntimeError, match="owned rollback"):
        with Session(engine) as db, db.begin():
            assert legacy_analysis.observe_jobs(db, read=lambda _request: None) == 2
            assert getattr(db.get(ModelRun, bad_id), "last_observed_at", None) is not None
            raise RuntimeError("owned rollback")
    with Session(engine) as db:
        for identity in (bad_id, good_id):
            row = db.get(ModelRun, identity)
            assert row.last_observed_at is None
            assert tuple(getattr(row, key) for key in CLOCKS) == before[identity]


def test_legacy_scanners_skip_locked_run_then_rotate(pair):
    engine, bad_id, good_id, _, _ = pair

    def scan():
        requests = []
        with Session(engine) as db, db.begin():
            assert legacy_analysis.observe_jobs(db, read=requests.append, limit=1) == 1
        return requests

    with Session(engine) as db, db.begin():
        db.scalar(select(ModelRun).where(ModelRun.id == bad_id).with_for_update())
        with ThreadPoolExecutor(max_workers=1) as pool:
            requests = pool.submit(scan).result(timeout=5)
        assert len(requests) == 1 and requests[0].run_id == good_id
    requests = scan()
    assert len(requests) == 1 and requests[0].run_id == bad_id
