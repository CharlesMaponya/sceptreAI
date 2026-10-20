"""Committed PostgreSQL admission phases around remote Kubernetes verification."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import jwt
import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import AttemptStatus, DatasetStatus, ProjectStatus
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import DeletionTombstone, WorkflowAttempt, WorkflowEvent
from automl_api.services import attempt_identity
from automl_api.services.dataset_digests import DatasetDigestError
from automl_api.services.workflow_state import StaleFence
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from test_attempt_identity import admit, head

pytest_plugins = ["test_attempt_identity"]


def records(engine, attempt_id):
    with Session(engine) as db:
        return list(db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt_id)))


def install_remote(identity, callback):
    _, _, k8s, review, pod, auth = identity
    cluster, job = head(identity)
    auth.create_token_review.side_effect = lambda *_a, **_k: (callback("review"), review)[1]
    k8s.core.read_namespaced_pod.side_effect = lambda *_a, **_k: (callback("pod"), pod)[1]

    def controller(*args, **_kwargs):
        callback(args[3])
        return cluster if args[3] == "rayclusters" else job

    k8s.custom.get_namespaced_custom_object.side_effect = controller


def test_remote_calls_release_pool_and_all_authority_locks(identity):
    scope, *_ = identity
    engine, run_id, attempt_id, *_ = scope
    observed = []

    def remote(stage):
        assert engine.pool.checkedout() == 0
        with Session(engine) as db, db.begin():
            attempt = db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == attempt_id)
                .with_for_update(nowait=True)
            )
            run = db.scalar(
                select(ModelRun).where(ModelRun.id == run_id).with_for_update(nowait=True)
            )
            assert db.scalar(
                select(Project).where(Project.id == attempt.project_id).with_for_update(nowait=True)
            )
            assert db.scalar(
                select(DatasetVersion)
                .where(DatasetVersion.id == run.dataset_version_id)
                .with_for_update(nowait=True)
            )
        assert not records(engine, attempt_id)
        observed.append(stage)

    install_remote(identity, remote)
    assert admit(identity)["attempt_id"] == str(attempt_id)
    assert observed == ["review", "pod", "rayclusters", "rayjobs"]
    assert len(records(engine, attempt_id)) == 1
    assert engine.pool.checkedout() == 0


@pytest.mark.parametrize("at", ["review", "pod", "rayjobs"])
@pytest.mark.parametrize(
    "change",
    [
        "stop",
        "expire",
        "fence",
        "identity",
        "namespace",
        "ray_job",
        "source_uri",
        "source_digest",
        "source_size",
        "source_archived",
        "project_archived",
        "delete_project",
        "delete_run",
    ],
)
def test_remote_state_change_cannot_publish_admission(identity, at, change):
    scope, *_ = identity
    engine, run_id, attempt_id, *_ = scope
    mutated = []

    def remote(stage):
        assert engine.pool.checkedout() == 0
        if stage != at:
            return
        with Session(engine) as db, db.begin():
            attempt = db.get(WorkflowAttempt, attempt_id, with_for_update=True)
            run = db.get(ModelRun, run_id, with_for_update=True)
            version = db.get(DatasetVersion, run.dataset_version_id)
            if change == "stop":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "expire":
                attempt.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "fence":
                attempt.fencing_token = uuid.uuid4().hex
            elif change == "identity":
                attempt.workload_identity = "different-worker"
            elif change == "namespace":
                run.k8s_namespace = "different-namespace"
            elif change == "ray_job":
                attempt.ray_job_name = "different-job"
            elif change == "source_uri":
                version.object_uri = "s3://another/source.csv"
            elif change == "source_digest":
                version.content_hash = "f" * 64
            elif change == "source_size":
                version.byte_size += 1
            elif change == "source_archived":
                version.status = DatasetStatus.ARCHIVED
            elif change == "project_archived":
                db.get(Project, run.project_id).status = ProjectStatus.ARCHIVED
            else:
                kind = "project" if change == "delete_project" else "run"
                db.add(
                    DeletionTombstone(
                        project_id=run.project_id,
                        requested_by_id=run.created_by_id,
                        resource_type=kind,
                        resource_id=run.project_id if kind == "project" else run.id,
                        reason="Committed admission race fixture",
                    )
                )
            mutated.append(change)

    install_remote(identity, remote)
    with pytest.raises((StaleFence, DatasetDigestError)):
        admit(identity)
    assert mutated == [change]
    assert not records(engine, attempt_id)
    assert engine.pool.checkedout() == 0
    # The admission failure must not undo or overwrite the remote actor's committed state.
    if change.startswith("delete_"):
        with Session(engine) as db, db.begin():
            for row in db.scalars(
                select(DeletionTombstone).where(
                    DeletionTombstone.project_id == db.get(ModelRun, run_id).project_id
                )
            ):
                db.delete(row)


@pytest.mark.parametrize("rotated", [False, True])
def test_concurrent_identity_replays_serialize_only_evidence(identity, rotated):
    scope, token, k8s, review, _, auth = identity
    engine, _, attempt_id, *_ = scope
    barrier = Barrier(2)

    def review_call(*_args, **_kwargs):
        barrier.wait(timeout=10)
        return review

    auth.create_token_review.side_effect = review_call
    tokens = [
        token,
        jwt.encode(
            {**jwt.decode(token, options={"verify_signature": False}), "jti": uuid.uuid4().hex},
            "unit-test-rotation-key-at-least-32-bytes",
            algorithm="HS256",
        )
        if rotated
        else token,
    ]
    factory = sessionmaker(bind=engine)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda value: attempt_identity.admit_attempt(factory, attempt_id, value, k8s),
                tokens,
            )
        )
    assert all(row["attempt_id"] == str(attempt_id) for row in results)
    events = records(engine, attempt_id)
    assert len(events) == (2 if rotated else 1)
    assert len({event.sequence for event in events}) == len(events)
    assert all(value not in str(event.payload) for event in events for value in tokens)
    assert engine.pool.checkedout() == 0


def test_final_sql_wait_cannot_reuse_an_expired_attempt(identity, monkeypatch):
    from sqlalchemy import event

    scope, *_ = identity
    engine, _, attempt_id, *_ = scope
    moment = [datetime.now(UTC)]
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = moment[0] + timedelta(seconds=60)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment[0]

    waited = []

    def after_lock(_connection, _cursor, statement, _parameters, _context, _many):
        if "FROM dataset_versions" in statement and "FOR SHARE" in statement:
            moment[0] += timedelta(seconds=61)
            waited.append(True)

    monkeypatch.setattr(attempt_identity, "datetime", Clock)
    event.listen(engine, "after_cursor_execute", after_lock)
    try:
        with pytest.raises(StaleFence, match="expired while validating"):
            admit(identity)
    finally:
        event.remove(engine, "after_cursor_execute", after_lock)
    assert waited == [True]
    assert not records(engine, attempt_id)


@pytest.mark.parametrize("phase", ["snapshot", "evidence"])
def test_committed_admission_acknowledgement_loss_replays_one_evidence_row(identity, phase):
    from contextlib import contextmanager

    scope, token, k8s, *_ = identity
    engine, _, attempt_id, *_ = scope
    real_factory = sessionmaker(bind=engine)
    commits = []

    @contextmanager
    def factory():
        with real_factory() as db:
            original = db.begin

            @contextmanager
            def transaction():
                with original():
                    yield
                commits.append(True)
                if len(commits) == (1 if phase == "snapshot" else 2):
                    raise OSError("Lost admission commit acknowledgement")

            db.begin = transaction
            yield db

    with pytest.raises(OSError, match="commit acknowledgement"):
        attempt_identity.admit_attempt(factory, attempt_id, token, k8s)
    assert len(records(engine, attempt_id)) == (0 if phase == "snapshot" else 1)
    assert admit(identity)["attempt_id"] == str(attempt_id)
    assert len(records(engine, attempt_id)) == 1
    assert engine.pool.checkedout() == 0
