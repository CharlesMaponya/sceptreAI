"""Governance snapshots allocate durable output versions under real concurrency."""

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Event
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import monitoring
from automl_api.db.session import get_db
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import ArtifactKind, RunKind, RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import event, select, text
from sqlalchemy.orm import Session

pytest_plugins = ["test_phase1_postgres_contracts"]


@pytest.fixture
def governance(phase1_engine, committed_tenant):
    engine = phase1_engine
    user_id, project_id = committed_tenant
    with Session(engine) as db, db.begin():
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="governance fixture")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            format="CSV",
            object_uri="embedded://fixture/source.csv",
            content_hash="a" * 64,
        )
        db.add(version)
        db.flush()
        source = ModelRun(
            project_id=project_id,
            dataset_version_id=version.id,
            created_by_id=user_id,
            run_kind=RunKind.TRAINING,
            task_type=TaskType.REGRESSION,
            status=RunStatus.SUCCEEDED,
            params={"candidate_models": ["RidgeCV"]},
            tags={"leaderboard": [{"model": "RidgeCV", "status": "succeeded", "metrics": {}}]},
        )
        db.add(source)
        db.flush()
        artifact = RunArtifact(
            project_id=project_id,
            model_run_id=source.id,
            kind=ArtifactKind.MODEL_OBJECT,
            name="model",
            object_uri="embedded://fixture/model.bin",
        )
        db.add(artifact)
        db.flush()
        registry = ModelRegistryEntry(
            project_id=project_id,
            model_run_id=source.id,
            model_artifact_id=artifact.id,
            model_name="RidgeCV",
            version=1,
            feature_space_hash="fixture",
        )
        db.add(registry)
        db.flush()
        deployment = ModelRun(
            project_id=project_id,
            dataset_version_id=version.id,
            created_by_id=user_id,
            run_kind=RunKind.DEPLOYMENT,
            task_type=TaskType.REGRESSION,
            status=RunStatus.SUCCEEDED,
            params={},
            tags={"registry_entry_id": str(registry.id)},
        )
        db.add(deployment)
        db.flush()
        c = SimpleNamespace(
            engine=engine,
            project_id=project_id,
            user_id=user_id,
            deployment_id=deployment.id,
            registry_id=registry.id,
            source_id=source.id,
            pids=Queue(),
            capture_pids=False,
        )
    app = FastAPI()
    app.include_router(monitoring.router, prefix="/api/v1")

    def database():
        with Session(engine, autoflush=False, expire_on_commit=False) as db:
            if c.capture_pids:
                c.pids.put(db.scalar(text("SELECT pg_backend_pid()")))
            yield db

    def user():
        with Session(engine) as db:
            return db.get(User, c.user_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = user
    path = (
        f"/api/v1/projects/{project_id}/operations/deployments/{c.deployment_id}/governance/reports"
    )

    def send(key="canonical", *, deployment_id=None):
        with TestClient(app, raise_server_exceptions=False) as client:
            return client.post(
                path
                if deployment_id is None
                else path.replace(str(c.deployment_id), str(deployment_id)),
                headers={"Idempotency-Key": key},
            )

    c.send = send
    return c


def reports(c):
    with Session(c.engine) as db:
        return list(
            db.scalars(
                select(RunArtifact).where(
                    RunArtifact.project_id == c.project_id,
                    RunArtifact.kind == ArtifactKind.GOVERNANCE_REPORT,
                )
            )
        )


def test_governance_sequential_generation_and_replay(governance):
    c = governance
    first = c.send()
    assert first.status_code == 201, first.text
    assert c.send().json() == first.json()
    second = c.send("second")
    assert second.status_code == 201, second.text
    assert (first.json()["version"], second.json()["version"]) == (1, 2)
    saved = reports(c)
    assert len(saved) == 2 and len({r.object_uri for r in saved}) == 2
    for report in saved:
        metadata = report.artifact_metadata
        assert metadata["desired_state"] == "object_write_pending"
        assert report.object_uri.startswith("pending://")
        assert hashlib.sha256(metadata["pending_json"].encode()).hexdigest() == report.content_hash
        assert (
            hashlib.sha256(metadata["pending_html"].encode()).hexdigest() == metadata["html_hash"]
        )
        assert json.loads(metadata["pending_json"])["model_identity"]["training_run_id"] == str(
            c.source_id
        )
    with Session(c.engine) as db:
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id)
                    )
                )
            )
            == 2
        )
        entries = list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))
        )
        assert len(entries) == 2 and {e.payload["report_id"] for e in entries} == {
            str(r.id) for r in saved
        }


@pytest.mark.parametrize("same_key", [False, True])
def test_governance_concurrent_generation(governance, same_key):
    c = governance
    entered, release = Event(), Event()
    c.capture_pids = True

    def hold_first(db, _):
        if not entered.is_set() and any(
            isinstance(row, RunArtifact) and row.kind == ArtifactKind.GOVERNANCE_REPORT
            for row in db.new
        ):
            entered.set()
            assert release.wait(15), "Timed out releasing the first report insert"

    event.listen(Session, "after_flush", hold_first)
    lock_observed = False
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            one = executor.submit(c.send, "first")
            assert entered.wait(15), "First request did not insert a report"
            c.pids.get(timeout=5)
            two = executor.submit(c.send, "first" if same_key else "second")
            try:
                pid = c.pids.get(timeout=5)
                deadline = monotonic() + 10
                with c.engine.connect() as observer:
                    while not two.done():
                        wait = observer.scalar(
                            text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                            {"pid": pid},
                        )
                        observer.rollback()
                        if wait == "Lock":
                            lock_observed = True
                            break
                        assert monotonic() < deadline
                        sleep(0.02)
            finally:
                release.set()
            a, b = one.result(timeout=20), two.result(timeout=20)
        assert a.status_code == b.status_code == 201, (a.text, b.text)
        if same_key:
            assert a.json() == b.json() and len(reports(c)) == 1
        else:
            assert (a.json()["version"], b.json()["version"]) == (1, 2)
            assert len({r.object_uri for r in reports(c)}) == 2
        assert lock_observed, "Competing report allocation did not wait for the first transaction"
    finally:
        release.set()
        event.remove(Session, "after_flush", hold_first)


@pytest.mark.parametrize("change", ["none", "detached", "source", "model_name"])
def test_governance_revalidates_binding_after_unlocked_audit_read(governance, monkeypatch, change):
    from automl_api.services import model_audit

    c = governance
    with Session(c.engine) as db, db.begin():
        deployment = db.get(ModelRun, c.deployment_id)
        explanation = ModelRun(
            project_id=c.project_id,
            dataset_version_id=deployment.dataset_version_id,
            created_by_id=c.user_id,
            run_kind=RunKind.EXPLAINABILITY,
            task_type=TaskType.REGRESSION,
            status=RunStatus.SUCCEEDED,
            params={"model_name": "RidgeCV"},
            tags={
                "source_training_run_id": str(c.source_id),
                "artifact_uri": "embedded://fixture/explanation.json",
            },
        )
        db.add(explanation)
    reads = []

    class Store:
        def read_bytes(self, uri):
            reads.append(uri)
            # A separate writer must complete while audit evidence is fetched.
            with Session(c.engine) as db, db.begin():
                db.execute(text("SET LOCAL lock_timeout = '1s'"))
                deployment = db.scalar(
                    select(ModelRun).where(ModelRun.id == c.deployment_id).with_for_update()
                )
                if change == "detached":
                    deployment.tags = {}
                elif change == "source":
                    db.get(ModelRegistryEntry, c.registry_id).model_run_id = c.deployment_id
                elif change == "model_name":
                    db.get(ModelRegistryEntry, c.registry_id).model_name = "Changed"
            return b"{}"

    monkeypatch.setattr(model_audit, "get_object_store", Store)
    response = c.send()
    assert len(reads) == 1
    assert response.status_code == (201 if change == "none" else 409), response.text
    if change != "none":
        assert response.json()["detail"]["code"] == "governance_binding_changed"
        assert reports(c) == []
        with Session(c.engine) as db:
            assert (
                list(
                    db.scalars(
                        select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id)
                    )
                )
                == []
            )
            assert (
                list(db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id)))
                == []
            )


@pytest.mark.parametrize("role", ["viewer", "editor", "admin"])
def test_governance_replay_rechecks_current_permission(governance, role):
    from datetime import UTC, datetime

    from automl_api.models.projects import ProjectMembership
    from sqlalchemy import delete

    c = governance
    original = c.send()
    assert original.status_code == 201
    with Session(c.engine) as db, db.begin():
        colleague = User(email=uuid.uuid4().hex + "@example.test", global_role="member")
        db.add(colleague)
        db.flush()
        colleague_id = colleague.id
        db.add(
            ProjectMembership(
                project_id=c.project_id,
                user_id=colleague.id,
                role=role,
                accepted_at=datetime.now(UTC),
            )
        )
    try:
        c.user_id = colleague_id
        replay = c.send()
        assert replay.status_code == (403 if role == "viewer" else 201), replay.text
        if role != "viewer":
            assert replay.json() == original.json()
        assert len(reports(c)) == 1
    finally:
        with Session(c.engine) as db, db.begin():
            db.execute(delete(User).where(User.id == colleague_id))


def test_governance_original_header_replays_after_binding_removed(governance):
    c = governance
    original = c.send()
    assert original.status_code == 201
    with Session(c.engine) as db, db.begin():
        db.get(ModelRun, c.deployment_id).tags = {}
    replay = c.send()
    assert replay.status_code == 201 and replay.json() == original.json()
    assert c.send("new-command").status_code == 409
    assert len(reports(c)) == 1


def test_governance_key_cannot_be_reused_for_another_deployment(governance):
    c = governance
    original = c.send()
    assert original.status_code == 201
    response = c.send(deployment_id=uuid.uuid4())
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "idempotency_key_reused"
    assert len(reports(c)) == 1


def test_governance_reconciler_preserves_distinct_report_bytes(governance, monkeypatch):
    from automl_api.services import reconciler

    c = governance
    assert c.send("first").status_code == c.send("second").status_code == 201
    saved = reports(c)
    expected = {
        r.id: (
            r.artifact_metadata["pending_json"].encode(),
            r.artifact_metadata["pending_html"].encode(),
        )
        for r in saved
    }
    stored = {}
    writes = []

    class Store:
        def put_bytes(self, key, content):
            writes.append(key)
            stored[key] = content
            return SimpleNamespace(uri="embedded://fixture/" + key)

    monkeypatch.setattr(reconciler, "get_object_store", Store)
    with Session(c.engine) as db, db.begin():
        entries = list(
            db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == c.project_id))
        )
        for entry in entries:
            result = reconciler.reconcile_governance_object(db, entry)
            assert result.artifact_metadata["desired_state"] == "object_written"
            assert "pending_json" not in result.artifact_metadata
            assert (
                stored[result.object_uri.removeprefix("embedded://fixture/")]
                == expected[result.id][0]
            )
            assert (
                stored[result.artifact_metadata["html_uri"].removeprefix("embedded://fixture/")]
                == expected[result.id][1]
            )
            reconciler.reconcile_governance_object(db, entry)
    assert len(writes) == len(set(writes)) == 4
    assert c.send("first").status_code == 201
    assert len(writes) == 4
