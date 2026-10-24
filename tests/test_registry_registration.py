"""HTTP registration verifies storage without SQL and revalidates current selection."""

from threading import local
from types import SimpleNamespace
from typing import Annotated

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes.operations import router
from automl_api.db.session import get_db
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.services import operations
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session
from test_registry_inspection import bound_run as bound_run
from test_registry_inspection import capacity_client as capacity_client
from test_registry_inspection import committed_tenant as committed_tenant
from test_registry_inspection import context_api as context_api
from test_registry_inspection import model_api as model_api
from test_registry_inspection import phase1_engine as phase1_engine
from test_registry_inspection import qualified_model as qualified_model
from test_registry_inspection import registry_case as registry_case
from test_registry_inspection import selected_scope as selected_scope


@pytest.fixture
def registration(registry_case, monkeypatch):
    c = registry_case
    with c.factory() as db, db.begin():
        db.execute(delete(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id))
        c.user_id = db.get(Project, c.project_id).owner_id
        run = db.get(ModelRun, c.run_id)
        run.tags = {**run.tags, "leaderboard_primary_metric": "rmse", "leaderboard": [c.entry]}
    activity = local()

    def checkout(*_args):
        activity.connections = getattr(activity, "connections", 0) + 1

    def checkin(*_args):
        activity.connections -= 1

    event.listen(c.engine, "checkout", checkout)
    event.listen(c.engine, "checkin", checkin)
    c.registration_reads = []

    native_open = c.store.open_stream

    def open_stream(uri, byte_range=None, *, version=None):
        count = getattr(activity, "connections", 0)
        c.registration_reads.append(count)
        assert count == 0, "HTTP registry storage read held caller SQL"
        assert uri == c.uri and version == "fixture-v1" and byte_range is None
        return native_open(uri, version=version)

    monkeypatch.setattr(
        operations, "get_object_store", lambda: SimpleNamespace(open_stream=open_stream)
    )
    app = FastAPI()
    app.include_router(router)

    def database():
        with Session(c.engine, expire_on_commit=False) as db:
            yield db

    def principal(db: Annotated[Session, Depends(get_db)]):
        return db.get(User, c.user_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = principal
    c.app = app
    c.path = f"/projects/{c.project_id}/operations/registry"
    c.payload = {"training_run_id": str(c.run_id), "model_name": c.model_name}

    def send(payload=None):
        with TestClient(app) as client:
            return client.post(c.path, json=c.payload if payload is None else payload)

    c.send = send
    try:
        yield c
    finally:
        from automl_api.models.workflows import DeletionTombstone

        with c.factory() as db, db.begin():
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )
        event.remove(c.engine, "checkout", checkout)
        event.remove(c.engine, "checkin", checkin)


def test_http_registry_size_read_releases_sql(registration):
    c = registration
    response = c.send()
    assert c.registration_reads == [0]
    assert response.status_code == 201, response.text
    with c.factory() as db:
        registry = db.scalars(
            select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
        ).one()
        assert db.get(RunArtifact, registry.model_artifact_id).byte_size == 10


def test_existing_registration_replays_without_storage(registration):
    c = registration
    first = c.send()
    assert first.status_code == 201, first.text
    c.state["unavailable"] = True
    assert c.send().json() == first.json()
    assert c.registration_reads == [0]


@pytest.mark.parametrize(
    "change",
    [
        "uri",
        "hash",
        "metric",
        "parameters",
        "schema",
        "data_hash",
        "candidate_status",
        "membership",
        "membership_expired",
        "inactive",
        "token",
        "project",
        "project_delete",
        "run_delete",
    ],
)
def test_registration_revalidates_after_storage_read(registration, change):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.datasets import DatasetVersion
    from automl_api.models.enums import ProjectRole, ProjectStatus
    from automl_api.models.projects import ProjectMembership
    from automl_api.models.workflows import DeletionTombstone
    from sqlalchemy import text

    c = registration

    def mutate():
        with c.factory() as db, db.begin():
            db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            run = db.get(ModelRun, c.run_id)
            if change in {"uri", "hash", "metric", "candidate_status"}:
                item = dict(run.tags["leaderboard"][0])
                item.update(
                    {
                        "uri": {"model_artifact_uri": "s3://fixture/changed"},
                        "hash": {"model_artifact_sha256": "c" * 64},
                        "metric": {"metrics": {"rmse": 0.9}},
                        "candidate_status": {"status": "failed"},
                    }[change]
                )
                run.tags = {**run.tags, "leaderboard": [item]}
            elif change == "parameters":
                run.params = {"changed": True}
            elif change in {"schema", "data_hash"}:
                version = db.get(DatasetVersion, run.dataset_version_id)
                if change == "schema":
                    version.schema_json = {"columns": [{"name": "changed"}]}
                else:
                    version.content_hash = "c" * 64
            elif change in {"membership", "membership_expired"}:
                member = db.scalars(
                    select(ProjectMembership).where(ProjectMembership.project_id == c.project_id)
                ).one()
                if change == "membership":
                    member.role = ProjectRole.VIEWER
                else:
                    member.expires_at = datetime.now(UTC) - timedelta(seconds=1)
            elif change == "inactive":
                db.get(User, c.user_id).is_active = False
            elif change == "token":
                db.get(User, c.user_id).token_version += 1
            elif change == "project":
                db.get(Project, c.project_id).status = ProjectStatus.ARCHIVED
            else:
                db.add(
                    DeletionTombstone(
                        project_id=c.project_id,
                        requested_by_id=c.user_id,
                        resource_type="project" if change == "project_delete" else "run",
                        resource_id=c.project_id if change == "project_delete" else c.run_id,
                        reason="owned registry registration test",
                    )
                )

    c.state["during_size"] = mutate
    response = c.send()
    assert response.status_code in {401, 403, 404, 409}, response.text
    assert c.registration_reads == [0]
    with c.factory() as db:
        assert (
            db.scalars(
                select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            ).all()
            == []
        )
        assert (
            len(db.scalars(select(RunArtifact).where(RunArtifact.project_id == c.project_id)).all())
            == 1
        )


@pytest.mark.parametrize("size", [-1, True, 1.5, 2**63])
def test_invalid_storage_size_cannot_create_registration(registration, size):
    c = registration
    c.state["size"] = size
    assert c.send().status_code == 409
    with c.factory() as db:
        assert (
            db.scalars(
                select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            ).all()
            == []
        )


def test_unavailable_artifact_has_no_registration_or_partial_artifact(registration):
    c = registration
    c.state["unavailable"] = True
    response = c.send()
    assert response.status_code == 409 and "owned unavailable" not in response.text
    assert c.registration_reads == [0]
    with c.factory() as db:
        assert (
            db.scalars(
                select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            ).all()
            == []
        )
        assert (
            len(db.scalars(select(RunArtifact).where(RunArtifact.project_id == c.project_id)).all())
            == 1
        )


@pytest.mark.parametrize("change", ["sibling", "parent_completion"])
def test_unrelated_training_progress_does_not_invalidate_selected_model(registration, change):
    from automl_api.models.enums import RunStatus

    c = registration

    def progress():
        with c.factory() as db, db.begin():
            run = db.get(ModelRun, c.run_id)
            if change == "sibling":
                run.tags = {
                    **run.tags,
                    "leaderboard": run.tags["leaderboard"]
                    + [{"model": "Forest", "status": "running"}],
                }
            else:
                run.status = RunStatus.FAILED

    c.state["during_size"] = progress
    response = c.send()
    assert response.status_code == 201, response.text
    assert c.registration_reads == [0]


def test_concurrent_registrations_share_one_entry_and_version(registration):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    c = registration
    barrier = Barrier(2)
    c.state["during_size"] = lambda: barrier.wait(10)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(c.send) for _ in range(2)]
        responses = [f.result(timeout=30) for f in futures]
    assert all(response.status_code == 201 for response in responses), [r.text for r in responses]
    assert responses[0].json() == responses[1].json()
    assert c.registration_reads == [0, 0]
    with c.factory() as db:
        rows = db.scalars(
            select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
        ).all()
        assert len(rows) == 1 and rows[0].version == 1
        assert (
            len(db.scalars(select(RunArtifact).where(RunArtifact.project_id == c.project_id)).all())
            == 2
        )


def test_registration_sql_loss_rolls_back_both_rows(registration):
    from sqlalchemy.exc import SQLAlchemyError

    c = registration
    lost = []

    def before_flush(db, *_args):
        if not lost and any(
            isinstance(row, ModelRegistryEntry) and row.project_id == c.project_id for row in db.new
        ):
            lost.append(True)
            db.connection().exec_driver_sql("SELECT pg_terminate_backend(pg_backend_pid())")

    event.listen(Session, "before_flush", before_flush)
    try:
        with pytest.raises(SQLAlchemyError):
            c.send()
    finally:
        event.remove(Session, "before_flush", before_flush)
    assert lost == [True]
    with c.factory() as db:
        assert (
            db.scalars(
                select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
            ).all()
            == []
        )
        assert (
            len(db.scalars(select(RunArtifact).where(RunArtifact.project_id == c.project_id)).all())
            == 1
        )
    assert c.send().status_code == 201
    assert c.registration_reads == [0, 0]


def test_lost_registration_commit_ack_replays_without_storage(registration):
    c = registration
    lost = []

    def before_flush(db, *_args):
        if any(
            isinstance(row, ModelRegistryEntry) and row.project_id == c.project_id for row in db.new
        ):
            db.info["owned_registration"] = True

    def after_commit(db):
        if db.info.get("owned_registration") and not lost:
            lost.append(True)
            raise OSError("owned registration commit reply loss")

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    try:
        with pytest.raises(OSError, match="owned registration commit reply loss"):
            c.send()
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
    assert lost == [True] and c.registration_reads == [0]
    assert c.send().status_code == 201
    assert c.registration_reads == [0]
    with c.factory() as db:
        assert (
            len(
                db.scalars(
                    select(ModelRegistryEntry).where(ModelRegistryEntry.project_id == c.project_id)
                ).all()
            )
            == 1
        )
        assert (
            len(db.scalars(select(RunArtifact).where(RunArtifact.project_id == c.project_id)).all())
            == 2
        )
