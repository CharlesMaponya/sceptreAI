"""Exercise upload allocation through HTTP, real PostgreSQL locks and filesystem writes."""

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import datasets as routes
from automl_api.db.session import get_db
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import WorkflowCommand
from automl_api.services import datasets, uploads
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import delete, select, text
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.mark.parametrize(
    "case",
    [
        "new",
        "existing",
        "same-content",
        "same-key",
        "changed-key-body",
        "resumable",
        "independent",
        "rollback",
    ],
)
def test_buffered_allocation_concurrency(
    phase1_engine, committed_tenant, monkeypatch, tmp_path, case
):
    actor_id, project_id = committed_tenant
    other_project_id = None
    if case in {"existing", "independent"}:
        with Session(phase1_engine) as db, db.begin():
            if case == "existing":
                db.add(Dataset(project_id=project_id, created_by_id=actor_id, name="concurrent"))
            else:
                other = Project(owner_id=actor_id, created_by_id=actor_id, name="independent")
                db.add(other)
                db.flush()
                other_project_id = other.id
                # Project ownership does not replace an accepted membership.
                from datetime import UTC, datetime

                from automl_api.models.enums import ProjectRole
                from automl_api.models.projects import ProjectMembership

                db.add(
                    ProjectMembership(
                        project_id=other.id,
                        user_id=actor_id,
                        role=ProjectRole.OWNER,
                        accepted_at=datetime.now(UTC),
                    )
                )

    store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="owned")
    first_write, release, second_write = threading.Event(), threading.Event(), threading.Event()
    second_database = threading.Event()
    mutex = threading.Lock()
    writes = []
    pids = {}
    native_put = store.put_bytes

    def put(key, content):
        with mutex:
            writes.append((key, content))
            position = len(writes)
        if position == 1:
            first_write.set()
            assert release.wait(12), "Test must release the first native call"
            if case == "rollback":
                raise RuntimeError("Injected failure before native write")
        else:
            second_write.set()
        return native_put(key, content)

    monkeypatch.setattr(store, "put_bytes", put)
    monkeypatch.setattr(datasets, "get_object_store", lambda: store)
    app = FastAPI()
    app.include_router(routes.router)

    def database(request: Request):
        with Session(phase1_engine) as db:
            db.execute(text("SET LOCAL statement_timeout = '10s'"))
            slot = request.headers["X-Test-Slot"]
            pids[slot] = db.scalar(text("SELECT pg_backend_pid()"))
            if slot == "second":
                second_database.set()
            yield db

    def actor():
        with Session(phase1_engine) as db:
            return db.get(User, actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = actor
    key = "upload-" + uuid.uuid4().hex
    content = b"feature,target\n1,2\n3,4\n"
    other_content = (
        content if case in {"same-content", "same-key"} else b"feature,target\n5,6\n7,8\n"
    )

    def post(slot):
        target = other_project_id if slot == "second" and other_project_id else project_id
        request_key = (
            key if slot == "first" or case in {"same-key", "changed-key-body"} else key + "-second"
        )
        with TestClient(app, raise_server_exceptions=False) as client:
            return client.post(
                f"/projects/{target}/datasets/upload",
                headers={"Idempotency-Key": request_key, "X-Test-Slot": slot},
                data={"dataset_name": "concurrent"},
                files={
                    "file": ("data.csv", content if slot == "first" else other_content, "text/csv")
                },
            )

    def register_resumable():
        # This is the shared SQL publication function used by the resumable factory.
        with Session(phase1_engine) as db, db.begin():
            db.execute(text("SET LOCAL statement_timeout = '10s'"))
            pids["second"] = db.scalar(text("SELECT pg_backend_pid()"))
            second_database.set()
            dataset, version = uploads._register_dataset_version(
                db,
                SimpleNamespace(
                    project_id=project_id,
                    created_by_id=actor_id,
                    dataset_name="concurrent",
                    description=None,
                    tags={},
                    original_filename="data.csv",
                    provider_driver="embedded",
                    completed_object_uri=store.uri_for_key("owned-resumable.csv"),
                    expected_object_digest="a" * 64,
                    byte_size=20,
                    data_region="local",
                    sensitivity="internal",
                    retention_until=None,
                    legal_hold=False,
                ),
            )
            return dataset.id, version.version_number

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(post, "first")
            second = None
            try:
                assert first_write.wait(8), "First request did not reach storage"
                second = pool.submit(
                    register_resumable if case == "resumable" else post,
                    *(() if case == "resumable" else ("second",)),
                )
                assert second_database.wait(5), "Second request did not reach PostgreSQL"
                blocked = False
                deadline = time.monotonic() + 5
                with Session(phase1_engine) as observer:
                    while time.monotonic() < deadline:
                        blockers = observer.scalar(
                            text("SELECT pg_blocking_pids(:pid)"), {"pid": pids["second"]}
                        )
                        if pids["first"] in blockers:
                            blocked = True
                            break
                        if second_write.is_set() or second.done():
                            break
                        time.sleep(0.01)
                early_write = second_write.is_set()
            finally:
                release.set()
            first_response = first.result(timeout=12)
            second_response = second.result(timeout=12) if second else None

        if case == "independent":
            assert not blocked and early_write, "Different projects must proceed independently"
        else:
            assert blocked and not early_write, (
                "Second upload reached storage before allocation was serialized"
            )
        assert first_response.status_code == (500 if case == "rollback" else 201), (
            first_response.text
        )
        if case == "resumable":
            assert second_response[1] == 2
        else:
            assert second_response.status_code == (409 if case == "changed-key-body" else 201), (
                second_response.text
            )
        if case == "same-key":
            assert first_response.json() == second_response.json()
        if case in {"same-key", "changed-key-body"}:
            assert len(writes) == 1

        with Session(phase1_engine) as db:
            rows = list(db.scalars(select(Dataset).where(Dataset.project_id == project_id)))
            assert len(rows) == 1
            versions = list(
                db.scalars(
                    select(DatasetVersion)
                    .where(DatasetVersion.dataset_id == rows[0].id)
                    .order_by(DatasetVersion.version_number)
                )
            )
            expected = (
                1 if case in {"same-key", "changed-key-body", "independent", "rollback"} else 2
            )
            assert [v.version_number for v in versions] == list(range(1, expected + 1))
            assert rows[0].latest_version_number == expected
            if case not in {"resumable", "same-key", "changed-key-body", "independent", "rollback"}:
                assert store.read_bytes(versions[0].object_uri) == content
                assert store.read_bytes(versions[1].object_uri) == other_content
                assert versions[0].object_uri != versions[1].object_uri
            commands = list(
                db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == project_id))
            )
            assert len(commands) == (2 if case in {"new", "existing", "same-content"} else 1)
    finally:
        release.set()
        if other_project_id:
            with Session(phase1_engine) as db, db.begin():
                db.execute(delete(Project).where(Project.id == other_project_id))
