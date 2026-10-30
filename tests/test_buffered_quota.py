"""Committed private and resumable reservations share one project budget."""

import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from automl_api.models.iam import User
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import DatasetUploadRequest
from automl_api.services import buffered_uploads, uploads
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.orm import sessionmaker
from test_buffered_execution import pending as pending
from test_buffered_input_admission import admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase2_upload_services import _payload
from test_upload_admission_workflow import admission_case as admission_case


def reserve(c, size, *, version_id=None):
    identifier = uuid.uuid4()
    key = f"projects/{c.project_id}/raw/{identifier}/input.csv"
    with c.factory() as db, db.begin():
        upload = uploads._admit_upload_session(
            db,
            db.get(User, c.actor_id),
            c.project_id,
            _payload(
                byte_size=size,
                retention_days=None,
                data_region=c.settings.object_store_region or "eu-west-1",
            ),
            origin=c.settings.upload_allowed_origins[0],
            settings=c.settings,
            session_id=identifier,
            driver_name="s3_compatible",
            protocol="multipart",
            object_uri=f"s3c://{c.settings.object_store_bucket}/{key}",
            object_key=key,
        )
        upload.dataset_version_id = version_id
        db.flush()
    return identifier


def prepare(c, phase):
    if phase == "published":
        c.run()
    elif phase in {"issued", "recorded"}:
        if phase == "issued":
            c.native.create_error = OSError("Unknown original write")
        else:
            c.native.read_error = OSError("Known original write not published")
        with pytest.raises(OSError):
            c.run()
    elif phase == "retired":
        with c.factory() as db, db.begin():
            row = db.get(BufferedUploadInput, c.identifier)
            row.phase, row.content = "retired", None


@pytest.mark.parametrize("phase", ["admitted", "issued", "recorded", "published", "retired"])
@pytest.mark.parametrize("family", ["buffered", "resumable"])
def test_private_input_retains_quota_against_both_admission_families(pending, phase, family):
    c = pending
    prepare(c, phase)
    size = len(c.body)
    c.settings = replace(c.settings, project_storage_quota_bytes=2 * size - 1)
    with pytest.raises(ValueError, match="quota"):
        if family == "buffered":
            c.admit(key=c.key + "-blocked", settings=c.settings)
        else:
            reserve(c, size)
    with c.factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(BufferedUploadInput)
                .where(BufferedUploadInput.project_id == c.project_id)
            )
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(DatasetUploadSession)
                .where(DatasetUploadSession.project_id == c.project_id)
            )
            == 0
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == c.project_id)
            )
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(OutboxEntry.project_id == c.project_id)
            )
            == 1
        )
    assert c.admit(settings=c.settings) == c.identifier


@pytest.mark.parametrize("family", ["buffered", "resumable"])
def test_published_private_data_uses_exactly_one_slot(pending, family):
    c = pending
    c.run()
    c.settings = replace(c.settings, project_storage_quota_bytes=2 * len(c.body))
    if family == "buffered":
        c.admit(key=c.key + "-second", settings=c.settings)
    else:
        reserve(c, len(c.body))
    with pytest.raises(ValueError, match="quota"):
        c.admit(key=c.key + "-third", settings=c.settings)


@pytest.mark.parametrize(
    "mutation", ["missing", "smaller", "unknown-size", "foreign-uri", "different-hash"]
)
def test_unqualified_publication_cannot_reduce_private_reservation(pending, mutation):
    c = pending
    c.run()
    with c.factory() as db, db.begin():
        version = db.scalar(select(DatasetVersion).where(DatasetVersion.project_id == c.project_id))
        if mutation == "missing":
            db.delete(version)
        elif mutation == "smaller":
            version.byte_size = 1
        elif mutation == "unknown-size":
            version.byte_size = None
        elif mutation == "foreign-uri":
            version.object_uri += "foreign"
        else:
            version.content_hash = "b" * 64
    c.settings = replace(c.settings, project_storage_quota_bytes=2 * len(c.body) - 1)
    with pytest.raises(ValueError, match="quota"):
        c.admit(key=c.key + "-blocked", settings=c.settings)


def test_active_resumable_reservation_blocks_buffered_admission(admission):
    c = admission
    c.settings = buffered_uploads.get_settings()
    size = len(c.body)
    c.settings = replace(c.settings, project_storage_quota_bytes=2 * size - 1)
    reserve(c, size)
    with pytest.raises(ValueError, match="quota"):
        c.admit(settings=c.settings)
    with c.factory() as db:
        assert (
            db.scalar(
                select(BufferedUploadInput.command_id).where(
                    BufferedUploadInput.project_id == c.project_id
                )
            )
            is None
        )
        assert (
            db.scalar(select(WorkflowCommand.id).where(WorkflowCommand.project_id == c.project_id))
            is None
        )


@pytest.mark.parametrize("family", ["buffered", "resumable"])
def test_resumable_metadata_and_active_reservation_are_not_charged_twice(pending, family):
    c = pending
    c.run()
    with c.factory() as db:
        version_id = db.scalar(
            select(DatasetVersion.id).where(DatasetVersion.project_id == c.project_id)
        )
    c.settings = replace(c.settings, project_storage_quota_bytes=3 * len(c.body))
    reserve(c, len(c.body), version_id=version_id)
    c.settings = replace(c.settings, project_storage_quota_bytes=2 * len(c.body))
    if family == "buffered":
        c.admit(key=c.key + "-second", settings=c.settings)
    else:
        reserve(c, len(c.body))
    with pytest.raises(ValueError, match="quota"):
        reserve(c, 1)


@pytest.mark.parametrize("bad", [-1, True, 1.5, "100"])
def test_invalid_configured_quota_cannot_admit_source(admission, bad):
    c = admission
    settings = replace(buffered_uploads.get_settings(), project_storage_quota_bytes=bad)
    with pytest.raises(ValueError, match="quota"):
        c.admit(settings=settings)
    with c.factory() as db:
        assert (
            db.scalar(
                select(BufferedUploadInput.command_id).where(
                    BufferedUploadInput.project_id == c.project_id
                )
            )
            is None
        )


def test_aggregate_quota_does_not_load_private_content_or_construct_sdk(pending):
    c = pending
    statements = []

    def observe(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)
            assert not re.search(r"\bbuffered_upload_inputs\.content\b", statement)

    event.listen(c.engine, "before_cursor_execute", observe)
    try:
        with c.factory() as db, db.begin():
            uploads._lock_project(db, c.project_id)
            uploads.require_storage_quota(db, c.project_id, 1, c.settings)
    finally:
        event.remove(c.engine, "before_cursor_execute", observe)
    assert any("buffered_upload_inputs" in value for value in statements)
    assert c.native.creates == []


def test_buffered_and_resumable_final_slot_is_serialized(admission_case):
    c = admission_case
    body = b"feature,target\n1,2\n"
    c.settings = replace(
        c.settings, project_storage_quota_bytes=len(body), max_upload_size_bytes=1000
    )
    c.payload = _payload(byte_size=len(body), retention_days=None)
    other_engine = create_engine(c.engine.url, pool_size=4, max_overflow=0)
    other_factory = sessionmaker(other_engine, expire_on_commit=False)
    barrier = threading.Barrier(2)

    def buffered():
        barrier.wait(timeout=10)
        return buffered_uploads.admit_input(
            other_factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            DatasetUploadRequest(dataset_name="same budget", filename="input.csv"),
            body,
            "buffered-final-slot",
            settings=c.settings,
        )

    def resumable():
        barrier.wait(timeout=10)
        return c.begin()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(buffered), pool.submit(resumable)]
            admitted, refused = [], []
            for future in futures:
                try:
                    admitted.append(future.result(timeout=20))
                except ValueError as exc:
                    assert "quota" in str(exc)
                    refused.append(exc)
        assert len(admitted) == len(refused) == 1
        with c.factory() as db:
            private = db.scalar(
                select(func.count())
                .select_from(BufferedUploadInput)
                .where(BufferedUploadInput.project_id == c.project_id)
            )
            direct = db.scalar(
                select(func.count())
                .select_from(DatasetUploadSession)
                .where(DatasetUploadSession.project_id == c.project_id)
            )
            assert private + direct == 1
    finally:
        with c.factory() as db, db.begin():
            for row in db.scalars(
                select(BufferedUploadInput)
                .where(BufferedUploadInput.project_id == c.project_id)
                .with_for_update()
            ):
                assert row.phase == "admitted" and row.native_receipt == {}
                assert (
                    db.get(WorkflowCommand, row.command_id).idempotency_key == "buffered-final-slot"
                )
                row.phase, row.content = "retired", None
                db.flush()
                db.delete(row)
        other_engine.dispose()


def test_zero_budget_refuses_first_buffered_input(admission):
    c = admission
    settings = replace(buffered_uploads.get_settings(), project_storage_quota_bytes=0)
    with pytest.raises(ValueError, match="quota"):
        c.admit(settings=settings)
    with c.factory() as db:
        assert (
            db.scalar(select(WorkflowCommand.id).where(WorkflowCommand.project_id == c.project_id))
            is None
        )


def test_another_projects_private_input_does_not_consume_our_budget(pending):
    from automl_api.models.projects import Project

    c = pending
    with c.factory() as db, db.begin():
        project = Project(owner_id=c.actor_id, created_by_id=c.actor_id, name="Quota isolation")
        db.add(project)
        db.flush()
        uploads._lock_project(db, project.id)
        uploads.require_storage_quota(
            db, project.id, 1, replace(c.settings, project_storage_quota_bytes=1)
        )
        db.delete(project)


def test_pending_sql_metadata_counts_with_autoflush_disabled(pending):
    from automl_api.models.enums import DatasetFormat, ObjectStoreType

    c = pending
    c.run()
    factory = sessionmaker(c.engine, autoflush=False, expire_on_commit=False)
    with pytest.raises(ValueError, match="quota"):
        with factory() as db, db.begin():
            uploads._lock_project(db, c.project_id)
            original = db.scalar(
                select(DatasetVersion).where(DatasetVersion.project_id == c.project_id)
            )
            db.add(
                DatasetVersion(
                    project_id=c.project_id,
                    dataset_id=original.dataset_id,
                    created_by_id=c.actor_id,
                    version_number=2,
                    format=DatasetFormat.CSV,
                    object_store_type=ObjectStoreType.S3_COMPATIBLE,
                    object_uri=original.object_uri + "-other",
                    content_hash="c" * 64,
                    byte_size=len(c.body),
                )
            )
            uploads.require_storage_quota(
                db,
                c.project_id,
                1,
                replace(c.settings, project_storage_quota_bytes=2 * len(c.body)),
            )
    with c.factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(DatasetVersion)
                .where(DatasetVersion.project_id == c.project_id)
            )
            == 1
        )


@pytest.mark.parametrize("at_limit", [False, True])
def test_actual_public_buffered_route_counts_private_input_and_replays_original_response(
    pending, monkeypatch, at_limit
):
    from types import SimpleNamespace

    from automl_api.api.deps import get_current_user
    from automl_api.api.routes import datasets as routes
    from automl_api.db.session import get_db
    from automl_api.services import datasets
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    c = pending
    c.settings = replace(
        c.settings, project_storage_quota_bytes=2 * len(c.body) - int(not at_limit)
    )
    writes = []

    def put(key, body):
        writes.append((key, body))
        return SimpleNamespace(uri=f"s3c://{c.settings.object_store_bucket}/{key}")

    monkeypatch.setattr(datasets, "get_object_store", lambda: SimpleNamespace(put_bytes=put))
    monkeypatch.setattr(datasets, "get_settings", lambda: c.settings)
    monkeypatch.setattr(routes, "get_settings", lambda: c.settings)
    if not at_limit:
        monkeypatch.setattr(
            datasets,
            "inspect_tabular_bytes",
            lambda *_: pytest.fail("Reject exhausted quota before inspection"),
        )
    app = FastAPI()
    app.include_router(routes.router)

    def database():
        with c.factory() as db:
            yield db

    def actor():
        with c.factory() as db:
            return db.get(User, c.actor_id)

    app.dependency_overrides[get_db] = database
    app.dependency_overrides[get_current_user] = actor

    def post(key):
        return TestClient(app).post(
            f"/projects/{c.project_id}/datasets/upload",
            headers={"Idempotency-Key": key},
            data={"dataset_name": "public buffered"},
            files={"file": ("public.csv", c.body, "text/csv")},
        )

    response = post("legacy-public-quota")
    if at_limit:
        assert response.status_code == 201, response.text
        assert len(writes) == 1
        denied = post("legacy-public-over-budget")
        assert denied.status_code == 422 and "quota" in denied.text
        c.settings = replace(c.settings, project_storage_quota_bytes=0)
        replay = post("legacy-public-quota")
        assert replay.status_code == 201 and replay.json() == response.json()
        assert len(writes) == 1
    else:
        assert response.status_code == 422 and "quota" in response.text
        assert writes == []
        with c.factory() as db:
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(WorkflowCommand)
                    .where(WorkflowCommand.project_id == c.project_id)
                )
                == 1
            )
