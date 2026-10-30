"""Real PostgreSQL admission keeps bytes private and never invokes a native store."""

import base64
import copy
import hashlib
import json
import signal
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from automl_api.models.buffered_uploads import BufferedUploadInput
from automl_api.models.enums import (
    AuthProvider,
    CommandStatus,
    GlobalRole,
    ProjectRole,
    ProjectStatus,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import OutboxEntry, WorkflowCommand
from automl_api.schemas.datasets import DatasetUploadRequest
from automl_api.services import buffered_uploads as service
from automl_api.services.workflow_state import begin_command, canonical_request_hash
from automl_api.storage.contracts import UploadContractError
from fastapi import HTTPException
from fault_worker_environment import environment
from sqlalchemy import delete, func, inspect, select, text
from sqlalchemy.orm import sessionmaker
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine


@pytest.fixture
def admission(phase1_engine, committed_tenant, monkeypatch):
    actor_id, project_id = committed_tenant
    factory = sessionmaker(phase1_engine, expire_on_commit=False)
    with factory() as db:
        version = db.get(User, actor_id).token_version
    c = SimpleNamespace(
        engine=phase1_engine,
        factory=factory,
        actor_id=actor_id,
        project_id=project_id,
        token_version=version,
        key="buffered-" + uuid.uuid4().hex,
        body=b"feature,target\nPRIVATE_INPUT_CANARY,1\n",
        extra_users=[],
    )
    c.payload = DatasetUploadRequest(
        dataset_name="private admission", filename="data.csv", tags={"source": "test"}
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Admission must not construct a storage SDK")

    for module in (
        "automl_api.storage.object_store",
        "automl_api.services.datasets",
        "automl_api.services.upload_admission",
    ):
        monkeypatch.setattr(module + ".get_object_store", forbidden)
    for name in (
        "S3ObjectStoreDriver",
        "GCSObjectStoreDriver",
        "AzureBlobObjectStoreDriver",
        "EmbeddedObjectStoreDriver",
    ):
        monkeypatch.setattr("automl_api.storage.object_store." + name, forbidden)

    def admit(**kwargs):
        values = dict(
            factory=c.factory,
            actor_id=c.actor_id,
            token_version=c.token_version,
            project_id=c.project_id,
            payload=c.payload,
            content=c.body,
            key=c.key,
        )
        values.update(kwargs)
        return service.admit_input(**values)

    c.admit = admit
    try:
        yield c
    finally:
        with factory() as db, db.begin():
            rows = list(
                db.scalars(
                    select(BufferedUploadInput).where(BufferedUploadInput.project_id == project_id)
                )
            )
            for row in rows:
                if row.phase == "issued":
                    row.phase, row.native_receipt = (
                        "recorded",
                        {"synthetic_test": "no native operation was issued"},
                    )
                    db.flush()
                row.phase, row.content = "retired", None
                db.flush()
                db.delete(row)
            db.flush()
            for actor in c.extra_users:
                db.execute(delete(User).where(User.id == actor))


def _counts(c):
    with c.factory() as db:
        return [
            db.scalar(
                select(func.count()).select_from(model).where(model.project_id == c.project_id)
            )
            for model in (BufferedUploadInput, WorkflowCommand, OutboxEntry)
        ]


def test_admission_commits_exact_private_source_and_minimal_delivery(admission, monkeypatch):
    c = admission
    actual = service.inspect_tabular_bytes

    def inspect_without_checkout(*args):
        assert c.engine.pool.checkedout() == 0
        return actual(*args)

    monkeypatch.setattr(service, "inspect_tabular_bytes", inspect_without_checkout)
    identifier = c.admit()
    assert _counts(c) == [1, 1, 1]
    with c.factory() as db:
        command = db.get(WorkflowCommand, identifier)
        row = service.original_input(db, command)
        assert "content" in inspect(row).unloaded
        assert row.content == c.body
        assert row.byte_size == len(c.body)
        assert row.content_sha256 == hashlib.sha256(c.body).hexdigest()
        assert row.phase == "admitted" and row.native_receipt == {}
        assert row.manifest["token_version"] == c.token_version
        assert row.manifest_digest == canonical_request_hash(row.manifest)
        assert command.request_payload == {
            **c.payload.model_dump(mode="json"),
            "content_sha256": hashlib.sha256(c.body).hexdigest(),
            "byte_size": len(c.body),
        }
        delivery = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == command.id))
        public = json.dumps([command.request_payload, command.response_payload, delivery.payload])
        assert "PRIVATE_INPUT_CANARY" not in public
        assert base64.b64encode(c.body).decode() not in public
        assert command.response_status is None and command.status == CommandStatus.PENDING


@pytest.mark.parametrize("change", ["bytes", "filename", "name", "tags", "description"])
def test_admission_conflicts_preserve_original_source(admission, change):
    c = admission
    identifier = c.admit()
    payload, body = c.payload.model_copy(deep=True), c.body
    if change == "bytes":
        body = b"feature,target\nCHANGED_PRIVATE_INPUT,2\n"
    else:
        field = {
            "filename": "filename",
            "name": "dataset_name",
            "tags": "tags",
            "description": "description",
        }[change]
        setattr(payload, field, {"changed": True} if change == "tags" else "changed.csv")
    for _ in range(2):
        with pytest.raises(HTTPException) as error:
            c.admit(payload=payload, content=body)
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "idempotency_key_reused"
    assert _counts(c) == [1, 1, 1]
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).content == c.body


@pytest.mark.parametrize("mode", ["identical", "changed", "two-keys"])
def test_concurrent_admission_has_one_original_per_key(admission, mode):
    c = admission
    barrier = threading.Barrier(2)

    def attempt(index):
        barrier.wait(timeout=5)
        try:
            return c.admit(
                key=c.key + (str(index) if mode == "two-keys" else ""),
                content=c.body if not index or mode != "changed" else b"feature,target\nother,2\n",
            )
        except HTTPException as exc:
            assert exc.status_code == 409
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        identifiers = list(pool.map(attempt, range(2)))
    assert _counts(c) == ([2, 2, 2] if mode == "two-keys" else [1, 1, 1])
    if mode == "identical":
        assert identifiers[0] == identifiers[1]
    elif mode == "changed":
        assert sum(item is not None for item in identifiers) == 1


@pytest.mark.parametrize(
    "state", ["inactive", "token", "archived", "deleting", "viewer", "expired"]
)
def test_current_authority_rejects_admission_and_existing_replay(admission, state):
    c = admission
    identifier = c.admit()
    with c.factory() as db, db.begin():
        user = db.get(User, c.actor_id)
        project = db.get(Project, c.project_id)
        member = db.scalar(
            select(ProjectMembership).where(
                ProjectMembership.project_id == c.project_id,
                ProjectMembership.user_id == c.actor_id,
            )
        )
        if state == "inactive":
            user.is_active = False
        elif state == "token":
            user.token_version += 1
        elif state == "archived":
            project.status = ProjectStatus.ARCHIVED
        elif state == "deleting":
            project.settings = {"deletion_in_progress": True}
        elif state == "viewer":
            member.role = ProjectRole.VIEWER
        else:
            member.expires_at = datetime(2000, 1, 1, tzinfo=UTC)
    for key in (c.key, c.key + "-new"):
        with pytest.raises(HTTPException):
            c.admit(key=key)
    assert _counts(c) == [1, 1, 1]
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).content == c.body


def test_authority_is_rechecked_after_sql_free_inspection(admission, monkeypatch):
    c = admission
    actual = service.inspect_tabular_bytes

    def revoke(*args):
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).is_active = False
        return actual(*args)

    monkeypatch.setattr(service, "inspect_tabular_bytes", revoke)
    with pytest.raises(HTTPException) as error:
        c.admit()
    assert error.value.status_code == 401 and _counts(c) == [0, 0, 0]


def test_atomic_admission_rolls_back_input_command_and_delivery_on_commit_failure(
    admission, monkeypatch
):
    c = admission
    actual = service.enqueue_outbox

    def fail(*args, **kwargs):
        actual(*args, **kwargs)
        raise RuntimeError("Injected failure before SQL commit")

    monkeypatch.setattr(service, "enqueue_outbox", fail)
    with pytest.raises(RuntimeError, match="Injected failure"):
        c.admit()
    assert _counts(c) == [0, 0, 0]
    monkeypatch.setattr(service, "enqueue_outbox", actual)
    assert c.admit() == c.admit() and _counts(c) == [1, 1, 1]


@pytest.mark.parametrize(
    "mutation", ["request", "type", "resource", "delivery", "duplicate-delivery"]
)
def test_changed_command_or_delivery_holds_instead_of_recreating_input(admission, mutation):
    c = admission
    identifier = c.admit()
    with c.factory() as db, db.begin():
        command = db.get(WorkflowCommand, identifier)
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.command_id == identifier))
        if mutation == "request":
            command.request_payload = {**command.request_payload, "filename": "changed.csv"}
        elif mutation == "type":
            command.resource_type = "foreign"
        elif mutation == "resource":
            command.resource_id = uuid.uuid4()
        elif mutation == "delivery":
            entry.payload = {"input_id": str(uuid.uuid4())}
        else:
            db.add(
                OutboxEntry(
                    project_id=c.project_id,
                    command_id=identifier,
                    event_key="duplicate",
                    topic=entry.topic,
                    aggregate_type=entry.aggregate_type,
                    aggregate_id=entry.aggregate_id,
                    payload=copy.deepcopy(entry.payload),
                )
            )
    with pytest.raises(UploadContractError):
        c.admit()
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).content == c.body


def test_legacy_pending_command_never_acquires_an_inferred_private_source(admission):
    c = admission
    with c.factory() as db, db.begin():
        begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.actor_id,
            operation=service.OPERATION,
            idempotency_key=c.key,
            payload=service._request(c.payload, c.body),
        )
    with pytest.raises(UploadContractError, match="missing"):
        c.admit()
    assert _counts(c) == [0, 1, 0]


@pytest.mark.parametrize("content", [b"", bytearray(b"file"), "file"])
def test_invalid_private_body_never_creates_a_command(admission, content):
    with pytest.raises(ValueError):
        admission.admit(content=content)
    assert _counts(admission) == [0, 0, 0]


@pytest.mark.parametrize("key", ["", " ", "x" * 256])
def test_invalid_key_never_creates_private_source(admission, key):
    with pytest.raises(ValueError):
        admission.admit(key=key)
    assert _counts(admission) == [0, 0, 0]


@pytest.mark.parametrize("bound", [0, True, 1])
def test_production_buffer_bound_is_checked_before_admission(admission, bound):
    settings = replace(
        service.get_settings(), environment="production", buffered_upload_max_bytes=bound
    )
    with pytest.raises(ValueError):
        admission.admit(settings=settings)
    assert _counts(admission) == [0, 0, 0]


def _cached_result(c):
    stamp = datetime.now(UTC).isoformat()
    common = dict(
        project_id=str(c.project_id),
        created_by_id=str(c.actor_id),
        created_at=stamp,
        updated_at=stamp,
    )
    dataset_id = str(uuid.uuid4())
    return {
        "dataset": dict(
            common, id=dataset_id, name=c.payload.dataset_name, latest_version_number=1, tags={}
        ),
        "version": dict(
            common,
            id=str(uuid.uuid4()),
            dataset_id=dataset_id,
            version_number=1,
            status="ready",
            format="csv",
            object_store_type="embedded",
            object_uri="embedded://owned/legacy.csv",
            original_filename=c.payload.filename,
            content_hash=hashlib.sha256(c.body).hexdigest(),
            byte_size=len(c.body),
            schema_json={},
            inferred_types_json={},
            quality_report_json={},
        ),
    }


@pytest.mark.parametrize(
    "fault", [None, "project", "dataset", "hash", "filename", "new-missing-input", "foreign-type"]
)
def test_cached_legacy_replay_and_missing_new_custody_are_distinct(admission, monkeypatch, fault):
    c = admission
    result = _cached_result(c)
    if fault == "project":
        result["version"]["project_id"] = str(uuid.uuid4())
    elif fault == "dataset":
        result["version"]["dataset_id"] = str(uuid.uuid4())
    elif fault == "hash":
        result["version"]["content_hash"] = "a" * 64
    elif fault == "filename":
        result["version"]["original_filename"] = "foreign.csv"
    with c.factory() as db, db.begin():
        command, _ = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.actor_id,
            operation=service.OPERATION,
            idempotency_key=c.key,
            payload=service._request(c.payload, c.body),
        )
        command.status, command.response_status, command.response_payload = (
            CommandStatus.SUCCEEDED,
            201,
            result,
        )
        command.resource_type = (
            service.PROTOCOL
            if fault == "new-missing-input"
            else ("foreign" if fault == "foreign-type" else service.OPERATION)
        )
        identifier = command.id
    monkeypatch.setattr(
        service,
        "inspect_tabular_bytes",
        lambda *_: pytest.fail("Cached replay must not reinspect input"),
    )
    if fault is None:
        assert c.admit() == identifier and c.admit() == identifier
    else:
        with pytest.raises(UploadContractError):
            c.admit()
    assert _counts(c) == [0, 1, 0]


def test_other_authorized_actor_cannot_rebind_original_input(admission):
    c = admission
    identifier = c.admit()
    with c.factory() as db, db.begin():
        other = User(
            email=f"buffered-other-{uuid.uuid4().hex}@example.test",
            full_name="Other admin",
            auth_provider=AuthProvider.SIMPLE,
            global_role=GlobalRole.ADMIN,
        )
        db.add(other)
        db.flush()
        actor_id, version = other.id, other.token_version
        c.extra_users.append(actor_id)
    with pytest.raises(HTTPException) as error:
        c.admit(actor_id=actor_id, token_version=version)
    assert error.value.status_code == 409
    with c.factory() as db:
        assert db.get(WorkflowCommand, identifier).actor_id == c.actor_id


def test_production_exact_bound_does_not_construct_a_provider(admission):
    c = admission
    settings = replace(
        service.get_settings(),
        environment="production",
        object_store_type="aws_s3",
        buffered_upload_max_bytes=len(c.body),
    )
    identifier = c.admit(settings=settings)
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).manifest["driver"] == "aws_s3"


def test_production_embedded_source_is_rejected_before_admission(admission):
    settings = replace(
        service.get_settings(), environment="production", object_store_type="embedded"
    )
    with pytest.raises(ValueError, match="forbidden"):
        admission.admit(settings=settings)
    assert _counts(admission) == [0, 0, 0]


@pytest.mark.parametrize(
    "configured,expected",
    [
        ("aws_s3", "aws_s3"),
        (" aws_s3 ", "aws_s3"),
        (" minio ", "s3_compatible"),
        (" azure_blob ", "azure_blob"),
        ("gcs", "gcs"),
    ],
)
def test_provider_configuration_is_canonical_without_sdk_construction(
    admission, configured, expected
):
    c = admission
    settings = replace(
        service.get_settings(),
        object_store_type=configured,
        object_store_endpoint="https://objects.test",
        object_store_access_key="test",
        object_store_secret_key="test",
        azure_storage_account="account",
    )
    identifier = c.admit(settings=settings)
    with c.factory() as db:
        assert db.get(BufferedUploadInput, identifier).manifest["driver"] == expected


@pytest.mark.parametrize(
    "configuration",
    [
        {"object_store_type": "unexpected"},
        {"object_store_type": "s3"},
        {"object_store_type": "azure"},
        {"object_store_type": "minio", "object_store_endpoint": ""},
        {"object_store_type": "minio", "object_store_access_key": ""},
        {"object_store_type": "minio", "object_store_secret_key": ""},
        {"object_store_type": "azure_blob", "azure_storage_account": ""},
        {"object_store_type": "azure_blob", "object_store_endpoint": ""},
    ],
)
def test_invalid_provider_configuration_is_rejected_before_inspection_and_input(
    admission, monkeypatch, configuration
):
    settings = replace(
        service.get_settings(),
        object_store_endpoint="https://objects.test",
        object_store_access_key="test",
        object_store_secret_key="test",
        azure_storage_account="account",
    )
    settings = replace(settings, **configuration)
    monkeypatch.setattr(
        service,
        "inspect_tabular_bytes",
        lambda *_: pytest.fail("Configuration must validate first"),
    )
    with pytest.raises(ValueError):
        admission.admit(settings=settings)
    assert _counts(admission) == [0, 0, 0]


@pytest.mark.parametrize("mode", ["before-commit", "after-commit"])
def test_process_loss_on_either_side_of_private_admission_commit(admission, tmp_path, mode):
    c = admission
    (tmp_path / "body.csv").write_bytes(c.body)
    (tmp_path / "payload.json").write_text(c.payload.model_dump_json())
    with (tmp_path / "worker.log").open("w") as output:
        process = subprocess.Popen(
            [
                sys.executable,
                "tests/support/buffered_input_fault_worker.py",
                str(c.actor_id),
                str(c.project_id),
                c.key,
                str(tmp_path),
                mode,
            ],
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment(c.engine),
        )
    try:
        marker = tmp_path / "checkpoint.json"
        # Cold scientific-stack imports can exceed15s on the qualification workstation.
        deadline = time.monotonic() + 45
        while not marker.exists() and time.monotonic() < deadline:
            assert process.poll() is None, (tmp_path / "worker.log").read_text()
            time.sleep(0.02)
        assert marker.is_file(), (
            "Inspect the same live worker; do not restart it on observation timeout"
        )
        witness = json.loads(marker.read_text())
        assert witness["phase"] == mode and witness["pid"] == process.pid
        assert witness["sha256"] == hashlib.sha256(c.body).hexdigest()
        assert witness["sql_checkouts"] == (1 if mode == "before-commit" else 0)
        assert _counts(c) == ([0, 0, 0] if mode == "before-commit" else [1, 1, 1])
        process.kill()
        assert process.wait(timeout=5) == -signal.SIGKILL
        deadline = time.monotonic() + 5
        with c.factory() as db:
            while time.monotonic() < deadline:
                db.execute(text("SELECT pg_stat_clear_snapshot()"))
                remaining = db.scalar(
                    text("SELECT count(*) FROM pg_stat_activity WHERE pid=:pid"),
                    {"pid": witness["backend_pid"]},
                )
                if not remaining:
                    break
                time.sleep(0.02)
        assert not remaining
        identifier = c.admit()
        if mode == "after-commit":
            assert str(identifier) == witness["command_id"]
        assert _counts(c) == [1, 1, 1]
        with c.factory() as db:
            assert db.get(BufferedUploadInput, identifier).content == c.body
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdin.close()
