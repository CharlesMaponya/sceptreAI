"""Terminal upload metadata is not proof that native upload authority retired."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.iam import User
from automl_api.models.projects import Project
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowCommand,
)
from automl_api.services import deletion, uploads
from automl_api.services import upload_admission as admission
from automl_api.services import upload_completion as completion
from automl_api.services import upload_retirement as retirement
from automl_api.storage.contracts import DeletionPending
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_phase2_upload_services import _settings
from test_project_management_integration import reconcile_deletion_stage
from test_upload_admission_deletion import _queue
from test_upload_admission_deletion import deletion_case as deletion_case
from test_upload_admission_workflow import admission_case as admission_case
from test_upload_completion_workflow import completion_case as completion_case
from test_upload_retirement_workflow import retirement_case as retirement_case
from test_upload_verification import verification_case as verification_case


def test_actual_project_delete_rejects_terminal_flag_without_native_retirement(deletion_case):
    c = deletion_case
    c.begin()
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).status = "aborted"
    constructors = c.constructions
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409 and c.constructions == constructors


def _remove_tombstones(c):
    with c.factory() as db, db.begin():
        db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
        db.execute(delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id))


@pytest.fixture
def closed_case(retirement_case, monkeypatch):
    c = retirement_case
    monkeypatch.setattr(admission, "get_settings", _settings)
    try:
        yield c
    finally:
        _remove_tombstones(c)


@pytest.fixture
def ready_case(verification_case, monkeypatch):
    c = verification_case
    monkeypatch.setattr(admission, "get_settings", lambda: c.settings)
    assert c.verify() == 1
    try:
        yield c
    finally:
        _remove_tombstones(c)


@pytest.mark.parametrize(
    "status", ["aborted", "expired", "failed", "quarantined", "ready", "unknown"]
)
@pytest.mark.parametrize("metadata", ["present", "erased", "relabelled"])
def test_all_terminal_flags_require_independent_native_closure(deletion_case, status, metadata):
    c = deletion_case
    c.begin()
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        row.status = status
        if metadata != "present":
            row.provider_state = {}
        if metadata == "relabelled":
            row.provider_driver = "embedded"
    before = c.constructions
    with pytest.raises(HTTPException):
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert c.constructions == before


def test_original_published_retirement_allows_project_delete_without_provider_reads(closed_case):
    c = closed_case
    c.cancel()
    events = list(c.events)
    assert _queue(c)
    assert c.events == events


def test_original_verified_completion_allows_project_delete_without_provider_reads(ready_case):
    c = ready_case
    calls = list(c.io)
    assert _queue(c)
    assert c.io == calls


@pytest.mark.parametrize("stage", ["runtime", "object_store", "prepared_store", "metadata"])
@pytest.mark.parametrize("corruption", ["marker", "source", "receipt", "command"])
def test_every_queued_stage_rechecks_original_retirement_before_effects(
    closed_case, monkeypatch, stage, corruption
):
    c = closed_case
    c.cancel()
    tombstone_id, entry_id = _queue(c)
    with c.factory() as db, db.begin():
        for item in db.scalars(
            select(DeletionStage).where(DeletionStage.tombstone_id == tombstone_id)
        ):
            if deletion.STAGES["project"].index(item.resource_class) < deletion.STAGES[
                "project"
            ].index(stage):
                item.status = "completed"
        row = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(row.provider_state)
        if corruption == "marker":
            state.pop(retirement.RECORD)
            state.pop(retirement.STARTED)
        elif corruption == "source":
            row.original_filename = "foreign.csv"
        elif corruption == "receipt":
            state[retirement.RECORD]["receipt"]["intent_digest"] = "foreign"
        else:
            command = db.get(
                WorkflowCommand,
                uuid.UUID(state[retirement.RECORD]["intent"]["command_id"]),
            )
            command.response_payload = {
                **command.response_payload,
                "retirement_receipt_digest": "foreign",
            }
        row.provider_state = state
    store, k8s = MagicMock(), MagicMock()
    monkeypatch.setattr(deletion, "get_object_store", store)
    with pytest.raises(DeletionPending):
        with c.factory() as db, db.begin():
            deletion.reconcile_deletion(db, db.get(OutboxEntry, entry_id), k8s)
    store.assert_not_called()
    assert not k8s.mock_calls
    with c.factory() as db:
        assert db.get(Project, c.project_id) is not None


@pytest.mark.parametrize("corruption", ["marker", "receipt", "version", "uri", "source", "store"])
def test_completed_upload_custody_cannot_be_replaced_by_a_ready_flag(
    ready_case, monkeypatch, corruption
):
    from dataclasses import replace

    c = ready_case
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(row.provider_state)
        if corruption == "marker":
            state.pop(completion.RECORD)
            state.pop(uploads.UPLOAD_COMPLETION_STARTED)
        elif corruption == "receipt":
            state[completion.RECORD]["receipt"]["byte_size"] = 1
        elif corruption == "version":
            state[completion.RECORD]["receipt"]["object_version"] = "foreign"
        elif corruption == "uri":
            row.completed_object_uri = "s3c://foreign/object"
        elif corruption == "source":
            row.object_key += "-changed"
        row.provider_state = state
    if corruption == "store":
        monkeypatch.setattr(
            admission, "get_settings", lambda: replace(c.settings, object_store_bucket="foreign")
        )
    with pytest.raises(HTTPException):
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)


def test_retirement_with_missing_all_metadata_is_held_by_independent_commands(closed_case):
    c = closed_case
    c.cancel()
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        row.provider_state = {}
        row.provider_driver = "embedded"
    with pytest.raises(HTTPException):
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)


@pytest.mark.parametrize("operation", [completion.PROTOCOL, retirement.PROTOCOL])
def test_orphan_native_completion_or_retirement_is_a_deletion_barrier(closed_case, operation):
    c = closed_case
    if operation == completion.PROTOCOL:
        c.send()
    else:
        c.cancel()
    with c.factory() as db, db.begin():
        db.execute(
            delete(WorkflowCommand).where(
                WorkflowCommand.resource_id == c.upload_id, WorkflowCommand.operation != operation
            )
        )
        db.delete(db.get(DatasetUploadSession, c.upload_id))
    with c.factory() as db:
        assert deletion._project_uploads_unsettled(db, c.project_id)


def test_legacy_remote_native_id_without_original_custody_is_not_inferred_safe(deletion_case):
    c = deletion_case
    c.begin()
    with c.factory() as db, db.begin():
        db.execute(delete(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id))
        row = db.get(DatasetUploadSession, c.upload_id)
        row.provider_state = {}
        row.status = "expired"
        row.expires_at = datetime.now(UTC) - timedelta(days=2)
    with c.factory() as db:
        assert deletion._project_uploads_unsettled(db, c.project_id)


def _issue(c, key):
    from automl_api.schemas.datasets import TransferCursorRead
    from automl_api.services.upload_instructions import issue_upload_instruction

    return issue_upload_instruction(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        TransferCursorRead(unit_number=1, offset=0, length=6),
        key,
        origin="https://app.test",
        settings=_settings(),
    )


def _finish_original_deletion(c, monkeypatch):
    from types import SimpleNamespace

    tombstone_id, entry_id = _queue(c)
    prefixes = []
    monkeypatch.setattr(
        deletion,
        "get_object_store",
        lambda *_: SimpleNamespace(
            driver_name=c.store.driver_name,
            uri_for_key=c.store.uri_for_key,
            delete_prefix=prefixes.append,
            prefix_absent=lambda prefix: prefix in prefixes,
        ),
    )
    for index in range(4):
        with c.factory() as db:
            assert reconcile_deletion_stage(db, db.get(OutboxEntry, entry_id), MagicMock()) is (
                index == 3
            )
    assert prefixes == [f"projects/{c.project_id}", f"automl/projects/{c.project_id}"]
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None
        assert db.get(DatasetUploadSession, c.upload_id) is None
        assert db.get(DeletionTombstone, tombstone_id).status == "complete"


@pytest.mark.parametrize(
    "field", ["source", "missing_source", "request_hash", "actor", "latest_receipt"]
)
def test_all_instruction_authorities_are_checked_not_only_latest_unit_pointer(closed_case, field):
    from automl_api.services.workflow_state import canonical_request_hash as digest

    c = closed_case
    _issue(c, "older")
    _issue(c, "latest")
    c.cancel()
    with c.factory() as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == "dataset.upload.instruction",
                WorkflowCommand.idempotency_key
                == ("latest" if field == "latest_receipt" else "older"),
            )
        )
        payload = copy.deepcopy(command.request_payload)
        if field == "source":
            payload["source"]["multipart_upload_id"] = "foreign"
            command.request_payload = payload
            command.request_hash = digest(payload)
        elif field == "missing_source":
            payload.pop("source")
            command.request_payload = payload
            command.request_hash = digest(payload)
        elif field == "actor":
            payload["actor_id"] = str(uuid.uuid4())
            command.request_payload = payload
            command.request_hash = digest(payload)
        elif field == "request_hash":
            command.request_hash = "foreign"
        else:
            record = copy.deepcopy(command.response_payload)
            record["receipt"]["url"] = "https://foreign.test/cap"
            command.response_payload = record
    with pytest.raises(HTTPException):
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)


@pytest.mark.parametrize("marker", ["verification", "instruction", "original_uri", "completed_uri"])
def test_embedded_relabelling_cannot_hide_remaining_native_evidence(deletion_case, marker):
    c = deletion_case
    c.begin()
    with c.factory() as db, db.begin():
        db.execute(delete(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id))
        row = db.get(DatasetUploadSession, c.upload_id)
        row.status = "aborted"
        row.provider_driver = "embedded"
        row.provider_state = {}
        if marker == "verification":
            row.provider_state = {uploads.UPLOAD_VERIFICATION_RECORD: {"phase": "published"}}
        elif marker == "instruction":
            row.instruction_state = {"1": {"instruction_id": "native"}}
        elif marker == "original_uri":
            row.provider_state = {"upload_object_uri": "s3c://bucket/native"}
        else:
            row.completed_object_uri = "s3c://bucket/native"
    with c.factory() as db:
        assert deletion._project_uploads_unsettled(db, c.project_id)


@pytest.mark.parametrize("seconds", [-1, 0, 1])
@pytest.mark.parametrize("closed", [False, True])
def test_azure_native_completion_does_not_revoke_live_upload_credentials(
    deletion_case, monkeypatch, seconds, closed
):
    from dataclasses import replace
    from types import SimpleNamespace

    from automl_api.schemas.datasets import CompleteUploadRequest, TransferCursorRead
    from automl_api.services.upload_instructions import issue_upload_instruction
    from automl_api.storage.azure import AzureBlobObjectStoreDriver
    from test_phase2_upload_drivers import AzureService

    c = deletion_case
    service = AzureService()
    c.store = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        container="bucket",
        account_name="account",
        service_client=service,
        sas_factory=lambda **kwargs: "sig=test",
    )
    c.settings = replace(
        c.settings,
        object_store_type="azure_blob",
        object_store_bucket="bucket",
        object_store_endpoint="https://account.blob.test",
        azure_storage_account="account",
    )
    monkeypatch.setattr(admission, "get_settings", lambda: c.settings)
    monkeypatch.setattr(completion, "get_object_store", lambda settings: c.store)
    c.begin()
    for key in ("original", "refresh"):
        issue_upload_instruction(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            TransferCursorRead(unit_number=1, offset=0, length=6),
            key,
            origin="https://app.test",
            settings=c.settings,
        )
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        blob = service.container.get_blob_client(row.object_key)
        expiry = row.expires_at
    blob.blocks = [
        SimpleNamespace(id="sceptre-00000001", size=6),
        SimpleNamespace(id="sceptre-00000002", size=4),
    ]
    if closed:
        completion.complete_upload_session(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            CompleteUploadRequest(sha256=c.payload.sha256),
            "azure-complete",
            settings=c.settings,
        )
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).status = "failed"

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = expiry + timedelta(seconds=seconds)
            return value if tz else value.replace(tzinfo=None)

    monkeypatch.setattr(deletion, "datetime", Clock)
    before = c.constructions
    with c.factory() as db:
        assert deletion._project_uploads_unsettled(db, c.project_id) is (not closed or seconds < 0)
    assert c.constructions == before


def test_original_retirement_allows_every_deletion_stage_with_instruction_history(
    closed_case, monkeypatch
):
    c = closed_case
    _issue(c, "first")
    _issue(c, "refresh")
    c.cancel()
    _finish_original_deletion(c, monkeypatch)


def test_original_verified_completion_allows_every_deletion_stage(ready_case, monkeypatch):
    _finish_original_deletion(ready_case, monkeypatch)


@pytest.fixture(autouse=True)
def local_test_connections_only(monkeypatch):
    import socket

    connect = socket.socket.connect

    def local(sock, address):
        if isinstance(address, tuple) and address[0] not in {"127.0.0.1", "::1", "localhost"}:
            raise AssertionError(
                "This owned PostgreSQL/provider test must not contact nonlocal services"
            )
        return connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", local)


def test_local_connection_guard_rejects_external_address_before_network_access():
    import socket

    with socket.socket() as sock, pytest.raises(AssertionError, match="nonlocal services"):
        sock.connect(("192.0.2.1", 443))


def test_instruction_history_beyond_one_batch_is_verified_without_native_reads(
    closed_case, monkeypatch
):
    from automl_api.services import upload_deletion

    original = upload_deletion._instruction_source
    checked = set()

    def inspect(upload, command):
        checked.add(command.id)
        return original(upload, command)

    monkeypatch.setattr(upload_deletion, "_instruction_source", inspect)
    c = closed_case
    for number in range(101):
        _issue(c, f"unit-refresh-{number}")
    c.cancel()
    events = list(c.events)
    with c.factory() as db:
        assert not deletion._project_uploads_unsettled(db, c.project_id)
    assert len(checked) == 101
    with c.factory() as db, db.begin():
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == c.project_id,
                WorkflowCommand.operation == "dataset.upload.instruction",
                WorkflowCommand.idempotency_key == "unit-refresh-100",
            )
        )
        command.request_hash = "corrupt-last-history-entry"
    with c.factory() as db:
        assert deletion._project_uploads_unsettled(db, c.project_id)
    assert c.events == events


def test_dataset_version_hold_alone_blocks_actual_project_deletion(ready_case):
    from automl_api.models.datasets import DatasetVersion

    c = ready_case
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        assert not row.legal_hold
        db.get(DatasetVersion, row.dataset_version_id).legal_hold = True
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409


@pytest.mark.parametrize("stage", ["runtime", "object_store", "prepared_store", "metadata"])
@pytest.mark.parametrize("policy", ["hold", "retention"])
def test_every_queued_stage_honors_dataset_version_policy_independently(
    ready_case, monkeypatch, stage, policy
):
    from automl_api.models.datasets import DatasetVersion

    c = ready_case
    tombstone_id, entry_id = _queue(c)
    with c.factory() as db, db.begin():
        for item in db.scalars(
            select(DeletionStage).where(DeletionStage.tombstone_id == tombstone_id)
        ):
            if deletion.STAGES["project"].index(item.resource_class) < deletion.STAGES[
                "project"
            ].index(stage):
                item.status = "completed"
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert not upload.legal_hold and upload.retention_until is None
        version = db.get(DatasetVersion, upload.dataset_version_id)
        if policy == "hold":
            version.legal_hold = True
        else:
            version.retention_until = datetime.now(UTC) + timedelta(days=1)
    store, k8s = MagicMock(), MagicMock()
    monkeypatch.setattr(deletion, "get_object_store", store)
    with pytest.raises(DeletionPending, match="retention or legal hold"):
        with c.factory() as db, db.begin():
            deletion.reconcile_deletion(db, db.get(OutboxEntry, entry_id), k8s)
    store.assert_not_called()
    assert not k8s.mock_calls


@pytest.mark.parametrize("seconds", [-1, 0, 1])
def test_dataset_version_retention_uses_current_deadline_with_positive_controls(
    ready_case, monkeypatch, seconds
):
    from automl_api.models.datasets import DatasetVersion

    c = ready_case
    now = datetime.now(UTC)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        db.get(DatasetVersion, row.dataset_version_id).retention_until = now + timedelta(
            seconds=seconds
        )

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(deletion, "datetime", Clock)
    with c.factory() as db:
        assert deletion._project_uploads_retained(db, c.project_id) is (seconds > 0)
    if seconds > 0:
        with pytest.raises(HTTPException):
            _queue(c)
    else:
        assert _queue(c)
