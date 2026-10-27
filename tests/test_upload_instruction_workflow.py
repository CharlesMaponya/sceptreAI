"""Instruction authority is durable before SQL-free native credential work."""

import copy
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import CommandStatus
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.workflows import WorkflowCommand
from automl_api.schemas.datasets import TransferCursorRead
from automl_api.services import upload_instructions as instructions
from automl_api.services import uploads
from automl_api.storage.contracts import TransferCursor, UploadContractError
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_workflow import admission_case as admission_case


def test_actual_instruction_route_releases_sql_for_construction_and_issuance(
    admission_case, monkeypatch
):
    c = admission_case
    c.begin()
    original = c.store.create_transfer_instruction
    seen = []

    def issue(upload, cursor):
        assert c.engine.pool.checkedout() == 0
        seen.append(upload.provider_id)
        return original(upload, cursor)

    monkeypatch.setattr(c.store, "create_transfer_instruction", issue)
    response = TestClient(c.app).post(
        f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/instructions",
        headers={"Origin": "https://app.test", "Idempotency-Key": "unit-1"},
        json={"cursor": {"unit_number": 1, "offset": 0, "length": 6}},
    )
    assert response.status_code == 200 and response.json()["cursor"]["length"] == 6
    assert seen == ["provider-1"]


@pytest.fixture
def instruction_case(admission_case, monkeypatch):
    c = admission_case
    c.begin()
    c.cursor = TransferCursorRead(unit_number=1, offset=0, length=6)
    c.instruction_key = "instruction-key"
    c.i_calls = []
    c.i_callback = None
    c.i_error = None
    c.i_patch = {}
    create = c.store.create_transfer_instruction

    def native(upload, cursor):
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db:
            row = db.get(DatasetUploadSession, c.upload_id)
            slot = row.instruction_state[str(cursor.unit_number)]
            command = db.get(WorkflowCommand, uuid.UUID(slot["command_id"]))
            record = instructions._journal(row, command)
            assert record["phase"] == "issued" and command.status == CommandStatus.RUNNING
        number = len(c.i_calls) + 1
        c.i_calls.append((upload, cursor))
        if c.i_callback:
            c.i_callback()
        if c.i_error:
            raise c.i_error
        result = create(upload, cursor)
        patch = dict(
            instruction_id=f"instruction-{number}",
            expires_at=min(
                upload.expires_at, instructions.datetime.now(UTC) + timedelta(minutes=15)
            ),
        )
        return replace(result, **{**patch, **c.i_patch})

    monkeypatch.setattr(c.store, "create_transfer_instruction", native)
    c.issue = lambda **kwargs: instructions.issue_upload_instruction(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        kwargs.get("cursor", c.cursor),
        kwargs.get("key", c.instruction_key),
        origin=kwargs.get("origin", "https://app.test"),
        settings=c.settings,
    )
    return c


def _command(db, c, key=None):
    return db.scalar(
        select(WorkflowCommand).where(
            WorkflowCommand.project_id == c.project_id,
            WorkflowCommand.operation == instructions.OPERATION,
            WorkflowCommand.idempotency_key == (key or c.instruction_key),
        )
    )


def _recorded(c, monkeypatch):
    save = instructions._save

    def fail(upload, command, record):
        save(upload, command, record)
        if record["phase"] == "published":
            raise OSError("publication rolled back")

    monkeypatch.setattr(instructions, "_save", fail)
    with pytest.raises(OSError):
        c.issue()
    monkeypatch.setattr(instructions, "_save", save)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert instructions._journal(row, _command(db, c))["phase"] == "recorded"


def _clock(monkeypatch, minutes=6):
    moment = instructions.datetime.now(UTC) + timedelta(minutes=minutes)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(instructions, "datetime", Clock)
    return moment


def _change(c, change):
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        if change == "actor":
            db.get(User, c.actor_id).is_active = False
        elif change == "token":
            db.get(User, c.actor_id).token_version += 1
        elif change == "membership":
            db.execute(
                delete(ProjectMembership).where(ProjectMembership.project_id == c.project_id)
            )
        elif change == "project":
            db.get(Project, c.project_id).settings = {"deletion_in_progress": True}
        elif change == "source":
            row.original_filename = "changed.csv"
        elif change == "hold":
            row.legal_hold = True
        elif change == "terminal":
            row.status = "failed"
        elif change == "completion":
            row.provider_state = {**row.provider_state, uploads.UPLOAD_COMPLETION_STARTED: True}
        elif change == "retirement":
            row.status = "retiring"


def test_known_instruction_replays_without_another_constructor_or_native_call(instruction_case):
    c = instruction_case
    first = c.issue()
    assert c.issue() == first
    assert len(c.i_calls) == 1 and c.constructions == 2
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        assert instructions._journal(row, command)["phase"] == "published"
        assert row.instruction_state["1"]["instruction_id"] == first.instruction_id


def test_known_reply_survives_publication_rollback_without_reissuance(
    instruction_case, monkeypatch
):
    c = instruction_case
    _recorded(c, monkeypatch)
    assert c.issue().instruction_id == "instruction-1"
    assert c.constructions == 2 and len(c.i_calls) == 1


@pytest.mark.parametrize(
    "change",
    ["actor", "token", "membership", "project", "source", "terminal", "completion", "retirement"],
)
def test_instruction_rejects_current_authority_before_native_work(instruction_case, change):
    c = instruction_case
    _change(c, change)
    with pytest.raises((HTTPException, UploadContractError)):
        c.issue()
    assert not c.i_calls and c.constructions == 1


@pytest.mark.parametrize(
    "change",
    [
        "actor",
        "token",
        "membership",
        "project",
        "source",
        "hold",
        "terminal",
        "completion",
        "retirement",
    ],
)
def test_known_reply_is_retained_but_not_delivered_after_native_callback_commit(
    instruction_case, change
):
    c = instruction_case
    c.i_callback = lambda: _change(c, change)
    with pytest.raises((HTTPException, UploadContractError)):
        c.issue()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        assert command.status == CommandStatus.RUNNING and command.response_status is None
        assert instructions._journal(row, command)["phase"] == "recorded"
    assert len(c.i_calls) == 1


def test_constructor_revocation_is_checked_before_credential_issuance(instruction_case):
    c = instruction_case
    c.on_construction = lambda: _change(c, "token")
    with pytest.raises(HTTPException) as error:
        c.issue()
    assert error.value.status_code == 401 and c.i_calls == []


@pytest.mark.parametrize("change", ["cursor", "origin"])
def test_instruction_key_cannot_change_original_request(instruction_case, change):
    c = instruction_case
    if change == "origin":
        c.settings = replace(
            c.settings, upload_allowed_origins=("https://app.test", "https://foreign.test")
        )
    c.issue()
    with pytest.raises((HTTPException, UploadContractError, ValueError)):
        c.issue(
            **(
                {"cursor": TransferCursorRead(unit_number=2, offset=6, length=4)}
                if change == "cursor"
                else {"origin": "https://foreign.test"}
            )
        )
    assert len(c.i_calls) == 1


def test_expired_known_reply_is_not_reissued_under_original_key(instruction_case, monkeypatch):
    c = instruction_case
    c.issue()
    _clock(monkeypatch, minutes=16)
    with pytest.raises(UploadContractError, match="expired"):
        c.issue()
    assert len(c.i_calls) == 1
    assert c.issue(key="explicit-refresh").instruction_id == "instruction-2"
    with pytest.raises(UploadContractError):
        c.issue()
    assert len(c.i_calls) == 2


def test_live_same_and_different_keys_cannot_steal_transfer_unit(instruction_case):
    c = instruction_case
    blocked = []

    def compete():
        for key in (c.instruction_key, "competing-key"):
            with pytest.raises(HTTPException) as error:
                c.issue(key=key)
            blocked.append(error.value.status_code)

    c.i_callback = compete
    assert c.issue().instruction_id == "instruction-1"
    assert blocked == [409, 409] and len(c.i_calls) == 1


def test_unknown_instruction_retries_original_target_only_after_lease_and_at_most_five_times(
    instruction_case, monkeypatch
):
    c = instruction_case
    c.i_error = OSError("delegation reply lost")
    for number in range(1, 6):
        with pytest.raises(OSError):
            c.issue()
        assert len(c.i_calls) == number
        with pytest.raises(HTTPException):
            c.issue()
        _clock(monkeypatch)
    with c.factory() as db, db.begin():
        command = _command(db, c)
        command.retry_count, command.max_retries = 0, 100
    with pytest.raises(UploadContractError, match="budget"):
        c.issue()
    assert len(c.i_calls) == 5 and all(value == c.i_calls[0] for value in c.i_calls)


def test_late_old_reply_is_retained_without_replacing_current_attempt(
    instruction_case, monkeypatch
):
    c = instruction_case

    def overlap():
        c.i_callback = None
        _clock(monkeypatch)
        c.winner = c.issue()

    c.i_callback = overlap
    with pytest.raises(UploadContractError, match="lost its native attempt"):
        c.issue()
    assert c.issue() == c.winner and len(c.i_calls) == 2
    with c.factory() as db:
        command = _command(db, c)
        assert len(command.response_payload["superseded_outcomes"]) == 1
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            instructions._journal(row, command)["receipt"]["instruction_id"]
            == c.winner.instruction_id
        )


@pytest.mark.parametrize(
    "change", ["request", "attempt", "receipt", "pointer", "projection", "counter"]
)
def test_instruction_custody_rejects_corruption_and_reporting_cannot_extend_budget(
    instruction_case, monkeypatch, change
):
    c = instruction_case
    _recorded(c, monkeypatch)
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        if change == "request":
            command.request_payload = {**command.request_payload, "store_digest": "foreign"}
        elif change == "counter":
            command.retry_count, command.max_retries = 100, 100
        else:
            record = copy.deepcopy(command.response_payload)
            state = copy.deepcopy(row.instruction_state)
            if change == "attempt":
                record["attempt"]["number"] = 4
            elif change == "receipt":
                record["receipt"]["url"] = "https://foreign.test/capability"
            elif change == "pointer":
                state["1"]["command_id"] = "00000000-0000-0000-0000-000000000000"
            elif change == "projection":
                state["1"]["receipt_digest"] = "foreign"
            command.response_payload = record
            row.instruction_state = state
    if change == "counter":
        assert c.issue().instruction_id == "instruction-1"
    else:
        with pytest.raises(UploadContractError):
            c.issue()
    assert len(c.i_calls) == 1


def test_real_gcs_driver_accepts_confirmed_partial_offset_and_original_session(
    admission_case, monkeypatch
):
    from automl_api.storage.gcs import GCSObjectStoreDriver
    from test_phase2_upload_drivers import GCSClient, GCSHttp

    c = admission_case
    c.store = GCSObjectStoreDriver(bucket="bucket", client=GCSClient(), http_client=GCSHttp())
    c.begin()
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).confirmed_bytes = 4
    cursor = TransferCursorRead(unit_number=1, offset=4, length=6)
    result = instructions.issue_upload_instruction(
        c.factory,
        c.actor_id,
        c.token_version,
        c.project_id,
        c.upload_id,
        cursor,
        "partial-offset",
        origin="https://app.test",
        settings=c.settings,
    )
    assert result.cursor.offset == 4 and result.cursor.length == 6
    assert result.headers["Content-Range"] == "bytes 4-9/10"
    with c.factory() as db:
        assert result.url == db.get(DatasetUploadSession, c.upload_id).multipart_upload_id


def test_different_transfer_units_keep_independent_owners_during_native_work(instruction_case):
    c = instruction_case

    def overlap():
        c.i_callback = None
        c.other = c.issue(
            key="unit-2", cursor=TransferCursorRead(unit_number=2, offset=6, length=4)
        )

    c.i_callback = overlap
    first = c.issue()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.instruction_state["1"]["instruction_id"] == first.instruction_id
        assert row.instruction_state["2"]["instruction_id"] == c.other.instruction_id
    assert len(c.i_calls) == 2


@pytest.mark.parametrize("phase", ["issued", "recorded", "published"])
def test_actual_owning_backend_loss_preserves_instruction_transaction_boundaries(
    instruction_case, monkeypatch, phase
):
    from sqlalchemy import event, text
    from sqlalchemy.exc import DBAPIError

    c = instruction_case
    killed = []

    def kill(session):
        if any(
            isinstance(row, WorkflowCommand)
            and row.operation == instructions.OPERATION
            and row.response_payload.get("phase") == phase
            for row in session.identity_map.values()
        ):
            pid = session.scalar(text("SELECT pg_backend_pid()"))
            with c.engine.connect() as control:
                assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(c.factory, "before_commit", kill)
    try:
        with pytest.raises(DBAPIError):
            c.issue()
    finally:
        event.remove(c.factory, "before_commit", kill)
    assert len(killed) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        command = _command(db, c)
        if phase == "issued":
            assert command is None and row.instruction_state == {}
        else:
            assert instructions._journal(row, command)["phase"] == (
                "recorded" if phase == "published" else "issued"
            )
    if phase == "recorded":
        with pytest.raises(HTTPException):
            c.issue()
        _clock(monkeypatch)
    assert c.issue().instruction_id == ("instruction-2" if phase == "recorded" else "instruction-1")
    assert len(c.i_calls) == (2 if phase == "recorded" else 1)


@pytest.mark.parametrize("known", [True, False])
def test_legacy_instruction_is_read_only_and_never_regenerates_uncertain_response(
    instruction_case, monkeypatch, known
):
    from automl_api.services.workflow_state import begin_command, transition_command

    c = instruction_case
    from test_phase2_upload_services import Driver

    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        value = Driver().create_transfer_instruction(
            uploads.provider_upload(row), TransferCursor(1, 0, 6)
        )
        reply = uploads.instruction_read(value)
        command, _ = begin_command(
            db,
            project_id=c.project_id,
            actor_id=c.actor_id,
            operation=instructions.OPERATION,
            idempotency_key=c.instruction_key,
            payload={
                "session_id": str(c.upload_id),
                "origin": "https://app.test",
                "cursor": c.cursor.model_dump(mode="json"),
            },
        )
        command.resource_id, command.resource_type = c.upload_id, instructions.OPERATION
        transition_command(command, CommandStatus.RUNNING)
        if known:
            transition_command(command, CommandStatus.SUCCEEDED)
            command.response_status = 200
            command.response_payload = reply.model_dump(mode="json")
            row.instruction_state = {
                "1": dict(
                    instruction_id=reply.instruction_id,
                    offset=0,
                    length=6,
                    expires_at=reply.expires_at.isoformat(),
                )
            }
    if known:
        assert c.issue() == reply
    else:
        with pytest.raises(UploadContractError, match="Legacy instruction"):
            c.issue()
    assert c.i_calls == [] and c.constructions == 1


def test_orphan_instruction_command_keeps_project_deletion_barrier(instruction_case):
    from automl_api.services.deletion import _project_uploads_unsettled

    c = instruction_case
    c.issue()
    with c.factory() as db, db.begin():
        db.execute(
            delete(WorkflowCommand).where(
                WorkflowCommand.resource_id == c.upload_id,
                WorkflowCommand.operation != "dataset.upload.instruction",
            )
        )
        db.delete(db.get(DatasetUploadSession, c.upload_id))
    with c.factory() as db:
        assert _project_uploads_unsettled(db, c.project_id)


def test_refreshed_authentication_can_publish_retained_reply_after_token_rotation(instruction_case):
    c = instruction_case
    c.i_callback = lambda: _change(c, "token")
    with pytest.raises(HTTPException):
        c.issue()
    c.token_version += 1
    c.i_callback = None
    assert c.issue().instruction_id == "instruction-1"
    assert len(c.i_calls) == 1 and c.constructions == 2


@pytest.mark.parametrize(
    "patch",
    [
        {"url": "javascript:invalid"},
        {"url": "https://storage.test/\r\nforeign"},
        {"instruction_id": ""},
        {"cursor": TransferCursor(2, 6, 4)},
    ],
)
def test_provider_cannot_publish_a_mismatched_or_invalid_instruction(instruction_case, patch):
    c = instruction_case
    c.i_patch = patch
    with pytest.raises(UploadContractError):
        c.issue()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert instructions._journal(row, _command(db, c))["phase"] == "unknown"
        assert "instruction_id" not in row.instruction_state["1"]


def test_erased_known_reply_cannot_downgrade_released_native_lease_into_reissuance(
    instruction_case, monkeypatch
):
    c = instruction_case
    _recorded(c, monkeypatch)
    with c.factory() as db, db.begin():
        command = _command(db, c)
        record = copy.deepcopy(command.response_payload)
        record["phase"] = "unknown"
        record.pop("receipt")
        command.response_payload = record
        row = db.get(DatasetUploadSession, c.upload_id)
        slots = copy.deepcopy(row.instruction_state)
        slots["1"].pop("receipt_digest")
        row.instruction_state = slots
    _clock(monkeypatch)
    with pytest.raises(UploadContractError):
        c.issue()
    assert len(c.i_calls) == 1 and c.constructions == 2


def test_expired_known_native_reply_is_retained_without_delivery_or_reissuance(instruction_case):
    c = instruction_case
    c.i_patch = {"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    with pytest.raises(UploadContractError, match="expired"):
        c.issue()
    with pytest.raises(UploadContractError, match="expired"):
        c.issue()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert instructions._journal(row, _command(db, c))["phase"] == "recorded"
    assert len(c.i_calls) == 1


@pytest.mark.parametrize("revoke", [False, True])
def test_azure_sdk_signer_uses_delegation_callback_outside_sql_and_keeps_original_receipt(
    admission_case, monkeypatch, revoke
):
    import base64

    from automl_api.storage.azure import AzureBlobObjectStoreDriver
    from azure.storage.blob import UserDelegationKey
    from test_phase2_upload_drivers import AzureService

    c = admission_case
    service = AzureService()
    calls = []

    def delegate(start, expiry):
        assert c.engine.pool.checkedout() == 0
        calls.append((start, expiry))
        if revoke:
            _change(c, "token")
        key = UserDelegationKey()
        key.signed_oid = "00000000-0000-0000-0000-000000000001"
        key.signed_tid = "00000000-0000-0000-0000-000000000002"
        key.signed_start = start.isoformat()
        key.signed_expiry = expiry.isoformat()
        key.signed_service = "b"
        key.signed_version = "2023-11-03"
        key.value = base64.b64encode(b"test-only-delegation-key").decode()
        return key

    service.get_user_delegation_key = delegate
    c.store = AzureBlobObjectStoreDriver(
        account_url="https://account.blob.test",
        container="bucket",
        account_name="account",
        service_client=service,
    )
    c.begin()

    def issue():
        return instructions.issue_upload_instruction(
            c.factory,
            c.actor_id,
            c.token_version,
            c.project_id,
            c.upload_id,
            TransferCursorRead(unit_number=1, offset=0, length=6),
            "azure-unit",
            origin="https://app.test",
            settings=c.settings,
        )

    if revoke:
        with pytest.raises(HTTPException):
            issue()
        c.token_version += 1
    result = issue()
    assert "sig=" in result.url and "comp=block" in result.url and result.cursor.offset == 0
    assert issue() == result and len(calls) == 1


def test_shared_cursor_mutation_during_constructor_cannot_change_committed_request(
    instruction_case,
):
    c = instruction_case
    c.on_construction = lambda: setattr(c.cursor, "length", 1)
    result = c.issue()
    assert result.cursor.length == 6 and c.i_calls[0][1].length == 6 and c.cursor.length == 1


def test_unit_checksum_normalization_preserves_original_request_and_capability(instruction_case):
    c = instruction_case
    c.cursor.checksum_sha256 = "A" * 64
    result = c.issue()
    assert result.cursor.checksum_sha256 == "a" * 64 and c.i_calls[0][1].checksum_sha256 == "a" * 64
    assert c.issue() == result and len(c.i_calls) == 1
