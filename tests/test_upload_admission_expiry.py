"""Retire unpublished authority without claiming native physical absence."""

from datetime import UTC, datetime, timedelta

import pytest
from automl_api import workflow_reconciler as worker
from automl_api.models.datasets import DatasetUploadSession
from automl_api.services import upload_admission as admission
from automl_api.services import uploads
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_workflow import admission_case as admission_case


def test_actual_worker_expires_unknown_admission_without_faking_native_absence(
    admission_case, monkeypatch
):
    c = admission_case
    c.native_error = OSError("Native session creation reply lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        expiry = db.get(DatasetUploadSession, c.upload_id).expires_at

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = expiry + timedelta(seconds=1)
            return value if tz else value.replace(tzinfo=None)

    monkeypatch.setattr(worker, "datetime", Clock)
    assert worker.cleanup_uploads_once(c.factory) == 1
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        record = upload.provider_state[admission.RECORD]
        assert upload.status == "expired" and record["phase"] == "retired"
        assert record["retirement"]["native_absence_confirmed"] is False
    assert c.store.aborted == 0 and len(c.calls) == 1


def _clock(monkeypatch, module, moment):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(module, "datetime", Clock)


def _unknown(c):
    c.native_error = OSError("creation acknowledgement lost")
    with pytest.raises(OSError):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        return row.expires_at


@pytest.mark.parametrize(
    "driver,protocol,scheme",
    [
        ("s3_compatible", "multipart", "s3c://bucket"),
        ("aws_s3", "multipart", "s3://bucket"),
        ("gcs", "resumable_offset", "gs://bucket"),
        ("azure_blob", "block_list", "az://account/bucket"),
    ],
)
def test_unknown_expiry_is_metadata_only_for_every_provider(
    admission_case, driver, protocol, scheme
):
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from automl_api.storage.contracts import UploadCapabilities

    c = admission_case
    c.store.driver_name = driver
    c.store.capabilities = lambda: UploadCapabilities(protocol, 1, False, False, False, False)
    c.store.uri_for_key = lambda key: f"{scheme}/{key}"
    expiry = _unknown(c)
    now = expiry + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now) == 1
    assert expire_pending_admissions(c.factory, now=now) == 0
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        record = row.provider_state[admission.RECORD]
        assert row.status == "expired" and row.multipart_upload_id == ""
        assert record["retirement"]["reason"] == "session_expired"
        assert record["retirement"]["native_absence_confirmed"] is False
    assert c.constructions == len(c.calls) == 1 and c.store.aborted == 0


def test_expiry_releases_quota_for_a_new_key_but_never_reissues_old_key(admission_case):
    import uuid
    from dataclasses import replace

    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from automl_api.storage.contracts import UploadContractError

    c = admission_case
    c.settings = replace(c.settings, project_storage_quota_bytes=10)
    expiry = _unknown(c)
    original = c.upload_id
    assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 1
    with pytest.raises(UploadContractError, match="expired"):
        c.begin()
    c.native_error = None
    key = uuid.uuid4().hex
    c.upload_id = uuid.uuid5(c.project_id, f"dataset.upload.begin:{c.actor_id}:{key}")
    assert c.begin(key=key).id == c.upload_id and c.upload_id != original
    assert len(c.calls) == 2
    with c.factory() as db:
        assert db.get(DatasetUploadSession, original).status == "expired"


def test_live_attempt_and_nonexpired_unknown_keep_their_reservation(admission_case):
    from automl_api.services.upload_admission_expiry import expire_pending_admissions

    c = admission_case

    def inspect():
        assert expire_pending_admissions(c.factory) == 0
        with c.factory() as db:
            assert db.get(DatasetUploadSession, c.upload_id).status == "initiated"

    c.callback = inspect
    assert c.begin().status == "uploading"
    with c.factory() as db:
        expiry = db.get(DatasetUploadSession, c.upload_id).expires_at
    assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 0


def test_known_reply_can_expire_after_actor_revocation_without_erasing_it(admission_case):
    from automl_api.models.iam import User
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from fastapi import HTTPException

    c = admission_case

    def revoke():
        with c.factory() as db, db.begin():
            db.get(User, c.actor_id).is_active = False

    c.callback = revoke
    with pytest.raises(HTTPException):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        receipt = row.provider_state[admission.RECORD]["receipt"]
        expiry = row.expires_at
    assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[admission.RECORD]["receipt"] == receipt
        assert row.status == "expired"
    assert len(c.calls) == 1 and c.store.aborted == 0


def test_late_reply_after_expiry_is_retained_without_reopening_admission(admission_case):
    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from automl_api.storage.contracts import UploadContractError
    from sqlalchemy import select

    c = admission_case

    def expire():
        with c.factory() as db:
            expiry = db.get(DatasetUploadSession, c.upload_id).expires_at
        assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 1

    c.callback = expire
    with pytest.raises(UploadContractError, match="lost its native attempt"):
        c.begin()
    with c.factory() as db:
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        row, record, result = admission._journal(db, command)
        assert row.status == "expired" and record["phase"] == "retired" and result is None
        assert command.status == CommandStatus.FAILED
        assert len(command.response_payload["superseded_admission_receipts"]) == 1
    assert len(c.calls) == 1 and c.store.aborted == 0


def test_exhausted_retry_budget_expires_after_lease_before_session_deadline(
    admission_case, monkeypatch
):
    from dataclasses import replace

    from automl_api.services.upload_admission_expiry import expire_pending_admissions

    c = admission_case
    c.settings = replace(c.settings, upload_session_ttl_seconds=3600)
    start = datetime.now(UTC)
    for index in range(5):
        _clock(monkeypatch, admission, start + timedelta(minutes=index * 6))
        expiry = _unknown(c)
    with c.factory() as db:
        record = db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD]
        lease = datetime.fromisoformat(record["attempt"]["expires_at"])
        assert record["attempt"]["number"] == 5 and lease < expiry
    assert expire_pending_admissions(c.factory, now=lease - timedelta(seconds=1)) == 0
    assert expire_pending_admissions(c.factory, now=lease + timedelta(seconds=1)) == 1
    with c.factory() as db:
        record = db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD]
        assert record["retirement"]["reason"] == "native_retry_budget_exhausted"
    assert len(c.calls) == 5


@pytest.mark.parametrize(
    "field",
    [
        "native-id",
        "completed-uri",
        "bytes",
        "instructions",
        "receipts",
        "completion-marker",
        "receipt-marker",
        "source",
        "command",
        "child-command",
    ],
)
def test_uncertain_transfer_history_cannot_be_declared_unpublished(admission_case, field):
    import copy
    import uuid

    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from automl_api.services.workflow_state import begin_command, transition_command
    from sqlalchemy import select

    c = admission_case
    expiry = _unknown(c)
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        state = copy.deepcopy(upload.provider_state)
        if field == "native-id":
            upload.multipart_upload_id = "unqualified"
        elif field == "completed-uri":
            upload.completed_object_uri = "s3c://bucket/unqualified"
        elif field == "bytes":
            upload.confirmed_bytes = 1
        elif field == "instructions":
            upload.instruction_state = {"1": {"instruction_id": "issued"}}
        elif field == "receipts":
            upload.transfer_receipts = [{"unit_number": 1}]
        elif field == "completion-marker":
            state["completion_protocol"] = uploads.UPLOAD_COMPLETION_PROTOCOL
        elif field == "receipt-marker":
            state[uploads.UPLOAD_VERIFICATION_RECORD] = {}
        elif field == "source":
            upload.original_filename = "changed.csv"
        elif field == "command":
            command = db.scalar(
                select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
            )
            command.request_hash = "corrupt"
        else:
            child, _ = begin_command(
                db,
                project_id=c.project_id,
                actor_id=c.actor_id,
                operation="dataset.upload.instruction",
                idempotency_key=uuid.uuid4().hex,
                payload={"session_id": str(c.upload_id)},
            )
            child.resource_id = c.upload_id
            transition_command(child, CommandStatus.RUNNING)
        upload.provider_state = state
    now = expiry + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now) == 0
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "initiated" and upload.lease_expires_at > now
        assert upload.provider_state[admission.RECORD]["phase"] == "unknown"
    assert c.store.aborted == 0 and len(c.calls) == 1


def test_expired_admission_keeps_project_deletion_blocked_for_native_cleanup(admission_case):
    from automl_api.models.iam import User
    from automl_api.services.deletion import request_deletion
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from fastapi import HTTPException

    c = admission_case
    expiry = _unknown(c)
    assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 1
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409 and "upload cleanup" in str(held.value.detail)


def test_retirement_proof_cannot_be_rolled_back_to_issuance(admission_case):
    import copy

    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.upload_admission_expiry import expire_pending_admissions
    from automl_api.storage.contracts import UploadContractError
    from sqlalchemy import select

    c = admission_case
    expiry = _unknown(c)
    assert expire_pending_admissions(c.factory, now=expiry + timedelta(seconds=1)) == 1
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        command = db.scalar(
            select(WorkflowCommand).where(WorkflowCommand.resource_id == c.upload_id)
        )
        state = copy.deepcopy(row.provider_state)
        state[admission.RECORD]["phase"] = "unknown"
        state[admission.RECORD].pop("retirement")
        row.status = "initiated"
        row.provider_state = state
        command.status = CommandStatus.RUNNING
    with c.factory() as db, pytest.raises(UploadContractError):
        admission._journal(db, db.get(WorkflowCommand, command.id))
    assert len(c.calls) == 1


def test_corrupt_expiry_record_does_not_starve_an_eligible_neighbor(admission_case):
    import uuid

    from automl_api.services.upload_admission_expiry import expire_pending_admissions

    c = admission_case
    first_expiry = _unknown(c)
    first = c.upload_id
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, first)
        filename = row.original_filename
        row.original_filename = "corrupt.csv"
    c.key = uuid.uuid4().hex
    c.upload_id = uuid.uuid5(c.project_id, f"dataset.upload.begin:{c.actor_id}:{c.key}")
    second_expiry = _unknown(c)
    now = max(first_expiry, second_expiry) + timedelta(seconds=1)
    assert expire_pending_admissions(c.factory, now=now, limit=1) == 0
    assert expire_pending_admissions(c.factory, now=now, limit=1) == 1
    with c.factory() as db, db.begin():
        assert db.get(DatasetUploadSession, c.upload_id).status == "expired"
        row = db.get(DatasetUploadSession, first)
        assert row.status == "initiated"
        row.original_filename = filename
    assert expire_pending_admissions(c.factory, now=now + timedelta(minutes=2), limit=1) == 1
    assert len(c.calls) == 2 and c.store.aborted == 0


def test_two_real_cleanup_workers_retire_one_admission_once(admission_case, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from automl_api.services import upload_admission_expiry as expiry

    c = admission_case
    deadline = _unknown(c) + timedelta(seconds=1)
    barrier = Barrier(2)
    original = expiry._expire

    def overlapping(*args):
        barrier.wait(timeout=10)
        return original(*args)

    monkeypatch.setattr(expiry, "_expire", overlapping)
    with ThreadPoolExecutor(max_workers=2) as pool:
        values = list(
            pool.map(lambda _: expiry.expire_pending_admissions(c.factory, now=deadline), range(2))
        )
    assert sorted(values) == [0, 1]
    assert len(c.calls) == 1 and c.store.aborted == 0


def test_expiry_preserves_hold_and_retention_metadata(admission_case):
    from automl_api.services.upload_admission_expiry import expire_pending_admissions

    c = admission_case
    c.payload.legal_hold = True
    c.payload.retention_days = 2
    deadline = _unknown(c) + timedelta(seconds=1)
    with c.factory() as db:
        retention = db.get(DatasetUploadSession, c.upload_id).retention_until
    assert retention > deadline
    assert expire_pending_admissions(c.factory, now=deadline) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "expired" and row.legal_hold and row.retention_until == retention
        assert (
            row.provider_state[admission.RECORD]["retirement"]["native_absence_confirmed"] is False
        )
    assert c.store.aborted == 0


def test_failed_late_creation_cannot_overwrite_terminal_expiry(admission_case):
    from automl_api.services.upload_admission_expiry import expire_pending_admissions

    c = admission_case
    c.native_error = OSError("late provider failure")

    def retire():
        with c.factory() as db:
            deadline = db.get(DatasetUploadSession, c.upload_id).expires_at + timedelta(seconds=1)
        assert expire_pending_admissions(c.factory, now=deadline) == 1

    c.callback = retire
    with pytest.raises(OSError, match="late provider failure"):
        c.begin()
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "expired" and row.provider_state[admission.RECORD]["phase"] == "retired"
        )
    assert c.store.aborted == 0


def test_pending_transfer_routes_reject_before_creating_instruction_command(admission_case):
    from automl_api.models.workflows import WorkflowCommand
    from fastapi.testclient import TestClient
    from sqlalchemy import select

    c = admission_case
    _unknown(c)
    with TestClient(c.app) as client:
        progress = client.get(f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/progress")
        instruction = client.post(
            f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/instructions",
            headers={"Origin": "https://app.test", "Idempotency-Key": "instruction"},
            json={"cursor": {"unit_number": 1, "offset": 0, "length": 6}},
        )
    assert progress.status_code == instruction.status_code == 409
    with c.factory() as db:
        commands = list(
            db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == c.project_id))
        )
        assert len(commands) == 1 and commands[0].operation == admission.OPERATION
    assert c.constructions == len(c.calls) == 1
