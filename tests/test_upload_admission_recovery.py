"""Background publication resumes only original committed admission authority."""

import pytest
from automl_api import workflow_reconciler as worker
from automl_api.models.datasets import DatasetUploadSession
from automl_api.services import upload_admission as admission
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_s3_sdk_retry_ownership import storage_http as storage_http
from test_upload_admission_workflow import admission_case as admission_case


def _recorded(c, monkeypatch):
    save = admission._save

    def fail(upload, record):
        save(upload, record)
        if record["phase"] == "published":
            raise OSError("Admission publication transaction failed")

    monkeypatch.setattr(admission, "_save", fail)
    with pytest.raises(OSError):
        c.begin()
    monkeypatch.setattr(admission, "_save", save)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "initiated"
            and row.provider_state[admission.RECORD]["phase"] == "recorded"
        )


def test_actual_worker_publishes_original_known_admission_without_native_creation(
    admission_case, monkeypatch
):
    c = admission_case
    _recorded(c, monkeypatch)
    assert worker.cleanup_uploads_once(c.factory) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "uploading"
            and row.provider_state[admission.RECORD]["phase"] == "published"
        )
    assert c.constructions == len(c.calls) == 1


def _clock(c, monkeypatch, *, minutes=6):
    from datetime import UTC, datetime, timedelta

    moment = admission.datetime.now(UTC) + timedelta(minutes=minutes)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return moment if tz else moment.replace(tzinfo=None)

    monkeypatch.setattr(admission, "datetime", Clock)
    monkeypatch.setattr(worker, "datetime", Clock)
    return moment


def _edit(c, change):
    import copy

    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import select

    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.resource_id == c.upload_id,
                WorkflowCommand.operation == admission.OPERATION,
            )
        )
        record = copy.deepcopy(row.provider_state[admission.RECORD])
        change(db, row, command, record)
        admission._save(row, record)


@pytest.mark.parametrize(
    "change",
    [
        "inactive",
        "token",
        "membership",
        "deletion",
        "source",
        "request",
        "attempt",
        "receipt",
        "store",
        "instructions",
    ],
)
def test_recovery_requires_original_authority_and_fresh_publication(
    admission_case, monkeypatch, change
):
    from dataclasses import replace

    from automl_api.models.iam import User
    from automl_api.models.projects import Project, ProjectMembership
    from automl_api.services.upload_admission_recovery import recover_pending_admissions
    from sqlalchemy import delete

    c = admission_case
    _recorded(c, monkeypatch)

    def mutate(db, row, command, record):
        if change == "inactive":
            db.get(User, c.actor_id).is_active = False
        elif change == "token":
            db.get(User, c.actor_id).token_version += 1
        elif change == "membership":
            db.execute(
                delete(ProjectMembership).where(ProjectMembership.project_id == c.project_id)
            )
        elif change == "deletion":
            db.get(Project, c.project_id).settings = {"deletion_in_progress": True}
        elif change == "source":
            row.original_filename = "changed.csv"
        elif change == "request":
            command.request_payload = {**command.request_payload, "origin": "https://foreign.test"}
        elif change == "attempt":
            record["attempt"]["actor_token_version"] += 1
        elif change == "receipt":
            record["receipt"]["provider_id"] = "foreign"
        elif change == "instructions":
            row.instruction_state = {"issued": True}

    _edit(c, mutate)
    if change == "store":
        monkeypatch.setattr(
            admission, "get_settings", lambda: replace(c.settings, object_store_bucket="foreign")
        )
    assert recover_pending_admissions(c.factory) == 0
    assert c.constructions == len(c.calls) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "initiated" and row.multipart_upload_id == ""
        assert row.lease_expires_at is not None


@pytest.mark.parametrize("snapshot", [None, True, -1, "0"])
def test_legacy_or_invalid_authentication_snapshot_never_guesses_current_token(
    admission_case, monkeypatch, snapshot
):
    from automl_api.services.upload_admission_recovery import recover_pending_admissions
    from automl_api.services.workflow_state import canonical_request_hash as digest

    c = admission_case
    _recorded(c, monkeypatch)

    def mutate(db, row, command, record):
        if snapshot is None:
            record["attempt"].pop("actor_token_version")
        else:
            record["attempt"]["actor_token_version"] = snapshot
        command.response_payload = {
            **command.response_payload,
            "admission_attempt_digest": digest(record["attempt"]),
        }

    _edit(c, mutate)
    assert recover_pending_admissions(c.factory) == 0
    assert len(c.calls) == c.constructions == 1
    if snapshot is None:
        assert c.begin().status == "uploading"
        assert len(c.calls) == c.constructions == 1


def test_unknown_live_attempt_is_not_stolen_and_expired_attempt_uses_original_request(
    admission_case, monkeypatch
):
    from automl_api.services.upload_admission_recovery import recover_pending_admissions

    c = admission_case
    c.native_error = OSError("creation reply lost")
    with pytest.raises(OSError):
        c.begin()
    assert recover_pending_admissions(c.factory) == 0
    assert len(c.calls) == 1
    c.native_error = None
    _clock(c, monkeypatch)
    assert recover_pending_admissions(c.factory) == 1
    assert c.calls[0] == c.calls[1]
    assert recover_pending_admissions(c.factory) == 0
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.provider_state[admission.RECORD]["attempt"]["number"] == 2
        assert row.status == "uploading"


def test_unknown_creation_exhausts_five_attempts_then_expires_without_claiming_absence(
    admission_case, monkeypatch
):
    from automl_api.services.upload_admission_recovery import recover_pending_admissions

    c = admission_case
    c.native_error = OSError("creation acknowledgement lost")
    with pytest.raises(OSError):
        c.begin()
    for number in range(2, 6):
        _clock(c, monkeypatch)
        assert recover_pending_admissions(c.factory) == 0
        assert len(c.calls) == number
    _clock(c, monkeypatch)
    assert worker.cleanup_uploads_once(c.factory) == 1
    assert len(c.calls) == 5
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "expired"
        assert (
            row.provider_state[admission.RECORD]["retirement"]["native_absence_confirmed"] is False
        )


@pytest.mark.parametrize(
    "driver,protocol,scheme",
    [
        ("gcs", "resumable_offset", "gs://bucket"),
        ("azure_blob", "block_list", "az://account/bucket"),
    ],
)
def test_unknown_opaque_provider_creation_is_held_without_another_creation(
    admission_case, monkeypatch, driver, protocol, scheme
):
    from automl_api.services.upload_admission_recovery import recover_pending_admissions
    from automl_api.storage.contracts import UploadCapabilities

    c = admission_case
    c.store.driver_name = driver
    c.store.capabilities = lambda: UploadCapabilities(protocol, 1, False, False, False, False)
    c.store.uri_for_key = lambda key: f"{scheme}/{key}"
    c.native_error = OSError("opaque creation response lost")
    with pytest.raises(OSError):
        c.begin()
    _clock(c, monkeypatch)
    assert recover_pending_admissions(c.factory) == 0
    assert len(c.calls) == c.constructions == 1


@pytest.mark.parametrize("fault", ["503", "disconnect"])
def test_actual_sdk_recovers_lost_creation_reply_without_second_native_creation(
    admission_case, storage_http, monkeypatch, fault
):
    from dataclasses import replace

    from automl_api.services.upload_admission_recovery import recover_pending_admissions
    from automl_api.storage.contracts import UploadThrottled
    from botocore.exceptions import BotoCoreError, ClientError
    from test_s3_sdk_retry_ownership import driver

    c = admission_case
    native = storage_http
    c.store = driver(native, True)
    c.settings = replace(
        c.settings,
        object_store_type="s3_compatible",
        object_store_endpoint=native.endpoint,
        object_store_bucket="bucket",
        object_store_region="us-east-1",
    )
    c.payload = c.payload.model_copy(update={"data_region": "us-east-1"})
    monkeypatch.setattr(admission, "get_settings", lambda: c.settings)
    native.fault = fault
    try:
        with pytest.raises((BotoCoreError, ClientError, UploadThrottled)):
            c.begin()
        assert len(native.uploads) == 1
        assert recover_pending_admissions(c.factory) == 0
        _clock(c, monkeypatch)
        assert recover_pending_admissions(c.factory) == 1
        assert len(native.uploads) == 1 and len(native.listings) == 2
        with c.factory() as db:
            row = db.get(DatasetUploadSession, c.upload_id)
            assert row.multipart_upload_id == native.uploads[0][1]
            assert row.object_key == native.uploads[0][0]
            assert row.status == "uploading"
    finally:
        c.store.client.close()
        c.store.presign_client.close()


def test_corrupt_oldest_row_does_not_starve_next_bounded_batch(admission_case, monkeypatch):
    import uuid

    from automl_api.services.upload_admission_recovery import recover_pending_admissions

    c = admission_case
    _recorded(c, monkeypatch)
    first = c.upload_id
    _edit(c, lambda db, row, command, record: record["attempt"].update(actor_token_version=999))
    c.key = uuid.uuid4().hex
    c.upload_id = uuid.uuid5(c.project_id, f"{admission.OPERATION}:{c.actor_id}:{c.key}")
    _recorded(c, monkeypatch)
    assert recover_pending_admissions(c.factory, limit=1) == 0
    assert recover_pending_admissions(c.factory, limit=1) == 1
    with c.factory() as db:
        assert db.get(DatasetUploadSession, first).status == "initiated"
        assert db.get(DatasetUploadSession, c.upload_id).status == "uploading"
    assert c.constructions == len(c.calls) == 2


def test_recovery_error_does_not_shorten_a_live_native_lease(admission_case, monkeypatch):
    from datetime import timedelta

    from automl_api.models.iam import User
    from automl_api.services.upload_admission_recovery import recover_pending_admissions

    c = admission_case
    c.native_error = OSError("lost reply")
    with pytest.raises(OSError):
        c.begin()
    now = _clock(c, monkeypatch)

    # The new owner commits its lease, then loses its reply: backoff must retain that lease.
    def inspect():
        assert c.engine.pool.checkedout() == 0
        with c.factory() as db:
            assert db.get(User, c.actor_id).token_version == c.token_version

    c.callback = inspect
    assert recover_pending_admissions(c.factory) == 0
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).lease_expires_at == now + timedelta(
            minutes=5
        )
    assert recover_pending_admissions(c.factory) == 0 and len(c.calls) == 2


def test_competing_background_owners_do_not_steal_committed_creation_attempt(
    admission_case, monkeypatch
):
    from automl_api.services.upload_admission_recovery import recover_pending_admissions

    c = admission_case
    c.native_error = OSError("creation reply lost")
    with pytest.raises(OSError):
        c.begin()
    c.native_error = None
    _clock(c, monkeypatch)
    competing = []
    c.callback = lambda: competing.append(recover_pending_admissions(c.factory))
    assert recover_pending_admissions(c.factory) == 1
    assert competing == [0] and len(c.calls) == 2


def test_recorded_recovery_after_expiry_is_retired_by_actual_worker(admission_case, monkeypatch):
    c = admission_case
    _recorded(c, monkeypatch)
    from datetime import UTC, datetime

    with c.factory() as db:
        expiry = db.get(DatasetUploadSession, c.upload_id).expires_at
    minutes = int((expiry - datetime.now(UTC)).total_seconds() / 60) + 1
    _clock(c, monkeypatch, minutes=minutes)
    assert worker.cleanup_uploads_once(c.factory) == 2
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.status == "expired" and row.multipart_upload_id == ""
        assert row.provider_state[admission.RECORD]["phase"] == "retired"
    assert len(c.calls) == 1
