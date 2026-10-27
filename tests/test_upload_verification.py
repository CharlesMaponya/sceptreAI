"""The real completion/outbox/verifier chain reads original bytes outside SQL."""

import copy
import io
from datetime import UTC, datetime

import pytest
from automl_api.models.datasets import DatasetUploadSession, DatasetVersion
from automl_api.models.enums import DatasetStatus, OutboxStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import OutboxEntry
from automl_api.services import reconciler, upload_completion
from automl_api.services import upload_verification as verification
from automl_api.storage.contracts import ObjectMetadata
from automl_api.workflow_reconciler import run_once
from sqlalchemy import select
from test_phase2_upload_services import _settings
from test_upload_completion_workflow import committed_tenant as committed_tenant
from test_upload_completion_workflow import completion_case as completion_case
from test_upload_completion_workflow import phase1_engine as phase1_engine


@pytest.fixture
def verification_case(completion_case, monkeypatch, request):
    c = completion_case
    c.content = b"a,b\n1,2\n\n\n"
    c.metadata_size = len(c.content)
    c.metadata_version = "original-v1"
    c.io = []
    c.on_io = None
    c.closed = []
    c.settings = _settings()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.upload_kind == (
            "offline_scoring" if getattr(request, "param", None) == "offline" else "dataset"
        )
        if getattr(request, "param", None) == "retained":
            assert upload.retention_until > datetime.now(UTC)
    c.send()
    with c.factory() as db:
        c.entry_id = db.scalar(
            select(OutboxEntry.id).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.aggregate_id == c.upload_id
            )
        )

    def event(kind):
        held = c.engine.pool.checkedout()
        c.io.append((kind, held))
        assert held == 0
        if c.on_io:
            c.on_io(kind)

    def metadata(uri, *, version=None):
        event("stat")
        assert version == "original-v1"
        return ObjectMetadata(
            uri, c.metadata_size, provider_headers={"version_id": c.metadata_version}
        )

    def stream(uri, byte_range=None, *, version=None):
        event("open")
        assert version == "original-v1" and byte_range is not None
        c.io.append((f"range:{byte_range.start}:{byte_range.end_inclusive}:{version}", 0))

        class Stream(io.BytesIO):
            def read(self, size=-1):
                event("read")
                return super().read(size)

            def close(self):
                if not self.closed:
                    event("close")
                    c.closed.append(True)
                super().close()

        return Stream(c.content[byte_range.start : byte_range.end_inclusive + 1])

    def store(*_):
        event("store")
        return c.store

    monkeypatch.setattr(c.store, "stat", metadata)
    monkeypatch.setattr(c.store, "open_stream", stream, raising=False)
    monkeypatch.setattr(verification, "get_object_store", store)
    monkeypatch.setattr(verification, "get_settings", lambda: c.settings)
    monkeypatch.setattr(reconciler, "get_settings", lambda: _settings())
    monkeypatch.setattr(reconciler, "UPLOAD_VERIFICATION_RANGE_BYTES", 3)
    original = verification.inspect_and_scan_stream

    def scan(*args, **kwargs):
        event("scan")
        return original(*args, **kwargs)

    monkeypatch.setattr(verification, "inspect_and_scan_stream", scan)
    c.verify = lambda: run_once(c.factory, worker_id="owned-upload-verifier")
    return c


def test_actual_dispatch_reaches_ready_using_original_version_and_bounded_ranges(verification_case):
    c = verification_case
    assert c.verify() == 1
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        entry = db.get(OutboxEntry, c.entry_id)
        version = db.get(DatasetVersion, upload.dataset_version_id)
        assert upload.status == "ready" and upload.observed_object_digest == c.sha256
        assert upload.scanner_status == "allowed" and upload.lease_owner is None
        assert (
            version.status == DatasetStatus.READY
            and version.content_hash_verification_status == "verified"
        )
        assert (
            version.quality_report_json["native_upload_custody"]["object_version"] == "original-v1"
        )
        assert entry.status == OutboxStatus.DELIVERED
        assert upload.provider_state[verification.RECORD]["inspection"]["sha256"] == c.sha256
        upload_completion.journal(db, upload)
    assert [kind for kind, _ in c.io if kind.startswith("range:")] == [
        "range:0:2:original-v1",
        "range:3:5:original-v1",
        "range:6:8:original-v1",
        "range:9:9:original-v1",
    ]
    assert len(c.closed) == 4 and all(held == 0 for _, held in c.io)
    assert c.send().session.status == "object_completed"


@pytest.mark.parametrize("size", [9, 11])
def test_exact_original_version_size_mismatch_never_reads_or_readies(verification_case, size):
    c = verification_case
    c.metadata_size = size
    assert c.verify() == 1
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "quarantined" and "byte size mismatch" in upload.quarantine_reason
        assert db.get(DatasetVersion, upload.dataset_version_id).status == DatasetStatus.FAILED
    assert not any(kind in {"open", "read", "scan"} for kind, _ in c.io)


def test_foreign_metadata_version_cannot_begin_scan(verification_case):
    c = verification_case
    c.metadata_version = "replacement"
    c.verify()
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status != "ready"
    assert not any(kind in {"open", "read", "scan"} for kind, _ in c.io)


@pytest.mark.parametrize("kind", ["digest", "scanner"])
def test_original_version_corruption_or_denied_content_is_quarantined(verification_case, kind):
    c = verification_case
    c.content = b"a,b\n9,8\n\n\n" if kind == "digest" else b"MZ BAD!!!\n"
    assert len(c.content) == 10
    c.verify()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "quarantined"
        assert (
            "SHA-256" in upload.quarantine_reason
            if kind == "digest"
            else upload.scanner_status == "denied"
        )
    assert c.closed and all(held == 0 for _, held in c.io)


@pytest.mark.parametrize(
    "fault", ["source", "receipt", "project", "lease", "attempt", "version", "format", "policy"]
)
def test_source_or_delivery_changes_during_reads_cannot_publish_ready(verification_case, fault):
    c = verification_case
    changed = False

    def change(kind):
        nonlocal changed
        if kind != "read" or changed:
            return
        changed = True
        with c.factory() as db, db.begin():
            upload = db.get(DatasetUploadSession, c.upload_id)
            entry = db.get(OutboxEntry, c.entry_id)
            if fault == "source":
                upload.expected_object_digest = "b" * 64
            elif fault == "receipt":
                state = copy.deepcopy(upload.provider_state)
                state[upload_completion.RECORD]["receipt"]["object_version"] = "replacement"
                upload.provider_state = state
            elif fault == "project":
                p = db.get(Project, c.project_id)
                p.settings = {**p.settings, "deletion_in_progress": True}
            elif fault == "lease":
                entry.lease_owner = "another-worker"
            elif fault == "attempt":
                entry.delivery_attempts += 1
            elif fault == "format":
                from automl_api.models.enums import DatasetFormat

                db.get(DatasetVersion, upload.dataset_version_id).format = DatasetFormat.PARQUET
            elif fault == "policy":
                c.settings.upload_scanner_signature_version = "changed-policy"
            else:
                db.get(DatasetVersion, upload.dataset_version_id).object_uri = "s3://foreign/key"

    c.on_io = change
    c.verify()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status != "ready" and verification.RECORD not in upload.provider_state
        assert db.get(DatasetVersion, upload.dataset_version_id).status != DatasetStatus.READY
    assert changed and c.closed


def test_legacy_topic_cannot_infer_current_upload_verification(verification_case):
    c = verification_case
    with c.factory() as db, db.begin():
        db.get(OutboxEntry, c.entry_id).topic = "upload.reconcile"
    c.verify()
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "object_completed"
    assert c.io == []


@pytest.mark.parametrize("verification_case", ["retained"], indirect=True)
def test_original_retention_timestamp_survives_typed_publication_and_verification(
    verification_case,
):
    c = verification_case
    c.verify()
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).status == "ready"


def test_scanner_timeout_preserves_failed_scan_without_ready_or_object_deletion(
    verification_case, monkeypatch
):
    c = verification_case

    def timeout(*args, **kwargs):
        assert c.engine.pool.checkedout() == 0
        raise TimeoutError("Owned scanner timed out")

    monkeypatch.setattr(verification, "inspect_and_scan_stream", timeout)
    c.verify()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        assert upload.status == "quarantined" and upload.scanner_status == "failed"


def test_old_reader_failure_cannot_fail_a_new_delivery_attempt(verification_case):
    c = verification_case
    changed = False

    def fail(kind):
        nonlocal changed
        if kind != "read" or changed:
            return
        changed = True
        with c.factory() as db, db.begin():
            db.get(OutboxEntry, c.entry_id).delivery_attempts += 1
        raise OSError("Old native reader failed after another claim")

    c.on_io = fail
    c.verify()
    with c.factory() as db:
        entry = db.get(OutboxEntry, c.entry_id)
        assert entry.status == OutboxStatus.CLAIMED and entry.last_error is None
        assert db.get(DatasetUploadSession, c.upload_id).status != "ready"
    assert changed and c.closed


def test_original_delivered_verification_selects_pinned_descriptor(verification_case):
    c = verification_case
    c.verify()
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        selected = verification.verified_object(db, upload, c.settings)
        assert selected.uri == upload.completed_object_uri
        assert selected.version == "original-v1" and selected.driver == "s3_compatible"
        db.commit()
    assert c.engine.pool.checkedout() == 0


@pytest.mark.parametrize(
    "fault",
    [
        "receipt",
        "source",
        "uri",
        "proof",
        "anchor",
        "delivery",
        "command",
        "scanner",
        "policy",
        "dataset",
    ],
)
def test_verified_descriptor_refuses_changed_or_unqualified_custody(verification_case, fault):
    from dataclasses import replace

    from automl_api.models.enums import CommandStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services.workflow_state import StaleFence
    from automl_api.storage.contracts import UploadContractError

    c = verification_case
    c.verify()
    settings = c.settings
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        entry = db.get(OutboxEntry, c.entry_id)
        if fault == "receipt":
            state = copy.deepcopy(upload.provider_state)
            state[upload_completion.RECORD]["receipt"]["object_version"] = "replacement"
            upload.provider_state = state
        elif fault == "source":
            upload.original_filename = "changed.csv"
        elif fault == "uri":
            upload.completed_object_uri += ".replacement"
        elif fault == "proof":
            state = copy.deepcopy(upload.provider_state)
            state[verification.RECORD]["inspection"]["sha256"] = "a" * 64
            upload.provider_state = state
        elif fault == "anchor":
            entry.payload = {k: v for k, v in entry.payload.items() if k != "verification_digest"}
        elif fault == "delivery":
            entry.status = OutboxStatus.PENDING
        elif fault == "command":
            db.get(WorkflowCommand, entry.command_id).status = CommandStatus.FAILED
        elif fault == "scanner":
            upload.scanner_status = "pending"
        elif fault == "policy":
            settings = replace(settings, upload_scanner_signature_version="changed")
        else:
            db.get(DatasetVersion, upload.dataset_version_id).original_filename = "changed.csv"
    with c.factory() as db:
        with pytest.raises((StaleFence, UploadContractError)):
            verification.verified_object(db, db.get(DatasetUploadSession, c.upload_id), settings)


@pytest.mark.parametrize("completion_case", ["held-dataset"], indirect=True)
def test_releasing_hold_preserves_original_verified_byte_proof(verification_case):
    c = verification_case
    c.verify()
    with c.factory() as db, db.begin():
        upload = db.get(DatasetUploadSession, c.upload_id)
        version = db.get(DatasetVersion, upload.dataset_version_id)
        original = copy.deepcopy(upload.provider_state[verification.RECORD])
        assert upload.legal_hold and version.legal_hold
        upload.legal_hold = version.legal_hold = False
    with c.factory() as db:
        upload = db.get(DatasetUploadSession, c.upload_id)
        descriptor = verification.verified_object(db, upload, c.settings)
        assert descriptor.uri == upload.completed_object_uri
        assert upload.provider_state[verification.RECORD] == original
