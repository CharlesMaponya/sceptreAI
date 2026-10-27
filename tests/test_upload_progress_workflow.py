"""Actual progress observation must leave SQL before provider work."""

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.services import uploads
from automl_api.storage.contracts import TransferReceipt, UploadContractError, UploadProgress
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import delete, select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_upload_admission_workflow import admission_case as admission_case


def test_actual_progress_route_releases_sql_for_constructor_and_native_read(
    admission_case, monkeypatch
):
    c = admission_case
    c.begin()
    original = c.store.query_progress
    observed = []

    def query(upload):
        assert c.engine.pool.checkedout() == 0
        observed.append(upload.provider_id)
        return original(upload)

    monkeypatch.setattr(c.store, "query_progress", query)
    response = TestClient(c.app).get(
        f"/projects/{c.project_id}/datasets/uploads/{c.upload_id}/progress"
    )
    assert response.status_code == 200 and response.json()["confirmed_bytes"] == 0
    assert observed == ["provider-1"]


def _observe(c):
    return uploads.observe_upload_progress(
        c.factory, c.actor_id, c.token_version, c.project_id, c.upload_id, settings=c.settings
    )


def _partial():
    return UploadProgress(6, 10, (TransferReceipt(1, 0, 6, "original"),), False)


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
            row.object_key += "-changed"
        elif change == "hold":
            row.legal_hold = True
        elif change == "completion":
            row.provider_state = {**row.provider_state, uploads.UPLOAD_COMPLETION_STARTED: True}
        elif change == "completion_command":
            from automl_api.models.enums import CommandStatus
            from automl_api.services.workflow_state import begin_command, transition_command

            command, _ = begin_command(
                db,
                project_id=c.project_id,
                actor_id=c.actor_id,
                operation=uploads.UPLOAD_COMPLETION_PROTOCOL,
                idempotency_key="missing-history",
                payload={},
            )
            command.resource_id, command.resource_type = (
                c.upload_id,
                uploads.UPLOAD_COMPLETION_PROTOCOL,
            )
            transition_command(command, CommandStatus.RUNNING)
        elif change == "retirement":
            row.status = "retiring"
        elif change == "projection":
            row.confirmed_bytes = 6
            row.transfer_receipts = [uploads.receipt_to_dict(_partial().receipts[0])]
        elif change == "terminal":
            row.status = "failed"
        elif change == "instructions":
            row.instruction_state = {"1": {"instruction_id": "new-capability"}}
        elif change == "anchor":
            from automl_api.models.workflows import WorkflowCommand

            command = db.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.resource_id == c.upload_id,
                    WorkflowCommand.operation == "dataset.upload.begin",
                )
            )
            command.response_payload = {
                **command.response_payload,
                "admission_intent_digest": "foreign",
            }


@pytest.mark.parametrize(
    "change",
    [
        "actor",
        "token",
        "membership",
        "project",
        "source",
        "completion",
        "completion_command",
        "retirement",
        "anchor",
    ],
)
def test_progress_rejects_stale_authority_before_sdk_construction(admission_case, change):
    c = admission_case
    c.begin()
    _change(c, change)
    with pytest.raises((HTTPException, UploadContractError)):
        _observe(c)
    assert c.constructions == 1


@pytest.mark.parametrize(
    "change",
    [
        "actor",
        "token",
        "membership",
        "project",
        "source",
        "completion",
        "completion_command",
        "retirement",
        "projection",
        "terminal",
        "instructions",
        "hold",
        "anchor",
    ],
)
def test_progress_does_not_publish_after_concurrent_commit(admission_case, monkeypatch, change):
    c = admission_case
    c.begin()

    def query(upload):
        assert c.engine.pool.checkedout() == 0
        _change(c, change)
        return _partial()

    monkeypatch.setattr(c.store, "query_progress", query)
    with pytest.raises((HTTPException, UploadContractError)):
        _observe(c)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.confirmed_bytes == (6 if change == "projection" else 0)
        assert row.transfer_receipts == (
            [uploads.receipt_to_dict(_partial().receipts[0])] if change == "projection" else []
        )


def test_constructor_commit_is_revalidated_before_provider_query(admission_case, monkeypatch):
    c = admission_case
    c.begin()
    queried = []
    c.on_construction = lambda: _change(c, "token")
    monkeypatch.setattr(c.store, "query_progress", lambda upload: queried.append(upload))
    with pytest.raises(HTTPException) as error:
        _observe(c)
    assert error.value.status_code == 401 and queried == []


def test_current_progress_publishes_exact_receipts_without_digest_verification(admission_case):
    c = admission_case
    c.begin()
    c.store.progress = UploadProgress(
        10, 10, (TransferReceipt(1, 0, 6, "one"), TransferReceipt(2, 6, 4, "two")), True
    )
    result = _observe(c)
    assert result.complete and result.status == "object_completed"
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.confirmed_bytes == 10 and row.completed_object_uri is None
        assert row.observed_object_digest is None and row.checksum_verified_at is None
        assert [r["etag"] for r in row.transfer_receipts] == ["one", "two"]


@pytest.mark.parametrize("status", ["ready", "aborted", "expired", "failed", "quarantined"])
def test_terminal_progress_is_fresh_cached_state_without_sdk(admission_case, status):
    c = admission_case
    c.begin()
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        row.status = status
        if status == "ready":
            row.confirmed_bytes = row.byte_size
    result = _observe(c)
    assert result.status == status and result.complete == (status == "ready")
    assert c.constructions == 1


@pytest.mark.parametrize(
    "progress",
    [
        None,
        UploadProgress(True, 10, (), False),
        UploadProgress(-1, 10, (), False),
        UploadProgress(11, 10, (), True),
        UploadProgress(0, 11, (), False),
        UploadProgress(0, 10, (), True),
        UploadProgress(6, 10, (), False),
        UploadProgress(6, 10, (None,), False),
        UploadProgress(6, 10, (TransferReceipt(1, 1, 6),), False),
        UploadProgress(6, 10, (TransferReceipt(1, 0, 5),), False),
        UploadProgress(6, 10, (TransferReceipt(True, 0, 6),), False),
        UploadProgress(6, 10, (TransferReceipt(3, 12, 6),), False),
        UploadProgress(10, 10, (TransferReceipt(1, 0, 6), TransferReceipt(1, 0, 6)), True),
    ],
)
def test_malformed_provider_progress_cannot_change_projection(admission_case, progress):
    c = admission_case
    c.begin()
    c.store.progress = progress
    with pytest.raises(UploadContractError):
        _observe(c)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.status == "uploading" and row.confirmed_bytes == 0 and row.transfer_receipts == []
        )


def test_late_older_progress_cannot_overwrite_a_committed_newer_observation(
    admission_case, monkeypatch
):
    c = admission_case
    c.begin()
    observations = []

    def query(upload):
        assert c.engine.pool.checkedout() == 0
        if not observations:
            observations.append("outer")
            assert _observe(c).confirmed_bytes == 6
            return UploadProgress(0, 10, (), False)
        return _partial()

    monkeypatch.setattr(c.store, "query_progress", query)
    with pytest.raises(UploadContractError, match="changed during"):
        _observe(c)
    with c.factory() as db:
        assert db.get(DatasetUploadSession, c.upload_id).confirmed_bytes == 6


def test_progress_errors_and_publication_rollback_preserve_existing_projection(
    admission_case, monkeypatch
):
    c = admission_case
    c.begin()

    def fail(upload):
        assert c.engine.pool.checkedout() == 0
        raise OSError("provider read failed")

    monkeypatch.setattr(c.store, "query_progress", fail)
    with pytest.raises(OSError):
        _observe(c)
    monkeypatch.setattr(c.store, "query_progress", lambda upload: _partial())
    read = uploads.progress_read

    def rollback(upload, *, complete):
        read(upload, complete=complete)
        raise OSError("projection publication failed")

    monkeypatch.setattr(uploads, "progress_read", rollback)
    with pytest.raises(OSError):
        _observe(c)
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert row.confirmed_bytes == 0 and row.transfer_receipts == []


@pytest.mark.parametrize("confirmed,complete", [(4, False), (10, True)])
def test_offset_progress_keeps_native_offsets_without_synthetic_receipts(
    admission_case, confirmed, complete
):
    from automl_api.storage.contracts import UploadCapabilities

    c = admission_case
    c.store.driver_name = "gcs"
    c.store.capabilities = lambda: UploadCapabilities(
        "resumable_offset", 1, False, False, False, False
    )
    c.store.uri_for_key = lambda key: "gs://bucket/" + key
    c.begin()
    c.store.progress = UploadProgress(confirmed, 10, (), complete)
    result = _observe(c)
    assert (
        result.confirmed_bytes == confirmed
        and result.complete == complete
        and result.receipts == []
    )
    assert (result.next_cursor.offset if result.next_cursor else None) == (
        4 if confirmed == 4 else None
    )


def test_sdk_progress_reads_1001_parts_over_real_http_without_sql_or_large_data(
    admission_case, monkeypatch
):
    import os
    from dataclasses import replace
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit
    from xml.sax.saxutils import escape

    from automl_api.storage.s3 import S3ObjectStoreDriver

    c = admission_case
    for key in list(os.environ):
        if key.lower().endswith("_proxy"):
            monkeypatch.delenv(key)
    queries = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert c.engine.pool.checkedout() == 0
            query = parse_qs(urlsplit(self.path).query)
            assert query["uploadId"] == ["provider-1"] and query["max-parts"] == ["1000"]
            marker = int(query.get("part-number-marker", [0])[0])
            queries.append(marker)
            first, last = (1, 1000) if marker == 0 else (1001, 1001)
            key = urlsplit(self.path).path.removeprefix("/bucket/")
            entries = "".join(
                f'<Part><PartNumber>{number}</PartNumber><Size>6</Size><ETag>"part-{number}"</ETag></Part>'
                for number in range(first, last + 1)
            )
            payload = (
                f"<ListPartsResult><Bucket>bucket</Bucket><Key>{escape(key)}</Key>"
                f"<UploadId>provider-1</UploadId><PartNumberMarker>{marker}</PartNumberMarker>"
                f"<NextPartNumberMarker>{last}</NextPartNumberMarker>"
                f"<IsTruncated>{str(last == 1000).lower()}</IsTruncated>"
                f"{entries}</ListPartsResult>"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoint = f"http://127.0.0.1:{server.server_port}"
    c.settings = replace(
        c.settings,
        object_store_endpoint=endpoint,
        object_store_bucket="bucket",
        max_upload_size_bytes=10000,
        project_storage_quota_bytes=10000,
    )
    c.payload = c.payload.model_copy(update={"byte_size": 6006})
    c.begin()
    c.store = S3ObjectStoreDriver(
        bucket="bucket",
        endpoint_url=endpoint,
        access_key="unit",
        secret_key="unit",
        compatible=True,
    )
    try:
        result = _observe(c)
        assert result.confirmed_bytes == 6006 and result.complete and len(result.receipts) == 1001
        assert queries == [0, 1000]
    finally:
        c.store.client.close()
        c.store.presign_client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_cached_ready_flag_alone_does_not_claim_confirmed_upload_bytes(admission_case):
    c = admission_case
    c.begin()
    with c.factory() as db, db.begin():
        db.get(DatasetUploadSession, c.upload_id).status = "ready"
    result = _observe(c)
    assert result.status == "ready" and not result.complete and result.confirmed_bytes == 0
    assert c.constructions == 1


def test_actual_postgres_backend_loss_rolls_back_progress_publication(admission_case):
    from sqlalchemy import event, text
    from sqlalchemy.exc import DBAPIError

    c = admission_case
    c.begin()
    c.store.progress = _partial()
    killed = []

    def kill(session):
        if any(
            isinstance(row, DatasetUploadSession) and row.confirmed_bytes == 6
            for row in session.dirty
        ):
            pid = session.scalar(text("SELECT pg_backend_pid()"))
            with c.engine.connect() as control:
                assert control.scalar(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
            killed.append(pid)

    event.listen(c.factory, "before_commit", kill)
    try:
        with pytest.raises(DBAPIError):
            _observe(c)
    finally:
        event.remove(c.factory, "before_commit", kill)
    assert len(killed) == 1
    with c.factory() as db:
        row = db.get(DatasetUploadSession, c.upload_id)
        assert (
            row.confirmed_bytes == 0 and row.status == "uploading" and row.transfer_receipts == []
        )
    assert _observe(c).confirmed_bytes == 6


def test_mismatched_actual_provider_target_is_rejected_before_query(admission_case, monkeypatch):
    c = admission_case
    c.begin()
    queries = []
    monkeypatch.setattr(c.store, "uri_for_key", lambda key: "s3c://foreign/" + key)
    monkeypatch.setattr(c.store, "query_progress", lambda upload: queries.append(upload))
    with pytest.raises(UploadContractError, match="target differs"):
        _observe(c)
    assert queries == []
