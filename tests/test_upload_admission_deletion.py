"""Deletion requires original native cleanup custody before every side effect."""

import copy
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
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
from automl_api.services import deletion
from automl_api.services import upload_admission as admission
from automl_api.services import upload_admission_cleanup as cleanup
from automl_api.storage.contracts import DeletionPending
from fastapi import HTTPException
from sqlalchemy import delete, select
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_project_management_integration import reconcile_deletion_stage
from test_upload_admission_cleanup import _command, _retired_known
from test_upload_admission_workflow import admission_case as admission_case


@pytest.fixture
def deletion_case(admission_case):
    c = admission_case
    try:
        yield c
    finally:
        with c.factory() as db, db.begin():
            db.execute(delete(DeletionStage).where(DeletionStage.project_id == c.project_id))
            db.execute(
                delete(DeletionTombstone).where(DeletionTombstone.project_id == c.project_id)
            )


def _cleaned(c):
    now = _retired_known(c)
    assert cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db, db.begin():
        db.get(User, c.actor_id).is_active = True
    return now


def test_project_delete_accepts_authenticated_native_admission_cleanup(deletion_case):
    c = deletion_case
    _cleaned(c)
    constructions = c.constructions
    with c.factory() as db, db.begin():
        result = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        assert result["status"] == "pending"
        assert deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id) == result
    assert c.constructions == constructions and c.store.aborted == 1


def _corrupt(db, c, field):
    row = db.get(DatasetUploadSession, c.upload_id)
    state = copy.deepcopy(row.provider_state)
    command = _command(db, c)
    if field == "all_metadata":
        state = {}
    elif field == "all_metadata_aborted":
        state = {}
        row.status = "aborted"
    elif field == "missing_session":
        db.delete(row)
        return
    elif field == "missing":
        state.pop(cleanup.RECORD)
    elif field == "forged_flag":
        state[cleanup.RECORD] = {"phase": "published", "native_absence_confirmed": True}
    elif field == "receipt":
        state[cleanup.RECORD]["receipt"]["cancelled"] = False
    elif field == "parent_anchor":
        original = db.get(
            WorkflowCommand, uuid.UUID(state[admission.RECORD]["intent"]["command_id"])
        )
        original.response_payload = {
            **original.response_payload,
            "admission_retirement_digest": "changed",
        }
    elif field == "source":
        row.original_filename = "foreign.csv"
    elif field == "marker_erased":
        state.pop(cleanup.RECORD)
        state[admission.RECORD]["phase"] = "unknown"
    elif field == "missing_command":
        db.delete(command)
    row.provider_state = state


@pytest.mark.parametrize(
    "field",
    [
        "all_metadata",
        "all_metadata_aborted",
        "missing_session",
        "missing",
        "forged_flag",
        "receipt",
        "parent_anchor",
        "source",
        "marker_erased",
        "missing_command",
    ],
)
def test_deletion_request_requires_independent_original_cleanup_custody(deletion_case, field):
    c = deletion_case
    _cleaned(c)
    with c.factory() as db, db.begin():
        _corrupt(db, c, field)
    constructors = c.constructions
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409 and "upload cleanup" in str(held.value.detail)
    assert c.constructions == constructors and c.store.aborted == 1
    with c.factory() as db:
        assert (
            db.scalar(
                select(DeletionTombstone.id).where(DeletionTombstone.project_id == c.project_id)
            )
            is None
        )


def _queue(c):
    with c.factory() as db, db.begin():
        result = deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
        entry = db.scalar(
            select(OutboxEntry).where(
                OutboxEntry.project_id == c.project_id, OutboxEntry.topic == "project.delete"
            )
        )
        return uuid.UUID(result["id"]), entry.id


@pytest.mark.parametrize("stage", ["runtime", "object_store", "prepared_store", "metadata"])
@pytest.mark.parametrize(
    "field",
    [
        "all_metadata",
        "all_metadata_aborted",
        "missing_session",
        "receipt",
        "source",
        "marker_erased",
    ],
)
def test_every_queued_deletion_stage_rechecks_admission_cleanup_custody(
    deletion_case, monkeypatch, stage, field
):
    c = deletion_case
    _cleaned(c)
    tombstone_id, entry_id = _queue(c)
    names = deletion.STAGES["project"]
    with c.factory() as db, db.begin():
        for row in db.scalars(
            select(DeletionStage).where(DeletionStage.tombstone_id == tombstone_id)
        ):
            if names.index(row.resource_class) < names.index(stage):
                row.status = "completed"
        _corrupt(db, c, field)
    store = MagicMock()
    k8s = MagicMock()
    monkeypatch.setattr(deletion, "get_object_store", store)
    with pytest.raises(DeletionPending, match="upload cleanup"):
        with c.factory() as db, db.begin():
            deletion.reconcile_deletion(db, db.get(OutboxEntry, entry_id), k8s)
    store.assert_not_called()
    assert not k8s.mock_calls
    with c.factory() as db:
        assert db.get(Project, c.project_id) is not None
        assert (db.get(DatasetUploadSession, c.upload_id) is None) == (field == "missing_session")
        assert db.get(DeletionTombstone, tombstone_id).status == "pending"


@pytest.mark.parametrize("queued", [False, True])
@pytest.mark.parametrize("policy", ["hold", "retention", "store"])
def test_confirmed_cleanup_does_not_bypass_current_policy_or_store_identity(
    deletion_case, monkeypatch, queued, policy
):
    from dataclasses import replace

    c = deletion_case
    _cleaned(c)
    entry_id = _queue(c)[1] if queued else None
    with c.factory() as db, db.begin():
        row = db.get(DatasetUploadSession, c.upload_id)
        if policy == "hold":
            row.legal_hold = True
        elif policy == "retention":
            row.retention_until = datetime.now(UTC) + timedelta(days=2)
    if policy == "store":
        monkeypatch.setattr(
            admission, "get_settings", lambda: replace(c.settings, object_store_bucket="foreign")
        )
    store = MagicMock()
    monkeypatch.setattr(deletion, "get_object_store", store)
    with pytest.raises(DeletionPending if queued else HTTPException):
        with c.factory() as db, db.begin():
            if queued:
                deletion.reconcile_deletion(db, db.get(OutboxEntry, entry_id), MagicMock())
            else:
                deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    store.assert_not_called()


def test_known_but_unpublished_cleanup_result_cannot_release_deletion(deletion_case, monkeypatch):
    c = deletion_case
    now = _retired_known(c)

    def fail(*_):
        raise OSError("cleanup publication connection lost")

    monkeypatch.setattr(cleanup, "_publish", fail)
    with pytest.raises(OSError):
        cleanup.cleanup_admission(c.factory, c.project_id, c.upload_id, now=now)
    with c.factory() as db, db.begin():
        db.get(User, c.actor_id).is_active = True
    with pytest.raises(HTTPException) as held:
        with c.factory() as db, db.begin():
            deletion.request_deletion(db, db.get(User, c.actor_id), c.project_id)
    assert held.value.status_code == 409 and c.store.aborted == 1


def test_confirmed_cleanup_allows_all_project_deletion_stages_and_replay(
    deletion_case, monkeypatch
):
    c = deletion_case
    _cleaned(c)
    with c.factory() as db:
        retirement = copy.deepcopy(
            db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD]["retirement"]
        )
    tombstone_id, entry_id = _queue(c)
    prefixes = []
    store = SimpleNamespace(
        driver_name=c.store.driver_name,
        uri_for_key=c.store.uri_for_key,
        delete_prefix=prefixes.append,
        prefix_absent=lambda prefix: prefix in prefixes,
    )
    monkeypatch.setattr(deletion, "get_object_store", lambda *_: store)
    for index in range(4):
        with c.factory() as db:
            if index < 3:
                assert (
                    db.get(DatasetUploadSession, c.upload_id).provider_state[admission.RECORD][
                        "retirement"
                    ]
                    == retirement
                )
            assert reconcile_deletion_stage(db, db.get(OutboxEntry, entry_id), MagicMock()) is (
                index == 3
            )
    assert retirement["native_absence_confirmed"] is False
    assert prefixes == [f"projects/{c.project_id}", f"automl/projects/{c.project_id}"]
    assert c.constructions == 2 and c.store.aborted == 1
    with c.factory() as db:
        assert db.get(Project, c.project_id) is None
        assert db.get(DatasetUploadSession, c.upload_id) is None
        assert db.get(DeletionTombstone, tombstone_id).status == "complete"
