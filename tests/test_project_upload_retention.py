"""Project-wide deletion cannot bypass a stored upload hold or retention deadline."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import DatasetUploadSession
from automl_api.models.enums import OutboxStatus
from automl_api.models.projects import Project
from automl_api.models.workflows import DeletionStage, DeletionTombstone, OutboxEntry
from automl_api.services import deletion
from automl_api.services.deletion_workflow import reconcile_deletion as reconcile_factory
from automl_api.storage.contracts import DeletionPending
from fastapi import HTTPException
from kubernetes.client import ApiException
from sqlalchemy import select
from test_project_management_integration import finish_deletion
from test_project_management_integration import workspace as workspace

STATUSES = ["ready", "quarantined", "failed", "aborted", "expired"]


@pytest.fixture
def retained_project(workspace, monkeypatch):
    db, user, project, run, store, uri, artifact_uri = workspace
    now = datetime.now(UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(deletion, "datetime", Clock)
    row = DatasetUploadSession(
        project_id=project.id,
        created_by_id=user.id,
        dataset_name="retained input",
        original_filename="input.csv",
        byte_size=4,
        part_size=4,
        total_parts=1,
        object_key=f"projects/{project.id}/raw/{uuid.uuid4()}",
        multipart_upload_id=uuid.uuid4().hex,
        resume_key=uuid.uuid4().hex,
        provider_driver=store.driver_name,
        status="ready",
        expires_at=now - timedelta(days=1),
        completed_object_uri=uri,
    )
    db.add(row)
    db.commit()
    return workspace, now, row


def set_policy(row, policy, now):
    row.legal_hold = policy == "hold"
    row.retention_until = {
        "future": now + timedelta(days=1),
        "past": now - timedelta(seconds=1),
        "equal": now,
    }.get(policy)


def queued(db, user, project, stage):
    response = deletion.request_deletion(db, user, project.id)
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    tombstone = db.get(DeletionTombstone, uuid.UUID(response["id"]))
    stages = list(
        db.scalars(select(DeletionStage).where(DeletionStage.tombstone_id == tombstone.id))
    )
    for name in deletion.STAGES["project"]:
        if name == stage:
            break
        next(item for item in stages if item.resource_class == name).status = "completed"
    db.commit()
    return entry, tombstone, stages


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["hold", "future"])
def test_project_request_rejects_retained_upload(retained_project, status, policy):
    (db, user, project, _, store, uri, artifact_uri), now, row = retained_project
    row.status = status
    set_policy(row, policy, now)
    db.commit()
    with pytest.raises(HTTPException) as caught:
        deletion.request_deletion(db, user, project.id)
    assert caught.value.status_code == 409
    assert "hold" in caught.value.detail or "retention" in caught.value.detail
    assert not project.settings.get("deletion_in_progress")
    assert (
        db.scalar(select(DeletionTombstone.id).where(DeletionTombstone.project_id == project.id))
        is None
    )
    assert store.exists(uri) and store.exists(artifact_uri)


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["hold", "future"])
@pytest.mark.parametrize("stage", deletion.STAGES["project"])
def test_queued_project_stage_rechecks_current_upload_policy(
    retained_project, monkeypatch, status, policy, stage
):
    (db, user, project, _, store, uri, artifact_uri), now, row = retained_project
    row.status = status
    db.commit()
    entry, tombstone, stages = queued(db, user, project, stage)
    # Policy may change after admission or a previous stage's checkpoint.
    set_policy(row, policy, now)
    db.commit()
    before = [(item.id, item.status) for item in stages]
    delete_prefix = MagicMock(side_effect=AssertionError("Retention reached native deletion"))
    monkeypatch.setattr(store, "delete_prefix", delete_prefix)
    k8s = MagicMock()
    with pytest.raises(DeletionPending, match="retention|hold"):
        deletion.reconcile_deletion(db, entry, k8s)
    assert [(item.id, item.status) for item in stages] == before
    assert tombstone.status == "pending"
    assert db.get(Project, project.id) is not None
    assert db.get(DatasetUploadSession, row.id) is row
    assert store.exists(uri) and store.exists(artifact_uri)
    delete_prefix.assert_not_called()
    assert not k8s.mock_calls


@pytest.mark.parametrize("status", STATUSES)
@pytest.mark.parametrize("policy", ["none", "past", "equal"])
def test_eligible_project_deletion_still_completes(retained_project, status, policy):
    (db, user, project, _, store, uri, artifact_uri), now, row = retained_project
    row.status = status
    set_policy(row, policy, now)
    db.commit()
    project_id, upload_id = project.id, row.id
    entry, tombstone, _ = queued(db, user, project, "runtime")
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=404)
    finish_deletion(db, entry, k8s)
    db.expire_all()
    assert db.get(Project, project_id) is None
    assert db.get(DatasetUploadSession, upload_id) is None
    assert tombstone.status == "complete"
    assert not store.exists(uri) and not store.exists(artifact_uri)


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_run_deletion_preserves_retained_project_input(retained_project, policy):
    (db, user, project, run, store, uri, artifact_uri), now, row = retained_project
    set_policy(row, policy, now)
    db.commit()
    deletion.request_deletion(db, user, project.id, run.id)
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    k8s = MagicMock()
    k8s.ray_job.side_effect = ApiException(status=404)
    finish_deletion(db, entry, k8s)
    assert db.get(Project, project.id) is project
    assert db.get(DatasetUploadSession, row.id) is row
    assert store.exists(uri) and not store.exists(artifact_uri)


@pytest.mark.parametrize("policy", ["hold", "future"])
def test_deletion_resumes_after_policy_releases(retained_project, policy):
    (db, user, project, _, store, uri, _), now, row = retained_project
    entry, tombstone, _ = queued(db, user, project, "runtime")
    set_policy(row, policy, now)
    db.commit()
    with pytest.raises(DeletionPending):
        deletion.reconcile_deletion(db, entry, MagicMock())
    assert store.exists(uri)
    set_policy(row, "equal", now)
    db.commit()
    finish_deletion(db, entry, MagicMock())
    assert tombstone.status == "complete"
    assert not store.exists(uri)


def test_policy_wait_does_not_consume_outbox_retry_budget(retained_project):
    (db, user, project, _, store, uri, _), now, row = retained_project
    entry, tombstone, _ = queued(db, user, project, "runtime")
    set_policy(row, "future", now)
    db.commit()
    for _ in range(32):
        entry.status = OutboxStatus.CLAIMED
        entry.lease_owner = "project-retention"
        entry.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
        entry.delivery_attempts += 1
        db.commit()
        from sqlalchemy.orm import Session

        entry_id = entry.id
        db.commit()
        reconcile_factory(
            lambda: Session(db.get_bind()), entry_id, "project-retention", k8s=MagicMock()
        )
        db.expire_all()
        assert entry.status == OutboxStatus.PENDING
        assert entry.delivery_attempts == 0
        assert "retention" in entry.last_error
        assert tombstone.status == "pending"
        assert store.exists(uri)
