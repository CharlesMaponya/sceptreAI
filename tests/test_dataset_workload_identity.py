"""Dataset attempt creation binds the service account to its project and stage."""

import re
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api import preparation_worker as worker
from automl_api.models.datasets import DatasetVersion, ProfilingJob
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.iam import User
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import profiling_jobs as jobs
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_local_profiling_delivery import profile_source  # noqa: F401
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


def enqueue(source, monkeypatch, *, prepared=False):
    engine, user_id, project_id, dataset_id, version_id, _ = source
    if prepared:
        monkeypatch.setattr(
            jobs,
            "require_prepared_source",
            lambda *_: SimpleNamespace(
                prepared_artifact_id=uuid.uuid4(), split_revision_id=uuid.uuid4()
            ),
        )
    with Session(engine) as db, db.begin():
        profile = ProfilingJob(
            project_id=project_id,
            created_by_id=user_id,
            dataset_id=dataset_id,
            dataset_version_id=version_id,
            status="queued",
            overview_json={"execution_mode": "kuberay"},
        )
        db.add(profile)
        db.flush()
        attempt = jobs._enqueue_distributed_profile(
            db, db.get(User, user_id), db.get(DatasetVersion, version_id), profile
        )
        return profile.id, attempt.id


@pytest.mark.parametrize("prepared", [False, True])
def test_initial_dataset_identity_and_existing_replay(profile_source, monkeypatch, prepared):  # noqa: F811
    engine, user_id, project_id, _, version_id, _ = profile_source
    profile_id, attempt_id = enqueue(profile_source, monkeypatch, prepared=prepared)
    stage = WorkflowStage.PREPARATION if prepared else WorkflowStage.SPLITTER
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.stage == stage
        assert attempt.workload_identity == f"project-{project_id}-{stage.value}"
        assert re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", attempt.workload_identity)
        assert len(attempt.workload_identity) <= 63
        # Replaying a historical attempt must preserve its already submitted identity.
        attempt.workload_identity = f"sceptre-dataset-{stage.value}"
        db.flush()
        replay = jobs._enqueue_distributed_profile(
            db,
            db.get(User, user_id),
            db.get(DatasetVersion, version_id),
            db.get(ProfilingJob, profile_id),
        )
        assert replay.id == attempt_id
        assert replay.workload_identity == f"sceptre-dataset-{stage.value}"
        entries = list(db.scalars(select(OutboxEntry).where(OutboxEntry.project_id == project_id)))
        assert len(entries) == 1


def test_committed_split_handoff_uses_project_preparation_identity(profile_source, monkeypatch):  # noqa: F811
    engine, _, project_id, _, _, store = profile_source
    profile_id, attempt_id = enqueue(profile_source, monkeypatch)
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, attempt_id)
        attempt.status = AttemptStatus.RUNNING
        attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=2)
        fence = attempt.fencing_token
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: Session(engine))
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    monkeypatch.setattr(worker, "admit_parent", lambda *_: None)
    result = {
        "project_id": str(project_id),
        "root_uri": store.put_bytes("split-fixture", b"fixture").uri,
        "byte_size": 7,
        "row_count": 2,
        "identity": {
            "split_digests": {role: "a" * 64 for role in ("train", "validation", "final_test")}
        },
        "uris": {},
    }
    worker._persist_split(profile_id, attempt_id, fence, result)
    with Session(engine) as db:
        profile = db.get(ProfilingJob, profile_id)
        preparation = db.get(
            WorkflowAttempt, uuid.UUID(profile.overview_json["workflow_attempt_id"])
        )
        assert preparation.stage == WorkflowStage.PREPARATION
        assert preparation.workload_identity == f"project-{project_id}-preparation"
        assert preparation.predecessor_attempt_id == attempt_id
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.SUCCEEDED
        entries = list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.topic == "ray.dataset.prepare.submit",
                    OutboxEntry.project_id == project_id,
                )
            )
        )
        assert len(entries) == 1
        assert entries[0].payload["attempt_id"] == str(preparation.id)
