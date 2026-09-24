"""Run only against a dedicated PostgreSQL test database; every test rolls back."""

import os
import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.db.base import Base
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import (
    ArtifactKind,
    AttemptStatus,
    DatasetFormat,
    GlobalRole,
    RunKind,
    RunStatus,
    TaskType,
    WorkflowStage,
)
from automl_api.models.iam import User
from automl_api.models.projects import Project, ProjectMembership
from automl_api.models.runs import ModelRegistryEntry, ModelRun, RunArtifact
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.schemas.projects import ProjectCreate, ProjectShareLinkCreate
from automl_api.services import deletion, projects
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    url = os.environ.get("SCEPTRE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Dedicated PostgreSQL test database required")
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    with engine.connect() as connection:
        transaction = connection.begin()
        db = Session(bind=connection, join_transaction_mode="create_savepoint")
        user = User(
            email=f"management-{uuid.uuid4()}@example.test",
            password_hash="test",
            global_role=GlobalRole.MEMBER,
        )
        db.add(user)
        db.flush()
        project = projects.create_project(db, user, ProjectCreate(name="Deletion test"))
        dataset = Dataset(project_id=project.id, created_by_id=user.id, name="Test CSV")
        db.add(dataset)
        db.flush()
        store = EmbeddedObjectStoreDriver(root=tmp_path, bucket="test")
        uri = store.put_bytes(f"projects/{project.id}/data/input.csv", b"x,y\n1,2").uri
        version = DatasetVersion(
            project_id=project.id,
            dataset_id=dataset.id,
            created_by_id=user.id,
            version_number=1,
            format=DatasetFormat.CSV,
            object_uri=uri,
            content_hash="a" * 64,
        )
        db.add(version)
        db.flush()
        run = ModelRun(
            project_id=project.id,
            dataset_version_id=version.id,
            created_by_id=user.id,
            run_kind=RunKind.TRAINING,
            task_type=TaskType.REGRESSION,
            status=RunStatus.FAILED,
            run_name="Test-v1",
            tags={},
            params={},
        )
        db.add(run)
        db.flush()
        artifact_uri = store.put_bytes(
            f"projects/{project.id}/runs/{run.id}/model.bin", b"test-model"
        ).uri
        artifact = RunArtifact(
            project_id=project.id,
            model_run_id=run.id,
            kind=ArtifactKind.MODEL_OBJECT,
            name="test-model",
            object_uri=artifact_uri,
        )
        db.add(artifact)
        db.flush()
        db.add(
            ModelRegistryEntry(
                project_id=project.id,
                model_run_id=run.id,
                model_artifact_id=artifact.id,
                model_name="Test",
                version=1,
                feature_space_hash="test",
            )
        )
        previous = None
        for generation in (1, 2):
            attempt = WorkflowAttempt(
                project_id=project.id,
                model_run_id=run.id,
                stage=WorkflowStage.TRAINING_RUN,
                logical_key=str(run.id),
                workload_identity="test",
                generation=generation,
                fencing_token=str(uuid.uuid4()),
                status=AttemptStatus.FAILED,
                predecessor_attempt_id=previous,
            )
            db.add(attempt)
            db.flush()
            previous = attempt.id
        monkeypatch.setattr(deletion, "get_object_store", lambda: store)
        yield db, user, project, run, store, uri, artifact_uri
        db.close()
        transaction.rollback()
    engine.dispose()


@pytest.mark.parametrize("whole_project", [False, True])
def test_delete_removes_files_and_fk_graph_without_deleting_other_projects(
    workspace, whole_project
):
    db, user, project, run, store, dataset_uri, artifact_uri = workspace
    other = projects.create_project(db, user, ProjectCreate(name="Preserved"))
    other_uri = store.put_bytes(f"projects/{other.id}/runs/keep/model", b"keep").uri
    prepared_uri = store.put_bytes(
        f"automl/projects/{project.id}/prepared/test/train.parquet", b"prepared"
    ).uri
    deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    deletion.reconcile_deletion(db, entry, MagicMock())
    db.flush()
    assert not store.exists(artifact_uri)
    assert store.exists(other_uri)
    assert store.exists(dataset_uri) is not whole_project
    assert store.exists(prepared_uri) is not whole_project
    assert db.scalar(select(ModelRun.id).where(ModelRun.id == run.id)) is None
    assert (db.scalar(select(Project.id).where(Project.id == project.id)) is None) is whole_project
    assert db.get(Project, other.id)


def test_active_run_prevents_any_deletion(workspace):
    db, user, project, run, store, _, artifact_uri = workspace
    run.status = RunStatus.RUNNING
    db.flush()
    with pytest.raises(HTTPException) as exc:
        deletion.request_deletion(db, user, project.id)
    assert exc.value.status_code == 409
    assert store.exists(artifact_uri)
    assert not project.settings.get("deletion_in_progress")


def test_invite_revoke_and_member_removal_are_project_scoped(workspace):
    db, owner, project, *_ = workspace
    invite, token = projects.create_project_share_link(
        db, owner, project.id, ProjectShareLinkCreate()
    )
    newcomer = User(
        email=f"invite-{uuid.uuid4()}@example.test",
        password_hash="test",
        global_role=GlobalRole.MEMBER,
    )
    db.add(newcomer)
    db.flush()
    projects.accept_project_share_link(db, newcomer, token)
    db.flush()
    membership = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == project.id, ProjectMembership.user_id == newcomer.id
        )
    )
    projects.remove_project_member(db, owner, project.id, membership.id)
    db.flush()
    assert not projects.user_has_project_role(db, newcomer, project.id)
    invite2, token2 = projects.create_project_share_link(
        db, owner, project.id, ProjectShareLinkCreate()
    )
    projects.revoke_project_invitation(db, owner, project.id, invite2.id)
    db.flush()
    with pytest.raises(ValueError, match="not found"):
        projects.accept_project_share_link(db, newcomer, token2)
    assert invite.used_count == 1
    with pytest.raises(HTTPException):
        projects.revoke_project_invitation(db, newcomer, project.id, invite.id)
    owner_member = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == project.id, ProjectMembership.user_id == owner.id
        )
    )
    with pytest.raises(HTTPException) as exc:
        projects.remove_project_member(db, owner, project.id, owner_member.id)
    assert exc.value.status_code == 409


@pytest.mark.parametrize("whole_project", [False, True])
def test_deletion_waits_for_runtime_removal_then_is_idempotent(workspace, whole_project):
    from automl_api.models.workflows import DeletionTombstone
    from kubernetes.client import ApiException

    db, user, project, run, store, _, artifact_uri = workspace
    run.k8s_job_name = "terminal-training"
    run.tags = {"orchestrator": "kuberay"}
    db.flush()
    first = deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    repeated = deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    assert repeated == first
    assert (
        len(
            db.scalars(
                select(DeletionTombstone).where(DeletionTombstone.project_id == project.id)
            ).all()
        )
        == 1
    )
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    k8s = MagicMock()
    with pytest.raises(RuntimeError, match="finish shutting down"):
        deletion.reconcile_deletion(db, entry, k8s)
    assert store.exists(artifact_uri)
    k8s.ray_job.side_effect = ApiException(status=403)
    with pytest.raises(ApiException) as exc:
        deletion.reconcile_deletion(db, entry, k8s)
    assert exc.value.status == 403
    assert store.exists(artifact_uri)
    k8s.ray_job.side_effect = ApiException(status=404)
    deletion.reconcile_deletion(db, entry, k8s)
    assert not store.exists(artifact_uri)


@pytest.mark.parametrize("whole_project", [False, True])
def test_registered_model_deletion_requires_explicit_deployment_cleanup(workspace, whole_project):
    db, user, project, run, store, _, artifact_uri = workspace
    registry = db.scalar(
        select(ModelRegistryEntry).where(ModelRegistryEntry.model_run_id == run.id)
    )
    deployment = ModelRun(
        project_id=project.id,
        created_by_id=user.id,
        dataset_version_id=run.dataset_version_id,
        run_kind=RunKind.DEPLOYMENT,
        task_type=TaskType.REGRESSION,
        status=RunStatus.CANCELLED,
        tags={"registry_entry_id": str(registry.id)},
        params={},
    )
    db.add(deployment)
    db.flush()
    with pytest.raises(HTTPException) as exc:
        deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    assert exc.value.status_code == 409
    assert "clean up" in exc.value.detail
    assert store.exists(artifact_uri)
    deployment.tags = {**deployment.tags, "runtime_cleaned_at": "2026-09-21T00:00:00Z"}
    assert (
        deletion.request_deletion(db, user, project.id, None if whole_project else run.id)["status"]
        == "pending"
    )


@pytest.mark.parametrize(
    "guard", ["preparing", "uploading", "legal_hold", "pending_command", "other_deletion"]
)
def test_project_deletion_preserves_inflight_or_protected_data(workspace, guard):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.datasets import DatasetUploadSession, ProfilingJob
    from automl_api.services.workflow_state import begin_command, enqueue_outbox

    db, user, project, run, store, _, artifact_uri = workspace
    if guard == "preparing":
        version = db.get(DatasetVersion, run.dataset_version_id)
        db.add(
            ProfilingJob(
                project_id=project.id,
                dataset_id=version.dataset_id,
                dataset_version_id=version.id,
                created_by_id=user.id,
                status="running",
            )
        )
    elif guard in {"uploading", "legal_hold"}:
        db.add(
            DatasetUploadSession(
                project_id=project.id,
                created_by_id=user.id,
                dataset_name="Protected input",
                original_filename="input.csv",
                byte_size=64,
                part_size=64,
                total_parts=1,
                object_key=f"projects/{project.id}/upload",
                multipart_upload_id="test-upload",
                resume_key="test-resume",
                status="verified" if guard == "legal_hold" else "uploading",
                legal_hold=guard == "legal_hold",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
    elif guard == "pending_command":
        command, _ = begin_command(
            db,
            project_id=project.id,
            actor_id=user.id,
            operation="training.launch",
            idempotency_key="test",
            payload={},
        )
        enqueue_outbox(
            db,
            command,
            topic="training.launch",
            aggregate_type="run",
            aggregate_id=run.id,
            payload={},
        )
    else:
        project.settings = {"deletion_in_progress": True}
    db.flush()
    with pytest.raises(HTTPException) as exc:
        deletion.request_deletion(db, user, project.id)
    assert exc.value.status_code == 409
    assert store.exists(artifact_uri)
    assert db.get(ModelRun, run.id) is not None


def test_deleting_a_parent_includes_transitive_analysis_runs(workspace):
    db, user, project, run, store, _, artifact_uri = workspace
    parent_id = run.id
    children = []
    for _ in range(2):
        child = ModelRun(
            project_id=project.id,
            created_by_id=user.id,
            dataset_version_id=run.dataset_version_id,
            run_kind=RunKind.TRAINING,
            task_type=TaskType.REGRESSION,
            status=RunStatus.SUCCEEDED,
            tags={"source_training_run_id": str(parent_id)},
            params={},
        )
        db.add(child)
        db.flush()
        children.append(child.id)
        parent_id = child.id
    deletion.request_deletion(db, user, project.id, run.id)
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    assert set(entry.payload["run_ids"]) == {str(value) for value in [run.id, *children]}
    deletion.reconcile_deletion(db, entry, MagicMock())
    assert all(
        db.scalar(select(ModelRun.id).where(ModelRun.id == value)) is None for value in children
    )
    assert not store.exists(artifact_uri)
