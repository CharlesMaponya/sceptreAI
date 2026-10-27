"""Committed owned tenant fixtures; teardown removes only each test's rows."""

import hashlib
import os
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
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
from automl_api.models.workflows import (
    DeletionStage,
    DeletionTombstone,
    OutboxEntry,
    WorkflowAttempt,
)
from automl_api.schemas.projects import ProjectCreate, ProjectShareLinkCreate
from automl_api.services import deletion, projects
from automl_api.storage.embedded import EmbeddedObjectStoreDriver
from digest_fixtures import verified_digest_metadata
from fastapi import HTTPException
from sqlalchemy import create_engine, delete, event, inspect, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, attributes


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    url = os.environ.get("SCEPTRE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Dedicated PostgreSQL test database required")
    assert make_url(url).database.endswith("_tests")
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    db = Session(engine, expire_on_commit=False)
    created_users = set()

    @event.listens_for(db, "after_flush")
    def own_users(session, _):
        created_users.update(row.id for row in session.new if isinstance(row, User))

    try:
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
            **verified_digest_metadata(),
            project_id=project.id,
            dataset_id=dataset.id,
            created_by_id=user.id,
            version_number=1,
            format=DatasetFormat.CSV,
            object_uri=uri,
            content_hash=hashlib.sha256(b"x,y\n1,2").hexdigest(),
            byte_size=len(b"x,y\n1,2"),
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
                logical_key=f"training-run:{run.id}",
                workload_identity="test",
                generation=generation,
                fencing_token=str(uuid.uuid4()),
                status=AttemptStatus.FAILED,
                predecessor_attempt_id=previous,
            )
            db.add(attempt)
            db.flush()
            previous = attempt.id
        monkeypatch.setattr(deletion, "get_object_store", lambda *_: store)
        db.commit()
        yield db, user, project, run, store, uri, artifact_uri
    finally:
        db.rollback()
        db.close()
        with Session(engine) as cleanup, cleanup.begin():
            project_ids = list(
                cleanup.scalars(
                    select(Project.id).where(
                        Project.created_by_id.in_(created_users)
                        | Project.owner_id.in_(created_users)
                    )
                )
            )
            history = select(DeletionTombstone.id).where(
                DeletionTombstone.requested_by_id.in_(created_users)
            )
            cleanup.execute(delete(DeletionStage).where(DeletionStage.tombstone_id.in_(history)))
            cleanup.execute(
                delete(DeletionTombstone).where(
                    DeletionTombstone.requested_by_id.in_(created_users)
                )
            )
            for table in reversed(Base.metadata.sorted_tables):
                if "project_id" in table.c and table not in {
                    DeletionStage.__table__,
                    DeletionTombstone.__table__,
                }:
                    cleanup.execute(delete(table).where(table.c.project_id.in_(project_ids)))
            cleanup.execute(delete(Project).where(Project.id.in_(project_ids)))
            cleanup.execute(delete(User).where(User.id.in_(created_users)))
        engine.dispose()


def record_known_ray_job(db, actor_id, run, attempt, *, namespace, name):
    from automl_api.models.enums import CommandStatus, OutboxStatus
    from automl_api.models.workflows import WorkflowCommand
    from automl_api.services import reconciler
    from automl_api.services.workflow_state import canonical_request_hash

    run.k8s_namespace, run.k8s_job_name = namespace, name
    run.tags = {**run.tags, "orchestrator": "kuberay"}
    attempt.ray_job_name = name
    attempt.logical_key = f"training-run:{run.id}"
    command = WorkflowCommand(
        project_id=run.project_id,
        actor_id=actor_id,
        operation="training.launch",
        idempotency_key=uuid.uuid4().hex,
        request_hash=canonical_request_hash({"run_id": str(run.id)}),
        request_payload={"run_id": str(run.id)},
        status=CommandStatus.SUCCEEDED,
    )
    db.add(command)
    db.flush()
    entry = OutboxEntry(
        project_id=run.project_id,
        command_id=command.id,
        event_key=uuid.uuid4().hex,
        topic="ray.training.submit",
        aggregate_type="model_run",
        aggregate_id=run.id,
        status=OutboxStatus.DELIVERED,
        payload={"attempt_id": str(attempt.id), "fencing_token": attempt.fencing_token},
    )
    db.add(entry)
    db.flush()
    manifest = reconciler.build_ray_job_manifest(run, attempt, name=name)
    uid = str(uuid.uuid4())
    reconciler._record_ray_submission(
        db,
        attempt,
        entry,
        manifest,
        {"metadata": {**manifest["metadata"], "uid": uid}},
        reconciler._ray_submission_sequence(db, attempt),
        {
            "request_sha256": canonical_request_hash(entry.payload),
            "manifest_sha256": canonical_request_hash(manifest),
        },
    )
    db.flush()
    return uid


def reconcile_deletion_stage(db, entry, k8s):
    from automl_api.models.enums import OutboxStatus
    from automl_api.services.deletion_workflow import reconcile_deletion as reconcile_factory

    tombstone_id = uuid.UUID(entry.payload["tombstone_id"])
    entry_id = getattr(entry, "id", None)
    db.commit()

    def factory():
        return Session(db.get_bind(), expire_on_commit=False)

    with factory() as claim, claim.begin():
        if entry_id is None:
            current = claim.scalar(
                select(OutboxEntry).where(
                    OutboxEntry.project_id == entry.project_id,
                    OutboxEntry.topic == entry.topic,
                    OutboxEntry.aggregate_id == entry.aggregate_id,
                )
            )
        else:
            current = claim.get(OutboxEntry, entry_id)
        if current is not None and current.status != OutboxStatus.DELIVERED:
            current.status = OutboxStatus.CLAIMED
            current.lease_owner = "deletion-test"
            current.lease_expires_at = datetime.now(UTC) + timedelta(seconds=60)
            current.delivery_attempts += 1
            entry_id = current.id
        else:
            entry_id = None
    if entry_id is not None:
        reconcile_factory(factory, entry_id, "deletion-test", k8s=k8s)
    identities = [(row, inspect(row).identity[0]) for row in db.identity_map.values()]
    with factory() as view:
        for row, identity in identities:
            current = view.get(type(row), identity)
            if current is None:
                db.expunge(row)
            else:
                for column in row.__table__.columns:
                    attributes.set_committed_value(row, column.key, getattr(current, column.key))
    with factory() as check:
        tombstone = check.get(DeletionTombstone, tombstone_id)
        return tombstone is not None and tombstone.status == "complete"


def finish_deletion(db, entry, k8s):
    for _ in range(12):
        if reconcile_deletion_stage(db, entry, k8s):
            return
    pytest.fail("Deletion stages did not converge")


@pytest.mark.parametrize("whole_project", [False, True])
def test_delete_removes_files_and_fk_graph_without_deleting_other_projects(
    workspace, whole_project
):
    db, user, project, run, store, dataset_uri, artifact_uri = workspace
    project_id, run_id = project.id, run.id
    other = projects.create_project(db, user, ProjectCreate(name="Preserved"))
    other_uri = store.put_bytes(f"projects/{other.id}/runs/keep/model", b"keep").uri
    prepared_uri = store.put_bytes(
        f"automl/projects/{project.id}/prepared/test/train.parquet", b"prepared"
    ).uri
    response = deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    tombstone_id = uuid.UUID(response["id"])
    stage = DeletionStage(
        project_id=project.id,
        tombstone_id=tombstone_id,
        resource_class="historical_audit",
        status="completed",
        retry_count=2,
        terminal_reason="Previously verified deletion",
    )
    db.add(stage)
    db.flush()
    stage_id = stage.id
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    replay = SimpleNamespace(
        project_id=project_id,
        payload=dict(entry.payload),
        topic=entry.topic,
        aggregate_id=entry.aggregate_id,
    )
    finish_deletion(db, entry, MagicMock())
    db.flush()
    assert not store.exists(artifact_uri)
    assert store.exists(other_uri)
    assert store.exists(dataset_uri) is not whole_project
    assert store.exists(prepared_uri) is not whole_project
    assert db.scalar(select(ModelRun.id).where(ModelRun.id == run_id)) is None
    assert (db.scalar(select(Project.id).where(Project.id == project_id)) is None) is whole_project
    assert db.get(Project, other.id)
    for _ in range(2):
        finish_deletion(db, replay, MagicMock())
    db.expire_all()
    tombstone = db.get(DeletionTombstone, tombstone_id)
    assert tombstone is not None
    assert tombstone.status == "complete" and tombstone.completed_at is not None
    retained = db.get(DeletionStage, stage_id)
    assert retained is not None and retained.retry_count == 2
    assert retained.terminal_reason == "Previously verified deletion"


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
    attempt = db.scalar(
        select(WorkflowAttempt)
        .where(WorkflowAttempt.model_run_id == run.id)
        .order_by(WorkflowAttempt.generation.desc())
        .limit(1)
    )
    original_uid = record_known_ray_job(
        db, user.id, run, attempt, namespace="management-test", name="terminal-training"
    )
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
    entry = db.scalar(
        select(OutboxEntry).where(
            OutboxEntry.project_id == project.id,
            OutboxEntry.topic.in_(["project.delete", "run.delete"]),
        )
    )
    k8s = MagicMock()
    k8s.ray_job.return_value = {
        "metadata": {
            "uid": original_uid,
            "name": run.k8s_job_name,
            "namespace": run.k8s_namespace,
            "labels": {
                "automl.platform/project-id": str(project.id),
                "automl.platform/run-id": str(run.id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/generation": str(attempt.generation),
            },
        }
    }
    k8s.core.list_namespaced_pod.return_value = SimpleNamespace(items=[], metadata=None)
    k8s.batch.list_namespaced_job.return_value = SimpleNamespace(items=[], metadata=None)
    k8s.custom.list_namespaced_custom_object.return_value = {"items": [], "metadata": {}}
    assert not reconcile_deletion_stage(db, entry, k8s)
    k8s.delete_ray_job.assert_called_once_with(
        "terminal-training", namespace="management-test", uid=original_uid
    )
    assert store.exists(artifact_uri)
    k8s.ray_job.side_effect = ApiException(status=403)
    with pytest.raises(ApiException) as exc:
        reconcile_deletion_stage(db, entry, k8s)
    assert exc.value.status == 403
    assert store.exists(artifact_uri)
    db.execute(
        update(DeletionStage)
        .where(DeletionStage.tombstone_id == uuid.UUID(first["id"]))
        .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )
    db.commit()
    k8s.ray_job.side_effect = ApiException(status=404)
    finish_deletion(db, entry, k8s)
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
    with pytest.raises(HTTPException, match="controlled qualification"):
        deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    assert (
        db.scalar(select(DeletionTombstone.id).where(DeletionTombstone.project_id == project.id))
        is None
    )
    assert store.exists(artifact_uri)


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
    finish_deletion(db, entry, MagicMock())
    assert all(
        db.scalar(select(ModelRun.id).where(ModelRun.id == value)) is None for value in children
    )
    assert not store.exists(artifact_uri)


def test_deletion_stages_survive_retry_without_repeating_completed_effects(workspace, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import OutboxStatus

    db, user, project, _, store, *_ = workspace
    project_id = project.id
    response = deletion.request_deletion(db, user, project_id)
    tombstone_id = uuid.UUID(response["id"])
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project_id))
    db.commit()
    assert set(
        db.scalars(
            select(DeletionStage.resource_class).where(DeletionStage.tombstone_id == tombstone_id)
        )
    ) == {"runtime", "object_store", "prepared_store", "metadata"}
    calls = []
    original = store.delete_prefix
    failed = False

    def delete_prefix(prefix):
        nonlocal failed
        calls.append(prefix)
        original(prefix)
        if prefix.startswith("automl/") and not failed:
            failed = True
            raise RuntimeError("Lost prepared deletion acknowledgement")

    monkeypatch.setattr(store, "delete_prefix", delete_prefix)

    def deliver():
        return reconcile_deletion_stage(db, entry, MagicMock())

    for _ in range(2):
        deliver()
        assert entry.status == OutboxStatus.PENDING
        assert entry.delivery_attempts == 0
    assert calls == [f"projects/{project_id}"]
    with pytest.raises(RuntimeError, match="Lost prepared"):
        deliver()
    assert db.get(Project, project_id) is not None
    assert db.get(DeletionTombstone, tombstone_id).status == "pending"
    assert (
        db.scalar(
            select(DeletionStage.status).where(
                DeletionStage.tombstone_id == tombstone_id,
                DeletionStage.resource_class == "object_store",
            )
        )
        == "completed"
    )
    deliver()  # An uncertain native call retains its lease and is not reissued.
    db.execute(
        update(DeletionStage)
        .where(
            DeletionStage.tombstone_id == tombstone_id,
            DeletionStage.resource_class == "prepared_store",
        )
        .values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )
    db.commit()
    deliver()  # Reconcile absence without repeating the native delete.
    deliver()
    assert calls == [f"projects/{project_id}", f"automl/projects/{project_id}"]
    assert db.get(Project, project_id) is None
    assert db.get(DeletionTombstone, tombstone_id).status == "complete"
    assert set(
        db.scalars(select(DeletionStage.status).where(DeletionStage.tombstone_id == tombstone_id))
    ) == {"completed"}


@pytest.mark.parametrize("whole_project", [False, True])
@pytest.mark.parametrize("active_kind", ["attempt", "scope"])
@pytest.mark.parametrize("already_queued", [False, True])
def test_terminal_run_cannot_be_deleted_while_workflow_can_still_write(
    workspace, whole_project, active_kind, already_queued
):
    from automl_api.models.workflows import (
        DatasetSplitRevision,
        PromotionalScope,
        PromotionalScopeMember,
    )

    db, user, project, run, store, _, artifact_uri = workspace
    if already_queued:
        deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
        entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    if active_kind == "attempt":
        attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run.id))
        attempt.status = AttemptStatus.RUNNING
    else:
        split = DatasetSplitRevision(
            project_id=project.id,
            dataset_version_id=run.dataset_version_id,
            revision=1,
            digest_scope="split",
            content_digest="a" * 64,
            train_digest="b" * 64,
            validation_digest="c" * 64,
            final_test_digest="d" * 64,
        )
        db.add(split)
        db.flush()
        scope = PromotionalScope(
            project_id=project.id,
            scope_key="live",
            split_revision_id=split.id,
            canonical_provider="aws",
            expected_members=1,
        )
        db.add(scope)
        db.flush()
        db.add(
            PromotionalScopeMember(
                project_id=project.id, scope_id=scope.id, model_run_id=run.id, ordinal=0
            )
        )
    db.flush()
    with pytest.raises(HTTPException, match="workflows to stop") as error:
        if already_queued:
            deletion.reconcile_deletion(db, entry, MagicMock())
        else:
            deletion.request_deletion(db, user, project.id, None if whole_project else run.id)
    assert error.value.status_code == 409
    assert store.exists(artifact_uri)
    tombstone = db.scalar(
        select(DeletionTombstone).where(DeletionTombstone.project_id == project.id)
    )
    assert (tombstone is not None) == already_queued
    if tombstone is not None:
        assert tombstone.status == "pending"


def test_legacy_deletion_commits_stage_plan_before_side_effects(workspace):
    from sqlalchemy import delete

    db, user, project, _, store, _, artifact_uri = workspace
    response = deletion.request_deletion(db, user, project.id)
    tombstone_id = uuid.UUID(response["id"])
    db.execute(delete(DeletionStage).where(DeletionStage.tombstone_id == tombstone_id))
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project.id))
    db.commit()
    k8s = MagicMock()
    assert deletion.reconcile_deletion(db, entry, k8s) is False
    db.commit()
    assert store.exists(artifact_uri)
    assert not k8s.mock_calls
    assert set(
        db.scalars(select(DeletionStage.status).where(DeletionStage.tombstone_id == tombstone_id))
    ) == {"pending"}
    finish_deletion(db, entry, k8s)
    assert not store.exists(artifact_uri)


@pytest.mark.parametrize("whole_project", [True, False])
def test_retention_waits_preserve_metadata_and_retry_budget(workspace, monkeypatch, whole_project):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import OutboxStatus
    from automl_api.storage.contracts import DeletionPending

    db, user, project, run, store, *_ = workspace
    project_id, run_id = project.id, run.id
    response = deletion.request_deletion(db, user, project_id, None if whole_project else run_id)
    tombstone_id = uuid.UUID(response["id"])
    entry = db.scalar(select(OutboxEntry).where(OutboxEntry.project_id == project_id))
    db.commit()
    original = store.delete_prefix
    retained = True

    def cleanup(prefix):
        original(prefix)
        if retained:
            raise DeletionPending("Provider retains deleted generations")

    monkeypatch.setattr(store, "delete_prefix", cleanup)

    def deliver():
        return reconcile_deletion_stage(db, entry, MagicMock())

    deliver()  # Runtime stage.
    for _ in range(32):  # Retention exceeds the ordinary 30-delivery failure budget.
        before = datetime.now(UTC)
        deliver()
        assert entry.status == OutboxStatus.PENDING
        assert entry.delivery_attempts == 0
        assert entry.available_at >= before + timedelta(seconds=3600)
        assert "retains" in entry.last_error
        assert db.get(Project, project_id) is not None
        assert db.get(ModelRun, run_id) is not None
        assert db.get(DeletionTombstone, tombstone_id).status == "pending"
    retained = False
    for _ in range(3 if whole_project else 2):
        deliver()
    assert db.get(ModelRun, run_id) is None
    assert (db.get(Project, project_id) is None) == whole_project
    assert db.get(DeletionTombstone, tombstone_id).status == "complete"
