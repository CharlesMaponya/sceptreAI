"""Durable command replay must obey the caller's current project permission."""

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.api.routes import monitoring, operations
from automl_api.models.enums import GlobalRole, ProjectRole
from automl_api.models.iam import User
from automl_api.models.projects import ProjectMembership
from automl_api.services import idempotency
from fastapi import HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from test_project_management_integration import workspace  # noqa: F401


class ProtectedResult(BaseModel):
    id: uuid.UUID
    value: str


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("access", ["outsider", "revoked", "expired", "viewer", "editor"])
def test_unauthorized_mutation_never_reads_a_command(workspace, monkeypatch, existing, access):  # noqa: F811
    db, owner, project, *_ = workspace
    stored = ProtectedResult(id=uuid.uuid4(), value="project-private result")
    execute = MagicMock(return_value=stored)
    kwargs = dict(
        operation="test.protected",
        idempotency_key="known-key",
        payload={},
        execute=execute,
        response_model=ProtectedResult,
        response_status=201,
    )
    if existing:
        assert idempotency.durable_mutation(db, owner, project.id, **kwargs) == stored
    member = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == project.id, ProjectMembership.user_id == owner.id
        )
    )
    user = owner
    if access == "outsider":
        user = User(email=f"outsider-{uuid.uuid4()}@example.test", global_role=GlobalRole.MEMBER)
        db.add(user)
    elif access == "revoked":
        db.delete(member)
    elif access == "expired":
        member.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        member.role = ProjectRole(access)
    db.flush()
    lookup = MagicMock(wraps=idempotency.begin_command)
    monkeypatch.setattr(idempotency, "begin_command", lookup)
    execute.reset_mock()
    if access == "editor":
        kwargs["minimum_role"] = ProjectRole.ADMIN
    with pytest.raises(HTTPException) as error:
        idempotency.durable_mutation(db, user, project.id, **kwargs)
    assert error.value.status_code == 403
    lookup.assert_not_called()
    execute.assert_not_called()


@pytest.mark.parametrize("role", [ProjectRole.EDITOR, ProjectRole.ADMIN])
def test_current_authorized_member_replays_without_executing(workspace, role):  # noqa: F811
    db, owner, project, *_ = workspace
    stored = ProtectedResult(id=uuid.uuid4(), value="project-private result")
    execute = MagicMock(return_value=stored)
    kwargs = dict(
        operation="test.protected",
        idempotency_key="known-key",
        payload={},
        execute=execute,
        response_model=ProtectedResult,
        response_status=201,
        minimum_role=role,
    )
    assert idempotency.durable_mutation(db, owner, project.id, **kwargs) == stored
    colleague = User(email=f"colleague-{uuid.uuid4()}@example.test", global_role=GlobalRole.MEMBER)
    db.add(colleague)
    db.flush()
    db.add(
        ProjectMembership(
            project_id=project.id, user_id=colleague.id, role=role, accepted_at=datetime.now(UTC)
        )
    )
    db.flush()
    for _ in range(2):
        assert idempotency.durable_mutation(db, colleague, project.id, **kwargs) == stored
    assert execute.call_count == 1


@pytest.mark.parametrize("name", ["deployment", "cleanup", "metric"])
def test_admin_route_requires_admin_before_replay(monkeypatch, name):
    module = monitoring if name == "metric" else operations

    class Inspected(Exception):
        pass

    def inspect(*_args, **kwargs):
        assert kwargs["minimum_role"] == ProjectRole.ADMIN
        raise Inspected

    monkeypatch.setattr(module, "durable_mutation", inspect)
    project_id, resource_id = uuid.uuid4(), uuid.uuid4()
    payload = SimpleNamespace(model_dump=lambda **_: {})
    with pytest.raises(Inspected):
        if name == "deployment":
            operations.deploy(
                project_id, resource_id, payload, MagicMock(), SimpleNamespace(), "key"
            )
        elif name == "cleanup":
            operations.cleanup(project_id, payload, MagicMock(), SimpleNamespace(), "key")
        else:
            monitoring.create_deployment_metric(
                project_id, resource_id, payload, MagicMock(), SimpleNamespace(), "key"
            )


@pytest.mark.parametrize("role", [ProjectRole.VIEWER, ProjectRole.EDITOR, ProjectRole.ADMIN])
def test_cleanup_http_replay_preserves_admin_boundary(workspace, monkeypatch, role):  # noqa: F811
    from automl_api.api.deps import get_current_user
    from automl_api.db.session import get_db
    from automl_api.schemas.operations import ArtifactCleanupRead, ArtifactCleanupRequest
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    db, owner, project, *_ = workspace
    payload = ArtifactCleanupRequest()
    stored = ArtifactCleanupRead(
        dry_run=True,
        artifact_count=1,
        artifact_bytes=10,
        artifact_ids=[uuid.uuid4()],
        deleted_object_uris=["embedded://test/project-private-artifact"],
        deleted_kubernetes_jobs=[],
        errors=[],
    )
    idempotency.durable_mutation(
        db,
        owner,
        project.id,
        operation="cleanup.execute",
        idempotency_key="known-key",
        payload=payload.model_dump(mode="json"),
        execute=lambda: stored,
        response_model=ArtifactCleanupRead,
        response_status=200,
        minimum_role=ProjectRole.ADMIN,
        resource_id=project.id,
    )
    member = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == project.id, ProjectMembership.user_id == owner.id
        )
    )
    member.role = role
    db.flush()
    app = FastAPI()
    app.include_router(operations.router)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_user] = lambda: owner
    execute = MagicMock(side_effect=AssertionError("replay executed cleanup"))
    monkeypatch.setattr(operations, "cleanup_project_resources", execute)
    with TestClient(app) as client:
        response = client.post(
            f"/projects/{project.id}/operations/cleanup",
            json=payload.model_dump(mode="json"),
            headers={"Idempotency-Key": "known-key"},
        )
    if role == ProjectRole.ADMIN:
        assert response.status_code == 200
        assert response.json() == stored.model_dump(mode="json")
    else:
        assert response.status_code == 403
        assert "project-private-artifact" not in response.text
    execute.assert_not_called()


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("role", [None, ProjectRole.VIEWER, ProjectRole.EDITOR])
def test_contract_mutation_rechecks_permission_before_replay(
    workspace, monkeypatch, existing, role  # noqa: F811
):
    from automl_api.api.routes import contracts as routes
    from automl_api.services import contracts

    db, owner, project, *_ = workspace
    stored = ProtectedResult(id=uuid.uuid4(), value="private feature contract")
    create = MagicMock(return_value=stored)

    def invoke():
        return routes._mutation(
            db,
            owner,
            project.id,
            "feature-contract.create",
            "known-key",
            {},
            create,
            ProtectedResult,
        )

    if existing:
        assert invoke() == stored
    member = db.scalar(
        select(ProjectMembership).where(
            ProjectMembership.project_id == project.id, ProjectMembership.user_id == owner.id
        )
    )
    if role is None:
        db.delete(member)
    else:
        member.role = role
    db.flush()
    lookup = MagicMock(wraps=contracts.begin_command)
    monkeypatch.setattr(contracts, "begin_command", lookup)
    create.reset_mock()
    if role == ProjectRole.EDITOR:
        assert invoke() == stored
        assert create.call_count == (0 if existing else 1)
    else:
        with pytest.raises(HTTPException) as error:
            invoke()
        assert error.value.status_code == 403
        lookup.assert_not_called()
        create.assert_not_called()
