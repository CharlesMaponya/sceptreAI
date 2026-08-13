from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from automl_api.models.enums import GlobalRole, ProjectRole
from automl_api.models.projects import ProjectMembership
from automl_api.schemas.projects import ProjectCreate, ProjectShareLinkCreate, ProjectUpdate
from automl_api.services import projects
from fastapi import HTTPException


class _ScalarResult:
    def __init__(self, values: list[object]) -> None:
        self.values = values

    def all(self) -> list[object]:
        return self.values

    def unique(self) -> _ScalarResult:
        return self


class _Session:
    def __init__(
        self,
        *,
        scalar_values: list[object | None] | None = None,
        get_values: list[object | None] | None = None,
        lists: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = list(scalar_values or [])
        self.get_values = list(get_values or [])
        self.lists = list(lists or [])
        self.added: list[object] = []
        self.flushes = 0

    def scalar(self, _statement: object) -> object | None:
        return self.scalar_values.pop(0) if self.scalar_values else None

    def scalars(self, _statement: object) -> _ScalarResult:
        return _ScalarResult(self.lists.pop(0))

    def get(self, _model: object, _identifier: object) -> object | None:
        return self.get_values.pop(0) if self.get_values else None

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def flush(self) -> None:
        self.flushes += 1
        for instance in self.added:
            if getattr(instance, "id", None) is None:
                instance.id = uuid.uuid4()


def _user(*, admin: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        global_role=GlobalRole.ADMIN if admin else GlobalRole.MEMBER,
    )


def test_project_role_checks_are_ranked_and_admins_bypass_membership() -> None:
    project_id = uuid.uuid4()
    user = _user()
    editor = SimpleNamespace(role=ProjectRole.EDITOR)

    assert projects.user_has_project_role(_Session(), _user(admin=True), project_id)
    assert projects.user_has_project_role(
        _Session(scalar_values=[editor]), user, project_id, ProjectRole.VIEWER
    )
    assert not projects.user_has_project_role(
        _Session(scalar_values=[editor]), user, project_id, ProjectRole.ADMIN
    )
    assert not projects.user_has_project_role(_Session(scalar_values=[None]), user, project_id)

    with pytest.raises(HTTPException, match="do not have access") as error:
        projects.require_project_role(_Session(scalar_values=[None]), user, project_id)
    assert error.value.status_code == 403


def test_project_crud_and_visibility() -> None:
    user = _user()
    visible = [SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4())]
    assert projects.list_visible_projects(_Session(lists=[visible]), _user(admin=True)) == visible
    assert projects.list_visible_projects(_Session(lists=[visible]), user) == visible

    db = _Session()
    project = projects.create_project(
        db,
        user,
        ProjectCreate(name="  Research  ", description="Models", settings={"tier": "dev"}),
    )
    assert project.name == "Research"
    assert project.object_prefix == f"projects/{project.id}"
    membership = next(item for item in db.added if isinstance(item, ProjectMembership))
    assert membership.role == ProjectRole.OWNER
    assert membership.user_id == user.id

    owner_membership = SimpleNamespace(role=ProjectRole.OWNER)
    stored = SimpleNamespace(id=project.id, name="Research", description="Models")
    update_db = _Session(
        scalar_values=[owner_membership, owner_membership],
        get_values=[stored],
    )
    updated = projects.update_project(
        update_db,
        user,
        project.id,
        ProjectUpdate(name="  Production ", description="Qualified"),
    )
    assert updated.name == "Production"
    assert updated.description == "Qualified"

    with pytest.raises(HTTPException, match="Project not found") as missing:
        projects.get_project_for_user(_Session(get_values=[None]), user, uuid.uuid4())
    assert missing.value.status_code == 404


def test_project_members_and_share_link_lifecycle() -> None:
    project_id = uuid.uuid4()
    user = _user(admin=True)
    members = [SimpleNamespace(user_id=user.id)]
    assert projects.list_project_members(_Session(lists=[members]), project_id) == members

    db = _Session()
    link, token = projects.create_project_share_link(
        db,
        user,
        project_id,
        ProjectShareLinkCreate(
            role=ProjectRole.EDITOR,
            permissions={"datasets": "write"},
            expires_in_days=3,
            max_uses=2,
        ),
    )
    assert len(token) >= 32
    assert link.project_id == project_id
    assert link.role == ProjectRole.EDITOR

    link.expires_at = datetime.now(UTC) + timedelta(days=1)
    link.used_count = 0
    link.max_uses = 2
    accepted_project = SimpleNamespace(id=project_id)
    accept_db = _Session(scalar_values=[link, None], get_values=[accepted_project])
    assert projects.accept_project_share_link(accept_db, user, token) is accepted_project
    accepted = next(item for item in accept_db.added if isinstance(item, ProjectMembership))
    assert accepted.role == ProjectRole.EDITOR
    assert link.used_count == 1

    existing = SimpleNamespace(
        role=ProjectRole.VIEWER,
        permissions={},
        accepted_at=None,
        expires_at=datetime.now(UTC),
    )
    link.used_count = 0
    accept_existing = _Session(
        scalar_values=[link, existing],
        get_values=[accepted_project],
    )
    projects.accept_project_share_link(accept_existing, user, token)
    assert existing.role == ProjectRole.EDITOR
    assert existing.accepted_at is not None
    assert existing.expires_at is None


@pytest.mark.parametrize(
    ("link", "message"),
    [
        (None, "not found"),
        (
            SimpleNamespace(expires_at=datetime.now(UTC) - timedelta(seconds=1)),
            "expired",
        ),
        (
            SimpleNamespace(
                expires_at=datetime.now(UTC) + timedelta(days=1),
                used_count=1,
                max_uses=1,
            ),
            "maximum number",
        ),
    ],
)
def test_invalid_project_share_links_fail_closed(link: object | None, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        projects.accept_project_share_link(_Session(scalar_values=[link]), _user(), "invite-token")


def test_share_link_rejects_missing_project_after_membership_update() -> None:
    link = SimpleNamespace(
        project_id=uuid.uuid4(),
        created_by_id=uuid.uuid4(),
        role=ProjectRole.VIEWER,
        permissions={},
        expires_at=datetime.now(UTC) + timedelta(days=1),
        used_count=0,
        max_uses=1,
    )
    db = _Session(scalar_values=[link, None], get_values=[None])

    with pytest.raises(ValueError, match="no longer exists"):
        projects.accept_project_share_link(db, _user(), "invite-token")

    assert link.used_count == 1
    assert len(db.added) == 1
