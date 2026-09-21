import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import GlobalRole, RunStatus
from automl_api.services.projects import project_journey
from fastapi import HTTPException


def test_journey_rejects_nonmembers_before_reading_project_workflows():
    db = MagicMock()
    db.get.return_value = SimpleNamespace(id=uuid.uuid4())
    db.scalar.return_value = None
    with pytest.raises(HTTPException) as failure:
        project_journey(
            db,
            SimpleNamespace(id=uuid.uuid4(), global_role=GlobalRole.MEMBER),
            db.get.return_value.id,
        )
    assert failure.value.status_code == 403
    # Only the membership lookup ran, not a workflow query.
    assert db.scalar.call_count == 1


def test_journey_reads_bounded_status_fields_only_from_the_authorized_project():
    project_id = uuid.uuid4()
    db = MagicMock()
    db.get.return_value = SimpleNamespace(id=project_id)
    db.scalar.side_effect = [
        uuid.uuid4(),
        "succeeded",
        RunStatus.RUNNING,
        RunStatus.SUCCEEDED,
        RunStatus.RUNNING,
    ]
    result = project_journey(db, SimpleNamespace(global_role=GlobalRole.ADMIN), project_id)
    assert result.dataset_uploaded
    assert result.profile_status == "succeeded"
    assert result.training_status == RunStatus.RUNNING
    assert result.analysis_status == RunStatus.SUCCEEDED
    assert result.deployment_status == RunStatus.RUNNING
    for call in db.scalar.call_args_list:
        statement = call.args[0]
        assert project_id in statement.compile().params.values()
        assert statement._limit_clause.value == 1


def test_empty_project_has_no_fabricated_completed_stages():
    db = MagicMock()
    db.get.return_value = SimpleNamespace(id=uuid.uuid4())
    db.scalar.return_value = None
    result = project_journey(
        db, SimpleNamespace(global_role=GlobalRole.ADMIN), db.get.return_value.id
    )
    assert result.model_dump() == {
        "dataset_uploaded": False,
        "profile_status": None,
        "training_status": None,
        "analysis_status": None,
        "deployment_status": None,
    }
