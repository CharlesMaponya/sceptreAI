"""Complete training-action replay against PostgreSQL; launch runtime is stubbed."""

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.schemas.training import ModelRunRead, TrainingAddModelsRequest, TrainingLaunchRead
from automl_api.services import training
from fastapi import HTTPException
from test_project_management_integration import workspace  # noqa: F401
from test_training_service_runtime import _estimate


@pytest.fixture
def action(workspace, monkeypatch):  # noqa: F811
    db, user, project, source, store, *_ = workspace
    source.target_column = "target"
    source.params = {"candidate_models": ["Ridge"]}
    for name in (
        "split",
        "feature_contract",
        "feature_registry",
        "feature_recipe",
        "feature_search_space",
        "estimator_catalog",
    ):
        source.params = {**source.params, f"{name}_revision_id": str(uuid.uuid4())}
    db.flush()
    monkeypatch.setattr(training, "get_object_store", lambda: store)

    def launch(_db, _user, _project, payload, *_args, **_kwargs):
        run = ModelRun(
            project_id=project.id,
            dataset_version_id=source.dataset_version_id,
            created_by_id=user.id,
            run_kind=RunKind.TRAINING,
            task_type=source.task_type,
            status=RunStatus.QUEUED,
            params=payload.model_dump(mode="json"),
            tags={},
        )
        db.add(run)
        db.flush()
        return TrainingLaunchRead(
            run=ModelRunRead.model_validate(run), estimate=_estimate(), manifest={}
        )

    launcher = MagicMock(side_effect=launch)
    monkeypatch.setattr(training, "launch_training_run", launcher)
    return db, user, project, source, launcher


@pytest.mark.parametrize("operation", ["restart", "add_models"])
def test_action_replays_complete_response_after_prerequisites_change(
    action, monkeypatch, operation
):
    db, user, project, source, launcher = action
    source.status = RunStatus.FAILED if operation == "restart" else RunStatus.SUCCEEDED
    db.flush()

    def invoke():
        if operation == "restart":
            return training.restart_training_run(db, user, project.id, source.id)
        return training.add_models_to_training_run(
            db, user, project.id, source.id, TrainingAddModelsRequest(candidate_models=["Ridge"])
        )

    initial = invoke()
    db.flush()
    if operation == "restart":
        monkeypatch.setattr(
            training, "get_object_store", lambda: pytest.fail("replay inspected storage")
        )
    else:
        source.tags = {**source.tags, "leaderboard": [{"model": "Ridge", "status": "succeeded"}]}
        monkeypatch.setattr(
            training, "candidate_catalog", lambda *_: pytest.fail("replay resolved catalog")
        )
    db.flush()
    db.expire_all()
    for _ in range(2):
        assert invoke().model_dump(mode="json") == initial.model_dump(mode="json")
    assert launcher.call_count == 1
    if operation == "add_models":
        assert source.tags["extension_run_ids"] == [str(initial.run.id)]
        assert initial.run.tags["leaderboard_parent_run_id"] == str(source.id)
    else:
        assert initial.run.tags["restarted_from_run_id"] == str(source.id)


def test_add_models_rejects_changed_request_before_mutable_validation(action, monkeypatch):
    db, user, project, source, launcher = action
    source.status = RunStatus.SUCCEEDED
    db.flush()
    training.add_models_to_training_run(
        db, user, project.id, source.id, TrainingAddModelsRequest(candidate_models=["Ridge"])
    )
    monkeypatch.setattr(
        training, "candidate_catalog", lambda *_: pytest.fail("replay resolved catalog")
    )
    with pytest.raises(HTTPException) as error:
        training.add_models_to_training_run(
            db,
            user,
            project.id,
            source.id,
            TrainingAddModelsRequest(candidate_models=["Ridge"], cv_folds=4),
        )
    assert error.value.status_code == 409
    assert launcher.call_count == 1


@pytest.mark.parametrize("operation", ["restart", "add_models"])
def test_pre_upgrade_launch_response_replays_with_lineage(action, monkeypatch, operation):
    from automl_api.services.workflow_state import begin_command

    db, user, project, source, launcher = action
    source.status = RunStatus.FAILED if operation == "restart" else RunStatus.SUCCEEDED
    db.flush()
    request = TrainingAddModelsRequest(candidate_models=["Ridge"])
    if operation == "restart":
        initial = training._restart_training_run(db, user, project.id, source.id)
        key = f"restart:{source.id}"
        payload = {}
    else:
        initial = training._add_models_to_training_run(db, user, project.id, source.id, request)
        key = f"add-models:{source.id}:Ridge"
        payload = request.model_dump(mode="json")
    command, _ = begin_command(
        db,
        project_id=project.id,
        actor_id=user.id,
        operation="training.launch",
        idempotency_key=key,
        payload=payload,
    )
    old_response = initial.model_dump(mode="json")
    old_response["run"]["tags"] = {}
    command.response_payload = old_response
    source.tags = {**source.tags, "leaderboard": [{"model": "Ridge", "status": "succeeded"}]}
    db.flush()
    monkeypatch.setattr(
        training, "get_object_store", lambda: pytest.fail("old replay inspected storage")
    )
    monkeypatch.setattr(
        training, "candidate_catalog", lambda *_: pytest.fail("old replay resolved catalog")
    )
    for _ in range(2):
        if operation == "restart":
            result = training.restart_training_run(db, user, project.id, source.id)
        else:
            result = training.add_models_to_training_run(db, user, project.id, source.id, request)
        assert result.model_dump(mode="json") == initial.model_dump(mode="json")
    assert launcher.call_count == 1


def test_add_models_keeps_parent_tags_written_during_launch(action):
    from sqlalchemy import update

    db, user, project, source, launcher = action
    source.status = RunStatus.SUCCEEDED
    db.flush()
    launch = launcher.side_effect
    sibling = str(uuid.uuid4())

    def write_parent(*args, **kwargs):
        result = launch(*args, **kwargs)
        db.execute(
            update(ModelRun)
            .where(ModelRun.id == source.id)
            .values(tags={"extension_run_ids": [sibling], "observation": "preserve"})
            .execution_options(synchronize_session=False)
        )
        return result

    launcher.side_effect = write_parent
    result = training.add_models_to_training_run(
        db, user, project.id, source.id, TrainingAddModelsRequest(candidate_models=["Ridge"])
    )
    assert source.tags["extension_run_ids"] == [sibling, str(result.run.id)]
    assert source.tags["observation"] == "preserve"


@pytest.mark.parametrize("conflict", ["payload", "lineage"])
def test_old_launch_key_cannot_alias_a_different_action(action, conflict):
    from automl_api.services.workflow_state import begin_command

    db, user, project, source, launcher = action
    source.status = RunStatus.SUCCEEDED
    db.flush()
    request = TrainingAddModelsRequest(candidate_models=["Ridge"])
    initial = training._add_models_to_training_run(db, user, project.id, source.id, request)
    command, _ = begin_command(
        db,
        project_id=project.id,
        actor_id=user.id,
        operation="training.launch",
        idempotency_key=f"add-models:{source.id}:Ridge",
        payload=request.model_dump(mode="json"),
    )
    command.response_payload = initial.model_dump(mode="json")
    if conflict == "payload":
        request = request.model_copy(update={"cv_folds": 4})
    else:
        db.get(ModelRun, initial.run.id).tags = {}
    db.flush()
    with pytest.raises(HTTPException) as error:
        training.add_models_to_training_run(db, user, project.id, source.id, request)
    assert error.value.status_code == 409
    assert launcher.call_count == 1


@pytest.mark.parametrize("operation", ["restart", "add_models"])
def test_lineage_failure_rolls_back_action_and_child_run(action, monkeypatch, operation):
    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import func, select

    db, user, project, source, launcher = action
    source.status = RunStatus.FAILED if operation == "restart" else RunStatus.SUCCEEDED
    db.flush()

    def counts():
        return tuple(
            db.scalar(select(func.count()).select_from(model).where(model.project_id == project.id))
            for model in (ModelRun, WorkflowCommand)
        )

    before = counts()
    refresh = db.refresh

    def fail_lineage(instance, **kwargs):
        if instance is source:
            raise RuntimeError("lost connection before lineage publication")
        return refresh(instance, **kwargs)

    monkeypatch.setattr(db, "refresh", fail_lineage)
    with pytest.raises(RuntimeError, match="lost connection"):
        with db.begin_nested():
            if operation == "restart":
                training.restart_training_run(db, user, project.id, source.id)
            else:
                training.add_models_to_training_run(
                    db,
                    user,
                    project.id,
                    source.id,
                    TrainingAddModelsRequest(candidate_models=["Ridge"]),
                )
    assert counts() == before
    assert launcher.call_count == 1


@pytest.mark.parametrize("scenario", ["same", "conflict", "different"])
def test_concurrent_add_models_preserves_one_action_per_key(action, monkeypatch, scenario):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from automl_api.models.datasets import Dataset, DatasetVersion
    from automl_api.models.enums import DatasetFormat, GlobalRole, ProjectRole
    from automl_api.models.iam import User
    from automl_api.models.projects import Project, ProjectMembership
    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import delete, select, text
    from sqlalchemy.orm import Session

    existing_db, _, _, template, _ = action
    engine = existing_db.get_bind().engine
    user_id, project_id, dataset_id, version_id, source_id = [uuid.uuid4() for _ in range(5)]
    try:
        with Session(engine) as db:
            db.add(
                User(
                    id=user_id,
                    email=f"replay-{user_id}@example.test",
                    password_hash="test",
                    global_role=GlobalRole.MEMBER,
                )
            )
            db.flush()
            db.add(
                Project(
                    id=project_id, owner_id=user_id, created_by_id=user_id, name="Concurrent replay"
                )
            )
            db.flush()
            db.add(
                ProjectMembership(
                    project_id=project_id,
                    user_id=user_id,
                    role=ProjectRole.OWNER,
                    accepted_at=datetime.now(UTC),
                )
            )
            db.add(
                Dataset(id=dataset_id, project_id=project_id, created_by_id=user_id, name="Test")
            )
            db.flush()
            db.add(
                DatasetVersion(
                    id=version_id,
                    project_id=project_id,
                    dataset_id=dataset_id,
                    created_by_id=user_id,
                    version_number=1,
                    format=DatasetFormat.CSV,
                    object_uri="embedded://test/concurrent",
                    content_hash="a" * 64,
                )
            )
            db.flush()
            db.add(
                ModelRun(
                    id=source_id,
                    project_id=project_id,
                    dataset_version_id=version_id,
                    created_by_id=user_id,
                    run_kind=RunKind.TRAINING,
                    task_type=template.task_type,
                    target_column="target",
                    status=RunStatus.SUCCEEDED,
                    params=dict(template.params),
                    tags={},
                )
            )
            db.commit()

        def launch(db, user, project_id, payload, *_args, **_kwargs):
            child = ModelRun(
                project_id=project_id,
                dataset_version_id=version_id,
                created_by_id=user.id,
                run_kind=RunKind.TRAINING,
                task_type=template.task_type,
                status=RunStatus.QUEUED,
                params=payload.model_dump(mode="json"),
                tags={},
            )
            db.add(child)
            db.flush()
            return TrainingLaunchRead(
                run=ModelRunRead.model_validate(child), estimate=_estimate(), manifest={}
            )

        monkeypatch.setattr(training, "launch_training_run", launch)
        barrier = Barrier(2)

        def invoke(index):
            with Session(engine) as db:
                db.execute(text("SET LOCAL lock_timeout = '10s'"))
                user = db.get(User, user_id)
                request = TrainingAddModelsRequest(
                    candidate_models=["Lasso" if scenario == "different" and index else "Ridge"],
                    cv_folds=4 if scenario == "conflict" and index else 3,
                )
                barrier.wait(timeout=10)
                try:
                    result = training.add_models_to_training_run(
                        db, user, project_id, source_id, request
                    )
                    db.commit()
                    return result.run.id
                except HTTPException as exc:
                    db.rollback()
                    assert exc.status_code == 409
                    assert exc.detail["code"] == "idempotency_key_reused"
                    return None

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(invoke, (0, 1)))
        expected = 2 if scenario == "different" else 1
        assert len(set(results) - {None}) == expected
        assert results.count(None) == (1 if scenario == "conflict" else 0)
        with Session(engine) as db:
            parent = db.get(ModelRun, source_id)
            children = list(
                db.scalars(
                    select(ModelRun).where(
                        ModelRun.project_id == project_id, ModelRun.id != source_id
                    )
                )
            )
            commands = list(
                db.scalars(select(WorkflowCommand).where(WorkflowCommand.project_id == project_id))
            )
            assert len(children) == len(commands) == expected
            assert set(parent.tags["extension_run_ids"]) == {str(child.id) for child in children}
            assert len(parent.tags["extension_run_ids"]) == expected
    finally:
        with Session(engine) as db:
            for model in (ModelRun, WorkflowCommand, DatasetVersion, Dataset):
                db.execute(delete(model).where(model.project_id == project_id))
            db.execute(delete(Project).where(Project.id == project_id))
            db.execute(delete(User).where(User.id == user_id))
            db.commit()


def test_large_model_selection_uses_bounded_replay_keys(action):
    from automl_api.models.workflows import WorkflowCommand
    from sqlalchemy import select

    db, user, project, source, launcher = action
    source.status = RunStatus.SUCCEEDED
    db.flush()
    models = [candidate.name for candidate in training.candidate_catalog(source.task_type)]
    assert len(f"add-models:{source.id}:{','.join(models)}") > 255
    request = TrainingAddModelsRequest(candidate_models=models)
    initial = training.add_models_to_training_run(db, user, project.id, source.id, request)
    assert training.add_models_to_training_run(db, user, project.id, source.id, request) == initial
    assert launcher.call_count == 1
    assert len(launcher.call_args.kwargs["idempotency_key"]) <= 255
    command = db.scalar(select(WorkflowCommand).where(WorkflowCommand.project_id == project.id))
    assert len(command.idempotency_key) <= 255
