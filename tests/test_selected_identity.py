"""Legacy selected requests become durable, project-scoped experiment identities."""

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion, ProfilingJob
from automl_api.models.enums import DatasetFormat, DatasetStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.workflows import (
    DatasetSplitRevision,
    ExperimentSpecRevision,
    PreparedArtifact,
    PromotionalScope,
    SearchObjectiveRevision,
)
from automl_api.preparation_worker import _feature_launch_revisions
from automl_api.schemas.contracts import ExperimentSpecCreate
from automl_api.schemas.training import TrainingEstimateRequest, TrainingLaunchRequest
from automl_api.services import contracts, training
from automl_api.services.experiment_identity import SPEC_SCOPE, resolve_training_identity
from digest_fixtures import verified_digest_metadata
from fastapi import HTTPException
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_service_runtime import _estimate


def publish_graph(db, user_id, project_id, task, version=None):
    if version is None:
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="selected-identity")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            **verified_digest_metadata(),
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            status=DatasetStatus.READY,
            format=DatasetFormat.CSV,
            object_uri="s3://identity-fixture/raw.csv",
            content_hash="a" * 64,
            byte_size=300,
            row_count=12,
            column_count=3,
            schema_json={"columns": [{"name": "x"}, {"name": "target"}, {"name": "time"}]},
        )
        db.add(version)
        db.flush()
    revision = (
        db.scalar(
            select(func.max(DatasetSplitRevision.revision)).where(
                DatasetSplitRevision.dataset_version_id == version.id
            )
        )
        or 0
    ) + 1
    split = DatasetSplitRevision(
        project_id=project_id,
        dataset_version_id=version.id,
        revision=revision,
        digest_scope="split-v1",
        content_digest="b" * 64,
        train_digest="1" * 64,
        validation_digest="2" * 64,
        final_test_digest="3" * 64,
        sealed_at=datetime.now(UTC),
        specification={
            "split_counts": {"train": 8, "validation": 2, "final_test": 2},
            "split_strategy": "temporal" if task == TaskType.TIME_SERIES else "hash",
            "time_column": "time" if task == TaskType.TIME_SERIES else None,
            "uris": {
                "train": "s3://identity-fixture/train",
                "validation": "s3://identity-fixture/validation",
                "final_input": "s3://identity-fixture/final",
            },
        },
    )
    db.add(split)
    db.flush()
    target = None if task == TaskType.CLUSTERING else "target"
    profile = ProfilingJob(
        project_id=project_id,
        dataset_id=version.dataset_id,
        dataset_version_id=version.id,
        created_by_id=user_id,
        target_column=target,
        status="succeeded",
        finished_at=datetime.now(UTC),
        overview_json={
            "split_revision_id": str(split.id),
            "task_inference": {"task_type": task.value},
        },
        feature_profiles_json={"target": {"distinct_count": 2, "sample_values": ["no", "yes"]}},
    )
    artifact = PreparedArtifact(
        project_id=project_id,
        dataset_version_id=version.id,
        revision=revision,
        digest_scope="prepared-v1",
        content_digest="c" * 64,
        object_uri="s3://identity-fixture/prepared",
        byte_size=200,
        row_count=12,
        specification={
            "source_digest": version.content_hash,
            "identity": {"split_digests": {"train": split.train_digest}},
        },
    )
    db.add_all([profile, artifact])
    db.flush()
    bindings = _feature_launch_revisions(
        db,
        profile,
        artifact,
        {
            "task_inference": {"task_type": task.value},
            "leakage_analysis": {"excluded_columns": []},
            "columns": [
                {"name": name, "semantic_type": "numerical_continuous", "quality_flags": []}
                for name in ("x", "target", "time")
            ],
        },
    )
    profile.overview_json = {**profile.overview_json, "launch_bindings": bindings}
    return version, profile, bindings


@pytest.fixture
def selected_scope(phase1_engine, committed_tenant, request):  # noqa: F811
    task = getattr(request, "param", TaskType.REGRESSION)
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        version, profile, bindings = publish_graph(db, user_id, project_id, task)
        version_id = version.id
    body = {
        "dataset_version_id": version_id,
        "task_type": task,
        "target_column": None if task == TaskType.CLUSTERING else "target",
        "candidate_models": [
            {TaskType.CLASSIFICATION: "LogisticRegression", TaskType.CLUSTERING: "KMeans"}.get(
                task, "Ridge"
            )
        ],
        "prefer_gpu": False,
    }
    try:
        yield phase1_engine, user_id, project_id, body, bindings
    finally:
        with Session(phase1_engine) as db, db.begin():
            db.execute(delete(PromotionalScope).where(PromotionalScope.project_id == project_id))
            db.execute(
                delete(ExperimentSpecRevision).where(
                    ExperimentSpecRevision.project_id == project_id
                )
            )
            db.execute(
                delete(DatasetSplitRevision).where(DatasetSplitRevision.project_id == project_id)
            )


@pytest.mark.parametrize(
    "selected_scope",
    [TaskType.REGRESSION, TaskType.CLASSIFICATION, TaskType.CLUSTERING, TaskType.TIME_SERIES],
    indirect=True,
)
def test_legacy_selected_resolves_and_replays_one_complete_identity(selected_scope):
    engine, _, project_id, body, bindings = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        identity = resolved.experiment_spec_revision_id
        assert identity is not None
        assert str(resolved.split_revision_id) == bindings["split_revision_id"]
        assert resolved.feature_registry_revision_id and resolved.feature_recipe_revision_id
        row = db.get(ExperimentSpecRevision, identity)
        assert row.digest_scope == SPEC_SCOPE
    with Session(engine) as db, db.begin():
        replay = resolve_training_identity(db, project_id, TrainingLaunchRequest(**body))
        assert replay.experiment_spec_revision_id == identity
        assert replay.estimator_catalog_revision_id == replay.catalog_revision_id
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 1
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(SearchObjectiveRevision)
                .where(SearchObjectiveRevision.project_id == project_id)
            )
            == 1
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_version_id", uuid.uuid4()),
        ("split_revision_id", uuid.uuid4()),
        ("catalog_revision_id", uuid.uuid4()),
        ("feature_recipe_revision_id", uuid.uuid4()),
        ("target_column", None),
        ("positive_label", "yes"),
        ("evaluation_column", "x"),
        ("entity_column", "x"),
        ("event_time_column", "time"),
        ("prediction_time_column", "time"),
        ("excluded_columns", ["x"]),
        ("cv_folds", 4),
        ("optimization_iterations", 6),
    ],
)
def test_explicit_spec_cannot_be_overridden(selected_scope, field, value):
    engine, _, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db,
                project_id,
                TrainingEstimateRequest(
                    experiment_spec_revision_id=resolved.experiment_spec_revision_id,
                    **{field: value},
                ),
            )
        assert error.value.status_code == 422
        assert error.value.detail == {"code": "experiment_spec_mismatch", "field": field}


def test_profile_changes_do_not_silently_change_a_legacy_identity(selected_scope):
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        old = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db, db.begin():
        version = db.get(DatasetVersion, body["dataset_version_id"])
        _, _, new_bindings = publish_graph(db, user_id, project_id, body["task_type"], version)
    with Session(engine) as db, db.begin():
        same = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        assert same.experiment_spec_revision_id == old.experiment_spec_revision_id
        assert str(same.split_revision_id) != new_bindings["split_revision_id"]
        explicit = resolve_training_identity(
            db,
            project_id,
            TrainingEstimateRequest(**body, split_revision_id=new_bindings["split_revision_id"]),
        )
        assert explicit.experiment_spec_revision_id != old.experiment_spec_revision_id


def test_concurrent_selected_requests_share_one_identity(selected_scope):
    engine, _, project_id, body, _ = selected_scope
    barrier = Barrier(4)

    def issue(_):
        with Session(engine) as db, db.begin():
            barrier.wait(timeout=5)
            return resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body)
            ).experiment_spec_revision_id

    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(issue, range(4)))
    assert len(set(ids)) == 1
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 1
        )


@pytest.mark.parametrize("change", ["body", "column", "binding"])
def test_modified_spec_or_graph_is_rejected(selected_scope, change):
    engine, _, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        resolved = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
    with Session(engine) as db, db.begin():
        spec = db.get(ExperimentSpecRevision, resolved.experiment_spec_revision_id)
        if change == "body":
            spec.specification = {**spec.specification, "tampered": True}
        if change == "column":
            spec.target_column = "time"
        if change == "binding":
            db.get(DatasetSplitRevision, resolved.split_revision_id).content_digest = "d" * 64
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db,
                project_id,
                TrainingEstimateRequest(
                    experiment_spec_revision_id=resolved.experiment_spec_revision_id
                ),
            )
        assert error.value.status_code == 409


def test_all_catalog_does_not_create_an_implicit_experiment(selected_scope):
    engine, _, project_id, body, _ = selected_scope
    body.pop("candidate_models")
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body, catalog_mode="all")
            )
        assert error.value.detail["code"] == "experiment_spec_required"
        assert (
            db.scalar(
                select(func.count())
                .select_from(ExperimentSpecRevision)
                .where(ExperimentSpecRevision.project_id == project_id)
            )
            == 0
        )


def test_public_spec_metadata_does_not_override_validated_identity(selected_scope):
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        frozen = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        payload = ExperimentSpecCreate(
            name="named-spec",
            specification={
                "identity": {"target_column": "wrong"},
                "runtime_env": {"pip": ["untrusted"]},
            },
            **frozen.model_dump(
                include=set(ExperimentSpecCreate.model_fields)
                & set(TrainingEstimateRequest.model_fields),
                exclude_none=True,
            ),
        )
        named = contracts.create_experiment_spec(db, db.get(User, user_id), project_id, payload)
        named_id = named.id
    with Session(engine) as db:
        actual = resolve_training_identity(
            db, project_id, TrainingEstimateRequest(experiment_spec_revision_id=named_id)
        )
        assert (
            actual.target_column == "target"
            and actual.dataset_version_id == body["dataset_version_id"]
        )


def test_reservation_replays_before_mutable_profile_or_capacity(selected_scope, monkeypatch):
    engine, user_id, project_id, body, _ = selected_scope
    client = MagicMock()
    client.estimate.side_effect = lambda **_: _estimate()
    monkeypatch.setattr(training, "_reconcile_active_runs", lambda *_: None)
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_: (None, {}))
    monkeypatch.setattr(
        training, "get_object_store", lambda: SimpleNamespace(exists=lambda _: True)
    )
    payload = TrainingEstimateRequest(**body, reserve_capacity=True)
    with Session(engine) as db, db.begin():
        first = training.estimate_training_run(
            db,
            db.get(User, user_id),
            project_id,
            payload,
            client,
            idempotency_key="reserve-identity",
        )
    assert first.experiment_spec_revision_id and first.capacity_reservation
    client.estimate.side_effect = AssertionError("capacity must not be probed on replay")
    monkeypatch.setattr(
        training,
        "_resolve_estimate_identity",
        MagicMock(side_effect=AssertionError("identity must not be resolved on replay")),
    )
    with Session(engine) as db, db.begin():
        replay = training.estimate_training_run(
            db,
            db.get(User, user_id),
            project_id,
            payload,
            client,
            idempotency_key="reserve-identity",
        )
        assert replay == first
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            training.estimate_training_run(
                db,
                db.get(User, user_id),
                project_id,
                payload.model_copy(update={"expected_minutes": 11}),
                client,
                idempotency_key="reserve-identity",
            )
        assert error.value.detail["code"] == "idempotency_key_reused"


@pytest.fixture
def capacity_client(monkeypatch):
    client = MagicMock()
    client.settings = SimpleNamespace(training_namespace="identity-test")
    client.estimate.side_effect = lambda **_: _estimate()
    monkeypatch.setattr(training, "_reconcile_active_runs", lambda *_: None)
    monkeypatch.setattr(training, "_latest_leakage_analysis", lambda *_: (None, {}))
    monkeypatch.setattr(
        training, "get_object_store", lambda: SimpleNamespace(exists=lambda _: True)
    )
    return client


@pytest.mark.parametrize("reserved", [False, True])
def test_http_legacy_estimate_and_launch_persist_identity(
    selected_scope, capacity_client, monkeypatch, reserved
):
    from automl_api.api.deps import get_current_user
    from automl_api.api.routes import training as routes
    from automl_api.db.session import get_db
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import CapacityReservation, OutboxEntry
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    engine, user_id, project_id, body, _ = selected_scope
    monkeypatch.setattr(training, "KubernetesTrainingClient", lambda: capacity_client)
    app = FastAPI()
    app.include_router(routes.router)

    def session():
        with Session(engine) as db:
            yield db

    def user():
        with Session(engine) as db:
            return db.get(User, user_id)

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[get_current_user] = user
    request = TrainingEstimateRequest(**body).model_dump(mode="json", exclude_unset=True)
    with TestClient(app) as client:
        response = client.post(
            f"/projects/{project_id}/training/estimate",
            json={**request, "reserve_capacity": reserved},
            headers={"Idempotency-Key": "http-estimate"},
        )
        assert response.status_code == 200, response.text
        estimate = response.json()
        launch_body = dict(request)
        if reserved:
            launch_body.update(
                capacity_reservation_id=estimate["capacity_reservation"]["id"],
                estimate_digest=estimate["estimate_digest"],
                capacity_profile_revision=estimate["capacity_profile_revision"],
            )
        url = f"/projects/{project_id}/training/runs"
        launch = client.post(url, json=launch_body, headers={"Idempotency-Key": "http-launch"})
        assert launch.status_code == 202, launch.text
        result = launch.json()
        run_id = uuid.UUID(result["run"]["id"])
        capacity_client.estimate.side_effect = AssertionError("replay cannot probe capacity")
        replay = client.post(url, json=launch_body, headers={"Idempotency-Key": "http-launch"})
        assert replay.status_code == 202 and replay.json() == result
        changed = client.post(
            url, json={**launch_body, "cv_folds": 4}, headers={"Idempotency-Key": "http-launch"}
        )
        assert changed.status_code == 409
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.params["experiment_spec_revision_id"] == estimate["experiment_spec_revision_id"]
        assert (
            run.params["search_objective_revision_id"]
            == estimate["resolved_launch_bindings"]["search_objective_revision_id"]
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(OutboxEntry.project_id == project_id)
            )
            == 1
        )
        reservations = db.scalars(
            select(CapacityReservation).where(CapacityReservation.project_id == project_id)
        ).all()
        assert len(reservations) == 1 and reservations[0].status == "consumed"
    capacity_client.create_job.assert_not_called()


@pytest.mark.parametrize(
    "problem",
    [
        "missing_profile",
        "foreign_split",
        "unsealed",
        "recipe_scope",
        "catalog_alias",
        "execution_hint",
    ],
)
def test_bad_legacy_graph_is_rejected_before_capacity(selected_scope, capacity_client, problem):
    from automl_api.models.workflows import FeatureRecipeRevision, OutboxEntry

    engine, user_id, project_id, body, bindings = selected_scope
    with Session(engine) as db:
        if problem == "missing_profile":
            db.execute(delete(ProfilingJob).where(ProfilingJob.project_id == project_id))
        if problem == "foreign_split":
            body = {**body, "split_revision_id": uuid.uuid4()}
        if problem == "unsealed":
            db.get(DatasetSplitRevision, uuid.UUID(bindings["split_revision_id"])).sealed_at = None
        if problem == "recipe_scope":
            recipe = db.get(
                FeatureRecipeRevision, uuid.UUID(bindings["feature_recipe_revision_id"])
            )
            recipe.specification = {**recipe.specification, "fitted_scope_digest": "wrong"}
        if problem == "catalog_alias":
            body = {
                **body,
                "catalog_revision_id": uuid.uuid4(),
                "estimator_catalog_revision_id": uuid.uuid4(),
            }
        if problem == "execution_hint":
            body = {**body, "execution_mode": "in_memory", "execution_mode_hint": "auto"}
        with pytest.raises(HTTPException) as error:
            training.launch_training_run(
                db,
                db.get(User, user_id),
                project_id,
                TrainingLaunchRequest(**body),
                capacity_client,
                idempotency_key="bad-graph",
            )
        assert error.value.status_code in (409, 422)
        capacity_client.estimate.assert_not_called()
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(OutboxEntry.project_id == project_id)
            )
            == 0
        )


@pytest.mark.parametrize("selected_scope", [TaskType.CLASSIFICATION], indirect=True)
def test_positive_label_and_legacy_execution_hint_are_bound(selected_scope):
    engine, _, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        frozen = resolve_training_identity(
            db,
            project_id,
            TrainingEstimateRequest(**body, positive_label="yes", execution_mode="in_memory"),
        )
        assert frozen.positive_label == "yes" and frozen.execution_mode_hint == "in_memory"
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body, positive_label="absent")
            )
        assert error.value.detail["field"] == "positive_label"


def test_concurrent_reserved_estimates_return_one_reservation(selected_scope, capacity_client):
    from automl_api.models.workflows import CapacityReservation

    engine, user_id, project_id, body, _ = selected_scope
    barrier = Barrier(2)

    def reserve(_):
        with Session(engine) as db, db.begin():
            user = db.get(User, user_id)
            barrier.wait(timeout=10)
            return training.estimate_training_run(
                db,
                user,
                project_id,
                TrainingEstimateRequest(**body, reserve_capacity=True),
                capacity_client,
                idempotency_key="concurrent-reserve",
            ).model_dump(mode="json")

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(reserve, range(2)))
    assert responses[0] == responses[1]
    with Session(engine) as db:
        for model in (ExperimentSpecRevision, SearchObjectiveRevision, CapacityReservation):
            assert (
                db.scalar(
                    select(func.count()).select_from(model).where(model.project_id == project_id)
                )
                == 1
            )


@pytest.mark.parametrize("action", ["restart", "add_models"])
def test_followup_actions_keep_recorded_identity_roles(selected_scope, capacity_client, action):
    from automl_api.models.enums import RunStatus
    from automl_api.models.runs import ModelRun
    from automl_api.schemas.training import TrainingAddModelsRequest

    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        initial = training.launch_training_run(
            db,
            db.get(User, user_id),
            project_id,
            TrainingLaunchRequest(**body),
            capacity_client,
            idempotency_key="followup-source",
        )
        run_id = initial.run.id
        source = db.get(ModelRun, run_id)
        original = dict(source.params)
        source.status = RunStatus.FAILED if action == "restart" else RunStatus.SUCCEEDED
    with Session(engine) as db, db.begin():
        user = db.get(User, user_id)
        if action == "restart":
            result = training.restart_training_run(db, user, project_id, run_id, capacity_client)
        else:
            result = training.add_models_to_training_run(
                db,
                user,
                project_id,
                run_id,
                TrainingAddModelsRequest(candidate_models=["LinearRegression"], prefer_gpu=False),
                capacity_client,
            )
        params = result.run.params
        for key in (
            "search_objective_revision_id",
            "split_revision_id",
            "feature_contract_revision_id",
            "feature_recipe_revision_id",
            "event_time_column",
        ):
            assert params[key] == original[key]
        if action == "restart":
            assert params["experiment_spec_revision_id"] == original["experiment_spec_revision_id"]


@pytest.mark.parametrize("selected_scope", [TaskType.CLASSIFICATION], indirect=True)
def test_declared_contract_label_cannot_be_overridden(selected_scope):
    from automl_api.models.workflows import FeatureContractRevision

    engine, _, project_id, body, bindings = selected_scope
    with Session(engine) as db:
        contract = db.get(
            FeatureContractRevision, uuid.UUID(bindings["feature_contract_revision_id"])
        )
        contract.specification = {**contract.specification, "positive_label": "yes"}
        from automl_api.services.workflow_state import canonical_request_hash

        contract.content_digest = canonical_request_hash(contract.specification)
        frozen = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        assert frozen.positive_label == "yes"
        with pytest.raises(HTTPException) as error:
            resolve_training_identity(
                db, project_id, TrainingEstimateRequest(**body, positive_label="no")
            )
        assert error.value.detail["field"] == "positive_label"


def test_analysis_capacity_does_not_require_training_preparation(selected_scope, capacity_client):
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db:
        db.execute(delete(ProfilingJob).where(ProfilingJob.project_id == project_id))
        # An unprepared analysis upload is valid input for scoring an existing model.
        result = training.estimate_analysis_resources(
            db,
            db.get(User, user_id),
            project_id,
            TrainingEstimateRequest(**{**body, "target_column": None}),
            capacity_client,
        )
        assert result.can_launch and result.experiment_spec_revision_id is None
        for model in (ExperimentSpecRevision, SearchObjectiveRevision):
            assert (
                db.scalar(
                    select(func.count()).select_from(model).where(model.project_id == project_id)
                )
                == 0
            )
        with pytest.raises(HTTPException) as error:
            training.estimate_training_run(
                db,
                db.get(User, user_id),
                project_id,
                TrainingEstimateRequest(**body),
                capacity_client,
            )
        assert error.value.detail["code"] == "preparation_required"


def test_scope_rejects_another_experiment_on_the_same_split(selected_scope, capacity_client):
    from automl_api.models.runs import ModelRun
    from automl_api.models.workflows import OutboxEntry

    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        first = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        other = resolve_training_identity(
            db, project_id, TrainingEstimateRequest(**body, cv_folds=4)
        )
        assert first.split_revision_id == other.split_revision_id
        assert first.experiment_spec_revision_id != other.experiment_spec_revision_id
        scope = PromotionalScope(
            project_id=project_id,
            scope_key="different-experiment",
            canonical_provider="aws",
            expected_members=2,
            split_revision_id=first.split_revision_id,
            experiment_spec_revision_id=other.experiment_spec_revision_id,
        )
        db.add(scope)
        db.flush()
        scope_id = scope.id
    with Session(engine) as db:
        with pytest.raises(HTTPException) as error:
            training.launch_training_run(
                db,
                db.get(User, user_id),
                project_id,
                TrainingLaunchRequest(**body, promotional_scope_id=scope_id),
                capacity_client,
                idempotency_key="wrong-scope-experiment",
            )
        assert error.value.status_code == 409 and "experiment revision" in error.value.detail
        capacity_client.estimate.assert_not_called()
        for model in (ModelRun, OutboxEntry):
            assert (
                db.scalar(
                    select(func.count()).select_from(model).where(model.project_id == project_id)
                )
                == 0
            )
    with Session(engine) as db, db.begin():
        db.get(
            PromotionalScope, scope_id
        ).experiment_spec_revision_id = first.experiment_spec_revision_id
        launched = training.launch_training_run(
            db,
            db.get(User, user_id),
            project_id,
            TrainingLaunchRequest(**body, promotional_scope_id=scope_id),
            capacity_client,
            idempotency_key="matching-scope-experiment",
        )
        assert launched.run.params["experiment_spec_revision_id"] == str(
            first.experiment_spec_revision_id
        )
        assert launched.manifest["desiredState"] == "barrier_pending"
        assert (
            db.scalar(
                select(func.count())
                .select_from(OutboxEntry)
                .where(OutboxEntry.project_id == project_id)
            )
            == 0
        )


@pytest.mark.parametrize(
    "metadata",
    [
        {"content_hash_scope": "multipart_manifest"},
        {"content_hash_algorithm": "md5"},
        {"content_hash_verification_status": "pending"},
        {"content_hash_verified_at": None},
    ],
)
def test_unverified_persisted_dataset_blocks_launch_before_capacity(
    selected_scope, capacity_client, metadata
):
    from automl_api.models.workflows import CapacityReservation, OutboxEntry
    from automl_api.services.dataset_digests import DatasetDigestError

    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        # A real published graph with valid provenance is the positive control.
        training.estimate_training_run(
            db, db.get(User, user_id), project_id, TrainingEstimateRequest(**body), capacity_client
        )
    capacity_client.estimate.assert_called()
    capacity_client.estimate.reset_mock()
    with Session(engine) as db, db.begin():
        version = db.get(DatasetVersion, body["dataset_version_id"])
        for key, value in metadata.items():
            setattr(version, key, value)
        db.flush()
        with pytest.raises(DatasetDigestError):
            training.launch_training_run(
                db,
                db.get(User, user_id),
                project_id,
                TrainingLaunchRequest(**body),
                capacity_client,
                idempotency_key="unverified-dataset",
            )
        capacity_client.estimate.assert_not_called()
        for model in (OutboxEntry, CapacityReservation):
            assert (
                db.scalar(
                    select(func.count()).select_from(model).where(model.project_id == project_id)
                )
                == 0
            )
