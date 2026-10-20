"""Typed public policy, immutable graph validation and worker feature admission."""

import copy
import uuid
from unittest.mock import MagicMock

import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import contracts as routes
from automl_api.db.session import get_db
from automl_api.models.enums import TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import (
    FeatureContractRevision,
    FeatureRecipeRevision,
    FeatureSearchSpaceRevision,
    WorkflowCommand,
)
from automl_api.schemas.contracts import (
    FeatureContractCreate,
    SearchObjectiveCreate,
    SearchSpaceCreate,
)
from automl_api.schemas.training import TrainingLaunchRequest
from automl_api.services import training
from automl_api.training import pipeline
from fastapi import FastAPI
from fastapi.testclient import TestClient
from feature_contract_fixtures import feature_specification, search_specification
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_selected_identity import capacity_client, selected_scope  # noqa: F401


@pytest.mark.parametrize(
    "path,value",
    [
        (("runtime_env",), {"pip": ["bad"]}),
        (("expression",), "__import__('os')"),
        (("source_columns",), ["x", "x"]),
        (("source_columns",), [42]),
        (("allowed_feature_families",), ["arbitrary_python"]),
        (("allowed_feature_families",), ["raw_passthrough", "raw_passthrough"]),
        (("disabled_feature_families",), []),
        (("value_fit_scope",), "all_rows"),
        (("final_test_isolated",), False),
        (("final_test_isolated",), 1),
        (("source_snapshot_digest",), "not-a-digest"),
        (("budgets", "maximum_expression_depth"), 3),
        (("budgets", "maximum_output_count"), 0),
        (("budgets", "maximum_output_count"), True),
        (("budgets", "maximum_batch_rows"), "8192"),
        (("budgets", "maximum_batch_rows"), 8193),
        (("budgets", "maximum_cardinality"), float("inf")),
        (("budgets", "memory"), "unbounded"),
    ],
)
def test_feature_request_is_closed_and_strict(path, value):
    body = feature_specification()
    destination = body
    for key in path[:-1]:
        destination = destination[key]
    destination[path[-1]] = value
    with pytest.raises(ValidationError):
        FeatureContractCreate(
            name="strict", task_type="regression", target_column="target", specification=body
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("python", "lambda x:x"),
        ("search_space", {"callback": "anything"}),
        ("runtime_env", {}),
        ("allowed_feature_families", ["rolling"]),
        ("recipe_revision", "bad"),
        ("feature_recipe_revision_id", "http://code"),
    ],
)
def test_feature_search_has_no_generic_execution_dictionary(field, value):
    body = search_specification()
    body[field] = value
    with pytest.raises(ValidationError):
        SearchSpaceCreate(
            name="strict-search",
            metric_name="rmse",
            metric_direction="minimize",
            specification=body,
        )


def test_objectives_do_not_accept_silent_runtime_controls():
    for specification in ({"fold_aggregation": "best"}, {"code": "import os"}, {"params": {}}):
        with pytest.raises(ValidationError):
            SearchObjectiveCreate(
                name="objective",
                metric_name="rmse",
                metric_direction="minimize",
                specification=specification,
            )
    with pytest.raises(ValidationError):
        SearchSpaceCreate(
            name="mismatch",
            metric_name="mae",
            metric_direction="minimize",
            specification=search_specification(),
        )


@pytest.fixture
def http_scope(selected_scope):  # noqa: F811
    engine, user_id, project_id, body, bindings = selected_scope
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
    with Session(engine) as db:
        contract = db.get(
            FeatureContractRevision, uuid.UUID(bindings["feature_contract_revision_id"])
        )
        search = db.get(
            FeatureSearchSpaceRevision,
            uuid.UUID(bindings["feature_search_space_revision_ids"]["rmse"]),
        )
        requests = {
            "feature-contracts": dict(
                name="typed-contract",
                task_type="regression",
                target_column="target",
                specification=copy.deepcopy(contract.specification),
            ),
            "feature-search-spaces": dict(
                name="typed-search",
                metric_name="rmse",
                metric_direction="minimize",
                specification=copy.deepcopy(search.specification),
            ),
            "search-objectives": dict(
                name="typed-objective",
                metric_name="rmse",
                metric_direction="minimize",
                specification={"fold_aggregation": "mean"},
            ),
        }
    with TestClient(app) as client:
        yield engine, project_id, client, requests


@pytest.mark.parametrize(
    "kind", ["feature-contracts", "feature-search-spaces", "search-objectives"]
)
def test_http_revision_is_typed_replayable_and_versioned(http_scope, kind):
    engine, project_id, client, bodies = http_scope
    body = bodies[kind]
    url = f"/projects/{project_id}/{kind}"
    first = client.post(
        url, json={**body, "expected_revision": 0}, headers={"Idempotency-Key": "typed-first"}
    )
    assert first.status_code == 200, first.text
    replay = client.post(
        url, json={**body, "expected_revision": 0}, headers={"Idempotency-Key": "typed-first"}
    )
    assert replay.json() == first.json()
    conflict = client.post(
        url, json={**body, "expected_revision": 0}, headers={"Idempotency-Key": "typed-conflict"}
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["current_revision"] == 1
    next_version = client.post(
        url, json={**body, "expected_revision": 1}, headers={"Idempotency-Key": "typed-next"}
    )
    assert next_version.status_code == 200 and next_version.json()["revision"] == 2
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == project_id)
            )
            == 2
        )


@pytest.mark.parametrize(
    "kind", ["feature-contracts", "feature-search-spaces", "search-objectives"]
)
def test_http_extra_runtime_fields_fail_before_revision_mutation(http_scope, kind):
    engine, project_id, client, bodies = http_scope
    body = copy.deepcopy(bodies[kind])
    body["specification"]["runtime_env"] = {"pip": ["bad"]}
    response = client.post(
        f"/projects/{project_id}/{kind}", json=body, headers={"Idempotency-Key": "untrusted"}
    )
    assert response.status_code == 422, response.text
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == project_id)
            )
            == 0
        )


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("feature-contracts", "source_snapshot_digest", "f" * 64),
        ("feature-contracts", "split_revision_id", str(uuid.uuid4())),
        ("feature-contracts", "prepared_artifact_id", str(uuid.uuid4())),
        ("feature-contracts", "source_columns", ["target", "x", "time"]),
        ("feature-contracts", "target_column", "x"),
        ("feature-contracts", "entity_column", "missing"),
        ("feature-contracts", "event_time_column", "time"),
        ("feature-contracts", "allowed_feature_families", ["numeric_unary"]),
        ("feature-search-spaces", "recipe_revision", "f" * 64),
        ("feature-search-spaces", "feature_recipe_revision_id", str(uuid.uuid4())),
        ("feature-search-spaces", "allowed_feature_families", ["numeric_unary"]),
        ("search-objectives", "metric_name", "mae"),
    ],
)
def test_semantic_mismatches_leave_no_public_command(http_scope, kind, field, value):
    engine, project_id, client, bodies = http_scope
    body = copy.deepcopy(bodies[kind])
    body["specification"][field] = value
    result = client.post(
        f"/projects/{project_id}/{kind}", json=body, headers={"Idempotency-Key": "bad-graph"}
    )
    assert result.status_code == 409, result.text
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == project_id)
            )
            == 0
        )


def test_openapi_has_concrete_feature_search_and_objective_schemas():
    app = FastAPI()
    app.include_router(routes.router)
    schemas = app.openapi()["components"]["schemas"]
    for name, nested in (
        ("FeatureContractCreate", "FeatureContractSpecification"),
        ("SearchSpaceCreate", "FeatureSearchSpecification"),
        ("SearchObjectiveCreate", "SearchObjectiveSpecification"),
    ):
        assert schemas[name]["properties"]["specification"]["$ref"].endswith("/" + nested)
        assert schemas[nested]["additionalProperties"] is False


@pytest.fixture
def bound_run(selected_scope, capacity_client):  # noqa: F811
    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db, db.begin():
        launched = training.launch_training_run(
            db,
            db.get(User, user_id),
            project_id,
            TrainingLaunchRequest(**body),
            capacity_client,
            idempotency_key="typed-worker",
        )
        run_id = launched.run.id
    return engine, run_id


def test_worker_reads_only_the_approved_compiled_view(bound_run):
    engine, run_id = bound_run
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert pipeline._bound_feature_columns(db, run) == ["x", "time", "target"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("feature_recipe_revision_id", str(uuid.uuid4())),
        ("primary_metric", "mae"),
        ("cv_folds", 4),
        ("event_time_column", "time"),
        ("excluded_columns", ["x"]),
    ],
)
def test_worker_rejects_mutated_admitted_policy(bound_run, field, value):
    engine, run_id = bound_run
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        run.params = {**run.params, field: value}
        with pytest.raises(ValueError, match="immutable specification"):
            pipeline._bound_feature_columns(db, run)


def test_actual_startup_stops_before_data_and_tracking_on_bad_feature_graph(bound_run, monkeypatch):
    engine, run_id = bound_run
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        recipe = db.get(FeatureRecipeRevision, uuid.UUID(run.params["feature_recipe_revision_id"]))
        recipe.specification = {**recipe.specification, "runtime_env": {"pip": ["bad"]}}
    monkeypatch.setattr(
        pipeline, "get_session_factory", lambda: lambda: Session(engine, expire_on_commit=False)
    )
    data = MagicMock(side_effect=AssertionError("invalid graph cannot read data"))
    tracking = MagicMock()
    monkeypatch.setattr(pipeline, "_load_prepared_training_frames", data)
    monkeypatch.setattr(pipeline, "_tracked_mlflow_run", tracking)
    with pytest.raises(ValueError, match="feature specification"):
        pipeline.execute_training_run(run_id)
    data.assert_not_called()
    tracking.assert_not_called()
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status.value == "queued"


@pytest.mark.parametrize(
    "selected_scope",
    [TaskType.REGRESSION, TaskType.CLASSIFICATION, TaskType.CLUSTERING, TaskType.TIME_SERIES],
    indirect=True,
)
def test_named_typed_revisions_are_usable_with_the_compiled_recipe(selected_scope):  # noqa: F811
    from automl_api.schemas.training import TrainingEstimateRequest
    from automl_api.services import contracts
    from automl_api.services.experiment_identity import CRITERIA, resolve_training_identity

    engine, user_id, project_id, body, _ = selected_scope
    if body["task_type"] == "classification":
        body = {**body, "positive_label": "yes"}
    with Session(engine) as db, db.begin():
        initial = resolve_training_identity(db, project_id, TrainingEstimateRequest(**body))
        original = db.get(FeatureContractRevision, initial.feature_contract_revision_id)
        named = contracts.create_revision(
            db,
            db.get(User, user_id),
            project_id,
            "feature-contract",
            FeatureContractCreate(
                name="named-contract",
                task_type=initial.task_type,
                target_column=initial.target_column,
                specification=original.specification,
            ),
        )
        assert named.digest_scope == "feature-contract-v2"
        original_search = db.get(
            FeatureSearchSpaceRevision, initial.feature_search_space_revision_id
        )
        named_search = contracts.create_revision(
            db,
            db.get(User, user_id),
            project_id,
            "feature-search-space",
            SearchSpaceCreate(
                name="named-search",
                metric_name=initial.primary_metric,
                metric_direction=original_search.metric_direction,
                specification=original_search.specification,
            ),
        )
        named_objective = contracts.create_revision(
            db,
            db.get(User, user_id),
            project_id,
            "search-objective",
            SearchObjectiveCreate(
                name="named-objective",
                metric_name=initial.primary_metric,
                metric_direction=original_search.metric_direction,
                specification={"feature_search_space_revision_id": named_search.id},
            ),
        )
        copied = initial.model_dump(mode="json", include=set(CRITERIA))
        copied["feature_contract_revision_id"] = str(named.id)
        copied["feature_search_space_revision_id"] = str(named_search.id)
        copied["search_objective_revision_id"] = str(named_objective.id)
        frozen = resolve_training_identity(db, project_id, TrainingEstimateRequest(**copied))
        assert frozen.feature_contract_revision_id == named.id
        assert frozen.positive_label == initial.positive_label


def test_changed_valid_policy_with_unchanged_digest_is_rejected(bound_run):
    from automl_api.models.workflows import FeatureContractRevision

    engine, run_id = bound_run
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        contract = db.get(
            FeatureContractRevision, uuid.UUID(run.params["feature_contract_revision_id"])
        )
        specification = copy.deepcopy(contract.specification)
        specification["budgets"]["maximum_output_count"] += 1
        contract.specification = specification
        with pytest.raises(ValueError, match="feature specification"):
            pipeline._bound_feature_columns(db, run)


@pytest.mark.parametrize("environment", ["production", "staging"])
def test_bound_feature_spec_is_mandatory_for_deployed_workers(monkeypatch, environment):
    from types import SimpleNamespace

    monkeypatch.setattr(pipeline, "get_settings", lambda: SimpleNamespace(environment=environment))
    with pytest.raises(ValueError, match="immutable experiment"):
        pipeline._bound_feature_columns(MagicMock(), SimpleNamespace(params={}))


def test_fenced_worker_cannot_use_the_local_legacy_shortcut(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="immutable experiment"):
        pipeline._bound_feature_columns(MagicMock(), SimpleNamespace(params={}))


def test_prepared_reader_projects_before_materializing_values(monkeypatch):
    from types import SimpleNamespace

    import pandas as pd

    frame = pd.DataFrame(
        {"x": [1, 2], "target": [0, 1], "unexpected": [9, 9], "row_id": ["a", "b"]}
    )
    dataset = MagicMock()
    dataset.schema.return_value.names = list(frame.columns)
    dataset.drop_columns.return_value = dataset
    selected = MagicMock()
    selected.to_pandas.return_value = frame[["x", "target"]]
    dataset.select_columns.return_value = selected
    monkeypatch.setattr(pipeline, "_ensure_ray", lambda: None)
    monkeypatch.setattr(pipeline, "_ray_source", lambda path, options: (path, None))
    monkeypatch.setattr(
        pipeline,
        "get_object_store",
        lambda: SimpleNamespace(
            dataframe_source=lambda _: SimpleNamespace(
                path="approved.parquet", filesystem_options={}
            )
        ),
    )
    monkeypatch.setattr(pipeline.ray.data, "read_parquet", lambda *_a, **_kw: dataset)
    actual = pipeline._load_prepared_role("s3://train", approved_columns=["x", "target"])
    assert list(actual.columns) == ["x", "target"]
    dataset.select_columns.assert_called_once_with(["x", "target"])
    dataset.to_pandas.assert_not_called()
    selected.to_pandas.assert_called_once()
    with pytest.raises(ValueError, match="approved feature view"):
        pipeline._load_prepared_role("s3://train", approved_columns=["missing", "target"])


def test_prepared_role_projection_reaches_both_splits_and_never_final(monkeypatch):
    from types import SimpleNamespace

    import pandas as pd

    read = MagicMock(return_value=pd.DataFrame({"x": [1], "target": [0]}))
    monkeypatch.setattr(pipeline, "_load_prepared_role", read)
    split = SimpleNamespace(
        specification={
            "uris": {
                "train": "s3://train",
                "validation": "s3://validation",
                "final_test": "s3://never-final",
            }
        }
    )
    pipeline._load_prepared_training_frames(split, approved_columns=["x", "target"])
    assert [call.args[0] for call in read.call_args_list] == ["s3://train", "s3://validation"]
    assert all(call.kwargs["approved_columns"] == ["x", "target"] for call in read.call_args_list)


def test_fenced_startup_passes_the_validated_view_to_the_reader(bound_run, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from automl_api.models.enums import AttemptStatus
    from automl_api.models.workflows import WorkflowAttempt

    engine, run_id = bound_run
    with Session(engine) as db, db.begin():
        attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        attempt.status = AttemptStatus.SUBMITTED
        attempt.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        attempt_id, fence = attempt.id, attempt.fencing_token
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    monkeypatch.setattr(
        pipeline, "get_session_factory", lambda: lambda: Session(engine, expire_on_commit=False)
    )
    read = MagicMock(side_effect=RuntimeError("stop after feature admission"))
    tracking = MagicMock()
    monkeypatch.setattr(pipeline, "_load_prepared_training_frames", read)
    monkeypatch.setattr(pipeline, "_tracked_mlflow_run", tracking)
    with pytest.raises(RuntimeError, match="after feature admission"):
        pipeline.execute_training_run(run_id)
    assert read.call_args.kwargs["approved_columns"] == ["x", "time", "target"]
    tracking.assert_not_called()


def test_another_projects_real_prepared_graph_cannot_be_referenced(http_scope):
    from automl_api.models.projects import Project
    from automl_api.models.workflows import DatasetSplitRevision
    from sqlalchemy import delete
    from test_selected_identity import publish_graph

    engine, project_id, client, bodies = http_scope
    with Session(engine) as db, db.begin():
        owner_id = db.get(Project, project_id).owner_id
        foreign = Project(owner_id=owner_id, created_by_id=owner_id, name="foreign-feature-graph")
        db.add(foreign)
        db.flush()
        foreign_id = foreign.id
        _, _, bindings = publish_graph(db, owner_id, foreign_id, TaskType.REGRESSION)
        contract = db.get(
            FeatureContractRevision, uuid.UUID(bindings["feature_contract_revision_id"])
        )
        foreign_specification = copy.deepcopy(contract.specification)
    try:
        body = {**bodies["feature-contracts"], "specification": foreign_specification}
        response = client.post(
            f"/projects/{project_id}/feature-contracts",
            json=body,
            headers={"Idempotency-Key": "foreign-graph"},
        )
        assert response.status_code == 409, response.text
        with Session(engine) as db:
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(WorkflowCommand)
                    .where(WorkflowCommand.project_id == project_id)
                )
                == 0
            )
    finally:
        with Session(engine) as db, db.begin():
            db.execute(
                delete(DatasetSplitRevision).where(DatasetSplitRevision.project_id == foreign_id)
            )
            db.execute(delete(Project).where(Project.id == foreign_id))


@pytest.mark.parametrize("feature_count", [500, 501])
def test_preparation_honors_the_compiled_feature_count_budget(feature_count):
    from types import SimpleNamespace

    from automl_api.preparation_worker import _feature_launch_revisions

    profile = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        target_column="target",
        overview_json={"split_revision_id": str(uuid.uuid4())},
    )
    artifact = SimpleNamespace(
        id=uuid.uuid4(),
        specification={
            "source_digest": "a" * 64,
            "identity": {"split_digests": {"train": "b" * 64}},
        },
    )
    result = {
        "task_inference": {"task_type": "regression"},
        "leakage_analysis": {"excluded_columns": []},
        "columns": [
            {"name": name, "semantic_type": "numerical_continuous", "quality_flags": []}
            for name in [*(f"x{i}" for i in range(feature_count)), "target"]
        ],
    }
    db = MagicMock()

    def ids():
        for call in db.add_all.call_args_list:
            for row in call.args[0]:
                if row.id is None:
                    row.id = uuid.uuid4()

    db.flush.side_effect = ids
    if feature_count > 500:
        with pytest.raises(ValueError, match="at most 500"):
            _feature_launch_revisions(db, profile, artifact, result)
        db.add_all.assert_not_called()
    else:
        _feature_launch_revisions(db, profile, artifact, result)
        registry = db.add_all.call_args_list[0].args[0][1]
        assert len(registry.specification["accepted"]) == feature_count


@pytest.mark.parametrize(
    "metric,direction", [("arbitrary_metric", "maximize"), ("rmse", "maximize")]
)
def test_http_unknown_or_reversed_objectives_do_not_create_commands(http_scope, metric, direction):
    engine, project_id, client, bodies = http_scope
    response = client.post(
        f"/projects/{project_id}/search-objectives",
        json={**bodies["search-objectives"], "metric_name": metric, "metric_direction": direction},
        headers={"Idempotency-Key": "bad-metric"},
    )
    assert (
        response.status_code == 422
        and response.json()["detail"]["code"] == "invalid_search_objective"
    )
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == project_id)
            )
            == 0
        )


def test_old_catalog_protocol_cannot_bypass_feature_admission(monkeypatch):
    import sys

    from automl_api.training import worker

    begin = MagicMock()
    sessions = MagicMock()
    monkeypatch.setattr(worker, "_enable_rapids_accelerator", lambda: False)
    monkeypatch.setattr(worker, "_begin_fenced_attempt", begin)
    monkeypatch.setattr(worker, "get_session_factory", sessions)
    monkeypatch.setattr(
        sys,
        "argv",
        ["worker", "--run-id", str(uuid.uuid4()), "--attempt-protocol", "training-catalog-v1"],
    )
    with pytest.raises(SystemExit) as error:
        worker.main()
    assert error.value.code == 2
    begin.assert_not_called()
    sessions.assert_not_called()


def test_entity_role_cannot_reinterpret_an_approved_predictor(bound_run):
    from automl_api.services.workflow_state import canonical_request_hash

    engine, run_id = bound_run
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        contract = db.get(
            FeatureContractRevision, uuid.UUID(run.params["feature_contract_revision_id"])
        )
        contract.specification = {**contract.specification, "entity_column": "x"}
        contract.content_digest = canonical_request_hash(contract.specification)
        with pytest.raises(ValueError, match="feature specification"):
            pipeline._bound_feature_columns(db, run)


@pytest.mark.parametrize("problem", ["output_budget", "non_raw_recipe"])
def test_compiled_execution_controls_cannot_be_silently_ignored(bound_run, problem):
    from automl_api.services.workflow_state import canonical_request_hash
    from automl_shared.feature_recipe import FeatureRecipe

    engine, run_id = bound_run
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        contract = db.get(
            FeatureContractRevision, uuid.UUID(run.params["feature_contract_revision_id"])
        )
        if problem == "output_budget":
            specification = copy.deepcopy(contract.specification)
            specification["budgets"]["maximum_output_count"] = 1
            contract.specification = specification
            contract.content_digest = canonical_request_hash(specification)
        else:
            recipe = db.get(
                FeatureRecipeRevision, uuid.UUID(run.params["feature_recipe_revision_id"])
            )
            recipe.specification = {
                **recipe.specification,
                "ratio_features": [{"name": "ratio", "numerator": "x", "denominator": "x"}],
            }
            core = {
                key: recipe.specification[key]
                for key in (
                    "revision",
                    "input_columns",
                    "target_column",
                    "excluded_leakage_columns",
                    "ratio_features",
                )
            }
            recipe.content_digest = FeatureRecipe.from_dict(core).digest
            search = db.get(
                FeatureSearchSpaceRevision,
                uuid.UUID(run.params["feature_search_space_revision_id"]),
            )
            search.specification = {
                **search.specification,
                "recipe_revision": recipe.content_digest,
            }
            search.content_digest = canonical_request_hash(search.specification)
        with pytest.raises(ValueError, match="feature specification") as error:
            pipeline._bound_feature_columns(db, run)
        expected = (
            "unsupported_feature_execution"
            if problem == "non_raw_recipe"
            else "feature_recipe_changed"
        )
        assert error.value.__cause__.detail["code"] == expected
