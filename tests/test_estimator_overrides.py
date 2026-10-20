"""Reviewed typed estimator overrides are bound to admission and actual computation."""

import copy
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from automl_api.api.deps import get_current_user
from automl_api.api.routes import training as routes
from automl_api.db.session import get_db
from automl_api.models.enums import RunStatus, TaskType
from automl_api.models.iam import User
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingCandidate, WorkflowCommand
from automl_api.schemas.estimator_parameters import PARAMETER_MODELS, POLICY_REVISION
from automl_api.schemas.training import (
    TrainingAddModelsRequest,
    TrainingEstimateRequest,
    TrainingLaunchRequest,
)
from automl_api.services import training
from automl_api.training import candidate_runtime, pipeline, tune_runtime
from automl_api.training.model_catalog import candidate_catalog
from automl_api.training.parameter_overrides import (
    overrides_digest,
    validate_bound_overrides,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sklearn.model_selection import KFold
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_selected_identity import capacity_client, selected_scope  # noqa: F401
from test_training_pipeline_edge_contracts import _mock_cluster_runtime
from test_training_pipeline_runtime import _mock_candidate_side_effects
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401


@pytest.mark.parametrize(
    "estimator,parameters",
    [
        ("NotReviewed", {}),
        ("Ridge", {"callback": "import os"}),
        ("Ridge", {"alpha": "2"}),
        ("Ridge", {"alpha": True}),
        ("Ridge", {"alpha": None}),
        ("Ridge", {"alpha": -1.0}),
        ("Ridge", {"alpha": float("inf")}),
        ("Ridge", {"alpha": float("nan")}),
        ("Ridge", {"fit_intercept": 1}),
        ("Ridge", {"n_jobs": 1000}),
        ("Ridge", {"device": "cuda"}),
        ("Ridge", {"runtime_env": {"pip": ["bad"]}}),
        ("Ridge", {"model__alpha": 2.0}),
        ("RandomForestRegressor", {"n_estimators": 301}),
        ("RandomForestRegressor", {"n_estimators": 2.5}),
        ("LogisticRegression", {"solver": "unreviewed"}),
        ("KMeans", {"n_clusters": 9}),
        ("DBSCAN", {"eps": 0.2}),
    ],
)
def test_parameter_objects_reject_unreviewed_execution_inputs(estimator, parameters):
    with pytest.raises(ValidationError):
        TrainingEstimateRequest(
            parameter_overrides=[{"estimator": estimator, "parameters": parameters}]
        )


def test_omitted_and_explicit_null_parameters_have_different_meanings():
    payload = TrainingEstimateRequest(
        parameter_overrides=[
            {"estimator": "LogisticRegression", "parameters": {"class_weight": None}}
        ]
    )
    assert payload.model_dump(mode="json")["parameter_overrides"] == [
        {"estimator": "LogisticRegression", "parameters": {"class_weight": None}}
    ]
    empty = TrainingEstimateRequest(parameter_overrides=[{"estimator": "LogisticRegression"}])
    assert empty.model_dump(mode="json")["parameter_overrides"] == [
        {"estimator": "LogisticRegression", "parameters": {}}
    ]
    for model in PARAMETER_MODELS.values():
        assert model.model_json_schema()["additionalProperties"] is False


def test_schema_is_discriminated_and_has_reviewed_ranges():
    schema = TrainingLaunchRequest.model_json_schema()
    items = schema["properties"]["parameter_overrides"]["items"]
    assert items["discriminator"]["propertyName"] == "estimator"
    assert len(items["oneOf"]) == len(PARAMETER_MODELS)
    ridge = PARAMETER_MODELS["Ridge"].model_json_schema()["properties"]["alpha"]
    assert ridge["minimum"] == 0.001 and ridge["maximum"] == 100


@pytest.mark.parametrize(
    "model", [TrainingEstimateRequest, TrainingLaunchRequest, TrainingAddModelsRequest]
)
def test_duplicate_parameter_overrides_are_not_ambiguous(model):
    request = {"parameter_overrides": [{"estimator": "Ridge", "parameters": {"alpha": 1.0}}] * 2}
    if model == TrainingAddModelsRequest:
        request["candidate_models"] = ["Ridge"]
    with pytest.raises(ValidationError):
        model(**request)


@pytest.fixture
def http_scope(selected_scope, capacity_client, monkeypatch):  # noqa: F811
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
    request = TrainingLaunchRequest(
        **body, parameter_overrides=[{"estimator": "Ridge", "parameters": {"alpha": 2.0}}]
    ).model_dump(mode="json", exclude_unset=True)
    with TestClient(app) as client:
        yield engine, user_id, project_id, request, client, capacity_client


def test_http_reservation_launch_replay_and_worker_intent_match(http_scope):
    engine, _, project_id, request, client, capacity = http_scope
    url = f"/projects/{project_id}/training"
    estimate = client.post(
        url + "/estimate",
        json={**request, "reserve_capacity": True},
        headers={"Idempotency-Key": "parameter-estimate"},
    )
    assert estimate.status_code == 200, estimate.text
    summary = estimate.json()
    launch_request = {
        **request,
        "capacity_reservation_id": summary["capacity_reservation"]["id"],
        "estimate_digest": summary["estimate_digest"],
        "capacity_profile_revision": summary["capacity_profile_revision"],
    }
    changed_reservation = copy.deepcopy(launch_request)
    changed_reservation["parameter_overrides"][0]["parameters"]["alpha"] = 3.0
    refused = client.post(
        url + "/runs",
        json=changed_reservation,
        headers={"Idempotency-Key": "changed-reserved-parameters"},
    )
    assert refused.status_code == 409, refused.text
    launch = client.post(
        url + "/runs", json=launch_request, headers={"Idempotency-Key": "parameter-launch"}
    )
    assert launch.status_code == 202, launch.text
    result = launch.json()
    assert result["run"]["params"]["parameter_overrides"] == request["parameter_overrides"]
    run_id = uuid.UUID(result["run"]["id"])
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        selected = [
            candidate for candidate in candidate_catalog(run.task_type) if candidate.name == "Ridge"
        ]
        validate_bound_overrides(db, run, selected)
        candidate = db.scalar(
            select(TrainingCandidate).where(TrainingCandidate.model_run_id == run_id)
        )
        assert candidate.params["parameter_overrides"] == {"alpha": 2.0}
    capacity.estimate.side_effect = AssertionError("replay cannot use capacity")
    replay = client.post(
        url + "/runs", json=launch_request, headers={"Idempotency-Key": "parameter-launch"}
    )
    assert replay.status_code == 202 and replay.json() == result
    changed = copy.deepcopy(launch_request)
    changed["parameter_overrides"][0]["parameters"]["alpha"] = 3.0
    conflict = client.post(
        url + "/runs", json=changed, headers={"Idempotency-Key": "parameter-launch"}
    )
    assert conflict.status_code == 409
    capacity.create_job.assert_not_called()


@pytest.mark.parametrize("problem", ["task", "unselected", "disabled"])
def test_selection_mismatches_fail_before_capacity_and_execution_intent(http_scope, problem):
    engine, _, project_id, request, client, capacity = http_scope
    if problem == "task":
        request["parameter_overrides"] = [
            {"estimator": "LogisticRegression", "parameters": {"C": 1.0}}
        ]
    elif problem == "unselected":
        request["parameter_overrides"] = [{"estimator": "ElasticNet", "parameters": {"alpha": 1.0}}]
    else:
        request["candidate_models"] = ["Ridge", "Lasso"]
        request["estimator_overrides"] = [{"estimator": "Ridge", "enabled": False}]
    result = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "bad-parameters"},
    )
    assert result.status_code == 422, result.text
    assert result.json()["detail"]["code"] == "invalid_estimator_parameters"
    capacity.estimate.assert_not_called()
    with Session(engine) as db:
        assert (
            db.scalar(
                select(func.count()).select_from(ModelRun).where(ModelRun.project_id == project_id)
            )
            == 0
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowCommand)
                .where(WorkflowCommand.project_id == project_id)
            )
            == 0
        )


@pytest.mark.parametrize("problem", ["digest", "body_and_digest", "candidate"])
def test_worker_does_not_accept_modified_parameter_intent(http_scope, problem):
    engine, _, project_id, request, client, _ = http_scope
    response = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "frozen-parameters"},
    )
    assert response.status_code == 202, response.text
    with Session(engine) as db:
        run = db.get(ModelRun, uuid.UUID(response.json()["run"]["id"]))
        if problem == "digest":
            run.params = {**run.params, "parameter_overrides_digest": "0" * 64}
        elif problem == "body_and_digest":
            rows = [{"estimator": "Ridge", "parameters": {"alpha": 3.0}}]
            run.params = {
                **run.params,
                "parameter_overrides": rows,
                "parameter_overrides_digest": overrides_digest(rows),
            }
        else:
            candidate = db.scalar(
                select(TrainingCandidate).where(TrainingCandidate.model_run_id == run.id)
            )
            candidate.params = {**candidate.params, "parameter_overrides": {"alpha": 3.0}}
        with pytest.raises(ValueError, match="admitted intent"):
            validate_bound_overrides(
                db,
                run,
                [
                    candidate
                    for candidate in candidate_catalog(run.task_type)
                    if candidate.name == "Ridge"
                ],
            )


def fit_runtime(monkeypatch):
    original_model = pipeline._supervised_model_pipeline
    _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(pipeline, "_supervised_model_pipeline", original_model)
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda candidate, **_: (candidate.estimator, "cpu"),
    )


@pytest.mark.parametrize("mode", ["fixed", "local_search", "durable_search", "tune", "baseline"])
def test_actual_fit_preserves_fixed_parameter_through_every_search_branch(monkeypatch, mode):
    from automl_api.training.catalog_contract import catalog_specification

    fit_runtime(monkeypatch)
    candidate = next(
        item for item in candidate_catalog(TaskType.REGRESSION) if item.name == "Ridge"
    )
    original_alpha = candidate.estimator.alpha
    original_catalog = catalog_specification(TaskType.REGRESSION)
    fixed = {"alpha": 2.0, **({"fit_intercept": True} if mode == "fixed" else {})}
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        params={
            "parameter_overrides": (
                [] if mode == "baseline" else [{"estimator": "Ridge", "parameters": fixed}]
            )
        },
        tags={},
    )
    frame = pd.DataFrame({"x": np.arange(36, dtype=float), "z": np.arange(36, dtype=float) % 5})
    target = frame["x"] * 2 + frame["z"]
    monkeypatch.delenv("AUTOML_ATTEMPT_ID", raising=False)
    monkeypatch.delenv("TRAINING_EXECUTION_MODE", raising=False)
    if mode == "durable_search":
        monkeypatch.setattr(pipeline, "_runtime_training_resources", lambda: (2, None, False))
        monkeypatch.setattr(
            "automl_api.training.trial_runtime.requires_durable_trials", lambda: True
        )
        monkeypatch.setattr(
            "automl_api.training.trial_runtime.evaluate_trial",
            lambda _run, _name, _id, _spec, callback, **_: callback(),
        )
    if mode == "tune":
        monkeypatch.setenv("TRAINING_EXECUTION_MODE", "ray")
        monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
        monkeypatch.setattr(
            pipeline,
            "get_object_store",
            lambda: SimpleNamespace(
                uri_for_key=lambda key: "memory://" + key,
                dataframe_source=lambda uri: SimpleNamespace(path=uri, filesystem_options={}),
            ),
        )
        monkeypatch.setattr(pipeline, "_ray_source", lambda path, options: (path, None))

        def tune_search(model, space, x, y, cv, scoring, **kwargs):
            assert "model__alpha" not in space and model.get_params()["model__alpha"] == 2.0
            assert kwargs["base_parameters"]["alpha"] == 2.0
            return {"params": {"model__fit_intercept": True}, "mean": -0.1, "std": 0.0}

        monkeypatch.setattr(tune_runtime, "search_candidate", tune_search)
    entry = pipeline._fit_candidate(
        candidate,
        frame.iloc[:30],
        target.iloc[:30],
        frame.iloc[30:],
        target.iloc[30:],
        TaskType.REGRESSION,
        2,
        KFold(3),
        "neg_root_mean_squared_error",
        run,
    )
    assert entry["status"] == "succeeded", entry.get("error")
    if mode != "baseline":
        assert entry["best_params"]["model__alpha"] == 2.0
        assert entry["_model"].named_steps["model"].alpha == 2.0
    assert candidate.estimator.alpha == original_alpha
    assert "model__alpha" in candidate.search_space
    assert catalog_specification(TaskType.REGRESSION) == original_catalog


def test_clustering_fixed_cluster_count_is_not_replaced_by_automatic_sweep(monkeypatch):
    _mock_cluster_runtime(monkeypatch)
    candidate = next(
        item for item in candidate_catalog(TaskType.CLUSTERING) if item.name == "KMeans"
    )
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        task_type=TaskType.CLUSTERING,
        params={"parameter_overrides": [{"estimator": "KMeans", "parameters": {"n_clusters": 3}}]},
        tags={},
    )
    generator = np.random.default_rng(42)
    frame = pd.DataFrame(generator.normal(size=(60, 2)))
    entry = pipeline._fit_clustering_candidate(candidate, frame, None, KFold(3), run)
    assert entry["status"] == "succeeded", entry.get("error")
    assert entry["_model"].named_steps["model"].n_clusters == 3
    assert entry["best_params"]["model__n_clusters"] == 3


@pytest.mark.parametrize("target", [candidate_runtime._execute, tune_runtime._cv_trial])
def test_remote_workers_reject_older_parameter_protocol_before_state(monkeypatch, target):
    from automl_api.services.workflow_state import StaleFence

    if target == candidate_runtime._execute:
        args = ("supervised", None, None, {})
        kwargs = {}
    else:
        args = ({},)
        kwargs = dict(model=None, features=None, target=None, cv=3, scoring="r2")
    with pytest.raises(StaleFence, match="parameter protocol"):
        target(*args, **kwargs, parameter_protocol="training-features-v1")


@pytest.mark.parametrize("selected_scope", [TaskType.CLASSIFICATION], indirect=True)
def test_multiclass_liblinear_is_rejected_from_bound_profile_before_capacity(
    selected_scope,  # noqa: F811
    capacity_client,  # noqa: F811
    monkeypatch,
):
    from automl_api.models.datasets import ProfilingJob

    engine, user_id, project_id, body, _ = selected_scope
    with Session(engine) as db:
        profile = db.scalar(select(ProfilingJob).where(ProfilingJob.project_id == project_id))
        profile.feature_profiles_json = {"target": {"distinct_count": 3}}
        request = TrainingLaunchRequest(
            **body,
            parameter_overrides=[
                {"estimator": "LogisticRegression", "parameters": {"solver": "liblinear"}}
            ],
        )
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as error:
            training.launch_training_run(
                db,
                db.get(User, user_id),
                project_id,
                request,
                capacity_client,
                idempotency_key="multiclass-override",
            )
        assert error.value.status_code == 422 and "binary" in error.value.detail["message"]
        capacity_client.estimate.assert_not_called()


def test_neighbor_count_cannot_exceed_the_known_inner_training_fold(http_scope):
    _, _, project_id, request, client, capacity = http_scope
    request["candidate_models"] = ["KNeighborsRegressor"]
    request["parameter_overrides"] = [
        {"estimator": "KNeighborsRegressor", "parameters": {"n_neighbors": 20}}
    ]
    response = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "too-many-neighbors"},
    )
    assert response.status_code == 422, response.text
    assert "smallest fitting fold" in response.json()["detail"]["message"]
    capacity.estimate.assert_not_called()


@pytest.mark.parametrize("selected_scope", [TaskType.CLUSTERING], indirect=True)
@pytest.mark.parametrize(
    "estimator,clusters,expected",
    [
        ("AgglomerativeClustering", 3, 422),
        ("SpectralClustering", 3, 422),
        ("AgglomerativeClustering", 2, 202),
        ("KMeans", 3, 202),
    ],
)
def test_transductive_cluster_count_must_fit_the_held_out_fold(
    http_scope, estimator, clusters, expected
):
    engine, _, project_id, request, client, capacity = http_scope
    request["candidate_models"] = [estimator]
    request["parameter_overrides"] = [
        {"estimator": estimator, "parameters": {"n_clusters": clusters}}
    ]
    result = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "cluster-fold-bound"},
    )
    assert result.status_code == expected, result.text
    if expected == 422:
        capacity.estimate.assert_not_called()
        assert "smallest fitting fold" in result.json()["detail"]["message"]
        with Session(engine) as db:
            assert not db.scalar(select(ModelRun.id).where(ModelRun.project_id == project_id))
            assert not db.scalar(
                select(WorkflowCommand.id).where(WorkflowCommand.project_id == project_id)
            )


def test_raw_nonfinite_json_has_a_controlled_validation_response(http_scope):
    import json

    from automl_api.main import create_app

    engine, user_id, project_id, request, _, _ = http_scope
    app = create_app()

    def session():
        with Session(engine) as db:
            yield db

    def user():
        with Session(engine) as db:
            return db.get(User, user_id)

    app.dependency_overrides[get_db] = session
    app.dependency_overrides[get_current_user] = user
    request["parameter_overrides"][0]["parameters"]["alpha"] = float("nan")
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            f"/api/v1/projects/{project_id}/training/runs",
            content=json.dumps(request),
            headers={"Content-Type": "application/json", "Idempotency-Key": "not-finite"},
        )
    assert response.status_code == 422, response.text
    assert response.json()["detail"][0]["type"] == "finite_number"


@pytest.mark.parametrize("action", ["restart", "add_models"])
def test_followup_actions_preserve_or_bind_the_requested_parameter_policy(http_scope, action):
    engine, user_id, project_id, request, client, capacity = http_scope
    launched = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "followup-parameters"},
    )
    assert launched.status_code == 202, launched.text
    run_id = uuid.UUID(launched.json()["run"]["id"])
    with Session(engine) as db, db.begin():
        db.get(ModelRun, run_id).status = (
            RunStatus.FAILED if action == "restart" else RunStatus.SUCCEEDED
        )
    with Session(engine) as db, db.begin():
        if action == "restart":
            result = training.restart_training_run(
                db, db.get(User, user_id), project_id, run_id, capacity
            )
            expected = request["parameter_overrides"]
        else:
            expected = [{"estimator": "ElasticNet", "parameters": {"alpha": 0.5}}]
            result = training.add_models_to_training_run(
                db,
                db.get(User, user_id),
                project_id,
                run_id,
                TrainingAddModelsRequest(
                    candidate_models=["ElasticNet"], parameter_overrides=expected, prefer_gpu=False
                ),
                capacity,
            )
        assert result.run.params["parameter_overrides"] == expected
        assert result.run.params["parameter_overrides_digest"] == overrides_digest(expected)


@pytest.fixture
def parameter_trials(runtime):  # noqa: F811
    from automl_api.services.workflow_state import canonical_request_hash

    engine, run, scope = runtime
    rows = [{"estimator": "Ridge", "parameters": {"alpha": 2.0, "fit_intercept": True}}]
    run.params = {
        **run.params,
        "parameter_overrides": rows,
        "parameter_overrides_digest": overrides_digest(rows),
        "parameter_override_policy_revision": POLICY_REVISION,
    }
    with Session(engine) as db, db.begin():
        db.get(ModelRun, run.id).params = run.params
        candidate = db.get(TrainingCandidate, scope[4])
        candidate.params = {
            "parameter_overrides": rows[0]["parameters"],
            "parameter_overrides_digest": canonical_request_hash(rows[0]["parameters"]),
        }
    return runtime


@pytest.mark.parametrize("fallback", [False, True])
def test_actual_durable_gpu_or_cpu_cv_preserves_fixed_parameters_and_replays(
    parameter_trials, monkeypatch, fallback
):
    from automl_api.models.workflows import TrainingTrial
    from sklearn.model_selection import cross_val_score

    engine, run, scope = parameter_trials
    fit_runtime(monkeypatch)
    monkeypatch.setattr(pipeline, "_runtime_training_resources", lambda: (2, "nvidia", False))
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda candidate, **kwargs: (
            candidate.estimator,
            "nvidia" if kwargs["gpu_vendor"] else "cpu",
        ),
    )
    calls = []

    def cv(model, *args, **kwargs):
        assert model.named_steps["model"].alpha == 2.0
        calls.append(True)
        if fallback and len(calls) == 1:
            raise RuntimeError("simulated GPU execution failure")
        return cross_val_score(model, *args, **kwargs)

    monkeypatch.setattr(pipeline, "cross_val_score", cv)
    candidate = next(
        item for item in candidate_catalog(TaskType.REGRESSION) if item.name == "Ridge"
    )
    frame = pd.DataFrame({"x": np.arange(36, dtype=float)})
    for _ in range(2):
        result = pipeline._fit_candidate(
            candidate,
            frame.iloc[:30],
            frame["x"].iloc[:30] * 2,
            frame.iloc[30:],
            frame["x"].iloc[30:] * 2,
            TaskType.REGRESSION,
            2,
            3,
            "neg_root_mean_squared_error",
            run,
        )
        assert result["status"] == "succeeded", result.get("error")
        assert result["best_params"]["model__alpha"] == 2.0
    assert len(calls) == (2 if fallback else 1)
    with Session(engine) as db:
        trials = list(
            db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        )
        assert len(trials) == (2 if fallback else 1)
        assert all(trial.parameters["parameters"]["alpha"] == 2.0 for trial in trials)
        assert all(trial.result_digest for trial in trials)


@pytest.mark.parametrize(
    "changed", [{"alpha": 3.0, "fit_intercept": True}, {"alpha": 2.0, "fit_intercept": 1}]
)
def test_trial_server_rejects_replaced_fixed_parameters_before_enrollment(
    parameter_trials, changed
):
    from automl_api.models.workflows import TrainingTrial
    from automl_api.services.training_trials import enroll_trial

    engine, run, scope = parameter_trials
    with Session(engine) as db:
        with pytest.raises(ValueError, match="fixed overrides"):
            enroll_trial(
                db,
                run_attempt_id=scope[2],
                run_fencing_token=scope[3],
                candidate_id=scope[4],
                suggestion_id="changed-fixed",
                parameters={"parameters": changed},
                execution_id="never-dispatched",
            )
        assert (
            db.scalar(
                select(func.count())
                .select_from(TrainingTrial)
                .where(TrainingTrial.candidate_id == scope[4])
            )
            == 0
        )


def test_empty_override_does_not_change_preexisting_launch_command_shape(http_scope):
    engine, _, project_id, request, client, capacity = http_scope
    request.pop("parameter_overrides")
    url = f"/projects/{project_id}/training/runs"
    initial = client.post(url, json=request, headers={"Idempotency-Key": "pre-parameters-launch"})
    assert initial.status_code == 202, initial.text
    with Session(engine) as db:
        command = db.scalar(
            select(WorkflowCommand).where(
                WorkflowCommand.project_id == project_id,
                WorkflowCommand.operation == "training.launch",
            )
        )
        assert "parameter_overrides" not in command.request_payload
    capacity.estimate.side_effect = AssertionError("stored launch response must replay")
    replay = client.post(
        url,
        json={**request, "parameter_overrides": []},
        headers={"Idempotency-Key": "pre-parameters-launch"},
    )
    assert replay.status_code == 202 and replay.json() == initial.json()


def test_erasing_policy_fields_cannot_turn_new_intent_into_legacy_execution(http_scope):
    engine, _, project_id, request, client, _ = http_scope
    response = client.post(
        f"/projects/{project_id}/training/runs",
        json=request,
        headers={"Idempotency-Key": "erased-policy"},
    )
    assert response.status_code == 202, response.text
    with Session(engine) as db:
        run = db.get(ModelRun, uuid.UUID(response.json()["run"]["id"]))
        run.params = {
            key: value
            for key, value in run.params.items()
            if key not in {"parameter_overrides", "parameter_overrides_digest"}
        }
        with pytest.raises(ValueError, match="missing from admitted intent"):
            validate_bound_overrides(db, run, [])


def test_fenced_worker_requires_a_parameter_policy_even_if_metadata_is_absent(monkeypatch):
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="missing from admitted intent"):
        validate_bound_overrides(MagicMock(), SimpleNamespace(params={}), [])


def test_catalog_and_parameter_schemas_cover_the_same_reviewed_estimators():
    from automl_api.schemas.estimator_parameters import PARAMETER_OVERRIDES

    names = set()
    for task in (
        TaskType.CLASSIFICATION,
        TaskType.REGRESSION,
        TaskType.TIME_SERIES,
        TaskType.CLUSTERING,
    ):
        for candidate in candidate_catalog(task):
            names.add(candidate.name)
            from sklearn.base import clone

            values = {}
            for key in PARAMETER_MODELS[candidate.name].model_fields:
                dimension = candidate.search_space.get(f"model__{key}")
                value = (
                    (dimension.low if hasattr(dimension, "low") else dimension.categories[0])
                    if dimension is not None
                    else 3
                )
                values[key] = value.item() if isinstance(value, np.generic) else value
            parsed = PARAMETER_OVERRIDES.validate_python(
                [{"estimator": candidate.name, "parameters": values}]
            )[0]
            fixed = parsed.parameters.model_dump(exclude_unset=True)
            configured = clone(candidate.estimator).set_params(**fixed)
            assert all(
                configured.get_params(deep=False)[name] == value for name, value in fixed.items()
            )
            assert PARAMETER_OVERRIDES.validate_python(
                [{"estimator": candidate.name, "parameters": {}}]
            )
    assert names == set(PARAMETER_MODELS)


@pytest.mark.parametrize("rows", [None, "Ridge", [None], [{"parameters": {}}]])
def test_malformed_stored_parameter_rows_fail_with_validation_error(rows):
    from automl_api.training.parameter_overrides import parse_overrides

    with pytest.raises(ValidationError):
        parse_overrides(rows)


@pytest.mark.parametrize("suggestion", [None, {"model__fit_intercept": 1}])
def test_trial_suggestions_cannot_change_the_fixed_value_type(parameter_trials, suggestion):
    from automl_api.training.parameter_overrides import validate_trial_overrides

    engine, run, scope = parameter_trials
    with Session(engine) as db:
        candidate = db.get(TrainingCandidate, scope[4])
        with pytest.raises(ValueError, match="suggestion"):
            validate_trial_overrides(
                run,
                candidate,
                {
                    "search": {"base_parameters": {"alpha": 2.0, "fit_intercept": True}},
                    "suggestion": suggestion,
                },
            )


def test_erasing_candidate_parameters_cannot_remove_the_trial_policy(parameter_trials):
    from automl_api.training.parameter_overrides import validate_trial_overrides

    engine, run, scope = parameter_trials
    with Session(engine) as db:
        candidate = db.get(TrainingCandidate, scope[4])
        candidate.params = {}
        with pytest.raises(ValueError, match="policy differs"):
            validate_trial_overrides(run, candidate, {})


def test_real_tune_search_preserves_fixed_alpha_and_replays(parameter_trials, tmp_path):
    import ray
    from automl_api.models.workflows import TrainingTrial
    from automl_api.services.workflow_state import canonical_request_hash
    from sklearn.linear_model import Ridge
    from skopt.space import Categorical

    engine, run, scope = parameter_trials
    fixed = {"alpha": 2.0}
    rows = [{"estimator": "Ridge", "parameters": fixed}]
    run.params = {
        **run.params,
        "parameter_overrides": rows,
        "parameter_overrides_digest": overrides_digest(rows),
    }
    with Session(engine) as db, db.begin():
        db.get(ModelRun, run.id).params = run.params
        db.get(TrainingCandidate, scope[4]).params = {
            "parameter_overrides": fixed,
            "parameter_overrides_digest": canonical_request_hash(fixed),
        }
    ray.shutdown()
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        object_store_memory=256 * 1024**2,
        runtime_env={"env_vars": {"AUTOML_ATTEMPT_ID": "", "AUTOML_FENCING_TOKEN": ""}},
    )
    try:
        model = Ridge(alpha=2.0)
        frame = pd.DataFrame({"x": np.arange(36, dtype=float)})
        results = [
            tune_runtime.search_candidate(
                model,
                {"fit_intercept": Categorical([True, False])},
                frame,
                frame.x * 2 + 1,
                3,
                "neg_root_mean_squared_error",
                iterations=2,
                max_concurrent=1,
                storage_path=str(tmp_path),
                name=name,
                run=run,
                candidate_name="Ridge",
                base_parameters=model.get_params(),
            )
            for name in ("fixed-initial", "fixed-replay")
        ]
        assert results[0]["params"] == results[1]["params"]
        assert results[0]["mean"] == results[1]["mean"]
        with Session(engine) as db:
            trials = list(
                db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
            )
            assert len(trials) == 2
            assert all(trial.status == "succeeded" and trial.result_digest for trial in trials)
            assert all(
                trial.parameters["search"]["base_parameters"]["alpha"] == 2.0
                and "alpha" not in trial.parameters["suggestion"]
                for trial in trials
            )
    finally:
        ray.shutdown()
