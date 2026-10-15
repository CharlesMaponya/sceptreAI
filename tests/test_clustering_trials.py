import pandas as pd
import pytest
from automl_api.models.enums import AttemptStatus, TaskType
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import TrainingCandidate, TrainingTrial, WorkflowAttempt
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline
from automl_api.training.model_catalog import CandidateSpec
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.datasets import make_blobs
from sklearn.model_selection import KFold
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_pipeline_edge_contracts import _mock_cluster_runtime
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401


@pytest.fixture
def cluster_runtime(runtime, monkeypatch):  # noqa: F811
    engine, run, scope = runtime
    original_filter = pipeline.CorrelatedFeatureFilter
    original_prepare = pipeline._preprocessor
    original_evaluate = pipeline.clustering_evaluation
    original_aggregate = pipeline.aggregate_fold_metrics
    _mock_cluster_runtime(monkeypatch)
    monkeypatch.setattr(pipeline, "CorrelatedFeatureFilter", original_filter)
    monkeypatch.setattr(pipeline, "_preprocessor", original_prepare)
    monkeypatch.setattr(pipeline, "clustering_evaluation", original_evaluate)
    monkeypatch.setattr(pipeline, "aggregate_fold_metrics", original_aggregate)
    with Session(engine) as db, db.begin():
        db.get(TrainingCandidate, scope[4]).estimator_key = "KMeans"
        db.get(ModelRun, run.id).task_type = TaskType.CLUSTERING
    run.task_type = TaskType.CLUSTERING
    features, _ = make_blobs(n_samples=90, centers=4, n_features=3, random_state=42)
    return engine, run, scope, pd.DataFrame(features)


def fit(runtime_scope, *, folds=3):
    _, run, _, features = runtime_scope
    return pipeline._fit_clustering_candidate(
        CandidateSpec("KMeans", KMeans(n_init=2, random_state=42), {}, "low", True),
        features,
        None,
        KFold(n_splits=folds, shuffle=True, random_state=42),
        run,
    )


def test_clustering_commits_before_preparation_and_replays_all_parameter_results(
    cluster_runtime, monkeypatch
):
    engine, _, scope, _ = cluster_runtime
    prepare = pipeline._preprocessor
    evaluate = pipeline.clustering_evaluation
    preparation_calls = []
    evaluation_calls = []

    def prepared():
        if len(preparation_calls) < 3:
            with Session(engine) as db:
                child = db.scalar(
                    select(WorkflowAttempt).where(
                        WorkflowAttempt.run_attempt_id == scope[2],
                        WorkflowAttempt.status == AttemptStatus.RUNNING,
                    )
                )
                assert child is not None and child.lease_owner
        preparation_calls.append(True)
        return prepare()

    def evaluated(*args):
        evaluation_calls.append(True)
        return evaluate(*args)

    monkeypatch.setattr(pipeline, "_preprocessor", prepared)
    monkeypatch.setattr(pipeline, "clustering_evaluation", evaluated)
    first = fit(cluster_runtime)
    assert first["status"] == "succeeded", first.get("error")
    assert len(evaluation_calls) == 21
    second = fit(cluster_runtime)
    assert second["status"] == "succeeded", second.get("error")
    assert second["best_params"] == first["best_params"]
    assert second["metrics"] == first["metrics"]
    assert len(evaluation_calls) == 21
    assert len(preparation_calls) == 5  # Three CV folds, then one full-data fit per call.
    with Session(engine) as db:
        rows = list(db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4])))
        assert len(rows) == 7 and all(row.status == "succeeded" for row in rows)
        assert all(row.result_digest for row in rows)


def test_changed_clustering_contract_rejected_before_preparation(cluster_runtime, monkeypatch):
    assert fit(cluster_runtime)["status"] == "succeeded"
    monkeypatch.setattr(pipeline, "_preprocessor", lambda: pytest.fail("unexpected preparation"))
    with pytest.raises(StaleFence):
        fit(cluster_runtime, folds=2)


def test_parent_loss_between_clustering_parameters_stops_computation(cluster_runtime, monkeypatch):
    engine, _, scope, _ = cluster_runtime
    original = pipeline.clustering_evaluation
    calls = []

    def evaluate(*args):
        result = original(*args)
        calls.append(True)
        if len(calls) == 3:
            with Session(engine) as db, db.begin():
                db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
        return result

    monkeypatch.setattr(pipeline, "clustering_evaluation", evaluate)
    with pytest.raises(StaleFence):
        fit(cluster_runtime)
    assert len(calls) == 3


def test_clustering_failure_is_durable_and_not_recomputed(cluster_runtime, monkeypatch):
    calls = []

    def evaluate(*args):
        calls.append(True)
        raise ValueError("invalid clusters fixture")

    monkeypatch.setattr(pipeline, "clustering_evaluation", evaluate)
    for _ in range(2):
        result = fit(cluster_runtime)
        assert result["status"] == "failed"
        assert "invalid clusters fixture" in result["error"]
    assert calls == [True]


def test_clustering_without_predict_replays_fold_results(cluster_runtime, monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda *_a, **_k: (AgglomerativeClustering(), "cpu"),
    )
    evaluate = pipeline.clustering_evaluation
    calls = []

    def evaluated(*args):
        calls.append(True)
        return evaluate(*args)

    monkeypatch.setattr(pipeline, "clustering_evaluation", evaluated)
    first = fit(cluster_runtime)
    second = fit(cluster_runtime)
    assert first["status"] == second["status"] == "succeeded"
    assert first["best_params"] == second["best_params"]
    assert first["metrics"] == second["metrics"]
    assert len(calls) == 21
