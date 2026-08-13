from __future__ import annotations

import io
import uuid
from contextlib import AbstractContextManager
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import RunStatus, TaskType
from automl_api.training import pipeline
from automl_api.training.model_catalog import CandidateSpec
from automl_api.training.pipeline import TournamentResult


class _Session(AbstractContextManager):
    def __init__(self, run: object | None, version: object | None = None) -> None:
        self.run = run
        self.version = version
        self.added: list[object] = []
        self.commits = 0

    def __enter__(self) -> _Session:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def scalar(self, _statement: object) -> object | None:
        return self.run

    def get(self, model: object, _identifier: object) -> object | None:
        return self.version if model is DatasetVersion else None

    def add(self, instance: object) -> None:
        self.added.append(instance)

    def commit(self) -> None:
        self.commits += 1


class _MlflowRun(AbstractContextManager):
    def __init__(self, run_id: str = "mlflow-run") -> None:
        self.info = SimpleNamespace(run_id=run_id)

    def __enter__(self) -> _MlflowRun:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def _run(
    *,
    status: RunStatus = RunStatus.QUEUED,
    task_type: TaskType = TaskType.REGRESSION,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        status=status,
        task_type=task_type,
        run_name="runtime-test",
        target_column="target",
        params={},
        tags={},
        started_at=None,
        finished_at=None,
        mlflow_run_id=None,
        failure_code=None,
        failure_message=None,
        plain_english_failure=None,
    )


def _result() -> TournamentResult:
    return TournamentResult(
        metrics={"rmse": 1.25},
        model=MagicMock(),
        params={
            "winner": "Ridge",
            "positive_label": None,
            "excluded_leakage_columns": ["target_copy"],
            "deduplicated_rows": 2,
        },
        leaderboard=[
            {
                "rank": 1,
                "model": "Ridge",
                "status": "succeeded",
                "mlflow_run_id": "candidate-run",
                "metrics": {"rmse": 1.25},
            }
        ],
        primary_metric="rmse",
    )


def test_execute_training_run_persists_mlflow_and_database_results(monkeypatch) -> None:
    run = _run()
    version = SimpleNamespace(object_uri="minio://datasets/train.csv")
    session = _Session(run, version)
    result = _result()
    persisted: list[tuple[uuid.UUID, TournamentResult, str]] = []
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: session)
    monkeypatch.setattr(pipeline, "_load_dataframe", lambda _version: pd.DataFrame())
    monkeypatch.setattr(pipeline, "_fit_model", lambda _dataframe, _run: result)
    monkeypatch.setattr(
        pipeline,
        "_persist_training_success",
        lambda run_id, value, mlflow_id: persisted.append((run_id, value, mlflow_id)) or True,
    )
    monkeypatch.setattr(pipeline, "_log_metrics_synchronously", MagicMock())
    monkeypatch.setattr(pipeline, "_log_sklearn_model", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "start_run", lambda **_kwargs: _MlflowRun())
    monkeypatch.setattr(pipeline.mlflow, "set_tracking_uri", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "set_experiment", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "set_tags", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "log_params", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "log_dict", MagicMock())

    assert pipeline.execute_training_run(run.id) == {"rmse": 1.25}
    assert run.status == RunStatus.RUNNING
    assert run.started_at is not None
    assert persisted == [(run.id, result, "mlflow-run")]
    assert session.commits == 1


def test_execute_training_run_handles_terminal_missing_and_failed_runs(monkeypatch) -> None:
    terminal = _run(status=RunStatus.CANCELLED)
    monkeypatch.setattr(
        pipeline,
        "get_session_factory",
        lambda: lambda: _Session(terminal, SimpleNamespace()),
    )
    assert pipeline.execute_training_run(terminal.id) == {}

    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: _Session(None))
    with pytest.raises(ValueError, match="was not found"):
        pipeline.execute_training_run(uuid.uuid4())

    active = _run()
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: _Session(active, None))
    with pytest.raises(ValueError, match="Dataset version"):
        pipeline.execute_training_run(active.id)

    version = SimpleNamespace()
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: _Session(active, version))
    monkeypatch.setattr(
        pipeline,
        "_load_dataframe",
        MagicMock(side_effect=RuntimeError("corrupt object")),
    )
    failed = MagicMock()
    monkeypatch.setattr(pipeline, "_mark_failed", failed)
    with pytest.raises(RuntimeError, match="corrupt object"):
        pipeline.execute_training_run(active.id)
    failed.assert_called_once()


@pytest.mark.parametrize(
    ("filename", "reader"),
    [
        ("train.csv", "read_csv"),
        ("train.json", "read_json"),
        ("train.jsonl", "read_json"),
        ("train.ndjson", "read_json"),
        ("train.parquet", "read_parquet"),
        ("train.xlsx", "read_excel"),
        ("train.xls", "read_excel"),
    ],
)
def test_dataframe_loader_dispatches_by_immutable_filename(
    monkeypatch,
    filename: str,
    reader: str,
) -> None:
    expected = pd.DataFrame({"value": [1]})
    store = MagicMock()
    store.read_bytes.return_value = b"content"
    monkeypatch.setattr(pipeline, "get_object_store", lambda: store)
    read = MagicMock(return_value=expected)
    monkeypatch.setattr(pipeline.pd, reader, read)
    version = SimpleNamespace(object_uri="minio://datasets/train", original_filename=filename)

    assert pipeline._load_dataframe(version) is expected
    assert isinstance(read.call_args.args[0], io.BytesIO)
    if filename.endswith((".jsonl", ".ndjson")):
        assert read.call_args.kwargs["lines"] is True
    elif filename.endswith(".json"):
        assert read.call_args.kwargs["lines"] is False


def test_dataframe_loader_rejects_unknown_format(monkeypatch) -> None:
    store = MagicMock()
    store.read_bytes.return_value = b"content"
    monkeypatch.setattr(pipeline, "get_object_store", lambda: store)
    with pytest.raises(ValueError, match="Unsupported training dataset format"):
        pipeline._load_dataframe(
            SimpleNamespace(object_uri="minio://datasets/train", original_filename="train.txt")
        )


def test_fit_model_validates_target_rows_features_and_candidate_catalog(monkeypatch) -> None:
    run = _run()
    with pytest.raises(ValueError, match="target column is missing"):
        pipeline._fit_model(pd.DataFrame({"feature": range(12)}), run)

    data = pd.DataFrame({"target": range(12), "target_copy": range(12)})
    leakage = SimpleNamespace(excluded_columns=["target_copy"])
    monkeypatch.setattr(pipeline, "detect_target_leakage", lambda *_args: leakage)
    with pytest.raises(ValueError, match="No training features remain"):
        pipeline._fit_model(data, run)

    small = pd.DataFrame({"feature": range(9), "target": range(9)})
    monkeypatch.setattr(
        pipeline,
        "detect_target_leakage",
        lambda *_args: SimpleNamespace(excluded_columns=[]),
    )
    with pytest.raises(ValueError, match="At least 10 rows"):
        pipeline._fit_model(small, run)

    data = pd.DataFrame({"feature": range(12), "target": range(12)})
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [])
    with pytest.raises(ValueError, match="No supported candidates"):
        pipeline._fit_model(data, run)


def test_fit_model_selects_successful_candidate_and_records_data_safety(monkeypatch) -> None:
    run = _run(task_type=TaskType.CLASSIFICATION)
    run.params = {
        "candidate_limit": 3,
        "optimization_iterations": 500,
        "cv_folds": 20,
        "excluded_leakage_columns": ["manual_proxy"],
    }
    rows = 24
    data = pd.DataFrame(
        {
            "feature": np.arange(rows),
            "manual_proxy": np.arange(rows),
            "automatic_proxy": np.arange(rows),
            "target": [1] * 8 + [0] * 16,
        }
    )
    data = pd.concat([data, data.iloc[[0]]], ignore_index=True)
    candidate = CandidateSpec(
        name="FixtureClassifier",
        estimator=MagicMock(),
        search_space={},
        cost_tier="low",
        default_selected=True,
    )
    monkeypatch.setattr(
        pipeline,
        "detect_target_leakage",
        lambda *_args: SimpleNamespace(excluded_columns=["automatic_proxy"]),
    )
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [candidate])
    monkeypatch.setattr(pipeline, "cross_validation_scoring", lambda *_args, **_kwargs: "score")
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    monkeypatch.setattr(pipeline, "_persist_partial_leaderboard", MagicMock())
    fitted_model = MagicMock()

    def fit_candidate(*args: object, **_kwargs: object) -> dict[str, object]:
        assert args[6] == 25
        assert args[7] == 5
        return {
            "rank": None,
            "model": candidate.name,
            "status": "succeeded",
            "cost_tier": "low",
            "primary_score": None,
            "metrics": {"balanced_accuracy": 0.8},
            "diagnostics": {},
            "best_params": {"model__depth": 2},
            "duration_seconds": 0.1,
            "error": None,
            "mlflow_run_id": "candidate-run",
            "_model": fitted_model,
        }

    monkeypatch.setattr(pipeline, "_fit_candidate", fit_candidate)

    result = pipeline._fit_model(data, run)

    assert result.model is fitted_model
    assert result.params["winner"] == candidate.name
    assert result.params["deduplicated_rows"] == 1
    assert result.params["excluded_leakage_columns"] == ["automatic_proxy", "manual_proxy"]
    assert result.params["positive_label"] == "1"


def test_fit_model_reports_all_candidate_failures(monkeypatch) -> None:
    run = _run()
    data = pd.DataFrame({"feature": range(20), "target": range(20)})
    candidate = CandidateSpec("Broken", MagicMock(), {}, "low", True)
    monkeypatch.setattr(
        pipeline,
        "detect_target_leakage",
        lambda *_args: SimpleNamespace(excluded_columns=[]),
    )
    monkeypatch.setattr(pipeline, "select_candidates", lambda *_args: [candidate])
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    monkeypatch.setattr(pipeline, "_persist_partial_leaderboard", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "_fit_candidate",
        lambda *_args, **_kwargs: {
            **pipeline._pending_candidate(candidate),
            "status": "failed",
            "error": "fixture failure",
        },
    )
    with pytest.raises(RuntimeError, match="Every candidate model failed"):
        pipeline._fit_model(data, run)


def _mock_candidate_side_effects(monkeypatch) -> tuple[MagicMock, MagicMock]:
    model = MagicMock()
    model.predict.return_value = np.array([1.0, 2.0])
    model.named_steps = {"correlation": SimpleNamespace(evidence_={"removed": []})}
    monkeypatch.setattr(pipeline, "_runtime_training_resources", lambda: (3, None, False))
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda *_args, **_kwargs: (MagicMock(), "cpu"),
    )
    monkeypatch.setattr(pipeline, "_supervised_model_pipeline", lambda *_args: model)
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "regression_evaluation",
        lambda *_args: ({"rmse": 0.5}, {"residuals": []}),
    )
    monkeypatch.setattr(
        pipeline,
        "classification_evaluation",
        lambda *_args, **_kwargs: ({"balanced_accuracy": 0.75}, {"classes": ["no", "yes"]}),
    )
    monkeypatch.setattr(
        pipeline,
        "_learning_curve_diagnostics",
        lambda *_args, **_kwargs: {"scoring": "fixture", "points": []},
    )
    monkeypatch.setattr(pipeline.mlflow, "active_run", lambda: _MlflowRun("parent-run"))
    monkeypatch.setattr(pipeline.mlflow, "start_run", lambda **_kwargs: _MlflowRun("candidate"))
    for name in ("set_tags", "log_params", "log_dict"):
        monkeypatch.setattr(pipeline.mlflow, name, MagicMock())
    monkeypatch.setattr(pipeline, "_log_metrics_synchronously", MagicMock())
    monkeypatch.setattr(pipeline, "_log_metric_synchronously", MagicMock())
    monkeypatch.setattr(pipeline, "_log_sklearn_model", MagicMock())
    mirror = MagicMock()
    monkeypatch.setattr(pipeline, "_mirror_candidate_evidence_to_parent", mirror)
    monkeypatch.setattr(
        pipeline,
        "_persist_candidate_model",
        lambda *_args: "minio://models/candidate.joblib",
    )
    return model, mirror


def test_fixed_candidate_executes_cv_evaluation_logging_and_persistence(monkeypatch) -> None:
    model, mirror = _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(pipeline, "cross_val_score", lambda *_args, **_kwargs: np.array([0.5, 0.7]))
    candidate = CandidateSpec("Fixture", MagicMock(), {}, "low", True)
    run = _run()
    train_x = pd.DataFrame({"value": range(8)})
    train_y = pd.Series(range(8))
    test_x = pd.DataFrame({"value": [8, 9]})
    test_y = pd.Series([8, 9])

    entry = pipeline._fit_candidate(
        candidate,
        train_x,
        train_y,
        test_x,
        test_y,
        TaskType.REGRESSION,
        5,
        3,
        "neg_root_mean_squared_error",
        run,
    )

    assert entry["status"] == "succeeded"
    assert entry["metrics"] == {"rmse": 0.5}
    assert entry["model_artifact_uri"] == "minio://models/candidate.joblib"
    assert entry["diagnostics"]["cross_validation"] == {
        "folds": 3,
        "scoring": "neg_root_mean_squared_error",
        "mean": 0.6,
        "standard_deviation": pytest.approx(0.1),
    }
    assert entry["diagnostics"]["runtime"]["cpu_threads"] == 3
    assert model.fit.called
    mirror.assert_called_once()


def test_tunable_classification_candidate_uses_search_result(monkeypatch) -> None:
    fitted, _mirror = _mock_candidate_side_effects(monkeypatch)
    fitted.predict.return_value = np.array(["yes", "no"])

    class _Search:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            assert kwargs["n_iter"] == 4
            assert kwargs["scoring"] == "balanced_accuracy"
            self.best_estimator_ = fitted
            self.best_params_ = {"model__depth": np.int64(3)}
            self.best_score_ = 0.8
            self.best_index_ = 0
            self.cv_results_ = {"std_test_score": np.array([0.05])}

        def fit(self, _features: object, _target: object) -> None:
            return None

    monkeypatch.setattr(pipeline, "BayesSearchCV", _Search)
    candidate = CandidateSpec(
        "TunableFixture",
        MagicMock(),
        {"model__depth": [2, 3]},
        "medium",
        True,
    )
    run = _run(task_type=TaskType.CLASSIFICATION)
    run.params = {"positive_label": "yes"}
    entry = pipeline._fit_candidate(
        candidate,
        pd.DataFrame({"value": range(8)}),
        pd.Series(["yes", "no"] * 4),
        pd.DataFrame({"value": [8, 9]}),
        pd.Series(["yes", "no"]),
        TaskType.CLASSIFICATION,
        4,
        3,
        "balanced_accuracy",
        run,
    )

    assert entry["status"] == "succeeded"
    assert entry["best_params"] == {"model__depth": 3}
    assert entry["metrics"] == {"balanced_accuracy": 0.75}


def test_accelerated_candidate_retries_once_on_cpu(monkeypatch) -> None:
    model, _mirror = _mock_candidate_side_effects(monkeypatch)
    accelerators = iter([(MagicMock(), "nvidia"), (MagicMock(), "cpu")])
    monkeypatch.setattr(
        pipeline,
        "configure_estimator_for_training",
        lambda *_args, **_kwargs: next(accelerators),
    )
    attempts = 0

    def cross_validate(*_args: object, **_kwargs: object) -> np.ndarray:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("GPU kernel failed")
        return np.array([0.8, 0.9])

    monkeypatch.setattr(pipeline, "cross_val_score", cross_validate)
    candidate = CandidateSpec("Accelerated", MagicMock(), {}, "high", True)
    entry = pipeline._fit_candidate(
        candidate,
        pd.DataFrame({"value": range(8)}),
        pd.Series(range(8)),
        pd.DataFrame({"value": [8, 9]}),
        pd.Series([8, 9]),
        TaskType.REGRESSION,
        3,
        2,
        "neg_root_mean_squared_error",
        _run(),
    )

    assert attempts == 2
    assert entry["status"] == "succeeded"
    assert model.fit.called


def test_cpu_candidate_failure_is_recorded_without_recursive_retry(monkeypatch) -> None:
    _mock_candidate_side_effects(monkeypatch)
    monkeypatch.setattr(
        pipeline,
        "cross_val_score",
        MagicMock(side_effect=ValueError("invalid training data")),
    )
    failed = MagicMock(return_value={"status": "failed", "error": "invalid training data"})
    monkeypatch.setattr(pipeline, "_failed_candidate", failed)
    candidate = CandidateSpec("Broken", MagicMock(), {}, "low", True)

    entry = pipeline._fit_candidate(
        candidate,
        pd.DataFrame({"value": range(8)}),
        pd.Series(range(8)),
        pd.DataFrame({"value": [8, 9]}),
        pd.Series([8, 9]),
        TaskType.REGRESSION,
        3,
        2,
        "neg_root_mean_squared_error",
        _run(),
    )

    assert entry["status"] == "failed"
    failed.assert_called_once()


def test_training_persistence_is_fenced_and_records_metrics(monkeypatch) -> None:
    result = _result()
    run = _run(status=RunStatus.RUNNING)
    session = _Session(run)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: session)

    assert pipeline._persist_training_success(run.id, result, "mlflow-parent") is True
    assert run.status == RunStatus.SUCCEEDED
    assert run.tags["winner"] == "Ridge"
    assert run.params["excluded_leakage_columns"] == ["target_copy"]
    assert len(session.added) == 1
    assert session.added[0].name == "rmse"
    assert session.added[0].higher_is_better is False

    terminal = _run(status=RunStatus.CANCELLED)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: _Session(terminal))
    assert pipeline._persist_training_success(terminal.id, result, "late-write") is False


def test_training_failure_persistence_respects_terminal_fence(monkeypatch) -> None:
    active = _run(status=RunStatus.RUNNING)
    session = _Session(active)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: session)
    pipeline._mark_failed(active.id, RuntimeError("training exploded"))
    assert active.status == RunStatus.FAILED
    assert active.failure_code == "TRAINING_PIPELINE_FAILED"
    assert active.failure_message == "training exploded"
    assert active.finished_at is not None
    assert session.commits == 1

    terminal = _run(status=RunStatus.CANCELLED)
    terminal_session = _Session(terminal)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: terminal_session)
    pipeline._mark_failed(terminal.id, RuntimeError("late failure"))
    assert terminal.status == RunStatus.CANCELLED
    assert terminal_session.commits == 0
