from __future__ import annotations

import sys
import uuid
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from automl_api.models.enums import RunKind, RunStatus, TaskType
from automl_api.training import analysis


class _Db:
    def __init__(self, values: dict[tuple[object, object], object] | None = None) -> None:
        self.values = values or {}
        self.added: list[object] = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def get(self, model: object, identity: object) -> object | None:
        return self.values.get((model, identity))

    def add(self, value: object) -> None:
        self.added.append(value)

    def commit(self) -> None:
        self.commits += 1


def _run(kind: RunKind, **overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": uuid.uuid4(),
        "project_id": uuid.uuid4(),
        "dataset_version_id": uuid.uuid4(),
        "run_kind": kind,
        "status": RunStatus.QUEUED,
        "task_type": TaskType.REGRESSION,
        "target_column": "target",
        "params": {},
        "tags": {},
        "started_at": None,
        "finished_at": None,
        "failure_code": None,
        "failure_message": None,
        "plain_english_failure": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_execute_analysis_run_dispatches_and_persists(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _run(RunKind.VALIDATION)
    version = SimpleNamespace()
    db = _Db(
        {
            (analysis.ModelRun, run.id): run,
            (analysis.DatasetVersion, run.dataset_version_id): version,
        }
    )
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(analysis, "_execute_validation", lambda *_args: {"metrics": {"score": 1}})
    persisted: list[tuple[uuid.UUID, dict]] = []
    monkeypatch.setattr(
        analysis, "_persist_analysis_result", lambda key, result: persisted.append((key, result))
    )
    result = analysis.execute_analysis_run(run.id)
    assert result == {"metrics": {"score": 1}}
    assert run.status == RunStatus.RUNNING and run.started_at is not None
    assert persisted == [(run.id, result)]


@pytest.mark.parametrize("missing", ["run", "version"])
def test_execute_analysis_run_rejects_missing_state(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    run_id = uuid.uuid4()
    run = None if missing == "run" else _run(RunKind.VALIDATION, id=run_id)
    values = {} if run is None else {(analysis.ModelRun, run_id): run}
    db = _Db(values)
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    with pytest.raises(ValueError, match="was not found|version was not found"):
        analysis.execute_analysis_run(run_id)


def test_execute_analysis_run_marks_unsupported_kind_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run(RunKind.TRAINING)
    db = _Db(
        {
            (analysis.ModelRun, run.id): run,
            (analysis.DatasetVersion, run.dataset_version_id): object(),
        }
    )
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    failures: list[Exception] = []
    monkeypatch.setattr(analysis, "_mark_analysis_failed", lambda _id, exc: failures.append(exc))
    with pytest.raises(ValueError, match="Unsupported analysis kind"):
        analysis.execute_analysis_run(run.id)
    assert len(failures) == 1


def test_clustering_external_validation_predict_and_refit(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = pd.DataFrame({"x": [0.0, 0.1, 9.0, 9.1], "truth": [0, 0, 1, 1]})
    monkeypatch.setattr(analysis, "_load_dataframe", lambda _version: frame.copy())
    monkeypatch.setattr(analysis, "_transformed_features", lambda _model, values: values)
    monkeypatch.setattr(
        analysis,
        "clustering_evaluation",
        lambda features, labels, reference: {
            "rows": float(len(features)),
            "referenced": float(reference is not None),
        },
    )
    run = _run(
        RunKind.VALIDATION,
        task_type=TaskType.CLUSTERING,
        target_column=None,
        params={"evaluation_column": "truth"},
    )

    predictive = SimpleNamespace(predict=lambda values: np.asarray([0, 0, 1, 1]))
    monkeypatch.setattr(analysis, "_load_model", lambda _run: predictive)
    result = analysis._execute_validation(run, object())
    assert result["diagnostics"]["validation_mode"] == "external_predict"
    assert result["diagnostics"]["external_evaluation"] is True

    refit = SimpleNamespace(fit_predict=lambda values: np.asarray([0, 0, 1, 1]))
    monkeypatch.setattr(analysis, "_load_model", lambda _run: refit)
    run.params = {}
    result = analysis._execute_validation(run, object())
    assert result["diagnostics"]["validation_mode"] == "external_refit"
    assert result["diagnostics"]["external_evaluation"] is False


def test_supervised_validation_rejects_missing_target(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(analysis, "_load_model", lambda _run: object())
    monkeypatch.setattr(analysis, "_load_dataframe", lambda _version: pd.DataFrame({"x": [1]}))
    with pytest.raises(ValueError, match="target column is missing"):
        analysis._execute_validation(_run(RunKind.VALIDATION), object())


def test_drift_execution_validates_reference_and_common_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference_id = uuid.uuid4()
    run = _run(
        RunKind.DRIFT, params={"reference_dataset_version_id": str(reference_id), "max_rows": 1}
    )
    current = SimpleNamespace(id=uuid.uuid4())
    with pytest.raises(ValueError, match="required"):
        analysis._execute_drift(_run(RunKind.DRIFT), current)

    reference = SimpleNamespace(id=reference_id, project_id=run.project_id)
    db = _Db({(analysis.DatasetVersion, reference_id): reference})
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    frames = {
        id(reference): pd.DataFrame({"a": range(150), "target": range(150)}),
        id(current): pd.DataFrame({"a": range(150), "target": range(150)}),
    }
    monkeypatch.setattr(analysis, "_load_dataframe", lambda version: frames[id(version)].copy())
    monkeypatch.setattr(
        analysis,
        "_run_evidently_report",
        lambda *_args: {"dataset_drift": False, "number_of_drifted_columns": 0},
    )
    result = analysis._execute_drift(run, current)
    assert result["diagnostics"]["reference_rows"] == 100
    assert result["diagnostics"]["current_rows"] == 100

    reference.project_id = uuid.uuid4()
    with pytest.raises(ValueError, match="not found"):
        analysis._execute_drift(run, current)
    reference.project_id = run.project_id
    frames[id(reference)] = pd.DataFrame({"left": [1]})
    frames[id(current)] = pd.DataFrame({"right": [1]})
    with pytest.raises(ValueError, match="no common"):
        analysis._execute_drift(run, current)


def test_first_nested_value_and_drift_summary_boundaries() -> None:
    nested = {"a": [{"b": {"wanted": 7}}]}
    assert analysis._first_nested_value(nested, "wanted") == 7
    assert analysis._first_nested_value(nested, "missing", "fallback") == "fallback"
    metrics, details = analysis._drift_summary(
        {
            "share_of_drifted_columns": 150,
            "drift_by_columns": {
                "a": {"drifted": True},
                "b": {"detected": True},
                "c": "ignored",
            },
        },
        3,
    )
    assert metrics["drift_share"] == 1
    assert details["dataset_drift"] is True
    metrics, details = analysis._drift_summary({"share_of_drifted_columns": -2}, 0)
    assert metrics["drift_share"] == 0 and details["dataset_drift"] is False


def test_evidently_report_supports_snapshot_and_legacy_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Preset:
        pass

    class Snapshot:
        def as_dict(self):
            return {"format": "snapshot"}

    class Report:
        def __init__(self, metrics=None):
            self.metrics = metrics

        def run(self, **_kwargs):
            return Snapshot()

    monkeypatch.setitem(
        sys.modules, "evidently.metric_preset", SimpleNamespace(DataDriftPreset=Preset)
    )
    monkeypatch.setitem(sys.modules, "evidently.report", SimpleNamespace(Report=Report))
    assert analysis._run_evidently_report(pd.DataFrame(), pd.DataFrame()) == {"format": "snapshot"}

    class DictSnapshot:
        def dict(self):
            return {"format": "dict"}

    class LegacyReport(Report):
        def __init__(self, positional):
            if not isinstance(positional, list):
                raise TypeError

        def run(self, **_kwargs):
            return DictSnapshot()

    monkeypatch.setitem(sys.modules, "evidently.report", SimpleNamespace(Report=LegacyReport))
    assert analysis._run_evidently_report(pd.DataFrame(), pd.DataFrame()) == {"format": "dict"}


def test_model_loading_paths_and_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    run = _run(RunKind.VALIDATION, params={"model_mlflow_run_id": "123"})
    monkeypatch.setattr(analysis.mlflow_sklearn, "load_model", lambda _uri: {"source": "mlflow"})
    assert analysis._load_model(run) == {"source": "mlflow"}

    monkeypatch.setattr(
        analysis.mlflow_sklearn,
        "load_model",
        lambda _uri: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    with pytest.raises(ValueError, match="MLflow model loading failed"):
        analysis._load_model(run)
    with pytest.raises(ValueError, match="no persisted artifact"):
        analysis._load_model(_run(RunKind.VALIDATION))

    explanation = _run(RunKind.EXPLAINABILITY)
    monkeypatch.setattr(analysis, "_rebuild_historical_model", lambda *_args: {"source": "rebuilt"})
    assert analysis._load_model(explanation, object()) == {"source": "rebuilt"}


def test_clustering_predictor_and_dense_transform_boundaries() -> None:
    direct = SimpleNamespace(predict=lambda frame: np.zeros(len(frame)))
    assert analysis._clustering_predictor(direct, pd.DataFrame({"x": [1]})) is direct.predict
    with pytest.raises(ValueError, match="cannot assign"):
        analysis._clustering_predictor(object(), pd.DataFrame({"x": [1]}))
    broken = SimpleNamespace(named_steps={"model": SimpleNamespace(labels_=[])})
    with pytest.raises(ValueError, match="no reusable fitted labels"):
        analysis._clustering_predictor(broken, pd.DataFrame({"x": [1]}))

    sparse = SimpleNamespace(toarray=lambda: [[1, 2]])
    assert analysis._dense_array(sparse).tolist() == [[1.0, 2.0]]
    prepare = SimpleNamespace(transform=lambda frame: np.asarray([[9]]))
    assert analysis._transformed_features(
        SimpleNamespace(named_steps={"prepare": prepare}), pd.DataFrame()
    ).tolist() == [[9]]
    frame = pd.DataFrame({"x": [1]})
    assert analysis._transformed_features(object(), frame) is frame


def test_persist_analysis_result_records_artifact_metrics_and_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _run(RunKind.DRIFT, params={"source_training_run_id": "source", "model_name": "Ridge"})
    db = _Db({(analysis.ModelRun, run.id): run})
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    stored = SimpleNamespace(uri="s3://drift")
    monkeypatch.setattr(
        analysis, "get_object_store", lambda: SimpleNamespace(put_bytes=lambda *_args: stored)
    )
    analysis._persist_analysis_result(
        run.id,
        {"metrics": {"rmse": np.float64(2)}, "diagnostics": {"ok": True}, "feature_importance": []},
    )
    assert run.status == RunStatus.SUCCEEDED and run.tags["artifact_uri"] == "s3://drift"
    assert len(db.added) == 2 and db.commits == 1

    missing_db = _Db()
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: missing_db)
    analysis._persist_analysis_result(run.id, {})
    assert missing_db.added == []


@pytest.mark.parametrize(
    ("message", "phrase"),
    [
        ("unknown column", "feature space"),
        ("MLflow artifact missing", "artifact could not be loaded"),
        ("OOM memory", "memory budget"),
        ("other", "Validation or explainability failed"),
    ],
)
def test_mark_analysis_failed_assigns_actionable_remediation(
    monkeypatch: pytest.MonkeyPatch, message: str, phrase: str
) -> None:
    run = _run(RunKind.VALIDATION)
    db = _Db({(analysis.ModelRun, run.id): run})
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    analysis._mark_analysis_failed(run.id, RuntimeError(message))
    assert run.status == RunStatus.FAILED
    assert phrase in run.plain_english_failure


def test_json_default_preserves_numpy_scalars() -> None:
    assert analysis._json_default(np.int64(4)) == 4
    assert analysis._json_default(uuid.UUID(int=0)) == str(uuid.UUID(int=0))


@pytest.mark.parametrize(
    ("kind", "handler"),
    [
        (RunKind.EXPLAINABILITY, "_execute_explainability"),
        (RunKind.DRIFT, "_execute_drift"),
    ],
)
def test_execute_analysis_dispatches_remaining_kinds(
    monkeypatch: pytest.MonkeyPatch, kind: RunKind, handler: str
) -> None:
    run = _run(kind)
    version = object()
    db = _Db(
        {
            (analysis.ModelRun, run.id): run,
            (analysis.DatasetVersion, run.dataset_version_id): version,
        }
    )
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(analysis, handler, lambda *_args: {"metrics": {}})
    monkeypatch.setattr(analysis, "_persist_analysis_result", lambda *_args: None)
    assert analysis.execute_analysis_run(run.id) == {"metrics": {}}


def test_feature_importance_and_shap_encoding_cover_degenerate_values() -> None:
    assert analysis.normalize_feature_importance([]) == []
    normalized = analysis.normalize_feature_importance(
        [
            {"feature": "finite", "mean_absolute_shap": 2.0},
            {"feature": "nan", "mean_absolute_shap": np.nan},
        ]
    )
    assert normalized[0]["feature"] == "finite"
    assert normalized[0]["contribution_percent"] == 100.0

    frame = pd.DataFrame({"number": [1.0, 2.0], "category": ["a", "b"]})
    encoded, decode = analysis._encode_shap_features(frame)
    assert encoded["category"].tolist() == [0.0, 1.0]
    decoded = decode(np.asarray([[3.0, -1.0], [4.0, 99.0]]))
    assert decoded["number"].tolist() == [3.0, 4.0]
    assert decoded["category"].isna().all()


def test_load_model_reads_object_store_mirror(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"artifact"
    monkeypatch.setattr(
        analysis,
        "get_object_store",
        lambda: SimpleNamespace(read_bytes=lambda _uri: payload),
    )
    monkeypatch.setattr(analysis.joblib, "load", lambda stream: stream.read())
    run = _run(RunKind.VALIDATION, params={"model_artifact_uri": "s3://model"})
    assert analysis._load_model(run) == payload


def test_rebuild_historical_model_validates_metadata_source_and_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = SimpleNamespace()
    with pytest.raises(ValueError, match="metadata is missing"):
        analysis._rebuild_historical_model(_run(RunKind.EXPLAINABILITY), version)

    source_id = uuid.uuid4()
    run = _run(
        RunKind.EXPLAINABILITY,
        params={"source_training_run_id": str(source_id), "model_name": "Ridge"},
    )
    db = _Db()
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    with pytest.raises(ValueError, match="source training run is missing"):
        analysis._rebuild_historical_model(run, version)

    source = _run(RunKind.TRAINING, id=source_id, tags={"leaderboard": []})
    db.values[(analysis.ModelRun, source_id)] = source
    with pytest.raises(ValueError, match="leaderboard entry is missing"):
        analysis._rebuild_historical_model(run, version)


def test_mark_analysis_failed_returns_when_run_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _Db()
    monkeypatch.setattr(analysis, "get_session_factory", lambda: lambda: db)
    analysis._mark_analysis_failed(uuid.uuid4(), RuntimeError("gone"))
    assert db.commits == 0
