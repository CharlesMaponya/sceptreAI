"""Prevent resplitting prepared validation data and refitting restored candidates."""

import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest
from automl_api.models.enums import TaskType
from automl_api.training import candidate_runtime, pipeline
from automl_api.training.model_catalog import CandidateSpec


@pytest.fixture
def experiment(monkeypatch):
    run = SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        task_type=TaskType.REGRESSION,
        target_column="target",
        params={"cv_folds": 2},
        tags={},
    )
    train = pd.DataFrame({"x": range(20), "target": [i % 2 for i in range(20)]})
    validation = pd.DataFrame({"x": range(100, 120), "target": [i % 2 for i in range(20)]})
    monkeypatch.setattr(
        pipeline, "detect_target_leakage", lambda *args: SimpleNamespace(excluded_columns=[])
    )
    monkeypatch.setattr(pipeline, "_persist_partial_leaderboard", MagicMock())
    return run, train, validation


@pytest.mark.parametrize(
    "task", [TaskType.REGRESSION, TaskType.CLASSIFICATION, TaskType.TIME_SERIES]
)
def test_prepared_validation_stays_separate_and_remote_winner_is_loaded_only_once(
    experiment, monkeypatch, task
):
    run, train, validation = experiment
    run.task_type = task
    metric = "accuracy" if task == TaskType.CLASSIFICATION else "rmse"
    run.params["primary_metric"] = metric
    if task == TaskType.CLASSIFICATION:
        run.params["positive_label"] = "1"
    specs = [CandidateSpec(name, MagicMock(), {}, "low", True) for name in ["First", "Second"]]
    monkeypatch.setattr(pipeline, "select_candidates", lambda *a: specs)
    monkeypatch.setattr(candidate_runtime, "enabled", lambda: True)
    captured = []

    def results(kind, jobs, snapshot):
        captured.extend(jobs)
        for index, candidate, arguments in reversed(jobs):
            yield (
                index,
                {
                    "model": candidate.name,
                    "status": "succeeded",
                    "metrics": {metric: 1 if index else 2},
                    "best_params": {},
                    "diagnostics": {},
                },
            )

    monkeypatch.setattr(candidate_runtime, "results", results)
    restore = MagicMock(return_value={"_model": "restored"})
    monkeypatch.setattr(pipeline, "_restore_candidate", restore)
    result = pipeline._fit_model(train, run, validation)
    assert result.model == "restored"
    winner = "First" if task == TaskType.CLASSIFICATION else "Second"
    assert result.params["winner"] == winner
    restore.assert_called_once()
    assert restore.call_args.args[1]["model"] == winner
    for _, _, arguments in captured:
        assert list(arguments[0]["x"]) == list(range(20))
        assert list(arguments[2]["x"]) == list(range(100, 120))
    assert all(
        entry["training_rows"] == entry["validation_rows"] == 20 for entry in result.leaderboard
    )


@pytest.mark.parametrize(
    "problem,message",
    [
        ("target", "missing the configured target"),
        ("feature", "missing training features"),
        ("missing_labels", "no rows with a target"),
    ],
)
def test_invalid_prepared_validation_fails_before_any_fit(
    experiment, monkeypatch, problem, message
):
    run, train, validation = experiment
    if problem == "target":
        validation = validation.drop(columns="target")
    elif problem == "feature":
        validation = validation.drop(columns="x")
    else:
        validation["target"] = None
    fit = MagicMock()
    monkeypatch.setattr(candidate_runtime, "results", fit)
    with pytest.raises(ValueError, match=message):
        pipeline._fit_model(train, run, validation)
    fit.assert_not_called()


@pytest.mark.parametrize("runtime", ["remote", "local"])
def test_checkpoint_reuse_never_submits_a_completed_candidate(experiment, monkeypatch, runtime):
    run, _, _ = experiment
    specs = [CandidateSpec(name, MagicMock(), {}, "low", True) for name in ["Done", "Next"]]
    completed = {"Done": {"model": "Done", "status": "succeeded", "metrics": {"rmse": 1}}}
    monkeypatch.setattr(candidate_runtime, "enabled", lambda: runtime == "remote")
    restored = MagicMock(return_value={**completed["Done"], "_model": "saved"})
    monkeypatch.setattr(pipeline, "_restore_candidate", restored)
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    fit = MagicMock(return_value={"model": "Next", "status": "succeeded"})
    monkeypatch.setattr(pipeline, "_fit_candidate", fit)
    remote = MagicMock(return_value=iter([(1, {"model": "Next", "status": "succeeded"})]))
    monkeypatch.setattr(candidate_runtime, "results", remote)
    results = list(pipeline._candidate_entries("supervised", specs, [(), ()], run, completed))
    assert [index for index, _ in results] == [0, 1]
    if runtime == "remote":
        restored.assert_not_called()
        fit.assert_not_called()
        assert [job[1].name for job in remote.call_args.args[1]] == ["Next"]
    else:
        restored.assert_called_once()
        fit.assert_called_once_with(specs[1], run)
        remote.assert_not_called()


@pytest.mark.parametrize(
    "problem",
    ["absent", "invalid", "unknown", "project", "version", "train", "validation", "temporal"],
)
def test_training_rejects_foreign_or_incomplete_split_bindings(experiment, problem):
    run, _, _ = experiment
    run.dataset_version_id = uuid.uuid4()
    run.params["split_revision_id"] = str(uuid.uuid4())
    split = SimpleNamespace(
        project_id=run.project_id,
        dataset_version_id=run.dataset_version_id,
        specification={"uris": {"train": "train", "validation": "validation"}},
    )
    if problem == "absent":
        run.params.clear()
    elif problem == "invalid":
        run.params["split_revision_id"] = "bad-id"
    elif problem == "unknown":
        split = None
    elif problem == "project":
        split.project_id = uuid.uuid4()
    elif problem == "version":
        split.dataset_version_id = uuid.uuid4()
    elif problem in {"train", "validation"}:
        del split.specification["uris"][problem]
    else:
        run.task_type = TaskType.TIME_SERIES
    db = MagicMock()
    db.get.return_value = split
    with pytest.raises(ValueError):
        pipeline._bound_split_revision(db, run)


@pytest.mark.parametrize(
    "task,max_rows",
    [
        (TaskType.TIME_SERIES, 5),
        (TaskType.CLASSIFICATION, 5),
        (TaskType.CLASSIFICATION, 0),
        (TaskType.REGRESSION, 100),
    ],
)
def test_candidate_sampling_keeps_row_alignment_and_temporal_order(task, max_rows):
    features = pd.DataFrame({"x": range(20)}, index=range(100, 120))
    target = pd.Series([0] * 20, index=features.index)
    sampled_x, sampled_y = pipeline._candidate_training_sample(
        features, target, max_rows=max_rows, task_type=task
    )
    assert sampled_x.index.equals(sampled_y.index)
    expected = 20 if max_rows <= 0 or max_rows >= 20 else max_rows
    assert len(sampled_x) == expected
    if task == TaskType.TIME_SERIES:
        assert list(sampled_x["x"]) == list(range(5))
    if expected == 20:
        assert sampled_x is features and sampled_y is target


def test_prepared_time_series_roles_cannot_read_sealed_final_test(monkeypatch):
    split = SimpleNamespace(
        id=uuid.uuid4(),
        specification={
            "uris": {
                "train": "training-only",
                "validation": "validation-only",
                "final_test": "forbidden",
            },
            "split_counts": {"train": 8000, "validation": 2000},
            "time_column": "timestamp",
        },
    )
    reader = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr(pipeline, "_load_prepared_role", reader)
    pipeline._load_prepared_training_frames(split, sample_rows=4000, task_type=TaskType.TIME_SERIES)
    assert [call.args[0] for call in reader.call_args_list] == ["training-only", "validation-only"]
    assert [call.kwargs["max_rows"] for call in reader.call_args_list] == [4000, 1000]
    assert all(call.kwargs["order_column"] == "timestamp" for call in reader.call_args_list)
