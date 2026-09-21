from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.training import candidate_runtime, pipeline


def setup_runtime(monkeypatch):
    monkeypatch.setenv("AUTOML_MODEL_PODS", "1")
    monkeypatch.setenv("AUTOML_CPU_THREADS", "4")
    monkeypatch.delenv("AUTOML_GPU_VENDOR", raising=False)
    monkeypatch.setattr(
        pipeline.mlflow,
        "active_run",
        lambda: SimpleNamespace(info=SimpleNamespace(run_id="parent", experiment_id="experiment")),
    )
    run = SimpleNamespace(
        id="run",
        project_id="project",
        params={},
        tags={},
        task_type="regression",
        target_column="target",
    )
    candidates = [SimpleNamespace(name=name, cost_tier="low") for name in ("A", "B")]
    executor = MagicMock()
    executor.remote.side_effect = ["a-ref", "b-ref"]
    remote = MagicMock(return_value=lambda _: executor)
    monkeypatch.setattr(candidate_runtime.ray, "remote", remote)
    monkeypatch.setattr(candidate_runtime.ray, "put", lambda args: args)
    monkeypatch.setattr(candidate_runtime.ray, "wait", lambda refs, **kw: ([refs[-1]], refs[:-1]))
    cancel = MagicMock()
    monkeypatch.setattr(candidate_runtime.ray, "cancel", cancel)
    jobs = [(index, candidate, ("frames",)) for index, candidate in enumerate(candidates)]
    return run, jobs, remote, executor, cancel


def test_whole_worker_cpu_reservation_and_completion_order(monkeypatch):
    run, jobs, remote, executor, cancel = setup_runtime(monkeypatch)
    monkeypatch.setattr(candidate_runtime.ray, "get", lambda ref: {"reference": ref})
    assert list(candidate_runtime.results("supervised", jobs, run)) == [
        (1, {"reference": "b-ref"}),
        (0, {"reference": "a-ref"}),
    ]
    assert remote.call_args.kwargs == {
        "num_cpus": 4.0,
        "num_gpus": 0,
        "max_calls": 1,
        "max_retries": 0,
    }
    assert executor.remote.call_count == 2
    assert executor.remote.call_args.args[-1]["tags"]["candidate_parent_run_id"] == "parent"
    cancel.assert_not_called()


def test_candidate_worker_crash_does_not_restart_other_models(monkeypatch):
    run, jobs, _, _, cancel = setup_runtime(monkeypatch)

    def get(reference):
        if reference == "b-ref":
            raise candidate_runtime.ray.exceptions.WorkerCrashedError()
        return {"model": "A", "status": "succeeded"}

    monkeypatch.setattr(candidate_runtime.ray, "get", get)
    entries = list(candidate_runtime.results("supervised", jobs, run))
    assert entries[0][1]["model"] == "B"
    assert entries[0][1]["status"] == "failed"
    assert entries[1][1]["status"] == "succeeded"
    cancel.assert_not_called()


def test_abandoning_tournament_cancels_unfinished_tasks(monkeypatch):
    run, jobs, _, _, cancel = setup_runtime(monkeypatch)
    monkeypatch.setattr(candidate_runtime.ray, "get", lambda ref: {})
    results = candidate_runtime.results("supervised", jobs, run)
    next(results)
    results.close()
    cancel.assert_called_once_with("a-ref", force=True)


@pytest.mark.parametrize("kind", ["supervised", "clustering"])
def test_candidate_process_returns_artifact_metadata_without_fitted_model(monkeypatch, kind):
    monkeypatch.setenv("AUTOML_CPU_THREADS", "2")
    monkeypatch.setenv("HOSTNAME", "dedicated-worker")
    monkeypatch.setattr(pipeline.mlflow, "set_tracking_uri", MagicMock())
    monkeypatch.setattr(pipeline.mlflow, "set_experiment", MagicMock())
    monkeypatch.setattr(pipeline, "_persist_candidate_phase", MagicMock())
    fit = MagicMock(
        return_value={
            "status": "succeeded",
            "_model": object(),
            "model_artifact_uri": "s3c://model",
        }
    )
    monkeypatch.setattr(pipeline, "_fit_candidate", fit)
    monkeypatch.setattr(pipeline, "_fit_clustering_candidate", fit)
    entry = candidate_runtime._execute(
        kind,
        SimpleNamespace(name="model"),
        (),
        {
            "id": "run",
            "tags": {"candidate_experiment_id": "experiment"},
        },
    )
    assert entry["worker_pod"] == "dedicated-worker"
    assert entry["phase"] == "succeeded"
    assert "_model" not in entry
    fit.assert_called_once()
    monkeypatch.delenv("TRAINING_EXECUTION_MODE")
