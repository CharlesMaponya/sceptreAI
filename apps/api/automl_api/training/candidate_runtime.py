"""Run each candidate exclusively on a Ray worker pod, with capacity backpressure."""

from __future__ import annotations

import os
from types import SimpleNamespace

import ray


def enabled() -> bool:
    return os.getenv("AUTOML_MODEL_PODS") == "1"


def _execute(kind, candidate, arguments, snapshot):
    # This process owns all logical CPUs on its worker. Nested Ray Tune trials
    # would wait for those same CPUs forever; search/folds run inside the pod.
    os.environ["TRAINING_EXECUTION_MODE"] = "candidate"
    from threadpoolctl import threadpool_limits

    from automl_api.training.worker import _enable_rapids_accelerator

    _enable_rapids_accelerator()
    from automl_api.training import pipeline

    run = SimpleNamespace(**snapshot)
    pipeline.mlflow.set_tracking_uri(pipeline.get_settings().mlflow_tracking_uri)
    pipeline.mlflow.set_experiment(experiment_id=run.tags["candidate_experiment_id"])
    # Exiting the process after one candidate releases native estimator memory.
    # Bound OpenMP/BLAS as well as estimators that expose n_jobs/thread_count.
    with threadpool_limits(limits=pipeline._runtime_training_resources()[0]):
        fit = (
            pipeline._fit_candidate if kind == "supervised" else pipeline._fit_clustering_candidate
        )
        pipeline._persist_candidate_phase(run.id, candidate.name, "preparing_data")
        entry = fit(candidate, *arguments, run)
    entry.pop("_model", None)
    entry["worker_pod"] = os.getenv("HOSTNAME", "unknown")
    entry["phase"] = entry["status"]
    return entry


def results(kind, jobs, run):
    """Yield finished metadata only; fitted models remain in durable storage.

    All jobs are submitted, but a task reserves the whole worker CPU allocation.
    The Ray autoscaler creates pods up to maxReplicas and Kubernetes schedules
    only pods whose requests fit. Remaining candidates wait without retrying.
    """
    import mlflow

    parent = mlflow.active_run()
    snapshot = {
        "id": run.id,
        "project_id": run.project_id,
        "params": run.params,
        "task_type": run.task_type,
        "target_column": run.target_column,
        "tags": {
            **run.tags,
            "candidate_parent_run_id": parent.info.run_id,
            "candidate_experiment_id": parent.info.experiment_id,
        },
    }
    # Reuse object-store references for the common frames across candidates.
    references = {}
    execute = ray.remote(
        num_cpus=float(os.environ["AUTOML_CPU_THREADS"]),
        num_gpus=1 if os.getenv("AUTOML_GPU_VENDOR") == "nvidia" else 0,
        max_calls=1,
        max_retries=0,
    )(_execute)
    pending = {}
    try:
        for index, candidate, arguments in jobs:
            # A top-level reference is resolved by Ray before invoking the task.
            # Store the tuple once per candidate, preserving shared pandas blocks
            # through Ray's object store rather than returning fitted estimators.
            argument_key = tuple(id(argument) for argument in arguments)
            if argument_key not in references:
                references[argument_key] = ray.put(arguments)
            reference = execute.remote(kind, candidate, references[argument_key], snapshot)
            pending[reference] = (index, candidate)
        while pending:
            ready, _ = ray.wait(list(pending), num_returns=1)
            for reference in ready:
                index, candidate = pending.pop(reference)
                try:
                    entry = ray.get(reference)
                except (
                    ray.exceptions.RayTaskError,
                    ray.exceptions.WorkerCrashedError,
                    ray.exceptions.OutOfMemoryError,
                ) as exc:
                    from automl_api.services.workflow_state import StaleFence

                    if isinstance(exc, ray.exceptions.RayTaskError) and isinstance(
                        exc.as_instanceof_cause(), StaleFence
                    ):
                        raise
                    entry = {
                        "model": candidate.name,
                        "status": "failed",
                        "phase": "failed",
                        "cost_tier": candidate.cost_tier,
                        "metrics": {},
                        "diagnostics": {},
                        "best_params": {},
                        "error": str(exc)[:2000],
                        "duration_seconds": None,
                        "mlflow_run_id": None,
                    }
                yield index, entry
    finally:
        for reference in pending:
            ray.cancel(reference, force=True)
