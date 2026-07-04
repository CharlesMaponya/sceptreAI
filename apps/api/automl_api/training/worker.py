from __future__ import annotations

import argparse
import os
import uuid

from automl_api.db.session import get_session_factory
from automl_api.models.enums import RunKind
from automl_api.models.runs import ModelRun
from automl_api.training.analysis import execute_analysis_run
from automl_api.training.pipeline import execute_training_run, tabular_automl_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_id = uuid.UUID(args.run_id)
    with get_session_factory()() as db:
        run = db.get(ModelRun, run_id)
        if run is None:
            raise ValueError(f"Run {run_id} was not found.")
        run_kind = run.run_kind
    execution_mode = os.getenv("TRAINING_EXECUTION_MODE", "direct").lower()
    print(
        f"Starting {run_kind.value} run {args.run_id} in {execution_mode} mode",
        flush=True,
    )
    if run_kind in {RunKind.VALIDATION, RunKind.EXPLAINABILITY, RunKind.DRIFT}:
        execute_analysis_run(run_id)
    elif execution_mode == "zenml":
        tabular_automl_pipeline(run_id=args.run_id)
    else:
        execute_training_run(run_id)
    print(f"Completed {run_kind.value} run {args.run_id}", flush=True)


if __name__ == "__main__":
    main()
