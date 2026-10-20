import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
import ray
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import TrainingTrial, WorkflowAttempt, WorkflowEvent
from automl_api.services.workflow_state import StaleFence
from automl_api.training import trial_runtime, tune_runtime
from automl_api.training.attempt_admission import TRAINING_PROTOCOL
from sklearn.linear_model import Ridge
from skopt.space import Real
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401


def searcher(runtime_scope, *, iterations=2):
    _, run, scope = runtime_scope
    execution = {
        "run": SimpleNamespace(id=run.id, project_id=run.project_id),
        "parent": (scope[2], scope[3]),
        "candidate_name": "Ridge",
        "specification": {"iterations": iterations, "parameters": Ridge().get_params()},
    }
    return tune_runtime._DurableTuneSearch(
        tune_runtime._dimensions({"alpha": Real(0.01, 10)}),
        metric="cv_score",
        mode="max",
        seed=42,
        max_concurrent=1,
        max_suggestions=iterations,
        execution=execution,
    )


def test_suggestion_commits_pending_attempt_and_dispatch_before_return(runtime):  # noqa: F811
    engine, _, scope = runtime
    search = searcher(runtime)
    config = search.suggest("ray-physical-1")
    assert search.suggest("ray-physical-1") == config
    with Session(engine) as db:
        trial = db.scalar(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.trial_id == trial.id))
        assert attempt.status == AttemptStatus.PENDING and not attempt.lease_owner
        events = list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
        )
        assert len(events) == 1
        assert events[0].payload == {"ray_trial_id": "ray-physical-1", "suggestion_id": "tune:0"}
        assert trial.parameters["suggestion"] == config


def test_changed_dispatch_contract_fails_before_return(runtime):  # noqa: F811
    searcher(runtime).suggest("ray-first")
    with pytest.raises(StaleFence):
        searcher(runtime, iterations=3).suggest("ray-second")


def test_checkpoint_retains_current_context_without_serializing_fences(runtime, tmp_path):  # noqa: F811
    search = searcher(runtime)
    config = search.suggest("physical")
    path = tmp_path / "search.json"
    search.save(str(path))
    assert runtime[2][3] not in path.read_text()
    restored = searcher(runtime)
    context = restored.execution
    restored.restore(str(path))
    assert restored.execution is context
    assert restored.suggest("physical") == config
    json.loads(path.read_text())


def test_superseded_parent_blocks_dispatched_worker(runtime, monkeypatch):  # noqa: F811
    engine, _, scope = runtime
    search = searcher(runtime)
    config = search.suggest("physical")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.SUPERSEDED
    compute = MagicMock(side_effect=AssertionError("unowned compute"))
    monkeypatch.setattr(tune_runtime, "_cv_result", compute)
    with pytest.raises(StaleFence):
        tune_runtime._cv_trial(
            config,
            model=Ridge(),
            features=None,
            target=None,
            cv=3,
            scoring="r2",
            execution=search.execution,
            parameter_protocol=TRAINING_PROTOCOL,
        )
    compute.assert_not_called()


def test_real_tune_workers_commit_and_replay_trials(runtime, tmp_path, monkeypatch):  # noqa: F811
    engine, run, scope = runtime
    # Workers must rely on the explicit context, not ambient parent fence variables.
    ray.shutdown()
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        object_store_memory=256 * 1024**2,
        runtime_env={"env_vars": {"AUTOML_ATTEMPT_ID": "", "AUTOML_FENCING_TOKEN": ""}},
    )
    try:
        frame = pd.DataFrame({"x": np.arange(36, dtype=float)})

        def fit(name):
            return tune_runtime.search_candidate(
                Ridge(),
                {"alpha": Real(0.01, 10)},
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
                base_parameters=Ridge().get_params(),
            )

        first = fit("initial")
        second = fit("replay")
        assert first["params"] == second["params"]
        assert first["mean"] == second["mean"]
        assert first["trials"] == second["trials"] == 2
        with Session(engine) as db:
            trials = list(
                db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
            )
            assert len(trials) == 2 and all(t.status == "succeeded" for t in trials)
            attempts = list(
                db.scalars(
                    select(WorkflowAttempt).where(
                        WorkflowAttempt.trial_id.in_([t.id for t in trials])
                    )
                )
            )
            assert len(attempts) == 2 and all(a.terminal_cas_version == 1 for a in attempts)
            events = list(
                db.scalars(
                    select(WorkflowEvent).where(
                        WorkflowEvent.attempt_id.in_([a.id for a in attempts])
                    )
                )
            )
            assert sum(e.event_type == "trial.terminal" for e in events) == 2
            assert sum(e.event_type == "trial.dispatch" for e in events) == 4
        # A worker replay must use the receipt even if scoring is unavailable now.
        monkeypatch.setattr(
            tune_runtime, "_cv_result", MagicMock(side_effect=AssertionError("rescored"))
        )
        monkeypatch.setattr(tune_runtime.tune, "report", MagicMock())
        search = searcher(runtime)
        # Use the actual persisted production contract and configuration.
        with Session(engine) as db:
            trial = db.get(TrainingTrial, trials[0].id)
            parameters = trial.parameters
        search.execution["specification"] = parameters["search"]
        tune_runtime._cv_trial(
            parameters["suggestion"],
            model=Ridge(),
            features=None,
            target=None,
            cv=3,
            scoring="unused",
            execution=search.execution,
            parameter_protocol=TRAINING_PROTOCOL,
        )
        tune_runtime._cv_result.assert_not_called()
    finally:
        ray.shutdown()


def test_dispatch_commit_ack_loss_replays_same_event(runtime, monkeypatch):  # noqa: F811
    engine, _, scope = runtime
    commits = []

    class LostCommit(Session):
        def commit(self):
            super().commit()
            commits.append(True)
            if len(commits) == 1:
                raise ConnectionError("lost dispatch acknowledgement")

    monkeypatch.setattr(trial_runtime, "get_session_factory", lambda: lambda: LostCommit(engine))
    search = searcher(runtime)
    with pytest.raises(trial_runtime.TrialExecutionUnresolved):
        search.suggest("physical")
    assert isinstance(search.suggest("physical"), dict)
    with Session(engine) as db:
        trial = db.scalar(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        attempt = db.scalar(select(WorkflowAttempt).where(WorkflowAttempt.trial_id == trial.id))
        events = list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt.id))
        )
        assert attempt.status == AttemptStatus.PENDING
        assert len(events) == 1


def test_corrupt_dispatch_evidence_rejects_replay(runtime):  # noqa: F811
    engine, _, scope = runtime
    search = searcher(runtime)
    search.suggest("physical")
    with Session(engine) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent)
            .join(WorkflowAttempt, WorkflowAttempt.id == WorkflowEvent.attempt_id)
            .where(
                WorkflowAttempt.run_attempt_id == scope[2],
                WorkflowEvent.event_type == "trial.dispatch",
            )
        )
        event.payload = {"ray_trial_id": "wrong"}
    with pytest.raises(StaleFence, match="dispatch evidence"):
        search.suggest("physical")


@pytest.mark.parametrize("wrapped", [False, True])
def test_tune_ownership_errors_are_not_ranked_as_model_failures(
    runtime,  # noqa: F811
    monkeypatch,
    tmp_path,
    wrapped,  # noqa: F811
):
    error = StaleFence("worker lost ownership")
    if wrapped:
        error = ray.exceptions.RayTaskError("_cv_trial", "worker lost ownership traceback", error)
        error = ray.cloudpickle.loads(ray.cloudpickle.dumps(error))
    monkeypatch.setattr(
        tune_runtime.tune,
        "Tuner",
        lambda *_a, **_k: SimpleNamespace(fit=lambda: [SimpleNamespace(error=error)]),
    )
    with pytest.raises(StaleFence, match="worker lost ownership"):
        tune_runtime.search_candidate(
            Ridge(),
            {"alpha": Real(0.01, 10)},
            None,
            None,
            3,
            "r2",
            iterations=1,
            storage_path=str(tmp_path),
            run=runtime[1],
            candidate_name="Ridge",
            base_parameters=Ridge().get_params(),
        )


def test_fenced_tune_requires_candidate_before_dispatch(runtime, monkeypatch, tmp_path):  # noqa: F811
    tuner = MagicMock(side_effect=AssertionError("unowned dispatch"))
    monkeypatch.setattr(tune_runtime.tune, "Tuner", tuner)
    with pytest.raises(StaleFence, match="candidate contract"):
        tune_runtime.search_candidate(
            Ridge(), {}, None, None, 3, "r2", iterations=1, storage_path=str(tmp_path)
        )
    tuner.assert_not_called()
