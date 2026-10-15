import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand
from automl_api.services import reconciler, training_trials
from automl_api.services.workflow_state import StaleFence, transition_attempt
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import enroll, trial_scope  # noqa: F401


@pytest.fixture
def replaced(trial_scope):  # noqa: F811
    engine, run_id, parent_id, fence, _ = trial_scope
    old = []
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        db.add(
            WorkflowCommand(
                project_id=run.project_id,
                actor_id=run.created_by_id,
                operation="training.launch",
                idempotency_key=uuid.uuid4().hex,
                request_hash="a" * 64,
                resource_type="model_run",
                resource_id=run.id,
            )
        )
        for index in range(2):
            attempt = enroll(
                db, trial_scope, suggestion_id=f"suggestion-{index}", execution_id=f"old-{index}"
            )
            context = dict(
                attempt_id=attempt.id,
                fencing_token=attempt.fencing_token,
                run_fencing_token=fence,
                worker_id=f"old-worker-{index}",
            )
            training_trials.start_trial(db, **context)
            old.append(context)
    with Session(engine) as db, db.begin():
        parent = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == parent_id).with_for_update()
        )
        replacement = reconciler._replace_lost_ray_attempt(db, parent, reason="worker lost")
        replacement_id, replacement_fence = replacement.id, replacement.fencing_token
    return trial_scope, old, replacement_id, replacement_fence


def activate(db, scope, replacement_id):
    parent = db.get(WorkflowAttempt, replacement_id)
    for state in (AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING):
        transition_attempt(parent, state, fencing_token=parent.fencing_token)
    db.get(ModelRun, scope[1]).status = RunStatus.RUNNING


def test_reconciler_replacements_can_enroll_and_finish_all_trials(replaced):
    scope, old, replacement_id, replacement_fence = replaced
    engine = scope[0]
    with Session(engine) as db, db.begin():
        assert all(
            db.get(WorkflowAttempt, c["attempt_id"]).status == AttemptStatus.SUPERSEDED for c in old
        )
        children = list(
            db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == replacement_id)
            )
        )
        assert len(children) == 2 and all(c.status == AttemptStatus.PENDING for c in children)
        ids = {c.predecessor_attempt_id: c.id for c in children}
        activate(db, scope, replacement_id)
    updated = (engine, scope[1], replacement_id, replacement_fence, scope[4])
    for index, previous in enumerate(old):
        with pytest.raises(StaleFence), Session(engine) as db, db.begin():
            training_trials.finish_trial(db, **previous, result={"score": 99})
        with Session(engine) as db, db.begin():
            child = enroll(
                db, updated, suggestion_id=f"suggestion-{index}", execution_id=f"new-{index}"
            )
            assert child.id == ids[previous["attempt_id"]]
            assert child.generation == 2
            context = dict(
                attempt_id=child.id,
                fencing_token=child.fencing_token,
                run_fencing_token=replacement_fence,
                worker_id=f"new-worker-{index}",
            )
            assert training_trials.start_trial(db, **context) is None
            training_trials.finish_trial(db, **context, result={"score": index})
        with Session(engine) as db, db.begin():
            child = enroll(
                db, updated, suggestion_id=f"suggestion-{index}", execution_id=f"new-{index}"
            )
            assert training_trials.start_trial(db, **context)["result"] == {"score": index}


@pytest.mark.parametrize("conflict", [False, True])
def test_concurrent_binding_of_replacement_stub_is_fenced(replaced, conflict):
    scope, _, parent_id, fence = replaced
    with Session(scope[0]) as db, db.begin():
        activate(db, scope, parent_id)
    updated = (scope[0], scope[1], parent_id, fence, scope[4])

    def bind(index):
        try:
            with Session(scope[0]) as db, db.begin():
                return enroll(db, updated, execution_id=f"new-{index if conflict else 0}").id
        except StaleFence:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(bind, range(2)))
    assert results.count("conflict") == int(conflict)
    assert len(set(results)) == (2 if conflict else 1)


@pytest.mark.parametrize(
    "invalid",
    [
        "wrong_workload",
        "wrong_logical_key",
        "claimed",
        "used",
        "owner",
        "expiry",
        "cas",
        "missing_predecessor",
        "wrong_predecessor",
        "wrong_parent",
        "prior_not_superseded",
        "budget",
    ],
)
def test_only_untouched_authorized_replacement_stub_can_bind(replaced, invalid):
    from datetime import UTC, datetime, timedelta

    scope, old, parent_id, fence = replaced
    with Session(scope[0]) as db, db.begin():
        activate(db, scope, parent_id)
        stub = db.scalar(
            select(WorkflowAttempt).where(
                WorkflowAttempt.run_attempt_id == parent_id,
                WorkflowAttempt.predecessor_attempt_id == old[0]["attempt_id"],
            )
        )
        if invalid == "wrong_workload":
            stub.workload_identity = "another-stage-worker"
        elif invalid == "wrong_logical_key":
            stub.logical_key = "another-trial"
        elif invalid == "claimed":
            stub.status = AttemptStatus.CLAIMED
        elif invalid == "used":
            stub.retry_count = 1
        elif invalid == "owner":
            stub.lease_owner = "another-owner"
        elif invalid == "expiry":
            stub.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
        elif invalid == "cas":
            stub.terminal_cas_version = 1
        elif invalid == "missing_predecessor":
            stub.predecessor_attempt_id = None
        elif invalid == "wrong_predecessor":
            stub.predecessor_attempt_id = old[1]["attempt_id"]
        elif invalid == "wrong_parent":
            db.get(WorkflowAttempt, parent_id).predecessor_attempt_id = None
        elif invalid == "prior_not_superseded":
            db.get(WorkflowAttempt, old[0]["attempt_id"]).status = AttemptStatus.FAILED
        else:
            stub.retry_budget = 1
    updated = (scope[0], scope[1], parent_id, fence, scope[4])
    with pytest.raises(StaleFence), Session(scope[0]) as db, db.begin():
        enroll(db, updated, execution_id="new")


@pytest.mark.parametrize(
    "child_status",
    [AttemptStatus.PENDING, AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING],
)
def test_child_budget_exhaustion_stops_entire_parent_replacement(
    trial_scope,  # noqa: F811
    child_status,
):
    from automl_api.models.workflows import TrainingTrial

    engine, run_id, parent_id, _, _ = trial_scope
    with Session(engine) as db, db.begin():
        child = enroll(db, trial_scope)
        child.retry_budget = 1
        if child_status != AttemptStatus.PENDING:
            for state in (AttemptStatus.CLAIMED, AttemptStatus.SUBMITTED, AttemptStatus.RUNNING):
                transition_attempt(child, state, fencing_token=child.fencing_token)
                if state == child_status:
                    break
        trial_id, child_id = child.trial_id, child.id
    with Session(engine) as db, db.begin():
        parent = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == parent_id).with_for_update()
        )
        assert parent.retry_budget > 1
        assert reconciler._replace_lost_ray_attempt(db, parent, reason="worker lost") is None
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.FAILED
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.FAILED
        expected = (
            AttemptStatus.CANCELLED
            if child_status == AttemptStatus.PENDING
            else AttemptStatus.FAILED
        )
        assert db.get(WorkflowAttempt, child_id).status == expected
        assert db.get(TrainingTrial, trial_id).status == expected.value
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id)
                    )
                )
            )
            == 2
        )


def test_recovered_runtime_claims_replacement_before_actual_cv(replaced, monkeypatch):
    import numpy as np
    from automl_api.training import trial_runtime
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import cross_val_score

    scope, _, parent_id, fence = replaced
    engine = scope[0]
    with Session(engine) as db, db.begin():
        activate(db, scope, parent_id)
        run = db.get(ModelRun, scope[1])
        db.flush()
        db.expunge(run)
    monkeypatch.setattr(trial_runtime, "get_session_factory", lambda: lambda: Session(engine))
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(parent_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    monkeypatch.setenv("TRAINING_EXECUTION_MODE", "candidate")
    calls = []

    def compute():
        with Session(engine) as db:
            child = db.scalar(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.run_attempt_id == parent_id,
                    WorkflowAttempt.status == AttemptStatus.RUNNING,
                )
            )
            assert child.generation == 2 and child.lease_owner
        calls.append(True)
        x = np.arange(24, dtype=float).reshape(-1, 1)
        return {"scores": cross_val_score(Ridge(), x, x.ravel() * 2 + 1, cv=3).tolist()}

    first = trial_runtime.evaluate_trial(
        run, "Ridge", "suggestion-0", {"alpha": 0.1, "seed": 42}, compute
    )
    assert (
        trial_runtime.evaluate_trial(
            run, "Ridge", "suggestion-0", {"alpha": 0.1, "seed": 42}, compute
        )
        == first
    )
    assert calls == [True] and len(first["scores"]) == 3


def test_replacement_transaction_rollback_preserves_every_child(replaced):
    scope, _, parent_id, _ = replaced
    engine = scope[0]
    with Session(engine) as db, db.begin():
        activate(db, scope, parent_id)
    with Session(engine) as db:
        before = {
            a.id: (a.status, a.fencing_token)
            for a in db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == scope[1])
            )
        }
    with (
        pytest.raises(RuntimeError, match="crash before commit"),
        Session(engine) as db,
        db.begin(),
    ):
        parent = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == parent_id).with_for_update()
        )
        assert reconciler._replace_lost_ray_attempt(db, parent, reason="worker lost") is not None
        db.flush()
        raise RuntimeError("crash before commit")
    with Session(engine) as db:
        after = {
            a.id: (a.status, a.fencing_token)
            for a in db.scalars(
                select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == scope[1])
            )
        }
        assert after == before
        assert db.get(ModelRun, scope[1]).status == RunStatus.RUNNING
