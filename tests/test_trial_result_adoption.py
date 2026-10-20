import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import numpy as np
import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand, WorkflowEvent
from automl_api.services import reconciler, training_trials
from automl_api.services.workflow_state import StaleFence
from automl_api.training import trial_runtime
from sklearn.linear_model import Ridge
from sklearn.model_selection import cross_val_score
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401
from test_training_publication_postgres import publication  # noqa: F401
from test_training_trial_enrollment import enroll, trial_scope  # noqa: F401
from test_training_trial_runtime import runtime  # noqa: F401
from test_trial_replacement_handoff import activate


@pytest.fixture
def completed(runtime):  # noqa: F811
    engine, run, scope = runtime
    with Session(engine) as db, db.begin():
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
    results = []
    for index in range(2):
        x = np.arange(24, dtype=float).reshape(-1, 1)
        payload = {
            "scores": cross_val_score(
                Ridge(alpha=index + 1),
                x,
                x.ravel() * 2 + 1,
                cv=3,
                n_jobs=1,
            ).tolist()
        }
        assert (
            trial_runtime.evaluate_trial(
                run,
                "Ridge",
                f"completed-{index}",
                {"alpha": index + 1},
                lambda payload=payload: payload,
            )
            == payload
        )
        results.append(payload)
    return runtime, results


def replace(runtime_scope):
    engine, run, scope = runtime_scope
    with Session(engine) as db, db.begin():
        parent = db.scalar(
            select(WorkflowAttempt).where(WorkflowAttempt.id == scope[2]).with_for_update()
        )
        new = reconciler._replace_lost_ray_attempt(db, parent, reason="parent lost")
        parent_id, fence = new.id, new.fencing_token
    with Session(engine) as db, db.begin():
        activate(db, scope, parent_id)
    return parent_id, fence


def test_reconciler_authorizes_completed_receipts_and_replacement_replays(completed, monkeypatch):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    parent_id, fence = replace(runtime_scope)
    with Session(engine) as db:
        parent = db.get(WorkflowAttempt, parent_id)
        assert len(parent.predecessor_checkpoint_allowlist) == 2
        assert all(ref["kind"] == "trial_result" for ref in parent.predecessor_checkpoint_allowlist)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(parent_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    compute = MagicMock(side_effect=AssertionError("completed CV recomputed"))
    for _ in range(2):
        for index, result in enumerate(results):
            assert (
                trial_runtime.evaluate_trial(
                    run, "Ridge", f"completed-{index}", {"alpha": index + 1}, compute
                )
                == result
            )
    compute.assert_not_called()
    with Session(engine) as db:
        children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(children) == 2
        assert all(
            c.status == AttemptStatus.SUCCEEDED
            and c.terminal_cas_version == 1
            and c.predecessor_attempt_id
            and c.retry_count == 0
            for c in children
        )
        adopted = list(
            db.scalars(
                select(WorkflowEvent).where(
                    WorkflowEvent.attempt_id.in_([c.id for c in children]),
                    WorkflowEvent.event_type == "trial.adopted",
                )
            )
        )
        assert len(adopted) == 2


@pytest.mark.parametrize(
    "invalid",
    [
        "missing_allowlist",
        "wrong_digest",
        "wrong_reference_attempt",
        "wrong_reference_trial",
        "payload",
        "event_digest",
        "event_type",
        "missing_event",
        "params",
        "cas",
        "source_budget",
        "parent_budget",
        "source_workload",
        "source_status",
        "missing_parent",
        "parent_status",
        "parent_workload",
    ],
)
def test_invalid_predecessor_adoption_never_computes(completed, monkeypatch, invalid):
    from automl_api.models.workflows import TrainingTrial

    runtime_scope, _ = completed
    engine, run, scope = runtime_scope
    parent_id, fence = replace(runtime_scope)
    with Session(engine) as db, db.begin():
        parent = db.get(WorkflowAttempt, parent_id)
        refs = parent.predecessor_checkpoint_allowlist
        ref = next(
            r
            for r in refs
            if db.get(TrainingTrial, uuid.UUID(r["trial_id"])).suggestion_id == "completed-0"
        )
        source = db.get(WorkflowAttempt, uuid.UUID(ref["attempt_id"]))
        event = db.scalar(
            select(WorkflowEvent).where(
                WorkflowEvent.attempt_id == source.id, WorkflowEvent.event_key == "trial.terminal"
            )
        )
        if invalid == "missing_allowlist":
            parent.predecessor_checkpoint_allowlist = []
        elif invalid in {"wrong_digest", "wrong_reference_attempt", "wrong_reference_trial"}:
            changed = dict(ref)
            key = {
                "wrong_digest": "result_digest",
                "wrong_reference_attempt": "attempt_id",
                "wrong_reference_trial": "trial_id",
            }[invalid]
            changed[key] = "f" * 64 if key == "result_digest" else str(uuid.uuid4())
            parent.predecessor_checkpoint_allowlist = [changed]
        elif invalid == "payload":
            event.payload = {"status": "succeeded", "result": {"scores": [999]}}
        elif invalid == "event_digest":
            event.result_digest = "f" * 64
        elif invalid == "event_type":
            event.event_type = "unrelated.event"
        elif invalid == "missing_event":
            db.delete(event)
        elif invalid == "params":
            db.get(TrainingTrial, source.trial_id).parameters = {"alpha": 99}
        elif invalid == "cas":
            source.terminal_cas_version = 2
        elif invalid == "source_budget":
            source.retry_budget = 1
        elif invalid == "parent_budget":
            parent.retry_budget = 1
        elif invalid == "source_workload":
            source.workload_identity = "another-worker"
        elif invalid == "source_status":
            source.status = AttemptStatus.FAILED
        elif invalid == "missing_parent":
            parent.predecessor_attempt_id = None
        elif invalid == "parent_status":
            db.get(WorkflowAttempt, scope[2]).status = AttemptStatus.FAILED
        else:
            parent.workload_identity = "another-worker"
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(parent_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    compute = MagicMock(side_effect=AssertionError("unauthorized scoring"))
    with pytest.raises(StaleFence):
        trial_runtime.evaluate_trial(run, "Ridge", "completed-0", {"alpha": 1}, compute)
    compute.assert_not_called()
    with Session(engine) as db:
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )


def test_predecessor_generation_cannot_change_after_result_replacement(completed):
    runtime_scope, _ = completed
    engine, _, scope = runtime_scope
    parent_id, _ = replace(runtime_scope)
    with pytest.raises(IntegrityError, match="predecessor lineage"):
        with Session(engine) as db, db.begin():
            db.get(WorkflowAttempt, scope[2]).generation = 9
    with Session(engine) as db:
        previous = db.get(WorkflowAttempt, scope[2])
        current = db.get(WorkflowAttempt, parent_id)
        assert current.generation == previous.generation + 1 == 2
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )


def test_concurrent_adoption_commits_one_new_terminal_receipt(completed):
    runtime_scope, results = completed
    engine, run, _ = runtime_scope
    parent_id, fence = replace(runtime_scope)
    compute = MagicMock(side_effect=AssertionError("recomputed"))

    def adopt(_):
        return trial_runtime.evaluate_trial(
            run, "Ridge", "completed-0", {"alpha": 1}, compute, parent_context=(parent_id, fence)
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(adopt, range(2))) == [results[0], results[0]]
    with Session(engine) as db:
        children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(children) == 1 and children[0].terminal_cas_version == 1
        events = list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == children[0].id))
        )
        assert {e.event_type for e in events} == {"trial.adopted", "trial.terminal"}
    compute.assert_not_called()


def test_adoption_commit_ack_loss_replays_without_computation(completed, monkeypatch):
    runtime_scope, results = completed
    engine, run, _ = runtime_scope
    parent_id, fence = replace(runtime_scope)
    commits = []

    class LostCommit(Session):
        def commit(self):
            super().commit()
            commits.append(True)
            if len(commits) == 1:
                raise ConnectionError("lost adoption acknowledgement")

    monkeypatch.setattr(trial_runtime, "get_session_factory", lambda: lambda: LostCommit(engine))
    compute = MagicMock(side_effect=AssertionError("recomputed"))
    with pytest.raises(trial_runtime.TrialExecutionUnresolved):
        trial_runtime.evaluate_trial(
            run, "Ridge", "completed-0", {"alpha": 1}, compute, parent_context=(parent_id, fence)
        )
    for _ in range(2):
        assert (
            trial_runtime.evaluate_trial(
                run,
                "Ridge",
                "completed-0",
                {"alpha": 1},
                compute,
                parent_context=(parent_id, fence),
            )
            == results[0]
        )
    compute.assert_not_called()


def test_failed_trial_is_adopted_as_failure_without_retry(completed):
    runtime_scope, _ = completed
    _, run, _ = runtime_scope
    failed = MagicMock(side_effect=ValueError("ordinary estimator failure"))
    with pytest.raises(ValueError):
        trial_runtime.evaluate_trial(run, "Ridge", "failed", {"alpha": 3}, failed)
    parent_id, fence = replace(runtime_scope)
    failed.reset_mock()
    for _ in range(2):
        with pytest.raises(trial_runtime.TrialEvaluationFailed, match="ordinary estimator failure"):
            trial_runtime.evaluate_trial(
                run, "Ridge", "failed", {"alpha": 3}, failed, parent_context=(parent_id, fence)
            )
    failed.assert_not_called()


def test_adoption_rollback_preserves_source_and_creates_no_receipt(completed):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    parent_id, fence = replace(runtime_scope)
    with Session(engine) as db:
        before = [
            (e.id, e.payload, e.result_digest)
            for e in db.scalars(
                select(WorkflowEvent)
                .join(WorkflowAttempt, WorkflowAttempt.id == WorkflowEvent.attempt_id)
                .where(WorkflowAttempt.run_attempt_id == scope[2])
                .order_by(WorkflowEvent.id)
            )
        ]
    with pytest.raises(ConnectionError), Session(engine) as db, db.begin():
        training_trials.enroll_trial(
            db,
            run_attempt_id=parent_id,
            run_fencing_token=fence,
            candidate_id=scope[4],
            suggestion_id="completed-0",
            parameters={"alpha": 1},
            execution_id="rolled-back",
        )
        raise ConnectionError("adoption before commit")
    with Session(engine) as db:
        assert not list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        after = [
            (e.id, e.payload, e.result_digest)
            for e in db.scalars(
                select(WorkflowEvent)
                .join(WorkflowAttempt, WorkflowAttempt.id == WorkflowEvent.attempt_id)
                .where(WorkflowAttempt.run_attempt_id == scope[2])
                .order_by(WorkflowEvent.id)
            )
        ]
        assert after == before
    compute = MagicMock(side_effect=AssertionError("recomputed"))
    assert (
        trial_runtime.evaluate_trial(
            run, "Ridge", "completed-0", {"alpha": 1}, compute, parent_context=(parent_id, fence)
        )
        == results[0]
    )


def test_multiple_replacements_keep_adoption_chain_and_never_refit(completed):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    compute = MagicMock(side_effect=AssertionError("recomputed"))
    for _ in range(2):
        parent_id, fence = replace(runtime_scope)
        for index, result in enumerate(results):
            assert (
                trial_runtime.evaluate_trial(
                    run,
                    "Ridge",
                    f"completed-{index}",
                    {"alpha": index + 1},
                    compute,
                    parent_context=(parent_id, fence),
                )
                == result
            )
        scope = (engine, run.id, parent_id, fence, scope[4])
        runtime_scope = engine, run, scope
    with Session(engine) as db:
        children = list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.model_run_id == run.id, WorkflowAttempt.trial_id.is_not(None)
                )
            )
        )
        assert len(children) == 6
        assert sorted(c.generation for c in children) == [1, 1, 2, 2, 3, 3]
        assert all(
            c.status == AttemptStatus.SUCCEEDED and c.terminal_cas_version == 1 for c in children
        )
    compute.assert_not_called()


def test_reconciler_rejects_corrupt_terminal_receipt_and_rolls_back_replacement(completed):
    runtime_scope, _ = completed
    engine, _, scope = runtime_scope
    with Session(engine) as db, db.begin():
        event = db.scalar(
            select(WorkflowEvent)
            .join(WorkflowAttempt, WorkflowAttempt.id == WorkflowEvent.attempt_id)
            .where(
                WorkflowAttempt.run_attempt_id == scope[2],
                WorkflowEvent.event_key == "trial.terminal",
            )
        )
        event.result_digest = "f" * 64
    with pytest.raises(StaleFence):
        replace(runtime_scope)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, scope[2]).status == AttemptStatus.RUNNING
        parents = list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.model_run_id == scope[1], WorkflowAttempt.trial_id.is_(None)
                )
            )
        )
        assert len(parents) == 1


def test_result_permissions_preserve_existing_object_checkpoint_allowlist(completed):
    runtime_scope, _ = completed
    engine, _, scope = runtime_scope
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, scope[2]).checkpoint_uri = "s3://checkpoint/manifest"
    parent_id, _ = replace(runtime_scope)
    with Session(engine) as db:
        allowlist = db.get(WorkflowAttempt, parent_id).predecessor_checkpoint_allowlist
        assert "s3://checkpoint/manifest" in allowlist
        assert len([ref for ref in allowlist if isinstance(ref, dict)]) == 2


def test_mixed_active_and_completed_trials_recover_under_same_parent(completed):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    with Session(engine) as db, db.begin():
        old_active = enroll(db, scope)
        old_active_id = old_active.id
    parent_id, fence = replace(runtime_scope)
    cached = MagicMock(side_effect=AssertionError("completed trial recomputed"))
    for index, result in enumerate(results):
        assert (
            trial_runtime.evaluate_trial(
                run,
                "Ridge",
                f"completed-{index}",
                {"alpha": index + 1},
                cached,
                parent_context=(parent_id, fence),
            )
            == result
        )
    fresh = MagicMock(return_value={"score": 9})
    assert trial_runtime.evaluate_trial(
        run,
        "Ridge",
        "suggestion-0",
        {"alpha": 0.1, "seed": 42},
        fresh,
        parent_context=(parent_id, fence),
    ) == {"score": 9}
    cached.assert_not_called()
    fresh.assert_called_once()
    with Session(engine) as db:
        children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(children) == 3 and all(c.status == AttemptStatus.SUCCEEDED for c in children)
        assert db.get(WorkflowAttempt, old_active_id).status == AttemptStatus.SUPERSEDED


def test_results_survive_replacements_that_die_before_adoption(completed):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    for _ in range(2):
        parent_id, fence = replace(runtime_scope)
        scope = (engine, run.id, parent_id, fence, scope[4])
        runtime_scope = engine, run, scope
    compute = MagicMock(side_effect=AssertionError("ancestor result recomputed"))
    for index, result in enumerate(results):
        assert (
            trial_runtime.evaluate_trial(
                run,
                "Ridge",
                f"completed-{index}",
                {"alpha": index + 1},
                compute,
                parent_context=(parent_id, fence),
            )
            == result
        )
    compute.assert_not_called()


def test_inherited_authorization_cannot_be_changed_during_later_recovery(completed):
    runtime_scope, _ = completed
    engine, run, scope = runtime_scope
    parent_id, fence = replace(runtime_scope)
    with Session(engine) as db, db.begin():
        parent = db.get(WorkflowAttempt, parent_id)
        refs = [dict(r) for r in parent.predecessor_checkpoint_allowlist]
        refs[0]["result_digest"] = "f" * 64
        parent.predecessor_checkpoint_allowlist = refs
    updated_scope = (engine, run.id, parent_id, fence, scope[4])
    with pytest.raises(StaleFence):
        replace((engine, run, updated_scope))
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, parent_id).status == AttemptStatus.RUNNING


def test_every_skipped_parent_must_keep_explicit_result_authorization(completed):
    runtime_scope, _ = completed
    engine, run, scope = runtime_scope
    middle_id, middle_fence = replace(runtime_scope)
    updated = (engine, run.id, middle_id, middle_fence, scope[4])
    parent_id, fence = replace((engine, run, updated))
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, middle_id).predecessor_checkpoint_allowlist = []
    compute = MagicMock(side_effect=AssertionError("unauthorized computation"))
    with pytest.raises(StaleFence):
        trial_runtime.evaluate_trial(
            run, "Ridge", "completed-0", {"alpha": 1}, compute, parent_context=(parent_id, fence)
        )
    compute.assert_not_called()


def test_actual_partial_local_search_recovers_receipts_and_only_scores_unfinished_trial(
    runtime,  # noqa: F811
    monkeypatch,
):
    from automl_api.models.workflows import TrainingTrial
    from automl_api.training import tune_runtime
    from test_training_local_search import search

    engine, run, scope = runtime
    with Session(engine) as db, db.begin():
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
    original = tune_runtime._cv_result
    calls = []

    def interrupted(config, **kwargs):
        if len(calls) == 2:
            raise StaleFence("synthetic process loss before third scoring")
        calls.append(config)
        return original(config, **kwargs)

    monkeypatch.setattr(tune_runtime, "_cv_result", interrupted)
    with pytest.raises(StaleFence, match="synthetic process loss"):
        search(runtime)
    with Session(engine) as db:
        before = {
            t.suggestion_id: t.parameters
            for t in db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        }
    parent_id, fence = replace(runtime)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(parent_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    resumed_calls = []

    def resumed(config, **kwargs):
        resumed_calls.append(config)
        return original(config, **kwargs)

    monkeypatch.setattr(tune_runtime, "_cv_result", resumed)
    result = search(runtime)
    assert result["trials"] == 3 and result["failed_trials"] == 0
    assert len(resumed_calls) == 1
    assert resumed_calls[0]["sceptre_suggestion_id"] == 2
    assert search(runtime) == result
    assert len(resumed_calls) == 1
    with Session(engine) as db:
        after = {
            t.suggestion_id: t.parameters
            for t in db.scalars(select(TrainingTrial).where(TrainingTrial.candidate_id == scope[4]))
        }
        assert after == before
        children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(children) == 3 and all(c.status == AttemptStatus.SUCCEEDED for c in children)
