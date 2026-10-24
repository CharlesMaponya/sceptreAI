"""Revocation denies fresh work while durable result recovery remains available."""

import pytest
from automl_api.models.enums import AttemptStatus, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowEvent
from automl_api.services import attempt_identity, training_trials
from automl_api.services.attempt_object_grants import revoke_attempt_grants
from automl_api.services.workflow_state import StaleFence
from automl_api.training import analysis, pipeline, trial_runtime, worker
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_attempt_identity import admit
from test_attempt_identity import identity as identity
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_preparation_admission import dataset_scope as dataset_scope
from test_training_publication_postgres import publication as publication
from test_training_trial_enrollment import enroll
from test_training_trial_enrollment import trial_scope as trial_scope
from test_training_trial_lifecycle import execution as execution
from test_training_trial_lifecycle import start
from test_training_trial_runtime import runtime as runtime
from test_trial_result_adoption import completed as completed
from test_trial_result_adoption import replace as replace_parent


@pytest.mark.parametrize("when", ["before", "review"])
def test_generic_identity_revocation_blocks_replay_and_remote_race(identity, when):
    scope, _, _, review, _, auth = identity
    engine, _, attempt_id, _, _ = scope
    assert admit(identity)["attempt_id"] == str(attempt_id)

    def revoke(*_args, **_kwargs):
        with Session(engine) as db, db.begin():
            revoke_attempt_grants(db, attempt_id)
        return review

    if when == "before":
        revoke()
        auth.create_token_review.reset_mock()
    else:
        auth.create_token_review.side_effect = revoke
    with pytest.raises(StaleFence):
        admit(identity)
    if when == "before":
        auth.create_token_review.assert_not_called()
    with Session(engine) as db:
        events = list(
            db.scalars(select(WorkflowEvent).where(WorkflowEvent.attempt_id == attempt_id))
        )
        assert sum(event.event_type == "attempt.identity" for event in events) == 1
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.RUNNING


@pytest.mark.parametrize("pending", [False, True])
def test_dataset_scope_checks_committed_and_pending_revocation(dataset_scope, pending):
    engine, _, attempt_id, *_ = dataset_scope
    with Session(engine, autoflush=False) as db, db.begin():
        assert attempt_identity._scope(db, attempt_id).id == attempt_id
        if not pending:
            revoke_attempt_grants(db, attempt_id)
    with Session(engine, autoflush=False) as db, db.begin():
        if pending:
            revoke_attempt_grants(db, attempt_id)
            assert any(isinstance(row, WorkflowEvent) for row in db.new)
        with pytest.raises(StaleFence):
            attempt_identity._scope(db, attempt_id)


@pytest.mark.parametrize(
    "operation", ["start", "heartbeat", "progress", "success", "failure", "complete"]
)
def test_revoked_training_parent_cannot_renew_or_publish(publication, operation):
    engine, run_id, attempt_id, fence, result = publication
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    with Session(engine) as db, db.begin():
        run = db.get(ModelRun, run_id)
        run.tags = {**run.tags, "winner_model_artifact_uri": "s3://test/winner"}
        revoke_attempt_grants(db, attempt_id)
        before = (db.get(WorkflowAttempt, attempt_id).lease_expires_at, dict(run.tags))
    with pytest.raises(StaleFence):
        if operation == "start":
            worker._begin_fenced_attempt(run_id, worker_id="owner")
        elif operation == "heartbeat":
            worker._heartbeat_fenced_attempt(run_id, (attempt_id, fence), "owner")
        elif operation == "progress":
            pipeline._persist_candidate_phase(run_id, "Ridge", "fitting")
        elif operation == "success":
            pipeline._persist_training_success(run_id, result, "mlflow")
        elif operation == "failure":
            worker._fail_fenced_attempt(
                run_id, (attempt_id, fence), ValueError("failed"), worker_id="owner"
            )
        else:
            worker._complete_fenced_attempt(run_id, (attempt_id, fence), worker_id="owner")
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        run = db.get(ModelRun, run_id)
        assert (attempt.lease_expires_at, run.tags) == before
        assert attempt.terminal_cas_version == 0 and run.status == RunStatus.RUNNING


def test_pending_training_revocation_blocks_the_shared_mutation_guard(publication):
    engine, run_id, attempt_id, *_ = publication
    with Session(engine, autoflush=False) as db, db.begin():
        revoke_attempt_grants(db, attempt_id)
        assert any(isinstance(row, WorkflowEvent) for row in db.new)
        with pytest.raises(StaleFence):
            pipeline._locked_run(db, run_id)


@pytest.mark.parametrize("operation", ["success", "failure"])
def test_revoked_analysis_does_not_write_native_output_or_metadata(
    analysis_scope, monkeypatch, operation
):
    engine, run_id, attempt_id, _, store, result, *_ = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    with Session(engine) as db, db.begin():
        revoke_attempt_grants(db, attempt_id)
        before = dict(db.get(ModelRun, run_id).tags)
    calls = []

    def unexpected(*_args, **_kwargs):
        calls.append(True)
        pytest.fail("Revoked analysis reached native output")

    monkeypatch.setattr(store, "put_bytes", unexpected)
    with pytest.raises(StaleFence):
        if operation == "success":
            analysis._persist_analysis_result(run_id, result)
        else:
            analysis._mark_analysis_failed(run_id, ValueError("late failure"))
    assert not calls
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).tags == before
        assert db.get(ModelRun, run_id).status == RunStatus.RUNNING
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0


@pytest.mark.parametrize("pending", [False, True])
def test_revoked_parent_cannot_enroll_a_new_trial(trial_scope, pending):
    engine, _, parent_id, *_ = trial_scope
    if not pending:
        with Session(engine) as db, db.begin():
            revoke_attempt_grants(db, parent_id)
    with Session(engine, autoflush=False) as db, db.begin():
        if pending:
            revoke_attempt_grants(db, parent_id)
        with pytest.raises(StaleFence):
            enroll(db, trial_scope)


@pytest.mark.parametrize("which", ["parent", "child"])
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("operation", ["enroll", "start", "heartbeat", "finish"])
def test_revocation_denies_trial_mutations(execution, which, pending, operation):
    engine, context, scope = execution
    if operation != "start":
        start(execution)
    revoked = scope[2] if which == "parent" else context["attempt_id"]
    if not pending:
        with Session(engine) as db, db.begin():
            revoke_attempt_grants(db, revoked)
    with Session(engine, autoflush=False) as db, db.begin():
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        before = (
            attempt.status,
            attempt.lease_expires_at,
            attempt.retry_count,
            attempt.terminal_cas_version,
        )
        if pending:
            revoke_attempt_grants(db, revoked)
        with pytest.raises(StaleFence):
            if operation == "enroll":
                enroll(db, scope)
            elif operation == "start":
                training_trials.start_trial(db, **context)
            elif operation == "heartbeat":
                training_trials.heartbeat_trial(db, **context)
            else:
                training_trials.finish_trial(db, **context, result={"cv_score": 0.9})
        assert (
            attempt.status,
            attempt.lease_expires_at,
            attempt.retry_count,
            attempt.terminal_cas_version,
        ) == before


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("which", ["parent", "child"])
def test_revoked_trial_retains_verified_terminal_replay(execution, failed, which):
    engine, context, scope = execution
    start(execution)
    values = {"failure": "estimator failed"} if failed else {"result": {"cv_score": 0.9}}
    with Session(engine) as db, db.begin():
        payload = training_trials.finish_trial(db, **context, **values)
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        parent = db.get(WorkflowAttempt, scope[2])
        reference = training_trials.trial_result_reference(db, parent, attempt)
        revoke_attempt_grants(db, parent.id if which == "parent" else attempt.id)
    with Session(engine) as db, db.begin():
        assert training_trials.start_trial(db, **{**context, "worker_id": "reader"}) == payload
        assert training_trials.finish_trial(db, **context, **values) == payload
        attempt = db.get(WorkflowAttempt, context["attempt_id"])
        assert (
            training_trials.trial_result_reference(db, db.get(WorkflowAttempt, scope[2]), attempt)
            == reference
        )
        assert attempt.terminal_cas_version == 1 and attempt.lease_expires_at is None
        with pytest.raises(StaleFence):
            training_trials.heartbeat_trial(db, **context)
        with pytest.raises(StaleFence):
            training_trials.finish_trial(db, **context, result={"cv_score": 0.1})


def test_committed_training_receipt_recovers_after_revocation(publication):
    engine, run_id, attempt_id, fence, result = publication
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    assert pipeline._persist_training_success(run_id, result, "mlflow")
    with Session(engine) as db, db.begin():
        receipt = dict(db.get(ModelRun, run_id).tags["training_result_receipt"])
        revoke_attempt_grants(db, attempt_id)
    assert worker._resume_committed_result(run_id, (attempt_id, fence))
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(ModelRun, run_id).tags["training_result_receipt"] == receipt
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1


def test_revoked_predecessor_receipts_remain_explicitly_adoptable(completed, monkeypatch):
    runtime_scope, results = completed
    engine, run, scope = runtime_scope
    with Session(engine) as db, db.begin():
        children = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == scope[2]))
        )
        receipts = {}
        for child in children:
            receipts[child.id] = training_trials.trial_result_reference(
                db, db.get(WorkflowAttempt, scope[2]), child
            )
            revoke_attempt_grants(db, child.id)
        revoke_attempt_grants(db, scope[2])
    parent_id, fence = replace_parent(runtime_scope)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(parent_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    with Session(engine) as db:
        parent = db.get(WorkflowAttempt, parent_id)
        assert sorted(
            parent.predecessor_checkpoint_allowlist, key=lambda ref: ref["attempt_id"]
        ) == sorted(receipts.values(), key=lambda ref: ref["attempt_id"])

    def forbidden():
        pytest.fail("An explicitly adopted completed trial recomputed CV")

    for index, result in enumerate(results):
        assert (
            trial_runtime.evaluate_trial(
                run, "Ridge", f"completed-{index}", {"alpha": index + 1}, forbidden
            )
            == result
        )
    with Session(engine) as db:
        for source_id, reference in receipts.items():
            child = db.get(WorkflowAttempt, source_id)
            assert (
                training_trials.trial_result_reference(db, db.get(WorkflowAttempt, scope[2]), child)
                == reference
            )
        adopted = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.run_attempt_id == parent_id))
        )
        assert len(adopted) == len(results) == 2
        assert all(
            child.status == AttemptStatus.SUCCEEDED
            and child.terminal_cas_version == 1
            and child.retry_count == 0
            for child in adopted
        )


def test_committed_analysis_receipt_recovers_after_revocation(analysis_scope, monkeypatch):
    engine, run_id, attempt_id, fence, store, result, _, post = analysis_scope
    worker._begin_fenced_attempt(run_id, worker_id="owner")
    analysis._persist_analysis_result(run_id, result)
    with Session(engine) as db, db.begin():
        receipt = dict(db.get(ModelRun, run_id).tags["analysis_result_receipt"])
        revoke_attempt_grants(db, attempt_id)
    calls = post.call_count

    def forbidden(*_args, **_kwargs):
        pytest.fail("Committed analysis recovery wrote native output again")

    monkeypatch.setattr(store, "put_bytes", forbidden)
    for _ in range(2):
        assert worker._resume_committed_result(run_id, (attempt_id, fence))
    assert post.call_count == calls
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(ModelRun, run_id).tags["analysis_result_receipt"] == receipt
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1
