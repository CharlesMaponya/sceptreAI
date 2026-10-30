"""One invalid analysis run must not abort and roll back the whole scan."""

from copy import deepcopy

import pytest
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.services import reconciler
from automl_api.workflow_reconciler import observe_once
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_analysis_admission import analysis_scope as analysis_scope
from test_analysis_observer_fairness import CLOCKS
from test_analysis_observer_fairness import pair as pair
from test_analysis_recovery import recovery as recovery
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_publication_postgres import publication as publication

INVALID = ["kind", "succeeded_without_cas", "receipt_without_cas", "tags_list"]


def invalidate(db, identity, condition):
    attempt = db.get(WorkflowAttempt, identity)
    run = db.get(ModelRun, attempt.model_run_id)
    if condition == "kind":
        run.run_kind = RunKind.TRAINING
    elif condition == "succeeded_without_cas":
        run.status = RunStatus.SUCCEEDED
    elif condition == "receipt_without_cas":
        run.tags = {**run.tags, "analysis_result_receipt": {"inconsistent": True}}
    else:
        run.tags = []


def snapshot(db, identities):
    result = {}
    for identity in identities:
        attempt = db.get(WorkflowAttempt, identity)
        run = db.get(ModelRun, attempt.model_run_id)
        result[identity] = (
            tuple(getattr(attempt, field) for field in CLOCKS),
            attempt.status,
            attempt.cleanup_state,
            run.status,
            deepcopy(run.tags),
        )
    return result


@pytest.mark.parametrize("condition", INVALID)
@pytest.mark.parametrize("limit", [1, 2])
def test_invalid_run_scope_rotates_and_preserves_original_workflow(pair, condition, limit):
    engine, bad_id, good_id, k8s = pair
    with Session(engine) as db, db.begin():
        invalidate(db, bad_id, condition)
    with Session(engine) as db:
        before = snapshot(db, (bad_id, good_id))
    requests = []
    for _ in range(2 if limit == 1 else 1):
        with Session(engine) as db, db.begin():
            assert (
                reconciler.observe_analysis_attempts(db, k8s, read=requests.append, limit=limit)
                == limit
            )
    assert len(requests) == 1 and requests[0].attempt_id == good_id
    with Session(engine) as db:
        assert snapshot(db, (bad_id, good_id)) == before
        assert all(
            db.get(WorkflowAttempt, identity).last_observed_at is not None
            for identity in (bad_id, good_id)
        )
    counts = []
    original = k8s.batch.read_namespaced_job.side_effect

    def read(**kwargs):
        counts.append(engine.pool.checkedout())
        return original(**kwargs)

    k8s.batch.read_namespaced_job.side_effect = read
    observed = reconciler.inspect_analysis_observation(k8s, requests[0])
    with Session(engine) as db, db.begin():
        assert (
            reconciler.observe_analysis_attempts(
                db, k8s, attempt_id=good_id, read=lambda _request: observed
            )
            == 1
        )
    assert counts == [0]
    with Session(engine) as db:
        assert snapshot(db, (bad_id, good_id)) == before
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.predecessor_attempt_id.in_([bad_id, good_id])
                )
            )
        )
    k8s.create_job.assert_not_called()
    k8s.delete_job.assert_not_called()


@pytest.mark.parametrize("condition", INVALID)
def test_actual_dispatcher_continues_after_invalid_analysis_scope(pair, condition):
    engine, bad_id, good_id, k8s = pair
    with Session(engine) as db, db.begin():
        invalidate(db, bad_id, condition)
    counts = []
    original = k8s.batch.read_namespaced_job.side_effect

    def read(**kwargs):
        counts.append(engine.pool.checkedout())
        return original(**kwargs)

    k8s.batch.read_namespaced_job.side_effect = read
    assert observe_once(lambda: Session(engine), k8s=k8s) == 2
    assert counts == [0]
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, good_id).last_observed_at is not None
        assert db.get(WorkflowAttempt, bad_id).last_observed_at is not None
        assert not list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.predecessor_attempt_id.in_([bad_id, good_id])
                )
            )
        )
    k8s.delete_job.assert_not_called()


def test_cancellation_keeps_cleanup_precedence_over_inconsistent_report(pair):
    engine, bad_id, good_id, k8s = pair
    with Session(engine) as db, db.begin():
        attempt = db.get(WorkflowAttempt, bad_id)
        run = db.get(ModelRun, attempt.model_run_id)
        run.status = RunStatus.CANCELLED
        run.tags = {**run.tags, "analysis_result_receipt": {"late_report": True}}
    requests = []
    with Session(engine) as db, db.begin():
        assert reconciler.observe_analysis_attempts(db, k8s, read=requests.append) == 2
    assert len(requests) == 1 and requests[0].attempt_id == good_id
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, bad_id)
        assert attempt.status == AttemptStatus.CANCELLED and attempt.cleanup_state == "queued"
        assert db.get(ModelRun, attempt.model_run_id).status == RunStatus.CANCELLED
        cleanup = list(
            db.scalars(
                select(OutboxEntry).where(
                    OutboxEntry.aggregate_id == attempt.model_run_id,
                    OutboxEntry.topic == "kubernetes.analysis.cancel",
                )
            )
        )
        assert len(cleanup) == 1 and cleanup[0].payload["attempt_id"] == str(bad_id)
    k8s.delete_job.assert_not_called()
