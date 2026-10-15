import uuid

import pytest
from automl_api.models.datasets import Dataset, DatasetVersion
from automl_api.models.enums import AttemptStatus, DatasetFormat, RunKind, RunStatus, TaskType
from automl_api.models.runs import Metric, ModelRun
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.workflow_state import StaleFence
from automl_api.training import pipeline, worker
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


@pytest.fixture
def publication(phase1_engine, committed_tenant, monkeypatch):  # noqa: F811
    user_id, project_id = committed_tenant
    with Session(phase1_engine) as db, db.begin():
        dataset = Dataset(project_id=project_id, created_by_id=user_id, name="publication")
        db.add(dataset)
        db.flush()
        version = DatasetVersion(
            project_id=project_id,
            dataset_id=dataset.id,
            created_by_id=user_id,
            version_number=1,
            format=DatasetFormat.CSV,
            object_uri="s3://test/train.csv",
            content_hash="a" * 64,
        )
        db.add(version)
        db.flush()
        run = ModelRun(
            project_id=project_id,
            dataset_version_id=version.id,
            created_by_id=user_id,
            run_kind=RunKind.TRAINING,
            status=RunStatus.RUNNING,
            task_type=TaskType.REGRESSION,
            target_column="target",
            tags={"winner": "original"},
        )
        db.add(run)
        db.flush()
        attempt = WorkflowAttempt(
            project_id=project_id,
            model_run_id=run.id,
            stage="training_run",
            logical_key=f"training:{run.id}",
            workload_identity="project-training",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            status=AttemptStatus.RUNNING,
        )
        db.add(attempt)
        db.flush()
        run_id, attempt_id, fence = run.id, attempt.id, attempt.fencing_token
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    monkeypatch.setattr(pipeline, "get_session_factory", lambda: lambda: Session(phase1_engine))
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: Session(phase1_engine))
    result = pipeline.TournamentResult(
        metrics={"r2": 0.9},
        model=None,
        params={},
        primary_metric="r2",
        leaderboard=[
            {"model": "winner", "status": "succeeded", "model_artifact_uri": "s3://test/winner"}
        ],
    )
    return phase1_engine, run_id, attempt_id, fence, result


@pytest.mark.parametrize("invalid", ["superseded", "cancelled", "wrong_fence"])
@pytest.mark.parametrize("write", ["success", "partial", "failure"])
def test_stale_training_writes_cannot_change_run(publication, monkeypatch, invalid, write):
    engine, run_id, attempt_id, _, result = publication
    with Session(engine) as db, db.begin():
        if invalid == "wrong_fence":
            db.get(WorkflowAttempt, attempt_id).fencing_token = "replacement-fence"
        else:
            db.get(WorkflowAttempt, attempt_id).status = AttemptStatus(invalid)
    with pytest.raises(StaleFence):
        if write == "success":
            pipeline._persist_training_success(run_id, result, "mlflow-result")
        elif write == "partial":
            pipeline._persist_partial_leaderboard(run_id, result.leaderboard, "r2")
        else:
            pipeline._mark_failed(run_id, RuntimeError("late failure"))
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.status == RunStatus.RUNNING and run.tags == {"winner": "original"}
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 0
        )


def test_terminal_publication_and_two_replays_preserve_one_result(publication):
    engine, run_id, attempt_id, fence, result = publication
    assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    worker._complete_fenced_attempt(run_id, (attempt_id, fence))
    for _ in range(2):
        with pytest.raises(StaleFence):
            worker._complete_fenced_attempt(run_id, (attempt_id, fence))
        assert not worker._fail_fenced_attempt(run_id, (attempt_id, fence), RuntimeError("late"))
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        run = db.get(ModelRun, run_id)
        assert attempt.status == AttemptStatus.SUCCEEDED and attempt.terminal_cas_version == 1
        assert attempt.checkpoint_uri == "s3://test/winner"
        assert run.status == RunStatus.SUCCEEDED and run.failure_message is None
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 1
        )


def test_lost_result_commit_acknowledgement_does_not_duplicate_metrics(publication):
    engine, run_id, _, _, result = publication
    for _ in range(3):
        assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    with Session(engine) as db:
        assert (
            db.scalar(select(func.count()).select_from(Metric).where(Metric.model_run_id == run_id))
            == 1
        )


def test_same_attempt_cannot_replace_a_committed_training_result(publication):
    _, run_id, _, _, result = publication
    assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    result.metrics = {"r2": 0.1}
    with pytest.raises(StaleFence):
        pipeline._persist_training_success(run_id, result, "different-result")


def test_worker_recovers_committed_result_without_retraining(publication):
    engine, run_id, attempt_id, fence, result = publication
    assert not worker._resume_committed_result(run_id, (attempt_id, fence))
    assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    assert not worker._fail_fenced_attempt(
        run_id, (attempt_id, fence), RuntimeError("commit acknowledgement lost")
    )
    assert worker._begin_fenced_attempt(run_id) == (attempt_id, fence)
    assert worker._resume_committed_result(run_id, (attempt_id, fence))
    with Session(engine) as db:
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1


def test_observer_finalizes_committed_result_after_worker_disappears(publication):
    from unittest.mock import MagicMock

    from automl_api.services.reconciler import observe_training_ray_jobs

    engine, run_id, attempt_id, _, result = publication
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).ray_job_name = "lost-training-worker"
    assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    client = MagicMock()
    with Session(engine) as db:
        assert observe_training_ray_jobs(db, client) == 1
        db.commit()
        assert observe_training_ray_jobs(db, client) == 0
        db.commit()
        assert db.get(ModelRun, run_id).status == RunStatus.SUCCEEDED
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(WorkflowAttempt)
                .where(WorkflowAttempt.model_run_id == run_id)
            )
            == 1
        )
    client.ray_job.assert_not_called()


def test_late_progress_and_failure_cannot_change_committed_result(publication):
    engine, run_id, _, _, result = publication
    assert pipeline._persist_training_success(run_id, result, "mlflow-result")
    with Session(engine) as db:
        original_tags = dict(db.get(ModelRun, run_id).tags)
    pipeline._persist_partial_leaderboard(run_id, [{"model": "late", "status": "running"}], "r2")
    pipeline._persist_candidate_phase(run_id, "winner", "late")
    pipeline._mark_failed(run_id, RuntimeError("late failure"))
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.status == RunStatus.RUNNING and run.tags == original_tags
        assert run.failure_message is None


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_result_writers_publish_one_metric_set(publication, conflicting):
    from concurrent.futures import ThreadPoolExecutor
    from copy import deepcopy
    from threading import Barrier

    engine, run_id, _, _, result = publication
    barrier = Barrier(2)

    def publish(index):
        candidate = deepcopy(result)
        if conflicting and index:
            candidate.metrics = {"r2": 0.1}
        barrier.wait(timeout=10)
        try:
            return pipeline._persist_training_success(run_id, candidate, "mlflow-result")
        except StaleFence:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(publish, range(2)))
    assert sum(outcomes) == (1 if conflicting else 2)
    with Session(engine) as db:
        metrics = list(db.scalars(select(Metric).where(Metric.model_run_id == run_id)))
        assert len(metrics) == 1
        assert metrics[0].value in ({0.9, 0.1} if conflicting else {0.9})


def test_late_worker_cannot_damage_successful_replacement(publication, monkeypatch):
    engine, run_id, old_id, old_fence, result = publication
    with Session(engine) as db, db.begin():
        old = db.get(WorkflowAttempt, old_id)
        old.status = AttemptStatus.SUPERSEDED
        db.flush()
        replacement = WorkflowAttempt(
            project_id=old.project_id,
            model_run_id=run_id,
            stage="training_run",
            logical_key=old.logical_key,
            workload_identity=old.workload_identity,
            generation=2,
            fencing_token=uuid.uuid4().hex,
            status=AttemptStatus.RUNNING,
            predecessor_attempt_id=old.id,
        )
        db.add(replacement)
        db.flush()
        new_id, new_fence = replacement.id, replacement.fencing_token
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(new_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", new_fence)
    assert pipeline._persist_training_success(run_id, result, "replacement-result")
    worker._complete_fenced_attempt(run_id, (new_id, new_fence))
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(old_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", old_fence)
    with pytest.raises(StaleFence):
        pipeline._persist_training_success(run_id, result, "late-result")
    assert not worker._fail_fenced_attempt(run_id, (old_id, old_fence), RuntimeError("late"))
    with Session(engine) as db:
        run = db.get(ModelRun, run_id)
        assert run.status == RunStatus.SUCCEEDED and run.mlflow_run_id == "replacement-result"
        assert db.get(WorkflowAttempt, new_id).terminal_cas_version == 1
