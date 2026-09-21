from __future__ import annotations

import uuid
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import automl_api.services.training as training_service
import automl_api.training.pipeline as training_pipeline
import pytest
from automl_api.models.datasets import DatasetVersion
from automl_api.models.enums import RunStatus
from automl_api.models.workflows import DatasetSplitRevision
from automl_api.training.pipeline import TournamentResult
from kubernetes.client import ApiException


@pytest.mark.parametrize("desired_state", ["kubernetes_submission_pending", "kubernetes_submitted"])
def test_api_reads_do_not_fail_managed_analysis_before_the_outbox_job_exists(desired_state):
    from unittest.mock import MagicMock

    run = _run()
    run.status = RunStatus.QUEUED
    run.tags = {"desired_state": desired_state}
    client = MagicMock()
    client.job_state.return_value = "missing"

    training_service._sync_run_status(FakeSession(run), run, client)

    assert run.status == RunStatus.QUEUED
    client.job_state.assert_not_called()


def test_reconciler_observes_submitted_analysis_jobs():
    from unittest.mock import MagicMock

    from automl_api.services.reconciler import observe_analysis_jobs

    run = _run()
    run.status = RunStatus.QUEUED
    run.tags = {"desired_state": "kubernetes_submitted"}
    db = MagicMock()
    db.scalars.return_value = [run]
    client = MagicMock()
    client.job_state.return_value = "running"

    assert observe_analysis_jobs(db, client) == 1
    assert run.status == RunStatus.RUNNING
    client.job_state.assert_called_once_with(run.k8s_job_name)
    db.flush.assert_called_once()


def test_superseded_attempt_cannot_lock_run_for_shared_result_writes(monkeypatch):
    from unittest.mock import MagicMock

    from automl_api.models.enums import AttemptStatus
    from automl_api.services.workflow_state import StaleFence

    run_id = uuid.uuid4()
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(uuid.uuid4()))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", "old-fence")
    db = MagicMock()
    db.scalar.return_value = SimpleNamespace(
        model_run_id=run_id, fencing_token="old-fence", status=AttemptStatus.SUPERSEDED
    )
    with pytest.raises(StaleFence):
        training_pipeline._locked_run(db, run_id)
    assert db.scalar.call_count == 1


@pytest.mark.parametrize("initial_status", ["pending", "running"])
def test_ray_cancel_fences_worker_and_queues_cleanup_before_return(monkeypatch, initial_status):
    from unittest.mock import MagicMock

    from automl_api.models.enums import AttemptStatus, CommandStatus

    run = _run()
    run.tags["orchestrator"] = "kuberay"
    attempt = SimpleNamespace(
        id=uuid.uuid4(),
        status=AttemptStatus(initial_status),
        fencing_token="active-fence",
        terminal_cas_version=0,
    )
    db = MagicMock()
    db.scalar.return_value = attempt
    command = SimpleNamespace(id=uuid.uuid4(), status=CommandStatus.PENDING)
    monkeypatch.setattr(training_service, "require_project_role", lambda *args: None)
    monkeypatch.setattr(training_service, "get_training_run", lambda *args, **kwargs: run)
    begin = MagicMock(return_value=(command, False))
    enqueue = MagicMock()
    monkeypatch.setattr(training_service, "begin_command", begin)
    monkeypatch.setattr(training_service, "enqueue_outbox", enqueue)
    client = MagicMock()

    def assert_lock_order(*args, **kwargs):
        assert db.scalar.call_count in {2, 4}
        assert kwargs["with_for_update"]

    db.refresh.side_effect = assert_lock_order
    training_service.cancel_training_run(
        db, SimpleNamespace(id=uuid.uuid4()), run.project_id, run.id, client
    )
    assert run.status == RunStatus.CANCELLED
    assert attempt.status == AttemptStatus.CANCELLED
    assert attempt.terminal_cas_version == 1
    assert command.status == CommandStatus.SUCCEEDED
    client.delete_job.assert_not_called()
    client.delete_ray_job.assert_not_called()
    assert enqueue.call_args.kwargs["topic"] == "ray.training.cancel"
    assert enqueue.call_args.kwargs["payload"]["attempt_id"] == str(attempt.id)

    training_service.cancel_training_run(
        db, SimpleNamespace(id=uuid.uuid4()), run.project_id, run.id, client
    )
    begin.assert_called_once()
    enqueue.assert_called_once()


def test_cancel_follows_replacement_created_while_waiting_for_attempt_lock():
    from unittest.mock import MagicMock

    old, current = SimpleNamespace(id=uuid.uuid4()), SimpleNamespace(id=uuid.uuid4())
    db = MagicMock()
    db.scalar.side_effect = [old, current, current, current]
    assert training_service._lock_latest_training_attempt(db, _run()) is current


class FakeSession(AbstractContextManager):
    def __init__(
        self,
        run: SimpleNamespace,
        version: SimpleNamespace | None = None,
    ) -> None:
        self.run = run
        self.version = version
        self.split = SimpleNamespace(
            project_id=run.project_id,
            dataset_version_id=run.dataset_version_id,
            specification={"uris": {"train": "s3://train", "validation": "s3://validation"}},
        )
        self.added: list[object] = []
        self.commits = 0
        self.flushes = 0
        self.locked_reads = 0

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def get(self, model: object, object_id: uuid.UUID) -> SimpleNamespace | None:
        if model is DatasetVersion:
            return self.version
        if model is DatasetSplitRevision:
            return self.split
        return self.run

    def scalar(self, statement: object) -> SimpleNamespace:
        assert getattr(statement, "_for_update_arg", None) is not None
        self.locked_reads += 1
        return self.run

    def add(self, item: object) -> None:
        self.added.append(item)

    def commit(self) -> None:
        self.commits += 1

    def flush(self) -> None:
        self.flushes += 1

    def refresh(
        self,
        instance: object,
        *,
        with_for_update: bool = False,
    ) -> None:
        if with_for_update:
            self.locked_reads += 1


class FakeKubernetesClient:
    def __init__(self) -> None:
        self.deleted_jobs: list[str] = []

    def delete_job(self, job_name: str) -> None:
        self.deleted_jobs.append(job_name)


class FakeMlflowRun(AbstractContextManager):
    def __enter__(self) -> SimpleNamespace:
        return SimpleNamespace(info=SimpleNamespace(run_id="mlflow-parent"))

    def __exit__(self, *args: object) -> None:
        return None


def _run(*, status: RunStatus = RunStatus.RUNNING) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        dataset_version_id=uuid.uuid4(),
        status=status,
        task_type=SimpleNamespace(value="regression"),
        run_name="cancellation-test",
        params={"split_revision_id": str(uuid.uuid4())},
        k8s_job_name="automl-train-test",
        started_at=None,
        tags={
            "completed_candidates": 1,
            "current_candidate": "RandomForestRegressor",
            "candidate_phase": "evaluating",
            "leaderboard": [
                {
                    "rank": 1,
                    "model": "Ridge",
                    "status": "succeeded",
                    "primary_score": 1.2,
                    "metrics": {"rmse": 1.2},
                    "error": None,
                },
                {
                    "rank": None,
                    "model": "RandomForestRegressor",
                    "status": "running",
                    "primary_score": None,
                    "metrics": {},
                    "error": None,
                },
                {
                    "rank": None,
                    "model": "ElasticNet",
                    "status": "pending",
                    "primary_score": None,
                    "metrics": {},
                    "error": None,
                },
            ],
        },
        finished_at=None,
        mlflow_run_id=None,
    )


TERMINAL_STATUSES = (
    RunStatus.SUCCEEDED,
    RunStatus.FAILED,
    RunStatus.CANCELLED,
    RunStatus.PREEMPTED,
)


def test_cancel_preserves_results_and_marks_only_interrupted_candidate(monkeypatch) -> None:
    run = _run()
    db = FakeSession(run)
    client = FakeKubernetesClient()
    monkeypatch.setattr(training_service, "require_project_role", lambda *args: None)
    monkeypatch.setattr(training_service, "get_training_run", lambda *args, **kwargs: run)

    result = training_service.cancel_training_run(
        db,
        SimpleNamespace(),
        run.project_id,
        run.id,
        client,
    )

    entries = {entry["model"]: entry for entry in result.tags["leaderboard"]}
    assert result.status == RunStatus.CANCELLED
    assert result.finished_at is not None
    assert client.deleted_jobs == ["automl-train-test"]
    assert entries["Ridge"]["status"] == "succeeded"
    assert entries["Ridge"]["metrics"] == {"rmse": 1.2}
    assert entries["RandomForestRegressor"]["status"] == "cancelled"
    assert "cancelled before" in entries["RandomForestRegressor"]["error"]
    assert entries["ElasticNet"]["status"] == "pending"
    assert result.tags["cancelled_candidate"] == "RandomForestRegressor"
    assert result.tags["current_candidate"] is None
    assert result.tags["candidate_phase"] == "cancelled"
    assert result.tags["cancelled_at"] == result.tags["candidate_phase_updated_at"]
    assert result.tags["completed_candidates"] == 1
    assert db.flushes == 1


def test_repeated_cancel_preserves_interrupted_candidate(monkeypatch) -> None:
    run = _run()
    db = FakeSession(run)
    client = FakeKubernetesClient()
    monkeypatch.setattr(training_service, "require_project_role", lambda *args: None)
    monkeypatch.setattr(training_service, "get_training_run", lambda *args, **kwargs: run)

    training_service.cancel_training_run(
        db,
        SimpleNamespace(),
        run.project_id,
        run.id,
        client,
    )
    result = training_service.cancel_training_run(
        db,
        SimpleNamespace(),
        run.project_id,
        run.id,
        client,
    )

    assert result.status == RunStatus.CANCELLED
    assert result.tags["cancelled_candidate"] == "RandomForestRegressor"
    assert result.tags["current_candidate"] is None
    assert client.deleted_jobs == ["automl-train-test"]
    assert db.locked_reads == 2
    assert db.flushes == 2


def test_resource_poll_cannot_restore_stale_candidate_after_cancel(monkeypatch) -> None:
    run = _run()
    started_at = datetime.now(UTC) - timedelta(minutes=2)
    run.params = {"candidate_limit": 3}
    run.started_at = started_at
    run.queued_at = started_at
    run.created_at = started_at
    run.cpu_request_cores = 1.0
    run.cpu_limit_cores = 2.0
    run.memory_request_mb = 1024
    run.memory_limit_mb = 2048
    run.gpu_requested = False
    db = FakeSession(run)
    cancelled_at = datetime.now(UTC)
    monkeypatch.setattr(training_service, "get_training_run", lambda *args, **kwargs: run)

    def cancel_before_resource_merge(
        instance: SimpleNamespace,
        *,
        with_for_update: bool = False,
    ) -> None:
        instance.status = RunStatus.CANCELLED
        instance.finished_at = cancelled_at
        instance.tags = training_service._cancelled_training_tags(instance.tags, cancelled_at)
        FakeSession.refresh(db, instance, with_for_update=with_for_update)

    db.refresh = cancel_before_resource_merge  # type: ignore[method-assign]
    client = SimpleNamespace(
        training_resource_usage=lambda *args: {
            "telemetry_available": True,
            "cpu_usage_cores": 0.5,
            "memory_usage_mb": 512,
        }
    )

    usage = training_service.training_resources(
        db,
        SimpleNamespace(),
        run.project_id,
        run.id,
        client,
    )

    assert run.tags["current_candidate"] is None
    assert run.tags["cancelled_candidate"] == "RandomForestRegressor"
    assert run.tags["candidate_phase"] == "cancelled"
    assert run.tags["resource_usage"]["cpu_usage_cores"] == 0.5
    assert usage.status == RunStatus.CANCELLED
    assert usage.current_candidate is None
    assert usage.last_candidate == "RandomForestRegressor"
    assert usage.current_phase == "cancelled"


def test_terminal_image_waiting_failure_deletes_job_before_marking_run_failed() -> None:
    run = _run()
    db = FakeSession(run)

    class FatalWaitingClient(FakeKubernetesClient):
        def job_state(self, job_name: str) -> str:
            assert job_name == "automl-train-test"
            return "terminal_waiting_failure"

        def job_failure_details(self, job_name: str) -> tuple[str, str]:
            assert job_name == "automl-train-test"
            return (
                "TRAINING_IMAGE_NOT_PRESENT",
                "The image is absent and imagePullPolicy is Never.",
            )

        def delete_job(self, job_name: str) -> None:
            assert run.status == RunStatus.RUNNING
            super().delete_job(job_name)

    client = FatalWaitingClient()

    training_service._sync_run_status(db, run, client)

    assert client.deleted_jobs == ["automl-train-test"]
    assert run.status == RunStatus.FAILED
    assert run.failure_code == "TRAINING_IMAGE_NOT_PRESENT"
    assert run.failure_message == "The image is absent and imagePullPolicy is Never."
    assert "not available inside this Kubernetes cluster" in run.plain_english_failure
    assert run.finished_at is not None
    assert db.flushes == 1


def test_terminal_waiting_failure_is_rechecked_after_run_lock() -> None:
    run = _run()
    db = FakeSession(run)

    class RecoveredClient(FakeKubernetesClient):
        states = iter(("terminal_waiting_failure", "running"))

        def job_state(self, job_name: str) -> str:
            return next(self.states)

        def job_failure_details(self, job_name: str) -> tuple[str, str]:
            raise AssertionError("a recovered Job must not be reported as failed")

    client = RecoveredClient()

    training_service._sync_run_status(db, run, client)

    assert run.status == RunStatus.RUNNING
    assert run.started_at is not None
    assert client.deleted_jobs == []
    assert db.locked_reads == 1
    assert db.flushes == 1


def test_image_pull_backoff_uses_first_observation_before_failing() -> None:
    run = _run()
    db = FakeSession(run)

    class BackoffClient(FakeKubernetesClient):
        def job_state(self, job_name: str) -> str:
            return "image_pull_backoff"

        def job_failure_details(self, job_name: str) -> tuple[str, str]:
            return "TRAINING_IMAGE_PULL_FAILED", "Registry is unavailable."

    client = BackoffClient()

    training_service._sync_run_status(db, run, client)

    first_seen = run.tags[training_service._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG]
    assert datetime.fromisoformat(first_seen).tzinfo is not None
    assert run.status == RunStatus.RUNNING
    assert client.deleted_jobs == []

    run.tags[training_service._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = (
        datetime.now(UTC) - timedelta(minutes=3)
    ).isoformat()
    training_service._sync_run_status(db, run, client)

    assert run.status == RunStatus.FAILED
    assert run.failure_code == "TRAINING_IMAGE_PULL_FAILED"
    assert client.deleted_jobs == ["automl-train-test"]


def test_image_pull_backoff_recovery_clears_first_observation() -> None:
    run = _run()
    run.tags[training_service._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG] = (
        datetime.now(UTC) - timedelta(seconds=30)
    ).isoformat()
    db = FakeSession(run)

    class RecoveredClient(FakeKubernetesClient):
        states = iter(("image_pull_backoff", "running"))

        def job_state(self, job_name: str) -> str:
            return next(self.states)

    client = RecoveredClient()

    training_service._sync_run_status(db, run, client)

    assert training_service._IMAGE_PULL_BACKOFF_FIRST_SEEN_TAG not in run.tags
    assert run.status == RunStatus.RUNNING
    assert client.deleted_jobs == []


def test_terminal_waiting_cleanup_failure_does_not_mark_run_failed() -> None:
    run = _run()
    db = FakeSession(run)

    class CleanupFailureClient(FakeKubernetesClient):
        def job_state(self, job_name: str) -> str:
            return "terminal_waiting_failure"

        def job_failure_details(self, job_name: str) -> tuple[str, str]:
            return "TRAINING_IMAGE_PULL_FAILED", "Registry is unavailable."

        def delete_job(self, job_name: str) -> None:
            raise ApiException(status=503, reason="Kubernetes API unavailable")

    with pytest.raises(ApiException):
        training_service._sync_run_status(db, run, CleanupFailureClient())

    assert run.status == RunStatus.RUNNING
    assert db.flushes == 0


def test_terminal_waiting_cleanup_404_still_marks_run_failed() -> None:
    run = _run()
    db = FakeSession(run)

    class MissingDuringCleanupClient(FakeKubernetesClient):
        def job_state(self, job_name: str) -> str:
            return "terminal_waiting_failure"

        def job_failure_details(self, job_name: str) -> tuple[str, str]:
            return "TRAINING_IMAGE_NOT_PRESENT", "The image is absent."

        def delete_job(self, job_name: str) -> None:
            raise ApiException(status=404, reason="already deleted")

    training_service._sync_run_status(db, run, MissingDuringCleanupClient())

    assert run.status == RunStatus.FAILED
    assert run.failure_code == "TRAINING_IMAGE_NOT_PRESENT"
    assert db.flushes == 1


@pytest.mark.parametrize("terminal_status", TERMINAL_STATUSES)
def test_terminal_run_is_not_restarted_by_worker(
    monkeypatch,
    terminal_status: RunStatus,
) -> None:
    run = _run(status=terminal_status)
    db = FakeSession(run)
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)

    result = training_pipeline.execute_training_run(run.id)

    assert result == {}
    assert run.status == terminal_status
    assert run.started_at is None
    assert db.locked_reads == 1
    assert db.commits == 0


@pytest.mark.parametrize("terminal_status", TERMINAL_STATUSES)
def test_terminal_run_rejects_late_candidate_phase_update(
    monkeypatch,
    terminal_status: RunStatus,
) -> None:
    run = _run(status=terminal_status)
    original_tags = run.tags
    db = FakeSession(run)
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)

    training_pipeline._persist_candidate_phase(run.id, "ElasticNet", "evaluating")

    assert run.tags is original_tags
    assert run.tags["current_candidate"] == "RandomForestRegressor"
    assert db.locked_reads == 1
    assert db.commits == 0


@pytest.mark.parametrize("terminal_status", TERMINAL_STATUSES)
def test_terminal_run_rejects_late_partial_leaderboard_update(
    monkeypatch,
    terminal_status: RunStatus,
) -> None:
    run = _run(status=terminal_status)
    original_tags = run.tags
    db = FakeSession(run)
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)

    training_pipeline._persist_partial_leaderboard(
        run.id,
        [
            {
                "rank": 1,
                "model": "RandomForestRegressor",
                "status": "succeeded",
                "primary_score": 0.9,
                "metrics": {"r2": 0.9},
                "error": None,
            }
        ],
        "r2",
    )

    assert run.tags is original_tags
    assert run.tags["current_candidate"] == "RandomForestRegressor"
    assert db.locked_reads == 1
    assert db.commits == 0


@pytest.mark.parametrize("terminal_status", TERMINAL_STATUSES)
def test_terminal_run_cannot_be_overwritten_by_late_failure(
    monkeypatch,
    terminal_status: RunStatus,
) -> None:
    run = _run(status=terminal_status)
    db = FakeSession(run)
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)

    training_pipeline._mark_failed(run.id, RuntimeError("late worker error"))

    assert run.status == terminal_status
    assert run.finished_at is None
    assert db.locked_reads == 1
    assert db.commits == 0


@pytest.mark.parametrize("terminal_status", TERMINAL_STATUSES)
def test_terminal_run_cannot_be_overwritten_by_late_success(
    monkeypatch,
    terminal_status: RunStatus,
) -> None:
    run = _run(status=terminal_status)
    db = FakeSession(run)
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)
    result = TournamentResult(
        metrics={"rmse": 0.9},
        model=object(),
        params={},
        leaderboard=[
            {
                "model": "RandomForestRegressor",
                "status": "succeeded",
                "metrics": {"rmse": 0.9},
                "mlflow_run_id": "candidate-run",
            }
        ],
        primary_metric="rmse",
    )

    persisted = training_pipeline._persist_training_success(
        run.id,
        result,
        "parent-run",
    )

    assert not persisted
    assert run.status == terminal_status
    assert run.mlflow_run_id is None
    assert run.finished_at is None
    assert db.added == []
    assert db.locked_reads == 1
    assert db.commits == 0


def test_worker_returns_no_metrics_when_success_persistence_is_rejected(monkeypatch) -> None:
    run = _run(status=RunStatus.QUEUED)
    db = FakeSession(run, version=SimpleNamespace())
    result = TournamentResult(
        metrics={"rmse": 0.9},
        model=object(),
        params={"winner": "RandomForestRegressor"},
        leaderboard=[
            {
                "model": "RandomForestRegressor",
                "status": "succeeded",
                "metrics": {"rmse": 0.9},
                "mlflow_run_id": "candidate-run",
            }
        ],
        primary_metric="rmse",
    )
    monkeypatch.setattr(training_pipeline, "get_session_factory", lambda: lambda: db)
    monkeypatch.setattr(
        training_pipeline,
        "_load_prepared_training_frames",
        lambda split, **_kwargs: (object(), object()),
    )
    monkeypatch.setattr(
        training_pipeline,
        "_fit_model",
        lambda dataframe, model_run, validation: result,
    )
    monkeypatch.setattr(
        training_pipeline,
        "get_settings",
        lambda: SimpleNamespace(mlflow_tracking_uri="http://mlflow.test"),
    )
    monkeypatch.setattr(training_pipeline.mlflow, "set_tracking_uri", lambda *args: None)
    monkeypatch.setattr(training_pipeline.mlflow, "set_experiment", lambda *args: None)
    monkeypatch.setattr(
        training_pipeline.mlflow,
        "start_run",
        lambda *args, **kwargs: FakeMlflowRun(),
    )
    monkeypatch.setattr(training_pipeline.mlflow, "set_tags", lambda *args: None)
    monkeypatch.setattr(training_pipeline.mlflow, "log_params", lambda *args: None)
    monkeypatch.setattr(training_pipeline.mlflow, "log_dict", lambda *args: None)
    monkeypatch.setattr(training_pipeline, "_log_metrics_synchronously", lambda *args: None)
    monkeypatch.setattr(training_pipeline.mlflow_sklearn, "log_model", lambda *args, **kwargs: None)
    monkeypatch.setattr(training_pipeline, "_persist_training_success", lambda *args: False)
    monkeypatch.setattr(
        training_pipeline,
        "_mark_failed",
        lambda *args: pytest.fail("rejected persistence must not be marked as a failure"),
    )

    metrics = training_pipeline.execute_training_run(run.id)

    assert metrics == {}
    assert run.status == RunStatus.RUNNING
    assert run.started_at is not None
    assert db.locked_reads == 1
    assert db.commits == 1
