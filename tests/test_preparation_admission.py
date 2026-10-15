import time
import uuid
from datetime import UTC, datetime, timedelta
from threading import Event, current_thread
from threading import enumerate as threads
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from automl_api import preparation_worker as worker
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.workflows import WorkflowAttempt
from automl_api.services.attempt_identity import attempt_audience
from automl_api.services.workflow_state import StaleFence
from automl_api.training import attempt_admission
from sqlalchemy.orm import Session
from test_local_profiling_delivery import profile_source  # noqa: F401
from test_phase1_postgres_contracts import committed_tenant, phase1_engine  # noqa: F401


@pytest.fixture(params=[WorkflowStage.SPLITTER, WorkflowStage.PREPARATION])
def dataset_scope(profile_source, monkeypatch, tmp_path, request):  # noqa: F811
    engine, user_id, project_id, dataset_id, version_id, store = profile_source
    stage = request.param
    attempt_id, fence = uuid.uuid4(), uuid.uuid4().hex
    with Session(engine) as db, db.begin():
        profile = ProfilingJob(
            project_id=project_id,
            created_by_id=user_id,
            dataset_id=dataset_id,
            dataset_version_id=version_id,
            status="queued",
            overview_json={"execution_mode": "kuberay", "workflow_attempt_id": str(attempt_id)},
        )
        db.add(profile)
        db.flush()
        db.add(
            WorkflowAttempt(
                id=attempt_id,
                project_id=project_id,
                stage=stage,
                dataset_version_id=version_id,
                logical_key=f"profile:{profile.id}:{stage.value}",
                workload_identity=f"sceptre-{stage.value}",
                generation=1,
                fencing_token=fence,
                status=AttemptStatus.SUBMITTED,
            )
        )
        profile_id = profile.id
    monkeypatch.setattr(worker, "get_session_factory", lambda: lambda: Session(engine))
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    monkeypatch.setenv("AUTOML_ATTEMPT_ID", str(attempt_id))
    monkeypatch.setenv("AUTOML_FENCING_TOKEN", fence)
    path = tmp_path / "pod-token"
    path.write_text("projected-unit-token")
    monkeypatch.setenv("AUTOML_IDENTITY_TOKEN_FILE", str(path))
    monkeypatch.setenv("ATTEMPT_CONTROL_BASE_URL", "https://control.invalid/internal/attempts")
    proof = {
        "attempt_id": str(attempt_id),
        "project_id": str(project_id),
        "generation": 1,
        "audience": attempt_audience(attempt_id, fence),
        "expires_at": int(time.time()) + 30,
    }

    def response(*_a, **_k):
        return httpx.Response(
            200, json=proof, request=httpx.Request("POST", "https://control.invalid")
        )

    post = MagicMock(side_effect=response)
    monkeypatch.setattr(attempt_admission.httpx, "post", post)
    return engine, profile_id, attempt_id, fence, stage, proof, post, store


def test_dataset_start_admits_one_owner_and_renews_within_proof(dataset_scope):
    engine, profile_id, attempt_id, fence, stage, proof, _, _ = dataset_scope
    assert worker._begin_attempt(profile_id, stage, worker_id="owner") == (attempt_id, fence)
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, attempt_id)
        assert attempt.lease_owner == "owner"
        assert attempt.lease_expires_at == datetime.fromtimestamp(proof["expires_at"], UTC)
    with pytest.raises(StaleFence):
        worker._begin_attempt(profile_id, stage, worker_id="another")
    proof["expires_at"] += 10
    worker._heartbeat_attempt(profile_id, (attempt_id, fence), "owner")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).lease_expires_at == datetime.fromtimestamp(
            proof["expires_at"], UTC
        )


@pytest.mark.parametrize("operation", ["start", "heartbeat", "progress", "publish", "failure"])
def test_expired_dataset_worker_cannot_renew_or_publish(dataset_scope, operation):
    engine, profile_id, attempt_id, fence, stage, _, _, _ = dataset_scope
    worker._begin_attempt(profile_id, stage, worker_id="owner")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
        project_id = db.get(WorkflowAttempt, attempt_id).project_id
    with pytest.raises(StaleFence):
        if operation == "start":
            worker._begin_attempt(profile_id, stage, worker_id="owner")
        elif operation == "heartbeat":
            worker._heartbeat_attempt(profile_id, (attempt_id, fence), "owner")
        elif operation == "progress":
            worker._record_preparation_progress(
                profile_id, attempt_id, fence, progress=0.5, milestone="progress"
            )
        elif operation == "failure":
            worker._record_failure(
                profile_id,
                attempt_id,
                ValueError("late error"),
                fencing_token=fence,
                worker_id="owner",
            )
        elif stage == WorkflowStage.SPLITTER:
            worker._persist_split(profile_id, attempt_id, fence, {"project_id": str(project_id)})
        else:
            worker._persist_profile(
                profile_id, attempt_id, fence, {}, SimpleNamespace(project_id=project_id)
            )
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).terminal_cas_version == 0
        assert db.get(ProfilingJob, profile_id).failure_message is None


def test_unverified_dataset_identity_never_claims(dataset_scope):
    engine, profile_id, attempt_id, _, stage, _, post, _ = dataset_scope
    post.side_effect = httpx.ConnectError("admission unavailable")
    with pytest.raises(StaleFence):
        worker._begin_attempt(profile_id, stage, worker_id="owner")
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.SUBMITTED
        assert db.get(WorkflowAttempt, attempt_id).lease_owner is None


def test_run_stage_renews_and_stops_thread(dataset_scope, monkeypatch):
    engine, profile_id, _, _, stage, _, _, _ = dataset_scope
    renewed = Event()
    original = worker._heartbeat_attempt

    def heartbeat(*args):
        original(*args)
        renewed.set()

    monkeypatch.setattr(worker, "_heartbeat_attempt", heartbeat)
    monkeypatch.setattr(worker, "HEARTBEAT_SECONDS", 0.01)

    def compute(*_a, **_k):
        assert renewed.wait(3)
        return {} if stage == WorkflowStage.SPLITTER else ({}, SimpleNamespace())

    monkeypatch.setattr(worker, "split_dataset", compute)
    monkeypatch.setattr(worker, "prepare_profile", compute)
    publish = MagicMock()
    monkeypatch.setattr(worker, "_persist_split", publish)
    monkeypatch.setattr(worker, "_persist_profile", publish)
    worker.run_stage(profile_id, stage)
    publish.assert_called_once()
    assert not any(t.name == "dataset-attempt-heartbeat" for t in threads())


def test_stale_failure_cannot_overwrite_replacement_profile(dataset_scope):
    engine, profile_id, attempt_id, fence, stage, _, _, _ = dataset_scope
    worker._begin_attempt(profile_id, stage, worker_id="owner")
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, attempt_id).status = AttemptStatus.SUPERSEDED
        db.get(ProfilingJob, profile_id).failure_message = "replacement evidence"
    worker._record_failure(
        profile_id, attempt_id, ValueError("old error"), fencing_token=fence, worker_id="owner"
    )
    with Session(engine) as db:
        assert db.get(ProfilingJob, profile_id).failure_message == "replacement evidence"
        assert db.get(WorkflowAttempt, attempt_id).terminal_reason is None


@pytest.mark.parametrize("error", [ValueError("invalid input"), StaleFence("uncertain ownership")])
def test_stage_error_stops_heartbeat_without_stale_failure_write(dataset_scope, monkeypatch, error):
    engine, profile_id, attempt_id, _, stage, _, _, _ = dataset_scope

    def compute(*_a, **_k):
        raise error

    monkeypatch.setattr(worker, "split_dataset", compute)
    monkeypatch.setattr(worker, "prepare_profile", compute)
    with pytest.raises(type(error)):
        worker.run_stage(profile_id, stage)
    with Session(engine) as db:
        expected = AttemptStatus.RUNNING if isinstance(error, StaleFence) else AttemptStatus.FAILED
        assert db.get(WorkflowAttempt, attempt_id).status == expected
    assert not any(t.name == "dataset-attempt-heartbeat" for t in threads())


def test_failed_heartbeat_prevents_stage_publication(dataset_scope, monkeypatch):
    engine, profile_id, attempt_id, _, stage, _, _, _ = dataset_scope
    monkeypatch.setattr(worker, "HEARTBEAT_SECONDS", 0.01)

    heartbeat_started = Event()
    heartbeat_threads = []

    def broken(*_):
        heartbeat_threads.append(current_thread())
        heartbeat_started.set()
        raise StaleFence("admission unavailable")

    monkeypatch.setattr(worker, "_heartbeat_attempt", broken)

    def compute(*_a, **_k):
        assert heartbeat_started.wait(3)
        thread = heartbeat_threads[0]
        thread.join(timeout=3)
        assert not thread.is_alive()
        return {} if stage == WorkflowStage.SPLITTER else ({}, SimpleNamespace())

    monkeypatch.setattr(worker, "split_dataset", compute)
    monkeypatch.setattr(worker, "prepare_profile", compute)
    publish = MagicMock()
    monkeypatch.setattr(worker, "_persist_split", publish)
    monkeypatch.setattr(worker, "_persist_profile", publish)
    with pytest.raises(StaleFence):
        worker.run_stage(profile_id, stage)
    publish.assert_not_called()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, attempt_id).status == AttemptStatus.RUNNING
        assert db.get(ProfilingJob, profile_id).failure_message is None
    assert not any(t.name == "dataset-attempt-heartbeat" for t in threads())


def test_mismatched_output_project_is_rejected_before_storage(dataset_scope, monkeypatch):
    _, profile_id, attempt_id, fence, stage, _, _, _ = dataset_scope
    store = MagicMock()
    monkeypatch.setattr(worker, "get_object_store", lambda: store)
    with pytest.raises(StaleFence, match="another project"):
        if stage == WorkflowStage.SPLITTER:
            worker._persist_split(profile_id, attempt_id, fence, {"project_id": str(uuid.uuid4())})
        else:
            worker._persist_profile(
                profile_id, attempt_id, fence, {}, SimpleNamespace(project_id=uuid.uuid4())
            )
    store.put_bytes.assert_not_called()


@pytest.mark.parametrize("invalid", ["failed_profile", "moved_profile", "logical_key"])
def test_admission_server_denies_inactive_dataset_scope_before_token_review(
    dataset_scope, invalid, monkeypatch
):
    from automl_api.services import attempt_identity

    review = MagicMock()
    monkeypatch.setattr(
        attempt_identity.client,
        "AuthenticationV1Api",
        lambda: SimpleNamespace(create_token_review=review),
    )
    engine, profile_id, attempt_id, _, _, _, _, _ = dataset_scope
    with Session(engine) as db, db.begin():
        if invalid == "logical_key":
            db.get(WorkflowAttempt, attempt_id).logical_key = "unrelated-scope"
        elif invalid == "moved_profile":
            profile = db.get(ProfilingJob, profile_id)
            profile.overview_json = {
                **profile.overview_json,
                "workflow_attempt_id": str(uuid.uuid4()),
            }
        else:
            db.get(ProfilingJob, profile_id).status = "failed"
    with pytest.raises(StaleFence), Session(engine) as db, db.begin():
        attempt_identity.admit_attempt(db, attempt_id, "not-a-token", MagicMock())
    review.assert_not_called()


def test_late_failure_preserves_inactive_profile(dataset_scope):
    engine, profile_id, attempt_id, fence, stage, _, _, _ = dataset_scope
    worker._begin_attempt(profile_id, stage, worker_id="owner")
    with Session(engine) as db, db.begin():
        profile = db.get(ProfilingJob, profile_id)
        profile.status = "failed"
        profile.failure_message = "control-plane decision"
    worker._record_failure(
        profile_id,
        attempt_id,
        ValueError("late worker error"),
        fencing_token=fence,
        worker_id="owner",
    )
    with Session(engine) as db:
        assert db.get(ProfilingJob, profile_id).failure_message == "control-plane decision"
        assert db.get(WorkflowAttempt, attempt_id).terminal_reason is None
