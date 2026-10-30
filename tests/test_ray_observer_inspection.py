"""Committed SQL checks for training/splitter/preparation Ray observation."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, RunKind, RunStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import WorkflowAttempt, WorkflowCommand
from automl_api.workflow_reconciler import observe_once
from kubernetes.client import ApiException
from ray_receipt_fixtures import seed_ray_receipt
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_local_profiling_delivery import profile_source as profile_source
from test_phase1_postgres_contracts import committed_tenant as committed_tenant
from test_phase1_postgres_contracts import phase1_engine as phase1_engine
from test_training_parent_expiry import expired as expired
from test_training_publication_postgres import publication as publication
from test_training_trial_enrollment import trial_scope as trial_scope


@pytest.fixture(params=["training", "splitter", "preparation"])
def ray_scope(profile_source, request):
    from test_ray_observer_identity import labeled_observation

    engine, user_id, project_id, dataset_id, version_id, _ = profile_source
    kind = request.param
    attempt_id = uuid.uuid4()
    with Session(engine) as db, db.begin():
        if kind == "training":
            resource = ModelRun(
                project_id=project_id,
                dataset_version_id=version_id,
                created_by_id=user_id,
                run_kind=RunKind.TRAINING,
                task_type="regression",
                status=RunStatus.RUNNING,
                k8s_namespace="observer-test",
                tags={"desired_state": "ray_submitted"},
            )
        else:
            resource = ProfilingJob(
                project_id=project_id,
                dataset_id=dataset_id,
                dataset_version_id=version_id,
                created_by_id=user_id,
                status="running",
                overview_json={
                    "workflow_attempt_id": str(attempt_id),
                    "ray_job_namespace": "observer-test",
                },
            )
        db.add(resource)
        db.flush()
        attempt = WorkflowAttempt(
            id=attempt_id,
            project_id=project_id,
            stage=WorkflowStage.TRAINING_RUN if kind == "training" else WorkflowStage(kind),
            logical_key=f"training:{resource.id}"
            if kind == "training"
            else f"profile:{resource.id}:{kind}",
            model_run_id=resource.id if kind == "training" else None,
            dataset_version_id=version_id,
            workload_identity="observer-test",
            generation=1,
            fencing_token=uuid.uuid4().hex,
            status=AttemptStatus.SUBMITTED,
            retry_budget=3,
            ray_job_name=f"ray-{attempt_id.hex}",
            lease_owner="worker",
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        db.add(attempt)
        command = WorkflowCommand(
            project_id=project_id,
            actor_id=user_id,
            operation="training.launch" if kind == "training" else "dataset.profile",
            idempotency_key=uuid.uuid4().hex,
            request_hash="a" * 64,
            resource_type="model_run" if kind == "training" else "profiling_job",
            resource_id=resource.id,
        )

        db.add(command)
        db.flush()
        seed_ray_receipt(db, attempt, resource, command, "observer-test")
        resource_id = resource.id
        native_job = labeled_observation(
            attempt, "observer-test", {"jobStatus": "RUNNING", "jobId": "observed-id"}
        )
    k8s = MagicMock()
    k8s.ray_job.return_value = native_job
    return engine, attempt_id, resource_id, kind, k8s


def factory_for(engine):
    return lambda: Session(engine)


def test_ray_read_releases_connection_and_parent_row_locks(ray_scope):
    engine, identity, resource_id, kind, k8s = ray_scope
    checked_out = []

    def read(*_, **__):
        checked_out.append(engine.pool.checkedout())
        assert checked_out[-1] == 0
        with Session(engine) as db, db.begin():
            db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == identity)
                .with_for_update(nowait=True)
            )
            model = ModelRun if kind == "training" else ProfilingJob
            db.scalar(select(model).where(model.id == resource_id).with_for_update(nowait=True))
        return k8s.ray_job.return_value

    k8s.ray_job.side_effect = read
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    assert checked_out == [0]
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.RUNNING
        assert db.get(WorkflowAttempt, identity).ray_submission_id == "observed-id"


@pytest.mark.parametrize("change", ["fence", "lease", "resource", "cancel_attempt", "job_name"])
def test_changed_state_prevents_publication_of_old_missing_observation(ray_scope, change):
    engine, identity, resource_id, kind, k8s = ray_scope
    changed = []

    def read(*_, **__):
        with Session(engine) as db, db.begin():
            attempt = db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == identity)
                .with_for_update(nowait=True)
            )
            if change == "fence":
                attempt.fencing_token = "new-fence"
            elif change == "lease":
                attempt.lease_expires_at = datetime.now(UTC) + timedelta(hours=1)
            elif change == "cancel_attempt":
                attempt.status = AttemptStatus.CANCELLED
            elif change == "job_name":
                attempt.ray_job_name = "new-physical-job"
            elif kind == "training":
                db.get(ModelRun, resource_id).status = RunStatus.CANCELLED
            else:
                profile = db.get(ProfilingJob, resource_id)
                profile.overview_json = {
                    **profile.overview_json,
                    "workflow_attempt_id": str(uuid.uuid4()),
                }
        changed.append(True)
        raise ApiException(status=404)

    k8s.ray_job.side_effect = read
    observe_once(factory_for(engine), k8s=k8s)
    assert changed == [True]
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, identity)
        assert attempt.status == (
            AttemptStatus.CANCELLED if change == "cancel_attempt" else AttemptStatus.SUBMITTED
        )
        assert not list(
            db.scalars(
                select(WorkflowAttempt.id).where(WorkflowAttempt.predecessor_attempt_id == identity)
            )
        )
        if kind == "training" and change == "resource":
            assert db.get(ModelRun, resource_id).status == RunStatus.CANCELLED
    k8s.delete_ray_job.assert_not_called()


def test_competing_missing_observations_create_one_generation(ray_scope):
    engine, identity, _, _, k8s = ray_scope
    competing = []

    def read(*_, **__):
        if not competing:
            competing.append(None)
            competing[0] = observe_once(factory_for(engine), k8s=k8s)
        raise ApiException(status=404)

    k8s.ray_job.side_effect = read
    observe_once(factory_for(engine), k8s=k8s)
    assert competing == [1]
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.SUPERSEDED
        assert (
            len(
                list(
                    db.scalars(
                        select(WorkflowAttempt.id).where(
                            WorkflowAttempt.predecessor_attempt_id == identity
                        )
                    )
                )
            )
            == 1
        )
    k8s.delete_ray_job.assert_not_called()


def test_unavailable_ray_read_preserves_work_and_can_retry(ray_scope):
    engine, identity, _, _, k8s = ray_scope
    k8s.ray_job.side_effect = ApiException(status=503)
    observe_once(factory_for(engine), k8s=k8s)
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.SUBMITTED
    k8s.ray_job.side_effect = None
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.RUNNING


def test_expired_lease_recovers_without_ray_read(ray_scope):
    engine, identity, _, _, k8s = ray_scope
    with Session(engine) as db, db.begin():
        db.get(WorkflowAttempt, identity).lease_expires_at = datetime.now(UTC) - timedelta(
            seconds=1
        )
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    k8s.ray_job.assert_not_called()
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.SUPERSEDED


def test_terminal_result_wins_over_inflight_missing_observation(ray_scope):
    engine, identity, resource_id, kind, k8s = ray_scope
    committed = []

    def read(*_, **__):
        with Session(engine) as db, db.begin():
            attempt = db.scalar(
                select(WorkflowAttempt)
                .where(WorkflowAttempt.id == identity)
                .with_for_update(nowait=True)
            )
            if kind == "training":
                attempt.status = AttemptStatus.RUNNING
                run = db.get(ModelRun, resource_id)
                run.tags = {
                    **run.tags,
                    "training_result_receipt": {"attempt_id": str(identity)},
                    "winner_model_artifact_uri": "s3://test/committed-model",
                }
            else:
                attempt.status = AttemptStatus.SUCCEEDED
                db.get(ProfilingJob, resource_id).status = "succeeded"
        committed.append(True)
        raise ApiException(status=404)

    k8s.ray_job.side_effect = read
    observe_once(factory_for(engine), k8s=k8s)
    assert committed == [True]
    with Session(engine) as db:
        assert db.get(WorkflowAttempt, identity).status == AttemptStatus.SUCCEEDED
        assert not list(
            db.scalars(
                select(WorkflowAttempt.id).where(WorkflowAttempt.predecessor_attempt_id == identity)
            )
        )
        if kind == "training":
            assert db.get(ModelRun, resource_id).status == RunStatus.SUCCEEDED


def test_retry_preserves_dataset_lineage_for_parent_and_active_children(expired):
    engine, run_id, parent_id, _, _ = expired
    with Session(engine) as db, db.begin():
        dataset_id = db.get(ModelRun, run_id).dataset_version_id
        old = list(
            db.scalars(select(WorkflowAttempt).where(WorkflowAttempt.model_run_id == run_id))
        )
        for row in old:
            row.dataset_version_id = dataset_id
        predecessors = {row.id for row in old}
    k8s = MagicMock()
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    k8s.ray_job.assert_not_called()
    with Session(engine) as db:
        successors = list(
            db.scalars(
                select(WorkflowAttempt).where(
                    WorkflowAttempt.predecessor_attempt_id.in_(predecessors)
                )
            )
        )
        assert len(successors) == len(predecessors) == 3
        for successor in successors:
            original = db.get(WorkflowAttempt, successor.predecessor_attempt_id)
            for name in (
                "dataset_version_id",
                "model_run_id",
                "trial_id",
                "scope_id",
                "conformance_campaign_id",
            ):
                assert getattr(successor, name) == getattr(original, name)
            assert successor.generation == original.generation + 1
            if successor.run_attempt_id is not None:
                assert (
                    db.get(WorkflowAttempt, successor.run_attempt_id).predecessor_attempt_id
                    == parent_id
                )


def test_native_ray_read_has_a_bounded_request_timeout():
    from types import SimpleNamespace

    from automl_api.services.kubernetes_training import KubernetesTrainingClient

    client = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    client._configured = True
    client.settings = SimpleNamespace(training_namespace="default")
    client.custom = MagicMock()
    client.ray_job("owned-job", namespace="owned-namespace")
    kwargs = client.custom.get_namespaced_custom_object.call_args.kwargs
    assert kwargs["name"] == "owned-job" and kwargs["namespace"] == "owned-namespace"
    assert kwargs["_request_timeout"] == 5
