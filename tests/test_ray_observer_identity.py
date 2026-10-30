"""Native identity observations must not mutate another logical workflow."""

import copy
from datetime import UTC, datetime

import pytest
from automl_api.models.datasets import ProfilingJob
from automl_api.models.enums import AttemptStatus, WorkflowStage
from automl_api.models.runs import ModelRun
from automl_api.models.workflows import OutboxEntry, WorkflowAttempt
from automl_api.workflow_reconciler import observe_once
from sqlalchemy import select
from sqlalchemy.orm import Session
from test_ray_observer_inspection import (
    committed_tenant as committed_tenant,
)
from test_ray_observer_inspection import (
    factory_for,
)
from test_ray_observer_inspection import (
    phase1_engine as phase1_engine,
)
from test_ray_observer_inspection import (
    profile_source as profile_source,
)
from test_ray_observer_inspection import (
    ray_scope as ray_scope,
)


def labeled_observation(attempt, namespace, status):
    labels = {
        "automl.platform/project-id": str(attempt.project_id),
        "automl.platform/attempt-id": str(attempt.id),
        "automl.platform/generation": str(attempt.generation),
    }
    if attempt.stage == WorkflowStage.TRAINING_RUN:
        labels["automl.platform/run-id"] = str(attempt.model_run_id)
    else:
        labels.update(
            {
                "automl.platform/profile-job-id": attempt.logical_key.split(":")[1],
                "automl.platform/workflow-stage": attempt.stage.value,
                "automl.platform/dataset-version-id": str(attempt.dataset_version_id),
            }
        )
    return {
        "metadata": {
            "name": attempt.ray_job_name,
            "namespace": namespace,
            "uid": "owned-fixture-ray-uid",
            "labels": labels,
        },
        "spec": {
            "jobId": f"sceptre-{attempt.fencing_token[:16]}",
            "submitterPodTemplate": {
                "spec": {
                    "serviceAccountName": attempt.workload_identity,
                    "containers": [
                        {
                            "name": "ray-job-submitter",
                            "env": [
                                {"name": "AUTOML_FENCING_TOKEN", "value": attempt.fencing_token},
                                {"name": "AUTOML_PROJECT_ID", "value": str(attempt.project_id)},
                                {"name": "AUTOML_ATTEMPT_ID", "value": str(attempt.id)},
                            ],
                        }
                    ],
                }
            },
        },
        "status": status,
    }


@pytest.fixture
def identified_scope(ray_scope):
    engine, identity, resource_id, kind, k8s = ray_scope
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, identity)
        job = labeled_observation(
            attempt,
            "observer-test",
            {"jobStatus": "RUNNING", "jobId": "observed-id", "rayClusterName": "owned-cluster"},
        )
    k8s.ray_job.return_value = job
    return ray_scope, job


def snapshot(scope):
    engine, identity, resource_id, kind, _ = scope
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, identity)
        resource = db.get(ModelRun if kind == "training" else ProfilingJob, resource_id)
        rows = [
            {
                column.key: copy.deepcopy(getattr(row, column.key))
                for column in row.__table__.columns
                if column.key != "last_observed_at"
            }
            for row in (attempt, resource)
        ]
        successors = list(
            db.scalars(
                select(WorkflowAttempt.id).where(WorkflowAttempt.predecessor_attempt_id == identity)
            )
        )
        deliveries = list(
            db.scalars(select(OutboxEntry.id).where(OutboxEntry.project_id == attempt.project_id))
        )
    return rows, successors, deliveries


@pytest.mark.parametrize("state", ["RUNNING", "FAILED"])
@pytest.mark.parametrize(
    "invalid",
    ["project-id", "attempt-id", "generation", "resource", "namespace", "name", "labels", "fence"],
)
def test_foreign_native_observation_cannot_change_committed_work(identified_scope, invalid, state):
    scope, job = identified_scope
    engine, _, _, kind, k8s = scope
    job["status"]["jobStatus"] = state
    if invalid in {"project-id", "attempt-id", "generation"}:
        job["metadata"]["labels"]["automl.platform/" + invalid] = "foreign"
    elif invalid == "resource":
        key = "run-id" if kind == "training" else "profile-job-id"
        job["metadata"]["labels"]["automl.platform/" + key] = "foreign"
    elif invalid == "labels":
        job["metadata"]["labels"] = None
    elif invalid == "fence":
        job["spec"]["jobId"] = "sceptre-foreign-fence"
    else:
        job["metadata"][invalid] = "foreign"
    before = snapshot(scope)
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    assert snapshot(scope) == before
    k8s.delete_ray_job.assert_not_called()


@pytest.mark.parametrize("uid", [None, "", "  ", 12])
def test_missing_or_malformed_native_uid_cannot_publish(identified_scope, uid):
    scope, job = identified_scope
    engine, _, _, _, k8s = scope
    job["metadata"]["uid"] = uid
    before = snapshot(scope)
    observe_once(factory_for(engine), k8s=k8s)
    assert snapshot(scope) == before


@pytest.mark.parametrize("ray_scope", ["splitter", "preparation"], indirect=True)
@pytest.mark.parametrize("field", ["workflow-stage", "dataset-version-id"])
@pytest.mark.parametrize("missing", [False, True])
def test_dataset_native_stage_and_version_must_match(identified_scope, field, missing):
    scope, job = identified_scope
    engine, _, _, _, k8s = scope
    key = "automl.platform/" + field
    if missing:
        job["metadata"]["labels"].pop(key)
    else:
        job["metadata"]["labels"][key] = "foreign"
    before = snapshot(scope)
    observe_once(factory_for(engine), k8s=k8s)
    assert snapshot(scope) == before


def test_owned_native_observation_publishes_expected_progress(identified_scope):
    scope, _ = identified_scope
    engine, identity, _, _, k8s = scope
    assert observe_once(factory_for(engine), k8s=k8s) == 1
    with Session(engine) as db:
        attempt = db.get(WorkflowAttempt, identity)
        assert attempt.status == AttemptStatus.RUNNING
        assert attempt.ray_submission_id == "observed-id"
        assert attempt.ray_cluster_name == "owned-cluster"
        assert attempt.lease_expires_at > datetime.now(UTC)


@pytest.mark.parametrize(
    "invalid",
    ["same_prefix", "missing", "duplicate", "service_account", "project_env", "attempt_env"],
)
def test_submitter_requires_full_fence_and_stage_identity(identified_scope, invalid):
    scope, job = identified_scope
    engine, _, _, _, k8s = scope
    pod = job["spec"]["submitterPodTemplate"]["spec"]
    env = pod["containers"][0]["env"]
    original = env[0]["value"]
    if invalid == "same_prefix":
        replacement = original[:16] + ("0" if original[16] != "0" else "1") + original[17:]
        env[0]["value"] = replacement
        assert replacement != original and replacement[:16] == original[:16]
        assert job["spec"]["jobId"] == f"sceptre-{replacement[:16]}"
    elif invalid == "missing":
        env.pop(0)
    elif invalid == "duplicate":
        env.append(dict(env[0]))
    elif invalid == "service_account":
        pod["serviceAccountName"] = "foreign-account"
    elif invalid == "project_env":
        env[1]["value"] = "foreign-project"
    else:
        env[2]["value"] = "foreign-attempt"
    before = snapshot(scope)
    observe_once(factory_for(engine), k8s=k8s)
    assert snapshot(scope) == before
