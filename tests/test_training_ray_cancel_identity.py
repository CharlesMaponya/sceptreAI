import copy
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from automl_api.models.enums import AttemptStatus
from automl_api.services import reconciler
from automl_api.services.kubernetes_training import KubernetesTrainingClient
from automl_api.services.workflow_state import StaleFence
from kubernetes.client import ApiException
from test_phase1_reconciler import _attempt, _entry, _run


def ray_job_identity(run, attempt):
    return {
        "metadata": {
            "name": attempt.ray_job_name,
            "namespace": run.k8s_namespace,
            "uid": str(uuid.uuid4()),
            "labels": {
                "automl.platform/project-id": str(attempt.project_id),
                "automl.platform/run-id": str(run.id),
                "automl.platform/attempt-id": str(attempt.id),
                "automl.platform/generation": str(attempt.generation),
            },
        }
    }


@pytest.fixture
def cancellation():
    run = _run()
    attempt = _attempt(run, AttemptStatus.RUNNING)
    attempt.ray_job_name = "existing-short-legacy-name"
    db = MagicMock()
    db.scalar.return_value = attempt
    db.get.return_value = run
    k8s = KubernetesTrainingClient.__new__(KubernetesTrainingClient)
    k8s._configured = True
    k8s.settings = SimpleNamespace(training_namespace="wrong-default-namespace")
    k8s.custom = MagicMock()
    job = ray_job_identity(run, attempt)
    k8s.custom.get_namespaced_custom_object.return_value = job
    return db, run, attempt, _entry(attempt, "ray.training.cancel"), k8s, job


@pytest.mark.parametrize("key", ["project-id", "run-id", "attempt-id", "generation"])
@pytest.mark.parametrize("missing", [False, True])
def test_foreign_or_incomplete_ray_labels_never_authorize_delete(cancellation, key, missing):
    db, _, attempt, entry, k8s, job = cancellation
    label = "automl.platform/" + key
    if missing:
        job["metadata"]["labels"].pop(label)
    else:
        job["metadata"]["labels"][label] = "foreign"
    with pytest.raises(StaleFence, match="identity"):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    k8s.custom.delete_namespaced_custom_object.assert_not_called()
    assert attempt.status == AttemptStatus.RUNNING and attempt.terminal_cas_version == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("uid", None),
        ("uid", ""),
        ("uid", "   "),
        ("uid", 12),
        ("name", "another-job"),
        ("name", None),
        ("namespace", "another-namespace"),
        ("namespace", None),
        ("labels", None),
        ("labels", []),
        ("labels", "bad"),
    ],
)
def test_malformed_ray_metadata_never_authorizes_delete(cancellation, field, value):
    db, _, attempt, entry, k8s, job = cancellation
    job["metadata"][field] = value
    with pytest.raises(StaleFence, match="identity"):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    k8s.custom.delete_namespaced_custom_object.assert_not_called()
    assert attempt.status == AttemptStatus.RUNNING


@pytest.mark.parametrize("job", [None, [], {"metadata": None}, {"metadata": []}])
def test_malformed_ray_response_never_authorizes_delete(cancellation, job):
    db, _, attempt, entry, k8s, _ = cancellation
    k8s.custom.get_namespaced_custom_object.return_value = job
    with pytest.raises(StaleFence, match="identity"):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    k8s.custom.delete_namespaced_custom_object.assert_not_called()
    assert attempt.status == AttemptStatus.RUNNING


@pytest.mark.parametrize(
    "status",
    [
        AttemptStatus.RUNNING,
        AttemptStatus.SUPERSEDED,
        AttemptStatus.FAILED,
        AttemptStatus.SUCCEEDED,
        AttemptStatus.CANCELLED,
    ],
)
def test_owned_ray_delete_uses_native_uid_and_stored_namespace(cancellation, status):
    db, run, attempt, entry, k8s, job = cancellation
    attempt.status = status
    reconciler.cancel_training_ray_job(db, entry, k8s)
    get = k8s.custom.get_namespaced_custom_object.call_args.kwargs
    assert get["namespace"] == run.k8s_namespace and get["name"] == attempt.ray_job_name
    assert get["_request_timeout"] == 5
    delete = k8s.custom.delete_namespaced_custom_object.call_args.kwargs
    assert delete["name"] == attempt.ray_job_name and delete["namespace"] == run.k8s_namespace
    assert delete["body"].preconditions.uid == job["metadata"]["uid"]
    assert delete["body"].propagation_policy == "Foreground"
    assert attempt.status == (
        AttemptStatus.CANCELLED if status == AttemptStatus.RUNNING else status
    )
    assert attempt.terminal_cas_version == (1 if status == AttemptStatus.RUNNING else 0)


@pytest.mark.parametrize(
    "error", [ApiException(status=403), ApiException(status=500), ConnectionError("unknown read")]
)
def test_native_read_error_preserves_attempt_for_retry(cancellation, error):
    db, _, attempt, entry, k8s, _ = cancellation
    k8s.custom.get_namespaced_custom_object.side_effect = error
    with pytest.raises(type(error)):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    k8s.custom.delete_namespaced_custom_object.assert_not_called()
    assert attempt.status == AttemptStatus.RUNNING


def test_already_absent_job_is_safe_idempotent_cancellation(cancellation):
    db, _, attempt, entry, k8s, _ = cancellation
    k8s.custom.get_namespaced_custom_object.side_effect = ApiException(status=404)
    for _ in range(2):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    k8s.custom.delete_namespaced_custom_object.assert_not_called()
    assert attempt.status == AttemptStatus.CANCELLED and attempt.terminal_cas_version == 1


def test_same_name_replacement_between_read_and_delete_survives(cancellation):
    db, _, attempt, entry, k8s, job = cancellation
    original_uid = job["metadata"]["uid"]
    replacement = copy.deepcopy(job)
    replacement["metadata"]["uid"] = str(uuid.uuid4())

    def native_delete(**kwargs):
        assert kwargs["body"].preconditions.uid == original_uid
        assert kwargs["body"].preconditions.uid != replacement["metadata"]["uid"]
        raise ApiException(status=409, reason="UID precondition failed")

    k8s.custom.delete_namespaced_custom_object.side_effect = native_delete
    with pytest.raises(ApiException) as caught:
        reconciler.cancel_training_ray_job(db, entry, k8s)
    assert caught.value.status == 409
    assert attempt.status == AttemptStatus.RUNNING and attempt.terminal_cas_version == 0


def test_disappearance_after_read_is_safe(cancellation):
    db, _, attempt, entry, k8s, _ = cancellation
    k8s.custom.delete_namespaced_custom_object.side_effect = ApiException(status=404)
    reconciler.cancel_training_ray_job(db, entry, k8s)
    assert attempt.status == AttemptStatus.CANCELLED


def test_lost_delete_reply_then_404_does_not_issue_a_second_delete(cancellation):
    db, _, attempt, entry, k8s, job = cancellation
    k8s.custom.get_namespaced_custom_object.side_effect = [job, ApiException(status=404)]
    k8s.custom.delete_namespaced_custom_object.side_effect = ConnectionError("lost ACK")
    with pytest.raises(ConnectionError):
        reconciler.cancel_training_ray_job(db, entry, k8s)
    assert attempt.status == AttemptStatus.RUNNING
    reconciler.cancel_training_ray_job(db, entry, k8s)
    assert k8s.custom.delete_namespaced_custom_object.call_count == 1
    assert attempt.status == AttemptStatus.CANCELLED and attempt.terminal_cas_version == 1
